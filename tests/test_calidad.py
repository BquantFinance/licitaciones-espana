"""Tests del pipeline de calidad sobre la semántica corregida de PLACSP."""

import argparse
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("calidad_licitaciones", RAIZ / "calidad" / "calidad_licitaciones.py")
calidad = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(calidad)


def _publicado():
    """Esquema de licitaciones_espana.parquet (v2026.02): importe_sin_iva = valor estimado."""
    return pd.DataFrame({
        "id": ["a", "a", "b", "c"],
        "expediente": ["E1", "E1", "E2", "E3"],
        "conjunto": ["licitaciones", "licitaciones", "licitaciones", "menores"],
        "tipo_contrato_code": [2.0, 2.0, 1.0, 2.0],
        "tipo_contrato": ["Servicios"] * 4,
        "procedimiento_code": [4.0, 4.0, 7.0, 6.0],
        "procedimiento": ["Negociado sin publicidad", "Negociado sin publicidad", "Contrato menor", "Asociación innovación"],
        "estado_code": ["PUB", "RES", "RES", "RES"],
        "estado": ["Publicada", "Resuelta", "Resuelta", "Resuelta"],
        # valor estimado (con prórrogas) muy superior al presupuesto con IVA
        "importe_sin_iva": [500000.0, 500000.0, np.nan, np.nan],
        "importe_con_iva": [121000.0, 121000.0, 60500.0, 1210.0],
        "importe_adjudicacion": [np.nan, 300000.0, 40000.0, 1000.0],
        "importe_adj_con_iva": [np.nan, 363000.0, 48400.0, 1210.0],
        "num_ofertas": [np.nan, 2, 3, 1],
        "fecha_updated": pd.to_datetime(["2024-01-01", "2024-05-01", "2024-02-01", "2024-03-01"], utc=True),
    })


def test_run_deduplica_y_usa_la_semantica_correcta(tmp_path):
    entrada = tmp_path / "nacional.parquet"
    _publicado().to_parquet(entrada, index=False)
    args = argparse.Namespace(input=str(entrada), output=str(tmp_path / "out"), sample=None,
                              ted=None, borme=None, sin_deduplicar=False)
    calidad.run(args)
    res = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")

    # Una fila por licitación (la versión 'RES' de 'a')
    assert res["id"].tolist() == ["a", "b", "c"]
    a = res.iloc[0]
    assert a["estado"] == "Resuelta"
    assert a["valor_estimado_contrato"] == 500000.0
    # Adjudicado 363k con IVA frente a un presupuesto de 121k con IVA: antes se
    # comparaba 300k sin IVA con el valor estimado (500k) y pasaba
    assert not a["INT-CONS-08"]
    assert a["INT-VAL-01"]
    # El código 7 es 'Derivado de acuerdo marco': ya no cuenta como contrato menor (VAL-14)
    b = res.iloc[1]
    assert b["procedimiento"] == "Derivado de acuerdo marco"
    assert b["INT-VAL-14"]
    assert res.iloc[2]["procedimiento"] == "Contrato menor"


def test_sin_deduplicar_conserva_todas_las_filas(tmp_path):
    entrada = tmp_path / "nacional.parquet"
    _publicado().to_parquet(entrada, index=False)
    args = argparse.Namespace(input=str(entrada), output=str(tmp_path / "out"), sample=None,
                              ted=None, borme=None, sin_deduplicar=True)
    calidad.run(args)
    res = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")
    assert len(res) == 4


def test_cons08_usa_el_par_con_iva_si_falta_sin_iva():
    df = pd.DataFrame({
        "importe_sin_iva": [np.nan, 100.0],
        "importe_adjudicacion": [90.0, 90.0],
        "importe_con_iva": [121.0, np.nan],
        "importe_adj_con_iva": [200.0, np.nan],
    })
    r = calidad.calcular_indicadores_base(df)
    assert r["INT-CONS-08"].tolist() == [False, True]
