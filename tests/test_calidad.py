"""Tests del pipeline de calidad sobre la semántica corregida de PLACSP."""

import argparse
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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


def test_run_evalua_todas_las_filas_con_la_semantica_correcta(tmp_path):
    entrada = tmp_path / "nacional.parquet"
    _publicado().to_parquet(entrada, index=False)
    args = argparse.Namespace(input=str(entrada), output=str(tmp_path / "out"), sample=None,
                              ted=None, borme=None, solo_ultima_version=False)
    calidad.run(args)
    todas = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")

    # Se evalúan todas las entradas publicadas, marcadas por versión
    assert todas["id"].tolist() == ["a", "a", "b", "c"]
    assert todas["es_ultima_version"].tolist() == [False, True, True, True]
    res = todas[todas["es_ultima_version"]].reset_index(drop=True)
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


def test_solo_ultima_version(tmp_path):
    entrada = tmp_path / "nacional.parquet"
    _publicado().to_parquet(entrada, index=False)
    args = argparse.Namespace(input=str(entrada), output=str(tmp_path / "out"), sample=None,
                              ted=None, borme=None, solo_ultima_version=True)
    calidad.run(args)
    res = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")
    assert res["id"].tolist() == ["a", "b", "c"]


def test_cons08_usa_el_par_con_iva_si_falta_sin_iva():
    df = pd.DataFrame({
        "importe_sin_iva": [np.nan, 100.0],
        "importe_adjudicacion": [90.0, 90.0],
        "importe_con_iva": [121.0, np.nan],
        "importe_adj_con_iva": [200.0, np.nan],
    })
    r = calidad.calcular_indicadores_base(df)
    assert r["INT-CONS-08"].tolist() == [False, True]


def test_fia12_salto_de_escala_entre_presupuesto_y_adjudicacion():
    df = pd.DataFrame({
        # URDINBERRI (issue #22), normal, justo 100 veces, sin presupuesto,
        # presupuesto 0, adjudicación 0 y el par con IVA si falta el sin IVA
        "importe_sin_iva": [2518819.27, 100.0, 100.0, np.nan, 0.0, 100.0, np.nan],
        "importe_adjudicacion": [2357531666.0, 99.0, 10000.0, 5.0, 5.0, 0.0, np.nan],
        "importe_con_iva": [np.nan] * 6 + [121.0],
        "importe_adj_con_iva": [np.nan] * 6 + [12100.0],
    })
    r = calidad.calcular_indicadores_base(df)["INT-FIA-12"]
    assert r.tolist()[:3] == [False, True, False]
    assert r.iloc[3:6].isna().all()
    assert r.iloc[6] == False  # noqa: E712
    # CONS-08 marca lo mismo y además los sobrecostes pequeños (5 %)
    assert calidad.calcular_indicadores_base(df)["INT-CONS-08"].tolist() == [False, True, False, True, True, True, False]


def _lista(s):
    """Valores de una columna con None en los nulos (NaN, None o pd.NA según pandas)."""
    return [None if pd.isna(v) else v for v in s]


def test_run_sirve_lo_publicado_y_al_lado_lo_corregido(tmp_path):
    urdinberri = "https://contrataciondelestado.es/sindicacion/PlataformasAgregadasSinMenores/15091104"
    df = pd.DataFrame({
        "id": [urdinberri, urdinberri, "irizar"],
        "expediente": ["01/2024", "01/2024", "E9"],
        "conjunto": ["agregacion", "agregacion", "licitaciones"],
        "tipo_contrato_code": [3.0, 3.0, 1.0],
        "procedimiento_code": [1.0, 1.0, 1.0],
        "estado_code": ["PUB", "RES", "RES"],
        "valor_estimado_contrato": [25188819.27, 25188819.27, 1.0],
        "importe_sin_iva": [2518819.27, 2518819.27, 1.0],
        "importe_con_iva": [np.nan, np.nan, 1.21],
        "importe_adjudicacion": [np.nan, 2357531666.0, 24450000.0],
        "importe_adj_con_iva": [np.nan, np.nan, 29584500.0],
        "num_ofertas": [np.nan, 4, 3],
        "fecha_updated": pd.to_datetime(["2024-06-10", "2025-03-11", "2024-01-01"], utc=True),
    })
    entrada = tmp_path / "nacional.parquet"
    df.to_parquet(entrada, index=False)
    args = argparse.Namespace(input=str(entrada), output=str(tmp_path / "out"), sample=None,
                              ted=None, borme=None, solo_ultima_version=False)
    calidad.run(args)
    res = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")

    # Lo publicado, intacto; lo corregido, al lado con su motivo
    assert res["importe_adjudicacion"].iloc[1] == 2357531666.0
    assert res["importe_adjudicacion_corregido"].iloc[1] == pytest.approx(2357531.666)
    assert _lista(res["correccion_importe_adjudicacion"]) == [None, "registro", None]
    assert res["valor_estimado_contrato"].tolist()[:2] == [25188819.27] * 2
    assert res["valor_estimado_contrato_corregido"].tolist() == [2518819.27, 2518819.27, 1.0]
    assert _lista(res["correccion_valor_estimado_contrato"]) == ["registro", "registro", None]
    # IRIZAR: el presupuesto de 1 EUR no es comparable; la adjudicación se mantiene
    assert res["importe_adjudicacion_corregido"].iloc[2] == 24450000.0
    assert np.isnan(res["importe_sin_iva_corregido"].iloc[2])
    assert _lista(res["correccion_importe_sin_iva"]) == [None, None, "no_comparable"]
    # Los indicadores evalúan lo publicado
    assert _lista(res["INT-FIA-12"]) == [None, False, False]
    assert not res["INT-CONS-08"].iloc[1]

    # Sin registro, la regla de escala corrige igual la adjudicación
    args.errores_fuente = ""
    calidad.run(args)
    res = pd.read_parquet(tmp_path / "out" / "calidad_licitaciones_resultado.parquet")
    assert _lista(res["correccion_importe_adjudicacion"]) == [None, "escala_x1000", None]
    assert res["correccion_valor_estimado_contrato"].isna().all()


def _nacional_cons20():
    return pd.DataFrame({
        "id": ["a", "a", "b", "e", "d"],
        "expediente": ["E1", "E1", "E2", "E2", "E4"],
        "nif_adjudicatario": ["B1", "B1", "B2", "B2", "B4"],
        "fecha_updated": pd.to_datetime(["2024-01-01", "2024-05-01", "2024-02-01", "2024-03-01",
                                         "2026-02-01"], utc=True),
    })


def test_cons20_se_une_por_version_y_respeta_la_cobertura_de_ted(tmp_path):
    ted = pd.DataFrame({
        "id": ["a", "b", "d"], "expediente": ["E1", "E2", "E4"], "nif_adjudicatario": ["B1", "B2", "B4"],
        # otra resolución que el nacional: la unión no depende de ella
        "fecha_updated": pd.to_datetime(["2024-05-01", "2024-02-01", "2026-02-01"], utc=True
                                        ).astype("datetime64[us, UTC]"),
        "_ted_validated": [True, False, False],
        "_ted_missing": [False, True, False],
        "_match_strategy": ["E1_E2", "", ""],
        "_ted_anio_cubierto": [True, True, False],
    })
    ruta = tmp_path / "crossval_sara.parquet"
    ted.to_parquet(ruta, index=False)
    res = calidad.calcular_cons20(_nacional_cons20(), str(ruta))
    # a: solo la versión evaluada (la última); b: missing; e: mismo expediente y
    # adjudicatario que b pero otro id, no hereda; d: 2026 sin TED, sin evaluar
    assert res.tolist()[1:3] == [True, False]
    assert pd.isna(res.iloc[0]) and pd.isna(res.iloc[3]) and pd.isna(res.iloc[4])

    # Un cruce antiguo (sin id ni fecha_updated) sigue uniéndose por expediente|adjudicatario
    ruta_antigua = tmp_path / "crossval_antiguo.parquet"
    ted.drop(columns=["id", "fecha_updated", "_ted_anio_cubierto"]).to_parquet(ruta_antigua, index=False)
    antiguo = calidad.calcular_cons20(_nacional_cons20(), str(ruta_antigua))
    assert antiguo.tolist() == [True, True, False, False, False]
