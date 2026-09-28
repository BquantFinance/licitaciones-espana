"""Importes corregidos junto a los publicados (calidad/correcciones.py, issue #22)."""

import re

import numpy as np
import pandas as pd
import pytest

from calidad import correcciones

URDINBERRI = "https://contrataciondelestado.es/sindicacion/PlataformasAgregadasSinMenores/15091104"


def _lista(s):
    """Valores de una columna con None en los nulos (NaN, None o pd.NA según pandas)."""
    return [None if pd.isna(v) else v for v in s]


def _importes(lic, adj, **otras):
    return pd.DataFrame({"id": [f"x{i}" for i in range(len(lic))],
                         "importe_sin_iva": lic, "importe_adjudicacion": adj, **otras})


def test_urdinberri_se_corrige_solo_en_las_versiones_con_el_valor_erroneo():
    df = pd.DataFrame({
        "id": [URDINBERRI] * 3 + ["otro"],
        # antes de adjudicar; adjudicada (publicado en origen); y, supuesta, ya corregida
        "importe_sin_iva": [2518819.27] * 4,
        "valor_estimado_contrato": [25188819.27, 25188819.27, 2518819.27, 25188819.27],
        "importe_adjudicacion": [np.nan, 2357531666.0, 2357531.67, 2357531666.0],
    }, index=[10, 20, 30, 40])
    antes = df.copy()
    c = correcciones.corregir_importes(df, correcciones.cargar_registro())

    pd.testing.assert_frame_equal(df, antes)  # lo publicado no se toca
    assert c.index.tolist() == [10, 20, 30, 40]
    adj = c["importe_adjudicacion_corregido"]
    assert pd.isna(adj.iloc[0]) and adj.iloc[1] == pytest.approx(2357531.666) and adj.iloc[2] == 2357531.67
    assert _lista(c["correccion_importe_adjudicacion"])[:3] == [None, "registro", None]
    assert c["valor_estimado_contrato_corregido"].tolist()[:3] == [2518819.27] * 3
    assert _lista(c["correccion_valor_estimado_contrato"])[:3] == ["registro", "registro", None]
    # Otro id con los mismos importes: no está en el registro; la regla de escala
    # lo corrige igual (93,6 % del presupuesto al dividir entre 1.000)
    assert c["correccion_importe_adjudicacion"].iloc[3] == "escala_x1000"
    assert c["correccion_valor_estimado_contrato"].isna().iloc[3]
    assert c["importe_sin_iva_corregido"].tolist() == df["importe_sin_iva"].tolist()
    assert c["correccion_importe_sin_iva"].isna().all()


@pytest.mark.parametrize("lic, adj, motivo, corregido", [
    # la coma perdida con las mismas cifras que el presupuesto (EGURKI, un menor)
    (469841.73, 46984173.0, "escala_x100", 469841.73),
    (5572.0, 557200.0, "escala_x100", 5572.0),
    # entre 1.000 y 10.000 EUR solo si las cifras coinciden
    (3999.28, 3999280.0, "escala_x1000", 3999.28),
    (3999.28, 3999728.0, "inverosimil", np.nan),
    # desde 10.000 EUR, por banda: RED ESPAÑOLA (81,7 % del presupuesto) y el borde
    (85702.48, 70000000.0, "escala_x1000", 70000.0),
    (100000.0, 10500000.0, "escala_x100", 105000.0),
    (100000.0, 50000000.0, "escala_x1000", 50000.0),
    (81628.56, 24882980.0, "inverosimil", np.nan),     # 30 % del presupuesto: fuera de banda
    (55700.0, 9000000.0, "inverosimil", np.nan),       # 161 veces: ni x100 ni x1000
    (22000.0, 1954024000.0, "inverosimil", np.nan),    # 88.819 veces
    # por debajo del salto no se toca
    (100000.0, 9999999.0, None, 9999999.0),
    (100000.0, 95000.0, None, 95000.0),
])
def test_salto_de_escala_en_la_adjudicacion(lic, adj, motivo, corregido):
    c = correcciones.corregir_importes(_importes([lic], [adj]))
    m = c["correccion_importe_adjudicacion"].iloc[0]
    assert (None if pd.isna(m) else m) == motivo
    v = c["importe_adjudicacion_corregido"].iloc[0]
    assert (np.isnan(v) and np.isnan(corregido)) or v == pytest.approx(corregido)
    assert c["correccion_importe_sin_iva"].isna().all()


def test_presupuesto_simbolico_se_vacia_y_la_adjudicacion_se_mantiene():
    # IRIZAR: presupuesto de 1 EUR y 24,45 M€ adjudicados; 0,10 EUR de precio unitario
    c = correcciones.corregir_importes(_importes([1.0, 0.10, 999.0], [24450000.0, 2831400.0, 100000.0]))
    assert c["importe_adjudicacion_corregido"].tolist() == [24450000.0, 2831400.0, 100000.0]
    assert c["correccion_importe_adjudicacion"].isna().all()
    assert c["importe_sin_iva_corregido"].isna().all()
    assert _lista(c["correccion_importe_sin_iva"]) == ["no_comparable"] * 3


def test_sin_par_no_hay_correccion():
    c = correcciones.corregir_importes(_importes([np.nan, 0.0, 1000.0, 1000.0], [5.0, 5000.0, np.nan, 0.0]))
    assert c["correccion_importe_adjudicacion"].isna().all()
    assert c["correccion_importe_sin_iva"].isna().all()
    assert c["importe_sin_iva_corregido"].tolist()[1:] == [0.0, 1000.0, 1000.0]


def test_salto_por_el_par_con_iva_no_se_corrige_por_escala():
    # sin presupuesto sin IVA: el salto se mide con IVA y la adjudicación sin IVA
    # no se reescala con un cociente de otra base
    df = _importes([np.nan], [1000000.0], importe_con_iva=[1210.0], importe_adj_con_iva=[1210000.0])
    c = correcciones.corregir_importes(df)
    assert c["correccion_importe_adjudicacion"].iloc[0] == "inverosimil"
    assert np.isnan(c["importe_adjudicacion_corregido"].iloc[0])


def test_registro_sin_valor_probable_deja_el_campo_vacio(tmp_path):
    ruta = tmp_path / "errores.csv"
    pd.DataFrame([{"fuente": "placsp", "id": "x0", "campo": "importe_adjudicacion",
                   "valor_publicado": "500", "valor_probable": "", "certeza": "probable",
                   "evidencia": "e", "referencia": "r", "verificado": "2026-09-28"},
                  {"fuente": "otra", "id": "x1", "campo": "importe_adjudicacion",
                   "valor_publicado": "700", "valor_probable": "7", "certeza": "probable",
                   "evidencia": "e", "referencia": "r", "verificado": "2026-09-28"}]).to_csv(ruta, index=False)
    registro = correcciones.cargar_registro(ruta)
    assert registro["id"].tolist() == ["x0"]  # solo la fuente pedida
    c = correcciones.corregir_importes(_importes([1000.0, 1000.0], [500.0, 700.0]), registro)
    assert np.isnan(c["importe_adjudicacion_corregido"].iloc[0])
    assert _lista(c["correccion_importe_adjudicacion"]) == ["registro", None]


def test_registro_ausente_o_incompleto(tmp_path):
    assert correcciones.cargar_registro(tmp_path / "no_existe.csv") is None
    assert correcciones.cargar_registro("") is None
    ruta = tmp_path / "mal.csv"
    pd.DataFrame({"fuente": ["placsp"], "id": ["a"]}).to_csv(ruta, index=False)
    with pytest.raises(ValueError, match="faltan"):
        correcciones.cargar_registro(ruta)


def test_sin_columnas_de_importes():
    c = correcciones.corregir_importes(pd.DataFrame({"id": ["a"], "objeto": ["x"]}))
    assert c.shape == (1, 0)
    # solo la adjudicación (tabla de resultados): solo se aplica el registro
    r = pd.DataFrame({"id": [URDINBERRI, "b"], "importe_adjudicacion": [2357531666.0, 2357531666.0]})
    c = correcciones.corregir_importes(r, correcciones.cargar_registro())
    assert c.columns.tolist() == ["importe_adjudicacion_corregido", "correccion_importe_adjudicacion"]
    assert _lista(c["correccion_importe_adjudicacion"]) == ["registro", None]


def test_resumen():
    c = correcciones.corregir_importes(_importes([1.0, 469841.73, 100.0], [24450000.0, 46984173.0, 90.0]))
    r = correcciones.resumen(c).set_index(["campo", "motivo"])["filas"].to_dict()
    assert r == {("importe_sin_iva", "no_comparable"): 1, ("importe_adjudicacion", "escala_x100"): 1}


def test_el_registro_del_repo_es_valido():
    reg = pd.read_csv(correcciones.REGISTRO, dtype=str, keep_default_na=False)
    assert reg.columns.tolist() == correcciones.COLUMNAS_REGISTRO
    assert not reg.duplicated(["fuente", "id", "campo", "valor_publicado"]).any()
    assert set(reg["fuente"]) <= {"placsp", "euskadi_api"}
    assert set(reg["certeza"]) <= {"confirmado", "probable"}
    for f in reg.itertuples(index=False):
        float(f.valor_publicado)
        assert f.valor_probable == "" or float(f.valor_probable) != float(f.valor_publicado)
        assert f.evidencia.strip() and f.referencia.startswith("https://")
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", f.verificado)
        if f.fuente == "placsp":
            assert f.campo in correcciones.CAMPOS_CORREGIBLES
            assert f.id.startswith("https://contrataciondelestado.es/sindicacion/")
