"""Tests de comun/historico.py (control del sesgo del superviviente)."""
import os

import pandas as pd
import pytest

from comun import historico as h


def _mtime(ruta, epoch):
    os.utime(ruta, (epoch, epoch))


# ── guardar_version ──────────────────────────────────────────

def test_guardar_version_nuevo_sin_cambios_y_actualizado(tmp_path):
    destino = tmp_path / "datos" / "contratos_2025.csv"
    assert h.guardar_version(destino, b"a;b\n1;2\n") == "nuevo"
    _mtime(destino, 1_700_000_000)                     # 2023-11-14T22:13:20Z

    assert h.guardar_version(destino, b"a;b\n1;2\n") == "sin_cambios"
    assert not (destino.parent / h.HISTORICO).exists()

    assert h.guardar_version(destino, b"a;b\n1;2\n3;4\n") == "actualizado"
    antigua = destino.parent / h.HISTORICO / "contratos_2025__20231114T221320Z.csv"
    assert antigua.read_bytes() == b"a;b\n1;2\n"       # la versión anterior no se pierde
    assert destino.read_bytes() == b"a;b\n1;2\n3;4\n"
    assert h.versiones(destino) == [antigua, destino]
    assert not list(destino.parent.glob(".*"))         # sin temporales


def test_guardar_version_desde_fichero_y_sellos_repetidos(tmp_path):
    destino = tmp_path / "x.zip"
    for i, contenido in enumerate([b"v1", b"v2", b"v3"]):
        part = tmp_path / "x.zip.part"
        part.write_bytes(contenido)
        h.guardar_version(destino, desde=part)
        _mtime(destino, 1_700_000_000)                  # mismo sello para todas
        assert not part.exists()
    assert [p.read_bytes() for p in h.versiones(destino)] == [b"v1", b"v2", b"v3"]


def test_guardar_version_exige_contenido_o_desde(tmp_path):
    with pytest.raises(ValueError):
        h.guardar_version(tmp_path / "a")
    with pytest.raises(ValueError):
        h.guardar_version(tmp_path / "a", b"x", desde=tmp_path / "b")


# ── acumular ─────────────────────────────────────────────────

def _df(filas, cols=("id", "importe")):
    return pd.DataFrame(filas, columns=list(cols))


def test_acumular_primera_descarga():
    out = h.acumular(None, _df([["1", "10"], ["2", "20"]]), "2026-01-01")
    assert out["_primera_descarga"].tolist() == ["2026-01-01"] * 2
    assert out["_en_ultima_descarga"].tolist() == [True, True]


def test_registro_retirado_y_modificado_se_conservan():
    t1 = h.acumular(None, _df([["1", "10"], ["2", "20"], ["3", "30"]]), "2026-01-01")
    # la administración retira el 2 y cambia el importe del 3
    t2 = h.acumular(t1, _df([["1", "10"], ["3", "35"]]), "2026-02-01")
    assert t2[["id", "importe", "_primera_descarga", "_ultima_descarga",
               "_en_ultima_descarga"]].values.tolist() == [
        ["1", "10", "2026-01-01", "2026-02-01", True],
        ["2", "20", "2026-01-01", "2026-01-01", False],    # retirado: sigue ahí
        ["3", "30", "2026-01-01", "2026-01-01", False],    # versión antigua
        ["3", "35", "2026-02-01", "2026-02-01", True],     # versión nueva
    ]


def test_registro_que_vuelve_a_aparecer_se_reactiva_sin_duplicarse():
    t1 = h.acumular(None, _df([["1", "10"], ["2", "20"]]), "d1")
    t2 = h.acumular(t1, _df([["1", "10"]]), "d2")
    t3 = h.acumular(t2, _df([["1", "10"], ["2", "20"]]), "d3")
    assert len(t3) == 2
    assert t3["_en_ultima_descarga"].tolist() == [True, True]
    assert t3["_ultima_descarga"].tolist() == ["d3", "d3"]


def test_duplicados_de_origen_como_multiconjunto():
    t1 = h.acumular(None, _df([["1", "10"], ["1", "10"]]), "d1")   # servido dos veces
    t2 = h.acumular(t1, _df([["1", "10"]]), "d2")                  # ahora una
    assert len(t2) == 2
    assert t2["_en_ultima_descarga"].tolist() == [True, False]
    t3 = h.acumular(t2, _df([["1", "10"], ["1", "10"], ["1", "10"]]), "d3")
    assert len(t3) == 3
    assert t3["_en_ultima_descarga"].tolist() == [True, True, True]


def test_ambito_limita_lo_que_se_marca_como_retirado():
    cols = ("anio", "id")
    t1 = h.acumular(None, _df([["2024", "a"], ["2025", "b"]], cols), "d1")
    # solo se vuelve a descargar 2025 (y ya no trae 'b')
    t2 = h.acumular(t1, _df([["2025", "c"]], cols), "d2", ambito=["anio"])
    estado = dict(zip(t2["id"], t2["_en_ultima_descarga"]))
    assert estado == {"a": True, "b": False, "c": True}   # 2024 no se ha comprobado


def test_columnas_nuevas_ignoradas_y_tipos():
    t1 = h.acumular(None, pd.DataFrame({"id": [1, 2], "_fecha_descarga": ["d1", "d1"]}), "d1")
    nuevos = pd.DataFrame({"id": ["1", "2"], "cpv": ["0913", None],
                           "_fecha_descarga": ["d2", "d2"]})
    t2 = h.acumular(t1, nuevos, "d2")
    assert len(t2) == 2                                   # 1 y "1" son el mismo valor
    assert t2["cpv"].iloc[0] == "0913" and pd.isna(t2["cpv"].iloc[1])   # columna nueva rellenada
    assert t2["_fecha_descarga"].tolist() == ["d1", "d1"] # no rompe la igualdad


def test_descarga_vacia_no_retira_nada():
    t1 = h.acumular(None, _df([["1", "10"]]), "d1")
    with pytest.raises(ValueError):
        h.acumular(t1, _df([]), "d2")
    t2 = h.acumular(t1, _df([]), "d2", permitir_vacio=True)
    assert t2["_en_ultima_descarga"].tolist() == [False]


def test_guardar_y_leer_registros_conserva_version_anterior(tmp_path):
    ruta = tmp_path / "registros.parquet"
    assert h.leer_registros(ruta) is None
    t1 = h.acumular(None, _df([["1", "10"]]), "d1")
    assert h.guardar_registros(t1, ruta) == "nuevo"
    t2 = h.acumular(h.leer_registros(ruta), _df([["2", "20"]]), "d2")
    assert h.guardar_registros(t2, ruta) == "actualizado"
    assert len(h.versiones(ruta)) == 2
    assert h.leer_registros(ruta)["id"].tolist() == ["1", "2"]
