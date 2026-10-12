"""scripts/esquema_release.py: esquema de una release leyendo solo los pies de los Parquet.

La release de prueba reproduce la estructura real (una carpeta por ZIP, la PLACSP partida en
licitaciones_completo/, carpetas v2026.02/ con Parquet y con ficheros que no son Parquet, LEEME.txt y
MANIFEST.csv de cada ZIP, una carpeta _historico/ que no es de la release) y los tipos de columna publicados.
"""

import csv
import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

RAIZ = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("esquema_release", RAIZ / "scripts" / "esquema_release.py")
er = importlib.util.module_from_spec(_spec)
sys.modules["esquema_release"] = er
_spec.loader.exec_module(er)


def _parquet(ruta, columnas, filas, grupo=None):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tabla = pa.table({n: pa.array([v] * filas, type=t) for n, (t, v) in columnas.items()})
    pq.write_table(tabla, ruta, row_group_size=grupo)


PLACSP = {"id": (pa.large_string(), "x"), "ano": (pa.float64(), 2021.0), "es_ultima_version": (pa.bool_(), True),
          "importe_adjudicacion": (pa.float64(), 1.5), "_origen": (pa.large_string(), None),
          "_en_ultima_descarga": (pa.bool_(), True)}


@pytest.fixture
def release(tmp_path):
    d = tmp_path / "release"
    for parte, filas in (("licitaciones_completo_2020_2021", 3), ("licitaciones_completo_2022_2023", 2)):
        z = "nacional_licitaciones_" + parte.split("completo_")[1]
        _parquet(d / z / "licitaciones_completo" / f"{parte}.parquet", PLACSP, filas, grupo=2)
        (d / z / "LEEME.txt").write_text("qué es\n", encoding="utf-8")
    _parquet(d / "ted" / "ted_es_can.parquet",
             {"ted_notice_id": (pa.string(), "1-2024"), "cancelled": (pa.string(), "0"),
              "_xml_eforms": (pa.string(), None), "importe_adjudicacion_corregido": (pa.float64(), 1.0),
              "correccion_importe_adjudicacion": (pa.string(), None), "_columna_extra_3": (pa.string(), ""),
              "_raro": (pa.string(), "a|b"), "dt_award": (pa.timestamp("us", tz="UTC"), None),
              "importe_texto": (pa.string(), "1.234,56"), "_importe_num": (pa.float64(), 1234.56)}, 4)
    _parquet(d / "ted" / "v2026.02" / "crossval_sara.parquet", {"id": (pa.string(), "z")}, 7)
    (d / "ted" / "v2026.02" / "LEEME_v2026.02.md").write_text("notas\n", encoding="utf-8")
    (d / "ted" / "MANIFEST.csv").write_text("fichero,filas\n", encoding="utf-8")
    _parquet(d / "ted" / "_historico" / "ted_es_can__20260101T000000Z.parquet", {"x": (pa.int64(), 1)}, 9)
    (d / "sin_parquet").mkdir()
    (d / "sin_parquet" / "LEEME.txt").write_text("nada\n", encoding="utf-8")
    return d


def _manifest(release, ruta, cambios=None, quitar=(), anadir=()):
    filas = []
    for zip_, items in er.describir(release).items():
        for it in items:
            filas.append({"zip": zip_, "fichero": it["fichero"],
                          "filas": it["filas"] if it["parquet"] else "",
                          "columnas": len(it["columnas"]) if it["parquet"] else ""})
    for f in filas:
        f.update((cambios or {}).get(f["fichero"], {}))
    filas = [f for f in filas if f["fichero"] not in quitar] + list(anadir)
    with open(ruta, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, ["zip", "fichero", "filas", "columnas", "bytes", "sha256", "nota"])
        w.writeheader()
        w.writerows(filas)
    return ruta


def test_describe_la_estructura_real(release):
    r = er.describir(release)
    assert list(r) == ["nacional_licitaciones_2020_2021.zip", "nacional_licitaciones_2022_2023.zip", "ted.zip"]
    ted = {i["fichero"]: i for i in r["ted.zip"]}
    assert set(ted) == {"ted_es_can.parquet", "v2026.02/crossval_sara.parquet", "v2026.02/LEEME_v2026.02.md"}
    assert ted["ted_es_can.parquet"]["filas"] == 4 and ted["ted_es_can.parquet"]["columnas"][0] == ("ted_notice_id", "string")
    assert ted["v2026.02/crossval_sara.parquet"]["v2026_02"] and not ted["ted_es_can.parquet"]["v2026_02"]
    assert not ted["v2026.02/LEEME_v2026.02.md"]["parquet"]
    parte = r["nacional_licitaciones_2020_2021.zip"][0]
    assert parte["fichero"] == "licitaciones_completo/licitaciones_completo_2020_2021.parquet"
    assert (parte["filas"], parte["grupos"]) == (3, 2)


def test_solo_lee_los_pies(release, monkeypatch):
    def prohibido(*a, **k):
        raise AssertionError("ha leído datos, no solo el pie")
    monkeypatch.setattr(pq, "read_table", prohibido)
    monkeypatch.setattr(pq.ParquetFile, "read", prohibido)
    monkeypatch.setattr(pq.ParquetFile, "read_row_group", prohibido)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", prohibido)
    assert er.describir(release)["ted.zip"]


def test_paginas_y_marcas(release, tmp_path):
    salida = tmp_path / "docs" / "esquema"
    assert er.main(["--datos", str(release), "--release", "v2026.10", "--salida", str(salida)]) == 0
    assert sorted(p.name for p in salida.iterdir()) == [
        "README.md", "nacional_licitaciones_2020_2021.md", "nacional_licitaciones_2022_2023.md", "ted.md"]
    ted = (salida / "ted.md").read_text(encoding="utf-8")
    assert "# `ted.zip`" in ted and "[ficha de la fuente](../fuentes/ted.md)" in ted
    assert "4 filas · 10 columnas · 1 grupo de filas" in ted
    assert "`importe_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos)" in ted
    assert "`_importe_num` | `double` | [marca](../ESQUEMA.md#valores-convertidos)" in ted
    assert "| 1 | `ted_notice_id` | `string` |  |" in ted
    assert "| 8 | `dt_award` | `timestamp[us, tz=UTC]` |  |" in ted
    assert "`_xml_eforms` | `string` | [marca](../ESQUEMA.md#ted)" in ted
    assert "`importe_adjudicacion_corregido` | `double` | [marca](../ESQUEMA.md#importes-corregidos)" in ted
    assert "`correccion_importe_adjudicacion` | `string` | [marca](../ESQUEMA.md#importes-corregidos)" in ted
    assert "`_columna_extra_3` | `string` | [marca](../ESQUEMA.md#calidad-del-origen)" in ted
    assert "`_raro` | `string` | [de la fuente](../fuentes/ted.md)" in ted
    assert "## Carpeta `v2026.02/`" in ted and "| `v2026.02/crossval_sara.parquet` | 7 | 1 |" in ted
    assert "| `v2026.02/LEEME_v2026.02.md` | — | — |" in ted
    assert "_historico" not in ted and "| `MANIFEST.csv` |" not in ted
    placsp = (salida / "nacional_licitaciones_2020_2021.md").read_text(encoding="utf-8")
    assert "[ficha de la fuente](../fuentes/placsp.md)" in placsp
    assert "`es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp)" in placsp
    assert "`_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones)" in placsp
    assert "| `LEEME.txt` |" not in placsp
    indice = (salida / "README.md").read_text(encoding="utf-8")
    assert "3 ZIP, 3 Parquet y 9 filas" in indice
    assert "| [`ted.zip`](ted.md) | 1 | 4 | 2 |" in indice
    assert "| [`nacional_licitaciones_2022_2023.zip`](nacional_licitaciones_2022_2023.md) | 1 | 2 | — |" in indice
    for p in salida.iterdir():                       # sin rutas de la máquina
        assert str(tmp_path) not in p.read_text(encoding="utf-8")


def test_la_salida_no_cambia_si_no_cambian_los_ficheros(release, tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    er.main(["--datos", str(release), "--release", "v2026.10", "--salida", str(a)])
    er.main(["--datos", str(release), "--release", "v2026.10", "--salida", str(b)])
    assert {p.name: p.read_bytes() for p in a.iterdir()} == {p.name: p.read_bytes() for p in b.iterdir()}


def test_comprueba_el_manifest(release, tmp_path, capsys):
    bien = _manifest(release, tmp_path / "MANIFEST.csv")
    salida = tmp_path / "s"
    assert er.main(["--datos", str(release), "--salida", str(salida), "--manifest", str(bien)]) == 0
    assert "iguales que en el MANIFEST" in capsys.readouterr().out

    mal = _manifest(release, tmp_path / "M2.csv", cambios={"ted_es_can.parquet": {"filas": 5}},
                    quitar={"v2026.02/crossval_sara.parquet"},
                    anadir=[{"zip": "ted.zip", "fichero": "falta.parquet", "filas": 1, "columnas": 1}])
    assert er.main(["--datos", str(release), "--salida", str(salida), "--manifest", str(mal)]) == 1
    err = capsys.readouterr().err
    assert "ted.zip ted_es_can.parquet: 4 filas y 10 columnas; el MANIFEST dice 5 y 10" in err
    assert "ted.zip v2026.02/crossval_sara.parquet: no está en el MANIFEST" in err
    assert "ted.zip falta.parquet: está en el MANIFEST y no en la carpeta" in err


def test_sin_ningun_parquet(tmp_path, capsys):
    vacia = tmp_path / "vacia"
    (vacia / "ted").mkdir(parents=True)
    (vacia / "ted" / "LEEME.txt").write_text("x", encoding="utf-8")
    assert er.main(["--datos", str(vacia), "--salida", str(tmp_path / "s")]) == 1
    assert "no hay ningún Parquet" in capsys.readouterr().err
    assert not (tmp_path / "s").exists()
    assert er.main(["--datos", str(tmp_path / "no_existe"), "--salida", str(tmp_path / "s")]) == 1


def test_parquet_sin_filas(tmp_path):
    d = tmp_path / "r"
    _parquet(d / "la_rioja" / "contratos_menores.parquet", {"TERC_CIF": (pa.string(), "x")}, 0)
    r = er.describir(d)
    assert r["la_rioja.zip"][0]["filas"] == 0
    assert er.main(["--datos", str(d), "--salida", str(tmp_path / "s")]) == 0
    assert "| `contratos_menores.parquet` | 0 | 1 |" in (tmp_path / "s" / "la_rioja.md").read_text(encoding="utf-8")
