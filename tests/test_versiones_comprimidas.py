"""Versiones archivadas comprimidas con gzip (comun/historico.py, regla 6).

Una versión de la capa cruda en _historico/ puede guardarse como <nombre>__<sello><ext>.gz sin perder
nada. Lo que se comprueba aquí: versiones() la lista en su sitio y en el mismo orden; los lectores que
reconstruyen desde TODAS las versiones (el Parquet de Cataluña y la unificación de la Comunidad de
Madrid) dan exactamente la misma salida con las versiones comprimidas que sin comprimir; y archivar()
no pisa una versión comprimida. Euskadi (páginas de ventana .json.gz) está en tests/test_euskadi.py."""
import csv
import gzip
import importlib.util
import io
import os
import shutil
import warnings
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from comun import historico as h

REPO_ROOT = Path(__file__).resolve().parents[1]


def comprimir(ruta):
    """Como se comprime una versión archivada: gzip del contenido exacto y el original se borra."""
    ruta = Path(ruta)
    destino = ruta.with_name(ruta.name + ".gz")
    with open(ruta, "rb") as src, gzip.open(destino, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    ruta.unlink()
    return destino


# ── comun.historico ──────────────────────────────────────────

def test_versiones_lista_las_comprimidas_en_su_sitio(tmp_path):
    destino = tmp_path / "pscp.csv"
    destino.write_text("a\n4\n")
    hist = tmp_path / h.HISTORICO
    hist.mkdir()
    nombres = ["pscp__20260101T000000Z.csv", "pscp__20260102T000000Z.csv", "pscp__20260102T000000Z_1.csv",
               "pscp__20260103T000000Z.csv"]
    for k, n in enumerate(nombres):
        (hist / n).write_text(f"a\n{k}\n")
    sin_comprimir = [p.name for p in h.versiones(destino)]
    comprimir(hist / nombres[0])
    comprimir(hist / nombres[2])
    lista = h.versiones(destino)
    assert [h.nombre_version(p).name for p in lista] == sin_comprimir          # mismo orden
    assert [p.name for p in lista][:3] == [nombres[0] + ".gz", nombres[1], nombres[2] + ".gz"]
    assert lista[-1] == destino
    with h.abrir_version(lista[0], "r", encoding="utf-8") as f:
        assert f.read() == "a\n0\n"
    with h.abrir_version(lista[1], "rb") as f:
        assert f.read() == b"a\n1\n"


def test_versiones_con_las_dos_copias_cuenta_la_original(tmp_path):
    destino = tmp_path / "x.csv"
    hist = tmp_path / h.HISTORICO
    hist.mkdir()
    v = hist / "x__20260101T000000Z.csv"
    v.write_text("a\n1\n")
    with gzip.open(hist / (v.name + ".gz"), "wb") as f:
        f.write(b"a\n1\n")
    assert h.versiones(destino) == [v]                        # sin repetirla; la vigente no existe


def test_versiones_cero_y_sin_historico(tmp_path):
    destino = tmp_path / "x.csv"
    assert h.versiones(destino) == []
    destino.write_text("a\n")
    assert h.versiones(destino) == [destino]
    (tmp_path / h.HISTORICO).mkdir()
    assert h.versiones(destino) == [destino]


def test_versiones_de_un_destino_que_ya_es_gz_no_cambian(tmp_path):
    # Descargas que ya vienen comprimidas (std.jsonl.gz): sus versiones son std.jsonl__<sello>.gz
    destino = tmp_path / "std.jsonl.gz"
    destino.write_bytes(gzip.compress(b"{}\n"))
    hist = tmp_path / h.HISTORICO
    hist.mkdir()
    v = hist / "std.jsonl__20261022T065023Z.gz"
    v.write_bytes(gzip.compress(b"{}\n"))
    assert h.versiones(destino) == [v, destino]
    assert h.nombre_version(tmp_path / "x__20260101T000000Z.csv") == tmp_path / "x__20260101T000000Z.csv"


def test_otro_fichero_comprimido_no_se_cuela(tmp_path):
    destino = tmp_path / "CM_2021[1].csv"
    hist = tmp_path / h.HISTORICO
    hist.mkdir()
    (hist / "CM_20211__20260101T000000Z.csv.gz").write_bytes(gzip.compress(b"otro"))
    (hist / "CM_2021[1]__20260101T000000Z.csv.gz").write_bytes(gzip.compress(b"este"))
    assert [p.name for p in h.versiones(destino)] == ["CM_2021[1]__20260101T000000Z.csv.gz"]


def test_archivar_no_pisa_una_version_comprimida_con_el_mismo_sello(tmp_path):
    destino = tmp_path / "x.csv"
    destino.write_text("a\nnuevo\n")
    os.utime(destino, (1_700_000_000, 1_700_000_000))          # 2023-11-14T22:13:20Z
    hist = tmp_path / h.HISTORICO
    hist.mkdir()
    previa = hist / "x__20231114T221320Z.csv.gz"
    previa.write_bytes(gzip.compress(b"a\nviejo\n"))
    archivo = h.archivar(destino)
    assert archivo.name == "x__20231114T221320Z_1.csv"
    assert gzip.decompress(previa.read_bytes()) == b"a\nviejo\n"
    assert [p.name for p in h.versiones(destino)] == [previa.name, archivo.name]


# ── Cataluña: el Parquet desde todas las versiones ───────────

def _cargar_cataluna():
    spec = importlib.util.spec_from_file_location("ccaa_cataluna_parquet_gz", REPO_ROOT / "scripts" / "ccaa_cataluna_parquet.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cp = _cargar_cataluna()


def _csv(ruta, filas, sep=";", encoding="utf-8"):
    buf = io.StringIO()
    csv.writer(buf, delimiter=sep, lineterminator="\n").writerows(filas)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(buf.getvalue().encode(encoding))
    return ruta


def _versiones_pscp(carpeta):
    """Tres versiones como las de la PSCP: ';' de separador y una coma en un nombre de columna (si la
    cabecera no se leyera, se probaría ',' primero y la tabla saldría desalineada), una fila que se
    retira, otra que cambia, una nueva, ceros a la izquierda y una CP1252 suelta."""
    cab = ["codi_ine10", "id", "import, euros", "nom", "enllac_publicacio"]
    url = "https://contractaciopublica.cat/ca/detall-publicacio/{}/1"
    v1 = [cab, ["0801930008", "1", "10", "Obra", url.format("aaaa")], ["0801930009", "2", "20", "Servei", url.format("bbbb")],
          ["0801930010", "3", "30", "Lot \"A\"", url.format("cccc")]]
    v2 = [cab, v1[1], ["0801930009", "2", "25", "Servei", url.format("bbbb")], v1[3], ["0801930011", "4", "40", "Nou", url.format("dddd")]]
    v3 = [cab, v2[2], v2[3], v2[4], ["0801930012", "5", "", "Línia", url.format("eeee")]]
    _csv(carpeta / "_historico" / "pscp__20260101T000000Z.csv", v1)
    _csv(carpeta / "_historico" / "pscp__20260108T000000Z.csv", v2, encoding="cp1252")
    ruta = _csv(carpeta / "pscp.csv", v3)
    os.utime(ruta, (1_780_000_000, 1_780_000_000))
    return ruta


@pytest.mark.parametrize("filas_por_lote", [1, 2, 1000])
def test_cataluna_mismo_parquet_con_versiones_comprimidas(tmp_path, monkeypatch, filas_por_lote):
    monkeypatch.setattr(cp, "FILAS_POR_LOTE", filas_por_lote)
    ruta = _versiones_pscp(tmp_path / "crudo")
    fechas = [f for _, f in cp.versiones_csv(ruta)]
    plano = tmp_path / "plano" / "x.parquet"
    n_plano, _ = cp.convert_to_parquet(ruta, plano, "prueba")
    for v in sorted((ruta.parent / "_historico").iterdir()):
        comprimir(v)
    assert [f for _, f in cp.versiones_csv(ruta)] == fechas == ["2026-01-01T00:00:00Z", "2026-01-08T00:00:00Z",
                                                               "2026-05-28T20:26:40Z"]
    assert cp._separadores(next((ruta.parent / "_historico").iterdir()))[0] == ";"
    gz = tmp_path / "gz" / "x.parquet"
    n_gz, _ = cp.convert_to_parquet(ruta, gz, "prueba")
    assert n_gz == n_plano == 6                         # 5 vigentes o retiradas + la versión vieja de la cambiada
    a, b = pd.read_parquet(plano), pd.read_parquet(gz)
    pd.testing.assert_frame_equal(a, b)
    assert list(a.columns[:3]) == ["codi_ine10", "id", "import, euros"]
    assert not a["_en_ultima_descarga"].all()           # lo retirado sigue ahí
    assert a["codi_ine10"].iloc[0] == "0801930008"


# ── Comunidad de Madrid: unificación desde csv_originales/ ──

@contextmanager
def _sin_efectos_de_importacion():
    filtros = warnings.filters[:]
    try:
        with patch.object(Path, "mkdir"), patch("logging.basicConfig"), patch("logging.FileHandler"):
            yield
    finally:
        warnings.filters[:] = filtros


def _cargar_cam():
    spec = importlib.util.spec_from_file_location(
        "descarga_cam_gz", REPO_ROOT / "comunidad_madrid" / "descarga_contratacion_comunidad_madrid_v1.py")
    mod = importlib.util.module_from_spec(spec)
    with _sin_efectos_de_importacion():
        spec.loader.exec_module(mod)
    return mod


cam = _cargar_cam()


def _csv_cam(ruta, filas):
    buf = io.StringIO()
    csv.writer(buf, delimiter=";", lineterminator="\r\n").writerows(filas)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(buf.getvalue().encode("utf-8-sig"))
    return ruta


def test_comunidad_madrid_misma_unificacion_con_versiones_comprimidas(tmp_path):
    cab = ["Referencia", "Entidad Adjudicadora", "Objeto", "Importe"]
    raiz = tmp_path / "csv_originales"
    _csv_cam(raiz / "_historico" / "contratos_2025__20260101T000000Z.csv",
             [cab, ["R1", "Ent", "Obra", "10"], ["R2", "Ent", "Servicio", "20"]])
    # una fila con un campo de más: se lee con el lector de respaldo (open de texto)
    _csv_cam(raiz / "_historico" / "contratos_2025__20260108T000000Z.csv",
             [cab, ["R1", "Ent", "Obra", "10"], ["R2", "Ent", "Servicio", "25", "extra"]])
    _csv_cam(raiz / "contratos_2025.csv", [cab, ["R2", "Ent", "Servicio", "25"], ["R3", "Ent", "Nuevo", "30"]])
    # un CSV que solo tiene versiones archivadas (sustituido por otras consultas)
    _csv_cam(raiz / "_historico" / "viejo_2024__20260101T000000Z.csv", [cab, ["V1", "Ent", "Antiguo", "5"]])
    plano = cam.ficheros_crudos(raiz)
    tablas = {n: cam.acumular_fichero(n, lista) for n, lista in plano.items()}
    for v in sorted((raiz / "_historico").iterdir()):
        comprimir(v)
    gz = cam.ficheros_crudos(raiz)
    assert list(gz) == list(plano) == ["contratos_2025.csv", "viejo_2024.csv"]
    assert [(f, vig) for _, f, vig in gz["contratos_2025.csv"]] == [(f, vig) for _, f, vig in plano["contratos_2025.csv"]]
    assert all(r.name.endswith(".csv.gz") for r, _, vig in gz["contratos_2025.csv"] if not vig)
    for n, lista in gz.items():
        pd.testing.assert_frame_equal(cam.acumular_fichero(n, lista), tablas[n])
    assert "_columna_extra_1" in tablas["contratos_2025.csv"].columns
    assert not tablas["viejo_2024.csv"]["_en_ultima_descarga"].any()
