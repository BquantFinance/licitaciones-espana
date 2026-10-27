"""Tests offline de regia/regia_valencia.py.

El portal de datos abiertos de la Generalitat Valenciana (CKAN, URL de descarga
de cada recurso) se simula con un ``requests.get`` falso. Los CSV tienen el
formato real (punto y coma, UTF-8, la cabecera de los grupos con un espacio tras
el separador, fechas «2022-02-17 00:00:00.0»); los nombres, NIF, direcciones y
correos son inventados.
"""

import csv
import gzip
import importlib.util
import io
import json
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "regia" / "regia_valencia.py"


def _cargar():
    spec = importlib.util.spec_from_file_location("regia_valencia", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
CAB_GRUPOS = ["nombre_gi", " num_regia", " nif", " fecha", " categoria", " subcategoria", " direccion", " email",
              " tipo_organizacion", " ambito_act1"]
GRUPOS = [
    ["ASOCIACION INVENTADA", "1", "G00000001", "2022-02-17 00:00:00.0", "Empresas y asociaciones", "Asociaciones",
     "CALLE INVENTADA 1 46005 València", "info@asociacion-inventada.es", "Asociación", "S"],
    ["EMPRESA INVENTADA SL", "2", "B00000002", "2023-05-02 00:00:00.0", "Empresas y asociaciones", "Empresas",
     "AVDA INVENTADA 2", "nombre.apellido@empresa-inventada.es", "Sociedad de responsabilidad limitada", "N"],
    ["Empresario individual", "3", "*********", "2023-06-01 00:00:00.0", "Servicios de consultoría", "",
     "CALLE DE UNA PERSONA 3", "persona.inventada@correo.es", "Empresario individual", "S"],
    ["Otros", "4", "*********", "2022-02-21 00:00:00.0", "Servicios de consultoría", "", "GREGORIO INVENTADO 3",
     "otra.persona@correo.es", "Otros", "S"],
]
CAB_REUNIONES = ["id_reunion", "tipo", "asuntos_tratados", "fecha", "huella"]
REUNIONES = [
    ["10", "Reunión presencial o telemática", "Presentación de un proyecto con D. Juan Inventado Pérez", "2024-03-01 10:00:00.0", " "],
    ["11", "Otro", "Aplicación del R.D. 115/2017 con la D.G. de Industria", "2024-04-02 12:00:00.0", "LEY/2024/1"],
    ["12", " ", " ", "2025-01-15 09:30:00.0", " "],
]
CAB_RG = ["id_reunion", "nombre_gi", "num_doc_identidad", "num_regia"]
RG = [["10", "ASOCIACION INVENTADA", "G00000001", "1"], ["11", "Otros", "*********", "4"],
      ["11", "EMPRESA INVENTADA SL", "B00000002", "2"], ["12", "Empresario individual", "*********", "3"]]
CAB_CARGOS = ["id_reunion", "cargo"]
CARGOS = [["10", "DG DE INDUSTRIA"], ["11", "DG DE INDUSTRIA"], ["11", "SECRETARÍA AUTONÓMICA DE ECONOMÍA"]]


def csv_gva(cabecera, filas):
    t = io.StringIO()
    w = csv.writer(t, delimiter=";", lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
    w.writerow(cabecera)
    w.writerows(filas)
    return t.getvalue().encode("utf-8")


class Respuesta:
    def __init__(self, codigo=200, cuerpo=b""):
        self.status_code = codigo
        self._cuerpo = cuerpo
        self.headers = {}

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._cuerpo), chunk_size):
            yield self._cuerpo[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def portal(monkeypatch):
    estado = {"cuerpos": {"grupos": csv_gva(CAB_GRUPOS, GRUPOS), "reuniones": csv_gva(CAB_REUNIONES, REUNIONES),
                          "reuniones_grupos": csv_gva(CAB_RG, RG), "reuniones_cargos": csv_gva(CAB_CARGOS, CARGOS)},
              "codigos": {}, "pedidas": [], "esperas": []}
    por_url = {url: nombre for nombre, (url, _) in M.RECURSOS.items()}

    def get(url, params=None, headers=None, timeout=None, stream=False):
        nombre = por_url[url]
        estado["pedidas"].append(nombre)
        assert "/api/" not in url and "@" not in headers["User-Agent"]
        return Respuesta(estado["codigos"].get(nombre, 200), estado["cuerpos"][nombre])
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr("time.sleep", lambda s: estado["esperas"].append(s))
    return estado


def todo_el_texto(salida):
    trozos = []
    for p in salida.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix == ".parquet":
            trozos.append(pd.read_parquet(p).to_csv())
        elif p.name.endswith(".gz"):
            trozos.append(gzip.decompress(p.read_bytes()).decode("utf-8"))
        else:
            trozos.append(p.read_bytes().decode("utf-8", "replace"))
    return "\n".join(trozos)


@pytest.mark.parametrize("nif, tipo, nombre, esperado", [
    ("*********", "Otros", "Otros", True), ("12345678Z", "", "X", True), ("X1234567L", "", "X", True),
    ("", "Empresario individual", "Empresario individual", True), ("G00000001", "Asociación", "ASOCIACION", False),
    ("B00000002", "Sociedad", "EMPRESA SL", False), ("", "Plataforma o red sin personalidad jurídica", "PLATAFORMA", False),
    ("B20000003", "Empresario individual", "SOCIEDAD INSCRITA COMO INDIVIDUAL S.L.", False),
    ("", "Empresario individual", "Nombre comercial de una persona", True)])
def test_es_persona_fisica(nif, tipo, nombre, esperado):
    assert M.es_persona_fisica(nif, tipo, nombre) is esperado


def test_sin_personas_no_toca_siglas():
    assert M.sin_personas("Proyecto con D. Juan Inventado Pérez y la empresa") == \
        ("Proyecto con [persona física] y la empresa", 1)
    for texto in ("Aplicación del R.D. 115/2017 con la D.G. de Industria", "Modifica el D. 110/2010",
                  "Reunión con la Sra. Consellera"):
        assert M.sin_personas(texto) == (texto, 0)


def test_descarga_sin_personas_ni_contactos(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    assert portal["pedidas"] == list(M.RECURSOS)
    assert portal["esperas"].count(M.PAUSA) == 3 and M.PAUSA >= 10   # Crawl-Delay: 10 del robots.txt
    grupos = pd.read_parquet(tmp_path / "grupos.parquet")
    assert sorted(grupos["nif"]) == ["B00000002", "G00000001"]
    assert "direccion" not in grupos.columns and "email" not in grupos.columns
    assert list(grupos.columns[:4]) == ["nombre_gi", "num_regia", "nif", "fecha"]   # cabecera sin espacios
    texto = todo_el_texto(tmp_path)
    for dato in ("@", "CALLE", "AVDA", "GREGORIO", "Juan Inventado", "*********", "Empresario individual"):
        assert dato not in texto, dato
    reu = pd.read_parquet(tmp_path / "reuniones.parquet").set_index("id_reunion")
    assert reu.loc["10", "asuntos_tratados"] == "Presentación de un proyecto con [persona física]"
    assert reu.loc["11", "asuntos_tratados"] == "Aplicación del R.D. 115/2017 con la D.G. de Industria"
    assert len(reu) == 3   # la reunión 12 se conserva aunque su único grupo sea una persona física
    rg = pd.read_parquet(tmp_path / "reuniones_grupos.parquet")
    assert sorted(zip(rg["id_reunion"], rg["num_regia"])) == [("10", "1"), ("11", "2")]
    assert len(pd.read_parquet(tmp_path / "reuniones_cargos.parquet")) == 3
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text())
    assert man["ficheros"]["grupos"]["filas_leidas"] == 4 and man["ficheros"]["grupos"]["filas_conservadas"] == 2
    assert all(len(f["sha256"]) == 64 for f in man["ficheros"].values())
    assert not (tmp_path / ".descargas").exists()


def test_grupo_dado_de_baja_se_conserva(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    portal["cuerpos"]["grupos"] = csv_gva(CAB_GRUPOS, [GRUPOS[1], GRUPOS[2]])
    assert M.main(["--salida", str(tmp_path)]) == 0
    g = pd.read_parquet(tmp_path / "grupos.parquet")
    assert dict(zip(g["nif"], g["_en_ultima_descarga"])) == {"G00000001": False, "B00000002": True}
    assert len(list((tmp_path / "raw" / "_historico").glob("grupos.csv__*.gz"))) == 1
    assert M.main(["--salida", str(tmp_path)]) == 0      # sin cambios: ninguna versión nueva
    assert len(list((tmp_path / "raw" / "_historico").glob("grupos.csv__*.gz"))) == 1


def test_un_recurso_que_falla_no_toca_nada(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = {p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv.gz")}
    portal["cuerpos"]["grupos"] = csv_gva(CAB_GRUPOS, [GRUPOS[1]])
    portal["codigos"]["reuniones_cargos"] = 404
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert {p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv.gz")} == antes
    assert "reuniones_cargos: no_existe HTTP 404" in (tmp_path / "raw" / "descarga_log.txt").read_text()
    assert not (tmp_path / ".descargas").exists()


def test_cero_grupos_no_toca_nada(portal, tmp_path):
    """Caso de cero resultados: un CSV de grupos vacío (o solo con personas físicas) no sustituye al anterior."""
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = (tmp_path / "grupos.parquet").read_bytes()
    for filas in ([], [GRUPOS[2], GRUPOS[3]]):
        portal["cuerpos"]["grupos"] = csv_gva(CAB_GRUPOS, filas)
        assert M.main(["--salida", str(tmp_path)]) == 1
        assert (tmp_path / "grupos.parquet").read_bytes() == antes
        assert not list((tmp_path / "raw").glob(".*.nuevo"))


def test_cabecera_cambiada(portal, tmp_path):
    portal["cuerpos"]["reuniones_cargos"] = csv_gva(["id_reunion", "cargo_publico"], CARGOS)
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert "faltan columnas: cargo" in (tmp_path / "raw" / "descarga_log.txt").read_text()
    assert not (tmp_path / "grupos.parquet").exists()
