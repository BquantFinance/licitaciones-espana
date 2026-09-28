"""
Tests offline de Euskadi/ccaa_euskadi.py (descarga) y
Euskadi/consolidacion_euskadi.py (consolidación).

Sin red: requests.get se sustituye por un servidor falso y los XLSX/JSON/CSV
se generan en tmp_path imitando la estructura de los ficheros reales
(cabeceras de los XLSX de Open Data, filas de título de REVASCON, JSON
2011-2013, CSV de Bilbao, páginas de la API KontratazioA).
"""

import importlib.util
import io
import json
import logging
import os
import re
import shutil
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest import mock

import openpyxl
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _cargar(nombre, fichero):
    spec = importlib.util.spec_from_file_location(nombre, REPO_ROOT / "Euskadi" / fichero)
    mod = importlib.util.module_from_spec(spec)
    # Sin basicConfig: importar no debe añadir handlers ni crear ficheros .log
    with mock.patch("logging.basicConfig"):
        spec.loader.exec_module(mod)
    return mod


ccaa = _cargar("ccaa_euskadi", "ccaa_euskadi.py")
cons = _cargar("consolidacion_euskadi", "consolidacion_euskadi.py")


# ─────────────────────────────────────────────────────────────
# Servidor HTTP falso
# ─────────────────────────────────────────────────────────────

class Resp:
    def __init__(self, status=200, content=b"", ctype="text/plain"):
        self.status_code = status
        self.content = content
        self.headers = {"Content-Type": ctype}

    @property
    def text(self):
        return self.content.decode("utf-8", "replace")

    def json(self):
        return json.loads(self.content)


def resp_json(data, status=200):
    return Resp(status, json.dumps(data, ensure_ascii=False).encode("utf-8"),
                "application/json;charset=UTF-8")


NO_ENCONTRADO = Resp(404, b"<html><body><h1>404 Not Found</h1></body></html>", "text/html")


class ApiFalsa:
    """Endpoint de KontratazioA: ?currentPage=N (1-based), 10 items por página."""

    def __init__(self, items, ignora_pagina=False, fallos=None):
        self.items = items
        self.ignora_pagina = ignora_pagina      # como A1/A2 en la descarga real
        self.fallos = dict(fallos or {})        # página → nº de 503 antes de responder
        self.pedidas = []

    def __call__(self, url):
        page = int(re.search(r"currentPage=(\d+)", url).group(1))
        self.pedidas.append(page)
        if self.fallos.get(page, 0) > 0:
            self.fallos[page] -= 1
            return Resp(503, b"<html>503 Service Unavailable</html>", "text/html")
        if self.ignora_pagina:
            page = 1
        trozo = self.items[(page - 1) * 10: page * 10]
        return resp_json({"totalItems": len(self.items),
                          "totalPages": max(1, -(-len(self.items) // 10)),
                          "currentPage": page, "itemsOfPage": len(trozo),
                          "items": trozo})


@pytest.fixture
def red(monkeypatch):
    """requests.get falso: rutas {prefijo de URL: Resp | callable(url)}; el resto, 404."""
    rutas, llamadas = {}, []

    def get(url, headers=None, timeout=None, **kwargs):
        llamadas.append(url)
        for prefijo in sorted(rutas, key=len, reverse=True):
            if url.startswith(prefijo):
                r = rutas[prefijo]
                return r(url) if callable(r) else r
        return NO_ENCONTRADO

    monkeypatch.setattr(ccaa.requests, "get", get)
    monkeypatch.setattr(ccaa.time, "sleep", lambda s: None)
    monkeypatch.setattr(ccaa, "stats", {"ok": 0, "fail": 0, "skip": 0, "bytes": 0})
    return rutas, llamadas


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    base = tmp_path / "datos_euskadi_contratacion_v4"
    for k, v in list(ccaa.DIRS.items()):
        monkeypatch.setitem(ccaa.DIRS, k, base / v.name)
    ccaa.setup_dirs()
    return base


@pytest.fixture
def entrada(tmp_path, monkeypatch):
    """Apunta la consolidación a un datos_euskadi_contratacion_v4 en tmp_path."""
    base = tmp_path / "datos_euskadi_contratacion_v4"
    for k, v in list(cons.PATHS.items()):
        monkeypatch.setitem(cons.PATHS, k, base / v.name)
        (base / v.name).mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cons, "INPUT_DIR", base)
    monkeypatch.setattr(cons, "OUTPUT_DIR", tmp_path / "euskadi_parquet")
    cons.OUTPUT_DIR.mkdir()
    return base


def poderes(n, desde=27000):
    return [{"id": desde + i, "identificationNumber": f"P48{i:05d}A",
             "name": f"Ayuntamiento {i}", "scope": "BIZKAIA",
             "entities": [{"id": i, "name": f"Entidad {i}"}],
             "_links": {"self": {"name": f"Ayuntamiento {i}",
                                 "href": f"https://api.euskadi.eus/procurements/"
                                         f"contracting-authorities/{desde + i}"}}}
            for i in range(n)]


def empresas(n):
    return [{"name": f"EMPRESA {i}, S.L.", "registrationNumber": f"{i:05d}",
             "identificationNumber": f"B95{i:06d}",
             "location": {"postalCode": "48001", "municipality": "Bilbao"},
             "economicActivities": [{"id": "1.692", "name": "Reparación"}]}
            for i in range(n)]


# ─────────────────────────────────────────────────────────────
# Ficheros de prueba
# ─────────────────────────────────────────────────────────────

def xlsx_bytes(cabecera, filas, antes=()):
    """XLSX con `antes` = filas previas a la cabecera (títulos, vacías…)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    n = 0
    for fila in [*antes, cabecera, *filas]:
        n += 1
        for j, v in enumerate(fila, 1):
            if v is not None:
                ws.cell(row=n, column=j, value=v)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# Cabecera (reducida) de los XLSX B1 de Open Data; 2022-2026 con mojibake
CAB_B1 = ["Nombre", "Descripción", "Colección", "Titulo del Contrato", "Expediente",
          "Estado de la tramitacion", "Fecha de publicación documento",
          "Fecha límite de presentación", "URL física", "XML datos", "XML metadatos"]
CAB_B1_2022 = ["Colecciï¿½n" if c == "Colección" else c for c in CAB_B1]


def fila_b1(exp, limite, cod):
    url = f"https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/{cod}"
    return [f"Contrato {exp}", f"Descripción {exp}", "Gobierno Vasco", f"TÍTULO {exp}", exp,
            "Adjudicación provisional / definitiva", "13/08/2020", limite,
            f"{url}/es_doc/index.html", f"{url}/es_doc/data/es_r01dpd{cod}",
            f"{url}/r01Index/{cod}-idxContent.xml"]


def item_json_b1(exp, limite, cod, con_subsanacion=True):
    """Anuncio de los JSON 2011-2013 (claves reales de Open Data Euskadi)."""
    url = f"http://opendata.euskadi.eus/contenidos/anuncio_contratacion/{cod}"
    item = {"documentName": f"Contrato {exp}", "documentDescription": f"Descripción {exp}",
            "contratacion_expediente": exp, "contratacion_titulo_contrato": f"TÍTULO {exp}",
            "contratacion_estado_tramitacion": "Formalización del contrato",
            "contratacion_fecha_de_publicacion_documento": "14/09/2015",
            "contratacion_fecha_limite_presentacion": limite,
            "procedureCollection": "Gobierno Vasco", "friendlyUrl": "",
            "physicalUrl": f"{url}/es_doc/es_arch_{cod}.html",
            "dataXML": f"{url}/es_doc/data/es_r01dpd{cod}",
            "metadataXML": f"{url}/r01Index/{cod}-idxContent.xml"}
    if con_subsanacion:     # clave que solo trae el JSON de 2011
        item["contratacion_subsanacion"] = ""
    return item


# REVASCON 2015-2018: 6 filas (título, fecha de actualización, vacías) antes
# de la cabecera, como en los XLSX reales
REVASCON_ANTES = [[], [None, "Registro de contratos del Sector Público de Euskadi del 2015"],
                  [], ["Fecha de última actualización", "16/08/2018"], [], []]
CAB_REVASCON = ["Estado contrato", "Título de contrato", "Tipo de contrato",
                "Código identificador del contrato", "Importe de licitación sin IVA",
                "IVA del importe de licitación", "Importe de adjudicación con IVA",
                "Adjudicatario", "Fecha de formalización"]
REV_1 = ["Finalización", "Servicio de vigilancia", "Servicios", "C02/013/2011_0001",
         "4.148.000", "18%(Tipo de IVA general)", "2.166.834",
         "Razón Social:GSI Profesionales,UTE:No", "08/02/2012"]
REV_2 = ["Ejecución", "Suministro de baterías", "Suministros", "S-084/2013329_0001",
         "138.000", "21%(Tipo de IVA general)", "1.455.954,56",
         "Razón Social:BUREAU BATERIAS,UTE:No", "12/12/2013"]
REVASCON_CSV = (
    "Estado de contrato;Poder adjudicador;Entidad impulsora;Código de contrato;Título;"
    "Tipo de contrato;Adjudicatario;Importe de adjudicación con IVA;Fecha de formalización\n"
    "Ejecución;Gobierno Vasco;Emakunde;02EMK/02S/2013336_0001;Distribución de publicaciones;"
    "Servicios;KERTAR ARABA, S.L.;105000;01/01/2013\n")

CAB_BILBAO = ("N expediente;Lote;Tipo contrato;Objeto;Presupuesto de licitacion IVA excluido;"
              "Contratista;Presupuesto de adjudicacion IVA excluido;Fecha de adjudicacion;"
              "Fecha de formalizacion\n")


# ═════════════════════════════════════════════════════════════
# DESCARGA (ccaa_euskadi.py)
# ═════════════════════════════════════════════════════════════

def test_descarga_no_lleva_pegada_la_consolidacion():
    # ccaa_euskadi.py llevaba una copia de consolidacion_euskadi.py tras su
    # main(): al importarlo, stats pasaba a {} (download() → KeyError) y
    # main() era la consolidación.
    assert not hasattr(ccaa, "consolidar_B1_contratos_master")
    assert set(ccaa.stats) == {"ok", "fail", "skip", "bytes"}
    assert "_probe_api" in ccaa.main.__code__.co_names


def test_rutas_no_dependen_del_cwd():
    assert ccaa.BASE_DIR == REPO_ROOT / "Euskadi" / "datos_euskadi_contratacion_v4"
    assert cons.INPUT_DIR == ccaa.BASE_DIR
    assert cons.OUTPUT_DIR == REPO_ROOT / "Euskadi" / "euskadi_parquet"


def test_download_refrescar_conserva_el_anterior_si_falla(red, tmp_path):
    rutas, llamadas = red
    url = "https://opendata.euskadi.eus/x/contratos.xlsx"
    dest = tmp_path / "contratos_2026.xlsx"
    viejo = b"PK\x03\x04" + b"v" * 300
    dest.write_bytes(viejo)

    assert ccaa.download(url, dest, "t") is True          # existe: no se pide
    assert llamadas == []

    rutas[url] = Resp(500, b"<html>error</html>", "text/html")
    assert ccaa.download(url, dest, "t", refrescar=True) is False
    assert dest.read_bytes() == viejo                      # se conserva

    nuevo = b"PK\x03\x04" + b"n" * 300
    rutas[url] = Resp(200, nuevo, "application/octet-stream")
    assert ccaa.download(url, dest, "t", refrescar=True) is True
    assert dest.read_bytes() == nuevo
    assert not list(tmp_path.glob("*.part"))


def test_paginate_reintenta_errores_transitorios(red, tmp_path):
    rutas, _ = red
    rutas["https://api.test/poderes"] = ApiFalsa(poderes(25), fallos={1: 1, 2: 2})

    ccaa._paginate_api("https://api.test/poderes", "A3", tmp_path, "poderes", delay=0)

    ficheros = sorted(tmp_path.glob("poderes_p*.json"))
    assert [f.name for f in ficheros] == [f"poderes_p{n:05d}.json" for n in (1, 2, 3)]
    ids = [it["id"] for f in ficheros for it in json.loads(f.read_text("utf-8"))["items"]]
    assert sorted(ids) == list(range(27000, 27025))
    assert ccaa.stats["fail"] == 0


def test_paginate_aborta_si_la_api_ignora_currentPage(red, tmp_path, caplog):
    # Descarga real de A1/A2: las 100 "páginas" eran la página 1 repetida
    rutas, _ = red
    api = ApiFalsa(poderes(35), ignora_pagina=True)
    rutas["https://api.test/contracts"] = api

    ccaa._paginate_api("https://api.test/contracts", "A1", tmp_path, "contratos",
                       max_pages=100, delay=0)

    assert [f.name for f in tmp_path.glob("contratos_p*.json")] == ["contratos_p00001.json"]
    assert api.pedidas == [1, 2]
    assert ccaa.stats["fail"] == 1
    assert "ignora currentPage" in caplog.text


def test_paginate_rehace_paginas_y_borra_las_sobrantes(red, tmp_path):
    rutas, _ = red
    for n in range(1, 5):      # instantánea anterior con 4 páginas
        (tmp_path / f"poderes_p{n:05d}.json").write_text(
            json.dumps({"items": [{"id": -n}]}) + " " * 200, encoding="utf-8")
    rutas["https://api.test/poderes"] = ApiFalsa(poderes(15))

    ccaa._paginate_api("https://api.test/poderes", "A3", tmp_path, "poderes", delay=0)

    ficheros = sorted(tmp_path.glob("poderes_p*.json"))
    assert [f.name for f in ficheros] == ["poderes_p00001.json", "poderes_p00002.json"]
    ids = [it["id"] for f in ficheros for it in json.loads(f.read_text("utf-8"))["items"]]
    assert sorted(ids) == list(range(27000, 27015))


@pytest.mark.parametrize("data, es", [
    ({"totalItems": 2, "totalPages": 1, "currentPage": 1, "items": [{"id": 1}, {"id": 2}]}, True),
    # Consulta sin resultados, tal como la sirve la API real (sin 'items')
    ({"totalItems": 0, "totalPages": 0, "currentPage": 1, "itemsOfPage": 0, "_links": {}}, True),
    # Sin 'items' pero con registros: respuesta rota, se reintenta
    ({"totalItems": 5, "totalPages": 1, "currentPage": 1}, False),
    ({"totalItems": 0}, False),
    ({"error": "Not Found"}, False),
    ([], False),
])
def test_es_pagina_acepta_consultas_vacias(data, es):
    assert ccaa._es_pagina(data) is es


def test_probe_solo_acepta_paginas_de_la_api(red):
    rutas, _ = red
    # JSON con 200 que no es una página (error, índice…) en el primer candidato
    rutas["https://opendata.euskadi.eus/api-procurements"] = resp_json({"error": "Not Found"})
    rutas["https://api.euskadi.eus/procurements/contracting-authorities"] = ApiFalsa(poderes(3))

    assert ccaa._probe_api() == {
        "authorities": "https://api.euskadi.eus/procurements/contracting-authorities"}


def test_se_refrescan_los_ficheros_que_siguen_cambiando(monkeypatch, dirs):
    llamadas = []

    def download(url, dest, label="", skip_retry_on_404=True, refrescar=False):
        llamadas.append((dest.name, refrescar))
        return False

    monkeypatch.setattr(ccaa, "download", download)
    monkeypatch.setattr(ccaa.time, "sleep", lambda s: None)
    monkeypatch.setattr(ccaa, "YEAR_NOW", 2026)
    ccaa.dl_B1_xlsx_anual()
    ccaa.dl_C1_bilbao()
    ccaa.dl_C2_vitoria()

    refresca = {n for n, r in llamadas if r}
    assert {"contratos_2025.xlsx", "contratos_2026.xlsx", "bilbao_2025.csv",
            "bilbao_2026.csv", "bilbao_tipo_obras.csv", "vitoria_menores.csv"} <= refresca
    assert not {"contratos_2024.xlsx", "bilbao_2024.csv", "contratos_2011.json"} & refresca


# ═════════════════════════════════════════════════════════════
# CONSOLIDACIÓN (consolidacion_euskadi.py)
# ═════════════════════════════════════════════════════════════

def test_parse_importes_formato_espanol():
    s = pd.Series(["1.234.567,89", "52.990", "964.44", "0", "0,21", " 1.455.954,56 € ",
                   "", None, 105000.0, "abc"], dtype=object)
    r = cons.parse_importes(s).tolist()
    assert r[:6] == [1234567.89, 52990.0, 964.44, 0.0, 0.21, 1455954.56]
    assert pd.isna(r[6]) and pd.isna(r[7]) and pd.isna(r[9])
    assert r[8] == 105000.0


def test_parse_fechas_formatos_mezclados_y_mes_primero():
    s = pd.Series(["07/11/2014 10:00", "24/09/2013", "04/09/2015 23:59:00",
                   "2019-03-15 00:00:00", "2021 B08-04", None], dtype=object)
    assert cons.parse_fechas(s).tolist()[:4] == [
        pd.Timestamp("2014-11-07 10:00"), pd.Timestamp("2013-09-24"),
        pd.Timestamp("2015-09-04 23:59"), pd.Timestamp("2019-03-15")]
    assert cons.parse_fechas(s)[4:].isna().all()
    # columna en mes/día/año: detectada por los valores con 2º campo > 12
    us = pd.Series(["6/25/2007", "1/8/2026", "12/31/2019"])
    assert cons.parse_fechas(us).tolist() == [
        pd.Timestamp("2007-06-25"), pd.Timestamp("2026-01-08"), pd.Timestamp("2019-12-31")]


def test_safe_str_columns_vacios_a_nulo_tambien_con_dtype_str():
    # En pandas 3 el texto se lee con dtype "str" (no "object")
    df = pd.read_csv(io.StringIO("a,b\nx,\ny,z\n"), keep_default_na=False)
    assert cons.safe_str_columns(df)["b"].isna().tolist() == [True, False]


def test_revascon_xlsx_con_filas_de_titulo_y_csv(entrada):
    d = entrada / "B2_revascon_historico"
    (d / "revascon_2015.xlsx").write_bytes(xlsx_bytes(CAB_REVASCON, [REV_1, REV_2], REVASCON_ANTES))
    # el mismo contrato idéntico en el XLSX de otro año
    (d / "revascon_2016.xlsx").write_bytes(xlsx_bytes(CAB_REVASCON, [REV_1], REVASCON_ANTES))
    (d / "revascon_2013.csv").write_text(REVASCON_CSV, encoding="utf-8")

    info = cons.consolidar_B2_revascon()
    df = pd.read_parquet(cons.OUTPUT_DIR / "revascon_historico.parquet")

    assert not [c for c in df.columns if c.startswith("unnamed")]
    # el contrato repetido en el XLSX de 2016 se conserva, marcado
    assert info["registros"] == len(df) == 4
    assert info["duplicados_marcados"] == 1
    assert df["_duplicado"].tolist() == [False, False, False, True]
    assert df.loc[df["_duplicado"], "_archivo_origen"].tolist() == ["revascon_2016.xlsx"]
    # cabecera y filas de título no quedan como datos
    assert set(df["estado_contrato"]) == {"Ejecución", "Finalización"}
    # CSV 2013-2014 y XLSX 2015-2018 en las mismas columnas; importes numéricos
    importes = dict(zip(df["código_identificador_del_contrato"],
                        df["importe_de_adjudicación_con_iva"]))
    assert importes == {"02EMK/02S/2013336_0001": 105000.0,
                        "C02/013/2011_0001": 2166834.0,
                        "S-084/2013329_0001": 1455954.56}
    assert set(df["título_de_contrato"]) == {"Distribución de publicaciones",
                                             "Servicio de vigilancia", "Suministro de baterías"}
    assert set(df["iva_del_importe_de_licitación"].dropna()) == {
        "18%(Tipo de IVA general)", "21%(Tipo de IVA general)"}


def test_contratos_master_json_y_xlsx_en_las_mismas_columnas(entrada):
    d = entrada / "B1_xlsx_sector_publico_anual"
    (d / "contratos_2011.xlsx").write_bytes(xlsx_bytes(CAB_B1, []))    # vacío (solo cabecera)
    (d / "contratos_2014.xlsx").write_bytes(xlsx_bytes(CAB_B1, [
        fila_b1("S-019-2014", "07/11/2014 10:00", "exp1"),
        fila_b1("S-020-2014", "28/08/2014", "exp2")]))
    (d / "contratos_2022.xlsx").write_bytes(xlsx_bytes(CAB_B1_2022, [
        fila_b1("B911-2022", "", "exp3")]))
    (d / "contratos_2011.json").write_text(json.dumps([
        item_json_b1("PA 22/15", "04/09/2015 23:59", "expjaso1"),
        item_json_b1("G/110/2013", "24/09/2013", "expjaso2")]), encoding="utf-8")
    # el JSON de 2012 es un subconjunto del de 2011 (sin las claves solo de 2011)
    (d / "contratos_2012.json").write_text(json.dumps([
        item_json_b1("G/110/2013", "24/09/2013", "expjaso2", con_subsanacion=False)]),
        encoding="utf-8")

    info = cons.consolidar_B1_contratos_master()
    df = pd.read_parquet(cons.OUTPUT_DIR / "contratos_master.parquet")

    # la fila del JSON 2012 que repite una del de 2011 se conserva, marcada
    assert info["registros"] == len(df) == 6
    assert info["duplicados_marcados"] == 1
    assert df.loc[df["_duplicado"], "_archivo_origen"].tolist() == ["contratos_2012.json"]
    assert info["rango_años"] == "2011-2022"
    for col in ("documentname", "physicalurl", "dataxml", "metadataxml",
                "contratacion_titulo_contrato", "colecciï¿½n"):
        assert col not in df.columns
    for col in ("nombre", "titulo_del_contrato", "expediente", "colección",
                "url_física", "xml_datos", "xml_metadatos"):
        assert df[col].notna().all(), col
    assert df["xml_datos"].str.startswith("http").all()
    limite = dict(zip(df["expediente"], df["fecha_límite_de_presentación"]))
    assert limite["S-019-2014"] == pd.Timestamp("2014-11-07 10:00")
    assert limite["S-020-2014"] == pd.Timestamp("2014-08-28")      # sin hora
    assert limite["G/110/2013"] == pd.Timestamp("2013-09-24")
    assert limite["PA 22/15"] == pd.Timestamp("2015-09-04 23:59")
    assert pd.isna(limite["B911-2022"])


def test_contratos_2021_filas_corridas_se_recolocan(entrada):
    # Estructura real de contratos_2021.xlsx (64.826 filas, 2026-02):
    # 18.000 filas con los valores desde "URL física" corridos 2 columnas a la
    # izquierda, 826 corridos 3 (desde "Órgano de Contratación") y, de éstas,
    # 21 con la cabecera corrida 1 desde "Fecha de publicación documento".
    cab = ["Nombre", "Colección", "Titulo del Contrato", "Objeto del Contrato",
           "Fecha de publicación documento", "Expediente", "Estado de la tramitacion",
           "Contrato menor", "Entidad que impulsa la contratación", "Órgano de Contratación",
           "Fecha límite de presentación", "URL amigable", "URL física", "XML datos",
           "XML metadatos", "Zip", "Fecha de creación", "Id. Institución", "Institución",
           "Id. Departamento", "Departamento"]
    u = "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/{}"
    ficha = u.format("expcm1/es_doc/es_arch_expcm1.html")
    cola = [ficha, u.format("expcm1/es_doc/data/es_r01dtpd1"),
            u.format("expcm1/r01Index/expcm1-idxContent.xml"), u.format("expcm1/opendata/expcm1.zip"),
            "18/03/2021", "r01epd0121", "Diputación Foral de Gipuzkoa", "r01etpd99", "Hacienda"]
    ini = ["Titulo", "Objeto", "18/03/2021"]
    cab_ok = ["Adjudicación provisional / definitiva", "Sí", "P4812700E - Ayto", "Alcaldía"]
    filas = [["Bien", "GV", *ini, "E-0", *cab_ok, "20/04/2021 12:00", u.format("amigable"), *cola],
             ["Corrida 2", "GV", *ini, "E-2", *cab_ok, *cola],
             ["Corrida 3", "GV", None, None, None, None, None, None, None, *cola[:-1]],
             ["Corrida 1+3", "GV", "Titulo", "Objeto", "E-13", *cab_ok, *cola[:5]]]
    (entrada / "B1_xlsx_sector_publico_anual" / "contratos_2021.xlsx").write_bytes(xlsx_bytes(cab, filas))

    cons.consolidar_B1_contratos_master()
    df = pd.read_parquet(cons.OUTPUT_DIR / "contratos_master.parquet").set_index("nombre")

    assert df["_columnas_corridas"].astype(object).where(df["_columnas_corridas"].notna(), None).to_dict() == {
        "Bien": None, "Corrida 2": "fecha_límite_de_presentación:2",
        "Corrida 3": "órgano_de_contratación:3",
        "Corrida 1+3": "fecha_de_publicación_documento:1,órgano_de_contratación:3"}
    for n in df.index:
        assert df.loc[n, "url_física"] == ficha, n
        assert df.loc[n, "xml_datos"] == cola[1], n
        assert df.loc[n, "zip"] == cola[3], n
        assert df.loc[n, "fecha_de_creación"] == pd.Timestamp("2021-03-18"), n
    for n in ("Bien", "Corrida 2"):
        assert df.loc[n, "institución"] == "Diputación Foral de Gipuzkoa"
        assert df.loc[n, "departamento"] == "Hacienda"
        assert df.loc[n, "órgano_de_contratación"] == "Alcaldía"
    assert df.loc["Bien", "fecha_límite_de_presentación"] == pd.Timestamp("2021-04-20 12:00")
    assert df.loc["Bien", "url_amigable"] == u.format("amigable")
    assert pd.isna(df.loc["Corrida 2", "fecha_límite_de_presentación"])
    assert pd.isna(df.loc["Corrida 2", "url_amigable"])
    # corrida 3: sin órgano en el fichero (su celda traía la URL física)
    assert pd.isna(df.loc["Corrida 3", "órgano_de_contratación"])
    assert df.loc["Corrida 3", "id._departamento"] == "r01etpd99"
    assert pd.isna(df.loc["Corrida 3", "departamento"])
    # cabecera corrida 1: el expediente estaba en la fecha de publicación
    fila = df.loc["Corrida 1+3"]
    assert (fila["expediente"], fila["estado_de_la_tramitacion"], fila["contrato_menor"],
            fila["entidad_que_impulsa_la_contratación"], fila["órgano_de_contratación"]) == (
        "E-13", *cab_ok)
    assert pd.isna(fila["fecha_de_publicación_documento"])
    assert pd.isna(fila["id._institución"])


def test_bilbao_importes_fechas_y_duplicados(entrada):
    d = entrada / "C1_bilbao"
    (d / "bilbao_2008.csv").write_text(
        CAB_BILBAO
        + "080617000001;1;Obras;Urbanización de la plaza;52.990;VICONSA, S.A.;964.440;"
          "6/25/2008;15/07/2008\n"
        + "080617000002;2;Servicios;Limpieza de colegios;1.234.567,89;LIMPIEZAS, S.L.;"
          "1.100.000,5;1/8/2008;20/02/2008\n", encoding="utf-8")
    # instantánea "abiertas" anterior: se descarta, solo vale la más reciente
    (d / "bilbao_abiertas_20250101.csv").write_text(
        CAB_BILBAO + "080617000009;1;Obras;Versión antigua;100;OTRA, S.A.;90;"
                     "6/25/2008;15/07/2008\n", encoding="utf-8")
    # instantánea actual: el mismo contrato de 2008 con espacios finales
    (d / "bilbao_abiertas_20260212.csv").write_text(
        CAB_BILBAO + "080617000001 ;1;Obras;Urbanización de la plaza ;52.990;VICONSA, S.A. ;"
                     "964.440;6/25/2008;15/07/2008\n", encoding="utf-8")

    info = cons.consolidar_C1_bilbao()
    df = pd.read_parquet(cons.OUTPUT_DIR / "bilbao_contratos.parquet").set_index("n_expediente")

    assert sorted(df.index) == ["080617000001", "080617000002"]   # 0 inicial conservado
    assert info["duplicados_eliminados"] == 1
    # se conserva la fila tal cual está en su fichero (sin quitar espacios)
    assert df.loc["080617000001", "contratista"] == "VICONSA, S.A."
    lic, adj = "presupuesto_de_licitacion_iva_excluido", "presupuesto_de_adjudicacion_iva_excluido"
    assert (df.loc["080617000001", lic], df.loc["080617000001", adj]) == (52990.0, 964440.0)
    assert (df.loc["080617000002", lic], df.loc["080617000002", adj]) == (1234567.89, 1100000.5)
    # adjudicación en mes/día (detectado por "6/25/2008"), formalización en día/mes
    assert df.loc["080617000001", "fecha_de_adjudicacion"] == pd.Timestamp("2008-06-25")
    assert df.loc["080617000002", "fecha_de_adjudicacion"] == pd.Timestamp("2008-01-08")
    assert df.loc["080617000002", "fecha_de_formalizacion"] == pd.Timestamp("2008-02-20")
    assert df["lote"].tolist() == [1, 2]


def test_bilbao_filas_repetidas_en_un_mismo_fichero_se_conservan(entrada):
    d = entrada / "C1_bilbao"
    fila = "080617000001;1;Obras;Urbanización;52.990;VICONSA, S.A.;964.440;6/25/2008;15/07/2008\n"
    # el Ayuntamiento la publica dos veces en el fichero anual…
    (d / "bilbao_2008.csv").write_text(CAB_BILBAO + fila + fila, encoding="utf-8")
    # …y la descarga por tipo la trae otra vez (solapamiento de descargas)
    (d / "bilbao_tipo_obras.csv").write_text(CAB_BILBAO + fila, encoding="utf-8")

    info = cons.consolidar_C1_bilbao()
    df = pd.read_parquet(cons.OUTPUT_DIR / "bilbao_contratos.parquet")
    assert len(df) == 2 and info["duplicados_eliminados"] == 1
    assert set(df["_archivo_origen"]) == {"bilbao_2008.csv"}


def test_empresas_repetidas_por_la_api_se_conservan_marcadas(entrada):
    emp = {"name": "OBRA PUBLICA LA RIBERA SL", "registrationNumber": "09618",
           "identificationNumber": "B00000000", "economicActivities": [{"id": 1}]}
    otra = dict(emp, name="CEVIAM EPC SL", registrationNumber="09622")
    pagina = {"totalItems": 3, "totalPages": 1, "currentPage": 1, "itemsOfPage": 3,
              "items": [emp, dict(emp), otra]}
    (entrada / "A4_api_empresas" / "empresas_p00001.json").write_text(
        json.dumps(pagina), encoding="utf-8")

    cons.consolidar_A4_empresas()
    df = pd.read_parquet(cons.OUTPUT_DIR / "empresas_licitadoras.parquet")
    assert df["registrationnumber"].tolist() == ["09618", "09618", "09622"]
    assert df["_duplicado"].tolist() == [False, True, False]


def test_poderes_distintos_con_mismo_contenido_no_se_fusionan(entrada):
    base = {"identificationNumber": "P0100252F", "name": "Junta Administrativa",
            "scope": "ARABA/ÁLAVA", "entities": [{"id": 1, "name": "Junta"}]}
    pagina = {"totalItems": 3, "totalPages": 1, "currentPage": 1, "itemsOfPage": 3,
              "items": [dict(base, id=27191), dict(base, id=27200), dict(base, id=27191)]}
    (entrada / "A3_api_poderes" / "poderes_p00001.json").write_text(
        json.dumps(pagina), encoding="utf-8")

    cons.consolidar_A3_poderes()
    df = pd.read_parquet(cons.OUTPUT_DIR / "poderes_adjudicadores.parquet")
    # los dos poderes distintos se conservan; el id repetido también, marcado
    assert df["id"].tolist() == [27191, 27200, 27191]
    assert df["_duplicado"].tolist() == [True, False, False]


def test_ultimos_90d_solo_la_instantanea_mas_reciente(entrada):
    d = entrada / "B3_ultimos_90_dias"
    (d / "ultimos_90d_20260901.xlsx").write_bytes(
        xlsx_bytes(["Nombre", "Expediente"], [["A", "E1"], ["B", "E2"]]))
    (d / "ultimos_90d_20260927.xlsx").write_bytes(
        xlsx_bytes(["Nombre", "Expediente"], [["B", "E2"], ["C", "E3"]]))

    cons.consolidar_B3_ultimos_90d()
    df = pd.read_parquet(cons.OUTPUT_DIR / "ultimos_90d.parquet")
    assert sorted(df["expediente"]) == ["E2", "E3"]
    assert "_year" not in df.columns       # no 20260927 como "año"


# ═════════════════════════════════════════════════════════════
# PIPELINE COMPLETO: descarga (HTTP falso) → consolidación
# ═════════════════════════════════════════════════════════════

def test_pipeline_descarga_y_consolidacion_offline(red, dirs, monkeypatch, tmp_path):
    rutas, _ = red
    monkeypatch.setattr(ccaa, "YEAR_NOW", 2026)
    api = "https://api.euskadi.eus/procurements"
    od = "https://opendata.euskadi.eus/contenidos/ds_contrataciones"
    bilbao = "https://www.bilbao.eus/opendata/datos/licitaciones"

    # Módulo A: API (A1/A2 ignoran currentPage, como en la descarga real)
    rutas[f"{api}/contracting-authorities"] = ApiFalsa(poderes(12))
    rutas[f"{api}/companies"] = ApiFalsa(empresas(15))
    rutas[f"{api}/contracts"] = ApiFalsa(poderes(30), ignora_pagina=True)
    rutas[f"{api}/contracting-notices"] = ApiFalsa(poderes(30), ignora_pagina=True)
    # Módulo B1: XLSX 2014 y 2026 + JSON 2011 (el resto, 404)
    xlsx = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    rutas[f"{od}/contrataciones_admin_2014/opendata/contratos.xlsx"] = Resp(200, xlsx_bytes(
        CAB_B1, [fila_b1("S-019-2014", "07/11/2014 10:00", "exp1"),
                 fila_b1("S-020-2014", "28/08/2014", "exp2")]), xlsx)
    rutas[f"{od}/contrataciones_admin_2026/opendata/contratos.xlsx"] = Resp(200, xlsx_bytes(
        CAB_B1_2022, [fila_b1("B911-2026", "", "exp3")]), xlsx)
    rutas[f"{od}/contrataciones_admin_2011/opendata/contratos.json"] = resp_json(
        [item_json_b1("PA 22/15", "04/09/2015 23:59", "expjaso1")])
    # Módulo B2: REVASCON CSV 2013 + XLSX 2015
    rutas[f"{od}/registro_contratos_2013/es_contracc/adjuntos/revascon-2013.csv"] = Resp(
        200, REVASCON_CSV.encode("utf-8"), "text/csv")
    rutas[f"{od}/contratos_euskadi_2015/es_contracc/adjuntos/"
          "Registro_de_contratos_del_Sector_Publico_de_Euskadi_del_2015.xlsx"] = Resp(
        200, xlsx_bytes(CAB_REVASCON, [REV_1, REV_2], REVASCON_ANTES), xlsx)
    # Módulo C1: Bilbao 2025 + tipo "obras" (solapa) + abiertas (espacios finales)
    fila_25 = ("250101000001;1;Obras;Reurbanización de calle;1.234.567,89;OBRAS, S.A.;"
               "1.000.000;13/03/2025;02/04/2025\n")
    rutas[f"{bilbao}?formato=csv&anio=2025&"] = Resp(200, (CAB_BILBAO + fila_25).encode(), "text/csv")
    rutas[f"{bilbao}?formato=csv&tipoContrato=obras&"] = Resp(
        200, (CAB_BILBAO + fila_25).encode(), "text/csv")
    rutas[f"{bilbao}?formato=csv&abiertas=true&"] = Resp(200, (
        CAB_BILBAO + fila_25.replace("OBRAS, S.A.;", "OBRAS, S.A. ;")
        + "260101000002;1;Servicios;Limpieza;52.990;LIMPIA, S.L.;50.000;;\n").encode(), "text/csv")

    ccaa.main()

    assert [f.name for f in (dirs / "A1_api_contratos").iterdir()] == ["contratos_p00001.json"]
    assert len(list((dirs / "A3_api_poderes").glob("*.json"))) == 2
    assert ccaa.stats["fail"] >= 2          # A1 y A2 (paginación ignorada) + 404

    # ── Consolidación sobre lo descargado ──────────────────
    for k, v in list(cons.PATHS.items()):
        monkeypatch.setitem(cons.PATHS, k, dirs / v.name)
    monkeypatch.setattr(cons, "INPUT_DIR", dirs)
    monkeypatch.setattr(cons, "OUTPUT_DIR", tmp_path / "euskadi_parquet")

    cons.main()

    out = tmp_path / "euskadi_parquet"
    st = json.loads((out / "stats.json").read_text(encoding="utf-8"))
    assert st["input_dir"] == "datos_euskadi_contratacion_v4"
    regs = {k: v["registros"] for k, v in st["datasets"].items()}
    assert regs == {"contratos_master": 4, "poderes_adjudicadores": 12,
                    "empresas_licitadoras": 15, "revascon_historico": 3,
                    "bilbao_contratos": 2, "ultimos_90d": 0}
    for nombre in ("contratos_master", "poderes_adjudicadores", "empresas_licitadoras",
                   "revascon_historico", "bilbao_contratos"):
        df = pd.read_parquet(out / f"{nombre}.parquet")
        assert len(df) == regs[nombre]
        assert list(df.columns) == st["datasets"][nombre]["lista_columnas"]
    assert (out / "README.md").exists()

    master = pd.read_parquet(out / "contratos_master.parquet")
    assert master["nombre"].notna().all() and master["xml_datos"].notna().all()
    bil = pd.read_parquet(out / "bilbao_contratos.parquet").set_index("n_expediente")
    assert bil.loc["250101000001", "presupuesto_de_licitacion_iva_excluido"] == 1234567.89
    assert bil.loc["260101000002", "presupuesto_de_adjudicacion_iva_excluido"] == 50000.0
    emp = pd.read_parquet(out / "empresas_licitadoras.parquet")
    assert {"name", "identificationnumber", "registrationnumber"} <= set(emp.columns)


# ─────────────────────────────────────────────────────────────
# Módulo A completo: /contracts por ventanas de fecha
# ─────────────────────────────────────────────────────────────

class ApiVentanas:
    """/contracts con award-date.gt/.lt (estrictos), orderBy/orderType, itemsOfPage
    y currentPage. ignora_pagina: siempre sirve la página 1 (sin_current: y sin
    decir qué página es, así solo se nota porque se repite). inestable: la
    página N sale de la lista girada N-1 puestos (orden distinto en cada
    petición: se repiten filas y otras no salen)."""

    def __init__(self, items, ignora_pagina=False, sin_current=False, inestable=False):
        self.items = list(items)
        self.ignora_pagina, self.sin_current = ignora_pagina, sin_current
        self.inestable = inestable
        self.caidas = set()             # subcadenas de URL que devuelven 503
        self.pedidas = []

    def __call__(self, url):
        q = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        self.pedidas.append(q)
        if any(c in url for c in self.caidas):
            return Resp(503, b"<html>503 Service Unavailable</html>", "text/html")
        gt, lt = q.get("award-date.gt"), q.get("award-date.lt")
        sel = [it for it in self.items if (gt is None and lt is None) or (
            it.get("awardDate") and (gt is None or it["awardDate"] > gt)
            and (lt is None or it["awardDate"] < lt))]
        sel.sort(key=lambda it: (it.get("awardDate") is None, it.get("awardDate") or "", it["id"]),
                 reverse=q.get("orderType") == "DESC")
        n = int(q["itemsOfPage"])
        pagina = 1 if self.ignora_pagina else int(q["currentPage"])
        orden = sel[pagina - 1:] + sel[:pagina - 1] if self.inestable and sel else sel
        trozo = orden[(pagina - 1) * n: pagina * n]
        data = {"totalItems": len(sel), "totalPages": -(-len(sel) // n), "itemsOfPage": len(trozo)}
        if sel:
            data["items"] = trozo
        # Sin resultados la API real no trae 'items': {totalItems: 0, totalPages: 0, ...}
        if not self.sin_current:
            data["currentPage"] = pagina
        return resp_json(data)


def contrato(i, fecha):
    return {"id": f"G-{i:03d}$X", "awardDate": fecha, "awardAmount": 1000.0 + i,
            "CIF": f"B{i:08d}", "socialReason": f"EMPRESA {i}, S.L."}


# 4 + 3 en 2025-01 (se parte: más de API_MAX_ITEMS_VENTANA), 2 en 2025-02,
# 1 anterior a 2025, 1 con fecha errónea (posteriores) y 1 sin fecha
CONTRATOS = ([contrato(i, f) for i, f in enumerate(
    ["2025-01-02", "2025-01-05", "2025-01-05", "2025-01-10",
     "2025-01-20", "2025-01-25", "2025-01-31", "2025-02-03", "2025-02-28",
     "1999-06-30", "2424-10-04"], 1)] + [contrato(12, None)])
URL_CONTRATOS = "https://api.euskadi.eus/procurements/contracts"
HOY = date(2025, 3, 15)


@pytest.fixture
def api_completa(red, dirs, monkeypatch):
    """Páginas de 3, ventanas de hasta 5 registros, meses desde 2025 y sin
    refresco de meses (solo 'posteriores')."""
    monkeypatch.setattr(ccaa, "API_ITEMS_POR_PAGINA", 3)
    monkeypatch.setattr(ccaa, "API_MAX_ITEMS_VENTANA", 5)
    monkeypatch.setattr(ccaa, "API_ANIO_MIN", 2025)
    monkeypatch.setattr(ccaa, "API_MESES_REFRESCO", 0)
    monkeypatch.setattr(ccaa, "API_DELAY", 0)
    rutas, _ = red

    def bajar(api):
        rutas[URL_CONTRATOS] = api
        ccaa._descargar_api_completa(URL_CONTRATOS, ccaa.API_COMPLETA["contracts"], hoy=HOY)
        d = ccaa.DIRS["api_contracts_full"]
        return d, json.loads((d / "_estado.json").read_text(encoding="utf-8"))
    return bajar


def _manifiesto(d, clave):
    return json.loads((d / clave / "_ventana.json").read_text(encoding="utf-8"))


def test_api_completa_por_ventanas_cuadra_con_totalItems(api_completa, entrada):
    d, estado = api_completa(ApiVentanas(CONTRATOS))

    assert estado["total_api"] == 12 and estado["faltan"] == 0
    assert estado["ventanas_incompletas"] == [] and estado["sin_ventana"] == 1
    esperados = {"anteriores": 1, "2025-01": 7, "2025-02": 2, "2025-03": 0, "posteriores": 1}
    for clave, n in esperados.items():
        man = _manifiesto(d, clave)
        assert man["completo"] and man["total_items"] == man["ids_unicos"] == len(man["ids"]) == n
    # 2025-01 (7 > 5) se parte en dos mitades que cuadran por separado
    trozos = _manifiesto(d, "2025-01")["trozos"]
    assert trozos[0]["partido"].startswith("más de 5")
    assert [t["total_items"] for t in trozos[1:]] == [4, 3] and all(t["completo"] for t in trozos[1:])
    assert not list(d.glob("*.part")) and not (d / "_historico").exists()

    # La consolidación deja cada contrato una vez (solapes de sin_ventana fuera)
    info = cons.consolidar_A1_api_contratos()
    df = pd.read_parquet(cons.OUTPUT_DIR / "api_contratos.parquet")
    assert info["registros"] == len(df) == 12
    assert set(df["id"]) == {c["id"] for c in CONTRATOS} and not df["id"].duplicated().any()
    assert df.set_index("id").loc["G-008$X", "awardAmount"] == 1008.0


@pytest.mark.parametrize("sin_current", [False, True])
def test_api_completa_pagina_repetida_aborta_sin_guardar(api_completa, sin_current, caplog):
    api = ApiVentanas(CONTRATOS, ignora_pagina=True, sin_current=sin_current)
    d, estado = api_completa(api)

    assert "ignora currentPage" in estado["abortado"]
    assert "ignora currentPage" in caplog.text and ccaa.stats["fail"] >= 1
    # anteriores (1 registro, 1 página) sí se guarda; 2025-01 no deja nada
    assert _manifiesto(d, "anteriores")["completo"]
    assert not list(d.glob("2025-01*")) and not list(d.glob("*.part"))
    assert not (d / "2025-02").exists() and not (d / "sin_ventana").exists()


def test_api_completa_reejecucion_no_repite_ni_machaca_ventanas_completas(api_completa, entrada):
    api = ApiVentanas(CONTRATOS)
    d, _ = api_completa(api)
    antes = {f: f.read_bytes() for f in d.glob("*/*.json")}

    # 2ª ejecución con los mismos datos: solo la página 1 de cada ventana completa
    api.pedidas.clear()
    d, estado = api_completa(api)
    assert estado["faltan"] == 0 and estado["ventanas_incompletas"] == []
    enero = [q for q in api.pedidas if q.get("award-date.gt", "").startswith("2024-12-31")]
    assert len(enero) == 1 and enero[0]["currentPage"] == "1"
    assert not any(q.get("award-date.lt") == "2025-01-17" for q in api.pedidas)  # sin trozos
    assert {f: f.read_bytes() for f in d.glob("*/*.json")} == antes
    assert not (d / "_historico").exists()        # 'posteriores' y sin_ventana idénticas

    # 3ª: 2025-02 cambia (un contrato nuevo y otro con otro importe): se vuelve a
    # bajar y la versión anterior queda entera en _historico/
    cambiado = dict(CONTRATOS[7], awardAmount=9999.0)
    api.items = CONTRATOS[:7] + [cambiado] + CONTRATOS[8:] + [contrato(13, "2025-02-14")]
    d, estado = api_completa(api)
    assert estado["faltan"] == 0
    # (sin_ventana también cambia: sus páginas traen el nuevo totalItems global)
    hist = {h.name.split("__")[0]: h for h in (d / "_historico").iterdir()}
    assert set(hist) == {"2025-02", "sin_ventana"}
    assert json.loads((hist["2025-02"] / "_ventana.json").read_text(encoding="utf-8"))["total_items"] == 2
    assert _manifiesto(d, "2025-02")["total_items"] == 3
    intactas = ("anteriores", "2025-01", "2025-03", "posteriores")
    assert {f: b for f, b in antes.items() if f.parent.name in intactas} == \
        {f: f.read_bytes() for f in d.glob("*/*.json") if f.parent.name in intactas}

    # 4ª: la API falla en 'posteriores' (que se refresca): no se publica nada
    # vacío ni se archiva la descarga buena
    api.caidas.add("award-date.gt=2025-03-31")
    post = (d / "posteriores" / "_ventana.json").read_bytes()
    d, _ = api_completa(api)
    assert (d / "posteriores" / "_ventana.json").read_bytes() == post
    assert {h.name.split("__")[0] for h in (d / "_historico").iterdir()} == set(hist)
    assert not list(d.glob("*.part"))

    # La consolidación acumula las dos versiones de 2025-02
    info = cons.consolidar_A1_api_contratos()
    df = pd.read_parquet(cons.OUTPUT_DIR / "api_contratos.parquet")
    assert info["registros"] == len(df) == 14       # 12 + nuevo + versión anterior
    g8 = df[df["id"] == "G-008$X"].set_index("awardAmount")["_en_ultima_descarga"]
    assert g8.to_dict() == {1008.0: False, 9999.0: True}
    assert df.loc[df["id"] != "G-008$X", "_en_ultima_descarga"].all()


def test_api_completa_filas_repetidas_por_la_api_no_la_dejan_incompleta(api_completa, entrada):
    # Como X19004620_…_1 el 2020-01-08: la API sirve dos veces la misma fila en
    # cualquier orden, y totalItems las cuenta las dos
    api = ApiVentanas(CONTRATOS + [dict(CONTRATOS[7])])
    d, estado = api_completa(api)

    man = _manifiesto(d, "2025-02")
    assert man["completo"] and man["total_items"] == 3 and man["ids_unicos"] == 2
    assert man["repetidos_api"] == {"G-008$X": 2}
    assert not any("partido" in t for t in man["trozos"])          # no se parte
    assert [p["orden"] for p in man["trozos"][0]["pasadas"]] == ["ASC", "DESC"]
    assert estado["total_api"] == 13 and estado["repetidos_api"] == 1
    assert estado["faltan"] == 0 and estado["ventanas_incompletas"] == [] and estado["sin_ventana"] == 1

    # 2ª ejecución: la ventana ya está completa, solo se pide su página 1
    api.pedidas.clear()
    d, estado = api_completa(api)
    febrero = [q for q in api.pedidas if q.get("award-date.gt") == "2025-01-31"]
    assert len(febrero) == 1 and estado["faltan"] == 0

    info = cons.consolidar_A1_api_contratos()
    df = pd.read_parquet(cons.OUTPUT_DIR / "api_contratos.parquet")
    assert set(df["id"]) == {c["id"] for c in CONTRATOS} and info["registros"] >= 12


def test_api_completa_paginacion_inestable_no_pasa_por_filas_repetidas(api_completa, entrada):
    # 4 contratos en febrero y páginas de 3: en ASC la página 2 repite G-201 y
    # G-204 no sale; la pasada DESC trae otros ids, así que no son filas
    # repetidas en origen: se suman y la ventana cuadra sin repetidos_api
    febrero = [contrato(200 + i, f"2025-02-{10 + i:02d}") for i in range(1, 5)]
    api = ApiVentanas([contrato(1, "2025-01-02")] + febrero, inestable=True)
    d, estado = api_completa(api)

    man = _manifiesto(d, "2025-02")
    assert man["completo"] and man["total_items"] == man["ids_unicos"] == 4
    assert "repetidos_api" not in man and set(man["ids"]) == {c["id"] for c in febrero}
    asc, desc = man["trozos"][0]["pasadas"]
    assert asc["repetidos"] == {"G-201$X": 2} and desc["repetidos"] == {"G-204$X": 2}
    assert estado["faltan"] == 0 and estado.get("repetidos_api") == 0


def test_download_refrescar_guarda_la_version_anterior_en_historico(red, tmp_path):
    rutas, _ = red
    url = "https://opendata.euskadi.eus/x/contratos.csv"
    dest = tmp_path / "C2_vitoria_gasteiz" / "vitoria_menores.csv"
    dest.parent.mkdir()
    v1 = ("exp;importe\n" + "A-1;100\n" * 40).encode()
    v2 = ("exp;importe\n" + "A-1;150\n" * 40).encode()
    rutas[url] = Resp(200, v1, "text/csv")
    assert ccaa.download(url, dest, refrescar=True)
    assert ccaa.download(url, dest, refrescar=True)          # igual: no se versiona
    assert not (dest.parent / "_historico").exists()
    rutas[url] = Resp(200, v2, "text/csv")
    assert ccaa.download(url, dest, refrescar=True)
    assert dest.read_bytes() == v2
    hist = list((dest.parent / "_historico").iterdir())
    assert len(hist) == 1 and hist[0].name.startswith("vitoria_menores__")
    assert hist[0].suffix == ".csv" and hist[0].read_bytes() == v1


def test_salida_cambia_todas_las_carpetas_de_la_descarga(monkeypatch, tmp_path):
    monkeypatch.setattr(ccaa, "DIRS", dict(ccaa.DIRS))
    monkeypatch.setattr(ccaa, "BASE_DIR", ccaa.BASE_DIR)
    nombres = {k: v.name for k, v in ccaa.DIRS.items()}

    assert ccaa.argumentos(["--salida", str(tmp_path / "eus")]).salida == tmp_path / "eus"
    ccaa.usar_carpeta(tmp_path / "eus")

    assert ccaa.BASE_DIR == tmp_path / "eus"
    assert ccaa.DIRS == {k: tmp_path / "eus" / n for k, n in nombres.items()}


def test_entrada_y_salida_de_la_consolidacion(monkeypatch, tmp_path):
    monkeypatch.setattr(cons, "PATHS", dict(cons.PATHS))
    monkeypatch.setattr(cons, "INPUT_DIR", cons.INPUT_DIR)
    monkeypatch.setattr(cons, "OUTPUT_DIR", cons.OUTPUT_DIR)
    nombres = {k: v.name for k, v in cons.PATHS.items()}

    args = cons.argumentos(["--entrada", str(tmp_path / "in"), "--salida", str(tmp_path / "out")])
    cons.usar_carpetas(args.entrada, args.salida)

    assert (cons.INPUT_DIR, cons.OUTPUT_DIR) == (tmp_path / "in", tmp_path / "out")
    assert cons.PATHS == {k: tmp_path / "in" / n for k, n in nombres.items()}
    # sin opciones no cambia nada
    cons.usar_carpetas(None, None)
    assert (cons.INPUT_DIR, cons.OUTPUT_DIR) == (tmp_path / "in", tmp_path / "out")


# ─────────────────────────────────────────────────────────────
# Log con --salida: en el VPS el repo se monta en solo lectura
# ─────────────────────────────────────────────────────────────

def _copia_en_solo_lectura(tmp_path, fichero):
    """Copia el script a tmp/repo/Euskadi (que el test deja en solo lectura, como /repo en el VPS)
    y lo carga capturando la llamada real a basicConfig. Devuelve (módulo, carpeta, el FileHandler
    que el script le pasa, con el formato que le pondría basicConfig)."""
    carpeta = tmp_path / "repo" / "Euskadi"
    carpeta.mkdir(parents=True)
    shutil.copy(REPO_ROOT / "Euskadi" / fichero, carpeta / fichero)
    spec = importlib.util.spec_from_file_location(f"_copia_{fichero[:-3]}", carpeta / fichero)
    mod = importlib.util.module_from_spec(spec)
    with mock.patch("logging.basicConfig") as basic:
        spec.loader.exec_module(mod)
    kwargs = basic.call_args.kwargs
    handler = next(h for h in kwargs["handlers"] if isinstance(h, logging.FileHandler))
    handler.setFormatter(logging.Formatter(kwargs["format"]))   # lo que hace basicConfig
    return mod, carpeta, handler


@pytest.mark.parametrize("fichero", ["ccaa_euskadi.py", "consolidacion_euskadi.py"])
def test_log_va_a_la_salida_con_el_script_en_solo_lectura(tmp_path, red, fichero):
    mod, carpeta, handler = _copia_en_solo_lectura(tmp_path, fichero)
    # El log que el script abre al importarse está junto a él (y se llama LOG_NOMBRE)
    assert handler.baseFilename == os.path.abspath(carpeta / mod.LOG_NOMBRE)
    handler.setLevel(logging.INFO)   # para comprobar que el traslado conserva el nivel
    raiz = logging.getLogger()
    raiz.addHandler(handler)
    nivel_raiz = raiz.level
    raiz.setLevel(logging.INFO)   # el nivel que pone el basicConfig real
    carpeta.chmod(0o555)
    try:
        # Sin --salida falla como en el VPS: el primer mensaje no puede abrir el log (en Windows
        # chmod no impide escribir y no hay geteuid; como root tampoco se puede comprobar)
        if os.name == "posix" and os.geteuid() != 0:
            with pytest.raises(PermissionError):
                mod.log.warning("antes de --salida")
        # La ejecución real, por main(): con la red simulada (todo 404) y la entrada vacía
        salida = tmp_path / "datos" / "salida"
        args = (["--salida", str(salida)] if fichero == "ccaa_euskadi.py"
                else ["--entrada", str(tmp_path / "vacia"), "--salida", str(salida)])
        try:
            mod.main(args)
        except SystemExit:
            pass
        # Una segunda --salida: el log sigue a la última
        otra = tmp_path / "datos" / "otra"
        if fichero == "ccaa_euskadi.py":
            mod.usar_carpeta(otra)
        else:
            mod.usar_carpetas(salida=otra)
        mod.log.warning("mensaje tras --salida: ñ €")
        texto = (otra / mod.LOG_NOMBRE).read_text(encoding="utf-8")
        assert re.search(r"^\d{4}-\d{2}-\d{2} [\d:,]+ \[WARNING\] mensaje tras --salida: ñ €$",
                         texto, re.MULTILINE), texto
        assert "mensaje tras --salida" not in (salida / mod.LOG_NOMBRE).read_text(encoding="utf-8")
        # Nada se ha escrito junto al script
        assert sorted(p.name for p in carpeta.iterdir()) == [fichero]
        propios = [h for h in raiz.handlers
                   if isinstance(h, logging.FileHandler) and str(tmp_path) in h.baseFilename]
        assert [h.baseFilename for h in propios] == [os.path.abspath(otra / mod.LOG_NOMBRE)]
        assert propios[0].level == logging.INFO
    finally:
        raiz.setLevel(nivel_raiz)
        carpeta.chmod(0o755)
        for h in list(raiz.handlers):
            if isinstance(h, logging.FileHandler) and str(tmp_path) in h.baseFilename:
                raiz.removeHandler(h)
                h.close()
