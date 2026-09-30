"""Tests offline de scripts/ccaa_canarias.py.

Los portales se simulan con un ``requests.get`` falso que sirve respuestas reales
grabadas el 2026-09-30 (tests/fixtures/canarias/), recortadas para que
sean pequeñas (menos filas o enlaces; mismas cabeceras, estructura y bytes):
- CKAN de datos.canarias.es: ficha real del conjunto de contratos adjudicados y
  formalizados; su CSV (10 registros reales: con LF y con CRLF dentro de un campo,
  comillas dobladas, un campo vacío y tres que no son menores) y el diccionario.
- Las Palmas de Gran Canaria: la página con el __NEXT_DATA__ real (podado) y la API
  (lista de años y 3 registros de 2016 y de 2025).
- Cabildo de Tenerife: lista de años y 2 registros de 2023 (uno sin duración y otro
  con fecha 0001-01-01); la API exige la cabecera Origin, como la real (sin ella, 400).
- SCS: la página con sus 14 enlaces y el ODS del 1T 2021 (content.xml real con 11
  filas por hoja).
- Gobierno: la página con dos departamentos y tres organismos (7 ODT) y dos ODT reales.
"""

import hashlib
import importlib.util
import json
import runpy
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import pyarrow.parquet as pq
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_canarias.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "canarias"
SLEEP_REAL = time.sleep


def _cargar():
    spec = importlib.util.spec_from_file_location("ccaa_canarias", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
ANIO = M.ahora().year          # el script decide qué refrescar con el año real


def fixture(nombre):
    return (FIXTURES / nombre).read_bytes()


def paquete_de_prueba():
    """La ficha de CKAN grabada, con el tamaño de los recursos que sirven las fixtures (la
    grabada anuncia los reales: 55.918.992 bytes el CSV)."""
    paquete = json.loads(fixture("gobierno_paquete.json"))
    for recurso, nombre in zip(paquete["result"]["resources"], ("gobierno_contratos.csv", "gobierno_diccionario.csv")):
        recurso["size"] = len(fixture(nombre))
    return paquete


PAQUETE = paquete_de_prueba()
URL_SHOW = f"{M.URL_CKAN}package_show?id={M.PAQUETE_GOBIERNO}"
URL_CSV = PAQUETE["result"]["resources"][0]["url"]
URL_DICC = PAQUETE["result"]["resources"][1]["url"]
LPGC_API = f"{M.URL_LPGC}/api/proxy/obligaciones"
URL_LPGC_ANIOS = f"{LPGC_API}/datos-anualizacion/88"


def url_lpgc(anio):
    return f"{LPGC_API}/datos-multiples-registros-por-ano/88/1/{anio}"


def url_tenerife(anio):
    return f"{M.URL_TENERIFE}/{anio}"


def enlaces_scs():
    return M.enlaces_scs(fixture("scs_pagina.html").decode("utf-8"))


def enlaces_gob():
    return M.enlaces_gobierno(fixture("gobierno_menores.html").decode("utf-8"))


# ---------------------------------------------------------------------------
# Portal simulado
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None):
        self.status_code = status
        self._body = body
        # Como los portales reales: con Content-Length salvo que se diga otra cosa
        self.headers = {"Content-Length": str(len(body))} if headers is None else headers

    def json(self):
        return json.loads(self._body.decode("utf-8"))

    @property
    def text(self):
        return self._body.decode("utf-8", "replace")

    def iter_content(self, chunk_size=8192):
        yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakePortal:
    """rutas: url -> bytes | código HTTP | (código, cuerpo[, cabeceras]) | lista (una
    respuesta por petición; la última se repite). Lo que no está da 404 con una página
    HTML. La API de Tenerife da 400 sin la cabecera Origin, como la real."""

    def __init__(self):
        self.rutas = {}
        self.llamadas = []

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        if params:
            url = requests.Request("GET", url, params=params).prepare().url
        headers = dict(headers or {})
        self.llamadas.append((url, headers))
        if url.startswith(M.URL_TENERIFE) and headers.get("Origin") != M.ORIGEN_TENERIFE:
            return FakeResponse(400, b'{"error":"origen"}')
        valor = self.rutas.get(url, 404)
        if isinstance(valor, list):
            valor = valor.pop(0) if len(valor) > 1 else valor[0]
        if isinstance(valor, int):
            return FakeResponse(valor, b"<html><body>Error</body></html>")
        if isinstance(valor, tuple):             # (código, cuerpo) o (código, cuerpo, cabeceras)
            return FakeResponse(*valor)
        return FakeResponse(200, valor)

    def pedidas(self, url):
        return sum(1 for u, _ in self.llamadas if u == url)


def json_bytes(datos):
    return json.dumps(datos, ensure_ascii=False).encode("utf-8")


def publicar_todo(portal):
    """Todas las fuentes con las respuestas grabadas: Las Palmas con dos años (ANIO,
    que se refresca en cada ejecución, y ANIO - 5, que no), Tenerife con dos."""
    portal.rutas[URL_SHOW] = json_bytes(paquete_de_prueba())
    portal.rutas[URL_CSV] = fixture("gobierno_contratos.csv")
    portal.rutas[URL_DICC] = fixture("gobierno_diccionario.csv")
    portal.rutas[M.PAGINA_LPGC] = fixture("lpgc_pagina.html")
    portal.rutas[URL_LPGC_ANIOS] = json_bytes([{"ano": ANIO}, {"ano": ANIO - 5}])
    portal.rutas[url_lpgc(ANIO)] = fixture("lpgc_2025.json")
    portal.rutas[url_lpgc(ANIO - 5)] = fixture("lpgc_2016.json")
    portal.rutas[M.URL_TENERIFE] = json_bytes([{"ano": ANIO - 1}, {"ano": ANIO - 4}])
    portal.rutas[url_tenerife(ANIO - 1)] = fixture("tenerife_2023.json")
    portal.rutas[url_tenerife(ANIO - 4)] = fixture("tenerife_2023.json")
    portal.rutas[M.URL_SCS] = fixture("scs_pagina.html")
    for enlace in enlaces_scs():
        portal.rutas[enlace["url"]] = fixture("scs_CM_2021_1erTrimestre.ods")
    portal.rutas[M.URL_GOBIERNO_MENORES] = fixture("gobierno_menores.html")
    for enlace in enlaces_gob():
        nombre = "gobierno_scs_2t_2026.odt" if "/san/scs/" in enlace["url"] else "gobierno_rtvc_4t_2023.odt"
        portal.rutas[enlace["url"]] = fixture(nombre)


@pytest.fixture
def portal(monkeypatch):
    fake = FakePortal()
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setattr(M, "PAUSA", 0)
    publicar_todo(fake)
    return fake


def correr(salida, *args):
    return M.main(["--salida", str(salida), *args])


def parquet(salida, serie):
    return pd.read_parquet(Path(salida) / f"{serie}.parquet")


def manifiesto(salida):
    return json.loads((Path(salida) / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))


def historicos(salida):
    return sorted(p.relative_to(salida).as_posix() for p in Path(salida).rglob("*") if p.is_file()
                  and "_historico" in p.parts)


def huella(ruta):
    return hashlib.sha256(Path(ruta).read_bytes()).hexdigest()


def filas_esperadas(lector, fichero, n=1):
    df, _ = lector(FIXTURES / fichero)
    return len(df) * n


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------

def test_csv_del_gobierno_se_lee_tal_cual():
    df, avisos = M.leer_csv(FIXTURES / "gobierno_contratos.csv")
    assert avisos == []
    assert len(df) == 10 and len(df.columns) == 23
    assert list(df.columns[:3]) == ["objeto_contrato", "entidad_adjudicadora", "procedimiento_contratacion"]
    # Saltos de línea dentro de un campo entrecomillado, LF y CRLF: se conservan tal cual
    assert df["objeto_contrato"].str.contains("\n").sum() == 3
    assert df["objeto_contrato"].str.contains("\r\n").tolist().count(True) == 1
    assert "mantenimiento preventivo\r\nanual de la instalación" in df["objeto_contrato"].iloc[-1]
    # Comillas dobladas: una comilla en el texto
    assert df["objeto_contrato"].str.contains('"').sum() >= 1
    # '_U' (no consta) se queda como texto; el campo vacío es el único nulo
    assert int((df == "_U").sum().sum()) == 59
    assert int(df.isna().sum().sum()) == 1 and df["formula_revision_precios"].isna().sum() == 1
    assert df["importe_ofertado"].iloc[0] == "4575" and df["adjudicataria_nif"].iloc[0] == "43667730E"
    # Todos los procedimientos, como se publican (7 menores; los otros 3, también)
    assert df["procedimiento_contratacion"].value_counts().to_dict() == {
        "Contrato menor": 7, "Abierto": 1, "Negociado sin publicidad": 1, "Abierto simplificado": 1}


def test_json_de_las_api_con_los_numeros_como_vienen_escritos():
    df, _ = M.leer_json(FIXTURES / "tenerife_2023.json")
    assert len(df) == 2
    assert list(df.columns) == ["id", "fecha", "ejercicio", "denominacion", "duracion", "importelicitacion",
                                "importeadjudicacion", "procedimientoutilizado", "publicidad", "licitadores",
                                "adjudicatario"]
    assert df["importelicitacion"].iloc[0] == "15000.00"          # no pasa por float (15000.0)
    assert df["duracion"].iloc[0] is None                          # "" en el JSON: campo vacío
    assert df["importeadjudicacion"].iloc[1] == "0.00" and df["duracion"].iloc[1] == "1"
    assert df["fecha"].str.startswith("0001-01-01").sum() == 1     # tal cual, sin convertir
    lpgc, _ = M.leer_json(FIXTURES / "lpgc_2016.json")
    assert lpgc["_esExterno"].tolist() == ["false"] * 3            # true/false como en el JSON
    assert lpgc["importe"].iloc[0] == "247.58" and lpgc["cif_adjudicatario"].iloc[0] == "-"
    # Los espacios de los bordes, tal cual (el portal deja uno al final del órgano)
    lpgc25, _ = M.leer_json(FIXTURES / "lpgc_2025.json")
    assert lpgc25["organo_de_contratacion"].iloc[0].endswith("Las Palmas de Gran Canaria ")


def test_ods_del_scs_con_sus_dos_hojas():
    df, avisos = M.leer_ods(FIXTURES / "scs_CM_2021_1erTrimestre.ods")
    assert set(df["_hoja"]) == {"Hoja1", "Hoja2"}
    h1 = df[df["_hoja"] == "Hoja1"]
    # Texto tal cual (la Hoja1 guarda los importes como texto con 'EUR')
    assert h1["Importe"].iloc[0] == "107.471,80 EUR" and h1["Número"].iloc[0] == "20"
    assert "Sección SERVICIO CANARIO DE LA SALUD" in h1["_titulo_tabla"].iloc[0]
    h2 = df[df["_hoja"] == "Hoja2"]
    # La Hoja2 guarda números: el valor guardado, no el formateado
    assert "107471.8" in h2["Importe (Eur)"].tolist()
    # Las notas del pie siguen siendo filas (no se borra nada)
    assert h2["Órgano de contratación"].str.startswith("Fecha de publicación").sum() == 1
    assert any("2 hojas" in a for a in avisos)


def test_odt_del_gobierno_con_su_contexto():
    df, avisos = M.leer_odt(FIXTURES / "gobierno_scs_2t_2026.odt")
    assert avisos == []
    assert list(df.columns[:4]) == ["Tipo contrato", "Número", "Importe (EUR)", "% (1)"]
    assert len(df) == 75
    primera = df.iloc[0]
    assert primera["_seccion"] == "SCS CONSEJERÍA DE SANIDAD"
    assert primera["_parrafo_previo"] == "Órgano de contratación: DG HEMODONACIÓN Y HEMOTERAPIA"
    assert (primera["Tipo contrato"], primera["Número"], primera["Importe (EUR)"]) == ("Servicios", "32", "55.744,86")
    ultima = df.iloc[-1]
    # La tabla de totales deja vacía la primera celda de la cabecera: se lee con los nombres de la primera
    assert ultima["Tipo contrato"] == "Total sección" and ultima["Número"] == "16906"
    assert ultima["_tabla"] == "resume-table" and ultima["_cabecera_tabla"] == " | Número | Importe (EUR) | % (1)"
    assert "Fecha en la que se extrae la información" in ultima["_pie"]
    assert (df["Tipo contrato"] == "Total órgano").sum() == 24
    # El texto de los párrafos, tal cual: el doble espacio del nombre del órgano se conserva
    assert "Órgano de contratación: DIR. GERENCIA  COMPLEJO HOSPITAL DE G.C. DR. NEGRÍN" in set(df["_parrafo_previo"])


def test_periodo_de_texto():
    assert M.periodo_de_texto("Contratos menores 2º trimestre 2026 (ODT)") == (2026, 2)
    assert M.periodo_de_texto("Contratos menores primer trimestre 2021 (ods).") == (2021, 1)
    assert M.periodo_de_texto("Contratos menores cuarto trimestre 2021 (ods).") == (2021, 4)
    assert M.periodo_de_texto("Contratos menores trimestrales 2025 (ods).") == (2025, None)
    assert M.periodo_de_texto("sin fecha", "presidencia-del-gobierno-1t-2024.odt") == (2024, None)
    assert M.periodo_de_texto("", "") == (None, None)


def test_enlaces_de_las_paginas():
    scs = enlaces_scs()
    assert sum(e["url"].endswith(".ods") for e in scs) == 9 and sum(e["url"].endswith(".pdf") for e in scs) == 5
    assert scs[0]["url"].startswith("https://www3.gobiernodecanarias.org/sanidad/scs/content/")
    gob = [e for e in enlaces_gob() if e["url"].endswith(".odt")]
    assert len(gob) == 7
    por = {(e["departamento"], e["organismo"]) for e in gob}
    assert por == {("Presidencia del Gobierno", None), ("Presidencia del Gobierno", "Consejo Económico y Social"),
                   ("Presidencia del Gobierno", "Ente Público Radiotelevión Canaria"),
                   ("Consejería de Sanidad", None), ("Consejería de Sanidad", "Servicio Canario de Salud")}
    assert M._nombre_local_gobierno(gob[0]["url"]) == "xi-legislatura/pregob/presidencia-del-gobierno-2-2026.odt"


# ---------------------------------------------------------------------------
# Primera descarga y ejecuciones siguientes
# ---------------------------------------------------------------------------

def test_primera_descarga_completa(tmp_path, portal):
    assert correr(tmp_path) == 0
    raw = tmp_path / "raw"
    gob = parquet(tmp_path, "gobierno_contratos")
    assert len(gob) == 10 and gob["_en_ultima_descarga"].all()
    assert (gob["_recurso"] == PAQUETE["result"]["resources"][0]["id"]).all()
    assert (gob["_fichero_publicado"] == "contratos_desde_01_01_2020.csv").all()
    assert (gob["_fuente"] == URL_CSV).all() and (gob["_archivo_origen"] == "gobierno_contratos/contratos.csv").all()
    assert gob["objeto_contrato"].str.contains("\n").sum() == 3
    assert gob["objeto_contrato"].str.contains("\r\n").sum() == 1
    lpgc = parquet(tmp_path, "las_palmas_gc")
    assert len(lpgc) == 6 and set(lpgc["_anio"]) == {str(ANIO), str(ANIO - 5)}
    assert set(lpgc["_obligacion"]) == {"88"}
    assert lpgc.columns[-3:].tolist() == ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]
    tfe = parquet(tmp_path, "cabildo_tenerife")
    assert len(tfe) == 4 and set(tfe["_anio"]) == {str(ANIO - 1), str(ANIO - 4)}
    scs = parquet(tmp_path, "scs_resumen")
    assert len(scs) == filas_esperadas(M.leer_ods, "scs_CM_2021_1erTrimestre.ods", 9)
    assert scs["_periodo"].str.contains("trimestr").all()
    gobr = parquet(tmp_path, "gobierno_resumen")
    assert len(gobr) == (filas_esperadas(M.leer_odt, "gobierno_scs_2t_2026.odt")
                         + filas_esperadas(M.leer_odt, "gobierno_rtvc_4t_2023.odt", 6))
    assert set(gobr["_departamento"]) == {"Presidencia del Gobierno", "Consejería de Sanidad"}
    assert set(gobr.loc[gobr["_archivo_origen"].str.contains("/san/scs/"), "_organismo"]) == {
        "Servicio Canario de Salud"}
    # Capa cruda y metadatos del portal
    assert (raw / "gobierno_contratos" / "contratos.csv").read_bytes() == fixture("gobierno_contratos.csv")
    assert (raw / "gobierno_contratos" / "diccionario.csv").exists()
    assert (raw / "gobierno_contratos" / "_paquete.json").exists()
    assert json.loads((raw / "las_palmas_gc" / "_obligacion.json").read_text())["id"] == 88
    assert (raw / "las_palmas_gc" / f"{ANIO - 5}.json").read_bytes() == fixture("lpgc_2016.json")
    assert (raw / "scs_resumen" / "_enlaces.json").exists() and (raw / "gobierno_resumen" / "_enlaces.json").exists()
    datos = manifiesto(tmp_path)
    assert all(e["publicado"] for e in datos.values())
    assert datos[f"las_palmas_gc/{ANIO}.json"]["serie"] == "las_palmas_gc"
    # La API de Tenerife se pide siempre con su Origin
    assert all(h.get("Origin") == M.ORIGEN_TENERIFE for u, h in portal.llamadas if u.startswith(M.URL_TENERIFE))
    # Sin metadatos de pandas: el Parquet es el mismo con pandas 2 y 3
    assert not (pq.read_schema(tmp_path / "las_palmas_gc.parquet").metadata or {}).get(b"pandas")
    assert historicos(tmp_path) == []


def test_segunda_ejecucion_identica_no_cambia_nada(tmp_path, portal):
    assert correr(tmp_path) == 0
    huellas = {p: huella(p) for p in tmp_path.glob("*.parquet")}
    portal.llamadas.clear()
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    assert {p: huella(p) for p in tmp_path.glob("*.parquet")} == huellas
    assert historicos(tmp_path) == []
    # El CSV del Gobierno no se vuelve a bajar si CKAN no lo da por cambiado
    assert portal.pedidas(URL_CSV) == 0 and portal.pedidas(URL_SHOW) == 1
    # Los años cerrados no se vuelven a pedir; el año en curso, sí
    assert portal.pedidas(url_lpgc(ANIO - 5)) == 0 and portal.pedidas(url_lpgc(ANIO)) == 1
    assert portal.pedidas(url_tenerife(ANIO - 1)) == 1 and portal.pedidas(url_tenerife(ANIO - 4)) == 0
    recientes = [e for e in enlaces_gob() if e["url"].endswith(".odt")
                 and (M.periodo_de_texto(e["texto"])[0] or 0) >= ANIO - 1]
    assert sum(portal.pedidas(e["url"]) for e in enlaces_gob()) == len(recientes)


def test_comprobar_todo_vuelve_a_pedirlo_todo(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.llamadas.clear()
    assert correr(tmp_path, "--comprobar-todo") == 0
    assert portal.pedidas(URL_CSV) == 1 and portal.pedidas(url_lpgc(ANIO - 5)) == 1
    assert historicos(tmp_path) == []            # mismo contenido: ninguna versión nueva


def test_registro_modificado_y_retirado_se_conserva(tmp_path, portal):
    assert correr(tmp_path) == 0
    datos = json.loads(fixture("lpgc_2025.json"))
    datos[0]["importe"] = 4000                   # cambia un registro
    del datos[1]                                 # y otro desaparece
    portal.rutas[url_lpgc(ANIO)] = json_bytes(datos)
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    df = parquet(tmp_path, "las_palmas_gc")
    del_anio = df[df["_anio"] == str(ANIO)]
    assert len(del_anio) == 4                    # 3 de antes (2 ya no publicadas) + la versión nueva
    viejo = del_anio[del_anio["importe"] == "3994"]
    assert len(viejo) == 1 and not viejo["_en_ultima_descarga"].iloc[0]
    nuevo = del_anio[del_anio["importe"] == "4000"]
    assert len(nuevo) == 1 and nuevo["_en_ultima_descarga"].iloc[0]
    assert int((~del_anio["_en_ultima_descarga"]).sum()) == 2
    assert df.loc[df["_anio"] == str(ANIO - 5), "_en_ultima_descarga"].all()   # el otro año, intacto
    assert any(p.startswith(f"raw/las_palmas_gc/_historico/{ANIO}__") for p in historicos(tmp_path))


def test_anio_que_la_api_deja_de_listar_queda_retirado(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[URL_LPGC_ANIOS] = json_bytes([{"ano": ANIO}])
    assert correr(tmp_path) == 0
    df = parquet(tmp_path, "las_palmas_gc")
    assert len(df) == 6                                                  # no se borra nada
    assert not df.loc[df["_anio"] == str(ANIO - 5), "_en_ultima_descarga"].any()
    assert df.loc[df["_anio"] == str(ANIO), "_en_ultima_descarga"].all()
    assert manifiesto(tmp_path)[f"las_palmas_gc/{ANIO - 5}.json"]["publicado"] is False


def test_lista_de_anios_que_falla_no_retira_nada(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[URL_LPGC_ANIOS] = 500
    portal.rutas[M.URL_TENERIFE] = json_bytes([])        # lista vacía: tampoco retira
    assert correr(tmp_path) == 1
    assert parquet(tmp_path, "las_palmas_gc")["_en_ultima_descarga"].all()
    assert parquet(tmp_path, "cabildo_tenerife")["_en_ultima_descarga"].all()


def test_respuesta_vacia_de_un_anio_con_filas_no_retira_y_es_error(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[url_lpgc(ANIO)] = b"[]"
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1
    df = parquet(tmp_path, "las_palmas_gc")
    assert len(df) == 6 and df["_en_ultima_descarga"].all()


def test_anio_nuevo_todavia_vacio_es_solo_un_aviso(tmp_path, portal):
    portal.rutas[URL_LPGC_ANIOS] = json_bytes([{"ano": ANIO + 1}, {"ano": ANIO}, {"ano": ANIO - 5}])
    portal.rutas[url_lpgc(ANIO + 1)] = b"[]"
    assert correr(tmp_path) == 0
    assert (tmp_path / "raw" / "las_palmas_gc" / f"{ANIO + 1}.json").exists()


def test_pagina_html_servida_con_200_no_machaca_nada(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[url_lpgc(ANIO)] = b"<!DOCTYPE html><html><body>Mantenimiento</body></html>"
    assert correr(tmp_path) == 1
    assert (tmp_path / "raw" / "las_palmas_gc" / f"{ANIO}.json").read_bytes() == fixture("lpgc_2025.json")
    assert parquet(tmp_path, "las_palmas_gc")["_en_ultima_descarga"].all()


def test_tenerife_sin_origin_da_400(portal):
    respuesta = requests.get(url_tenerife(ANIO - 1), headers={})
    assert respuesta.status_code == 400


def ckan_sin_conjunto(portal, estado=404):
    """CKAN confirma que el conjunto no existe, ni por su nombre ni por su id (como el real:
    JSON con «Not Found Error» y HTTP 404)."""
    no_existe = json_bytes({"success": False, "error": {"__type": "Not Found Error", "message": "No encontrado"}})
    for ident in (M.PAQUETE_GOBIERNO, M.PAQUETE_GOBIERNO_ID, PAQUETE["result"]["id"]):
        portal.rutas[f"{M.URL_CKAN}package_show?id={ident}"] = (estado, no_existe)


def test_ckan_404_retira_el_conjunto(tmp_path, portal):
    assert correr(tmp_path) == 0
    ckan_sin_conjunto(portal)
    assert correr(tmp_path) == 1                 # retirar el conjunto entero es un error: hay que revisarlo
    df = parquet(tmp_path, "gobierno_contratos")
    assert len(df) == 10 and not df["_en_ultima_descarga"].any()
    assert correr(tmp_path) == 0                 # ya retirado: la siguiente solo lo avisa
    assert len(parquet(tmp_path, "gobierno_contratos")) == 10


def test_ckan_que_falla_no_retira(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[URL_SHOW] = 503
    assert correr(tmp_path) == 1
    assert parquet(tmp_path, "gobierno_contratos")["_en_ultima_descarga"].all()


def test_csv_cambiado_en_ckan_se_baja_y_acumula(tmp_path, portal):
    assert correr(tmp_path) == 0
    paquete = paquete_de_prueba()
    paquete["result"]["resources"][0]["last_modified"] = "2026-10-01T04:01:03.948110"
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    texto = fixture("gobierno_contratos.csv").decode("utf-8")
    cabecera, resto = texto.split("\r\n", 1)
    primero, demas = resto.split("\r\n", 1)
    portal.rutas[URL_CSV] = (cabecera + "\r\n" + primero.replace('"4575"', '"4600"') + "\r\n" + demas).encode("utf-8")
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    df = parquet(tmp_path, "gobierno_contratos")
    assert len(df) == 11
    assert not df.loc[df["importe_ofertado"] == "4575", "_en_ultima_descarga"].iloc[0]
    assert df.loc[df["importe_ofertado"] == "4600", "_en_ultima_descarga"].iloc[0]
    assert any(p.startswith("raw/gobierno_contratos/_historico/contratos__") for p in historicos(tmp_path))


def test_csv_sin_identificadores_no_se_aplica(tmp_path, portal):
    assert correr(tmp_path) == 0
    paquete = paquete_de_prueba()
    paquete["result"]["resources"][0]["size"] = 1
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    portal.rutas[URL_CSV] = b'"otra";"cabecera"\r\n"a";"b"\r\n'
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1
    df = parquet(tmp_path, "gobierno_contratos")
    assert len(df) == 10 and df["_en_ultima_descarga"].all() and "otra" not in df.columns


def test_enlace_que_la_pagina_deja_de_publicar_queda_retirado(tmp_path, portal):
    assert correr(tmp_path) == 0
    pagina = fixture("scs_pagina.html").decode("utf-8")
    quitado = enlaces_scs()[0]
    href = quitado["url"].split("/sanidad/scs/")[1]
    assert f'./{href}' in pagina
    portal.rutas[M.URL_SCS] = pagina.replace(f'./{href}', "./content/otra/CM_otro.pdf").encode("utf-8")
    assert correr(tmp_path) == 0
    df = parquet(tmp_path, "scs_resumen")
    fuera = df["_archivo_origen"] == f"scs_resumen/{Path(quitado['url']).name}"
    assert fuera.sum() > 0 and not df.loc[fuera, "_en_ultima_descarga"].any()
    assert df.loc[~fuera, "_en_ultima_descarga"].all()


def test_pagina_sin_enlaces_no_retira(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[M.URL_GOBIERNO_MENORES] = b"<html><body>Estamos actualizando el portal</body></html>"
    assert correr(tmp_path) == 1
    assert parquet(tmp_path, "gobierno_resumen")["_en_ultima_descarga"].all()


def test_documento_que_da_404_queda_retirado(tmp_path, portal):
    assert correr(tmp_path) == 0
    odt = next(e for e in enlaces_gob() if "/san/scs/" in e["url"] and e["url"].endswith(".odt"))
    portal.rutas[odt["url"]] = 404
    assert correr(tmp_path, "--comprobar-todo") == 0
    df = parquet(tmp_path, "gobierno_resumen")
    del_scs = df["_archivo_origen"].str.contains("/san/scs/")
    assert not df.loc[del_scs, "_en_ultima_descarga"].any() and df.loc[~del_scs, "_en_ultima_descarga"].all()


def test_filtro_de_anios(tmp_path, portal):
    assert correr(tmp_path, "--desde", str(ANIO), "--hasta", str(ANIO)) == 0
    assert portal.pedidas(url_lpgc(ANIO)) == 1 and portal.pedidas(url_lpgc(ANIO - 5)) == 0
    assert portal.pedidas(url_tenerife(ANIO - 1)) == 0
    assert portal.pedidas(URL_CSV) == 1                     # el CSV del Gobierno no va por años
    pedidos_scs = [e for e in enlaces_scs() if e["url"].endswith(".ods") and portal.pedidas(e["url"])]
    assert all(M.periodo_de_texto(e["texto"])[0] == ANIO for e in pedidos_scs)
    # Lo que queda fuera del filtro no se da por retirado en la ejecución siguiente
    assert correr(tmp_path, "--desde", str(ANIO), "--hasta", str(ANIO)) == 0
    assert all(e["publicado"] for e in manifiesto(tmp_path).values())


def test_fuentes_sueltas(tmp_path, portal):
    assert correr(tmp_path, "--fuentes", "cabildo_tenerife") == 0
    assert (tmp_path / "cabildo_tenerife.parquet").exists()
    assert not (tmp_path / "las_palmas_gc.parquet").exists()
    assert portal.pedidas(URL_SHOW) == 0
    with pytest.raises(SystemExit):
        correr(tmp_path, "--fuentes", "no_existe")


def test_pagina_de_las_palmas_que_falla_usa_la_tabla_conocida(tmp_path, portal):
    portal.rutas[M.PAGINA_LPGC] = 500
    assert correr(tmp_path, "--fuentes", "las_palmas_gc") == 0
    assert len(parquet(tmp_path, "las_palmas_gc")) == 6
    assert not (tmp_path / "raw" / "las_palmas_gc" / "_obligacion.json").exists()


def test_manifiesto_corrupto_no_descarga_nada(tmp_path, portal):
    (tmp_path / "raw").mkdir(parents=True)
    (tmp_path / "raw" / "_manifiesto.json").write_text("{roto", encoding="utf-8")
    assert correr(tmp_path) == 1
    assert portal.llamadas == [] and not list(tmp_path.glob("*.parquet"))


def test_version_ilegible_conserva_las_filas_del_parquet_anterior(tmp_path, portal):
    assert correr(tmp_path) == 0
    antes = parquet(tmp_path, "las_palmas_gc")
    (tmp_path / "raw" / "las_palmas_gc" / f"{ANIO - 5}.json").write_text("[{roto", encoding="utf-8")
    assert correr(tmp_path, "--solo-parquet") == 1
    despues = parquet(tmp_path, "las_palmas_gc")
    assert len(despues) == len(antes) and despues["_en_ultima_descarga"].all()


def test_solo_parquet_reconstruye_igual(tmp_path, portal):
    assert correr(tmp_path) == 0
    huellas = {p.name: huella(p) for p in tmp_path.glob("*.parquet")}
    for p in tmp_path.glob("*.parquet"):
        p.unlink()
    assert correr(tmp_path, "--solo-parquet") == 0
    assert {p.name: huella(p) for p in tmp_path.glob("*.parquet")} == huellas


def test_ejecutable_como_script(tmp_path, monkeypatch, portal):
    # Desde la línea de órdenes (python scripts/ccaa_canarias.py ...): sale con el código de main
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--salida", str(tmp_path), "--fuentes", "scs_resumen"])
    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert salida.value.code == 0
    assert (tmp_path / "scs_resumen.parquet").exists()
    # Los enlaces relativos de la página del SCS cuelgan de /sanidad/scs/
    assert urljoin(M.URL_SCS, "./content/x.ods") == "https://www3.gobiernodecanarias.org/sanidad/scs/content/x.ods"


def test_anio_retirado_que_vuelve_a_publicarse(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[URL_LPGC_ANIOS] = json_bytes([{"ano": ANIO}])
    assert correr(tmp_path) == 0
    assert not parquet(tmp_path, "las_palmas_gc").loc[lambda d: d["_anio"] == str(ANIO - 5),
                                                      "_en_ultima_descarga"].any()
    portal.rutas[URL_LPGC_ANIOS] = json_bytes([{"ano": ANIO}, {"ano": ANIO - 5}])
    portal.llamadas.clear()
    assert correr(tmp_path) == 0
    # Un año cerrado que vuelve a la lista se pide otra vez y vuelve a estar publicado
    assert portal.pedidas(url_lpgc(ANIO - 5)) == 1
    df = parquet(tmp_path, "las_palmas_gc")
    assert len(df) == 6 and df["_en_ultima_descarga"].all()
    assert manifiesto(tmp_path)[f"las_palmas_gc/{ANIO - 5}.json"]["publicado"] is True


def test_enlace_retirado_que_vuelve_a_publicarse(tmp_path, portal):
    assert correr(tmp_path) == 0
    pagina = fixture("scs_pagina.html").decode("utf-8")
    viejo = [e for e in enlaces_scs() if e["url"].endswith(".ods")][-1]       # el más antiguo (2021)
    href = "./" + viejo["url"].split("/sanidad/scs/")[1]
    portal.rutas[M.URL_SCS] = pagina.replace(href, "./content/otra/CM_otro.pdf").encode("utf-8")
    assert correr(tmp_path) == 0
    portal.rutas[M.URL_SCS] = fixture("scs_pagina.html")
    assert correr(tmp_path) == 0
    assert parquet(tmp_path, "scs_resumen")["_en_ultima_descarga"].all()


# ---------------------------------------------------------------------------
# Escenarios de las revisiones (re-ejecución y fidelidad)
# ---------------------------------------------------------------------------

CRLF = "\r\n"


def registros_csv():
    """(cabecera, [registros]) del CSV de la fixture, tal cual (un registro puede llevar
    saltos de línea dentro de un campo)."""
    texto = fixture("gobierno_contratos.csv").decode("utf-8")
    trozos, dentro, inicio = [], False, 0
    for i, c in enumerate(texto):
        if c == '"':
            dentro = not dentro
        elif not dentro and texto.startswith(CRLF, i):
            trozos.append(texto[inicio:i])
            inicio = i + 2
    return trozos[0], trozos[1:]


def csv_de(cabecera, registros):
    return (cabecera + CRLF + CRLF.join(registros) + CRLF).encode("utf-8")


def servir_csv(portal, contenido, **recurso):
    """CKAN anuncia el CSV cambiado (tamaño y fecha nuevos) y lo sirve."""
    paquete = paquete_de_prueba()
    paquete["result"]["resources"][0].update({"size": len(contenido), **recurso})
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    portal.rutas[URL_CSV] = contenido


def test_version_rechazada_no_congela_el_fichero(tmp_path, portal):
    assert correr(tmp_path) == 0
    # CKAN sirve una vez otro CSV (otra cabecera): no se aplica y es un error...
    servir_csv(portal, b'"otra";"cabecera"\r\n"a";"b"\r\n', last_modified="2026-10-01T00:00:00")
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1
    # ...pero la versión buena siguiente se aplica: 4575 cambia a 4600 y el último contrato se retira
    cabecera, regs = registros_csv()
    servir_csv(portal, csv_de(cabecera, [regs[0].replace('"4575"', '"4600"')] + regs[1:-1]),
               last_modified="2026-10-02T00:00:00")
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0                 # la versión rechazada ya no es la vigente: solo un aviso
    df = parquet(tmp_path, "gobierno_contratos")
    assert len(df) == 11
    assert df.loc[df["importe_ofertado"] == "4575", "_en_ultima_descarga"].tolist() == [False]
    assert df.loc[df["importe_ofertado"] == "4600", "_en_ultima_descarga"].tolist() == [True]
    assert int((~df["_en_ultima_descarga"]).sum()) == 2
    assert "otra" not in df.columns
    # Y la reconstrucción desde cero da lo mismo
    antes = huella(tmp_path / "gobierno_contratos.parquet")
    (tmp_path / "gobierno_contratos.parquet").unlink()
    assert correr(tmp_path, "--solo-parquet", "--fuentes", "gobierno_contratos") == 0
    assert huella(tmp_path / "gobierno_contratos.parquet") == antes


def test_ckan_de_uno_a_dos_csv_y_despues_solo_el_nuevo(tmp_path, portal):
    assert correr(tmp_path) == 0
    paquete = paquete_de_prueba()
    a = paquete["result"]["resources"][0]
    id_b = "bbbbbbbb-1111-2222-3333-444444444444"
    cabecera, regs = registros_csv()
    contenido_b = csv_de(cabecera, regs[5:])
    b = dict(a, id=id_b, name="Contratos 2026 (CSV)", position=2, size=len(contenido_b),
             url=a["url"].replace(a["id"], id_b).replace("contratos_desde_01_01_2020", "contratos_2026"))
    paquete["result"]["resources"].append(b)
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    portal.rutas[b["url"]] = contenido_b
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    # CKAN quita el recurso antiguo y deja solo el nuevo (así se sustituye un fichero en CKAN)
    paquete["result"]["resources"] = [r for r in paquete["result"]["resources"] if r["id"] != a["id"]]
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    datos = manifiesto(tmp_path)
    assert datos["gobierno_contratos/contratos_bbbbbbbb.csv"]["publicado"] is True     # el recurso vigente
    assert datos["gobierno_contratos/contratos.csv"]["publicado"] is False               # el retirado
    df = parquet(tmp_path, "gobierno_contratos")
    vigentes = df[df["_en_ultima_descarga"]]
    assert len(vigentes) == 5 and set(vigentes["_archivo_origen"]) == {"gobierno_contratos/contratos_bbbbbbbb.csv"}
    assert set(vigentes["_recurso"]) == {id_b}


def test_404_que_no_es_de_ckan_no_retira(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[URL_SHOW] = 404                 # un 404 en HTML (un proxy, otra ruta de la API)
    assert correr(tmp_path) == 1
    assert parquet(tmp_path, "gobierno_contratos")["_en_ultima_descarga"].all()


def test_conjunto_renombrado_se_sigue_por_su_id(tmp_path, portal):
    assert correr(tmp_path) == 0
    no_existe = json_bytes({"success": False, "error": {"__type": "Not Found Error", "message": "No encontrado"}})
    portal.rutas[URL_SHOW] = (404, no_existe)
    paquete = paquete_de_prueba()
    paquete["result"]["name"] = "contratos-del-gobierno-de-canarias"
    portal.rutas[f"{M.URL_CKAN}package_show?id={PAQUETE['result']['id']}"] = json_bytes(paquete)
    assert correr(tmp_path) == 0
    assert parquet(tmp_path, "gobierno_contratos")["_en_ultima_descarga"].all()


def test_json_que_no_son_registros_no_se_aplica(tmp_path, portal):
    assert correr(tmp_path) == 0
    portal.rutas[url_lpgc(ANIO)] = b'[{"mensaje": "Servicio no disponible temporalmente"}]'
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1                 # la versión vigente no trae id: error y no se aplica
    df = parquet(tmp_path, "las_palmas_gc")
    assert len(df) == 6 and df["_en_ultima_descarga"].all() and "mensaje" not in df.columns


def test_manifiesto_perdido_con_datos_no_hace_nada(tmp_path, portal):
    assert correr(tmp_path) == 0
    antes = {p.name: huella(p) for p in tmp_path.glob("*.parquet")}
    (tmp_path / "raw" / "_manifiesto.json").unlink()
    portal.llamadas.clear()
    assert correr(tmp_path) == 1
    assert portal.llamadas == []
    assert {p.name: huella(p) for p in tmp_path.glob("*.parquet")} == antes


def test_copia_actual_perdida_se_reconstruye_desde_el_historico(tmp_path, portal):
    assert correr(tmp_path) == 0
    datos = json.loads(fixture("lpgc_2025.json"))
    datos[0]["importe"] = 4000
    portal.rutas[url_lpgc(ANIO)] = json_bytes(datos)
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    antes = parquet(tmp_path, "las_palmas_gc")
    (tmp_path / "raw" / "las_palmas_gc" / f"{ANIO}.json").unlink()        # queda solo la versión antigua
    (tmp_path / "las_palmas_gc.parquet").unlink()
    assert correr(tmp_path, "--solo-parquet") == 0
    despues = parquet(tmp_path, "las_palmas_gc")
    assert len(despues) == len(antes) - 1                   # la versión nueva (4000) se perdió con su copia
    assert set(despues.loc[despues["_anio"] == str(ANIO), "importe"]) == {"3994", "535.52", "577.4"}


def test_lector_que_ya_no_saca_filas_conserva_las_anteriores(tmp_path, portal, monkeypatch):
    assert correr(tmp_path) == 0
    antes = parquet(tmp_path, "gobierno_resumen")
    original = M.LECTORES["odt"]

    def lector(ruta):
        if "/san/scs/" in str(ruta):
            return pd.DataFrame(columns=["_seccion"]), []
        return original(ruta)
    monkeypatch.setitem(M.LECTORES, "odt", lector)
    assert correr(tmp_path, "--solo-parquet", "--fuentes", "gobierno_resumen") == 1
    despues = parquet(tmp_path, "gobierno_resumen")
    assert len(despues) == len(antes) and despues["_en_ultima_descarga"].all()


def test_version_ilegible_intermedia_no_impide_aplicar_las_siguientes(tmp_path, portal, monkeypatch):
    assert correr(tmp_path) == 0
    datos = json.loads(fixture("lpgc_2025.json"))
    datos[0]["importe"] = 4000
    portal.rutas[url_lpgc(ANIO)] = json_bytes(datos)
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 0
    antes = parquet(tmp_path, "las_palmas_gc")
    # La primera versión (en _historico/) deja de poder leerse y llega otra nueva (4000 -> 4100)
    historico = next((tmp_path / "raw" / "las_palmas_gc" / "_historico").glob(f"{ANIO}__*.json"))
    historico.write_text("[{roto", encoding="utf-8")
    datos[0]["importe"] = 4100
    portal.rutas[url_lpgc(ANIO)] = json_bytes(datos)
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1                 # la versión ilegible es un error...
    despues = parquet(tmp_path, "las_palmas_gc")
    # ...pero la nueva se aplica, y lo que solo salía de la ilegible (3994, ya retirado) se conserva
    assert len(despues) == len(antes) + 1
    assert despues.loc[despues["importe"] == "3994", "_en_ultima_descarga"].tolist() == [False]
    assert despues.loc[despues["importe"] == "4000", "_en_ultima_descarga"].tolist() == [False]
    assert despues.loc[despues["importe"] == "4100", "_en_ultima_descarga"].tolist() == [True]


def test_dos_enlaces_con_el_mismo_fichero_se_bajan_los_dos(tmp_path, portal):
    pagina = fixture("scs_pagina.html").decode("utf-8")
    enlace = [e for e in enlaces_scs() if e["url"].endswith(".ods")][-1]
    ruta = enlace["url"].split("/sanidad/scs/")[1]
    otra = ruta.replace(ruta.split("/")[1], "11111111-1111-1111-1111-111111111111")
    con_otra = fixture("scs_CM_2021_1erTrimestre.ods").replace(b"CM_2021", b"CM_2021")   # mismo ODS
    portal.rutas[M.urljoin(M.URL_SCS, "./" + otra)] = con_otra
    extra = f'<a href="./{otra}">{enlace["texto"]}</a>'
    for orden, html in enumerate([pagina.replace("</body>", extra + "</body>"),
                                  pagina.replace(f'href="./{ruta}"', f'href="./{otra}"', 1)
                                  .replace("</body>", f'<a href="./{ruta}">{enlace["texto"]}</a></body>')]):
        portal.rutas[M.URL_SCS] = html.encode("utf-8")
        assert correr(tmp_path, "--fuentes", "scs_resumen") == 0
        datos = manifiesto(tmp_path)
        del_nombre = {rel: e["url"] for rel, e in datos.items() if Path(rel).name.startswith(Path(ruta).stem)}
        assert len(del_nombre) == 2 and all(e["publicado"] for e in datos.values())
        # El que ya tenía el nombre lo conserva aunque la página cambie el orden
        assert del_nombre[f"scs_resumen/{Path(ruta).name}"] == enlace["url"]


def test_csv_sin_content_length_se_compara_con_el_tamano_de_ckan(tmp_path, portal):
    assert correr(tmp_path) == 0
    cabecera, regs = registros_csv()
    cortado = csv_de(cabecera, regs[:4])
    paquete = paquete_de_prueba()
    paquete["result"]["resources"][0]["last_modified"] = "2026-10-01T00:00:00"      # CKAN: tamaño completo
    portal.rutas[URL_SHOW] = json_bytes(paquete)
    portal.rutas[URL_CSV] = (200, cortado, {"Content-Encoding": "gzip"})             # llega cortado, sin longitud
    SLEEP_REAL(1.1)
    assert correr(tmp_path) == 1
    assert (tmp_path / "raw" / "gobierno_contratos" / "contratos.csv").read_bytes() == fixture("gobierno_contratos.csv")
    assert parquet(tmp_path, "gobierno_contratos")["_en_ultima_descarga"].all()


def test_retirada_de_golpe_de_mas_de_la_mitad_no_se_aplica(tmp_path, portal):
    assert correr(tmp_path) == 0
    pagina = fixture("scs_pagina.html").decode("utf-8")
    for enlace in [e for e in enlaces_scs() if e["url"].endswith(".ods")][1:]:      # queda 1 de 9
        pagina = pagina.replace("./" + enlace["url"].split("/sanidad/scs/")[1], "./content/x/quitado.pdf")
    portal.rutas[M.URL_SCS] = pagina.encode("utf-8")
    assert correr(tmp_path) == 1
    assert parquet(tmp_path, "scs_resumen")["_en_ultima_descarga"].all()
    assert all(e["publicado"] for e in manifiesto(tmp_path).values())


def test_texto_de_opendocument_tal_cual():
    import xml.etree.ElementTree as ET
    t = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
    tb = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
    o = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    parrafo = ET.fromstring(f'<text:p xmlns:text="{t}">a<text:s text:c="3"/>b<text:tab/>c<text:s/>d</text:p>')
    assert M._texto_nodo(parrafo) == "a   b\tc d"
    tabla = ET.fromstring(
        f'<table:table xmlns:table="{tb}" xmlns:office="{o}" xmlns:text="{t}"><table:table-row>'
        f'<table:table-cell office:value-type="float" office:value="5600.00" table:number-columns-repeated="2">'
        f'<text:p>5.600,00</text:p></table:table-cell>'
        f'<table:table-cell office:value-type="float" office:value="12345678901234567890"><text:p>1,2E+19</text:p>'
        f'</table:table-cell><table:covered-table-cell office:value-type="string"><text:p>oculto</text:p>'
        f'</table:covered-table-cell></table:table-row>'
        f'<table:table-row table:number-rows-repeated="1000"><table:table-cell/></table:table-row></table:table>')
    filas, tapadas = M._filas_tabla(tabla)
    # El valor guardado, sin pasar por float, repetido las veces que dice el fichero
    assert filas == [["5600.00", "5600.00", "12345678901234567890"]]
    assert tapadas == 1


def test_json_anidado_con_sus_numeros(tmp_path):
    ruta = tmp_path / "anidado.json"
    ruta.write_text('[{"id": 1, "importe": 1.50, "lotes": [{"importe": 1.50}], "nota": "  a  "}]', encoding="utf-8")
    df, _ = M.leer_json(ruta)
    assert df.iloc[0].to_dict() == {"id": "1", "importe": "1.50", "lotes": '[{"importe": 1.5}]', "nota": "  a  "}


def test_404_de_casi_todos_los_documentos_no_se_aplica_de_golpe(tmp_path, portal):
    assert correr(tmp_path) == 0
    # La página sigue enlazando los ODS, pero el portal los ha movido: dan 404 casi todos
    ods = [e for e in enlaces_scs() if e["url"].endswith(".ods")]
    for enlace in ods[1:]:
        portal.rutas[enlace["url"]] = 404
    assert correr(tmp_path, "--fuentes", "scs_resumen", "--comprobar-todo") == 1
    assert parquet(tmp_path, "scs_resumen")["_en_ultima_descarga"].all()
    # Un solo documento con 404 sí se da por retirado
    for enlace in ods[1:-1]:
        portal.rutas[enlace["url"]] = fixture("scs_CM_2021_1erTrimestre.ods")
    assert correr(tmp_path, "--fuentes", "scs_resumen", "--comprobar-todo") == 0
    df = parquet(tmp_path, "scs_resumen")
    retirado = df["_archivo_origen"] == f"scs_resumen/{Path(ods[-1]['url']).name}"
    assert not df.loc[retirado, "_en_ultima_descarga"].any() and df.loc[~retirado, "_en_ultima_descarga"].all()
