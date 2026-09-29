"""Tests offline de scripts/ccaa_andalucia_menores.py.

El CKAN de datos abiertos de la Junta (package_search paginado, package_show con
404 si el conjunto no existe y los ficheros subidos, que CKAN anuncia en su host
interno y se sirven en www.juntadeandalucia.es) se simula con un
``requests.get`` falso. Los CSV reproducen el formato real de cada año (medido
el 2026-09-29): '|' y CRLF; 2018-2023 en ZIP; cp1252 hasta 2025 y UTF-8 en
2026; campos rellenos de espacios desde 2023; coma decimal y fechas dd/mm/aaaa
en 2025; ' FECHA_ADJUDICACION' con un espacio en la cabecera de 2022.
"""

import gzip
import importlib.util
import io
import json
import runpy
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_andalucia_menores.py"
SLEEP_REAL = time.sleep


def _cargar():
    spec = importlib.util.spec_from_file_location("ccaa_andalucia_menores", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
ANIO = datetime.now().year          # el script decide qué refrescar con el año real
ANIOS = list(range(2018, max(ANIO, 2026) + 1))
INTERNO = "https://gdc-pdpopendata-ckan.paas.junta-andalucia.es/datosabiertos/portal"
PUBLICO = "https://www.juntadeandalucia.es/datosabiertos/portal"
SAS = "Servicio Andaluz de Salud"

CABECERA = ["ID_EXPEDIENTE", "ORGANO_CONTRATACION", "NUM_EXPEDIENTE", "TITULO", "DESCRIPCION", "TIPO_CONTRATO",
            "PROCEDIMIENTO_ADJUDICACION", "DURACION_CONTRATO", "DURACION_MEDIDA", "LUGAR_EJECUCION_CODIGO",
            "LUGAR_EJECUCION_DENOMINACION", "VALOR_ESTIMADO", "IMPORTE_ADJUDICACION_SIN_IVA",
            "IMPORTE_ADJUDICACION_CON_IVA", "FINANCIACION_EUROPEA", "FINANCIADO_POR", "TASA_COFINANCIACION",
            "NUM_LICITADORES_PRESENTADOS", "NIF_ADJUDICATARIO", "ADJUDICATARIO_DENOMINACION", "FECHA_ADJUDICACION",
            "FECHA_FORMALIZACION", "ESTADO"]
# Anchos fijos de 2023-2026 (medidos en el CSV de 2025)
ANCHOS = [12, 500, 100, 300, 300, 100, 10, 10, 10, 10, 100, 20, 20, 20, 2, 20, 20, 20, 20, 100, 30, 30, 20]
IMPORTES = ("VALOR_ESTIMADO", "IMPORTE_ADJUDICACION_SIN_IVA", "IMPORTE_ADJUDICACION_CON_IVA")


# ---------------------------------------------------------------------------
# Ficheros con el formato de cada año
# ---------------------------------------------------------------------------

def fila(anio, n, organo=SAS, **extra):
    """Un contrato menor con los valores normalizados (punto decimal, fecha ISO):
    el formato de cada año se aplica al escribirlo (publicacion)."""
    base = {"ID_EXPEDIENTE": f"{anio % 100}{n:04d}", "ORGANO_CONTRATACION": organo,
            "NUM_EXPEDIENTE": f"CONTR {anio} {n:010d}", "TITULO": f"Suministro {n} de material fungible",
            "DESCRIPCION": "", "TIPO_CONTRATO": "Suministros", "PROCEDIMIENTO_ADJUDICACION": "MENOR",
            "DURACION_CONTRATO": "1", "DURACION_MEDIDA": "M", "LUGAR_EJECUCION_CODIGO": "ES618",
            "LUGAR_EJECUCION_DENOMINACION": "Sevilla", "VALOR_ESTIMADO": "", "IMPORTE_ADJUDICACION_SIN_IVA": "1210.5",
            "IMPORTE_ADJUDICACION_CON_IVA": "1464.71", "FINANCIACION_EUROPEA": "N", "FINANCIADO_POR": "",
            "TASA_COFINANCIACION": "", "NUM_LICITADORES_PRESENTADOS": "1", "NIF_ADJUDICATARIO": "B92214741",
            "ADJUDICATARIO_DENOMINACION": "TECNOLOGÍAS DIGITALES, S.L.", "FECHA_ADJUDICACION": f"{anio}-03-04",
            "FECHA_FORMALIZACION": "", "ESTADO": "Resuelto"}
    base.update(extra)
    return base


def formato(anio):
    return {"zip": anio <= 2023, "relleno": anio >= 2023, "coma": anio == 2025, "ddmm": anio >= 2025,
            "nif_pyc": 2022 <= anio <= 2024, "codificacion": "utf-8" if anio >= 2026 else "cp1252"}


def _valor(columna, valor, fmt):
    if valor and columna in IMPORTES and fmt["coma"]:
        valor = valor.replace(".", ",")
    if valor and columna.startswith("FECHA"):
        a, m, d = valor.split("-")
        valor = f"{d}/{m}/{a}" if fmt["ddmm"] else f"{valor}T00:00:00+0100"
    if valor and columna == "NIF_ADJUDICATARIO" and fmt["nif_pyc"]:
        valor += ";"
    return valor


def csv_anio(anio, filas, fmt=None, cabecera=None, final=True):
    """El CSV de un año tal como lo publica la Junta: '|', CRLF, comillas
    estándar solo donde hacen falta y, desde 2023, relleno hasta el ancho fijo."""
    fmt = fmt or formato(anio)
    cabecera = cabecera or ([" FECHA_ADJUDICACION" if c == "FECHA_ADJUDICACION" else
                             " FECHA_FORMALIZACION" if c == "FECHA_FORMALIZACION" else c for c in CABECERA]
                            if anio == 2022 else CABECERA)
    lineas = ["|".join(cabecera)]
    for f in filas:
        campos = []
        for columna, ancho in zip(CABECERA, ANCHOS):
            valor = _valor(columna, f.get(columna, ""), fmt)
            if fmt["relleno"] or columna == "ESTADO":
                valor = valor.ljust(ancho)
            if "|" in valor or '"' in valor:
                valor = '"' + valor.replace('"', '""') + '"'
            campos.append(valor)
        lineas.append("|".join(campos))
    return ("\r\n".join(lineas) + ("\r\n" if final else "")).encode(fmt["codificacion"])


def _zip(miembros):
    datos = io.BytesIO()
    with zipfile.ZipFile(datos, "w", zipfile.ZIP_DEFLATED) as archivo:
        for nombre, contenido in miembros.items():
            archivo.writestr(zipfile.ZipInfo(nombre, date_time=(2023, 6, 15, 8, 37, 40)), contenido,
                             zipfile.ZIP_DEFLATED)
    return datos.getvalue()


def publicacion(anio, filas, **kw):
    """(nombre publicado, bytes) del fichero de un año: ZIP hasta 2023 (el último
    CSV sin CRLF final, como el de 2018) y CSV suelto desde 2024."""
    if formato(anio)["zip"]:
        return f"menores-{anio}-v1.csv.zip", _zip({f"Menores {anio} v1.csv": csv_anio(anio, filas, final=False, **kw)})
    return f"menores_{anio}_v1_20260618.csv", csv_anio(anio, filas, **kw)


# ---------------------------------------------------------------------------
# CKAN simulado
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, json_data=None, body=b"", headers=None):
        self.status_code = status
        self._json = json_data
        self._body = body
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("no es JSON")
        return self._json

    @property
    def text(self):
        return self._body.decode("utf-8", "replace")

    def iter_content(self, chunk_size=8192):
        yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _nombre(anio):
    return f"contratacion-menor-plataforma-de-contratacion-andalucia-{anio}"


class FakeCKAN:
    """paquetes: nombre -> paquete; ocultos: los que package_show da y la búsqueda no;
    urls: url pública -> bytes | código HTTP | (bytes, cabeceras) | lista (una por petición);
    busqueda / show: código HTTP que devuelven (None = normal)."""

    def __init__(self):
        self.paquetes = {}
        self.ocultos = set()
        self.urls = {}
        self.busqueda = None
        self.show = {}
        self.filas_por_pagina = None
        self.llamadas = []

    def publicar(self, anio, filas, nombre_fichero=None, contenido=None, recurso=None, modificado="2026-07-07T11:31:44",
                 **kw):
        nombre, datos = publicacion(anio, filas, **kw)
        nombre = nombre_fichero or nombre
        datos = datos if contenido is None else contenido
        pid = f"pkg{anio}-0000-4000-8000-000000000000"
        rid = recurso or f"csv{anio}-0000-4000-8000-000000000000"
        camino = f"/dataset/{pid}/resource/{rid}/download/{nombre}"
        self.paquetes[_nombre(anio)] = {
            "id": pid, "name": _nombre(anio),
            "title": (f"Contratación Menor en {anio} publicada en la Plataforma de Contratación de la Junta de "
                      "Andalucía"),
            "resources": [
                {"id": rid, "format": "CSV", "url": INTERNO + camino, "size": len(datos), "last_modified": modificado,
                 "position": 0, "state": "active", "url_type": "upload"},
                {"id": f"json{anio}", "format": "JSON", "position": 1, "state": "active", "url_type": "upload",
                 "url": f"{INTERNO}/dataset/{pid}/resource/json{anio}/download/{Path(nombre).stem}.json"}]}
        self.urls[PUBLICO + camino] = datos
        return PUBLICO + camino

    def url(self, anio):
        recurso = self.paquetes[_nombre(anio)]["resources"][0]
        return M.url_descarga(recurso["url"])

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.llamadas.append((url, dict(params or {})))
        if url == M.URL_API + "package_search":
            if self.busqueda:
                return FakeResponse(status=self.busqueda)
            lista = [p for n, p in sorted(self.paquetes.items()) if n not in self.ocultos]
            lista.append({"id": "ctpda", "name": "contratos-menores-adjudicados",
                          "title": "Contratos menores adjudicados de CTPDA", "resources": []})
            lista.append({"id": "icma", "name": "indice-de-comercio-al-por-menor-de-andalucia",
                          "title": "Índice de Comercio al por Menor de Andalucía (ICMA)", "resources": []})
            ini = int(params["start"])
            n = self.filas_por_pagina or int(params["rows"])
            return FakeResponse(json_data={"success": True, "result": {"count": len(lista),
                                                                       "results": lista[ini:ini + n]}})
        if url == M.URL_API + "package_show":
            ident = params["id"]
            if self.show.get(ident):
                return FakeResponse(status=self.show[ident])
            paquete = next((p for p in self.paquetes.values() if ident in (p["id"], p["name"])), None)
            if paquete is None:
                return FakeResponse(status=404, json_data={"success": False, "error": {
                    "__type": "Not Found Error", "message": "No encontrado"}})
            return FakeResponse(json_data={"success": True, "result": paquete})
        assert not url.startswith(INTERNO), f"se ha pedido el host interno: {url}"
        cuerpo = self.urls.get(url, 404)
        if isinstance(cuerpo, list):
            cuerpo = cuerpo.pop(0) if len(cuerpo) > 1 else cuerpo[0]
        if isinstance(cuerpo, int):
            return FakeResponse(status=cuerpo, body=b"<html><body>Not Found</body></html>")
        if isinstance(cuerpo, tuple):
            return FakeResponse(body=cuerpo[0], headers=cuerpo[1])
        return FakeResponse(body=cuerpo, headers={"Content-Length": str(len(cuerpo))})

    def pedidas(self, url):
        return sum(1 for u, _ in self.llamadas if u == url)


def _filas_del_anio(anio):
    return [fila(anio, 1), fila(anio, 2, organo="Agencia Pública Andaluza de Educación",
                                   NIF_ADJUDICATARIO="630****1948",
                                   ADJUDICATARIO_DENOMINACION="VIZOSO ASTORGA, BENIGNA")]


@pytest.fixture
def portal(monkeypatch):
    fake = FakeCKAN()
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    for anio in ANIOS:
        fake.publicar(anio, _filas_del_anio(anio))
    return fake


def _ejecutar(salida, *args):
    return M.main(["--salida", str(salida), *args])


def _log(salida):
    return (salida / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def _manifiesto(salida):
    return json.loads((salida / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))


def _parquet(salida):
    return pd.read_parquet(salida / M.PARQUET)


def _v(serie):
    return [None if pd.isna(v) else v for v in serie]


def _anio(df, anio):
    return df[df["_anio"] == str(anio)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Descubrimiento y descarga
# ---------------------------------------------------------------------------

def test_descubre_por_ckan_y_guarda_los_originales(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    raw = tmp_path / "raw"
    man = _manifiesto(tmp_path)
    for anio in ANIOS:
        paquete = portal.paquetes[_nombre(anio)]
        recurso = paquete["resources"][0]
        ext = ".csv.zip" if anio <= 2023 else ".csv"
        guardado = raw / str(anio) / f"menores_{anio}{ext}"
        url = recurso["url"].replace(INTERNO, PUBLICO)
        assert guardado.read_bytes() == portal.urls[url]                     # el original, tal cual
        entrada = man[f"{anio}/menores_{anio}"]
        assert entrada["url"] == url and entrada["url_ckan"] == recurso["url"]    # host público, no el interno
        assert entrada["archivo"] == f"{anio}/menores_{anio}{ext}" and entrada["publicado"] is True
        assert (entrada["conjunto"], entrada["recurso"], entrada["anio"]) == (_nombre(anio), recurso["id"], anio)
        assert entrada["tamano_ckan"] == len(portal.urls[url]) and entrada["nombre_publicado"] == Path(url).name
        assert list(entrada["versiones"].values())[0]["nombre_publicado"] == Path(url).name
    # El JSON (los mismos datos) no se descarga
    assert not [u for u, _ in portal.llamadas if u.endswith(".json")]
    catalogo = json.loads((raw / "catalogo_ckan.json").read_text(encoding="utf-8"))
    assert sorted(catalogo) == sorted(_nombre(a) for a in ANIOS)
    # Los demás conjuntos con 'menor' y 'contrat' se avisan; los que no son de contratación, no
    log = _log(tmp_path)
    assert "contratos-menores-adjudicados (Contratos menores adjudicados de CTPDA)" in log
    assert "indice-de-comercio" not in log


def test_busqueda_paginada(portal, tmp_path):
    portal.filas_por_pagina = 2
    assert _ejecutar(tmp_path) == 0
    assert portal.pedidas(M.URL_API + "package_search") == (len(ANIOS) + 2 + 1) // 2
    assert len(_manifiesto(tmp_path)) == len(ANIOS)


# ---------------------------------------------------------------------------
# Lectura y Parquet
# ---------------------------------------------------------------------------

def test_parquet_texto_tal_cual_con_el_formato_de_cada_anio(portal, tmp_path):
    portal.publicar(2018, [fila(2018, 1, TITULO='Representación del espectáculo "La extinta poética"   ',
                                IMPORTE_ADJUDICACION_SIN_IVA="7903.89", NIF_ADJUDICATARIO="B-92182336"),
                           fila(2018, 2, FECHA_ADJUDICACION="")])
    portal.publicar(2025, [fila(2025, 1, TITULO="01-GR-2503-AM | MENOR/OBRAS/CC/11/2025",
                                IMPORTE_ADJUDICACION_SIN_IVA="39567.5", IMPORTE_ADJUDICACION_CON_IVA=".36"),
                           fila(2025, 2, organo="IFAPA Centro Hinojosa del Duque", TITULO="Señalización – 5 € «ÜÑ»")])
    portal.publicar(2026, [fila(2026, 1, TITULO="CALIBRACIÓN ENAC – ESPECTROFOTÓMETRO",
                                IMPORTE_ADJUDICACION_SIN_IVA=".01")])
    assert _ejecutar(tmp_path) == 0

    tabla = pq.read_table(tmp_path / M.PARQUET)
    tipos = {f.name: f.type for f in tabla.schema}
    assert all(pa.types.is_string(t) for c, t in tipos.items() if c not in M.DERIVADAS + ("_en_ultima_descarga",))
    assert all(pa.types.is_float64(tipos[c]) for c in M.DERIVADAS) and pa.types.is_boolean(tipos["_en_ultima_descarga"])
    df = tabla.to_pandas()
    # Columnas publicadas en su orden (las de 2022 con espacio, aparte) y después las del script
    assert list(df.columns) == CABECERA + [" FECHA_ADJUDICACION", " FECHA_FORMALIZACION"] + list(M.ORDEN_METADATOS)
    assert len(df) == 2 * len(ANIOS) - 1 and df["_en_ultima_descarga"].all()           # 2026, con una fila

    a18 = _anio(df, 2018)
    assert a18["TITULO"].tolist() == ['Representación del espectáculo "La extinta poética"   ',
                                      "Suministro 2 de material fungible"]           # comillas dobles resueltas
    assert a18["IMPORTE_ADJUDICACION_SIN_IVA"].tolist() == ["7903.89", "1210.5"]
    assert a18["NIF_ADJUDICATARIO"].tolist()[0] == "B-92182336"                      # sin normalizar
    assert _v(a18["FECHA_ADJUDICACION"]) == ["2018-03-04T00:00:00+0100", None]       # vacío = nulo
    assert a18["ESTADO"].tolist() == ["Resuelto" + " " * 12] * 2                     # relleno conservado
    assert set(a18["_miembro"]) == {"Menores 2018 v1.csv"}
    assert set(a18["_fichero_publicado"]) == {"menores-2018-v1.csv.zip"}

    a22 = _anio(df, 2022)
    assert _v(a22["FECHA_ADJUDICACION"]) == [None, None]
    assert a22[" FECHA_ADJUDICACION"].tolist() == ["2022-03-04T00:00:00+0100"] * 2
    assert a22["NIF_ADJUDICATARIO"].tolist()[0] == "B92214741;"

    a25 = _anio(df, 2025)
    assert a25["TITULO"].tolist()[0] == "01-GR-2503-AM | MENOR/OBRAS/CC/11/2025".ljust(300)   # '|' entre comillas
    assert a25["TITULO"].tolist()[1] == "Señalización – 5 € «ÜÑ»".ljust(300)                 # cp1252
    assert a25["ORGANO_CONTRATACION"].tolist()[0] == SAS.ljust(500)
    assert a25["IMPORTE_ADJUDICACION_SIN_IVA"].tolist()[0] == "39567,5".ljust(20)            # coma decimal
    assert a25["FECHA_ADJUDICACION"].tolist()[0] == "04/03/2025".ljust(30)
    assert a25["_importe_adjudicacion_sin_iva_num"].tolist() == [39567.5, 1210.5]
    assert a25["_importe_adjudicacion_con_iva_num"].tolist() == [0.36, 1464.71]
    assert _v(a25["_valor_estimado_num"]) == [None, None]                                   # vacío (relleno)
    assert a25["VALOR_ESTIMADO"].tolist() == [" " * 20] * 2                                 # tal cual

    a26 = _anio(df, 2026)
    assert a26["TITULO"].tolist() == ["CALIBRACIÓN ENAC – ESPECTROFOTÓMETRO".ljust(300)]     # UTF-8
    assert a26["_importe_adjudicacion_sin_iva_num"].tolist() == [0.01]
    assert a26[["_fuente", "_conjunto", "_recurso", "_archivo_origen"]].values.tolist() == [[
        portal.url(2026), _nombre(2026), portal.paquetes[_nombre(2026)]["resources"][0]["id"], "2026/menores_2026"]]
    assert _v(a26["_miembro"]) == [None] and _v(a26["_lineas_unidas"]) == [None]


@pytest.mark.parametrize("texto, esperado", [
    ("1210.5", 1210.5), ("39567,5", 39567.5), (" 14999.04  ", 14999.04), (".01", 0.01), (",36", 0.36),
    ("-5", -5.0), ("7", 7.0), ("", None), ("   ", None), (None, None), ("FEDER", None), ("1.234,56", None),
    ("1,234.56", None), ("12 €", None), ("2020-12-21T00:00:00+0100", None),
    ("15.000", None), ("1,234", None), ("-2.500", None),                  # ¿miles o decimales? no se sabe
    ("0,125", 0.125), ("1234.567", 1234.567), ("14999,999", 14999.999),
])
def test_importes_num(texto, esperado):
    resultado = M.importes_num(pd.Series([texto], dtype=object)).tolist()[0]
    assert (esperado is None and pd.isna(resultado)) or resultado == esperado


@pytest.mark.parametrize("texto, codificacion, esperada", [
    ("A|B\r\n1|Señalización – 5 € de la Consejería\r\n", "cp1252", "cp1252"),
    ("A|B\r\n1|CALIBRACIÓN ENAC – ESPECTROFOTÓMETRO\r\n", "utf-8", "utf-8"),
    ("A|B\r\n1|Póliza año\x81\r\n", "latin-1", "latin-1"),          # 0x81 no existe en cp1252
    ("A|B\r\n1|SIN ACENTOS\r\n", "cp1252", "utf-8"),                # ASCII: vale UTF-8
])
def test_detectar_codificacion(tmp_path, texto, codificacion, esperada):
    ruta = tmp_path / "f.csv"
    ruta.write_bytes(texto.encode(codificacion))
    assert M._detectar_codificacion(ruta) == esperada
    df, avisos = M.leer_csv(ruta)
    assert df["B"].tolist() == [texto.split("\r\n")[1].split("|")[1]] and not avisos


def test_utf8_con_un_byte_suelto_en_cp1252_no_se_estropea_entero(tmp_path):
    filas = [f"{i}|Consejería de Educación|Reparación nº {i}" for i in range(200)]
    datos = ("ID|ORGANO|TITULO\r\n" + "\r\n".join(filas) + "\r\n").encode("utf-8")
    ruta = tmp_path / "f.csv"
    ruta.write_bytes(datos.replace("nº 7\r".encode("utf-8"), "nº 7\r".encode("cp1252"), 1))   # un 'º' en cp1252
    assert M._detectar_codificacion(ruta) == M.UTF8_CON_SUELTOS
    df, avisos = M.leer_csv(ruta)
    assert set(df["ORGANO"]) == {"Consejería de Educación"}                  # sin mojibake
    assert df["TITULO"].tolist()[7] == "Reparación nº 7" and df["TITULO"].tolist()[8] == "Reparación nº 8"
    assert any("UTF-8 con 1 bytes sueltos" in a for a in avisos)


def test_cp1252_con_algo_de_utf8_se_lee_como_cp1252_tal_cual(tmp_path):
    ruta = tmp_path / "f.csv"
    ruta.write_bytes("ID|TITULO\r\n1|Señalización de la Consejería\r\n".encode("cp1252") + "2|Ó\r\n".encode("utf-8"))
    assert M._detectar_codificacion(ruta) == "cp1252"
    df, avisos = M.leer_csv(ruta)
    assert df["TITULO"].tolist() == ["Señalización de la Consejería",
                                     "Ó".encode("utf-8").decode("cp1252")]         # 'Ã“': tal cual, en cp1252
    assert any("1 caracteres en UTF-8 dentro de un fichero en cp1252" in a for a in avisos)


def test_registro_partido_en_tres_lineas_y_con_fin_de_linea_lf(tmp_path):
    cab = "|".join(f"c{i}" for i in range(6))
    ruta = tmp_path / "x.csv"
    ruta.write_bytes((f"{cab}\n1|a|b|c|d|e\n2|título\nsigue\ny acaba|b|c|d|e\n3|a|b|c|d|e\n").encode("utf-8"))
    df, avisos = M.leer_csv(ruta)
    assert df["c0"].tolist() == ["1", "2", "3"]
    assert df["c1"].tolist()[1] == "título\nsigue\ny acaba"                   # con el fin de línea del fichero
    assert _v(df["_lineas_unidas"]) == [None, "3", None] and not [a for a in avisos if "revisar" in a]


def test_importe_con_espacio_en_el_nombre_de_la_columna():
    df = pd.DataFrame({" IMPORTE_ADJUDICACION_SIN_IVA": ["10,5", None], "VALOR_ESTIMADO": ["7", "8"]}, dtype=object)
    M.anadir_derivadas(df)
    assert df["_importe_adjudicacion_sin_iva_num"].tolist()[0] == 10.5
    assert df["_valor_estimado_num"].tolist() == [7.0, 8.0] and df["_importe_adjudicacion_con_iva_num"].isna().all()


def test_zip_con_restos_de_otros_sistemas(tmp_path):
    ruta = tmp_path / "menores_2019.csv.zip"
    ruta.write_bytes(_zip({"Menores 2019 v1.csv": csv_anio(2019, [fila(2019, 1)]),
                           "__MACOSX/._Menores 2019 v1.csv": b"\x00\x05\x16\x07basura|x",
                           "LEEME.txt": b"hola"}))
    df, avisos = M.leer_version(ruta)
    assert set(df["_miembro"]) == {"Menores 2019 v1.csv"} and len(df) == 1
    assert any("__MACOSX" in a and "se ignora" in a for a in avisos)
    assert any("LEEME.txt: no es un CSV" in a for a in avisos)


def test_registro_partido_por_un_salto_de_linea_se_une(portal, tmp_path):
    """Como el expediente 533944 de 2020: un salto de línea sin comillas en el
    título parte el registro en dos líneas (4 y 20 campos)."""
    normal = "|".join(["533943", SAS] + [""] * 20 + ["Resuelto"])
    partido = ("533944|Fundación Pública Andaluza Progreso y Salud|2020/4425|Suministro de dos shaker.\r\n"
               "''Gasto realizado para la ejec|Suministro de dos shaker|Suministros|MENOR|1|D|ES618|Sevilla|1090|1090|"
               "1318.9|S|FEDER|80|1|B82509852;|LABNET  BIOTECNICA;|2020-12-21T00:00:00+0100||Resuelto            ")
    csv = ("|".join(CABECERA) + "\r\n" + normal + "\r\n" + partido + "\r\n" + normal.replace("533943", "533945"))
    portal.publicar(2020, [], contenido=_zip({"Menores 2020 v1.csv": csv.encode("cp1252")}))
    assert _ejecutar(tmp_path) == 0
    a20 = _anio(_parquet(tmp_path), 2020)
    assert a20["ID_EXPEDIENTE"].tolist() == ["533943", "533944", "533945"]
    unido = a20.iloc[1]
    assert unido["TITULO"] == "Suministro de dos shaker.\r\n''Gasto realizado para la ejec"
    assert (unido["NIF_ADJUDICATARIO"], unido["ADJUDICATARIO_DENOMINACION"], unido["ESTADO"]) == (
        "B82509852;", "LABNET  BIOTECNICA;", "Resuelto            ")
    assert unido["FINANCIADO_POR"] == "FEDER" and unido["DESCRIPCION"] == "Suministro de dos shaker"
    assert unido["_importe_adjudicacion_sin_iva_num"] == 1090.0
    assert _v(a20["_lineas_unidas"]) == [None, "2", None]
    assert "1 registros partidos por un salto de línea sin comillas" in _log(tmp_path)


@pytest.mark.parametrize("registros, esperado, lineas", [
    ([["a", "b", "c"]], [["a", "b", "c"]], [None]),
    ([["a", "b"], ["x", "c"]], [["a", "b\r\nx", "c"]], [2]),                       # 2 + 2 - 1 = 3
    ([["a", "b"], ["x"], ["y", "z"]], [["a", "b\r\nx\r\ny", "z"]], [3]),              # tres trozos
    ([["a"], ["b"], ["c", "d", "e"]], [["a"], ["b"], ["c", "d", "e"]], [None] * 3),  # uno completo no se pega
    ([["a", "b"], ["x", "y", "z"]], [["a", "b"], ["x", "y", "z"]], [None, None]),   # el segundo está completo
    ([["a", "b"], ["x", "y"], ["z", "w"]], [["a", "b\r\nx", "y"], ["z", "w"]], [2, None]),
    ([["a", "b"], ["x", "y", "z", "w"]], [["a", "b"], ["x", "y", "z", "w"]], [None, None]),   # no cuadra: se deja
    ([["a", "b"]], [["a", "b"]], [None]),                                            # corto al final
    ([["101", "x"], ["102", "y"]], [["101", "x"], ["102", "y"]], [None, None]),     # empieza como un registro
    ([["101", "x"], ["", "y"]], [["101", "x\r\n", "y"]], [2]),                      # trozo con el primer campo vacío
])
def test_unir_partidos_solo_si_cuadra(registros, esperado, lineas):
    assert M.unir_partidos(registros, 3) == (esperado, lineas)


def test_comilla_literal_y_campos_de_mas(tmp_path):
    ruta = tmp_path / "x.csv"
    cab = "|".join(f"c{i}" for i in range(5))
    ruta.write_bytes((f'{cab}\r\n1|"Obra A|org|10|x\r\n2|Obra B|org|20|y\r\n3|Obra "C"|org|30|z|sobra\r\n')
                     .encode("utf-8"))
    df, avisos = M.leer_csv(ruta)
    assert df["c0"].tolist() == ["1", "2", "3"] and df["c1"].tolist() == ['"Obra A', "Obra B", 'Obra "C"']
    assert _v(df["_columna_extra_1"]) == [None, None, "sobra"]
    assert any("comillas literales" in a for a in avisos) and any("más campos" in a for a in avisos)


def test_fichero_sin_filas_y_zip_sin_csv(tmp_path):
    vacio = tmp_path / "vacio.csv"
    vacio.write_bytes(("|".join(CABECERA) + "\r\n").encode("cp1252"))
    df, _ = M.leer_version(vacio)
    assert len(df) == 0 and list(df.columns) == CABECERA
    nada = tmp_path / "nada.csv.zip"
    nada.write_bytes(_zip({"leeme.txt": b"hola"}))
    with pytest.raises(ValueError, match="ningún CSV"):
        M.leer_version(nada)


def test_url_descarga():
    interna = f"{INTERNO}/dataset/p/resource/r/download/menores_2025_v1_20260618.csv"
    assert M.url_descarga(interna) == f"{PUBLICO}/dataset/p/resource/r/download/menores_2025_v1_20260618.csv"
    assert M.url_descarga("http://www.juntadeandalucia.es/datosabiertos/portal/dataset/p/x.csv") == (
        f"{PUBLICO}/dataset/p/x.csv")
    assert M.url_descarga("https://otro.sitio.es/menores.csv") == "https://otro.sitio.es/menores.csv"


# ---------------------------------------------------------------------------
# Sesgo del superviviente: versiones, retirados y fallos
# ---------------------------------------------------------------------------

def test_registro_retirado_y_modificado_se_conservan(portal, tmp_path):
    portal.publicar(ANIO, [fila(ANIO, 1), fila(ANIO, 2), fila(ANIO, 3)])
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)          # la versión nueva tiene que llevar otra fecha (resolución: 1 s)
    portal.publicar(ANIO, [fila(ANIO, 1), fila(ANIO, 3, IMPORTE_ADJUDICACION_SIN_IVA="999"), fila(ANIO, 4)],
                    nombre_fichero=f"menores_{ANIO}02_v1_20261020.csv", modificado="2026-10-21T09:00:00")
    assert _ejecutar(tmp_path) == 0

    actual = _anio(_parquet(tmp_path), ANIO)
    ids = [f"{ANIO % 100}{n:04d}".ljust(12) for n in (1, 2, 3, 3, 4)]
    assert actual["ID_EXPEDIENTE"].tolist() == ids
    assert actual["_en_ultima_descarga"].tolist() == [True, False, False, True, True]
    assert actual["_importe_adjudicacion_sin_iva_num"].tolist() == [1210.5, 1210.5, 1210.5, 999.0, 1210.5]
    # Cada fila, con el fichero publicado en el que apareció por primera vez
    assert actual["_fichero_publicado"].tolist() == [f"menores_{ANIO}_v1_20260618.csv"] * 3 + [
        f"menores_{ANIO}02_v1_20261020.csv"] * 2
    assert actual["_primera_descarga"].nunique() == 2
    # _fuente y _fecha_descarga, los de la versión en la que apareció cada fila
    assert actual["_fuente"].tolist()[:3] == [actual["_fuente"][0]] * 3 and actual["_fuente"][0] != actual["_fuente"][4]
    assert actual["_fuente"].tolist()[3:] == [portal.url(ANIO)] * 2
    assert actual["_fecha_descarga"].tolist() == actual["_primera_descarga"].tolist()
    raw = tmp_path / "raw" / str(ANIO)
    assert len(M.versiones_fichero(tmp_path / "raw", f"{ANIO}/menores_{ANIO}")) == 2
    assert [p.name for p in raw.iterdir() if p.is_file()] == [f"menores_{ANIO}.csv"]      # un solo fichero del año
    otros = _parquet(tmp_path)
    assert otros.loc[otros["_anio"] != str(ANIO), "_en_ultima_descarga"].all()


def test_de_zip_a_csv_es_otra_version_del_mismo_anio(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    # La Junta vuelve a publicar 2019 como CSV suelto, con una fila más
    nuevo = csv_anio(2019, _filas_del_anio(2019) + [fila(2019, 3)])
    portal.publicar(2019, [], nombre_fichero="menores_2019_v2_20270101.csv", contenido=nuevo,
                    modificado="2027-01-02T10:00:00")
    assert _ejecutar(tmp_path) == 0
    a19 = _anio(_parquet(tmp_path), 2019)
    assert len(a19) == 3 and a19["_en_ultima_descarga"].all()          # las dos de antes no se duplican
    assert _v(a19["_miembro"]) == ["Menores 2019 v1.csv"] * 2 + [None]
    assert set(a19["_archivo_origen"]) == {"2019/menores_2019"}
    raw = tmp_path / "raw" / "2019"
    assert sorted(p.name for p in raw.iterdir() if p.is_file()) == ["menores_2019.csv", "menores_2019.csv.zip"]
    assert _manifiesto(tmp_path)["2019/menores_2019"]["archivo"] == "2019/menores_2019.csv"
    assert "cambia de formato (menores_2019.csv.zip -> menores_2019.csv)" in _log(tmp_path)


def test_conjunto_retirado_por_ckan_conserva_sus_filas(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    del portal.paquetes[_nombre(2020)]                  # package_show da 404
    assert _ejecutar(tmp_path) == 0
    df = _parquet(tmp_path)
    assert _anio(df, 2020)["_en_ultima_descarga"].tolist() == [False, False]
    assert df.loc[df["_anio"] != "2020", "_en_ultima_descarga"].all()
    entrada = _manifiesto(tmp_path)["2020/menores_2020"]
    assert entrada["publicado"] is False and "404" in entrada["detalle"]
    assert (tmp_path / "raw" / "2020" / "menores_2020.csv.zip").exists()
    assert "2020/menores_2020: CKAN ya no tiene" in _log(tmp_path)


def test_la_busqueda_no_lo_devuelve_pero_sigue_publicado(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    portal.ocultos.add(_nombre(2021))                  # el índice de búsqueda va por detrás
    assert _ejecutar(tmp_path) == 0
    assert _parquet(tmp_path)["_en_ultima_descarga"].all()
    assert _manifiesto(tmp_path)["2021/menores_2021"]["publicado"] is True
    assert "2021: la búsqueda no devuelve" in _log(tmp_path)


@pytest.mark.parametrize("codigo", [500, 403])
def test_package_show_con_error_no_retira_nada(portal, tmp_path, codigo):
    assert _ejecutar(tmp_path) == 0
    portal.ocultos.add(_nombre(2021))
    portal.show[portal.paquetes[_nombre(2021)]["id"]] = codigo          # se pregunta por el id del conjunto
    assert _ejecutar(tmp_path) == 1
    assert _parquet(tmp_path)["_en_ultima_descarga"].all()
    assert _manifiesto(tmp_path)["2021/menores_2021"]["publicado"] is True
    assert "2021/menores_2021: no se pudo comprobar su conjunto" in _log(tmp_path).split("ERRORES")[-1]


def test_conjunto_renombrado_no_se_retira(portal, tmp_path):
    """La Junta cambia el nombre y el título del conjunto de un año: por su id sigue ahí."""
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    paquete = portal.paquetes.pop(_nombre(2020))
    paquete["name"], paquete["title"] = "menores-plataforma-junta-2020", "Relación de contratos menores 2020"
    portal.paquetes[paquete["name"]] = paquete
    assert _ejecutar(tmp_path, "--comprobar-todo") == 0
    assert _anio(_parquet(tmp_path), 2020)["_en_ultima_descarga"].all()
    entrada = _manifiesto(tmp_path)["2020/menores_2020"]
    assert entrada["publicado"] is True and entrada["conjunto"] == "menores-plataforma-junta-2020"
    assert "2020: la búsqueda no devuelve menores-plataforma-junta-2020, pero sigue publicado" in _log(tmp_path)


def test_si_falla_un_package_show_no_se_guarda_el_catalogo(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    catalogo = (tmp_path / "raw" / "catalogo_ckan.json").read_bytes()
    SLEEP_REAL(1.1)
    portal.show[portal.paquetes[_nombre(2019)]["id"]] = 500
    assert _ejecutar(tmp_path) == 0                      # se descarga con los recursos de la búsqueda
    assert (tmp_path / "raw" / "catalogo_ckan.json").read_bytes() == catalogo
    assert not (tmp_path / "raw" / M.HISTORICO).exists()
    assert "no se guarda raw/catalogo_ckan.json" in _log(tmp_path)


def test_catalogo_caido_no_retira_nada(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    SLEEP_REAL(1.1)
    portal.busqueda = 503
    assert _ejecutar(tmp_path) == 1
    despues = _parquet(tmp_path)
    pd.testing.assert_frame_equal(despues, antes)
    assert all(i["publicado"] for i in _manifiesto(tmp_path).values())
    assert "catálogo CKAN" in _log(tmp_path).split("ERRORES")[-1]
    assert not (tmp_path / M.HISTORICO).exists()                 # el Parquet no ha cambiado


def test_catalogo_sin_ningun_conjunto_de_la_serie_no_retira_nada(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.ocultos.update(portal.paquetes)                       # cero resultados de la serie
    assert _ejecutar(tmp_path) == 1
    assert _parquet(tmp_path)["_en_ultima_descarga"].all()
    assert all(i["publicado"] for i in _manifiesto(tmp_path).values())
    assert "no devuelve ningún conjunto de la serie" in _log(tmp_path)


def test_primera_ejecucion_con_el_catalogo_vacio(portal, tmp_path):
    portal.ocultos.update(portal.paquetes)
    assert _ejecutar(tmp_path) == 1
    assert not (tmp_path / M.PARQUET).exists()
    assert "no devuelve ningún conjunto de la serie" in _log(tmp_path)


@pytest.mark.parametrize("fallo", [503, 404, b"<!DOCTYPE html><html><body>Mantenimiento</body></html>"])
def test_anio_que_falla_sale_con_1_y_no_pierde_la_copia(portal, tmp_path, fallo):
    assert _ejecutar(tmp_path) == 0
    original = portal.urls[portal.url(ANIO)]
    SLEEP_REAL(1.1)
    portal.urls[portal.url(ANIO)] = fallo
    assert _ejecutar(tmp_path) == 1
    raw = tmp_path / "raw" / str(ANIO) / f"menores_{ANIO}.csv"
    assert raw.read_bytes() == original and len(M.versiones_fichero(tmp_path / "raw", f"{ANIO}/menores_{ANIO}")) == 1
    assert _parquet(tmp_path)["_en_ultima_descarga"].all()
    assert _manifiesto(tmp_path)[f"{ANIO}/menores_{ANIO}"]["publicado"] is True
    assert f"{ANIO}/menores_{ANIO}" in _log(tmp_path).split("ERRORES")[-1]


def test_un_anio_que_falla_no_impide_los_demas(portal, tmp_path):
    portal.urls[portal.url(2019)] = 503
    assert _ejecutar(tmp_path) == 1
    df = _parquet(tmp_path)
    assert sorted(set(df["_anio"])) == sorted(str(a) for a in ANIOS if a != 2019)
    assert "2019/menores_2019: HTTP 503" in _log(tmp_path).split("ERRORES")[-1]


def test_anio_confirmado_que_falta_es_un_error(portal, tmp_path):
    del portal.paquetes[_nombre(2019)]
    assert _ejecutar(tmp_path) == 1
    assert "2019: año publicado" in _log(tmp_path).split("ERRORES")[-1]


def test_anio_sin_conjunto_que_no_estaba_confirmado_se_anota(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(M, "CONFIRMADOS", range(2019, 2027))
    del portal.paquetes[_nombre(2018)]
    assert _ejecutar(tmp_path) == 0
    assert "AÑOS SIN CONJUNTO EN EL CATÁLOGO: 2018" in _log(tmp_path)


def test_conjunto_sin_csv_es_un_error(portal, tmp_path):
    portal.paquetes[_nombre(2019)]["resources"] = portal.paquetes[_nombre(2019)]["resources"][1:]   # solo JSON
    assert _ejecutar(tmp_path) == 1
    assert "no tiene ningún recurso CSV" in _log(tmp_path).split("ERRORES")[-1]


def test_version_vigente_sin_filas_no_retira_nada_y_es_un_error(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.publicar(ANIO, [], modificado="2026-11-01T00:00:00")          # solo la cabecera
    assert _ejecutar(tmp_path) == 1
    actual = _anio(_parquet(tmp_path), ANIO)
    assert len(actual) == 2 and actual["_en_ultima_descarga"].all()
    assert "no tiene filas; no se marca nada como retirado (es la versión vigente" in _log(tmp_path).split(
        "ERRORES")[-1]


@pytest.mark.parametrize("cabeceras", [{}, {"Content-Encoding": "gzip"}])
def test_descarga_cortada_sin_content_length_la_delata_el_tamano_de_ckan(portal, tmp_path, cabeceras):
    url = portal.publicar(ANIO, [fila(ANIO, n) for n in range(1, 6)])
    completo = portal.urls[url]
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    cortado = b"\r\n".join(completo.split(b"\r\n")[:3]) + b"\r\n"          # cabecera y 2 filas: corte limpio
    portal.urls[url] = (cortado, cabeceras)
    assert _ejecutar(tmp_path) == 1
    assert _anio(_parquet(tmp_path), ANIO)["_en_ultima_descarga"].all()
    assert (tmp_path / "raw" / str(ANIO) / f"menores_{ANIO}.csv").read_bytes() == completo
    assert "sin Content-Length" in _log(tmp_path).split("ERRORES")[-1]


@pytest.mark.parametrize("comprimir", [gzip.compress, lambda d: b"BZh91AY&SY" + d, lambda d: b"Rar!\x1a\x07\x00" + d])
def test_un_comprimido_no_se_toma_por_un_csv(portal, tmp_path, comprimir):
    url = portal.publicar(ANIO, [fila(ANIO, 1)])
    portal.urls[url] = comprimir(portal.urls[url])
    assert _ejecutar(tmp_path) == 1
    assert str(ANIO) not in set(_parquet(tmp_path)["_anio"])
    assert "COMPRIMIDO, no un CSV" in _log(tmp_path).split("ERRORES")[-1]


def test_descarga_cortada_se_vuelve_a_pedir(portal, tmp_path):
    bueno = portal.urls[portal.url(ANIO)]
    portal.urls[portal.url(ANIO)] = [(bueno[:-40], {"Content-Length": str(len(bueno))}), bueno]
    zip_bueno = portal.urls[portal.url(2019)]
    portal.urls[portal.url(2019)] = [zip_bueno[: len(zip_bueno) // 2], zip_bueno]     # sin su índice
    assert _ejecutar(tmp_path) == 0
    assert portal.pedidas(portal.url(ANIO)) == 2 and portal.pedidas(portal.url(2019)) == 2
    assert (tmp_path / "raw" / str(ANIO) / f"menores_{ANIO}.csv").read_bytes() == bueno


def test_solo_se_vuelven_a_pedir_el_actual_el_anterior_y_lo_que_cambia_en_ckan(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    assert _ejecutar(tmp_path) == 0
    assert portal.pedidas(portal.url(2019)) == 1 and portal.pedidas(portal.url(ANIO)) == 2
    assert portal.pedidas(portal.url(ANIO - 1)) == 2
    # CKAN da el recurso de 2019 por cambiado (otra fecha de modificación): se vuelve a pedir
    portal.paquetes[_nombre(2019)]["resources"][0]["last_modified"] = "2027-02-01T00:00:00"
    assert _ejecutar(tmp_path) == 0
    assert portal.pedidas(portal.url(2019)) == 2 and portal.pedidas(portal.url(2020)) == 1
    assert _ejecutar(tmp_path, "--comprobar-todo") == 0
    assert portal.pedidas(portal.url(2020)) == 2
    # Ningún fichero de datos ha cambiado de verdad (el catálogo sí: otra fecha de 2019)
    assert not [p for p in (tmp_path / "raw").rglob(M.HISTORICO) if p.parent.name.isdigit()]


def test_version_con_la_cabecera_cambiada_no_se_aplica(portal, tmp_path):
    """Sin ID_EXPEDIENTE ni NUM_EXPEDIENTE (aquí, la cabecera rellena de espacios como los
    datos) acumular() casaría filas de contratos distintos: la versión no se aplica."""
    portal.publicar(ANIO, [fila(ANIO, 1), fila(ANIO, 2), fila(ANIO, 3)])
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    SLEEP_REAL(1.1)
    cabecera = [c.ljust(len(c) + 3) for c in CABECERA]
    portal.publicar(ANIO, [], contenido=csv_anio(ANIO, [fila(ANIO, 3), fila(ANIO, 4)], cabecera=cabecera),
                    nombre_fichero=f"menores_{ANIO}_v2.csv", modificado="2026-12-01T00:00:00")
    assert _ejecutar(tmp_path) == 1
    pd.testing.assert_frame_equal(_parquet(tmp_path), antes)                  # el año conserva lo que tenía
    assert len(M.versiones_fichero(tmp_path / "raw", f"{ANIO}/menores_{ANIO}")) == 2      # la versión, guardada
    errores = _log(tmp_path).split("ERRORES")[-1]
    assert f"{ANIO}/menores_{ANIO}: la versión del" in errores and "no trae ID_EXPEDIENTE, NUM_EXPEDIENTE" in errores


def test_primera_version_sin_identificadores_no_entra(portal, tmp_path):
    url = portal.publicar(ANIO, [fila(ANIO, 1)])
    portal.urls[url] = "Expediente|Importe\r\nA|10\r\n".encode("cp1252")    # otro CSV subido por error
    assert _ejecutar(tmp_path) == 1
    assert str(ANIO) not in set(_parquet(tmp_path)["_anio"])


def test_caida_brusca_de_filas_se_avisa(portal, tmp_path):
    portal.publicar(ANIO, [fila(ANIO, n) for n in range(1, 21)])
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.publicar(ANIO, [fila(ANIO, 1)], modificado="2026-12-01T00:00:00")
    assert _ejecutar(tmp_path) == 0
    assert _anio(_parquet(tmp_path), ANIO)["_en_ultima_descarga"].sum() == 1
    assert "cambia o retira 19 de las 20 filas vigentes (más de la mitad)" in _log(tmp_path).split("AVISOS")[-1]


def test_version_con_otro_relleno_se_avisa(portal, tmp_path):
    """Si la Junta vuelve a exportar un año con otro formato, todas sus filas salen cambiadas: se avisa."""
    portal.publicar(2019, [fila(2019, n) for n in range(1, 6)])
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    relleno = dict(formato(2019), relleno=True)
    portal.publicar(2019, [], nombre_fichero="menores_2019_v2.csv", modificado="2027-01-01T00:00:00",
                    contenido=csv_anio(2019, [fila(2019, n) for n in range(1, 6)], fmt=relleno))
    assert _ejecutar(tmp_path) == 0
    a19 = _anio(_parquet(tmp_path), 2019)
    assert a19["_en_ultima_descarga"].tolist() == [False] * 5 + [True] * 5      # nada se pierde
    assert "cambia o retira 5 de las 5 filas vigentes" in _log(tmp_path).split("AVISOS")[-1]


def test_vuelta_a_un_zip_identico_tras_un_csv(portal, tmp_path):
    zip_v1 = portal.urls[portal.publicar(2019, [fila(2019, 1), fila(2019, 2)])]
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.publicar(2019, [], nombre_fichero="menores_2019_v2.csv", contenido=csv_anio(2019, [fila(2019, 1)]),
                    modificado="2027-01-01T00:00:00")
    assert _ejecutar(tmp_path) == 0
    assert _anio(_parquet(tmp_path), 2019)["_en_ultima_descarga"].tolist() == [True, False]
    SLEEP_REAL(1.1)
    portal.publicar(2019, [], nombre_fichero="menores-2019-v1.csv.zip", contenido=zip_v1,
                    modificado="2027-02-01T00:00:00")
    assert _ejecutar(tmp_path) == 0
    a19 = _anio(_parquet(tmp_path), 2019)
    assert a19["_en_ultima_descarga"].tolist() == [True, True]                 # vuelve a estar publicada
    assert _manifiesto(tmp_path)["2019/menores_2019"]["archivo"] == "2019/menores_2019.csv.zip"
    assert len(M.versiones_fichero(tmp_path / "raw", "2019/menores_2019")) == 3


def test_copia_de_seguridad_en_raw_no_es_una_version(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    SLEEP_REAL(1.1)
    viejo = _zip({"Menores 2019 v1.csv": csv_anio(2019, [fila(2019, 1)], final=False)})
    (tmp_path / "raw" / "2019" / "menores_2019.csv.zip.bak_20260929_120000123").write_bytes(viejo)
    (tmp_path / "raw" / "2019" / "notas.txt").write_text("copia a mano", encoding="utf-8")
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    pd.testing.assert_frame_equal(_parquet(tmp_path), antes)


@pytest.mark.parametrize("nombre, base", [
    ("menores_2023.csv.zip", "menores_2023"), ("menores_2025.csv", "menores_2025"),
    ("menores_2023.csv__20260929T101500Z.zip", "menores_2023"), ("menores_2025__20260929T101500Z.csv", "menores_2025"),
    ("menores_2025__20260929T101500Z_1.csv", "menores_2025"), ("menores_2021_csv2021b.csv", "menores_2021_csv2021b"),
    ("menores_2019.zip", "menores_2019"), ("menores_2019.csv.zip.bak_20260929_120000123", None),
    (".menores_2025.csv.part", None), ("notas.txt", None), ("menores_2025.json", None),
])
def test_base_de(nombre, base):
    assert M.base_de(nombre) == base


def test_manifiesto_corrupto_para_sin_tocar_nada(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    del portal.paquetes[_nombre(2020)]
    assert _ejecutar(tmp_path) == 0
    antes = (tmp_path / M.PARQUET).read_bytes()
    ruta = tmp_path / "raw" / "_manifiesto.json"
    corrupto = ruta.read_text(encoding="utf-8")[:-40]                         # JSON cortado
    ruta.write_text(corrupto, encoding="utf-8")
    pedidas = len(portal.llamadas)
    assert _ejecutar(tmp_path) == 1
    assert _ejecutar(tmp_path, "--solo-parquet") == 1
    assert ruta.read_text(encoding="utf-8") == corrupto                      # no se sobrescribe
    assert (tmp_path / M.PARQUET).read_bytes() == antes and len(portal.llamadas) == pedidas
    assert "_manifiesto.json no se puede leer" in _log(tmp_path).split("ERRORES")[-1]


def test_manifiesto_perdido_no_retira_ni_dice_que_no_hay_copia(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    del portal.paquetes[_nombre(2020)]
    (tmp_path / "raw" / "_manifiesto.json").unlink()
    assert _ejecutar(tmp_path) == 1
    assert _anio(_parquet(tmp_path), 2020)["_en_ultima_descarga"].all()
    errores = _log(tmp_path).split("ERRORES")[-1]
    assert "2020: el catálogo no lo devuelve y raw/2020/ tiene copias" in errores
    assert "no se tiene ninguna copia" not in errores


def test_ejecucion_sin_cambios_no_reescribe_nada(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    antes = (tmp_path / M.PARQUET).read_bytes()
    SLEEP_REAL(1.1)
    assert _ejecutar(tmp_path) == 0
    assert (tmp_path / M.PARQUET).read_bytes() == antes
    assert not list(tmp_path.rglob(M.HISTORICO))
    entrada = _manifiesto(tmp_path)[f"{ANIO}/menores_{ANIO}"]
    assert entrada["comprobado"] > entrada["fecha_descarga"]         # la comprobación, en el manifiesto


def test_regla3_un_arreglo_de_lectura_llega_a_todas_las_versiones(portal, tmp_path, monkeypatch):
    """El Parquet se construye con el código actual desde todas las versiones de raw/."""
    portal.publicar(ANIO, [fila(ANIO, 1), fila(ANIO, 2)])
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.publicar(ANIO, [fila(ANIO, 1)], modificado="2026-11-01T00:00:00")
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    leer = M.leer_version

    def arreglada(ruta):
        df, avisos = leer(ruta)
        if "TITULO" in df.columns:
            df["TITULO"] = df["TITULO"].map(lambda v: v.upper() if isinstance(v, str) else v)
        return df, avisos

    monkeypatch.setattr(M, "leer_version", arreglada)
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    despues = _parquet(tmp_path)
    assert len(despues) == len(antes)                                            # sin duplicar
    assert despues["TITULO"].str.strip().str.startswith("SUMINISTRO").all()      # también la versión antigua
    assert despues["_en_ultima_descarga"].tolist() == antes["_en_ultima_descarga"].tolist()
    assert despues["_primera_descarga"].tolist() == antes["_primera_descarga"].tolist()


def test_fichero_que_ya_no_esta_en_raw_conserva_sus_filas(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    (tmp_path / "raw" / "2019" / "menores_2019.csv.zip").unlink()
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    pd.testing.assert_frame_equal(_parquet(tmp_path), antes)
    assert "2019/menores_2019 ya no está en raw/" in _log(tmp_path)


def test_version_ilegible_no_quita_filas_de_la_salida(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    antes = _parquet(tmp_path)
    (tmp_path / "raw" / "2019" / "menores_2019.csv.zip").write_bytes(b"PK\x03\x04 roto")
    assert _ejecutar(tmp_path, "--solo-parquet") == 1
    pd.testing.assert_frame_equal(_parquet(tmp_path), antes)
    log = _log(tmp_path)
    assert "2019/menores_2019: no se pudo leer la versión menores_2019.csv.zip" in log.split("ERRORES")[-1]
    assert "2019/menores_2019 1 versiones no se han podido leer; se conservan sus 2 filas" in log


def test_primera_version_ilegible(portal, tmp_path, monkeypatch):
    real = M.validar_contenido
    portal.urls[portal.url(2019)] = b"PK\x03\x04 roto"
    monkeypatch.setattr(M, "validar_contenido", lambda ruta: (None, False))     # se guarda aunque esté roto
    assert _ejecutar(tmp_path, "--solo-descarga", "--desde", "2019", "--hasta", "2019") == 0
    monkeypatch.setattr(M, "validar_contenido", real)
    assert _ejecutar(tmp_path, "--solo-parquet") == 1
    assert not (tmp_path / M.PARQUET).exists()                             # nada que escribir: solo 2019, ilegible
    assert "2019/menores_2019: no se pudo leer la versión" in _log(tmp_path).split("ERRORES")[-1]


def test_ckan_que_responde_sin_exito_no_retira_nada(portal, tmp_path, monkeypatch):
    assert _ejecutar(tmp_path) == 0
    real = portal.get

    def sin_exito(url, params=None, **kw):
        if url == M.URL_API + "package_search":
            return FakeResponse(json_data={"success": False, "error": {"message": "Solr caído"}})
        return real(url, params=params, **kw)

    monkeypatch.setattr(requests, "get", sin_exito)
    assert _ejecutar(tmp_path) == 1
    assert _parquet(tmp_path)["_en_ultima_descarga"].all()
    assert "Solr caído" in _log(tmp_path).split("ERRORES")[-1]


def test_recurso_que_no_esta_activo_no_se_descarga(portal, tmp_path):
    paquete = portal.paquetes[_nombre(2021)]
    borrado = dict(paquete["resources"][0], id="csv2021-borrado", state="deleted", position=3,
                   url=f"{INTERNO}/dataset/{paquete['id']}/resource/csv2021-borrado/download/viejo.csv")
    paquete["resources"].append(borrado)
    assert _ejecutar(tmp_path) == 0
    assert portal.pedidas(M.url_descarga(borrado["url"])) == 0
    assert sorted(f for f in _manifiesto(tmp_path) if f.startswith("2021/")) == ["2021/menores_2021"]


def test_varios_csv_en_un_conjunto(portal, tmp_path):
    paquete = portal.paquetes[_nombre(2021)]
    extra = dict(paquete["resources"][0], id="csv2021b-0000", position=2,
                 url=f"{INTERNO}/dataset/{paquete['id']}/resource/csv2021b-0000/download/menores_2021_anexo.csv")
    paquete["resources"].append(extra)
    portal.urls[M.url_descarga(extra["url"])] = csv_anio(2021, [fila(2021, 7)])
    assert _ejecutar(tmp_path) == 0
    a21 = _anio(_parquet(tmp_path), 2021)
    assert sorted(set(a21["_archivo_origen"])) == ["2021/menores_2021", "2021/menores_2021_csv2021b"]
    assert len(a21) == 3
    # El segundo desaparece del conjunto: sus filas se conservan como retiradas
    SLEEP_REAL(1.1)
    paquete["resources"].pop()
    assert _ejecutar(tmp_path) == 0                                      # sin volver a descargar 2021
    a21 = _anio(_parquet(tmp_path), 2021)
    assert a21.groupby("_archivo_origen")["_en_ultima_descarga"].all().to_dict() == {
        "2021/menores_2021": True, "2021/menores_2021_csv2021b": False}


def test_solo_parquet_no_toca_la_red(portal, tmp_path):
    assert _ejecutar(tmp_path) == 0
    pedidas = len(portal.llamadas)
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    assert len(portal.llamadas) == pedidas


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--salida", str(tmp_path), "--desde", str(ANIO)])
    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert salida.value.code == 0
    assert portal.pedidas(portal.url(2019)) == 0                         # --desde: solo desde ese año
    assert set(_parquet(tmp_path)["_anio"]) == {str(ANIO)}


def test_salida_por_defecto_en_el_repo():
    assert M.SALIDA == REPO_ROOT / "ccaa_andalucia_menores"
