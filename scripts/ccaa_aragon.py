#!/usr/bin/env python3
"""
=============================================================================
CONTRATACIÓN PÚBLICA - ARAGÓN (Gobierno de Aragón + Ayuntamiento de Zaragoza)
=============================================================================
Las licitaciones del Gobierno de Aragón publicadas desde 2018 ya llegan por
PLACSP (nacional/). Este script añade lo que PLACSP no tiene: contratos
menores, encargos a medios propios, el histórico anterior a 2018 y el
Ayuntamiento de Zaragoza.

FUENTES
-------
1. Aragón Open Data (CKAN) - https://opendata.aragon.es
   package_show lista los recursos de cada dataset y se descargan TODOS los
   años/series que publique (sin listas fijas de años):
   - registro-de-contratos-de-la-comunidad-autonoma-de-aragon-desde-2023
       Registro de contratos: mayores, menores y encargos desde 2023 (NIF,
       CPV, importes, nº de ofertas). Acumulativo: se comprueba en cada
       ejecución si ha cambiado.
   - contratos-gobierno-de-aragon (id 9c05a8a6-ef3f-4223-94a5-9669a6e5a48e)
       Contratos adjudicados desde 2009 y contratos menores 2014-2025, un
       recurso por año. Algunos años son SpreadsheetML (.xls.xml), que pandas
       no lee: se parsean con xml.etree; si el mismo año está también en CSV
       se usa el CSV.
   - anuncios-del-perfil-del-contratante-del-gobierno-de-aragon
       Anuncios del perfil de contratante (hasta el 12-2-2021).
   Los recursos se agrupan por serie (nombre sin año ni formato) y año; de
   cada grupo se descarga un formato (CSV > XLSX > XLS > ODS > JSON > XML) y,
   si falla, el siguiente.
2. Ayuntamiento de Zaragoza - OCDS en el registro de Open Contracting
   Partnership: se descargan tal cual los JSONL.gz anuales que enlace la
   página de la publicación y se aplanan releases, awards, contracts y
   parties (una fila por elemento, el resto de listas como JSON).
3. Ayuntamiento de Zaragoza - API REST de contratación (opcional,
   --zaragoza-api): listado paginado de contratos; cada ejecución completa se
   guarda como un único JSON con todas las páginas tal cual.

CONFIRMADO / A VERIFICAR EN VIVO
--------------------------------
Los portales no eran accesibles desde el entorno donde se escribió el script:
solo se ha probado con tests offline (requests simulado).
  CONFIRMADO (página oficial del dataset):
    - Los tres identificadores de dataset de arriba y el id 9c05a8a6-... .
    - Que contratos-gobierno-de-aragon mezcla CSV y SpreadsheetML (.xls.xml).
  A VERIFICAR EN VIVO:
    - Base de la API CKAN: se prueban CKAN_API_CANDIDATAS en orden
      (/api/3/action y /ckan/api/3/action) y se usa la primera que responde.
    - Nombres de los recursos: serie y año se deducen del nombre y de la URL
      (serie_de / anio_de); si el portal los nombra de otra forma, revisarlos.
    - Formato de cada recurso: campo 'format' de CKAN, parámetro 'formato=' de
      las URL GA_OD_Core o extensión (formato_recurso).
    - Zaragoza OCDS: que la publicación 1 del registro sea el Ayuntamiento de
      Zaragoza y que la página enlace 'download?name=AAAA.jsonl.gz'
      (ZARAGOZA_OCDS_PUBLICACION / PATRON_ENLACE_OCDS).
    - Zaragoza API: endpoint contrato.json con parámetros rows/start y
      respuesta {totalCount, result} (ZARAGOZA_API_CONTRATOS).
  No se usa el DataStore de CKAN (datastore_search / datastore/dump): se
  sirven los ficheros originales.

HISTÓRICO (sesgo del superviviente, comun/historico.py)
-------------------------------------------------------
- Ninguna descarga machaca la anterior: guardar_version deja la versión
  previa en <carpeta>/_historico/ si el contenido cambió.
- Cada parquet se construye con TODAS las versiones de cada fichero crudo
  (acumular en orden cronológico, ámbito = fichero): una fila que la
  administración retire o cambie sigue en la salida con
  _en_ultima_descarga=False. Los ficheros que el portal deja de listar se
  conservan con todas sus filas marcadas _en_ultima_descarga=False.
- Una descarga fallida no toca nada; una versión sin filas no retira nada.

SALIDA (por defecto <repo>/aragon/, independiente del directorio actual)
------
aragon/raw/<dataset>/<id_recurso>.<ext>     originales tal cual (+ _historico/)
aragon/raw/zaragoza_ocds/<AAAA>.jsonl.gz    OCDS tal cual (+ _historico/)
aragon/raw/_manifiesto.json                 URL, fechas, tamaño y sha256 de cada fichero
aragon/<dataset>__<serie>.parquet           p. ej. contratos_gobierno__contratos_menores.parquet
aragon/zaragoza_ocds_{releases,awards,contracts,parties}.parquet
aragon/zaragoza_api_contratos.parquet       (solo con --zaragoza-api)

Parquet: todas las filas y columnas, todo como texto (sin perder ceros a la
izquierda ni valores no numéricos; celdas vacías = nulo). Si los ficheros de
una serie tienen esquemas distintos se conservan todas las columnas (unión).
No se deduplican filas. Columnas añadidas: _fuente, _dataset, _recurso,
_recurso_id, _formato, _anio_recurso, _archivo_origen, _url_origen, _hoja,
_fila_origen, _encabezado (títulos sobre la cabecera, si los hay),
_fecha_descarga, _primera_descarga, _ultima_descarga, _en_ultima_descarga.

Uso:
    python scripts/ccaa_aragon.py                  # descarga + parquet
    python scripts/ccaa_aragon.py --solo-parquet   # regenera parquet desde raw/
    python scripts/ccaa_aragon.py --comprobar-todo # vuelve a pedir también años cerrados
    python scripts/ccaa_aragon.py --sin-zaragoza
    python scripts/ccaa_aragon.py --zaragoza-api
=============================================================================
"""

import argparse
import csv
import gzip
import hashlib
import html
import io
import json
import math
import os
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from datetime import time as hora_del_dia
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import COLUMNAS_META, acumular, guardar_version, versiones  # noqa: E402

# ===========================================================================
# CONFIGURACIÓN
# ===========================================================================
DIR_SALIDA = Path(__file__).resolve().parent.parent / "aragon"

FUENTE_ARAGON = "https://opendata.aragon.es"
PAGINA_DATASET = "https://opendata.aragon.es/datos/catalogo/dataset/{id}"
CKAN_API_CANDIDATAS = [
    "https://opendata.aragon.es/api/3/action",
    "https://opendata.aragon.es/ckan/api/3/action",
]

DATASETS = [
    {"clave": "registro_contratos",
     "id": "registro-de-contratos-de-la-comunidad-autonoma-de-aragon-desde-2023",
     "acumulativo": True},
    {"clave": "contratos_gobierno",
     "id": "contratos-gobierno-de-aragon",
     "ids_alternativos": ["9c05a8a6-ef3f-4223-94a5-9669a6e5a48e"]},
    {"clave": "anuncios_perfil",
     "id": "anuncios-del-perfil-del-contratante-del-gobierno-de-aragon"},
]

ZARAGOZA_OCDS_PUBLICACION = "https://data.open-contracting.org/en/publication/1"
PATRON_ENLACE_OCDS = re.compile(r"""href=["']([^"']*download\?name=([^"'&]+?\.jsonl\.gz))["']""", re.I)
ZARAGOZA_API_CONTRATOS = "https://www.zaragoza.es/sede/servicio/contratacion-publica/contrato.json"
FILAS_POR_PAGINA_API = 500
MAX_PAGINAS_API = 2000

PREFERENCIA_FORMATOS = ["csv", "xlsx", "xls", "ods", "json", "xml"]
FORMATOS_CKAN = {
    "csv": "csv", "text/csv": "csv", "tsv": "csv",
    "xlsx": "xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "xls": "xls", "excel": "xls", "application/vnd.ms-excel": "xls",
    "ods": "ods", "application/vnd.oasis.opendocument.spreadsheet": "ods",
    "json": "json", "application/json": "json",
    "xml": "xml", "application/xml": "xml", "text/xml": "xml", "spreadsheetml": "xml",
}

REINTENTOS = 4
ESPERA_BASE = 2            # segundos; backoff exponencial: 2, 4, 8
TIMEOUT = 120
ESTADOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
CABECERAS = {"User-Agent": "licitaciones-espana/1.0 (+https://github.com/BquantFinance/licitaciones-espana)"}

# Columnas añadidas (van al final, en este orden). Las de IGNORAR cambian de
# una versión a otra del mismo fichero sin que cambie el registro: no cuentan
# al comparar filas en acumular().
METADATOS = ["_fuente", "_dataset", "_recurso", "_recurso_id", "_formato", "_anio_recurso",
             "_archivo_origen", "_url_origen", "_hoja", "_fila_origen", "_linea", "_encabezado",
             "_fecha_descarga"]
IGNORAR = ("_fecha_descarga", "_fila_origen", "_linea", "_encabezado")

TABLAS_OCDS = ("awards", "contracts", "parties")
MAX_FILAS_CABECERA = 30

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))


# ===========================================================================
# UTILIDADES
# ===========================================================================

def anio_en_curso():
    return date.today().year


def ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_de_epoch(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fecha_a_epoch(valor):
    """Fecha ISO (CKAN la da en UTC sin zona) -> epoch, o None."""
    if not valor:
        return None
    try:
        fecha = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        return None
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.timestamp()


def sin_acentos(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto)) if not unicodedata.combining(c))


def sha256_fichero(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


class Resumen:
    """Lo descargado, lo que no cambió, los parquet escritos y lo que falló."""

    def __init__(self):
        self.descargados = []    # (fichero, estado, bytes, motivo)
        self.sin_cambios = []
        self.parquets = []       # (ruta, filas, columnas, estado)
        self.avisos = []
        self.fallos = []

    def descargado(self, fichero, estado, tam, motivo):
        self.descargados.append((fichero, estado, tam, motivo))
        print(f"    {estado}: {fichero} ({tam / 1024:.0f} KB)")

    def aviso(self, texto):
        self.avisos.append(texto)
        print(f"  AVISO: {texto}")

    def fallo(self, texto):
        self.fallos.append(texto)
        print(f"  ERROR: {texto}")

    def imprimir(self, titulo):
        print("\n" + "=" * 70)
        print(f"RESUMEN - {titulo}")
        print("=" * 70)
        total = sum(t for _, _, t, _ in self.descargados)
        print(f"Ficheros pedidos al portal: {len(self.descargados)} ({total / 1024 / 1024:.1f} MB)")
        for fichero, estado, tam, motivo in self.descargados:
            print(f"  {estado:12} {fichero} ({tam / 1024:.0f} KB; {motivo})")
        print(f"Ficheros ya descargados que no hacía falta volver a pedir: {len(self.sin_cambios)}")
        print(f"Parquet: {len(self.parquets)}")
        for ruta, filas, columnas, estado in self.parquets:
            print(f"  {ruta.name}: {filas:,} filas x {columnas} columnas ({estado})")
        if self.avisos:
            print(f"\nAVISOS ({len(self.avisos)}):")
            for texto in self.avisos:
                print(f"  - {texto}")
        if self.fallos:
            print(f"\nFALLOS ({len(self.fallos)}) - vuelve a ejecutar el script para reintentarlos:")
            for texto in self.fallos:
                print(f"  - {texto}")
        else:
            print("\nSin fallos.")


class Manifiesto:
    """raw/_manifiesto.json: de dónde sale cada fichero crudo (URL, fechas,
    tamaño, sha256) y su estado: 'publicado' (el portal lo sirve y es el que se
    usa), 'alternativo' (otro formato del mismo contenido; no se usa) o
    'retirado' (el portal ya no lo lista; sus filas se conservan)."""

    def __init__(self, ruta):
        self.ruta = Path(ruta)
        self.entradas = {}
        if self.ruta.exists():
            self.entradas = json.loads(self.ruta.read_text(encoding="utf-8"))

    def guardar(self):
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_name(f".{self.ruta.name}.tmp")
        tmp.write_text(json.dumps(self.entradas, ensure_ascii=False, indent=1, sort_keys=True),
                       encoding="utf-8")
        os.replace(tmp, self.ruta)

    def registrar(self, clave, destino, estado, meta):
        """Anota una descarga correcta (estado de guardar_version)."""
        ahora = ahora_iso()
        entrada = dict(self.entradas.get(clave, {}))
        entrada.update(meta)
        if estado != "sin_cambios" or "fecha_descarga" not in entrada:
            entrada["fecha_descarga"] = ahora if estado != "sin_cambios" else iso_de_epoch(destino.stat().st_mtime)
        entrada["fecha_comprobacion"] = ahora
        entrada["bytes"] = destino.stat().st_size
        entrada["sha256"] = sha256_fichero(destino)
        self.entradas[clave] = entrada


# ===========================================================================
# DESCARGAS (reintentos con backoff; nunca se machaca la versión anterior)
# ===========================================================================

class ErrorDescarga(Exception):
    def __init__(self, mensaje, estado=None):
        super().__init__(mensaje)
        self.estado = estado


def esperar(intento):
    time.sleep(ESPERA_BASE * 2 ** (intento - 1))


def obtener(url, params=None):
    """GET con reintentos (red, 429, 5xx). Un 4xx es definitivo: ErrorDescarga(.estado)."""
    error = None
    for intento in range(1, REINTENTOS + 1):
        try:
            r = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT)
        except requests.RequestException as e:
            error = f"{type(e).__name__}: {e}"
        else:
            if r.status_code < 400:
                return r
            if r.status_code not in ESTADOS_REINTENTABLES:
                raise ErrorDescarga(f"HTTP {r.status_code}", r.status_code)
            error = f"HTTP {r.status_code}"
        if intento < REINTENTOS:
            esperar(intento)
    raise ErrorDescarga(f"{error} (tras {REINTENTOS} intentos)")


def comprobar_contenido(ruta, formato):
    """Rechaza respuestas que no son el fichero (vacías, páginas HTML de error...)."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    limpia = cabeza.lstrip(b"\xef\xbb\xbf \t\r\n")
    if not limpia:
        raise ErrorDescarga("respuesta vacía")
    minus = limpia.lower()
    es_html = minus.startswith((b"<!doctype html", b"<html")) or b"<body" in minus[:1024]
    if es_html and not (formato == "xls" and b"<table" in minus):
        raise ErrorDescarga("el servidor devolvió una página HTML en lugar del fichero")
    if formato == "json" and limpia[:1] not in (b"{", b"["):
        raise ErrorDescarga("la respuesta no es JSON")
    if formato in ("xlsx", "ods") and not cabeza.startswith(b"PK"):
        raise ErrorDescarga("la respuesta no es un fichero XLSX/ODS")
    if formato == "gz" and not cabeza.startswith(b"\x1f\x8b"):
        raise ErrorDescarga("la respuesta no es un fichero gzip")


def descargar(url, destino, formato=None, params=None):
    """Descarga `url` en `destino` sin machacar la versión anterior
    (guardar_version: si cambió, la anterior pasa a _historico/).

    Escribe en un temporal y solo lo instala si la descarga terminó bien y el
    contenido es válido: si falla, el fichero anterior queda intacto y se lanza
    ErrorDescarga. Devuelve (estado, bytes) con estado 'nuevo', 'actualizado'
    o 'sin_cambios'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_name(f".{destino.name}.part")
    error = None
    try:
        for intento in range(1, REINTENTOS + 1):
            try:
                with requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT, stream=True) as r:
                    if r.status_code >= 400 and r.status_code not in ESTADOS_REINTENTABLES:
                        raise ErrorDescarga(f"HTTP {r.status_code}", r.status_code)
                    if r.status_code >= 400:
                        error = f"HTTP {r.status_code}"
                    else:
                        with open(parcial, "wb") as f:
                            for trozo in r.iter_content(chunk_size=1 << 16):
                                if trozo:
                                    f.write(trozo)
                        comprobar_contenido(parcial, formato)
                        tam = parcial.stat().st_size
                        return guardar_version(destino, desde=parcial), tam
            except ErrorDescarga:
                raise
            except (requests.RequestException, OSError) as e:
                error = f"{type(e).__name__}: {e}"
            if intento < REINTENTOS:
                esperar(intento)
        raise ErrorDescarga(f"{error} (tras {REINTENTOS} intentos)")
    finally:
        if parcial.exists():
            parcial.unlink()


# ===========================================================================
# LECTURA DE FICHEROS COMO TEXTO
# ===========================================================================

def decodificar(datos):
    """utf-8 (con o sin BOM), utf-16 con BOM, cp1252 y, si nada vale, latin-1."""
    if datos.startswith(b"\xef\xbb\xbf"):
        return datos[3:].decode("utf-8", errors="replace")
    if datos.startswith((b"\xff\xfe", b"\xfe\xff")):
        return datos.decode("utf-16")
    for codificacion in ("utf-8", "cp1252"):
        try:
            return datos.decode(codificacion)
        except UnicodeDecodeError:
            pass
    return datos.decode("latin-1")


def tipo_contenido(ruta):
    """Formato real por el contenido (hay .xls que son SpreadsheetML o HTML)."""
    with open(ruta, "rb") as f:
        cabeza = f.read(8192)
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith(b"PK\x03\x04"):
        return "ods" if b"opendocument.spreadsheet" in cabeza[:200] else "xlsx"
    if cabeza.startswith(b"\x1f\x8b"):
        return "gz"
    limpia = cabeza.lstrip(b"\xef\xbb\xbf \t\r\n")
    minus = limpia.lower()
    if minus.startswith(b"<?xml") or minus.startswith(b"<workbook"):
        if b"urn:schemas-microsoft-com:office:spreadsheet" in minus or b"excel.sheet" in minus:
            return "spreadsheetml"
        return "html" if b"<html" in minus else "xml"
    if minus.startswith(b"<") and (b"<table" in minus or b"<html" in minus or b"<!doctype" in minus):
        return "html"
    if limpia[:1] in (b"{", b"["):
        return "json"
    return "csv"


def celda_a_texto(valor):
    """Valor de una celda de Excel -> texto (None si está vacía). Los enteros
    guardados como número salen sin '.0'; las fechas en ISO."""
    if valor is None:
        return None
    if isinstance(valor, str):
        return valor if valor != "" else None
    if isinstance(valor, np.datetime64):
        valor = pd.Timestamp(valor)
    elif isinstance(valor, np.generic):
        valor = valor.item()          # escalares de numpy -> Python
    if isinstance(valor, bool):
        return "TRUE" if valor else "FALSE"
    if isinstance(valor, float):
        if math.isnan(valor):
            return None
        if valor.is_integer() and abs(valor) < 1e16:
            return str(int(valor))
        return repr(float(valor))
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, (pd.Timestamp, datetime)):
        if pd.isna(valor):
            return None
        if (valor.hour, valor.minute, valor.second, valor.microsecond) == (0, 0, 0, 0):
            return valor.date().isoformat()
        return valor.isoformat(sep=" ")
    if isinstance(valor, (date, hora_del_dia)):
        return valor.isoformat()
    try:
        if pd.isna(valor):
            return None
    except (TypeError, ValueError):
        pass
    return str(valor)


def filas_csv(ruta):
    """Registros de un CSV con el módulo csv: una fila con más campos que la
    cabecera no se pierde (las columnas de más se llaman 'Unnamed: N')."""
    texto = decodificar(Path(ruta).read_bytes()).replace("\x00", "")
    primera = texto.split("\n", 1)[0]
    separador = max([";", ",", "\t", "|"], key=lambda s: len(next(csv.reader([primera], delimiter=s))))
    return [("", list(csv.reader(io.StringIO(texto, newline=""), delimiter=separador)))]


def filas_excel(ruta, tipo):
    motor = {"xls": "xlrd", "xlsx": "openpyxl", "ods": "odf"}[tipo]
    hojas = pd.read_excel(ruta, sheet_name=None, header=None, dtype=object, engine=motor,
                          keep_default_na=False, na_values=[])
    return [(str(nombre), [[celda_a_texto(v) for v in fila] for fila in df.itertuples(index=False, name=None)])
            for nombre, df in hojas.items()]


def _local(etiqueta):
    return etiqueta.rsplit("}", 1)[-1]


def _atributo(elemento, nombre):
    for clave, valor in elemento.attrib.items():
        if _local(clave) == nombre:
            return valor
    return None


def filas_spreadsheetml(ruta):
    """Hojas de un XML Spreadsheet 2003 (.xls.xml), que pandas no lee. Respeta
    ss:Index (celdas y filas saltadas) y ss:MergeAcross (celdas combinadas)."""
    hojas, filas, nombre = [], None, None
    for evento, elem in ET.iterparse(str(ruta), events=("start", "end")):
        etiqueta = _local(elem.tag)
        if evento == "start":
            if etiqueta == "Worksheet":
                nombre = _atributo(elem, "Name") or f"Hoja{len(hojas) + 1}"
                filas = []
            continue
        if etiqueta == "Row" and filas is not None:
            indice = _atributo(elem, "Index")
            if indice:
                while len(filas) < int(indice) - 1:
                    filas.append([])
            fila = []
            for celda in elem:
                if _local(celda.tag) != "Cell":
                    continue
                columna = _atributo(celda, "Index")
                if columna:
                    fila.extend([None] * (int(columna) - 1 - len(fila)))
                dato = next((h for h in celda if _local(h.tag) == "Data"), None)
                fila.append(None if dato is None else ("".join(dato.itertext()) or None))
                fila.extend([None] * int(_atributo(celda, "MergeAcross") or 0))
            filas.append(fila)
            elem.clear()
        elif etiqueta == "Worksheet":
            hojas.append((nombre, filas or []))
            filas = None
            elem.clear()
    return hojas


def filas_html(ruta):
    """Tablas de un .xls que en realidad es HTML."""
    from bs4 import BeautifulSoup
    sopa = BeautifulSoup(decodificar(Path(ruta).read_bytes()), "html.parser")
    hojas = []
    for n, tabla in enumerate(sopa.find_all("table"), 1):
        filas = []
        for tr in tabla.find_all("tr"):
            fila = []
            for celda in tr.find_all(["td", "th"]):
                fila.append(celda.get_text(" ", strip=True) or None)
                try:
                    fila.extend([None] * (int(celda.get("colspan") or 1) - 1))
                except ValueError:
                    pass
            filas.append(fila)
        hojas.append((f"tabla{n}", filas))
    return hojas


def _con_contenido(valor):
    return valor is not None and str(valor).strip() != ""


def nombres_columnas(cabecera, ancho):
    """Nombres de columna: espacios y saltos de línea colapsados; vacíos ->
    'Unnamed: N'; repetidos -> 'nombre.1', 'nombre.2' (como pandas)."""
    nombres, vistos = [], {}
    for j in range(ancho):
        valor = cabecera[j] if j < len(cabecera) else None
        nombre = " ".join(str(valor).split()) if _con_contenido(valor) else f"Unnamed: {j}"
        base, n = nombre, vistos.get(nombre, 0)
        while nombre in vistos:
            n += 1
            nombre = f"{base}.{n}"
        vistos[base] = n
        vistos[nombre] = 0
        nombres.append(nombre)
    return nombres


def tabla_desde_filas(filas):
    """Filas crudas (listas de texto) -> (DataFrame de texto, títulos previos).

    La cabecera es la primera fila (de las 30 primeras con contenido) que tiene
    al menos la mitad de celdas llenas que la más llena: así se saltan los
    títulos ('CONTRATOS MENORES 1er TRIMESTRE 2024'), que se devuelven aparte.
    Se conservan todas las filas de datos (también totales o repetidas); solo
    se descartan las filas totalmente vacías. _fila_origen es el número de fila
    (o registro) en el fichero, contando desde 1.
    """
    con_datos = [i for i, fila in enumerate(filas) if any(_con_contenido(v) for v in fila)]
    if not con_datos:
        return None, None
    muestra = con_datos[:MAX_FILAS_CABECERA]
    llenas = {i: sum(1 for v in filas[i] if _con_contenido(v)) for i in muestra}
    maximo = max(llenas.values())
    umbral = max(min(2, maximo), (maximo + 1) // 2)
    cabecera = next(i for i in muestra if llenas[i] >= umbral)
    titulos = " | ".join(str(v).strip() for i in con_datos if i < cabecera for v in filas[i] if _con_contenido(v))
    datos = [i for i in con_datos if i > cabecera]
    ancho = max([len(filas[cabecera])] + [len(filas[i]) for i in datos])
    while ancho > 0 and not any(len(filas[i]) >= ancho and _con_contenido(filas[i][ancho - 1])
                                for i in [cabecera] + datos):
        ancho -= 1
    registros = [[None if v is None or v == "" else v for v in filas[i][:ancho]]
                 + [None] * (ancho - len(filas[i])) for i in datos]
    df = pd.DataFrame(registros, columns=nombres_columnas(filas[cabecera], ancho), dtype=object)
    df["_fila_origen"] = [str(i + 1) for i in datos]
    return df, (titulos or None)


class NumeroJSON(str):
    """Número JSON con su texto original ('1000.50' sigue siendo '1000.50')."""


def cargar_json(texto):
    return json.loads(texto, parse_float=NumeroJSON, parse_int=NumeroJSON, parse_constant=NumeroJSON)


def json_texto(valor):
    """Serializa a JSON respetando el texto original de los números."""
    if isinstance(valor, NumeroJSON):
        return str(valor)
    if isinstance(valor, dict):
        return "{" + ",".join(f"{json.dumps(str(k), ensure_ascii=False)}:{json_texto(v)}"
                              for k, v in valor.items()) + "}"
    if isinstance(valor, list):
        return "[" + ",".join(json_texto(v) for v in valor) + "]"
    return json.dumps(valor, ensure_ascii=False)


def valor_json(valor):
    """Valor JSON -> texto: números con su texto original, listas/objetos como JSON."""
    if valor is None:
        return None
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (dict, list)):
        return json_texto(valor)
    return str(valor)


def aplanar(objeto, prefijo="", salida=None):
    """Objeto JSON -> {ruta/de/campo: texto}. Las listas se guardan como JSON."""
    salida = {} if salida is None else salida
    for clave, valor in objeto.items():
        nombre = f"{prefijo}/{clave}" if prefijo else str(clave)
        if isinstance(valor, dict) and valor:
            aplanar(valor, nombre, salida)
        else:
            salida[nombre] = valor_json(valor)
    return salida


CLAVES_LISTA_JSON = ("data", "datos", "records", "result", "results", "resultados", "items", "rows")


def registros_json(objeto):
    """Lista de registros de un JSON: la raíz si es lista, o la lista bajo
    'data', 'records', 'result'..."""
    if isinstance(objeto, list):
        return objeto
    if isinstance(objeto, dict):
        for clave in CLAVES_LISTA_JSON:
            valor = objeto.get(clave)
            if isinstance(valor, list):
                return valor
            if isinstance(valor, dict):            # CKAN: {"result": {"records": [...]}}
                for subclave in CLAVES_LISTA_JSON:
                    if isinstance(valor.get(subclave), list):
                        return valor[subclave]
        listas = [v for v in objeto.values() if isinstance(v, list)]
        if len(listas) == 1:
            return listas[0]
        return [objeto]
    return []


def tabla_json(ruta):
    registros = registros_json(cargar_json(decodificar(Path(ruta).read_bytes())))
    if registros and all(isinstance(r, list) for r in registros):
        df, _ = tabla_desde_filas([[valor_json(v) for v in r] for r in registros])
        return df
    filas = [aplanar(r) if isinstance(r, dict) else {"valor": valor_json(r)} for r in registros]
    df = pd.DataFrame(filas, dtype=object)
    df["_fila_origen"] = [str(i) for i in range(1, len(filas) + 1)]
    return df


def leer_tabular(ruta):
    """Fichero crudo -> {'datos': [DataFrame por hoja]}, todo como texto."""
    tipo = tipo_contenido(ruta)
    if tipo == "json":
        df = tabla_json(ruta)
        return {"datos": [df] if df is not None else []}
    if tipo == "csv":
        hojas = filas_csv(ruta)
    elif tipo in ("xls", "xlsx", "ods"):
        hojas = filas_excel(ruta, tipo)
    elif tipo == "spreadsheetml":
        hojas = filas_spreadsheetml(ruta)
    elif tipo == "html":
        hojas = filas_html(ruta)
    else:
        raise ValueError(f"formato '{tipo}' no tabular: el original se conserva en raw/ pero no se convierte")
    frames = []
    for hoja, filas in hojas:
        df, titulos = tabla_desde_filas(filas)
        if df is None:
            continue
        if hoja:
            df["_hoja"] = hoja
        if titulos:
            df["_encabezado"] = titulos
        frames.append(df)
    return {"datos": frames}


# ===========================================================================
# HISTÓRICO -> PARQUET
# ===========================================================================

PATRON_SELLO = re.compile(r"__(\d{8}T\d{6}Z)(?:_\d+)?$")


def fechas_version(ruta, destino, entrada):
    """(fecha de la descarga de esa versión, fecha de su última comprobación)."""
    if ruta == destino:
        descarga = entrada.get("fecha_descarga") or iso_de_epoch(destino.stat().st_mtime)
        return descarga, entrada.get("fecha_comprobacion") or descarga
    m = PATRON_SELLO.search(ruta.stem)
    if m:
        fecha = datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        fecha = iso_de_epoch(ruta.stat().st_mtime)
    return fecha, fecha


def acumular_versiones(destino, entrada, leer, meta, resumen, retirado=False):
    """Une TODAS las versiones guardadas de un fichero crudo, de la más antigua
    a la actual, con acumular() (ámbito: el propio fichero). Devuelve
    {tabla: DataFrame}. Si el portal ya no lista el fichero (retirado=True)
    todas sus filas quedan con _en_ultima_descarga=False."""
    acumulado = {}
    for ruta in versiones(destino):
        fecha_descarga, fecha = fechas_version(ruta, destino, entrada)
        try:
            tablas = leer(ruta)
        except Exception as e:  # noqa: BLE001 - se informa y se sigue con las demás versiones
            resumen.fallo(f"{meta['_archivo_origen']}: no se pudo leer la versión {ruta.name} ({e})")
            continue
        for i, (nombre, frames) in enumerate(tablas.items()):
            frames = [f for f in frames if len(f)]
            if not frames:
                if i == 0:
                    resumen.aviso(f"{meta['_archivo_origen']}: la versión {ruta.name} no tiene filas; "
                                  "no se marca nada como retirado")
                continue
            df = pd.concat(frames, ignore_index=True, sort=False)
            for columna, valor in meta.items():
                df[columna] = valor
            df["_fecha_descarga"] = fecha_descarga
            acumulado[nombre] = acumular(acumulado.get(nombre), df, fecha,
                                         ambito=["_archivo_origen"], ignorar=IGNORAR)
    if retirado:
        for df in acumulado.values():
            df["_en_ultima_descarga"] = False
    return acumulado


def escribir_parquet(frames, ruta, resumen):
    """Une los DataFrames (unión de columnas), pone los metadatos al final y
    escribe el parquet (todo texto salvo _en_ultima_descarga) sin machacar la
    versión anterior (guardar_version)."""
    df = pd.concat(frames, ignore_index=True, sort=False)
    finales = METADATOS + list(COLUMNAS_META)
    orden = [c for c in df.columns if c not in finales] + [c for c in finales if c in df.columns]
    df = df[orden]
    esquema = pa.schema([pa.field(str(c), pa.bool_() if c == "_en_ultima_descarga" else pa.string())
                         for c in df.columns])
    tabla = pa.Table.from_pandas(df, schema=esquema, preserve_index=False)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_name(f".{ruta.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        estado = guardar_version(ruta, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()
    resumen.parquets.append((ruta, len(df), len(df.columns), estado))
    print(f"  parquet {ruta.name}: {len(df):,} filas x {len(df.columns)} columnas ({estado})")
    return df


# ===========================================================================
# GOBIERNO DE ARAGÓN (CKAN)
# ===========================================================================

class ClienteCKAN:
    """package_show probando las bases candidatas; recuerda la que funciona."""

    def __init__(self, candidatas=None):
        self.candidatas = list(candidatas or CKAN_API_CANDIDATAS)
        self.base = None

    def package_show(self, identificadores):
        ultimo = None
        for base in ([self.base] if self.base else self.candidatas):
            for ident in identificadores:
                try:
                    datos = obtener(f"{base}/package_show", params={"id": ident}).json()
                except (ErrorDescarga, ValueError) as e:
                    ultimo = e
                    continue
                if isinstance(datos, dict) and datos.get("success") and isinstance(datos.get("result"), dict):
                    self.base = base
                    return datos["result"]
                ultimo = "respuesta sin 'success'"
        raise ErrorDescarga(f"package_show {identificadores[0]}: {ultimo}")


def nombre_de_url(url):
    return unquote(urlparse(url).path.rstrip("/").rsplit("/", 1)[-1])


def formato_recurso(recurso):
    """csv | xlsx | xls | ods | json | xml, o None si no es tabular."""
    url = recurso.get("url") or ""
    partes = urlparse(url)
    if partes.path.lower().endswith(".xls.xml"):
        return "xml"
    formato = FORMATOS_CKAN.get((recurso.get("format") or "").strip().lower())
    if formato:
        return formato
    consulta = parse_qs(partes.query)
    for clave in ("formato", "format"):
        valor = (consulta.get(clave) or [""])[0].strip().lower()
        if valor in FORMATOS_CKAN:
            return FORMATOS_CKAN[valor]
    nombre = nombre_de_url(url).lower()
    return FORMATOS_CKAN.get(nombre.rsplit(".", 1)[-1]) if "." in nombre else None


PATRON_ANIO = re.compile(r"(?<!\d)(19[89]\d|20\d{2})(?!\d)")
PATRON_DESDE_HASTA = re.compile(r"\b(desde|hasta|a partir de)\s+(el\s+)?(a[nñ]o\s+)?(19|20)\d{2}\b", re.I)
PALABRAS_FORMATO = {"csv", "json", "xml", "xls", "xlsx", "excel", "ods", "spreadsheetml", "tsv"}
PALABRAS_VACIAS = {"de", "del", "en", "el", "la", "los", "las", "y", "ano", "anio", "anno",
                   "ejercicio", "formato", "format", "datos", "fichero", "archivo"}


def anio_de(*textos):
    """Año del recurso ('Contratos menores 2019'); None si no hay uno solo
    ('desde 2023' no cuenta: es el inicio de un registro acumulativo)."""
    for texto in textos:
        anios = set(PATRON_ANIO.findall(PATRON_DESDE_HASTA.sub(" ", texto or "")))
        if len(anios) == 1:
            return int(anios.pop())
        if len(anios) > 1:
            return None
    return None


def serie_de(nombre):
    """Serie del recurso: nombre sin años, formatos ni palabras vacías en los
    extremos ('Contratos menores del año 2019 (CSV)' -> 'contratos_menores')."""
    palabras = re.findall(r"[a-z0-9]+", PATRON_ANIO.sub(" ", sin_acentos(nombre).lower()))
    palabras = [p for p in palabras if p not in PALABRAS_FORMATO]
    while palabras and palabras[-1] in PALABRAS_VACIAS:
        palabras.pop()
    while palabras and palabras[0] in PALABRAS_VACIAS:
        palabras.pop(0)
    return "_".join(palabras)[:80].strip("_") or "recurso"


def id_recurso(recurso):
    rid = str(recurso.get("id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9-]{1,80}", rid):
        rid = hashlib.sha1((recurso.get("url") or "").encode()).hexdigest()[:16]
    return rid


def extension(url, formato):
    return ".xls.xml" if urlparse(url).path.lower().endswith(".xls.xml") else f".{formato}"


def motivo_descarga(destino, entrada, info, anio_actual, acumulativo, comprobar_todo):
    """Por qué hay que (volver a) pedir un recurso, o None si ya se tiene y
    su año está cerrado. Volver a pedirlo nunca machaca: guardar_version."""
    if not destino.exists():
        return "nuevo"
    if comprobar_todo:
        return "comprobación completa"
    if acumulativo:
        return "dataset acumulativo"
    modificado = fecha_a_epoch(info["recurso"].get("last_modified") or info["recurso"].get("metadata_modified"))
    comprobado = fecha_a_epoch(entrada.get("fecha_comprobacion")) or destino.stat().st_mtime
    if modificado and modificado > comprobado:
        return "actualizado en el portal"
    if info["anio"] is None or info["anio"] >= anio_actual - 1:
        return "año en curso o sin año"
    return None


def obtener_recurso(info, ds, salida, manifiesto, resumen, anio_actual, comprobar_todo):
    """Descarga (si hace falta) un recurso. Devuelve (clave, error): clave None
    si no hay ninguna copia utilizable; error si no se pudo refrescar."""
    destino = salida / "raw" / ds["clave"] / f"{info['rid']}{extension(info['url'], info['formato'])}"
    clave = destino.relative_to(salida).as_posix()
    entrada = manifiesto.entradas.get(clave, {})
    meta = {"origen": "ckan", "dataset": ds["id"], "clave_dataset": ds["clave"], "recurso": info["nombre"],
            "recurso_id": info["rid"], "formato": info["formato"], "serie": info["serie"],
            "anio": info["anio"], "url": info["url"]}
    motivo = motivo_descarga(destino, entrada, info, anio_actual, ds.get("acumulativo", False), comprobar_todo)
    if motivo is None:
        manifiesto.entradas[clave] = {**entrada, **meta}
        resumen.sin_cambios.append(clave)
        return clave, None
    print(f"  {info['nombre']} [{info['formato']}] ({motivo})")
    try:
        estado, tam = descargar(info["url"], destino, info["formato"])
    except ErrorDescarga as e:
        error = f"{ds['id']} / {info['nombre']} [{info['formato']}]: {e}"
        if destino.exists():
            manifiesto.entradas[clave] = {**entrada, **meta}
            return clave, error + " (se conserva la copia anterior)"
        return None, error
    manifiesto.registrar(clave, destino, estado, meta)
    resumen.descargado(clave, estado, tam, motivo)
    return clave, None


def procesar_dataset_ckan(cliente, ds, salida, manifiesto, resumen, anio_actual, comprobar_todo=False):
    print(f"\n[CKAN] {ds['id']}")
    try:
        paquete = cliente.package_show([ds["id"], *ds.get("ids_alternativos", [])])
    except ErrorDescarga as e:
        resumen.fallo(f"{ds['id']}: {e}; se conservan las copias anteriores")
        return
    grupos, listados = {}, set()
    for recurso in paquete.get("resources") or []:
        url = (recurso.get("url") or "").strip()
        if not url:
            continue
        rid = id_recurso(recurso)
        listados.add(rid)
        formato = formato_recurso(recurso)
        if formato is None:
            resumen.aviso(f"{ds['id']}: recurso no tabular omitido: {recurso.get('name') or url} "
                          f"({recurso.get('format') or 'sin formato'})")
            continue
        nombre = (recurso.get("name") or "").strip() or nombre_de_url(url)
        info = {"recurso": recurso, "rid": rid, "url": url, "formato": formato, "nombre": nombre,
                "serie": serie_de(nombre), "anio": anio_de(nombre, nombre_de_url(url))}
        grupos.setdefault((info["serie"], info["anio"]), {}).setdefault(formato, []).append(info)
    print(f"  {len(listados)} recursos, {len(grupos)} grupos serie/año")

    elegidos = set()
    for (serie, anio), por_formato in sorted(grupos.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0)):
        pendientes = []
        for formato in sorted(por_formato, key=PREFERENCIA_FORMATOS.index):
            claves, errores = [], []
            for info in por_formato[formato]:
                clave, error = obtener_recurso(info, ds, salida, manifiesto, resumen, anio_actual, comprobar_todo)
                claves.append(clave)
                if error:
                    errores.append(error)
            if all(claves):
                elegidos.update(claves)
                for error in errores:
                    resumen.fallo(error)
                for error in pendientes:
                    resumen.aviso(f"{error} -> se usa el formato {formato.upper()}")
                break
            pendientes.extend(errores)
        else:
            for error in pendientes:
                resumen.fallo(error)

    # Estado de cada fichero crudo del dataset: 'publicado' (el que se usa),
    # 'alternativo' (otro formato de un grupo que se sirve en un formato
    # preferido) o 'retirado' (el portal ya no lista el recurso, o lo sirve
    # ahora en otro formato): sus filas siguen en el parquet con
    # _en_ultima_descarga=False.
    ids_elegidos = {manifiesto.entradas[c]["recurso_id"] for c in elegidos}
    for clave, entrada in manifiesto.entradas.items():
        if entrada.get("origen") != "ckan" or entrada.get("dataset") != ds["id"]:
            continue
        rid = entrada.get("recurso_id")
        if clave in elegidos:
            entrada["estado"] = "publicado"
        elif rid in listados and rid not in ids_elegidos:
            entrada["estado"] = "alternativo"
        else:
            if entrada.get("estado") != "retirado":
                resumen.aviso(f"{clave}: el portal ya no sirve este fichero; se conservan sus filas "
                              "con _en_ultima_descarga=False")
            entrada["estado"] = "retirado"


def generar_parquets_ckan(salida, manifiesto, resumen):
    grupos = {}
    for clave, entrada in sorted(manifiesto.entradas.items()):
        if entrada.get("origen") == "ckan" and entrada.get("estado", "publicado") in ("publicado", "retirado"):
            grupos.setdefault((entrada["clave_dataset"], entrada["serie"]), []).append((clave, entrada))
    for (clave_dataset, serie), entradas in sorted(grupos.items()):
        frames = []
        for clave, entrada in sorted(entradas, key=lambda x: (x[1].get("anio") or 0, x[0])):
            meta = {"_fuente": PAGINA_DATASET.format(id=entrada["dataset"]), "_dataset": entrada["dataset"],
                    "_recurso": entrada.get("recurso"), "_recurso_id": entrada.get("recurso_id"),
                    "_formato": entrada.get("formato"),
                    "_anio_recurso": str(entrada["anio"]) if entrada.get("anio") else None,
                    "_archivo_origen": clave, "_url_origen": entrada.get("url")}
            tablas = acumular_versiones(salida / clave, entrada, leer_tabular, meta, resumen,
                                        retirado=entrada.get("estado") == "retirado")
            frames.extend(tablas.values())
        if frames:
            escribir_parquet(frames, salida / f"{clave_dataset}__{serie}.parquet", resumen)


# ===========================================================================
# AYUNTAMIENTO DE ZARAGOZA
# ===========================================================================

def descubrir_ocds_zaragoza():
    """{nombre: url} de los JSONL.gz que enlaza la página de la publicación:
    los anuales si los hay (juntos son el total) y si no el completo."""
    respuesta = obtener(ZARAGOZA_OCDS_PUBLICACION)
    base = getattr(respuesta, "url", None) or ZARAGOZA_OCDS_PUBLICACION
    enlaces = {}
    for href, nombre in PATRON_ENLACE_OCDS.findall(respuesta.text):
        enlaces.setdefault(unquote(html.unescape(nombre)), urljoin(base, html.unescape(href)))
    anuales = {n: u for n, u in enlaces.items() if re.fullmatch(r"\d{4}\.jsonl\.gz", n)}
    return dict(sorted(anuales.items())) if anuales else {n: u for n, u in enlaces.items() if n == "full.jsonl.gz"}


def procesar_zaragoza_ocds(salida, manifiesto, resumen, anio_actual, comprobar_todo=False):
    print(f"\n[ZARAGOZA OCDS] {ZARAGOZA_OCDS_PUBLICACION}")
    try:
        enlaces = descubrir_ocds_zaragoza()
    except ErrorDescarga as e:
        resumen.fallo(f"Zaragoza OCDS: no se pudo leer la página de la publicación ({e}); "
                      "se conservan las copias anteriores")
        return
    if not enlaces:
        resumen.fallo("Zaragoza OCDS: la página no enlaza ningún .jsonl.gz (¿ha cambiado el registro?)")
        return
    listados = set()
    for nombre, url in enlaces.items():
        destino = salida / "raw" / "zaragoza_ocds" / nombre
        clave = destino.relative_to(salida).as_posix()
        listados.add(clave)
        m = re.match(r"(\d{4})\.", nombre)
        anio = int(m.group(1)) if m else None
        meta = {"origen": "zaragoza_ocds", "url": url, "anio": anio, "estado": "publicado"}
        entrada = manifiesto.entradas.get(clave, {})
        if destino.exists() and not comprobar_todo and anio is not None and anio < anio_actual - 1:
            manifiesto.entradas[clave] = {**entrada, **meta}
            resumen.sin_cambios.append(clave)
            continue
        motivo = "nuevo" if not destino.exists() else "año en curso o fichero completo"
        try:
            estado, tam = descargar(url, destino, "gz")
        except ErrorDescarga as e:
            resumen.fallo(f"Zaragoza OCDS {nombre}: {e}"
                          + (" (se conserva la copia anterior)" if destino.exists() else ""))
            if destino.exists():
                manifiesto.entradas[clave] = {**entrada, **meta}
            continue
        manifiesto.registrar(clave, destino, estado, meta)
        resumen.descargado(clave, estado, tam, motivo)
    for clave, entrada in manifiesto.entradas.items():
        if entrada.get("origen") == "zaragoza_ocds" and clave not in listados:
            if entrada.get("estado") != "retirado":
                resumen.aviso(f"{clave}: el registro OCP ya no lo enlaza; se conservan sus filas")
            entrada["estado"] = "retirado"


def releases_ocds(objeto):
    """Releases de una línea: release suelto, paquete de releases o de records
    (de estos, el compiledRelease)."""
    if not isinstance(objeto, dict):
        return []
    if isinstance(objeto.get("releases"), list):
        return [r for r in objeto["releases"] if isinstance(r, dict)]
    if isinstance(objeto.get("records"), list):
        salida = []
        for registro in objeto["records"]:
            if isinstance(registro.get("compiledRelease"), dict):
                salida.append(registro["compiledRelease"])
            else:
                salida.extend(r for r in registro.get("releases") or [] if isinstance(r, dict) and "ocid" in r)
        return salida
    return [objeto] if "ocid" in objeto else []


def leer_ocds(ruta):
    """JSONL(.gz) OCDS -> {'releases', 'awards', 'contracts', 'parties'}: una
    fila por release / elemento, campos anidados como 'tender/value/amount' y
    listas como JSON (números con su texto original)."""
    filas = {"releases": [], "awards": [], "contracts": [], "parties": []}
    abrir = gzip.open if tipo_contenido(ruta) == "gz" else open
    with abrir(ruta, "rt", encoding="utf-8") as f:
        for n, linea in enumerate(f, 1):
            if not linea.strip():
                continue
            try:
                objeto = cargar_json(linea)
            except ValueError as e:
                raise ValueError(f"línea {n} no es JSON válido: {e}") from e
            for release in releases_ocds(objeto):
                base = {"ocid": str(release.get("ocid")) if release.get("ocid") is not None else None,
                        "release_id": str(release.get("id")) if release.get("id") is not None else None}
                fila = aplanar({k: v for k, v in release.items() if k not in TABLAS_OCDS})
                fila["_linea"] = str(n)
                filas["releases"].append(fila)
                for tabla in TABLAS_OCDS:
                    for elemento in release.get(tabla) or []:
                        if isinstance(elemento, dict):
                            filas[tabla].append({**base, **aplanar(elemento), "_linea": str(n)})
    return {tabla: [pd.DataFrame(lista, dtype=object)] if lista else [] for tabla, lista in filas.items()}


def generar_parquets_zaragoza_ocds(salida, manifiesto, resumen):
    por_tabla = {}
    for clave, entrada in sorted(manifiesto.entradas.items()):
        if entrada.get("origen") != "zaragoza_ocds" or entrada.get("estado") not in ("publicado", "retirado"):
            continue
        meta = {"_fuente": ZARAGOZA_OCDS_PUBLICACION, "_archivo_origen": clave, "_url_origen": entrada.get("url")}
        tablas = acumular_versiones(salida / clave, entrada, leer_ocds, meta, resumen,
                                    retirado=entrada.get("estado") == "retirado")
        for tabla, df in tablas.items():
            por_tabla.setdefault(tabla, []).append(df)
    for tabla in ("releases", *TABLAS_OCDS):
        if por_tabla.get(tabla):
            escribir_parquet(por_tabla[tabla], salida / f"zaragoza_ocds_{tabla}.parquet", resumen)


def procesar_zaragoza_api(salida, manifiesto, resumen):
    """Listado completo de la API REST: solo se guarda si se han bajado todas
    las páginas (un JSON con la lista de respuestas tal cual)."""
    print(f"\n[ZARAGOZA API] {ZARAGOZA_API_CONTRATOS}")
    destino = salida / "raw" / "zaragoza_api" / "contrato.json"
    clave = destino.relative_to(salida).as_posix()
    paginas, inicio = [], 0
    try:
        for _ in range(MAX_PAGINAS_API):
            respuesta = obtener(ZARAGOZA_API_CONTRATOS, params={"rows": FILAS_POR_PAGINA_API, "start": inicio})
            pagina = respuesta.json()
            resultado = (pagina.get("result") or []) if isinstance(pagina, dict) else []
            paginas.append(pagina)
            inicio += len(resultado)
            total = int(pagina.get("totalCount") or 0) if isinstance(pagina, dict) else 0
            if not resultado or inicio >= total:
                break
        else:
            raise ErrorDescarga(f"más de {MAX_PAGINAS_API} páginas")
    except (ErrorDescarga, ValueError) as e:
        resumen.fallo(f"Zaragoza API: {e}; no se guarda un listado incompleto")
        return
    contenido = json.dumps(paginas, ensure_ascii=False).encode("utf-8")
    estado = guardar_version(destino, contenido)
    manifiesto.registrar(clave, destino, estado, {"origen": "zaragoza_api", "url": ZARAGOZA_API_CONTRATOS,
                                                  "estado": "publicado"})
    resumen.descargado(clave, estado, len(contenido), f"{inicio} contratos en {len(paginas)} páginas")


def leer_api_zaragoza(ruta):
    paginas = cargar_json(decodificar(Path(ruta).read_bytes()))
    filas = []
    for pagina in paginas if isinstance(paginas, list) else [paginas]:
        for elemento in (pagina.get("result") or []) if isinstance(pagina, dict) else []:
            if isinstance(elemento, dict):
                filas.append(aplanar(elemento))
    df = pd.DataFrame(filas, dtype=object)
    df["_fila_origen"] = [str(i) for i in range(1, len(filas) + 1)]
    return {"datos": [df]}


def generar_parquet_zaragoza_api(salida, manifiesto, resumen):
    frames = []
    for clave, entrada in sorted(manifiesto.entradas.items()):
        if entrada.get("origen") == "zaragoza_api" and (salida / clave).exists():
            meta = {"_fuente": ZARAGOZA_API_CONTRATOS, "_archivo_origen": clave, "_url_origen": entrada.get("url")}
            frames.extend(acumular_versiones(salida / clave, entrada, leer_api_zaragoza, meta, resumen).values())
    if frames:
        escribir_parquet(frames, salida / "zaragoza_api_contratos.parquet", resumen)


# ===========================================================================
# MAIN
# ===========================================================================

def main(argv=None):
    parser = argparse.ArgumentParser(description="Contratación pública de Aragón y del Ayuntamiento de Zaragoza")
    parser.add_argument("--salida", type=Path, default=None, help=f"carpeta de salida (por defecto {DIR_SALIDA})")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar: regenerar los parquet desde raw/")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años cerrados para detectar cambios o retiradas")
    parser.add_argument("--sin-zaragoza", action="store_true", help="no descargar el OCDS de Zaragoza")
    parser.add_argument("--zaragoza-api", action="store_true", help="descargar también la API REST de Zaragoza")
    args = parser.parse_args(argv)

    salida = Path(args.salida) if args.salida else DIR_SALIDA
    (salida / "raw").mkdir(parents=True, exist_ok=True)
    manifiesto = Manifiesto(salida / "raw" / "_manifiesto.json")
    resumen = Resumen()
    anio_actual = anio_en_curso()
    inicio = datetime.now()
    print("=" * 70)
    print("CONTRATACIÓN PÚBLICA - ARAGÓN")
    print("=" * 70)
    print(f"Salida: {salida}")

    if not args.solo_parquet:
        cliente = ClienteCKAN()
        for ds in DATASETS:
            procesar_dataset_ckan(cliente, ds, salida, manifiesto, resumen, anio_actual, args.comprobar_todo)
            manifiesto.guardar()
        if not args.sin_zaragoza:
            procesar_zaragoza_ocds(salida, manifiesto, resumen, anio_actual, args.comprobar_todo)
            manifiesto.guardar()
        if args.zaragoza_api:
            procesar_zaragoza_api(salida, manifiesto, resumen)
            manifiesto.guardar()

    print("\n[PARQUET]")
    generar_parquets_ckan(salida, manifiesto, resumen)
    generar_parquets_zaragoza_ocds(salida, manifiesto, resumen)
    generar_parquet_zaragoza_api(salida, manifiesto, resumen)

    resumen.imprimir(f"ARAGÓN ({datetime.now() - inicio})")
    return 1 if resumen.fallos else 0


if __name__ == "__main__":
    sys.exit(main())
