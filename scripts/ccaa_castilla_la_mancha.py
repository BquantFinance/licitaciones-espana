#!/usr/bin/env python3
"""
=============================================================================
CASTILLA-LA MANCHA - CONTRATOS MENORES (Junta, SESCAM, sector público y UCLM)
=============================================================================
Descarga los contratos menores de Castilla-La Mancha que publica la propia
comunidad fuera de la Plataforma de Contratación del Sector Público, tal como
los publica (con todas sus versiones), y genera un Parquet por conjunto de
datos con todas las filas y columnas de todos los años, como texto.

Ejecutar:  python scripts/ccaa_castilla_la_mancha.py [--salida DIR] [--desde AÑO] [--hasta AÑO]
           [--fuente jccm|uclm] [--solo-descarga] [--solo-parquet] [--comprobar-todo]

Conjuntos de datos (un Parquet cada uno; columna _unidad = qué es cada fila):
    menores_junta    Junta (gestor de expedientes PICOS): XLS/XLSX/ZIP trimestrales y
                     anuales desde 2019. Una fila por contrato, con CIF, importe y fecha de
                     publicación en PLACE; los nombres de las columnas cambian de un año a
                     otro ('CIF Adjudicatario(s)', 'C.I.F. Adjudicatario(s)'...) y se dejan
                     como vienen. El fichero anual ("año COMPLETO") se solapa con los
                     trimestrales pero no siempre es su suma (2023: 50.069 filas frente a
                     36.451): se guardan todos y _periodo (1T..4T / anual) los distingue.
                     Hasta 2023 incluyen la UCLM.
    caja_pagadora    Junta, menores pagados por caja pagadora: XLSX trimestral desde 2026.
                     Una fila por factura, con NIF.
    sector_publico   "Sector público regional" 2015-2018: ZIP trimestrales (o semestrales)
                     con un XLS/XLSX por consejería u organismo, cada uno con su esquema
                     (unas 440 columnas en total), y los del SESCAM (RAR, ZIP o XLSX, uno
                     también en XML de Excel 2003): esos son líneas de factura, ~100.000-
                     125.000 por trimestre (_miembro dice de qué fichero sale cada fila).
                     Algún fichero va repetido dentro del ZIP: se conserva tal cual.
    sescam           Servicio de Salud (SESCAM), 2019-: RAR/ZIP trimestrales con un XLSX por
                     tipo de compra (farmacia, suministros). OJO: CADA FILA ES UNA LÍNEA DE
                     FACTURA (gerencia, artículo, proveedor y nº de factura), NO UN CONTRATO,
                     y no trae NIF. Del orden de 150.000 filas por trimestre.
    informe_menores  "Informe de Contratación Administrativa del Sector Público" (datos
                     estadísticos, 2019-2022): relación de menores de todo el sector público
                     regional (sin adjudicatario) y hojas de totales.
    uclm             Universidad de Castilla-La Mancha: tabla HTML de menores por ejercicio
                     (2017-) con NIF, proveedor, expediente, objeto, duración e importe; la
                     del año en curso trae además la unidad funcional.
    otros            Enlaces de menores que no casan con ninguna serie: se descargan y se
                     leen igual (revisar la clasificación, sale en los avisos).

Salida (por defecto <repo>/ccaa_castilla_la_mancha/):
    raw/<dataset>/<año>/<fichero original>   p.ej. raw/sescam/2023/CM_PRIMER_TRIMESTRE_2023.rar
    raw/uclm/<año>/contratosMenores{Anteriores,Actuales}_<año>.html
    raw/.../_historico/                versiones anteriores de cada fichero (nunca se borran)
    raw/inventario_jccm.json           enlaces que publica el portal de la Junta
    raw/inventario_uclm.json           ejercicios que ofrece la UCLM
    raw/_manifiesto.json               URL, título, año, periodo, fecha de descarga y última
                                       comprobación de cada fichero y si se sigue publicando
    raw/descarga_log.txt               resumen de cada ejecución (se añade al final)
    <dataset>.parquet                  todos los años (todas las columnas, texto)
    _historico/                        versiones anteriores de los Parquet

Columnas añadidas: _fuente (URL), _dataset, _titulo (texto del enlace o de la
página), _anio (año del portal), _trimestre, _periodo, _unidad, _archivo_origen
(ruta en raw/), _miembro (fichero dentro del ZIP/RAR), _hoja (solo Excel),
_fecha_descarga y, de comun/historico.py, _primera_descarga, _ultima_descarga y
_en_ultima_descarga. Un registro que el portal retira o modifica NO desaparece:
sigue en el Parquet con _en_ultima_descarga=False (sesgo del superviviente).

Qué se descarga:
- Junta: para cada año del filtro del portal, la página de ficheros de
  transparencia del concepto "Contratación menor" (todos sus enlaces) y la de
  "Datos estadísticos" (solo los enlaces que dicen "menores"); también
  cualquier concepto nuevo con "menor" en el nombre. Cada enlace se clasifica
  por su texto (y si no, por el nombre del fichero) y se guarda con su nombre
  publicado. Los ZIP y RAR se guardan tal cual y se descomprimen (en un
  temporal, también los anidados) para leer sus hojas.
- UCLM: la página de menores anteriores es un formulario ASP.NET: un GET da
  los campos ocultos (__VIEWSTATE, __EVENTVALIDATION...) y los ejercicios del
  desplegable, y un POST por ejercicio (botón "Buscar") devuelve la tabla
  entera, sin paginar. La de menores actuales es un GET (el año en curso: el
  siguiente al último de los anteriores). De cada respuesta se guarda la tabla
  de resultados tal cual la sirve, en un HTML mínimo: el resto de la página
  cambia en cada petición sin que cambien los datos (fecha del día en la
  cabecera, __VIEWSTATE y __EVENTVALIDATION cifrados, de 5 a 9 MB) y guardarlo
  crearía una versión nueva en cada ejecución.
- Se completa lo que falta y se vuelven a pedir los ficheros del año en curso
  y del anterior; los más antiguos solo con --comprobar-todo (o si cambia su
  URL). Todo pasa por comun.historico.guardar_version: si no cambia no se toca
  y si cambia la copia anterior queda en _historico/. Lo que el portal deja de
  enlazar queda como retirado (sus filas se conservan), pero solo si su página
  se ha podido leer y enlaza algún fichero: si el portal falla no se retira
  nada.
- Los RAR se leen con rarfile si tiene un programa (unrar, unar, 7z o bsdtar)
  y si no (o si falla) con libarchive-c (pip install libarchive-c; usa la
  biblioteca libarchive del sistema). libarchive 3.7 da un error de CRC con al
  menos un RAR válido (el del SESCAM del 2º trimestre de 2016, dentro del ZIP
  de sector público; unrar lo lee bien). Un RAR que no se puede leer se
  conserva igual y queda como PENDIENTE en el resumen (código de salida 1); sus
  filas entran en cuanto se pueda leer.
- Dentro de los ZIP/RAR solo se leen como tablas los ficheros cuyo contenido
  casa con su extensión (XLS, XLSX, CSV, HTML, XML de Excel 2003); el resto
  (PDF, páginas web guardadas en MHTML...) se anota en los avisos y se queda
  en el original. Una hoja .xls con 65.536 filas (el máximo del formato) se
  avisa: puede estar truncada en origen (le pasa al SESCAM de 2015).

FUENTES
-------
Verificado en vivo el 2026-09-27 (confianza A):
  - https://contratacion.castillalamancha.es/ficheros-transparencia?concepto=113&year={AÑO}
    (Drupal; filtro de años 2015-2026, sin paginar): 82 ficheros de menores:
    14 ZIP de sector público 2015-2018, 36 de la Junta 2019-2026 (XLS, XLSX y
    2 ZIP; 6 anuales), 2 de caja pagadora 2026 y 30 del SESCAM 2019-2026 (25
    RAR, uno RAR5, y 5 ZIP). Concepto 117 ("Datos estadísticos"): 4 ficheros
    de menores 2019-2022. Los demás conceptos (formalizados, modificados, obras,
    desviaciones) no traen menores.
  - https://contratos.apps.uclm.es/contratosMenoresAnteriores.aspx: ejercicios
    2017-2025 (30.716, 24.588, 21.299, 19.924, 26.691, 26.783, 28.747, 29.289
    y 29.036 filas), cada uno en una sola tabla sin paginar (hasta 18 MB por
    respuesta); el POST no necesita la cookie de sesión y los mismos campos
    ocultos sirven para todos los ejercicios.
  - https://contratos.apps.uclm.es/contratosMenoresActuales.aspx: año en curso
    (2026: 16.498 filas), con la columna Un.Funcional.
VERIFICAR EN VIVO:
  - Si el portal de la Junta publica menores de años nuevos con otros textos
    (van a "otros" con un aviso) o cambia la estructura de la página (sin
    enlaces: error y no se retira nada).
  - Si la UCLM pagina la tabla algún año (se detecta y se da como error).
=============================================================================
"""

import argparse
import codecs
import datetime as dt
import html
import importlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
import time
import unicodedata
import warnings
import zipfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, versiones  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

ANIO_MINIMO_JCCM = 2015   # primer año del filtro del portal (si no se puede leer)

# Portal de contratación de la Junta: "Ficheros de datos de transparencia"
URL_JCCM = "https://contratacion.castillalamancha.es"
URL_FICHEROS = URL_JCCM + "/ficheros-transparencia"
RUTA_FICHEROS = "/sites/default/files/"
CONCEPTO_MENORES = "113"       # "Contratación menor: JCCM y SESCAM": todos sus enlaces
CONCEPTO_ESTADISTICA = "117"   # "Datos estadísticos": solo los enlaces de menores
CONCEPTOS_FIJOS = {CONCEPTO_MENORES: "Contratación menor: JCCM y SESCAM",
                   CONCEPTO_ESTADISTICA: "Datos estadísticos"}

# Perfil de contratante de la UCLM (ASP.NET WebForms)
URL_UCLM = "https://contratos.apps.uclm.es/"
URL_UCLM_ANTERIORES = URL_UCLM + "contratosMenoresAnteriores.aspx"
URL_UCLM_ACTUALES = URL_UCLM + "contratosMenoresActuales.aspx"
CAMPO_EJERCICIO_UCLM = "ctl00$cph_Contenidos$ddl_contrato_anterior"
BOTON_BUSCAR_UCLM = "ctl00$cph_Contenidos$lbtn_buscar"
TABLA_UCLM = "cph_Contenidos_gv_expediente"

# Conjuntos de datos: descripción y qué es cada fila (columna _unidad)
DATASETS = {
    "menores_junta": {"descripcion": "Contratos menores de la Junta (gestor PICOS)",
                      "unidad": "contrato menor"},
    "caja_pagadora": {"descripcion": "Contratos menores de la Junta pagados por caja pagadora",
                      "unidad": "contrato menor (una factura de caja pagadora)"},
    "sector_publico": {"descripcion": "Contratos menores del sector público regional (2015-2018)",
                       "unidad": "contrato menor; en los ficheros del SESCAM que trae, línea de factura"},
    "sescam": {"descripcion": "Contratos menores del SESCAM (líneas de factura)",
               "unidad": "línea de factura por artículo y gerencia (no es un contrato)"},
    "informe_menores": {"descripcion": "Informe de Contratación Administrativa: fichero de menores",
                        "unidad": "contrato menor (y hojas de totales)"},
    "uclm": {"descripcion": "Contratos menores de la Universidad de Castilla-La Mancha",
             "unidad": "contrato menor (expediente)"},
    "otros": {"descripcion": "Enlaces de menores sin clasificar (revisar)",
              "unidad": "sin clasificar"},
}

# Clasificación de un enlace por su texto (sin acentos y en minúsculas), en orden
CLASIFICACION = [
    ("caja_pagadora", r"\bcaja\s+pagadora\b"),
    ("sescam", r"\bsescam\b"),
    ("informe_menores", r"\binforme\b.*\bmenores\b"),
    ("sector_publico", r"\bsector\s+publico\b"),
    ("menores_junta", r"\bjccm\b"),
]

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_castilla_la_mancha"
TITULO = "CASTILLA-LA MANCHA - CONTRATOS MENORES"


# ============================================================================
# UTILIDADES COMUNES (bloque de ccaa_murcia.py adaptado: POST, ZIP/RAR anidados,
# tablas HTML y Parquet por partes para ficheros de cientos de miles de filas)
# ============================================================================

CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 120
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
PAUSA = 0.3                  # entre peticiones, para no cargar el portal
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL, otra hoja u otro miembro sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_dataset", "_titulo", "_anio", "_trimestre", "_periodo", "_unidad",
                    "_archivo_origen", "_miembro", "_hoja", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


def iso(momento):
    """Fecha ISO 8601 en UTC al segundo: '2026-09-27T14:43:00Z' (ordenable como texto)."""
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fecha_version(ruta):
    """Fecha de una versión de un fichero crudo: el sello que guardar_version pone
    en _historico/ (fecha de esa copia) o, para la copia actual, su fecha de
    modificación. Al pasar a _historico/ la copia conserva la misma fecha."""
    ruta = Path(ruta)
    if ruta.parent.name == HISTORICO:
        sellos = re.findall(r"__(\d{8}T\d{6}Z)", ruta.name)
        if sellos:
            return iso(datetime.strptime(sellos[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc))
    return iso(datetime.fromtimestamp(ruta.stat().st_mtime, timezone.utc))


def sin_acentos(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto)) if not unicodedata.combining(c))


class ErrorPortal(Exception):
    def __init__(self, mensaje, codigo=None):
        super().__init__(mensaje)
        self.codigo = codigo


class LectorNoDisponible(RuntimeError):
    """Falta la biblioteca o el programa para abrir un fichero (p.ej. un RAR): el
    original se conserva y queda como pendiente."""


def _espera(intento, respuesta=None):
    """Backoff exponencial, o lo que pida el servidor en Retry-After."""
    valor = (getattr(respuesta, "headers", None) or {}).get("Retry-After") if respuesta is not None else None
    if valor and str(valor).strip().isdigit():
        return min(float(valor), ESPERA_MAXIMA)
    return min(ESPERA_BASE * 2 ** (intento - 1), ESPERA_MAXIMA)


def _pedir(metodo, url, **kwargs):
    """Petición con reintentos (red, 429, 5xx); un 4xx lanza ErrorPortal con el código."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = metodo(url, headers=CABECERAS, timeout=TIMEOUT_API, **kwargs)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def pedir_texto(url, params=None):
    """GET de una página HTML con reintentos."""
    return _pedir(lambda u, **kw: requests.get(u, params=params, **kw), url).text


def pedir_post(url, datos):
    """POST de un formulario (postback de ASP.NET) con reintentos."""
    return _pedir(lambda u, **kw: requests.post(u, data=datos, **kw), url).text


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes ('zip' incluye XLSX)."""
    if cabeza.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    if cabeza.startswith(b"Rar!\x1a\x07"):
        return "rar"
    if cabeza.startswith(b"7z\xbc\xaf\x27\x1c"):
        return "7z"
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith(b"%PDF"):
        return "pdf"
    if cabeza.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "csv"                                      # texto UTF-16
    texto = cabeza.lstrip(b"\xef\xbb\xbf").lstrip()
    inicio = texto[:4096].lower()
    if texto[:1] in (b"{", b"["):
        return "json"
    if texto[:1] == b"<":
        if b"urn:schemas-microsoft-com:office:spreadsheet" in inicio or b'progid="excel.sheet"' in inicio:
            return "xmlss"                                # hoja XML de Excel 2003 (SpreadsheetML)
        return "html" if b"html" in inicio[:1024] or b"<table" in inicio[:1024] else "xml"
    if b"multipart/related" in inicio:
        return "mhtml"                                    # página web guardada (MHTML)
    return "csv"


def formato_fichero(ruta):
    """Formato real de un fichero: el de sus primeros bytes y, si es un ZIP, si
    es una hoja de cálculo (XLSX, ODS) o un archivo comprimido."""
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato != "zip":
        return formato
    try:
        with zipfile.ZipFile(ruta) as archivo:
            nombres = archivo.namelist()
            if any(n.startswith("xl/") for n in nombres):
                return "xlsx"
            if "mimetype" in nombres and b"opendocument" in archivo.read("mimetype"):
                return "ods"
    except (zipfile.BadZipFile, OSError, KeyError):
        pass
    return "zip"


FORMATOS_ARCHIVO = ("zip", "rar", "7z")
MENSAJE_RAR = ("hace falta libarchive-c (pip install libarchive-c; usa libarchive del sistema) o rarfile "
               "(pip install rarfile) con unrar, unar, 7z o bsdtar")


def _modulo(nombre):
    """Módulo opcional (libarchive, rarfile) o None si no está o no carga (p.ej.
    libarchive-c sin la biblioteca del sistema)."""
    try:
        return importlib.import_module(nombre)
    except Exception:
        return None


def _abrir_libarchive(libarchive, ruta):
    """Lector de libarchive con el formato fijado por la firma del fichero: con
    la detección automática, un RAR que guarda un XLSX sin comprimir se leería
    como el ZIP de dentro."""
    with open(ruta, "rb") as f:
        cabeza = f.read(8)
    formato = ("rar5" if cabeza.startswith(b"Rar!\x1a\x07\x01\x00") else "rar" if cabeza.startswith(b"Rar!\x1a\x07")
               else "7zip" if cabeza.startswith(b"7z\xbc\xaf\x27\x1c") else "all")
    return libarchive.file_reader(str(ruta), format_name=formato)


def _integridad(ruta, formato):
    """Motivo por el que un ZIP (o XLSX) descargado está incompleto o dañado, o
    None. Los RAR no se comprueban así: un RAR válido que el descompresor de aquí
    no sepa abrir (libarchive falla con alguno) tiene que guardarse igual; los
    cortados los delata el Content-Length."""
    if formato not in ("zip", "xlsx", "ods"):
        return None
    try:
        with zipfile.ZipFile(ruta) as archivo:
            malo = archivo.testzip()
        return f"miembro dañado: {malo}" if malo else None
    except Exception as e:
        return f"{type(e).__name__}: {str(e)[:150]}"


def validar_contenido(ruta, tipo):
    """(motivo, reintentar): por qué la descarga no es el fichero esperado (una
    página HTML servida con 200, un archivo cortado...), o (None, False) si vale.
    Un fichero de datos con otra extensión se acepta: se lee por su contenido."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía", False
    formato = formato_fichero(ruta)
    if formato in ("html", "xml") and tipo not in ("html", "xml"):
        return f"la respuesta es {formato.upper()}, no un fichero de datos", False
    danado = _integridad(ruta, formato)
    if danado:
        return f"fichero {formato.upper()} incompleto o dañado ({danado})", True
    return None, False


def _tipo(url):
    extension = Path(unquote(urlparse(url).path)).suffix.lower().lstrip(".")
    return extension or None


def descargar(url, destino, params=None, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que es un fichero de datos completo (no una
    página HTML de error, ni un ZIP/RAR cortado) y lo entrega a guardar_version():
    si el contenido no cambió no se toca nada y si cambió la copia previa pasa a
    _historico/. Reintenta con backoff los fallos de red, 429, 5xx y las
    descargas incompletas.
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es un fichero de datos) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    detalle = ""
    try:
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                with requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_DESCARGA,
                                  stream=True) as respuesta:
                    codigo = respuesta.status_code
                    if codigo in (404, 410):
                        return "no_existe", f"HTTP {codigo}"
                    if codigo not in CODIGOS_REINTENTABLES:
                        if codigo >= 400:
                            return "error", f"HTTP {codigo}"
                        escritos = 0
                        with open(tmp, "wb") as f:
                            for trozo in respuesta.iter_content(chunk_size=1 << 16):
                                if trozo:
                                    f.write(trozo)
                                    escritos += len(trozo)
                        cabeceras = getattr(respuesta, "headers", None) or {}
                        esperado = str(cabeceras.get("Content-Length") or "")
                        if esperado.isdigit() and not cabeceras.get("Content-Encoding") and int(esperado) != escritos:
                            detalle = f"descarga incompleta ({escritos:,} de {int(esperado):,} bytes)"
                        else:
                            motivo, reintentar = validar_contenido(tmp, tipo)
                            if not motivo:
                                return guardar_version(destino, desde=tmp), ""
                            if not reintentar:
                                return "invalido", motivo
                            detalle = motivo
                    else:
                        detalle = f"HTTP {codigo}"
            except ERRORES_RED as e:
                detalle = f"{type(e).__name__}: {str(e)[:150]}"
            except requests.exceptions.RequestException as e:
                return "error", f"{type(e).__name__}: {str(e)[:150]}"
            if intento < INTENTOS:
                time.sleep(_espera(intento, respuesta))
        return "error", f"{detalle} (tras {INTENTOS} intentos)"
    finally:
        if tmp.exists():
            tmp.unlink()


def guardar_json(destino, datos):
    """Guarda metadatos del portal (inventarios) conservando sus versiones."""
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    return guardar_version(destino, contenido)


class Manifiesto:
    """raw/_manifiesto.json: de dónde sale cada fichero crudo, cuándo se descargó
    su versión actual (fecha_descarga), cuándo se comprobó por última vez
    (comprobado) y si el portal lo sigue publicando."""

    def __init__(self, raw):
        self.raw = Path(raw)
        self.ruta = self.raw / "_manifiesto.json"
        try:
            self.datos = json.loads(self.ruta.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            self.datos = {}

    def rel(self, ruta):
        return Path(ruta).relative_to(self.raw).as_posix()

    def get(self, rel):
        return self.datos.get(rel, {})

    def registrar(self, ruta, url, estado, **extra):
        entrada = self.datos.setdefault(self.rel(ruta), {})
        entrada.update(extra)
        entrada["url"] = url
        entrada["publicado"] = True
        entrada.pop("retirado_desde", None)
        entrada.pop("detalle", None)
        if estado == "sin_cambios" and entrada.get("fecha_descarga"):
            entrada["comprobado"] = iso(ahora())
        else:
            entrada["fecha_descarga"] = entrada["comprobado"] = fecha_version(ruta)
        self.guardar()

    def retirar(self, ruta, detalle):
        entrada = self.datos.setdefault(self.rel(ruta), {})
        entrada["publicado"] = False
        entrada.setdefault("retirado_desde", iso(ahora()))
        entrada["comprobado"] = iso(ahora())
        entrada["detalle"] = detalle
        self.guardar()

    def guardar(self):
        self.raw.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_name(f".{self.ruta.name}.tmp")
        tmp.write_text(json.dumps(self.datos, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.ruta)


class Resumen:
    """Lo descargado, lo retirado, lo pendiente (originales que no se pueden leer
    aquí) y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = []
        self.retirados = []
        self.avisos = []
        self.pendientes = []
        self.fallidos = []
        self.parquets = []

    def descarga(self, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios.append(etiqueta)
        else:
            self.descargados.append(f"{etiqueta} ({estado})")

    def texto(self):
        lineas = ["=" * 70]
        problemas = len(self.fallidos) + len(self.pendientes)
        if problemas:
            lineas.append(f"⚠️ {self.titulo}: COMPLETADO CON ERRORES ({problemas})")
        else:
            lineas.append(f"✅ {self.titulo}: COMPLETADO")
        lineas += ["=" * 70, f"Inicio: {iso(self.inicio)}  Fin: {iso(ahora())}"]

        def bloque(titulo, elementos):
            if elementos:
                lineas.append(f"\n{titulo} ({len(elementos)}):")
                lineas.extend(f"   - {e}" for e in elementos)

        bloque("DESCARGADOS (versión nueva)", self.descargados)
        bloque("SIN CAMBIOS", self.sin_cambios)
        bloque("RETIRADOS POR EL PORTAL (se conservan con _en_ultima_descarga=False)", self.retirados)
        bloque("PARQUET", [f"{n}: {f:,} filas x {c} columnas ({r:,} ya no publicadas)"
                           for n, f, c, r in self.parquets])
        bloque("AVISOS", self.avisos)
        bloque("PENDIENTES - el original está guardado pero no se puede leer aquí; sus filas faltan "
               "en el Parquet", self.pendientes)
        bloque("ERRORES - vuelve a ejecutar el script para reintentarlos", self.fallidos)
        return "\n".join(lineas)

    def cerrar(self, raw):
        texto = self.texto()
        print("\n" + texto)
        raw = Path(raw)
        raw.mkdir(parents=True, exist_ok=True)
        with open(raw / "descarga_log.txt", "a", encoding="utf-8") as f:
            f.write(texto + "\n\n")
        return 1 if self.fallidos or self.pendientes else 0


# ----------------------------------------------------------------------------
# Lectura de ficheros como texto (todas las filas y columnas, sin convertir nada)
# ----------------------------------------------------------------------------

def _detectar_codificacion(ruta):
    """Primera codificación capaz de decodificar el fichero COMPLETO. cp1252 va
    antes que latin-1 (latin-1 acepta cualquier byte y dejaría '€', '’'... como
    caracteres de control)."""
    with open(ruta, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    candidatas = ["utf-8-sig"] if inicio.startswith(b"\xef\xbb\xbf") else ["utf-8"]
    for codificacion in candidatas + ["cp1252", "latin-1"]:
        decodificador = codecs.getincrementaldecoder(codificacion)()
        try:
            with open(ruta, "rb") as f:
                while bloque := f.read(1 << 20):
                    decodificador.decode(bloque)
            decodificador.decode(b"", final=True)
            return codificacion
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detectar_separador(ruta, codificacion):
    """Separador más frecuente (fuera de comillas) en la primera línea con texto."""
    with open(ruta, encoding=codificacion, newline="") as f:
        primera = next((linea for linea in f if linea.strip()), "")
    cuentas = dict.fromkeys([";", ",", "\t", "|"], 0)
    entre_comillas = False
    for caracter in primera:
        if caracter == '"':
            entre_comillas = not entre_comillas
        elif not entre_comillas and caracter in cuentas:
            cuentas[caracter] += 1
    mejor = max(cuentas, key=cuentas.get)
    return mejor if cuentas[mejor] else ","


def _lineas_de_datos(ruta):
    """Líneas no vacías del fichero menos la cabecera (para avisar si el
    analizador junta líneas: comillas desparejadas, saltos de línea en campos)."""
    with open(ruta, "rb") as f:
        return max(sum(1 for linea in f if linea.strip()) - 1, 0)


def _nombres_columnas(cabecera):
    """Nombres únicos como los pone pandas: vacíos -> 'Unnamed: i', repetidos -> 'X.1'."""
    nombres, vistos = [], set()
    for i, valor in enumerate(cabecera):
        nombre = "" if valor is None else str(valor)
        if nombre == "":
            nombre = f"Unnamed: {i}"
        if nombre in vistos:
            k = 1
            while f"{nombre}.{k}" in vistos:
                k += 1
            nombre = f"{nombre}.{k}"
        vistos.add(nombre)
        nombres.append(nombre)
    return nombres


def _leer_csv_tolerante(ruta, sep, codificacion):
    """Lectura que no descarta nada: como el módulo csv, pero una comilla
    literal al principio de un campo no se traga los registros siguientes
    (comun.lectura_csv); los campos que sobran respecto a la cabecera van a
    columnas _columna_extra_N. Devuelve (df, filas con campos de más,
    comillas literales)."""
    with open(ruta, encoding=codificacion, newline="") as f:
        filas, literales = registros_csv(f.read(), sep)
    if not filas:
        return pd.DataFrame(), 0, 0
    nombres = _nombres_columnas(filas[0])
    ancho = max(len(fila) for fila in filas)
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    datos = [[v if v != "" else None for v in fila] + [None] * (ancho - len(fila)) for fila in filas[1:]]
    df = pd.DataFrame(datos, columns=nombres, dtype=object)
    extra = [c for c in nombres if c.startswith("_columna_extra_")]
    con_extra = int(df[extra].notna().any(axis=1).sum()) if extra else 0
    vacias = [c for c in extra if df[c].isna().all()]
    return df.drop(columns=vacias), con_extra, literales


def leer_csv(ruta, nombre=None):
    """CSV como texto: dtype=str, sin convertir 'NA', 'N/A', 'NULL'... en nulos
    (solo el campo vacío es nulo) y sin perder filas ni campos."""
    ruta = Path(ruta)
    nombre = nombre or ruta.name
    avisos = []
    codificacion = _detectar_codificacion(ruta)
    sep = _detectar_separador(ruta, codificacion)
    try:
        with warnings.catch_warnings():
            # "Length of header or names does not match": pandas perdería campos
            warnings.simplefilter("error", pd.errors.ParserWarning)
            df = pd.read_csv(ruta, sep=sep, encoding=codificacion, dtype=str, keep_default_na=False,
                             na_values=[""], index_col=False, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), [f"{nombre}: fichero sin cabecera ni filas"]
    except (pd.errors.ParserError, pd.errors.ParserWarning):
        df, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
        if con_extra:
            avisos.append(f"{nombre}: {con_extra:,} filas con más campos que la cabecera; "
                          "los campos de más se conservan en columnas _columna_extra_N")
        if literales:
            avisos.append(f"{nombre}: {literales:,} comillas literales al principio de un campo "
                          "(se conservan en el texto; sin ellas se tragarían los registros siguientes)")
    if codificacion != "utf-16":
        lineas = _lineas_de_datos(ruta)
        if lineas != len(df):
            # Puede ser una comilla literal que se traga registros enteros:
            # se vuelve a leer sin que lo haga (comun.lectura_csv)
            tolerante, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
            if literales and len(tolerante) > len(df):
                avisos.append(f"{nombre}: {literales:,} comillas literales al principio de un campo se "
                              f"tragaban {len(tolerante) - len(df):,} registros; se leen sin tragárselos "
                              "(la comilla se conserva en el texto)")
                if con_extra:
                    avisos.append(f"{nombre}: {con_extra:,} filas con más campos que la cabecera; "
                                  "los campos de más se conservan en columnas _columna_extra_N")
                df = tolerante
        if lineas != len(df):
            avisos.append(f"{nombre}: {len(df):,} filas leídas de {lineas:,} líneas de datos "
                          "(campos entrecomillados con saltos de línea o comillas desparejadas): revisar")
    return df, avisos


def _celda_texto(valor):
    """Valor de una celda de Excel como texto (None si está vacía)."""
    if valor is None:
        return None
    if isinstance(valor, str):
        return valor if valor != "" else None
    if isinstance(valor, bool):
        return str(valor)
    if isinstance(valor, float):
        if math.isnan(valor):
            return None
        return str(int(valor)) if valor.is_integer() else repr(valor)
    if isinstance(valor, datetime):
        if valor.tzinfo is None and valor.time() == dt.time(0):
            return valor.date().isoformat()
        return valor.isoformat(sep=" ")
    if isinstance(valor, (dt.date, dt.time)):
        return valor.isoformat()
    return str(valor)


PATRON_DATO = re.compile(r"-?\d+(?:[.,]\d+)*|\d{4}-\d{2}-\d{2}(?:[ T][\d:.]+)?|[A-Z]?\d{7,8}[A-Z]?|[A-Z]\d{7}[A-Z0-9]",
                         re.IGNORECASE)


def _parece_registro(fila):
    """¿La fila detectada como cabecera son datos? Lo son si la mayoría de sus
    celdas con valor son números, fechas o NIF (una cabecera son rótulos)."""
    valores = [str(v).strip() for v in fila if v is not None and str(v).strip()]
    return bool(valores) and sum(bool(PATRON_DATO.fullmatch(v)) for v in valores) * 2 > len(valores)


def _hoja_a_df(filas, nombre, hoja, avisos):
    """Tabla de una hoja: detecta la fila de cabecera (las filas de título de
    encima se anotan en los avisos) y conserva todas las filas con algún valor."""
    # Número de fila original de cada fila con algún valor (para el aviso)
    numeradas = [(i, list(f)) for i, f in enumerate(filas) if any(v is not None for v in f)]
    if not numeradas:
        return None
    filas = [f for _, f in numeradas]
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    if pos:
        titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
        avisos.append(f"{nombre} [{hoja}]: {numeradas[pos][0]} filas antes de la cabecera "
                      f"(no son datos): {titulo[:200]}")
    ancho = max(len(f) for f in filas)
    cabecera = filas[pos] + [None] * (ancho - len(filas[pos]))
    if _parece_registro(cabecera):
        # Hoja sin cabecera (p.ej. una tabla auxiliar de códigos y NIF): su
        # primera fila es un registro, no los nombres de las columnas
        avisos.append(f"{nombre} [{hoja}]: sin fila de cabecera (la primera fila son datos: "
                      f"{' | '.join(str(v) for v in cabecera if v is not None)[:120]}); columnas columna_1…")
        cabecera = [f"columna_{i}" for i in range(1, ancho + 1)]
        pos -= 1
    datos = [f + [None] * (ancho - len(f)) for f in filas[pos + 1:]]
    df = pd.DataFrame(datos, columns=_nombres_columnas(cabecera), dtype=object)
    # Columnas sin nombre y sin ningún valor: restos del rango usado de Excel
    vacias = [c for c, v in zip(df.columns, cabecera) if v is None and df[c].isna().all()]
    df = df.drop(columns=vacias)
    df["_hoja"] = hoja
    return df


def _leer_xlsx(ruta, nombre=None):
    import openpyxl

    nombre = nombre or Path(ruta).name
    avisos, partes = [], []
    # Se abre como fichero: openpyxl rechaza por la extensión un .xlsx publicado
    # con nombre .xls, y el formato ya se ha decidido por el contenido.
    fichero = open(ruta, "rb")
    try:
        libro = openpyxl.load_workbook(fichero, read_only=True, data_only=True)
    except BaseException:
        fichero.close()
        raise
    try:
        for hoja in libro.worksheets:
            hoja.reset_dimensions()   # no fiarse de la dimensión declarada en el fichero
            filas = ([_celda_texto(v) for v in fila] for fila in hoja.iter_rows(values_only=True))
            df = _hoja_a_df(filas, nombre, hoja.title, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.close()
        fichero.close()
    return partes, avisos


def _leer_xls(ruta, nombre=None):
    xlrd = _modulo("xlrd")
    if xlrd is None:
        raise LectorNoDisponible("hace falta el paquete xlrd para leer .xls (pip install 'xlrd>=2.0.1')")

    def texto(celda, modo_fecha):
        if celda.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
            return None
        if celda.ctype == xlrd.XL_CELL_DATE:
            try:
                return _celda_texto(xlrd.xldate_as_datetime(celda.value, modo_fecha))
            except Exception:
                return _celda_texto(celda.value)
        if celda.ctype == xlrd.XL_CELL_BOOLEAN:
            return str(bool(celda.value))
        if celda.ctype == xlrd.XL_CELL_ERROR:
            return xlrd.error_text_from_code.get(celda.value, "#ERROR")
        return _celda_texto(celda.value)

    nombre = nombre or Path(ruta).name
    avisos, partes = [], []
    libro = xlrd.open_workbook(str(ruta), on_demand=True)
    try:
        for hoja in libro.sheets():
            filas = ([texto(c, libro.datemode) for c in hoja.row(i)] for i in range(hoja.nrows))
            df = _hoja_a_df(filas, nombre, hoja.name, avisos)
            if df is not None:
                partes.append(df)
            if hoja.nrows >= FILAS_MAXIMAS_XLS:
                avisos.append(f"{nombre} [{hoja.name}]: {hoja.nrows:,} filas, el máximo de un .xls: "
                              "la hoja puede estar truncada en origen")
    finally:
        libro.release_resources()
    return partes, avisos


NS_EXCEL_XML = "{urn:schemas-microsoft-com:office:spreadsheet}"


def _dato_xml(dato):
    """Valor de un <Data> de SpreadsheetML como texto, como las demás hojas:
    números y fechas por _celda_texto, el resto tal cual."""
    if dato is None:
        return None
    texto = "".join(dato.itertext())
    tipo = dato.get(NS_EXCEL_XML + "Type")
    try:
        if tipo == "Number":
            return _celda_texto(float(texto))
        if tipo == "DateTime":
            return _celda_texto(datetime.fromisoformat(texto))
        if tipo == "Boolean":
            return str(texto.strip() not in ("0", "", "false", "False"))
    except ValueError:
        pass
    return texto if texto != "" else None


def _leer_xml_excel(ruta, nombre=None):
    """Hoja de cálculo XML de Excel 2003 (SpreadsheetML), en streaming: cada
    <Row> es una fila y cada <Cell> su <Data>, respetando las celdas que se
    saltan (ss:Index) y las combinadas (ss:MergeAcross). Las filas que se saltan
    no importan: las vacías no son datos."""
    import xml.etree.ElementTree as ET

    nombre = nombre or Path(ruta).name
    avisos, partes, filas, hoja = [], [], [], None
    for evento, elemento in ET.iterparse(str(ruta), events=("start", "end")):
        if elemento.tag == NS_EXCEL_XML + "Worksheet" and evento == "start":
            hoja, filas = elemento.get(NS_EXCEL_XML + "Name"), []
        elif elemento.tag == NS_EXCEL_XML + "Row" and evento == "end":
            fila = []
            for celda in elemento.findall(NS_EXCEL_XML + "Cell"):
                columna = celda.get(NS_EXCEL_XML + "Index")
                if columna:
                    fila.extend([None] * (int(columna) - 1 - len(fila)))
                fila.append(_dato_xml(celda.find(NS_EXCEL_XML + "Data")))
                fila.extend([None] * int(celda.get(NS_EXCEL_XML + "MergeAcross") or 0))
            filas.append(fila)
            elemento.clear()
        elif elemento.tag == NS_EXCEL_XML + "Worksheet" and evento == "end":
            df = _hoja_a_df(filas, nombre, hoja, avisos)
            if df is not None:
                partes.append(df)
            filas = []
            elemento.clear()
    return partes, avisos


class _LectorTablaHTML(HTMLParser):
    """Filas de la primera <table> de una página: [(es_cabecera, [textos])]. El
    texto de cada celda se conserva tal cual (entidades resueltas, sin recortar
    espacios); las tablas anidadas suman su texto a la celda que las contiene."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.filas = []
        self._fila = self._celda = None
        self._cabecera = True
        self._tablas = 0
        self._terminada = False

    def _cerrar_celda(self):
        if self._celda is not None:
            self._fila.append("".join(self._celda))
            self._celda = None

    def _cerrar_fila(self):
        self._cerrar_celda()
        if self._fila:
            self.filas.append((self._cabecera, self._fila))
        self._fila = None

    def handle_starttag(self, tag, attrs):
        if self._terminada:
            return
        if tag == "table":
            self._tablas += 1
        elif self._tablas == 1 and tag == "tr":
            self._cerrar_fila()
            self._fila, self._cabecera = [], True
        elif self._tablas == 1 and tag in ("td", "th"):
            if self._fila is None:
                self._fila, self._cabecera = [], True
            self._cerrar_celda()
            self._celda = []
            self._cabecera = self._cabecera and tag == "th"
        elif tag == "br" and self._celda is not None:
            self._celda.append("\n")

    def handle_endtag(self, tag):
        if self._terminada:
            return
        if tag == "table":
            if self._tablas == 1:
                self._cerrar_fila()
                self._terminada = True
            self._tablas -= 1
        elif self._tablas == 1 and tag in ("td", "th"):
            self._cerrar_celda()
        elif self._tablas == 1 and tag == "tr":
            self._cerrar_fila()

    def handle_data(self, data):
        if self._celda is not None and not self._terminada:
            self._celda.append(data)


def leer_html(ruta, nombre=None):
    """Tabla HTML (la de la UCLM) como texto: la primera fila de <th> es la
    cabecera y cada <tr> con <td> un registro, con el texto de cada celda tal
    cual. Las celdas de más van a columnas _columna_extra_N."""
    ruta = Path(ruta)
    nombre = nombre or ruta.name
    lector = _LectorTablaHTML()
    lector.feed(ruta.read_bytes().decode("utf-8", errors="replace"))
    lector.close()
    lector._cerrar_fila()
    filas = lector.filas
    if not filas:
        return pd.DataFrame(), [f"{nombre}: no trae ninguna tabla"]
    cabecera = filas[0][1] if filas[0][0] else []
    datos = [f for _, f in (filas[1:] if cabecera else filas)]
    avisos = [] if cabecera else [f"{nombre}: tabla sin fila de cabecera; columnas columna_1…"]
    ancho = max([len(cabecera)] + [len(f) for f in datos])
    nombres = (_nombres_columnas(cabecera) if cabecera else [f"columna_{i}" for i in range(1, ancho + 1)])
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    df = pd.DataFrame([[v if v != "" else None for v in f] + [None] * (ancho - len(f)) for f in datos],
                      columns=nombres, dtype=object)
    extra = sum(len(f) > len(cabecera) for f in datos) if cabecera else 0
    if extra:
        avisos.append(f"{nombre}: {extra:,} filas con más celdas que la cabecera "
                      "(se conservan en columnas _columna_extra_N)")
    return df, avisos


# ----------------------------------------------------------------------------
# ZIP, RAR y 7z: se guardan tal cual y se descomprimen en un temporal para leer
# sus tablas (también los archivos anidados, p.ej. un RAR dentro de un ZIP)
# ----------------------------------------------------------------------------

# Qué contenidos se leen como tabla según la extensión del miembro (un .xls puede
# ser en realidad un XLSX, una tabla HTML, un CSV o un XML de Excel 2003)
FORMATOS_MIEMBRO = {".xlsx": ("xlsx", "xls"), ".xlsm": ("xlsx",), ".xls": ("xls", "xlsx", "html", "csv", "xmlss"),
                    ".csv": ("csv",), ".htm": ("html",), ".html": ("html",), ".xml": ("xmlss",),
                    "": ("xlsx", "xls", "html", "xmlss")}
PROFUNDIDAD_MAXIMA = 5
FILAS_MAXIMAS_XLS = 65536


def _nombre_zip(info):
    """Nombre de un miembro de un ZIP: UTF-8 si lo declara; si no, el ZIP lo
    guarda en la página de códigos OEM de Windows (cp850 en España)."""
    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("cp850")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def _destino_miembro(carpeta, i, miembro):
    """Ruta en disco de un miembro extraído: un número y su extensión (el nombre
    original, que puede traer carpetas o caracteres raros, va en _miembro)."""
    return Path(carpeta) / f"{i:05d}{Path(miembro).suffix.lower()[:10]}"


def _extraer_zip(ruta, carpeta):
    with zipfile.ZipFile(ruta) as archivo:
        for i, info in enumerate(archivo.infolist()):
            if info.is_dir():
                continue
            nombre = _nombre_zip(info)
            destino = _destino_miembro(carpeta, i, nombre)
            with archivo.open(info) as origen, open(destino, "wb") as f:
                shutil.copyfileobj(origen, f, 1 << 20)
            yield nombre, destino


def _extraer_libarchive(libarchive, ruta, carpeta):
    with _abrir_libarchive(libarchive, ruta) as archivo:
        for i, entrada in enumerate(archivo):
            if not entrada.isfile:
                continue
            destino = _destino_miembro(carpeta, i, entrada.pathname)
            with open(destino, "wb") as f:
                for bloque in entrada.get_blocks():
                    f.write(bloque)
            yield entrada.pathname, destino


def _extraer_rarfile(rarfile, ruta, carpeta):
    with rarfile.RarFile(str(ruta)) as archivo:
        for i, info in enumerate(archivo.infolist()):
            if info.is_dir():
                continue
            destino = _destino_miembro(carpeta, i, info.filename)
            with archivo.open(info) as origen, open(destino, "wb") as f:
                shutil.copyfileobj(origen, f, 1 << 20)
            yield info.filename, destino


def _con_programa(rarfile):
    """¿Tiene rarfile un programa con el que descomprimir (unrar, unar, 7z, bsdtar)?"""
    try:
        rarfile.tool_setup()
        return True
    except Exception:
        return False


def _extractores(formato):
    """[(nombre, función(ruta, carpeta) -> [(miembro, ruta extraída)])] con los
    que se puede abrir un formato de archivo, por orden de preferencia: el ZIP
    con zipfile; el RAR con rarfile si tiene un programa (unrar es la referencia)
    y con libarchive (que falla con algún RAR válido); el 7z con libarchive.
    LectorNoDisponible si no hay ninguno."""
    if formato == "zip":
        return [("zipfile", _extraer_zip)]
    extractores = []
    rarfile = _modulo("rarfile") if formato == "rar" else None
    if rarfile is not None and _con_programa(rarfile):
        extractores.append(("rarfile", lambda ruta, carpeta: _extraer_rarfile(rarfile, ruta, carpeta)))
    libarchive = _modulo("libarchive")
    if libarchive is not None:
        extractores.append(("libarchive", lambda ruta, carpeta: _extraer_libarchive(libarchive, ruta, carpeta)))
    if not extractores:
        raise LectorNoDisponible(f"no se puede abrir un {formato.upper()}: "
                                 + (MENSAJE_RAR if formato == "rar" else "hace falta libarchive-c"))
    return extractores


def descomprimir(ruta, carpeta):
    """[(miembro, ruta extraída)] de todos los ficheros de un ZIP/RAR/7z, con el
    primer extractor que lo abre entero: si uno falla a mitad se prueba el
    siguiente desde el principio (nunca quedan miembros a medias ni repetidos)."""
    formato = formato_fichero(ruta)
    extractores, errores = _extractores(formato), []
    for nombre, extractor in extractores:
        destino = Path(tempfile.mkdtemp(dir=carpeta))
        try:
            return list(extractor(ruta, destino))
        except Exception as e:
            errores.append(f"{nombre}: {type(e).__name__}: {str(e)[:150]}")
            shutil.rmtree(destino, ignore_errors=True)
    detalle = f"{Path(ruta).name}: no se pudo descomprimir ({'; '.join(errores)})"
    if formato == "rar" and "rarfile" not in dict(extractores):
        # libarchive falla con algunos RAR válidos: con unrar se puede leer
        raise LectorNoDisponible(f"{detalle}; {MENSAJE_RAR}")
    raise ValueError(detalle)


def _ignorable(miembro):
    """Restos de otros sistemas dentro de un archivo, que no son datos."""
    partes = Path(miembro.replace("\\", "/")).parts
    return (any(p == "__MACOSX" for p in partes) or partes[-1].startswith(("._", "~$"))
            or partes[-1].lower() in ("thumbs.db", ".ds_store", "desktop.ini"))


def extraer(ruta, carpeta, prefijo="", profundidad=0):
    """(miembro, ruta en disco, formato) de todos los ficheros de un ZIP/RAR/7z,
    entrando en los archivos que contiene ('a.zip' -> 'b.rar/c.xlsx')."""
    for miembro, fichero in descomprimir(ruta, carpeta):
        nombre = prefijo + miembro
        formato = formato_fichero(fichero)
        if formato in FORMATOS_ARCHIVO and profundidad < PROFUNDIDAD_MAXIMA:
            yield from extraer(fichero, fichero.parent, nombre + "/", profundidad + 1)
            fichero.unlink()
        else:
            yield nombre, fichero, formato


def leer_archivo(ruta):
    """Tablas de un ZIP/RAR/7z, unidas, con el miembro de cada fila en _miembro.
    Todo o nada: si algún miembro no se puede abrir (p.ej. falta con qué leer un
    RAR anidado) se lanza la excepción y la versión se vuelve a intentar en la
    siguiente ejecución."""
    ruta = Path(ruta)
    avisos, partes, leidos = [], [], 0
    with tempfile.TemporaryDirectory(prefix="clm_") as tmp:
        for miembro, fichero, formato in extraer(ruta, tmp):
            nombre = f"{ruta.name}/{miembro}"
            if _ignorable(miembro):
                avisos.append(f"{nombre}: se ignora (no son datos)")
            elif formato not in FORMATOS_MIEMBRO.get(Path(miembro).suffix.lower(), ()):
                avisos.append(f"{nombre}: no es una tabla ({formato.upper()}); "
                              "se conserva en el original sin leer")
            else:
                df, avisos_miembro = _leer_simple(fichero, formato, nombre)
                avisos.extend(avisos_miembro)
                leidos += 1
                if len(df):
                    df["_miembro"] = miembro
                    partes.append(df)
            fichero.unlink()
    if not leidos:
        raise ValueError(f"{ruta.name}: el archivo no contiene ninguna tabla")
    if not partes:
        return pd.DataFrame(columns=["_miembro"]), avisos
    return pd.concat(partes, ignore_index=True, sort=False), avisos


def _leer_simple(ruta, formato, nombre=None):
    nombre = nombre or Path(ruta).name
    if formato == "csv":
        return leer_csv(ruta, nombre)
    if formato in ("xlsx", "xls", "xmlss"):
        partes, avisos = {"xlsx": _leer_xlsx, "xls": _leer_xls, "xmlss": _leer_xml_excel}[formato](ruta, nombre)
        if len(partes) > 1:
            avisos.append(f"{nombre}: {len(partes)} hojas con datos; se unen (columna _hoja)")
        if not partes:
            return pd.DataFrame(columns=["_hoja"]), avisos
        return pd.concat(partes, ignore_index=True, sort=False), avisos
    if formato == "html":
        return leer_html(ruta, nombre)
    raise ValueError(f"{nombre}: formato {formato.upper()} no tabular")


def leer_tabla(ruta):
    """Lee un fichero tabular (CSV, XLSX, XLS, tabla HTML o un ZIP/RAR/7z con
    ellos, según su contenido real) con todas sus filas y columnas como texto.
    Devuelve (DataFrame, avisos)."""
    ruta = Path(ruta)
    formato = formato_fichero(ruta)
    if formato in FORMATOS_ARCHIVO:
        return leer_archivo(ruta)
    return _leer_simple(ruta, formato)


# ----------------------------------------------------------------------------
# Registros acumulados (comun/historico.py) y Parquet
# ----------------------------------------------------------------------------

def _como_texto(serie):
    """Serie de texto (None = nulo) para escribirla como string en Parquet."""
    if isinstance(serie.dtype, pd.StringDtype):
        return serie
    valores = serie.astype(object)
    if pd.api.types.infer_dtype(valores, skipna=True) in ("string", "empty"):
        return valores
    return valores.map(lambda v: v if isinstance(v, str) else (None if pd.isna(v) else str(v)))


def ordenar_columnas(df):
    """Columnas del portal (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def _tabla_arrow(df):
    """Tabla Arrow con todas las columnas como texto (_en_ultima_descarga booleana)."""
    df = ordenar_columnas(df).copy()
    campos = []
    for columna in df.columns:
        if columna == "_en_ultima_descarga":
            df[columna] = df[columna].astype(bool)
            campos.append(pa.field(columna, pa.bool_()))
        else:
            df[columna] = _como_texto(df[columna])
            campos.append(pa.field(str(columna), pa.string()))
    return pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)


def _origenes_previos(destino):
    """Valores de _archivo_origen del Parquet anterior (sin cargarlo entero)."""
    if not Path(destino).exists():
        return []
    columna = pq.read_table(destino, columns=["_archivo_origen"]).column(0)
    return [str(v) for v in columna.unique().to_pylist() if v is not None]


def _filas_previas(destino, rel):
    """Filas de un fichero crudo en el Parquet anterior (o None)."""
    tabla = pq.read_table(destino, filters=[("_archivo_origen", "==", rel)])
    return tabla.to_pandas() if tabla.num_rows else None


def unir_partes(partes, destino):
    """Escribe `destino` con las partes (Parquet temporales, uno por fichero
    crudo) en una sola tabla: la unión de sus columnas (las del portal por orden
    de aparición y después las del script), nulas donde una parte no las trae.
    Se escribe parte a parte (sin cargar todo en memoria) en un temporal que pasa
    por guardar_version: la versión anterior del Parquet queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    nombres = list(dict.fromkeys(c for parte in partes for c in pq.read_schema(parte).names))
    orden = [c for c in nombres if c not in ORDEN_METADATOS] + [c for c in ORDEN_METADATOS if c in nombres]
    esquema = pa.schema([pa.field(c, pa.bool_() if c == "_en_ultima_descarga" else pa.string()) for c in orden])
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        with pq.ParquetWriter(tmp, esquema, compression="snappy") as escritor:
            for parte in partes:
                tabla = pq.read_table(parte)
                columnas = [tabla.column(c) if c in tabla.column_names else pa.nulls(tabla.num_rows, campo.type)
                            for c, campo in zip(esquema.names, esquema)]
                escritor.write_table(pa.Table.from_arrays(columnas, schema=esquema))
        return guardar_version(destino, desde=tmp), len(esquema)
    finally:
        if tmp.exists():
            tmp.unlink()


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen):
    """Registros de un fichero crudo a lo largo de todas sus versiones.

    anterior: filas de este fichero en el Parquet anterior (o None). Solo se
    aplican las versiones posteriores a su _ultima_descarga, en orden
    cronológico, con acumular(): nada de lo visto se pierde; lo que el portal
    retira o cambia queda con _en_ultima_descarga=False. Después se aplica la
    última comprobación: sin cambios (actualiza _ultima_descarga) o retirado
    por el portal (todo el fichero pasa a _en_ultima_descarga=False).
    Una versión que no se puede leer (p.ej. un RAR sin con qué abrirlo) queda
    en los pendientes o en los errores y se vuelve a intentar la próxima vez.
    """
    info = manifiesto.get(rel)
    corte = None
    if anterior is not None and len(anterior) and "_ultima_descarga" in anterior.columns:
        corte = anterior["_ultima_descarga"].dropna().astype(str).max()
    lista = versiones(actual)
    comprobado = info.get("comprobado")
    posterior = comprobado is not None and (corte is None or comprobado > corte)
    retirado = info.get("publicado") is False and posterior
    reconfirmado = (posterior and not retirado and bool(lista) and lista[-1] == Path(actual)
                    and comprobado > fecha_version(actual))
    ignorar = METADATOS_ORIGEN
    ultima = None
    for version in lista:
        fecha = fecha_version(version)
        nueva = corte is None or fecha > corte
        if not nueva and not (reconfirmado and version == lista[-1]):
            continue
        try:
            df, avisos = leer_tabla(version)
        except LectorNoDisponible as e:
            resumen.pendientes.append(f"{rel}: versión {version.name}: {e}")
            continue
        except Exception as e:
            resumen.fallidos.append(f"{rel}: no se pudo leer la versión {version.name}: {e}")
            continue
        resumen.avisos.extend(avisos)
        df = df.copy()
        for columna, valor in metadatos.items():
            df[columna] = valor
        df["_fecha_descarga"] = fecha
        ultima = df
        if not nueva:
            continue
        try:
            anterior = acumular(anterior, df, fecha, ignorar=ignorar)
        except ValueError:
            resumen.avisos.append(f"{rel}: la versión del {fecha} no tiene filas; no se marca nada como retirado")
    if anterior is None or not len(anterior):
        return anterior
    if retirado:
        anterior = acumular(anterior, pd.DataFrame(), comprobado, ignorar=ignorar, permitir_vacio=True)
    elif reconfirmado and ultima is not None and len(ultima):
        anterior = acumular(anterior, ultima, comprobado, ignorar=ignorar)
    return anterior


def construir_parquet(destino, ficheros, raw, manifiesto, resumen):
    """Genera `destino` con los registros acumulados de `ficheros`
    (lista de (ruta_actual, rel, metadatos)) partiendo del Parquet anterior.

    Se procesa fichero a fichero (las filas previas de cada uno se leen del
    Parquet anterior con un filtro) y cada resultado va a un Parquet temporal:
    así un conjunto de millones de filas (SESCAM) no tiene que caber entero en
    memoria. Las filas de ficheros que ya no se procesan se conservan: si el
    fichero sigue en raw/ se vuelve a procesar con sus versiones; si no, se
    copian tal cual del Parquet anterior.
    Devuelve (filas, columnas) o None si no hay nada que escribir.
    """
    destino = Path(destino)
    try:
        previos = _origenes_previos(destino)
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    ficheros = list(ficheros)
    procesados = {rel for _, rel, _ in ficheros}
    huerfanos = []
    for rel in previos:
        if rel in procesados:
            continue
        if (Path(raw) / rel).exists():
            grupo = _filas_previas(destino, rel)
            metadatos = {c: grupo[c].iloc[0] for c in METADATOS_ORIGEN
                         if c in grupo.columns and c not in ("_miembro", "_hoja", "_fecha_descarga")}
            ficheros.append((Path(raw) / rel, rel, metadatos))
            procesados.add(rel)
        else:
            huerfanos.append(rel)
    filas = retiradas = 0
    with tempfile.TemporaryDirectory(prefix=f".{destino.stem}_", dir=destino.parent) as tmp:
        partes = []

        def guardar_parte(registros):
            nonlocal filas, retiradas
            parte = Path(tmp) / f"{len(partes):05d}.parquet"
            pq.write_table(_tabla_arrow(registros), parte, compression="snappy")
            partes.append(parte)
            filas += len(registros)
            retiradas += int((~registros["_en_ultima_descarga"].astype(bool)).sum())

        for actual, rel, metadatos in ficheros:
            anterior = _filas_previas(destino, rel) if rel in previos else None
            registros = acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen)
            if registros is not None and len(registros):
                guardar_parte(registros)
        for rel in huerfanos:
            grupo = _filas_previas(destino, rel)
            resumen.avisos.append(f"{destino.name}: {rel} ya no está en raw/; se conservan sus {len(grupo):,} filas")
            guardar_parte(grupo)
        if not partes:
            return None
        estado, columnas = unir_partes(partes, destino)
    resumen.parquets.append((destino.name, filas, columnas, retiradas))
    print(f"  💾 {destino.name}: {filas:,} filas ({estado})")
    return filas, columnas


# ============================================================================
# CASTILLA-LA MANCHA: PORTAL DE CONTRATACIÓN DE LA JUNTA
# ============================================================================

PATRON_ENLACE = re.compile(r"""<a\b[^>]*?\bhref\s*=\s*["']([^"']+)["'][^>]*>(.*?)</a\s*>""", re.I | re.S)
PATRON_FILA_VISTA = re.compile(r"""<div\b[^>]*\bclass\s*=\s*["'][^"']*\bviews-row\b[^"']*["'][^>]*>(.*?)</div>""",
                               re.I | re.S)
PATRON_ETIQUETA = re.compile(r"<[^>]+>")
PATRON_OPCION = re.compile(r"""<option\b[^>]*?\bvalue\s*=\s*["']([^"']*)["'][^>]*>(.*?)</option\s*>""", re.I | re.S)
PATRON_ANIO = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
ORDINALES = {"primer": 1, "primero": 1, "segundo": 2, "tercer": 3, "tercero": 3, "cuarto": 4}
PATRONES_PERIODO = [
    re.compile(r"(?<![\d.])([1-4])\s*(?:o|a|er|ro|do|to)?\.?\s*(trimestre|semestre)\b"),   # "1º trimestre"
    re.compile(r"\b(primer|primero|segundo|tercer|tercero|cuarto)\s+(trimestre|semestre)\b"),
    re.compile(r"\b(trimestre|semestre)\s+([1-4])(?!\d)"),                                  # "trimestre 3-2019"
]
PATRON_ANUAL = re.compile(r"\b(completo|anual|anuales)\b")


def texto_html(fragmento):
    """Texto visible de un fragmento de HTML (sin etiquetas ni espacios de más)."""
    return " ".join(html.unescape(PATRON_ETIQUETA.sub(" ", fragmento)).split())


def _normalizar(texto):
    return " ".join(re.sub(r"[_\-]+", " ", sin_acentos(texto).lower()).split())


def opciones_select(pagina, nombre):
    """[(valor, texto)] de las opciones del <select name=nombre> de una página."""
    m = re.search(r"""<select\b[^>]*\bname\s*=\s*["']%s["'][^>]*>(.*?)</select\s*>""" % re.escape(nombre),
                  pagina, re.I | re.S)
    if not m:
        return []
    return [(html.unescape(v).strip(), texto_html(t)) for v, t in PATRON_OPCION.findall(m.group(1))]


def enlaces_ficheros(pagina):
    """[(url, texto)] de los ficheros (/sites/default/files/ del portal) que lista
    una página de ficheros de transparencia (bloques views-row del listado)."""
    enlaces, vistos = [], set()
    anfitrion = urlparse(URL_JCCM).netloc
    for bloque in PATRON_FILA_VISTA.findall(pagina) or [pagina]:
        for href, texto in PATRON_ENLACE.findall(bloque):
            url = urljoin(URL_JCCM + "/", html.unescape(href).strip())
            partes = urlparse(url)
            if partes.netloc != anfitrion or RUTA_FICHEROS not in partes.path or url in vistos:
                continue
            vistos.add(url)
            enlaces.append((url, texto_html(texto)))
    return enlaces


def periodo_de(*textos):
    """(periodo, trimestre) del primer texto que lo diga: ('3T', '3'), ('2S',
    None), ('anual', None) o (None, None)."""
    for texto in textos:
        t = _normalizar(texto)
        for i, patron in enumerate(PATRONES_PERIODO):
            m = patron.search(t)
            if not m:
                continue
            if i == 0:
                numero, clase = m.group(1), m.group(2)
            elif i == 1:
                numero, clase = ORDINALES[m.group(1)], m.group(2)
            else:
                numero, clase = m.group(2), m.group(1)
            return (f"{numero}T", str(numero)) if clase == "trimestre" else (f"{numero}S", None)
        if PATRON_ANUAL.search(t):
            return "anual", None
    return None, None


def clasificar(texto, nombre=""):
    """Conjunto de datos de un enlace por su texto (y si no, por el nombre del fichero)."""
    for candidato in (texto, nombre):
        t = _normalizar(candidato)
        for clave, patron in CLASIFICACION:
            if re.search(patron, t):
                return clave
    return "otros"


def conceptos_a_consultar(portada):
    """{id: nombre} de los conceptos del filtro que se consultan: los fijos y
    cualquiera con "menor" en el nombre. Devuelve también los años del filtro."""
    conceptos = {v: t for v, t in opciones_select(portada, "concepto") if v.isdigit()}
    anios = sorted(int(v) for v, _ in opciones_select(portada, "year") if v.isdigit())
    elegidos = dict(CONCEPTOS_FIJOS)
    elegidos.update({c: n for c, n in conceptos.items() if c in CONCEPTOS_FIJOS or "menor" in _normalizar(n)})
    return elegidos, anios


def _todos_los_enlaces(concepto, nombre):
    """¿Se toman todos los enlaces del concepto o solo los que dicen "menores"?"""
    return concepto == CONCEPTO_MENORES or (concepto != CONCEPTO_ESTADISTICA and "menor" in _normalizar(nombre))


def descubrir_jccm(resumen, filtro):
    """Enlaces de menores del portal de la Junta. Devuelve (enlaces, paginas) o
    None si no se puede leer ni la portada: enlaces = [dict], paginas =
    {(concepto, año): nº de ficheros que enlaza la página (de cualquier tipo)}
    solo de las páginas leídas."""
    print("\n🔎 Portal de contratación de la Junta: ficheros de transparencia...")
    try:
        portada = pedir_texto(URL_FICHEROS)
    except ErrorPortal as e:
        resumen.fallidos.append(f"jccm: no se pudo leer {URL_FICHEROS} ({e}); se conservan las copias "
                                "y no se retira nada")
        return None
    conceptos, anios = conceptos_a_consultar(portada)
    if not anios:
        resumen.avisos.append(f"jccm: {URL_FICHEROS} no ofrece años en el filtro; se consultan "
                              f"{ANIO_MINIMO_JCCM}-{ahora().year}")
        anios = list(range(ANIO_MINIMO_JCCM, ahora().year + 1))
    enlaces, paginas, vistos = [], {}, {}
    for concepto, nombre_concepto in sorted(conceptos.items()):
        todos = _todos_los_enlaces(concepto, nombre_concepto)
        for anio in sorted(set(anios) | filtro.anios_locales):
            if not filtro.incluye(anio):
                continue
            try:
                pagina = pedir_texto(URL_FICHEROS, params={"concepto": concepto, "year": anio})
            except ErrorPortal as e:
                resumen.fallidos.append(f"jccm: no se pudo leer la página del concepto {concepto} y el año "
                                        f"{anio} ({e}); no se retira nada de ella")
                continue
            finally:
                time.sleep(PAUSA)
            listados = enlaces_ficheros(pagina)
            paginas[(concepto, anio)] = len(listados)
            for url, texto in listados:
                nombre = unquote(Path(urlparse(url).path).name)
                if not todos and "menor" not in _normalizar(texto):
                    continue
                if url in vistos:
                    if vistos[url] != (concepto, anio):
                        resumen.avisos.append(f"jccm: {url} aparece en {vistos[url]} y en {(concepto, anio)}; "
                                              "se descarga una vez")
                    continue
                vistos[url] = (concepto, anio)
                dataset = clasificar(texto, nombre)
                periodo, trimestre = periodo_de(texto, nombre)
                anio_texto = PATRON_ANIO.findall(texto)
                if anio_texto and int(anio_texto[0]) != anio:
                    resumen.avisos.append(f"jccm: '{texto}' está en el año {anio} del portal y su texto dice "
                                          f"{anio_texto[0]}; se usa el del portal ({url})")
                if dataset == "otros":
                    resumen.avisos.append(f"jccm: '{texto}' ({url}) no casa con ninguna serie; va a 'otros'")
                enlaces.append({"concepto": concepto, "anio": anio, "url": url, "titulo": texto,
                                "nombre": nombre, "dataset": dataset, "periodo": periodo, "trimestre": trimestre})
        print(f"   concepto {concepto} ({nombre_concepto}): "
              f"{sum(e['concepto'] == concepto for e in enlaces)} ficheros de menores")
    # Dos enlaces distintos con el mismo nombre en el mismo año y conjunto: se
    # distinguen por la carpeta del portal (p.ej. 2023-06) para no mezclarlos
    rutas = {}
    for e in enlaces:
        rutas.setdefault((e["dataset"], e["anio"], e["nombre"]), []).append(e)
    for grupo in rutas.values():
        if len(grupo) > 1:
            for e in grupo:
                e["nombre"] = f"{Path(urlparse(e['url']).path).parent.name}_{e['nombre']}"
    return enlaces, paginas


def descargar_jccm(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    """Descarga los ficheros de menores del portal de la Junta y retira lo que
    las páginas leídas ya no enlazan."""
    filtro.anios_locales = {int(i["anio"]) for i in manifiesto.datos.values()
                            if i.get("fuente") == "jccm" and str(i.get("anio", "")).isdigit()}
    descubierto = descubrir_jccm(resumen, filtro)
    if descubierto is None:
        return
    enlaces, paginas = descubierto
    guardar_json(raw / "inventario_jccm.json", {
        "enlaces": [{k: e[k] for k in ("concepto", "anio", "titulo", "url", "dataset", "periodo", "trimestre")}
                    for e in enlaces],
        "paginas": {f"concepto={c}&year={a}": n for (c, a), n in sorted(paginas.items())}})
    anio_actual = ahora().year
    enlazados = set()
    for e in sorted(enlaces, key=lambda e: (e["dataset"], e["anio"], e["nombre"])):
        destino = raw / e["dataset"] / str(e["anio"]) / e["nombre"]
        rel = manifiesto.rel(destino)
        enlazados.add(rel)
        info = manifiesto.get(rel)
        if (destino.exists() and not comprobar_todo and e["anio"] < anio_actual - 1
                and info.get("url") == e["url"] and info.get("publicado", True)):
            resumen.sin_cambios.append(f"{rel} (ya descargado; --comprobar-todo para volver a pedirlo)")
            continue
        estado, detalle = descargar(e["url"], destino, tipo=_tipo(e["url"]))
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, e["url"], estado, fuente="jccm", dataset=e["dataset"], anio=e["anio"],
                                 concepto=e["concepto"], titulo=e["titulo"], periodo=e["periodo"],
                                 trimestre=e["trimestre"])
            resumen.descarga(rel, estado)
            print(f"  ✅ {rel}: {estado}")
        else:
            # Enlazado y no se puede bajar: se conserva la copia y se reintenta
            resumen.fallidos.append(f"{rel}: {detalle or estado} ({e['url']})")
            print(f"  ❌ {rel}: {detalle or estado}")
    # Retirados: solo de páginas leídas que enlazan algún fichero
    for rel, info in sorted(manifiesto.datos.items()):
        if info.get("fuente") != "jccm" or rel in enlazados or info.get("publicado") is False:
            continue
        pagina = (str(info.get("concepto")), int(info.get("anio", 0)))
        if pagina not in paginas or not (raw / rel).exists():
            continue
        if not paginas[pagina]:
            resumen.fallidos.append(f"jccm: la página del concepto {pagina[0]} y el año {pagina[1]} no enlaza "
                                    f"ningún fichero y antes enlazaba {rel}; no se retira nada")
            continue
        manifiesto.retirar(raw / rel, f"{URL_FICHEROS}?concepto={pagina[0]}&year={pagina[1]} ya no lo enlaza")
        resumen.retirados.append(f"{rel}: la página ya no lo enlaza; se conservan sus filas")
        print(f"  🗑️ {rel}: retirado por el portal")


# ============================================================================
# UNIVERSIDAD DE CASTILLA-LA MANCHA (ASP.NET WebForms)
# ============================================================================

PATRON_INPUT = re.compile(r"<input\b[^>]*>", re.I)
PATRON_ATRIBUTO = re.compile(r"""([\w:$.-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')""")
PATRON_FILA_DATOS = re.compile(r"<tr\b[^>]*>\s*<td\b", re.I)


def campos_ocultos(pagina):
    """Campos ocultos de un formulario ASP.NET (__VIEWSTATE, __EVENTVALIDATION...)."""
    campos = {}
    for etiqueta in PATRON_INPUT.findall(pagina):
        atributos = {k.lower(): html.unescape(a if a or not b else b) for k, a, b in PATRON_ATRIBUTO.findall(etiqueta)}
        if atributos.get("type", "").lower() == "hidden" and atributos.get("name"):
            campos[atributos["name"]] = atributos.get("value", "")
    return campos


def tabla_html(pagina, id_tabla):
    """El fragmento <table id=id_tabla>…</table> de una página, tal cual, o None."""
    inicio = re.search(r"""<table\b[^>]*\bid\s*=\s*["']%s["'][^>]*>""" % re.escape(id_tabla), pagina, re.I)
    if not inicio:
        return None
    profundidad = 1
    for etiqueta in re.finditer(r"<(/?)table\b[^>]*>", pagina[inicio.end():], re.I):
        profundidad += -1 if etiqueta.group(1) else 1
        if profundidad == 0:
            return pagina[inicio.start():inicio.end() + etiqueta.end()]
    return None


def documento_tabla(titulo, url, fragmento):
    """HTML mínimo con la tabla tal cual la sirve la página (lo que se guarda)."""
    return ("<!DOCTYPE html>\n<html lang=\"es\">\n<head>\n<meta charset=\"utf-8\" />\n"
            f"<title>{html.escape(titulo)}</title>\n</head>\n<body>\n"
            f"<!-- Tabla {TABLA_UCLM} de {html.escape(url)}, tal cual la sirve el portal -->\n"
            f"{fragmento}\n</body>\n</html>\n").encode("utf-8")


def descargar_tabla_uclm(url, destino, titulo, datos=None):
    """GET (datos=None) o POST de una página de menores de la UCLM; guarda su
    tabla de resultados. Devuelve (estado, detalle, filas)."""
    try:
        pagina = pedir_texto(url) if datos is None else pedir_post(url, datos)
    except ErrorPortal as e:
        return "error", str(e), 0
    fragmento = tabla_html(pagina, TABLA_UCLM)
    if fragmento is None:
        return "invalido", f"la respuesta no trae la tabla {TABLA_UCLM}", 0
    estado = guardar_version(destino, documento_tabla(titulo, url, fragmento))
    detalle = ("la tabla está paginada: solo se ha guardado la página que sirve el portal"
               if "Page$" in fragmento else "")
    return estado, detalle, len(PATRON_FILA_DATOS.findall(fragmento))


def descargar_uclm(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    """Menores anteriores (un POST por ejercicio) y actuales (GET) de la UCLM.
    Lo que ya no se ofrece queda como retirado, solo si se ha podido leer la
    lista de ejercicios."""
    print(f"\n📦 uclm: {DATASETS['uclm']['descripcion']}")
    try:
        portada = pedir_texto(URL_UCLM_ANTERIORES)
    except ErrorPortal as e:
        resumen.fallidos.append(f"uclm: no se pudo leer {URL_UCLM_ANTERIORES} ({e}); se conservan las copias "
                                "y no se retira nada")
        return
    campos = campos_ocultos(portada)
    anios = sorted({int(v) for v, _ in opciones_select(portada, CAMPO_EJERCICIO_UCLM) if v.isdigit()})
    if not anios or "__VIEWSTATE" not in campos:
        resumen.fallidos.append(f"uclm: {URL_UCLM_ANTERIORES} no trae el formulario de ejercicios; "
                                "no se retira nada")
        return
    anio_actuales = max(anios) + 1
    guardar_json(raw / "inventario_uclm.json", {"anteriores": anios, "actuales": anio_actuales})
    anio_actual = ahora().year
    publicados = set()
    paginas = [(a, URL_UCLM_ANTERIORES, f"contratosMenoresAnteriores_{a}.html",
                f"Contratos Menores Anteriores {a}", "anual") for a in anios]
    paginas.append((anio_actuales, URL_UCLM_ACTUALES, f"contratosMenoresActuales_{anio_actuales}.html",
                    f"Contratos Menores Actuales ({anio_actuales})", "en curso"))
    for anio, url, nombre, titulo, periodo in paginas:
        destino = raw / "uclm" / str(anio) / nombre
        rel = manifiesto.rel(destino)
        publicados.add(rel)
        if not filtro.incluye(anio):
            continue
        if (destino.exists() and not comprobar_todo and anio < anio_actual - 1
                and manifiesto.get(rel).get("publicado", True)):
            resumen.sin_cambios.append(f"{rel} (ya descargado; --comprobar-todo para volver a pedirlo)")
            continue
        datos = None
        if url == URL_UCLM_ANTERIORES:
            datos = dict(campos, __EVENTTARGET=BOTON_BUSCAR_UCLM, __EVENTARGUMENT="")
            datos[CAMPO_EJERCICIO_UCLM] = str(anio)
        estado, detalle, filas = descargar_tabla_uclm(url, destino, titulo, datos)
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, url, estado, fuente="uclm", dataset="uclm", anio=anio, titulo=titulo,
                                 periodo=periodo, trimestre=None, metodo="GET" if datos is None else "POST",
                                 filas=filas)
            resumen.descarga(f"{rel} ({filas:,} filas)", estado)
            print(f"  ✅ {anio}: {filas:,} filas ({estado})")
            if detalle:
                resumen.fallidos.append(f"{rel}: {detalle}")
        else:
            resumen.fallidos.append(f"{rel}: {detalle or estado} ({url}, ejercicio {anio})")
            print(f"  ❌ {anio}: {detalle or estado}")
    for ruta in sorted((raw / "uclm").glob("*/*.html")) if (raw / "uclm").is_dir() else []:
        rel = manifiesto.rel(ruta)
        if rel not in publicados and manifiesto.get(rel).get("publicado", True):
            manifiesto.retirar(ruta, f"{URL_UCLM} ya no ofrece ese ejercicio en esa página")
            resumen.retirados.append(f"{rel}: la UCLM ya no lo ofrece; se conservan sus filas")
            print(f"  🗑️ {rel}: retirado por el portal")


# ============================================================================
# PARQUET Y LÍNEA DE ÓRDENES
# ============================================================================

class Filtro:
    """Años que se consultan (--desde / --hasta) y años de las copias locales."""

    def __init__(self, desde=None, hasta=None):
        self.desde, self.hasta = desde, hasta
        self.anios_locales = set()

    def incluye(self, anio):
        return (self.desde is None or anio >= self.desde) and (self.hasta is None or anio <= self.hasta)


def ficheros_dataset(raw, clave):
    """Copias actuales de un conjunto de datos (sin _historico/ ni temporales)."""
    carpeta = Path(raw) / clave
    if not carpeta.is_dir():
        return []
    return sorted(r for r in carpeta.rglob("*")
                  if r.is_file() and not r.name.startswith(".")
                  and HISTORICO not in r.relative_to(carpeta).parts)


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    for clave, dataset in DATASETS.items():
        ficheros = []
        for ruta in ficheros_dataset(raw, clave):
            rel = manifiesto.rel(ruta)
            info = manifiesto.get(rel)
            anio = info.get("anio", ruta.parent.name)
            ficheros.append((ruta, rel, {
                "_fuente": info.get("url"), "_dataset": clave, "_titulo": info.get("titulo"),
                "_anio": None if anio is None else str(anio), "_trimestre": info.get("trimestre"),
                "_periodo": info.get("periodo"), "_unidad": dataset["unidad"], "_archivo_origen": rel}))
        destino = salida / f"{clave}.parquet"
        if ficheros or destino.exists():
            print(f"  {clave}: {len(ficheros)} ficheros")
            construir_parquet(destino, ficheros, raw, manifiesto, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores de Castilla-La Mancha "
                                                 "(Junta, SESCAM, sector público y UCLM)")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--desde", type=int, default=None, help="primer año que se consulta (por defecto todos)")
    parser.add_argument("--hasta", type=int, default=None, help="último año que se consulta (por defecto todos)")
    parser.add_argument("--fuente", choices=("jccm", "uclm"), action="append",
                        help="solo esta fuente (se puede repetir; por defecto las dos)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar los Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años antiguos ya descargados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    fuentes = args.fuente or ["jccm", "uclm"]
    filtro = Filtro(args.desde, args.hasta)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Fuentes: {', '.join(fuentes)}  Años: {args.desde or 'todos'}-{args.hasta or 'todos'}\n"
          f"Destino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        if "jccm" in fuentes:
            descargar_jccm(raw, manifiesto, resumen, filtro, args.comprobar_todo)
        if "uclm" in fuentes:
            descargar_uclm(raw, manifiesto, resumen, filtro, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
