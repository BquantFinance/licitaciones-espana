#!/usr/bin/env python3
"""
=============================================================================
CONTRATOS MENORES DE AYUNTAMIENTOS PUBLICADOS FUERA DE LA PLACSP
=============================================================================
Solo ~30 % de los municipios carga sus contratos menores en el feed 1143 de la
Plataforma de Contratación del Sector Público. Este script descarga los que
publican en su propio portal algunos ayuntamientos grandes, tal cual (con todas
sus versiones), y genera un Parquet por municipio con todas las filas y
columnas publicadas, como texto.

Ejecutar:  python scripts/municipios_menores.py [--salida DIR] [--municipio M [M ...]]
           [--solo-procesar] [--comprobar-todo]

Municipios (clave de --municipio, código INE):
    gijon                Gijón (33024)
    vigo                 Vigo (36057)
    valladolid           Valladolid (47186)
    fuenlabrada          Fuenlabrada (28058)
    leganes              Leganés (28074)
    malaga               Málaga (29067)
    cordoba              Córdoba (14021)
    santa_cruz_tenerife  Santa Cruz de Tenerife (38038)

Salida (por defecto <repo>/municipios_menores/):
    raw/<municipio>/...               ficheros originales con su nombre publicado
    raw/<municipio>/_inventario.json  todo lo que enlaza el portal (también PDF y demás)
    raw/.../_historico/               versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json              URL, periodo, fecha de descarga y última comprobación de
                                      cada fichero y si el portal lo sigue publicando
    raw/_fallos_origen.json           ficheros que no se pueden bajar: fallo, primer y último
                                      intento e intentos seguidos (versiones en raw/_historico/)
    raw/descarga_log.txt              resumen de cada ejecución (se añade al final)
    <municipio>_menores.parquet       todas las filas de todos los ficheros (texto)
    _historico/                       versiones anteriores de los Parquet

Columnas: las publicadas, con el nombre que trae cada fichero (no se unifican
entre municipios ni entre años; eso es otro paso), y las añadidas: _fuente
(URL), _municipio, _codigo_ine, _anio, _trimestre, _mes (periodo que el portal
asigna al fichero), _titulo (texto del enlace o nombre del recurso),
_archivo_origen (ruta en raw/), _hoja, _titulo_tabla (texto de las filas de
encima de la cabecera: título, entidad, periodo...), _fecha_descarga y, de
comun/historico.py, _primera_descarga, _ultima_descarga y _en_ultima_descarga.
_trimestre es "1".."4" o un tramo acumulado ("1-3"); _mes, "01".."12" o un tramo
("07-08"); _anio, un año o un tramo ("2021-2023") si el fichero abarca varios.
Un registro que el portal retira o modifica NO desaparece: sigue en el Parquet
con _en_ultima_descarga=False (control del sesgo del superviviente).

Qué se descarga:
- De cada municipio se lee la lista de ficheros que publica (página, CKAN o
  URL fija) y se bajan los que son tablas (CSV, JSON, XLS, XLSX, ODS). Los PDF,
  DOCX, ODT y ZIP de PDF no se extraen: se anotan en el inventario y en el
  resumen (NO ESTRUCTURADOS). Si un conjunto se publica en varios formatos se
  baja uno (XLSX > XLS > ODS > CSV > JSON).
- Se completan los que faltan y se vuelven a pedir los del año en curso y el
  anterior (y los que cambian de URL); los más antiguos solo con
  --comprobar-todo. Todo pasa por comun.historico.guardar_version: si no
  cambia no se toca y si cambia la copia anterior queda en _historico/.
- Lo que la lista deja de enlazar queda como retirado (sus filas se
  conservan), pero solo si la lista se ha podido leer entera y enlaza algún
  fichero: si el portal falla no se retira nada y el script acaba con código 1.
  Si vuelve a enlazarse se vuelve a pedir (aunque sea de un año cerrado) y sus
  filas vuelven a _en_ultima_descarga=True. En Fuenlabrada, cuya lista pierde
  entradas, no salir en ella no basta: se vuelve a pedir la URL y solo un
  404/410 lo retira. Un fichero enlazado que no se puede bajar (4xx, HTML en
  vez de la tabla) es un error y su copia anterior se conserva; uno enlazado
  dos veces (dos conjuntos del CKAN con la misma URL) se baja una sola vez.
- Errores de origen permanentes (raw/_fallos_origen.json, RegistroFallos): un
  fichero que falla igual (el mismo 404/410 de un recurso que se sigue
  enlazando, o la misma respuesta que no es la tabla, con la misma URL) en
  todos sus intentos desde hace al menos 20 horas (dos como mínimo) se avisa
  en el resumen (ERRORES DE ORIGEN CONOCIDOS) y se sigue pidiendo, pero no da
  código 1: así el semanal no falla siempre por un enlace roto del portal.
  La primera vez, si cambia el fallo o la URL, y con fallos pasajeros (red,
  429, 5xx, descargas cortadas, otros 4xx) el script sale con código 1,
  aunque se repitan. Si el fichero vuelve a bajarse sale del registro
  (RECUPERADOS). No se retira nada por un fallo.

Lectura (todo como texto, sin convertir nada): CSV con comun.lectura_csv
(detecta codificación y separador; las filas de título de encima de la
cabecera van a _titulo_tabla), JSON con los números tal cual vienen escritos,
Excel (XLSX con openpyxl, deshaciendo el escape _x000D_ de OOXML; XLS con
xlrd) y ODS (content.xml: párrafos de una celda separados por saltos de
línea, que el lector de pandas pierde; comprobado contra el XLSX del mismo
trimestre de Málaga: iguales las 2.376 celdas). Se leen todas las hojas
(columna _hoja), salvo en Valladolid (ver abajo). La cabecera es la primera
fila con al menos el 60 % de las celdas llenas, o la anterior si esa trae
datos y la anterior son solo rótulos (cabeceras sobre celdas combinadas); una
hoja cuya primera fila es mitad o más números, fechas o NIF no tiene cabecera
(columnas columna_1…). Así ningún registro acaba de nombre de columna.

FUENTES
-------
Verificado en vivo el 2026-09-27 (confianza A: código HTTP, formato y filas):
  - Gijón: https://opendata.gijon.es/descargar.php?id=725&tipo=JSON (conjunto
    "Contratos menores adjudicados", actualización diaria; también CSV, XML,
    XLS: el CSV lleva los decimales con coma sin comillas y no se puede
    separar). Un solo fichero con 2018-2026: 63.978 contratos, 19 columnas.
    Ayuntamiento, FMC, Divertia, EMTUSA, PDM, FMSS, Promoción Empresarial,
    EMA y EMVISA. cif_adjudicatario enmascarado en personas físicas
    ("**35**63**", ~21 %). _anio = columna ejercicio.
  - Vigo: https://datos.vigo.org/data/sector-publico/contratos-menores-{AA}.csv
    (AA = 19..26; 2017 y 2018 dan 404). Catálogo CKAN datos-ckan.vigo.org
    (contratos_menores-AA, también JSON y XLS; es opcional: el 2026-09-28 no
    respondía y se sigue con las URL conocidas tras 2 intentos). 9 columnas
    (8 en 2019, sin expediente), sin NIF: 956-2.067 filas al año.
  - Valladolid: https://www.valladolid.gob.es/es/perfil-contratante/
    contratos-menores-volumen-contratacion-tipo-procedimiento: una página por
    año (ano-2017..ano-2026) y subpáginas por entidad. Ayuntamiento: XLSX del
    sistema contable SICALWIN con la hoja OPERACIONES SICALWIN (una fila por
    operación contable AD/ADO/D/O de todos los procedimientos; columna
    PROCEDIMIENTO), sin NIF salvo 2018 (columna Tercero). OJO: los ficheros de
    cada año son ACUMULADOS (1T, 1T-2T, 1T-3T, año): _trimestre "1", "1-2",
    "1-3", "1-4"; para contar hay que quedarse con el último de cada año. En
    2018 un solo fichero anual con la hoja DATOS MENORES, que repite las
    operaciones de contratación menor de OPERACIONES SICALWIN. 2017: una hoja
    por área (NOMBRE DE TERCERO, CONCEPTO, IMPORTE). Solo se cargan esas hojas
    de registros: las tablas dinámicas y de totales, las listas de códigos y
    los ficheros "Modelo procedimiento"/"Resumen anual" (agregados) quedan en
    el original y se anotan en los avisos. La cabecera de las hojas de
    registros es su primera fila (medido en los 40 ficheros); las hojas sin
    cabecera son la lista de códigos de área (Hoja1, Hoja3 en 2018: '01 | 01.
    Alcaldía', a veces tras una fila vacía) y los totales por área de 2017
    (TOTALES), que no se cargan: el aviso va en la lista de hojas que no se
    cargan ("Hoja1 (140 filas, sin fila de cabecera)"). Al pie de OPERACIONES
    SICALWIN de 2018 y 2025 hay una fila de total (solo Importe): se conserva,
    así se publica. Fundaciones de Cultura y Deportes (2024-2026): solo PDF,
    DOCX y un ZIP de PDF (no se extraen).
  - Fuenlabrada: https://transparencia.ayto-fuenlabrada.es/contratos/menores/
    (y /page/N/, 25 por página; archivo de la taxonomía grupo_contratos sin
    API REST y que ignora orderby/order): pagina por fecha y las entradas con
    la misma fecha cambian de orden entre consultas, así que en una lectura
    faltan unas y se repiten otras (el 2026-09-28, 96 únicas de 99 filas); lo
    que falta se baja en otra ejecución. Tabla con fecha, nombre y enlace; 99
    ficheros 2015-2026 (XLS, XLSX y ODS; hasta 2019 uno por organismo:
    Ayuntamiento, CIFE, IMLSP, OTAF, PMC, PMD, FUMECO) con CIF. Una hoja por
    organismo con una fila de
    título (_titulo_tabla) y filas de subtotal "Total <adjudicatario>"
    intercaladas (así se publican). La lista incluye también encargos a medios
    propios, basados en acuerdo marco y contratos Next Generation 2021-2025
    (_titulo lo indica). En algunas hojas de 2019 (p.ej. Sumario de
    AYTO-1T-2019.ods; avisos de columnas sin nombre en PMC-1T, PMC-3T y el 4T)
    hay debajo una segunda tabla con otra cabecera (formato del Tribunal de
    Cuentas: NIFENTIDAD, OBJETO...): su fila de cabecera queda como una fila
    más y sus columnas de más como 'Unnamed: 9'… (no se pierde nada, pero hay
    que separarla al unificar).
  - Leganés: https://www.leganes.org/web/transparencia/contratos-menores
    (Liferay; da 403 al User-Agent de curl, no al del script). XLSX mensual o
    trimestral con NIF 2019-2026 (informe con ENTIDAD, AÑO y periodo encima de
    la cabecera: _titulo_tabla), XLS mensual 2016-2017, CSV de 2015. En PDF:
    feb-sep 2016, ene-jun 2017, 2018 y noviembre de 2023. El enlace "Menores
    Septiembre 2023" apunta al PDF de septiembre de 2016 y "Menores agosto
    2026" (/documents/131847/...) devuelve la portada del sitio: redirige al
    acceso de Liferay, el documento no es público (comprobado el 2026-09-29;
    error de origen permanente).
  - Málaga: CKAN https://datosabiertos.malaga.eu (package_search q=menores):
    77 conjuntos "Contratos menores N trimestre AAAA - Ayuntamiento de Málaga"
    (2016-2026) y "- CEMI" (2016-2024), cada uno con PDF y XLSX/XLS (y ODS
    desde 2025): se baja la hoja de cálculo. Ayuntamiento: CIF solo en 3T
    2017, 1T 2018, 2T-3T 2021 y desde 2024 (el resto, solo el nombre del
    tercero); CEMI: NIF desde el 2T de 2018 (sus ficheros traen además una
    hoja Hoja2 con la lista de tipos de contrato). El XLSX del 4T de 2020 del
    Ayuntamiento da 404 (error de origen permanente; ese trimestre solo está
    en PDF) y el conjunto de CEMI del 2T de 2020 enlaza el XLSX del 1T (se
    baja una vez). Unos 2/3 de los menores de 2025 del Ayuntamiento también
    están en el feed 1143 de la PLACSP (mismo NIF e importe).
  - Córdoba: CKAN https://datosabiertos.cordoba.es (q=menores): conjuntos
    "Contratos menores" (CSV 2021-2024 y XLS 2021-2023, que se solapan, y
    XLSX 2023 de lo publicado en PLACSP) y "Contratación administrativa - Contratos menores"
    (ODS de 1T y 3T de 2024 declarados XLS, XLS de 2023 y de 2T 2026, con
    NIF). En PDF: trimestres de 2021-2025 y 1T 2026, y las "tomas de
    conocimiento" de la Junta de Gobierno.
  - Santa Cruz de Tenerife: https://www.santacruzdetenerife.es/gobiernoabierto/
    transparencia/contratos: CONTRATOS_MENORES_2023_CSV.csv y
    CONTRATOS_MENORES_2024_CSV.csv (cp1252, una fila de título encima de la
    cabecera, sin adjudicatario ni NIF). En PDF: 2016-2022, y 2023-2024
    repetidos; 2025 en PDF, DOCX y ODT (no se extraen). Los resúmenes
    (Resumen_menores_AAAA_CSV.csv, INFORMACION_CONTRATOS_MENORES_2025.*: nº de
    contratos e importe total) no son registros y no se cargan.
Otras fuentes conocidas NO incorporadas (inventario del 2026-09-27): Móstoles
(PDF mensual 2018-), Alcalá de Henares (XLSX 2024 pasado de un informe
contable en PDF), A Coruña (XLS/ODS trimestral; 403 por ASN desde la nube).
VERIFICAR EN VIVO:
  - Que las páginas mantienen su estructura (si una lista sale vacía es un
    error y no se retira nada).
  - Leganés: si el enlace de agosto de 2026 se corrige.
=============================================================================
"""

import argparse
import codecs
import collections
import csv
import datetime as dt
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
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

URL_GIJON = "https://opendata.gijon.es/descargar.php?id=725&tipo=JSON"
URL_VIGO = "https://datos.vigo.org/data/sector-publico/contratos-menores-{aa}.csv"
URL_CKAN_VIGO = "https://datos-ckan.vigo.org/api/3/action/package_search"
VIGO_DESDE = 2019                      # 2017 y 2018 dan 404
VIGO_CONFIRMADOS = range(2019, 2026)   # años publicados: un 404 es un error
URL_VALLADOLID = ("https://www.valladolid.gob.es/es/perfil-contratante/"
                  "contratos-menores-volumen-contratacion-tipo-procedimiento")
URL_FUENLABRADA = "https://transparencia.ayto-fuenlabrada.es/contratos/menores/"
URL_LEGANES = "https://www.leganes.org/web/transparencia/contratos-menores"
URL_CKAN_MALAGA = "https://datosabiertos.malaga.eu/api/3/action/package_search"
URL_CKAN_CORDOBA = "https://datosabiertos.cordoba.es/api/3/action/package_search"
URL_SANTA_CRUZ = "https://www.santacruzdetenerife.es/gobiernoabierto/transparencia/contratos"

CONSULTA_CKAN = "menores"
FILAS_CKAN = 100
MAX_PAGINAS = 100                      # tope de páginas de un listado paginado

MUNICIPIOS = {
    "gijon": {"nombre": "Gijón", "codigo_ine": "33024",
              "descripcion": "JSON único 2018- (Ayuntamiento y entes municipales), con CIF"},
    "vigo": {"nombre": "Vigo", "codigo_ine": "36057", "descripcion": "CSV anual 2019-, sin NIF"},
    "valladolid": {"nombre": "Valladolid", "codigo_ine": "47186",
                   "descripcion": "XLSX acumulados del sistema contable 2017-, sin NIF (salvo 2018)"},
    "fuenlabrada": {"nombre": "Fuenlabrada", "codigo_ine": "28058",
                    "descripcion": "XLS/XLSX/ODS trimestral 2015-, con CIF",
                    # La lista pagina por fecha y las entradas con la misma fecha cambian de
                    # orden entre consultas: en cada lectura pueden faltar algunas
                    "lista_incompleta": True},
    "leganes": {"nombre": "Leganés", "codigo_ine": "28074",
                "descripcion": "XLSX mensual o trimestral 2015-, con NIF"},
    "malaga": {"nombre": "Málaga", "codigo_ine": "29067",
               "descripcion": "CKAN: XLSX/XLS/ODS trimestral 2016- (Ayuntamiento y CEMI)"},
    "cordoba": {"nombre": "Córdoba", "codigo_ine": "14021",
                "descripcion": "CKAN: CSV/XLS/XLSX/ODS 2021- con NIF (el resto en PDF)"},
    "santa_cruz_tenerife": {"nombre": "Santa Cruz de Tenerife", "codigo_ine": "38038",
                            "descripcion": "CSV anual 2023-2024, sin adjudicatario"},
}

SALIDA = Path(__file__).resolve().parent.parent / "municipios_menores"
TITULO = "CONTRATOS MENORES DE AYUNTAMIENTOS (FUERA DE LA PLACSP)"


# ============================================================================
# UTILIDADES COMUNES (bloque de ccaa_murcia.py adaptado: ODS, JSON con los
# números tal cual, filas de título de CSV y Excel en _titulo_tabla, ficheros
# de un municipio a partir del manifiesto)
# ============================================================================

CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 60
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
PAUSA = 0.3                  # entre peticiones, para no cargar el portal
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")
PARAMETROS_VOLATILES = {"t"}             # ?t=<marca de tiempo> de Liferay (Leganés)
INTENTOS_OPCIONAL = 2                    # catálogos opcionales (CKAN de Vigo)
# Errores de origen permanentes (RegistroFallos): los fallos que pueden serlo (404/410 o una
# respuesta que no es la tabla) y lo que tiene que pasar entre el primer intento fallido y el actual
FALLOS_ORIGEN = "_fallos_origen.json"
ESTADOS_PERMANENTES = ("no_existe", "invalido")
SEPARACION_PERMANENTE = dt.timedelta(hours=20)

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL u otra hoja sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_municipio", "_codigo_ine", "_anio", "_trimestre", "_mes", "_titulo",
                    "_archivo_origen", "_hoja", "_titulo_tabla", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")
# Metadatos que pueden cambiar de una fila a otra del mismo fichero
METADATOS_POR_FILA = ("_anio", "_hoja", "_titulo_tabla", "_fecha_descarga")


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


def _espera(intento, respuesta=None):
    """Backoff exponencial, o lo que pida el servidor en Retry-After."""
    valor = (getattr(respuesta, "headers", None) or {}).get("Retry-After") if respuesta is not None else None
    if valor and str(valor).strip().isdigit():
        return min(float(valor), ESPERA_MAXIMA)
    return min(ESPERA_BASE * 2 ** (intento - 1), ESPERA_MAXIMA)


def _pedir(url, params=None, intentos=INTENTOS):
    """GET con reintentos (red, 429, 5xx); un 4xx lanza ErrorPortal con el código."""
    detalle = ""
    for intento in range(1, intentos + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_API)
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
        if intento < intentos:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {intentos} intentos)")


def pedir_json(url, params=None, intentos=INTENTOS):
    """GET a una API JSON con reintentos; una respuesta que no es JSON también se reintenta."""
    detalle = ""
    for intento in range(1, intentos + 1):
        respuesta = _pedir(url, params, intentos)
        try:
            return respuesta.json()
        except ValueError as e:
            detalle = f"respuesta no JSON ({str(e)[:80]})"
        if intento < intentos:
            time.sleep(_espera(intento))
    raise ErrorPortal(f"{detalle} (tras {intentos} intentos)")


def pedir_texto(url, params=None):
    """GET de una página HTML con reintentos; un 4xx lanza ErrorPortal."""
    return _pedir(url, params).text


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes ('zip' incluye XLSX y ODS)."""
    if cabeza.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith(b"%PDF"):
        return "pdf"
    if cabeza.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "csv"                                      # texto UTF-16
    texto = cabeza.lstrip(b"\xef\xbb\xbf").lstrip()
    if texto[:1] in (b"{", b"["):
        return "json"
    if texto[:1] == b"<":
        return "html" if b"html" in texto[:1024].lower() else "xml"
    return "csv"


def formato_fichero(ruta):
    """Formato real de un fichero: el de sus primeros bytes y, si es un ZIP, si
    es una hoja de cálculo (XLSX, ODS), un documento (DOCX, ODT) u otro ZIP."""
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato != "zip":
        return formato
    try:
        with zipfile.ZipFile(ruta) as archivo:
            nombres = archivo.namelist()
            if any(n.startswith("xl/") for n in nombres):
                return "xlsx"
            if any(n.startswith("word/") for n in nombres):
                return "docx"
            if "mimetype" in nombres:
                tipo = archivo.read("mimetype")
                if b"opendocument.spreadsheet" in tipo:
                    return "ods"
                if b"opendocument.text" in tipo:
                    return "odt"
    except (zipfile.BadZipFile, OSError, KeyError):
        pass
    return "zip"


HOJAS_CALCULO = ("xlsx", "xls", "ods")
TABULARES = ("csv", "json") + HOJAS_CALCULO


def _integridad(ruta, formato):
    """Motivo por el que un XLSX/ODS descargado está incompleto o dañado, o None."""
    if formato not in ("xlsx", "ods"):
        return None
    try:
        with zipfile.ZipFile(ruta) as archivo:
            malo = archivo.testzip()
        return f"miembro dañado: {malo}" if malo else None
    except Exception as e:
        return f"{type(e).__name__}: {str(e)[:150]}"


def validar_contenido(ruta, tipo):
    """(motivo, reintentar): por qué la descarga no es la tabla esperada (una
    página HTML servida con 200, un PDF, un XLSX cortado...), o (None, False).
    Una hoja de cálculo con otra extensión se acepta (se lee por su contenido);
    un JSON o un CSV tienen que ser JSON o CSV."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía", False
    formato = formato_fichero(ruta)
    if formato not in TABULARES:           # p.ej. una página HTML de error servida con 200
        return f"la respuesta es {formato.upper()}, no una tabla", False
    if tipo in HOJAS_CALCULO and formato not in HOJAS_CALCULO or tipo in ("csv", "json") and formato != tipo:
        return f"se esperaba {tipo.upper()} y llegó {formato.upper()}", False
    danado = _integridad(ruta, formato)
    if danado:
        return f"fichero {formato.upper()} incompleto o dañado ({danado})", True
    return None, False


def descargar(url, destino, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que es la tabla esperada (no una página
    HTML de error ni un fichero cortado) y lo entrega a guardar_version(): si
    el contenido no cambió no se toca nada y si cambió la copia previa pasa a
    _historico/. Reintenta con backoff los fallos de red, 429, 5xx y las
    descargas incompletas.
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es la tabla esperada) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    detalle = ""
    try:
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                with requests.get(url, headers=CABECERAS, timeout=TIMEOUT_DESCARGA, stream=True) as respuesta:
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
    (comprobado), si el portal lo sigue publicando y su periodo (metadatos)."""

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

    def de_municipio(self, clave):
        return {rel: e for rel, e in self.datos.items() if rel.startswith(clave + "/")}

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


def _instante(texto):
    """Instante de una fecha escrita con iso() ('2026-09-27T14:43:00Z')."""
    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class RegistroFallos:
    """raw/_fallos_origen.json: los ficheros que el portal enlaza (o que las fuentes dan por
    publicados) y no se han podido bajar, con el fallo (estado y detalle de descargar()), la URL, el
    primer y el último intento fallido y cuántos intentos seguidos han fallado igual.

    ERROR DE ORIGEN PERMANENTE: el mismo fichero falla igual (mismo estado y detalle y la misma URL
    sin parámetros volátiles) en todos sus intentos desde hace al menos SEPARACION_PERMANENTE (dos
    intentos como mínimo), y el fallo es del origen: 404/410 de un recurso que se sigue enlazando o
    una respuesta que no es la tabla (ESTADOS_PERMANENTES; p.ej. Leganés sirve la portada en lugar
    del XLSX de agosto de 2026 y el XLSX del 4T-2020 de Málaga da 404). Se avisa en el log y en el
    resumen (ERRORES DE ORIGEN CONOCIDOS), se vuelve a pedir en cada ejecución y no da código 1.
    Dan código 1 siempre: un fallo nuevo (la primera vez), uno que cambia (otro estado, otro detalle
    u otra URL), uno que se repite en menos de SEPARACION_PERMANENTE (un 404 de unos minutos no es
    permanente) y los pasajeros (red, 429, 5xx, descarga cortada u otros 4xx), aunque se repitan.
    Así el registro no esconde ningún error nuevo.
    Cuando el fichero vuelve a bajarse sale del registro (RECUPERADOS); si vuelve a fallar, es un
    fallo nuevo. El registro no retira nada: un fichero enlazado que falla conserva su copia y sus
    filas. Se guarda con guardar_version: cada versión anterior queda en raw/_historico/."""

    def __init__(self, raw, momento):
        self.raw = Path(raw)
        self.ruta = self.raw / FALLOS_ORIGEN
        self.momento = iso(momento)
        self.aviso = None
        try:
            self.datos = json.loads(self.ruta.read_text(encoding="utf-8"))
            if not isinstance(self.datos, dict):
                raise ValueError("no es un objeto JSON")
        except FileNotFoundError:
            self.datos = {}
        except ValueError as e:
            # Sin registro, todo fallo es nuevo y da código 1: nunca esconde nada
            self.datos = {}
            self.aviso = (f"{FALLOS_ORIGEN} ilegible ({str(e)[:100]}): los fallos de esta ejecución cuentan "
                          "como nuevos; la copia ilegible queda en _historico/")

    def fallo(self, municipio, rel, url, estado, detalle):
        """Anota que `rel` no se ha podido bajar en esta ejecución y devuelve su entrada."""
        firma = f"{estado}: {detalle}"
        clave_url = _clave_url(url)
        previa = self.datos.get(rel) or {}
        if previa.get("firma") == firma and previa.get("clave_url") == clave_url:
            # Cada ruta se pide una vez por ejecución (_sin_duplicados, _asignar_rutas)
            entrada = dict(previa, url=url, ultima=self.momento, veces=int(previa.get("veces") or 1) + 1)
        else:
            entrada = {"municipio": municipio, "url": url, "clave_url": clave_url, "estado": estado,
                       "detalle": detalle, "firma": firma, "primera": self.momento, "ultima": self.momento,
                       "veces": 1}
        self.datos[rel] = entrada
        return entrada

    @staticmethod
    def permanente(entrada):
        """Si la entrada es un error de origen permanente (ver la clase)."""
        try:
            separacion = _instante(entrada["ultima"]) - _instante(entrada["primera"])
            veces = int(entrada.get("veces") or 0)
        except (KeyError, TypeError, ValueError):
            return False
        return entrada.get("estado") in ESTADOS_PERMANENTES and veces >= 2 and separacion >= SEPARACION_PERMANENTE

    def resuelto(self, rel):
        """`rel` se ha bajado, se ha retirado o ya no es un fallo: sale del registro. Devuelve su
        entrada (None si no estaba)."""
        return self.datos.pop(rel, None)

    def olvidar(self, municipio, rels):
        """Fallos de `municipio` cuyo fichero el portal ya no enlaza (su lista se ha leído entera y
        no está en `rels`): no se vuelven a pedir y salen del registro. Devuelve [(rel, entrada)]."""
        fuera = [rel for rel in self.datos if rel.startswith(municipio + "/") and rel not in rels]
        return [(rel, self.datos.pop(rel)) for rel in fuera]

    def guardar(self):
        """Escribe el registro (sin él y sin fallos no se crea el fichero)."""
        if not self.datos and not self.ruta.exists():
            return None
        return guardar_json(self.ruta, self.datos)


class Resumen:
    """Lo descargado, lo que no existe, lo retirado, lo que no es una tabla y lo que falló: los
    errores (código 1) y los errores de origen conocidos (RegistroFallos: no cambian el código)."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = collections.Counter()
        self.no_publicados = {}
        self.retirados = []
        self.no_estructurados = {}
        self.avisos = []
        self.fallidos = []
        self.conocidos = []
        self.recuperados = []
        self.parquets = []

    def descarga(self, municipio, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios[municipio] += 1
        else:
            self.descargados.append(f"{etiqueta} ({estado})")

    def no_publicado(self, grupo, valor):
        self.no_publicados.setdefault(grupo, []).append(valor)

    @staticmethod
    def _rangos(valores):
        if not all(isinstance(v, int) for v in valores):
            return ", ".join(str(v) for v in valores)
        valores, tramos = sorted(set(valores)), []
        for v in valores:
            if tramos and v == tramos[-1][1] + 1:
                tramos[-1][1] = v
            else:
                tramos.append([v, v])
        return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in tramos)

    @classmethod
    def _no_estructurados(cls, municipio, lista):
        motivos = collections.Counter(r.motivo for r in lista)
        anios = sorted({a for r in lista for a in _anios(r.anio)})
        return (f"{municipio}: {len(lista)} ficheros (" + ", ".join(f"{m}: {n}" for m, n in sorted(motivos.items()))
                + f"); años {cls._rangos(anios) if anios else '?'}")

    def texto(self):
        lineas = ["=" * 70]
        if self.fallidos:
            lineas.append(f"⚠️ {self.titulo}: COMPLETADO CON ERRORES ({len(self.fallidos)})")
        elif self.conocidos:
            lineas.append(f"✅ {self.titulo}: COMPLETADO (con {len(self.conocidos)} errores de origen conocidos)")
        else:
            lineas.append(f"✅ {self.titulo}: COMPLETADO")
        lineas += ["=" * 70, f"Inicio: {iso(self.inicio)}  Fin: {iso(ahora())}"]

        def bloque(titulo, elementos):
            if elementos:
                lineas.append(f"\n{titulo} ({len(elementos)}):")
                lineas.extend(f"   - {e}" for e in elementos)

        bloque("DESCARGADOS (versión nueva)", self.descargados)
        bloque("SIN CAMBIOS O YA DESCARGADOS (--comprobar-todo vuelve a pedir los años cerrados)",
               [f"{m}: {n} ficheros" for m, n in self.sin_cambios.items()])
        if self.no_publicados:
            lineas.append("\nNO PUBLICADOS EN EL PORTAL (404):")
            lineas.extend(f"   - {g}: {self._rangos(v)}" for g, v in self.no_publicados.items())
        bloque("RETIRADOS POR EL PORTAL (se conservan con _en_ultima_descarga=False)", self.retirados)
        bloque("NO SE EXTRAEN (PDF y otros formatos, o no son registros; lista en raw/<municipio>/_inventario.json)",
               [self._no_estructurados(m, lista) for m, lista in self.no_estructurados.items()])
        bloque("PARQUET", [f"{n}: {f:,} filas x {c} columnas ({r:,} ya no publicadas)"
                           for n, f, c, r in self.parquets])
        bloque("AVISOS", self.avisos)
        bloque("RECUPERADOS (fallaban y vuelven a bajarse; salen de raw/_fallos_origen.json)", self.recuperados)
        bloque("ERRORES DE ORIGEN CONOCIDOS (fallan igual en intentos seguidos: se reintentan en cada ejecución "
               "y no cambian el código de salida; raw/_fallos_origen.json)", self.conocidos)
        bloque("ERRORES - vuelve a ejecutar el script para reintentarlos", self.fallidos)
        return "\n".join(lineas)

    def cerrar(self, raw):
        texto = self.texto()
        print("\n" + texto)
        raw = Path(raw)
        raw.mkdir(parents=True, exist_ok=True)
        with open(raw / "descarga_log.txt", "a", encoding="utf-8") as f:
            f.write(texto + "\n\n")
        return 1 if self.fallidos else 0


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


def _lineas(texto):
    return re.split(r"\r\n|\r|\n", texto)


def _detectar_separador(texto):
    """Separador más frecuente (fuera de comillas) en las primeras líneas con
    texto (la primera puede ser un título sin separadores)."""
    cuentas = dict.fromkeys([";", ",", "\t", "|"], 0)
    for linea in [x for x in _lineas(texto[:1 << 16]) if x.strip()][:20]:
        entre_comillas = False
        for caracter in linea:
            if caracter == '"':
                entre_comillas = not entre_comillas
            elif not entre_comillas and caracter in cuentas:
                cuentas[caracter] += 1
    mejor = max(cuentas, key=cuentas.get)
    return mejor if cuentas[mejor] else ","


def _ancho_habitual(texto, sep):
    """Nº de campos más frecuente en los primeros registros (el de la cabecera
    y los datos, aunque encima haya una fila de título más corta)."""
    lector = csv.reader(io.StringIO(texto[:1 << 18], newline=""), delimiter=sep)
    anchos = collections.Counter(len(fila) for _, fila in zip(range(60), lector) if fila)
    return anchos.most_common(1)[0][0] if anchos else None


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


def _es_dato(valor):
    return valor is not None and bool(PATRON_DATO.fullmatch(str(valor).strip()))


def _parece_registro(fila):
    """¿La fila detectada como cabecera son datos? Lo son si al menos la mitad
    de sus celdas con valor son números, fechas o NIF (una cabecera son
    rótulos): ['19', 'ALCALDÍA'] es la primera fila de una lista de códigos."""
    valores = [str(v).strip() for v in fila if v is not None and str(v).strip()]
    return bool(valores) and sum(bool(PATRON_DATO.fullmatch(v)) for v in valores) * 2 >= len(valores)


def _etiqueta(nombre, hoja):
    """Cómo se nombra una tabla en los avisos: el fichero y, si es de una hoja, la hoja."""
    return nombre if hoja is None else f"{nombre} [{hoja}]"


AVISO_SIN_CABECERA = "sin fila de cabecera"


def _tabla(filas, nombre, hoja, avisos):
    """Tabla de una hoja o de un CSV: detecta la fila de cabecera (el texto de
    las filas de encima, título, entidad, periodo..., va a la columna
    _titulo_tabla) y conserva todas las filas con algún valor. Devuelve un
    DataFrame (puede no tener filas) o None si no hay ningún valor."""
    filas = [list(f) for f in filas if any(v is not None for v in f)]
    if not filas:
        return None
    etiqueta = _etiqueta(nombre, hoja)
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    # Cabecera con huecos (rótulos sobre celdas combinadas: 'Expte.', -, 'Descripción',
    # -, -, 'Total'...) que no llega al umbral: si la fila elegida trae datos y la
    # anterior son solo rótulos, la cabecera es la anterior (si no, el primer
    # registro acabaría de cabecera)
    if (pos > 0 and any(_es_dato(v) for v in filas[pos]) and llenas[pos - 1] >= 2
            and not any(_es_dato(v) for v in filas[pos - 1])):
        pos -= 1
    titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
    ancho = max(len(f) for f in filas)
    cabecera = filas[pos] + [None] * (ancho - len(filas[pos]))
    if _parece_registro(cabecera):
        # Tabla sin cabecera (p.ej. una lista auxiliar de códigos y NIF): su
        # primera fila es un registro, no los nombres de las columnas
        avisos.append(f"{etiqueta}: {AVISO_SIN_CABECERA} (la primera fila son datos: "
                      f"{' | '.join(str(v) for v in cabecera if v is not None)[:120]}); columnas columna_1…")
        cabecera = [f"columna_{i}" for i in range(1, ancho + 1)]
        pos -= 1
    datos = [f + [None] * (ancho - len(f)) for f in filas[pos + 1:]]
    nombres = _nombres_columnas(cabecera)
    df = pd.DataFrame(datos, columns=nombres, dtype=object)
    # Columnas sin nombre y sin ningún valor: restos del rango usado o separadores finales
    vacias = [c for c, v in zip(nombres, cabecera) if v is None and df[c].isna().all()]
    df = df.drop(columns=vacias)
    sin_nombre = [c for c, v in zip(nombres, cabecera) if v is None and c not in vacias]
    if sin_nombre:
        avisos.append(f"{etiqueta}: {len(sin_nombre)} columnas con valores y sin nombre en la cabecera "
                      f"(se conservan como {', '.join(sin_nombre[:5])})")
    if hoja is not None:
        df["_hoja"] = hoja
    if titulo:
        df["_titulo_tabla"] = titulo
    return df


def leer_csv(ruta, nombre=None):
    """CSV como texto: solo el campo vacío es nulo ('NA', 'NULL'... se conservan)
    y no se pierde ninguna fila ni campo: una comilla literal al principio de
    un campo no se traga los registros siguientes (comun.lectura_csv) y los
    campos de más respecto a la cabecera van a columnas 'Unnamed: i'."""
    ruta = Path(ruta)
    nombre = nombre or ruta.name
    avisos = []
    with open(ruta, encoding=_detectar_codificacion(ruta), newline="") as f:
        texto = f.read()
    if texto.startswith("﻿"):
        texto = texto[1:]
    sep = _detectar_separador(texto)
    filas, literales = registros_csv(texto, sep, _ancho_habitual(texto, sep))
    if literales:
        avisos.append(f"{nombre}: {literales:,} comillas literales al principio de un campo "
                      "(se conservan en el texto; sin ellas se tragarían los registros siguientes)")
    lineas = sum(1 for linea in _lineas(texto) if linea != "")
    if lineas != len(filas):
        avisos.append(f"{nombre}: {len(filas):,} registros en {lineas:,} líneas "
                      "(campos entrecomillados con saltos de línea)")
    df = _tabla([[v if v != "" else None for v in fila] for fila in filas], nombre, None, avisos)
    return (pd.DataFrame() if df is None else df), avisos


_ESCAPE_OOXML = re.compile(r"_x([0-9A-Fa-f]{4})_")


def _celda_xlsx(valor):
    """Como _celda_texto, deshaciendo el escape de OOXML de los caracteres de
    control que openpyxl deja tal cual ('_x000D_' es un retorno de carro)."""
    if isinstance(valor, str) and "_x" in valor:
        valor = _ESCAPE_OOXML.sub(lambda m: chr(int(m.group(1), 16)), valor)
    return _celda_texto(valor)


def _leer_xlsx(ruta, nombre):
    import openpyxl

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
            filas = ([_celda_xlsx(v) for v in fila] for fila in hoja.iter_rows(values_only=True))
            df = _tabla(filas, nombre, hoja.title, avisos)
            if df is not None:
                partes.append((hoja.title, df))
    finally:
        libro.close()
        fichero.close()
    return partes, avisos


def _leer_xls(ruta, nombre):
    try:
        import xlrd
    except ImportError:
        raise RuntimeError("hace falta el paquete xlrd para leer .xls (pip install 'xlrd>=2.0.1')") from None

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

    avisos, partes = [], []
    libro = xlrd.open_workbook(str(ruta), on_demand=True)
    try:
        for hoja in libro.sheets():
            filas = ([texto(c, libro.datemode) for c in hoja.row(i)] for i in range(hoja.nrows))
            df = _tabla(filas, nombre, hoja.name, avisos)
            if df is not None:
                partes.append((hoja.name, df))
    finally:
        libro.release_resources()
    return partes, avisos


_TABLE = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
_OFFICE = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"
_TEXT = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"


def _texto_nodo_ods(nodo):
    """Texto de un nodo de texto de ODS tal como se ve: <text:s> son espacios,
    <text:tab> un tabulador y <text:line-break> un salto de línea."""
    if nodo.tag == _TEXT + "s":
        return " " * int(nodo.get(_TEXT + "c", "1"))
    if nodo.tag == _TEXT + "tab":
        return "\t"
    if nodo.tag == _TEXT + "line-break":
        return "\n"
    if nodo.tag == _OFFICE + "annotation":
        return ""
    return (nodo.text or "") + "".join(_texto_nodo_ods(hijo) + (hijo.tail or "") for hijo in nodo)


def _valor_ods(celda):
    """Valor de una celda de ODS como texto, igual que en XLSX: el número, la
    fecha o el booleano guardado (no el texto formateado) y, en las de texto,
    los párrafos separados por saltos de línea."""
    tipo = celda.get(_OFFICE + "value-type")
    if tipo in ("float", "percentage", "currency"):
        valor = celda.get(_OFFICE + "value")
        try:
            return _celda_texto(float(valor))
        except (TypeError, ValueError):
            return valor
    if tipo == "date":
        valor = celda.get(_OFFICE + "date-value")
        try:
            return _celda_texto(datetime.fromisoformat(valor))
        except (TypeError, ValueError):
            return valor
    if tipo == "time":
        valor = celda.get(_OFFICE + "time-value") or ""
        m = re.fullmatch(r"PT(\d+)H(\d+)M(\d+)(?:\.(\d+))?S", valor)
        if m and int(m.group(1)) < 24:
            return dt.time(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                           int((m.group(4) or "0")[:6].ljust(6, "0"))).isoformat()
        return valor or None
    if tipo == "boolean":
        valor = celda.get(_OFFICE + "boolean-value")
        return {"true": "True", "false": "False"}.get(valor, valor)
    if celda.get(_OFFICE + "string-value") is not None:
        return celda.get(_OFFICE + "string-value") or None
    texto = "\n".join(_texto_nodo_ods(p) for p in celda if p.tag in (_TEXT + "p", _TEXT + "h"))
    return texto if texto != "" else None


def _leer_ods(ruta, nombre):
    """Hojas de un ODS leyendo su content.xml (celdas y filas repetidas, celdas
    combinadas vacías). No se usa pandas: une los párrafos de una celda sin
    salto de línea y cambiaría el texto."""
    avisos, partes = [], []
    hoja, filas = None, []
    with zipfile.ZipFile(ruta) as archivo, archivo.open("content.xml") as contenido:
        for evento, elemento in ET.iterparse(contenido, events=("start", "end")):
            if evento == "start":
                if elemento.tag == _TABLE + "table":
                    hoja, filas = elemento.get(_TABLE + "name"), []
                continue
            if elemento.tag == _TABLE + "table-row":
                fila, vacias = [], 0
                for celda in elemento:
                    if celda.tag not in (_TABLE + "table-cell", _TABLE + "covered-table-cell"):
                        continue
                    repetir = int(celda.get(_TABLE + "number-columns-repeated", "1"))
                    valor = _valor_ods(celda) if celda.tag == _TABLE + "table-cell" else None
                    if valor is None:
                        vacias += repetir          # las vacías del final no se materializan
                        continue
                    fila.extend([None] * vacias + [valor] * repetir)
                    vacias = 0
                if fila:
                    filas.extend([fila] * int(elemento.get(_TABLE + "number-rows-repeated", "1")))
                elemento.clear()
            elif elemento.tag == _TABLE + "table":
                df = _tabla(filas, nombre, hoja, avisos)
                if df is not None:
                    partes.append((hoja, df))
                filas = []
                elemento.clear()
    return partes, avisos


def _buscar_registros(datos, profundidad=0):
    """Primera lista de objetos de un JSON (la raíz, result.records, contratos.contrato...)."""
    if isinstance(datos, list) and all(isinstance(x, dict) for x in datos):
        return datos
    if isinstance(datos, dict) and profundidad < 4:
        claves = ["records", "result", "results", "data", "items"]
        for clave in claves + [k for k in datos if k not in claves]:
            encontrado = _buscar_registros(datos.get(clave), profundidad + 1)
            if encontrado is not None:
                return encontrado
    return None


def _leer_json(ruta):
    """Registros de un JSON como texto; los números, tal como vienen escritos
    (60 -> '60', 1.10 -> '1.10': no pasan por float)."""
    with open(ruta, encoding=_detectar_codificacion(ruta)) as f:
        datos = json.loads(f.read(), parse_int=str, parse_float=str, parse_constant=str)
    registros = _buscar_registros(datos)
    if registros is None:
        raise ValueError("el JSON no contiene una lista de registros")

    def texto(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v if v != "" else None
        return json.dumps(v, ensure_ascii=False)

    columnas = list(dict.fromkeys(k for r in registros for k in r))
    return pd.DataFrame([[texto(r.get(c)) for c in columnas] for r in registros], columns=columnas, dtype=object)


def leer_tabla(ruta, nombre=None, hoja_valida=None):
    """Lee un fichero tabular (CSV, JSON, XLSX, XLS u ODS, según su contenido
    real) con todas sus filas y columnas como texto. hoja_valida(df): si se da,
    solo se cargan las hojas para las que devuelve True (las demás se anotan).
    Devuelve (DataFrame, avisos)."""
    ruta = Path(ruta)
    nombre = nombre or ruta.name
    formato = formato_fichero(ruta)
    if formato == "csv":
        return leer_csv(ruta, nombre)
    if formato == "json":
        return _leer_json(ruta), []
    if formato not in HOJAS_CALCULO:
        raise ValueError(f"formato {formato.upper()} no tabular")
    lector = {"xlsx": _leer_xlsx, "xls": _leer_xls, "ods": _leer_ods}[formato]
    partes, avisos = lector(ruta, nombre)
    cargadas, descartadas, sin_filas = [], [], []
    for hoja, df in partes:
        if not len(df):
            sin_filas.append(f"{hoja} ({' | '.join(str(c) for c in df.columns if not str(c).startswith('_'))[:80]})")
        elif hoja_valida is not None and not hoja_valida(df):
            # Una hoja que no se carga (lista de códigos, totales...) y que no tiene cabecera se dice
            # en su línea: suelto, el aviso parecería de las hojas de registros (Valladolid)
            prefijo = f"{_etiqueta(nombre, hoja)}: {AVISO_SIN_CABECERA} ("
            sin_cabecera = [a for a in avisos if a.startswith(prefijo)]
            avisos = [a for a in avisos if not a.startswith(prefijo)]
            descartadas.append(f"{hoja} ({len(df):,} filas{', ' + AVISO_SIN_CABECERA if sin_cabecera else ''})")
        else:
            cargadas.append(df)
    if sin_filas:
        avisos.append(f"{nombre}: hojas sin filas de datos: {'; '.join(sin_filas)}")
    if descartadas:
        avisos.append(f"{nombre}: no se cargan {len(descartadas)} hojas de resumen o auxiliares "
                      f"(quedan en el original): {', '.join(descartadas)}")
    if not cargadas:
        return pd.DataFrame(columns=["_hoja"]), avisos
    return pd.concat(cargadas, ignore_index=True, sort=False), avisos


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


def escribir_parquet(df, destino):
    """Parquet con todas las columnas como texto (_en_ultima_descarga booleana).
    Se escribe en un temporal y pasa por guardar_version: la versión anterior
    del Parquet queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df = ordenar_columnas(df).copy()
    campos = []
    for columna in df.columns:
        if columna == "_en_ultima_descarga":
            df[columna] = df[columna].astype(bool)
            campos.append(pa.field(columna, pa.bool_()))
        else:
            df[columna] = _como_texto(df[columna])
            campos.append(pa.field(str(columna), pa.string()))
    tabla = pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen, lector=leer_tabla):
    """Registros de un fichero crudo a lo largo de todas sus versiones.

    anterior: filas de este fichero en el Parquet anterior (o None). Solo se
    aplican las versiones posteriores a su _ultima_descarga, en orden
    cronológico, con acumular(): nada de lo visto se pierde; lo que el portal
    retira o cambia queda con _en_ultima_descarga=False. Después se aplica la
    última comprobación: sin cambios (actualiza _ultima_descarga) o retirado
    por el portal (todo el fichero pasa a _en_ultima_descarga=False).
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
            df, avisos = lector(version)
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
        if not len(df):
            resumen.avisos.append(f"{rel}: la versión del {fecha} no tiene registros que cargar; "
                                  "no se marca nada como retirado")
            continue
        anterior = acumular(anterior, df, fecha, ignorar=ignorar)
    if anterior is None or not len(anterior):
        return anterior
    if retirado:
        anterior = acumular(anterior, pd.DataFrame(), comprobado, ignorar=ignorar, permitir_vacio=True)
    elif reconfirmado and ultima is not None and len(ultima):
        anterior = acumular(anterior, ultima, comprobado, ignorar=ignorar)
    return anterior


def construir_parquet(destino, ficheros, raw, manifiesto, resumen, lector=leer_tabla):
    """Genera `destino` con los registros acumulados de `ficheros`
    (lista de (ruta_actual, rel, metadatos)), cada uno desde todas sus versiones en
    raw/ con el código actual.

    Las filas de ficheros que ya no se procesan se conservan: si el fichero
    sigue en raw/ se vuelve a procesar con sus versiones; si no, se copian tal
    cual del Parquet anterior.
    """
    destino = Path(destino)
    try:
        previo = leer_registros(destino)
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    por_fichero = {}
    if previo is not None and "_archivo_origen" in previo.columns:
        por_fichero = {str(k): g for k, g in previo.groupby("_archivo_origen", sort=False)}
    ficheros = list(ficheros)
    procesados = {rel for _, rel, _ in ficheros}
    for rel, grupo in por_fichero.items():
        if rel not in procesados and (Path(raw) / rel).exists():
            metadatos = {c: grupo[c].iloc[0] for c in METADATOS_ORIGEN
                         if c in grupo.columns and c not in METADATOS_POR_FILA and grupo[c].nunique(dropna=False) == 1}
            ficheros.append((Path(raw) / rel, rel, metadatos))
            procesados.add(rel)
    partes = []
    for actual, rel, metadatos in ficheros:
        # Con el código actual desde todas las versiones del crudo (regla 3): si solo se aplicaran
        # las versiones posteriores al Parquet anterior, un arreglo de lectura no llegaría nunca a
        # las filas ya guardadas
        por_fichero.pop(rel, None)
        registros = acumular_fichero(actual, rel, None, metadatos, manifiesto, resumen, lector)
        if registros is not None and len(registros):
            partes.append(registros)
    for rel, grupo in por_fichero.items():
        resumen.avisos.append(f"{destino.name}: {rel} ya no está en raw/; se conservan sus {len(grupo):,} filas")
        partes.append(grupo)
    if not partes:
        return None
    df = pd.concat(partes, ignore_index=True, sort=False)
    estado = escribir_parquet(df, destino)
    retiradas = int((~df["_en_ultima_descarga"].astype(bool)).sum())
    resumen.parquets.append((destino.name, len(df), len(df.columns), retiradas))
    print(f"  💾 {destino.name}: {len(df):,} filas ({estado})")
    return df


# ============================================================================
# DESCUBRIMIENTO: qué publica cada ayuntamiento
# ============================================================================

EXTENSIONES_TABLA = ("csv", "json") + HOJAS_CALCULO
PREFERENCIA_FORMATOS = ("xlsx", "xls", "ods", "csv", "json")
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
         "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
ORDINALES = {"primer": 1, "primero": 1, "segundo": 2, "tercer": 3, "tercero": 3, "cuarto": 4, "ultimo": 4,
             "i": 1, "ii": 2, "iii": 3, "iv": 4}


@dataclass
class Recurso:
    """Un fichero que publica el portal. rel: ruta en raw/ (empieza por la
    clave del municipio). sondeo: URL construida por el script (un 404 es "no
    publicado", o un error si confirmado); si no, la enlaza el portal (un 404
    es un error y lo que deja de enlazar queda como retirado)."""
    url: str
    rel: str
    formato: str = None
    titulo: str = None
    anio: str = None
    trimestre: str = None
    mes: str = None
    sondeo: bool = False
    confirmado: bool = False
    motivo: str = None        # por qué no se extrae (solo en los no estructurados)

    def metadatos(self):
        valores = {"_anio": self.anio, "_trimestre": self.trimestre, "_mes": self.mes, "_titulo": self.titulo}
        return {k: v for k, v in valores.items() if v is not None}


def _anios(texto):
    return [int(a) for a in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", str(texto or ""))]


def _tramo(valores, ancho=None):
    """'2021' o '2021-2023' (o con ceros: '07-08') de una lista de números."""
    valores = sorted(set(valores))
    if not valores:
        return None
    formato = (lambda v: str(v).zfill(ancho)) if ancho else str
    return formato(valores[0]) if len(valores) == 1 else f"{formato(valores[0])}-{formato(valores[-1])}"


def periodo_de_texto(texto):
    """(año, trimestre, mes) que dice un título o nombre de fichero: '1er
    Trimestre 2018', '4-TR-2022', 'Menores julio y agosto 2024',
    'CONTRATOS_MENORES_3T', 'II trimestre 2026'... Cada uno es None, un valor
    o un tramo ('2021-2023', '07-08')."""
    t = sin_acentos(texto or "").lower()
    t = re.sub(r"[_+\-.,;:/()\[\]]", " ", t)
    trimestres = {int(m.group(1)) for m in re.finditer(
        r"(?<![\d])([1-4])\s*(?:er|o|a|r)?\s*(?:t|tr|trim\w*)(?![a-z])", t)}
    trimestres |= {ORDINALES[m.group(1)] for m in re.finditer(
        r"\b(primero?|segundo|tercero?|cuarto|ultimo|i{1,3}|iv)\s+trim\w*", t)}
    meses = {MESES[m.group(1)] for m in re.finditer(r"\b(" + "|".join(MESES) + r")\b", t)}
    return _tramo(_anios(t)), _tramo(trimestres), _tramo(meses, 2)


def _texto_html(fragmento):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", fragmento or "")).split())


def _enlaces(pagina, base):
    """(url absoluta, texto) de cada <a href> de una página (sin comentarios HTML)."""
    pagina = re.sub(r"<!--.*?-->", "", pagina, flags=re.S)
    return [(urljoin(base, html.unescape(href).strip()), _texto_html(texto))
            for href, texto in re.findall(r"""<a\b[^>]*?\bhref\s*=\s*["']([^"']*)["'][^>]*>(.*?)</a>""",
                                          pagina, flags=re.S | re.I)
            if href.strip() and not href.strip().startswith(("#", "mailto:", "tel:", "javascript:"))]


def _nombre_url(url):
    return unquote(Path(urlparse(url).path).name)


def _extension(nombre):
    return Path(nombre).suffix.lower().lstrip(".")


def _nombre_seguro(nombre):
    """Nombre de fichero local: sin separadores de ruta ni punto inicial."""
    nombre = re.sub(r"[\\/\x00-\x1f]", "_", str(nombre)).strip().lstrip(".")
    return nombre[:200] or "fichero"


def _clave_url(url):
    """URL sin fragmento ni parámetros volátiles (?t=... de Liferay): identifica el fichero."""
    p = urlparse(url)
    consulta = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in PARAMETROS_VOLATILES]
    return urlunparse(p._replace(query=urlencode(consulta), fragment=""))


def _recurso(url, rel, titulo=None, textos=(), formato=None):
    """Recurso de un fichero enlazado. Su periodo es el del primero de `textos`
    (texto del enlace, nombre del fichero...) que dice un año o, si ninguno lo
    dice, el primero que dice algo."""
    periodos = [periodo_de_texto(t) for t in textos if t]
    anio, trimestre, mes = next((p for p in periodos if p[0]), next((p for p in periodos if any(p)), (None,) * 3))
    return Recurso(url=url, rel=rel, formato=formato or _extension(_nombre_url(url)) or None, titulo=titulo,
                   anio=anio, trimestre=trimestre, mes=mes)


def _separar(recursos):
    """(tablas, no estructurados): lo que se descarga y lo que solo se anota."""
    tablas, otros = [], []
    for r in recursos:
        if r.formato in EXTENSIONES_TABLA and not r.motivo:
            tablas.append(r)
        else:
            r.motivo = r.motivo or (r.formato or "sin extensión").upper()
            otros.append(r)
    return tablas, otros


# --- Gijón -------------------------------------------------------------------

def descubrir_gijon(resumen):
    """Un único JSON con todos los años (actualización diaria)."""
    return [Recurso(url=URL_GIJON, rel="gijon/contratos_menores_adjudicados.json", formato="json",
                    titulo="Contratos menores adjudicados")], []


def leer_gijon(ruta):
    """El JSON de Gijón trae todos los años: _anio es su columna 'ejercicio'."""
    df, avisos = leer_tabla(ruta)
    if "ejercicio" in df.columns:
        df["_anio"] = df["ejercicio"]
    else:
        avisos.append(f"{Path(ruta).name}: no trae la columna 'ejercicio'; _anio queda vacío")
    return df, avisos


# --- Vigo --------------------------------------------------------------------

def descubrir_vigo(resumen):
    """CSV anual con URL fija (un 404 en un año no confirmado es 'no publicado')
    y, si responde, el catálogo CKAN, por si publica algún año con otro nombre."""
    recursos, vistos = [], set()
    for anio in range(VIGO_DESDE, ahora().year + 1):
        url = URL_VIGO.format(aa=f"{anio % 100:02d}")
        recursos.append(Recurso(url=url, rel=f"vigo/{_nombre_url(url)}", formato="csv", anio=str(anio),
                                sondeo=True, confirmado=anio in VIGO_CONFIRMADOS))
        vistos.add(url)
    try:
        # Catálogo opcional (las URL ya se conocen): pocos intentos para no esperar minutos
        paquetes = _paquetes_ckan(URL_CKAN_VIGO, "contratos menores", intentos=INTENTOS_OPCIONAL)
    except ErrorPortal as e:
        resumen.avisos.append(f"vigo: catálogo CKAN ({URL_CKAN_VIGO}): {e}; se sigue con las URL conocidas")
        return recursos, []
    for paquete in paquetes:
        for recurso in paquete.get("resources") or []:
            url = (recurso.get("url") or "").strip()
            m = re.search(r"contratos-menores-(\d{2}|\d{4})\.csv$", url, re.I)
            if m and url not in vistos:
                anio = int(m.group(1)) if len(m.group(1)) == 4 else 2000 + int(m.group(1))
                recursos.append(Recurso(url=url, rel=f"vigo/{_nombre_seguro(_nombre_url(url))}", formato="csv",
                                        titulo=paquete.get("title"), anio=str(anio), sondeo=True))
                vistos.add(url)
    return recursos, []


# --- Valladolid ----------------------------------------------------------------

def _trimestre_valladolid(nombre):
    """Tramo de trimestres de un fichero de Valladolid (son acumulados):
    'PRIMER TRIMESTE' -> '1', 'PRIMER SEMESTRE' -> '1-2', 'PRIMER,SEGUNDO Y
    TERCER TRIMES' -> '1-3', 'EJERCICIO 2025' -> '1-4', '3º TRIME.' -> '3'."""
    t = sin_acentos(nombre).upper()
    if "SEMESTRE" in t or re.search(r"PRIMER\s+Y\s+SEGUNDO", t):
        return "1-2"
    if re.search(r"PRIMER\s*,\s*SEGUNDO\s+Y\s+TERCER", t):
        return "1-3"
    if re.search(r"\bPRIMER\s+TRIMEST", t):
        return "1"
    m = re.search(r"(?<!\d)([1-4])\s*O?\s*TRIM", t)
    if m:
        return m.group(1)
    if re.search(r"\b(EJERCICIO|ANUAL)\b", t):
        return "1-4"
    return None


def descubrir_valladolid(resumen):
    """Página índice -> una página por año -> ficheros (.ficheros/) de la página
    del año y de sus subpáginas por entidad."""
    indice = pedir_texto(URL_VALLADOLID)
    anios = {}
    for url, _ in _enlaces(indice, URL_VALLADOLID):
        m = re.search(r"/contratos-menores-volumen-contratacion-tipo-procedimiento/ano-(\d{4})/?$",
                      urlparse(url).path)
        if m:
            anios.setdefault(m.group(1), url)
    if not anios:
        raise ErrorPortal(f"{URL_VALLADOLID} no enlaza ninguna página de año")
    recursos, vistos = [], set()

    def ficheros(pagina, base, anio, entidad):
        for url, _ in _enlaces(pagina, base):
            if ".ficheros/" not in urlparse(url).path or url in vistos:
                continue
            vistos.add(url)
            nombre = _nombre_url(url)
            recursos.append(Recurso(url=url, rel=f"valladolid/{anio}/{_nombre_seguro(nombre)}",
                                    formato=_extension(nombre) or None, anio=anio,
                                    trimestre=_trimestre_valladolid(nombre),
                                    titulo=f"{entidad}: {nombre}" if entidad else nombre))

    for anio, url_anio in sorted(anios.items()):
        pagina = pedir_texto(url_anio)
        time.sleep(PAUSA)
        ficheros(pagina, url_anio, anio, None)
        subpaginas = []
        for url, _ in _enlaces(pagina, url_anio):
            m = re.search(rf"/ano-{anio}/([^/.]+)/?$", urlparse(url).path)
            if m and url not in subpaginas:
                subpaginas.append(url)
        for url_sub in subpaginas:
            ficheros(pedir_texto(url_sub), url_sub, anio, _nombre_url(url_sub))
            time.sleep(PAUSA)
    return _separar(recursos)


def _hoja_de_registros_valladolid(df):
    """Hojas con registros: OPERACIONES SICALWIN / DATOS MENORES ('Nº Operación')
    y las de 2017 por áreas ('NOMBRE DE TERCERO'). Las demás son tablas
    dinámicas, totales o listas de códigos."""
    columnas = {sin_acentos(str(c)).strip().lower() for c in df.columns}
    return bool({"no operacion", "nombre de tercero"} & columnas)


def leer_valladolid(ruta):
    return leer_tabla(ruta, hoja_valida=_hoja_de_registros_valladolid)


# --- Fuenlabrada ---------------------------------------------------------------

def _filas_fuenlabrada(pagina, base):
    """(fecha, nombre, url) de cada fila de la tabla del listado (<td data-head>)."""
    pagina = re.sub(r"<!--.*?-->", "", pagina, flags=re.S)
    filas = []
    for tr in re.findall(r"<tr\b[^>]*>(.*?)</tr>", pagina, flags=re.S | re.I):
        celdas = {k.strip().lower(): v for k, v in re.findall(
            r"""<td\b[^>]*data-head\s*=\s*["']([^"']+)["'][^>]*>(.*?)</td>""", tr, flags=re.S | re.I)}
        for url, _ in _enlaces(celdas.get("documento", ""), base):
            filas.append((_texto_html(celdas.get("fecha")), _texto_html(celdas.get("nombre")), url))
    return filas


def descubrir_fuenlabrada(resumen):
    """Listado de WordPress paginado (/page/N/) hasta la primera página sin filas."""
    recursos, vistos = [], set()
    for n in range(1, MAX_PAGINAS + 1):
        url_pagina = URL_FUENLABRADA if n == 1 else urljoin(URL_FUENLABRADA, f"page/{n}/")
        try:
            pagina = pedir_texto(url_pagina)
        except ErrorPortal as e:
            if n > 1 and e.codigo == 404:
                break
            raise
        filas = _filas_fuenlabrada(pagina, url_pagina)
        if not filas:
            break
        for _, nombre, url in filas:
            if url in vistos:            # las páginas se solapan
                continue
            vistos.add(url)
            fichero = _nombre_url(url)
            r = _recurso(url, "", titulo=nombre or fichero, textos=(nombre, fichero))
            r.rel = f"fuenlabrada/{r.anio or 'sin_anio'}/{_nombre_seguro(fichero)}"
            recursos.append(r)
        time.sleep(PAUSA)
    return _separar(recursos)


# --- Leganés -------------------------------------------------------------------

def descubrir_leganes(resumen):
    """Enlaces a /documents/ (biblioteca de documentos de Liferay) de la página;
    el periodo sale del texto del enlace ('Menores agosto 2026')."""
    recursos, vistos = [], {}
    for url, texto in _enlaces(pedir_texto(URL_LEGANES), URL_LEGANES):
        partes = unquote(urlparse(url).path).split("/")
        if len(partes) < 5 or partes[1] != "documents":
            continue
        clave = _clave_url(url)
        if clave in vistos:
            if texto and texto != vistos[clave].titulo:
                resumen.avisos.append(f"leganes: el mismo fichero se enlaza como '{vistos[clave].titulo}' y "
                                      f"como '{texto}' ({clave})")
            continue
        nombre = partes[4]        # /documents/<grupo>/<carpeta>/<nombre>/<uuid>
        r = _recurso(url, "", titulo=texto or nombre, textos=(texto, nombre), formato=_extension(nombre) or None)
        r.rel = f"leganes/{r.anio or 'sin_anio'}/{_nombre_seguro(nombre)}"
        vistos[clave] = r
        recursos.append(r)
    return _separar(recursos)


# --- CKAN (Málaga y Córdoba) ---------------------------------------------------

def _paquetes_ckan(url_api, consulta, intentos=INTENTOS):
    """Todos los conjuntos de datos de package_search (paginado)."""
    paquetes, inicio, total = {}, 0, None
    for _ in range(MAX_PAGINAS):
        datos = pedir_json(url_api, params={"q": consulta, "rows": FILAS_CKAN, "start": inicio}, intentos=intentos)
        if not isinstance(datos, dict) or datos.get("success") is False:
            raise ErrorPortal(f"respuesta de CKAN sin éxito: {str(datos)[:150]}")
        resultado = datos.get("result") or {}
        lote = resultado.get("results") or []
        total = resultado.get("count", total)
        for paquete in lote:
            paquetes[paquete.get("id") or paquete.get("name")] = paquete   # páginas solapadas: una vez
        inicio += len(lote)
        if not lote or (total is not None and inicio >= total):
            break
        time.sleep(PAUSA)
    return list(paquetes.values())


def _formato_ckan(recurso):
    """Formato de un recurso: la extensión de su URL o, si no la tiene, el declarado."""
    extension = _extension(_nombre_url(recurso.get("url") or ""))
    if extension:
        return extension
    return (recurso.get("format") or "").lower().strip(".") or None


def _grupo_ckan(recurso):
    """Recursos que son el mismo fichero en varios formatos: mismo nombre sin
    extensión (Córdoba) o, si el nombre es solo el formato ('xlsx', 'pdf'),
    todo el conjunto de datos (Málaga)."""
    nombre = re.sub(r"\.(pdf|xlsx?|ods|csv|json|docx?|odt|zip)$", "", (recurso.get("name") or "").strip(), flags=re.I)
    clave = sin_acentos(nombre).lower().strip()
    return "" if clave in ("", "pdf", "xls", "xlsx", "ods", "csv", "json", "excel") else clave


def descubrir_ckan(clave, url_api):
    """Conjuntos 'Contratos menores...' del CKAN municipal. De cada fichero
    publicado en varios formatos se baja el preferido (XLSX > XLS > ODS > CSV >
    JSON); los que solo están en PDF u otro formato se anotan."""
    recursos, usados = [], set()
    for paquete in sorted(_paquetes_ckan(url_api, CONSULTA_CKAN), key=lambda p: p.get("name") or ""):
        titulo = paquete.get("title") or paquete.get("name") or ""
        if not re.search(r"\bcontratos\s+menores\b", sin_acentos(titulo).lower()):
            continue
        grupos = collections.OrderedDict()
        for recurso in paquete.get("resources") or []:
            if (recurso.get("url") or "").strip():
                grupos.setdefault(_grupo_ckan(recurso), []).append(recurso)
        for lista in grupos.values():
            formatos = [_formato_ckan(r) for r in lista]
            mejor = next((f for f in PREFERENCIA_FORMATOS if f in formatos), None)
            for recurso, formato in zip(lista, formatos):
                url = recurso["url"].strip()
                nombre_recurso = (recurso.get("name") or "").strip()
                titulo_recurso = titulo if _grupo_ckan(recurso) == "" else f"{titulo} / {nombre_recurso}"
                fichero = _nombre_seguro(_nombre_url(url))
                rel = f"{clave}/{paquete.get('name')}/{fichero}"
                if rel in usados:
                    rel = f"{clave}/{paquete.get('name')}/{(recurso.get('id') or '')[:8]}_{fichero}"
                usados.add(rel)
                # El periodo, del nombre del recurso y del conjunto (no del nombre del
                # fichero: el CSV de Córdoba se llama ..._2020.csv y trae 2021-2024)
                r = _recurso(url, rel, titulo=titulo_recurso, textos=(f"{nombre_recurso} {titulo}",),
                             formato=formato)
                if mejor is not None and formato != mejor:
                    r.motivo = f"mismo fichero en {(formato or '?').upper()} (se baja el {mejor.upper()})"
                recursos.append(r)
    return _separar(recursos)


def descubrir_malaga(resumen):
    return descubrir_ckan("malaga", URL_CKAN_MALAGA)


def descubrir_cordoba(resumen):
    return descubrir_ckan("cordoba", URL_CKAN_CORDOBA)


# --- Santa Cruz de Tenerife ----------------------------------------------------

def descubrir_santa_cruz(resumen):
    """Ficheros CONTRATOS_MENORES_* de la página de contratos (los resúmenes
    anuales, con el nº de contratos y el importe total, se anotan pero no son
    registros)."""
    recursos, vistos = [], set()
    for url, texto in _enlaces(pedir_texto(URL_SANTA_CRUZ), URL_SANTA_CRUZ):
        nombre = _nombre_url(url)
        if not re.search(r"menores", nombre, re.I) or url in vistos:
            continue
        vistos.add(url)
        r = _recurso(url, "", titulo=texto or nombre, textos=(nombre, texto))
        r.rel = f"santa_cruz_tenerife/{r.anio or 'sin_anio'}/{_nombre_seguro(nombre)}"
        if not re.match(r"contratos_menores_", sin_acentos(nombre), re.I):
            r.motivo = "resumen (no son registros)"
        recursos.append(r)
    return _separar(recursos)


ADAPTADORES = {
    "gijon": (descubrir_gijon, leer_gijon),
    "vigo": (descubrir_vigo, leer_tabla),
    "valladolid": (descubrir_valladolid, leer_valladolid),
    "fuenlabrada": (descubrir_fuenlabrada, leer_tabla),
    "leganes": (descubrir_leganes, leer_tabla),
    "malaga": (descubrir_malaga, leer_tabla),
    "cordoba": (descubrir_cordoba, leer_tabla),
    "santa_cruz_tenerife": (descubrir_santa_cruz, leer_tabla),
}


# ============================================================================
# DESCARGA Y PARQUET (común a todos los municipios)
# ============================================================================

def _cerrado(recurso, anio_actual):
    """Periodo ya cerrado: su último año es anterior al año pasado (el fichero
    de un año sigue cambiando en los primeros meses del siguiente)."""
    anios = _anios(recurso.anio)
    return bool(anios) and max(anios) < anio_actual - 1


def _rel_con_hash(rel, clave_url):
    ruta = Path(rel)
    return (ruta.parent / f"{ruta.stem}__{hashlib.sha1(clave_url.encode()).hexdigest()[:8]}{ruta.suffix}").as_posix()


def _sin_duplicados(clave, recursos, resumen):
    """Un fichero enlazado varias veces (p.ej. desde dos conjuntos de datos del
    CKAN: en Málaga el de CEMI 2T 2020 enlaza el XLSX del 1T) se descarga una
    sola vez, con los datos del primer enlace: si no, sus filas saldrían dos
    veces."""
    vistos, unicos = {}, []
    for r in recursos:
        clave_url = _clave_url(r.url)
        if clave_url in vistos:
            resumen.avisos.append(f"{clave}: el mismo fichero se enlaza como '{vistos[clave_url].titulo}' y como "
                                  f"'{r.titulo}'; se descarga una vez ({r.url})")
            continue
        vistos[clave_url] = r
        unicos.append(r)
    return unicos


def _asignar_rutas(clave, recursos, manifiesto):
    """Ruta local estable de cada recurso: la que ya tiene en el manifiesto su
    URL o la propuesta; si dos URL distintas de esta lista caen en la misma,
    la segunda lleva un hash de su URL."""
    por_url = {_clave_url(e["url"]): rel for rel, e in manifiesto.de_municipio(clave).items() if e.get("url")}
    claves = [_clave_url(r.url) for r in recursos]
    # Las rutas de las URL ya conocidas se reservan antes (el orden de la lista puede cambiar)
    usados = {por_url[k]: k for k in claves if k in por_url}
    for r, clave_url in zip(recursos, claves):
        if clave_url in por_url:
            r.rel = por_url[clave_url]
        elif r.rel in usados and usados[r.rel] != clave_url:
            r.rel = _rel_con_hash(r.rel, clave_url)
        usados[r.rel] = clave_url


def _fallo_de_fichero(clave, rel, url, estado, detalle, resumen, registro, texto=None):
    """Un fichero enlazado (o que las fuentes dan por publicado) que no se puede bajar: se conserva
    su copia anterior y se vuelve a pedir en la próxima ejecución. Un error de origen permanente ya
    conocido (RegistroFallos) se avisa sin código 1; cualquier otro fallo da código 1."""
    entrada = registro.fallo(clave, rel, url, estado, detalle)
    texto = texto or f"{clave} {rel}: {detalle or estado} ({url})"
    if entrada["veces"] > 1:
        texto += f"; falla igual desde {entrada['primera']} ({entrada['veces']} intentos seguidos)"
    if RegistroFallos.permanente(entrada):
        resumen.conocidos.append(texto)
        print(f"  ⚠️ {rel}: {detalle or estado} (error de origen conocido desde {entrada['primera']})")
    else:
        resumen.fallidos.append(texto)
        print(f"  ❌ {rel}: {detalle or estado}")


def _bajado(clave, rel, estado, resumen, registro):
    """Un fichero bajado: si fallaba, sale del registro de fallos (RECUPERADOS)."""
    entrada = registro.resuelto(rel)
    if entrada is not None:
        resumen.recuperados.append(f"{clave} {rel}: vuelve a bajarse ({estado}); fallaba desde {entrada.get('primera')} "
                                   f"({entrada.get('firma')}; {entrada.get('veces')} intentos)")


def procesar_municipio(clave, raw, manifiesto, resumen, comprobar_todo=False, registro=None):
    """Lee la lista de ficheros del municipio, descarga los que faltan o pueden
    haber cambiado y marca como retirados los que el portal ya no enlaza. Los
    ficheros que no se pueden bajar pasan por el registro de fallos de origen
    (RegistroFallos; sin `registro`, se abre y se guarda aquí)."""
    if registro is None:
        registro = RegistroFallos(raw, ahora())
        try:
            return procesar_municipio(clave, raw, manifiesto, resumen, comprobar_todo, registro)
        finally:
            registro.guardar()
    config = MUNICIPIOS[clave]
    descubrir = ADAPTADORES[clave][0]
    print(f"\n📦 {config['nombre']} ({clave}): {config['descripcion']}")
    try:
        recursos, otros = descubrir(resumen)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{clave}: no se pudo leer la lista de ficheros ({e}); "
                                "no se descarga ni se retira nada")
        print(f"  ❌ {e}")
        return
    recursos = _sin_duplicados(clave, recursos, resumen)
    _asignar_rutas(clave, recursos, manifiesto)
    guardar_json(raw / clave / "_inventario.json",
                 [dict(asdict(r), estructurado=r.motivo is None) for r in recursos + otros])
    if otros:
        resumen.no_estructurados[clave] = otros
    enlazados = [r for r in recursos if not r.sondeo]
    anio_actual = ahora().year
    for r in recursos:
        destino = raw / r.rel
        entrada = manifiesto.get(r.rel)
        # Un cambio de URL, aunque sea solo el ?t= de Liferay, es una versión nueva, y
        # uno que se había retirado y vuelve a enlazarse se vuelve a pedir
        if (destino.exists() and _cerrado(r, anio_actual) and not comprobar_todo and entrada.get("url") == r.url
                and entrada.get("publicado", True)):
            resumen.sin_cambios[clave] += 1
            continue
        estado, detalle = descargar(r.url, destino, tipo=r.formato)
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, r.url, estado, municipio=clave, sondeo=r.sondeo, metadatos=r.metadatos())
            resumen.descarga(clave, f"{clave} {r.rel}", estado)
            _bajado(clave, r.rel, estado, resumen, registro)
            print(f"  ✅ {r.rel}: {estado}")
        elif r.sondeo and estado == "invalido" and not destino.exists() and not r.confirmado:
            registro.resuelto(r.rel)
            resumen.no_publicado(clave, int(r.anio) if (r.anio or "").isdigit() else r.rel)   # página HTML con 200
        elif r.sondeo and estado == "no_existe":
            if destino.exists():
                registro.resuelto(r.rel)
                manifiesto.retirar(destino, detalle)
                resumen.retirados.append(f"{clave} {r.rel}: el portal ya no lo sirve ({detalle}); "
                                         "se conservan sus filas")
            elif r.confirmado:
                _fallo_de_fichero(clave, r.rel, r.url, estado, detalle, resumen, registro,
                                  texto=f"{clave} {r.anio}: año publicado según las fuentes y ahora no "
                                        f"disponible ({detalle}; {r.url})")
            else:
                registro.resuelto(r.rel)
                resumen.no_publicado(clave, int(r.anio) if (r.anio or "").isdigit() else r.rel)
        else:
            # Enlazado (o confirmado) y no se puede bajar: se conserva la copia y se reintenta
            _fallo_de_fichero(clave, r.rel, r.url, estado, detalle, resumen, registro)
    if not enlazados:
        if not any(r.sondeo for r in recursos):
            resumen.fallidos.append(f"{clave}: el portal no enlaza ningún fichero con tablas; no se retira nada")
        return
    publicados = {r.rel for r in recursos}
    if not config.get("lista_incompleta"):
        # La lista se ha leído entera (si no, no se llega aquí): los fallos de ficheros que ya no
        # enlaza no se vuelven a pedir. Con una lista que pierde entradas no se sabe
        for rel, entrada in registro.olvidar(clave, publicados):
            resumen.avisos.append(f"{clave} {rel}: el portal ya no lo enlaza; sale del registro de fallos "
                                  f"(fallaba desde {entrada.get('primera')}: {entrada.get('firma')})")
    for rel, entrada in manifiesto.de_municipio(clave).items():
        if rel in publicados or entrada.get("sondeo") or not entrada.get("publicado", True):
            continue
        motivo = "el portal ya no lo enlaza"
        if config.get("lista_incompleta"):
            # La lista pierde entradas (ver MUNICIPIOS): no salir en ella no basta; se
            # vuelve a pedir su URL y solo un 404/410 lo retira
            estado, detalle = descargar(entrada["url"], raw / rel, tipo=_extension(rel) or None)
            time.sleep(PAUSA)
            if estado in ESTADOS_OK:
                manifiesto.registrar(raw / rel, entrada["url"], estado)
                resumen.descarga(clave, f"{clave} {rel} (no sale en la lista, pero su URL sigue publicada)", estado)
                _bajado(clave, rel, estado, resumen, registro)
                continue
            if estado != "no_existe":
                _fallo_de_fichero(clave, rel, entrada["url"], estado, detalle, resumen, registro,
                                  texto=f"{clave} {rel}: no sale en la lista y su URL no responde bien "
                                        f"({detalle or estado}); no se retira")
                continue
            motivo += f" y su URL da {detalle}"
        registro.resuelto(rel)
        manifiesto.retirar(raw / rel, motivo)
        resumen.retirados.append(f"{clave} {rel}: {motivo}; se conservan sus filas")
        print(f"  🗑️ {rel}: retirado por el portal")


def generar_parquet(clave, salida, raw, manifiesto, resumen):
    """<municipio>_menores.parquet con todas las versiones de todos sus ficheros."""
    config = MUNICIPIOS[clave]
    ficheros = []
    for rel, entrada in sorted(manifiesto.de_municipio(clave).items()):
        ruta = raw / rel
        if not ruta.exists():
            continue
        metadatos = dict(entrada.get("metadatos") or {})
        metadatos.update({"_fuente": entrada.get("url"), "_municipio": config["nombre"],
                          "_codigo_ine": config["codigo_ine"], "_archivo_origen": rel})
        ficheros.append((ruta, rel, metadatos))
    destino = salida / f"{clave}_menores.parquet"
    if ficheros or destino.exists():
        construir_parquet(destino, ficheros, raw, manifiesto, resumen, lector=ADAPTADORES[clave][1])


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores que publican algunos "
                                                 "ayuntamientos fuera de la PLACSP")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--municipio", action="extend", nargs="+", choices=list(MUNICIPIOS), metavar="MUNICIPIO",
                        help=f"uno o varios de: {', '.join(MUNICIPIOS)} (por defecto, todos)")
    parser.add_argument("--solo-procesar", action="store_true",
                        help="no descargar: solo generar los Parquet con lo que hay en raw/")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los ficheros de años cerrados ya descargados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    claves = list(dict.fromkeys(args.municipio)) if args.municipio else list(MUNICIPIOS)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Municipios: {', '.join(claves)}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_procesar:
        registro = RegistroFallos(raw, resumen.inicio)
        if registro.aviso:
            resumen.avisos.append(registro.aviso)
        try:
            for clave in claves:
                procesar_municipio(clave, raw, manifiesto, resumen, args.comprobar_todo, registro)
        finally:
            registro.guardar()
    print("\n🧱 Generando Parquet...")
    for clave in claves:
        generar_parquet(clave, salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
