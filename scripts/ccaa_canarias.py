#!/usr/bin/env python3
"""
=============================================================================
CANARIAS - CONTRATOS MENORES (Gobierno, SCS, Las Palmas de GC y Cabildo de Tenerife)
=============================================================================
Descarga, tal como se publican (con todas sus versiones), las fuentes de
contratos menores de Canarias que no son la PLACSP (nacional/) ni lo que ya
baja scripts/municipios_menores.py (Santa Cruz de Tenerife), y genera un
Parquet por serie con todas sus filas y columnas, como texto.

Ejecutar:  python scripts/ccaa_canarias.py [--salida DIR] [--fuentes a,b,...] [--desde AÑO]
           [--hasta AÑO] [--solo-descarga] [--solo-parquet] [--comprobar-todo]

Series (--fuentes, por defecto todas; una tabla cada una):
    gobierno_contratos   Gobierno de Canarias: «Contratos adjudicados y formalizados», CSV del
                         CKAN de datos.canarias.es (desde 2020; todos los procedimientos, contrato
                         a contrato: 68.880 de sus 90.280 filas son «Contrato menor»)
    las_palmas_gc        Ayuntamiento de Las Palmas de Gran Canaria: «Relación de contratos
                         menores» de su portal de transparencia (API JSON por año, 2016-)
    cabildo_tenerife     Cabildo Insular de Tenerife: relación de contratos menores de su portal
                         de transparencia (API JSON por año, 2019-; sin NIF del adjudicatario)
    scs_resumen          Servicio Canario de la Salud: «Contratos menores trimestrales» (ODS,
                         2021-): AGREGADOS por órgano y tipo de contrato (número e importe), no
                         contrato a contrato
    gobierno_resumen     Portal de transparencia del Gobierno: contratos menores de cada
                         departamento y organismo por trimestre (ODT, 3T 2023-): AGREGADOS por
                         órgano y tipo de contrato (número e importe)

Salida (por defecto <repo>/ccaa_canarias/):
    raw/gobierno_contratos/contratos.csv       el CSV tal cual (se llame como se llame en CKAN)
    raw/gobierno_contratos/diccionario.csv     el diccionario de datos que publica el Gobierno
    raw/gobierno_contratos/_paquete.json       la ficha de CKAN (package_show), con sus versiones
    raw/las_palmas_gc/<año>.json               la respuesta de la API de cada año, tal cual
    raw/las_palmas_gc/_obligacion.json         la definición de la tabla en el portal (campos)
    raw/cabildo_tenerife/<año>.json            la respuesta de la API de cada año, tal cual
    raw/scs_resumen/<nombre publicado>.ods     los ODS tal cual; _enlaces.json, los enlaces de la página
    raw/gobierno_resumen/<ruta publicada>.odt  los ODT tal cual (xi-legislatura/<dpto>/...);
                                               _enlaces.json, los enlaces de la página
    raw/**/_historico/                         versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json                       URL, periodo, fecha de descarga, última comprobación,
                                               si se sigue publicando y los datos de cada versión
    raw/descarga_log.txt                       resumen de cada ejecución (se añade al final)
    <serie>.parquet, _historico/               registros acumulados de cada serie

Columnas: las publicadas, con su nombre y su valor tal cual (texto; los números de
las API y de los ODS, como vienen escritos o guardados: 5600.00 -> '5600.00'). Nada
se limpia ni se quita: '_U' ("no consta") y '_Z' ("no aplica") del Gobierno se
quedan como texto, los espacios de los bordes se conservan y las filas de títulos,
totales y notas de los agregados son filas. Cuando la cabecera cambia de un fichero a
otro ('Importe (Eur)' e 'Importe ', 'Órgano de contratación' y 'órgano...'), el mismo
dato queda en columnas distintas, como se publica. En los JSON, '' y null quedan
nulos, igual que una clave que no viene. Los nombres que empiezan por '_' y no están
en la lista de abajo son del portal (la API de Las Palmas trae _esExterno,
_idExterno, _idLocal e _identidad).
Añadidas por el script:
- De origen (no cuentan al comparar versiones): _fuente (URL descargada),
  _archivo_origen (el fichero en raw/), _fecha_descarga, _anio y _trimestre (del
  año pedido o del texto del enlace), _periodo (texto del enlace), _departamento y
  _organismo (encabezados de la página del Gobierno), _recurso y
  _fichero_publicado (CKAN), _obligacion (id de la tabla en el portal de Las Palmas)
  y _hoja.
- Del contenido de los agregados (sí cuentan): _titulo_tabla (filas de título
  encima de la cabecera de una hoja), _seccion (encabezado del ODT), _tabla (nombre
  de la tabla en el ODT), _cabecera_tabla (su fila de cabecera tal cual),
  _parrafo_previo (el último párrafo antes de la tabla: «Órgano de contratación:
  ...») y _pie (los párrafos tras la última tabla: notas y fecha de extracción).
- De comun/historico.py: _primera_descarga, _ultima_descarga (fecha de la última
  versión que trae la fila; la última comprobación está en el manifiesto) y
  _en_ultima_descarga.

Sesgo del superviviente (docs/PRINCIPIOS.md, regla 3; comun/historico.py):
- Capa cruda: cada descarga pasa por guardar_version; si no cambia no se toca y si
  cambia la copia anterior va a _historico/.
- Parquet: se construye con el código actual desde TODAS las versiones de raw/, en
  orden, con acumular() por fichero: lo que el portal retira o cambia sigue con
  _en_ultima_descarga=False. Las filas de un fichero que ya no está en raw/, o con
  alguna versión que no se puede leer (y que no salen de las demás), se copian del
  Parquet anterior; si el código actual no saca ninguna fila de un fichero que las
  tenía, también, y es un error.
- Retirado: un año que la API deja de listar, un recurso que CKAN deja de tener o un
  enlace que la página deja de publicar (o que da 404) pasa a
  _en_ultima_descarga=False con todas sus filas. Si la lista, la ficha o la página
  fallan o no traen nada de la serie, no se retira nada (y es un error). Tampoco si
  de golpe dejarían de estar publicados, o darían 404, más de la mitad de los ficheros
  de una serie (y más de UMBRAL_RETIRADA): una lista a medias o un portal que ha
  movido los ficheros; es un error. El conjunto de CKAN
  entero solo se da por retirado si CKAN confirma («Not Found Error») que no existe
  ni por su nombre ni por su id (un 404 de otra cosa no confirma nada), y es un
  error; si lo han renombrado, se sigue por el id.
- Un recurso de CKAN conserva su fichero en raw/ aunque pase a ser el único; un
  recurso nuevo que llega solo es otra versión de contratos.csv. Dos enlaces
  distintos con el mismo nombre de fichero se bajan los dos (el segundo, con la
  huella de su URL en el nombre).
- Una versión sin filas no retira nada; si la vigente queda sin filas después de
  haberlas tenido, es un error. Una versión de un fichero de contratos sin alguna de
  sus columnas de identificación (IDENTIFICADORES: expediente_numero y
  licitacion_enlace en el CSV del Gobierno, id en las API; otra cabecera, otro
  fichero, un mensaje del portal) no se aplica: es un error mientras sea la vigente y
  las versiones posteriores se aplican con normalidad. Una versión que cambia o
  retira más de la mitad de las filas vigentes se avisa (¿otra exportación?).
- El manifiesto guarda los retirados y los datos de cada versión: si falta y ya hay
  descargas o Parquet, no se hace nada (es un error; hay que restaurarlo).
- No hay semilla: ninguna de estas fuentes está en el release v2026.02.

Qué se pide en cada ejecución (una petición cada vez, con pausa de 1 s; la primera
descarga completa son unas 320 peticiones, casi todas ODT de 10-30 KB, y la semanal
unas 160, las ~150 de los ODT del año en curso y el anterior):
- Gobierno: package_show de la ficha; el CSV (56 MB) y el diccionario, solo si no se
  tienen, si CKAN los da por cambiados (URL, tamaño o fecha de modificación) o con
  --comprobar-todo.
- Las Palmas y Tenerife: la lista de años y los años que faltan, el año en curso y
  el anterior; los cerrados que ya se tienen, solo con --comprobar-todo.
- SCS y Gobierno (agregados): la página de enlaces y los ficheros que faltan, los del
  año en curso y el anterior y los que cambian de URL; el resto, --comprobar-todo.
- Un año o un enlace que se tenía por retirado y el portal vuelve a publicar se pide
  otra vez, aunque sea antiguo: así vuelve a constar publicado (sus filas, vigentes).
Sale con 1 si algo que existe no se pudo descargar o leer.

FUENTES
-------
Verificado en vivo el 2026-09-30 (confianza A); detalle y cifras en
docs/COBERTURA.md (§3 y §5.2):
1. Gobierno de Canarias, CKAN https://datos.canarias.es/catalogos/general/api/3/action/
   package_show?id=contratos-adjudicados-y-formalizados-del-gobierno-de-canarias
   (organización Consejería de Hacienda; frecuencia mensual; último CSV del 2026-09-01).
   - CSV UTF-8 sin BOM, ';', todos los campos entre comillas (comillas dobladas), CRLF;
     90.280 registros, de los que 2.250 llevan un LF y 873 un CRLF dentro de un campo.
     23 columnas; sin nombre del adjudicatario (solo adjudicataria_nif).
   - Menores por año de adjudicación: 7.310 (2020), 10.415, 10.157, 11.207, 11.106,
     12.491 (2025) y 6.194 (2026 hasta agosto). Es la PLACSP del Gobierno: el 99,7 %
     (68.693) está en nuestro 1143 por el idEvl de licitacion_enlace; importe_ofertado es
     el importe de adjudicación SIN IVA de la PLACSP (99,8 %); NIF igual en el 99,8 %.
     El CPV solo viene en 16.244 menores (el resto, '_U').
   - El conjunto «...del Gobierno de Canarias. 2019» (excluidos menores) no se baja.
2. Las Palmas de Gran Canaria, https://transparencia.laspalmasgc.es/contratos/relacion-contratos-menores
   (plataforma «cloudtransparencia», Next.js). La definición de la tabla (obligación 88)
   va en __NEXT_DATA__; los datos, en /api/proxy/obligaciones/datos-anualizacion/88 (años)
   y /api/proxy/obligaciones/datos-multiples-registros-por-ano/88/1/<año> (1 = castellano).
   - 10.702 menores 2016-2026: 1.643, 1.640, 1.359, 951, 899, 1.058, 1.040, 619, 655, 632
     y 206. Campos: importe, ejercicio, url_origen, denominacion, n_expediente,
     adjudicatario, tipo_contrato, cif_adjudicatario, organo_de_contratacion y los internos
     _esExterno/_idExterno/_idLocal/_identidad/id. Sin fecha ni CPV.
   - 2018-2026 están en la PLACSP (url_origen con idEvl: el 100 % de los que lo traen);
     2016-2017 (3.283) no. La página dice «importe de adjudicación (incluyendo el IVA)»,
     pero frente a la PLACSP es SIN IVA en 2018-2021 y 2025-2026, CON IVA en 2023-2024 y
     mezclado en 2022.
   - El catálogo datosabiertos.laspalmasgc.es solo tiene «Contratos Menores 2017» (lo
     mismo que la API) y su HTTPS tiene el certificado caducado.
3. Cabildo de Tenerife, https://transparencia.tenerife.es/contratos/contratos-menores
   (la misma plataforma, versión anterior): https://webadmin.transparencia.tenerife.es/
   api/112/relacion-contratos-menores (años) y .../<año>. Sin la cabecera
   Origin: https://transparencia.tenerife.es responde 400.
   - 6.996 menores 2019-2025: 40, 7, 50, 22, 1.823, 2.425 y 2.629. Campos: id, fecha
     (466 de 2023 y 197 de 2024 con '0001-01-01T00:00:00'), ejercicio, denominacion,
     duracion, importelicitacion, importeadjudicacion (SIN IVA frente a la PLACSP: 96,6 %),
     procedimientoutilizado, publicidad, licitadores, adjudicatario. Sin NIF, sin órgano
     y sin expediente.
   - Solo un tercio está en el 1143 (por el objeto): unos 1.700 al año de 2023-2025 no.
4. SCS, https://www3.gobiernodecanarias.org/sanidad/scs/contenidoGenerico.jsp?
   idDocument=ecd71051-3421-11e4-bd1a-07940e8f0252&idCarpeta=08d3bd15-af33-11dd-a7d2-0594d2361b6c
   - Un ODS por año (una hoja por trimestre) de 2022 a 2026 y uno por trimestre en 2021,
     cada uno con su PDF. Son RESÚMENES: por órgano y tipo de contrato, número, importe y
     porcentaje. 2025: 89.340 menores (34.025, 24.203, 13.908 y 17.204 por trimestre).
     Sin los tramitados por anticipo de caja fija. Fuente: RECO y SEFLOGIC.
   - El «buscador de contratos menores» que enlaza es el de formalizados del Gobierno
     (la serie 1): unos 1.500 menores del SCS al año contrato a contrato. No hay fuente
     pública contrato a contrato del resto (hay que pedirla por acceso a la información).
5. Gobierno, https://www.gobiernodecanarias.org/transparencia/temas/contratos-convenios-
   subvenciones/contratacion-y-concesion-servicios/actividad-contractual/menores/
   - «número de contratos menores formalizados, especificando el importe global»: 284
     ODT (y sus PDF), uno por departamento u organismo (29 carpetas, SCS incluido) y
     trimestre, del 3T 2023 al 2T 2026. Cada ODT: un encabezado (la sección), un párrafo
     «Órgano de contratación: ...» y una tabla (Tipo contrato, Número, Importe (EUR),
     % (1)) por órgano, la tabla «Total sección» y las notas con la fecha de extracción.
=============================================================================
"""

import argparse
import codecs
import csv
import hashlib
import io
import json
import math
import os
import re
import sys
import time
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse
from xml.etree import ElementTree as ET

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

URL_CKAN = "https://datos.canarias.es/catalogos/general/api/3/action/"
PAQUETE_GOBIERNO = "contratos-adjudicados-y-formalizados-del-gobierno-de-canarias"
PAQUETE_GOBIERNO_ID = "c915a5c5-a0da-4e3d-8a97-039f46add0ac"   # id del conjunto (no cambia si lo renombran)
URL_LPGC = "https://transparencia.laspalmasgc.es"
PAGINA_LPGC = URL_LPGC + "/contratos/relacion-contratos-menores"
OBLIGACION_LPGC = 88                 # id de «Relación de contratos menores» (verificado el 2026-09-30)
IDIOMA_LPGC = 1                      # castellano
URL_TENERIFE = "https://webadmin.transparencia.tenerife.es/api/112/relacion-contratos-menores"
ORIGEN_TENERIFE = "https://transparencia.tenerife.es"     # sin esta cabecera Origin la API da 400
URL_SCS = ("https://www3.gobiernodecanarias.org/sanidad/scs/contenidoGenerico.jsp?"
           "idDocument=ecd71051-3421-11e4-bd1a-07940e8f0252&idCarpeta=08d3bd15-af33-11dd-a7d2-0594d2361b6c")
URL_GOBIERNO_MENORES = ("https://www.gobiernodecanarias.org/transparencia/temas/contratos-convenios-subvenciones/"
                        "contratacion-y-concesion-servicios/actividad-contractual/menores/")
RAIZ_DOCUMENTOS_GOBIERNO = "/actividad-contractual/menores/doc/"

SERIES = ("gobierno_contratos", "las_palmas_gc", "cabildo_tenerife", "scs_resumen", "gobierno_resumen")
TITULOS = {
    "gobierno_contratos": "Gobierno de Canarias: contratos adjudicados y formalizados (CKAN, CSV)",
    "las_palmas_gc": "Ayuntamiento de Las Palmas de Gran Canaria: relación de contratos menores (API)",
    "cabildo_tenerife": "Cabildo Insular de Tenerife: relación de contratos menores (API)",
    "scs_resumen": "Servicio Canario de la Salud: contratos menores trimestrales (ODS, agregados)",
    "gobierno_resumen": "Gobierno de Canarias: contratos menores por departamento y trimestre (ODT, agregados)",
}
# Columnas que tiene que traer toda versión de un fichero de contratos para aplicarse: sin
# ellas no es la serie (otra cabecera, otro fichero subido por error, un mensaje del portal)
IDENTIFICADORES = {"gobierno_contratos": ("expediente_numero", "licitacion_enlace"),
                   "las_palmas_gc": ("id",), "cabildo_tenerife": ("id",)}
# Retirada de golpe: si en una ejecución dejarían de estar publicados más de la mitad de los
# ficheros de una serie (y más de estos), no se retira nada y es un error
UMBRAL_RETIRADA = 3

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_canarias"
TITULO = "CANARIAS - CONTRATOS MENORES"

# ============================================================================
# UTILIDADES COMUNES (bloque de ccaa_andalucia_menores.py, ccaa_la_rioja.py y
# municipios_menores.py adaptado: cabeceras por petición, JSON con los números
# como vienen escritos, ODS y ODT leídos de su content.xml)
# ============================================================================

CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 60
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
PAUSA = 1.0                  # entre peticiones, para no cargar los portales
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")

# Columnas que añade el script. Las de origen no cuentan al comparar registros entre
# versiones (el mismo registro servido desde otra URL o con otro nombre sigue siendo
# el mismo); las de contexto salen del propio fichero y sí cuentan.
METADATOS_ORIGEN = ("_fuente", "_anio", "_trimestre", "_periodo", "_departamento", "_organismo", "_recurso",
                    "_fichero_publicado", "_obligacion", "_archivo_origen", "_hoja", "_fecha_descarga")
CONTEXTO_CONTENIDO = ("_titulo_tabla", "_seccion", "_tabla", "_cabecera_tabla", "_parrafo_previo", "_pie")
ORDEN_METADATOS = (CONTEXTO_CONTENIDO + METADATOS_ORIGEN
                   + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"))


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


def _normalizar(texto):
    return " ".join(sin_acentos(texto).lower().split())


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


def _pedir(url, params=None, cabeceras=None, devolver=()):
    """GET con reintentos (red, 429, 5xx); un 4xx lanza ErrorPortal con el código, salvo los
    de `devolver`, que devuelven la respuesta (el 404 de CKAN, cuyo cuerpo dice si el conjunto
    no existe)."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, params=params, headers={**CABECERAS, **(cabeceras or {})},
                                     timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400 and codigo not in devolver:
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


def pedir_json(url, params=None, cabeceras=None):
    """GET a una API JSON con reintentos; una respuesta que no es JSON también se reintenta."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = _pedir(url, params, cabeceras)
        try:
            return respuesta.json()
        except ValueError as e:
            detalle = f"respuesta no JSON ({str(e)[:80]})"
        if intento < INTENTOS:
            time.sleep(_espera(intento))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def pedir_texto(url, params=None, cabeceras=None):
    """GET de una página HTML con reintentos; un 4xx lanza ErrorPortal."""
    return _pedir(url, params, cabeceras).text


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes ('zip' incluye ODS, ODT y XLSX)."""
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
    """Formato real de un fichero: el de sus primeros bytes y, si es un ZIP, si es
    una hoja de cálculo (ODS, XLSX) o un documento de texto (ODT)."""
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato != "zip":
        return formato
    try:
        with zipfile.ZipFile(ruta) as archivo:
            nombres = archivo.namelist()
            if "mimetype" in nombres:
                tipo = archivo.read("mimetype")
                if b"opendocument.spreadsheet" in tipo:
                    return "ods"
                if b"opendocument.text" in tipo:
                    return "odt"
            if any(n.startswith("xl/") for n in nombres):
                return "xlsx"
    except (zipfile.BadZipFile, OSError, KeyError):
        pass
    return "zip"


def validar_contenido(ruta, tipo):
    """(motivo, reintentar): por qué la descarga no es el fichero esperado (una
    página HTML servida con 200, un JSON cortado, un ODS dañado...), o (None, False).
    tipo: 'csv', 'json' (una lista de registros), 'ods' u 'odt'."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía", False
    formato = formato_fichero(ruta)
    if formato != tipo:
        return f"se esperaba {tipo.upper()} y llegó {formato.upper()}", False
    if tipo == "json":
        try:
            with open(ruta, encoding="utf-8-sig") as f:
                datos = json.load(f)
        except ValueError as e:
            return f"JSON incompleto o dañado ({str(e)[:80]})", True
        if not isinstance(datos, list) or not all(isinstance(x, dict) for x in datos):
            return "el JSON no es una lista de registros", False
    if tipo in ("ods", "odt"):
        try:
            with zipfile.ZipFile(ruta) as archivo:
                malo = archivo.testzip()
                if "content.xml" not in archivo.namelist():
                    return f"el {tipo.upper()} no tiene content.xml", False
        except Exception as e:
            return f"fichero {tipo.upper()} incompleto o dañado ({type(e).__name__}: {str(e)[:100]})", True
        if malo:
            return f"fichero {tipo.upper()} dañado (miembro {malo})", True
    return None, False


def descargar(url, destino, tipo, cabeceras=None, tamano=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que llega entero (Content-Length) y que es el
    fichero esperado (no una página HTML de error ni un fichero cortado) y lo entrega
    a guardar_version(). Sin Content-Length utilizable (p.ej. con Content-Encoding), se
    compara con `tamano`, el que anuncia el portal (CKAN). Si el contenido no cambió no se
    toca nada y si cambió la copia
    previa pasa a _historico/. Reintenta con backoff los fallos de red, 429, 5xx y las
    descargas incompletas. Devuelve (estado, detalle): 'nuevo' | 'actualizado' |
    'sin_cambios', 'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    detalle = ""
    try:
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                with requests.get(url, headers={**CABECERAS, **(cabeceras or {})}, timeout=TIMEOUT_DESCARGA,
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
                        cabeceras_resp = getattr(respuesta, "headers", None) or {}
                        esperado = str(cabeceras_resp.get("Content-Length") or "")
                        con_longitud = esperado.isdigit() and not cabeceras_resp.get("Content-Encoding")
                        if con_longitud and int(esperado) != escritos:
                            detalle = f"descarga incompleta ({escritos:,} de {int(esperado):,} bytes)"
                        elif not con_longitud and isinstance(tamano, int) and tamano > 0 and tamano != escritos:
                            detalle = (f"descarga de {escritos:,} bytes y el portal anuncia {tamano:,} (sin "
                                       "Content-Length: puede estar cortada)")
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
    """Guarda metadatos del portal (fichas, inventarios de enlaces) conservando sus versiones."""
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    return guardar_version(destino, contenido)


class Manifiesto:
    """raw/_manifiesto.json, por fichero crudo (su ruta en raw/): serie, de dónde sale,
    periodo, cuándo se descargó su versión actual (fecha_descarga), cuándo se comprobó
    por última vez (comprobado), si el portal lo sigue publicando y, en `versiones`,
    la URL y los datos del portal de cada versión descargada (por su fecha)."""

    def __init__(self, raw):
        self.raw = Path(raw)
        self.ruta = self.raw / "_manifiesto.json"
        self.existia = self.ruta.exists()
        try:
            self.datos = json.loads(self.ruta.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self.datos = {}
        except ValueError as e:
            # Sin él se perderían los retirados y los datos de cada versión: no se sigue
            raise RuntimeError(f"{self.ruta} no se puede leer ({e}); no se descarga ni se regenera nada: "
                               "revisarlo o restaurarlo") from e
        if not isinstance(self.datos, dict):
            raise RuntimeError(f"{self.ruta} no es un objeto JSON; no se descarga ni se regenera nada")

    def rel(self, ruta):
        return Path(ruta).relative_to(self.raw).as_posix()

    def get(self, rel):
        return self.datos.get(rel, {})

    def de_serie(self, serie):
        return {rel: e for rel, e in self.datos.items() if e.get("serie") == serie}

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
            fecha = fecha_version(ruta)
            entrada["fecha_descarga"] = entrada["comprobado"] = fecha
        entrada.setdefault("versiones", {}).setdefault(entrada["fecha_descarga"], dict(extra, url=url))
        self.guardar()

    def comprobar(self, rel):
        """Anota que un fichero se ha visto publicado sin volver a descargarlo."""
        entrada = self.datos.get(rel)
        if entrada is not None:
            entrada["comprobado"] = iso(ahora())
            self.guardar()

    def retirar(self, rel, detalle):
        entrada = self.datos.setdefault(rel, {})
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
    """Lo descargado, lo que sigue igual, lo retirado, los avisos y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = {}
        self.retirados = []
        self.avisos = []
        self.fallidos = []
        self.parquets = []

    def descarga(self, serie, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios[serie] = self.sin_cambios.get(serie, 0) + 1
        else:
            self.descargados.append(f"{etiqueta} ({estado})")

    def texto(self):
        lineas = ["=" * 70]
        if self.fallidos:
            lineas.append(f"⚠️ {self.titulo}: COMPLETADO CON ERRORES ({len(self.fallidos)})")
        else:
            lineas.append(f"✅ {self.titulo}: COMPLETADO")
        lineas += ["=" * 70, f"Inicio: {iso(self.inicio)}  Fin: {iso(ahora())}"]

        def bloque(titulo, elementos):
            if elementos:
                lineas.append(f"\n{titulo} ({len(elementos)}):")
                lineas.extend(f"   - {e}" for e in elementos)

        bloque("DESCARGADOS (versión nueva)", self.descargados)
        bloque("SIN CAMBIOS O YA DESCARGADOS (--comprobar-todo vuelve a pedir los años cerrados)",
               [f"{s}: {n} ficheros" for s, n in self.sin_cambios.items()])
        bloque("RETIRADOS POR EL PORTAL (se conservan con _en_ultima_descarga=False)", self.retirados)
        bloque("PARQUET", [f"{n}: {f:,} filas x {c} columnas ({r:,} ya no publicadas)"
                           for n, f, c, r in self.parquets])
        bloque("AVISOS", self.avisos)
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


class Filtro:
    """Años que se descargan (--desde / --hasta). Con filtro, un fichero sin año no entra."""

    def __init__(self, desde=None, hasta=None):
        self.desde, self.hasta = desde, hasta

    @property
    def activo(self):
        return self.desde is not None or self.hasta is not None

    def incluye(self, anio):
        if anio is None:
            return not self.activo
        return (self.desde is None or anio >= self.desde) and (self.hasta is None or anio <= self.hasta)


# ----------------------------------------------------------------------------
# Lectura de ficheros como texto (todas las filas y columnas, sin convertir nada)
# ----------------------------------------------------------------------------

def _detectar_codificacion(ruta):
    """Primera codificación capaz de decodificar el fichero COMPLETO: UTF-8 y, si no,
    cp1252 (antes que latin-1, que acepta cualquier byte y dejaría '€' o '’' como
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


def _detectar_separador(texto):
    """Separador más frecuente (fuera de comillas) en la primera línea con texto."""
    primera = next((linea for linea in re.split(r"\r\n|\r|\n", texto[:1 << 16]) if linea.strip()), "")
    cuentas = dict.fromkeys([";", ",", "\t", "|"], 0)
    entre_comillas = False
    for caracter in primera:
        if caracter == '"':
            entre_comillas = not entre_comillas
        elif not entre_comillas and caracter in cuentas:
            cuentas[caracter] += 1
    mejor = max(cuentas, key=cuentas.get)
    return mejor if cuentas[mejor] else ","


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


def leer_csv(ruta):
    """CSV como texto, sin perder filas ni campos: solo el campo vacío es nulo ('_U',
    'NA', 'NULL'... se quedan como texto) y cada valor va tal cual, con sus saltos de
    línea y comillas. Se lee con el módulo csv (comillas estándar: un ';' o un salto de
    línea entre comillas son parte del campo); si falla, o si una comilla literal al
    principio de un campo se tragaría registros enteros, con comun.lectura_csv. Los
    campos que sobran respecto a la cabecera van a _columna_extra_N; a un registro con
    menos campos le faltan los últimos (nulos), con un aviso. Devuelve (df, avisos)."""
    ruta = Path(ruta)
    avisos = []
    codificacion = _detectar_codificacion(ruta)
    with open(ruta, encoding=codificacion, newline="") as f:
        texto = f.read()
    if texto.startswith("﻿"):
        texto = texto[1:]
    sep = _detectar_separador(texto)
    csv.field_size_limit(max(csv.field_size_limit(), 1 << 30))
    try:
        registros = [r for r in csv.reader(io.StringIO(texto, newline=""), delimiter=sep) if r]
    except csv.Error:
        registros = None
    tolerantes, literales = registros_csv(texto, sep)
    tolerantes = [r for r in tolerantes if r != [""]]
    if registros is None or (literales and len(tolerantes) > len(registros)):
        if literales:
            avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo se leen como "
                          "texto (se conservan; si no, se tragarían los registros siguientes)")
        registros = tolerantes
    if not registros:
        return pd.DataFrame(), [f"{ruta.name}: fichero sin cabecera ni filas"]
    cabecera, datos = registros[0], registros[1:]
    ancho = len(cabecera)
    largo = max(len(r) for r in [cabecera] + datos)
    nombres = _nombres_columnas(cabecera) + [f"_columna_extra_{k}" for k in range(1, largo - ancho + 1)]
    con_extra = sum(1 for r in datos if len(r) > ancho)
    cortos = sum(1 for r in datos if len(r) < ancho)
    if con_extra:
        avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; los campos de más se "
                      "conservan en columnas _columna_extra_N")
    if cortos:
        avisos.append(f"{ruta.name}: {cortos:,} filas con menos campos que la cabecera (les faltan los últimos): "
                      "revisar")
    df = pd.DataFrame([[v if v != "" else None for v in r] + [None] * (largo - len(r)) for r in datos],
                      columns=nombres, dtype=object)
    return df, avisos


def _texto_json(valor):
    """Valor de un registro JSON como texto: las cadenas tal cual ('' es nulo), los
    números como vienen escritos (parse_float=str), true/false/null como en el JSON y
    los objetos o listas, en JSON."""
    if valor is None:
        return None
    if isinstance(valor, str):
        return valor if valor != "" else None
    return json.dumps(valor, ensure_ascii=False)


def leer_json(ruta):
    """Registros de una respuesta JSON de las API de transparencia (una lista de
    objetos) como texto, con las columnas en el orden en que aparecen. Los números no
    pasan por float: 5600.00 -> '5600.00'; true/false/null, como en el JSON ('' y null
    quedan nulos, igual que una clave que no viene). Un objeto o una lista anidados van
    en JSON, con sus números como números. Devuelve (df, avisos)."""
    with open(ruta, encoding="utf-8-sig") as f:
        texto = f.read()
    datos = json.loads(texto, parse_int=str, parse_float=str, parse_constant=str)
    if not isinstance(datos, list) or not all(isinstance(r, dict) for r in datos):
        raise ValueError("el JSON no es una lista de registros")
    anidados = None
    columnas = list(dict.fromkeys(k for r in datos for k in r))
    filas = []
    for i, registro in enumerate(datos):
        fila = []
        for c in columnas:
            valor = registro.get(c)
            if isinstance(valor, (dict, list)):
                if anidados is None:
                    anidados = json.loads(texto)          # otra vez, con los números como números
                valor = json.dumps(anidados[i][c], ensure_ascii=False)
            fila.append(_texto_json(valor))
        filas.append(fila)
    return pd.DataFrame(filas, columns=columnas, dtype=object), []


_TABLE = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
_OFFICE = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"
_TEXT = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"


def _texto_nodo(nodo):
    """Texto de un nodo de texto de OpenDocument tal como se ve: <text:s> son espacios,
    <text:tab> un tabulador y <text:line-break> un salto de línea; las anotaciones no."""
    if nodo.tag == _TEXT + "s":
        return " " * int(nodo.get(_TEXT + "c", "1"))
    if nodo.tag == _TEXT + "tab":
        return "\t"
    if nodo.tag == _TEXT + "line-break":
        return "\n"
    if nodo.tag == _OFFICE + "annotation":
        return ""
    return (nodo.text or "") + "".join(_texto_nodo(hijo) + (hijo.tail or "") for hijo in nodo)


def _valor_ods(celda):
    """Valor de una celda de ODS como texto: el número, la fecha o el booleano tal como está
    guardado en el fichero (office:value..., sin pasar por float: '5600.00' no se vuelve
    '5600' ni un número largo pierde cifras) y, en las de texto, los párrafos separados por
    saltos de línea."""
    tipo = celda.get(_OFFICE + "value-type")
    if tipo in ("float", "percentage", "currency"):
        return celda.get(_OFFICE + "value") or None
    if tipo == "date":
        return celda.get(_OFFICE + "date-value") or None
    if tipo == "time":
        return celda.get(_OFFICE + "time-value") or None
    if tipo == "boolean":
        return celda.get(_OFFICE + "boolean-value") or None
    if celda.get(_OFFICE + "string-value") is not None:
        return celda.get(_OFFICE + "string-value") or None
    texto = "\n".join(_texto_nodo(p) for p in celda if p.tag in (_TEXT + "p", _TEXT + "h"))
    return texto if texto != "" else None


def _celda_odt(celda):
    """Valor de una celda de una tabla de ODT: su texto (párrafos separados por saltos
    de línea); si es una celda con valor numérico sin texto, el valor guardado."""
    texto = "\n".join(_texto_nodo(p) for p in celda if p.tag in (_TEXT + "p", _TEXT + "h"))
    if texto != "":
        return texto
    return _valor_ods(celda)


def _filas_tabla(tabla, valor=_valor_ods):
    """(filas, tapadas): las filas (listas de valores) de una tabla de OpenDocument, con las
    celdas y filas repetidas (number-*-repeated), y cuántas celdas tapadas por una
    combinación traen contenido (no se ven en la hoja y no se leen, como en pandas). Las
    celdas vacías del final de una fila no se materializan y una fila vacía no es un
    registro."""
    filas, tapadas = [], 0
    for fila in tabla.iter(_TABLE + "table-row"):
        valores, vacias = [], 0
        for celda in fila:
            if celda.tag not in (_TABLE + "table-cell", _TABLE + "covered-table-cell"):
                continue
            repetir = int(celda.get(_TABLE + "number-columns-repeated", "1"))
            if celda.tag == _TABLE + "covered-table-cell":
                tapadas += repetir if _valor_ods(celda) is not None else 0
                dato = None
            else:
                dato = valor(celda)
            if dato is None:
                vacias += repetir
                continue
            valores.extend([None] * vacias + [dato] * repetir)
            vacias = 0
        if valores:            # una fila vacía (a veces repetida hasta el final de la hoja) no es un registro
            filas.extend([valores] * int(fila.get(_TABLE + "number-rows-repeated", "1")))
    return filas, tapadas


PATRON_DATO = re.compile(r"-?\d+(?:[.,]\d+)*|\d{4}-\d{2}-\d{2}(?:[ T][\d:.]+)?|[A-Z]?\d{7,8}[A-Z]?|[A-Z]\d{7}[A-Z0-9]",
                         re.IGNORECASE)


def _es_dato(valor):
    return valor is not None and bool(PATRON_DATO.fullmatch(str(valor).strip()))


def _parece_registro(fila):
    """¿La fila detectada como cabecera son datos? Lo son si al menos la mitad de sus
    celdas con valor son números, fechas o NIF (una cabecera son rótulos)."""
    valores = [str(v).strip() for v in fila if v is not None and str(v).strip()]
    return bool(valores) and sum(bool(PATRON_DATO.fullmatch(v)) for v in valores) * 2 >= len(valores)


def _tabla_hoja(filas, nombre, hoja, avisos):
    """Tabla de una hoja: detecta la fila de cabecera (el texto de las filas de
    encima, título y periodo, va a la columna _titulo_tabla) y conserva todas las
    filas con algún valor (también las de totales y notas). Devuelve un DataFrame o
    None si la hoja no tiene ningún valor."""
    filas = [list(f) for f in filas if any(v is not None for v in f)]
    if not filas:
        return None
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    # Cabecera con huecos que no llega al umbral: si la fila elegida trae datos y la
    # anterior son solo rótulos, la cabecera es la anterior
    if (pos > 0 and any(_es_dato(v) for v in filas[pos]) and llenas[pos - 1] >= 2
            and not any(_es_dato(v) for v in filas[pos - 1])):
        pos -= 1
    titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
    ancho = max(len(f) for f in filas)
    cabecera = filas[pos] + [None] * (ancho - len(filas[pos]))
    if _parece_registro(cabecera):
        avisos.append(f"{nombre} [{hoja}]: sin fila de cabecera (la primera fila son datos: "
                      f"{' | '.join(str(v) for v in cabecera if v is not None)[:120]}); columnas columna_1…")
        cabecera = [f"columna_{i}" for i in range(1, ancho + 1)]
        pos -= 1
    datos = [f + [None] * (ancho - len(f)) for f in filas[pos + 1:]]
    nombres = _nombres_columnas(cabecera)
    df = pd.DataFrame(datos, columns=nombres, dtype=object)
    # Columnas sin nombre y sin ningún valor: restos del rango usado de la hoja
    vacias = [c for c, v in zip(nombres, cabecera) if v is None and df[c].isna().all()]
    df = df.drop(columns=vacias)
    df["_hoja"] = hoja
    df["_titulo_tabla"] = titulo or None
    return df


def leer_ods(ruta):
    """Hojas de un ODS (leídas de su content.xml: pandas uniría los párrafos de una
    celda sin salto de línea y pasaría los números por float) con todas sus filas como
    texto, unidas (columna _hoja). Devuelve (df, avisos)."""
    ruta = Path(ruta)
    avisos, partes = [], []
    with zipfile.ZipFile(ruta) as archivo:
        raiz = ET.fromstring(archivo.read("content.xml"))
    hojas = raiz.find(f"{_OFFICE}body/{_OFFICE}spreadsheet")
    for tabla in (hojas.findall(_TABLE + "table") if hojas is not None else []):
        filas, tapadas = _filas_tabla(tabla)
        if tapadas:
            avisos.append(f"{ruta.name} [{tabla.get(_TABLE + 'name')}]: {tapadas} celdas tapadas por una combinación "
                          "traen contenido (no se ven en la hoja; no se leen)")
        df = _tabla_hoja(filas, ruta.name, tabla.get(_TABLE + "name"), avisos)
        if df is not None:
            partes.append(df)
    if len(partes) > 1:
        avisos.append(f"{ruta.name}: {len(partes)} hojas con datos; se unen (columna _hoja)")
    if not partes:
        return pd.DataFrame(columns=["_hoja"]), avisos
    return pd.concat(partes, ignore_index=True, sort=False), avisos


def _bloques_odt(nodo):
    """Párrafos (('p', texto tal cual)) y tablas (('tabla', nodo)) del cuerpo de un ODT, en
    orden, entrando en secciones y listas."""
    for hijo in nodo:
        if hijo.tag in (_TEXT + "p", _TEXT + "h"):
            yield "p", _texto_nodo(hijo)
        elif hijo.tag == _TABLE + "table":
            yield "tabla", hijo
        elif hijo.tag in (_TEXT + "section", _TEXT + "list", _TEXT + "list-item", _TEXT + "list-header",
                          _TEXT + "soft-page-break"):
            yield from _bloques_odt(hijo)


def leer_odt(ruta):
    """Tablas de un ODT de contratos menores del Gobierno, fila a fila, como texto.

    Cada tabla tiene su fila de cabecera (Tipo contrato, Número, Importe (EUR), % (1));
    todas las tablas del documento se leen con los nombres de la primera (la de «Total
    sección» deja vacía la primera celda de la cabecera) y la cabecera propia de cada
    una va, tal cual, a _cabecera_tabla. Contexto de cada fila (del documento, con su
    texto tal cual, espacios incluidos): _seccion (el encabezado o, si no hay, el primer
    párrafo con texto), _parrafo_previo (el último párrafo con texto antes de la tabla:
    «Órgano de contratación: ...»), _tabla (el nombre de la tabla en el documento) y _pie
    (los párrafos con texto tras la última tabla, separados por saltos de línea: notas y
    «Fecha en la que se extrae la información»). Las filas sin ningún valor no son
    registros. Devuelve (df, avisos)."""
    ruta = Path(ruta)
    avisos = []
    with zipfile.ZipFile(ruta) as archivo:
        raiz = ET.fromstring(archivo.read("content.xml"))
    cuerpo = raiz.find(f"{_OFFICE}body/{_OFFICE}text")
    if cuerpo is None:
        raise ValueError(f"{ruta.name}: el ODT no tiene cuerpo de texto")
    encabezado = next((_texto_nodo(h) for h in cuerpo.iter(_TEXT + "h") if _texto_nodo(h).strip()), None)
    bloques = list(_bloques_odt(cuerpo))
    parrafos = [b[1] for b in bloques if b[0] == "p" and b[1].strip()]
    seccion = encabezado or (parrafos[0] if parrafos else None)
    ultima = max((i for i, b in enumerate(bloques) if b[0] == "tabla"), default=None)
    pie = ("\n".join(b[1] for b in bloques[ultima + 1:] if b[0] == "p" and b[1].strip())
           if ultima is not None else None)
    nombres_doc, partes, previo, tapadas = None, [], None, 0
    for bloque, contenido in bloques:
        if bloque == "p":
            if contenido.strip():
                previo = contenido
            continue
        filas, n_tapadas = _filas_tabla(contenido, _celda_odt)
        tapadas += n_tapadas
        if not filas:
            continue
        cabecera, datos = filas[0], filas[1:]
        ancho = max(len(f) for f in filas)
        propios = _nombres_columnas(cabecera + [None] * (ancho - len(cabecera)))
        if nombres_doc is None:
            nombres_doc = propios
        nombres = nombres_doc if len(nombres_doc) == ancho else propios
        if nombres is propios and nombres_doc is not propios:
            avisos.append(f"{ruta.name}: la tabla {contenido.get(_TABLE + 'name')} tiene {ancho} columnas y la "
                          f"primera {len(nombres_doc)}; se lee con su propia cabecera")
        df = pd.DataFrame([f + [None] * (ancho - len(f)) for f in datos], columns=nombres, dtype=object)
        df["_seccion"] = seccion
        df["_tabla"] = contenido.get(_TABLE + "name")
        df["_cabecera_tabla"] = " | ".join("" if v is None else str(v) for v in cabecera)
        df["_parrafo_previo"] = previo
        df["_pie"] = pie or None
        partes.append(df)
    if tapadas:
        avisos.append(f"{ruta.name}: {tapadas} celdas tapadas por una combinación traen contenido (no se ven; "
                      "no se leen)")
    if not partes:
        return pd.DataFrame(columns=["_seccion"]), avisos + [f"{ruta.name}: el ODT no tiene tablas con datos"]
    return pd.concat(partes, ignore_index=True, sort=False), avisos


LECTORES = {"csv": leer_csv, "json": leer_json, "ods": leer_ods, "odt": leer_odt}


def leer_version(ruta):
    """Una versión de un fichero crudo (según su contenido real) con todas sus filas
    y columnas como texto. Devuelve (df, avisos)."""
    formato = formato_fichero(ruta)
    if formato not in LECTORES:
        raise ValueError(f"{Path(ruta).name}: formato {formato.upper()} no se lee")
    return LECTORES[formato](ruta)


# ----------------------------------------------------------------------------
# Registros acumulados (comun/historico.py) y Parquet
# ----------------------------------------------------------------------------

def _texto_celda(valor):
    if valor is None or valor is pd.NA:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    return valor if isinstance(valor, str) else str(valor)


def ordenar_columnas(df):
    """Columnas del portal (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def escribir_parquet(df, destino):
    """Parquet con todas las columnas como texto (_en_ultima_descarga booleana), sin
    los metadatos de pandas: así el fichero es el mismo con pandas 2 y 3 (en pandas 3
    las columnas leídas de un Parquet son 'str' y las nuevas 'object', y los metadatos
    cambiarían sin que cambie ningún dato). Se escribe en un temporal y pasa por
    guardar_version: la versión anterior queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df = ordenar_columnas(df)
    columnas, campos = [], []
    for nombre in df.columns:
        if nombre == "_en_ultima_descarga":
            columnas.append(pa.array(df[nombre].astype(bool).tolist(), type=pa.bool_()))
            campos.append(pa.field(nombre, pa.bool_()))
        else:
            columnas.append(pa.array([_texto_celda(v) for v in df[nombre].astype(object).tolist()],
                                     type=pa.string()))
            campos.append(pa.field(str(nombre), pa.string()))
    tabla = pa.Table.from_arrays(columnas, schema=pa.schema(campos))
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def acumular_fichero(actual, rel, manifiesto, resumen, identificadores=()):
    """Registros de un fichero crudo a lo largo de todas sus versiones en raw/, leídas
    con el código actual (regla 3) y aplicadas en orden cronológico con acumular():
    nada de lo visto se pierde; lo que el portal retira o cambia queda con
    _en_ultima_descarga=False. Si el portal ha retirado el fichero, todas sus filas
    pasan a _en_ultima_descarga=False. Devuelve (registros, nº de versiones ilegibles).
    - Una versión que no se puede leer (excepción) es un error y no se aplica; cuenta en
      las ilegibles (generar_parquet conserva lo que solo salía de ella).
    - Una versión sin filas no retira nada; si es la vigente y alguna anterior tenía
      filas, es un error (en una primera versión vacía, un aviso: el portal puede
      listar un año antes de cargarlo).
    - Una versión sin alguna de las columnas `identificadores` (otra cabecera, otro
      fichero o un mensaje del portal) no se aplica: es un error mientras sea la vigente
      y un aviso cuando ya hay otra después, que se aplica con normalidad.
    - Una versión que cambia o retira más de la mitad de las filas vigentes se avisa."""
    info = manifiesto.get(rel)
    registros, ilegibles = None, 0
    lista = versiones(actual)
    for version in lista:
        fecha = fecha_version(version)
        vigente = version == lista[-1]
        try:
            df, avisos = leer_version(version)
        except Exception as e:
            resumen.fallidos.append(f"{rel}: no se pudo leer la versión {version.name}: {e}")
            ilegibles += 1
            continue
        resumen.avisos.extend(avisos)
        if not len(df):
            mensaje = f"{rel}: la versión del {fecha} ({version.name}) no tiene filas; no se marca nada como retirado"
            if vigente and registros is not None and len(registros):
                resumen.fallidos.append(mensaje + " (es la versión vigente y la anterior tenía filas: revisar)")
            else:
                resumen.avisos.append(mensaje)
            continue
        faltan = [c for c in identificadores if c not in df.columns]
        if faltan:
            mensaje = (f"{rel}: la versión del {fecha} ({version.name}) no trae {', '.join(faltan)} (cabecera: "
                       f"{', '.join(map(str, list(df.columns)[:6]))}…); no se aplica")
            if vigente:
                resumen.fallidos.append(mensaje + " (es la versión vigente: revisar lo publicado)")
            else:
                resumen.avisos.append(mensaje + " (hay versiones posteriores, que sí se aplican)")
            continue
        datos = (info.get("versiones") or {}).get(fecha) or {}
        df = df.copy()
        df["_fuente"] = datos.get("url") or info.get("url")
        for columna, clave in (("_anio", "anio"), ("_trimestre", "trimestre"), ("_periodo", "periodo"),
                               ("_departamento", "departamento"), ("_organismo", "organismo"),
                               ("_recurso", "recurso"), ("_fichero_publicado", "nombre_publicado"),
                               ("_obligacion", "obligacion")):
            valor = datos.get(clave, info.get(clave))
            if valor is not None:
                df[columna] = str(valor)
        df["_archivo_origen"] = rel
        df["_fecha_descarga"] = fecha
        vigentes = 0 if registros is None else int(registros["_en_ultima_descarga"].sum())
        registros = acumular(registros, df, fecha, ignorar=METADATOS_ORIGEN)
        siguen = int((registros["_en_ultima_descarga"] & (registros["_primera_descarga"] < fecha)).sum())
        if vigentes and (vigentes - siguen) * 2 > vigentes:
            resumen.avisos.append(f"{rel}: la versión del {fecha} cambia o retira {vigentes - siguen:,} de las "
                                  f"{vigentes:,} filas vigentes (más de la mitad): ¿otra exportación u otro formato? "
                                  "revisar")
    if registros is None or not len(registros):
        return registros, ilegibles
    comprobado = info.get("comprobado")
    if info.get("publicado") is False and comprobado:
        registros = acumular(registros, pd.DataFrame(), comprobado, ignorar=METADATOS_ORIGEN, permitir_vacio=True)
    return registros, ilegibles


# Copia en _historico/ que escribe guardar_version: <nombre>__<AAAAMMDDTHHMMSSZ>[_N].<ext>
PATRON_VERSION_HISTORICA = re.compile(r"(?P<base>.+)__\d{8}T\d{6}Z(?:_\d+)?(?P<ext>\.[^.]+)")
EXTENSIONES_DATOS = {"gobierno_contratos": (".csv",), "las_palmas_gc": (".json",), "cabildo_tenerife": (".json",),
                     "scs_resumen": (".ods",), "gobierno_resumen": (".odt",)}


def ficheros_serie(raw, serie):
    """Ficheros de datos de una serie en raw/<serie>/, por su ruta actual: los que tienen
    copia actual y los que solo tienen versiones en _historico/ (si la copia actual se
    perdió, versiones() las encuentra igual). Sin los metadatos (_paquete.json,
    _enlaces.json, _obligacion.json) ni el diccionario del Gobierno."""
    carpeta = Path(raw) / serie
    if not carpeta.is_dir():
        return []
    salida = set()
    for ruta in carpeta.rglob("*"):
        if not ruta.is_file():
            continue
        if HISTORICO in ruta.relative_to(carpeta).parts:
            m = PATRON_VERSION_HISTORICA.fullmatch(ruta.name)
            if ruta.parent.name != HISTORICO or not m:
                continue
            ruta = ruta.parent.parent / f"{m['base']}{m['ext']}"
        if (ruta.name.startswith(("_", ".")) or ruta.suffix.lower() not in EXTENSIONES_DATOS[serie]
                or (serie == "gobierno_contratos" and ruta.name == "diccionario.csv")):
            continue
        salida.add(ruta)
    return sorted(salida)


def _filas_ausentes(previas, nuevas):
    """Filas de `previas` (las de un fichero en el Parquet anterior) que no están en
    `nuevas`, comparando como multiconjunto el contenido que tienen las dos (sin las
    columnas de origen ni las de control), con sus marcas tal cual."""
    previas = previas.reset_index(drop=True)
    if nuevas is None or not len(nuevas):
        return previas
    excluir = set(METADATOS_ORIGEN) | {"_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"}
    comunes = [c for c in previas.columns if c in nuevas.columns and c not in excluir]
    if not comunes:
        return previas

    def claves(df):
        texto = pd.DataFrame({c: [("\x01" + t) if (t := _texto_celda(v)) is not None else "\x00"
                                  for v in df[c].astype(object).tolist()] for c in comunes})
        valores = pd.util.hash_pandas_object(texto, index=False).to_numpy()
        return list(zip(valores.tolist(), pd.Series(valores).groupby(valores).cumcount().tolist()))

    vistas = set(claves(nuevas.reset_index(drop=True)))
    return previas.iloc[[i for i, par in enumerate(claves(previas)) if par not in vistas]]


def generar_parquet(salida, raw, serie, manifiesto, resumen):
    """Genera <salida>/<serie>.parquet con los registros acumulados de los ficheros de
    la serie en raw/, cada uno desde todas sus versiones con el código actual. Un fallo de
    lectura no quita filas de la salida:
    - si una versión no se puede leer, las filas del Parquet anterior de ese fichero que no
      salen de las demás versiones se conservan con sus marcas (las versiones posteriores
      se siguen aplicando);
    - si el código actual no saca ninguna fila de un fichero que en el Parquet anterior las
      tenía, se conservan las anteriores y es un error;
    - las filas de un fichero del que ya no queda ninguna copia en raw/ (ni en _historico/)
      se copian del Parquet anterior, con la retirada del manifiesto aplicada."""
    destino = Path(salida) / f"{serie}.parquet"
    try:
        previo = leer_registros(destino)
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    por_fichero = {}
    if previo is not None and "_archivo_origen" in previo.columns:
        por_fichero = {str(k): g for k, g in previo.groupby("_archivo_origen", sort=False)}
    identificadores = IDENTIFICADORES.get(serie, ())
    partes = []
    for actual in ficheros_serie(raw, serie):
        rel = manifiesto.rel(actual)
        registros, ilegibles = acumular_fichero(actual, rel, manifiesto, resumen, identificadores)
        previas = por_fichero.pop(rel, None)
        if previas is not None and len(previas):
            if ilegibles:
                faltan = _filas_ausentes(previas, registros)
                if len(faltan):
                    resumen.avisos.append(f"{destino.name}: {rel}: {ilegibles} versiones no se han podido leer; se "
                                          f"conservan {len(faltan):,} filas del Parquet anterior que no salen de las "
                                          "demás")
                    registros = (faltan if registros is None or not len(registros)
                                 else pd.concat([registros, faltan], ignore_index=True, sort=False))
            elif registros is None or not len(registros):
                resumen.fallidos.append(f"{destino.name}: {rel}: el código actual no saca ninguna fila y el Parquet "
                                        f"anterior tenía {len(previas):,}; se conservan (revisar el lector)")
                registros = previas
        if registros is not None and len(registros):
            partes.append(registros)
    for rel, grupo in por_fichero.items():
        info = manifiesto.get(rel)
        if info.get("publicado") is False and info.get("comprobado"):
            grupo = acumular(grupo, pd.DataFrame(), info["comprobado"], ignorar=METADATOS_ORIGEN, permitir_vacio=True)
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
# DESCARGA POR SERIE
# ============================================================================

def _debe_pedirse(destino, anio, comprobar_todo, forzar=False):
    """Se pide lo que falta, el año en curso y el anterior (siguen cambiando), lo que hay
    que volver a comprobar (`forzar`: se tenía por retirado y el portal lo vuelve a
    publicar, o cambia su URL) y todo con --comprobar-todo; un fichero sin año, siempre."""
    return (comprobar_todo or forzar or not Path(destino).exists() or anio is None
            or anio >= ahora().year - 1)


def _registrar_descarga(serie, destino, url, estado, detalle, manifiesto, resumen, etiqueta, **extra):
    """Anota el resultado de una descarga y devuelve si el fichero sigue publicado. Una
    versión nueva o sin cambios va al manifiesto (True). Un 404 de algo que se tenía no se
    retira aquí (False): el llamante lo quita de los vigentes y _retirar_ausentes lo retira
    con el umbral de la serie (un 404 de casi todo es un portal que ha movido los ficheros,
    no una retirada). Cualquier otro fallo es un error y no cambia nada (True)."""
    if estado in ESTADOS_OK:
        manifiesto.registrar(destino, url, estado, serie=serie, **extra)
        resumen.descarga(serie, etiqueta, estado)
        print(f"  ✅ {etiqueta}: {estado}")
        return True
    if estado == "no_existe" and Path(destino).exists():
        print(f"  ⚠️ {etiqueta}: el portal da {detalle}")
        return False
    resumen.fallidos.append(f"{etiqueta}: {detalle or estado} ({url})")
    print(f"  ❌ {etiqueta}: {detalle or estado}")
    return True


def _retirar_ausentes(manifiesto, resumen, serie, vigentes, motivo, umbral=True, motivos=None):
    """Da por retirados los ficheros de la serie que se tenían publicados y ya no están
    en `vigentes` (rutas en raw/): el portal, leído entero y con algo de la serie, ya
    no los publica o da 404 (`motivos`: el motivo de cada uno, si no es `motivo`).
    Devuelve los retirados. Con `umbral`, si de golpe faltarían más de la mitad de los
    ficheros publicados de la serie (y más de UMBRAL_RETIRADA), no se retira nada y es un
    error: una página o una lista a medias, o un portal que ha movido los ficheros, no
    puede dar por retirado todo lo demás."""
    motivos = motivos or {}
    publicados = [rel for rel, e in manifiesto.de_serie(serie).items() if e.get("publicado", True)]
    candidatos = sorted(rel for rel in publicados if rel not in vigentes)
    if umbral and len(candidatos) > max(UMBRAL_RETIRADA, len(publicados) // 2):
        resumen.fallidos.append(f"{serie}: {len(candidatos)} de los {len(publicados)} ficheros publicados dejarían de "
                                f"estarlo de golpe ({motivo}, o dan 404); no se retira nada: ¿página o lista a medias, "
                                "ficheros movidos? revisar")
        return []
    for rel in candidatos:
        manifiesto.retirar(rel, motivos.get(rel, motivo))
        resumen.retirados.append(f"{rel}: {motivos.get(rel, motivo)}; se conservan sus filas")
        print(f"  🗑️ {rel}: retirado por el portal")
    return candidatos


# --- Gobierno: CKAN --------------------------------------------------------------

def ckan(accion, **params):
    """Llamada a la API de CKAN de datos.canarias.es. Devuelve `result`; lanza ErrorPortal si
    falla. Lleva codigo=404 solo si CKAN confirma que el conjunto no existe (su JSON de error
    con «Not Found Error», venga con 200 o con 404): un 404 de otra cosa (un proxy, una ruta de
    la API que cambia) no confirma nada y va sin código."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        try:
            respuesta = _pedir(URL_CKAN + accion, params or None, devolver=(404, 410))
        except ErrorPortal as e:
            raise ErrorPortal(f"CKAN {accion}: {e}") from e
        try:
            datos = respuesta.json()
        except ValueError:
            detalle = f"respuesta no JSON (HTTP {respuesta.status_code})"
            if respuesta.status_code in (404, 410):
                break               # un 404 que no es de CKAN: no se reintenta ni confirma nada
            if intento < INTENTOS:
                time.sleep(_espera(intento))
            continue
        if isinstance(datos, dict) and datos.get("success") is True:
            return datos.get("result")
        error = (datos.get("error") if isinstance(datos, dict) else None) or {}
        error = error if isinstance(error, dict) else {}
        codigo = 404 if error.get("__type") == "Not Found Error" else None
        raise ErrorPortal(f"CKAN {accion}: {error.get('message') or 'respuesta sin éxito'} "
                          f"(HTTP {respuesta.status_code})", codigo)
    raise ErrorPortal(f"CKAN {accion}: {detalle}")


def _es_diccionario(recurso):
    texto = _normalizar(f"{recurso.get('name') or ''} {recurso.get('url') or ''}")
    return "diccionario" in texto


def _recursos_csv(paquete):
    """Recursos CSV activos del conjunto, por posición: (datos, diccionarios)."""
    datos, diccionarios = [], []
    for recurso in sorted(paquete.get("resources") or [], key=lambda r: r.get("position") or 0):
        if recurso.get("state") not in (None, "active"):
            continue
        nombre = unquote(Path(urlparse(recurso.get("url") or "").path).name).lower()
        if str(recurso.get("format") or "").strip().lower() == "csv" or nombre.endswith(".csv"):
            (diccionarios if _es_diccionario(recurso) else datos).append(recurso)
    return datos, diccionarios


def _asignar_locales(recursos, base, manifiesto, serie):
    """[(nombre local, recurso)]: cada recurso conserva el fichero que ya tenía (por su id en
    el manifiesto); los demás van a `base` si está libre y, si no, a
    <base sin extensión>_<id>.csv. Un recurso nuevo que llega solo va a `base`: si el
    Gobierno sustituye el CSV por otro, es otra versión del mismo fichero."""
    previos = {e.get("recurso"): Path(rel).name for rel, e in manifiesto.de_serie(serie).items()
               if e.get("recurso") and Path(rel).name != "diccionario.csv"}
    if len(recursos) == 1:
        return [(previos.get(recursos[0].get("id")) or base, recursos[0])]
    asignados, usados = {}, set()
    for i, recurso in enumerate(recursos):
        local = previos.get(recurso.get("id"))
        if local and local not in usados:
            asignados[i] = local
            usados.add(local)
    for i, recurso in enumerate(recursos):
        if i not in asignados:
            local = base if base not in usados else f"{Path(base).stem}_{str(recurso.get('id') or i)[:8]}.csv"
            asignados[i] = local
            usados.add(local)
    return [(asignados[i], recursos[i]) for i in range(len(recursos))]


def _ids_paquete(raw):
    """Por dónde preguntar a CKAN por el conjunto: su nombre, el id guardado en
    _paquete.json (no cambia aunque lo renombren) y el id verificado el 2026-09-30."""
    ids = [PAQUETE_GOBIERNO]
    try:
        guardado = json.loads((Path(raw) / "gobierno_contratos" / "_paquete.json").read_text(encoding="utf-8"))
        if isinstance(guardado, dict) and guardado.get("id"):
            ids.append(str(guardado["id"]))
    except (OSError, ValueError):
        pass
    ids.append(PAQUETE_GOBIERNO_ID)
    return list(dict.fromkeys(ids))


def descargar_gobierno_contratos(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    """El CSV de contratos adjudicados y formalizados del Gobierno y su diccionario,
    si no se tienen, si CKAN los da por cambiados o con --comprobar-todo. Un recurso
    que el conjunto deja de tener queda como retirado. El conjunto entero solo se da por
    retirado si CKAN confirma que no existe ni por su nombre ni por su id (y es un error);
    si CKAN falla, no se retira nada."""
    serie = "gobierno_contratos"
    carpeta = Path(raw) / serie
    print(f"\n📦 {TITULOS[serie]}")
    paquete, no_existe = None, []
    for ident in _ids_paquete(raw):
        try:
            paquete = ckan("package_show", id=ident)
            break
        except ErrorPortal as e:
            if e.codigo != 404:
                resumen.fallidos.append(f"{serie}: {e}; no se descarga ni se retira nada")
                print(f"  ❌ {serie}: {e}")
                return
            no_existe.append(f"{ident}: {e}")
        finally:
            time.sleep(PAUSA)
    if paquete is None:
        retirados = _retirar_ausentes(manifiesto, resumen, serie, set(),
                                      f"CKAN ya no tiene el conjunto {PAQUETE_GOBIERNO}", umbral=False)
        mensaje = f"{serie}: CKAN confirma que el conjunto no existe ({'; '.join(no_existe)})"
        if retirados:
            resumen.fallidos.append(mensaje + f"; se dan por retirados {len(retirados)} ficheros con todas sus filas: "
                                    "revisar")
        else:
            resumen.avisos.append(mensaje)
        return
    if no_existe:
        resumen.avisos.append(f"{serie}: el conjunto ya no responde como {PAQUETE_GOBIERNO}; se sigue por su id "
                              f"({paquete.get('id')}, ahora «{paquete.get('name')}»)")
    guardar_json(carpeta / "_paquete.json", paquete)
    datos, diccionarios = _recursos_csv(paquete)
    if not datos:
        resumen.fallidos.append(f"{serie}: el conjunto {paquete.get('name')} no tiene ningún CSV de datos; "
                                "no se retira nada")
        return
    if len(datos) > 1:
        resumen.avisos.append(f"{serie}: {len(datos)} CSV de datos en el conjunto ("
                              + ", ".join(str(r.get("name")) for r in datos) + "); se bajan todos")
    vigentes, motivos = set(), {}
    for local, recurso in _asignar_locales(datos, "contratos.csv", manifiesto, serie) + [
            ("diccionario.csv", r) for r in diccionarios[:1]]:
        destino = carpeta / local
        rel = manifiesto.rel(destino)
        vigentes.add(rel)
        url = (recurso.get("url") or "").strip()
        tamano = recurso.get("size") if isinstance(recurso.get("size"), int) else None
        extra = {"recurso": recurso.get("id"), "nombre_publicado": unquote(Path(urlparse(url).path).name),
                 "tamano_ckan": recurso.get("size"), "modificado_ckan": recurso.get("last_modified")}
        if not url:
            resumen.fallidos.append(f"{serie}: el recurso {recurso.get('id')} no tiene URL")
            continue
        info = manifiesto.get(rel)
        igual = info.get("url") == url and all(info.get(k) == extra[k] for k in ("recurso", "tamano_ckan",
                                                                                  "modificado_ckan"))
        if destino.exists() and igual and info.get("publicado", True) and not comprobar_todo:
            manifiesto.comprobar(rel)
            resumen.descarga(serie, rel, "sin_cambios")
            print(f"  = {rel}: sin cambios en CKAN")
            continue
        print(f"  ⬇️ {rel}: {url}")
        estado, detalle = descargar(url, destino, "csv", tamano=tamano)
        time.sleep(PAUSA)
        if not _registrar_descarga(serie, destino, url, estado, detalle, manifiesto, resumen, rel, **extra):
            vigentes.discard(rel)
            motivos[rel] = f"el portal da {detalle} ({url})"
    _retirar_ausentes(manifiesto, resumen, serie, vigentes, "el conjunto de CKAN ya no tiene ese CSV", motivos=motivos)


# --- Las Palmas de Gran Canaria y Cabildo de Tenerife: API de transparencia -------

def _datos_next(pagina):
    """El JSON __NEXT_DATA__ de una página Next.js (o None)."""
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>', pagina, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except ValueError:
        return None


def obligacion_lpgc(carpeta, resumen):
    """Id de la tabla «Relación de contratos menores» en el portal de Las Palmas, de la
    definición que trae la página (__NEXT_DATA__); la definición se guarda en
    _obligacion.json. Si la página falla o no la trae, se usa la conocida
    (OBLIGACION_LPGC) con un aviso."""
    try:
        pagina = pedir_texto(PAGINA_LPGC)
    except ErrorPortal as e:
        resumen.avisos.append(f"las_palmas_gc: la página {PAGINA_LPGC} falla ({e}); se usa la tabla "
                              f"{OBLIGACION_LPGC}")
        return OBLIGACION_LPGC
    finally:
        time.sleep(PAUSA)
    props = ((_datos_next(pagina) or {}).get("props") or {}).get("pageProps") or {}
    obligacion = props.get("obligacion") or {}
    nombre = _normalizar(obligacion.get("obligacion") or "")
    if not isinstance(obligacion.get("id"), int) or "contratos menores" not in nombre:
        resumen.avisos.append(f"las_palmas_gc: la página no trae la definición de la relación de contratos "
                              f"menores; se usa la tabla {OBLIGACION_LPGC}")
        return OBLIGACION_LPGC
    guardar_json(Path(carpeta) / "_obligacion.json",
                 {"id": obligacion.get("id"), "obligacion": obligacion.get("obligacion"),
                  "estructura": obligacion.get("estructura"), "metadatos": props.get("metadatosSSR")})
    if obligacion["id"] != OBLIGACION_LPGC:
        resumen.avisos.append(f"las_palmas_gc: la relación de contratos menores es ahora la tabla "
                              f"{obligacion['id']} (era {OBLIGACION_LPGC})")
    return obligacion["id"]


def _anios_api(url, cabeceras, serie, resumen):
    """Años de una API ([{"ano": 2025}, ...]) o None si falla (no se retira nada)."""
    try:
        datos = pedir_json(url, cabeceras=cabeceras)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{serie}: la lista de años ({url}) falla: {e}; no se descarga ni se retira nada")
        print(f"  ❌ {serie}: lista de años: {e}")
        return None
    finally:
        time.sleep(PAUSA)
    anios = sorted({int(d["ano"]) for d in datos if isinstance(d, dict) and str(d.get("ano", "")).isdigit()}) \
        if isinstance(datos, list) else []
    if not anios:
        resumen.fallidos.append(f"{serie}: la lista de años ({url}) no trae ninguno; no se descarga ni se retira nada")
        return None
    return anios


def _descargar_anios(raw, serie, anios, url_anio, cabeceras, manifiesto, resumen, filtro, comprobar_todo, **extra):
    """Descarga el JSON de cada año de la lista y da por retirados los que se tenían y la
    lista ya no trae (solo dentro del filtro de años: fuera no se ha mirado)."""
    carpeta = Path(raw) / serie
    vigentes, motivos = set(), {}
    for anio in anios:
        destino = carpeta / f"{anio}.json"
        rel = manifiesto.rel(destino)
        vigentes.add(rel)
        if not filtro.incluye(anio):
            continue
        info = manifiesto.get(rel)
        url = url_anio(anio)
        forzar = info.get("publicado") is False or info.get("url") not in (None, url)
        if not _debe_pedirse(destino, anio, comprobar_todo, forzar):
            manifiesto.comprobar(rel)
            resumen.descarga(serie, rel, "sin_cambios")
            continue
        estado, detalle = descargar(url, destino, "json", cabeceras)
        time.sleep(PAUSA)
        if not _registrar_descarga(serie, destino, url, estado, detalle, manifiesto, resumen, rel, anio=anio, **extra):
            vigentes.discard(rel)
            motivos[rel] = f"el portal da {detalle} ({url})"
    fuera = {rel for rel, e in manifiesto.de_serie(serie).items() if not filtro.incluye(e.get("anio"))}
    _retirar_ausentes(manifiesto, resumen, serie, (vigentes | fuera) - set(motivos), "la API ya no lista ese año",
                      motivos=motivos)


def descargar_las_palmas(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    serie = "las_palmas_gc"
    print(f"\n📦 {TITULOS[serie]}")
    carpeta = Path(raw) / serie
    carpeta.mkdir(parents=True, exist_ok=True)
    obligacion = obligacion_lpgc(carpeta, resumen)
    base = f"{URL_LPGC}/api/proxy/obligaciones"
    cabeceras = {"Accept": "application/json"}
    anios = _anios_api(f"{base}/datos-anualizacion/{obligacion}", cabeceras, serie, resumen)
    if anios is None:
        return
    _descargar_anios(raw, serie, anios,
                     lambda a: f"{base}/datos-multiples-registros-por-ano/{obligacion}/{IDIOMA_LPGC}/{a}",
                     cabeceras, manifiesto, resumen, filtro, comprobar_todo, obligacion=obligacion)


def descargar_tenerife(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    serie = "cabildo_tenerife"
    print(f"\n📦 {TITULOS[serie]}")
    cabeceras = {"Accept": "application/json", "Origin": ORIGEN_TENERIFE}
    anios = _anios_api(URL_TENERIFE, cabeceras, serie, resumen)
    if anios is None:
        return
    _descargar_anios(raw, serie, anios, lambda a: f"{URL_TENERIFE}/{a}", cabeceras, manifiesto, resumen,
                     filtro, comprobar_todo)


# --- Agregados: páginas de enlaces del SCS y del Gobierno -------------------------

def _texto_html(fragmento):
    import html
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", fragmento)).split())


PATRON_TRIMESTRE = re.compile(r"(\d)\s*(?:º|°|ª|o)?\s*(?:er|º)?\s*trimestre\D{0,12}((?:19|20)\d{2})", re.IGNORECASE)
ORDINALES = {"primer": 1, "primero": 1, "segundo": 2, "tercer": 3, "tercero": 3, "cuarto": 4}


def periodo_de_texto(*textos):
    """(año, trimestre) del texto de un enlace o del nombre del fichero: '2º trimestre
    2026' -> (2026, 2), 'primer trimestre 2021' -> (2021, 1), '... 2025 (ods)' ->
    (2025, None). None si no hay año."""
    for texto in textos:
        normal = _normalizar(unquote(texto or ""))
        m = PATRON_TRIMESTRE.search(normal)
        if m:
            return int(m.group(2)), int(m.group(1))
        m = re.search(r"\b(primer|primero|segundo|tercer|tercero|cuarto)\s+trimestre\D{0,12}((?:19|20)\d{2})", normal)
        if m:
            return int(m.group(2)), ORDINALES[m.group(1)]
    for texto in textos:
        anios = re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", unquote(texto or ""))
        if anios:
            return int(anios[-1]), None
    return None, None


def enlaces_scs(pagina):
    """Enlaces a documentos de contratos menores de la página del SCS (el texto del
    enlace o el nombre del fichero dicen 'menores' o 'CM_'): [{texto, url}], con la URL
    absoluta (los enlaces son ./content/<uuid>/<fichero>)."""
    salida = []
    for m in re.finditer(r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>', pagina, re.S | re.I):
        texto = _texto_html(m.group(2))
        url = urljoin(URL_SCS, _texto_html(m.group(1)))
        nombre = unquote(Path(urlparse(url).path).name)
        if (re.search(r"\.(ods|pdf|xlsx?|csv)$", nombre, re.I)
                and ("menores" in _normalizar(texto) or re.match(r"(?i)cm_|contratos[-_ ]menores", nombre))):
            salida.append({"texto": texto, "url": url})
    return salida


def enlaces_gobierno(pagina):
    """Enlaces a documentos de la página de contratos menores del Gobierno, con el
    departamento (encabezado del bloque) y el organismo (subencabezado de nivel 2,
    para los enlaces de nivel 3): [{departamento, organismo, texto, url}]."""
    salida, departamento, organismo = [], None, None
    patron = re.compile(
        r"<span class=['\"]\s*tit_conten_grande['\"][^>]*>(?P<dpto>.*?)</span>"
        r"|<li class=['\"]mapa_web_nivel_2['\"]>\s*<span>(?P<org>[^<]+)</span>"
        r"|<li class=['\"]mapa_web_nivel_(?P<nivel>\d)['\"]>(?:(?!</li>).)*?<a\s[^>]*href=\"(?P<url>[^\"]+)\"[^>]*>"
        r"(?P<texto>.*?)</a>", re.S | re.I)
    for m in patron.finditer(pagina):
        if m.group("dpto") is not None:
            departamento, organismo = _texto_html(m.group("dpto")), None
        elif m.group("org") is not None:
            organismo = _texto_html(m.group("org"))
        else:
            url = urljoin(URL_GOBIERNO_MENORES, _texto_html(m.group("url")))
            if not re.search(r"\.(odt|pdf|ods|docx?|xlsx?|csv)$", urlparse(url).path, re.I):
                continue
            salida.append({"departamento": departamento, "organismo": organismo if m.group("nivel") != "2" else None,
                           "texto": _texto_html(m.group("texto")), "url": url})
    return salida


def _nombre_local_gobierno(url):
    """Ruta en raw/gobierno_resumen/ de un documento: la que tiene bajo
    .../actividad-contractual/menores/doc/ (xi-legislatura/pregob/...odt) o, si está en
    otro sitio, toda su ruta bajo otros/ (dos documentos con el mismo nombre en carpetas
    distintas no se pisan)."""
    camino = unquote(urlparse(url).path)
    if RAIZ_DOCUMENTOS_GOBIERNO in camino:
        return camino.split(RAIZ_DOCUMENTOS_GOBIERNO, 1)[1]
    return "otros/" + camino.lstrip("/")


def _nombre_alterno(local, url):
    """Nombre local de un enlace con el mismo nombre de fichero que otro:
    <nombre>_u<8 cifras de la huella de su URL>.<ext>. No depende del orden de la página."""
    ruta = Path(local)
    huella = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    return ruta.with_name(f"{ruta.stem}_u{huella}{ruta.suffix}").as_posix()


def _descargar_enlaces(raw, serie, enlaces, tipo, local_de, manifiesto, resumen, filtro, comprobar_todo):
    """Descarga los documentos `tipo` de una lista de enlaces y da por retirados los que se
    tenían y la página ya no enlaza. Los que faltan, los del año en curso y el anterior,
    los que cambian de URL y los que se tenían por retirados se piden siempre; los demás,
    con --comprobar-todo. Si dos enlaces distintos dan el mismo nombre de fichero se bajan
    los dos: el que ya lo tenía (por su URL en el manifiesto) lo conserva y los demás van a
    _nombre_alterno (así el orden de la página no cambia qué fichero es cuál)."""
    carpeta = Path(raw) / serie
    grupos = {}
    for enlace in enlaces:
        if not urlparse(enlace["url"]).path.lower().endswith("." + tipo):
            continue
        lista = grupos.setdefault(local_de(enlace["url"]), [])
        if all(e["url"] != enlace["url"] for e in lista):
            lista.append(enlace)
    if not grupos:
        resumen.fallidos.append(f"{serie}: la página no enlaza ningún {tipo.upper()}; no se descarga ni se retira nada")
        return
    elegidos = []
    for local, lista in grupos.items():
        if len(lista) > 1:
            dueno = manifiesto.get(f"{serie}/{local}").get("url")
            lista = sorted(lista, key=lambda e: e["url"] != dueno)
            resumen.avisos.append(f"{serie}: {len(lista)} enlaces distintos dan el mismo fichero {local}; se bajan "
                                  "todos (los siguientes al primero, con la huella de su URL en el nombre)")
        elegidos.append((local, lista[0]))
        elegidos += [(_nombre_alterno(local, e["url"]), e) for e in lista[1:]]
    vigentes, motivos = set(), {}
    for local, enlace in elegidos:
        destino = carpeta / local
        rel = manifiesto.rel(destino)
        vigentes.add(rel)
        anio, trimestre = periodo_de_texto(enlace["texto"], Path(urlparse(enlace["url"]).path).name)
        if not filtro.incluye(anio):
            continue
        extra = {"anio": anio, "trimestre": trimestre, "periodo": enlace["texto"],
                 "nombre_publicado": unquote(Path(urlparse(enlace["url"]).path).name)}
        if enlace.get("departamento"):
            extra["departamento"] = enlace["departamento"]
        if enlace.get("organismo"):
            extra["organismo"] = enlace["organismo"]
        info = manifiesto.get(rel)
        forzar = info.get("url") != enlace["url"] or info.get("publicado") is False
        if not _debe_pedirse(destino, anio, comprobar_todo, forzar):
            manifiesto.comprobar(rel)
            resumen.descarga(serie, rel, "sin_cambios")
            continue
        estado, detalle = descargar(enlace["url"], destino, tipo)
        time.sleep(PAUSA)
        if not _registrar_descarga(serie, destino, enlace["url"], estado, detalle, manifiesto, resumen, rel, **extra):
            vigentes.discard(rel)
            motivos[rel] = f"el portal da {detalle} ({enlace['url']})"
    fuera = {rel for rel, e in manifiesto.de_serie(serie).items() if not filtro.incluye(e.get("anio"))}
    _retirar_ausentes(manifiesto, resumen, serie, (vigentes | fuera) - set(motivos), "la página ya no lo enlaza",
                      motivos=motivos)


def descargar_scs(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    serie = "scs_resumen"
    print(f"\n📦 {TITULOS[serie]}")
    try:
        pagina = pedir_texto(URL_SCS)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{serie}: la página {URL_SCS} falla: {e}; no se descarga ni se retira nada")
        return
    finally:
        time.sleep(PAUSA)
    enlaces = enlaces_scs(pagina)
    if enlaces:
        guardar_json(Path(raw) / serie / "_enlaces.json", enlaces)
    _descargar_enlaces(raw, serie, enlaces, "ods", lambda url: unquote(Path(urlparse(url).path).name),
                       manifiesto, resumen, filtro, comprobar_todo)


def descargar_gobierno_resumen(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    serie = "gobierno_resumen"
    print(f"\n📦 {TITULOS[serie]}")
    try:
        pagina = pedir_texto(URL_GOBIERNO_MENORES)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{serie}: la página {URL_GOBIERNO_MENORES} falla: {e}; no se descarga ni se "
                                "retira nada")
        return
    finally:
        time.sleep(PAUSA)
    enlaces = enlaces_gobierno(pagina)
    if enlaces:
        guardar_json(Path(raw) / serie / "_enlaces.json", enlaces)
    _descargar_enlaces(raw, serie, enlaces, "odt", _nombre_local_gobierno, manifiesto, resumen, filtro,
                       comprobar_todo)


DESCARGAS = {"gobierno_contratos": descargar_gobierno_contratos, "las_palmas_gc": descargar_las_palmas,
             "cabildo_tenerife": descargar_tenerife, "scs_resumen": descargar_scs,
             "gobierno_resumen": descargar_gobierno_resumen}


def _lista_series(texto):
    series = [s.strip() for s in (texto or "").split(",") if s.strip()] or list(SERIES)
    desconocidas = [s for s in series if s not in SERIES]
    if desconocidas:
        raise argparse.ArgumentTypeError(f"series desconocidas: {', '.join(desconocidas)} (hay: {', '.join(SERIES)})")
    return series


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores de Canarias (Gobierno, SCS, "
                                                 "Las Palmas de Gran Canaria y Cabildo de Tenerife)")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--fuentes", type=_lista_series, default=list(SERIES),
                        help=f"series separadas por comas (por defecto todas: {','.join(SERIES)})")
    parser.add_argument("--desde", type=int, default=None, help="primer año que se descarga (por defecto todos)")
    parser.add_argument("--hasta", type=int, default=None, help="último año que se descarga (por defecto todos)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar los Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años cerrados y lo que CKAN no da por cambiado")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    filtro = Filtro(args.desde, args.hasta)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Series: {', '.join(args.fuentes)}\nAños: {args.desde or 'todos'}-{args.hasta or 'todos'}\n"
          f"Destino: {salida.resolve()}")
    resumen = Resumen(TITULO)
    try:
        manifiesto = Manifiesto(raw)
    except RuntimeError as e:
        resumen.fallidos.append(str(e))
        return resumen.cerrar(raw)
    if not manifiesto.existia and (any(ficheros_serie(raw, s) for s in SERIES) or any(salida.glob("*.parquet"))):
        # Sin él se perderían los retirados y los datos de cada versión (URL, periodo...)
        resumen.fallidos.append(f"{manifiesto.ruta} no existe y ya hay descargas o Parquet: no se descarga ni se "
                                "regenera nada (restaurarlo)")
        return resumen.cerrar(raw)
    if not args.solo_parquet:
        for serie in args.fuentes:
            DESCARGAS[serie](raw, manifiesto, resumen, filtro, args.comprobar_todo)
    if not args.solo_descarga:
        print("\n🧱 Generando Parquet...")
        for serie in args.fuentes:
            generar_parquet(salida, raw, serie, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
