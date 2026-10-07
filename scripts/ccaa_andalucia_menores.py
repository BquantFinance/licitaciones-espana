#!/usr/bin/env python3
"""
=============================================================================
ANDALUCÍA - CONTRATOS MENORES DE LA JUNTA (CKAN de datos abiertos)
=============================================================================
Descarga la serie «Contratación Menor en {año} publicada en la Plataforma de
Contratación de la Junta de Andalucía» del portal de datos abiertos de la
Junta (CKAN), tal como se publica (con todas sus versiones), y genera un
Parquet con todas las filas y columnas de todos los años, como texto.

Ejecutar:  python scripts/ccaa_andalucia_menores.py [--salida DIR] [--desde AÑO] [--hasta AÑO]
           [--solo-descarga] [--solo-parquet] [--comprobar-todo]

Qué es: los contratos menores (art. 118 LCSP) que se publican en la Plataforma
de Contratación de la Junta: consejerías, delegaciones, agencias, empresas y
fundaciones de la Junta y el Servicio Andaluz de Salud (SAS), uno por fila, con
órgano, nº de expediente, título, descripción, tipo, duración, NUTS, valor
estimado, importes sin y con IVA, financiación europea, licitadores, NIF y
nombre del adjudicatario, fechas de adjudicación y formalización y estado. Es
la misma plataforma que lee scripts/ccaa_andalucia.py por su buscador (JSON),
pero sin su tope de 10.000 resultados por consulta: aquí el SAS está entero
(98.976 de las 124.824 filas de 2025). No trae universidades, diputaciones ni
ayuntamientos: no publican en esa plataforma.

Salida (por defecto <repo>/ccaa_andalucia_menores/):
    raw/<año>/menores_<año>.csv       el CSV de cada año tal como se publica (2018-2023 van en
                                      ZIP: menores_<año>.csv.zip)
    raw/<año>/_historico/             versiones anteriores de cada fichero (nunca se borran)
    raw/catalogo_ckan.json            los conjuntos de la serie tal como los describe CKAN
                                      (package_show), con sus versiones en raw/_historico/
    raw/_manifiesto.json              por fichero anual: conjunto y recurso de CKAN, nombre
                                      publicado, URL, tamaño y fecha de modificación según CKAN,
                                      fecha de descarga, última comprobación, si se sigue
                                      publicando y los datos de CKAN de cada versión descargada
    raw/descarga_log.txt              resumen de cada ejecución (se añade al final)
    contratos_menores.parquet         todos los años: las columnas publicadas, como texto
    _historico/                       versiones anteriores del Parquet

Columnas: las publicadas, con su nombre y su valor tal cual, sin recortar:
- Desde 2023 cada campo va relleno con espacios hasta un ancho fijo
  (ORGANO_CONTRATACION a 500 caracteres, NIF_ADJUDICATARIO a 20...): de ahí que el
  CSV de 2025 pese 224 MB. Hasta 2022 solo va relleno ESTADO (y en 2018-2021, los
  títulos entrecomillados).
- Formatos de cada año, tal cual: importes con punto decimal salvo en 2025 (coma),
  a veces sin el cero ('.01', ',36'); fechas ISO con zona hasta 2024
  ('2024-03-01T00:00:00+0100') y dd/mm/aaaa en 2025-2026; el NIF, a veces con ';' al
  final (casi todos en 2022-2024: 'B12345678;'), con guion, enmascarado en personas
  físicas ('630****1948;') o extranjero.
- La cabecera de 2022 trae ' FECHA_ADJUDICACION' y ' FECHA_FORMALIZACION' con un
  espacio delante: son columnas aparte (nulas en los demás años).
- ID_EXPEDIENTE no es único: en 2021 y 2022 el SAS (expedientes '+6.…') y la Junta
  comparten números (5.645 y 6.912 repetidos) y en 2025 hay 35 contratos del IFAPA
  publicados dos veces con otro órgano. Se conservan todas las filas.
Añadidas por el script:
- _valor_estimado_num, _importe_adjudicacion_sin_iva_num e
  _importe_adjudicacion_con_iva_num: el importe publicado leído como número
  (float64; nulo si el texto no es un número con un solo separador decimal).
- _fuente (URL descargada), _conjunto y _recurso (CKAN), _anio (año del conjunto),
  _fichero_publicado (nombre del fichero en CKAN, que lleva la fecha de extracción),
  _miembro (CSV dentro del ZIP), _lineas_unidas (ver abajo), _archivo_origen (el
  fichero anual en raw/: <año>/menores_<año>, sin extensión), _fecha_descarga y, de
  comun/historico.py, _primera_descarga, _ultima_descarga y _en_ultima_descarga.

Lectura (verificada con los 9 ficheros del 2026-09-29: mismas filas y celdas con
valor que el original y el mismo texto, celda a celda):
- Codificación por fichero: 2018-2025 en cp1252 y 2026 en UTF-8 (se detecta con el
  fichero entero, como en ccaa_murcia.py; si casi todo es UTF-8, los bytes sueltos que
  no lo son se leen como cp1252 y se avisa). Los CSV en cp1252 traen además algún texto
  que la Junta publicó en UTF-8 ('reparaciÃ³n' en 2018, 'MensajerÃ­a' en 2023): se lee
  como cp1252, tal cual, y se avisa. Separador '|', CRLF, comillas estándar
  (un '|' dentro de un título entrecomillado es texto: 19 filas en 2025).
- Registros partidos: en 2020 un título trae un salto de línea sin comillas
  (expediente 533944). Leído tal cual salen dos filas con las columnas corridas (el
  NIF en FINANCIADO_POR, el estado en ADJUDICATARIO_DENOMINACION); se vuelven a unir
  con el salto de línea dentro del título y _lineas_unidas='2'. Solo se unen trozos
  que se quedan cortos y juntos suman exactamente los campos de la cabecera.

Sesgo del superviviente (docs/PRINCIPIOS.md, regla 3; comun/historico.py):
- Capa cruda: cada descarga pasa por guardar_version; si el contenido no cambia no
  se toca nada y si cambia la copia anterior va a _historico/. El fichero de un año
  es uno aunque la Junta lo sustituya por otro con otro nombre (menores_2025_v1_
  20260618.csv) o pase de ZIP a CSV: son versiones del mismo fichero anual.
- Parquet: se construye con el código actual desde TODAS las versiones de raw/, en
  orden, con acumular(): lo que el portal retira o cambia sigue con
  _en_ultima_descarga=False. _ultima_descarga es la fecha de la última versión que
  trae la fila (la última comprobación está en el manifiesto): una ejecución sin
  cambios en el portal no reescribe el Parquet.
- Retirado: si CKAN confirma que un conjunto ya no existe (package_show por su id,
  que no cambia aunque la Junta lo renombre, da 404) o el conjunto deja de tener ese
  CSV, sus filas pasan a _en_ultima_descarga=False. Si el catálogo falla o no
  devuelve ningún conjunto de la serie, no se retira nada. Si dos CSV de un año
  pasan a ser uno, las filas del que desaparece quedan retiradas aunque estén en el
  otro (son ficheros distintos).
- Una versión sin filas no retira nada; si es la vigente, es un error. Una versión sin
  las columnas ID_EXPEDIENTE y NUM_EXPEDIENTE (cabecera cambiada, u otro CSV subido
  por error) no se aplica: es un error y el año conserva lo que tenía. Una versión
  que cambia o retira más de la mitad de las filas vigentes se avisa: si la Junta
  vuelve a exportar un año con otro formato (relleno, decimales, fechas), todas sus
  filas salen como cambiadas (la versión anterior, con _en_ultima_descarga=False).
- Solo cuentan como versiones las copias que escribe el script: una copia de
  seguridad en raw/<año>/ ('menores_2019.csv.zip.bak_…') no. Si una versión no se
  puede leer, el año conserva en el Parquet las filas que tenía.
- Un manifiesto que no se puede leer para la ejecución (código 1) sin tocar nada: sin
  él se perderían los retirados y los datos de cada versión.
- No hay semilla: la fuente no está en el release v2026.02.

Qué se descarga (una petición cada vez, con pausa):
- package_search (q=menor) descubre los conjuntos de la serie por su nombre
  (contratacion-menor-plataforma-de-contratacion-andalucia-<año>) o su título
  («Contratación Menor en <año>…»); package_show da sus recursos al día. Un año que
  ya se tiene y la búsqueda no devuelve se pregunta por el id de su conjunto. Los demás
  conjuntos con 'menor' y 'contrat' se listan en los avisos (hoy, los menores del
  CTPDA).
- De cada conjunto, el recurso CSV (el JSON trae lo mismo y no se baja). CKAN lo
  anuncia en su host interno (gdc-pdpopendata-ckan.paas.junta-andalucia.es), que no
  resuelve desde fuera: se pide el mismo camino en www.juntadeandalucia.es.
- Se descargan los años que faltan, el año en curso y el anterior, y cualquier año
  cuyo recurso cambie en CKAN (id, nombre, tamaño o fecha de modificación: la Junta
  sustituye el fichero de un año durante el siguiente, p.ej. el de 2025 el
  2026-07-07); los demás, solo con --comprobar-todo. Cada descarga se comprueba con
  su Content-Length (o, si no lo trae, con el tamaño que da CKAN) y, si es un ZIP,
  con su índice; un gzip, 7z, RAR u otro comprimido no se toma por un CSV.
- Sale con 1 si no se pudo leer el catálogo, descargar o leer algo que existe, si un
  año confirmado (2018-2026) no está y no se tiene ninguna copia (o raw/ tiene copias
  que el manifiesto no conoce), si una versión no trae los identificadores, si la
  versión vigente de un año no tiene filas o si el manifiesto no se puede leer.

FUENTES
-------
Verificado en vivo en la descarga de producción el 2026-09-29 (confianza A):
  - https://www.juntadeandalucia.es/datosabiertos/portal/api/3/action/package_search
    y package_show (404 con «Not Found Error» si el conjunto no existe).
  - 9 conjuntos, 2018-2026 (organización economia-hacienda-y-fondos-europeos, CC BY 4.0),
    con un CSV y un JSON cada uno: 768.647 registros (768.648 filas: uno va partido),
    544.898 del SAS. Descarga completa en 5 min 41 s (525 MB); el Parquet (78 MB), en
    32 s con 2,1 GB de memoria.
Desde algunas redes www.juntadeandalucia.es corta la conexión (pasó en el entorno de
desarrollo): hay que ejecutarlo desde una red con acceso.
=============================================================================
"""

import argparse
import codecs
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, archivar, guardar_version  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

HOST_PORTAL = "www.juntadeandalucia.es"
RUTA_PORTAL = "/datosabiertos/portal"
URL_PORTAL = f"https://{HOST_PORTAL}{RUTA_PORTAL}"
URL_API = URL_PORTAL + "/api/3/action/"
CONSULTA_CKAN = "menor"          # package_search; la serie se reconoce por el nombre o el título
FILAS_CKAN = 100
# Conjuntos de la serie: por el nombre (contratacion-menor-plataforma-de-contratacion-andalucia-2025)
# o, si la Junta lo cambia, por el título («Contratación Menor en 2025 publicada en la Plataforma...»)
PATRON_NOMBRE = re.compile(r"^contratacion-menor-plataforma-de-contratacion-andalucia-((?:19|20)\d{2})$")
PATRON_TITULO = re.compile(r"^contratacion menor en ((?:19|20)\d{2})\b")
ANIO_MINIMO = 2018               # primer año de la serie (para anotar los años no publicados)
# Años publicados según el catálogo (verificado el 2026-09-29): si faltan y no se tienen, es un error
CONFIRMADOS = range(2018, 2027)

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_andalucia_menores"
PARQUET = "contratos_menores.parquet"
TITULO = "ANDALUCÍA - CONTRATOS MENORES DE LA JUNTA (CKAN)"


# ============================================================================
# UTILIDADES COMUNES (bloque de ccaa_murcia.py y ccaa_castilla_la_mancha.py
# adaptado: API de CKAN, CSV sueltos o en ZIP y Parquet por partes)
# ============================================================================

CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 60
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
PAUSA = 1.0                  # entre peticiones, para no cargar el portal
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")

# Columnas que tiene que traer toda versión del CSV de un año: sin ellas no se sabe
# qué contratos son los mismos (acumular compararía columnas que no los identifican)
IDENTIFICADORES = ("ID_EXPEDIENTE", "NUM_EXPEDIENTE")
# Importes publicados (texto) que el script lee además como número, en una
# columna aparte: _<columna en minúsculas>_num (float64; nulo si el texto no es
# un número). Cada año los publica con su formato: punto decimal (2018-2024 y
# 2026), coma decimal (2025) y a veces sin el cero ('.01', ',36').
IMPORTES = ("VALOR_ESTIMADO", "IMPORTE_ADJUDICACION_SIN_IVA", "IMPORTE_ADJUDICACION_CON_IVA")
DERIVADAS = tuple(f"_{c.lower()}_num" for c in IMPORTES)
# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL, con otro nombre de fichero, dentro de un ZIP
# o en una línea en vez de dos sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_conjunto", "_recurso", "_anio", "_fichero_publicado", "_miembro",
                    "_lineas_unidas", "_archivo_origen", "_fecha_descarga")
ORDEN_METADATOS = DERIVADAS + METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


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


def pedir_json(url, params=None):
    """GET a una API JSON con reintentos (red, 429, 5xx, respuesta no JSON).
    Un 4xx no se reintenta: se lanza ErrorPortal con el código."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.json()
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except ValueError as e:
            detalle = f"respuesta no JSON ({str(e)[:80]})"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes."""
    if cabeza.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith(b"%PDF"):
        return "pdf"
    if cabeza.startswith((b"\x1f\x8b", b"7z\xbc\xaf\x27\x1c", b"Rar!\x1a\x07", b"BZh", b"\xfd7zXZ\x00",
                          b"\x28\xb5\x2f\xfd")):
        return "comprimido"                               # gzip, 7z, rar, bz2, xz o zstd: no es un CSV
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
    es una hoja de cálculo (XLSX) o un archivo comprimido."""
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato != "zip":
        return formato
    try:
        with zipfile.ZipFile(ruta) as archivo:
            if any(n.startswith("xl/") for n in archivo.namelist()):
                return "xlsx"
    except (zipfile.BadZipFile, OSError):
        pass
    return "zip"


def _integridad(ruta, formato):
    """Motivo por el que un ZIP descargado está incompleto o dañado, o None."""
    if formato != "zip":
        return None
    try:
        with zipfile.ZipFile(ruta) as archivo:
            malo = archivo.testzip()
        return f"miembro dañado: {malo}" if malo else None
    except Exception as e:
        return f"{type(e).__name__}: {str(e)[:150]}"


def validar_contenido(ruta):
    """(motivo, reintentar): por qué la descarga no es el CSV (o el ZIP con el
    CSV) esperado —una página HTML servida con 200, un ZIP cortado...— o
    (None, False) si vale."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía", False
    formato = formato_fichero(ruta)
    if formato not in ("csv", "zip"):
        return f"la respuesta es {formato.upper()}, no un CSV", False
    danado = _integridad(ruta, formato)
    if danado:
        return f"fichero ZIP incompleto o dañado ({danado})", True
    return None, False


def descargar(url, destino, params=None, tamano=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que llega entero (por el Content-Length o,
    si el servidor no lo manda o comprime la respuesta, por el tamaño `tamano`
    que da CKAN) y que es un CSV o un ZIP sano (no una página HTML de error ni
    otro comprimido) y lo entrega a
    guardar_version(): si el contenido no cambió no se toca nada y si cambió la
    copia previa pasa a _historico/. Reintenta con backoff los fallos de red,
    429, 5xx y las descargas incompletas.
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es un CSV) o 'error'.
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
                        con_longitud = esperado.isdigit() and not cabeceras.get("Content-Encoding")
                        if con_longitud and int(esperado) != escritos:
                            detalle = f"descarga incompleta ({escritos:,} de {int(esperado):,} bytes)"
                        elif not con_longitud and isinstance(tamano, int) and tamano > 0 and tamano != escritos:
                            detalle = (f"descarga de {escritos:,} bytes y CKAN da {tamano:,} (sin Content-Length: "
                                       "puede estar cortada)")
                        else:
                            motivo, reintentar = validar_contenido(tmp)
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
    """Guarda metadatos del portal (catálogo) conservando sus versiones."""
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    return guardar_version(destino, contenido)


class Manifiesto:
    """raw/_manifiesto.json, por fichero anual (<año>/menores_<año>: el fichero que
    publica la Junta para un año, se llame como se llame y venga en CSV o en ZIP): de dónde sale,
    cuándo se descargó su versión actual (fecha_descarga), cuándo se comprobó
    por última vez (comprobado), si el portal lo sigue publicando y, en
    `versiones`, los datos de CKAN de cada versión descargada (por su fecha)."""

    def __init__(self, raw):
        self.raw = Path(raw)
        self.ruta = self.raw / "_manifiesto.json"
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

    def get(self, fichero):
        return self.datos.get(fichero, {})

    def ficheros_de(self, anio):
        return sorted(h for h, i in self.datos.items() if i.get("anio") == anio)

    def registrar(self, fichero, ruta, url, estado, **extra):
        entrada = self.datos.setdefault(fichero, {})
        entrada.update(extra)
        entrada["archivo"] = self.rel(ruta)
        entrada["url"] = url
        entrada["publicado"] = True
        entrada.pop("retirado_desde", None)
        entrada.pop("detalle", None)
        if estado == "sin_cambios" and entrada.get("fecha_descarga"):
            entrada["comprobado"] = iso(ahora())
        else:
            fecha = fecha_version(ruta)
            entrada["fecha_descarga"] = entrada["comprobado"] = fecha
            entrada.setdefault("versiones", {})[fecha] = dict(extra, url=url, archivo=self.rel(ruta))
        self.guardar()

    def retirar(self, fichero, detalle):
        entrada = self.datos.setdefault(fichero, {})
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
    """Lo descargado, lo que no existe (años sin conjunto), lo retirado y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = []
        self.no_publicados = []
        self.retirados = []
        self.avisos = []
        self.fallidos = []
        self.parquets = []

    def descarga(self, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios.append(etiqueta)
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
        bloque("SIN CAMBIOS", self.sin_cambios)
        if self.no_publicados:
            anios = ", ".join(str(a) for a in sorted(self.no_publicados))
            lineas.append(f"\nAÑOS SIN CONJUNTO EN EL CATÁLOGO: {anios}")
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


# ----------------------------------------------------------------------------
# Lectura de los CSV como texto (todas las filas y columnas, sin convertir nada)
# ----------------------------------------------------------------------------

# Letras del castellano que no son ASCII: deciden entre las codificaciones de un byte
_LETRAS_ES = re.compile("[ÁÉÍÓÚÑÜáéíóúñü]")
# De un byte, por preferencia a igualdad de letras: cp1252 antes que latin-1 (latin-1 dejaría
# '€', '’'... como caracteres de control) y cp850 la última
_UN_BYTE = ("cp1252", "latin-1", "cp850")


# UTF-8 con algún byte suelto que no lo es: esos bytes se leen como cp1252 (latin-1 los cinco
# que cp1252 no define). La Junta mezcla codificaciones: los CSV de 2018-2025 (cp1252) traen algo
# de texto en UTF-8 y el de 2026 es UTF-8; un solo byte cp1252 en un UTF-8 no puede estropearlo entero.
UTF8_CON_SUELTOS = "utf-8+cp1252"
_SIN_CP1252 = (0x81, 0x8D, 0x8F, 0x90, 0x9D)
_NO_ASCII_UTF8 = re.compile("[\u0080-\ud7ff\ue000-\U0010ffff]")
_BYTE_SUELTO = re.compile("[\udc80-\udcff]")      # surrogateescape: un byte que no es UTF-8


def _bytes_sueltos(error):
    trozo = error.object[error.start:error.end]
    return "".join(bytes([b]).decode("latin-1" if b in _SIN_CP1252 else "cp1252") for b in trozo), error.end


codecs.register_error("andalucia_bytes_sueltos", _bytes_sueltos)


def _abrir(ruta, codificacion):
    """El CSV como texto con la codificación detectada (UTF8_CON_SUELTOS: UTF-8 y los
    bytes sueltos como cp1252)."""
    if codificacion == UTF8_CON_SUELTOS:
        return open(ruta, encoding="utf-8-sig", errors="andalucia_bytes_sueltos", newline="")
    return open(ruta, encoding=codificacion, newline="")


def _detectar_codificacion(ruta, avisos=None, nombre=None):
    """UTF-8 (o UTF-16) si decodifica el fichero COMPLETO. Si casi todo es UTF-8 (al menos
    10 caracteres en UTF-8 por cada byte suelto que no lo es), UTF8_CON_SUELTOS. Si no, la
    codificación de un byte que da más letras del castellano (á, Ñ...): cp1252, latin-1 y
    cp850 aceptan casi cualquier byte, y la equivocada no falla, cambia las letras (como en
    ccaa_murcia.py). Las mezclas se anotan en `avisos`."""
    with open(ruta, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    utf8 = "utf-8-sig" if inicio.startswith(b"\xef\xbb\xbf") else "utf-8"
    nombre = nombre or Path(ruta).name
    validos = sueltos = 0
    decodificador = codecs.getincrementaldecoder(utf8)(errors="surrogateescape")
    with open(ruta, "rb") as f:
        while True:
            bloque = f.read(1 << 20)
            texto = decodificador.decode(bloque, final=not bloque)
            validos += len(_NO_ASCII_UTF8.findall(texto))
            sueltos += len(_BYTE_SUELTO.findall(texto))
            if not bloque:
                break
    if not sueltos:
        return utf8
    if validos >= 10 * sueltos:
        if avisos is not None:
            avisos.append(f"{nombre}: UTF-8 con {sueltos:,} bytes sueltos que no lo son (y {validos:,} caracteres "
                          "en UTF-8): esos bytes se leen como cp1252")
        return UTF8_CON_SUELTOS
    letras = {}
    for codificacion in _UN_BYTE:
        decodificador = codecs.getincrementaldecoder(codificacion)()
        n = 0
        try:
            with open(ruta, "rb") as f:
                while bloque := f.read(1 << 20):
                    n += len(_LETRAS_ES.findall(decodificador.decode(bloque)))
            decodificador.decode(b"", final=True)
        except UnicodeDecodeError:
            continue
        letras[codificacion] = n
    elegida = max(letras, key=lambda c: (letras[c], -_UN_BYTE.index(c)))
    if validos and avisos is not None:
        avisos.append(f"{nombre}: {validos:,} caracteres en UTF-8 dentro de un fichero en {elegida}: se leen como "
                      f"{elegida}, tal cual")
    return elegida


def _detectar_separador(ruta, codificacion):
    """Separador más frecuente (fuera de comillas) en la primera línea con texto."""
    with _abrir(ruta, codificacion) as f:
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


def _salto_de_linea(ruta, codificacion):
    """Fin de línea del fichero ('\\r\\n', '\\n' o '\\r'), por su primer bloque."""
    with _abrir(ruta, codificacion) as f:
        inicio = f.read(1 << 16)
    return "\r\n" if "\r\n" in inicio else "\n" if "\n" in inicio else "\r" if "\r" in inicio else "\n"


def unir_partidos(registros, ancho, salto="\r\n"):
    """Registros de datos (sin la cabecera) con los que un salto de línea sin
    comillas dentro de un campo parte en varias líneas vueltos a unir, y cuántas
    líneas ocupa cada uno (None si una). En 2020 la Junta publica así un título
    (expediente 533944): leído tal cual salen dos filas, una con 4 campos y otra
    con 20 corridos a la izquierda (el NIF en FINANCIADO_POR, el estado en
    ADJUDICATARIO_DENOMINACION...). Solo se unen trozos seguidos que se quedan
    todos cortos y que juntos, con el salto de línea (`salto`) dentro del campo
    en el que cae, suman exactamente `ancho` campos, y ninguno de los siguientes
    empieza como un registro (un primer campo solo de cifras, como ID_EXPEDIENTE):
    dos líneas truncadas no se pegan entre sí. Si no, se dejan como están."""
    salida, lineas, i, n = [], [], 0, len(registros)
    while i < n:
        campos = registros[i]
        if len(campos) < ancho:
            unido, j = list(campos), i + 1
            while (len(unido) < ancho and j < n and len(registros[j]) < ancho
                   and not registros[j][0].strip().isdigit()):
                siguiente = registros[j]
                unido = unido[:-1] + [unido[-1] + salto + siguiente[0]] + list(siguiente[1:])
                j += 1
            if j - i > 1 and len(unido) == ancho:
                salida.append(unido)
                lineas.append(j - i)
                i = j
                continue
        salida.append(campos)
        lineas.append(None)
        i += 1
    return salida, lineas


def _registros(ruta, sep, codificacion):
    """Registros del CSV con el módulo csv, sin cargar el texto entero."""
    csv.field_size_limit(max(csv.field_size_limit(), 1 << 30))
    with _abrir(ruta, codificacion) as f:
        return [r for r in csv.reader(f, delimiter=sep) if r]


def leer_csv(ruta, nombre=None):
    """CSV como texto, sin perder filas ni campos: solo el campo vacío es nulo
    ('NA', 'N/A', 'NULL'... se quedan como texto) y cada valor va tal cual, con
    sus espacios. Se lee registro a registro con el módulo csv (comillas
    estándar: un '|' o un salto de línea entre comillas son parte del campo):
    - una comilla literal al principio de un campo que se tragaría registros
      enteros se lee como texto (comun.lectura_csv);
    - un registro partido por un salto de línea sin comillas se vuelve a unir
      (unir_partidos) y lleva en _lineas_unidas cuántas líneas ocupa;
    - los campos que sobran respecto a la cabecera van a _columna_extra_N y a un
      registro con menos campos le faltan los últimos (nulos), con un aviso."""
    ruta = Path(ruta)
    nombre = nombre or ruta.name
    avisos = []
    codificacion = _detectar_codificacion(ruta, avisos, nombre)
    sep = _detectar_separador(ruta, codificacion)
    try:
        registros = _registros(ruta, sep, codificacion)
    except csv.Error:
        registros = None                  # p.ej. un byte nulo: se lee con el lector tolerante
    lineas = _lineas_de_datos(ruta) if codificacion != "utf-16" else None
    if registros is None or (lineas is not None and lineas != len(registros) - 1):
        # Puede ser una comilla literal que se traga registros enteros:
        # se vuelve a leer sin que lo haga (comun.lectura_csv)
        with _abrir(ruta, codificacion) as f:
            tolerantes, literales = registros_csv(f.read(), sep)
        tolerantes = [r for r in tolerantes if r != [""]]
        if registros is None or (literales and len(tolerantes) > len(registros)):
            if literales:
                avisos.append(f"{nombre}: {literales:,} comillas literales al principio de un campo se leen como "
                              "texto (se conservan; si no, se tragarían los registros siguientes)")
            registros = tolerantes
    if not registros:
        return pd.DataFrame(), [f"{nombre}: fichero sin cabecera ni filas"]
    cabecera, datos = registros[0], registros[1:]
    ancho = len(cabecera)
    datos, unidas = unir_partidos(datos, ancho, _salto_de_linea(ruta, codificacion))
    n_unidos = sum(1 for u in unidas if u)
    if n_unidos:
        ejemplos = [datos[k][0] for k, u in enumerate(unidas) if u][:5]
        avisos.append(f"{nombre}: {n_unidos:,} registros partidos por un salto de línea sin comillas dentro de un "
                      f"campo se vuelven a unir (_lineas_unidas; primer campo: {', '.join(ejemplos)})")
    largo = max(len(r) for r in [cabecera] + datos)
    nombres = _nombres_columnas(cabecera) + [f"_columna_extra_{k}" for k in range(1, largo - ancho + 1)]
    con_extra = sum(1 for r in datos if len(r) > ancho)
    cortos = sum(1 for r in datos if len(r) < ancho)
    if con_extra:
        avisos.append(f"{nombre}: {con_extra:,} filas con más campos que la cabecera; los campos de más se "
                      "conservan en columnas _columna_extra_N")
    if cortos:
        avisos.append(f"{nombre}: {cortos:,} filas con menos campos que la cabecera (les faltan los últimos): revisar")
    df = pd.DataFrame([[v if v != "" else None for v in r] + [None] * (largo - len(r)) for r in datos],
                      columns=nombres, dtype=object)
    if n_unidos:
        df["_lineas_unidas"] = [None if u is None else str(u) for u in unidas]
    if lineas is not None and lineas != len(df) + sum(u - 1 for u in unidas if u):
        avisos.append(f"{nombre}: {len(df):,} filas leídas de {lineas:,} líneas de datos "
                      "(campos entrecomillados con saltos de línea o comillas desparejadas): revisar")
    return df, avisos


def _nombre_zip(info):
    """Nombre de un miembro de un ZIP: UTF-8 si lo declara; si no, el ZIP lo
    guarda en la página de códigos OEM de Windows (cp850 en España)."""
    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode("cp437").decode("cp850")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return info.filename


def _ignorable(miembro):
    """Restos de otros sistemas dentro de un archivo, que no son datos."""
    partes = Path(miembro.replace("\\", "/")).parts
    return (any(p == "__MACOSX" for p in partes) or partes[-1].startswith(("._", "~$"))
            or partes[-1].lower() in ("thumbs.db", ".ds_store", "desktop.ini"))


def leer_zip(ruta):
    """CSV de un ZIP, unidos, con el nombre de cada uno en _miembro. Los miembros
    que no son CSV se anotan en los avisos y se quedan en el original."""
    ruta = Path(ruta)
    avisos, partes, leidos = [], [], 0
    with zipfile.ZipFile(ruta) as archivo, tempfile.TemporaryDirectory(prefix="and_men_") as tmp:
        for i, info in enumerate(archivo.infolist()):
            if info.is_dir():
                continue
            miembro = _nombre_zip(info)
            nombre = f"{ruta.name}/{miembro}"
            if _ignorable(miembro):
                avisos.append(f"{nombre}: se ignora (no son datos)")
                continue
            if not miembro.lower().endswith(".csv"):
                avisos.append(f"{nombre}: no es un CSV; se conserva en el original sin leer")
                continue
            destino = Path(tmp) / f"{i:05d}.csv"
            with archivo.open(info) as origen, open(destino, "wb") as f:
                shutil.copyfileobj(origen, f, 1 << 20)
            df, avisos_miembro = leer_csv(destino, nombre)
            avisos.extend(avisos_miembro)
            leidos += 1
            if len(df):
                df["_miembro"] = miembro
                partes.append(df)
            destino.unlink()
    if not leidos:
        raise ValueError(f"{ruta.name}: el ZIP no contiene ningún CSV")
    if not partes:
        return pd.DataFrame(columns=["_miembro"]), avisos
    return pd.concat(partes, ignore_index=True, sort=False), avisos


def leer_version(ruta):
    """Una versión del fichero de un año (CSV o ZIP con el CSV, según su
    contenido) con todas sus filas y columnas como texto. Devuelve (df, avisos)."""
    ruta = Path(ruta)
    formato = formato_fichero(ruta)
    if formato == "zip":
        return leer_zip(ruta)
    if formato == "csv":
        return leer_csv(ruta)
    raise ValueError(f"{ruta.name}: formato {formato.upper()} (se esperaba un CSV o un ZIP con el CSV)")


# ----------------------------------------------------------------------------
# Registros acumulados (comun/historico.py) y Parquet
# ----------------------------------------------------------------------------

# Copias que escribe el script: la actual (menores_2023.csv.zip, menores_2025.csv) y las de
# _historico/ con el sello de guardar_version (menores_2023.csv__20260929T101500Z.zip,
# menores_2025__20260929T101500Z.csv, con _N si dos coinciden)
PATRON_COPIA = re.compile(r"(menores_\d{4}(?:_[0-9A-Za-z-]+)?)(?:\.csv)?(?:__\d{8}T\d{6}Z(?:_\d+)?)?\.(?:csv|zip)")


def base_de(nombre):
    """Nombre del fichero anual al que pertenece una copia cruda, sin el sello de
    _historico/ ni la extensión ('menores_2023.csv__20260929T101500Z.zip' ->
    'menores_2023'), o None si no es una copia del script: una copia de
    seguridad ('menores_2019.csv.zip.bak_…') o cualquier otro fichero de raw/<año>/
    no cuenta como versión."""
    m = PATRON_COPIA.fullmatch(nombre)
    return m.group(1) if m else None


def versiones_fichero(raw, fichero):
    """Versiones guardadas de un fichero anual (<año>/menores_<año>), de la más
    antigua a la actual, sea cual sea su extensión: si la Junta pasa de publicar
    un ZIP a un CSV suelto, los dos son versiones del mismo fichero."""
    carpeta = Path(raw) / Path(fichero).parent
    base = Path(fichero).name
    lista = []
    for directorio in (carpeta, carpeta / HISTORICO):
        if directorio.is_dir():
            lista += [r for r in directorio.iterdir() if r.is_file() and base_de(r.name) == base]
    return sorted(lista, key=lambda r: (fecha_version(r), r.parent.name != HISTORICO, r.name))


def ficheros_en_raw(raw):
    """Ficheros anuales (<año>/menores_<año>...) con alguna copia en raw/<año>/."""
    ficheros = set()
    raw = Path(raw)
    if not raw.is_dir():
        return ficheros
    for carpeta in raw.iterdir():
        if not (carpeta.is_dir() and carpeta.name.isdigit()):
            continue
        for directorio in (carpeta, carpeta / HISTORICO):
            if directorio.is_dir():
                ficheros |= {f"{carpeta.name}/{base_de(r.name)}" for r in directorio.iterdir()
                             if r.is_file() and base_de(r.name)}
    return ficheros


def _como_texto(serie):
    """Serie de texto (None = nulo) para escribirla como string en Parquet."""
    if isinstance(serie.dtype, pd.StringDtype):
        return serie
    valores = serie.astype(object)
    if pd.api.types.infer_dtype(valores, skipna=True) in ("string", "empty"):
        return valores
    return valores.map(lambda v: v if isinstance(v, str) else (None if pd.isna(v) else str(v)))


PATRON_IMPORTE = re.compile(r"-?(?:\d+(?:[.,]\d*)?|[.,]\d+)")
# Un solo separador con tres cifras detrás ('15.000', '1,234'): miles o decimales, no se sabe
PATRON_AMBIGUO = re.compile(r"-?[1-9]\d{0,2}[.,]\d{3}")


def importes_num(serie):
    """Importes publicados como texto -> float64. Vale un número con un solo
    separador decimal, punto o coma ('14999.04', '39567,5', '.01', ',36'),
    con los espacios de relleno; cualquier otra cosa (texto, dos separadores,
    separador de miles o un separador con tres cifras detrás, como '15.000', que
    puede ser de miles) queda nula: la columna derivada nunca se inventa un
    importe, y el texto publicado sigue en su columna."""
    serie = pd.Series(serie)
    numeros = []
    for valor in serie.astype(object):
        texto = valor.strip() if isinstance(valor, str) else ""
        valido = PATRON_IMPORTE.fullmatch(texto) and not PATRON_AMBIGUO.fullmatch(texto)
        numeros.append(float(texto.replace(",", ".")) if valido else float("nan"))
    return pd.Series(numeros, index=serie.index, dtype="float64")


def anadir_derivadas(df):
    """Añade las columnas DERIVADAS. Toma el importe de la columna publicada con
    ese nombre, también si llega con espacios alrededor del nombre (como las
    fechas en la cabecera de 2022)."""
    for columna, derivada in zip(IMPORTES, DERIVADAS):
        fuentes = [c for c in df.columns if isinstance(c, str) and c.strip() == columna and c not in DERIVADAS]
        if not fuentes:
            df[derivada] = pd.Series(float("nan"), index=df.index, dtype="float64")
            continue
        texto = df[fuentes[0]].astype(object)
        for otra in fuentes[1:]:
            texto = texto.where(texto.notna(), df[otra].astype(object))
        df[derivada] = importes_num(texto).to_numpy()
    return df


def ordenar_columnas(df):
    """Columnas del portal (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def _tipo_arrow(columna):
    return pa.bool_() if columna == "_en_ultima_descarga" else pa.float64() if columna in DERIVADAS else pa.string()


def _tabla_arrow(df):
    """Tabla Arrow con todas las columnas como texto (_en_ultima_descarga
    booleana y los importes derivados float64)."""
    df = ordenar_columnas(df).copy()
    campos = []
    for columna in df.columns:
        tipo = _tipo_arrow(columna)
        if tipo == pa.bool_():
            df[columna] = df[columna].astype(bool)
        elif tipo == pa.float64():
            df[columna] = pd.to_numeric(df[columna], errors="coerce").astype("float64")
        else:
            df[columna] = _como_texto(df[columna])
        campos.append(pa.field(str(columna), tipo))
    return pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)


def _origenes_previos(destino):
    """Valores de _archivo_origen del Parquet anterior (sin cargarlo entero)."""
    if not Path(destino).exists():
        return []
    columna = pq.read_table(destino, columns=["_archivo_origen"]).column(0)
    return [str(v) for v in columna.unique().to_pylist() if v is not None]


def _filas_previas(destino, fichero):
    """Filas de un fichero anual en el Parquet anterior (o None)."""
    tabla = pq.read_table(destino, filters=[("_archivo_origen", "==", fichero)])
    return tabla.to_pandas() if tabla.num_rows else None


def unir_partes(partes, destino, previas=()):
    """Escribe `destino` con las partes (Parquet temporales, una por fichero anual) en una
    sola tabla: la unión de sus columnas, nulas donde una parte no las trae. Las
    del Parquet anterior (`previas`) siguen en su orden (nunca se pierde una
    columna, aunque esté vacía, y regenerar sin cambios da el mismo fichero) y
    las nuevas van detrás por orden de aparición; al final, las del script
    (ORDEN_METADATOS). Se
    escribe parte a parte (sin cargar todo en memoria) en un temporal que pasa
    por guardar_version: la versión anterior queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    nombres = list(dict.fromkeys(list(previas) + [c for parte in partes for c in pq.read_schema(parte).names]))
    orden = [c for c in nombres if c not in ORDEN_METADATOS] + [c for c in ORDEN_METADATOS if c in nombres]
    esquema = pa.schema([pa.field(c, _tipo_arrow(c)) for c in orden])
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


def acumular_fichero(raw, fichero, manifiesto, resumen):
    """Registros del fichero de un año a lo largo de todas sus versiones en raw/,
    leídas con el código actual (regla 3) y aplicadas en orden cronológico con
    acumular(): nada de lo visto se pierde; lo que el portal retira o cambia
    queda con _en_ultima_descarga=False. Si el portal ha retirado el fichero,
    todas sus filas pasan a _en_ultima_descarga=False. Una versión que no se
    puede leer queda en los errores y se vuelve a intentar la próxima vez.
    Devuelve (registros, nº de versiones que no se han podido leer).
    _ultima_descarga es la fecha de la última versión que trae la fila, no la
    de la última comprobación (esa está en el manifiesto): así una ejecución
    sin cambios en el portal no reescribe el Parquet.
    Una versión sin filas no retira nada (casi siempre es un fallo de la
    publicación); si es la vigente, además, es un error: hay que revisarla.
    Una versión sin las columnas IDENTIFICADORES (cabecera cambiada o un CSV que
    no es el de menores) no se aplica: es un error y el fichero conserva lo que
    tenía. Una que cambia o retira más de la mitad de las filas vigentes se avisa
    (¿otro formato, relleno o una publicación parcial?)."""
    info = manifiesto.get(fichero)
    anio = Path(fichero).parent.name
    registros, ilegibles = None, 0
    lista = versiones_fichero(raw, fichero)
    for version in lista:
        fecha = fecha_version(version)
        try:
            df, avisos = leer_version(version)
        except Exception as e:
            resumen.fallidos.append(f"{fichero}: no se pudo leer la versión {version.name}: {e}")
            ilegibles += 1
            continue
        resumen.avisos.extend(avisos)
        faltan = [c for c in IDENTIFICADORES if c not in df.columns]
        if faltan:
            resumen.fallidos.append(f"{fichero}: la versión del {fecha} ({version.name}) no trae {', '.join(faltan)} "
                                    f"(cabecera: {', '.join(map(str, list(df.columns)[:6]))}…); no se aplica: revisar")
            ilegibles += 1
            continue
        datos = (info.get("versiones") or {}).get(fecha) or {}
        df["_fuente"] = datos.get("url")
        df["_conjunto"] = datos.get("conjunto")
        df["_recurso"] = datos.get("recurso")
        df["_anio"] = anio
        df["_fichero_publicado"] = datos.get("nombre_publicado")
        for columna in ("_miembro", "_lineas_unidas"):
            if columna not in df.columns:
                df[columna] = None
        df["_archivo_origen"] = fichero
        df["_fecha_descarga"] = fecha
        try:
            vigentes = 0 if registros is None else int(registros["_en_ultima_descarga"].sum())
            registros = acumular(registros, df, fecha, ignorar=METADATOS_ORIGEN)
            siguen = int((registros["_en_ultima_descarga"] & (registros["_primera_descarga"] < fecha)).sum())
            if vigentes and (vigentes - siguen) * 2 > vigentes:
                resumen.avisos.append(f"{fichero}: la versión del {fecha} cambia o retira {vigentes - siguen:,} de las "
                                      f"{vigentes:,} filas vigentes (más de la mitad): ¿otro formato (relleno, "
                                      "decimales, fechas) o una publicación parcial? revisar")
        except ValueError:
            mensaje = (f"{fichero}: la versión del {fecha} ({version.name}) no tiene filas; "
                       "no se marca nada como retirado")
            if version == lista[-1]:
                resumen.fallidos.append(mensaje + " (es la versión vigente: revisar el fichero publicado)")
            else:
                resumen.avisos.append(mensaje)
    if registros is None or not len(registros):
        return registros, ilegibles
    comprobado = info.get("comprobado")
    if info.get("publicado") is False and comprobado:
        registros = acumular(registros, pd.DataFrame(), comprobado, ignorar=METADATOS_ORIGEN, permitir_vacio=True)
    return anadir_derivadas(registros), ilegibles


def generar_parquet(salida, raw, manifiesto, resumen):
    """Genera el Parquet con los registros acumulados de todos los ficheros
    anuales de raw/, cada uno desde todas sus versiones con el código actual.

    Se procesa fichero a fichero y cada resultado va a un Parquet temporal (un año
    con relleno ocupa cientos de MB en memoria). Las filas de un fichero anual del
    que ya no queda ninguna copia en raw/, o con alguna versión que no se ha podido
    leer (sale en los errores), se copian tal cual del Parquet anterior: un fallo
    de lectura no quita filas de la salida.
    Devuelve (filas, columnas) o None si no hay nada que escribir.
    """
    print("\n🧱 Generando Parquet...")
    destino = Path(salida) / PARQUET
    try:
        previos = _origenes_previos(destino)
        columnas_previas = pq.read_schema(destino).names if destino.exists() else []
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    en_raw = ficheros_en_raw(raw)
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

        # En orden de año; un fichero del que ya no queda ninguna copia en raw/, en su sitio
        for fichero in sorted(en_raw | set(previos)):
            if fichero in en_raw:
                registros, ilegibles = acumular_fichero(raw, fichero, manifiesto, resumen)
                if not ilegibles or fichero not in previos:
                    if registros is not None and len(registros):
                        print(f"  {fichero}: {len(registros):,} filas")
                        guardar_parte(registros)
                    continue
                motivo = f"{ilegibles} versiones no se han podido leer"
            else:
                motivo = "ya no está en raw/"
            grupo = _filas_previas(destino, fichero)
            resumen.avisos.append(f"{destino.name}: {fichero} {motivo}; se conservan sus {len(grupo):,} filas "
                                  "del Parquet anterior")
            guardar_parte(grupo)
        if not partes:
            return None
        estado, columnas = unir_partes(partes, destino, columnas_previas)
    resumen.parquets.append((destino.name, filas, columnas, retiradas))
    print(f"  💾 {destino.name}: {filas:,} filas ({estado})")
    return filas, columnas


# ============================================================================
# CKAN DE LA JUNTA: DESCUBRIMIENTO Y DESCARGA
# ============================================================================

def ckan(accion, **params):
    """Llamada a la API de CKAN. Devuelve `result`; lanza ErrorPortal si falla
    (codigo=404 si el conjunto no existe)."""
    datos = pedir_json(URL_API + accion, params or None)
    if not isinstance(datos, dict) or datos.get("success") is not True:
        error = (datos.get("error") if isinstance(datos, dict) else None) or {}
        codigo = 404 if error.get("__type") == "Not Found Error" else None
        raise ErrorPortal(f"CKAN {accion}: {error.get('message') or 'respuesta sin éxito'}", codigo)
    return datos.get("result")


def _normalizar(texto):
    return " ".join(sin_acentos(texto).lower().split())


def anio_de_conjunto(paquete):
    """Año de un conjunto de la serie (por su nombre o, si no, por su título), o None."""
    m = PATRON_NOMBRE.match(str(paquete.get("name") or ""))
    if m:
        return int(m.group(1))
    m = PATRON_TITULO.match(_normalizar(paquete.get("title") or ""))
    return int(m.group(1)) if m else None


def nombre_publicado(url):
    """Nombre del fichero publicado (último tramo de la URL, sin %xx)."""
    return unquote(Path(urlparse(url).path).name)


def extension(nombre):
    """Extensión del fichero local de un año según el nombre publicado."""
    nombre = nombre.lower()
    return next((e for e in (".csv.zip", ".zip", ".csv") if nombre.endswith(e)), ".csv")


def url_descarga(url):
    """URL pública de un recurso. Los ficheros subidos a este CKAN se sirven en
    el portal (https://www.juntadeandalucia.es/datosabiertos/portal/dataset/...),
    pero CKAN los anuncia con su host interno (gdc-pdpopendata-ckan.paas.
    junta-andalucia.es), que no resuelve desde fuera: se pide el mismo camino
    en el portal. Una URL de otro sitio se deja tal cual."""
    partes = urlparse(url)
    if partes.path.startswith(RUTA_PORTAL + "/"):
        return urlunparse(("https", HOST_PORTAL, partes.path, partes.params, partes.query, ""))
    return url


def recursos_csv(paquete):
    """Recursos CSV de un conjunto (el JSON trae lo mismo y no se descarga), por posición."""
    lista = []
    for recurso in sorted(paquete.get("resources") or [], key=lambda r: r.get("position") or 0):
        if recurso.get("state") not in (None, "active"):
            continue
        nombre = nombre_publicado(recurso.get("url") or "").lower()
        if str(recurso.get("format") or "").strip().lower() == "csv" or nombre.endswith((".csv", ".csv.zip")):
            lista.append(recurso)
    return lista


def asignar_ficheros(anio, recursos, manifiesto):
    """[(fichero anual, (paquete, recurso))] de los recursos CSV de un año. Con uno
    solo (lo normal) va a <año>/menores_<año> aunque cambie su id: cuando la Junta
    sustituye el CSV de un año por otro, es otra versión del mismo fichero. Con
    varios, cada recurso conserva el fichero anual que ya tenía y los nuevos van
    al principal si está libre o a <año>/menores_<año>_<id>."""
    base = f"{anio}/menores_{anio}"
    if len(recursos) == 1:
        return [(base, recursos[0])]
    previos = {manifiesto.get(h).get("recurso"): h for h in manifiesto.ficheros_de(anio)}
    asignados, usados = {}, set()
    for i, (_, recurso) in enumerate(recursos):
        fichero = previos.get(recurso.get("id"))
        if fichero and fichero not in usados:
            asignados[i] = fichero
            usados.add(fichero)
    for i, (_, recurso) in enumerate(recursos):
        if i not in asignados:
            fichero = base if base not in usados else f"{base}_{str(recurso.get('id') or i)[:8]}"
            asignados[i] = fichero
            usados.add(fichero)
    return [(asignados[i], recursos[i]) for i in range(len(recursos))]


def descargar_fichero(raw, fichero, anio, paquete, recurso, manifiesto, resumen, comprobar_todo=False):
    """Descarga el CSV de un año si no se tiene, si es del año en curso o del
    anterior (siguen creciendo), si CKAN dice que ha cambiado (otro recurso,
    nombre, tamaño o fecha de modificación) o con --comprobar-todo."""
    info = manifiesto.get(fichero)
    url_ckan = (recurso.get("url") or "").strip()
    nombre = nombre_publicado(url_ckan)
    datos = {"anio": anio, "conjunto": paquete.get("name"), "id_conjunto": paquete.get("id"),
             "recurso": recurso.get("id"), "nombre_publicado": nombre, "url_ckan": url_ckan,
             "tamano_ckan": recurso.get("size"), "modificado_ckan": recurso.get("last_modified")}
    if not url_ckan:
        resumen.fallidos.append(f"{fichero}: el recurso {recurso.get('id')} de {paquete.get('name')} no tiene URL")
        return
    url = url_descarga(url_ckan)
    destino = Path(raw) / f"{fichero}{extension(nombre)}"
    actual = Path(raw) / info["archivo"] if info.get("archivo") else None
    igual = all(info.get(k) == datos[k] for k in ("recurso", "nombre_publicado", "tamano_ckan", "modificado_ckan"))
    if (actual is not None and actual.exists() and igual and info.get("publicado", True)
            and not comprobar_todo and anio < ahora().year - 1):
        resumen.sin_cambios.append(f"{fichero} ({nombre}; ya descargado y sin cambios en CKAN: "
                                   "--comprobar-todo para volver a pedirlo)")
        return
    if actual is not None and actual != destino and destino.exists():
        # Una copia de este formato de antes de `actual` (p.ej. el ZIP de antes del CSV): a _historico/, para
        # que si vuelve el mismo contenido sea la última versión y no quede 'sin_cambios' con su fecha antigua
        archivar(destino)
    print(f"  ⬇️ {fichero}: {url}")
    estado, detalle = descargar(url, destino, tamano=datos["tamano_ckan"])
    time.sleep(PAUSA)
    if estado not in ESTADOS_OK:
        resumen.fallidos.append(f"{fichero}: {detalle or estado} ({url})")
        print(f"  ❌ {fichero}: {detalle or estado}")
        return
    if actual is not None and actual != destino and actual.exists():
        resumen.avisos.append(f"{fichero}: el fichero publicado cambia de formato ({actual.name} -> {destino.name}); "
                              "las dos son versiones del mismo año")
    tamano = destino.stat().st_size
    if isinstance(datos["tamano_ckan"], int) and datos["tamano_ckan"] != tamano:
        resumen.avisos.append(f"{fichero}: CKAN da {datos['tamano_ckan']:,} bytes y se han descargado {tamano:,} "
                              f"({nombre})")
    manifiesto.registrar(fichero, destino, url, estado, **datos)
    resumen.descarga(f"{fichero} ({nombre}, {tamano:,} bytes)", estado)
    print(f"  ✅ {fichero}: {estado}")


class Filtro:
    """Años que se descargan (--desde / --hasta)."""

    def __init__(self, desde=None, hasta=None):
        self.desde, self.hasta = desde, hasta

    def incluye(self, anio):
        return (self.desde is None or anio >= self.desde) and (self.hasta is None or anio <= self.hasta)


def buscar_conjuntos(resumen):
    """Conjuntos de la serie en el catálogo (package_search). Devuelve
    ({año: [paquete]}, otros) o None si CKAN no responde; otros = conjuntos con
    'menor' y 'contrat' que no son de la serie (para revisarlos)."""
    print("\n🔎 Catálogo CKAN de la Junta...")
    paquetes, inicio, total = {}, 0, None
    try:
        while True:
            resultado = ckan("package_search", q=CONSULTA_CKAN, rows=FILAS_CKAN, start=inicio) or {}
            lote = resultado.get("results") or []
            total = resultado.get("count", total)
            for paquete in lote:
                paquetes[paquete.get("id") or paquete.get("name")] = paquete   # páginas solapadas: una vez
            inicio += len(lote)
            if not lote or (total is not None and inicio >= total):
                break
            time.sleep(PAUSA)
    except ErrorPortal as e:
        resumen.fallidos.append(f"catálogo CKAN ({URL_API}package_search): {e}; no se descarga ni se retira nada")
        return None
    serie, otros = {}, []
    for paquete in paquetes.values():
        anio = anio_de_conjunto(paquete)
        if anio is not None:
            serie.setdefault(anio, []).append(paquete)
        elif "contrat" in _normalizar(paquete.get("title") or ""):
            otros.append(f"{paquete.get('name')} ({paquete.get('title')})")
    print(f"   {len(paquetes)} conjuntos con '{CONSULTA_CKAN}'; de la serie: {', '.join(map(str, sorted(serie)))}")
    return serie, sorted(otros)


def descargar_todo(raw, manifiesto, resumen, filtro, comprobar_todo=False):
    """Descubre los conjuntos de la serie en el catálogo y descarga el CSV de
    cada año. Lo que el catálogo deja de publicar queda como retirado (sus filas
    se conservan), pero solo si CKAN confirma que ya no existe (404): si el
    catálogo falla o no devuelve ningún conjunto de la serie, no se retira nada."""
    encontrado = buscar_conjuntos(resumen)
    if encontrado is None:
        return
    serie, otros = encontrado
    if otros:
        resumen.avisos.append(f"{len(otros)} conjuntos del catálogo con '{CONSULTA_CKAN}' y 'contrat' no son de la "
                              "serie (revisar): " + "; ".join(otros))
    if not serie:
        resumen.fallidos.append(f"el catálogo no devuelve ningún conjunto de la serie (q={CONSULTA_CKAN}); "
                                "no se descarga ni se retira nada")
        return
    # package_show de cada conjunto: sus recursos al día (el índice de búsqueda puede ir por detrás)
    completos, fallos_show = {}, 0
    for anio in sorted(serie):
        lista = []
        for paquete in serie[anio]:
            ident = paquete.get("id") or paquete.get("name")
            try:
                completo = ckan("package_show", id=ident)
            except ErrorPortal as e:
                resumen.avisos.append(f"{anio}: package_show de {paquete.get('name')} falló ({e}); se usan los "
                                      "recursos de la búsqueda")
                completo, fallos_show = paquete, fallos_show + 1
            finally:
                time.sleep(PAUSA)
            completos[completo.get("id") or ident] = completo
            lista.append(completo)
        serie[anio] = lista
    # Ficheros que ya se tienen y cuyo conjunto no ha salido en la búsqueda: se pregunta por el id del
    # conjunto, que no cambia aunque la Junta le cambie el nombre. Solo un 404 los da por retirados.
    conocidos = set(completos) | {p.get("name") for p in completos.values()}
    for fichero, info in sorted(manifiesto.datos.items()):
        anio, ident = info.get("anio"), info.get("id_conjunto") or info.get("conjunto")
        if (not ident or ident in conocidos or not isinstance(anio, int) or not filtro.incluye(anio)
                or info.get("publicado") is False):
            continue
        try:
            paquete = ckan("package_show", id=ident)
        except ErrorPortal as e:
            if e.codigo in (404, 410):
                manifiesto.retirar(fichero, f"CKAN ya no tiene el conjunto {info.get('conjunto')} ({e})")
                resumen.retirados.append(f"{fichero}: CKAN ya no tiene {info.get('conjunto')}; se conservan sus filas")
                print(f"  🗑️ {fichero}: retirado por el portal")
            else:
                resumen.fallidos.append(f"{fichero}: no se pudo comprobar su conjunto {info.get('conjunto')} ({e}); "
                                        "no se retira nada")
            continue
        finally:
            time.sleep(PAUSA)
        completos[paquete.get("id") or ident] = paquete
        conocidos |= {paquete.get("id"), paquete.get("name"), ident}
        serie.setdefault(anio, []).append(paquete)
        resumen.avisos.append(f"{anio}: la búsqueda no devuelve {paquete.get('name')}, pero sigue publicado "
                              "(package_show)")
    if fallos_show:
        resumen.avisos.append("no se guarda raw/catalogo_ckan.json: algún package_show ha fallado y sería una "
                              "versión que no es la de CKAN")
    else:
        guardar_json(Path(raw) / "catalogo_ckan.json", {p.get("name") or i: p for i, p in completos.items()})

    anio_actual = ahora().year
    for anio in sorted(serie):
        if not filtro.incluye(anio):
            continue
        paquetes = serie[anio]
        if len(paquetes) > 1:
            resumen.avisos.append(f"{anio}: {len(paquetes)} conjuntos de la serie para el mismo año: "
                                  + ", ".join(p.get("name") for p in paquetes))
        recursos = [(p, r) for p in paquetes for r in recursos_csv(p)]
        print(f"\n📦 {anio}: {', '.join(p.get('name') for p in paquetes)} ({len(recursos)} CSV)")
        if not recursos:
            resumen.fallidos.append(f"{anio}: {', '.join(p.get('name') for p in paquetes)} no tiene ningún recurso "
                                    "CSV; no se retira nada")
            continue
        asignados = asignar_ficheros(anio, recursos, manifiesto)
        for fichero, (paquete, recurso) in asignados:
            descargar_fichero(raw, fichero, anio, paquete, recurso, manifiesto, resumen, comprobar_todo)
        # Un fichero anual que el conjunto (leído entero) ya no tiene
        vigentes = {h for h, _ in asignados}
        for fichero in manifiesto.ficheros_de(anio):
            if fichero not in vigentes and manifiesto.get(fichero).get("publicado", True):
                manifiesto.retirar(fichero, "el conjunto ya no tiene ese recurso CSV")
                resumen.retirados.append(f"{fichero}: el conjunto ya no tiene ese CSV; se conservan sus filas")
                print(f"  🗑️ {fichero}: retirado por el portal")

    en_raw = {int(f.split("/")[0]) for f in ficheros_en_raw(raw)}
    for anio in range(ANIO_MINIMO, anio_actual + 1):
        if anio in serie or not filtro.incluye(anio) or manifiesto.ficheros_de(anio):
            continue
        if anio in en_raw:
            resumen.fallidos.append(f"{anio}: el catálogo no lo devuelve y raw/{anio}/ tiene copias que el manifiesto "
                                    "no conoce (¿manifiesto perdido?); no se comprueba ni se retira nada")
        elif anio in CONFIRMADOS:
            resumen.fallidos.append(f"{anio}: año publicado (verificado el 2026-09-29) y el catálogo ya no lo "
                                    "devuelve; no se tiene ninguna copia")
        else:
            resumen.no_publicados.append(anio)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores de la Junta de Andalucía "
                                                 "(CKAN de datos abiertos, con el SAS)")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--desde", type=int, default=None, help="primer año que se descarga (por defecto todos)")
    parser.add_argument("--hasta", type=int, default=None, help="último año que se descarga (por defecto todos)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar el Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar el Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años cerrados que CKAN no da por cambiados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    filtro = Filtro(args.desde, args.hasta)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Años: {args.desde or 'todos'}-{args.hasta or 'todos'}\nDestino: {salida.resolve()}")
    resumen = Resumen(TITULO)
    try:
        manifiesto = Manifiesto(raw)
    except RuntimeError as e:
        resumen.fallidos.append(str(e))
        return resumen.cerrar(raw)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, filtro, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquet(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
