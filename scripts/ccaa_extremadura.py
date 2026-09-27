#!/usr/bin/env python3
"""
=============================================================================
EXTREMADURA - REGISTRO DE CONTRATOS DE LA JUNTA (menores, mayores e incidencias)
=============================================================================
Descarga todos los documentos que enlazan las páginas trimestrales
"Contratos e incidencias inscritas en el Registro de Contratos" de juntaex.es
tal cual se publican (con todas sus versiones) y genera un Parquet por serie
con todas las filas y columnas de todos los trimestres, como texto.

Ejecutar:  python scripts/ccaa_extremadura.py [--salida DIR] [--solo-descarga]
           [--solo-parquet | --solo-procesar] [--comprobar-todo]

Series (un documento por trimestre y tipo):
    registro_contratos_menores      contratos menores inscritos (casi todos del SES)
    registro_contratos_mayores      contratos no menores inscritos
    registro_contratos_incidencias  modificaciones, ampliaciones de plazo, prórrogas y
                                    resoluciones (un fichero por tipo en 2022 y 1T 2023;
                                    desde 2T 2023 un único listado de incidencias);
                                    la columna _tipo dice de qué documento sale cada fila
    registro_contratos_otros        documentos con otro nombre (solo si aparece alguno)
Los resúmenes estadísticos anuales (PDF) se guardan en raw/resumen_estadistico/
pero no son tablas: no entran en ningún Parquet.

Salida (por defecto <repo>/ccaa_extremadura/):
    raw/<tipo>/<tipo>_<año>_<trimestre>.<ext>  p.ej. raw/menores/menores_2024_3T.xlsx,
                                       raw/menores/menores_2023_2T-3T.xlsx (dos trimestres)
    raw/<tipo>/_historico/             versiones anteriores de cada fichero (nunca se borran)
    raw/paginas.json                   páginas del registro encontradas y qué enlaza cada una
    raw/_manifiesto.json               de cada fichero: URL publicada, página, nombre publicado,
                                       uuid, sha256, fechas y si el portal lo sigue enlazando
    raw/descarga_log.txt               resumen de cada ejecución (se añade al final)
    registro_contratos_<serie>.parquet todas las filas de la serie (todas las columnas, texto)
    _historico/                        versiones anteriores de los Parquet

Columnas añadidas: _fuente (URL descargada, con su ?t=), _pagina (página que la
enlaza), _dataset (serie), _tipo (menores, mayores, incidencias, modificaciones,
ampliaciones_plazo, prorrogas, resoluciones u otros), _nombre_publicado (nombre
del fichero en el portal), _anio, _trimestre ('3T'; '2T-3T' si el documento
cubre dos), _archivo_origen (ruta en raw/), _hoja, _fecha_descarga y, de
comun/historico.py, _primera_descarga, _ultima_descarga y _en_ultima_descarga.
Un registro que el portal retira o modifica NO desaparece: sigue en el Parquet
con _en_ultima_descarga=False (control del sesgo del superviviente).

Qué se descarga:
- Páginas: las que devuelve el buscador del portal (todas sus páginas de
  resultados), las conocidas (PAGINAS_CONOCIDAS, con los slugs irregulares),
  las de ejecuciones anteriores (raw/paginas.json) y, para cada trimestre desde
  1T 2022 hasta el actual que ninguna de ellas cubra, los slugs
  registro-contratos-{t}t-{AAAA} y registro-contratos-{t}t-{AA} (con
  --comprobar-todo, los de todos los trimestres).
- Documentos: todos los enlaces /documents/... de esas páginas (salvo
  imágenes). El tipo sale del nombre publicado (o del texto del enlace) y el
  año y el trimestre del nombre o, si no los trae, de la página. Cada documento
  se identifica por su uuid del gestor documental y se guarda con un nombre
  normalizado y único (si dos documentos enlazados caen en el mismo nombre, el
  nuevo lleva los 8 primeros caracteres de su uuid).
- Se vuelven a pedir los documentos del año en curso y del anterior y los que
  el portal enlaza con otra versión (?t= distinto); los demás, solo con
  --comprobar-todo. Todo pasa por comun.historico.guardar_version.
- Un documento que ninguna página enlaza ya queda como retirado (sus filas se
  conservan con _en_ultima_descarga=False). Si el buscador falla o no devuelve
  nada, si una página da un error, si el buscador lista una página que da 404
  o si una página que enlazaba documentos deja de enlazar ninguno, no se
  retira nada y la ejecución termina con error (código 1).
- Un trimestre de TRIMESTRES_CONFIRMADOS sin listado de menores (ni enlazado
  ni descargado antes) es un error; los trimestres cerrados posteriores que
  falten se anotan como no publicados.

FUENTES (verificado en vivo el 2026-09-27)
------------------------------------------
- Buscador: https://www.juntaex.es/buscador?q="Contratos e incidencias inscritas
  en el Registro de Contratos"&sort=modified-&delta=75 -> 21 resultados
  (con delta=20 hay dos páginas de resultados: &start=2). Son 17 páginas
  trimestrales y 4 de resumen estadístico anual (2022-2025), todas con el título
  "Contratos e incidencias inscritas en el Registro de Contratos" y el periodo
  en og:description ("1er. Trimestre de 2026", "2º y 3er. Trimestres de
  2023.", "Resumen estadístico del ejercicio 2025"...).
- Cubren 1T 2022-2T 2026 sin huecos: 2T y 3T de 2023 van en una sola página y
  en un solo fichero por tipo ("CONTRATOS MENORES 2 y 3T 2023.xlsx"). Slugs
  irregulares: registro-contratos (2T 2022), publicacion-registro-contratos
  (3T 2022), registro-contratos-1t-23 y
  contratos-incidencias-inscritas-registro-contratos (2T 2026; los slugs
  registro-contratos-2t-2026/-2t-26 dan 404). Un slug inexistente da HTTP 404
  ("Contenido no encontrado").
- Documentos: /documents/77055/621084/<nombre>/<uuid>?t=<ms>. El uuid
  identifica el documento; t es su fecha de modificación (cambia si se
  sustituye). Nombres muy irregulares: "LISTADO CONTRATOS MENORES 1T 2022.xls",
  "ListadoMENORES_3T_2022_CM1666594182411.xlsx", "LISTADOS CONTRATOS MENORES
  3º T 2024.xlsx", "LISTADO CONTRATOS MENORES 2º TRIMESTRE 2025.xlsx",
  "CONTRATOS MENORES 3ºT 2025.xlsx", "MENORES 4º T 2025.xlsx", "LISTADO
  CONTRATOS MENORES 2T 26.xlsx", "Listado de Modificacioens de contratos 2T
  2022.xls"... Cada página trimestral enlaza MAYORES, MENORES y, en 2022 y 1T
  2023, MODIFICACIONES, AMPLIACIONES DE PLAZO, PRÓRROGAS y RESOLUCIONES (2T
  2022 solo modificaciones); desde 2T 2023, un listado de INCIDENCIAS. Las de
  resumen enlazan 4 PDF cada una.
- Formatos: .xls (BIFF) en 2022 y 1T 2023 (salvo menores 3T 2022, .xlsx) y
  .xlsx desde 2T 2023; una hoja por fichero, cabecera en la primera fila.
- Tres esquemas de columnas en menores, que se unen sin perder ninguna:
  2022-1T 2023 ("Nº Contrato", "Consejería", "Órgano", "CIF/NIF Contratista",
  "Código CPV"...; 1T 2022 trae además "Usuario", "Validado" y "Fecha
  validación"; las celdas empiezan por un espacio y se conservan así), 2T-3T
  2023 ("Número de registro de contrato", "Código del expediente",
  "Consejería/organismo/entidad", "NIF", "Denominación contratista", sin CPV) y
  desde 4T 2023 ("RECO-Órg.Gestor", "Consejería/Organismo/Entidad", "Código
  CPV", "NIF", "Denominación Adjudicatario"...; 2024 sin "Contrato Mixto",
  "Plurianual" ni los importes sin impuestos).
- Las API de Liferay que listarían la carpeta 621084 (headless-delivery y
  jsonws) dan 403: no hay más descubrimiento que las páginas.
- Serie anterior (2016-2021), de la Intervención General: estaba en
  http://www.juntaex.es/ig/relacion-de-contratos-menores (y /ig/contratos-
  menores---2021, /ig/registro-de-contratos), con ficheros en
  /filescms/ig/uploaded_files/Contratos/Menores/<año>/ (XLS trimestrales, p.ej.
  3_trimestre_2016_menores.xls, y PDF por órgano, pdf_1t/2021_1_69_1.pdf).
  Hoy todo eso da 404 en www.juntaex.es por HTTPS; el buscador indexa las
  mismas rutas en instituciones.juntaex.es, que desde el entorno donde se
  escribió no responde por HTTPS (respuesta vacía) y por HTTP lo bloquea el
  proxy. La Wayback Machine guarda la página (20210418051334, 20220516210346),
  pero tampoco era accesible. NO se descarga: pendiente de verificar desde otra red.
=============================================================================
"""

import argparse
import codecs
import datetime as dt
import hashlib
import json
import math
import os
import re
import sys
import time
import unicodedata
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, unquote, unquote_plus, urljoin, urlparse

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

BASE = "https://www.juntaex.es"
URL_PAGINA = BASE + "/w/{slug}"
URL_BUSCADOR = BASE + "/buscador"
CONSULTA_BUSCADOR = '"Contratos e incidencias inscritas en el Registro de Contratos"'
POR_PAGINA_BUSCADOR = 75            # el máximo que ofrece el buscador (5, 10, 20, 30, 50 o 75)
MAX_PAGINAS_BUSCADOR = 20
# Lo que dice el título de toda página del registro (sin acentos, minúsculas)
TITULO_REGISTRO = "registro de contratos"

PRIMER_TRIMESTRE = (2022, 1)        # primer trimestre publicado en estas páginas
# Slugs que se prueban para un trimestre que ninguna página cubre
SLUGS_TRIMESTRE = ("registro-contratos-{t}t-{anio}", "registro-contratos-{t}t-{aa:02d}")

# Páginas verificadas el 2026-09-27 (slug: periodo que declaran). Se visitan
# siempre, aunque el buscador no las devuelva.
PAGINAS_CONOCIDAS = {
    "registro-contratos-1t-2022": "1T 2022",
    "registro-contratos": "2T 2022",
    "publicacion-registro-contratos": "3T 2022",
    "registro-contratos-4t-2022": "4T 2022",
    "registro-contratos-1t-23": "1T 2023",
    "registro-contratos-2t-2023": "2T y 3T 2023",
    "registro-contratos-4t-2023": "4T 2023",
    **{f"registro-contratos-{t}t-{a}": f"{t}T {a}" for a in (2024, 2025) for t in range(1, 5)},
    "registro-contratos-1t-2026": "1T 2026",
    "contratos-incidencias-inscritas-registro-contratos": "2T 2026",
    "contratos-anio-2022": "resumen estadístico 2022",
    **{f"resumen-estadistico-de-contratos-{a}": f"resumen estadístico {a}" for a in (2023, 2024, 2025)},
}

# Trimestres cuyo listado de menores consta publicado (2026-09-27): si falta
# alguno (ni enlazado ni descargado antes), es un error.
TRIMESTRES_CONFIRMADOS = [(a, t) for a in range(2022, 2026) for t in range(1, 5)] + [(2026, 1), (2026, 2)]

# Tipos de documento, en el orden en que se buscan en el nombre publicado (o
# en el texto del enlace) sin acentos y en minúsculas. Sin \b: hay nombres
# pegados como "ListadoMENORES_3T_2022" o "ListadoAumentoPlazo_3T_2022".
TIPOS = (
    ("incidencias", r"incidencia"),
    ("modificaciones", r"modific"),
    ("ampliaciones_plazo", r"ampliaci|aumento ?(?:de ?)?plazo"),
    ("prorrogas", r"prorroga"),
    ("resoluciones", r"resoluci"),
    ("menores", r"menor"),
    ("mayores", r"mayor"),
    ("resumen_estadistico", r"resumen|estadistic|volumen presupuestario|listado (?:de \w+ )?por "),
)
# Resúmenes estadísticos: qué listado es cada PDF (para el nombre local)
SUBTIPOS_RESUMEN = (
    ("contratistas", r"contratista"),
    ("procedimientos", r"procedimiento"),
    ("organos", r"organo|consejeria|entidad"),
    ("tipos_contrato", r"tipo"),
)
# Parquet de cada serie: tipos de documento que reúne
SERIES = {
    "registro_contratos_menores": ("menores",),
    "registro_contratos_mayores": ("mayores",),
    "registro_contratos_incidencias": ("incidencias", "modificaciones", "ampliaciones_plazo", "prorrogas",
                                       "resoluciones"),
    "registro_contratos_otros": ("otros",),
}
# Series en las que se marca (_repetido_de) el contrato que ya estaba en un
# listado anterior, y columnas con su número de registro (según el esquema)
SERIES_CON_REPETIDOS = ("registro_contratos_menores", "registro_contratos_mayores")
COLUMNAS_NUMERO = ("Número de registro de contrato", "Nº Contrato")
EXTENSIONES_TABLA = (".xlsx", ".xls", ".csv")
EXTENSIONES_IMAGEN = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".bmp")

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_extremadura"
TITULO = "EXTREMADURA - REGISTRO DE CONTRATOS DE LA JUNTA"


# ============================================================================
# UTILIDADES COMUNES (mismo bloque en ccaa_castilla_leon.py, ccaa_murcia.py y
# ccaa_navarra.py): descarga con reintentos, versiones, lectura como texto,
# acumulación de registros y Parquet. Aquí además: columnas de origen propias
# (METADATOS_ORIGEN), PDF en formato_contenido/validar_contenido e informes en
# el Resumen.
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

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL u otra hoja sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_pagina", "_dataset", "_tipo", "_nombre_publicado", "_anio", "_trimestre",
                    "_archivo_origen", "_hoja", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_repetido_de", "_primera_descarga", "_ultima_descarga",
                                      "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


def iso(momento):
    """Fecha ISO 8601 en UTC al segundo: '2026-09-27T14:43:00Z' (ordenable como texto)."""
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def a_epoch(valor):
    """Fecha ISO de CKAN/OpenDataSoft (sin zona = UTC) -> epoch, o None."""
    if not valor:
        return None
    try:
        fecha = datetime.fromisoformat(str(valor).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.timestamp()


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


def url_completa(url, params=None):
    return requests.Request("GET", url, params=params).prepare().url


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


def pedir_texto(url):
    """GET de una página HTML con reintentos (red, 429, 5xx); un 4xx lanza
    ErrorPortal."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.text
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


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes."""
    if cabeza.startswith(b"PK\x03\x04"):
        return "xlsx"
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


def validar_contenido(ruta, tipo):
    """Motivo por el que la descarga no es el fichero esperado (p.ej. una página
    HTML de error servida con 200), o None si es válida."""
    if tipo is None:
        return None
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía"
    formato = formato_contenido(cabeza)
    if formato in ("html", "xml"):
        return f"la respuesta es {formato.upper()}, no {tipo.upper()}"
    if tipo in ("xlsx", "xls"):
        return None if formato in ("xlsx", "xls") else f"se esperaba una hoja de cálculo y llegó {formato}"
    if tipo not in ("csv", "json", "pdf"):
        return None                     # otro formato: basta con que no sea una página
    return None if formato == tipo else f"se esperaba {tipo.upper()} y llegó {formato}"


def descargar(url, destino, params=None, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que es el tipo de fichero esperado y lo
    entrega a guardar_version(): si el contenido no cambió no se toca nada y si
    cambió la copia previa pasa a _historico/. Reintenta con backoff los fallos
    de red, 429 y 5xx (también cortes a mitad de descarga).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
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
                        with open(tmp, "wb") as f:
                            for trozo in respuesta.iter_content(chunk_size=1 << 16):
                                if trozo:
                                    f.write(trozo)
                        motivo = validar_contenido(tmp, tipo)
                        if motivo:
                            return "invalido", motivo
                        return guardar_version(destino, desde=tmp), ""
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
    """Guarda metadatos del portal (catálogo, fichas) conservando sus versiones."""
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
    """Lo descargado, lo que no existe (404 por año...), lo retirado y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = []
        self.no_publicados = {}
        self.retirados = []
        self.avisos = []
        self.fallidos = []
        self.parquets = []
        self.informes = []          # [(título, [líneas])]

    def descarga(self, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios.append(etiqueta)
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
            lineas.append("\nNO PUBLICADOS EN EL PORTAL (404 / sin fichero):")
            lineas.extend(f"   - {g}: {self._rangos(v)}" for g, v in self.no_publicados.items())
        bloque("RETIRADOS POR EL PORTAL (se conservan con _en_ultima_descarga=False)", self.retirados)
        bloque("PARQUET", [f"{n}: {f:,} filas x {c} columnas ({r:,} ya no publicadas)"
                           for n, f, c, r in self.parquets])
        for titulo, informe in self.informes:
            bloque(titulo, informe)
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


def leer_csv(ruta):
    """CSV como texto: dtype=str, sin convertir 'NA', 'N/A', 'NULL'... en nulos
    (solo el campo vacío es nulo) y sin perder filas ni campos."""
    ruta = Path(ruta)
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
        return pd.DataFrame(), [f"{ruta.name}: fichero sin cabecera ni filas"]
    except (pd.errors.ParserError, pd.errors.ParserWarning):
        df, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
        if con_extra:
            avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; "
                          "los campos de más se conservan en columnas _columna_extra_N")
        if literales:
            avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo "
                          "(se conservan en el texto; sin ellas se tragarían los registros siguientes)")
    if codificacion != "utf-16":
        lineas = _lineas_de_datos(ruta)
        if lineas != len(df):
            # Puede ser una comilla literal que se traga registros enteros:
            # se vuelve a leer sin que lo haga (comun.lectura_csv)
            tolerante, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
            if literales and len(tolerante) > len(df):
                avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo se "
                              f"tragaban {len(tolerante) - len(df):,} registros; se leen sin tragárselos "
                              "(la comilla se conserva en el texto)")
                if con_extra:
                    avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; "
                                  "los campos de más se conservan en columnas _columna_extra_N")
                df = tolerante
        if lineas != len(df):
            avisos.append(f"{ruta.name}: {len(df):,} filas leídas de {lineas:,} líneas de datos "
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


def _leer_xlsx(ruta):
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
            filas = ([_celda_texto(v) for v in fila] for fila in hoja.iter_rows(values_only=True))
            df = _hoja_a_df(filas, Path(ruta).name, hoja.title, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.close()
        fichero.close()
    return partes, avisos


def _leer_xls(ruta):
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
            df = _hoja_a_df(filas, Path(ruta).name, hoja.name, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.release_resources()
    return partes, avisos


def _buscar_registros(datos, profundidad=0):
    """Primera lista de objetos de un JSON (la raíz, result.records, data...)."""
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
    with open(ruta, encoding=_detectar_codificacion(ruta)) as f:
        registros = _buscar_registros(json.load(f))
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


def leer_tabla(ruta):
    """Lee un fichero tabular (CSV, XLSX, XLS o JSON, según su contenido real)
    con todas sus filas y columnas como texto. Devuelve (DataFrame, avisos)."""
    ruta = Path(ruta)
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato == "csv":
        return leer_csv(ruta)
    if formato in ("xlsx", "xls"):
        partes, avisos = (_leer_xlsx if formato == "xlsx" else _leer_xls)(ruta)
        if len(partes) > 1:
            avisos.append(f"{ruta.name}: {len(partes)} hojas con datos; se unen (columna _hoja)")
        if not partes:
            return pd.DataFrame(columns=["_hoja"]), avisos
        return pd.concat(partes, ignore_index=True, sort=False), avisos
    if formato == "json":
        return _leer_json(ruta), []
    raise ValueError(f"formato {formato.upper()} no tabular")


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


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen):
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
            df, avisos = leer_tabla(version)
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


def construir_parquet(destino, ficheros, raw, manifiesto, resumen, derivar=None):
    """Genera `destino` con los registros acumulados de `ficheros`
    (lista de (ruta_actual, rel, metadatos)) partiendo del Parquet anterior.

    Las filas de ficheros que ya no se procesan (retirados, otro formato...) se
    conservan: si el fichero sigue en raw/ se vuelve a procesar con sus
    versiones; si no, se copian tal cual del Parquet anterior. derivar(df) ->
    df añade columnas calculadas sobre la tabla entera antes de escribirla
    (no cuentan al comparar versiones: los ficheros no las traen).
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
                         if c in grupo.columns and c not in ("_hoja", "_fecha_descarga")}
            ficheros.append((Path(raw) / rel, rel, metadatos))
            procesados.add(rel)
    partes = []
    for actual, rel, metadatos in ficheros:
        registros = acumular_fichero(actual, rel, por_fichero.pop(rel, None), metadatos, manifiesto, resumen)
        if registros is not None and len(registros):
            partes.append(registros)
    for rel, grupo in por_fichero.items():
        resumen.avisos.append(f"{destino.name}: {rel} ya no está en raw/; se conservan sus {len(grupo):,} filas")
        partes.append(grupo)
    if not partes:
        return None
    df = pd.concat(partes, ignore_index=True, sort=False)
    if derivar is not None:
        df = derivar(df)
    estado = escribir_parquet(df, destino)
    retiradas = int((~df["_en_ultima_descarga"].astype(bool)).sum())
    resumen.parquets.append((destino.name, len(df), len(df.columns), retiradas))
    print(f"  💾 {destino.name}: {len(df):,} filas ({estado})")
    return df


# ============================================================================
# EXTREMADURA: PÁGINAS DEL REGISTRO Y DOCUMENTOS QUE ENLAZAN
# ============================================================================

def normalizar(texto):
    """Texto sin acentos, en minúsculas y con los espacios colapsados, para
    buscar palabras (NFKD convierte además 'º' en 'o')."""
    return " ".join(sin_acentos(texto or "").lower().split())


def sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


class _AnalizadorHTML(HTMLParser):
    """Título, etiquetas <meta>, enlaces (href y texto) y texto de una página."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.titulo = ""
        self.meta = {}
        self.enlaces = []
        self.textos = []
        self._en_titulo = False
        self._abiertos = []

    def handle_starttag(self, tag, attrs):
        atributos = dict(attrs)
        if tag == "meta":
            clave = (atributos.get("property") or atributos.get("name") or "").lower()
            if clave and clave not in self.meta:
                self.meta[clave] = atributos.get("content") or ""
        elif tag == "title":
            self._en_titulo = True
        elif tag == "a":
            self._abiertos.append((atributos.get("href"), []))

    def handle_endtag(self, tag):
        if tag == "title":
            self._en_titulo = False
        elif tag == "a" and self._abiertos:
            self._cerrar_enlace()

    def handle_data(self, data):
        if self._en_titulo:
            self.titulo += data
        for _, trozos in self._abiertos:
            trozos.append(data)
        self.textos.append(data)

    def _cerrar_enlace(self):
        href, trozos = self._abiertos.pop()
        if href and href.strip():
            self.enlaces.append((href.strip(), " ".join(" ".join(trozos).split())))

    def close(self):
        super().close()
        while self._abiertos:            # <a> sin cerrar
            self._cerrar_enlace()


def analizar_html(texto):
    analizador = _AnalizadorHTML()
    analizador.feed(texto)
    analizador.close()
    return analizador


# Periodo en un texto ya normalizado: "1t 2022", "3o t 2024", "3ot 2025",
# "2o trimestre 2025", "2 y 3t 2023", "_3t_2022_", "2t 26", "1er. trimestre de
# 2026", "2o y 3er. trimestres de 2023", "registro-contratos-1t-23"...
_ORDINAL = r"(?:\s*(?:er|o|a|°)\.?)?"
PATRON_TRIMESTRE = re.compile(
    r"(?<!\d)([1-4])" + _ORDINAL + r"(?:\s*(?:y|e|-|/|,)\s*([1-4])" + _ORDINAL + r")?"
    r"\s*t(?:rim(?:estre)?s?)?(?![a-z])\.?"
    r"(?:[\s_.-]*(?:del?\s+)?(?:(?:ano|ejercicio)\s+)?(\d{4}|\d{2})(?!\d))?")
PATRON_ANIO = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
PATRON_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
PATRON_LOCAL = re.compile(r"(?P<tipo>[a-z_]+?)_(?P<anio>\d{4})(?:_(?P<t1>[1-4])T(?:-(?P<t2>[1-4])T)?)?(?:_.*)?\.\w+")
PATRON_TOTAL = re.compile(r"(?:\bde|encontrado)\s+(\d[\d.]*)\s+resultados")


def periodo(texto):
    """(año, trimestres) que declara un texto: nombre publicado, texto de un
    enlace, descripción o slug de una página. '1er. Trimestre de 2026' ->
    (2026, (1,)); 'CONTRATOS MENORES 2 y 3T 2023.xlsx' -> (2023, (2, 3));
    'LISTADO CONTRATOS MENORES 2T 26.xlsx' -> (2026, (2,)); 'Resumen
    estadístico del ejercicio 2025' -> (2025, ()). Año None si no lo dice."""
    texto = normalizar(texto)
    anio, trimestres = None, ()
    encontrado = PATRON_TRIMESTRE.search(texto)
    if encontrado:
        trimestres = tuple(sorted({int(g) for g in encontrado.group(1, 2) if g}))
        if encontrado.group(3):
            anio = int(encontrado.group(3)) + (2000 if len(encontrado.group(3)) == 2 else 0)
    if anio is None:
        anios = set(PATRON_ANIO.findall(texto))
        anio = int(anios.pop()) if len(anios) == 1 else None
    if anio is not None and not 2000 <= anio <= 2099:
        anio = None
    return anio, trimestres


def etiqueta_trimestre(trimestres):
    """(3,) -> '3T'; (2, 3) -> '2T-3T'; () -> None."""
    return "-".join(f"{t}T" for t in trimestres) or None


def periodo_local(nombre):
    """(año, trimestre) de un nombre local ('menores_2023_2T-3T.xlsx' -> (2023,
    '2T-3T')), para una copia sin entrada en el manifiesto."""
    encontrado = PATRON_LOCAL.fullmatch(nombre)
    if not encontrado:
        return None, None
    trimestres = tuple(int(t) for t in encontrado.group("t1", "t2") if t)
    return int(encontrado.group("anio")), etiqueta_trimestre(trimestres)


def tipo_documento(nombre, texto="", en_resumen=False):
    """Tipo de un documento por su nombre publicado o, si no lo dice, por el
    texto del enlace (TIPOS). Sin tipo, en una página de resumen estadístico es
    un resumen y en otra, 'otros'."""
    for fuente in (nombre, texto):
        normal = normalizar(fuente)
        for tipo, patron in TIPOS:
            if re.search(patron, normal):
                return tipo
    return "resumen_estadistico" if en_resumen else "otros"


def subtipo_resumen(nombre, texto=""):
    for fuente in (nombre, texto):
        normal = normalizar(fuente)
        for subtipo, patron in SUBTIPOS_RESUMEN:
            if re.search(patron, normal):
                return subtipo
    return None


def partes_documento(url):
    """(nombre publicado, clave) de un enlace del gestor documental,
    /documents/<grupo>/<carpeta>/<nombre>/<uuid>?t=<ms>. La clave es el uuid
    (no cambia aunque el documento se sustituya) o, si no lo trae, la ruta."""
    ruta = urlparse(url).path
    tramos = [t for t in ruta.split("/documents/", 1)[-1].split("/") if t]
    uuid = tramos[-1].lower() if tramos and PATRON_UUID.fullmatch(tramos[-1]) else None
    if uuid:
        tramos = tramos[:-1]
    return (unquote_plus(tramos[-1]) if tramos else ""), uuid or unquote(ruta)


def url_pagina(href):
    """URL canónica (sin ?inheritRedirect=... ni #) de una página /w/<slug> del
    portal, o None si el enlace no es de una."""
    partes = urlparse(urljoin(BASE + "/", href))
    encontrado = re.fullmatch(r"/w/([^/]+)/?", partes.path)
    if not encontrado or partes.netloc.lower() not in ("www.juntaex.es", "juntaex.es"):
        return None
    return URL_PAGINA.format(slug=encontrado.group(1))


def enlaces_documentos(url, analisis):
    """[{url, nombre, texto}] de los documentos que enlaza una página: enlaces
    /documents/... del portal que no son imágenes, sin repetir."""
    lista, vistos = [], set()
    for href, texto in analisis.enlaces:
        direccion = urljoin(url, href)
        partes = urlparse(direccion)
        if "/documents/" not in partes.path or partes.netloc.lower() not in ("www.juntaex.es", "juntaex.es"):
            continue
        nombre, _ = partes_documento(direccion)
        if Path(nombre).suffix.lower() in EXTENSIONES_IMAGEN or direccion in vistos:
            continue
        vistos.add(direccion)
        lista.append({"url": direccion, "nombre": nombre, "texto": texto})
    return lista


@dataclass
class Documento:
    """Un documento enlazado por las páginas del registro."""
    url: str                    # como lo enlaza la página (con ?t=)
    nombre: str                 # nombre publicado (tramo de la URL, decodificado)
    texto: str                  # texto del enlace
    clave: str                  # uuid del gestor documental (o la ruta si no lo trae)
    pagina: str                 # primera página que lo enlaza
    paginas: list = field(default_factory=list)
    tipo: str = "otros"
    anio: int = None
    trimestres: tuple = ()
    subtipo: str = None
    rel: str = None             # ruta local, relativa a raw/

    @property
    def extension(self):
        extension = Path(self.nombre).suffix.lower()
        return extension if re.fullmatch(r"\.[a-z0-9]{1,5}", extension) else ""

    @property
    def trimestre(self):
        return etiqueta_trimestre(self.trimestres)

    @property
    def base(self):
        """Nombre local sin extensión: menores_2024_3T, menores_2023_2T-3T,
        resumen_estadistico_2024_organos..."""
        partes = [self.tipo, str(self.anio) if self.anio else "sin_periodo"]
        partes += [p for p in (self.trimestre, self.subtipo) if p]
        return "_".join(partes)

    def version_portal(self):
        """Fecha de la versión del documento según el portal (?t=, en ms), o None."""
        valor = (parse_qs(urlparse(self.url).query).get("t") or [""])[0]
        if not valor.isdigit():
            return None
        try:
            return iso(datetime.fromtimestamp(int(valor) / 1000, timezone.utc))
        except (OverflowError, OSError, ValueError):
            return None

    def metadatos(self):
        """Lo que se guarda en el manifiesto de cada fichero descargado."""
        return {"clave": self.clave, "nombre_publicado": self.nombre, "texto_enlace": self.texto,
                "pagina": self.pagina, "paginas": list(self.paginas), "tipo": self.tipo, "anio": self.anio,
                "trimestre": self.trimestre, "trimestres": list(self.trimestres),
                "version_portal": self.version_portal()}


def leer_pagina(url):
    """Una página del portal: ('ok', info) si es del registro; ('no_existe',
    detalle) con 404/410; ('ajena', título) si es otra cosa y ('error',
    detalle) si no se puede leer."""
    try:
        texto = pedir_texto(url)
    except ErrorPortal as e:
        return ("no_existe" if e.codigo in (404, 410) else "error"), str(e)
    analisis = analizar_html(texto)
    titulo = " ".join((analisis.meta.get("og:title") or analisis.titulo).split())
    if TITULO_REGISTRO not in normalizar(titulo):
        return "ajena", titulo or "sin título"
    descripcion = " ".join(analisis.meta.get("og:description", "").split())
    slug = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    anio, trimestres = periodo(descripcion)
    anio_slug, trimestres_slug = periodo(slug)
    return "ok", {"titulo": titulo, "descripcion": descripcion, "anio": anio or anio_slug,
                  "trimestres": list(trimestres or trimestres_slug),
                  "resumen": "resumen estadistico" in normalizar(descripcion) or slug.startswith("resumen-"),
                  "documentos": enlaces_documentos(url, analisis)}


def paginas_del_buscador(resumen):
    """Páginas del registro que devuelve el buscador del portal, con todas sus
    páginas de resultados. Devuelve (urls, completo): completo es False si el
    buscador falla o no devuelve ninguna."""
    urls, total = [], None
    parametros = {"q": CONSULTA_BUSCADOR, "sort": "modified-", "delta": POR_PAGINA_BUSCADOR}
    for numero in range(1, MAX_PAGINAS_BUSCADOR + 1):
        direccion = url_completa(URL_BUSCADOR, {**parametros, **({"start": numero} if numero > 1 else {})})
        try:
            analisis = analizar_html(pedir_texto(direccion))
        except ErrorPortal as e:
            resumen.fallidos.append(f"buscador del portal ({direccion}): {e}; se sigue con las páginas "
                                    "conocidas y no se retira nada")
            return urls, False
        time.sleep(PAUSA)
        nuevas = []
        for href, texto in analisis.enlaces:
            url = url_pagina(href)
            if url and TITULO_REGISTRO in normalizar(texto) and url not in urls and url not in nuevas:
                nuevas.append(url)
        urls += nuevas
        encontrado = PATRON_TOTAL.search(normalizar(" ".join(analisis.textos)))
        if encontrado:
            total = int(encontrado.group(1).replace(".", ""))
        if not nuevas or (total is not None and len(urls) >= total):
            break
    if not urls:
        resumen.fallidos.append(f"buscador del portal ({url_completa(URL_BUSCADOR, parametros)}): no devuelve "
                                "ninguna página del registro; no se retira nada")
        return urls, False
    print(f"   Buscador: {len(urls)} páginas del registro" + (f" ({total} resultados)" if total is not None else ""))
    return urls, True


def leer_inventario(raw):
    """Páginas leídas en la ejecución anterior (raw/paginas.json)."""
    try:
        datos = json.loads((Path(raw) / "paginas.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}
    return datos if isinstance(datos, dict) else {}


def trimestre_actual():
    hoy = ahora()
    return hoy.year, (hoy.month - 1) // 3 + 1


def trimestres_entre(desde, hasta):
    anio, t = desde
    while (anio, t) <= tuple(hasta):
        yield anio, t
        anio, t = (anio, t + 1) if t < 4 else (anio + 1, 1)


def clasificar(doc, pagina, resumen):
    """Tipo, año y trimestres de un documento: el tipo por su nombre publicado
    (o el texto del enlace); el periodo por el nombre o el texto del enlace y,
    lo que no digan, por la página."""
    doc.tipo = tipo_documento(doc.nombre, doc.texto, pagina.get("resumen", False))
    anio, trimestres = periodo(doc.nombre)
    anio_texto, trimestres_texto = periodo(doc.texto)
    anio, trimestres = anio or anio_texto, trimestres or trimestres_texto
    anio_pagina, trimestres_pagina = pagina.get("anio"), tuple(pagina.get("trimestres") or ())
    if doc.tipo == "resumen_estadistico":
        doc.subtipo = subtipo_resumen(doc.nombre, doc.texto)
        trimestres = trimestres_pagina = ()
    elif trimestres and anio_pagina and trimestres_pagina and (
            (anio or anio_pagina, trimestres) != (anio_pagina, trimestres_pagina)):
        resumen.avisos.append(f"{doc.nombre}: el nombre dice {etiqueta_trimestre(trimestres)} {anio or anio_pagina} "
                              f"y la página ({doc.pagina}) {etiqueta_trimestre(trimestres_pagina)} {anio_pagina}; "
                              "se usa el del nombre")
    doc.anio = anio or anio_pagina
    doc.trimestres = tuple(trimestres or trimestres_pagina)
    if doc.anio is None or (not doc.trimestres and doc.tipo != "resumen_estadistico"):
        resumen.avisos.append(f"{doc.nombre}: sin año o trimestre en el nombre, el enlace ni la página "
                              f"({doc.pagina}); se guarda como {doc.base}")
    if doc.tipo == "otros":
        resumen.avisos.append(f"{doc.nombre} ('{doc.texto}', {doc.pagina}): no es de ningún tipo conocido; "
                              "se guarda en raw/otros/ (revisar)")


def documentos_enlazados(leidas, resumen):
    """Documentos que enlazan las páginas leídas, sin repetir (por su clave),
    con su tipo, año y trimestres."""
    documentos = {}
    for url in sorted(leidas):
        for enlace in leidas[url]["documentos"]:
            nombre, clave = partes_documento(enlace["url"])
            if clave in documentos:
                if url not in documentos[clave].paginas:
                    documentos[clave].paginas.append(url)
                continue
            doc = Documento(url=enlace["url"], nombre=nombre, texto=enlace["texto"], clave=clave, pagina=url,
                            paginas=[url])
            clasificar(doc, leidas[url], resumen)
            documentos[clave] = doc
    return sorted(documentos.values(), key=lambda d: (d.tipo, d.anio or 0, d.trimestres, d.nombre, d.clave))


def descubrir(raw, resumen, comprobar_todo=False):
    """Recorre las páginas del registro: las del buscador, las conocidas, las de
    la ejecución anterior y los slugs de los trimestres que ninguna cubre.
    Guarda raw/paginas.json. Devuelve (documentos, completo): completo es False
    si algo impide saber qué ha dejado de enlazarse (entonces no se retira nada)."""
    print("\n🔎 Páginas del Registro de Contratos...")
    previas = leer_inventario(raw)
    del_buscador, completo = paginas_del_buscador(resumen)
    conocidas = {URL_PAGINA.format(slug=slug) for slug in PAGINAS_CONOCIDAS}
    leidas, conservadas, visitadas = {}, {}, set()

    def visitar(url):
        nonlocal completo
        visitadas.add(url)
        estado, info = leer_pagina(url)
        time.sleep(PAUSA)
        antes = previas.get(url) or {}
        if estado == "ok":
            leidas[url] = info
            if not info["documentos"] and antes.get("documentos"):
                resumen.fallidos.append(f"{url}: la página ya no enlaza ningún documento; no se retira nada")
                completo = False
            elif not info["documentos"]:
                resumen.avisos.append(f"{url}: página del registro sin documentos enlazados")
            elif url not in previas:
                print(f"   + {url} ({info['descripcion'] or 'sin descripción'}): {len(info['documentos'])} documentos")
            return estado
        problema = None
        if estado == "error":
            problema = f"{info}"
        elif url in del_buscador:
            problema = f"el buscador la lista pero da {info}"
        elif estado == "ajena" and antes.get("documentos"):
            problema = f"ya no es una página del registro (título: {info})"
        if problema:
            resumen.fallidos.append(f"{url}: {problema}; no se retira nada")
            print(f"  ❌ {url}: {problema}")
            completo = False
            if antes:
                conservadas[url] = antes
        elif url in previas:
            resumen.avisos.append(f"{url}: la página ya no está ({info}); lo que solo enlazaba ella queda "
                                  "como retirado")
        elif url in conocidas:
            resumen.avisos.append(f"{url}: página conocida que ya no está ({info})")
        return estado

    for url in sorted(set(del_buscador) | conocidas | set(previas)):
        visitar(url)

    # Trimestres que no cubre ninguna página leída: slugs candidatos
    cubiertos = set()
    for info in leidas.values():
        cubiertos |= {(info["anio"], t) for t in info["trimestres"]}
        for enlace in info["documentos"]:
            anio, trimestres = periodo(enlace["nombre"])
            cubiertos |= {(anio or info["anio"], t) for t in trimestres}
    for anio, t in trimestres_entre(PRIMER_TRIMESTRE, trimestre_actual()):
        if (anio, t) in cubiertos and not comprobar_todo:
            continue
        for plantilla in SLUGS_TRIMESTRE:
            url = URL_PAGINA.format(slug=plantilla.format(t=t, anio=anio, aa=anio % 100))
            if url not in visitadas:
                visitar(url)

    guardar_json(Path(raw) / "paginas.json", {**conservadas, **leidas})
    documentos = documentos_enlazados(leidas, resumen)
    print(f"   {len(leidas)} páginas del registro, {len(documentos)} documentos enlazados")
    if not documentos:
        resumen.fallidos.append("ninguna página del registro enlaza documentos; no se retira nada")
        completo = False
    return documentos, completo


def asignar_rutas(documentos, manifiesto):
    """Ruta local (relativa a raw/) de cada documento: la que ya tiene en el
    manifiesto (por su clave) o <tipo>/<base><ext>. Si esa ruta es de otro
    documento que sigue enlazado, se añaden los 8 primeros caracteres del
    uuid. Un documento nuevo que cae en la ruta de otro que ya no se enlaza la
    ocupa: es una versión nueva del mismo listado (la anterior queda en
    _historico/ y sus filas que ya no están, con _en_ultima_descarga=False)."""
    ruta_de = {}
    for rel, entrada in sorted(manifiesto.datos.items()):
        if entrada.get("clave"):
            ruta_de.setdefault(entrada["clave"], rel)
    enlazados = {doc.clave for doc in documentos}
    ocupadas = {rel for clave, rel in ruta_de.items() if clave in enlazados}
    for doc in documentos:
        doc.rel = ruta_de.get(doc.clave)
    for doc in sorted((d for d in documentos if d.rel is None), key=lambda d: (d.nombre, d.clave)):
        rel = f"{doc.tipo}/{doc.base}{doc.extension}"
        if rel in ocupadas:
            resto = doc.clave if PATRON_UUID.fullmatch(doc.clave) else hashlib.sha1(doc.clave.encode()).hexdigest()
            rel = f"{doc.tipo}/{doc.base}_{resto[:8]}{doc.extension}"
            if rel in ocupadas:
                rel = f"{doc.tipo}/{doc.base}_{hashlib.sha1(doc.clave.encode()).hexdigest()}{doc.extension}"
        doc.rel = rel
        ocupadas.add(rel)


def descargar_documentos(raw, documentos, manifiesto, resumen, comprobar_todo=False):
    """Descarga cada documento enlazado en su ruta local (guardar_version). Los
    de años anteriores al pasado que ya se tienen, enlazados con la misma
    versión (?t=), solo se vuelven a pedir con --comprobar-todo."""
    anio_actual = ahora().year
    print(f"\n📦 Documentos enlazados: {len(documentos)}")
    for doc in documentos:
        destino = Path(raw) / doc.rel
        entrada = manifiesto.get(doc.rel)
        if (destino.exists() and not comprobar_todo and entrada.get("url") == doc.url
                and entrada.get("publicado", True) and doc.anio and doc.anio < anio_actual - 1):
            resumen.sin_cambios.append(f"{doc.rel} (ya descargado y enlazado con la misma versión; "
                                       "--comprobar-todo para volver a pedirlo)")
            continue
        estado, detalle = descargar(doc.url, destino, tipo=doc.extension.lstrip(".") or None)
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, doc.url, estado, sha256=sha256(destino), **doc.metadatos())
            resumen.descarga(doc.rel, estado)
            print(f"  ✅ {doc.rel}: {estado}")
        else:
            # Enlazado y no se puede bajar: se conserva la copia y se reintenta
            resumen.fallidos.append(f"{doc.rel}: {detalle or estado} ({doc.url}, enlazado en {doc.pagina})")
            print(f"  ❌ {doc.rel}: {detalle or estado}")


def retirar_no_enlazados(raw, documentos, manifiesto, resumen):
    """Los ficheros cuyo documento ya no enlaza ninguna página quedan como
    retirados (sus filas se conservan con _en_ultima_descarga=False)."""
    enlazados = {doc.clave for doc in documentos}
    for rel, entrada in sorted(manifiesto.datos.items()):
        ruta = Path(raw) / rel
        if (entrada.get("clave") and entrada["clave"] not in enlazados and entrada.get("publicado", True)
                and ruta.exists()):
            manifiesto.retirar(ruta, "ninguna página del registro lo enlaza")
            resumen.retirados.append(f"{rel} ({entrada.get('nombre_publicado')}): ninguna página del registro lo "
                                     "enlaza; se conservan sus filas")
            print(f"  🗑️ {rel}: retirado por el portal")


def comprobar_trimestres(documentos, manifiesto, resumen):
    """Un trimestre confirmado sin listado de menores (ni enlazado ni
    descargado antes) es un error; los trimestres cerrados posteriores que
    falten se anotan como no publicados."""
    hay = {(doc.anio, t) for doc in documentos if doc.tipo == "menores" for t in doc.trimestres}
    hay |= {(e.get("anio"), t) for e in manifiesto.datos.values() if e.get("tipo") == "menores"
            for t in e.get("trimestres") or ()}
    for anio, t in TRIMESTRES_CONFIRMADOS:
        if (anio, t) not in hay:
            resumen.fallidos.append(f"menores {t}T {anio}: trimestre publicado según las fuentes y ninguna "
                                    "página del registro lo enlaza")
    anio, t = trimestre_actual()
    cerrado = (anio, t - 1) if t > 1 else (anio - 1, 4)
    ultimo = max(TRIMESTRES_CONFIRMADOS, default=PRIMER_TRIMESTRE)
    siguiente = (ultimo[0], ultimo[1] + 1) if ultimo[1] < 4 else (ultimo[0] + 1, 1)
    for anio, t in trimestres_entre(siguiente, cerrado):
        if (anio, t) not in hay:
            resumen.no_publicado("menores (trimestres cerrados sin listado todavía)", f"{t}T {anio}")


def descargar_todo(raw, manifiesto, resumen, comprobar_todo=False):
    documentos, completo = descubrir(raw, resumen, comprobar_todo)
    asignar_rutas(documentos, manifiesto)
    descargar_documentos(raw, documentos, manifiesto, resumen, comprobar_todo)
    if completo:
        retirar_no_enlazados(raw, documentos, manifiesto, resumen)
    comprobar_trimestres(documentos, manifiesto, resumen)


# ----------------------------------------------------------------------------
# Parquet por serie
# ----------------------------------------------------------------------------

def archivos_serie(raw, manifiesto, tipos):
    """(ruta, rel, entrada del manifiesto) de las copias actuales de los
    documentos tabulares de estos tipos (el del manifiesto o, sin él, la
    carpeta), por año y trimestre."""
    lista = []
    for ruta in sorted(Path(raw).glob("*/*")):
        if not ruta.is_file() or ruta.name.startswith(".") or ruta.suffix.lower() not in EXTENSIONES_TABLA:
            continue
        rel = manifiesto.rel(ruta)
        entrada = manifiesto.get(rel)
        if (entrada.get("tipo") or ruta.parent.name) in tipos:
            lista.append((ruta, rel, entrada))
    return sorted(lista, key=lambda x: (x[2].get("anio") or periodo_local(x[0].name)[0] or 0,
                                        x[2].get("trimestres") or [], x[1]))


def metadatos_fichero(serie, ruta, rel, entrada):
    """Columnas de origen de las filas de un fichero (del manifiesto o, sin
    entrada, de su nombre local)."""
    anio, trimestre = entrada.get("anio"), entrada.get("trimestre")
    if anio is None:
        anio, trimestre = periodo_local(ruta.name)
    return {"_fuente": entrada.get("url"), "_pagina": entrada.get("pagina"), "_dataset": serie,
            "_tipo": entrada.get("tipo") or ruta.parent.name,
            "_nombre_publicado": entrada.get("nombre_publicado") or ruta.name,
            "_anio": None if anio is None else str(anio), "_trimestre": trimestre, "_archivo_origen": rel}


def marcar_repetidos(df):
    """_repetido_de: si el número de registro del contrato (sin los espacios de
    alrededor) ya estaba en un listado anterior, el _archivo_origen del primero;
    si no, nulo. No se quita ninguna fila. Solo número idéntico: la numeración
    CM005815/23 y la CM0000005815/2023 son de contratos distintos."""
    df = df.copy()
    numero = pd.Series([None] * len(df), index=df.index, dtype=object)
    for columna in COLUMNAS_NUMERO:
        if columna in df.columns:
            numero = numero.where(numero.notna(), df[columna])
    numero = numero.map(lambda v: (v.strip() or None) if isinstance(v, str) else None)

    def orden(fichero):
        rel, anio, trimestre = fichero
        return (int(anio) if isinstance(anio, str) and anio.isdigit() else 0,
                trimestre if isinstance(trimestre, str) else "", rel)

    ficheros = df[["_archivo_origen", "_anio", "_trimestre"]].drop_duplicates("_archivo_origen")
    posicion = {f[0]: i for i, f in enumerate(sorted(ficheros.itertuples(index=False, name=None), key=orden))}
    tabla = pd.DataFrame({"numero": numero, "posicion": df["_archivo_origen"].map(posicion),
                          "fichero": df["_archivo_origen"]}).dropna(subset=["numero"])
    primero = tabla.sort_values("posicion", kind="stable").drop_duplicates("numero").set_index("numero")["fichero"]
    de = numero.map(primero).astype(object)
    df["_repetido_de"] = de.where(de.notna() & (de != df["_archivo_origen"]), None)
    return df


def informe_trimestres(df, resumen):
    """Filas de menores por año y trimestre (cuántas ya no se publican y cuántas
    repiten un contrato de un listado anterior)."""
    claves = df[["_anio", "_trimestre"]].astype(object).where(df[["_anio", "_trimestre"]].notna(), "?")
    vigentes = df["_en_ultima_descarga"].astype(bool)
    repetidas = df["_repetido_de"].notna() if "_repetido_de" in df.columns else pd.Series(False, index=df.index)
    lineas = []
    for (anio, trimestre), filas in claves.groupby(["_anio", "_trimestre"], sort=True).groups.items():
        retiradas, repetidos = int((~vigentes.loc[filas]).sum()), int(repetidas.loc[filas].sum())
        notas = [f"{retiradas:,} ya no publicadas"] if retiradas else []
        notas += [f"{repetidos:,} ya estaban en un listado anterior (_repetido_de)"] if repetidos else []
        lineas.append(f"{anio} {trimestre}: {len(filas):,} filas" + (f" ({'; '.join(notas)})" if notas else ""))
    resumen.informes.append(("CONTRATOS MENORES POR TRIMESTRE", lineas))


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    for serie, tipos in SERIES.items():
        ficheros = [(ruta, rel, metadatos_fichero(serie, ruta, rel, entrada))
                    for ruta, rel, entrada in archivos_serie(raw, manifiesto, tipos)]
        destino = Path(salida) / f"{serie}.parquet"
        if ficheros or destino.exists():
            derivar = marcar_repetidos if serie in SERIES_CON_REPETIDOS else None
            df = construir_parquet(destino, ficheros, raw, manifiesto, resumen, derivar=derivar)
            if df is not None and serie == "registro_contratos_menores":
                informe_trimestres(df, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga el Registro de Contratos de la Junta de Extremadura "
                                                 "(contratos menores, mayores e incidencias)")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", "--solo-procesar", dest="solo_parquet", action="store_true",
                        help="no descargar; solo generar los Parquet con lo que hay en raw/")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir todos los documentos y probar los slugs de todos los trimestres")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Destino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
