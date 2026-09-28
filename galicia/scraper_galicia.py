"""
Scraper COMPLETO – Contratos Públicos de Galicia
=================================================
TODO el histórico, TODOS los organismos, TODOS los campos posibles.

    pip install requests pandas pyarrow python-dateutil beautifulsoup4
    python galicia/scraper_galicia.py
    python galicia/scraper_galicia.py --organismo 48
    python galicia/scraper_galicia.py merge --semilla /ruta/contratos_galicia_publicado.parquet

Fases: base (listados JSON de LIC y CM por organismo) -> detail (ficha HTML de
cada contrato, en caché SQLite) -> merge (tabla final contratos_galicia.csv y
.parquet). Todo se puede repetir y reanudar (--resume).

SESGO DEL SUPERVIVIENTE (comun/historico.py; docs/CONTINUACION.md §2)
El portal retira y cambia contratos: nada de lo descargado alguna vez se pierde.
- Capa cruda: contratos_galicia_base.csv es la descarga en curso (se escribe
  organismo a organismo, como siempre). Una descarga nueva (sin --resume) no
  borra la anterior: su CSV y su Parquet pasan a _historico/ (archivar) y, al
  terminar, si la nueva es idéntica vuelve la anterior (con su fecha) y no
  queda nada nuevo en _historico/ (lo mismo que guardar_version). El Parquet
  base, la tabla final y los ficheros de save_outputs se escriben siempre con
  guardar_version. No se empieza una descarga nueva mientras la anterior no
  esté en la tabla final (hay que ejecutar antes 'merge' o seguirla con
  --resume): así ninguna descarga se queda fuera de la tabla.
- Manifiesto de la descarga (contratos_galicia_base_progress.json, además de
  los organismos completados para --resume): fecha_descarga (inicio de la
  descarga, en UTC; se conserva al reanudar) y el ámbito que se ha vuelto a
  leer COMPLETO, por organismo: LIC si su paginación trae exactamente
  recordsTotal filas con id distinto, y cada ventana de CM que trae
  exactamente recordsFiltered filas (el portal da ahí el total de la ventana;
  recordsTotal es el del organismo) con id distinto y todas con 'publicado'
  dentro de la ventana. Una ventana o un organismo vacíos (0 filas) no cuentan
  como leídos: una respuesta vacía no retira nada.
- Caché de detalle (SQLite): nunca se borra. Una ficha ya descargada ('done')
  no se sustituye por un error ni por una ficha vacía (sin pares ni tablas);
  si el portal la cambia, la anterior pasa a la tabla detail_cache_historico.
  Sin --resume, 'detail' vuelve a intentar todas las fichas que no están
  'done' (antes borraba la caché y las pedía todas); --force-detail las vuelve
  a pedir todas.
- Tabla final: acumular(anterior, nuevos, fecha, ambito) con la tabla final
  anterior (su CSV, que guarda el texto tal cual) y el CSV base de la
  descarga. Cada contrato que se ha visto alguna vez sigue en ella con
  _primera_descarga, _ultima_descarga y _en_ultima_descarga: uno que el portal
  retira queda con _en_ultima_descarga=False y uno que cambia aparece dos
  veces (la versión anterior con False y la nueva). Solo se dan por retirados
  los contratos del ámbito de la descarga (LIC de un organismo completo, CM con
  'publicado' en una ventana completa): una ejecución parcial (--organismo,
  --skip-cm/--skip-lic, un corte, --resume, ventanas incompletas) no retira nada
  fuera de lo que ha leído, y una descarga vacía no escribe nada. Las filas se
  comparan por las 12 columnas del listado con los valores como quedan en el
  Parquet (números como número, fechas como fecha): el mismo contrato escrito
  '4' o '4.0' (el tipo de la columna depende de qué más trae el organismo) no
  es un cambio. Las columnas de la ficha no se comparan: salen de la caché (o,
  si la caché no tiene la ficha, de la tabla final anterior). Con una sola
  descarga la tabla es la de siempre más las 3 columnas de control al final.
  Repetir 'merge' sin descarga nueva no cambia nada.
- --semilla <parquet publicado> (repetible): el publicado se incorpora como la
  instantánea más antigua. Solo se añaden las filas cuya clave estable
  (_tipo, id) no está en la tabla (descarga nueva, contratos retirados y
  semillas ya incorporadas), y solo del ámbito de la descarga (fuera de él no
  se sabe si el portal las sigue listando), con _origen='release v2026.02' (o
  --origen-semilla) y _en_ultima_descarga=False; nunca se modifica ni se
  duplica una fila de la descarga. (_tipo, id) es único en el publicado
  (1.685.789 filas); el id solo no lo es (25.593 ids son a la vez CM y LIC).
  Errores conocidos del publicado (v2026.02, scraper antiguo) y qué se hace:
  * importe inflado x10/x100: el scraper antiguo quitaba el punto decimal del
    número JSON (674.78 -> 67478; 14900.0 -> 149000). No se puede deshacer con
    certeza (67478 puede ser 674.78 o 6747.8), así que en las filas añadidas
    'importe' queda vacío y el valor publicado va a 'importe_semilla'. (Solo
    es seguro cuando acaba en 0: entonces es x10.)
  * estado: el texto 'nan' (NaN del scraper antiguo) en los CM queda vacío,
    como lo escribe el scraper actual; '6.0' se conserva.
  * publicado y modificado se escriben como en el CSV base (AAAA-MM-DD, con
    la hora si no es medianoche); el publicado no tiene detalle HTML (las
    filas añadidas salen con detail_status 'missing') ni _primera_descarga /
    _ultima_descarga (no se sabe cuándo se descargaron).
  Una semilla que sea una salida de este script (con _en_ultima_descarga) se
  toma tal cual y necesita --origen-semilla; no puede ser la propia tabla
  final de --output.
"""

import argparse
import csv
import filecmp
import hashlib
import importlib.util
import json
import numbers
import os
from pathlib import Path
import random
import re
import signal
import sqlite3
import sys
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import unicodedata

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (  # noqa: E402
    ANADIDA,
    COLUMNAS_META,
    FUERA_AMBITO,
    IGNORAR_POR_DEFECTO,
    ORIGEN_SEMILLA,
    acumular,
    archivar,
    guardar_version,
    imprimir_informe_semilla,
    informe_semilla,
    seleccionar_semilla,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "galicia"
BASE_URL = "https://www.contratosdegalicia.gal"
DATE_ORIGIN = "2000-01-01"  # barrer hasta aquí, sin parar antes

PAGE_SIZE = 100
DELAY = 0.5
MAX_RETRIES = 3

# Ventana para CM (meses). El browser usa 3 meses. NO paramos antes.
CM_WINDOW_MONTHS = 3

# Auto-guardar cada N organismos
AUTOSAVE_EVERY = 10

DEFAULT_OUTPUT_DIR = DATA_DIR
BASE_CSV_NAME = "contratos_galicia_base.csv"
BASE_PARQUET_NAME = "contratos_galicia_base.parquet"
FINAL_CSV_NAME = "contratos_galicia.csv"
FINAL_PARQUET_NAME = "contratos_galicia.parquet"
DETAIL_DB_NAME = "contratos_galicia_detail.sqlite3"
BASE_PROGRESS_NAME = "contratos_galicia_base_progress.json"
DEFAULT_LOG_PATH = DATA_DIR / "scraper_galicia.log"
HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None
_LOG_PATH = None
DETAIL_THREAD_LOCAL = threading.local()
DETAIL_WORKERS = 8
DETAIL_BATCH_SIZE = 250
DETAIL_DELAY = 0.5
DETAIL_JITTER = 0.2
DETAIL_MAX_ATTEMPTS = 4
BAN_ERROR_THRESHOLD = 20
BASE_READ_CHUNKSIZE = 50000
DETAIL_QUERY_BATCH_SIZE = 250

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8,ca;q=0.7",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

COLS_CM = [
    {"data": "id",            "name": "id",            "orderable": "true"},
    {"data": "publicado",     "name": "publicado",     "orderable": "true"},
    {"data": "objeto",        "name": "objeto",        "orderable": "true"},
    {"data": "importe",       "name": "importe",       "orderable": "true"},
    {"data": "nif",           "name": "nif",           "orderable": "true"},
    {"data": "adjudicatario", "name": "adjudicatario", "orderable": "true"},
    {"data": "duracion",      "name": "duracion",      "orderable": "false"},
]

COLS_LIC = [
    {"data": "id",          "name": "id",         "orderable": "true"},
    {"data": "publicado",   "name": "publicado",  "orderable": "true"},
    {"data": "objeto",      "name": "objeto",     "orderable": "true"},
    {"data": "importe",     "name": "importe",    "orderable": "true"},
    {"data": "estadoDesc",  "name": "estado",     "orderable": "true"},
    {"data": "modificado",  "name": "modificado", "orderable": "true"},
]

BASE_EXPORT_FIELDS = [
    "id",
    "objeto",
    "importe",
    "estado",
    "estadoDesc",
    "publicado",
    "modificado",
    "_organismo_id",
    "_tipo",
    "nif",
    "adjudicatario",
    "duracion",
]

# Al comparar filas del listado entre descargas (listing_fingerprint), estas
# columnas cuentan por su valor (como en el Parquet) y no por su texto: el CSV
# base escribe 4 o 4.0 según qué otras filas traiga el organismo.
LISTING_NUMERIC_COLUMNS = ("id", "importe", "estado", "_organismo_id")
LISTING_DATE_COLUMNS = ("publicado", "modificado")
# Semilla publicada por el scraper antiguo: su importe inflado (ver arriba).
SEED_AMOUNT_COLUMN = "importe_semilla"
SEED_KEY = ["_tipo", "id"]
# Columnas de contenido con que seleccionar_semilla compara una fila de la
# semilla con la clave incompleta (no hay ninguna en v2026.02).
SEED_CONTENT_COLUMNS = ("_organismo_id", "objeto", "publicado", "nif", "adjudicatario")


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

class ScraperError(RuntimeError):
    """Error controlado del scraper cuando la descarga queda incompleta."""


def configure_log_path(path):
    global _LOG_PATH
    _LOG_PATH = Path(path) if path else None
    if _LOG_PATH:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LOG_PATH.write_text("", encoding="utf-8")


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    if _LOG_PATH:
        with _LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(f"{line}\n")

def log_warn(msg):  log(msg, "WARN")
def log_err(msg):   log(msg, "ERROR")
def log_debug(msg): log(msg, "DEBUG")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────

class Session:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)
        self.n_requests = 0
        self.n_errors = 0
        self.current_org_id = None
        self._init()

    def _request(
        self,
        url,
        method="GET",
        params=None,
        data=None,
        timeout=30,
        headers=None,
        retry=0,
        label="",
        count_error=True,
    ):
        self.n_requests += 1
        request_label = label or url

        try:
            response = self.s.request(
                method,
                url,
                params=params,
                data=data,
                timeout=timeout,
                headers=headers,
            )
        except requests.RequestException as exc:
            if retry < MAX_RETRIES:
                time.sleep(2 ** retry)
                return self._request(
                    url,
                    method=method,
                    params=params,
                    data=data,
                    timeout=timeout,
                    headers=headers,
                    retry=retry + 1,
                    label=label,
                    count_error=count_error,
                )
            if count_error:
                self.n_errors += 1
            raise ScraperError(f"{request_label}: {exc}") from exc

        if response.status_code == 429 and retry < MAX_RETRIES:
            wait = 2 ** (retry + 2)
            log_warn(f"{request_label}: 429 rate-limit, espera {wait}s...")
            time.sleep(wait)
            return self._request(
                url,
                method=method,
                params=params,
                data=data,
                timeout=timeout,
                headers=headers,
                retry=retry + 1,
                label=label,
                count_error=count_error,
            )

        if response.status_code >= 500 and retry < MAX_RETRIES:
            time.sleep(3)
            return self._request(
                url,
                method=method,
                params=params,
                data=data,
                timeout=timeout,
                headers=headers,
                retry=retry + 1,
                label=label,
                count_error=count_error,
            )

        if not response.ok:
            if count_error:
                self.n_errors += 1
            raise ScraperError(f"{request_label}: HTTP {response.status_code}")

        return response

    def _init(self):
        log("Obteniendo cookies de sesión...")
        self._request(
            f"{BASE_URL}/resultadoIndex.jsp?lang=es",
            timeout=30,
            label="bootstrap sesión",
        )
        log(f"Cookies OK: {list(self.s.cookies.get_dict().keys())}")

    def visit_org_page(self, org_id):
        """Visita la página del organismo para establecer contexto de sesión."""
        self.s.headers["Referer"] = (
            f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es"
        )
        try:
            self._request(
                f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es",
                timeout=15,
                label=f"visita organismo {org_id}",
            )
            self.current_org_id = org_id
        except ScraperError as exc:
            log_warn(f"No se pudo visitar la página del organismo {org_id}: {exc}")

    def ensure_org_context(self, org_id):
        if self.current_org_id != org_id:
            self.visit_org_page(org_id)

    def get_json(self, url, params, retry=0, headers=None, count_error=True):
        try:
            response = self._request(
                url,
                method="GET",
                params=params,
                timeout=30,
                headers=headers,
                retry=retry,
                label=url,
                count_error=count_error,
            )
            return response.json()
        except ValueError as exc:
            if count_error:
                self.n_errors += 1
            raise ScraperError(f"{url}: respuesta JSON inválida") from exc

    def post_html(self, url, data, retry=0, headers=None, count_error=True):
        response = self._request(
            url,
            method="POST",
            data=data,
            timeout=30,
            headers=headers,
            retry=retry,
            label=url,
            count_error=count_error,
        )
        return response.text


DETAIL_FIELD_MAP = {
    "referencia": "detail_referencia",
    "objeto": "detail_objeto",
    "tipo_de_tramitacion": "detail_tipo_tramitacion",
    "tipo_de_procedimiento": "detail_tipo_procedimiento",
    "tipo_de_contrato": "detail_tipo_contrato",
    "orzamento_base_de_licitacion": "detail_presupuesto_base_text",
    "valor_estimado": "detail_valor_estimado_text",
    "n_lotes": "detail_num_lotes_text",
    "no_lotes": "detail_num_lotes_text",
    "sistema_de_contratacion": "detail_sistema_contratacion",
    "tipo_de_financiamento": "detail_tipo_financiacion",
    "fecha_de_difusion_en_la_plataforma_de_contratos": "detail_fecha_difusion_text",
    "sello": "detail_sello",
    "fecha_formalizacion": "detail_fecha_formalizacion_text",
    "organo": "detail_organo",
    "direccion": "detail_direccion",
    "localidad": "detail_localidad",
    "c_p": "detail_cp",
    "telefono": "detail_telefono",
    "fax": "detail_fax",
    "correo_electronico": "detail_correo_electronico",
    "contrato_sara": "detail_contrato_sara",
    "contratacion_centralizada": "detail_contratacion_centralizada",
    "lei_nacional_de_aplicacion": "detail_ley_nacional_aplicacion",
    "contrato_mixto": "detail_contrato_mixto",
    "subasta_electronica": "detail_subasta_electronica",
    "compra_publica_estratexica": "detail_compra_publica_estrategica",
    "compra_publica_estrategica": "detail_compra_publica_estrategica",
    "direccion_electronico": "detail_direccion_electronica",
    "acuerdo_marco": "detail_acuerdo_marco",
    "prorroga": "detail_prorroga",
    "modificacion_objetiva_y_o_subjetiva": "detail_modificacion",
}

DETAIL_EXPORT_FIELDS = [
    "detail_status",
    "detail_attempts",
    "detail_last_http_status",
    "detail_last_error",
    "detail_updated_at",
    "detail_html_sha256",
    "detail_page_title",
    "detail_url",
    "detail_referencia",
    "detail_objeto",
    "detail_tipo_tramitacion",
    "detail_tipo_procedimiento",
    "detail_tipo_contrato",
    "detail_presupuesto_base_text",
    "detail_presupuesto_base_eur",
    "detail_valor_estimado_text",
    "detail_valor_estimado_eur",
    "detail_num_lotes_text",
    "detail_num_lotes",
    "detail_sistema_contratacion",
    "detail_tipo_financiacion",
    "detail_fecha_difusion_text",
    "detail_fecha_difusion",
    "detail_fecha_formalizacion_text",
    "detail_fecha_formalizacion",
    "detail_sello",
    "detail_organo",
    "detail_direccion",
    "detail_localidad",
    "detail_cp",
    "detail_telefono",
    "detail_fax",
    "detail_correo_electronico",
    "detail_contrato_sara",
    "detail_contratacion_centralizada",
    "detail_ley_nacional_aplicacion",
    "detail_contrato_mixto",
    "detail_subasta_electronica",
    "detail_compra_publica_estrategica",
    "detail_acuerdo_marco",
    "detail_prorroga",
    "detail_modificacion",
    "detail_cpv_codes",
    "detail_nuts_codes",
    "detail_publicaciones_count",
    "detail_documentos_count",
    "detail_adjudicaciones_count",
    "detail_cambios_count",
    "detail_pairs_count",
    "detail_tables_count",
    "detail_adjudicaciones_json",
    "detail_campos_extra_json",
]


def strip_accents(value):
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def normalize_label(value):
    value = strip_accents(value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def compact_json(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def compress_text(value):
    if not value:
        return None
    return sqlite3.Binary(zlib.compress(value.encode("utf-8"), level=9))


def decompress_text(value):
    if value in (None, b"", ""):
        return None
    if isinstance(value, str):
        return value
    return zlib.decompress(value).decode("utf-8")


def parse_amount(value):
    if not value:
        return None
    cleaned = strip_accents(str(value))
    match = re.search(r"(-?[\d\.\,]+)", cleaned)
    if not match:
        return None
    amount = match.group(1).replace(".", "").replace(",", ".")
    try:
        return float(amount)
    except ValueError:
        return None


def parse_date_text(value):
    if not value:
        return None
    text = str(value).strip()
    try:
        dt = pd.to_datetime(text, errors="raise", dayfirst=True)
    except Exception:
        return None
    if pd.isna(dt):
        return None
    return dt.isoformat()


def nearest_heading_text(tag):
    heading = tag.find_previous(["h2", "h3", "h4"])
    if heading:
        return " ".join(heading.get_text(" ", strip=True).split())
    return ""


def extract_links(tag):
    links = []
    for anchor in tag.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if not href:
            continue
        try:
            resolved = requests.compat.urljoin(BASE_URL, href)
        except ValueError:
            # El portal devuelve a veces hrefs malformados; conservar el valor
            # bruto es mejor que tumbar todo el batch de detalle.
            resolved = href
        if resolved not in links:
            links.append(resolved)
    return links


def extract_detail_pairs(soup):
    pairs = []
    for dl in soup.find_all("dl"):
        labels = dl.find_all("dt")
        values = dl.find_all("dd")
        if not labels or not values:
            continue
        section = nearest_heading_text(dl)
        for dt, dd in zip(labels, values):
            label = " ".join(dt.get_text(" ", strip=True).split())
            value = " ".join(dd.get_text(" ", strip=True).split())
            if label or value:
                pairs.append(
                    {
                        "section": section,
                        "label": label,
                        "key": normalize_label(label),
                        "value": value,
                        "links": extract_links(dd),
                    }
                )
    return pairs


def extract_detail_tables(soup):
    tables = []
    for index, table in enumerate(soup.find_all("table"), start=1):
        headers = []
        header_row = table.find("tr")
        if header_row:
            headers = [
                " ".join(cell.get_text(" ", strip=True).split())
                for cell in header_row.find_all(["th", "td"])
            ]
        rows = []
        for row_index, row in enumerate(table.find_all("tr")[1:], start=1):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            values = [" ".join(cell.get_text(" ", strip=True).split()) for cell in cells]
            row_data = values
            if headers and len(headers) == len(values):
                row_data = dict(zip(headers, values))
            rows.append(
                {
                    "row_index": row_index,
                    "values": row_data,
                    "links": [extract_links(cell) for cell in cells],
                }
            )
        if headers or rows:
            tables.append(
                {
                    "index": index,
                    "section": nearest_heading_text(table),
                    "headers": headers,
                    "rows": rows,
                }
            )
    return tables


def unique_join(values):
    seen = []
    for value in values:
        cleaned = str(value).strip()
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return ", ".join(seen) if seen else None


def normalize_table_row(values):
    if isinstance(values, dict):
        normalized = {}
        for key, value in values.items():
            norm_key = normalize_label(key) or "value"
            normalized[norm_key] = value
        return normalized
    return values


def map_table_fields(tables):
    cpv_codes = []
    nuts_codes = []
    publicaciones_count = 0
    documentos_count = 0
    adjudicaciones_count = 0
    cambios_count = 0

    for table in tables:
        headers = tuple(normalize_label(header) for header in table.get("headers", []))
        normalized_rows = [normalize_table_row(row["values"]) for row in table.get("rows", [])]

        if "codigo_cpv" in headers:
            for row in normalized_rows:
                if isinstance(row, dict):
                    cpv_codes.append(row.get("codigo_cpv"))
                elif row:
                    cpv_codes.append(row[0])
            continue

        if "nut" in headers:
            for row in normalized_rows:
                if isinstance(row, dict):
                    nuts_codes.append(row.get("nut"))
                elif row:
                    nuts_codes.append(row[0])
            continue

        if headers == ("perfil", "bop", "dog", "boe", "fecha_envio_doue"):
            publicaciones_count += len(normalized_rows)
            continue

        if headers == ("titulo", "fecha", "estado", "descarga"):
            documentos_count += len(normalized_rows)
            continue

        if "adjudicatario" in headers and "importe" in headers:
            adjudicaciones_count += len(normalized_rows)
            continue

        if headers == ("fecha", "cambio"):
            cambios_count += len(normalized_rows)

    return {
        "detail_cpv_codes": unique_join(cpv_codes),
        "detail_nuts_codes": unique_join(nuts_codes),
        "detail_publicaciones_count": publicaciones_count or None,
        "detail_documentos_count": documentos_count or None,
        "detail_adjudicaciones_count": adjudicaciones_count or None,
        "detail_cambios_count": cambios_count or None,
    }


def detail_unmapped_fields(pairs, tables):
    """Lo que la ficha ofrece y no tiene columna propia; antes solo quedaba en la caché
    SQLite (y solo con raw activado):

    - filas de las tablas de adjudicación (adjudicatario, importe... por lote): en LIC es
      el único sitio con el adjudicatario y el importe adjudicado, y solo se contaban;
    - etiquetas sin columna en DETAIL_FIELD_MAP y repeticiones de las mapeadas (p. ej.
      una "Fecha formalización" por lote), de las que solo se guardaba la primera.
    """
    adjudicaciones = []
    for table in tables:
        headers = {normalize_label(header) for header in table.get("headers", [])}
        if "adjudicatario" in headers and "importe" in headers:
            adjudicaciones.extend(row["values"] for row in table.get("rows", []))
    extra = []
    seen = set()
    for pair in pairs:
        target_key = DETAIL_FIELD_MAP.get(pair["key"])
        if target_key and target_key not in seen:
            seen.add(target_key)
            continue
        extra.append({"section": pair["section"], "label": pair["label"], "value": pair["value"]})
    return {
        "detail_adjudicaciones_json": compact_json(adjudicaciones) if adjudicaciones else None,
        "detail_campos_extra_json": compact_json(extra) if extra else None,
    }


def map_detail_fields(page_title, pairs, tables):
    mapped = {
        "detail_page_title": page_title,
        "detail_pairs_count": len(pairs),
        "detail_tables_count": len(tables),
        "detail_status": "done",
    }
    seen = set()
    for pair in pairs:
        source_key = pair["key"]
        target_key = DETAIL_FIELD_MAP.get(source_key)
        if not target_key or target_key in seen:
            continue
        mapped[target_key] = pair["value"]
        seen.add(target_key)
    mapped.update(detail_unmapped_fields(pairs, tables))

    mapped["detail_presupuesto_base_eur"] = parse_amount(mapped.get("detail_presupuesto_base_text"))
    mapped["detail_valor_estimado_eur"] = parse_amount(mapped.get("detail_valor_estimado_text"))
    mapped["detail_num_lotes"] = parse_amount(mapped.get("detail_num_lotes_text"))
    mapped["detail_fecha_difusion"] = parse_date_text(mapped.get("detail_fecha_difusion_text"))
    mapped["detail_fecha_formalizacion"] = parse_date_text(mapped.get("detail_fecha_formalizacion_text"))
    mapped.update(map_table_fields(tables))
    return mapped


def parse_detail_html(html):
    soup = BeautifulSoup(html, "html.parser")
    page_title = " ".join(soup.title.get_text(" ", strip=True).split()) if soup.title else ""
    if "Detalle proced" not in page_title:
        raise ScraperError(f"Página de detalle inválida: {page_title or 'sin título'}")
    pairs = extract_detail_pairs(soup)
    tables = extract_detail_tables(soup)
    mapped = map_detail_fields(page_title, pairs, tables)
    return {
        "page_title": page_title,
        "pairs": pairs,
        "tables": tables,
        "mapped": mapped,
    }


def build_detail_payload(record_type, record_id, organismo_id, lang="es"):
    payload = {
        "N": str(record_id),
        "OR": str(organismo_id),
        "ID": "LC",
        "ID2": str(organismo_id),
        "lang": lang,
        "S": "C" if record_type == "LIC" else "CM",
    }
    if record_type == "CM":
        payload["N"] = f"CM{record_id}"
    return payload


def detail_record_key(record_type, record_id, organismo_id):
    return f"{record_type}:{record_id}:{organismo_id}"


def get_detail_session():
    session = getattr(DETAIL_THREAD_LOCAL, "session", None)
    if session is None:
        session = Session()
        DETAIL_THREAD_LOCAL.session = session
    return session


# ─────────────────────────────────────────────────────────────────────────────
# PARAMS BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _cols_params(cols):
    p = {}
    for i, col in enumerate(cols):
        p[f"columns[{i}][data]"] = col["data"]
        p[f"columns[{i}][name]"] = col["name"]
        p[f"columns[{i}][searchable]"] = "true"
        p[f"columns[{i}][orderable]"] = col["orderable"]
        p[f"columns[{i}][search][value]"] = ""
        p[f"columns[{i}][search][regex]"] = "false"
    return p


def params_cm(start, length, date_start, date_end, draw=1):
    p = {
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "1",
        "order[0][dir]": "desc",
        "datestart": date_start,
        "dateend": date_end,
        "_": str(int(time.time() * 1000)),
    }
    p.update(_cols_params(COLS_CM))
    return p


def params_lic(start, length, draw=1):
    p = {
        "draw": str(draw),
        "start": str(start),
        "length": str(length),
        "search[value]": "",
        "search[regex]": "false",
        "order[0][column]": "1",
        "order[0][dir]": "desc",
        "idioma": "es",
        "total": "",
        "estados": "",
        "_": str(int(time.time() * 1000)),
    }
    p.update(_cols_params(COLS_LIC))
    return p

# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def discover(session, max_id, workers=5):
    log(f"Descubriendo organismos (IDs 1–{max_id})...")
    found = {}
    failed_lic = []
    failed_cm = []

    def probe_lic(org_id):
        url = f"{BASE_URL}/api/v1/organismos/{org_id}/licitaciones/table"
        p = params_lic(0, 1)
        try:
            return session.get_json(url, p, retry=0).get("recordsTotal", 0)
        except ScraperError as exc:
            return exc

    def probe_cm(org_id):
        url = f"{BASE_URL}/api/v1/organismos/{org_id}/contratosmenores/table"
        d_end = datetime.now()
        d_start = d_end - timedelta(days=90)
        # Referer dinámico
        headers_copy = dict(session.s.headers)
        headers_copy["Referer"] = f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es"
        p = params_cm(0, 1, d_start.strftime("%Y-%m-%d"), d_end.strftime("%Y-%m-%d"))
        try:
            return session.get_json(url, p, retry=0, headers=headers_copy).get("recordsTotal", 0)
        except ScraperError as exc:
            return exc

    # Fase 1: licitaciones (paralelo, rápido)
    log("Fase 1/2 → licitaciones (paralelo)...")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(probe_lic, i): i for i in range(1, max_id + 1)}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 100 == 0:
                sys.stdout.write(f"\r    {done}/{max_id}...")
                sys.stdout.flush()
            oid = futs[f]
            n = f.result()
            if isinstance(n, ScraperError):
                failed_lic.append((oid, str(n)))
                continue
            if n > 0:
                found[oid] = {"cm": 0, "lic": n}
    print()

    # Fase 2: CM (secuencial, necesita Referer)
    log("Fase 2/2 → contratosmenores (secuencial, Referer dinámico)...")
    for i in range(1, max_id + 1):
        if i % 50 == 0:
            sys.stdout.write(f"\r    {i}/{max_id}...")
            sys.stdout.flush()
        n = probe_cm(i)
        if isinstance(n, ScraperError):
            failed_cm.append((i, str(n)))
            continue
        if n > 0:
            if i not in found:
                found[i] = {"cm": 0, "lic": 0}
            found[i]["cm"] = n
    print()

    if failed_lic or failed_cm:
        sample = failed_lic[:2] + failed_cm[:2]
        raise ScraperError(
            "Discovery incompleto: "
            f"fallos LIC={len(failed_lic)} CM={len(failed_cm)} | ejemplo={sample}"
        )

    # Tabla resumen
    log(f"{len(found)} organismos activos:")
    print()
    print(f"    {'ORG':>6}  │  {'CM(3m)':>10}  │  {'LIC':>8}  │  {'TOTAL':>11}")
    print(f"    {'─'*6}──┼──{'─'*10}──┼──{'─'*8}──┼──{'─'*11}")
    sum_cm = sum_lic = 0
    result = []
    for oid in sorted(found):
        cm = found[oid]["cm"]
        lic = found[oid]["lic"]
        sum_cm += cm
        sum_lic += lic
        result.append((oid, cm, lic))
        print(f"    {oid:>6}  │  {cm:>10,}  │  {lic:>8,}  │  {cm+lic:>11,}")
    print(f"    {'─'*6}──┼──{'─'*10}──┼──{'─'*8}──┼──{'─'*11}")
    print(f"    {'TOTAL':>6}  │  {sum_cm:>10,}  │  {sum_lic:>10,}  │  {sum_cm+sum_lic:>11,}")
    print(f"\n    CM(3m) = solo últimos 3 meses. El scraping barre TODO hasta {DATE_ORIGIN}.")
    print()
    return result


# ─────────────────────────────────────────────────────────────────────────────
# PAGINATION
# ─────────────────────────────────────────────────────────────────────────────

def paginate_lic(session, org_id, informe=None):
    """Todas las licitaciones de un organismo.

    informe (dict, opcional): se anota en informe["LIC"] si la paginación está
    completa (exactamente recordsTotal filas, todas con id distinto, y alguna):
    solo entonces sus licitaciones cuentan como vueltas a leer (ámbito) y las
    que falten se pueden dar por retiradas.
    """
    url = f"{BASE_URL}/api/v1/organismos/{org_id}/licitaciones/table"
    session.visit_org_page(org_id)

    all_recs = []
    start = 0
    total = None
    draw = 1

    while True:
        p = params_lic(start, PAGE_SIZE, draw)
        draw += 1
        data = session.get_json(url, p)

        if total is None:
            total = data.get("recordsTotal", 0)
            if total == 0:
                if informe is not None:
                    informe["LIC"] = {"declarados": 0, "filas": 0, "unicos": 0, "completo": False}
                return []
            log(f"Org {org_id} LIC: {total:,} registros")

        recs = data.get("data", [])
        if not recs:
            break

        if start == 0:
            log_debug(f"Org {org_id} LIC campos: {list(recs[0].keys())}")

        all_recs.extend(recs)
        start += len(recs)

        pct = min(100, start * 100 / total)
        sys.stdout.write(f"\r    Org {org_id} LIC: {start:,}/{total:,} ({pct:.0f}%)")
        sys.stdout.flush()

        if start >= total:
            break
        time.sleep(DELAY)

    if total and total > 0:
        # Por ids únicos: si la paginación repite filas (orden por fecha con empates) el
        # número de filas cuadra aunque falten licitaciones, y to_dataframe quita el duplicado
        unique = len({str(r.get("id")) for r in all_recs})
        ok = "✓" if len(all_recs) == total and unique == total else "⚠"
        sys.stdout.write(f"\r    Org {org_id} LIC: {len(all_recs):,}/{total:,} {ok}          \n")
        sys.stdout.flush()
        if len(all_recs) != total or unique != total:
            log_warn(
                f"Org {org_id} LIC: DESAJUSTE esperados={total:,} descargados={len(all_recs):,} "
                f"únicos={unique:,} (sus licitaciones no se dan por retiradas en esta descarga)"
            )
    if informe is not None:
        unique = len({str(r.get("id")) for r in all_recs if r.get("id") not in (None, "")})
        informe["LIC"] = {
            "declarados": total or 0,
            "filas": len(all_recs),
            "unicos": unique,
            "completo": bool(total) and len(all_recs) == unique == total,
        }

    for r in all_recs:
        r["_organismo_id"] = org_id
        r["_tipo"] = "LIC"
    return all_recs


def paginate_cm_window(session, org_id, date_start, date_end, informe=None):
    """Pagina CM para una ventana de fecha.

    informe (dict, opcional): recibe 'filtrados' (recordsFiltered de la primera
    página: los contratos de la ventana según el portal, verificado en vivo el
    2026-09-28), 'filas' y 'unicos' (ids distintos) para comprobar que la
    ventana ha llegado completa.
    """
    url = f"{BASE_URL}/api/v1/organismos/{org_id}/contratosmenores/table"
    all_recs = []
    start = 0
    total = None
    draw = 1
    if informe is not None:
        informe.update(filtrados=None, filas=0, unicos=0)

    while True:
        p = params_cm(start, PAGE_SIZE, date_start, date_end, draw)
        draw += 1
        data = session.get_json(url, p)

        if total is None:
            total = data.get("recordsTotal", 0)
            if informe is not None:
                informe["filtrados"] = data.get("recordsFiltered")
            if total == 0:
                return [], 0

        recs = data.get("data", [])
        if not recs:
            break

        all_recs.extend(recs)
        start += len(recs)

        if total > PAGE_SIZE:
            pct = min(100, start * 100 / total)
            sys.stdout.write(f"\r      [{date_start}→{date_end}] {start:,}/{total:,} ({pct:.0f}%)")
            sys.stdout.flush()

        if start >= total:
            break
        time.sleep(DELAY)

    if total and total > PAGE_SIZE:
        sys.stdout.write(f"\r      [{date_start}→{date_end}] {len(all_recs):,}/{total:,} ✓          \n")
        sys.stdout.flush()

    if informe is not None:
        informe["filas"] = len(all_recs)
        informe["unicos"] = len({r.get("id") for r in all_recs if r.get("id") not in (None, "")})
    return all_recs, total or 0


def window_check(recs, date_start, date_end, informe):
    """Completa el informe de una ventana de CM (paginate_cm_window): 'fuera'
    (filas sin 'publicado' o con él fuera de la ventana), 'sin_id' y 'completa':
    exactamente recordsFiltered filas, con id distinto y todas dentro de la
    ventana. Solo una ventana completa y no vacía cuenta como vuelta a leer."""
    fechas = parse_datetime_series(pd.Series([r.get("publicado") for r in recs], dtype=object)).dt.normalize()
    dentro = fechas.between(pd.Timestamp(date_start), pd.Timestamp(date_end))
    informe["fuera"] = int((~dentro).sum())
    informe["sin_id"] = sum(1 for r in recs if r.get("id") in (None, ""))
    try:
        filtrados = int(informe.get("filtrados"))
    except (TypeError, ValueError):
        filtrados = None
    informe["completa"] = bool(
        filtrados is not None
        and filtrados > 0
        and informe["filas"] == informe["unicos"] == filtrados
        and not informe["fuera"]
        and not informe["sin_id"]
    )
    return informe


def paginate_cm_full(session, org_id, informe=None):
    """
    CM: barre TODAS las ventanas de CM_WINDOW_MONTHS desde hoy hasta DATE_ORIGIN.
    SIN parar antes — recorre todo el rango completo.

    informe (dict, opcional): informe["CM"] recibe las ventanas completas y no
    vacías ('ventanas': [[desde, hasta], ...], ver window_check), que son el
    ámbito de la descarga en CM, y las incompletas (se avisan en el log: sus
    contratos no se dan por retirados).
    """
    session.visit_org_page(org_id)

    now = datetime.now()
    d_end = now
    min_date = datetime.strptime(DATE_ORIGIN, "%Y-%m-%d")

    all_recs = []
    seen_ids = set()
    first_debug = True
    total_windows = 0
    windows_with_data = 0
    reported_total = 0
    complete_windows = []
    incomplete_windows = []

    # Calcular número de ventanas para progreso
    temp = now
    n_total_windows = 0
    while temp > min_date:
        n_total_windows += 1
        temp -= relativedelta(months=CM_WINDOW_MONTHS)

    date_end = now.strftime("%Y-%m-%d")
    log(
        f"Org {org_id} CM: barriendo {n_total_windows} ventanas de "
        f"{CM_WINDOW_MONTHS} meses [{DATE_ORIGIN} → {date_end}]"
    )

    while d_end > min_date:
        d_start = d_end - relativedelta(months=CM_WINDOW_MONTHS)
        if d_start < min_date:
            d_start = min_date

        ds = d_start.strftime("%Y-%m-%d")
        de = d_end.strftime("%Y-%m-%d")
        total_windows += 1

        window = {}
        recs, reported = paginate_cm_window(session, org_id, ds, de, informe=window)
        reported_total = max(reported_total, reported)
        window_check(recs, ds, de, window)
        if window["completa"]:
            complete_windows.append([ds, de])
        elif window["filas"] or window["filtrados"]:
            incomplete_windows.append(dict(window, desde=ds, hasta=de))
            log_warn(
                f"Org {org_id} CM [{ds}→{de}]: ventana incompleta (el portal declara "
                f"{window['filtrados']}, llegan {window['filas']:,} filas, {window['unicos']:,} ids "
                f"distintos, {window['fuera']:,} fuera de la ventana y {window['sin_id']:,} sin id): "
                "sus contratos no se dan por retirados en esta descarga"
            )

        new_recs = []
        for r in recs:
            rid = r.get("id")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                new_recs.append(r)

        if new_recs:
            windows_with_data += 1
            if first_debug:
                log_debug(f"Org {org_id} CM campos: {list(new_recs[0].keys())}")
                first_debug = False
            log(f"    Org {org_id} CM [{ds}→{de}]: {len(new_recs):,} nuevos (server: {reported:,}) | acum: {len(all_recs)+len(new_recs):,}")
            all_recs.extend(new_recs)

        # Progreso de ventanas
        if total_windows % 10 == 0:
            log(f"    Org {org_id} CM: ventana {total_windows}/{n_total_windows} | registros: {len(all_recs):,}")

        d_end = d_start - timedelta(days=1)

    if all_recs:
        log(f"    Org {org_id} CM TOTAL: {len(all_recs):,} registros únicos ({windows_with_data}/{total_windows} ventanas con datos)")
    # recordsTotal es el total del organismo (ignora las fechas): si las ventanas devuelven
    # menos, hay contratos menores que no se han descargado (fecha fuera de
    # [DATE_ORIGIN, hoy] o vacía, paginación inestable...). Antes no se comprobaba.
    if len(all_recs) < reported_total:
        log_warn(
            f"Org {org_id} CM: DESAJUSTE el portal declara {reported_total:,} y las ventanas "
            f"[{DATE_ORIGIN} → {date_end}] devuelven {len(all_recs):,} únicos"
        )
    if informe is not None:
        informe["CM"] = {
            "declarados": reported_total,
            "ventanas": complete_windows,
            "incompletas": incomplete_windows,
        }

    for r in all_recs:
        r["_organismo_id"] = org_id
        r["_tipo"] = "CM"
    return all_recs


# ─────────────────────────────────────────────────────────────────────────────
# BASE SCRAPE PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def iso_utc(momento=None):
    """Fecha ISO 8601 en UTC al segundo ('2026-09-28T06:20:15Z'), ordenable como texto."""
    momento = momento or datetime.now(timezone.utc)
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def file_date_iso(path):
    """Fecha de modificación de un fichero (iso_utc)."""
    return iso_utc(datetime.fromtimestamp(Path(path).stat().st_mtime, timezone.utc))


def read_base_manifest(output_dir):
    """Contenido del progreso/manifiesto de la descarga base ({} si no hay o no se lee)."""
    path = Path(output_dir) / BASE_PROGRESS_NAME
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def prepare_detail_outputs(output_dir, resume=False):
    """Carpeta de salida del detalle. La caché SQLite nunca se borra (antes, sin
    --resume se borraba entera y se perdía todo el detalle ya descargado)."""
    del resume
    Path(output_dir).mkdir(parents=True, exist_ok=True)


def start_base_download(output_dir):
    """Empieza una descarga base nueva (sin --resume) sin perder la anterior.

    Si la descarga anterior (con manifiesto) aún no está en la tabla final, no se
    empieza (ScraperError): hay que ejecutar antes 'merge' o seguirla con
    --resume. El CSV y el Parquet base anteriores pasan a _historico/ (archivar)
    y se anotan en el manifiesto para que publish_base, si la descarga nueva
    resulta idéntica, devuelva la anterior a su sitio. Devuelve el manifiesto.
    """
    output_dir = Path(output_dir)
    base_csv = output_dir / BASE_CSV_NAME
    previous = read_base_manifest(output_dir)
    if base_csv.exists():
        if previous.get("fecha_descarga") and not previous.get("acumulada"):
            raise ScraperError(
                f"La descarga base del {previous['fecha_descarga']} ({base_csv}) todavía no está en "
                f"{FINAL_CSV_NAME}: ejecuta antes 'merge' (o termina esa descarga con 'base --resume'). "
                "Así ninguna descarga se queda fuera de la tabla final."
            )
        if not previous.get("fecha_descarga"):
            log_warn(
                f"{base_csv.name} sin manifiesto (versión anterior del scraper): no se sabe si ya está en "
                f"{FINAL_CSV_NAME}; se guarda en {base_csv.parent / '_historico'}"
            )
    apartadas = {}
    for name in (BASE_CSV_NAME, BASE_PARQUET_NAME):
        path = output_dir / name
        if path.exists():
            apartadas[name] = str(archivar(path))
            log(f"Descarga base anterior: {name} → {apartadas[name]}")
    return {"fecha_descarga": iso_utc(), "ambito": {}, "acumulada": False, "apartadas": apartadas}


def resume_base_manifest(output_dir):
    """Manifiesto de la descarga que se reanuda (--resume). Un progreso de una
    versión anterior del scraper no tiene fecha ni ámbito: la fecha es la del CSV
    base y el ámbito de sus organismos se deduce al acumular (merge)."""
    output_dir = Path(output_dir)
    manifest = read_base_manifest(output_dir)
    if manifest.get("fecha_descarga"):
        manifest.setdefault("ambito", {})
        manifest.setdefault("apartadas", {})
        return manifest
    base_csv = output_dir / BASE_CSV_NAME
    return {
        "fecha_descarga": file_date_iso(base_csv) if base_csv.exists() else iso_utc(),
        "ambito": {},
        "acumulada": False,
        "apartadas": {},
    }


def scope_from_report(informe):
    """Ámbito de un organismo (manifiesto) a partir de lo que ha informado su
    paginación: LIC si está completa y CM con sus ventanas completas y no vacías
    (window_check), más el diagnóstico de las incompletas."""
    scope = {}
    lic = informe.get("LIC")
    if lic is not None:
        scope["LIC"] = bool(lic.get("completo"))
        scope["LIC_informe"] = {k: lic[k] for k in ("declarados", "filas", "unicos")}
    cm = informe.get("CM")
    if cm is not None:
        scope["CM"] = list(cm.get("ventanas") or [])
        if cm.get("incompletas"):
            scope["CM_incompletas"] = [
                {k: w.get(k) for k in ("desde", "hasta", "filtrados", "filas", "unicos", "fuera", "sin_id")}
                for w in cm["incompletas"]
            ]
    return scope


def close_version(path, apartada):
    """guardar_version para un fichero escrito en su sitio después de apartar
    (archivar) la versión anterior: si es idéntico a ella, la anterior vuelve a
    su sitio con su fecha y no queda nada nuevo en _historico/ ('sin_cambios');
    si cambió, la anterior se queda en _historico/ ('actualizado'); sin
    anterior, 'nuevo'."""
    path = Path(path)
    if not apartada or not Path(apartada).exists():
        return "nuevo"
    if path.exists() and filecmp.cmp(path, apartada, shallow=False):
        os.replace(apartada, path)
        return "sin_cambios"
    return "actualizado"


def publish_base(output_dir, manifest, label="[BASE FINAL] "):
    """Cierra la versión del CSV base (close_version) y genera su Parquet
    (csv_to_parquet, con guardar_version). Devuelve la ruta del Parquet o None."""
    output_dir = Path(output_dir)
    base_csv = output_dir / BASE_CSV_NAME
    apartadas = manifest.get("apartadas") or {}
    if not base_csv.exists():
        log_warn(f"{label}Sin {BASE_CSV_NAME}: la descarga no ha traído ningún contrato.")
        return None
    state = close_version(base_csv, apartadas.get(BASE_CSV_NAME))
    log(f"{label}{BASE_CSV_NAME}: {state}")
    parquet_path = finalize_base_parquet(output_dir, label=label)
    if parquet_path:
        state = close_version(parquet_path, apartadas.get(BASE_PARQUET_NAME))
        log(f"{label}{BASE_PARQUET_NAME}: {state}")
    return parquet_path


def mark_base_accumulated(output_dir, fecha):
    """Anota en el manifiesto que la descarga base ya está en la tabla final."""
    path = Path(output_dir) / BASE_PROGRESS_NAME
    manifest = read_base_manifest(output_dir)
    if not manifest.get("fecha_descarga"):
        return
    manifest["acumulada"] = True
    manifest["acumulada_en"] = iso_utc()
    manifest["fecha_acumulada"] = fecha
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(compact_json(manifest), encoding="utf-8")
    tmp_path.replace(path)


def run_base_scrape(
    session,
    output_dir,
    organismo=None,
    max_org_id=2000,
    discovery_workers=5,
    skip_cm=False,
    skip_lic=False,
    resume=False,
    autosave_every=AUTOSAVE_EVERY,
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if resume:
        completed_orgs, previous_stats = load_base_resume(output_dir)
        manifest = resume_base_manifest(output_dir)
    else:
        manifest = start_base_download(output_dir)
        completed_orgs, previous_stats = set(), {}
    manifest["opciones"] = {"organismo": organismo, "skip_cm": skip_cm, "skip_lic": skip_lic}
    log(f"Descarga base del {manifest['fecha_descarga']}")

    if organismo:
        org_list = [(organismo, None, None)]
        log(f"Modo organismo único: {organismo}")
    else:
        org_list = discover(session, max_org_id, discovery_workers)

    if not org_list:
        raise ScraperError("No se encontraron organismos.")

    pending_orgs = [item for item in org_list if item[0] not in completed_orgs]
    skipped_orgs = len(org_list) - len(pending_orgs)
    if skipped_orgs:
        log(f"Resume base: saltando {skipped_orgs} organismos ya completados.")

    stats = {
        "records_total": int(previous_stats.get("records_total", 0)),
        "cm_total": int(previous_stats.get("cm_total", 0)),
        "lic_total": int(previous_stats.get("lic_total", 0)),
        "completed_orgs_count": len(completed_orgs),
    }
    # Checkpoint inicial: fija el tamaño del CSV base antes del primer organismo
    # para poder recortar también un corte durante su escritura.
    save_base_progress(output_dir, completed_orgs, stats=stats, manifest=manifest)
    for idx, (org_id, est_cm, est_lic) in enumerate(pending_orgs, start=1):
        org_records = []
        report = {}
        log(f"{'═'*60}")
        log(f"BASE organismo {org_id} ({idx}/{len(pending_orgs)})")
        log(f"{'═'*60}")

        if not skip_lic and (est_lic is None or est_lic > 0):
            recs = paginate_lic(session, org_id, informe=report)
            org_records.extend(recs)
            stats["lic_total"] += len(recs)
            stats["records_total"] += len(recs)

        if not skip_cm and (est_cm is None or est_cm > 0):
            recs = paginate_cm_full(session, org_id, informe=report)
            org_records.extend(recs)
            stats["cm_total"] += len(recs)
            stats["records_total"] += len(recs)

        append_base_records(org_records, output_dir, label=f"[BASE ORG {org_id}] ")
        completed_orgs.add(org_id)
        # Lo que se ha vuelto a leer completo de este organismo (ámbito de la
        # descarga): con él merge decide qué contratos se dan por retirados.
        manifest["ambito"][str(org_id)] = scope_from_report(report)
        manifest["acumulada"] = False
        stats["completed_orgs_count"] = len(completed_orgs)
        save_base_progress(output_dir, completed_orgs, stats=stats, manifest=manifest)

        if idx % autosave_every == 0:
            log(f"BASE checkpoint {idx}/{len(pending_orgs)} organismos.")

        log(
            "BASE PROGRESO: "
            f"{stats['records_total']:,} total | CM:{stats['cm_total']:,} "
            f"LIC:{stats['lic_total']:,} | Org completados:{len(completed_orgs):,}"
        )
        print()

    save_base_progress(output_dir, completed_orgs, stats=stats, manifest=manifest)
    base_parquet_path = publish_base(output_dir, manifest, label="[BASE FINAL] ")
    return {
        "base_csv_path": str(Path(output_dir) / BASE_CSV_NAME),
        "base_parquet_path": str(base_parquet_path) if base_parquet_path else None,
        "org_list": org_list,
        "stats": stats,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLEANING & EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def clean_html(val):
    if not isinstance(val, str):
        return val
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", val)).strip()


def _datetime_in_ns_range(values):
    # pandas 3 devuelve fechas fuera del rango de datetime64[ns] (p. ej. erratas
    # como el año 0201) en vez de NaT, y asignarlas a la serie ns revienta.
    return values.where(values.between(pd.Timestamp.min, pd.Timestamp.max))


def parse_datetime_series(series):
    """Intenta respetar ISO primero y cae a day-first para formatos locales."""
    text = series.astype("string")
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    iso_mask = text.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)

    # Formato explícito: sin él pandas infiere el formato del primer valor y
    # convierte en NaT los que usan otra variante (p. ej. "2026-03-02" junto a
    # "2026-03-01T10:00:00").
    if iso_mask.any():
        iso_values = text[iso_mask].str.replace(
            r"(Z|[+-]\d{2}:?\d{2})$",
            "",
            regex=True,
        )
        parsed.loc[iso_mask] = _datetime_in_ns_range(
            pd.to_datetime(iso_values, errors="coerce", format="ISO8601")
        )

    non_iso_mask = ~iso_mask
    if non_iso_mask.any():
        parsed.loc[non_iso_mask] = _datetime_in_ns_range(
            pd.to_datetime(
                text[non_iso_mask],
                errors="coerce",
                dayfirst=True,
                format="mixed",
            )
        )
    return parsed


PLAIN_DECIMAL_RE = re.compile(r"-?\d+(?:\.\d{1,2})?")


def parse_importe_value(value):
    """
    Importe de la API de tabla → float.

    La API devuelve el importe como número JSON (674.78): se respeta tal cual.
    Antes se le quitaba el "." como si fuera separador de miles y quedaba
    inflado x10/x100 (674.78 → 67478). El texto se interpreta en formato
    español ("1.234,56 €") salvo que sea un decimal simple con punto ("674.78").
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, numbers.Number):
        return float(value)
    text = str(value).replace("€", "").strip()
    if not PLAIN_DECIMAL_RE.fullmatch(text):
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def to_dataframe(records):
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # "string" además de "object": en pandas 3 el texto usa el dtype `str`, que
    # "object" solo incluye por compatibilidad (deprecado, desaparece en pandas 4).
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(clean_html)

    for col in df.columns:
        if any(h in col.lower() for h in ("fecha", "publicado", "plazo", "apertura", "modificado")):
            df[col] = parse_datetime_series(df[col])

    for col in df.columns:
        if any(h in col.lower() for h in ("importe", "precio", "valor", "presupuesto")):
            df[col] = pd.to_numeric(
                df[col].map(parse_importe_value),
                errors="coerce",
            ).astype("float64")

    if "id" in df.columns and "_tipo" in df.columns:
        n = len(df)
        df = df.drop_duplicates(subset=["id", "_tipo"])
        d = n - len(df)
        if d:
            log(f"Eliminados {d:,} duplicados")

    return df


def save_csv(records, path, label=""):
    """Convierte y guarda CSV."""
    if not records:
        log_warn(f"{label}No hay datos para guardar.")
        return pd.DataFrame()

    df = to_dataframe(records)
    if df.empty:
        return df

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # guardar_version: si ya existía y cambia, la versión anterior va a _historico/
    tmp_path = path.with_name(f".{path.name}.nuevo")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig", sep=";")
    state = guardar_version(path, desde=tmp_path)
    mb = path.stat().st_size / 1024 / 1024

    n_cm = len(df[df["_tipo"] == "CM"]) if "_tipo" in df.columns else 0
    n_lic = len(df[df["_tipo"] == "LIC"]) if "_tipo" in df.columns else 0
    n_orgs = df["_organismo_id"].nunique() if "_organismo_id" in df.columns else 0

    log(f"{label}CSV: {path} ({state})")
    log(f"{label}  {len(df):,} filas ({mb:.1f} MB) | CM: {n_cm:,} | LIC: {n_lic:,} | Orgs: {n_orgs}")

    if "importe" in df.columns:
        total_eur = df["importe"].sum()
        if pd.notna(total_eur):
            log(f"{label}  Importe total: {total_eur:,.2f} €")

    log(f"{label}  Columnas ({len(df.columns)}): {list(df.columns)}")
    return df


def save_parquet(df, path, label=""):
    """Guarda Parquet cuando pyarrow está disponible."""
    if df.empty:
        return None
    if not HAS_PYARROW:
        log_warn(f"{label}Parquet no generado: falta pyarrow.")
        return None

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.nuevo")
    df.to_parquet(tmp_path, index=False, engine="pyarrow")
    state = guardar_version(path, desde=tmp_path)
    mb = path.stat().st_size / 1024 / 1024
    log(f"{label}Parquet: {path} ({state})")
    log(f"{label}  {len(df):,} filas ({mb:.1f} MB)")
    return path


# Fechas que se guardan como datetime en Parquet (el README usa
# df_gal['publicado'].dt.year); en el CSV van como texto ISO.
PARQUET_DATE_COLUMNS = (
    "publicado",
    "modificado",
    "detail_fecha_difusion",
    "detail_fecha_formalizacion",
)
# Códigos con pinta de número que deben seguir siendo texto (hay NIF/NIPC
# puramente numéricos de empresas portuguesas, CPs, teléfonos...). Las fechas
# de control (_primera_descarga, _ultima_descarga) y _origen son texto.
PARQUET_TEXT_COLUMNS = (
    "nif",
    "detail_referencia",
    "detail_cp",
    "detail_telefono",
    "detail_fax",
    "_primera_descarga",
    "_ultima_descarga",
    "_origen",
)
# Booleanas: en el CSV, 'True' / 'False'.
PARQUET_BOOL_COLUMNS = ("_en_ultima_descarga",)


def csv_to_parquet(csv_path, parquet_path, label="", chunksize=BASE_READ_CHUNKSIZE):
    """
    CSV (;) → Parquet por chunks con ParquetWriter, sin cargar el CSV entero en
    memoria (el merge final son ~1.7M filas x 62 columnas).

    1ª pasada: tipo estable por columna mirando TODO el CSV (numérica solo si
    todos sus valores lo son, como haría un read_csv completo). 2ª pasada:
    convierte cada chunk a ese esquema y lo escribe. Se publica con
    guardar_version: si el Parquet ya existía y cambia, el anterior va a
    _historico/; si es idéntico no se toca.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)

    def read_chunks():
        return pd.read_csv(
            csv_path,
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
            chunksize=chunksize,
        )

    columns = None
    numeric = {}
    integer = {}
    total_rows = 0
    for chunk in read_chunks():
        if columns is None:
            columns = list(chunk.columns)
            for column in columns:
                if column not in PARQUET_DATE_COLUMNS + PARQUET_TEXT_COLUMNS + PARQUET_BOOL_COLUMNS:
                    numeric[column] = integer[column] = True
        total_rows += len(chunk)
        for column in numeric:
            if not numeric[column]:
                continue
            values = chunk[column]
            parsed = pd.to_numeric(values, errors="coerce")
            if (parsed.isna() & values.notna()).any():
                numeric[column] = integer[column] = False
            elif parsed.dtype.kind != "i":
                # Decimales o vacíos: float64, como en un read_csv completo.
                integer[column] = False
    if not total_rows:
        return None

    fields = []
    for column in columns:
        if column in PARQUET_DATE_COLUMNS:
            fields.append(pa.field(column, pa.timestamp("ns")))
        elif column in PARQUET_BOOL_COLUMNS:
            fields.append(pa.field(column, pa.bool_()))
        elif integer.get(column):
            fields.append(pa.field(column, pa.int64()))
        elif numeric.get(column):
            fields.append(pa.field(column, pa.float64()))
        else:
            fields.append(pa.field(column, pa.string()))
    schema = pa.schema(fields)

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = parquet_path.with_name(parquet_path.name + ".tmp")
    try:
        with pq.ParquetWriter(tmp_path, schema) as writer:
            for chunk in read_chunks():
                for column in columns:
                    if column in PARQUET_DATE_COLUMNS:
                        chunk[column] = parse_datetime_series(chunk[column])
                    elif column in PARQUET_BOOL_COLUMNS:
                        chunk[column] = chunk[column].map({"True": True, "False": False}).astype(object)
                    elif numeric.get(column):
                        chunk[column] = pd.to_numeric(chunk[column], errors="coerce")
                writer.write_table(pa.Table.from_pandas(chunk, schema=schema, preserve_index=False))
        state = guardar_version(parquet_path, desde=tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    mb = parquet_path.stat().st_size / 1024 / 1024
    log(f"{label}Parquet: {parquet_path} ({state})")
    log(f"{label}  {total_rows:,} filas ({mb:.1f} MB)")
    return parquet_path


def save_dataset(records, output_dir, csv_name, parquet_name, label=""):
    """Guarda un dataset CSV + Parquet con nombres configurables."""
    output_dir = Path(output_dir)
    csv_path = output_dir / csv_name
    parquet_path = output_dir / parquet_name
    df = save_csv(records, csv_path, label=label)
    saved_parquet = None
    if not df.empty:
        saved_parquet = save_parquet(df, parquet_path, label=label)
    return df, csv_path, saved_parquet


def save_outputs(records, output_dir, label=""):
    return save_dataset(records, output_dir, FINAL_CSV_NAME, FINAL_PARQUET_NAME, label=label)


def append_base_records(records, output_dir, label=""):
    if not records:
        return 0

    output_dir = Path(output_dir)
    base_csv_path = output_dir / BASE_CSV_NAME
    df = to_dataframe(records)
    for column in BASE_EXPORT_FIELDS:
        if column not in df.columns:
            df[column] = pd.NA
    # El esquema fijo del CSV base descarta cualquier campo nuevo de la API: que se vea
    dropped = [column for column in df.columns if column not in BASE_EXPORT_FIELDS]
    if dropped:
        log_warn(f"{label}campos de la API sin columna en el CSV base (se descartan): {dropped}")
    df = df.reindex(columns=BASE_EXPORT_FIELDS)

    output_dir.mkdir(parents=True, exist_ok=True)
    write_header = not base_csv_path.exists()
    df.to_csv(
        base_csv_path,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
        sep=";",
    )
    log(f"{label}BASE CSV += {len(df):,} filas → {base_csv_path}")
    return len(df)


def finalize_base_parquet(output_dir, label=""):
    output_dir = Path(output_dir)
    base_csv_path = output_dir / BASE_CSV_NAME
    base_parquet_path = output_dir / BASE_PARQUET_NAME
    if not base_csv_path.exists():
        return None
    if not HAS_PYARROW:
        log_warn(f"{label}Base Parquet no generado: falta pyarrow.")
        return None

    return csv_to_parquet(base_csv_path, base_parquet_path, label=label)


def save_base_progress(output_dir, completed_orgs, stats=None, manifest=None):
    """Checkpoint de la descarga base. manifest (run_base_scrape): fecha de la
    descarga, ámbito por organismo, si ya está en la tabla final y las versiones
    anteriores apartadas en _historico/ (ver start_base_download)."""
    progress_path = Path(output_dir) / BASE_PROGRESS_NAME
    csv_path = Path(output_dir) / BASE_CSV_NAME
    progress = dict(manifest or {})
    progress.update({
        "saved_at": datetime.now().isoformat(),
        "completed_orgs": sorted(completed_orgs),
        # Tamaño del CSV base que solo contiene organismos completos: al reanudar
        # se recorta lo escrito después (organismo cortado o sin checkpoint).
        "base_csv_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
    })
    if stats:
        progress["stats"] = stats
    # Escritura atómica: un JSON a medio escribir dejaba --resume sin organismos
    # completados y se volvían a añadir todos al CSV (filas duplicadas).
    tmp_path = progress_path.with_name(progress_path.name + ".tmp")
    tmp_path.write_text(compact_json(progress), encoding="utf-8")
    tmp_path.replace(progress_path)
    return progress_path


def load_base_resume(output_dir):
    output_dir = Path(output_dir)
    progress_path = output_dir / BASE_PROGRESS_NAME
    csv_path = output_dir / BASE_CSV_NAME
    completed_orgs = set()
    stats = {}
    payload = None

    if progress_path.exists():
        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
            completed_orgs = set(payload.get("completed_orgs", []))
            stats = payload.get("stats", {})
        except Exception as exc:
            # Se infiere desde el CSV (abajo) en vez de reanudar sin organismos
            # completados, que duplicaría todo el CSV base.
            payload = None
            completed_orgs = set()
            stats = {}
            log_warn(f"No se pudo leer el progreso base: {exc}")

    if payload is not None:
        csv_bytes = payload.get("base_csv_bytes")
        if csv_bytes is not None and csv_path.exists() and csv_path.stat().st_size > csv_bytes:
            # Filas escritas tras el último checkpoint: organismo cortado a mitad
            # (o sin marcar como completado). Se descartan y se vuelve a
            # descargar, en vez de dejar filas duplicadas o una línea rota.
            log_warn(
                "Resume base: descartando "
                f"{csv_path.stat().st_size - csv_bytes:,} bytes del CSV base "
                "posteriores al último checkpoint (organismo incompleto)."
            )
            if csv_bytes:
                with csv_path.open("r+b") as fh:
                    fh.truncate(csv_bytes)
            else:
                csv_path.unlink()
    elif csv_path.exists():
        try:
            df = pd.read_csv(
                csv_path,
                sep=";",
                encoding="utf-8-sig",
                usecols=["_organismo_id"],
                low_memory=False,
            )
            completed_orgs = set(int(value) for value in df["_organismo_id"].dropna().unique())
        except Exception as exc:
            log_warn(f"No se pudo inferir el progreso base desde CSV: {exc}")

    return completed_orgs, stats


def normalize_record_id(value):
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def detail_db_path(output_dir):
    return Path(output_dir) / DETAIL_DB_NAME


def init_detail_db(output_dir):
    db_path = detail_db_path(output_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detail_cache (
            record_type TEXT NOT NULL,
            record_id TEXT NOT NULL,
            organismo_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_http_status INTEGER,
            updated_at TEXT NOT NULL,
            detail_url TEXT,
            page_title TEXT,
            html_sha256 TEXT,
            mapped_json TEXT,
            raw_gzip BLOB,
            PRIMARY KEY (record_type, record_id, organismo_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_detail_status
        ON detail_cache (status, organismo_id, record_type)
        """
    )
    existing_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(detail_cache)")
    }
    if "raw_gzip" not in existing_columns:
        conn.execute("ALTER TABLE detail_cache ADD COLUMN raw_gzip BLOB")
    # Fichas 'done' que el portal ha cambiado después: la versión anterior se
    # guarda aquí antes de sustituirla (persist_detail_results); nunca se borra.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detail_cache_historico (
            record_type TEXT NOT NULL,
            record_id TEXT NOT NULL,
            organismo_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_http_status INTEGER,
            updated_at TEXT NOT NULL,
            detail_url TEXT,
            page_title TEXT,
            html_sha256 TEXT,
            mapped_json TEXT,
            raw_gzip BLOB,
            archived_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_detail_historico_clave
        ON detail_cache_historico (record_type, record_id, organismo_id)
        """
    )
    conn.commit()
    return conn


def query_detail_rows(conn, records):
    if not records:
        return {}
    rows = {}

    for start in range(0, len(records), DETAIL_QUERY_BATCH_SIZE):
        batch = records[start : start + DETAIL_QUERY_BATCH_SIZE]
        clauses = []
        params = []
        for record in batch:
            clauses.append("(record_type=? AND record_id=? AND organismo_id=?)")
            params.extend(
                [
                    record["_tipo"],
                    normalize_record_id(record["id"]),
                    int(record["_organismo_id"]),
                ]
            )
        sql = (
            "SELECT record_type, record_id, organismo_id, status, attempts, "
            "last_error, last_http_status, updated_at, detail_url, page_title, "
            "html_sha256, mapped_json, raw_gzip "
            f"FROM detail_cache WHERE {' OR '.join(clauses)}"
        )
        for row in conn.execute(sql, params):
            rows[(row["record_type"], row["record_id"], row["organismo_id"])] = dict(row)
    return rows


def iter_base_chunks(base_csv_path, chunksize=BASE_READ_CHUNKSIZE, as_text=False):
    base_csv_path = Path(base_csv_path)
    # as_text: valores tal cual están en el CSV ("" si vacío), sin inferir tipos
    # por chunk (evita "nan", "3.0" o NIFs numéricos convertidos a float).
    text_options = {"dtype": str, "keep_default_na": False} if as_text else {}
    yield from pd.read_csv(
        base_csv_path,
        sep=";",
        encoding="utf-8-sig",
        low_memory=False,
        chunksize=chunksize,
        **text_options,
    )


def iter_detail_batches(
    base_csv_path,
    conn,
    only_type="all",
    batch_size=DETAIL_BATCH_SIZE,
    force=False,
    only_org_id=None,
    retryable_only=False,
    retryable_ignore_max_attempts=False,
    ignore_max_attempts=False,
):
    """Lotes (por organismo) de registros del CSV base cuyo detalle hay que pedir:
    los que no están en la caché, los que no están 'done' (hasta
    DETAIL_MAX_ATTEMPTS intentos, salvo ignore_max_attempts: 'detail' sin
    --resume) y, con force, también los 'done'."""
    batch = []
    current_org = None

    def flush():
        nonlocal batch
        if not batch:
            return None
        existing = query_detail_rows(conn, batch)
        todo = []
        for record in batch:
            key = (record["_tipo"], normalize_record_id(record["id"]), int(record["_organismo_id"]))
            cached = existing.get(key)
            if retryable_only:
                if cached is None or cached["status"] != "retryable":
                    continue
                if cached["attempts"] >= DETAIL_MAX_ATTEMPTS and not retryable_ignore_max_attempts:
                    continue
                record["_detail_attempts"] = cached["attempts"]
                todo.append(record)
                continue
            if force or cached is None:
                record["_detail_attempts"] = 0
                todo.append(record)
                continue
            if cached["status"] == "done":
                continue
            if cached["attempts"] >= DETAIL_MAX_ATTEMPTS and not ignore_max_attempts:
                continue
            record["_detail_attempts"] = cached["attempts"]
            todo.append(record)
        batch = []
        return todo or None

    for chunk in iter_base_chunks(base_csv_path):
        for record in chunk.to_dict("records"):
            if only_type != "all" and record["_tipo"] != only_type:
                continue
            org_id = int(record["_organismo_id"])
            if only_org_id is not None and org_id != int(only_org_id):
                continue
            if current_org is None:
                current_org = org_id
            if org_id != current_org or len(batch) >= batch_size:
                todo = flush()
                current_org = org_id
                if todo:
                    yield todo
            batch.append(
                {
                    "id": normalize_record_id(record["id"]),
                    "_tipo": record["_tipo"],
                    "_organismo_id": org_id,
                }
            )
    todo = flush()
    if todo:
        yield todo


def classify_detail_error(message):
    status = None
    match = re.search(r"HTTP (\d+)", message)
    if match:
        status = int(match.group(1))
    # Sin código HTTP = error de red (timeout, conexión cortada...): es
    # transitorio, así que retryable para que --retryable-only lo rescate.
    retryable = status is None or status in (403, 429) or status >= 500
    if "Página de detalle inválida" in message:
        retryable = True
    return retryable, status


def fetch_detail_batch(
    batch,
    detail_delay=DETAIL_DELAY,
    detail_jitter=DETAIL_JITTER,
    store_raw=True,
    stop_event=None,
):
    session = get_detail_session()
    results = []
    if not batch:
        return results
    session.ensure_org_context(batch[0]["_organismo_id"])

    for index, record in enumerate(batch):
        if index > 0:
            time.sleep(max(0.0, detail_delay + random.uniform(0, detail_jitter)))
        if stop_event is not None and stop_event.is_set():
            # Parada ordenada (baneo, Ctrl+C...): no lanzar más peticiones y
            # devolver lo ya descargado para que se guarde.
            break

        record_type = record["_tipo"]
        record_id = normalize_record_id(record["id"])
        organismo_id = int(record["_organismo_id"])
        payload = build_detail_payload(record_type, record_id, organismo_id)
        detail_url = f"{BASE_URL}/licitacion?N={payload['N']}"
        try:
            session.ensure_org_context(organismo_id)
            html = session.post_html(
                f"{BASE_URL}/licitacion",
                payload,
                count_error=True,
            )
            parsed = parse_detail_html(html)
            mapped = parsed["mapped"]
            mapped["detail_url"] = detail_url
            raw_payload = None
            if store_raw:
                raw_payload = compact_json(
                    {
                        "pairs": parsed["pairs"],
                        "tables": parsed["tables"],
                    }
                )
            results.append(
                {
                    "record_type": record_type,
                    "record_id": record_id,
                    "organismo_id": organismo_id,
                    "status": "done",
                    "attempts": int(record.get("_detail_attempts", 0)) + 1,
                    "last_error": None,
                    "last_http_status": None,
                    "updated_at": datetime.now().isoformat(),
                    "detail_url": detail_url,
                    "page_title": parsed["page_title"],
                    "html_sha256": hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest(),
                    "mapped_json": compact_json(mapped),
                    "raw_gzip": compress_text(raw_payload),
                }
            )
        except ScraperError as exc:
            retryable, http_status = classify_detail_error(str(exc))
            results.append(
                {
                    "record_type": record_type,
                    "record_id": record_id,
                    "organismo_id": organismo_id,
                    "status": "retryable" if retryable else "failed",
                    "attempts": int(record.get("_detail_attempts", 0)) + 1,
                    "last_error": str(exc),
                    "last_http_status": http_status,
                    "updated_at": datetime.now().isoformat(),
                    "detail_url": detail_url,
                    "page_title": None,
                    "html_sha256": None,
                    "mapped_json": None,
                    "raw_gzip": None,
                }
            )
    return results


def _detail_has_content(row):
    """Si una ficha 'done' de la caché trae algún par o tabla (no está vacía)."""
    try:
        mapped = json.loads(row.get("mapped_json") or "{}")
    except ValueError:
        return False
    return bool(mapped.get("detail_pairs_count") or mapped.get("detail_tables_count"))


def _raw_bytes(value):
    return None if value is None else bytes(value)


def persist_detail_results(conn, results):
    """Guarda en la caché los resultados de fetch_detail_batch sin perder nunca
    una ficha ya descargada:

    - una ficha 'done' no se sustituye por un error (retryable/failed) ni por
      una ficha vacía (sin pares ni tablas): se conserva la anterior;
    - si el portal la ha cambiado (otro mapped_json o crudo), la anterior pasa
      a detail_cache_historico antes de guardar la nueva.

    Devuelve {'conservadas': n, 'archivadas': n}.
    """
    counts = {"conservadas": 0, "archivadas": 0}
    if not results:
        return counts
    existing = query_detail_rows(
        conn,
        [
            {"_tipo": r["record_type"], "id": r["record_id"], "_organismo_id": r["organismo_id"]}
            for r in results
        ],
    )
    to_write = []
    to_archive = []
    archived_at = iso_utc()
    for result in results:
        key = (result["record_type"], normalize_record_id(result["record_id"]), int(result["organismo_id"]))
        old = existing.get(key)
        if old is not None and old["status"] == "done":
            if result["status"] != "done" or (not _detail_has_content(result) and _detail_has_content(old)):
                counts["conservadas"] += 1
                continue
            if result.get("raw_gzip") is None and old.get("raw_gzip") is not None \
                    and result.get("mapped_json") == old.get("mapped_json"):
                # Misma ficha pedida con --no-raw-detail: se conserva el crudo.
                result = dict(result, raw_gzip=old["raw_gzip"])
            if (result.get("mapped_json") != old.get("mapped_json")
                    or _raw_bytes(result.get("raw_gzip")) != _raw_bytes(old.get("raw_gzip"))):
                to_archive.append(dict(old, archived_at=archived_at))
        to_write.append(result)
    if to_archive:
        conn.executemany(
            """
            INSERT INTO detail_cache_historico (
                record_type, record_id, organismo_id, status, attempts,
                last_error, last_http_status, updated_at, detail_url,
                page_title, html_sha256, mapped_json, raw_gzip, archived_at
            ) VALUES (
                :record_type, :record_id, :organismo_id, :status, :attempts,
                :last_error, :last_http_status, :updated_at, :detail_url,
                :page_title, :html_sha256, :mapped_json, :raw_gzip, :archived_at
            )
            """,
            to_archive,
        )
        counts["archivadas"] = len(to_archive)
    if counts["conservadas"]:
        log_warn(
            f"DETALLE: {counts['conservadas']:,} fichas ya descargadas se conservan "
            "(el portal ha dado un error o una ficha vacía)"
        )
    results = to_write
    if not results:
        conn.commit()
        return counts
    conn.executemany(
        """
        INSERT INTO detail_cache (
            record_type, record_id, organismo_id, status, attempts,
            last_error, last_http_status, updated_at, detail_url,
            page_title, html_sha256, mapped_json, raw_gzip
        ) VALUES (
            :record_type, :record_id, :organismo_id, :status, :attempts,
            :last_error, :last_http_status, :updated_at, :detail_url,
            :page_title, :html_sha256, :mapped_json, :raw_gzip
        )
        ON CONFLICT(record_type, record_id, organismo_id) DO UPDATE SET
            status=excluded.status,
            attempts=excluded.attempts,
            last_error=excluded.last_error,
            last_http_status=excluded.last_http_status,
            updated_at=excluded.updated_at,
            detail_url=excluded.detail_url,
            page_title=excluded.page_title,
            html_sha256=excluded.html_sha256,
            mapped_json=excluded.mapped_json,
            raw_gzip=excluded.raw_gzip
        """,
        results,
    )
    conn.commit()
    return counts


def run_detail_enrichment(
    base_csv_path,
    output_dir,
    workers=DETAIL_WORKERS,
    batch_size=DETAIL_BATCH_SIZE,
    only_type="all",
    force=False,
    store_raw=True,
    detail_delay=DETAIL_DELAY,
    detail_jitter=DETAIL_JITTER,
    only_org_id=None,
    retryable_only=False,
    retryable_ignore_max_attempts=False,
    ignore_max_attempts=False,
):
    conn = init_detail_db(output_dir)
    pending_batches = iter_detail_batches(
        base_csv_path,
        conn,
        only_type=only_type,
        batch_size=batch_size,
        force=force,
        only_org_id=only_org_id,
        retryable_only=retryable_only,
        retryable_ignore_max_attempts=retryable_ignore_max_attempts,
        ignore_max_attempts=ignore_max_attempts,
    )

    submitted = {}
    processed = done = retryable = failed = 0
    recent_ban_errors = 0
    stop_event = threading.Event()

    def submit_next(pool):
        try:
            batch = next(pending_batches)
        except StopIteration:
            return False
        future = pool.submit(
            fetch_detail_batch,
            batch,
            detail_delay=detail_delay,
            detail_jitter=detail_jitter,
            store_raw=store_raw,
            stop_event=stop_event,
        )
        submitted[future] = len(batch)
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            for _ in range(workers * 2):
                if not submit_next(pool):
                    break

            while submitted:
                future = next(as_completed(submitted))
                submitted.pop(future)
                batch_results = future.result()
                persist_detail_results(conn, batch_results)

                processed += len(batch_results)
                done += sum(1 for item in batch_results if item["status"] == "done")
                retryable += sum(1 for item in batch_results if item["status"] == "retryable")
                failed += sum(1 for item in batch_results if item["status"] == "failed")

                batch_ban_errors = sum(
                    1
                    for item in batch_results
                    if item["status"] == "retryable" and item["last_http_status"] in (403, 429)
                )
                recent_ban_errors = recent_ban_errors + batch_ban_errors if batch_ban_errors else 0
                if recent_ban_errors >= BAN_ERROR_THRESHOLD:
                    raise ScraperError(
                        "Posible baneo temporal en detalle HTML "
                        f"({recent_ban_errors} respuestas 403/429 seguidas). Reintenta más tarde con --resume."
                    )

                if processed % 1000 == 0 or batch_results:
                    log(
                        "DETALLE: "
                        f"{processed:,} procesados | done:{done:,} retryable:{retryable:,} failed:{failed:,}"
                    )

                while len(submitted) < workers * 2:
                    if not submit_next(pool):
                        break
        except BaseException:
            # Baneo, Ctrl+C o error: sin esto el pool seguía ejecutando todos los
            # lotes encolados contra el portal (hasta workers*2 lotes) y luego
            # descartaba sus resultados. Se cancelan los encolados, los que están
            # en curso paran tras su petición actual y lo descargado se guarda.
            stop_event.set()
            pool.shutdown(wait=True, cancel_futures=True)
            for pending in submitted:
                if not pending.cancelled() and pending.exception() is None:
                    persist_detail_results(conn, pending.result())
            raise

    log(
        "DETALLE COMPLETO: "
        f"{processed:,} procesados | done:{done:,} retryable:{retryable:,} failed:{failed:,}"
    )
    return {
        "processed": processed,
        "done": done,
        "retryable": retryable,
        "failed": failed,
        "db_path": str(detail_db_path(output_dir)),
    }


def load_detail_map(conn, records):
    rows = query_detail_rows(conn, records)
    mapped = {}
    for key, row in rows.items():
        payload = {
            "detail_status": row.get("status"),
            "detail_attempts": row.get("attempts"),
            "detail_last_http_status": row.get("last_http_status"),
            "detail_last_error": row.get("last_error"),
            "detail_updated_at": row.get("updated_at"),
            "detail_html_sha256": row.get("html_sha256"),
        }
        if row.get("mapped_json"):
            payload.update(json.loads(row["mapped_json"]))
        if "detail_campos_extra_json" not in payload and row.get("raw_gzip"):
            # Fichas descargadas antes de exportar estos campos: salen del crudo cacheado
            # sin volver a pedirlas al portal
            raw = json.loads(decompress_text(row["raw_gzip"]) or "{}")
            payload.update(detail_unmapped_fields(raw.get("pairs", []), raw.get("tables", [])))
        payload["detail_url"] = row.get("detail_url")
        payload["detail_page_title"] = row.get("page_title")
        mapped[key] = payload
    return mapped


# ─────────────────────────────────────────────────────────────────────────────
# TABLA FINAL ACUMULADA (sesgo del superviviente) Y SEMILLA
# ─────────────────────────────────────────────────────────────────────────────

def read_text_csv(path, usecols=None, chunksize=None):
    """CSV del scraper (;) como texto tal cual ('' si vacío), sin inferir tipos."""
    return pd.read_csv(
        path,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        usecols=usecols,
        chunksize=chunksize,
    )


def _is_blank(value):
    if value is None:
        return True
    if isinstance(value, str):
        return value == ""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _as_text(series):
    """Serie de texto (object) con '' en los nulos."""
    values = series.astype(object)
    if values.map(lambda v: isinstance(v, str)).all():
        return values
    return pd.Series(["" if _is_blank(v) else str(v) for v in values], index=series.index, dtype=object)


def _org_keys(series):
    """Id de organismo como texto comparable ('48', también si viene como 48.0)."""
    return np.array([normalize_record_id(v) if not _is_blank(v) else "" for v in series.astype(object)], dtype=object)


def listing_fingerprint(df):
    """Huella (uint64) del contenido de cada fila en las 12 columnas del listado
    (BASE_EXPORT_FIELDS) con los valores como quedan en el Parquet: los números
    (LISTING_NUMERIC_COLUMNS) como número y las fechas (LISTING_DATE_COLUMNS) como
    instante, así que '4' y '4.0' o '2024-01-15' y '2024-01-15 00:00:00' son el
    mismo valor; un texto que no es número o fecha cuenta tal cual. Dos filas con
    la misma huella son el mismo registro para acumular() (que también compara
    por un hash de 64 bits de las filas). Las columnas de la ficha, de control y
    de la semilla no cuentan."""
    canon = {}
    for column in BASE_EXPORT_FIELDS:
        if column in df.columns:
            text = _as_text(df[column])
        else:
            text = pd.Series("", index=df.index, dtype=object)
        if column in LISTING_NUMERIC_COLUMNS:
            value = pd.to_numeric(text.where(text != ""), errors="coerce").astype("float64")
            canon[column] = value
            canon[f"{column}|texto"] = text.where(value.isna(), "")
        elif column in LISTING_DATE_COLUMNS:
            value = parse_datetime_series(text.where(text != ""))
            canon[column] = value.to_numpy(dtype="datetime64[ns]").view("int64")
            canon[f"{column}|texto"] = text.where(value.isna(), "")
        else:
            canon[column] = text
    return pd.util.hash_pandas_object(pd.DataFrame(canon, index=df.index), index=False).to_numpy()


def effective_scope(manifest, new):
    """Ámbito de la descarga por organismo (clave: su id como texto): el del
    manifiesto (scope_from_report) y, para los organismos del CSV base que no lo
    tienen (descargados por una versión anterior del scraper o CSV base sin
    manifiesto), los tipos que trae el CSV, enteros ('LIC' y CM 'todo'): esa
    versión abortaba ante cualquier error, así que lo que está es completo salvo
    por ventanas cortadas que no sabía detectar."""
    recorded = {str(key): value for key, value in (manifest.get("ambito") or {}).items()}
    derived = {}
    if len(new):
        pairs = pd.DataFrame({"org": _org_keys(new["_organismo_id"]), "tipo": _as_text(new["_tipo"]).to_numpy()})
        for org, tipo in pairs.drop_duplicates().itertuples(index=False):
            if not org or org in recorded:
                continue
            info = derived.setdefault(org, {})
            if tipo == "LIC":
                info["LIC"] = True
            elif tipo == "CM":
                info["CM"] = "todo"
    if derived:
        log_warn(
            f"{len(derived)} organismos del CSV base sin ámbito en el manifiesto (descarga de una versión "
            "anterior del scraper): se toma como leído todo lo que trae de cada tipo"
        )
    scope = dict(recorded)
    scope.update(derived)
    return scope


def rows_in_scope(df, ambito):
    """True en las filas (columnas del CSV: _tipo, _organismo_id, publicado) que la
    descarga ha vuelto a leer completas (effective_scope): LIC de un organismo
    con 'LIC' y CM de un organismo con 'publicado' dentro de una de sus ventanas
    completas (cualquier CM si es 'todo'). Fuera de eso no se sabe si el portal
    los sigue listando."""
    inside = np.zeros(len(df), dtype=bool)
    if not ambito or not len(df):
        return inside
    orgs = _org_keys(df["_organismo_id"])
    kinds_all = _as_text(df["_tipo"]).to_numpy()
    groups = pd.Series(np.arange(len(df))).groupby(orgs).indices
    for org, positions in groups.items():
        info = ambito.get(org)
        if not info:
            continue
        kinds = kinds_all[positions]
        if info.get("LIC"):
            inside[positions[kinds == "LIC"]] = True
        windows = info.get("CM")
        cm = positions[kinds == "CM"]
        if not windows or not len(cm):
            continue
        if windows == "todo":
            inside[cm] = True
            continue
        dates = parse_datetime_series(
            _as_text(df["publicado"].iloc[cm]).reset_index(drop=True).replace("", np.nan)
        ).dt.normalize().to_numpy(dtype="datetime64[ns]")
        starts = np.array([np.datetime64(window[0], "ns") for window in windows])
        ends = np.array([np.datetime64(window[1], "ns") for window in windows])
        order = np.argsort(starts)
        starts, ends = starts[order], ends[order]
        found = np.searchsorted(starts, dates, side="right") - 1
        ok = ~np.isnat(dates) & (found >= 0)
        ok[ok] = dates[ok] <= ends[found[ok]]
        inside[cm[ok]] = True
    return inside


def read_previous_final(output_dir):
    """Registros de la tabla final anterior, de su CSV (el texto tal cual), sin las
    columnas de la ficha (se vuelven a sacar al escribir). None si no hay. Una
    tabla de una versión anterior del scraper (sin columnas de control) cuenta
    como una descarga con la fecha del fichero. Sin el CSV no se puede acumular
    sin perder texto: con solo el Parquet final se para (ScraperError)."""
    output_dir = Path(output_dir)
    final_csv = output_dir / FINAL_CSV_NAME
    final_parquet = output_dir / FINAL_PARQUET_NAME
    if not final_csv.exists():
        if final_parquet.exists():
            raise ScraperError(
                f"Existe {final_parquet} sin {FINAL_CSV_NAME}: la tabla final se acumula desde su CSV, así "
                "que no se toca. Recupera el CSV (p. ej. de _historico/) o usa ese Parquet como --semilla "
                "desde otra carpeta de salida."
            )
        return None
    previous = read_text_csv(final_csv, usecols=lambda column: column not in DETAIL_EXPORT_FIELDS)
    if not len(previous):
        return None
    missing = [column for column in COLUMNAS_META if column not in previous.columns]
    if missing and len(missing) < len(COLUMNAS_META):
        raise ScraperError(f"{final_csv} tiene solo algunas columnas de control (faltan {missing}): no se toca")
    if missing:
        fecha = file_date_iso(final_csv)
        log_warn(f"{final_csv.name} sin columnas de control (versión anterior del scraper): sus filas cuentan "
                 f"como descargadas el {fecha}")
        previous["_primera_descarga"] = fecha
        previous["_ultima_descarga"] = fecha
        previous["_en_ultima_descarga"] = True
    else:
        flags = previous["_en_ultima_descarga"].map({"True": True, "False": False})
        if flags.isna().any():
            raise ScraperError(f"{final_csv}: valores inesperados en _en_ultima_descarga; no se toca")
        previous["_en_ultima_descarga"] = flags.astype(bool)
    return previous


def accumulate_listing(previous, new, fecha, ambito):
    """acumular() del CSV base (`new`) con la tabla final anterior (`previous`):
    las filas se comparan por listing_fingerprint (una columna _huella; el resto
    va en `ignorar`) y solo se dan por retiradas las del ámbito de la descarga
    (rows_in_scope; columna _ambito: 'dentro' en todo `new` y en las filas de
    `previous` del ámbito, 'fuera' en las demás). Devuelve (tabla, resumen)."""
    new = new.reset_index(drop=True)
    new["_huella"] = listing_fingerprint(new)
    new["_ambito"] = "dentro"
    summary = {"descargadas": len(new), "anteriores": 0, "altas": len(new), "retiradas": 0, "fuera_ambito": 0}
    if previous is None:
        out = acumular(None, new, fecha)
    else:
        previous = previous.reset_index(drop=True)
        inside = rows_in_scope(previous, ambito)
        previous["_huella"] = listing_fingerprint(previous)
        previous["_ambito"] = np.where(inside, "dentro", "fuera")
        current_before = previous["_en_ultima_descarga"].to_numpy(dtype=bool).copy()
        columns = dict.fromkeys(list(previous.columns) + list(new.columns))
        ignorar = tuple(IGNORAR_POR_DEFECTO) + tuple(
            column for column in columns if column != "_huella" and column not in COLUMNAS_META
        )
        out = acumular(previous, new, fecha, ambito=["_ambito"], ignorar=ignorar)
        n_previous = len(previous)
        current_after = out["_en_ultima_descarga"].to_numpy(dtype=bool)[:n_previous]
        seen = out["_ultima_descarga"].astype(object).to_numpy()[:n_previous] == fecha
        summary.update(
            anteriores=n_previous,
            altas=len(out) - n_previous,
            retiradas=int((current_before & ~current_after).sum()),
            # Vigentes que esta descarga no ha vuelto a ver y no se dan por
            # retiradas porque no son de su ámbito.
            fuera_ambito=int((current_before & ~inside & ~seen).sum()),
        )
    return out.drop(columns=["_huella", "_ambito"]), summary


def _csv_text(series):
    """Valores de una columna (tipada, p. ej. de un Parquet publicado) como los
    escribe el CSV base: enteros sin '.0', decimales con repr, fechas AAAA-MM-DD
    (con la hora si no es medianoche) y '' en los nulos."""
    if pd.api.types.is_datetime64_any_dtype(series):
        values = series
        if getattr(values.dt, "tz", None) is not None:
            values = values.dt.tz_convert("UTC").dt.tz_localize(None)
        long_format = values.dt.strftime("%Y-%m-%d %H:%M:%S")
        short_format = values.dt.strftime("%Y-%m-%d")
        text = long_format.where(values.dt.normalize() != values, short_format)
        return text.astype(object).where(values.notna(), "")
    if pd.api.types.is_bool_dtype(series):
        return pd.Series(["" if _is_blank(v) else str(bool(v)) for v in series.astype(object)],
                         index=series.index, dtype=object)
    if pd.api.types.is_integer_dtype(series):
        return pd.Series(["" if _is_blank(v) else str(int(v)) for v in series.astype(object)],
                         index=series.index, dtype=object)
    if pd.api.types.is_float_dtype(series):
        return pd.Series(["" if _is_blank(v) else repr(float(v)) for v in series.astype(object)],
                         index=series.index, dtype=object)
    return _as_text(series)


def seed_as_text(df, published):
    """Filas de una semilla como texto del CSV (_csv_text). published=True (un
    publicado del scraper antiguo, sin columnas de control) corrige sus errores
    conocidos (ver el docstring del módulo): 'importe' queda vacío y su valor
    inflado va a importe_semilla; 'estado' = 'nan' queda vacío."""
    out = pd.DataFrame(
        {column: _csv_text(df[column]) for column in df.columns if column != "_en_ultima_descarga"},
        index=df.index,
    )
    if published:
        if "importe" in out.columns:
            out[SEED_AMOUNT_COLUMN] = out["importe"]
            out["importe"] = ""
        if "estado" in out.columns:
            out["estado"] = out["estado"].where(out["estado"].str.lower() != "nan", "")
    return out.reset_index(drop=True)


def _key_frame(df):
    """Clave estable (_tipo, id) como texto comparable (None si falta)."""
    return pd.DataFrame(
        {
            "_tipo": [None if _is_blank(v) else str(v).strip() for v in df["_tipo"].astype(object)],
            "id": [None if _is_blank(v) else normalize_record_id(v) for v in df["id"].astype(object)],
        },
        dtype=object,
    )


def check_seeds(semillas, output_dir):
    """Cada semilla existe y no es una salida de esta carpeta (el publicado es la
    única copia histórica: no se lee y se sustituye a la vez)."""
    outputs = {
        (Path(output_dir) / name).resolve()
        for name in (FINAL_CSV_NAME, FINAL_PARQUET_NAME, BASE_CSV_NAME, BASE_PARQUET_NAME)
    }
    for path in semillas or ():
        path = Path(path)
        if not path.exists():
            raise ScraperError(f"No existe la semilla {path}")
        if path.resolve() in outputs:
            raise ScraperError(f"La semilla {path} es una salida de {output_dir}: usa otra carpeta de salida")


def apply_seed(acumulado, path, ambito, origen_semilla=None):
    """Añade a `acumulado` las filas de la semilla `path` (parquet) cuya clave
    (_tipo, id) no está en la tabla, solo del ámbito de la descarga
    (rows_in_scope), con _origen (el suyo o origen_semilla / 'release v2026.02')
    y _en_ultima_descarga=False (seleccionar_semilla, como sembrar). Nunca
    modifica ni duplica una fila de la tabla. Devuelve (tabla, informe, fichas
    de la semilla por posición en la tabla, si trae columnas de la ficha)."""
    path = Path(path)
    seed = pd.read_parquet(path)
    ours = "_en_ultima_descarga" in seed.columns
    if ours and not origen_semilla:
        raise ScraperError(f"{path} es una salida de este script (tiene _en_ultima_descarga): "
                           "indica su origen con --origen-semilla")
    missing = [column for column in SEED_KEY + ["_organismo_id", "publicado"] if column not in seed.columns]
    if missing:
        raise ScraperError(f"La semilla {path} no tiene las columnas {missing}")
    origen = origen_semilla or ORIGEN_SEMILLA
    seed = seed.reset_index(drop=True)
    columns = [c for c in dict.fromkeys(SEED_KEY + ["_organismo_id", "publicado"] + list(SEED_CONTENT_COLUMNS))
               if c in seed.columns]
    view = pd.DataFrame({column: _csv_text(seed[column]) for column in columns})
    seed_keys = _key_frame(view)
    inside = rows_in_scope(view, ambito)
    contenido = [c for c in SEED_CONTENT_COLUMNS if c in view.columns and c in acumulado.columns]
    motivo = seleccionar_semilla(
        _key_frame(acumulado),
        seed_keys,
        lambda filas: acumulado.iloc[filas][contenido].reset_index(drop=True),
        lambda filas: view.iloc[filas][contenido].reset_index(drop=True),
        inside,
    )
    informe = informe_semilla(motivo, origen, seed_keys)
    informe["ruta"] = str(path)
    outside = motivo == FUERA_AMBITO
    if outside.any():
        by_org = pd.Series(_org_keys(view["_organismo_id"])[outside]).value_counts()
        detalle = {f"organismo {org}": int(n) for org, n in by_org.head(10).items()}
        if len(by_org) > 10:
            detalle["otros"] = int(by_org.iloc[10:].sum())
        informe["fuera_ambito_detalle"] = detalle
    imprimir_informe_semilla(informe)

    rows = np.flatnonzero(motivo == ANADIDA)
    added = seed_as_text(seed.iloc[rows], published=not ours)
    own = added["_origen"] if "_origen" in added.columns else pd.Series("", index=added.index, dtype=object)
    added["_origen"] = own.where(own != "", origen)
    added["_en_ultima_descarga"] = False
    for column in ("_primera_descarga", "_ultima_descarga"):
        if column not in added.columns:
            # No se sabe cuándo se descargaron las filas del publicado.
            added[column] = ""
    details = {}
    detail_columns = [column for column in DETAIL_EXPORT_FIELDS if column in added.columns]
    if detail_columns:
        offset = len(acumulado)
        for i, record in enumerate(added[detail_columns].to_dict("records")):
            details[offset + i] = record
        added = added.drop(columns=detail_columns)
    out = pd.concat([acumulado, added], ignore_index=True, sort=False)
    return out, informe, details


def _detail_key(record):
    """(tipo, id, organismo) de una fila para la caché de detalle, o None."""
    tipo, record_id, org = record.get("_tipo"), record.get("id"), record.get("_organismo_id")
    if _is_blank(tipo) or _is_blank(record_id) or _is_blank(org):
        return None
    try:
        org = int(float(str(org)))
    except ValueError:
        return None
    return (str(tipo), normalize_record_id(record_id), org)


def _choose_detail(cached, previous):
    """Ficha de una fila de la tabla final: la 'done' de la caché; si la caché no
    la tiene 'done', la de la tabla anterior (o de la semilla) si era 'done' (una
    caché que se ha perdido o sustituido no borra fichas de la tabla); si no, la
    de la caché y, sin nada en la caché, la anterior."""
    if cached and cached.get("detail_status") == "done":
        return cached
    if previous and previous.get("detail_status") == "done":
        return previous
    if cached:
        return cached
    if previous and previous.get("detail_status") not in (None, "", "missing"):
        return previous
    return {}


def _csv_value(value):
    return "" if _is_blank(value) else value


def write_final_csv(acumulado, tmp_path, conn, fieldnames, previous_csv, n_previous, seed_details, chunksize):
    """Escribe la tabla final por trozos: columnas del listado y de control de
    `acumulado` y las de la ficha (_choose_detail) de la caché o, a la vez que se
    leen por trozos del CSV final anterior (sus n_previous filas son las
    primeras de `acumulado`, en el mismo orden), de la tabla anterior."""
    previous_chunks = None
    if n_previous:
        previous_chunks = read_text_csv(
            previous_csv,
            usecols=lambda column: column in DETAIL_EXPORT_FIELDS or column in ("_tipo", "id", "_organismo_id"),
            chunksize=chunksize,
        )
    total_rows = 0
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for start in range(0, len(acumulado), chunksize):
            records = acumulado.iloc[start : start + chunksize].to_dict("records")
            keys = [_detail_key(record) for record in records]
            detail_map = load_detail_map(
                conn,
                [{"_tipo": key[0], "id": key[1], "_organismo_id": key[2]} for key in keys if key],
            )
            previous_rows = []
            if previous_chunks is not None and start < n_previous:
                previous_rows = next(previous_chunks).to_dict("records")
            rows = []
            for i, (record, key) in enumerate(zip(records, keys)):
                if i < len(previous_rows):
                    previous = previous_rows[i]
                    if _detail_key(previous) != key:
                        raise ScraperError(f"Tabla final anterior desalineada en la fila {start + i + 1}; no se toca")
                else:
                    previous = seed_details.get(start + i)
                detail = _choose_detail(detail_map.get(key) if key else None, previous)
                row = {column: _csv_value(record.get(column)) for column in fieldnames
                       if column not in DETAIL_EXPORT_FIELDS}
                for column in DETAIL_EXPORT_FIELDS:
                    row[column] = detail.get(column)
                if not row.get("detail_status"):
                    row["detail_status"] = "missing"
                rows.append(row)
            writer.writerows(rows)
            total_rows += len(rows)
            log(f"MERGE: {total_rows:,} filas")
    return total_rows


def merge_base_and_detail(output_dir, chunksize=BASE_READ_CHUNKSIZE, semillas=(), origen_semilla=None):
    """Tabla final contratos_galicia.csv/.parquet: la tabla anterior acumulada con
    la descarga base (accumulate_listing), las semillas (apply_seed) y las fichas
    de la caché de detalle. Se publica con guardar_version (la anterior va a
    _historico/; si no cambia nada no se toca). Una descarga base vacía no
    cambia nada (ScraperError)."""
    output_dir = Path(output_dir)
    base_csv_path = output_dir / BASE_CSV_NAME
    final_csv_path = output_dir / FINAL_CSV_NAME
    final_parquet_path = output_dir / FINAL_PARQUET_NAME
    check_seeds(semillas, output_dir)

    manifest = read_base_manifest(output_dir)
    fecha = manifest.get("fecha_descarga") or file_date_iso(base_csv_path)
    new = read_text_csv(base_csv_path)
    if not len(new):
        raise ScraperError(f"{base_csv_path} no tiene ningún contrato: una descarga vacía no cambia la tabla final")
    base_columns = list(new.columns)
    ambito = effective_scope(manifest, new)
    previous = read_previous_final(output_dir)
    n_previous = 0 if previous is None else len(previous)
    acumulado, summary = accumulate_listing(previous, new, fecha, ambito)
    del previous, new

    seed_details = {}
    seeded = 0
    for path in semillas or ():
        before = len(acumulado)
        acumulado, _, details = apply_seed(acumulado, path, ambito, origen_semilla)
        seeded += len(acumulado) - before
        seed_details.update(details)

    helper = set(BASE_EXPORT_FIELDS) | set(base_columns) | set(DETAIL_EXPORT_FIELDS) | set(COLUMNAS_META)
    fieldnames = list(dict.fromkeys(
        base_columns
        + DETAIL_EXPORT_FIELDS
        + list(COLUMNAS_META)
        + [column for column in acumulado.columns if column not in helper]
    ))
    conn = init_detail_db(output_dir)
    # Se escribe a un temporal y se publica al terminar (guardar_version): un merge
    # cortado no deja un CSV final truncado que parezca completo.
    tmp_csv_path = final_csv_path.with_name(final_csv_path.name + ".tmp")
    try:
        total_rows = write_final_csv(
            acumulado, tmp_csv_path, conn, fieldnames, final_csv_path, n_previous, seed_details, chunksize
        )
        state = guardar_version(final_csv_path, desde=tmp_csv_path)
    finally:
        conn.close()
        if tmp_csv_path.exists():
            tmp_csv_path.unlink()
    log(f"[FINAL] {final_csv_path}: {state} ({total_rows:,} filas)")

    parquet_path = None
    if HAS_PYARROW:
        parquet_path = csv_to_parquet(final_csv_path, final_parquet_path, "[FINAL] ", chunksize=chunksize)
    mark_base_accumulated(output_dir, fecha)
    current = int(acumulado["_en_ultima_descarga"].astype(bool).sum())
    log(
        f"[FINAL] descarga del {fecha}: {summary['descargadas']:,} filas; tabla anterior {summary['anteriores']:,} "
        f"| altas (contratos nuevos o cambiados) {summary['altas']:,} | retiradas en esta descarga "
        f"{summary['retiradas']:,} | vigentes sin volver a ver, fuera del ámbito {summary['fuera_ambito']:,} "
        f"| de semillas {seeded:,} | total {len(acumulado):,} ({current:,} en la última descarga)"
    )
    return final_csv_path, parquet_path


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(description="Scraper completo Contratos Públicos de Galicia")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["all", "base", "detail", "merge"],
        default="all",
        help="Fase a ejecutar (default: all)",
    )
    parser.add_argument("--organismo", type=int, default=None)
    parser.add_argument(
        "--max-org-id",
        type=int,
        default=2000,
        help="Hasta qué ID probar (default: 2000)",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--delay", type=float, default=DELAY)
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument(
        "--skip-cm",
        action="store_true",
        help="Saltar contratos menores",
    )
    parser.add_argument(
        "--skip-lic",
        action="store_true",
        help="Saltar licitaciones",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Reanudar la descarga base en curso (sin él, se empieza otra; la anterior pasa a _historico/). "
            "En detail, respeta el máximo de intentos de las fichas fallidas (sin él se reintentan todas; "
            "la caché de detalle nunca se borra)"
        ),
    )
    parser.add_argument(
        "--semilla",
        type=Path,
        action="append",
        default=[],
        help=(
            "Parquet publicado (p. ej. contratos_galicia.parquet de v2026.02) que se incorpora en merge como la "
            "instantánea más antigua: solo las filas cuya clave (_tipo, id) no está en la tabla y del ámbito "
            "de la descarga. Repetible"
        ),
    )
    parser.add_argument(
        "--origen-semilla",
        default=None,
        help=f"_origen de las filas añadidas desde --semilla (por defecto '{ORIGEN_SEMILLA}')",
    )
    parser.add_argument(
        "--autosave-every",
        type=int,
        default=AUTOSAVE_EVERY,
        help="Frecuencia de checkpoints de progreso base",
    )
    parser.add_argument(
        "--detail-workers",
        type=int,
        default=DETAIL_WORKERS,
        help="Workers para detalle HTML (default: 8)",
    )
    parser.add_argument(
        "--detail-batch-size",
        type=int,
        default=DETAIL_BATCH_SIZE,
        help="Tamaño de lote para detalle HTML",
    )
    parser.add_argument(
        "--detail-delay",
        type=float,
        default=DETAIL_DELAY,
        help="Delay base entre peticiones de detalle por worker",
    )
    parser.add_argument(
        "--detail-jitter",
        type=float,
        default=DETAIL_JITTER,
        help="Jitter aleatorio adicional por petición de detalle",
    )
    parser.add_argument(
        "--only-type",
        choices=["all", "LIC", "CM"],
        default="all",
        help="Limitar el enriquecimiento a LIC, CM o ambos",
    )
    parser.add_argument(
        "--force-detail",
        action="store_true",
        help=(
            "Reprocesar detalle aunque ya exista en caché (una ficha ya descargada no se pierde: se conserva "
            "si el portal da error o una ficha vacía y, si cambió, la anterior va a detail_cache_historico)"
        ),
    )
    parser.add_argument(
        "--retryable-only",
        action="store_true",
        help="Procesar solo registros del detalle actualmente en estado retryable",
    )
    parser.add_argument(
        "--retryable-ignore-max-attempts",
        action="store_true",
        help="Permitir reintentar retryables aunque hayan alcanzado el máximo de intentos",
    )
    parser.set_defaults(store_raw_detail=True)
    parser.add_argument(
        "--no-raw-detail",
        dest="store_raw_detail",
        action="store_false",
        help="No guardar pairs/tables crudos comprimidos en SQLite",
    )
    return parser


def main(argv=None):
    global PAGE_SIZE, DELAY

    parser = build_parser()
    args = parser.parse_args(argv)

    PAGE_SIZE = args.page_size
    DELAY = args.delay
    output_dir = Path(args.output).expanduser()
    configure_log_path(args.log_path)
    t0 = time.time()
    base_csv_path = output_dir / BASE_CSV_NAME

    print()
    print("=" * 70)
    print("  SCRAPER COMPLETO — CONTRATOS PÚBLICOS DE GALICIA")
    print("  Objetivo: base JSON + detalle HTML incremental/reanudable")
    print("=" * 70)
    print(f"  Modo:          {args.mode}")
    if args.organismo:
        print(f"  Organismo:     {args.organismo}")
    else:
        print(f"  Organismos:    auto (1–{args.max_org_id})")
    print(f"  CM:            ventanas {CM_WINDOW_MONTHS} meses, hasta {DATE_ORIGIN}{'  [SKIP]' if args.skip_cm else ''}")
    print(f"  LIC:           todo el histórico{'  [SKIP]' if args.skip_lic else ''}")
    print(f"  Page size:     {PAGE_SIZE}  │  Delay: {DELAY}s")
    print(
        f"  Detalle HTML:  workers={args.detail_workers} batch={args.detail_batch_size} "
        f"delay={args.detail_delay}s jitter={args.detail_jitter}s"
    )
    print(f"  Solo detalle:  {args.only_type}")
    if args.retryable_only:
        print("  Rescue mode:   solo retryable")
        print(
            "  Max attempts:  "
            + ("ignorado para retryable" if args.retryable_ignore_max_attempts else "respetado")
        )
    print(f"  Resume:        {'sí' if args.resume else 'no'}")
    print(f"  Raw detalle:   {'sí' if args.store_raw_detail else 'no'}")
    print(f"  Auto-save:     cada {args.autosave_every} organismos")
    for seed in args.semilla:
        print(f"  Semilla:       {seed} ({args.origen_semilla or ORIGEN_SEMILLA})")
    print(f"  Output dir:    {output_dir}")
    print(f"  Log:           {args.log_path}")
    print("=" * 70)
    print()

    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)

        base_stats = None
        detail_stats = None
        final_csv_path = None
        final_parquet_path = None

        if args.semilla:
            if args.mode not in ("all", "merge"):
                raise ScraperError("--semilla solo se aplica en merge (o all).")
            # Antes de descargar nada: una semilla que no existe o que es una salida.
            check_seeds(args.semilla, output_dir)

        if args.mode in ("all", "base"):
            if args.skip_cm and args.skip_lic:
                raise ScraperError("No puedes ejecutar base con --skip-cm y --skip-lic a la vez.")
            session = Session()
            base_stats = run_base_scrape(
                session=session,
                output_dir=output_dir,
                organismo=args.organismo,
                max_org_id=args.max_org_id,
                discovery_workers=args.workers,
                skip_cm=args.skip_cm,
                skip_lic=args.skip_lic,
                resume=args.resume,
                autosave_every=args.autosave_every,
            )

        if args.mode in ("all", "detail"):
            if not base_csv_path.exists():
                raise ScraperError(f"No existe el base CSV: {base_csv_path}")
            # La caché de detalle nunca se borra (antes, sin --resume, se borraba
            # entera y se perdían las fichas ya descargadas). Sin --resume se
            # reintentan todas las que no están 'done', como hacía empezar de cero.
            prepare_detail_outputs(output_dir, resume=args.resume or args.retryable_only)
            detail_stats = run_detail_enrichment(
                base_csv_path=base_csv_path,
                output_dir=output_dir,
                workers=args.detail_workers,
                batch_size=args.detail_batch_size,
                only_type=args.only_type,
                force=args.force_detail,
                store_raw=args.store_raw_detail,
                detail_delay=args.detail_delay,
                detail_jitter=args.detail_jitter,
                only_org_id=args.organismo,
                retryable_only=args.retryable_only,
                retryable_ignore_max_attempts=args.retryable_ignore_max_attempts,
                ignore_max_attempts=not args.resume,
            )

        if args.mode in ("all", "merge"):
            if not base_csv_path.exists():
                raise ScraperError(f"No existe el base CSV: {base_csv_path}")
            final_csv_path, final_parquet_path = merge_base_and_detail(
                output_dir, semillas=args.semilla, origen_semilla=args.origen_semilla
            )

        elapsed = time.time() - t0
        hours = int(elapsed // 3600)
        mins = int((elapsed % 3600) // 60)
        secs = int(elapsed % 60)

        print()
        print("=" * 70)
        print(f"  COMPLETADO en {hours}h {mins}m {secs}s")
        if base_stats:
            stats = base_stats["stats"]
            print(f"  Base total:   {stats['records_total']:,} registros")
            print(f"  Base CM:      {stats['cm_total']:,}")
            print(f"  Base LIC:     {stats['lic_total']:,}")
            print(f"  Base CSV:     {Path(base_stats['base_csv_path']).resolve()}")
            if base_stats.get("base_parquet_path"):
                print(f"  Base Parquet: {Path(base_stats['base_parquet_path']).resolve()}")
        if detail_stats:
            print(
                f"  Detail:       done={detail_stats['done']:,} "
                f"retryable={detail_stats['retryable']:,} failed={detail_stats['failed']:,}"
            )
            print(f"  Detail DB:    {Path(detail_stats['db_path']).resolve()}")
        if final_csv_path:
            print(f"  Final CSV:    {Path(final_csv_path).resolve()}")
        if final_parquet_path:
            print(f"  Final Parquet:{Path(final_parquet_path).resolve()}")
        print("=" * 70)
        print()
        return 0
    except KeyboardInterrupt:
        log_warn("Ejecución interrumpida. Puedes reanudar con --resume.")
        return 130
    except ScraperError as exc:
        log_err(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
