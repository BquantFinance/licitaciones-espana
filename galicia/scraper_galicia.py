"""
Scraper COMPLETO – Contratos Públicos de Galicia
=================================================
TODO el histórico, TODOS los organismos, TODOS los campos posibles.

    pip install requests pandas pyarrow python-dateutil
    python galicia/scraper_galicia.py
    python galicia/scraper_galicia.py --organismo 48
"""

import argparse
import csv
import hashlib
import importlib.util
import json
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
from datetime import datetime, timedelta
import unicodedata

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

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

def paginate_lic(session, org_id):
    """Todas las licitaciones de un organismo."""
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
        ok = "✓" if len(all_recs) == total else "⚠"
        sys.stdout.write(f"\r    Org {org_id} LIC: {len(all_recs):,}/{total:,} {ok}          \n")
        sys.stdout.flush()
        if len(all_recs) != total:
            log_warn(f"Org {org_id} LIC: DESAJUSTE esperados={total:,} descargados={len(all_recs):,}")

    for r in all_recs:
        r["_organismo_id"] = org_id
        r["_tipo"] = "LIC"
    return all_recs


def paginate_cm_window(session, org_id, date_start, date_end):
    """Pagina CM para una ventana de fecha."""
    url = f"{BASE_URL}/api/v1/organismos/{org_id}/contratosmenores/table"
    all_recs = []
    start = 0
    total = None
    draw = 1

    while True:
        p = params_cm(start, PAGE_SIZE, date_start, date_end, draw)
        draw += 1
        data = session.get_json(url, p)

        if total is None:
            total = data.get("recordsTotal", 0)
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

    return all_recs, total or 0


def paginate_cm_full(session, org_id):
    """
    CM: barre TODAS las ventanas de CM_WINDOW_MONTHS desde hoy hasta DATE_ORIGIN.
    SIN parar antes — recorre todo el rango completo.
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

        recs, reported = paginate_cm_window(session, org_id, ds, de)

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

    for r in all_recs:
        r["_organismo_id"] = org_id
        r["_tipo"] = "CM"
    return all_recs


# ─────────────────────────────────────────────────────────────────────────────
# BASE SCRAPE PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def remove_if_exists(path):
    path = Path(path)
    if path.exists():
        path.unlink()


def prepare_base_outputs(output_dir, resume=False):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if resume:
        return
    remove_if_exists(output_dir / BASE_CSV_NAME)
    remove_if_exists(output_dir / BASE_PARQUET_NAME)
    remove_if_exists(output_dir / BASE_PROGRESS_NAME)


def prepare_detail_outputs(output_dir, resume=False):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if resume:
        return
    remove_if_exists(output_dir / DETAIL_DB_NAME)


def prepare_final_outputs(output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    remove_if_exists(output_dir / FINAL_CSV_NAME)
    remove_if_exists(output_dir / FINAL_PARQUET_NAME)


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
    prepare_base_outputs(output_dir, resume=resume)
    completed_orgs, previous_stats = load_base_resume(output_dir) if resume else (set(), {})

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
    for idx, (org_id, est_cm, est_lic) in enumerate(pending_orgs, start=1):
        org_records = []
        log(f"{'═'*60}")
        log(f"BASE organismo {org_id} ({idx}/{len(pending_orgs)})")
        log(f"{'═'*60}")

        if not skip_lic and (est_lic is None or est_lic > 0):
            recs = paginate_lic(session, org_id)
            org_records.extend(recs)
            stats["lic_total"] += len(recs)
            stats["records_total"] += len(recs)

        if not skip_cm and (est_cm is None or est_cm > 0):
            recs = paginate_cm_full(session, org_id)
            org_records.extend(recs)
            stats["cm_total"] += len(recs)
            stats["records_total"] += len(recs)

        append_base_records(org_records, output_dir, label=f"[BASE ORG {org_id}] ")
        completed_orgs.add(org_id)
        stats["completed_orgs_count"] = len(completed_orgs)
        save_base_progress(output_dir, completed_orgs, stats=stats)

        if idx % autosave_every == 0:
            log(f"BASE checkpoint {idx}/{len(pending_orgs)} organismos.")

        log(
            "BASE PROGRESO: "
            f"{stats['records_total']:,} total | CM:{stats['cm_total']:,} "
            f"LIC:{stats['lic_total']:,} | Org completados:{len(completed_orgs):,}"
        )
        print()

    save_base_progress(output_dir, completed_orgs, stats=stats)
    base_parquet_path = finalize_base_parquet(output_dir, label="[BASE FINAL] ")
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


def parse_datetime_series(series):
    """Intenta respetar ISO primero y cae a day-first para formatos locales."""
    text = series.astype("string")
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    iso_mask = text.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)

    if iso_mask.any():
        iso_values = text[iso_mask].str.replace(
            r"(Z|[+-]\d{2}:?\d{2})$",
            "",
            regex=True,
        )
        parsed.loc[iso_mask] = pd.to_datetime(iso_values, errors="coerce")

    non_iso_mask = ~iso_mask
    if non_iso_mask.any():
        parsed.loc[non_iso_mask] = pd.to_datetime(
            text[non_iso_mask],
            errors="coerce",
            dayfirst=True,
        )
    return parsed


def to_dataframe(records):
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(clean_html)

    for col in df.columns:
        if any(h in col.lower() for h in ("fecha", "publicado", "plazo", "apertura", "modificado")):
            df[col] = parse_datetime_series(df[col])

    for col in df.columns:
        if any(h in col.lower() for h in ("importe", "precio", "valor", "presupuesto")):
            try:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                    .str.replace("€", "", regex=False)
                    .str.strip()
                    .pipe(pd.to_numeric, errors="coerce")
                )
            except (AttributeError, TypeError, ValueError):
                pass

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
    df.to_csv(path, index=False, encoding="utf-8-sig", sep=";")
    mb = path.stat().st_size / 1024 / 1024

    n_cm = len(df[df["_tipo"] == "CM"]) if "_tipo" in df.columns else 0
    n_lic = len(df[df["_tipo"] == "LIC"]) if "_tipo" in df.columns else 0
    n_orgs = df["_organismo_id"].nunique() if "_organismo_id" in df.columns else 0

    log(f"{label}CSV: {path}")
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
    df.to_parquet(path, index=False, engine="pyarrow")
    mb = path.stat().st_size / 1024 / 1024
    log(f"{label}Parquet: {path}")
    log(f"{label}  {len(df):,} filas ({mb:.1f} MB)")
    return path


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

    df = pd.read_csv(base_csv_path, sep=";", encoding="utf-8-sig", low_memory=False)
    return save_parquet(df, base_parquet_path, label=label)


def save_base_progress(output_dir, completed_orgs, stats=None):
    progress_path = Path(output_dir) / BASE_PROGRESS_NAME
    progress = {
        "saved_at": datetime.now().isoformat(),
        "completed_orgs": sorted(completed_orgs),
    }
    if stats:
        progress["stats"] = stats
    progress_path.write_text(compact_json(progress), encoding="utf-8")
    return progress_path


def load_base_resume(output_dir):
    output_dir = Path(output_dir)
    progress_path = output_dir / BASE_PROGRESS_NAME
    csv_path = output_dir / BASE_CSV_NAME
    completed_orgs = set()
    stats = {}

    if progress_path.exists():
        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
            completed_orgs = set(payload.get("completed_orgs", []))
            stats = payload.get("stats", {})
        except Exception as exc:
            log_warn(f"No se pudo leer el progreso base: {exc}")
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


def iter_base_chunks(base_csv_path, chunksize=BASE_READ_CHUNKSIZE):
    base_csv_path = Path(base_csv_path)
    yield from pd.read_csv(
        base_csv_path,
        sep=";",
        encoding="utf-8-sig",
        low_memory=False,
        chunksize=chunksize,
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
):
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
            if cached["attempts"] >= DETAIL_MAX_ATTEMPTS:
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
    retryable = status in (403, 429) or (status is not None and status >= 500)
    if "Página de detalle inválida" in message:
        retryable = True
    return retryable, status


def fetch_detail_batch(batch, detail_delay=DETAIL_DELAY, detail_jitter=DETAIL_JITTER, store_raw=True):
    session = get_detail_session()
    results = []
    if not batch:
        return results
    session.ensure_org_context(batch[0]["_organismo_id"])

    for index, record in enumerate(batch):
        if index > 0:
            time.sleep(max(0.0, detail_delay + random.uniform(0, detail_jitter)))

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


def persist_detail_results(conn, results):
    if not results:
        return
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
    )

    submitted = {}
    processed = done = retryable = failed = 0
    recent_ban_errors = 0

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
        )
        submitted[future] = len(batch)
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
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
        payload["detail_url"] = row.get("detail_url")
        payload["detail_page_title"] = row.get("page_title")
        mapped[key] = payload
    return mapped


def merge_base_and_detail(output_dir, chunksize=BASE_READ_CHUNKSIZE):
    output_dir = Path(output_dir)
    base_csv_path = output_dir / BASE_CSV_NAME
    final_csv_path = output_dir / FINAL_CSV_NAME
    final_parquet_path = output_dir / FINAL_PARQUET_NAME
    conn = init_detail_db(output_dir)

    fieldnames = None
    total_rows = 0
    with final_csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = None
        for chunk in iter_base_chunks(base_csv_path, chunksize=chunksize):
            records = chunk.to_dict("records")
            detail_map = load_detail_map(conn, records)
            merged_rows = []
            for record in records:
                key = (
                    record["_tipo"],
                    normalize_record_id(record["id"]),
                    int(record["_organismo_id"]),
                )
                detail = detail_map.get(key, {})
                merged = dict(record)
                for column in DETAIL_EXPORT_FIELDS:
                    merged[column] = detail.get(column)
                if not merged.get("detail_status"):
                    merged["detail_status"] = "missing"
                merged_rows.append(merged)
            if merged_rows and writer is None:
                fieldnames = list(merged_rows[0].keys())
                writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter=";")
                writer.writeheader()
            if writer:
                writer.writerows(merged_rows)
            total_rows += len(merged_rows)
            log(f"MERGE: {total_rows:,} filas")

    parquet_path = None
    if HAS_PYARROW:
        df = pd.read_csv(final_csv_path, sep=";", encoding="utf-8-sig", low_memory=False)
        parquet_path = save_parquet(df, final_parquet_path, "[FINAL] ")
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
        help="Reanudar usando el CSV base / SQLite de detalle existentes",
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
        help="Reprocesar detalle aunque ya exista en caché",
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
            prepare_detail_outputs(output_dir, resume=args.resume)
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
            )

        if args.mode in ("all", "merge"):
            if not base_csv_path.exists():
                raise ScraperError(f"No existe el base CSV: {base_csv_path}")
            prepare_final_outputs(output_dir)
            final_csv_path, final_parquet_path = merge_base_and_detail(output_dir)

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
