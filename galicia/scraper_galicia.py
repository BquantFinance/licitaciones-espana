"""
Scraper COMPLETO – Contratos Públicos de Galicia
=================================================
TODO el histórico, TODOS los organismos, TODOS los campos posibles.

    pip install requests pandas python-dateutil
    python scraper_contratos_galicia.py
    python scraper_contratos_galicia.py --organismo 48
"""

import requests
import pandas as pd
import time
import re
import sys
import os
import json
import argparse
import signal
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.contratosdegalicia.gal"
DATE_END = datetime.now().strftime("%Y-%m-%d")
DATE_ORIGIN = "2000-01-01"  # barrer hasta aquí, sin parar antes

PAGE_SIZE = 100
DELAY = 0.5
MAX_RETRIES = 3

# Ventana para CM (meses). El browser usa 3 meses. NO paramos antes.
CM_WINDOW_MONTHS = 3

# Auto-guardar cada N organismos
AUTOSAVE_EVERY = 10

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


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)

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
        self._init()

    def _init(self):
        log("Obteniendo cookies de sesión...")
        try:
            r = self.s.get(f"{BASE_URL}/resultadoIndex.jsp?lang=es", timeout=30)
            r.raise_for_status()
            log(f"Cookies OK: {list(self.s.cookies.get_dict().keys())}")
        except Exception as e:
            log_warn(f"No se obtuvieron cookies: {e}")

    def visit_org_page(self, org_id):
        """Visita la página del organismo para establecer contexto de sesión."""
        self.s.headers["Referer"] = (
            f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es"
        )
        try:
            self.s.get(
                f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es",
                timeout=15
            )
        except:
            pass

    def get_json(self, url, params, retry=0):
        self.n_requests += 1
        try:
            r = self.s.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError:
            code = r.status_code
            if code == 429 and retry < MAX_RETRIES:
                wait = 2 ** (retry + 2)
                log_warn(f"429 rate-limit, espera {wait}s...")
                time.sleep(wait)
                return self.get_json(url, params, retry + 1)
            if code >= 500 and retry < MAX_RETRIES:
                time.sleep(3)
                return self.get_json(url, params, retry + 1)
            self.n_errors += 1
            log_err(f"HTTP {code}")
            return None
        except Exception as e:
            if retry < MAX_RETRIES:
                time.sleep(3)
                return self.get_json(url, params, retry + 1)
            self.n_errors += 1
            log_err(f"{e}")
            return None


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
# DETAIL ENDPOINT DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def probe_detail_endpoints(session, sample_ids_cm, sample_ids_lic):
    """
    Prueba distintos endpoints de detalle para descubrir cuáles devuelven
    más campos que la tabla. Devuelve el endpoint que funcione o None.
    """
    log("Probando endpoints de detalle para obtener TODOS los campos...")

    candidates = [
        # (url_template, description)
        ("{base}/api/v1/contratosmenores/{id}", "api contratosmenores"),
        ("{base}/api/v1/licitaciones/{id}", "api licitaciones"),
        ("{base}/api/v1/expedientes/{id}", "api expedientes"),
        ("{base}/api/v1/contratos/{id}", "api contratos"),
        ("{base}/api/v1/contratosmenores/{id}/detalle", "api cm detalle"),
        ("{base}/api/v1/licitaciones/{id}/detalle", "api lic detalle"),
    ]

    working = {"cm": None, "lic": None}

    # Probar con IDs de CM
    for cid in sample_ids_cm[:3]:
        for tmpl, desc in candidates:
            url = tmpl.format(base=BASE_URL, id=cid)
            try:
                r = session.s.get(url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and len(data) > 3:
                        fields = list(data.keys())
                        log(f"  ✓ CM detalle OK: {desc} → {len(fields)} campos: {fields[:10]}...")
                        working["cm"] = tmpl
                        break
            except:
                continue
        if working["cm"]:
            break

    # Probar con IDs de LIC
    for lid in sample_ids_lic[:3]:
        for tmpl, desc in candidates:
            url = tmpl.format(base=BASE_URL, id=lid)
            try:
                r = session.s.get(url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and len(data) > 3:
                        fields = list(data.keys())
                        log(f"  ✓ LIC detalle OK: {desc} → {len(fields)} campos: {fields[:10]}...")
                        working["lic"] = tmpl
                        break
            except:
                continue
        if working["lic"]:
            break

    if not working["cm"] and not working["lic"]:
        log_warn("  No se encontró endpoint de detalle. Se usarán solo datos de tabla.")

    return working


def fetch_detail(session, record_id, template):
    """Obtiene detalle de un registro."""
    if not template:
        return None
    url = template.format(base=BASE_URL, id=record_id)
    try:
        r = session.s.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def discover(session, max_id, workers=5):
    log(f"Descubriendo organismos (IDs 1–{max_id})...")
    found = {}

    def probe_lic(org_id):
        url = f"{BASE_URL}/api/v1/organismos/{org_id}/licitaciones/table"
        p = params_lic(0, 1)
        try:
            r = session.s.get(url, params=p, timeout=10)
            if r.status_code == 200:
                return r.json().get("recordsTotal", 0)
        except:
            pass
        return 0

    def probe_cm(org_id):
        url = f"{BASE_URL}/api/v1/organismos/{org_id}/contratosmenores/table"
        d_end = datetime.now()
        d_start = d_end - timedelta(days=90)
        # Referer dinámico
        headers_copy = dict(session.s.headers)
        headers_copy["Referer"] = f"{BASE_URL}/consultaOrganismo.jsp?OR={org_id}&N={org_id}&lang=es"
        p = params_cm(0, 1, d_start.strftime("%Y-%m-%d"), d_end.strftime("%Y-%m-%d"))
        try:
            r = requests.get(url, params=p, headers=headers_copy,
                             cookies=session.s.cookies.get_dict(), timeout=10)
            if r.status_code == 200:
                return r.json().get("recordsTotal", 0)
        except:
            pass
        return 0

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
        if n > 0:
            if i not in found:
                found[i] = {"cm": 0, "lic": 0}
            found[i]["cm"] = n
    print()

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

        if data is None:
            log_err(f"Org {org_id} LIC: fallo en start={start}")
            break

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

        if data is None:
            break

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

    log(f"Org {org_id} CM: barriendo {n_total_windows} ventanas de {CM_WINDOW_MONTHS} meses [{DATE_ORIGIN} → {DATE_END}]")

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
# ENRICH WITH DETAIL
# ─────────────────────────────────────────────────────────────────────────────

def enrich_records(session, records, detail_templates, batch_label=""):
    """Enriquece registros con el endpoint de detalle si existe."""
    if not records:
        return records

    tipo = records[0].get("_tipo", "")
    template = detail_templates.get("cm" if tipo == "CM" else "lic")

    if not template:
        return records

    log(f"Enriqueciendo {len(records):,} registros {tipo} con detalle...")

    enriched = []
    for i, rec in enumerate(records):
        rid = rec.get("id")
        if rid:
            detail = fetch_detail(session, rid, template)
            if detail and isinstance(detail, dict):
                merged = {**rec, **detail}
                enriched.append(merged)
            else:
                enriched.append(rec)
        else:
            enriched.append(rec)

        if (i + 1) % 500 == 0:
            log(f"    {batch_label} detalle: {i+1:,}/{len(records):,}")

        time.sleep(DELAY * 0.3)

    # Contar campos extra
    if enriched:
        table_fields = set(records[0].keys())
        all_fields = set(enriched[0].keys())
        extra = all_fields - table_fields
        if extra:
            log(f"    Campos extra del detalle: {sorted(extra)}")

    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLEANING & EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def clean_html(val):
    if not isinstance(val, str):
        return val
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", val)).strip()


def to_dataframe(records):
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(clean_html)

    for col in df.columns:
        if any(h in col.lower() for h in ("fecha", "publicado", "plazo", "apertura", "modificado")):
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

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
            except:
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

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", sep=";")
    mb = os.path.getsize(path) / 1024 / 1024

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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    global PAGE_SIZE, DELAY

    parser = argparse.ArgumentParser(description="Scraper COMPLETO Contratos Públicos de Galicia")
    parser.add_argument("--organismo", type=int, default=None)
    parser.add_argument("--max-org-id", type=int, default=2000,
                        help="Hasta qué ID probar (default: 2000)")
    parser.add_argument("--output", default="output_contratos_galicia")
    parser.add_argument("--delay", type=float, default=DELAY)
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--no-detail", action="store_true",
                        help="No intentar endpoint de detalle")
    parser.add_argument("--skip-cm", action="store_true",
                        help="Saltar contratos menores")
    parser.add_argument("--skip-lic", action="store_true",
                        help="Saltar licitaciones")
    args = parser.parse_args()

    PAGE_SIZE = args.page_size
    DELAY = args.delay
    csv_path = os.path.join(args.output, "contratos_galicia_COMPLETO.csv")
    t0 = time.time()

    print()
    print("=" * 70)
    print("  SCRAPER COMPLETO — CONTRATOS PÚBLICOS DE GALICIA")
    print("  Objetivo: TODO el histórico, TODOS los organismos, TODOS los campos")
    print("=" * 70)
    if args.organismo:
        print(f"  Organismo:     {args.organismo}")
    else:
        print(f"  Organismos:    auto (1–{args.max_org_id})")
    print(f"  CM:            ventanas {CM_WINDOW_MONTHS} meses, hasta {DATE_ORIGIN}{'  [SKIP]' if args.skip_cm else ''}")
    print(f"  LIC:           todo el histórico{'  [SKIP]' if args.skip_lic else ''}")
    print(f"  Detalle:       {'NO' if args.no_detail else 'probar endpoints extra'}")
    print(f"  Page size:     {PAGE_SIZE}  │  Delay: {DELAY}s")
    print(f"  Auto-save:     cada {AUTOSAVE_EVERY} organismos")
    print(f"  Output:        {csv_path}")
    print("=" * 70)
    print()

    session = Session()
    all_records = []

    # ── Ctrl+C handler ───────────────────────────────────────────────────
    def save_on_interrupt(sig, frame):
        print("\n")
        log_warn(f"Ctrl+C → Guardando {len(all_records):,} registros...")
        save_csv(all_records, csv_path, "[PARCIAL] ")
        elapsed = time.time() - t0
        log(f"Parcial guardado en {int(elapsed//60)}m{int(elapsed%60)}s")
        sys.exit(0)

    signal.signal(signal.SIGINT, save_on_interrupt)

    # ── Discovery ────────────────────────────────────────────────────────
    if args.organismo:
        org_list = [(args.organismo, 1, 1)]
        log(f"Modo organismo único: {args.organismo}")
    else:
        org_list = discover(session, args.max_org_id, args.workers)

    if not org_list:
        log_err("No se encontraron organismos.")
        return

    # ── Probe detail endpoints ───────────────────────────────────────────
    detail_templates = {"cm": None, "lic": None}
    if not args.no_detail:
        # Recoger sample IDs de la primera página del primer org con datos
        sample_cm_ids = []
        sample_lic_ids = []

        for oid, cm_est, lic_est in org_list[:5]:
            if lic_est and lic_est > 0 and not sample_lic_ids:
                session.visit_org_page(oid)
                url = f"{BASE_URL}/api/v1/organismos/{oid}/licitaciones/table"
                data = session.get_json(url, params_lic(0, 5))
                if data and data.get("data"):
                    sample_lic_ids = [r["id"] for r in data["data"] if "id" in r]

            if cm_est and cm_est > 0 and not sample_cm_ids:
                session.visit_org_page(oid)
                url = f"{BASE_URL}/api/v1/organismos/{oid}/contratosmenores/table"
                d_end = datetime.now()
                d_start = d_end - timedelta(days=90)
                data = session.get_json(url, params_cm(0, 5, d_start.strftime("%Y-%m-%d"), d_end.strftime("%Y-%m-%d")))
                if data and data.get("data"):
                    sample_cm_ids = [r["id"] for r in data["data"] if "id" in r]

            if sample_cm_ids and sample_lic_ids:
                break

        detail_templates = probe_detail_endpoints(session, sample_cm_ids, sample_lic_ids)

    # ── Main scraping loop ───────────────────────────────────────────────
    n_orgs = len(org_list)
    orgs_done = 0

    for idx, (org_id, est_cm, est_lic) in enumerate(org_list, 1):
        log(f"{'═'*60}")
        log(f"Organismo {org_id} ({idx}/{n_orgs})")
        log(f"{'═'*60}")

        # Licitaciones
        if not args.skip_lic and (est_lic is None or est_lic > 0):
            recs = paginate_lic(session, org_id)
            if recs and detail_templates.get("lic"):
                recs = enrich_records(session, recs, detail_templates, f"Org {org_id} ")
            all_records.extend(recs)

        # Contratos menores (barrido completo)
        if not args.skip_cm and (est_cm is None or est_cm > 0):
            recs = paginate_cm_full(session, org_id)
            if recs and detail_templates.get("cm"):
                recs = enrich_records(session, recs, detail_templates, f"Org {org_id} ")
            all_records.extend(recs)

        orgs_done += 1

        # Progreso global
        elapsed = time.time() - t0
        n_cm = sum(1 for r in all_records if r.get("_tipo") == "CM")
        n_lic = sum(1 for r in all_records if r.get("_tipo") == "LIC")
        log(f"PROGRESO: {len(all_records):,} total (CM:{n_cm:,} LIC:{n_lic:,}) | "
            f"Org {idx}/{n_orgs} | Req:{session.n_requests:,} Err:{session.n_errors} | "
            f"{int(elapsed//60)}m{int(elapsed%60)}s")

        # Auto-save
        if orgs_done % AUTOSAVE_EVERY == 0:
            log(f"Auto-guardando ({len(all_records):,} registros)...")
            save_csv(all_records, csv_path, "[AUTO-SAVE] ")

        print()

    # ── Final export ─────────────────────────────────────────────────────
    log(f"Guardando resultado final: {len(all_records):,} registros...")
    df = save_csv(all_records, csv_path, "[FINAL] ")

    elapsed = time.time() - t0
    hours = int(elapsed // 3600)
    mins = int((elapsed % 3600) // 60)
    secs = int(elapsed % 60)

    print()
    print("=" * 70)
    print(f"  COMPLETADO en {hours}h {mins}m {secs}s")
    print(f"  {len(df):,} registros totales")
    if "_tipo" in df.columns:
        print(f"  CM:  {len(df[df['_tipo']=='CM']):,}")
        print(f"  LIC: {len(df[df['_tipo']=='LIC']):,}")
    if "_organismo_id" in df.columns:
        print(f"  Organismos: {df['_organismo_id'].nunique()}")
    print(f"  Columnas: {len(df.columns)}")
    print(f"  {session.n_requests:,} requests ({session.n_errors} errores)")
    print(f"  → {os.path.abspath(csv_path)}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()




