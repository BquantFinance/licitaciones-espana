#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 DESCARGA BASE — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Capa base del pipeline Euskadi: descarga JSON/XLSX/CSV con arquitectura
 API-first y fallback a históricos de Open Data.

 Este script deja la materia prima para:
   1. consolidación (`consolidacion_euskadi.py`)
   2. enriquecimiento incremental de detalle (`detalle_euskadi.py`)

 FUENTE CENTRAL: KontratazioA — Plataforma de Contratación Pública en Euskadi
   → 800+ poderes adjudicadores (GV, Diputaciones, Ayuntamientos, OOAA)
   → Registro de Contratos (REVASCON) + Perfil de Contratante

 MÓDULO A — API REST KontratazioA (JSON paginado, 10 items/pág fijo)
   A1. Contracts        — Muestra 1000 registros (bulk = B1 XLSX)
   A2. Contracting Notices — Muestra 1000 registros (bulk = B1 XLSX)
   A3. Contracting Authorities — 800+ poderes adjudicadores (completo)
   A4. Companies         — Empresas en Registro de Licitadores (completo)

 MÓDULO B — XLSX/CSV Históricos (Open Data Euskadi)
   B1. Contratos Sector Público completo (2011-2026)  → XLSX anual
   B2. REVASCON agregado anual (2013-2018)            → CSV/XLSX
   B3. Contratos últimos 90 días (ventana móvil)      → XLSX

 MÓDULO C — Portales municipales independientes (datos NO centralizados)
   C1. Bilbao — contratos adjudicados (2005-2026)     → CSV
   C2. Vitoria-Gasteiz — contratos menores            → CSV

 Notas:
   · Los módulos B1/B2/B3 son exports del mismo REVASCON → redundantes con A1
     pero se mantienen como backup y para series históricas pre-API.
   · C1/C2 son portales propios que publican datos que PUEDEN no estar en
     KontratazioA (especialmente contratos menores municipales).
   · El detalle por expediente ya no vive aquí: se resuelve después, de forma
     incremental, en `detalle_euskadi.py`.
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_DIR = SCRIPT_DIR / "datos_euskadi_contratacion_v4"
DEFAULT_LOG_PATH = SCRIPT_DIR / "descarga_euskadi_v4.log"


def build_dirs(base_dir: Path) -> dict[str, Path]:
    return {
        # Módulo A: API REST
        "api_contracts": base_dir / "A1_api_contratos",
        "api_notices": base_dir / "A2_api_anuncios",
        "api_authorities": base_dir / "A3_api_poderes",
        "api_companies": base_dir / "A4_api_empresas",
        # Módulo B: XLSX históricos
        "xlsx_anual": base_dir / "B1_xlsx_sector_publico_anual",
        "revascon_hist": base_dir / "B2_revascon_historico",
        "ultimos_90d": base_dir / "B3_ultimos_90_dias",
        # Módulo C: Portales municipales
        "bilbao": base_dir / "C1_bilbao",
        "vitoria": base_dir / "C2_vitoria_gasteiz",
    }

HEADERS = {
    "User-Agent": "Mozilla/5.0 (investigacion-academica) contratacion-euskadi/4.0",
    "Accept": "application/json, */*",
}
TIMEOUT  = 120
DELAY    = 1.5   # segundos entre peticiones
RETRIES  = 3

YEAR_NOW = datetime.now().year
YEAR_MIN_GV = 2011  # Primer año XLSX disponible
YEAR_MIN_BILBAO = 2005  # Bilbao publica desde 2005


def build_logger(log_path: Path | None = None) -> logging.Logger:
    logger = logging.getLogger("euskadi.download")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def configure_runtime(
    base_dir: Path | None = None,
    log_path: Path | None = None,
) -> None:
    global BASE_DIR, DIRS, LOG_PATH, log, stats

    BASE_DIR = (base_dir or DEFAULT_BASE_DIR).resolve()
    DIRS = build_dirs(BASE_DIR)
    LOG_PATH = (log_path or DEFAULT_LOG_PATH).resolve()
    log = build_logger(LOG_PATH)
    stats = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0}


BASE_DIR = DEFAULT_BASE_DIR
DIRS = build_dirs(BASE_DIR)
LOG_PATH = DEFAULT_LOG_PATH
log = logging.getLogger("euskadi.download")
log.addHandler(logging.NullHandler())
stats = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def setup_dirs():
    for d in DIRS.values():
        d.mkdir(parents=True, exist_ok=True)


def is_real_data(content: bytes, ext: str) -> bool:
    """Descarta respuestas de error disfrazadas (HTML 404 en vez de datos)."""
    if len(content) < 200:
        return False
    head = content[:500].lower()
    if b"<html" in head and (b"404" in head or b"error" in head):
        return False
    if ext == ".xlsx" and content[:4] != b"PK\x03\x04":
        return False
    if ext == ".xls" and content[:8] != b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return False
    if ext == ".json" and content.strip()[:1] not in (b"{", b"["):
        return False
    return True


def download(url: str, dest: Path, label: str = "",
             skip_retry_on_404: bool = True) -> bool:
    """Descarga un fichero con reintentos. 404 no se reintenta."""
    if dest.exists() and dest.stat().st_size > 100:
        log.info("  SKIP  %s", dest.name)
        stats["skip"] += 1
        return True

    tag = label or dest.name
    for attempt in range(1, RETRIES + 1):
        try:
            log.info("  GET [%d/%d] %s", attempt, RETRIES, tag)
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)

            # 404 = definitivo, no reintentar
            if r.status_code == 404 and skip_retry_on_404:
                log.warning("  404  %s — saltando", tag)
                stats["fail"] += 1
                return False

            if r.status_code == 200 and is_real_data(r.content, dest.suffix):
                dest.write_bytes(r.content)
                size = len(r.content)
                stats["ok"] += 1
                stats["bytes"] += size
                log.info("  OK   %s  (%.1f KB)", dest.name, size / 1024)
                return True
            else:
                log.warning("  WARN status=%s size=%d  %s",
                            r.status_code, len(r.content), tag)
        except Exception as e:
            log.warning("  ERR  intento %d: %s", attempt, e)

        time.sleep(DELAY * attempt)

    stats["fail"] += 1
    log.error("  FAIL  %s", tag)
    return False


# ═══════════════════════════════════════════════════════════════
# MÓDULO A — API REST KONTRATAZIOA
# ═══════════════════════════════════════════════════════════════
#
# La API de contrataciones públicas de Euskadi expone 4 endpoints:
#   · Contracts            (C)   → contratos registrados en REVASCON
#   · Contracting Notices  (CN)  → anuncios del Perfil de Contratante
#   · Contracting Authorities (CA) → poderes adjudicadores
#   · Companies            (CO)  → empresas licitadoras
#
# El base URL exacto se autodescubre probando candidatos (la
# documentación oficial no publica un base URL estable).
# ═══════════════════════════════════════════════════════════════

# Candidatos de base URL para la API REST (probados en orden)
API_BASE_CANDIDATES = [
    # Patrón del enlace Swagger UI en la web
    "https://opendata.euskadi.eus/api-procurements",
    # Patrón alternativo (namespace del servlet)
    "https://opendata.euskadi.eus/webopd00-apicontract",
    # Patrón api.euskadi.eus (usado por otras APIs como meteo)
    "https://api.euskadi.eus/procurements",
    # Patrón con /api/ explícito
    "https://opendata.euskadi.eus/api/procurements",
]

# Sufijos de endpoint por tipo de recurso
API_ENDPOINTS = {
    "contracts":    ["/contracts", "/api/contracts",
                     "?api=procurements&_type=contracts",
                     "/es?api=procurements"],
    "notices":      ["/contracting-notices", "/api/contracting-notices",
                     "?api=procurements&_type=contracting-notices",
                     "/notices"],
    "authorities":  ["/contracting-authorities", "/api/contracting-authorities",
                     "?api=procurements&_type=contracting-authorities",
                     "/authorities"],
    "companies":    ["/companies", "/api/companies",
                     "?api=procurements&_type=companies"],
}


def _probe_api() -> dict:
    """
    Autodescubrimiento de endpoints de la API.
    Prueba combinaciones de base_url + endpoint hasta encontrar
    las que devuelven JSON válido.

    Devuelve dict con las URLs funcionales, ej:
        {"contracts": "https://...?currentPage=1",
         "notices": "https://...", ...}
    """
    log.info("  Probando endpoints de la API REST...")
    working = {}

    for resource, suffixes in API_ENDPOINTS.items():
        if resource in working:
            continue
        for base in API_BASE_CANDIDATES:
            if resource in working:
                break
            for suffix in suffixes:
                # Construir URL de prueba — probar currentPage (KontratazioA)
                # y _page (genérico) como param de paginación
                sep = "&" if "?" in suffix else "?"
                test_url = f"{base}{suffix}{sep}currentPage=1"
                try:
                    r = requests.get(test_url, headers=HEADERS, timeout=30)
                    if r.status_code == 200:
                        ct = r.headers.get("Content-Type", "")
                        # Aceptar si es JSON
                        if "json" in ct or "javascript" in ct:
                            data = r.json()
                            if isinstance(data, (dict, list)):
                                # Extraer el base_url funcional (sin paginación)
                                api_url = f"{base}{suffix}"
                                working[resource] = api_url
                                log.info("    ✓ %s → %s", resource, api_url)
                                break
                        # Aceptar si parece JSON aunque CT sea text
                        elif r.text.strip().startswith(("{", "[")):
                            data = r.json()
                            if isinstance(data, (dict, list)):
                                api_url = f"{base}{suffix}"
                                working[resource] = api_url
                                log.info("    ✓ %s → %s", resource, api_url)
                                break
                except Exception:
                    pass

    return working


def _paginate_api(api_url: str, resource_name: str, dest_dir: Path,
                  prefix: str, max_pages: int = 5000, delay: float = DELAY):
    """
    Descarga paginada de la API REST de KontratazioA.

    La API tiene página fija de 10 items (ignora _pageSize).
    Paginación: ?currentPage=N (1-based).
    Estructura respuesta: {totalItems, totalPages, currentPage,
                           itemsOfPage, items: [...]}
    """
    sep = "&" if "?" in api_url else "?"

    # ── Página 1: descubrir totalItems y totalPages ─────────
    first_url = f"{api_url}{sep}currentPage=1"
    try:
        r = requests.get(first_url, headers=HEADERS, timeout=TIMEOUT)
        data = r.json()
    except Exception as e:
        log.error("  ERR %s: no se pudo leer página 1: %s", resource_name, e)
        stats["fail"] += 1
        return

    total_items = data.get("totalItems", 0)
    total_pages = data.get("totalPages", 1)
    page_size = data.get("itemsOfPage", 10)

    log.info("  %s: %d items en %d páginas (fijo %d items/pág)",
             resource_name, total_items, total_pages, page_size)

    # Limitar páginas máximas
    pages_to_download = min(total_pages, max_pages)
    if total_pages > max_pages:
        log.warning("  ⚠ Limitado a %d/%d páginas (%.0f%% de %d items)",
                    max_pages, total_pages,
                    100 * max_pages / total_pages, total_items)

    # ── Guardar página 1 ────────────────────────────────────
    dest = dest_dir / f"{prefix}_p{1:05d}.json"
    if not (dest.exists() and dest.stat().st_size > 100):
        dest.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        stats["ok"] += 1
        stats["bytes"] += dest.stat().st_size
    else:
        stats["skip"] += 1

    # ── Páginas 2..N ────────────────────────────────────────
    errors_consec = 0
    for page in range(2, pages_to_download + 1):
        dest = dest_dir / f"{prefix}_p{page:05d}.json"

        if dest.exists() and dest.stat().st_size > 100:
            stats["skip"] += 1
            continue

        url = f"{api_url}{sep}currentPage={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                log.warning("  %s: status %d en page %d", resource_name, r.status_code, page)
                errors_consec += 1
                if errors_consec >= 5:
                    log.error("  %s: 5 errores consecutivos — abortando.", resource_name)
                    break
                time.sleep(delay * 2)
                continue

            page_data = r.json()
            items = page_data.get("items", [])
            if not items:
                log.info("  %s: página %d vacía — fin.", resource_name, page)
                break

            dest.write_text(
                json.dumps(page_data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            size = dest.stat().st_size
            stats["ok"] += 1
            stats["bytes"] += size
            errors_consec = 0

            # Progreso cada 50 páginas o en la última
            if page % 50 == 0 or page == pages_to_download:
                pct = 100 * page / pages_to_download
                log.info("  %s: p%d/%d (%.0f%%) — %d items descargados",
                         resource_name, page, pages_to_download, pct,
                         page * page_size)

        except Exception as e:
            log.warning("  ERR %s page %d: %s", resource_name, page, e)
            stats["fail"] += 1
            errors_consec += 1
            if errors_consec >= 5:
                log.error("  %s: 5 errores consecutivos — abortando.", resource_name)
                break

        time.sleep(delay)



def dl_A_api(api_urls: dict):
    """
    Descarga los 4 endpoints de la API REST de KontratazioA.

    Estrategia según tamaño:
      · authorities (~800 items, ~80 pág)   → descarga completa
      · companies   (~miles, ~cientos pág)  → descarga completa
      · contracts   (655K+ items, 65K+ pág) → MUESTRA (XLSX = fuente bulk)
      · notices     (grande)                → MUESTRA (XLSX = fuente bulk)

    La API tiene página fija de 10 items (no configurable, usa currentPage=N).
    Los XLSX de B1 contienen los mismos datos de contracts en formato
    tabular, descargables en 2 minutos vs ~27h por API.
    """

    # ── Endpoints pequeños: descarga completa ───────────────
    small_endpoints = [
        ("authorities",  "A3_Poderes",   "api_authorities", "poderes"),
        ("companies",    "A4_Empresas",  "api_companies",   "empresas"),
    ]
    for resource, name, dir_key, prefix in small_endpoints:
        log.info("=" * 60)
        log.info("A. API REST — %s (descarga completa)", name)
        log.info("=" * 60)
        if resource not in api_urls:
            log.warning("  ⚠ Endpoint %s no descubierto — saltando.", resource)
            continue
        _paginate_api(
            api_url=api_urls[resource],
            resource_name=name,
            dest_dir=DIRS[dir_key],
            prefix=prefix,
            max_pages=5000,   # sin límite práctico para datasets pequeños
            delay=0.5,        # más rápido para pocos registros
        )

    # ── Endpoints grandes: muestra (bulk data = XLSX B1) ────
    # Contratos: 655K+ items × 10/pág = 65K+ peticiones (~27h)
    # La misma data está en B1_xlsx como XLSX descargable en 2 min
    API_SAMPLE_PAGES = 100  # 100 págs × 10 items = 1000 registros de muestra

    large_endpoints = [
        ("contracts",  "A1_Contratos",  "api_contracts", "contratos"),
        ("notices",    "A2_Anuncios",   "api_notices",   "anuncios"),
    ]
    for resource, name, dir_key, prefix in large_endpoints:
        log.info("=" * 60)
        log.info("A. API REST — %s (muestra %d págs)", name, API_SAMPLE_PAGES)
        log.info("=" * 60)
        if resource not in api_urls:
            log.warning("  ⚠ Endpoint %s no descubierto — saltando.", resource)
            continue
        log.info("  ℹ La API tiene página fija de 10 items (no configurable).")
        log.info("    Descarga bulk inviable (~27h). Usando XLSX (B1) como")
        log.info("    fuente principal. API = muestra de %d registros.", API_SAMPLE_PAGES * 10)
        _paginate_api(
            api_url=api_urls[resource],
            resource_name=name,
            dest_dir=DIRS[dir_key],
            prefix=prefix,
            max_pages=API_SAMPLE_PAGES,
            delay=0.3,        # delay corto para la muestra
        )


# ═══════════════════════════════════════════════════════════════
# MÓDULO B — XLSX/CSV HISTÓRICOS (OPEN DATA EUSKADI)
# ═══════════════════════════════════════════════════════════════
#
# Exports periódicos de los mismos datos de REVASCON/KontratazioA
# en formato XLSX. Útiles como:
#   · Backup del módulo A
#   · Series históricas pre-API (antes de 2020/2024)
#   · Formato tabular listo para análisis (vs JSON)
# ═══════════════════════════════════════════════════════════════

def dl_B1_xlsx_anual():
    """
    B1. Contratos Administrativos del Sector Público Vasco (XLSX anuales)
    Fuente: Open Data Euskadi → export anual de KontratazioA.
    Incluye contratos de GV + OOAA + poderes adheridos.
    Desde 2019 incluye contratos menores → ficheros de 15-27 MB.
    """
    log.info("=" * 60)
    log.info("B1. XLSX — CONTRATOS SECTOR PÚBLICO (anual, 2011-%d)", YEAR_NOW)
    log.info("=" * 60)
    d = DIRS["xlsx_anual"]
    base = "https://opendata.euskadi.eus/contenidos/ds_contrataciones"

    for year in range(YEAR_MIN_GV, YEAR_NOW + 1):
        url = f"{base}/contrataciones_admin_{year}/opendata/contratos.xlsx"
        dest = d / f"contratos_{year}.xlsx"
        download(url, dest, f"XLSX-{year}")
        time.sleep(DELAY)

    # ── JSON fallback: 2011-2013 XLSX están vacíos (solo cabeceras)
    #    pero los JSON de Open Data SÍ contienen los datos completos.
    log.info("  B1-fix: descargando JSON 2011-2013 (XLSX vacíos)…")
    for year in (2011, 2012, 2013):
        dest_json = d / f"contratos_{year}.json"
        if dest_json.exists() and dest_json.stat().st_size > 500:
            log.info("  SKIP  %s (%.0f KB)", dest_json.name,
                     dest_json.stat().st_size / 1024)
            stats["skip"] += 1
            continue
        url_json = f"{base}/contrataciones_admin_{year}/opendata/contratos.json"
        download(url_json, dest_json, f"JSON-{year}")
        time.sleep(DELAY)


def dl_B2_revascon_historico():
    """
    B2. REVASCON — Registro de Contratos Sector Público (agregado anual)
    Formato más rico que B1 para el período 2013-2018.
    Desde 2019 el modelo cambió a publicación por poder adjudicador,
    cubierto por la API (módulo A).
    """
    log.info("=" * 60)
    log.info("B2. REVASCON HISTÓRICO (agregado anual, 2013-2018)")
    log.info("=" * 60)
    d = DIRS["revascon_hist"]
    base = "https://opendata.euskadi.eus/contenidos/ds_contrataciones"

    # URLs conocidas (CSV donde exista, XLSX como fallback)
    sources = {
        2013: {
            "csv": f"{base}/registro_contratos_2013/es_contracc/adjuntos/revascon-2013.csv",
        },
        2014: {
            "csv": f"{base}/registro_contratos_2014/es_contracc/adjuntos/revascon-2014.csv",
            "xlsx": (f"{base}/contratos_euskadi_2014/es_contracc/adjuntos/"
                     "Registro_de_contratos_del_Sector_Publico_de_Euskadi_del_2014.xlsx"),
        },
    }
    # 2015-2018: solo XLSX disponible
    for y in range(2015, 2019):
        sources[y] = {
            "xlsx": (f"{base}/contratos_euskadi_{y}/es_contracc/adjuntos/"
                     f"Registro_de_contratos_del_Sector_Publico_de_Euskadi_del_{y}.xlsx"),
        }

    for year, urls in sorted(sources.items()):
        # Intentar CSV primero
        if "csv" in urls:
            dest_csv = d / f"revascon_{year}.csv"
            if download(urls["csv"], dest_csv, f"REVASCON-{year}-CSV"):
                time.sleep(DELAY)
                continue

        # Fallback a XLSX
        if "xlsx" in urls:
            dest_xlsx = d / f"revascon_{year}.xlsx"
            download(urls["xlsx"], dest_xlsx, f"REVASCON-{year}-XLSX")

        time.sleep(DELAY)


def dl_B3_ultimos_90d():
    """
    B3. Snapshot de contratos de los últimos 90 días.
    Ventana móvil con datos recientes de toda la CAE.
    """
    log.info("=" * 60)
    log.info("B3. CONTRATOS ÚLTIMOS 90 DÍAS (snapshot)")
    log.info("=" * 60)
    d = DIRS["ultimos_90d"]
    base = ("https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
            "contrataciones_ultimos_dias/opendata/contratos")
    hoy = datetime.now().strftime("%Y%m%d")
    download(f"{base}.xlsx", d / f"ultimos_90d_{hoy}.xlsx", "90-días")


# ═══════════════════════════════════════════════════════════════
# MÓDULO C — PORTALES MUNICIPALES INDEPENDIENTES
# ═══════════════════════════════════════════════════════════════
#
# Bilbao y Vitoria tienen portales open data propios con contratos
# que PUEDEN incluir datos no centralizados en KontratazioA,
# especialmente contratos menores municipales.
# ═══════════════════════════════════════════════════════════════

def dl_C1_bilbao():
    """
    C1. Bilbao — Contratos adjudicados (2005-presente).
    Portal propio: bilbao.eus/opendata. CSV con todos los tipos.
    """
    log.info("=" * 60)
    log.info("C1. BILBAO — CONTRATOS ADJUDICADOS (CSV, 2005-%d)", YEAR_NOW)
    log.info("=" * 60)
    d = DIRS["bilbao"]
    base = "https://www.bilbao.eus/opendata/datos/licitaciones"

    # Descarga por año (serie completa)
    for year in range(YEAR_MIN_BILBAO, YEAR_NOW + 1):
        url = f"{base}?formato=csv&anio={year}&idioma=es"
        dest = d / f"bilbao_{year}.csv"
        download(url, dest, f"Bilbao-{year}")
        time.sleep(DELAY)

    # Descarga por tipo de contrato (histórico completo)
    for tipo in ("obras", "servicios", "suministros"):
        url = f"{base}?formato=csv&tipoContrato={tipo}&idioma=es"
        dest = d / f"bilbao_tipo_{tipo}.csv"
        download(url, dest, f"Bilbao-tipo-{tipo}")
        time.sleep(DELAY)

    # Licitaciones abiertas (snapshot)
    hoy = datetime.now().strftime("%Y%m%d")
    url = f"{base}?formato=csv&abiertas=true&idioma=es"
    dest = d / f"bilbao_abiertas_{hoy}.csv"
    download(url, dest, "Bilbao-abiertas")


def dl_C2_vitoria():
    """
    C2. Vitoria-Gasteiz — Contratos menores formalizados.
    Fuente específica para menores del Ayuntamiento.
    """
    log.info("=" * 60)
    log.info("C2. VITORIA-GASTEIZ — CONTRATOS MENORES (CSV)")
    log.info("=" * 60)
    d = DIRS["vitoria"]
    base = ("https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
            "contratos_menores_formalizados/opendata/contratos_menores")

    download(f"{base}.csv", d / "vitoria_menores.csv", "Vitoria-menores-CSV")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Descarga centralizada de contratación pública de Euskadi",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
        help="Directorio donde se guardarán los JSON/XLSX/CSV descargados.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Ruta del fichero de log de descarga.",
    )
    return parser


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_runtime(base_dir=args.base_dir, log_path=args.log_path)

    t0 = time.time()
    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║  CONTRATACIÓN PÚBLICA DE EUSKADI — DESCARGA CENTRAL v4  ║")
    log.info("║  API-first · XLSX fallback · Portales municipales       ║")
    log.info("║  Fecha: %s                                  ║",
             datetime.now().strftime("%Y-%m-%d"))
    log.info("╚═══════════════════════════════════════════════════════════╝")

    setup_dirs()

    log.info("=" * 60)
    log.info("FASE 0: DESCUBRIMIENTO API REST KONTRATAZIOA")
    log.info("=" * 60)
    api_urls = _probe_api()

    if api_urls:
        log.info("  Endpoints descubiertos: %d/4", len(api_urls))
        for key, value in api_urls.items():
            log.info("    · %s → %s", key, value)
    else:
        log.warning("  ⚠ Ningún endpoint API descubierto.")
        log.warning("    Se usarán exclusivamente los XLSX históricos.")

    if api_urls:
        dl_A_api(api_urls)

    dl_B1_xlsx_anual()
    dl_B2_revascon_historico()
    dl_B3_ultimos_90d()
    dl_C1_bilbao()
    dl_C2_vitoria()

    elapsed = time.time() - t0
    log.info("═" * 60)
    log.info("RESUMEN v4")
    log.info("─" * 60)
    log.info("  Descargados:  %d ficheros", stats["ok"])
    log.info("  Existentes:   %d (skip)", stats["skip"])
    log.info("  Fallidos:     %d", stats["fail"])
    log.info("  Volumen:      %.1f MB", stats["bytes"] / 1024 / 1024)
    log.info("  Tiempo:       %.0f s", elapsed)
    if api_urls:
        log.info("  API endpoints: %s", ", ".join(api_urls.keys()))
    else:
        log.info("  API endpoints: ninguno (solo XLSX/CSV)")
    log.info("═" * 60)

    log.info("\nEstructura:")
    log.info("  A1_api_contratos/          ← JSON muestra 1K registros (bulk = B1)")
    log.info("  A2_api_anuncios/           ← JSON muestra 1K anuncios (bulk = B1)")
    log.info("  A3_api_poderes/            ← JSON completo — 800+ poderes adjudicadores")
    log.info("  A4_api_empresas/           ← JSON completo — empresas licitadoras")
    log.info("  B1_xlsx_sector_publico/    ← XLSX anuales (2011-%d) — FUENTE PRINCIPAL", YEAR_NOW)
    log.info("  B2_revascon_historico/     ← CSV/XLSX 2013-2018 — serie histórica")
    log.info("  B3_ultimos_90_dias/        ← XLSX snapshot reciente")
    log.info("  C1_bilbao/                 ← CSV contratos municipales (2005-%d)", YEAR_NOW)
    log.info("  C2_vitoria_gasteiz/        ← CSV contratos menores municipales")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
