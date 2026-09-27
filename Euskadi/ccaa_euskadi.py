#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 DESCARGA CENTRALIZADA — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Arquitectura limpia API-first con fallback a XLSX históricos.

 FUENTE CENTRAL: KontratazioA — Plataforma de Contratación Pública en Euskadi
   → 800+ poderes adjudicadores (GV, Diputaciones, Ayuntamientos, OOAA)
   → Registro de Contratos (REVASCON) + Perfil de Contratante

 MÓDULO A — API REST KontratazioA (JSON paginado)
   A1. Contracts        — muestra/sonda de paginación (?currentPage=N a secas)
   A2. Contracting Notices — muestra/sonda de paginación
   A1c/A2c. Contracts / Contracting Notices COMPLETOS (655K contratos con
       importe, adjudicatario y CIF; 656K anuncios con presupuesto): descarga
       por ventanas mensuales de fecha, ver dl_A_api_completa()
   A3. Contracting Authorities — 800+ poderes adjudicadores (completo)
   A4. Companies         — Empresas en Registro de Licitadores (completo)

 MÓDULO B — XLSX/CSV Históricos (Open Data Euskadi)
   B1. Contratos Sector Público completo (2011-2026)  → XLSX anual
   B2. REVASCON agregado anual (2013-2018)            → CSV/XLSX
   B3. Contratos últimos 90 días (ventana móvil)      → XLSX
   B4. REVASCON por poder adjudicador y año (2018-…)  → XLSX

 MÓDULO C — Portales municipales independientes (datos NO centralizados)
   C1. Bilbao — contratos adjudicados (2005-2026)     → CSV
   C2. Vitoria-Gasteiz — contratos (menores) formalizados → CSV/XLSX

 Notas:
   · Los módulos B1/B2/B3 son exports del mismo REVASCON → redundantes con A1
     pero se mantienen como backup y para series históricas pre-API.
   · C1/C2 son portales propios que publican datos que PUEDEN no estar en
     KontratazioA (especialmente contratos menores municipales).
   · Sesgo del superviviente (comun/historico.py): ninguna descarga machaca la
     anterior. Si un fichero que se vuelve a bajar ha cambiado, la versión
     previa pasa a <carpeta>/_historico/ y la consolidación acumula todas.
═══════════════════════════════════════════════════════════════════════════════
"""

import html
import re
import shutil
import sys
import requests
import time
import json
import logging
from collections import Counter
from pathlib import Path
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode, urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, guardar_version  # noqa: E402

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

# Rutas relativas al script (no al cwd): consolidacion_euskadi.py las busca ahí
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR / "datos_euskadi_contratacion_v4"
DIRS = {
    # Módulo A: API REST
    "api_contracts":     BASE_DIR / "A1_api_contratos",
    "api_notices":       BASE_DIR / "A2_api_anuncios",
    "api_authorities":   BASE_DIR / "A3_api_poderes",
    "api_companies":     BASE_DIR / "A4_api_empresas",
    # Módulo B: XLSX históricos
    "xlsx_anual":        BASE_DIR / "B1_xlsx_sector_publico_anual",
    "revascon_hist":     BASE_DIR / "B2_revascon_historico",
    "ultimos_90d":       BASE_DIR / "B3_ultimos_90_dias",
    "revascon_poder":    BASE_DIR / "B4_revascon_por_poder",
    # Módulo A completo: ventanas de fecha de /contracts y /contracting-notices
    "api_contracts_full": BASE_DIR / "A1_api_contratos_completo",
    "api_notices_full":   BASE_DIR / "A2_api_anuncios_completo",
    # Módulo C: Portales municipales
    "bilbao":            BASE_DIR / "C1_bilbao",
    "vitoria":           BASE_DIR / "C2_vitoria_gasteiz",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (investigacion-academica) contratacion-euskadi/4.0",
    "Accept": "application/json, */*",
}
TIMEOUT  = 120
DELAY    = 1.5   # segundos entre peticiones
RETRIES  = 3

YEAR_NOW = datetime.now().year
YEAR_MIN_GV     = 2011    # Primer año XLSX disponible
YEAR_MIN_BILBAO = 2005    # Bilbao publica desde 2005

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCRIPT_DIR / "descarga_euskadi_v4.log",
                            encoding="utf-8", delay=True),
    ],
)
log = logging.getLogger(__name__)

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
    if ext == ".json" and not content.strip()[:1] in (b"{", b"["):
        return False
    return True


def download(url: str, dest: Path, label: str = "",
             skip_retry_on_404: bool = True, refrescar: bool = False) -> bool:
    """
    Descarga un fichero con reintentos. 404 no se reintenta.
    refrescar=True vuelve a descargarlo aunque ya exista (ficheros que siguen
    cambiando: año en curso, históricos acumulados); si falla, se conserva
    el fichero anterior. Si la nueva descarga es distinta, la anterior no se
    machaca: pasa a <carpeta>/_historico/<nombre>__<AAAAMMDDTHHMMSSZ><ext>
    (comun.historico.guardar_version) y la consolidación acumula todas las
    versiones, así no se pierde lo que la administración retire o cambie.
    """
    if not refrescar and dest.exists() and dest.stat().st_size > 100:
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
                # Escritura atómica (un corte a medias no deja un fichero
                # truncado) y sin machacar la versión anterior si cambió
                estado = guardar_version(dest, contenido=r.content)
                size = len(r.content)
                stats["ok"] += 1
                stats["bytes"] += size
                log.info("  OK   %s  (%.1f KB, %s)", dest.name, size / 1024, estado)
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


def _es_pagina(data) -> bool:
    """¿Es una página de la API? {totalItems, totalPages, ..., items: [...]}. Una
    consulta sin resultados (p.ej. un mes sin contratos) llega sin 'items':
    {totalItems: 0, totalPages: 0, currentPage: 1, itemsOfPage: 0, _links}."""
    if not isinstance(data, dict):
        return False
    if isinstance(data.get("items"), list):
        return True
    return "items" not in data and data.get("totalItems") == 0 and data.get("totalPages") == 0


def _get_pagina(url: str, resource_name: str, page: int):
    """
    GET de una página de la API con RETRIES reintentos (5xx, timeout, JSON
    roto). Devuelve el dict de la página o None si falla en todos.
    """
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            data = r.json() if r.status_code == 200 else None
            if _es_pagina(data):
                return data
            log.warning("  %s: status %d / sin 'items' en page %d (intento %d/%d)",
                        resource_name, r.status_code, page, attempt, RETRIES)
        except Exception as e:
            log.warning("  ERR %s page %d (intento %d/%d): %s",
                        resource_name, page, attempt, RETRIES, e)
        if attempt < RETRIES:
            time.sleep(DELAY * attempt)
    return None


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
                            # Solo una página de la API ({..., items: [...]}),
                            # no cualquier JSON (errores, índices, swagger…)
                            if _es_pagina(data):
                                # Extraer el base_url funcional (sin paginación)
                                api_url = f"{base}{suffix}"
                                working[resource] = api_url
                                log.info("    ✓ %s → %s", resource, api_url)
                                break
                        # Aceptar si parece JSON aunque CT sea text
                        elif r.text.strip().startswith(("{", "[")):
                            data = r.json()
                            if _es_pagina(data):
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

    Cada ejecución vuelve a bajar todas las páginas (el catálogo cambia y las
    páginas se desplazan: reutilizar las de una ejecución anterior mezclaría
    instantáneas y nunca actualizaría los datos).
    """
    sep = "&" if "?" in api_url else "?"

    # ── Página 1: descubrir totalItems y totalPages ─────────
    first_url = f"{api_url}{sep}currentPage=1"
    data = _get_pagina(first_url, resource_name, 1)
    if data is None:
        log.error("  ERR %s: no se pudo leer página 1", resource_name)
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
    dest.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    stats["ok"] += 1
    stats["bytes"] += dest.stat().st_size

    # ── Páginas 2..N ────────────────────────────────────────
    errors_consec = 0
    completo = True               # sin páginas perdidas ni abortos
    ultima = pages_to_download    # última página válida del catálogo
    for page in range(2, pages_to_download + 1):
        dest = dest_dir / f"{prefix}_p{page:05d}.json"

        url = f"{api_url}{sep}currentPage={page}"
        page_data = _get_pagina(url, resource_name, page)
        if page_data is None:
            log.error("  %s: page %d perdida tras %d intentos", resource_name, page, RETRIES)
            stats["fail"] += 1
            completo = False
            errors_consec += 1
            if errors_consec >= 5:
                log.error("  %s: 5 errores consecutivos — abortando.", resource_name)
                break
            time.sleep(delay * 2)
            continue

        # Si la API ignora currentPage devuelve siempre la página 1: guardar
        # sus copias haría pasar 10 registros por una muestra de miles
        if page_data.get("currentPage", page) != page:
            log.error("  %s: se pidió la página %d y la API devolvió la %s "
                      "(ignora currentPage) — abortando.",
                      resource_name, page, page_data.get("currentPage"))
            stats["fail"] += 1
            completo = False
            break

        items = page_data.get("items", [])
        if not items:
            log.warning("  %s: página %d vacía (totalPages=%d) — fin.",
                        resource_name, page, total_pages)
            ultima = page - 1
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

        time.sleep(delay)

    # Páginas de ejecuciones anteriores que ya no existen (el catálogo ha
    # encogido): la consolidación las mezclaría con las actuales
    if completo:
        for f in dest_dir.glob(f"{prefix}_p*.json"):
            n = f.stem.rsplit("_p", 1)[-1]
            if n.isdigit() and int(n) > ultima:
                f.unlink()



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
        log.info("  ℹ Muestra/sonda con ?currentPage=N a secas (hasta %d registros);",
                 API_SAMPLE_PAGES * 10)
        log.info("    la descarga completa es dl_A_api_completa (ventanas de fecha).")
        _paginate_api(
            api_url=api_urls[resource],
            resource_name=name,
            dest_dir=DIRS[dir_key],
            prefix=prefix,
            max_pages=API_SAMPLE_PAGES,
            delay=0.3,        # delay corto para la muestra
        )


# ═══════════════════════════════════════════════════════════════
# MÓDULO A (COMPLETO) — /contracts y /contracting-notices POR VENTANAS
# ═══════════════════════════════════════════════════════════════
#
# CONFIRMADO con la descarga real de 2026-02 (páginas de A1/A2):
#   · /contracts: totalItems=655.518; cada item trae id (texto, p.ej.
#     "G-042$24PYD1244_00001"), awardDate "AAAA-MM-DD", awardAmount,
#     awardAmountWithoutVAT, CIF, socialReason, CPV, minorContract,
#     contractType/ProcedureType/ProcedureStatus y _links.
#   · /contracting-notices: totalItems=656.503; id numérico, first/
#     lastPublicationDate, budgetWithoutVAT, sara, numberBidders…
#   · Sin más parámetros que ?currentPage=N la API devuelve siempre la página 1
#     (A3/A4 sí paginan) y el orden por defecto es awardDate DESC, con fechas
#     erróneas como 2424-10-04 o 2219-03-25 al principio.
#
# A VERIFICAR EN VIVO (portal bloqueado desde el entorno de desarrollo; los
# parámetros son los que usa código de terceros de 2025-26):
#   · que con itemsOfPage/orderBy/orderType/lang la API respete currentPage
#     (si no, se aborta con un mensaje claro: no se guardan copias de la pág. 1);
#   · que los filtros award-date.gt/.lt y publication-date.gt/.lt acepten
#     "AAAA-MM-DD" (si se ignoran, se aborta; si el formato no casa, las
#     ventanas salen vacías y el resumen avisa de los registros que faltan);
#   · si gt/lt son estrictos (se piden gt=día anterior y lt=día siguiente, así
#     no hay huecos en ningún caso; los solapes se descartan al consolidar);
#   · el máximo de registros paginables por consulta (API_MAX_ITEMS_VENTANA):
#     si una ventana lo supera se parte en dos; si la paginación se corta antes
#     de totalItems también, y un solo día se completa en orden ASC + DESC.
#   · qué fecha filtra publication-date en los anuncios (primera o última).
# ═══════════════════════════════════════════════════════════════

API_BASE = "https://api.euskadi.eus/procurements"
API_ITEMS_POR_PAGINA = 50
API_MAX_ITEMS_VENTANA = 10_000
API_ANIO_MIN = 2000          # antes: una ventana "anteriores" (que se parte si hace falta)
API_MESES_REFRESCO = 2       # últimos meses que se vuelven a bajar siempre
API_DELAY = 0.3
API_IDIOMA = "SPANISH"

API_COMPLETA = {
    "contracts": {
        "nombre": "A1_Contratos_completo", "dir": "api_contracts_full",
        "ruta": "/contracts", "filtro": "award-date", "orden": "awardDate",
        "campos_fecha": ("awardDate",),
    },
    "notices": {
        "nombre": "A2_Anuncios_completo", "dir": "api_notices_full",
        "ruta": "/contracting-notices", "filtro": "publication-date",
        "orden": "lastPublicationDate",
        "campos_fecha": ("firstPublicationDate", "lastPublicationDate"),
    },
}


class ApiNoPagina(RuntimeError):
    """La API no pagina o no filtra como se espera: seguir solo guardaría basura."""


def _url_api(api_url: str, cfg: dict, pagina: int, orden: str = "ASC",
             desde: date = None, hasta: date = None) -> str:
    """URL de una página. [desde, hasta] (días incluidos) se pide como
    gt=desde-1 y lt=hasta+1; date.min / date.max = sin ese límite."""
    params = {"currentPage": pagina, "itemsOfPage": API_ITEMS_POR_PAGINA,
              "orderBy": cfg["orden"], "orderType": orden, "lang": API_IDIOMA}
    if desde is not None and desde > date.min:
        params[cfg["filtro"] + ".gt"] = (desde - timedelta(days=1)).isoformat()
    if hasta is not None and hasta < date.max:
        params[cfg["filtro"] + ".lt"] = (hasta + timedelta(days=1)).isoformat()
    sep = "&" if "?" in api_url else "?"
    return f"{api_url}{sep}{urlencode(params)}"


def _get_pagina_api(url: str, nombre: str, pagina: int):
    """Como _get_pagina, pero devuelve (dict, bytes tal como llegan) o (None, None)."""
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            data = r.json() if r.status_code == 200 else None
            if _es_pagina(data):
                return data, r.content
            log.warning("  %s: status %d / sin 'items' en page %d (intento %d/%d)",
                        nombre, r.status_code, pagina, attempt, RETRIES)
        except Exception as e:
            log.warning("  ERR %s page %d (intento %d/%d): %s",
                        nombre, pagina, attempt, RETRIES, e)
        if attempt < RETRIES:
            time.sleep(DELAY * attempt)
    return None, None


def _clave_item(item):
    """Identificador de un item: su 'id' o, si no lo trae, el item entero."""
    if isinstance(item, dict) and item.get("id") is not None:
        return item["id"]
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def _fuera_de_rango(item, cfg, desde, hasta) -> bool:
    """True si ninguna fecha del item cae en [desde-1, hasta+1] (solo estadística)."""
    fechas = []
    for campo in cfg["campos_fecha"]:
        try:
            fechas.append(date.fromisoformat(str(item.get(campo))[:10]))
        except (TypeError, ValueError):
            pass
    if not fechas:
        return False
    lo = desde - timedelta(days=1) if desde > date.min else date.min
    hi = hasta + timedelta(days=1) if hasta < date.max else date.max
    return not any(lo <= f <= hi for f in fechas)


def _pasada(api_url, cfg, desde, hasta, orden, carpeta, vistos, primera=None,
            parar=None, max_paginas=None) -> dict:
    """
    Pide las páginas 1..N de [desde, hasta] (None, None = sin filtro) en `orden`,
    guarda cada respuesta tal cual (<desde>_<hasta>_<orden>_pNNNNN.json) y añade
    sus ids a `vistos`. Para al reunir totalItems ids, al acabar las páginas, con
    una página vacía o que solo repite ids, al llegar a max_paginas o si
    parar(vistos). Lanza ApiNoPagina (sin guardar esa página) si la API devuelve
    otra página que la pedida o una página idéntica a otra ya servida en la pasada.
    info["repetidos"]: {id: veces} de los ids servidos más de una vez en la pasada.
    """
    nombre = cfg["nombre"]
    rango = f"{desde.isoformat()}_{hasta.isoformat()}" if desde else "sin_filtro"
    etiqueta = f"{rango}_{orden.lower()}"
    info = {"desde": desde.isoformat() if desde else None,
            "hasta": hasta.isoformat() if hasta else None, "orden": orden,
            "total_items": None, "paginas": 0, "items": 0, "ids_nuevos": 0,
            "fuera_de_rango": 0, "fin": None, "repetidos": {}}
    pagina, respuesta = 1, primera
    firmas = set()          # ids de cada página ya servida en esta pasada
    servidos = Counter()    # veces que la pasada sirve cada id
    while True:
        if respuesta is None:
            time.sleep(API_DELAY)
            respuesta = _get_pagina_api(_url_api(api_url, cfg, pagina, orden, desde, hasta),
                                        nombre, pagina)
        data, contenido = respuesta
        if data is None:
            stats["fail"] += 1
            info["fin"] = f"página {pagina} fallida"
            break
        if data.get("currentPage", pagina) != pagina:
            raise ApiNoPagina(
                f"{nombre}: se pidió la página {pagina} de {etiqueta} y la API devolvió "
                f"la {data.get('currentPage')} (ignora currentPage) — abortando sin "
                f"guardar la ventana; revisar los parámetros de paginación.")
        if pagina == 1:
            info["total_items"] = int(data.get("totalItems") or 0)
        items = data.get("items") or []
        if not items:
            info["fin"] = "página vacía"
            break
        firma = tuple(_clave_item(it) for it in items)
        if firma in firmas:
            raise ApiNoPagina(
                f"{nombre}: la página {pagina} de {etiqueta} repite una página anterior "
                f"(la API ignora currentPage) — abortando sin guardar la ventana; "
                f"revisar los parámetros de paginación.")
        firmas.add(firma)
        (carpeta / f"{etiqueta}_p{pagina:05d}.json").write_bytes(contenido)
        stats["ok"] += 1
        stats["bytes"] += len(contenido)
        nuevos = 0
        for it in items:
            k = _clave_item(it)
            servidos[k] += 1
            if k not in vistos:
                vistos.add(k)
                nuevos += 1
            if desde is not None and _fuera_de_rango(it, cfg, desde, hasta):
                info["fuera_de_rango"] += 1
        info["paginas"] += 1
        info["items"] += len(items)
        info["ids_nuevos"] += nuevos
        if len(vistos) >= info["total_items"]:
            info["fin"] = "completa"
            break
        if parar is not None and parar(vistos):
            info["fin"] = "objetivo"
            break
        if nuevos == 0:
            info["fin"] = f"página {pagina} repetida"
            break
        if data.get("totalPages") is not None and pagina >= int(data["totalPages"] or 0):
            info["fin"] = "última página"
            break
        if max_paginas and pagina >= max_paginas:
            info["fin"] = "máximo de páginas"
            break
        pagina, respuesta = pagina + 1, None
    info["repetidos"] = {k: n for k, n in servidos.items() if n > 1}
    return info


def _bajar_rango(api_url, cfg, desde, hasta, carpeta, vistos, trozos, primera=None,
                 repetidos=None):
    """
    Descarga [desde, hasta] en `carpeta`. Si totalItems supera
    API_MAX_ITEMS_VENTANA, o la paginación no llega a reunir totalItems ids
    (ni en orden ASC ni completando en DESC), se parte en dos mitades hasta
    llegar a un día. Añade los ids a `vistos` y el detalle a `trozos`.

    La API sirve algunas filas dos veces, idénticas (p.ej. 2020-01-08: 293 filas,
    291 ids). Si la pasada ASC sirve totalItems filas pero menos ids, una pasada
    DESC completa lo decide: con los mismos ids y las mismas repeticiones son
    filas repetidas en origen (van a `repetidos`, {id: copias}, y el trozo está
    completo); si trae otros ids, la paginación es inestable y se suman.
    """
    if repetidos is None:
        repetidos = {}
    if primera is None:
        time.sleep(API_DELAY)
        primera = _get_pagina_api(_url_api(api_url, cfg, 1, "ASC", desde, hasta), cfg["nombre"], 1)
    if primera[0] is None:
        stats["fail"] += 1
        trozos.append({"desde": desde.isoformat(), "hasta": hasta.isoformat(),
                       "completo": False, "fin": "página 1 fallida"})
        return
    total = int(primera[0].get("totalItems") or 0)

    def partir(motivo):
        mitad = desde + (hasta - desde) // 2
        log.info("    %s %s…%s (%d registros, %s): se parte en dos", cfg["nombre"],
                 desde, hasta, total, motivo)
        trozos.append({"desde": desde.isoformat(), "hasta": hasta.isoformat(),
                       "total_items": total, "partido": motivo})
        _bajar_rango(api_url, cfg, desde, mitad, carpeta, vistos, trozos, repetidos=repetidos)
        _bajar_rango(api_url, cfg, mitad + timedelta(days=1), hasta, carpeta, vistos, trozos,
                     repetidos=repetidos)

    if total > API_MAX_ITEMS_VENTANA and hasta > desde:
        partir(f"más de {API_MAX_ITEMS_VENTANA} por consulta")
        return
    propios = set()
    asc = _pasada(api_url, cfg, desde, hasta, "ASC", carpeta, propios, primera)
    pasadas, copias = [asc], {}
    if len(propios) < total and asc["items"] == total and asc["repetidos"]:
        en_desc = set()
        desc = _pasada(api_url, cfg, desde, hasta, "DESC", carpeta, en_desc)
        pasadas.append(desc)
        if desc["items"] == total and en_desc == propios and desc["repetidos"] == asc["repetidos"]:
            copias = asc["repetidos"]
        propios |= en_desc
    elif len(propios) < total:
        pasadas.append(_pasada(api_url, cfg, desde, hasta, "DESC", carpeta, propios))
    vistos |= propios
    for k, n in copias.items():
        repetidos[k] = max(n, repetidos.get(k, 0))
    filas = len(propios) + sum(n - 1 for n in copias.values())
    trozo = {"desde": desde.isoformat(), "hasta": hasta.isoformat(),
             "total_items": total, "ids_unicos": len(propios),
             "completo": filas == total, "pasadas": pasadas}
    if copias:
        trozo["repetidos_api"] = copias
    trozos.append(trozo)
    if filas < total and hasta > desde:
        partir(f"paginación cortada en {len(propios)}")


def _ventanas_api(hoy: date):
    """[(clave, desde, hasta)]: anteriores a API_ANIO_MIN, un mes por ventana
    hasta el mes de `hoy` y posteriores (fechas futuras o erróneas: 2424-10-04)."""
    ventanas = [("anteriores", date.min, date(API_ANIO_MIN - 1, 12, 31))]
    ini = date(API_ANIO_MIN, 1, 1)
    while (ini.year, ini.month) <= (hoy.year, hoy.month):
        sig = date(ini.year + (ini.month == 12), ini.month % 12 + 1, 1)
        ventanas.append((f"{ini.year:04d}-{ini.month:02d}", ini, sig - timedelta(days=1)))
        ini = sig
    ventanas.append(("posteriores", ini, date.max))
    return ventanas


def _leer_manifiesto(carpeta: Path):
    try:
        return json.loads((carpeta / "_ventana.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _sello(momento: datetime) -> str:
    return momento.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _publicar_ventana(tmp: Path, final: Path):
    """Pone la descarga `tmp` como versión actual de la ventana; la anterior (si
    la hay) no se borra: pasa a <dir>/_historico/<clave>__<AAAAMMDDTHHMMSSZ>/."""
    if final.exists():
        previo = _leer_manifiesto(final) or {}
        try:
            momento = datetime.fromisoformat(previo["descargado"])
        except (KeyError, TypeError, ValueError):
            momento = datetime.fromtimestamp(final.stat().st_mtime, timezone.utc)
        archivo = final.parent / HISTORICO / f"{final.name}__{_sello(momento)}"
        archivo.parent.mkdir(exist_ok=True)
        n = 1
        while archivo.exists():
            archivo = archivo.with_name(f"{final.name}__{_sello(momento)}_{n}")
            n += 1
        final.rename(archivo)
    tmp.rename(final)


def _mismas_paginas(a: Path, b: Path) -> bool:
    """¿Las dos descargas de una ventana tienen las mismas páginas, byte a byte?"""
    pa = sorted(f.name for f in a.glob("*_p[0-9]*.json"))
    pb = sorted(f.name for f in b.glob("*_p[0-9]*.json"))
    return pa == pb and all((a / n).read_bytes() == (b / n).read_bytes() for n in pa)


def _filas(ids, repetidos) -> int:
    """Filas que suman `ids` distintos con las copias de más de `repetidos` ({id: copias})."""
    return len(ids) + sum(n - 1 for n in repetidos.values())


def _comprobar_filtro(cfg, clave, total, total_global):
    """Una ventana acotada con tantos registros como la API sin filtro = filtro ignorado."""
    if total_global > API_ITEMS_POR_PAGINA and total >= total_global:
        raise ApiNoPagina(
            f"{cfg['nombre']}: la ventana {clave} devuelve {total} registros, los mismos "
            f"que sin filtro: la API ignora {cfg['filtro']}.gt/.lt — abortando sin "
            f"guardar la ventana; revisar el nombre y formato del filtro.")


def _ventana_api(api_url, cfg, d, clave, desde, hasta, total_global, refrescar):
    """
    Descarga una ventana (con reanudación) y devuelve sus ids.

    - Ventana ya completa y no a refrescar: solo se pide su página 1 y, si
      totalItems no ha cambiado, se conserva sin volver a bajarla.
    - Si hay que bajarla, se escribe en <clave>.part/ y se publica al acabar;
      la versión anterior pasa a _historico/ (la consolidación acumula todas:
      lo que la administración retire se conserva marcado).
    - Si la nueva descarga no reúne totalItems ids se repite una vez (y se queda
      el intento con más ids); si sigue incompleta se guarda igual (son datos
      reales) con completo=False y la siguiente ejecución la vuelve a intentar.
      Las filas que la API sirve repetidas (ver _bajar_rango) cuentan:
      completo = ids + copias de más == totalItems; van al manifiesto en
      repetidos_api ({id: copias}).
    - Una descarga fallida (sin página 1) o vacía cuando antes había registros no
      se publica: se conserva la anterior. Una idéntica a la anterior tampoco
      (no se llena _historico/ de copias).
    """
    nombre = cfg["nombre"]
    final = d / clave
    previo = _leer_manifiesto(final)
    primera = None
    if previo and previo.get("completo") and not refrescar:
        time.sleep(API_DELAY)
        primera = _get_pagina_api(_url_api(api_url, cfg, 1, "ASC", desde, hasta), nombre, 1)
        if primera[0] is None:
            log.warning("  %s %s: no se pudo comprobar; se conserva la descarga anterior",
                        nombre, clave)
            return set(previo.get("ids", []))
        total = int(primera[0].get("totalItems") or 0)
        _comprobar_filtro(cfg, clave, total, total_global)
        if total == previo.get("total_items"):
            stats["skip"] += 1
            return set(previo.get("ids", []))
        log.info("  %s %s: totalItems %s → %d, se vuelve a descargar (la anterior "
                 "se conserva en %s/)", nombre, clave, previo.get("total_items"), total, HISTORICO)

    partes = (d / f"{clave}.part", d / f"{clave}.reintento.part")
    mejor = None
    for intento, tmp in enumerate(partes, 1):
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        inicio = datetime.now(timezone.utc)
        if primera is None:
            time.sleep(API_DELAY)
            primera = _get_pagina_api(_url_api(api_url, cfg, 1, "ASC", desde, hasta), nombre, 1)
        total = int(primera[0].get("totalItems") or 0) if primera[0] is not None else None
        vistos, trozos, repetidos = set(), [], {}
        try:
            if total is not None:
                _comprobar_filtro(cfg, clave, total, total_global)
            _bajar_rango(api_url, cfg, desde, hasta, tmp, vistos, trozos, primera, repetidos)
        except ApiNoPagina:
            for parte in partes:
                shutil.rmtree(parte, ignore_errors=True)
            raise
        # El reintento no sustituye a un intento que reunió más ids
        nota = (total is not None, len(vistos))
        if mejor is None or nota > (mejor[2] is not None, len(mejor[3])):
            if mejor is not None:
                shutil.rmtree(mejor[0], ignore_errors=True)
            mejor = (tmp, inicio, total, vistos, trozos, repetidos)
        else:
            shutil.rmtree(tmp, ignore_errors=True)
        if (total is not None and _filas(vistos, repetidos) == total) or intento == 2:
            break
        log.warning("  %s %s: %d ids únicos de %s — se repite la ventana",
                    nombre, clave, len(vistos), total)
        primera = None
    tmp, inicio, total, vistos, trozos, repetidos = mejor
    completo = total is not None and _filas(vistos, repetidos) == total
    ids_previos = set(previo.get("ids", [])) if previo else set()
    if total is None or (not vistos and ids_previos):
        shutil.rmtree(tmp, ignore_errors=True)
        stats["fail"] += 1
        log.error("  %s %s: %s — no se publica; se conserva la descarga anterior",
                  nombre, clave, "sin respuesta de la API" if total is None
                  else f"0 registros (antes {len(ids_previos)})")
        return ids_previos
    if (previo and previo.get("total_items") == total
            and bool(previo.get("completo")) == completo and _mismas_paginas(tmp, final)):
        shutil.rmtree(tmp, ignore_errors=True)
        stats["skip"] += 1
        log.info("  %s %s: sin cambios (%d ids)", nombre, clave, len(vistos))
        return vistos

    manifiesto = {
        "clave": clave, "desde": desde.isoformat(), "hasta": hasta.isoformat(),
        "api_url": api_url, "filtro": cfg["filtro"], "orden": cfg["orden"],
        "items_por_pagina": API_ITEMS_POR_PAGINA, "descargado": inicio.isoformat(),
        "total_items": total, "ids_unicos": len(vistos), "completo": completo,
        "trozos": trozos, "ids": sorted(vistos, key=str),
    }
    if repetidos:
        manifiesto["repetidos_api"] = repetidos
    (tmp / "_ventana.json").write_text(json.dumps(manifiesto, ensure_ascii=False),
                                       encoding="utf-8")
    _publicar_ventana(tmp, final)
    nivel = logging.INFO if completo else logging.ERROR
    log.log(nivel, "  %s %s: %d ids únicos de %s%s%s", nombre, clave, len(vistos), total,
            f" (+{_filas((), repetidos)} filas que la API sirve repetidas)" if repetidos else "",
            "" if completo else " — INCOMPLETA (se reintentará en la próxima ejecución)")
    if not completo:
        stats["fail"] += 1
    return vistos


def _resto_sin_ventana(api_url, cfg, d, ids_ventanas, total_global, repetidos=0) -> int:
    """
    Registros que no caen en ninguna ventana (sin fecha: los filtros no los
    devuelven). Se buscan en la API sin filtro, en orden DESC (los nulos suelen ir
    al final en ASC) y ASC, hasta API_MAX_ITEMS_VENTANA registros por orden.
    Devuelve cuántos se encontraron. Las páginas guardadas repiten registros de
    otras ventanas: son artefactos de la descarga y se descartan al consolidar.
    `repetidos`: copias de más que la API sirve en las ventanas (no faltan).
    """
    faltan = total_global - repetidos - len(ids_ventanas)
    if faltan <= 0:
        return 0
    log.warning("  %s: las ventanas reúnen %d ids de %d — buscando %d sin fecha…",
                cfg["nombre"], len(ids_ventanas), total_global, faltan)
    tmp = d / "sin_ventana.part"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    inicio = datetime.now(timezone.utc)
    vistos = set()

    def encontrados(v):
        return len(v - ids_ventanas)

    pasadas = []
    try:
        for orden in ("DESC", "ASC"):
            pasadas.append(_pasada(api_url, cfg, None, None, orden, tmp, vistos,
                                   parar=lambda v: encontrados(v) >= faltan,
                                   max_paginas=max(1, API_MAX_ITEMS_VENTANA // API_ITEMS_POR_PAGINA)))
            if encontrados(vistos) >= faltan:
                break
    except ApiNoPagina:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    nuevos = vistos - ids_ventanas
    manifiesto = {"clave": "sin_ventana", "descargado": inicio.isoformat(), "parcial": True,
                  "total_items": total_global, "faltaban": faltan, "completo": len(nuevos) >= faltan,
                  "ids_unicos": len(nuevos), "trozos": [{"pasadas": pasadas}],
                  "ids": sorted(nuevos, key=str)}
    if not nuevos or _mismas_paginas(tmp, d / "sin_ventana"):
        shutil.rmtree(tmp, ignore_errors=True)   # nada nuevo: se conserva la anterior
        return len(nuevos)
    (tmp / "_ventana.json").write_text(json.dumps(manifiesto, ensure_ascii=False), encoding="utf-8")
    _publicar_ventana(tmp, d / "sin_ventana")
    return len(nuevos)


def _descargar_api_completa(api_url: str, cfg: dict, hoy: date = None):
    """Descarga completa de un endpoint por ventanas de fecha (ver arriba)."""
    nombre = cfg["nombre"]
    d = DIRS[cfg["dir"]]
    d.mkdir(parents=True, exist_ok=True)
    hoy = hoy or date.today()
    log.info("=" * 60)
    log.info("A. API REST — %s (descarga completa por ventanas de fecha)", nombre)
    log.info("=" * 60)
    estado = {"recurso": nombre, "api_url": api_url, "inicio": datetime.now(timezone.utc).isoformat()}

    def escribir_estado():
        estado["fin"] = datetime.now(timezone.utc).isoformat()
        (d / "_estado.json").write_text(json.dumps(estado, ensure_ascii=False, indent=2),
                                        encoding="utf-8")

    data, _ = _get_pagina_api(_url_api(api_url, cfg, 1, "DESC"), nombre, 1)
    if data is None:
        log.error("  %s: no se pudo leer el total sin filtro — saltando", nombre)
        stats["fail"] += 1
        estado["abortado"] = "sin respuesta de la API"
        escribir_estado()
        return
    total_global = int(data.get("totalItems") or 0)
    estado["total_api"] = total_global
    log.info("  %s: %d registros en la API", nombre, total_global)

    ventanas = _ventanas_api(hoy)
    refrescar = {"posteriores"} | {c for c, _, _ in ventanas[-1 - API_MESES_REFRESCO:-1]}
    ids = set()
    incompletas = []
    repetidos = {}     # {id: copias} de todas las ventanas (se solapan en un día)
    try:
        for clave, desde, hasta in ventanas:
            ids |= _ventana_api(api_url, cfg, d, clave, desde, hasta, total_global,
                                refrescar=clave in refrescar)
            man = _leer_manifiesto(d / clave) or {}
            if not man.get("completo"):
                incompletas.append(clave)
            for k, n in (man.get("repetidos_api") or {}).items():
                repetidos[k] = max(n, repetidos.get(k, 0))
        ids_ventanas = set(ids)
        estado["repetidos_api"] = _filas((), repetidos)
        estado["sin_ventana"] = _resto_sin_ventana(api_url, cfg, d, ids_ventanas, total_global,
                                                   estado["repetidos_api"])
    except ApiNoPagina as e:
        log.error("  %s", e)
        stats["fail"] += 1
        estado["abortado"] = str(e)
        escribir_estado()
        return
    estado["ids_en_ventanas"] = len(ids_ventanas)
    estado["faltan"] = max(0, total_global - estado["repetidos_api"] - len(ids_ventanas)
                           - estado["sin_ventana"])
    estado["ventanas_incompletas"] = incompletas
    escribir_estado()
    if estado["faltan"] or incompletas:
        log.error("  %s: faltan %d de %d registros; ventanas incompletas: %s",
                  nombre, estado["faltan"], total_global, ", ".join(incompletas) or "ninguna")
    else:
        log.info("  %s: completo — %d registros", nombre, total_global)


def dl_A_api_completa(api_urls: dict, recursos=("contracts", "notices")):
    """
    A1c/A2c. /contracts y /contracting-notices COMPLETOS por ventanas de fecha
    (importes, adjudicatario, CIF, presupuesto…: lo que no tiene B1).
    Las respuestas se guardan tal cual en A1_api_contratos_completo/<ventana>/
    y A2_api_anuncios_completo/<ventana>/ con un _ventana.json por ventana
    (totalItems, ids únicos, trozos) y un _estado.json con el resumen.
    Reanudable: las ventanas completas no se vuelven a bajar salvo que cambie
    su totalItems (o sean de los últimos API_MESES_REFRESCO meses).
    """
    for recurso in recursos:
        cfg = API_COMPLETA[recurso]
        _descargar_api_completa(api_urls.get(recurso) or API_BASE + cfg["ruta"], cfg)


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
        # El año en curso y el anterior siguen creciendo: se vuelven a bajar
        download(url, dest, f"XLSX-{year}", refrescar=year >= YEAR_NOW - 1)
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


# B3: la URL original (contrataciones_ultimos_dias) no genera nada; la de Open
# Data es ultimas_contrataciones_admin (a verificar en vivo). Se prueba en orden.
B3_URLS = [
    "https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
    "ultimas_contrataciones_admin/opendata/contratos.xlsx",
    "https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
    "contrataciones_ultimos_dias/opendata/contratos.xlsx",
]


def dl_B3_ultimos_90d():
    """
    B3. Snapshot de contratos de los últimos 90 días.
    Ventana móvil con datos recientes de toda la CAE. Cada día es un fichero
    distinto (ultimos_90d_AAAAMMDD.xlsx): las instantáneas no se sobrescriben.
    """
    log.info("=" * 60)
    log.info("B3. CONTRATOS ÚLTIMOS 90 DÍAS (snapshot)")
    log.info("=" * 60)
    d = DIRS["ultimos_90d"]
    hoy = datetime.now().strftime("%Y%m%d")
    for i, url in enumerate(B3_URLS):
        if download(url, d / f"ultimos_90d_{hoy}.xlsx", "90-días" + (" (URL antigua)" if i else "")):
            break


# ─────────────────────────────────────────────────────────────
# B4. REVASCON POR PODER ADJUDICADOR Y AÑO (2018-…)
# ─────────────────────────────────────────────────────────────
# Desde 2019 el Registro de Contratos se publica como un dataset por poder
# adjudicador y año ("Registro de contratos de <poder> del <AAAA>"), p.ej.
# contratos_poder86_2020. Los IDs de poder NO son los de la API (UPV/EHU es
# 16317 aquí y 37 en /contracting-authorities): se descubren buscando
# "contratos_poder<ID>_<AAAA>" en las páginas del catálogo y se guardan en
# B4_revascon_por_poder/_poderes_descubiertos.json (acumulativo: se puede
# añadir IDs a mano).
#
# A VERIFICAR EN VIVO: la URL de búsqueda del catálogo (se prueban las de
# REVASCON_CATALOGO; basta con que la página contenga enlaces a los datasets),
# que el XLSX esté en www.euskadi.eus con el nombre doc_contratos_poder… (si no,
# se busca el enlace en la ficha index.shtml) y el año de inicio.
REVASCON_ANIO_MIN = 2018
REVASCON_PODER_XLSX = ("https://www.euskadi.eus/contenidos/ds_contrataciones/"
                       "contratos_poder{id}_{anio}/es_contracc/adjuntos/"
                       "doc_contratos_poder{id}_{anio}.xlsx")
REVASCON_PODER_FICHA = ("https://opendata.euskadi.eus/webopd00-dataset/es/contenidos/"
                        "ds_contrataciones/contratos_poder{id}_{anio}/es_contracc/index.shtml")
REVASCON_CATALOGO = [
    # (nombre, URL con {pagina}, primera página)
    ("opendata_euskadi",
     "https://opendata.euskadi.eus/catalogo-datos/?r01kQry=tC:euskadi;tF:opendata;"
     "tT:ds_contrataciones;m:documentName.LIKE.Registro%20de%20contratos;"
     "p:Inter;pp:r01PageSize.100,r01PageNum.{pagina}", 1),
    ("datos_gob_es",
     "https://datos.gob.es/apidata/catalog/dataset/title/Registro%20de%20contratos.json"
     "?_pageSize=50&_page={pagina}", 0),
]
REVASCON_CATALOGO_MAX_PAGINAS = 200
# Poderes conocidos (se prueban aunque el catálogo no responda)
REVASCON_PODERES_SEMILLA = (86, 16317)
_RE_PODER = re.compile(r"contratos_poder(\d+)_((?:19|20)\d{2})")
_RE_ENLACE_DATOS = re.compile(r"""href\s*=\s*["']([^"'<>]+?\.(csv|xlsx|xls|json)(?:\?[^"'<>]*)?)["']""",
                              re.IGNORECASE)


def _texto(ruta: Path) -> str:
    return ruta.read_bytes().decode("utf-8", errors="replace")


def _enlaces_datos(pagina_html: str, base_url: str):
    """URLs absolutas de ficheros de datos (csv/xlsx/xls/json) enlazados en una página."""
    vistos, out = set(), []
    for href, ext in _RE_ENLACE_DATOS.findall(pagina_html):
        url = urljoin(base_url, html.unescape(href))
        if url not in vistos:
            vistos.add(url)
            out.append((url, ext.lower()))
    return out


def _descubrir_poderes_revascon(d: Path) -> dict:
    """{id: {"anios": set, "fuentes": set}} con lo guardado + lo que aparezca en el catálogo."""
    registro = d / "_poderes_descubiertos.json"
    poderes = {}
    try:
        previo = json.loads(registro.read_text(encoding="utf-8"))
        for pid, info in previo.get("poderes", {}).items():
            poderes[int(pid)] = {"anios": set(info.get("anios", [])),
                                 "fuentes": set(info.get("fuentes", []))}
    except (OSError, ValueError, AttributeError):
        pass
    for pid in REVASCON_PODERES_SEMILLA:
        poderes.setdefault(pid, {"anios": set(), "fuentes": set()})["fuentes"].add("semilla")

    cat = d / "_catalogo"
    cat.mkdir(parents=True, exist_ok=True)
    for nombre, plantilla, primera in REVASCON_CATALOGO:
        ext = ".json" if ".json" in plantilla else ".html"
        en_esta_busqueda = set()
        for pagina in range(primera, primera + REVASCON_CATALOGO_MAX_PAGINAS):
            dest = cat / f"{nombre}_p{pagina:03d}{ext}"
            if not download(plantilla.format(pagina=pagina), dest, f"catálogo {nombre} p{pagina}",
                            refrescar=True):
                break
            pares = {(int(p), int(a)) for p, a in _RE_PODER.findall(_texto(dest))}
            if not pares - en_esta_busqueda:   # vacía o repetida: fin de los resultados
                break
            en_esta_busqueda |= pares
            for pid, anio in pares:
                info = poderes.setdefault(pid, {"anios": set(), "fuentes": set()})
                info["anios"].add(anio)
                info["fuentes"].add(nombre)
            time.sleep(DELAY)

    registro.write_text(json.dumps({
        "actualizado": datetime.now(timezone.utc).isoformat(),
        "poderes": {str(pid): {"anios": sorted(v["anios"]), "fuentes": sorted(v["fuentes"])}
                    for pid, v in sorted(poderes.items())},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    log.info("  %d poderes adjudicadores (%d pares poder-año en el catálogo)",
             len(poderes), sum(len(v["anios"]) for v in poderes.values()))
    return poderes


def _revascon_desde_ficha(pid: int, anio: int, d: Path, refrescar: bool) -> bool:
    """Si el XLSX no está en la ruta esperada, busca el enlace en la ficha del dataset."""
    fichas = d / "_fichas"
    fichas.mkdir(parents=True, exist_ok=True)
    url_ficha = REVASCON_PODER_FICHA.format(id=pid, anio=anio)
    ficha = fichas / f"contratos_poder{pid}_{anio}.html"
    if not download(url_ficha, ficha, f"ficha poder{pid}-{anio}", refrescar=True):
        return False
    ok = False
    for url, ext in _enlaces_datos(_texto(ficha), url_ficha):
        if f"contratos_poder{pid}_{anio}" not in url and "adjuntos" not in url:
            continue
        base = re.sub(r"[^\w.-]", "_", url.rsplit("/", 1)[-1].split("?")[0])
        ok |= download(url, d / f"contratos_poder{pid}_{anio}__{base}",
                       f"REVASCON-poder{pid}-{anio} ({base})", refrescar=refrescar)
    return ok


def dl_B4_revascon_por_poder():
    """
    B4. REVASCON por poder adjudicador y año (XLSX), para cada poder descubierto y
    cada año desde REVASCON_ANIO_MIN (también los años que el catálogo no lista).
    El año en curso y el anterior se vuelven a bajar (versionados en _historico/).
    """
    log.info("=" * 60)
    log.info("B4. REVASCON POR PODER ADJUDICADOR (XLSX, %d-%d)", REVASCON_ANIO_MIN, YEAR_NOW)
    log.info("=" * 60)
    d = DIRS["revascon_poder"]
    d.mkdir(parents=True, exist_ok=True)
    poderes = _descubrir_poderes_revascon(d)
    for pid in sorted(poderes):
        for anio in range(REVASCON_ANIO_MIN, YEAR_NOW + 1):
            refrescar = anio >= YEAR_NOW - 1
            dest = d / f"contratos_poder{pid}_{anio}.xlsx"
            ok = download(REVASCON_PODER_XLSX.format(id=pid, anio=anio), dest,
                          f"REVASCON-poder{pid}-{anio}", refrescar=refrescar)
            if not ok and anio in poderes[pid]["anios"]:
                _revascon_desde_ficha(pid, anio, d, refrescar)
            time.sleep(DELAY)


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
        # El año en curso y el anterior siguen creciendo: se vuelven a bajar
        download(url, dest, f"Bilbao-{year}", refrescar=year >= YEAR_NOW - 1)
        time.sleep(DELAY)

    # Descarga por tipo de contrato (histórico completo, siempre actualizado)
    for tipo in ("obras", "servicios", "suministros"):
        url = f"{base}?formato=csv&tipoContrato={tipo}&idioma=es"
        dest = d / f"bilbao_tipo_{tipo}.csv"
        download(url, dest, f"Bilbao-tipo-{tipo}", refrescar=True)
        time.sleep(DELAY)

    # Licitaciones abiertas (snapshot)
    hoy = datetime.now().strftime("%Y%m%d")
    url = f"{base}?formato=csv&abiertas=true&idioma=es"
    dest = d / f"bilbao_abiertas_{hoy}.csv"
    download(url, dest, "Bilbao-abiertas")

    # Sin filtros: 165 registros solo salían en abiertas=true (ni por año ni por
    # tipo), y las instantáneas "abiertas" antiguas no se consolidan. A verificar
    # en vivo que sin anio/tipoContrato/abiertas se sirva todo el histórico.
    download(f"{base}?formato=csv&idioma=es", d / "bilbao_sin_filtros.csv",
             "Bilbao-sin-filtros", refrescar=True)


# Datasets de Vitoria-Gasteiz en el catálogo de Open Data Euskadi. La URL de
# los ficheros no se conoce (a verificar en vivo): se toman los enlaces a
# CSV (o XLSX/XLS/JSON si no hay CSV) de la página del catálogo.
VITORIA_CATALOGO = {
    "contratos_formalizados":
        "https://opendata.euskadi.eus/catalogo/-/contratos-formalizados/",
    "contratos_menores_formalizados":
        "https://opendata.euskadi.eus/catalogo/-/contratos-menores-formalizados/",
}


def dl_C2_vitoria():
    """
    C2. Vitoria-Gasteiz — Contratos formalizados y contratos menores formalizados.
    Se descargan los ficheros enlazados en la página de cada dataset del
    catálogo (vitoria_<dataset>__<fichero>) y, además, la URL antigua de menores
    (vitoria_menores.csv, que no genera nada: se mantiene por si vuelve).
    Todos se vuelven a bajar en cada ejecución, versionados en _historico/.
    """
    log.info("=" * 60)
    log.info("C2. VITORIA-GASTEIZ — CONTRATOS (MENORES) FORMALIZADOS")
    log.info("=" * 60)
    d = DIRS["vitoria"]
    base = ("https://opendata.euskadi.eus/contenidos/ds_contrataciones/"
            "contratos_menores_formalizados/opendata/contratos_menores")

    download(f"{base}.csv", d / "vitoria_menores.csv", "Vitoria-menores-CSV",
             refrescar=True)   # fichero único que se va actualizando

    cat = d / "_catalogo"
    cat.mkdir(parents=True, exist_ok=True)
    for clave, url_cat in VITORIA_CATALOGO.items():
        pagina = cat / f"{clave}.html"
        if not download(url_cat, pagina, f"Vitoria catálogo {clave}", refrescar=True):
            continue
        enlaces = _enlaces_datos(_texto(pagina), url_cat)
        for preferido in (("csv",), ("xlsx", "xls"), ("json",)):
            elegidos = [u for u, ext in enlaces if ext in preferido]
            if elegidos:
                break
        if not elegidos:
            log.warning("  Vitoria %s: la página del catálogo no enlaza ficheros de datos "
                        "(revisar en vivo)", clave)
        for url in elegidos:
            nombre = re.sub(r"[^\w.-]", "_", url.rsplit("/", 1)[-1].split("?")[0])
            download(url, d / f"vitoria_{clave}__{nombre}", f"Vitoria {clave} {nombre}",
                     refrescar=True)
            time.sleep(DELAY)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║  CONTRATACIÓN PÚBLICA DE EUSKADI — DESCARGA CENTRAL v4  ║")
    log.info("║  API-first · XLSX fallback · Portales municipales       ║")
    log.info("║  Fecha: %s                                  ║",
             datetime.now().strftime("%Y-%m-%d"))
    log.info("╚═══════════════════════════════════════════════════════════╝")

    setup_dirs()

    # ── FASE 0: Autodescubrimiento de la API ────────────────────
    log.info("=" * 60)
    log.info("FASE 0: DESCUBRIMIENTO API REST KONTRATAZIOA")
    log.info("=" * 60)
    api_urls = _probe_api()

    if api_urls:
        log.info("  Endpoints descubiertos: %d/4", len(api_urls))
        for k, v in api_urls.items():
            log.info("    · %s → %s", k, v)
    else:
        log.warning("  ⚠ Ningún endpoint API descubierto.")
        log.warning("    Se usarán exclusivamente los XLSX históricos.")

    # ── MÓDULO A: API REST (fuente principal) ───────────────────
    if api_urls:
        dl_A_api(api_urls)

    # ── MÓDULO B: XLSX históricos (backup + pre-API) ────────────
    dl_B1_xlsx_anual()
    dl_B2_revascon_historico()
    dl_B3_ultimos_90d()
    dl_B4_revascon_por_poder()

    # ── MÓDULO C: Portales municipales ──────────────────────────
    dl_C1_bilbao()
    dl_C2_vitoria()

    # ── MÓDULO A completo (lo más largo, reanudable): importes ──
    dl_A_api_completa(api_urls)

    # ── RESUMEN ─────────────────────────────────────────────────
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
    log.info("  A1_api_contratos/          ← JSON muestra (sonda de paginación)")
    log.info("  A2_api_anuncios/           ← JSON muestra (sonda de paginación)")
    log.info("  A1_api_contratos_completo/ ← JSON por ventana de fecha — 655K contratos con importes")
    log.info("  A2_api_anuncios_completo/  ← JSON por ventana de fecha — 656K anuncios")
    log.info("  A3_api_poderes/            ← JSON completo — 800+ poderes adjudicadores")
    log.info("  A4_api_empresas/           ← JSON completo — empresas licitadoras")
    log.info("  B1_xlsx_sector_publico/    ← XLSX anuales (2011-%d) — metadatos de anuncios", YEAR_NOW)
    log.info("  B2_revascon_historico/     ← CSV/XLSX 2013-2018 — serie histórica")
    log.info("  B3_ultimos_90_dias/        ← XLSX snapshot reciente")
    log.info("  B4_revascon_por_poder/     ← XLSX REVASCON por poder y año (%d-%d)",
             REVASCON_ANIO_MIN, YEAR_NOW)
    log.info("  C1_bilbao/                 ← CSV contratos municipales (2005-%d)", YEAR_NOW)
    log.info("  C2_vitoria_gasteiz/        ← CSV/XLSX contratos (menores) formalizados")
    log.info("  */_historico/              ← versiones anteriores de lo que ha cambiado")


if __name__ == "__main__":
    main()
