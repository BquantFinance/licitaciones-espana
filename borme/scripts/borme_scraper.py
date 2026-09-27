#!/usr/bin/env python3
"""
BORME PDF Scraper v1.0
======================
Descarga todos los PDFs del Boletín Oficial del Registro Mercantil (BORME)
desde boe.es, iterando por fecha: los enlazados en el índice HTML del día más
los que lista el sumario de la API de datos abiertos del BOE (secciones A, B y C).

Uso:
    python borme_scraper.py --start 2009-01-01 --end 2025-12-31 --output ./borme_pdfs
    python borme_scraper.py --start 2001-01-02 --end 2026-02-17 --output ./borme_pdfs --workers 6
    python borme_scraper.py --resume --output ./borme_pdfs  # retoma desde donde se quedó

Estructura de salida:
    borme_pdfs/
    ├── 2001/
    │   ├── 01/
    │   │   ├── 02/
    │   │   │   ├── BORME-A-2001-1-02.pdf
    │   │   │   ├── BORME-A-2001-1-28.pdf
    │   │   │   ├── BORME-C-2001-1000.pdf
    │   │   │   ├── BORME-S-2001-1.pdf
    │   │   │   └── ...
    │   │   └── ...
    │   └── ...
    ├── manifest.csv          ← registro de descargas (tipo A/B/C/S)
    └── scraper_state.json    ← estado para --resume

Licencia de datos:
    Basado en datos de la Agencia Estatal Boletín Oficial del Estado
    https://www.boe.es
    Condiciones: https://www.boe.es/informacion/aviso_legal/index.php#reutilizacion

Autor: BQuant Finance
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://www.boe.es"
INDEX_PATTERN = "/borme/dias/{year:04d}/{month:02d}/{day:02d}/index.php"
# Sumario oficial del día (API de datos abiertos del BOE, APIsumarioBORME.pdf):
# lista cada documento de las secciones A (actos inscritos), B (otros actos) y
# C (anuncios y avisos legales) con su url_pdf, desde 2009. Los días sin BORME
# responde 404. Se usa además del índice HTML para no depender de qué enlaces
# muestre la página index.php.
SUMARIO_API_PATTERN = "/datosabiertos/api/borme/sumario/{year:04d}{month:02d}{day:02d}"
USER_AGENT = (
    "BQuant-BORME-Scraper/1.0 "
    "(investigación académica; contacto: bquantfinance.com) "
    "Python-requests"
)
DEFAULT_DELAY = 1.0  # seconds between requests (be respectful)
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # exponential backoff factor

# BORME no se publica sábados, domingos ni festivos en Madrid
# Festivos nacionales fijos (no incluye festivos autonómicos)
FESTIVOS_FIJOS = {
    (1, 1),    # Año Nuevo
    (1, 6),    # Epifanía
    (5, 1),    # Día del Trabajo
    (8, 15),   # Asunción
    (10, 12),  # Fiesta Nacional
    (11, 1),   # Todos los Santos
    (12, 6),   # Constitución
    (12, 8),   # Inmaculada
    (12, 25),  # Navidad
}

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("borme_scraper")


# ─────────────────────────────────────────────
#  SESSION
# ─────────────────────────────────────────────
def create_session():
    """Session con retry automático y user-agent identificado."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


# ─────────────────────────────────────────────
#  DATE UTILITIES
# ─────────────────────────────────────────────
def is_publishing_day(d: date) -> bool:
    """BORME se publica L-V. Festivos los detecta el servidor (404 o vacío).
    Solo saltamos sábados y domingos que es 100% seguro."""
    return d.weekday() < 5  # 0=lunes ... 4=viernes


def date_range(start: date, end: date):
    """Genera todas las fechas entre start y end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ─────────────────────────────────────────────
#  HTML PARSING (sin BeautifulSoup — regex puro)
# ─────────────────────────────────────────────
# Extraer TODAS las URLs a PDF del HTML — sin filtrar por sección ni nada.
# Captura href a cualquier .pdf dentro de /borme/dias/
PDF_HREF_RE = re.compile(
    r'href="(/borme/dias/\d{4}/\d{2}/\d{2}/pdfs/[^"]+\.pdf)"',
    re.IGNORECASE,
)


def extract_pdf_links(html: str) -> List[dict]:
    """Extrae TODOS los PDFs únicos del HTML del sumario BORME.

    Estrategia simple: buscar todos los href a .pdf, deduplicar por URL.
    La sección se infiere del nombre del archivo (A=actos, B=otros, C=segunda, S=sumario).
    Sin parsing de h3/h4 — así funciona con cualquier formato HTML (2001-2026).

    Returns:
        Lista de dicts: {url, pdf_filename, tipo}
        Deduplicados por URL.
    """
    return _links_from_paths(m.group(1) for m in PDF_HREF_RE.finditer(html))


# PDFs en la respuesta de la API de sumarios (XML o JSON): url_pdf absoluta
# (https://www.boe.es/borme/dias/.../pdfs/BORME-X-....pdf) o relativa
SUMARIO_PDF_RE = re.compile(
    r'(?:https?://(?:www\.)?boe\.es)?(/(?:borme|boe)/dias/\d{4}/\d{2}/\d{2}/pdfs/BORME-[A-Z]-[^"\'<>\s\\]+?\.pdf)',
    re.IGNORECASE,
)


def extract_pdf_links_sumario(texto: str) -> List[dict]:
    """PDFs de todas las secciones listados en el sumario de la API de datos abiertos.

    Se buscan las url_pdf en el texto en vez de recorrer el árbol XML/JSON, así
    no depende del formato de respuesta (en JSON las barras pueden ir como '\\/').
    """
    return _links_from_paths(m.group(1) for m in SUMARIO_PDF_RE.finditer(texto.replace("\\/", "/")))


def _tipo_desde_nombre(pdf_filename: str) -> str:
    """Sección inferida del nombre: BORME-A, BORME-B, BORME-C, BORME-S, o legacy."""
    upper_fn = pdf_filename.upper()
    if "BORME-A-" in upper_fn:
        return "A"  # Sección Primera — Actos inscritos
    if "BORME-B-" in upper_fn:
        return "B"  # Sección Primera — Otros actos
    if "BORME-C-" in upper_fn:
        return "C"  # Sección Segunda — Anuncios y avisos legales
    if "BORME-S-" in upper_fn:
        return "S"  # Sumario
    if upper_fn.startswith("R"):
        return "C"  # Legacy Sección Segunda (2001-2008)
    if upper_fn.startswith("A"):
        return "A"  # Legacy Sección Primera (2001-2008)
    return "otro"


def _links_from_paths(paths) -> List[dict]:
    """[{url, pdf_filename, tipo}] deduplicados por URL, en orden de aparición."""
    seen_urls = set()
    results = []
    for url_path in paths:
        if url_path in seen_urls:
            continue
        seen_urls.add(url_path)
        pdf_filename = url_path.split("/")[-1]
        results.append({
            "url": url_path,
            "pdf_filename": pdf_filename,
            "tipo": _tipo_desde_nombre(pdf_filename),
        })
    return results


def detect_no_borme(html: str) -> bool:
    """Detecta si la página indica que no hay BORME ese día."""
    indicators = [
        "no se publica",
        "no hay sumario",
        "no se ha publicado",
        "día inhábil",
        "Error 404",
    ]
    html_lower = html.lower()
    return any(ind.lower() in html_lower for ind in indicators)


# ─────────────────────────────────────────────
#  STATE MANAGEMENT
# ─────────────────────────────────────────────
class ScraperState:
    """Persiste estado para poder resumir descargas interrumpidas."""

    def __init__(self, output_dir: Path):
        self.state_file = output_dir / "scraper_state.json"
        self.state = self._load()

    def _load(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file, "r") as f:
                return json.load(f)
        return {
            "last_completed_date": None,
            "total_pdfs": 0,
            "total_days_processed": 0,
            "total_days_skipped": 0,
            "total_bytes": 0,
            "errors": [],
        }

    def save(self):
        # Escritura atómica: un corte a medias no debe dejar un JSON corrupto
        # que impida arrancar con --resume
        tmp = self.state_file.with_name(self.state_file.name + ".tmp")
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=2, default=str)
        os.replace(tmp, self.state_file)

    @property
    def last_date(self) -> Optional[date]:
        d = self.state.get("last_completed_date")
        if d:
            return date.fromisoformat(d)
        return None

    def mark_completed(self, d: date, n_pdfs: int, n_bytes: int):
        """d: marca de agua para --resume (todos los días hasta d están completos)."""
        self.state["last_completed_date"] = d.isoformat()
        self.state["total_pdfs"] += n_pdfs
        self.state["total_days_processed"] += 1
        self.state["total_bytes"] += n_bytes
        # Save every 10 days
        if self.state["total_days_processed"] % 10 == 0:
            self.save()

    def mark_skipped(self):
        self.state["total_days_skipped"] += 1

    def add_error(self, d: date, error: str):
        self.state["errors"].append({"date": d.isoformat(), "error": error})
        if len(self.state["errors"]) > 1000:
            self.state["errors"] = self.state["errors"][-500:]


# ─────────────────────────────────────────────
#  MANIFEST (CSV log de todas las descargas)
# ─────────────────────────────────────────────
class Manifest:
    """CSV log de cada PDF descargado. Thread-safe."""

    HEADER = ["date", "pdf_filename", "tipo", "url", "size_bytes", "sha256"]

    def __init__(self, output_dir: Path):
        self.path = output_dir / "manifest.csv"
        self._file = None
        self._writer = None
        self._lock = threading.Lock()

    def open(self):
        exists = self.path.exists()
        self._file = open(self.path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        if not exists:
            self._writer.writerow(self.HEADER)
            self._file.flush()

    def write(self, row: dict):
        with self._lock:
            self._writer.writerow([row.get(h, "") for h in self.HEADER])
            self._file.flush()

    def close(self):
        if self._file:
            self._file.close()

    def get_downloaded_urls(self) -> set:
        """Lee manifest existente para saber qué ya se descargó."""
        urls = set()
        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    urls.add(row.get("url", ""))
        return urls


# ─────────────────────────────────────────────
#  MAIN SCRAPER
# ─────────────────────────────────────────────
class DiaIncompleto(RuntimeError):
    """El día no se completó (fallo de red/HTTP en el índice o en algún PDF):
    no debe darse por completado, para que --resume lo reintente."""

    def __init__(self, msg: str, n_pdfs: int = 0, n_bytes: int = 0):
        super().__init__(msg)
        self.n_pdfs = n_pdfs
        self.n_bytes = n_bytes


def scrape_day(
    session: requests.Session,
    d: date,
    output_dir: Path,
    manifest: Manifest,
    already_downloaded: set,
    delay: float,
    dl_lock: Optional[threading.Lock] = None,
    usar_sumario_api: bool = True,
) -> Tuple[int, int]:
    """Scrape un día completo. Thread-safe si se pasa dl_lock.

    Los PDFs del día son la unión de los enlazados en el índice HTML y los que
    lista el sumario de la API de datos abiertos (secciones A, B y C).

    Lanza DiaIncompleto si el índice, el sumario o algún PDF no se pudo
    descargar; devolver (0, 0) queda reservado para días sin BORME (404 / sin PDFs).
    """

    def _is_downloaded(url):
        if dl_lock:
            with dl_lock:
                return url in already_downloaded
        return url in already_downloaded

    def _mark_downloaded(url):
        if dl_lock:
            with dl_lock:
                already_downloaded.add(url)
        else:
            already_downloaded.add(url)

    url = BASE_URL + INDEX_PATTERN.format(year=d.year, month=d.month, day=d.day)

    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as e:
        log.warning(f"  ⚠️  Error fetching index {d}: {e}")
        raise DiaIncompleto(f"Error descargando índice: {e}") from e

    if resp.status_code == 429:
        log.error(f"  🚫 429 RATE LIMITED en índice {d} — esperando 30s y reintentando")
        time.sleep(30)
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as e:
            raise DiaIncompleto(f"Error descargando índice: {e}") from e
        if resp.status_code not in (200, 404):
            log.error(f"  🚫 Reintento fallido para {d}: HTTP {resp.status_code}")
            raise DiaIncompleto(f"HTTP {resp.status_code} en índice tras 429")

    if resp.status_code == 404:
        log.debug(f"  404 para {d} (festivo/no publicación)")
        pdf_links = []
    elif resp.status_code != 200:
        log.warning(f"  ⚠️  HTTP {resp.status_code} para {d}")
        raise DiaIncompleto(f"HTTP {resp.status_code} en índice")
    else:
        html = resp.text
        # Primero los enlaces: una frase como "no se publica" en el texto de la
        # página no debe descartar un día que sí enlaza PDFs
        pdf_links = extract_pdf_links(html)
        if not pdf_links and detect_no_borme(html):
            log.debug(f"  No hay BORME para {d}")

    error_sumario = None
    if usar_sumario_api:
        try:
            vistos = {link["url"] for link in pdf_links}
            for link in fetch_sumario_links(session, d):
                if link["url"] not in vistos:
                    vistos.add(link["url"])
                    pdf_links.append(link)
        except DiaIncompleto as e:
            error_sumario = e

    if not pdf_links:
        if error_sumario is not None:
            # Sin sumario no se puede afirmar que el día no tenga BORME
            raise error_sumario
        log.debug(f"  Sin PDFs encontrados para {d}")
        return 0, 0

    # Directorio plano: output_dir/YYYY/MM/DD/
    day_dir = output_dir / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}"
    day_dir.mkdir(parents=True, exist_ok=True)

    n_downloaded = 0
    total_bytes = 0
    n_fallidos = 0

    for link in pdf_links:
        pdf_url = link["url"]

        # Skip ya descargados
        if _is_downloaded(pdf_url):
            continue

        full_url = BASE_URL + pdf_url
        local_path = day_dir / link["pdf_filename"]

        # Skip si el archivo ya existe en disco
        if local_path.exists() and local_path.stat().st_size > 0:
            _mark_downloaded(pdf_url)
            continue

        # Descargar con retry en 429
        try:
            time.sleep(delay)
            pdf_resp = session.get(full_url, timeout=60)

            # Retry on 429
            if pdf_resp.status_code == 429:
                log.error(f"    🚫 429 RATE LIMITED descargando {link['pdf_filename']} — esperando 30s")
                time.sleep(30)
                pdf_resp = session.get(full_url, timeout=60)

            pdf_resp.raise_for_status()
        except requests.RequestException as e:
            log.warning(f"    ⚠️  Error descargando {link['pdf_filename']}: {e}")
            n_fallidos += 1
            continue

        content = pdf_resp.content

        # Validar que es PDF
        if not content[:5] == b"%PDF-":
            log.warning(f"    ⚠️  {link['pdf_filename']} no es PDF válido (primeros bytes: {content[:20]})")
            n_fallidos += 1
            continue

        # Guardar vía .part + rename: un corte a medias no deja un PDF truncado
        # que la siguiente ejecución daría por descargado
        tmp_path = local_path.with_name(local_path.name + ".part")
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, local_path)

        sha256 = hashlib.sha256(content).hexdigest()

        manifest.write({
            "date": d.isoformat(),
            "pdf_filename": link["pdf_filename"],
            "tipo": link["tipo"],
            "url": pdf_url,
            "size_bytes": len(content),
            "sha256": sha256,
        })

        _mark_downloaded(pdf_url)
        n_downloaded += 1
        total_bytes += len(content)

    if n_fallidos:
        raise DiaIncompleto(f"{n_fallidos} PDFs sin descargar", n_downloaded, total_bytes)
    if error_sumario is not None:
        # Se descargó lo del índice HTML, pero sin el sumario pueden faltar
        # secciones: el día queda pendiente para --resume
        raise DiaIncompleto(str(error_sumario), n_downloaded, total_bytes)
    return n_downloaded, total_bytes


def fetch_sumario_links(session: requests.Session, d: date) -> List[dict]:
    """PDFs del día según el sumario de la API de datos abiertos del BOE.

    404 → día sin BORME ([]). Errores de red / 5xx / 429 → DiaIncompleto (el día
    se reintenta). Otros 4xx → aviso y [] (el índice HTML sigue valiendo).
    """
    url = BASE_URL + SUMARIO_API_PATTERN.format(year=d.year, month=d.month, day=d.day)
    try:
        resp = session.get(url, timeout=30, headers={"Accept": "application/xml"})
    except requests.RequestException as e:
        log.warning(f"  ⚠️  Error en sumario API {d}: {e}")
        raise DiaIncompleto(f"Error descargando sumario API: {e}") from e
    if resp.status_code == 404:
        return []
    if resp.status_code == 429 or resp.status_code >= 500:
        log.warning(f"  ⚠️  HTTP {resp.status_code} en sumario API {d}")
        raise DiaIncompleto(f"HTTP {resp.status_code} en sumario API")
    if resp.status_code != 200:
        log.warning(f"  ⚠️  HTTP {resp.status_code} en sumario API {d}: se usa solo el índice HTML")
        return []
    return extract_pdf_links_sumario(resp.text)


def run(args):
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    state = ScraperState(output_dir)
    manifest = Manifest(output_dir)

    # Determinar rango de fechas
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    # Si --resume, avanzar al día siguiente del último completado
    if args.resume and state.last_date:
        resume_from = state.last_date + timedelta(days=1)
        if resume_from > start:
            log.info(f"📂 Resumiendo desde {resume_from} (último completado: {state.last_date})")
            start = resume_from

    if start > end:
        log.info("✅ Nada que hacer — rango ya completado")
        return

    # Cargar URLs ya descargadas del manifest
    already_downloaded = manifest.get_downloaded_urls()
    dl_lock = threading.Lock()  # protege already_downloaded
    log.info(f"📋 {len(already_downloaded):,} PDFs ya en manifest")

    manifest.open()

    workers = getattr(args, 'workers', 1)
    usar_sumario_api = not getattr(args, 'sin_sumario_api', False)
    total_days = (end - start).days + 1
    log.info(f"🚀 BORME Scraper: {start} → {end} ({total_days:,} días)")
    log.info(f"📁 Output: {output_dir}")
    log.info(f"⚡ Workers: {workers} | Delay: {args.delay}s")
    log.info("")

    # Filtrar solo días laborables
    work_days = [d for d in date_range(start, end) if is_publishing_day(d)]
    skip_days = total_days - len(work_days)
    state.state["total_days_skipped"] += skip_days
    log.info(f"📅 {len(work_days):,} días laborables, {skip_days:,} saltados (fines de semana/festivos)")

    # Contador de progreso thread-safe
    progress = {"done": 0, "pdfs": 0, "bytes": 0}
    progress_lock = threading.Lock()

    def process_day(d):
        """Procesa un día completo. Thread-safe."""
        # Cada worker usa su propia session
        session = create_session()
        try:
            n_pdfs, n_bytes = scrape_day(
                session, d, output_dir, manifest, already_downloaded, args.delay, dl_lock,
                usar_sumario_api=usar_sumario_api,
            )
            with progress_lock:
                progress["done"] += 1
                progress["pdfs"] += n_pdfs
                progress["bytes"] += n_bytes
                pct = progress["done"] / len(work_days) * 100
                if n_pdfs > 0:
                    log.info(f"[{pct:5.1f}%] {d} ✓ {n_pdfs} PDFs ({n_bytes / 1024:.0f} KB)")
                else:
                    log.info(f"[{pct:5.1f}%] {d}")
            return d, n_pdfs, n_bytes, True
        except Exception as e:
            with progress_lock:
                progress["done"] += 1
            log.error(f"  ✗ {d}: {e}")
            state.add_error(d, str(e))
            # Día incompleto: cuenta lo descargado, pero no se da por completado
            return d, getattr(e, "n_pdfs", 0), getattr(e, "n_bytes", 0), False

    # Marca de agua para --resume: last_completed_date solo avanza por días
    # laborables consecutivos completados sin error (en paralelo terminan
    # desordenados, y un día fallido debe reintentarse al retomar)
    completados = set()
    siguiente = [0]  # índice en work_days del primer día aún no completado

    def registrar(d, n_pdfs, n_bytes, ok):
        if ok:
            completados.add(d)
        while siguiente[0] < len(work_days) and work_days[siguiente[0]] in completados:
            siguiente[0] += 1
        marca = work_days[siguiente[0] - 1] if siguiente[0] else start - timedelta(days=1)
        state.mark_completed(marca, n_pdfs, n_bytes)

    try:
        if workers <= 1:
            # Modo secuencial (original)
            for d in work_days:
                registrar(*process_day(d))
        else:
            # Modo paralelo
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_day, d): d for d in work_days}
                try:
                    for future in as_completed(futures):
                        try:
                            registrar(*future.result())
                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            d = futures[future]
                            log.error(f"  ✗ {d} futuro: {e}")
                            state.add_error(d, str(e))
                except KeyboardInterrupt:
                    # Cancelar los días encolados: si no, el with espera a TODOS
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise

    except KeyboardInterrupt:
        log.info("\n⏸️  Interrumpido por usuario")
    finally:
        state.save()
        manifest.close()

        # Resumen final
        s = state.state
        log.info("")
        log.info("═" * 50)
        log.info("  RESUMEN FINAL")
        log.info("═" * 50)
        log.info(f"  Días procesados:  {s['total_days_processed']:,}")
        log.info(f"  Días saltados:    {s['total_days_skipped']:,}")
        log.info(f"  PDFs descargados: {s['total_pdfs']:,}")
        log.info(f"  Bytes totales:    {s['total_bytes'] / 1024 / 1024:.1f} MB")
        log.info(f"  Errores:          {len(s['errors'])}")
        log.info(f"  Último día:       {s['last_completed_date']}")
        log.info(f"  Estado guardado:  {state.state_file}")
        log.info(f"  Manifest:         {manifest.path}")
        log.info("═" * 50)


# ─────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="BORME PDF Scraper — descarga PDFs del Boletín Oficial del Registro Mercantil",
        epilog=(
            "Basado en datos de la Agencia Estatal Boletín Oficial del Estado\n"
            "https://www.boe.es\n"
            "Condiciones: https://www.boe.es/informacion/aviso_legal/index.php#reutilizacion"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--start", default="2009-01-01",
        help="Fecha inicio YYYY-MM-DD (default: 2009-01-01, inicio edición electrónica)"
    )
    parser.add_argument(
        "--end", default=date.today().isoformat(),
        help="Fecha fin YYYY-MM-DD (default: hoy)"
    )
    parser.add_argument(
        "--output", "-o", default="./borme_pdfs",
        help="Directorio de salida (default: ./borme_pdfs)"
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Segundos entre requests por worker (default: {DEFAULT_DELAY})"
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=1,
        help="Workers paralelos — cada uno procesa un día distinto (default: 1, recomendado: 4-8)"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Retomar desde el último día completado"
    )
    parser.add_argument(
        "--sin-sumario-api", action="store_true",
        help=("No consultar el sumario de la API de datos abiertos del BOE "
              "(solo los PDFs enlazados en el índice HTML del día)")
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Logging detallado"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run(args)


if __name__ == "__main__":
    main()
