#!/usr/bin/env python3
"""
BORME PDF Scraper v1.0
======================
Descarga todos los PDFs del Boletín Oficial del Registro Mercantil (BORME)
desde boe.es, iterando por fecha.

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

def _default_output_dir():
    """Use LICITACIONES_TMP_DIR if set, otherwise cwd/borme_pdfs."""
    tmp = os.environ.get("LICITACIONES_TMP_DIR", "")
    if tmp:
        return os.path.join(tmp, "borme", "pdfs")
    return "borme_pdfs"


# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
BASE_URL = "https://www.boe.es"
INDEX_PATTERN = "/borme/dias/{year:04d}/{month:02d}/{day:02d}/index.php"
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
    seen_urls = set()
    results = []

    for match in PDF_HREF_RE.finditer(html):
        url_path = match.group(1)
        if url_path in seen_urls:
            continue
        seen_urls.add(url_path)

        pdf_filename = url_path.split("/")[-1]

        # Tipo inferido del nombre: BORME-A, BORME-B, BORME-C, BORME-S, o legacy
        tipo = "otro"
        upper_fn = pdf_filename.upper()
        if "BORME-A-" in upper_fn:
            tipo = "A"  # Sección Primera — Actos inscritos
        elif "BORME-B-" in upper_fn:
            tipo = "B"  # Sección Primera — Otros actos
        elif "BORME-C-" in upper_fn:
            tipo = "C"  # Sección Segunda — Anuncios y avisos legales
        elif "BORME-S-" in upper_fn:
            tipo = "S"  # Sumario
        elif upper_fn.startswith("R"):
            tipo = "C"  # Legacy Sección Segunda (2001-2008)
        elif upper_fn.startswith("A"):
            tipo = "A"  # Legacy Sección Primera (2001-2008)

        results.append({
            "url": url_path,
            "pdf_filename": pdf_filename,
            "tipo": tipo,
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
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

    @property
    def last_date(self) -> Optional[date]:
        d = self.state.get("last_completed_date")
        if d:
            return date.fromisoformat(d)
        return None

    def mark_completed(self, d: date, n_pdfs: int, n_bytes: int):
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
def scrape_day(
    session: requests.Session,
    d: date,
    output_dir: Path,
    manifest: Manifest,
    already_downloaded: set,
    delay: float,
    dl_lock: Optional[threading.Lock] = None,
) -> Tuple[int, int]:
    """Scrape un día completo. Thread-safe si se pasa dl_lock."""

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
        return 0, 0

    if resp.status_code == 404:
        log.debug(f"  404 para {d} (festivo/no publicación)")
        return 0, 0

    if resp.status_code == 429:
        log.error(f"  🚫 429 RATE LIMITED en índice {d} — esperando 30s y reintentando")
        time.sleep(30)
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException:
            return 0, 0
        if resp.status_code != 200:
            log.error(f"  🚫 Reintento fallido para {d}: HTTP {resp.status_code}")
            return 0, 0

    if resp.status_code != 200:
        log.warning(f"  ⚠️  HTTP {resp.status_code} para {d}")
        return 0, 0

    html = resp.text

    if detect_no_borme(html):
        log.debug(f"  No hay BORME para {d}")
        return 0, 0

    pdf_links = extract_pdf_links(html)

    if not pdf_links:
        log.debug(f"  Sin PDFs encontrados para {d}")
        return 0, 0

    # Directorio plano: output_dir/YYYY/MM/DD/
    day_dir = output_dir / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}"
    day_dir.mkdir(parents=True, exist_ok=True)

    n_downloaded = 0
    total_bytes = 0

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
            continue

        content = pdf_resp.content

        # Validar que es PDF
        if not content[:5] == b"%PDF-":
            log.warning(f"    ⚠️  {link['pdf_filename']} no es PDF válido (primeros bytes: {content[:20]})")
            continue

        # Guardar
        with open(local_path, "wb") as f:
            f.write(content)

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

    return n_downloaded, total_bytes


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
                session, d, output_dir, manifest, already_downloaded, args.delay, dl_lock
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
            return d, n_pdfs, n_bytes
        except Exception as e:
            with progress_lock:
                progress["done"] += 1
            log.error(f"  ✗ {d}: {e}")
            state.add_error(d, str(e))
            return d, 0, 0

    try:
        if workers <= 1:
            # Modo secuencial (original)
            for d in work_days:
                result = process_day(d)
                state.mark_completed(result[0], result[1], result[2])
        else:
            # Modo paralelo
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_day, d): d for d in work_days}
                for future in as_completed(futures):
                    try:
                        d, n_pdfs, n_bytes = future.result()
                        state.mark_completed(d, n_pdfs, n_bytes)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        d = futures[future]
                        log.error(f"  ✗ {d} futuro: {e}")
                        state.add_error(d, str(e))

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
        "--output", "-o", default=_default_output_dir(),
        help="Directorio de salida (default: $LICITACIONES_TMP_DIR/borme/pdfs or ./borme_pdfs)"
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
        "--verbose", "-v", action="store_true",
        help="Logging detallado"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run(args)


if __name__ == "__main__":
    main()
