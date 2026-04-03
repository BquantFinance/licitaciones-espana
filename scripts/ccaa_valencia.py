#!/usr/bin/env python3
"""
Descarga de datos abiertos de la Comunitat Valenciana.

Pipeline de descarga sobre la API CKAN de dadesobertes.gva.es. Descubre los
recursos CSV por dataset, genera un manifiesto reproducible y descarga en
paralelo los archivos originales.
"""

import argparse
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://dadesobertes.gva.es"
API_URL = f"{BASE_URL}/api/3/action/package_show"
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_DOWNLOAD_DIR = REPO_ROOT / "valencia_datos"
DEFAULT_LOG_PATH = SCRIPT_DIR / "ccaa_valencia.log"
DEFAULT_DOWNLOAD_WORKERS = 6
CSV_FORMATS = {"CSV", "TEXT/CSV"}
REQUEST_TIMEOUT = (20, 300)
LOGGER_NAME = "ccaa_valencia"
_THREAD_LOCAL = threading.local()


DATASETS = {
    "contratacion": [
        "eco-gvo-contratos-2014",
        "eco-gvo-contratos-2015",
        "eco-gvo-contratos-2016",
        "eco-gvo-contratos-2017",
        "eco-gvo-contratos-2018",
        "eco-gvo-contratos-2019",
        "eco-gvo-contratos-2020",
        "eco-gvo-contratos-2021",
        "eco-gvo-contratos-2022",
        "eco-gvo-contratos-2023",
        "eco-gvo-contratos-2024",
        "eco-gvo-contratos-2025",
        "eco-contratos-dana",
    ],
    "subvenciones": [
        "eco-gvo-subv-2022",
        "eco-gvo-subv-2023",
        "eco-gvo-subv-2024",
        "eco-gvo-subv-2025",
        "eco-ayudas-dana",
        "eco-pmp-subvenciones",
    ],
    "presupuestos": [
        "sec-nefis-visor-2024",
        "sec-nefis-visor-2025",
    ],
    "convenios": [
        "gob-convenios-2018",
        "gob-convenios-2019",
        "gob-convenios-2020",
        "gob-convenios-2021",
        "gob-convenios-2022",
    ],
    "lobbies": [
        "sec-regia-actividades",
        "sec-regia-grupos",
    ],
    "empleo": [
        "tra-eres-ertes-v2-2025",
        "tra-eres-ertes-v2-2024",
        "emp-erte-dana-cv",
        "emp-erte-dana-pob",
        "emp-erte-dana-agr",
        "tra-ocu-contratos-2024",
        "tra-ocu-contratos-2023",
        "tra-ocu-contratos-2022",
    ],
    "paro": [
        "datos-de-paro-en-la-comunitat-valenciana",
        "datos-del-paro-en-la-comunidad-valenciana-2024",
        "tra-reg-paro-2024",
        "tra-reg-paro-2023",
        "tra-reg-paro-2022",
        "tra-reg-paro-2021",
        "tra-reg-paro-2020",
        "tra-reg-paro-2019",
        "tra-reg-paro-2018",
        "tra-reg-paro-2017",
    ],
    "siniestralidad": [
        "tra-accidentes-2024",
        "tra-accidentes-2023",
        "tra-accidentes-2022",
        "tra-accidentes-2021",
        "tra-accidentes-2020",
        "tra-accidentes-2019",
        "tra-accidentes-2018",
        "tra-accidentes-2017",
        "tra-accidentes-2016",
        "tra-accidentes-2015",
    ],
    "patrimonio": [
        "hac-bie-inm",
        "hac-bie-inmuebles",
        "hac-bie-inmateriales",
    ],
    "entidades": [
        "sec-mapets",
        "soc-asociaciones",
    ],
    "territorio": [
        "edu-centros",
    ],
    "turismo": [
        "tur-gestur-vt",
        "tur-gestur-cr",
        "tur-gestur-ca",
        "tur-gestur-ap",
        "tur-gestur-afp",
        "tur-gestur-agv",
        "tur-gestur-alb",
        "dades-turisme-hotels-comunitat-valenciana",
        "dades-turisme-campings-comunitat-valenciana",
        "dades-turisme-allotjament-rural-comunitat-valenciana",
        "dades-turisme-agencies-viatges-comunitat-valenciana",
        "dades-turisme-habitatges-comunitat-valenciana-2025",
        "dades-turisme-actiu-comunitat-valenciana",
    ],
    "sanidad": [
        "sanidad-sip",
        "san-reg-centros-2020",
        "sal-tm-cv",
        "sal-tmb-cv",
    ],
    "transporte": [
        "tra-hyr-atmv-horaris-i-rutes",
    ],
}


@dataclass(frozen=True)
class ResourceDownload:
    category: str
    dataset_id: str
    dataset_title: str
    package_metadata_modified: Optional[str]
    resource_id: str
    resource_name: str
    resource_url: str
    resource_format: str
    resource_last_modified: Optional[str]
    output_path: Path


def build_logger(log_path: Path) -> logging.Logger:
    log_path = log_path.expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "licitaciones-espana/valencia-downloader"})
    return session


def get_thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        _THREAD_LOCAL.session = session
    return session


def parse_categories_arg(raw_categories: Optional[str]) -> Optional[list[str]]:
    if not raw_categories:
        return None
    return [chunk.strip() for chunk in raw_categories.split(",") if chunk.strip()]


def resolve_selected_categories(raw_categories: Optional[str]) -> list[str]:
    selected = parse_categories_arg(raw_categories)
    if selected is None:
        return list(DATASETS.keys())

    invalid = [category for category in selected if category not in DATASETS]
    if invalid:
        raise ValueError(f"Categorías no válidas: {invalid}")
    return selected


def build_runtime_paths(download_dir: str | Path) -> dict[str, Path]:
    """Build the raw-download artifact paths anchored to the chosen root."""
    base_dir = Path(download_dir).expanduser().resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    return {
        "download_dir": base_dir,
        "manifest": base_dir / "valencia_manifest.json",
        "summary": base_dir / "valencia_summary.json",
    }


def sanitize_filename(name: str) -> str:
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, "_")
    while "  " in name:
        name = name.replace("  ", " ")
    return name[:200].strip().replace(" ", "_")


def extract_csv_resources(info: dict) -> list[dict]:
    return [
        resource
        for resource in info.get("resources", [])
        if (resource.get("format") or "").upper() in CSV_FORMATS
    ]


def build_output_filename(resource: dict, used_names: set[str]) -> str:
    base_name = sanitize_filename(resource.get("name") or resource.get("id") or "data")
    if not base_name.lower().endswith(".csv"):
        base_name += ".csv"

    candidate = base_name
    suffix_counter = 2
    stem = Path(base_name).stem
    suffix = Path(base_name).suffix
    resource_id = (resource.get("id") or "resource")[:8]

    if candidate.lower() in used_names:
        candidate = f"{stem}__{resource_id}{suffix}"

    while candidate.lower() in used_names:
        candidate = f"{stem}__{resource_id}_{suffix_counter}{suffix}"
        suffix_counter += 1

    used_names.add(candidate.lower())
    return candidate


def load_previous_manifest(manifest_path: Path) -> dict[str, dict]:
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    resources = payload.get("resources", [])
    return {resource["resource_id"]: resource for resource in resources if resource.get("resource_id")}


def get_dataset_info(session: requests.Session, dataset_id: str, logger: logging.Logger) -> Optional[dict]:
    try:
        response = session.get(API_URL, params={"id": dataset_id}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if payload.get("success"):
            return payload.get("result", {})
        logger.warning("CKAN devolvió success=false para %s", dataset_id)
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        logger.warning("HTTP %s al consultar %s", status, dataset_id)
    except requests.exceptions.Timeout:
        logger.warning("Timeout consultando %s", dataset_id)
    except requests.RequestException as exc:
        logger.warning("Error consultando %s: %s", dataset_id, exc)
    return None


def build_download_plan(
    session: requests.Session,
    selected_categories: list[str],
    download_dir: Path,
    logger: logging.Logger,
) -> tuple[list[ResourceDownload], list[dict], list[str]]:
    """Discover CKAN datasets/resources and return the concrete CSV plan."""
    plan: list[ResourceDownload] = []
    dataset_rows: list[dict] = []
    errors: list[str] = []
    used_names_by_category: dict[str, set[str]] = {}

    for category in selected_categories:
        category_dir = download_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        used_names = used_names_by_category.setdefault(category, set())

        for dataset_id in DATASETS[category]:
            info = get_dataset_info(session, dataset_id, logger)
            if not info:
                errors.append(dataset_id)
                continue

            csv_resources = extract_csv_resources(info)
            dataset_rows.append(
                {
                    "category": category,
                    "dataset_id": dataset_id,
                    "title": info.get("title"),
                    "metadata_modified": info.get("metadata_modified"),
                    "csv_resources": len(csv_resources),
                }
            )

            if not csv_resources:
                logger.warning("%s no tiene recursos CSV", dataset_id)
                continue

            for resource in csv_resources:
                filename = build_output_filename(resource, used_names)
                plan.append(
                    ResourceDownload(
                        category=category,
                        dataset_id=dataset_id,
                        dataset_title=info.get("title") or dataset_id,
                        package_metadata_modified=info.get("metadata_modified"),
                        resource_id=resource.get("id") or filename,
                        resource_name=resource.get("name") or filename,
                        resource_url=resource.get("url") or "",
                        resource_format=resource.get("format") or "",
                        resource_last_modified=resource.get("last_modified"),
                        output_path=category_dir / filename,
                    )
                )

    return plan, dataset_rows, errors


def should_download_resource(
    resource: ResourceDownload,
    previous_manifest: dict[str, dict],
    force: bool = False,
) -> bool:
    """Skip unchanged resources when the previous manifest still matches."""
    if force or not resource.output_path.exists():
        return True

    previous = previous_manifest.get(resource.resource_id)
    if not previous:
        return False

    return any(
        [
            previous.get("url") != resource.resource_url,
            previous.get("package_metadata_modified") != resource.package_metadata_modified,
            previous.get("resource_last_modified") != resource.resource_last_modified,
        ]
    )


def resource_to_manifest_row(resource: ResourceDownload, status: str, **extra) -> dict:
    row = {
        "category": resource.category,
        "dataset_id": resource.dataset_id,
        "dataset_title": resource.dataset_title,
        "resource_id": resource.resource_id,
        "resource_name": resource.resource_name,
        "format": resource.resource_format,
        "url": resource.resource_url,
        "output_path": str(resource.output_path),
        "package_metadata_modified": resource.package_metadata_modified,
        "resource_last_modified": resource.resource_last_modified,
        "status": status,
    }
    row.update(extra)
    return row


def download_resource(resource: ResourceDownload) -> dict:
    session = get_thread_session()
    tmp_path = resource.output_path.with_suffix(resource.output_path.suffix + ".part")
    resource.output_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()

    try:
        response = session.get(resource.resource_url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        with tmp_path.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)
        tmp_path.replace(resource.output_path)
        size_bytes = resource.output_path.stat().st_size
        return resource_to_manifest_row(
            resource,
            "downloaded",
            size_bytes=size_bytes,
            duration_seconds=round(time.perf_counter() - start, 3),
        )
    except requests.exceptions.Timeout:
        error = "Timeout"
    except requests.RequestException as exc:
        error = str(exc)
    except OSError as exc:
        error = str(exc)

    if tmp_path.exists():
        tmp_path.unlink(missing_ok=True)

    return resource_to_manifest_row(
        resource,
        "failed",
        error=error[:200],
        duration_seconds=round(time.perf_counter() - start, 3),
    )


def run_downloads(
    plan: list[ResourceDownload],
    previous_manifest: dict[str, dict],
    download_workers: int,
    force: bool,
    logger: logging.Logger,
) -> list[dict]:
    """Download pending resources in parallel and preserve a full manifest row."""
    results: list[dict] = []
    pending: list[ResourceDownload] = []

    for resource in plan:
        if should_download_resource(resource, previous_manifest, force=force):
            pending.append(resource)
        else:
            results.append(
                resource_to_manifest_row(
                    resource,
                    "skipped",
                    size_bytes=resource.output_path.stat().st_size if resource.output_path.exists() else None,
                )
            )

    if not pending:
        return sorted(results, key=lambda row: (row["category"], row["dataset_id"], row["resource_name"]))

    max_workers = max(1, min(download_workers, len(pending)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(download_resource, resource): resource for resource in pending}
        for future in as_completed(future_map):
            row = future.result()
            results.append(row)
            if row["status"] == "failed":
                logger.warning("Fallo descargando %s: %s", row["resource_name"], row.get("error"))

    return sorted(results, key=lambda row: (row["category"], row["dataset_id"], row["resource_name"]))


def build_summary(
    selected_categories: list[str],
    dataset_rows: list[dict],
    resource_rows: list[dict],
    errors: list[str],
    duration_seconds: float,
) -> dict:
    category_summary: dict[str, dict] = {}
    for category in selected_categories:
        rows = [row for row in resource_rows if row["category"] == category]
        category_summary[category] = {
            "resources": len(rows),
            "downloaded": sum(row["status"] == "downloaded" for row in rows),
            "skipped": sum(row["status"] == "skipped" for row in rows),
            "failed": sum(row["status"] == "failed" for row in rows),
            "size_bytes": sum((row.get("size_bytes") or 0) for row in rows),
        }

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "selected_categories": selected_categories,
        "datasets_discovered": len(dataset_rows),
        "resources_discovered": len(resource_rows),
        "downloaded": sum(row["status"] == "downloaded" for row in resource_rows),
        "skipped": sum(row["status"] == "skipped" for row in resource_rows),
        "failed": sum(row["status"] == "failed" for row in resource_rows),
        "total_size_bytes": sum((row.get("size_bytes") or 0) for row in resource_rows),
        "duration_seconds": round(duration_seconds, 2),
        "errors": errors,
        "categories": category_summary,
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Descarga datasets CSV de Valencia desde CKAN.")
    parser.add_argument(
        "--categories",
        help="Lista separada por comas de categorías a descargar (por defecto, todas).",
    )
    parser.add_argument(
        "--download-dir",
        default=str(DEFAULT_DOWNLOAD_DIR),
        help=f"Directorio de descarga de CSVs (default: {DEFAULT_DOWNLOAD_DIR}).",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=DEFAULT_DOWNLOAD_WORKERS,
        help=f"Número de descargas concurrentes (default: {DEFAULT_DOWNLOAD_WORKERS}).",
    )
    parser.add_argument(
        "--log-path",
        default=str(DEFAULT_LOG_PATH),
        help=f"Ruta del log (default: {DEFAULT_LOG_PATH}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-descarga aunque el manifiesto previo indique que el recurso no cambió.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = build_runtime_paths(args.download_dir)
    logger = build_logger(Path(args.log_path))
    selected_categories = resolve_selected_categories(args.categories)
    previous_manifest = load_previous_manifest(paths["manifest"])
    session = build_session()

    logger.info("Inicio Valencia CKAN")
    logger.info("Directorio descarga: %s", paths["download_dir"])
    logger.info("Categorías: %s", ", ".join(selected_categories))

    start = time.perf_counter()
    plan, dataset_rows, errors = build_download_plan(session, selected_categories, paths["download_dir"], logger)
    logger.info("Datasets descubiertos: %s", len(dataset_rows))
    logger.info("Recursos CSV descubiertos: %s", len(plan))

    resource_rows = run_downloads(
        plan,
        previous_manifest=previous_manifest,
        download_workers=args.download_workers,
        force=args.force,
        logger=logger,
    )

    duration_seconds = time.perf_counter() - start
    summary = build_summary(selected_categories, dataset_rows, resource_rows, errors, duration_seconds)
    manifest = {
        "generated_at": summary["generated_at"],
        "base_url": BASE_URL,
        "selected_categories": selected_categories,
        "datasets": dataset_rows,
        "resources": resource_rows,
        "summary": summary,
    }

    write_json(paths["manifest"], manifest)
    write_json(paths["summary"], summary)

    logger.info(
        "Descarga completada: %s descargados, %s omitidos, %s fallidos en %.2fs",
        summary["downloaded"],
        summary["skipped"],
        summary["failed"],
        summary["duration_seconds"],
    )
    logger.info("Manifiesto: %s", paths["manifest"])
    logger.info("Resumen: %s", paths["summary"])
    logger.info(
        "Siguiente paso: python scripts/ccaa_valencia_parquet.py --input-dir %s --output-dir %s",
        paths["download_dir"],
        REPO_ROOT / "valencia",
    )
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
