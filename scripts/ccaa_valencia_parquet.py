"""
Conversión de CSV a Parquet para Valencia.

Procesa los CSV descargados por `ccaa_valencia.py`, detecta codificación y
separador automáticamente y genera parquet por categoría con un resumen final.
"""

import argparse
import json
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_DIR = REPO_ROOT / "valencia_datos"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "valencia"
DEFAULT_LOG_PATH = SCRIPT_DIR / "ccaa_valencia_parquet.log"
DEFAULT_WORKERS = 4
ENCODINGS = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
SEPARATORS = [";", ",", "\t"]
LOGGER_NAME = "ccaa_valencia_parquet"


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


def parse_categories_arg(raw_categories: Optional[str]) -> Optional[list[str]]:
    if not raw_categories:
        return None
    return [chunk.strip() for chunk in raw_categories.split(",") if chunk.strip()]


def resolve_selected_categories(input_dir: Path, raw_categories: Optional[str]) -> list[str]:
    existing = sorted(path.name for path in input_dir.iterdir() if path.is_dir())
    selected = parse_categories_arg(raw_categories)
    if selected is None:
        return existing

    invalid = [category for category in selected if category not in existing]
    if invalid:
        raise ValueError(f"Categorías no válidas para {input_dir}: {invalid}")
    return selected


def detect_encoding_and_sep(filepath: Path) -> tuple[str, str]:
    """Probe a small CSV sample to infer a workable encoding and separator."""
    for encoding in ENCODINGS:
        for separator in SEPARATORS:
            try:
                dataframe = pd.read_csv(
                    filepath,
                    encoding=encoding,
                    sep=separator,
                    nrows=5,
                    on_bad_lines="skip",
                )
            except Exception:
                continue
            if len(dataframe.columns) > 1:
                return encoding, separator
    return "utf-8", ";"


def coerce_object_columns_for_parquet(dataframe: pd.DataFrame) -> pd.DataFrame:
    converted = dataframe.copy()
    for column in converted.columns:
        if converted[column].dtype == "object":
            converted[column] = converted[column].astype("string")
    return converted


def parquet_row_count(parquet_path: Path) -> int:
    return pq.ParquetFile(parquet_path).metadata.num_rows


def should_convert(csv_path: Path, parquet_path: Path, force: bool = False) -> bool:
    if force or not parquet_path.exists():
        return True
    return csv_path.stat().st_mtime > parquet_path.stat().st_mtime


def convert_to_parquet(csv_path_str: str, parquet_path_str: str) -> dict:
    """Worker-friendly conversion helper used by the process pool."""
    csv_path = Path(csv_path_str)
    parquet_path = Path(parquet_path_str)

    start = time.perf_counter()
    encoding, separator = detect_encoding_and_sep(csv_path)
    dataframe = pd.read_csv(
        csv_path,
        encoding=encoding,
        sep=separator,
        low_memory=False,
        on_bad_lines="skip",
    )
    dataframe = coerce_object_columns_for_parquet(dataframe)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(parquet_path, index=False, compression="snappy")

    csv_size_bytes = csv_path.stat().st_size
    parquet_size_bytes = parquet_path.stat().st_size
    return {
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "status": "converted",
        "rows": len(dataframe),
        "csv_size_bytes": csv_size_bytes,
        "parquet_size_bytes": parquet_size_bytes,
        "encoding": encoding,
        "separator": separator,
        "duration_seconds": round(time.perf_counter() - start, 3),
    }


def build_conversion_plan(input_dir: Path, output_dir: Path, categories: list[str]) -> list[tuple[Path, Path, str]]:
    plan = []
    for category in categories:
        category_dir = input_dir / category
        output_category = output_dir / category
        output_category.mkdir(parents=True, exist_ok=True)
        for csv_path in sorted(category_dir.glob("*.csv")):
            parquet_path = output_category / f"{csv_path.stem.replace(' ', '_')}.parquet"
            plan.append((csv_path, parquet_path, category))
    return plan


def run_conversion(
    plan: list[tuple[Path, Path, str]],
    workers: int,
    force: bool,
    logger: logging.Logger,
) -> list[dict]:
    """Convert pending CSVs in parallel and keep skipped files in the summary."""
    results = []
    pending = []

    for csv_path, parquet_path, category in plan:
        if should_convert(csv_path, parquet_path, force=force):
            pending.append((csv_path, parquet_path, category))
            continue

        results.append(
            {
                "csv_path": str(csv_path),
                "parquet_path": str(parquet_path),
                "category": category,
                "status": "skipped",
                "rows": parquet_row_count(parquet_path),
                "csv_size_bytes": csv_path.stat().st_size,
                "parquet_size_bytes": parquet_path.stat().st_size,
            }
        )

    if not pending:
        return sorted(results, key=lambda row: (row["category"], row["parquet_path"]))

    max_workers = max(1, min(workers, len(pending)))
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(convert_to_parquet, str(csv_path), str(parquet_path)): (csv_path, parquet_path, category)
            for csv_path, parquet_path, category in pending
        }
        for future in as_completed(future_map):
            csv_path, parquet_path, category = future_map[future]
            try:
                result = future.result()
                result["category"] = category
                results.append(result)
            except Exception as exc:
                logger.warning("Fallo convirtiendo %s: %s", csv_path.name, exc)
                results.append(
                    {
                        "csv_path": str(csv_path),
                        "parquet_path": str(parquet_path),
                        "category": category,
                        "status": "failed",
                        "error": str(exc)[:200],
                    }
                )

    return sorted(results, key=lambda row: (row["category"], row["parquet_path"]))


def build_summary(
    input_dir: Path,
    output_dir: Path,
    categories: list[str],
    results: list[dict],
    duration_seconds: float,
) -> dict:
    category_summary = {}
    for category in categories:
        rows = [row for row in results if row["category"] == category]
        category_summary[category] = {
            "files": len(rows),
            "converted": sum(row["status"] == "converted" for row in rows),
            "skipped": sum(row["status"] == "skipped" for row in rows),
            "failed": sum(row["status"] == "failed" for row in rows),
            "rows": sum((row.get("rows") or 0) for row in rows),
            "csv_size_bytes": sum((row.get("csv_size_bytes") or 0) for row in rows),
            "parquet_size_bytes": sum((row.get("parquet_size_bytes") or 0) for row in rows),
        }

    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "categories": category_summary,
        "files_total": len(results),
        "converted": sum(row["status"] == "converted" for row in results),
        "skipped": sum(row["status"] == "skipped" for row in results),
        "failed": sum(row["status"] == "failed" for row in results),
        "rows_total": sum((row.get("rows") or 0) for row in results),
        "csv_size_bytes": sum((row.get("csv_size_bytes") or 0) for row in results),
        "parquet_size_bytes": sum((row.get("parquet_size_bytes") or 0) for row in results),
        "duration_seconds": round(duration_seconds, 2),
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convierte CSV de Valencia a Parquet.")
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help=f"Directorio de entrada con CSVs (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directorio de salida con parquet (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--categories",
        help="Lista separada por comas de categorías a convertir (por defecto, todas las presentes).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Número de conversiones paralelas (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--log-path",
        default=str(DEFAULT_LOG_PATH),
        help=f"Ruta del log (default: {DEFAULT_LOG_PATH}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reconstruye parquet aunque ya exista y esté actualizado.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not input_dir.exists():
        raise SystemExit(f"No existe el directorio de entrada: {input_dir}")

    logger = build_logger(Path(args.log_path))
    categories = resolve_selected_categories(input_dir, args.categories)
    logger.info("Conversión Valencia CSV -> Parquet")
    logger.info("Entrada: %s", input_dir)
    logger.info("Salida: %s", output_dir)
    logger.info("Categorías: %s", ", ".join(categories))

    start = time.perf_counter()
    plan = build_conversion_plan(input_dir, output_dir, categories)
    results = run_conversion(plan, workers=args.workers, force=args.force, logger=logger)
    summary = build_summary(input_dir, output_dir, categories, results, time.perf_counter() - start)
    summary_path = input_dir / "valencia_parquet_summary.json"
    write_json(summary_path, {"results": results, "summary": summary})

    logger.info(
        "Conversión completada: %s convertidos, %s omitidos, %s fallidos en %.2fs",
        summary["converted"],
        summary["skipped"],
        summary["failed"],
        summary["duration_seconds"],
    )
    logger.info("Resumen: %s", summary_path)
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
