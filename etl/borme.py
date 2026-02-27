"""
BORME integration: scrape, parse, ingest into borme schema, and anomaly detection.
Pipeline tree:
  Step 1 (scraper) → Step 2 (parser) ─┬─► ingest: load into borme.empresas / borme.cargos
                                       └─► anomalias: [optional anonymize] → match vs L0 nacional
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

from etl.config import get_database_url

logger = logging.getLogger("etl.borme")

_ETL_ROOT = Path(__file__).resolve().parent.parent


def get_borme_schema() -> str:
    return os.environ.get("BORME_SCHEMA", "borme")


def get_borme_tmp_dir() -> Path:
    raw = os.environ.get("LICITACIONES_TMP_DIR", "")
    if not raw:
        raw = str(_ETL_ROOT / "tmp")
    return Path(raw) / "borme"


def anos_to_date_range(anos: str) -> tuple[str, str]:
    """Convert '2020-2023' or '2024' to (start_date, end_date) strings."""
    parts = anos.split("-")
    if len(parts) == 2 and len(parts[0]) == 4 and len(parts[1]) == 4:
        return f"{parts[0]}-01-01", f"{parts[1]}-12-31"
    elif len(parts) == 1 and len(parts[0]) == 4:
        return f"{parts[0]}-01-01", f"{parts[0]}-12-31"
    else:
        return f"{parts[0]}-01-01", f"{parts[-1]}-12-31"


def run_scraper(anos: str) -> Path:
    """Run borme_scraper.py with date range. Returns output dir."""
    start, end = anos_to_date_range(anos)
    output_dir = get_borme_tmp_dir() / "pdfs"
    output_dir.mkdir(parents=True, exist_ok=True)
    script = _ETL_ROOT / "borme" / "scripts" / "borme_scraper.py"
    cmd = [
        sys.executable, str(script),
        "--start", start, "--end", end,
        "--output", str(output_dir), "--resume",
    ]
    logger.info("BORME scraper: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return output_dir


def run_parser(pdfs_dir: Path) -> Path:
    """Run borme_batch_parser.py. Returns output dir with parquets."""
    output_dir = get_borme_tmp_dir() / "parsed"
    output_dir.mkdir(parents=True, exist_ok=True)
    script = _ETL_ROOT / "borme" / "scripts" / "borme_batch_parser.py"
    cmd = [
        sys.executable, str(script),
        "--input", str(pdfs_dir), "--output", str(output_dir), "--resume",
    ]
    logger.info("BORME parser: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return output_dir


def run_anonymize(parsed_dir: Path) -> Path:
    """Run borme_anonymize.py. Returns output dir."""
    output_dir = get_borme_tmp_dir() / "anonymized"
    output_dir.mkdir(parents=True, exist_ok=True)
    script = _ETL_ROOT / "borme" / "scripts" / "borme_anonymize.py"
    cmd = [
        sys.executable, str(script),
        "--input", str(parsed_dir), "--output", str(output_dir),
    ]
    logger.info("BORME anonymize: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return output_dir


def run_anomalias(parsed_dir: Path, anonimizar: bool = False) -> Path:
    """Run anomaly detector. Optionally anonymize first."""
    source_dir = parsed_dir
    if anonimizar:
        source_dir = run_anonymize(parsed_dir)

    output_dir = get_borme_tmp_dir() / "anomalias"
    output_dir.mkdir(parents=True, exist_ok=True)

    placsp_parquet = _export_placsp_to_parquet()

    script = _ETL_ROOT / "borme" / "scripts" / "borme_placsp_match.py"
    cmd = [
        sys.executable, str(script),
        "--borme", str(source_dir),
        "--placsp", str(placsp_parquet),
        "--output", str(output_dir),
    ]
    logger.info("BORME anomalias: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return output_dir


def _export_placsp_to_parquet() -> Path:
    """Query L0 nacional_licitaciones and write to a temp parquet for the match script."""
    db_url = get_database_url()
    if not db_url:
        raise ValueError("Database not configured")
    l0_schema = os.environ.get("L0_DB_SCHEMA", os.environ.get("DB_SCHEMA", "l0"))
    output = get_borme_tmp_dir() / "placsp_export.parquet"
    query = f"""
        SELECT id, expediente, objeto, organo_contratante,
               tipo_contrato, procedimiento, estado,
               importe_sin_iva, importe_con_iva,
               importe_adjudicacion, importe_adj_con_iva,
               adjudicatario, nif_adjudicatario,
               num_ofertas, fecha_adjudicacion, fecha_publicacion,
               nuts, urgencia
        FROM "{l0_schema}".nacional_licitaciones
        WHERE adjudicatario IS NOT NULL
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    with psycopg2.connect(db_url) as conn:
        df = pd.read_sql(query, conn)
    df.to_parquet(str(output), index=False)
    logger.info("Exported %d PLACSP rows to %s", len(df), output)
    return output


def load_borme_to_db(parsed_dir: Path, subconjunto: str) -> tuple[int, int]:
    """Load parsed BORME parquet into borme schema tables."""
    db_url = get_database_url()
    if not db_url:
        raise ValueError("Database not configured")
    schema = get_borme_schema()

    if subconjunto == "empresas":
        parquet = parsed_dir / "borme_empresas.parquet"
        table = "empresas"
        conflict_cols = "(fecha_borme, num_entrada, empresa_norm)"
    elif subconjunto == "cargos":
        parquet = parsed_dir / "borme_cargos.parquet"
        table = "cargos"
        conflict_cols = "(fecha_borme, num_entrada, cargo, persona, tipo_acto)"
    else:
        raise ValueError(f"Unknown BORME subconjunto: {subconjunto}")

    if not parquet.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet}")

    df = pd.read_parquet(str(parquet))
    logger.info("Loading %d rows into %s.%s", len(df), schema, table)

    with psycopg2.connect(db_url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        cols = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(f'"{c}"' for c in cols)
        insert_sql = (
            f'INSERT INTO "{schema}"."{table}" ({col_names}) '
            f'VALUES ({placeholders}) '
            f'ON CONFLICT {conflict_cols} DO NOTHING'
        )

        rows = df.where(df.notna(), None).values.tolist()
        inserted = 0
        skipped = 0
        batch_size = 1000
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for row in batch:
                    cur.execute(insert_sql, row)
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
        conn.commit()

    logger.info("Loaded %s.%s: inserted=%d, skipped=%d", schema, table, inserted, skipped)
    return inserted, skipped
