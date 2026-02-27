"""Per-service schema migration tracking.

Bootstraps a scheduler.schema_migrations table, compares on-disk SQL files
against recorded checksums, and applies pending migrations on demand.
"""

import hashlib
import logging
from pathlib import Path
from typing import NamedTuple

import psycopg2
import psycopg2.extras

from etl.config import get_database_url

logger = logging.getLogger("etl.schema_check")

_BOOTSTRAP_DDL = """\
CREATE SCHEMA IF NOT EXISTS scheduler;

CREATE TABLE IF NOT EXISTS scheduler.schema_migrations (
    id          BIGSERIAL PRIMARY KEY,
    filename    TEXT NOT NULL UNIQUE,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by  TEXT NOT NULL,
    success     BOOLEAN NOT NULL DEFAULT TRUE,
    error_msg   TEXT
);

CREATE OR REPLACE VIEW scheduler.schema_version AS
SELECT
    COUNT(*)            AS migrations_applied,
    MAX(applied_at)     AS last_migration_at,
    MAX(applied_by)     AS current_version,
    BOOL_AND(success)   AS all_healthy
FROM scheduler.schema_migrations;
"""


class MigrationStatus(NamedTuple):
    pending: list[str]
    applied: list[str]
    tampered: list[str]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _get_version() -> str:
    from etl import __version__
    return f"etl:{__version__}"


def _schemas_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "schemas"


def _sql_files(schemas_dir: Path | None = None) -> list[str]:
    d = schemas_dir or _schemas_dir()
    return sorted(f.name for f in d.iterdir() if f.suffix == ".sql")


def bootstrap(conn) -> None:
    """Ensure the migration tracking table exists (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(_BOOTSTRAP_DDL)
    conn.commit()


def check(conn, schemas_dir: Path | None = None) -> MigrationStatus:
    """Compare on-disk SQL files against the migration table."""
    d = schemas_dir or _schemas_dir()
    files = _sql_files(d)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT filename, checksum FROM scheduler.schema_migrations WHERE success = TRUE"
        )
        applied_map = {r["filename"]: r["checksum"] for r in cur.fetchall()}

    pending, applied, tampered = [], [], []
    for f in files:
        if f not in applied_map:
            pending.append(f)
        elif applied_map[f] != sha256(d / f):
            tampered.append(f)
        else:
            applied.append(f)

    return MigrationStatus(pending=pending, applied=applied, tampered=tampered)


def log_check(status: MigrationStatus) -> None:
    """Log startup migration check results with structured prefixes."""
    version = _get_version()
    total = len(status.pending) + len(status.applied) + len(status.tampered)
    parts = [f"{len(status.applied)} applied"]
    if status.pending:
        parts.append(f"{len(status.pending)} pending ({', '.join(status.pending)})")
    if status.tampered:
        parts.append(f"{len(status.tampered)} tampered ({', '.join(status.tampered)})")
    logger.info("[schema-check] %s — %d migrations: %s", version, total, ", ".join(parts))
    for f in status.tampered:
        logger.warning("[schema-check] WARNING %s — checksum mismatch: %s", version, f)


def record(conn, filename: str, checksum: str, success: bool = True, error_msg: str | None = None) -> None:
    """Record a migration result in scheduler.schema_migrations."""
    version = _get_version()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scheduler.schema_migrations (filename, checksum, applied_by, success, error_msg) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (filename) DO UPDATE SET checksum = EXCLUDED.checksum, "
            "applied_at = NOW(), applied_by = EXCLUDED.applied_by, success = EXCLUDED.success, error_msg = EXCLUDED.error_msg",
            (filename, checksum, version, success, error_msg),
        )
    conn.commit()


def apply_pending(conn, schemas_dir: Path | None = None) -> list[dict]:
    """Apply all pending migrations and record them.

    Note: cmd_init_db uses its own apply loop to support pre/post hooks.
    This function is for the simple case (API trigger, no hooks).
    """
    d = schemas_dir or _schemas_dir()
    status = check(conn, d)
    version = _get_version()
    results = []

    for filename in status.pending:
        path = d / filename
        checksum = sha256(path)
        sql = path.read_text(encoding="utf-8")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            record(conn, filename, checksum)
            logger.info("[schema-apply] %s — applied %s (SHA256: %s) [OK]", version, filename, checksum[:12])
            results.append({"filename": filename, "checksum": checksum, "success": True})
        except Exception as e:
            conn.rollback()
            err = str(e)
            try:
                record(conn, filename, checksum, success=False, error_msg=err)
            except Exception:
                pass
            logger.error("[schema-apply] %s — FAILED %s: %s", version, filename, err)
            results.append({"filename": filename, "checksum": checksum, "success": False, "error": err})

    return results


def get_rows(conn) -> list[dict]:
    """Return all migration rows for the /migrations endpoint."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT filename, checksum, applied_at, applied_by, success, error_msg "
            "FROM scheduler.schema_migrations ORDER BY id"
        )
        rows = cur.fetchall()
    return [
        {**r, "applied_at": r["applied_at"].isoformat() if r["applied_at"] else None}
        for r in rows
    ]
