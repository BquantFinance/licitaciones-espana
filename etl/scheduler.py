"""
Configuración y registro de ejecuciones del scheduler ETL.
Schema scheduler: tasks (conjunto, subconjunto, schedule_expr), runs (por ejecución).
Poblado de tasks vía "licitia-etl scheduler register"; registro de runs desde cmd_ingest.
Bucle de ejecución: scheduler run (próxima ejecución según Mensual/Trimestral/Anual, PID file, señales).
"""

import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from zoneinfo import ZoneInfo

SCHEDULER_TZ = ZoneInfo("Europe/Madrid")
SCHEDULER_HOUR = 2  # 02:00 local
SCHEDULER_PID_FILENAME = "licitia-etl-scheduler.pid"
SCHEDULER_LOG_FILENAME = "scheduler.log"
MAX_RUN_DURATION_HOURS = int(os.environ.get("SCHEDULER_MAX_RUN_HOURS", "6"))

VALID_SCHEDULE_EXPRS = ("Diario", "Semanal", "Mensual", "Trimestral", "Semestral", "Anual")


def validate_schedule_expr(expr: Optional[str], default: Optional[str] = None) -> str:
    if expr is None:
        if default is not None and default in VALID_SCHEDULE_EXPRS:
            return default
        raise ValueError(f"Frecuencia no válida: {expr}. Opciones: {VALID_SCHEDULE_EXPRS}")
    if expr not in VALID_SCHEDULE_EXPRS:
        raise ValueError(f"Frecuencia no válida: {expr}. Opciones: {VALID_SCHEDULE_EXPRS}")
    return expr


# Debug session log path (NDJSON). Set DEBUG_LOG_PATH to override; used when present.
_DEBUG_LOG_PATH = os.environ.get("DEBUG_LOG_PATH", "")


def _debug_log(location: str, message: str, data: dict, hypothesis_id: str = "") -> None:
    """Append one NDJSON line to the debug session log file (if DEBUG_LOG_PATH set)."""
    if not _DEBUG_LOG_PATH:
        return
    import json
    payload = {
        "id": f"log_{int(__import__('time').time() * 1000)}",
        "timestamp": int(__import__('time').time() * 1000),
        "location": location,
        "message": message,
        "data": data,
        "hypothesisId": hypothesis_id,
    }
    if os.environ.get("DEBUG_SESSION_ID"):
        payload["sessionId"] = os.environ.get("DEBUG_SESSION_ID")
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except OSError:
        pass


def _scheduler_log_prefix() -> str:
    """Prefijo de timestamp en horario GMT+1 para líneas del scheduler."""
    now = datetime.now(SCHEDULER_TZ)
    return now.strftime("%Y-%m-%d %H:%M:%S GMT+1")


def _log_scheduler(msg: str, detailed: bool = False, exc: Optional[BaseException] = None) -> None:
    """Escribe en scheduler.log con timestamp GMT+1. Si detailed y exc, añade traza del fallo."""
    prefix = f"[{_scheduler_log_prefix()}] "
    log_path = get_scheduler_log_path()
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(prefix + msg + "\n")
            if detailed and exc is not None:
                import traceback
                for line in traceback.format_exception(type(exc), exc, exc.__traceback__):
                    for part in line.rstrip().split("\n"):
                        f.write(prefix + part + "\n")
    except OSError:
        print(prefix + msg, file=sys.stderr)


def get_scheduler_log_path() -> Path:
    """Ruta del archivo de log del scheduler (mismo directorio que el PID file)."""
    return get_scheduler_pid_path().parent / SCHEDULER_LOG_FILENAME

# Frecuencias por defecto (plan Release 1.0 / readme_dataset.md)
# schedule_expr: etiqueta legible; un runner externo puede mapear a cron (ej. Mensual -> 0 2 1 * *)
DEFAULT_SCHEDULE_EXPR: dict[tuple[str, str], str] = {}
# Se rellena en _build_default_schedules() desde CONJUNTOS_REGISTRY + reglas por conjunto


def _build_default_schedules() -> dict[tuple[str, str], str]:
    from etl.ingest_l0 import CONJUNTOS_REGISTRY
    out: dict[tuple[str, str], str] = {}
    for conjunto, reg in CONJUNTOS_REGISTRY.items():
        if conjunto == "test":
            continue
        subconjuntos = reg.get("subconjuntos", ())
        for sub in subconjuntos:
            if conjunto == "nacional":
                out[(conjunto, sub)] = "Mensual"
            elif conjunto == "ted":
                out[(conjunto, sub)] = "Trimestral"
            elif conjunto == "catalunya":
                out[(conjunto, sub)] = "Trimestral"
            elif conjunto == "valencia":
                out[(conjunto, sub)] = "Mensual"
            elif conjunto == "andalucia":
                out[(conjunto, sub)] = "Trimestral"
            elif conjunto == "euskadi":
                out[(conjunto, sub)] = "Trimestral"
            elif conjunto == "madrid":
                out[(conjunto, sub)] = "Trimestral" if sub == "comunidad" else "Anual"
            else:
                out[(conjunto, sub)] = "Trimestral"
    # BORME ingest: separate from CONJUNTOS_REGISTRY (anomalías is on-demand only)
    out[("borme", "ingest")] = "Trimestral"
    return out


def get_default_schedule_expr(conjunto: str, subconjunto: str) -> str:
    if not DEFAULT_SCHEDULE_EXPR:
        DEFAULT_SCHEDULE_EXPR.update(_build_default_schedules())
    return DEFAULT_SCHEDULE_EXPR.get((conjunto, subconjunto), "Trimestral")


def ensure_scheduler_schema(conn: "psycopg2.extensions.connection") -> None:
    """Crea el schema scheduler, tablas y migraciones si no existen (ej. para health check)."""
    from pathlib import Path
    schema_dir = Path(__file__).resolve().parent.parent / "schemas"
    for name in ("008_scheduler.sql", "009_scheduler_runs_pid.sql"):
        path = schema_dir / name
        if path.exists():
            with conn.cursor() as cur:
                cur.execute(path.read_text(encoding="utf-8"))


def get_task_id(conn: "psycopg2.extensions.connection", conjunto: str, subconjunto: str) -> Optional[int]:
    """Devuelve task_id para (conjunto, subconjunto) o None si no existe."""
    with conn.cursor() as cur:
        cur.execute(
            'SELECT task_id FROM scheduler.tasks WHERE conjunto = %s AND subconjunto = %s AND enabled = true',
            (conjunto, subconjunto),
        )
        row = cur.fetchone()
        return row[0] if row else None


def has_running_run(conn: "psycopg2.extensions.connection", task_id: int) -> bool:
    """True si existe un run con status=running para esta tarea (evitar solapamiento)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM scheduler.runs WHERE task_id = %s AND status = 'running' LIMIT 1",
            (task_id,),
        )
        return cur.fetchone() is not None


def insert_run_start(conn: "psycopg2.extensions.connection", task_id: int) -> int:
    """Inserta run con status=running. Devuelve run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO scheduler.runs (task_id, status) VALUES (%s, 'running') RETURNING run_id""",
            (task_id,),
        )
        return cur.fetchone()[0]


def update_run_process_id(conn: "psycopg2.extensions.connection", run_id: int, process_id: int) -> None:
    """Registra el PID del proceso que ejecuta esta run (ingest)."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE scheduler.runs SET process_id = %s WHERE run_id = %s",
            (process_id, run_id),
        )


def update_run_progress(
    conn: "psycopg2.extensions.connection",
    run_id: int,
    rows_inserted: int,
    rows_omitted: int,
    error_message: Optional[str] = None,
) -> None:
    """Update cumulative progress on a running run (does NOT set finished_at or change status)."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE scheduler.runs
               SET rows_inserted = %s, rows_omitted = %s, error_message = %s
               WHERE run_id = %s AND status = 'running'""",
            (rows_inserted, rows_omitted, error_message, run_id),
        )


def update_run_finish(
    conn: "psycopg2.extensions.connection",
    run_id: int,
    status: str,
    rows_inserted: Optional[int] = None,
    rows_omitted: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    """Actualiza run con finished_at, status y opcionalmente filas/error."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE scheduler.runs
               SET finished_at = NOW(), status = %s, rows_inserted = %s, rows_omitted = %s, error_message = %s
               WHERE run_id = %s""",
            (status, rows_inserted, rows_omitted, error_message, run_id),
        )


def recover_stale_runs(conn: "psycopg2.extensions.connection") -> int:
    """Detect orphaned 'running' runs (dead PID or exceeded max duration) and mark them failed.

    Called on API startup and at each scheduler loop tick to self-heal after
    container restarts or process crashes.  Returns the number of recovered runs.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT run_id, process_id, started_at, task_id FROM scheduler.runs WHERE status = 'running'"
        )
        stale = cur.fetchall()

    if not stale:
        return 0

    recovered = 0
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=MAX_RUN_DURATION_HOURS)

    for row in stale:
        pid = row.get("process_id")
        started = row.get("started_at")
        reason: str | None = None

        if pid:
            try:
                os.kill(pid, 0)
                if started and started < cutoff:
                    reason = f"Timeout: running for >{MAX_RUN_DURATION_HOURS}h"
                else:
                    continue
            except ProcessLookupError:
                reason = f"Process {pid} not found (container restarted?)"
            except PermissionError:
                continue
        else:
            if started and started < cutoff:
                reason = f"Timeout: no PID recorded, running >{MAX_RUN_DURATION_HOURS}h"
            else:
                continue

        if reason:
            update_run_finish(conn, row["run_id"], "failed", error_message=reason)
            recovered += 1

    return recovered


def register_tasks(
    conn: "psycopg2.extensions.connection",
    conjuntos: Optional[list[str]] = None,
    task_pairs: Optional[list[tuple[str, str]]] = None,
) -> tuple[int, int, list[tuple[str, str]]]:
    """
    Inserta o actualiza scheduler.tasks desde CONJUNTOS_REGISTRY y frecuencias por defecto.
    Si task_pairs es proporcionado, solo registra esos (conjunto, subconjunto) exactos.
    Si conjuntos no es None, solo se registran tareas de esos conjuntos.
    ON CONFLICT (conjunto, subconjunto) DO UPDATE schedule_expr, updated_at.
    Devuelve (insertadas, actualizadas, lista de (conjunto, subconjunto) registrados).
    """
    schedules = _build_default_schedules()
    if task_pairs:
        pairs_to_register = {(c, s): schedules.get((c, s), "Trimestral") for c, s in task_pairs}
    elif conjuntos is not None:
        conjuntos_set = set(conjuntos)
        pairs_to_register = {k: v for k, v in schedules.items() if k[0] in conjuntos_set}
    else:
        pairs_to_register = schedules
    inserted, updated = 0, 0
    registered: list[tuple[str, str]] = []
    with conn.cursor() as cur:
        for (conjunto, subconjunto), schedule_expr in pairs_to_register.items():
            cur.execute(
                """INSERT INTO scheduler.tasks (conjunto, subconjunto, schedule_expr, enabled, updated_at)
                   VALUES (%s, %s, %s, true, NOW())
                   ON CONFLICT (conjunto, subconjunto)
                   DO UPDATE SET schedule_expr = EXCLUDED.schedule_expr, updated_at = NOW()""",
                (conjunto, subconjunto, schedule_expr),
            )
            registered.append((conjunto, subconjunto))
            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1
    return inserted, updated, registered


def get_run_by_process_id(conn: "psycopg2.extensions.connection", process_id: int) -> Optional[dict[str, Any]]:
    """Devuelve la run (run_id, task_id, conjunto, subconjunto) con process_id dado, o None."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """SELECT r.run_id, r.task_id, t.conjunto, t.subconjunto
               FROM scheduler.runs r
               JOIN scheduler.tasks t ON t.task_id = r.task_id
               WHERE r.process_id = %s
               ORDER BY r.started_at DESC
               LIMIT 1""",
            (process_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def delete_task(conn: "psycopg2.extensions.connection", task_id: int) -> None:
    """Elimina la tarea (y sus runs por CASCADE)."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scheduler.tasks WHERE task_id = %s", (task_id,))


def list_tasks_with_last_run(conn: "psycopg2.extensions.connection") -> list[dict[str, Any]]:
    """Lista tareas con última ejecución (para scheduler status)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT t.task_id, t.conjunto, t.subconjunto, t.schedule_expr, t.enabled, t.created_at,
                   r.run_id AS last_run_id, r.started_at AS last_started_at, r.finished_at AS last_finished_at,
                   r.status AS last_status, r.rows_inserted AS last_rows_inserted, r.rows_omitted AS last_rows_omitted,
                   r.process_id AS last_process_id, r.error_message AS last_error_message
            FROM scheduler.tasks t
            LEFT JOIN LATERAL (
              SELECT run_id, started_at, finished_at, status, rows_inserted, rows_omitted, process_id, error_message
              FROM scheduler.runs
              WHERE task_id = t.task_id
              ORDER BY started_at DESC
              LIMIT 1
            ) r ON true
            ORDER BY t.conjunto, t.subconjunto
        """)
        return [dict(row) for row in cur.fetchall()]


def list_running_runs(conn: "psycopg2.extensions.connection") -> list[dict[str, Any]]:
    """List all runs with status='running', including task metadata."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT r.run_id, r.task_id, t.conjunto, t.subconjunto, r.process_id, r.started_at
            FROM scheduler.runs r
            JOIN scheduler.tasks t ON t.task_id = r.task_id
            WHERE r.status = 'running'
            ORDER BY r.started_at DESC
        """)
        return [dict(row) for row in cur.fetchall()]


def get_current_running_run(conn: "psycopg2.extensions.connection") -> Optional[dict[str, Any]]:
    """Devuelve la run activa más reciente (status=running) con task metadata, o None."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT r.run_id, r.task_id, r.started_at, r.status, r.process_id,
                   r.rows_inserted, r.rows_omitted, r.error_message,
                   t.conjunto, t.subconjunto
            FROM scheduler.runs r
            JOIN scheduler.tasks t ON t.task_id = r.task_id
            WHERE r.status = 'running'
            ORDER BY r.started_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return dict(row) if row else None


def stop_runs_by_ids(
    conn: "psycopg2.extensions.connection", run_ids: list[int]
) -> tuple[int, list[str]]:
    """
    Mark runs as failed (stopped by user). Optionally signal the process if process_id is set.
    Returns (number of runs stopped, list of error messages for any failures).
    """
    if not run_ids:
        return 0, []
    stopped = 0
    errors: list[str] = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT run_id, process_id, status FROM scheduler.runs WHERE run_id = ANY(%s)",
            (run_ids,),
        )
        rows = cur.fetchall()
    for row in rows:
        run_id = row["run_id"]
        pid = row.get("process_id")
        status = row.get("status")
        if status != "running":
            continue
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError:
                errors.append(f"run_id={run_id}: cannot signal process {pid}")
            except Exception as e:
                errors.append(f"run_id={run_id}: {e}")
        try:
            update_run_finish(
                conn, run_id, "failed", error_message="Stopped by user"
            )
            stopped += 1
        except Exception as e:
            errors.append(f"run_id={run_id}: {e}")
    return stopped, errors


def get_scheduler_pid_path() -> Path:
    """Ruta del PID file del scheduler (LICITACIONES_TMP_DIR o /tmp)."""
    base = os.environ.get("LICITACIONES_TMP_DIR", "")
    if not base:
        base = "/tmp"
    return Path(base) / SCHEDULER_PID_FILENAME


def is_scheduler_loop_running() -> bool:
    """Return True if the scheduler loop process is alive (PID file exists and process responds)."""
    path = get_scheduler_pid_path()
    if not path.exists():
        return False
    try:
        pid = int(path.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, OSError):
        return False


def get_next_run_at(
    schedule_expr: str,
    last_finished_at: Optional[datetime],
    reference_now: Optional[datetime] = None,
) -> datetime:
    """
    Calcula la próxima ejecución según schedule_expr (ver VALID_SCHEDULE_EXPRS) y la última finalización.
    Hora fija 02:00 Europe/Madrid. Si last_finished_at es None (nunca ejecutada), devuelve reference_now o now
    para que la comparación next_at <= now en get_tasks_due sea coherente (mismo instante).
    """
    from datetime import date
    # Optional override for local testing (e.g. SCHEDULER_NOW_OVERRIDE=2026-03-02T10:00:00)
    now_override = os.environ.get("SCHEDULER_NOW_OVERRIDE")
    if now_override:
        try:
            from datetime import datetime as dt_parse
            parsed = dt_parse.fromisoformat(now_override.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=SCHEDULER_TZ)
            reference_now = parsed.astimezone(SCHEDULER_TZ)
        except (ValueError, TypeError):
            pass
    now = reference_now if reference_now is not None else datetime.now(SCHEDULER_TZ)
    # #region agent log
    _debug_log(
        "scheduler.py:get_next_run_at",
        "get_next_run_at entry",
        {
            "schedule_expr": schedule_expr,
            "last_finished_at": last_finished_at.isoformat() if last_finished_at else None,
            "now": now.isoformat(),
        },
        "H2",
    )
    # #endregion
    if last_finished_at is None:
        # #region agent log
        _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (no last run)", {"result": now.isoformat()}, "H2")
        # #endregion
        return now
    last_fin_tz = last_finished_at.astimezone(SCHEDULER_TZ)
    ref = last_fin_tz.date()
    year, month = ref.year, ref.month
    expr = (schedule_expr or "").strip().lower()
    # Next run = first scheduled slot *after* last_finished_at (not after "now"), so we don't skip the due slot
    if expr == "diario":
        cand = datetime(last_fin_tz.year, last_fin_tz.month, last_fin_tz.day,
                        SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        if cand > last_fin_tz:
            return cand
        next_day = last_fin_tz.date() + timedelta(days=1)
        return datetime(next_day.year, next_day.month, next_day.day,
                        SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
    if expr == "semanal":
        # Monday = weekday 0
        days_since_monday = last_fin_tz.weekday()  # 0=Mon..6=Sun
        this_monday = last_fin_tz.date() - timedelta(days=days_since_monday)
        cand = datetime(this_monday.year, this_monday.month, this_monday.day,
                        SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        if cand > last_fin_tz:
            return cand
        next_monday = this_monday + timedelta(days=7)
        return datetime(next_monday.year, next_monday.month, next_monday.day,
                        SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
    if expr == "mensual":
        cand = datetime(year, month, 1, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        if cand > last_fin_tz:
            # #region agent log
            _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (mensual cand)", {"result": cand.isoformat()}, "H2")
            # #endregion
            return cand
        if month == 12:
            next_d = date(year + 1, 1, 1)
        else:
            next_d = date(year, month + 1, 1)
        res = datetime(next_d.year, next_d.month, next_d.day, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        # #region agent log
        _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (mensual next)", {"result": res.isoformat()}, "H2")
        # #endregion
        return res
    if expr == "trimestral":
        # Día 1 de ene/abr/jul/oct — first quarter slot strictly after last_finished_at
        quarters = [date(year, m, 1) for m in (1, 4, 7, 10)]
        for d in quarters:
            cand = datetime(d.year, d.month, d.day, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
            if cand > last_fin_tz:
                # #region agent log
                _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (trimestral)", {"result": cand.isoformat()}, "H2")
                # #endregion
                return cand
        res = datetime(year + 1, 1, 1, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        # #region agent log
        _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (trimestral next year)", {"result": res.isoformat()}, "H2")
        # #endregion
        return res
    if expr == "semestral":
        semesters = [date(year, 1, 1), date(year, 7, 1)]
        for d in semesters:
            cand = datetime(d.year, d.month, d.day, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
            if cand > last_fin_tz:
                return cand
        return datetime(year + 1, 1, 1, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
    if expr == "anual":
        cand = datetime(year, 1, 1, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        if cand > last_fin_tz:
            # #region agent log
            _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (anual)", {"result": cand.isoformat()}, "H2")
            # #endregion
            return cand
        res = datetime(year + 1, 1, 1, SCHEDULER_HOUR, 0, 0, tzinfo=SCHEDULER_TZ)
        # #region agent log
        _debug_log("scheduler.py:get_next_run_at", "get_next_run_at exit (anual next)", {"result": res.isoformat()}, "H2")
        # #endregion
        return res
    # Por defecto: trimestral
    return get_next_run_at("Trimestral", last_finished_at, reference_now=now)


def get_tasks_due(conn: "psycopg2.extensions.connection") -> list[dict[str, Any]]:
    """Tareas habilitadas cuya próxima ejecución es <= ahora y no tienen run en 'running'. Incluye next_run_at."""
    now_override = os.environ.get("SCHEDULER_NOW_OVERRIDE")
    if now_override:
        try:
            from datetime import datetime as dt_parse
            parsed = dt_parse.fromisoformat(now_override.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=SCHEDULER_TZ)
            now = parsed.astimezone(SCHEDULER_TZ)
        except (ValueError, TypeError):
            now = datetime.now(SCHEDULER_TZ)
    else:
        now = datetime.now(SCHEDULER_TZ)
    tasks = list_tasks_with_last_run(conn)
    due = []
    # #region agent log
    _debug_log(
        "scheduler.py:get_tasks_due",
        "get_tasks_due entry",
        {"now": now.isoformat(), "total_tasks": len(tasks)},
        "H3",
    )
    # #endregion
    for t in tasks:
        if not t.get("enabled"):
            # #region agent log
            _debug_log("scheduler.py:get_tasks_due", "task skipped", {"conjunto": t.get("conjunto"), "subconjunto": t.get("subconjunto"), "reason": "enabled=false"}, "H3")
            # #endregion
            continue
        if has_running_run(conn, t["task_id"]):
            # #region agent log
            _debug_log("scheduler.py:get_tasks_due", "task skipped", {"conjunto": t.get("conjunto"), "subconjunto": t.get("subconjunto"), "reason": "has_running_run"}, "H3")
            # #endregion
            continue
        last_fin = t.get("last_finished_at")
        next_at = get_next_run_at(t.get("schedule_expr") or "Trimestral", last_fin, reference_now=now)
        if next_at <= now:
            row = dict(t)
            row["next_run_at"] = next_at
            due.append(row)
            # #region agent log
            _debug_log("scheduler.py:get_tasks_due", "task due", {"conjunto": t.get("conjunto"), "subconjunto": t.get("subconjunto"), "next_run_at": next_at.isoformat()}, "H3")
            # #endregion
        else:
            # #region agent log
            _debug_log("scheduler.py:get_tasks_due", "task skipped (next_at > now)", {"conjunto": t.get("conjunto"), "subconjunto": t.get("subconjunto"), "next_run_at": next_at.isoformat(), "now": now.isoformat()}, "H3")
            # #endregion
    # #region agent log
    _debug_log("scheduler.py:get_tasks_due", "get_tasks_due exit", {"due_count": len(due)}, "H3")
    # #endregion
    return due


def _build_task_cmd(conjunto: str, subconjunto: str) -> list[str]:
    """Build the CLI command list for a scheduled task."""
    from datetime import date
    if conjunto == "borme":
        y = date.today().year
        return [sys.executable, "-m", "etl.cli", "borme", "ingest", "--anos", f"{y}-{y}"]
    cmd = [sys.executable, "-m", "etl.cli", "ingest", conjunto, subconjunto]
    if conjunto in ("nacional", "ted"):
        y = date.today().year
        cmd.extend(["--anos", f"{y}-{y}"])
    return cmd


def run_single_task(db_url: str, conjunto: str, subconjunto: str) -> int:
    """Ejecuta una sola run de ingest para (conjunto, subconjunto). Devuelve el código de salida del ingest."""
    import subprocess as sp
    cmd = _build_task_cmd(conjunto, subconjunto)
    try:
        rc = sp.run(cmd, check=False)
        return int(rc.returncode) if rc.returncode is not None else 0
    except Exception:
        return 1


def run_scheduler_loop(
    db_url: str,
    tick_seconds: int = 60,
    pid_path: Optional[Path] = None,
) -> None:
    """
    Bucle del scheduler: cada tick_seconds segundos comprueba tareas debidas, lanza ingest por subprocess
    y escribe/elimina PID file. Al recibir SIGTERM/SIGINT termina el paso actual y sale.
    Si ya existe un proceso con el PID del archivo y está vivo, sale con mensaje.
    """
    path = pid_path or get_scheduler_pid_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        try:
            pid = int(path.read_text().strip())
            if pid:
                os.kill(pid, 0)
                _log_scheduler("El scheduler ya está en ejecución.")
                sys.exit(1)
        except (ValueError, OSError):
            pass
    path.write_text(str(os.getpid()))
    shutdown = False

    def _signal_handler(_sig: int, _frame: Any) -> None:
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    import time as _time
    tick_count = 0
    try:
        while not shutdown:
            tick_count += 1
            # #region agent log
            _debug_log("scheduler.py:run_scheduler_loop", "scheduler tick", {"tick_count": tick_count}, "H1")
            # #endregion
            try:
                with psycopg2.connect(db_url) as conn:
                    conn.autocommit = False
                    recovered = recover_stale_runs(conn)
                    conn.commit()
                    if recovered:
                        _log_scheduler(f"Recuperados {recovered} run(s) huérfanos.")
                with psycopg2.connect(db_url) as conn:
                    due = get_tasks_due(conn)
                # #region agent log
                _debug_log("scheduler.py:run_scheduler_loop", "get_tasks_due result", {"due_count": len(due)}, "H5")
                # #endregion
                for task in due:
                    if shutdown:
                        break
                    conjunto = task["conjunto"]
                    subconjunto = task["subconjunto"]
                    cmd = _build_task_cmd(conjunto, subconjunto)
                    try:
                        p = subprocess.Popen(cmd)
                        p.wait()
                        if p.returncode == 0:
                            _log_scheduler(
                                f"Tarea ejecutada correctamente. PID={p.pid} conjunto={conjunto} subconjunto={subconjunto}"
                            )
                        else:
                            _log_scheduler(
                                f"Tarea falló. PID={p.pid} conjunto={conjunto} subconjunto={subconjunto} code={p.returncode}"
                            )
                    except Exception as e:
                        _log_scheduler(
                            f"Tarea falló. conjunto={conjunto} subconjunto={subconjunto}: {e}",
                            detailed=True,
                            exc=e,
                        )
                    if shutdown:
                        break
            except Exception as e:
                _log_scheduler(f"Error en el bucle del scheduler: {e}", detailed=True, exc=e)
            for _ in range(tick_seconds):
                if shutdown:
                    break
                _time.sleep(1)
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
