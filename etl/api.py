import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from etl.cli import cmd_scheduler_register, cmd_scheduler_run, cmd_scheduler_stop, _comprobar_base_datos
from etl.config import get_database_url
from etl.ingest_l0 import CONJUNTOS_REGISTRY
from etl.scheduler import (
    VALID_SCHEDULE_EXPRS,
    _build_default_schedules,
    delete_task,
    ensure_scheduler_schema,
    get_current_running_run,
    get_next_run_at,
    get_task_id,
    is_scheduler_loop_running,
    list_running_runs,
    list_tasks_with_last_run,
    recover_stale_runs,
    register_tasks,
    stop_runs_by_ids,
    validate_schedule_expr,
)


def _ingest_log_path() -> Path:
    return Path(os.environ.get("LICITACIONES_TMP_DIR", "/tmp")) / "ingest.log"


def _borme_log_path() -> Path:
    return Path(os.environ.get("LICITACIONES_TMP_DIR", "/tmp")) / "borme.log"


class IngestRunBody(BaseModel):
    """Body for POST /ingest/run. Same options as CLI ingest command."""

    conjunto: str
    subconjunto: str | None = None
    anos: str | None = None
    solo_descargar: bool = False
    solo_procesar: bool = False


class SchedulerRegisterBody(BaseModel):
    """Body for POST /scheduler/register. Optional list of conjuntos; if empty, register all."""

    conjuntos: list[str] = []
    tasks: list[dict] = []
    schedule_expr: str | None = None


class SchedulerRunBody(BaseModel):
    """Body for POST /scheduler/run. Optional conjunto/subconjunto for single task; detach to run loop in background."""

    conjunto: str | None = None
    subconjunto: str | None = None
    detach: bool = False


class SchedulerRunsStopBody(BaseModel):
    run_ids: list[int]


class SchedulerUnregisterBody(BaseModel):
    tasks: list[dict]


class BormeIngestBody(BaseModel):
    anos: str


class BormeAnomaliasBody(BaseModel):
    anos: str
    anonimizar: bool = False

app = FastAPI(
    title="ETL API",
    version="1.2.1",
    description="Microservicio ETL: ingest L0, scheduler, BORME.",
)


@app.on_event("startup")
def _startup_recover_stale_runs():
    """Auto-heal stale 'running' scheduler records left by dead containers."""
    db_url = get_database_url()
    if not db_url:
        return
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            count = recover_stale_runs(conn)
            conn.commit()
            if count:
                print(f"[startup] Recovered {count} stale scheduler run(s).")
    except Exception as e:
        print(f"[startup] Could not recover stale runs: {e}")


@app.on_event("startup")
def _startup_schema_check():
    """Detect-only schema migration check on boot (no auto-apply)."""
    from etl import schema_check
    from etl.cli import INIT_MIGRATIONS
    url = get_database_url()
    if not url:
        return
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        try:
            schema_check.bootstrap(conn)
            status = schema_check.check(conn)
            schema_check.log_check(status)
            init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
            app.state.migration_status = {
                "pending": len(status.pending),
                "pending_infra": len(init_pending),
                "applied": len(status.applied),
                "tampered": len(status.tampered),
            }
        finally:
            conn.close()
    except Exception:
        app.state.migration_status = None


def _serialize_row(row: dict) -> dict:
    """Convert a row dict to JSON-serializable form (datetime -> ISO string)."""
    out = dict(row)
    for key in ("started_at", "finished_at", "last_started_at", "last_finished_at", "next_run_at", "created_at"):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out


@app.get("/health", summary="Health check", description="Liveness, DB connectivity, and migration status.")
def health():
    db_url = get_database_url()
    db_ok = False
    if db_url:
        try:
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            db_ok = True
        except Exception:
            pass
    migrations = getattr(app.state, "migration_status", None)
    return {"status": "ok", "db": db_ok, "migrations": migrations}


@app.get("/migrations", summary="Migration audit trail", description="Returns all recorded schema migrations.")
def get_migrations():
    from etl import schema_check
    from etl.cli import INIT_MIGRATIONS
    db_url = get_database_url()
    if not db_url:
        return JSONResponse(status_code=503, content={"detail": "Database not configured"})
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        try:
            status = schema_check.check(conn)
            rows = schema_check.get_rows(conn)
        finally:
            conn.close()
        from etl import __version__
        init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
        return {
            "service": "etl",
            "version": __version__,
            "status": {
                "pending": len(status.pending),
                "pending_infra": len(init_pending),
                "applied": len(status.applied),
                "tampered": len(status.tampered),
            },
            "pending_files": status.pending,
            "tampered_files": status.tampered,
            "migrations": rows,
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"detail": str(e)})


@app.post("/init-db", summary="Apply init-db migrations", description="Applies infrastructure migrations (dim, scheduler). Returns results for dashboard.")
def post_init_db():
    """Run init-db logic and return JSON for the frontend migration apply flow."""
    from etl import schema_check
    from etl.cli import run_init_db, _schema_dir, INIT_MIGRATIONS
    exit_code, results = run_init_db(schema_dir=_schema_dir())
    # Refresh migration status so next /health reflects applied count
    db_url = get_database_url()
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            try:
                schema_check.bootstrap(conn)
                status = schema_check.check(conn)
                init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
                app.state.migration_status = {
                    "pending": len(status.pending),
                    "pending_infra": len(init_pending),
                    "applied": len(status.applied),
                    "tampered": len(status.tampered),
                }
            finally:
                conn.close()
        except Exception:
            pass
    failed = [r for r in results if not r.get("success")]
    return {
        "ok": exit_code == 0,
        "message": f"Init-db: {len(results)} processed, {len(failed)} failed.",
        "results": results,
    }


@app.get(
    "/ingest/conjuntos",
    summary="List ingest conjuntos and subconjuntos",
    description="Returns the same conjunto/subconjunto catalog as the CLI (ingest --subconjuntos).",
)
def ingest_conjuntos():
    """List available conjuntos and their subconjuntos for ingest."""
    conjuntos = [
        {
            "id": cid,
            "subconjuntos": list(reg.get("subconjuntos", [])),
        }
        for cid, reg in sorted(CONJUNTOS_REGISTRY.items())
        if cid != "test"
    ]
    return {"conjuntos": conjuntos}


@app.post(
    "/ingest/run",
    summary="Run ingest for a conjunto/subconjunto (non-blocking)",
    description="Validates inputs then spawns the CLI ingest command as a background process. "
    "Returns immediately with 200. The subprocess registers its own scheduler run "
    "and updates it on completion. Poll GET /ingest/current-run for status and "
    "GET /ingest/log for live subprocess output.",
)
def ingest_run(body: IngestRunBody):
    """Spawn a background ingest process for the given conjunto/subconjunto."""
    conjunto = (body.conjunto or "").strip().lower()
    if not conjunto:
        return JSONResponse(
            status_code=422,
            content={"detail": "conjunto is required"},
        )
    if conjunto not in CONJUNTOS_REGISTRY or conjunto == "test":
        return JSONResponse(
            status_code=422,
            content={
                "detail": f"Conjunto no reconocido: {conjunto}. Admitidos: {', '.join(sorted(c for c in CONJUNTOS_REGISTRY if c != 'test'))}.",
            },
        )
    reg = CONJUNTOS_REGISTRY[conjunto]
    subconjuntos = list(reg.get("subconjuntos", []))
    subconjunto = (body.subconjunto or "").strip() or None
    if subconjunto is None and len(subconjuntos) == 1:
        subconjunto = subconjuntos[0]
    if subconjunto is None and len(subconjuntos) > 1:
        return JSONResponse(
            status_code=422,
            content={"detail": f"Para '{conjunto}' indique subconjunto. Válidos: {', '.join(subconjuntos)}."},
        )
    if subconjunto and subconjunto not in subconjuntos:
        return JSONResponse(
            status_code=422,
            content={"detail": f"Subconjunto no reconocido para {conjunto}: {subconjunto}."},
        )
    anos = (body.anos or "").strip() or ""
    if reg.get("requires_anos") and not anos:
        return JSONResponse(
            status_code=422,
            content={"detail": "Obligatorio indicar anos X-Y para este conjunto (ej. 2023-2023)."},
        )

    cmd = [sys.executable, "-m", "etl.cli", "ingest", conjunto]
    if subconjunto:
        cmd.append(subconjunto)
    if anos:
        cmd.extend(["--anos", anos])
    if body.solo_descargar:
        cmd.append("--solo-descargar")
    if body.solo_procesar:
        cmd.append("--solo-procesar")

    log_path = _ingest_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        log_file = open(log_path, "w", encoding="utf-8")
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"No se pudo iniciar el ingest: {e}"},
        )

    label = f"{conjunto}/{subconjunto}" if subconjunto else conjunto
    return {
        "ok": True,
        "message": f"Ingest de {label} iniciado en segundo plano (PID: {p.pid}). "
        f"Consulte el panel para seguir el progreso.",
    }


@app.post(
    "/scheduler/register",
    summary="Register scheduler tasks",
    description="Populate scheduler.tasks from CONJUNTOS_REGISTRY. Optional body: { \"conjuntos\": [\"nacional\", ...] }. If conjuntos empty or omitted, register all. Returns 200 with summary.",
)
def scheduler_register(body: SchedulerRegisterBody | None = None):
    """Register scheduler tasks for given conjuntos or all."""
    if body and body.schedule_expr is not None:
        try:
            validate_schedule_expr(body.schedule_expr)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    if body and body.tasks:
        task_pairs = [(t["conjunto"], t["subconjunto"]) for t in body.tasks]
        schedule_overrides: dict[tuple[str, str], str] = {}
        global_expr = body.schedule_expr
        for t in body.tasks:
            key = (t["conjunto"], t["subconjunto"])
            task_expr = t.get("schedule_expr") or global_expr
            if task_expr:
                try:
                    validate_schedule_expr(task_expr)
                except ValueError as e:
                    raise HTTPException(status_code=422, detail=str(e))
                schedule_overrides[key] = task_expr
        url = get_database_url()
        if not url:
            return JSONResponse(status_code=503, content={"ok": False, "message": "Database not configured"})
        try:
            with psycopg2.connect(url) as conn:
                conn.autocommit = False
                ensure_scheduler_schema(conn)
                conn.commit()
                inserted, updated, registered = register_tasks(
                    conn,
                    task_pairs=task_pairs,
                    schedule_overrides=schedule_overrides,
                )
                conn.commit()
        except Exception as e:
            return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
        return {
            "ok": True,
            "message": f"{len(registered)} tareas registradas.",
            "registered": len(registered),
            "inserted": inserted,
            "updated": updated,
        }
    conjuntos = (body and body.conjuntos) or []
    args = argparse.Namespace(conjuntos=conjuntos)
    try:
        rc = cmd_scheduler_register(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Register failed (see server logs)"})
    return {"ok": True, "message": "Tasks registered"}


@app.get(
    "/scheduler/defaults",
    summary="Scheduler default schedule expressions",
    description="Returns the list of valid schedule expressions and the default schedule_expr per known task.",
)
def scheduler_defaults():
    """Return valid schedule expressions and per-task defaults from CONJUNTOS_REGISTRY."""
    raw = _build_default_schedules()
    defaults = [
        {"conjunto": c, "subconjunto": s, "schedule_expr": expr}
        for (c, s), expr in sorted(raw.items())
    ]
    return {"valid_exprs": list(VALID_SCHEDULE_EXPRS), "defaults": defaults}


@app.post(
    "/scheduler/run",
    summary="Run scheduler or single task",
    description="If conjunto (and subconjunto if needed) given: run that task once (blocking, 200). If detach=true: start full scheduler loop in background (202). Reuses CLI logic.",
)
def scheduler_run(body: SchedulerRunBody | None = None):
    """Run a single task or start the scheduler loop in background."""
    body = body or SchedulerRunBody()
    conjunto = (body.conjunto or "").strip() or None
    subconjunto = (body.subconjunto or "").strip() or None
    if not body.detach and not conjunto:
        return JSONResponse(
            status_code=422,
            content={"detail": "Indique conjunto y subconjunto para una tarea, o detach=true para el bucle en background."},
        )
    if conjunto and subconjunto is None and conjunto in CONJUNTOS_REGISTRY:
        subs = list(CONJUNTOS_REGISTRY[conjunto].get("subconjuntos", ()))
        if len(subs) == 1:
            subconjunto = subs[0]
        elif len(subs) > 1:
            return JSONResponse(
                status_code=422,
                content={"detail": f"El conjunto '{conjunto}' tiene varios subconjuntos. Indique uno: {', '.join(subs)}."},
            )
    if conjunto and not subconjunto and not body.detach:
        return JSONResponse(
            status_code=422,
            content={"detail": "Indique subconjunto para esta tarea."},
        )
    args = argparse.Namespace(
        conjunto=conjunto,
        subconjunto=subconjunto,
        run_all=body.detach,
        tick_seconds=60,
        detach=body.detach,
    )
    try:
        rc = cmd_scheduler_run(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Run failed (see server logs)"})
    if body.detach:
        return JSONResponse(status_code=202, content={"ok": True, "message": "Scheduler started in background"})
    return {"ok": True, "message": "Task completed"}


@app.post(
    "/scheduler/stop",
    summary="Stop scheduler process",
    description="Send SIGTERM to the scheduler process (same as CLI 'scheduler stop'). Returns 200.",
)
def scheduler_stop():
    """Stop the scheduler process."""
    args = argparse.Namespace()
    try:
        rc = cmd_scheduler_stop(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Stop failed (scheduler not running or invalid PID)"})
    return {"ok": True, "message": "Scheduler stop requested"}


@app.post(
    "/scheduler/runs/stop",
    summary="Stop selected running runs",
    description="Send SIGTERM to the processes of the given run_ids and mark them as failed.",
)
def scheduler_runs_stop(body: SchedulerRunsStopBody):
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(status_code=503, content={"ok": False, "message": "Database not configured"})
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            stopped, errors = stop_runs_by_ids(conn, body.run_ids)
            conn.commit()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    msg = f"{stopped} ejecución(es) detenida(s)."
    if errors:
        msg += f" Errores: {'; '.join(errors)}"
    return {"ok": True, "stopped": stopped, "message": msg}


@app.post(
    "/scheduler/recover",
    summary="Recover stale scheduler runs",
    description="Detect orphaned 'running' records (dead PID or timed-out) and mark them as failed. Safe to call at any time.",
)
def scheduler_recover():
    """Manually trigger stale-run recovery."""
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(status_code=503, content={"ok": False, "message": "Database not configured"})
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            count = recover_stale_runs(conn)
            conn.commit()
    except psycopg2.Error as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    return {"ok": True, "recovered": count}


@app.post(
    "/scheduler/unregister",
    summary="Unregister scheduler tasks",
    description="Delete scheduled tasks by (conjunto, subconjunto) pairs. Runs are deleted by CASCADE. Running processes are NOT stopped.",
)
def scheduler_unregister(body: SchedulerUnregisterBody):
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(status_code=503, content={"ok": False, "message": "Database not configured"})
    if not body.tasks:
        return JSONResponse(status_code=422, content={"ok": False, "message": "No tasks specified."})
    try:
        deleted = 0
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            for t in body.tasks:
                conjunto = t.get("conjunto", "")
                subconjunto = t.get("subconjunto", "")
                task_id = get_task_id(conn, conjunto, subconjunto)
                if task_id is not None:
                    delete_task(conn, task_id)
                    deleted += 1
            conn.commit()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    return {"ok": True, "deleted": deleted, "message": f"{deleted} tarea(s) eliminada(s)."}


@app.get("/status", summary="Database status", description="Checks the database connection and returns availability.")
def status():
    ok, msg = _comprobar_base_datos()
    if ok:
        return {"status": "ok", "database": "connected"}
    return JSONResponse(
        status_code=503,
        content={"status": "error", "database": "unavailable"},
    )


@app.get("/scheduler/status", summary="Scheduler tasks status", description="Lists all registered scheduler tasks with their last run info and computed next_run_at.")
def scheduler_status():
    url = get_database_url()
    if url is None:
        return JSONResponse(
            status_code=503,
            content={"tasks": []},
        )
    try:
        with psycopg2.connect(url) as conn:
            rows = list_tasks_with_last_run(conn)
    except psycopg2.Error:
        return JSONResponse(
            status_code=503,
            content={"tasks": []},
        )
    list_of_dicts = []
    for row in rows:
        r = dict(row)
        if r.get("last_status") == "running":
            r["next_run_at"] = None
        else:
            next_at = get_next_run_at(
                r.get("schedule_expr") or "Trimestral",
                r.get("last_finished_at"),
            )
            r["next_run_at"] = next_at
        list_of_dicts.append(_serialize_row(r))
    loop_running = is_scheduler_loop_running()
    return {"tasks": list_of_dicts, "loop_running": loop_running}


@app.get("/scheduler/running", summary="Running scheduler runs", description="Lists all currently running ingest runs with task metadata.")
def scheduler_running():
    url = get_database_url()
    if url is None:
        return JSONResponse(status_code=503, content={"runs": []})
    try:
        with psycopg2.connect(url) as conn:
            rows = list_running_runs(conn)
    except psycopg2.Error:
        return JSONResponse(status_code=503, content={"runs": []})
    return {"runs": [_serialize_row(r) for r in rows]}


@app.get(
    "/ingest/log",
    summary="Tail the ingest subprocess log",
    description="Returns the last N lines of the ingest log file written by the background subprocess. "
    "Poll this while an ingest is running to show live progress.",
)
def ingest_log(lines: int = 80):
    """Read the tail of the ingest subprocess log."""
    log_path = _ingest_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"lines": [], "exists": True, "total_lines": 0}
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}


@app.get("/ingest/current-run", summary="Current running ingest job", description="Returns the currently running ingest scheduler run, if any.")
def ingest_current_run():
    """Return the currently running ingest run (if any)."""
    url = get_database_url()
    if url is None:
        return JSONResponse(status_code=503, content={"running": False, "run": None})
    try:
        with psycopg2.connect(url) as conn:
            run = get_current_running_run(conn)
    except psycopg2.Error:
        return JSONResponse(status_code=503, content={"running": False, "run": None})
    if not run:
        return {"running": False, "run": None}
    return {"running": True, "run": _serialize_row(run)}


@app.get("/db-info", summary="Database info (schemas and tables)", description="Returns database schemas with sizes, table listing, and total database size.")
def db_info():
    url = get_database_url()
    if url is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "database unavailable"},
        )
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT n.nspname AS schema_name,
                           pg_size_pretty(SUM(pg_total_relation_size(c.oid))::bigint) AS size
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND n.nspname NOT LIKE 'pg_temp_%%'
                    GROUP BY n.nspname
                    ORDER BY SUM(pg_total_relation_size(c.oid)) DESC
                """)
                sizes = cur.fetchall()
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND table_schema NOT LIKE 'pg_temp_%%'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                """)
                tables = cur.fetchall()
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                db_total = cur.fetchone()
    except psycopg2.Error:
        return JSONResponse(
            status_code=503,
            content={"detail": "database error"},
        )
    return {
        "schemas": [{"schema_name": r[0], "size": r[1]} for r in sizes],
        "tables": [{"schema": r[0], "name": r[1]} for r in tables],
        "total_size": db_total[0] if db_total else None,
    }


@app.post("/borme/ingest", summary="BORME: scrape + parse + load into borme schema")
def borme_ingest(body: BormeIngestBody):
    """Spawn background CLI process for BORME ingest."""
    cmd = [sys.executable, "-m", "etl.cli", "borme", "ingest", "--anos", body.anos]
    log_path = _borme_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_file = open(log_path, "w", encoding="utf-8")
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"Could not start BORME ingest: {e}"},
        )
    return {"ok": True, "message": f"BORME ingest started (PID: {p.pid})", "pid": p.pid, "anos": body.anos}


@app.post("/borme/anomalias", summary="BORME: anomaly detector vs L0 nacional")
def borme_anomalias(body: BormeAnomaliasBody):
    """Spawn background CLI process for BORME anomaly detection."""
    cmd = [sys.executable, "-m", "etl.cli", "borme", "anomalias", "--anos", body.anos]
    if body.anonimizar:
        cmd.append("--anonimizar")
    log_path = _borme_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_file = open(log_path, "w", encoding="utf-8")
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"Could not start BORME anomalias: {e}"},
        )
    return {"ok": True, "message": f"BORME anomalías started (PID: {p.pid})", "pid": p.pid, "anos": body.anos, "anonimizar": body.anonimizar}


@app.get("/borme/jobs/{job_id}", summary="BORME job status")
def borme_job_status(job_id: str):
    """Check status of a BORME job by PID."""
    try:
        pid = int(job_id)
        os.kill(pid, 0)
        alive = True
    except (ValueError, ProcessLookupError, PermissionError):
        alive = False
    log_path = _borme_log_path()
    log_lines = []
    if log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            log_lines = text.splitlines()[-50:]
        except OSError:
            pass
    return {"job_id": job_id, "alive": alive, "status": "running" if alive else "finished", "log_tail": log_lines}


@app.get("/borme/log", summary="Tail the BORME subprocess log")
def borme_log(lines: int = 80):
    """Read the tail of the BORME subprocess log."""
    log_path = _borme_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"lines": [], "exists": True, "total_lines": 0}
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}
