"""
CLI de este microservicio ETL. Punto de entrada: licitia-etl.

Comandos: status, init-db, ingest, health, scheduler, db-info, borme.
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv

from etl import __version__
from etl.config import (
    get_database_url,
    get_db_schema,
    get_ingest_batch_size,
)
from etl.scheduler import VALID_SCHEDULE_EXPRS, validate_schedule_expr
from etl.ingest_l0 import (
    CATALUNYA_PARQUET_PATHS,
    CONJUNTOS_REGISTRY,
    _tmp_dir,
    format_conjunto_help,
    get_cleanup_dirs,
    get_table_name,
    infer_column_defs_from_parquet,
    load_parquet_to_l0,
)


INIT_MIGRATIONS = (
    "001_dim_cpv.sql",
    "002_dim_ccaa.sql",
    "002b_dim_provincia.sql",
    "003_dim_dir3.sql",
    "008_scheduler.sql",
    "009_scheduler_runs_pid.sql",
    "011_nacional_new_columns.sql",
)

BORME_MIGRATIONS = ("010_borme.sql",)


def _load_env() -> None:
    """Carga el .env de este servicio (directorio actual y raíz de este workspace)."""
    workspace_root = Path(__file__).resolve().parent.parent
    load_dotenv()
    load_dotenv(workspace_root / ".env")


def _schema_dir() -> Path:
    """Directorio de esquemas: LICITACIONES_SCHEMAS_DIR o por defecto schemas/ en este workspace."""
    base = os.environ.get("LICITACIONES_SCHEMAS_DIR")
    if base:
        return Path(base)
    return Path(__file__).resolve().parent.parent / "schemas"


def _comprobar_base_datos() -> tuple[bool, str]:
    """
    Comprueba la conexión a PostgreSQL. Subrutina usada por status.
    Devuelve (éxito, mensaje). En error, mensaje describe el fallo.
    """
    url = get_database_url()
    if url is None:
        return False, "Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env)."
    host = os.environ.get("DB_HOST", "?")
    port = os.environ.get("DB_PORT", "5432")
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, f"ETL conectado correctamente a la base de datos en {host}:{port}."
    except psycopg2.Error as e:
        return False, f"ETL no puede conectar con la base de datos: {e}"


def cmd_status(_args: argparse.Namespace) -> int:
    """Comando status: comprueba base de datos; imprime mensajes en español."""
    ok_db, msg_db = _comprobar_base_datos()
    print(msg_db if ok_db else msg_db, file=sys.stderr if not ok_db else sys.stdout)
    if not ok_db:
        return 1
    print("ETL operativo. Base de datos accesible.")
    return 0


def run_init_db(url: str | None = None, schema_dir: Path | None = None) -> tuple[int, list[dict]]:
    """Run init-db logic: create schemas, apply INIT_MIGRATIONS, run DIR3 hook. Returns (exit_code, results)."""
    from etl import schema_check

    url = url or get_database_url()
    if not url:
        return 1, [{"filename": "", "success": False, "error": "Database not configured"}]
    db_schema = get_db_schema()
    if not db_schema or not db_schema.replace("_", "").isalnum():
        return 1, [{"filename": "", "success": False, "error": "DB_SCHEMA not set or invalid"}]
    schema_dir = schema_dir or _schema_dir()
    if not schema_dir.exists():
        return 1, [{"filename": "", "success": False, "error": f"Schema dir not found: {schema_dir}"}]

    etl_schemas_sql = f"CREATE SCHEMA IF NOT EXISTS dim; CREATE SCHEMA IF NOT EXISTS {db_schema};"
    try:
        with psycopg2.connect(url) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                for stmt in etl_schemas_sql.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
            conn.commit()
    except psycopg2.Error as e:
        return 1, [{"filename": "", "success": False, "error": str(e)}]

    conn = psycopg2.connect(url)
    conn.autocommit = False
    results: list[dict] = []
    try:
        schema_check.bootstrap(conn)
        status = schema_check.check(conn, schema_dir)
        init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
        if not init_pending:
            return 0, []

        for filename in init_pending:
            path = schema_dir / filename
            if not path.exists():
                conn.close()
                return 1, results + [{"filename": filename, "success": False, "error": f"File not found: {path}"}]
            checksum = schema_check.sha256(path)
            sql = path.read_text(encoding="utf-8")
            try:
                with conn.cursor() as cur:
                    if filename == "003_dim_dir3.sql":
                        cur.execute("DROP TABLE IF EXISTS dim.dim_dir3 CASCADE")
                    cur.execute(sql)
                conn.commit()
            except psycopg2.Error as e:
                conn.rollback()
                pgcode = getattr(e, "pgcode", None)
                msg = str(e).lower()
                skip_codes = ("42P07", "23505")
                if pgcode in skip_codes or "already exists" in msg or "duplicate key" in msg:
                    schema_check.record(conn, filename, checksum)
                    results.append({"filename": filename, "success": True})
                    continue
                try:
                    schema_check.record(conn, filename, checksum, success=False, error_msg=str(e))
                except Exception:
                    pass
                results.append({"filename": filename, "success": False, "error": str(e)})
                conn.close()
                return 1, results

            schema_check.record(conn, filename, checksum)
            results.append({"filename": filename, "success": True})

            if filename == "003_dim_dir3.sql":
                try:
                    from etl.dir3_ingest import run_dir3_ingest
                    run_dir3_ingest(url)
                except Exception as e:
                    conn.close()
                    return 1, results + [{"filename": "dir3_ingest", "success": False, "error": str(e)}]
    finally:
        if not conn.closed:
            conn.close()

    return 0, results


def cmd_init_db(args: argparse.Namespace) -> int:
    """Aplica las migraciones de esquema y rellena dim. Requiere DB_SCHEMA en .env. Crea schemas dim y DB_SCHEMA si no existen."""
    from etl import schema_check

    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    db_schema = get_db_schema()
    if not db_schema:
        print("Falta DB_SCHEMA. Defina la variable de entorno DB_SCHEMA en el .env de este servicio.", file=sys.stderr)
        print("Ejemplo: DB_SCHEMA=raw", file=sys.stderr)
        return 1
    if not db_schema.replace("_", "").isalnum():
        print("DB_SCHEMA debe ser un identificador válido (letras, números y guión bajo).", file=sys.stderr)
        return 1
    schema_dir = getattr(args, "schema_dir", None) or _schema_dir()
    if not schema_dir.exists():
        print(f"Directorio de esquemas no encontrado: {schema_dir}", file=sys.stderr)
        return 1

    url = get_database_url()
    etl_schemas_sql = f"CREATE SCHEMA IF NOT EXISTS dim; CREATE SCHEMA IF NOT EXISTS {db_schema};"
    try:
        with psycopg2.connect(url) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                for stmt in etl_schemas_sql.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error creando schemas (dim, {db_schema}): {e}", file=sys.stderr)
        return 1
    print(f"Schemas dim, {db_schema} comprobados (CREATE IF NOT EXISTS).")

    conn = psycopg2.connect(url)
    conn.autocommit = False
    try:
        schema_check.bootstrap(conn)
        status = schema_check.check(conn, schema_dir)
        schema_check.log_check(status)
        other_pending = [f for f in status.pending if f not in INIT_MIGRATIONS]
        if other_pending:
            print(f"[schema-apply] Ficheros no-infra detectados (se aplican bajo demanda): {', '.join(other_pending)}")
    finally:
        if not conn.closed:
            conn.close()

    exit_code, results = run_init_db(url=url, schema_dir=schema_dir)
    version = f"etl:{__version__}"
    for r in results:
        if r.get("success"):
            print(f"[schema-apply] {version} — applied {r['filename']} [OK]")
        else:
            print(f"[schema-apply] Error: {r.get('filename', '')} — {r.get('error', '')}", file=sys.stderr)
    if exit_code == 0:
        print("init-db completado.")
    return exit_code


def _parse_anos(anos_str: str) -> tuple[int, int]:
    """Parsea --anos X-Y y devuelve (ano_inicio, ano_fin). Lanza ValueError si el formato es inválido."""
    s = (anos_str or "").strip()
    if "-" not in s:
        raise ValueError("Formato de --anos debe ser X-Y (ej. 2020-2023)")
    a, b = s.split("-", 1)
    try:
        inicio = int(a.strip())
        fin = int(b.strip())
    except ValueError:
        raise ValueError("Los años en --anos deben ser números (ej. 2020-2023)")
    if inicio > fin:
        raise ValueError("El año de inicio no puede ser mayor que el de fin en --anos")
    return inicio, fin


def cmd_ingest_test(args: argparse.Namespace) -> int:
    """Ejecuta la suite de tests de ingest (pytest), E2E por conjuntos, o limpieza del schema test."""
    if getattr(args, "ingest_delete", False) and not getattr(args, "ingest_conjuntos", False):
        return _drop_e2e_schema()
    if getattr(args, "ingest_conjuntos", False):
        return _cmd_ingest_test_conjuntos_e2e(args)

    import pytest as _pytest

    etl_root = Path(__file__).resolve().parent.parent
    test_path = str(etl_root / "tests" / "test_ingest_l0.py")
    pytest_args = [
        test_path,
        "-v",
        "-s",  # sin captura: stdout/stderr y logs visibles
        "--log-cli-level=INFO",
        "--log-cli-format=%(levelname)s [%(name)s] %(message)s",
        "--tb=short",
    ]
    if getattr(args, "ingest_integration", False):
        pass  # Incluir tests marcados con @pytest.mark.integration
    else:
        pytest_args.extend(["-m", "not integration"])
    if getattr(args, "ingest_verbose", False):
        pytest_args.append("-vv")
        pytest_args.extend(["--log-cli-level=DEBUG"])

    print("Ejecutando tests de ingest:", " ".join(pytest_args), file=sys.stderr)
    return _pytest.main(pytest_args)


E2E_SCHEMA = "test"


def _drop_e2e_schema() -> int:
    """Elimina el schema usado por el E2E (test) para dejar la BD limpia."""
    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    try:
        with psycopg2.connect(get_database_url()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{E2E_SCHEMA}" CASCADE')
        print(f"Schema '{E2E_SCHEMA}' eliminado.", file=sys.stderr)
        return 0
    except psycopg2.Error as e:
        print(f"Error eliminando schema {E2E_SCHEMA}: {e}", file=sys.stderr)
        return 1


def _cmd_ingest_test_conjuntos_e2e(args: argparse.Namespace) -> int:
    """E2E: flujo completo (descarga si aplica + ingest) de un subconjunto por conjunto en schema 'test'."""
    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    if not get_db_schema():
        print("Falta DB_SCHEMA. Defina la variable de entorno DB_SCHEMA en el .env de este servicio.", file=sys.stderr)
        return 1
    try:
        with psycopg2.connect(get_database_url()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{E2E_SCHEMA}"')
    except psycopg2.Error as e:
        print(f"Error creando schema {E2E_SCHEMA}: {e}", file=sys.stderr)
        return 1
    conjuntos = sorted(CONJUNTOS_REGISTRY.keys())
    print(f"E2E ingest: un subconjunto por conjunto en schema '{E2E_SCHEMA}' ({len(conjuntos)} conjuntos).", file=sys.stderr)
    for conjunto in conjuntos:
        reg = CONJUNTOS_REGISTRY[conjunto]
        subconjuntos = list(reg["subconjuntos"])
        subconjunto = subconjuntos[0]
        anos = "2026-2026" if reg.get("requires_anos") else ""
        print(f"  Ingest {conjunto} {subconjunto} ...", file=sys.stderr)
        fake = argparse.Namespace(
            conjunto=conjunto,
            subconjunto=subconjunto,
            anos=anos,
            solo_descargar=False,
            solo_procesar=False,
            e2e_schema=E2E_SCHEMA,
        )
        rc = cmd_ingest(fake)
        if rc != 0:
            if getattr(args, "ingest_delete", False):
                _drop_e2e_schema()
            print(f"E2E falló en {conjunto} {subconjunto}.", file=sys.stderr)
            return rc
    if getattr(args, "ingest_delete", False):
        _drop_e2e_schema()
    print("E2E completado: un subconjunto de cada conjunto verificado.", file=sys.stderr)
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """Comando ingest: descarga/genera parquet y carga en tablas L0 del schema de trabajo (nacional, catalunya, valencia, andalucia)."""
    if getattr(args, "conjunto", "") == "test":
        return cmd_ingest_test(args)

    conjunto = getattr(args, "conjunto", "")
    if getattr(args, "ingest_list_subconjuntos", False):
        if conjunto not in CONJUNTOS_REGISTRY:
            print(
                f"Conjunto no reconocido: {conjunto}. Admitidos: {', '.join(CONJUNTOS_REGISTRY)}.",
                file=sys.stderr,
            )
            return 1
        reg = CONJUNTOS_REGISTRY[conjunto]
        subconjuntos = list(reg["subconjuntos"])
        print(f"Subconjuntos para '{conjunto}' ({len(subconjuntos)}):")
        for s in subconjuntos:
            print(f"  {s}")
        print()
        print("Argumentos para este conjunto:")
        print(format_conjunto_help(conjunto, reg))
        return 0

    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    db_schema = getattr(args, "e2e_schema", None) or get_db_schema()
    if not db_schema:
        print(
            "Falta DB_SCHEMA. Defina la variable de entorno DB_SCHEMA en el .env de este servicio.",
            file=sys.stderr,
        )
        return 1
    conjunto = getattr(args, "conjunto", "")
    subconjunto = getattr(args, "subconjunto", None)

    if conjunto not in CONJUNTOS_REGISTRY:
        print(
            f"Conjunto no reconocido: {conjunto}. Admitidos: {', '.join(CONJUNTOS_REGISTRY)}.",
            file=sys.stderr,
        )
        return 1
    reg = CONJUNTOS_REGISTRY[conjunto]

    if subconjunto is None:
        if len(reg["subconjuntos"]) == 1:
            subconjunto = reg["subconjuntos"][0]
        else:
            print(
                f"Para '{conjunto}' indique subconjunto. Válidos: {', '.join(reg['subconjuntos'])}.",
                file=sys.stderr,
            )
            return 1
    if subconjunto not in reg["subconjuntos"]:
        print(
            f"Subconjunto no reconocido para {conjunto}: {subconjunto}. Válidos: {', '.join(reg['subconjuntos'])}.",
            file=sys.stderr,
        )
        return 1

    anos_str = getattr(args, "anos", "")
    ano_inicio = ano_fin = None
    if anos_str:
        try:
            ano_inicio, ano_fin = _parse_anos(anos_str)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
    if reg.get("requires_anos") and not anos_str:
        print("Obligatorio indicar --anos X-Y para este conjunto (ej. 2023-2023).", file=sys.stderr)
        return 1

    get_path = reg["get_parquet_path"]
    if conjunto == "nacional":
        if not ano_inicio or not ano_fin:
            ano_inicio, ano_fin = _parse_anos(anos_str or "0-0")
        parquet_path = get_path(subconjunto, ano_inicio, ano_fin)
    else:
        parquet_path = get_path(subconjunto)

    table_name = get_table_name(conjunto, subconjunto)
    etl_root = Path(__file__).resolve().parent.parent
    solo_descargar = getattr(args, "solo_descargar", False)
    solo_procesar = getattr(args, "solo_procesar", False)

    # Scheduler: registrar run al inicio (evitar solapamiento) y al finalizar
    run_id: Optional[int] = None
    ingest_result: list = [None, 0, 0, None]  # status, rows_inserted, rows_omitted, error_message
    db_url = get_database_url()
    if db_url:
        try:
            from etl.scheduler import get_task_id, has_running_run, insert_run_start, update_run_finish, update_run_process_id
            with psycopg2.connect(db_url) as conn:
                conn.autocommit = False
                task_id = get_task_id(conn, conjunto, subconjunto)
                if task_id and has_running_run(conn, task_id):
                    print("[ingest] Ya hay una ejecución en curso para esta tarea (scheduler).", file=sys.stderr)
                    return 1
                if task_id:
                    run_id = insert_run_start(conn, task_id)
                    update_run_process_id(conn, run_id, os.getpid())
                    conn.commit()
        except (ImportError, psycopg2.Error, Exception):
            run_id = None

    try:
        if not solo_procesar:
            run_script = solo_descargar or True  # siempre descarga/generación; con --solo-procesar no se entra aquí
        else:
            run_script = False
        if run_script:
            print("[ingest] Fase: descarga/generación parquet (ejecutando script...).", file=sys.stderr)
            run_env = os.environ.copy()
            run_env["LICITACIONES_TMP_DIR"] = str(_tmp_dir())
            if "script_module" in reg:
                script_conjunto = reg["script_conjunto_arg"][subconjunto]
                cmd = [
                    sys.executable,
                    "-m",
                    reg["script_module"],
                    "--conjunto",
                    script_conjunto,
                    "--anos",
                    f"{ano_inicio}-{ano_fin}",
                ]
                if solo_descargar:
                    cmd.append("--solo-descargar")
            elif "script_commands" in reg:
                # Madrid: lista de [script, ...args] por subconjunto
                run_cwd = etl_root
                for cmd_parts in reg["script_commands"][subconjunto]:
                    cmd = [sys.executable] + list(cmd_parts)
                    print(f"Ejecutando: {' '.join(cmd)}", file=sys.stderr)
                    try:
                        rc = subprocess.run(cmd, cwd=run_cwd, env=run_env, check=False)
                        if rc.returncode != 0:
                            print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                            print(f"Script finalizó con código {rc.returncode}.", file=sys.stderr)
                            ingest_result[0], ingest_result[3] = "failed", f"Script finalizó con código {rc.returncode}"
                            return rc.returncode
                    except FileNotFoundError as e:
                        print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                        print(f"No se pudo ejecutar: {e}", file=sys.stderr)
                        ingest_result[0], ingest_result[3] = "failed", str(e)
                        return 1
                if solo_descargar:
                    print("Descarga/generación completada (--solo-descargar).")
                    ingest_result[0] = "ok"
                    return 0
            elif "scripts" in reg:
                run_cwd = etl_root / reg["script_cwd"] if reg.get("script_cwd") else etl_root
                if conjunto == "ted":
                    # TED: un solo script con download y --years
                    if ano_inicio is None or ano_fin is None:
                        print("TED requiere --anos X-Y; no se pudo obtener rango de años.", file=sys.stderr)
                        return 1
                    cmd = [
                        sys.executable,
                        reg["scripts"][0],
                        "download",
                        "--years",
                        f"{ano_inicio}-{ano_fin}",
                    ]
                    print(f"Ejecutando: {' '.join(cmd)}", file=sys.stderr)
                    try:
                        rc = subprocess.run(cmd, cwd=etl_root, env=run_env, check=False)
                        if rc.returncode != 0:
                            print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                            print(f"Script finalizó con código {rc.returncode}.", file=sys.stderr)
                            return rc.returncode
                    except FileNotFoundError as e:
                        print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                        print(f"No se pudo ejecutar: {e}", file=sys.stderr)
                        return 1
                else:
                    for script_rel in reg["scripts"]:
                        cmd = [sys.executable, script_rel]
                        if "script_subconjunto_arg" in reg and script_rel.endswith("ccaa_andalucia.py"):
                            cmd.append(reg["script_subconjunto_arg"][subconjunto])
                        elif conjunto == "andalucia" and "ccaa_andalucia_parquet.py" in script_rel:
                            cmd.extend(["--subconjunto", subconjunto])
                        elif conjunto == "valencia" and "ccaa_valencia.py" in script_rel:
                            cmd.extend(["--categories", subconjunto])
                        elif conjunto == "valencia" and "ccaa_valencia_parquet.py" in script_rel:
                            cmd.extend(["--categories", subconjunto])
                        elif conjunto == "catalunya" and "ccaa_cataluna_parquet.py" in script_rel:
                            parquet_rel = CATALUNYA_PARQUET_PATHS[subconjunto]
                            cmd.extend(["--parquet-rel", parquet_rel])
                        elif conjunto == "catalunya" and "ccaa_cataluna.py" in script_rel and "subconjunto_to_limit_datasets" in reg:
                            ids = reg["subconjunto_to_limit_datasets"].get(subconjunto)
                            if ids:
                                cmd.extend(["--limit-datasets", ids])
                        print(f"Ejecutando: {' '.join(cmd)}", file=sys.stderr)
                        try:
                            rc = subprocess.run(cmd, cwd=run_cwd, env=run_env, check=False)
                            if rc.returncode != 0:
                                print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                                print(f"Script finalizó con código {rc.returncode}.", file=sys.stderr)
                                return rc.returncode
                        except FileNotFoundError as e:
                            print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                            print(f"No se pudo ejecutar: {e}", file=sys.stderr)
                            return 1
                if solo_descargar:
                    print("Descarga/generación completada (--solo-descargar).")
                    return 0
            else:
                if solo_descargar:
                    print("No hay script configurado para este conjunto.", file=sys.stderr)
                    return 1
            if "script_module" in reg:
                print(f"Ejecutando: {' '.join(cmd)}", file=sys.stderr)
                try:
                    rc = subprocess.run(cmd, cwd=etl_root, env=run_env, check=False)
                    if rc.returncode != 0:
                        print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                        print(f"El script finalizó con código {rc.returncode}.", file=sys.stderr)
                        return rc.returncode
                except FileNotFoundError as e:
                    print("[ingest] Error en fase descarga/generación; considerar reintentar.", file=sys.stderr)
                    print(f"No se pudo ejecutar el script: {e}", file=sys.stderr)
                    return 1
                if solo_descargar:
                    print("Descarga completada (--solo-descargar).")
                    return 0

        if solo_descargar:
            return 0

        # --- Batch loading: detect partials or single Parquet ---
        output_dir = parquet_path.parent
        partial_glob = sorted(output_dir.glob("_part_*.parquet"))
        use_partials = len(partial_glob) > 0

        parquet_files: list[Path]
        if use_partials:
            parquet_files = partial_glob
            print(f"[ingest] Encontrados {len(parquet_files)} parciales en {output_dir}.", file=sys.stderr)
        elif parquet_path.exists():
            parquet_files = [parquet_path]
        else:
            print(f"Parquet no encontrado: {parquet_path}. Ejecute sin --solo-procesar o genere el parquet antes.", file=sys.stderr)
            ingest_result[0], ingest_result[3] = "failed", "Parquet no encontrado"
            return 1

        print(f"[ingest] Fase: carga en BD ({len(parquet_files)} {'parcial' if len(parquet_files) == 1 else 'parciales'}).", file=sys.stderr)
        column_defs = reg.get("column_defs")
        natural_id_col = reg.get("natural_id_col")

        total_inserted = 0
        total_skipped = 0
        batch_errors: list[str] = []

        for batch_idx, pq in enumerate(parquet_files, start=1):
            label = f"[{batch_idx}/{len(parquet_files)}]"
            print(f"[ingest] {label} Cargando {pq.name}...", file=sys.stderr)

            if column_defs is None and pq.exists():
                column_defs = infer_column_defs_from_parquet(pq)

            try:
                inserted, skipped = load_parquet_to_l0(
                    get_database_url(),
                    db_schema,
                    table_name,
                    pq,
                    get_ingest_batch_size(),
                    column_defs=column_defs,
                    natural_id_col=natural_id_col,
                )
                total_inserted += inserted
                total_skipped += skipped
                print(f"[ingest] {label} +{inserted} insertadas, {skipped} omitidas (acum: {total_inserted}+{total_skipped}).", file=sys.stderr)

                if run_id and db_url:
                    try:
                        from etl.scheduler import update_run_progress
                        with psycopg2.connect(db_url) as _conn:
                            update_run_progress(_conn, run_id, total_inserted, total_skipped,
                                                f"Batch {batch_idx}/{len(parquet_files)}")
                            _conn.commit()
                    except Exception:
                        pass

                if use_partials:
                    try:
                        pq.unlink(missing_ok=True)
                    except OSError:
                        pass

            except Exception as e:
                msg = f"Error en batch {batch_idx}: {e}"
                print(f"[ingest] {msg}", file=sys.stderr)
                batch_errors.append(msg)
                continue

        if batch_errors and total_inserted == 0:
            ingest_result[0] = "failed"
            ingest_result[3] = "; ".join(batch_errors[:3])
            return 1

        ingest_result[0] = "ok"
        ingest_result[1] = total_inserted
        ingest_result[2] = total_skipped
        if batch_errors:
            ingest_result[3] = f"{len(batch_errors)} batch(es) fallido(s): {'; '.join(batch_errors[:3])}"

        print(f"[ingest] Ingesta completada: {total_inserted} insertadas, {total_skipped} omitidas.", file=sys.stderr)

        if not use_partials and not getattr(args, "ingest_keep_parquet", False) and parquet_path.exists():
            try:
                parquet_path.unlink(missing_ok=True)
                print("[ingest] Parquet eliminado del directorio temporal.", file=sys.stderr)
            except OSError as e:
                print(f"[ingest] No se pudo eliminar el parquet temporal: {e}", file=sys.stderr)
        for cleanup_dir in get_cleanup_dirs(conjunto, subconjunto):
            if cleanup_dir.exists():
                try:
                    shutil.rmtree(cleanup_dir, ignore_errors=True)
                    print(f"[ingest] Directorio de artefactos eliminado: {cleanup_dir}", file=sys.stderr)
                except OSError as e:
                    print(f"[ingest] No se pudo eliminar {cleanup_dir}: {e}", file=sys.stderr)
        return 0
    finally:
        if run_id and db_url:
            try:
                from etl.scheduler import update_run_finish
                with psycopg2.connect(db_url) as conn:
                    update_run_finish(
                        conn,
                        run_id,
                        ingest_result[0] or "failed",
                        ingest_result[1],
                        ingest_result[2],
                        ingest_result[3],
                    )
                    conn.commit()
            except Exception:
                pass


def cmd_health(_args: argparse.Namespace) -> int:
    """Comprueba conexión a BD y opcionalmente schema scheduler. Exit 0/1 para supervisión."""
    ok, msg = _comprobar_base_datos()
    print(msg, file=sys.stderr if not ok else sys.stdout)
    if not ok:
        return 1
    url = get_database_url()
    if url:
        try:
            from etl.scheduler import ensure_scheduler_schema
            with psycopg2.connect(url) as conn:
                ensure_scheduler_schema(conn)
            with psycopg2.connect(url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM scheduler.tasks LIMIT 1")
                    cur.fetchone()
            print("Schema scheduler: OK.", file=sys.stdout)
        except Exception:
            print("Schema scheduler: no disponible (ejecute init-db y scheduler register).", file=sys.stderr)
    return 0


def cmd_scheduler_register(args: argparse.Namespace) -> int:
    """Pobla scheduler.tasks desde CONJUNTOS_REGISTRY y frecuencias por defecto; opcionalmente solo para los conjuntos indicados."""
    url = get_database_url()
    if not url:
        print("Falta configuración de base de datos (DB_HOST, DB_NAME, DB_USER).", file=sys.stderr)
        return 1
    conjuntos = getattr(args, "conjuntos", None) or []
    valid = [c for c in CONJUNTOS_REGISTRY if c != "test"] + ["borme"]
    if conjuntos:
        invalid = [c for c in conjuntos if c not in valid]
        if invalid:
            print(f"Conjunto(s) no reconocidos: {', '.join(invalid)}. Válidos: {', '.join(valid)}.", file=sys.stderr)
            return 1
        conjuntos_filter = conjuntos
    else:
        conjuntos_filter = None
    frecuencia = getattr(args, "frecuencia", None)
    schedule_overrides = None
    if frecuencia:
        try:
            validate_schedule_expr(frecuencia)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
        from etl.scheduler import get_all_task_pairs
        all_pairs = get_all_task_pairs()
        if conjuntos_filter:
            conjuntos_set = set(conjuntos_filter)
            pairs = {k for k in all_pairs if k[0] in conjuntos_set}
        else:
            pairs = all_pairs
        schedule_overrides = {pair: frecuencia for pair in pairs}
    try:
        from etl.scheduler import ensure_scheduler_schema, register_tasks
        with psycopg2.connect(url) as conn:
            conn.autocommit = False
            ensure_scheduler_schema(conn)
            conn.commit()
            inserted, updated, registered = register_tasks(conn, conjuntos=conjuntos_filter, schedule_overrides=schedule_overrides)
            conn.commit()
        print(f"scheduler.tasks: {inserted} insertadas, {updated} actualizadas.")
        for conj, sub in sorted(registered):
            print(f"  {conj} / {sub}")
        print("Para ejecutar las tareas: licitia-etl scheduler run.")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_scheduler_status(_args: argparse.Namespace) -> int:
    """Lista tareas con última ejecución y próxima programada (tabla tipo docker ps)."""
    url = get_database_url()
    if not url:
        print("Falta configuración de base de datos (DB_HOST, DB_NAME, DB_USER).", file=sys.stderr)
        return 1
    try:
        from etl.scheduler import get_next_run_at, list_tasks_with_last_run
        with psycopg2.connect(url) as conn:
            rows = list_tasks_with_last_run(conn)
        if not rows:
            print("No hay tareas registradas. Ejecute: licitia-etl scheduler register")
            return 0
        fmt = "%-12s %-28s %-12s %-20s %-10s %-8s %-8s %-20s"
        print("ESTADO = último run (scheduled/failed/running). FILAS = insertadas+omitidas. PID = proceso del último run.")
        print(fmt % ("CONJUNTO", "SUBCONJUNTO", "FRECUENCIA", "ÚLTIMA EJECUCIÓN", "ESTADO", "PID", "FILAS", "PRÓXIMA EJECUCIÓN"))
        print("-" * 132)
        for r in rows:
            conj = (r.get("conjunto") or "")[:12]
            sub = (r.get("subconjunto") or "")[:28]
            freq = (r.get("schedule_expr") or "")[:12]
            started = r.get("last_started_at")
            last = (str(started)[:19] if started else "-")[:20]
            raw_status = r.get("last_status")
            status = "scheduled" if raw_status == "ok" else (raw_status or "-")
            status = status[:10]
            pid_val = r.get("last_process_id")
            pid_str = str(pid_val) if pid_val is not None else "-"
            pid_str = pid_str[:8]
            ins, omit = r.get("last_rows_inserted"), r.get("last_rows_omitted")
            filas = f"{ins or 0}+{omit or 0}" if (ins is not None or omit is not None) else "-"
            if raw_status == "running":
                next_run = "-"
            else:
                next_at = get_next_run_at(r.get("schedule_expr") or "Trimestral", r.get("last_finished_at"))
                next_run = str(next_at)[:19].replace("T", " ") if next_at else "-"
            next_run = (next_run or "-")[:20]
            print(fmt % (conj, sub, freq, last, status, pid_str, filas, next_run))
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_scheduler_run(args: argparse.Namespace) -> int:
    """Inicia el scheduler: bucle de todas las tareas (--all o sin args) o ejecuta una tarea por conjunto/subconjunto. Con -d no bloquea."""
    url = get_database_url()
    if not url:
        print("Falta configuración de base de datos (DB_HOST, DB_NAME, DB_USER).", file=sys.stderr)
        return 1
    from etl.scheduler import get_scheduler_pid_path, run_scheduler_loop, run_single_task
    conjunto = getattr(args, "conjunto", None)
    subconjunto = getattr(args, "subconjunto", None)
    run_all = getattr(args, "run_all", False)
    tick = getattr(args, "tick_seconds", 60) or 60
    detach = getattr(args, "detach", False)

    # Una sola tarea: conjunto (y subconjunto si hace falta)
    if conjunto is not None:
        if subconjunto is None and conjunto in CONJUNTOS_REGISTRY:
            subs = CONJUNTOS_REGISTRY[conjunto].get("subconjuntos", ())
            if len(subs) > 1:
                print(f"El conjunto '{conjunto}' tiene varios subconjuntos. Indique uno: {', '.join(subs)}", file=sys.stderr)
                return 1
            subconjunto = subs[0] if subs else ""
        if not subconjunto:
            print("Indique subconjunto (ej. licitia-etl scheduler run nacional consultas_preliminares).", file=sys.stderr)
            return 1
        rc = run_single_task(url, conjunto, subconjunto)
        return rc

    # Bucle: todas las tareas debidas
    if detach:
        pid_path = get_scheduler_pid_path()
        log_path = pid_path.parent / "scheduler.log"
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, "-m", "etl.cli", "scheduler", "run", "--all", "--tick-seconds", str(tick)]
        log_file = open(log_path, "a")
        kwargs = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        elif sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(
                subprocess, "DETACHED_PROCESS", 0x00000008
            )
        try:
            p = subprocess.Popen(cmd, **kwargs)
        except Exception as e:
            print(f"Error al arrancar el scheduler en segundo plano: {e}", file=sys.stderr)
            return 1
        time.sleep(1)
        print(f"Scheduler iniciado en segundo plano. PID: {p.pid}. Log: {log_path}")
        print("Para detener: licitia-etl scheduler stop. Para estado: licitia-etl scheduler status.")
        return 0
    run_scheduler_loop(url, tick_seconds=tick, pid_path=get_scheduler_pid_path())
    return 0


def cmd_scheduler_logs(_args: argparse.Namespace) -> int:
    """Imprime por pantalla el contenido de scheduler.log."""
    from etl.scheduler import get_scheduler_log_path
    log_path = get_scheduler_log_path()
    if not log_path.exists():
        print("No existe el archivo de log del scheduler (el scheduler aún no ha escrito en él).", file=sys.stderr)
        return 0
    try:
        print(log_path.read_text(encoding="utf-8"), end="")
    except OSError as e:
        print(f"No se pudo leer el log: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_scheduler_stop(_args: argparse.Namespace) -> int:
    """Detiene el proceso del scheduler (SIGTERM al PID del archivo)."""
    from etl.scheduler import get_scheduler_pid_path
    path = get_scheduler_pid_path()
    if not path.exists():
        print("No hay archivo de PID del scheduler (el scheduler no está en ejecución).", file=sys.stderr)
        return 1
    try:
        pid = int(path.read_text().strip())
    except (ValueError, OSError):
        print("El archivo de PID del scheduler no es válido.", file=sys.stderr)
        return 1
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        print(f"El proceso del scheduler (PID {pid}) no está en ejecución.", file=sys.stderr)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return 1
    os.kill(pid, signal.SIGTERM)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
    print("Se envió SIGTERM al scheduler. Debe terminar en breve.")
    return 0


def cmd_scheduler_unregister(args: argparse.Namespace) -> int:
    """Elimina tarea(s): por PID (detiene el proceso y elimina la tarea) o --all (elimina todas las tareas)."""
    url = get_database_url()
    if not url:
        print("Falta configuración de base de datos (DB_HOST, DB_NAME, DB_USER).", file=sys.stderr)
        return 1
    unregister_all = getattr(args, "unregister_all", False)
    pid_arg = getattr(args, "pid", None)
    if unregister_all and pid_arg is not None:
        print("Indique solo PID o solo --all, no ambos.", file=sys.stderr)
        return 1
    if not unregister_all and pid_arg is None:
        print("Indique el PID de la run (ver 'scheduler status') o use --all para eliminar todas las tareas.", file=sys.stderr)
        return 1
    try:
        from etl.scheduler import (
            delete_task,
            get_run_by_process_id,
            update_run_finish,
        )
        with psycopg2.connect(url) as conn:
            conn.autocommit = False
            if unregister_all:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM scheduler.tasks")
                    n = cur.fetchone()[0]
                    cur.execute("DELETE FROM scheduler.tasks")
                conn.commit()
                print(f"Eliminadas {n} tareas del scheduler.")
                return 0
            pid = pid_arg
            run_info = get_run_by_process_id(conn, pid)
            if not run_info:
                print(f"No existe ninguna run con process_id={pid}. Compruebe el PID en 'scheduler status'.", file=sys.stderr)
                return 1
            task_id = run_info["task_id"]
            run_id = run_info["run_id"]
            conjunto, subconjunto = run_info["conjunto"], run_info["subconjunto"]
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"Enviado SIGTERM al proceso {pid}.")
            except ProcessLookupError:
                pass
            except OSError:
                pass
            update_run_finish(conn, run_id, "failed", error_message="Detenido por unregister")
            delete_task(conn, task_id)
            conn.commit()
            print(f"Tarea {conjunto} / {subconjunto} (task_id={task_id}) eliminada del scheduler.")
            return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_db_info(_args: argparse.Namespace) -> int:
    """Muestra información de la BD: tamaño por schema y tablas relevantes."""
    url = get_database_url()
    if not url:
        print("Falta configuración de base de datos (DB_HOST, DB_NAME, DB_USER).", file=sys.stderr)
        return 1
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                db_total = cur.fetchone()
                print(f"Base de datos: {db_total[0] if db_total else '?'}\n")
                print("Tamaño por schema:")
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
                for row in sizes:
                    print(f"  {row[0]}: {row[1]}")
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND table_schema NOT LIKE 'pg_temp_%%'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                """)
                tables = cur.fetchall()
                print("\nTablas:")
                for sch, name in tables:
                    print(f"  {sch}.{name}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def _ensure_borme_schema() -> None:
    """Apply 010_borme.sql if not yet applied (on-demand DDL for BORME subsystem)."""
    from etl import schema_check

    url = get_database_url()
    if not url:
        return
    schema_dir = _schema_dir()
    with psycopg2.connect(url) as conn:
        conn.autocommit = False
        schema_check.bootstrap(conn)
        for f in BORME_MIGRATIONS:
            try:
                applied = schema_check.ensure_file(conn, f, schema_dir)
                if applied:
                    print(f"[borme] DDL aplicado bajo demanda: {f}")
            except Exception as e:
                print(f"[borme] Error aplicando {f}: {e}", file=sys.stderr)


def cmd_borme(args: argparse.Namespace) -> int:
    """BORME: ingest or anomaly detection."""
    borme_cmd = getattr(args, "borme_cmd", None)
    if not borme_cmd:
        print("Indique un subcomando: ingest o anomalias. Use 'licitia-etl borme --help' para más info.", file=sys.stderr)
        return 1
    _ensure_borme_schema()
    from etl.borme import run_scraper, run_parser, load_borme_to_db, run_anomalias
    if borme_cmd == "ingest":
        pdfs_dir = run_scraper(args.anos)
        parsed_dir = run_parser(pdfs_dir)
        ins_e, skip_e = load_borme_to_db(parsed_dir, "empresas")
        ins_c, skip_c = load_borme_to_db(parsed_dir, "cargos")
        print(f"BORME ingest: empresas inserted={ins_e} skipped={skip_e}, cargos inserted={ins_c} skipped={skip_c}")
        return 0
    elif borme_cmd == "anomalias":
        pdfs_dir = run_scraper(args.anos)
        parsed_dir = run_parser(pdfs_dir)
        output = run_anomalias(parsed_dir, anonimizar=getattr(args, "anonimizar", False))
        print(f"BORME anomalías completadas. Resultados en: {output}")
        return 0
    else:
        print("Subcomando BORME no reconocido.", file=sys.stderr)
        return 1


_HELP_EPILOG = """
Atribución
  Basado en: https://github.com/BquantFinance/licitaciones-espana
  CLI desarrollado por: Sergio Berino, Grupo TOP Digital

Orden recomendado
  status → init-db → ingest; para supervisión: health, scheduler status, db-info.

Versión: ver CHANGELOG.md para el historial de cambios.
"""


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(
        prog="licitia-etl",
        description="CLI del microservicio ETL: comprobar estado, aplicar esquemas, ingestar datos y gestionar el scheduler.",
        epilog=_HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}\nVer CHANGELOG.md para el historial.")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponibles")

    subparsers.add_parser(
        "status",
        help="Comprueba que la conexión a la base de datos sea correcta.",
        description="Comprueba que la conexión a la base de datos sea correcta. Útil como primer paso tras configurar el ETL. Sale con 0 si todo es correcto.",
    ).set_defaults(func=cmd_status)

    init_parser = subparsers.add_parser(
        "init-db",
        help="Aplica esquemas y rellena dimensiones (CPV, DIR3, etc.).",
        description="Aplica los esquemas de dimensiones y de trabajo e ingesta datos auxiliares (p. ej. CPV, DIR3). Es idempotente. Requiere base de datos configurada.",
    )
    init_parser.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directorio donde se encuentran los ficheros de esquema (por defecto el del servicio)",
    )
    init_parser.set_defaults(func=cmd_init_db, schema_dir=None)

    _conjuntos_summary = ", ".join(
        f"{c} ({len(r['subconjuntos'])})" for c, r in sorted(CONJUNTOS_REGISTRY.items())
    )
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Descarga/genera parquet e ingesta en tablas L0 del schema de trabajo.",
        description=(
            "Ingest real: conjunto + subconjunto (ej. nacional consultas_preliminares --anos 2026-2026). "
            f"Conjuntos: {_conjuntos_summary}. "
            "Use --subconjuntos para listar subconjuntos de un conjunto (ej. licitia-etl ingest valencia --subconjuntos). "
            "Requiere base de datos configurada. La carga es idempotente: re-ejecutar omite filas ya existentes.\n\n"
            "Si conjunto='test' se ejecuta la suite de tests en lugar de un ingest real. "
            "Véase el grupo 'Opciones solo para conjunto test' más abajo."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ingest_parser.add_argument(
        "conjunto",
        type=str,
        help="Conjunto: %s; o 'test' para la suite de tests" % ", ".join(sorted(CONJUNTOS_REGISTRY)),
    )
    ingest_parser.add_argument(
        "subconjunto",
        type=str,
        nargs="?",
        default=None,
        help="Subconjunto (ej. consultas_preliminares). Obligatorio si el conjunto tiene varios; para ted puede omitirse (solo hay ted_es_can).",
    )
    group_test = ingest_parser.add_argument_group(
        "Opciones solo para conjunto 'test'",
        "Solo tienen efecto cuando conjunto='test'. Por defecto se ejecutan solo tests unitarios (pytest).",
    )
    group_test.add_argument(
        "--integration",
        dest="ingest_integration",
        action="store_true",
        help="Incluir test de integración (idempotencia en BD con parquet mínimo y tabla temporal)",
    )
    group_test.add_argument(
        "--conjuntos",
        dest="ingest_conjuntos",
        action="store_true",
        help="E2E: flujo completo (descarga + ingest) de un subconjunto por conjunto en schema 'test'.",
    )
    group_test.add_argument(
        "--delete",
        dest="ingest_delete",
        action="store_true",
        help="Eliminar el schema 'test'. Solo: licitia-etl ingest test --delete. Con --conjuntos: elimina 'test' al finalizar el E2E.",
    )
    group_test.add_argument(
        "--verbose",
        "-v",
        dest="ingest_verbose",
        action="store_true",
        help="Más detalle en la traza (pytest -vv y log DEBUG)",
    )
    group_ingest = ingest_parser.add_argument_group(
        "Opciones para ingest real",
        "Solo tienen efecto cuando conjunto es uno de: %s." % ", ".join(sorted(CONJUNTOS_REGISTRY)),
    )
    group_ingest.add_argument(
        "--subconjuntos",
        dest="ingest_list_subconjuntos",
        action="store_true",
        help="Listar subconjuntos y argumentos del conjunto (ej. licitia-etl ingest valencia --subconjuntos)",
    )
    group_ingest.add_argument(
        "--anos",
        type=str,
        default="",
        metavar="X-Y",
        help="Rango de años (obligatorio para nacional; ej. 2020-2023)",
    )
    group_ingest.add_argument(
        "--solo-descargar",
        action="store_true",
        help="Solo ejecutar descarga/generación del parquet, sin cargar en BD",
    )
    group_ingest.add_argument(
        "--solo-procesar",
        action="store_true",
        help="Solo cargar parquet en L0 (el archivo debe existir ya)",
    )
    group_ingest.add_argument(
        "--keep-parquet",
        dest="ingest_keep_parquet",
        action="store_true",
        help="No eliminar el parquet del directorio temporal tras una ingest correcta (por defecto se elimina)",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    subparsers.add_parser(
        "health",
        help="Comprueba conexión a la base de datos y, si existe, el schema del scheduler.",
        description="Comprueba conexión a la base de datos y, si existe, el schema del scheduler. Devuelve código 0/1 para supervisión.",
    ).set_defaults(func=cmd_health)

    sched_parser = subparsers.add_parser(
        "scheduler",
        help="Gestiona las tareas programadas de ingest (registro, estado, ejecución y parada).",
    )
    sched_sub = sched_parser.add_subparsers(dest="scheduler_cmd", help="Subcomando", required=True)
    _sched_registry_list = ", ".join(
        f"{c} ({len(r['subconjuntos'])})" for c, r in sorted(CONJUNTOS_REGISTRY.items()) if c != "test"
    ) + ", borme (1)"
    reg_parser = sched_sub.add_parser(
        "register",
        help="Registra tareas por conjunto. Sin argumentos: todos; con argumentos: solo los indicados. Posibles: %s." % _sched_registry_list,
    )
    reg_parser.add_argument(
        "conjuntos",
        nargs="*",
        metavar="conjunto",
        help="Conjunto(s) a registrar (por defecto: todos). Valores: %s." % ", ".join(sorted(c for c in CONJUNTOS_REGISTRY if c != "test") + ["borme"]),
    )
    reg_parser.add_argument(
        "--frecuencia",
        default=None,
        metavar="FRECUENCIA",
        help="Frecuencia de ejecución para todas las tareas registradas. Opciones: %s. Por defecto cada tarea usa su frecuencia predeterminada." % ", ".join(VALID_SCHEDULE_EXPRS),
    )
    reg_parser.set_defaults(func=cmd_scheduler_register)
    sched_sub.add_parser(
        "status",
        help="Lista las tareas registradas con su última ejecución y estado.",
    ).set_defaults(func=cmd_scheduler_status)
    run_parser = sched_sub.add_parser(
        "run",
        help="Ejecuta el bucle de tareas (--all o sin args) o una sola tarea por conjunto/subconjunto. Con -d no bloquea.",
    )
    run_parser.add_argument(
        "conjunto",
        nargs="?",
        default=None,
        metavar="conjunto",
        help="Si se indica: ejecuta solo esta tarea una vez. Sin esto ni --all: bucle de todas las debidas.",
    )
    run_parser.add_argument(
        "subconjunto",
        nargs="?",
        default=None,
        metavar="subconjunto",
        help="Subconjunto (necesario si conjunto tiene varios; ej. consultas_preliminares para nacional).",
    )
    run_parser.add_argument(
        "--all",
        action="store_true",
        dest="run_all",
        help="Ejecutar el bucle de todas las tareas debidas (por defecto si no se indica conjunto).",
    )
    run_parser.add_argument("--tick-seconds", type=int, default=60, metavar="N", help="Intervalo del bucle en segundos (por defecto 60).")
    run_parser.add_argument(
        "--detach",
        "-d",
        action="store_true",
        help="Ejecutar en segundo plano (salida en scheduler.log). Para detener: scheduler stop.",
    )
    run_parser.set_defaults(func=cmd_scheduler_run)
    sched_sub.add_parser(
        "stop",
        help="Detiene el proceso del scheduler iniciado con 'scheduler run'.",
    ).set_defaults(func=cmd_scheduler_stop)
    sched_sub.add_parser(
        "logs",
        help="Imprime por pantalla el contenido de scheduler.log.",
    ).set_defaults(func=cmd_scheduler_logs)
    unreg_parser = sched_sub.add_parser(
        "unregister",
        help="Elimina una tarea por PID (detiene el proceso si está en curso) o todas las tareas con --all.",
    )
    unreg_parser.add_argument(
        "pid",
        type=int,
        nargs="?",
        default=None,
        metavar="PID",
        help="PID del proceso (run) a detener y cuya tarea eliminar. Ver PID en 'scheduler status'.",
    )
    unreg_parser.add_argument(
        "--all",
        action="store_true",
        dest="unregister_all",
        help="Elimina todas las tareas del scheduler (no detiene el proceso del scheduler; use 'scheduler stop' para eso).",
    )
    unreg_parser.set_defaults(func=cmd_scheduler_unregister)

    subparsers.add_parser(
        "db-info",
        help="Muestra el tamaño de los esquemas relevantes y la lista de tablas.",
        description="Muestra el tamaño de los esquemas relevantes y la lista de tablas (L0, dim, scheduler). Útil para operaciones y supervisión.",
    ).set_defaults(func=cmd_db_info)

    borme_parser = subparsers.add_parser(
        "borme",
        help="BORME: scrape, ingest, anomaly detection (registro mercantil).",
        description="BORME: scrape de datos del registro mercantil, ingest en schema borme, y detector de anomalías vs L0 nacional.",
    )
    borme_sub = borme_parser.add_subparsers(dest="borme_cmd", help="Subcomando BORME")

    borme_ingest = borme_sub.add_parser("ingest", help="Scrape + parse + load into borme schema")
    borme_ingest.add_argument("--anos", required=True, help="Year range: 2020-2026 or 2024")

    borme_anomalias = borme_sub.add_parser("anomalias", help="Run anomaly detector vs L0 nacional")
    borme_anomalias.add_argument("--anos", required=True, help="Year range: 2020-2026 or 2024")
    borme_anomalias.add_argument("--anonimizar", action="store_true", help="Anonymize before matching")

    borme_parser.set_defaults(func=cmd_borme)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0
    if getattr(args, "schema_dir", None) is None:
        args.schema_dir = _schema_dir()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
