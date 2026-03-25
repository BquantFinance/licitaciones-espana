# Changelog

Todos los cambios notables del CLI y del microservicio ETL se documentan aquí.

## [1.1.2] — 2026-03-25

### Upstream sync (BquantFinance/licitaciones-espana)
- **Andalucía scraper refactor** (`scripts/ccaa_andalucia.py`): improved coverage and stability (upstream PR #5); 11 new tests in `tests/test_ccaa_andalucia.py`
- **Asturias downloader script** (`scripts/ccaa_asturias.py`): new standalone script to fetch contracts from Asturias open-data portal to Parquet; see `scripts/README.md` for usage. Ingest wiring (CONJUNTOS_REGISTRY) deferred to 1.2.0.
- **Calidad analytics module** (`calidad/calidad_licitaciones.py`): 20 quality indicators on contracts dataset; standalone tool, not part of ingest pipeline
- `requirements.txt`: `requests>=2.31.0` added; `numpy<2` pin preserved

### Fork changes
- **Data hygiene**: removed `catalunya/README.md` (doc-only); `.gitignore` updated to cover `asturias_data/`, `ccaa_Andalucia/perfiles_cache.json`, `catalunya/`
- **No new HTTP API routes** added in this release; API surface unchanged from 1.1.1
- **No new CLI subcommands** added; ingest catalog visible via `GET /ingest/conjuntos` (8 conjuntos)

### Notes
- API version string updated from 1.1.0 → 1.1.2
- 4 schema migrations on disk pending application (`005_catalunya.sql`, `006_valencia.sql`, `007_views.sql`, `010_borme.sql`) — apply via `POST /init-db` or `licitia-etl init-db`

---

## [1.2.0] - 2026-03-03

### Añadido

- **Campo `loop_running` en `GET /scheduler/status`.** Indica si el daemon del scheduler está activo (comprobación por PID file y señal al proceso).
- **`GET /scheduler/running`.** Devuelve la lista de ejecuciones con estado `running` (run_id, task_id, conjunto, subconjunto, process_id, started_at).
- **`POST /scheduler/register` con `tasks[]`.** Acepta un cuerpo `{ "tasks": [{ "conjunto": "...", "subconjunto": "..." }, ...] }` para registrar solo los pares indicados (además del registro por `conjuntos[]`).
- **`POST /scheduler/runs/stop`.** Recibe `{ "run_ids": [ ... ] }`; envía SIGTERM a los procesos de las ejecuciones indicadas y las marca como `failed` con mensaje «Detenido por usuario».
- **`POST /scheduler/unregister`.** Recibe `{ "tasks": [{ "conjunto", "subconjunto" }, ...] }`; elimina las tareas programadas correspondientes (y sus runs por CASCADE).

### Corregido

- **`get_next_run_at`.** La comparación para decidir el siguiente slot programado usa `last_finished_at` (en zona Europe/Madrid) en lugar de `now`, de modo que una tarea programada para el mismo día se ejecute correctamente después de una ejecución previa.

---

## [1.0.0] - 2026-02-17 - Release producción (scheduler completo)

### Añadido

- **Scheduler con config en BD:** Schema `scheduler` con tablas `scheduler.tasks` y `scheduler.runs`. Poblado con `licitia-etl scheduler register`; ejecución con `scheduler run` (intervalo `--tick-seconds`, por defecto 60). Frecuencias: Mensual (día 1 a 02:00 Europe/Madrid), Trimestral (ene/abr/jul/oct), Anual (1 enero).
- **scheduler run / stop:** `scheduler run` inicia el daemon; PID file en directorio temporal; `scheduler stop` envía SIGTERM y elimina el PID file. Con `-d` el daemon se ejecuta en segundo plano y la salida va a `scheduler.log`.
- **scheduler status:** Tabla con última ejecución, estado (**scheduled** cuando la última run fue ok, **running** / **failed**), PID, filas y columna **PRÓXIMA EJECUCIÓN** (fecha/hora de la siguiente ejecución programada).
- **Registro de runs desde ingest:** Cada ingest registra un run al inicio (evita solapamiento) y actualiza al finalizar (status, filas, error). `scheduler status` refleja el estado actual.
- **Comandos `health` y `db-info`:** Comprobación de BD y schema scheduler; tamaño y listado de tablas para supervisión.

### Corregido

- **get_tasks_due:** Uso de `reference_now` en `get_next_run_at` para que las tareas nunca ejecutadas se consideren debidas en el mismo tick (evita desfase de milisegundos que dejaba `due_count` en 0).
- **insert_run_start:** Firma correcta `insert_run_start(conn, task_id)` en `cmd_ingest`.



### Nota

- El ETL no crea ni mantiene `dim.cpv_router`; es responsabilidad del indexador si se usa.

---

## [0.2.0] - Ingesta L0 definitiva (todos los conjuntos + PK surrogada + tests)

### Añadido

- **Comando `ingest` (todos los conjuntos):** `licitia-etl ingest <conjunto> <subconjunto> [--anos X-Y] [--solo-descargar|--solo-procesar]`. Conjuntos: **nacional** (5 subconjuntos), **catalunya** (5), **valencia** (14), **andalucia** (1). Requiere `DB_*` y `DB_SCHEMA`; nacional requiere además `--anos` X-Y.
- **PK surrogada en tablas L0:** Todas las tablas L0 usan `l0_id` BIGSERIAL PRIMARY KEY y `natural_id` TEXT UNIQUE NOT NULL (URL o identificador de fuente), para evitar PK de texto largas y facilitar búsquedas por otros índices.
- **Tablas L0 en DB_SCHEMA:** Tablas `<conjunto>_<subconjunto>` con columnas del parquet (o inferidas) más extensiones CPV (`principal_prefix4`, `principal_prefix6`, `secondary_prefix6`) e `ingested_at`. Carga idempotente con `ON CONFLICT (natural_id) DO NOTHING`.
- **Registro de conjuntos (`CONJUNTOS_REGISTRY`):** Configuración por conjunto (subconjuntos, resolución de parquet, scripts, columnas). Catalunya/Valencia/Andalucía infieren esquema desde el parquet.
- **Integración con scripts:** Nacional invoca `nacional.licitaciones`; catalunya/valencia/andalucía ejecutan los scripts en `scripts/` (ccaa_cataluna, ccaa_valencia_parquet, ccaa_andalucia, etc.) cuando el parquet no existe.
- **Guía de usuario:** Sección «Comando ingest» en `docs/quick-guide-deploy.md` con tabla de conjuntos/subconjuntos, variables de entorno y ejemplos mínimos.
- **Rutina de tests (pytest):** `tests/test_ingest_l0.py` con tests unitarios (registro, rutas, derive_cpv_prefixes, inferencia de esquema) y test de idempotencia marcado con `@pytest.mark.integration` (requiere DB). Ejecución: `pytest tests/ -v` o `pytest tests/ -v -m "not integration"` para solo unitarios.
- **Dependencia pytest** en requirements.txt.

### Cambiado

- **ingest_l0:** `ensure_l0_table` y `load_parquet_to_l0` aceptan `column_defs` y `natural_id_col` opcionales para soportar fuentes genéricas (catalunya, valencia, andalucía). Nacional sigue usando el esquema fijo de 40 columnas.
- **CLI:** `cmd_ingest` valida conjunto y subconjunto contra el registro y enruta scripts y parquet según el conjunto.

### Nota

- E2E recomendado: `init-db` y luego `ingest nacional consultas_preliminares --anos 2026-2026`; re-ejecutar y comprobar 0 insertadas y N omitidas. Para catalunya/valencia/andalucía, ejecutar antes los scripts en `scripts/` para generar los parquets.

---

## [0.1.1] - Datos estáticos dim y schemas en Postgres init

### Añadido

- **Schemas en init de Postgres:** En el script de inicialización del microservicio Postgres (`init.d/01-pgvector.sql`) se crean los esquemas `dim`, `l0`, `l1` y `l2` tras la extensión pgvector, de modo que al levantar el contenedor las capas existan antes de que el ETL ejecute init-db.
- **Datos estáticos dim en init-db:** init-db aplica y rellena la capa dim: `dim.dim_ccaa`, `dim.dim_provincia` y `dim.dim_dir3`. Los esquemas 002_dim_ccaa y 002b_dim_provincia crean las tablas en el schema `dim` con tipos válidos en PostgreSQL (INTEGER sin precisión); la FK de provincias referencia `dim.dim_ccaa(num_code)`.
- **Ingesta DIR3 desde Listado Unidades AGE:** Tras aplicar `003_dim_dir3.sql`, init-db descarga el XLSX del Listado de información básica de unidades orgánicas de la AGE (administracionelectronica.gob.es / datos.gob.es), lo parsea con pandas (openpyxl), mapea columnas a `dim.dim_dir3` e inserta (TRUNCATE + INSERT). Logging en español con prefijo `[dir3_ingest]`; User-Agent, timeout y reintentos en la descarga. La URL es configurable con `DIR3_XLSX_URL`.
- **Dependencia openpyxl** en requirements.txt para lectura de XLSX.

### Cambiado

- **Orden de migraciones:** `SCHEMA_FILES` en init-db queda reducido a la capa dim: `001_dim_cpv.sql`, `002_dim_ccaa.sql`, `002b_dim_provincia.sql`, `003_dim_dir3.sql`, más scheduler (008, 009). cpv_router no es responsabilidad del ETL.
- **FK recursiva dim_dir3:** La restricción `fk_dim_dir3_parent` es `DEFERRABLE INITIALLY DEFERRED` para permitir insertar filas en cualquier orden; se comprueba al commit.

### Nota

- Las fuentes oficiales de los catálogos estáticos (CPV, DIR3, códigos de provincias y CCAA) se documentarán en una iteración posterior.

---

## [0.1.0] - Primera iteración publicable

### Añadido

- **Guía rápida de despliegue:** Documento `docs/quick-guide-deploy.md` (en español): requisitos, arranque rápido, tabla de comandos del CLI, variables de entorno y enlace a notas operativas. Sustituye a `how-to-use-deployment.md`.
- **Comando `status`:** Comprueba en un solo comando la conexión a la base de datos. Mensajes en español. Sustituye a comandos previos de comprobación, que pasan a ser subrutinas internas.
- **CLI en español:** Descripciones, ayuda de argumentos y mensajes de éxito/error en español.
- **Ayuda con atribución:** En `licitia-etl --help` se muestra el origen del fork (BquantFinance/licitaciones-espana) y la autoría del CLI (Sergio Berino, Grupo TOP Digital).
- **Descubribilidad:** En la ayuda se listan las integraciones (PostgreSQL), las variables de entorno (.env) y el orden recomendado: `status` → `init-db` → `ingest`.
- **Mensajes de retorno estándar:** Mensajes claros para salud (conexión DB, estado global) y para init-db / ingest; stderr para errores, exit 0/1 documentados.
- **Versión 0.1.0** fijada en el paquete; `--version` muestra la versión e indica CHANGELOG.md para el historial.

### Cambiado

- La lógica de comprobación de BD se usa desde `status` mediante `_comprobar_base_datos()`.
- Comentarios y docstrings en `cli.py` y `config.py` traducidos al español (invariante de microservicio atómico mantenido).

### Extensión futura (no implementado en esta iteración)

- Prefijo de log opcional (p. ej. variable `LICITIA_ETL_LOG_PREFIX`) para logs agregados; queda para una iteración posterior.
