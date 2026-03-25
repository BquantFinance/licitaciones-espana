# ETL API Reference

> **Interactive docs:** When the ETL service is running, full Swagger UI is available at `GET /docs` and ReDoc at `GET /redoc`.

Base URL: `http://<host>:8000` (default; mapped to host port 8002 in Docker Compose)

---

## Health & Status

### `GET /health`

Health check — returns service liveness and database connectivity.

**Response:**

```json
{
  "status": "ok",
  "db": true,
  "migrations": {
    "pending": 0,
    "pending_infra": 0,
    "applied": 11,
    "tampered": 0
  }
}
```

`pending_infra` is the number of infrastructure migrations (those in `INIT_MIGRATIONS`) that `POST /init-db` can apply.

### `GET /migrations`

Migration audit trail — all recorded schema migrations with their status.

**Response (200):**

```json
{
  "migrations": [
    {
      "id": 1,
      "filename": "001_dim_cpv.sql",
      "applied_at": "2026-02-17T10:00:00",
      "success": true
    }
  ]
}
```

### `POST /init-db`

Apply pending infrastructure migrations (`INIT_MIGRATIONS`). Safe to call multiple times (idempotent).

**Response (200):**

```json
{
  "ok": true,
  "message": "Init-db: 2 processed, 0 failed.",
  "results": [
    { "file": "011_nacional_new_columns.sql", "success": true }
  ]
}
```

### `GET /status`

Database status — checks the database connection.

**Response (200):**

```json
{ "status": "ok", "database": "connected" }
```

**Response (503):**

```json
{ "status": "error", "database": "unavailable" }
```

### `GET /db-info`

Database info — schemas with sizes, table listing, and total database size.

**Response (200):**

```json
{
  "schemas": [{ "schema_name": "l0", "size": "1200 MB" }],
  "tables": [{ "schema": "l0", "name": "contratos" }],
  "total_size": "1500 MB"
}
```

---

## Ingest

### `GET /ingest/conjuntos`

List available conjuntos and their subconjuntos (same catalog as the CLI).

**Response:**

```json
{
  "conjuntos": [
    { "id": "nacional", "subconjuntos": ["contratos", "licitaciones"] }
  ]
}
```

### `POST /ingest/run`

Run ingest for a conjunto/subconjunto (non-blocking). Spawns a CLI subprocess.

**Request body:**

```json
{
  "conjunto": "nacional",
  "subconjunto": "contratos",
  "anos": "2023-2024",
  "solo_descargar": false,
  "solo_procesar": false
}
```

| Field            | Type   | Required | Description                          |
|------------------|--------|----------|--------------------------------------|
| `conjunto`       | string | yes      | Conjunto ID from the registry        |
| `subconjunto`    | string | no       | Subconjunto; auto-selected if only 1 |
| `anos`           | string | no       | Year range, e.g. `"2023-2024"`       |
| `solo_descargar` | bool   | no       | Download only, skip processing       |
| `solo_procesar`  | bool   | no       | Process only, skip download           |

**Response (200):**

```json
{ "ok": true, "message": "Ingest de nacional/contratos iniciado en segundo plano (PID: 12345)..." }
```

### `GET /ingest/current-run`

Current running ingest job — returns the active scheduler run record, if any.

**Response:**

```json
{ "running": true, "run": { "conjunto": "nacional", "subconjunto": "contratos", "started_at": "2025-01-15T10:00:00" } }
```

### `GET /ingest/log`

Tail the ingest subprocess log.

**Query params:**

| Param   | Type | Default | Description              |
|---------|------|---------|--------------------------|
| `lines` | int  | 80      | Number of lines to return |

**Response:**

```json
{ "lines": ["line1", "line2"], "exists": true, "total_lines": 150 }
```

### `GET /ingest/current-run`

Current running ingest job — returns the active scheduler run record, if any.

**Response (200):**

```json
{
  "running": true,
  "run": {
    "conjunto": "nacional",
    "subconjunto": "licitaciones",
    "started_at": "2026-03-25T10:00:00"
  }
}
```

Returns `{ "running": false }` when no ingest is active.

### `GET /ingest/log`

Tail the ingest subprocess log.

**Query params:**

| Param   | Type | Default | Description               |
|---------|------|---------|---------------------------|
| `lines` | int  | 80      | Number of lines to return |

**Response (200):**

```json
{ "lines": ["line1", "line2"], "exists": true, "total_lines": 150 }
```

---

## Scheduler

### `GET /scheduler/status`

Scheduler tasks status — lists all registered tasks with last run info and computed `next_run_at`.

**Response:**

```json
{
  "tasks": [
    {
      "conjunto": "nacional",
      "subconjunto": "contratos",
      "last_status": "ok",
      "last_finished_at": "2025-01-15T12:00:00",
      "next_run_at": "2025-04-15T00:00:00"
    }
  ]
}
```

### `POST /scheduler/register`

Register scheduler tasks from CONJUNTOS_REGISTRY.

**Request body (optional):**

```json
{ "conjuntos": ["nacional", "galicia"] }
```

If `conjuntos` is empty or omitted, all conjuntos are registered.

**Response:**

```json
{ "ok": true, "message": "Tasks registered" }
```

### `POST /scheduler/run`

Run a single scheduler task (blocking) or start the full scheduler loop in background.

**Request body:**

```json
{
  "conjunto": "nacional",
  "subconjunto": "contratos",
  "detach": false
}
```

| Field         | Type   | Required | Description                                    |
|---------------|--------|----------|------------------------------------------------|
| `conjunto`    | string | no       | Conjunto ID (required if `detach=false`)        |
| `subconjunto` | string | no       | Subconjunto; auto-selected if only 1            |
| `detach`      | bool   | no       | `true` to start background scheduler loop (202) |

**Response (200):** `{ "ok": true, "message": "Task completed" }`

**Response (202):** `{ "ok": true, "message": "Scheduler started in background" }`

### `POST /scheduler/stop`

Stop the scheduler process (sends SIGTERM).

**Response:**

```json
{ "ok": true, "message": "Scheduler stop requested" }
```

### `GET /scheduler/running`

Running scheduler runs — lists all currently active ingest runs with task metadata.

**Response (200):**

```json
{
  "runs": [
    {
      "run_id": 42,
      "task_id": 3,
      "conjunto": "nacional",
      "subconjunto": "licitaciones",
      "process_id": 12345,
      "started_at": "2026-03-25T10:00:00"
    }
  ]
}
```

### `POST /scheduler/runs/stop`

Stop selected running runs by PID — sends SIGTERM and marks them as failed.

**Request body:**

```json
{ "run_ids": [42, 43] }
```

**Response (200):**

```json
{ "ok": true, "stopped": 2, "message": "2 ejecución(es) detenida(s)." }
```

### `POST /scheduler/recover`

Recover stale scheduler runs — marks orphaned "running" records (dead PID or timed-out) as failed. Safe to call at any time.

**Response (200):**

```json
{ "ok": true, "recovered": 2 }
```

### `POST /scheduler/unregister`

Delete scheduled tasks by `(conjunto, subconjunto)` pairs. Runs are deleted by CASCADE. Running processes are **not** stopped.

**Request body:**

```json
{
  "tasks": [
    { "conjunto": "nacional", "subconjunto": "licitaciones" }
  ]
}
```

**Response (200):**

```json
{ "ok": true, "deleted": 1, "message": "1 tarea(s) eliminada(s)." }
```

---

## BORME

### `POST /borme/ingest`

BORME: scrape + parse + load into borme schema. Spawns a background process.

**Request body:**

```json
{ "anos": "2024-2024" }
```

**Response:**

```json
{ "ok": true, "message": "BORME ingest started (PID: 12345)", "pid": 12345, "anos": "2024-2024" }
```

### `POST /borme/anomalias`

BORME: anomaly detector vs L0 nacional. Spawns a background process.

**Request body:**

```json
{ "anos": "2024-2024", "anonimizar": false }
```

| Field        | Type   | Required | Description                          |
|--------------|--------|----------|--------------------------------------|
| `anos`       | string | yes      | Year range, e.g. `"2024-2024"`       |
| `anonimizar` | bool   | no       | Anonymize contractor names in output |

**Response:**

```json
{ "ok": true, "message": "BORME anomalías started (PID: 12345)", "pid": 12345, "anos": "2024-2024", "anonimizar": false }
```

### `GET /borme/jobs/{job_id}`

BORME job status — check if a BORME background process is still running.

**Path params:** `job_id` — the PID returned by `/borme/ingest` or `/borme/anomalias`.

**Response:**

```json
{ "job_id": "12345", "alive": true, "status": "running", "log_tail": ["..."] }
```

### `GET /borme/log`

Tail the BORME subprocess log.

**Query params:**

| Param   | Type | Default | Description               |
|---------|------|---------|---------------------------|
| `lines` | int  | 80      | Number of lines to return |

**Response (200):**

```json
{ "lines": ["line1", "line2"], "exists": true, "total_lines": 50 }
```

---

## Nacional schema — columns (v1.2.0)

Tables `nacional_*` expose the following columns after migration `011_nacional_new_columns.sql` is applied.

### New columns in v1.2.0 (applied via `POST /init-db`)

| Column | Type | Description |
|--------|------|-------------|
| `valor_estimado_contrato` | `NUMERIC(18,2)` | `EstimatedOverallContractAmount` from UBL budget block. Previously mapped (incorrectly) to `importe_sin_iva`. |
| `doc_legal_nombre` | `TEXT` | PCAP document name |
| `doc_legal_url` | `TEXT` | PCAP document URL (HTML entities decoded) |
| `doc_tecnico_nombre` | `TEXT` | PPT document name |
| `doc_tecnico_url` | `TEXT` | PPT document URL (HTML entities decoded) |
| `docs_adicionales` | `JSONB` | Additional documents array: `[{ "nombre", "url", "hash" }]` |
| `criterios_adjudicacion` | `JSONB` | Award criteria array: `[{ "tipo": "OBJ\|SUBJ", "descripcion", "nota", "subtipo", "peso" }]` |
| `requisitos_solvencia` | `JSONB` | Solvency requirements: `{ "tecnica": [...], "economica": [...], "especificos": [...] }` |

### Import mapping fix (v1.2.0)

| UBL field | Column (< v1.2.0) | Column (≥ v1.2.0) |
|-----------|-------------------|-------------------|
| `EstimatedOverallContractAmount` | `importe_sin_iva` ❌ | `valor_estimado_contrato` ✅ |
| `TaxExclusiveAmount` | — | `importe_sin_iva` ✅ |
| `TotalAmount` | `importe_con_iva` ✅ | `importe_con_iva` ✅ (unchanged) |

> Rows ingested before v1.2.0 will have `NULL` in all new columns. Re-ingest to populate them.
