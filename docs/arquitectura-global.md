# Arquitectura global del ETL y sistema de migraciones

Guía de referencia para el equipo sobre el pipeline de ingesta, la estructura de
esquemas y el sistema de migraciones DDL (`schema_check`).

---

## 1. Visión general del pipeline

```
PLACSP (contrataciondelestado.es)
  │
  ▼
 .zip  ──descarga──▶  .atom  ──parsing──▶  .parquet  ──ingesta──▶  PostgreSQL L0
```

El flujo completo descarga paquetes ZIP desde la Plataforma de Contratación del
Sector Público (PLACSP), descomprime los ficheros Atom/XML, los parsea a
columnas tabulares en formato Parquet y los carga en tablas PostgreSQL de la capa
L0 (datos brutos).

### Microservicios del stack

| Servicio          | Descripción                                                     |
|-------------------|-----------------------------------------------------------------|
| **etl**           | Backend: CLI (`licitia-etl`) y API REST (FastAPI, puerto 8002)  |
| **frontend-etl**  | Dashboard web para monitorización, scheduler y migraciones      |
| **indexer**       | Transformación L1 (canonicalización) y L2 (enriquecimiento)     |
| **embedding**     | Generación de embeddings vectoriales (pgvector)                 |
| **postgres**      | Base de datos PostgreSQL con extensión pgvector                 |

### Capas de esquema en PostgreSQL

| Esquema | Contenido                                                         |
|---------|-------------------------------------------------------------------|
| `dim`   | Dimensiones estáticas: CPV, CCAA, provincias, DIR3                |
| `l0`    | Datos brutos de ingesta (una tabla por conjunto × subconjunto)    |
| `l1`    | Datos canonicalizados (normalización de tipos, claves foráneas)   |
| `l2`    | Datos enriquecidos (embeddings, métricas derivadas)               |

---

## 2. Sistema de migraciones (`schema_check`)

El módulo `etl/schema_check.py` implementa un sistema ligero de control de
migraciones DDL por checksum.

### Tabla de registro

```sql
scheduler.schema_migrations (
    id          BIGSERIAL PRIMARY KEY,
    filename    TEXT NOT NULL UNIQUE,
    checksum    TEXT NOT NULL,          -- SHA-256 del fichero .sql
    applied_at  TIMESTAMPTZ DEFAULT NOW(),
    applied_by  TEXT NOT NULL,          -- "etl:<version>"
    success     BOOLEAN DEFAULT TRUE,
    error_msg   TEXT
)
```

### Clasificación de ficheros

Al arrancar el ETL, `_startup_schema_check` en `etl/api.py` ejecuta:

1. `schema_check.bootstrap(conn)` — crea la tabla si no existe.
2. `schema_check.check(conn)` — compara cada `*.sql` en `schemas/` con los
   checksums almacenados.

Cada fichero se clasifica como:

| Estado        | Significado                                             |
|---------------|---------------------------------------------------------|
| **pending**   | Fichero en disco que no aparece en la tabla de registro |
| **applied**   | Fichero registrado y cuyo SHA-256 coincide              |
| **tampered**  | Fichero registrado pero cuyo SHA-256 difiere (editado)  |

Adicionalmente, `pending_infra` es el subconjunto de ficheros pendientes que
están listados en `INIT_MIGRATIONS` (dimensiones y scheduler), es decir, los que
el dashboard puede aplicar automáticamente.

---

## 3. Flujo de detección y aplicación

### Detección (arranque del ETL)

```
ETL startup
  └─ _startup_schema_check()
       └─ schema_check.check()
            └─ app.state.migration_status = {
                 pending, pending_infra, applied, tampered
               }
```

### Consulta desde el frontend

1. El dashboard (`frontend-etl`) hace polling a `GET /api/status`.
2. La respuesta incluye `migrations.etl.pending_infra`.
3. Si `pending_infra > 0`, el dashboard muestra un banner:
   *«N migraciones pendientes de aplicar»*.

### Aplicación desde el dashboard

1. El usuario pulsa el botón **Aplicar**.
2. El frontend envía `POST /api/etl/init-db`.
3. El backend ejecuta `run_init_db()`, que aplica **solo** los ficheros listados
   en `INIT_MIGRATIONS` (dimensiones, scheduler y DDL infra).
4. Tras la aplicación, `app.state.migration_status` se recalcula y el dashboard
   refleja el nuevo estado.

---

## 4. Cómo añadir una migración nueva

Pasos para añadir un cambio DDL al sistema:

1. **Crear el fichero SQL**
   ```
   schemas/NNN_nombre.sql
   ```
   Donde `NNN` es el siguiente número secuencial. El fichero debe contener el
   DDL con cláusulas idempotentes (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`).

2. **Registrar en `INIT_MIGRATIONS`** (si aplica)
   En `etl/cli.py`, añadir el nombre del fichero a la tupla `INIT_MIGRATIONS`
   para que sea aplicable desde el dashboard.

3. **Actualizar `NACIONAL_PARQUET_COLUMNS`** (si la migración añade columnas L0)
   En `etl/ingest_l0.py`, añadir las nuevas tuplas `(nombre_columna, tipo_sql)`
   a `NACIONAL_PARQUET_COLUMNS` para que las ingestas futuras incluyan las
   columnas.

4. **Actualizar los parsers** (si los campos vienen del Atom)
   En `nacional/licitaciones.py`, modificar `parsear_entry()` y/o
   `parsear_entry_cpm()` para extraer los nuevos campos del XML.

5. **Desplegar**
   Al arrancar el ETL detectará automáticamente los ficheros pendientes. El
   dashboard mostrará el banner y un clic en «Aplicar» ejecuta la migración.

6. **Verificar**
   Consultar `GET /migrations` y confirmar que la migración aparece como
   `applied` con `success: true`.

---

## 5. Diferencia entre migraciones infra y L0

### Migraciones infra (dimensiones / scheduler)

- Crean tablas nuevas en esquemas fijos (`dim.*`, `scheduler.*`).
- Ejemplos: `001_dim_cpv.sql`, `002_dim_ccaa.sql`, `008_scheduler.sql`.
- Operación: `CREATE TABLE IF NOT EXISTS`.
- Se aplican desde el dashboard (`INIT_MIGRATIONS`).

### Migraciones L0 (datos)

- Alteran tablas de datos existentes que pueden tener millones de filas.
- Operación típica: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- Ejemplo: `011_nacional_new_columns.sql` añade columnas a tablas `nacional_*`.
- Las tablas L0 también se crean en tiempo de ingesta por `ensure_l0_table()`
  con `CREATE TABLE IF NOT EXISTS`, por lo que una tabla nueva ya incluirá las
  columnas desde su creación.
- `ensure_l0_table()` usa `NACIONAL_PARQUET_COLUMNS` como definición de columnas
  por defecto para las tablas nacionales.

---

## 6. Esquemas JSONB de referencia

Para los esquemas completos de las columnas JSONB, consultar el documento de
diseño en el monorepo padre:

> [`../../docs/plans/2026-03-25-ddl-nacional-v2-design.md`](../../docs/plans/2026-03-25-ddl-nacional-v2-design.md)

### Resumen de columnas JSONB

| Columna                   | Tipo   | Estructura de primer nivel                                              |
|---------------------------|--------|-------------------------------------------------------------------------|
| `docs_adicionales`        | JSONB  | Array de objetos `{nombre, url, hash}`                                  |
| `criterios_adjudicacion`  | JSONB  | Array de objetos `{tipo, descripcion, nota, subtipo, peso}`             |
| `requisitos_solvencia`    | JSONB  | Objeto con claves `tecnica`, `economica`, `requisitos_especificos`; cada una es un array de objetos `{nombre, descripcion}` |

- `docs_adicionales`: documentos complementarios al pliego (más allá de PCAP y
  PPT que tienen columnas propias).
- `criterios_adjudicacion`: cada criterio indica si es objetivo (`OBJ`) o
  subjetivo (`SUBJ`), con peso numérico y descripción.
- `requisitos_solvencia`: agrupados por tipo de solvencia, con nombre traducido
  al español según el catálogo CODICE.
