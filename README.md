# licitaciones-espana — ETL Microservice

Fork de [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana) que extiende el repositorio original de extracción de datos públicos de contratación con un **CLI** (`licitia-etl`) y una **API REST** (FastAPI) para su uso como microservicio conectable.

Última sincronización con upstream: commit `68e469b`, 2026-03-23 (rama `feat/upstream-sync-1.1.2`).

## Contribución y gobernanza

Consulta `CONTRIBUTING.md` para el flujo obligatorio de contribución (issue -> branch -> PR), políticas de revisión y criterios de bypass para owner/admin en casos excepcionales.

## ¿Cómo contribuir?

1. Crea una issue con contexto y propuesta.
2. Crea una rama desde `main` con prefijo `feat/`, `fix/` o `chore/`.
3. Implementa los cambios y valida con tests/checks.
4. Abre una PR enlazando la issue (por ejemplo: `Closes #123`).
5. Espera a que los checks obligatorios de CI estén en verde para hacer merge.

## Arquitectura

```
┌─────────────────────────────────────────────┐
│          licitaciones-espana (ETL)           │
│                                             │
│  ┌──────────┐          ┌──────────────────┐ │
│  │   CLI    │          │    API REST      │ │
│  │licitia-etl│         │  FastAPI :8000   │ │
│  └────┬─────┘          └───────┬──────────┘ │
│       │                        │            │
│       ▼                        ▼            │
│  ┌──────────────────────────────────────┐   │
│  │     Core: schemas, ingest, scheduler │   │
│  └──────────────────┬───────────────────┘   │
│                     │                       │
└─────────────────────┼───────────────────────┘
                      ▼
               ┌─────────────┐
               │ PostgreSQL   │
               │ (cualquiera) │
               └─────────────┘
```

**Sin dependencias externas.** La única conexión es a una base de datos PostgreSQL configurable por variables de entorno. No depende de ningún otro microservicio ni componente externo.

---

## Fuentes de datos

| Fuente | Tipo | Subconjuntos | Frecuencia |
|--------|------|--------------|------------|
| Nacional (PLACSP) | L0 conjunto | licitaciones, agregacion_ccaa, contratos_menores, encargos_medios_propios, consultas_preliminares | Según lo configurado |
| Catalunya | L0 conjunto | contratacion_registro, subvenciones_raisc, convenios, presupuestos_aprobados, rrhh_altos_cargos | Según lo configurado |
| Valencia | L0 conjunto | contratacion, subvenciones, presupuestos, +11 más | Según lo configurado |
| Andalucía | L0 conjunto | licitaciones, menores | Según lo configurado |
| Euskadi | L0 conjunto | contratos_master, poderes_adjudicadores, empresas_licitadoras, +3 más | Según lo configurado |
| Madrid | L0 conjunto | comunidad, ayuntamiento | Según lo configurado |
| TED | L0 conjunto | ted_es_can | Según lo configurado |
| **Galicia** | **L0 conjunto** | **contratos** | **Trimestral** |
| **BORME** | **Schema separado** | **empresas, cargos** | **Trimestral** |

---

## Interfaces

### CLI (`licitia-etl`)

Instalable via `pip install -e .`. Punto de entrada: `etl.cli:main`.

| Comando | Descripción |
|---------|-------------|
| `licitia-etl status` | Comprueba conexión a la base de datos |
| `licitia-etl init-db` | Aplica esquemas y carga datos estáticos (CPV, DIR3, Provincias, CCAA) |
| `licitia-etl ingest` *conjunto* *subconjunto* [*--anos*] | Descarga/genera parquet e ingesta en tablas L0 |
| `licitia-etl scheduler register` [*conjuntos*] | Registra tareas programadas |
| `licitia-etl scheduler run` | Inicia el scheduler (ejecuta tareas según frecuencia) |
| `licitia-etl scheduler stop` | Detiene el proceso del scheduler |
| `licitia-etl scheduler status` | Lista tareas con última ejecución, estado y próxima ejecución |
| `licitia-etl health` | Comprueba BD y schema scheduler (código de salida 0/1) |
| `licitia-etl db-info` | Muestra tamaño por schema y listado de tablas |
| `licitia-etl borme ingest --anos 2020-2026` | Scrape + parse + load en schema borme |
| `licitia-etl borme anomalias --anos 2020-2026 [--anonimizar]` | Detector de anomalías vs L0 nacional |

Orden recomendado: `status` → `init-db` → `ingest`. Para ejecución programada: `scheduler register` → `scheduler run`.

### API REST (FastAPI)

Puerto configurable (defecto: 8000). Expone las mismas operaciones que el CLI para integración programática.

Documentación interactiva disponible en `/docs` (Swagger UI). Referencia completa en `docs/api-reference.md`.

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Estado del servicio, conectividad BD y migraciones pendientes |
| `/status` | GET | Disponibilidad de la BD (OK / 503) |
| `/db-info` | GET | Schemas, tablas y tamaño total de la BD |
| `/migrations` | GET | Historial de migraciones aplicadas |
| `/init-db` | POST | Aplica migraciones de infraestructura pendientes |
| `/ingest/conjuntos` | GET | Catálogo de conjuntos/subconjuntos disponibles |
| `/ingest/run` | POST | Lanza una ingesta (mismo efecto que `licitia-etl ingest`) |
| `/ingest/current-run` | GET | Ejecución de ingesta activa en el scheduler |
| `/ingest/log` | GET | Tail del log del subproceso de ingesta |
| `/scheduler/status` | GET | Estado de tareas programadas y estado del bucle (`loop_running`) |
| `/scheduler/running` | GET | Ejecuciones activas con metadatos de tarea |
| `/scheduler/register` | POST | Registra tareas del scheduler (acepta `conjuntos[]` o `tasks[]`) |
| `/scheduler/run` | POST | Arranca el scheduler o ejecuta una tarea concreta |
| `/scheduler/stop` | POST | Detiene el proceso del scheduler (SIGTERM) |
| `/scheduler/runs/stop` | POST | Detiene ejecuciones seleccionadas por `run_id` (SIGTERM) |
| `/scheduler/recover` | POST | Marca ejecuciones huérfanas como fallidas |
| `/scheduler/unregister` | POST | Elimina tareas programadas por `(conjunto, subconjunto)` |
| `/borme/ingest` | POST | Ingesta BORME (non-blocking) |
| `/borme/anomalias` | POST | Detección de anomalías BORME (non-blocking) |
| `/borme/jobs/{job_id}` | GET | Estado de un job BORME |
| `/borme/log` | GET | Tail del log BORME |

---

## Qué hace este servicio

1. **Esquemas y datos estáticos** — Crea schemas (`dim`, `scheduler`, L0) y carga dimensiones estáticas (CPV ~9.450 códigos, CCAA, Provincias, DIR3).
2. **Ingesta L0** — Descarga datos de fuentes públicas (Parquet/XLSX), los transforma y carga en tablas L0 del schema de trabajo.
3. **Scheduler** — Ejecuta ingestas programadas por frecuencia (Mensual/Trimestral/Anual) con registro de ejecuciones, log y próxima ejecución.
4. **Supervisión** — Health check con conectividad BD, info de schemas y tablas.

### Conjuntos soportados

Los conjuntos de datos se registran en `CONJUNTOS_REGISTRY` (`etl/ingest_l0.py`). Cada conjunto define sus subconjuntos, frecuencia de actualización y lógica de descarga/procesamiento.

---

## Setup

### Docker (recomendado)

```bash
cp .env.example .env
# Editar .env con credenciales de la BD
docker compose up -d
docker compose exec etl licitia-etl init-db
docker compose exec etl licitia-etl ingest nacional licitaciones
```

### Local

```bash
cp .env.example .env
pip install -e .
licitia-etl status
licitia-etl init-db
```

### Configuración

Variables de entorno en `.env`:

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DB_HOST` | Host de PostgreSQL | `localhost` |
| `DB_PORT` | Puerto | `5432` |
| `DB_NAME` | Nombre de la BD | `licitaciones` |
| `DB_USER` | Usuario | `postgres` |
| `DB_PASSWORD` | Contraseña | — |
| `DB_SCHEMA` | Schema de trabajo (L0) | `raw` |
| `DIM_SCHEMA` | Schema de dimensiones | `dim` |
| `INGEST_BATCH_SIZE` | Tamaño de lote para inserts | `8192` |

---

## Estructura del proyecto

```
etl/
├── cli.py           # CLI (argparse) — punto de entrada licitia-etl
├── api.py           # API REST (FastAPI)
├── config.py        # Configuración (env vars → DATABASE_URL)
├── ingest_l0.py     # Lógica de ingesta L0 + CONJUNTOS_REGISTRY
├── scheduler.py     # Scheduler (register, run, stop, status)
├── dir3_ingest.py   # Ingesta de datos DIR3 (XLSX público)
├── __init__.py
└── __main__.py
schemas/             # DDL SQL (dim, scheduler, L0)
nacional/            # Scripts de extracción — conjunto nacional
catalunya/           # Scripts de extracción — conjunto catalán
valencia/            # Scripts de extracción — conjunto valenciano
tests/               # Tests (unit + integration)
```

---

## Últimos cambios (v1.2.0 — 2026-03-25)

- **DDL Nacional v2**: 8 nuevas columnas en tablas `nacional_*` — `valor_estimado_contrato` (corrección de mapeo de importes), referencias documentales PCAP/PPT (`doc_legal_*`, `doc_tecnico_*`, `docs_adicionales`), `criterios_adjudicacion` y `requisitos_solvencia` (ambas JSONB).
- **Migración 011** aplicable vía `POST /init-db` o `licitia-etl init-db`.
- **Fix importe**: `EstimatedOverallContractAmount` ya no se mapea a `importe_sin_iva` sino a `valor_estimado_contrato`.
- **Sanitización de URLs**: entidades HTML (`&amp;` → `&`) normalizadas en todas las URLs extraídas de atoms PLACSP.
- Ver [CHANGELOG.md](CHANGELOG.md) para el detalle completo.

## Historial de versiones

| Versión | Fecha | Resumen |
|---------|-------|---------|
| **1.2.0** | 2026-03-25 | DDL Nacional v2: 8 columnas nuevas, fix importe, criterios adjudicación, requisitos solvencia |
| **1.1.2** | 2026-03-25 | Sync upstream: Andalucía refactor, Asturías script, calidad module; data hygiene |
| **1.1.1** | 2026-03-03 | Scheduler CRUD, `loop_running`, `POST /scheduler/runs/stop`, `POST /scheduler/unregister` |
| **1.1.0** | 2026-02-27 | Galicia L0, BORME ingest + anomalías, API REST completa |
| **1.0.0** | 2026-02-17 | Scheduler, `init-db`, primera release de producción |

Ver [CHANGELOG.md](CHANGELOG.md) para el historial completo.

---

## Créditos

- **Repositorio original de extracción de datos públicos:** [@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
- **Mantenedor del CLI y API (licitia-etl):** [Sergio Berino](https://es.linkedin.com/in/sergio-emilio-berino-a19a53328/en) | [Grupo TOP Digital](https://grupotopdigital.es/)
