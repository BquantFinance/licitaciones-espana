# licitaciones-espana — ETL Microservice

Fork de [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana) que extiende el repositorio original de extracción de datos públicos de contratación con un **CLI** (`licitia-etl`) y una **API REST** (FastAPI) para su uso como microservicio conectable.

Sincronizado con BquantFinance/licitaciones-espana (commit e594735, 2026-02-27).

## Arquitectura

```
┌─────────────────────────────────────────────┐
│          licitaciones-espana (ETL)           │
│                                             │
│  ┌──────────┐          ┌──────────────────┐ │
│  │   CLI    │          │    API REST      │ │
│  │licitia-etl│         │  FastAPI :8001   │ │
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

Puerto configurable (defecto: 8001). Expone las mismas operaciones que el CLI para integración programática.

Documentación interactiva disponible en `/docs` (Swagger UI). Referencia completa en `docs/api-reference.md`.

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Estado del servicio y conectividad BD |
| `/ingest/conjuntos` | GET | Catálogo de conjuntos/subconjuntos disponibles |
| `/ingest/run` | POST | Lanza una ingesta (mismo efecto que `licitia-etl ingest`) |
| `/ingest/status` | GET | Estado de la ingesta en curso |
| `/scheduler/tasks` | GET | Lista tareas programadas con estado |
| `/scheduler/register` | POST | Registra tareas del scheduler (acepta `conjuntos[]` o `tasks[]`) |
| `/scheduler/run` | POST | Arranca el scheduler |
| `/scheduler/stop` | POST | Detiene el scheduler |
| `/scheduler/status` | GET | Estado de tareas programadas y estado del bucle (`loop_running`) |
| `/scheduler/running` | GET | Ejecuciones activas con metadatos de tarea |
| `/scheduler/runs/stop` | POST | Detiene ejecuciones seleccionadas por `run_id` (SIGTERM) |
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

## Changelog

### v1.1.0 (2026-02-27)

- Upstream sync con BquantFinance/licitaciones-espana (commit e594735)
- **Galicia** como nuevo L0 conjunto
- **BORME** integration (ingest + detección de anomalías)
- API REST y CLI para las nuevas fuentes de datos
- Limpieza de datos estáticos (todos los datos provienen de scrapers en runtime)
- Documentación de la API en `/docs`
- Uso de disco del host vía API (`/statfs-validation`)

---

## Últimos cambios (v1.1.2 — 2026-03-25)

- **Sync upstream**: Andalucía scraper refactorizado (11 nuevos tests), nuevo script `ccaa_asturias.py` para el portal open-data de Asturias (wiring de ingesta diferido a 1.2.0).
- **Calidad analytics**: módulo `calidad/calidad_licitaciones.py` con 20 indicadores de calidad; herramienta standalone, no integrada en el pipeline de ingesta.
- **8 conjuntos** disponibles vía `GET /ingest/conjuntos`; superficie de API sin cambios respecto a 1.1.1.
- `requirements.txt`: añadido `requests>=2.31.0`; pin `numpy<2` preservado.
- Ver [CHANGELOG.md](CHANGELOG.md) para el detalle completo de cambios.

## Historial de cambios anterior (v1.2)

- **Scheduler CRUD completo** — Nuevos endpoints: `POST /scheduler/register` acepta lista de `(conjunto, subconjunto)`, `GET /scheduler/running` (ejecuciones activas), `POST /scheduler/runs/stop` (detener por run_id), `POST /scheduler/unregister` (eliminar tareas programadas).
- **Estado del bucle** — `GET /scheduler/status` incluye campo `loop_running` para conocer si el daemon del scheduler está activo.
- **Fix get_next_run_at** — Corrección en la función de cálculo de próxima ejecución: la comparación ahora usa `last_finished_at` en lugar de `now` para determinar el próximo slot programado.

---

## Créditos

- **Repositorio original de extracción de datos públicos:** [@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
- **Mantenedor del CLI y API (licitia-etl):** [Sergio Berino](https://es.linkedin.com/in/sergio-emilio-berino-a19a53328/en) | [Grupo TOP Digital](https://grupotopdigital.es/)
