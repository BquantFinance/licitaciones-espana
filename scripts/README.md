# scripts/

Standalone utility and downloader scripts. These run independently of `etl ingest` and are not wired into `CONJUNTOS_REGISTRY`.

## Downloaders / scrapers by CCAA

| Script | What it does |
|--------|-------------|
| `ccaa_andalucia.py` | Scrapes licitaciones from Junta de Andalucía, outputs CSV |
| `ccaa_andalucia_parquet.py` | Converts Andalucía CSV (from above) to Parquet |
| `ccaa_asturias.py` | Downloads all years of Asturias contracts, outputs Parquet to `asturias_data/` |
| `ccaa_cataluna.py` | Full download of Catalunya public contracting data, outputs CSV |
| `ccaa_cataluna_contratosmenores.py` | Catalunya minor contracts scraper (bypasses 10k limit via multi-dimensional segmentation) |
| `ccaa_cataluna_parquet.py` | Converts Catalunya CSV to Parquet |
| `ccaa_valencia.py` | Full download of Comunitat Valenciana open data, outputs CSV |
| `ccaa_valencia_parquet.py` | Converts Valencia CSVs to Parquet (one file per category) |

## Utilities

| Script | What it does |
|--------|-------------|
| `cpv_dim_convert.py` | Reads `schemas/cpv_code.sql` and produces `schemas/001b_dim_cpv.sql` with CPV dimension table |
| `create_indexes_l0_nacional.sql` | B-tree indexes on L0 national tables; run with `psql -f` after ingest |
| `health.py` | Checks Python version, virtualenv, required packages, and optional tmp/staging dirs |
| `validate_schemas_parquet.py` | (WIP) Future schema validation for Parquet → SQL translation phase |

---

## Running `ccaa_asturias.py` manually

```bash
# From the repo root (with virtualenv active):
python scripts/ccaa_asturias.py
```

Output is written to `asturias_data/asturias_contracts_ALL_YEARS.parquet` (plus a CSV fallback and a 1000-row sample).

Dependencies: `requests`, `pandas`, `numpy`, `pyarrow`.

> **Note:** Asturias is **not** wired into `etl ingest` yet. Full L0 ingest and scheduler integration is planned for **1.2.0** (requires DDL/schema work for the CCAA layer).
