# calidad/

Analytics module from upstream (added in 1.1.2). **Not part of the ingest pipeline.**

`calidad_licitaciones.py` computes 20 quality indicators over national PLACSP licitaciones data, with optional cross-validation against TED and BORME datasets. It reads existing Parquet files and produces `calidad/calidad_licitaciones_resultado.parquet` as output.

## Standalone usage

```bash
# Minimum: national data only
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet

# With TED and BORME cross-validation
python calidad/calidad_licitaciones.py \
  -i nacional/licitaciones_espana.parquet \
  --ted ted/crossval_sara_v2.parquet \
  --borme borme_empresas.parquet

# Limit rows for testing
python calidad/calidad_licitaciones.py \
  -i nacional/licitaciones_espana.parquet \
  -s 200000
```

This module is **read-only** with respect to the ingest pipeline — it consumes Parquet files produced by `etl ingest` but does not feed back into the ETL or the database schema.
