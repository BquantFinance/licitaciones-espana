# Andalucía: licitaciones estándar y menores (proxy Elasticsearch del portal); semilla por id_expediente.
LIMITE=20h
CMD_PRIMERA='python /repo/scripts/ccaa_andalucia.py scrape --salida /datos --semilla /semillas/release_v2026.02/extraido/andalucia/licitaciones_andalucia.parquet'
CMD_SEMANAL="$CMD_PRIMERA"
