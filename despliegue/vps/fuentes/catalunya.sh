# Catalunya (Socrata y CKAN: RPC, Generalitat, PSCP, Barcelona...). Sin semilla: el script no la admite;
# el crudo guarda versiones en _historico/ y el Parquet acumula todas (_en_ultima_descarga).
LIMITE=8h
CMD_PRIMERA='set -e; python /repo/scripts/ccaa_cataluna.py --salida /datos/crudo && python /repo/scripts/ccaa_cataluna_parquet.py --entrada /datos/crudo --salida /datos/parquet'
CMD_SEMANAL="$CMD_PRIMERA"
