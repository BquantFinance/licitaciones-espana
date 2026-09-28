# Catalunya (Socrata y CKAN: RPC, Generalitat, PSCP, Barcelona...). Sin semilla: el script no la admite;
# el crudo guarda versiones en _historico/ y el Parquet acumula todas (_en_ultima_descarga).
# Si falla algún dataset, el Parquet se construye igual (acumula versiones: una descarga parcial no
# retira nada) y la ejecución sale con el código de la descarga.
LIMITE=8h
CMD_PRIMERA='python /repo/scripts/ccaa_cataluna.py --salida /datos/crudo; a=$?; python /repo/scripts/ccaa_cataluna_parquet.py --entrada /datos/crudo --salida /datos/parquet || exit $?; exit $a'
CMD_SEMANAL="$CMD_PRIMERA"
