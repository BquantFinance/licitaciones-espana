# Valencia REGCON y resto de datasets de dadesobertes.gva.es. Sin semilla (el repo la descarta: release incompatible).
LIMITE=6h
CMD_PRIMERA='set -e; python /repo/scripts/ccaa_valencia.py --salida /datos/crudo && python /repo/scripts/ccaa_valencia_parquet.py --entrada /datos/crudo --salida /datos/parquet'
CMD_SEMANAL="$CMD_PRIMERA"
