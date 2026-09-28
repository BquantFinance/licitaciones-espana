# Euskadi: descarga (API /contracts completa, XLSX, municipios) y consolidación.
LIMITE=14h
CMD_PRIMERA='set -e; python /repo/Euskadi/ccaa_euskadi.py --salida /datos/crudo && python /repo/Euskadi/consolidacion_euskadi.py --entrada /datos/crudo --salida /datos/parquet'
CMD_SEMANAL="$CMD_PRIMERA"
