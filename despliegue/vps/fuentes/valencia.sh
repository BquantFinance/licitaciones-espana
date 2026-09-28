# Valencia REGCON y resto de datasets de dadesobertes.gva.es. Sin semilla (el repo la descarta: release incompatible).
# ccaa_valencia.py sale con 1 si falla cualquier dataset; el Parquet se construye igual, porque acumula
# cada CSV sobre todas sus versiones y una descarga parcial no retira nada. Sale con el peor código.
LIMITE=6h
CMD_PRIMERA='python /repo/scripts/ccaa_valencia.py --salida /datos/crudo; a=$?; python /repo/scripts/ccaa_valencia_parquet.py --entrada /datos/crudo --salida /datos/parquet || exit $?; exit $a'
CMD_SEMANAL="$CMD_PRIMERA"
