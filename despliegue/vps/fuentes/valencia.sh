# Valencia REGCON y resto de datasets de dadesobertes.gva.es. Sin semilla (el repo la descarta: release incompatible).
# ccaa_valencia.py sale con 1 si falla cualquier dataset; el Parquet se construye igual, porque acumula
# cada CSV sobre todas sus versiones y una descarga parcial no retira nada. Sale con el peor código.
# Solo contratación (REGCON y contratos DANA): la web es de contratación pública. Empleo, paro,
# turismo... se dejan para el modelo; lo ya descargado de ellos no se toca.
LIMITE=6h
CMD_PRIMERA='python /repo/scripts/ccaa_valencia.py --salida /datos/crudo --categorias contratacion; a=$?; python /repo/scripts/ccaa_valencia_parquet.py --entrada /datos/crudo --salida /datos/parquet --categorias contratacion || exit $?; exit $a'
CMD_SEMANAL="$CMD_PRIMERA"
