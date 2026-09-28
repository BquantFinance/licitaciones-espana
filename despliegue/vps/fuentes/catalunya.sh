# Catalunya (Socrata y CKAN: RPC, Generalitat, PSCP, Barcelona...). El crudo guarda versiones en _historico/
# y el Parquet acumula todas (_en_ultima_descarga). La semilla del release añade, por clave, lo que la ventana
# móvil ya no sirve (SEMILLAS en ccaa_cataluna_parquet.py: 751.187 filas del RPC, 85.397 de la PSCP...).
# Si falla algún dataset, el Parquet se construye igual (acumula versiones: una descarga parcial no
# retira nada) y la ejecución sale con el código de la descarga; si falla la conversión de alguno, con 1.
# Solo contratación (Socrata y Open Data Barcelona): la web es de contratación pública. Subvenciones,
# presupuestos, RRHH... se dejan para el modelo; lo ya descargado de ellas no se toca.
LIMITE=8h
CMD_PRIMERA='python /repo/scripts/ccaa_cataluna.py --salida /datos/crudo --categorias contratacion; a=$?; python /repo/scripts/ccaa_cataluna_parquet.py --entrada /datos/crudo --salida /datos/parquet --categorias contratacion --semilla /semillas/release_v2026.02/extraido/catalunya || exit $?; exit $a'
CMD_SEMANAL="$CMD_PRIMERA"
