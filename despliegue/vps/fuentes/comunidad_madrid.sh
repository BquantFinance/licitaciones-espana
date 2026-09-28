# Comunidad de Madrid. El script escribe junto a sí mismo: se ejecuta una copia idéntica dentro de /datos.
# Primera vez: los CSV del release, con su fecha, son la primera versión de csv_originales/ (así lo pide
# el propio script); la descarga los compara y deja los anteriores en csv_originales/_historico/.
# --semilla solo se admite en 'unificar'.
LIMITE=20h
PREPARAR_PRIMERA='if [ ! -d "$DATOS/csv_originales" ]; then cp -r --preserve=timestamps "$H/semillas/release_v2026.02/extraido/comunidad_madrid/csv_originales" "$DATOS/" && chmod -R u+w "$DATOS/csv_originales" && echo "copiados $(ls "$DATOS/csv_originales" | wc -l) CSV del release"; else echo "csv_originales ya existe: no se toca"; fi'
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
cp /repo/comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py /datos/ && sha256sum /repo/comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py /datos/descarga_contratacion_comunidad_madrid_v1.py
python /datos/descarga_contratacion_comunidad_madrid_v1.py todo
python /datos/descarga_contratacion_comunidad_madrid_v1.py unificar --semilla /semillas/release_v2026.02/extraido/comunidad_madrid/contratacion_comunidad_madrid_completo.parquet
CMD
CMD_SEMANAL="$CMD_PRIMERA"
