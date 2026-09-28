# Comunidad de Madrid. El script escribe junto a sí mismo: se ejecuta una copia idéntica dentro de /datos.
# Primera vez: los CSV del release, con su fecha, son la primera versión de csv_originales/ (así lo pide
# el propio script); la descarga los compara y deja los anteriores en csv_originales/_historico/.
# La copia es atómica (a un directorio temporal y después mv): una copia cortada no se confunde con
# una hecha. El semanal no arranca sin csv_originales/ (la primera ejecución tiene que haberse hecho).
# --semilla solo se admite en 'unificar'.
LIMITE=20h
read -r -d '' PREPARAR_PRIMERA <<'PREP'
set -e
if [ -d "$DATOS/csv_originales" ]; then echo "csv_originales ya existe: no se toca"; exit 0; fi
ORIG="$H/semillas/release_v2026.02/extraido/comunidad_madrid/csv_originales"
rm -rf "$DATOS/.csv_originales.tmp"   # solo puede ser una copia anterior cortada de este mismo paso
cp -r --preserve=timestamps "$ORIG" "$DATOS/.csv_originales.tmp"
chmod -R u+w "$DATOS/.csv_originales.tmp"
[ "$(ls "$DATOS/.csv_originales.tmp" | wc -l)" -eq "$(ls "$ORIG" | wc -l)" ] || { echo "copia incompleta"; exit 1; }
mv "$DATOS/.csv_originales.tmp" "$DATOS/csv_originales"
echo "copiados $(ls "$DATOS/csv_originales" | wc -l) CSV del release a csv_originales/"
PREP
PREPARAR_SEMANAL='[ -d "$DATOS/csv_originales" ] || { echo "falta csv_originales/: primero la ejecución primera"; exit 1; }'
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
cp /repo/comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py /datos/ && sha256sum /repo/comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py /datos/descarga_contratacion_comunidad_madrid_v1.py
python /datos/descarga_contratacion_comunidad_madrid_v1.py todo
python /datos/descarga_contratacion_comunidad_madrid_v1.py unificar --semilla /semillas/release_v2026.02/extraido/comunidad_madrid/contratacion_comunidad_madrid_completo.parquet
CMD
CMD_SEMANAL="$CMD_PRIMERA"
