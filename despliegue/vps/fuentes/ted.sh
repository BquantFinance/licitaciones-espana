# TED (ted/ted_module.py). Escribe junto al script: se ejecuta una copia idéntica dentro de /datos.
# La validación contra PLACSP es aparte (cadena de calidad).
LIMITE=8h
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
cp /repo/ted/ted_module.py /datos/ted_module.py && sha256sum /repo/ted/ted_module.py /datos/ted_module.py
python /datos/ted_module.py download --semilla /semillas/release_v2026.02/extraido/ted/ted_es_can.parquet
CMD
CMD_SEMANAL="$CMD_PRIMERA"
