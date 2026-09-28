# TED (ted/ted_module.py). Escribe junto al script: se ejecuta una copia idéntica dentro de /datos.
# La validación contra PLACSP es aparte (cadena de calidad).
# `download` sale con 1 si no guarda ted_es_can.parquet (año de la API incompleto, sin datos, semilla
# ausente) o si el CSV de algún año trae registros irregulares (se guardan en
# ted_can_<año>_registros_irregulares.csv y lo descargado también: es un aviso). En la primera, además,
# falla si al terminar no existe ted_es_can.parquet. En el semanal no se puede exigir que el fichero
# cambie (guardar_version no lo toca si el contenido es idéntico).
LIMITE=8h
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
cp /repo/ted/ted_module.py /datos/ted_module.py && sha256sum /repo/ted/ted_module.py /datos/ted_module.py
python /datos/ted_module.py download --semilla /semillas/release_v2026.02/extraido/ted/ted_es_can.parquet
[ -s /datos/ted_es_can.parquet ] || { echo "TED no ha guardado ted_es_can.parquet"; exit 1; }
CMD
read -r -d '' CMD_SEMANAL <<'CMD'
set -e
cp /repo/ted/ted_module.py /datos/ted_module.py && sha256sum /repo/ted/ted_module.py /datos/ted_module.py
python /datos/ted_module.py download --semilla /semillas/release_v2026.02/extraido/ted/ted_es_can.parquet
CMD
