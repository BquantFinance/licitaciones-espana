#!/bin/bash
: "${TRABAJO:?Defina TRABAJO: carpeta de trabajo fuera del repo (ZIP, salidas, logs)}"
PY=${PY:-python}
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
# Procesado completo de la PLACSP con el código de la rama (--semilla v2026.02)
cd "$REPO"
( while true; do echo "$(date -u +%H:%M:%S) $(free -m | awk '/Mem:/{print "usada " $3 " MB, disponible " $7 " MB"}') $(df -h "$TRABAJO" | awk 'NR==2{print "disco libre " $4}')"; sleep 60; done ) > $TRABAJO/regen/logs/recursos.log 2>&1 &
VIGIA=$!
$PY $REPO/herramientas/sesion_2026_09_27/regeneracion_placsp/medir.py $PY nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-2026 \
    --data-dir $TRABAJO/regen/zips --output-dir $TRABAJO/regen/salida --procesos 3 --sin-csv \
    --semilla $TRABAJO/placsp_real/semillas/licitaciones_espana.parquet --semilla $TRABAJO/placsp_real/semillas/licitaciones_completo_2012_2026.parquet \
    > $TRABAJO/regen/logs/procesado.log 2>&1
echo "procesado exit=$?" >> $TRABAJO/regen/logs/procesado.log
kill $VIGIA
