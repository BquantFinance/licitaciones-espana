#!/bin/bash
: "${TRABAJO:?Defina TRABAJO: carpeta de trabajo fuera del repo (ZIP, salidas, logs)}"
PY=${PY:-python}
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
# Espera a los 4 tramos, lanza la ejecución normal (ventanas a refrescar, posteriores,
# _estado.json y registros sin fecha) y consolida y mide los menores.
E=$TRABAJO/euskadi
while ps -eo args | grep -q "[c]ontratos_api_tramo.py"; do sleep 60; done
echo "$(date -u +%T) tramos terminados; ejecución normal"
cd "$REPO"/Euskadi && $PY $E/contratos_api.py > $E/contratos_api_final.log 2>&1
echo "exit=$?"; tail -5 $E/contratos_api_final.log
cat $REPO/Euskadi/datos_euskadi_contratacion_v4/A1_api_contratos_completo/_estado.json | head -30
echo "$(date -u +%T) consolidación"
$PY $E/consolidar_medir.py > $E/consolidar_medir.log 2>&1; echo "exit=$?"; tail -25 $E/consolidar_medir.log
