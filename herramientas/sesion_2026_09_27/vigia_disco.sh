#!/bin/bash
: "${TRABAJO:?Defina TRABAJO: carpeta de trabajo fuera del repo (ZIP, salidas, logs)}"
PY=${PY:-python}
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
# Pausa (SIGSTOP) las descargas de Madrid y Euskadi si queda poco disco y las reanuda (SIGCONT) al liberarse
S=$TRABAJO
while true; do
  libre=$(df --output=avail -k "$TRABAJO" | tail -1)
  pids=$(ps -eo pid,args | grep -E "[d]escarga_contratacion_comunidad_madrid_v1.py menores|[c]ontratos_api_tramo.py|[c]ontratos_api.py" | awk '{print $1}')
  if [ "$libre" -lt 2621440 ]; then
    for p in $pids; do kill -STOP $p 2>/dev/null; done; echo "$(date -u +%T) PAUSA (libre ${libre} KB)" >> $S/vigia_disco.log
  elif [ "$libre" -gt 3670016 ]; then
    for p in $pids; do kill -CONT $p 2>/dev/null; done
  fi
  sleep 60
done
