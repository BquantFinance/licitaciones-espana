#!/bin/bash
: "${TRABAJO:?Defina TRABAJO: carpeta de trabajo fuera del repo (ZIP, salidas, logs)}"
PY=${PY:-python}
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
# Tras procesar.sh: tabla reducida -> cruce TED -> calidad (todas las versiones;
# si no cabe en memoria, solo la última versión). Cada paso deja su log.
R=$TRABAJO/regen
MED=$REPO/herramientas/sesion_2026_09_27/regeneracion_placsp/medir.py
cd "$REPO"
until grep -q "procesado exit=" $R/logs/procesado.log 2>/dev/null; do sleep 60; done
if ! grep -q "procesado exit=0" $R/logs/procesado.log; then echo "EL PROCESADO FALLÓ: no se sigue"; exit 1; fi
# Nombre con años: desde el nombre fijo es un enlace (nacional/licitaciones.py, nombres_salida); sin
# enlaces (p.ej. Windows sin el modo de desarrollador), el nombre fijo
P=$R/salida/licitaciones_completo_2012_2026.parquet
[ -e "$P" ] || P=$R/salida/licitaciones_completo.parquet
[ -f "$P" ] || { echo "no está $P"; ls -la $R/salida; exit 1; }
mkdir -p $R/entrega
echo "$(date -u +%T) reducir"; $PY $MED $PY $R/reducir.py $P $R/entrega/nacional.parquet > $R/logs/reducir.log 2>&1 || { echo "reducir falló"; tail -20 $R/logs/reducir.log; exit 1; }
echo "$(date -u +%T) cruce TED"; $PY $MED $PY $R/cruce_ted.py $R/entrega/nacional.parquet $R/externos/ted/ted_es_can.parquet $R/entrega/ted > $R/logs/cruce_ted.log 2>&1 || { echo "cruce TED falló"; tail -30 $R/logs/cruce_ted.log; exit 1; }
echo "$(date -u +%T) calidad (todas las versiones)"
if $PY $MED $PY calidad/calidad_licitaciones.py -i $R/entrega/nacional.parquet -o $R/entrega/calidad \
      --ted $R/entrega/ted/crossval_sara.parquet --borme $R/externos/borme/data/borme_empresas_pub.parquet > $R/logs/calidad.log 2>&1; then
  echo "calidad completa"
else
  echo "$(date -u +%T) calidad con todas las versiones falló ($(tail -1 $R/logs/calidad.log)); se repite con --solo-ultima-version"
  mv $R/logs/calidad.log $R/logs/calidad_todas_fallo.log
  $PY $MED $PY calidad/calidad_licitaciones.py -i $R/entrega/nacional.parquet -o $R/entrega/calidad_ultima \
      --ted $R/entrega/ted/crossval_sara.parquet --borme $R/externos/borme/data/borme_empresas_pub.parquet \
      --solo-ultima-version > $R/logs/calidad.log 2>&1 || { echo "calidad falló"; tail -30 $R/logs/calidad.log; exit 1; }
fi
echo "$(date -u +%T) CADENA TERMINADA"; ls -la $R/entrega $R/entrega/* | head -40
