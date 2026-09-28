#!/bin/bash
# Segunda versión de la cadena (la primera se quedó sin memoria en el cruce TED con las 55
# columnas): TED y calidad sobre subconjuntos de columnas; calidad con la última versión.
: "${TRABAJO:?Defina TRABAJO}"
PY=${PY:-python}
REPO=${REPO:-$(cd "$(dirname "$0")/../../.." && pwd)}
R=$TRABAJO/regen
MED=$REPO/herramientas/sesion_2026_09_27/regeneracion_placsp/medir.py
H=$REPO/herramientas/sesion_2026_09_27/regeneracion_placsp
cd "$REPO"
P=$R/salida/licitaciones_completo_2012_2026.parquet
E=$R/entrega; mkdir -p $E
CODIGOS=tipo_contrato_code,procedimiento_code,estado_code,subtipo_code
TED=id,expediente,organo_contratante,nif_organo,dependencia,tipo_contrato,procedimiento,estado,importe_sin_iva,importe_adjudicacion,adjudicatario,nif_adjudicatario,cpv_principal,fecha_adjudicacion,fecha_updated,conjunto,ano,tipo_registro,valor_estimado_contrato,$CODIGOS
CAL=id,expediente,tipo_contrato,procedimiento,estado,importe_sin_iva,importe_con_iva,importe_adjudicacion,importe_adj_con_iva,adjudicatario,nif_adjudicatario,num_ofertas,cpv_principal,ubicacion,nuts,fecha_limite,fecha_adjudicacion,fecha_publicacion,fecha_updated,url,conjunto,valor_estimado_contrato,organo_contratante,nif_organo,ano,tipo_registro,$CODIGOS
echo "$(date -u +%T) contraste con v2026.02"
$PY $MED $PY $H/comparar_publicado.py $P $TRABAJO/placsp_real/semillas/licitaciones_espana.parquet $E/contraste_v2026_02.json > $R/logs/contraste.log 2>&1; tail -3 $R/logs/contraste.log
echo "$(date -u +%T) subconjunto TED"; $PY $H/subconjunto.py $P $E/nacional_ted.parquet $TED > $R/logs/sub_ted.log 2>&1 || { cat $R/logs/sub_ted.log; exit 1; }
echo "$(date -u +%T) cruce TED"; $PY $MED $PY $H/cruce_ted.py $E/nacional_ted.parquet $R/externos/ted/ted_es_can.parquet $E/ted > $R/logs/cruce_ted.log 2>&1 || { echo "cruce TED falló"; tail -5 $R/logs/cruce_ted.log; exit 1; }
tail -1 $R/logs/cruce_ted.log
echo "$(date -u +%T) subconjunto calidad"; $PY $H/subconjunto.py $P $E/nacional_calidad.parquet $CAL > $R/logs/sub_cal.log 2>&1 || { cat $R/logs/sub_cal.log; exit 1; }
echo "$(date -u +%T) calidad (última versión)"
$PY $MED $PY calidad/calidad_licitaciones.py -i $E/nacional_calidad.parquet -o $E/calidad_ultima \
    --ted $E/ted/crossval_sara.parquet --borme $R/externos/borme/data/borme_empresas_pub.parquet \
    --solo-ultima-version > $R/logs/calidad.log 2>&1 || { echo "calidad falló"; tail -5 $R/logs/calidad.log; exit 1; }
tail -1 $R/logs/calidad.log
echo "$(date -u +%T) CADENA TERMINADA"
