# PLACSP (nacional/licitaciones.py). Semillas: las dos tablas nacionales del release v2026.02.
# Primera vez: descarga en paralelo como herramientas/sesion_2026_09_27/regeneracion_placsp/descargar.sh
# (desde 2012) y procesado con semilla. Semanal: descarga lo que cambió y reprocesa todo.
CPUS=4; MEM=12g; LIMITE=10h
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
ANO=$(date +%Y); S=/semillas/release_v2026.02/extraido/nacional
mkdir -p zips salida logs_descarga
: > logs_descarga/fin.txt
bajar() { python /repo/nacional/licitaciones.py --solo-descargar --conjunto "$1" --anos "$2" --data-dir /datos/zips --output-dir /datos/tmp_descarga > "logs_descarga/$1_$2.log" 2>&1; echo "$1 $2 exit=$?" >> logs_descarga/fin.txt; }
bajar licitaciones 2012-2018 & bajar licitaciones 2019-2021 & bajar licitaciones 2022-2023 & bajar licitaciones 2024-$ANO &
bajar agregacion 2016-$ANO & bajar menores 2018-2021 & bajar menores 2022-$ANO & bajar encargos 2022-$ANO & bajar consultas 2022-$ANO &
wait
cat logs_descarga/fin.txt
if grep -v 'exit=0$' logs_descarga/fin.txt; then echo "ALGUNA DESCARGA FALLÓ"; exit 1; fi
python /repo/nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-$ANO --data-dir /datos/zips --output-dir /datos/salida --procesos 3 --sin-csv --semilla $S/licitaciones_espana.parquet --semilla $S/licitaciones_completo_2012_2026.parquet
CMD
read -r -d '' CMD_SEMANAL <<'CMD'
set -e
ANO=$(date +%Y); S=/semillas/release_v2026.02/extraido/nacional
python /repo/nacional/licitaciones.py --conjunto todos --anos 2012-$ANO --data-dir /datos/zips --output-dir /datos/salida --procesos 3 --sin-csv --semilla $S/licitaciones_espana.parquet --semilla $S/licitaciones_completo_2012_2026.parquet
CMD
