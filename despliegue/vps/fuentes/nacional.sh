# PLACSP (nacional/licitaciones.py). Semillas: las dos tablas nacionales del release v2026.02.
# Primera vez: descarga en paralelo como herramientas/sesion_2026_09_27/regeneracion_placsp/descargar.sh
# (desde 2012) y procesado con semilla. Semanal: descarga lo que cambió y reprocesa todo.
#
# --solo-descargar sale con 0 aunque falle algún ZIP (licitaciones.py): el éxito de la descarga se
# decide aquí con las 9 líneas de control y el informe de cada log («; error: X», «AÑOS SIN NINGÚN
# FICHERO»). Sin descarga completa no se procesa: un (conjunto, año) sin ZIP quedaría fuera del ámbito
# de la semilla. Sin `set -e` en la descarga: mataría la subshell antes de escribir su línea de control.
# Las partes temporales de un procesado cortado (salida/.*.partes-*) no las recoge el script en otro
# contenedor (su dueño se identifica por hostname y PID): se apartan a salida/_partes_huerfanas/.
CPUS=4; MEM=12g; LIMITE=10h
read -r -d '' CMD_PRIMERA <<'CMD'
ANO=$(date +%Y); S=/semillas/release_v2026.02/extraido/nacional
mkdir -p zips salida logs_descarga
for d in /datos/salida/.*.partes-*; do
  [ -e "$d" ] || continue
  mkdir -p /datos/salida/_partes_huerfanas && mv "$d" /datos/salida/_partes_huerfanas/ && echo "apartada parte huérfana: $d"
done
: > logs_descarga/fin.txt
bajar() { python /repo/nacional/licitaciones.py --solo-descargar --conjunto "$1" --anos "$2" --data-dir /datos/zips --output-dir /datos/tmp_descarga > "logs_descarga/$1_$2.log" 2>&1; echo "$1 $2 exit=$?" >> logs_descarga/fin.txt; }
bajar licitaciones 2012-2018 & bajar licitaciones 2019-2021 & bajar licitaciones 2022-2023 & bajar licitaciones 2024-$ANO &
bajar agregacion 2016-$ANO & bajar menores 2018-2021 & bajar menores 2022-$ANO & bajar encargos 2022-$ANO & bajar consultas 2022-$ANO &
wait
cat logs_descarga/fin.txt
if [ "$(grep -c 'exit=0$' logs_descarga/fin.txt)" -ne 9 ]; then echo "DESCARGA INCOMPLETA: no hay 9 descargas con exit=0"; exit 1; fi
if grep -H -E '; error: [^-]|AÑOS SIN NINGÚN FICHERO' logs_descarga/*.log; then echo "DESCARGA INCOMPLETA: ZIP con error o años sin ficheros (ver arriba)"; exit 1; fi
set -e
python /repo/nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-$ANO --data-dir /datos/zips --output-dir /datos/salida --procesos 3 --sin-csv --semilla $S/licitaciones_espana.parquet --semilla $S/licitaciones_completo_2012_2026.parquet
CMD
read -r -d '' CMD_SEMANAL <<'CMD'
ANO=$(date +%Y); S=/semillas/release_v2026.02/extraido/nacional
for d in /datos/salida/.*.partes-*; do
  [ -e "$d" ] || continue
  mkdir -p /datos/salida/_partes_huerfanas && mv "$d" /datos/salida/_partes_huerfanas/ && echo "apartada parte huérfana: $d"
done
python /repo/nacional/licitaciones.py --conjunto todos --anos 2012-$ANO --data-dir /datos/zips --output-dir /datos/salida --procesos 3 --sin-csv --semilla $S/licitaciones_espana.parquet --semilla $S/licitaciones_completo_2012_2026.parquet 2>&1 | tee /datos/ultima_semanal.log
rc=${PIPESTATUS[0]}
[ "$rc" -ne 0 ] && exit "$rc"
# Un ZIP que no se pudo refrescar se sigue leyendo en su versión anterior (no retira nada), pero se avisa
if grep -E '; error: [^-]|AÑOS SIN NINGÚN FICHERO' /datos/ultima_semanal.log; then echo "AVISO: algún ZIP no se refrescó (código 11)"; exit 11; fi
CMD
