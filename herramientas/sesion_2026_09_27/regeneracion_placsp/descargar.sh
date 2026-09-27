#!/bin/bash
: "${TRABAJO:?Defina TRABAJO: carpeta de trabajo fuera del repo (ZIP, salidas, logs)}"
PY=${PY:-python}
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$REPO"
R=$TRABAJO/regen
bajar() { $PY nacional/licitaciones.py --solo-descargar --conjunto $1 --anos $2 --data-dir $R/zips --output-dir $R/tmp_out > $R/logs/descarga_$1_$2.log 2>&1; echo "$1 $2 exit=$?" >> $R/logs/descarga_fin.txt; }
bajar licitaciones 2013-2018 &
bajar licitaciones 2019-2021 &
bajar licitaciones 2022-2023 &
bajar licitaciones 2024-2026 &
bajar agregacion 2016-2026 &
bajar menores 2018-2021 &
bajar menores 2022-2026 &
bajar encargos 2022-2026 &
bajar consultas 2022-2026 &
wait
echo TODO_TERMINADO >> $R/logs/descarga_fin.txt
