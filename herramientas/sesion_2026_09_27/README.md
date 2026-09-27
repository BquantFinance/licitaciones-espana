# Herramientas de la sesión del 2026-09-27

Scripts de apoyo para reanudar desde otra sesión lo que se hizo aquí:
- la regeneración de la PLACSP y de calidad para la issue #6;
- la descarga completa de `/contracts` de Euskadi;
- la medición de la cobertura de contratos menores (`docs/COBERTURA.md` §0 y §5).

No son parte de los scrapers. Todos trabajan fuera del repo, en `$TRABAJO`:

```bash
export TRABAJO=/ruta/fuera/del/repo     # ZIP, salidas y logs (~30 GB para la PLACSP completa)
export PY=/ruta/al/python               # con las dependencias de requirements.txt
```

## Regeneración PLACSP + calidad (issue #6)
Estructura esperada:
- `$TRABAJO/regen/{zips,salida,logs,entrega}`.
- `$TRABAJO/placsp_real/semillas/`: `licitaciones_espana.parquet` y `licitaciones_completo_2012_2026.parquet` del release v2026.02.
- `$TRABAJO/regen/externos/ted/ted_es_can.parquet` y `$TRABAJO/regen/externos/borme/data/borme_empresas_pub.parquet`: del LFS del repo.

Pasos:
1. `regeneracion_placsp/descargar.sh`: todos los ZIP de los 5 conjuntos en paralelo (~12 GB, ~1 h). Al acabar escribe `TODO_TERMINADO` en `logs/descarga_fin.txt`.
2. `regeneracion_placsp/procesar.sh`: `nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-2026 --procesos 3 --sin-csv` con las dos semillas. Pico de ~5 GB de RAM y ~10 GB de partes en disco.
   - Los ZIP de un conjunto se pueden borrar en cuanto el log los da por leídos (✓). Antes hay que guardar su sha256 para las notas del release.
3. `regeneracion_placsp/cadena.sh`: espera al procesado y hace tres pasos, cada uno con su log.
   - `reducir.py`: columnas de v2026.02, más `valor_estimado_contrato`, `sara`, las marcas de versión y la procedencia.
   - `cruce_ted.py`: `ted/run_ted_crossvalidation.py` con otras rutas, con `anios_ted` (cobertura del snapshot TED).
   - `calidad/calidad_licitaciones.py`: con todas las versiones y, si no cabe en memoria, `--solo-ultima-version`.
4. `regeneracion_placsp/comparar_publicado.py <nacional regenerado> <v2026.02> <salida.json>`: contraste por fichero de origen (memoria acotada), con las cifras de la PR #24 como referencia.
5. `regeneracion_placsp/publicar_release.py crear|subir|ver`: release en **borrador** (nunca publica), con el token de la sesión (`GITHUB_TOKEN`) y comprobación del límite de 2 GiB por fichero. Plantilla de notas: `notas_release.md`.

`medir.py` envuelve un comando e imprime el tiempo y el pico de memoria (no hay `/usr/bin/time`).

## Euskadi `/contracts` completo
1. `euskadi/contratos_api_tramo.py <año_desde> <año_hasta>`: una ventana mensual tras otra de un tramo, con la reanudación y la publicación por ventana del scraper. Se lanzaron 4 a la vez desde `Euskadi/`: 2000-2019, 2020-2021, 2022-2023 y 2024-2026.
2. `euskadi/terminar.sh`: espera a los tramos y lanza la ejecución normal (`euskadi/contratos_api.py`: ventanas a refrescar, `posteriores`, `_estado.json` y registros sin fecha). Después consolida y mide (`euskadi/consolidar_medir.py`).
   - La consolidación escribe en `$TRABAJO/euskadi/consolidado`, **no** en `Euskadi/euskadi_parquet`, que tiene datos del LFS.

## Cobertura de contratos menores
- `cobertura/medir_placsp.py <parquet PLACSP> <prefijo>`: menores del 1143 por CCAA (NUTS), año y tipo de órgano (DIR3).
- `cobertura/municipios_placsp.py <parquet PLACSP> <salida.csv>`: municipios con algún menor en el 1143 frente a los del INE.
- `cobertura/contar_regionales.py <TRABAJO>`: menores por año de cada fuente regional ya descargada.
- `cobertura/montar_seccion5.py`: monta §5 de COBERTURA con los inventarios de los agentes.

## Varios
`vigia_disco.sh`: pausa (SIGSTOP) las descargas largas (Madrid, Euskadi) si quedan menos de 2,5 GB libres y las reanuda (SIGCONT) al pasar de 3,5 GB.
