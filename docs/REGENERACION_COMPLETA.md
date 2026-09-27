# Reconstrucción completa desde los ZIP oficiales

Este procedimiento complementa la reparación estricta por versión descrita en
`REGENERACION_IMPORTES.md`. Relee todas las entradas de los 78 archivos que el
nacional publicado identifica en `archivo_origen`, sin depender de las fechas
que se perdieron al generar aquel parquet.

## Código y fuentes

- Parser, calidad y TED: revisión `627008b1b40158418b432f92f1a8757e646249bf`
  de la PR #23 del mantenedor. Aún no integrada en `main` al comenzar el trabajo.
- `patches/pr23-ted-nulos.patch`: corrección adicional para campos nullable de
  pandas y expedientes vacíos, con una prueba de regresión.
- Python 3.11+ y `requirements-regeneracion.txt`; esta ejecución usa pandas 3.
- Los ZIP actuales pueden diferir de los descargados para el release original,
  especialmente los anuales de 2026. La comparación registra las diferencias;
  esta selección no equivale a descargar todos los meses disponibles hasta hoy.
- TED y BORME se recalculan contra los parquets publicados del repositorio, no
  contra una nueva descarga íntegra de esos dos sistemas externos.

## Ejecución

Desde la raíz del checkout de trabajo, con el inventario guardado y la copia del
parser en `artifacts/issue-6/upstream-pr23`:

La revisión del parser se prepara una vez (Git LFS no debe descargar todos los
datasets ajenos a este trabajo):

```bash
git fetch origin refs/pull/23/head
GIT_LFS_SKIP_SMUDGE=1 git worktree add --detach artifacts/issue-6/upstream-pr23 \
  627008b1b40158418b432f92f1a8757e646249bf
git -C artifacts/issue-6/upstream-pr23 apply "$(pwd)/docs/patches/pr23-ted-nulos.patch"
git lfs pull --include='nacional/licitaciones_espana.parquet,ted/ted_es_can.parquet,borme/data/borme_empresas_pub.parquet' --exclude=''
python -m pip install -r requirements-regeneracion.txt
```

El inventario se obtiene de las columnas `conjunto` y `archivo_origen` del
nacional publicado, eliminando únicamente pares duplicados de ese inventario,
no filas del dataset. Se guarda como lista de objetos JSON con esos dos campos.

```bash
python -m nacional.descargar_fuentes \
  --inventario artifacts/issue-6/fuentes_requeridas.json \
  --destino artifacts/issue-6/fuentes --workers 3

python -m nacional.regenerar_historico \
  --inventario artifacts/issue-6/fuentes_requeridas.json \
  --fuentes artifacts/issue-6/fuentes \
  --output artifacts/issue-6/reconstruido_v2 \
  --parser-root artifacts/issue-6/upstream-pr23 --workers 3

python -m nacional.consolidar_historico \
  --inventario artifacts/issue-6/fuentes_requeridas.json \
  --particiones artifacts/issue-6/reconstruido_v2 \
  --original nacional/licitaciones_espana.parquet \
  --output artifacts/issue-6/entrega/nacional.parquet

python -m nacional.recalcular_calidad \
  --nacional artifacts/issue-6/entrega/nacional.parquet \
  --ted ted/ted_es_can.parquet \
  --borme borme/data/borme_empresas_pub.parquet \
  --parser-root artifacts/issue-6/upstream-pr23 \
  --output artifacts/issue-6/entrega

python -m nacional.validar_regeneracion \
  --nacional artifacts/issue-6/entrega/nacional.parquet \
  --calidad artifacts/issue-6/entrega/calidad/calidad_licitaciones_resultado.parquet \
  --informe artifacts/issue-6/entrega/validacion.json
```

Durante esta ejecución se solapan descarga y reconstrucción mediante
`--esperar-descargas`. Las particiones terminadas se verifican por hash y se
reutilizan al reanudar. Si cambia el parser o una fuente, se exige otro directorio
de salida. No se sobrescriben los parquets publicados.

## Garantías y límites

- Cada ZIP se comprueba y registra con URL, SHA-256 y tamaño.
- Se conserva una fila por entrada ATOM, incluidas versiones repetidas. Las
  marcas `n_versiones`, `es_ultima_version` y `entrada_repetida` se calculan
  después sobre el conjunto completo, no por bloque.
- Se preserva el detalle de adjudicatarios, lotes, criterios, resultados y
  modificaciones como JSON en `detalle.parquet` de cada partición. No se usa
  para reemplazar silenciosamente los importes principales del expediente.
- Las entradas borradas del feed se conservan en `borrados.jsonl` separado.
  No se elimina su histórico de anuncios.
- Los timestamps se almacenan en microsegundos para conservar fechas originales
  fuera del rango de nanosegundos, como el año 0001. Calidad puede marcarlas
  como inválidas; no se convierten automáticamente en fechas plausibles.
- Cualquier entrada XML no parseada o sin identidad/fecha válida impide dar
  la partición por completa. La consolidación exige todas las particiones.
- La comparación por importes usa versiones no ambiguas: no multiplica filas
  mediante un join muchos-a-muchos entre anuncios repetidos.
- El cruce TED usa el valor estimado corregido y la última versión. Para llevar
  el resultado a calidad se une por `id` y `fecha_updated`, evitando aplicarlo a
  expedientes homónimos de otro órgano o a versiones históricas no evaluadas.
  Esas otras versiones quedan sin evaluación TED, no como falsos positivos.
  El snapshot TED publicado cubre 2010–2025: una ausencia de coincidencia en
  2026 también se deja sin evaluar; no se convierte en un resultado negativo.
  Una coincidencia encontrada sí se conserva. Incluso en años cubiertos,
  "sin coincidencia" no prueba por sí solo que el contrato no se publicara.
- El indicador BORME sigue siendo el contraste de nombres del pipeline del
  mantenedor: no constituye una verificación fiscal o jurídica de la empresa.

Los informes y hashes de la entrega son la evidencia de qué se reconstruyó.
Una ejecución de muestra, pruebas unitarias o el mero cambio de esquema no
sustituyen esa validación completa.
