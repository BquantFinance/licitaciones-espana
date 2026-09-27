# Regeneración de la issue #6 — 27 de septiembre de 2026

## Estado

**Reconstrucción completa y validación final PASS.** Nacional y calidad contienen
8.721.484 filas. Se han recalculado los 20 indicadores; calidad tiene 93 columnas
y ocupa 1.792 MB con compresión Zstandard. El release oficial no se ha sustituido.

El verificador independiente compara exactamente los 71 campos nacionales en
todas las filas: cero modificaciones en calidad, cero claves de procedencia
duplicadas, cero incoherencias en marcas de versión y cero scores incoherentes.
Pasan las 51 pruebas locales y las 88 pruebas TED del pipeline parcheado.

## Nacional: resultado comprobado

Se recuperaron los **78 ZIP oficiales**, 10,97 GB comprimidos. La descarga
terminó en 19 minutos y 17 segundos. Se procesaron todas las entradas sin
descartes ni errores, conservando las versiones y los detalles en particiones
separadas. La reconstrucción se solapó con la descarga.

| Comprobación | Resultado |
| --- | ---: |
| Filas del nacional publicado | 8.693.891 |
| Filas del nacional reconstruido | 8.721.484 |
| Diferencia de filas | +27.593 |
| Identificadores distintos `(conjunto, id)` | 4.744.012 |
| Entradas repetidas, conservadas y marcadas | 88.487 |
| Fechas de actualización nulas en el publicado | 35.627 |
| Fechas de actualización nulas en el reconstruido | 0 |
| Versiones antiguas identificables ausentes en el nuevo | **0** |
| Filas comparables sin ambigüedad por versión y archivo | 8.612.333 |
| `importe_sin_iva` modificado en esas filas | **4.383.029** |
| Diferencias entre antiguo `importe_sin_iva` y nuevo `valor_estimado_contrato` | **0** |
| Diferencias en `importe_con_iva` de esas filas | **0** |
| Importes sin IVA informados en el nuevo dataset | 8.715.779 |

La recuperación de fechas se hace releyendo `atom:updated` con el formato ISO,
sin inferirlo a partir del primer valor de la columna. Las fechas sin
milisegundos ya no se pierden al convivir con fechas que sí los tienen.

Los recuentos de comparación por versión excluyen claves ambiguas y fechas
nulas del parquet anterior. No se afirma una correspondencia individual que
no se ha podido demostrar para esas filas.

## Por qué hay más filas

73 archivos tienen exactamente el mismo recuento que el publicado. Estas cinco
fuentes oficiales contienen más entradas que las atribuidas a ellas en el
parquet anterior:

| Archivo | Antes | Ahora | Diferencia |
| --- | ---: | ---: | ---: |
| Agregación, enero de 2026 | 16.543 | 20.172 | +3.629 |
| Menores, enero de 2026 | 40.465 | 51.681 | +11.216 |
| Licitaciones, enero de 2026 | 39.393 | 48.913 | +9.520 |
| Consultas, anual de 2026 | 548 | 930 | +382 |
| Encargos, anual de 2026 | 317 | 3.163 | +2.846 |

Se sirve una nueva extracción del mismo inventario de fuentes, no una copia
idéntica del snapshot antiguo. Tampoco equivale a incorporar todos los ZIP
mensuales posteriores a enero de 2026.
No disponemos de los ZIP originales de aquel release para distinguir mediante
sus hashes una ampliación de la fuente de una extracción anterior incompleta.

## Semántica y código

- `EstimatedOverallContractAmount` → `valor_estimado_contrato`.
- `TaxExclusiveAmount` → `importe_sin_iva`.
- `TotalAmount` → `importe_con_iva`.
- Parser y pipelines de la PR #23, fijados en
  `627008b1b40158418b432f92f1a8757e646249bf`, más el parche de nulos y
  consolidación de resultados de TED. Las 88 pruebas TED pasan con ese parche.
- Esta revisión también corrige códigos, etiquetas, fechas de publicación y
  conserva detalles. La entrega **no es exclusivamente un cambio de dos
  columnas** sobre el dataset publicado.
- Se conserva toda la historia; para agregar por expediente hay que usar
  `es_ultima_version`. Los importes principales de adjudicación mantienen la
  semántica del primer resultado del parser; el detalle de lotes va separado.

## TED, BORME y límites

Los cruces usan las copias publicadas de TED (591.047 filas, años 2010–2025) y
BORME (9.254.668 filas). No se ha vuelto a descargar íntegramente ninguno de
esos dos sistemas. Sus hashes quedan registrados.

TED se recalcula sobre la última versión y usa el valor estimado. El resultado
que se incorpora a calidad se enlaza por `id` y `fecha_updated`, evitando
propagaciones por simples coincidencias de expediente y adjudicatario.
Una ausencia de coincidencia fuera de los años cubiertos por TED queda sin
evaluar; una coincidencia positiva encontrada sí se conserva. No encontrar una
coincidencia en este snapshot no demuestra que una contratación no se publicara.

BORME conserva el contraste por nombre normalizado del pipeline del mantenedor;
no constituye una comprobación fiscal o jurídica de identidad.

## Archivos y reproducción

Directorio local de entrega: `artifacts/issue-6/entrega/`.

- `nacional.parquet`: histórico reconstruido con las marcas de versión.
- `nacional.json`: cobertura, comparación, hashes y manifiestos de las 78 fuentes.
- `calidad/calidad_licitaciones_resultado.parquet`: resultado final de los 20 indicadores.
- `ted/`: resultados del cruce y tabla de validación por versión.
- `validacion.json`: comprobaciones de integridad de la entrega, resultado PASS.

Los comandos, dependencias y garantías están en
[REGENERACION_COMPLETA.md](REGENERACION_COMPLETA.md). Las fuentes y particiones
permanecen en `artifacts/issue-6/fuentes/` y `artifacts/issue-6/reconstruido_v2/`
para permitir revisar o reanudar el cálculo.

SHA-256 del nacional reconstruido:
`e811667274c09a80a6a104b1e7edaf1eefad23db66356ea797149c2bda3dcd4c`.

SHA-256 del parquet de calidad:
`5b64a572cfa32eeeb6d3941dece0448018d97695cc9cfcadbcbf069045045a76`.

Paquetes preparados: `calidad_regenerada.zip` y `nacional_regenerado.zip`.
Cada paquete incorpora los informes y la validación. `SHA256SUMS` identifica
los parquets, los paquetes y el parche de código; `LEEME.md` explica la entrega.

El cruce identifica 234.767 registros SARA: 234.419 evaluados y 348 sin
cobertura anual del snapshot TED. La unión con calidad puede alcanzar varias
copias de una misma versión; estos recuentos no son el número de filas del
indicador. Los diagnósticos brutos TED se conservan separados.
