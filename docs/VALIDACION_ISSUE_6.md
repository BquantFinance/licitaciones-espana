# Issue #6: revisión y validación del 27 de septiembre de 2026

> Informe histórico de la primera fase. La reconstrucción completa ya terminó;
> véase [el resultado final](RESULTADO_REGENERACION_ISSUE_6.md).

## Situación en GitHub

- [Comentario de 686f6c61, 27 de marzo](https://github.com/BquantFinance/licitaciones-espana/issues/6#issuecomment-4142176179):
  identificó el mapeo y advirtió del impacto en calidad, TED y outputs.
- [PR #7 de sergioberino](https://github.com/BquantFinance/licitaciones-espana/pull/7):
  integrada el 30 de marzo; corrigió parser y añadió cuatro tests.
- [Respuesta del mantenedor, 12 de mayo](https://github.com/BquantFinance/licitaciones-espana/issues/6#issuecomment-4430231478):
  reconoció el error. No hay evidencia de rechazo de ese diagnóstico.
- [Petición de elCanosail, 27 de septiembre](https://github.com/BquantFinance/licitaciones-espana/issues/6#issuecomment-5855113376):
  solicita regenerar el parquet de calidad y ofrece contrastarlo con su ingesta.
- [PR #23 del mantenedor](https://github.com/BquantFinance/licitaciones-espana/pull/23):
  abierta al revisar, con correcciones más amplias. Su normalizador explica que
  no puede recuperar `TaxExclusiveAmount` sin volver a los ATOM.

Base de esta corrección: `main` en `6648927bb61241ff32edbb9be1665ef3eda4b344`.
PR #23 revisada en `627008b1b40158418b432f92f1a8757e646249bf`.

## Cambios locales

- `nacional/reparar_importes.py`: recuperación por versión exacta desde XML;
  mantiene filas, orden, duplicados y los campos ajenos al arreglo. Comprueba
  también que `TotalAmount` coincide. Emite informe con hashes y cobertura.
- Calidad rechaza el esquema antiguo y evita reutilizar resultados con
  indicadores ya calculados. No se presenta la presencia de una columna como
  prueba suficiente de que los importes proceden de fuentes correctas.
- La guía de regeneración documenta fuentes, comandos, límites y dependencia
  del cruce TED corregido antes de regenerar sus indicadores externos.
- No se modifica el parser que ya estaba arreglado ni se replica la PR #23.

## Evidencia real, no fixture

Se descargó mediante Git LFS el nacional publicado:

```text
nacional/licitaciones_espana.parquet
filas: 8.693.891
SHA-256: 83f7510963919310cc877e799e345e02f0c91883757465234767be105092b15b
```

Se descargó también el [ZIP oficial de licitaciones de 2012](https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2012.zip).
SHA-256: `de130e278de9877fe5840ba1263de75d1894949155989050edfad5cc76247763`.
Contiene 19.000 entradas. El nacional publicado tiene 19.000 filas con ese
`archivo_origen`; 37 carecen de `fecha_updated`.

El intento con las 19.000 filas **falló sin publicar salida**, como se esperaba:
no hay clave de versión completa para las 37 filas. Se creó después una
**muestra explícita** con las 18.963 filas cuya fecha sí está disponible.
No se han eliminado filas de ningún resultado presentado como completo.

| Comprobación de la muestra | Resultado |
| --- | ---: |
| Filas antes / después | 18.963 / 18.963 |
| Coincidencias exactas con ATOM | 18.963 |
| `importe_sin_iva` modificado | 13.840 |
| `importe_sin_iva` informado después | 18.963 |
| `valor_estimado_contrato` informado | 10.592 |
| Otros campos idénticos, incluyendo total y versiones | 47 |
| Filas en calidad recalculada, indicadores base | 18.963 |
| Cambios en `INT-VAL-01` | 8.371 |
| Cambios en `INT-VAL-03` | 74 |
| Cambios en `INT-CONS-08` | 111 |
| Cambios en `INT-FIA-08` | 15 |

Los indicadores de cuantiles se calcularon sobre la muestra completa, no sobre
los 8,7 millones. No deben extrapolarse sus métricas al histórico entero.

Artefactos locales, excluidos de Git en este checkout:

- `artifacts/issue-6/nacional_2012_original.parquet`: las 19.000 filas.
- `artifacts/issue-6/muestra_2012_original.parquet`: muestra antes del arreglo.
- `artifacts/issue-6/muestra_2012_corregida.parquet` y `.json`: muestra recuperada e informe.
- `artifacts/issue-6/calidad_muestra_2012/calidad_licitaciones_resultado.parquet`.
- `artifacts/issue-6/validacion_muestra.json`: verificación de campos e indicadores.
- `artifacts/issue-6/fuentes/licitaciones/`: copia persistente del ZIP oficial.
- `artifacts/issue-6/fuentes_requeridas.json`: inventario de los 78 archivos
  fuente referenciados por el nacional completo; **no se han descargado los 78**.

## Pruebas y límites

`python -m pytest -q`: **43 passed** con Python 3.13.13, pandas 3.0.6 y
pyarrow 25.0.1. Incluye 14 pruebas nuevas: versiones, duplicados, ZIP,
importe ausente, fuentes incompletas, contradicciones, XML truncado,
rechazo de importes no finitos, protección de salida, consultas sin presupuesto,
fechas ausentes y recalculado de calidad. Las advertencias de deprecación
proceden del parser y del scraper de Galicia existentes.

El nacional completo tiene **35.627 fechas de actualización nulas**. La
recuperación exacta propuesta se detiene ante esas filas. Completar el release
requiere resolver su correspondencia histórica con evidencia adicional o
regenerar un snapshot nuevo desde todas las fuentes y documentar las diferencias;
no equivale a aplicar esta reparación parcial al histórico publicado.

**No está regenerado el dataset completo ni actualizado el release oficial.**
Tampoco se han recalculado TED/BORME. La cuenta usada tiene permisos de lectura,
no de publicación en el repositorio del mantenedor. Este trabajo deja un cambio
local verificable y una muestra real para acompañar la corrección pendiente.
