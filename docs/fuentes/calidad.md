# Calidad: 21 indicadores sobre la PLACSP

- **En la release:** `calidad_licitaciones_resultado.zip`: el Parquet de v2026.02 tal cual, en su carpeta `v2026.02/` (8.693.891 filas, 70 columnas, 20 indicadores con la [numeración anterior](#numeración-de-los-indicadores): el de salto de escala, hoy PROPIO-FIA-01, es posterior), sin regenerar: está calculado sobre los datos nacionales de v2026.02, con sus errores. En v2026.02 iba en `calidad_licitaciones_resultado.rar`.
- **Script:** [`calidad/calidad_licitaciones.py`](../../calidad/calidad_licitaciones.py) y [`calidad/correcciones.py`](../../calidad/correcciones.py).
- **Portal:** calculado por el proyecto sobre la PLACSP, el TED y el BORME.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#lo-que-calcula-el-proyecto).

**21 indicadores** = 19 sobre la propia PLACSP + INT-COM-31 (cruce con el TED) + INT-CONS-18 (cruce con el BORME). Sin `--ted` ni `--borme`, el pipeline evalúa los 19 base. Los identificadores son los del sistema de indicadores de la Fundación Hay Derecho, salvo PROPIO-FIA-01, que es del proyecto ([numeración](#numeración-de-los-indicadores)).

Pipeline de calidad que aplica **21 indicadores** de validez, consistencia y fiabilidad sobre el dataset nacional (PLACSP), cruzando con TED y BORME. Al lado de cada importe publicado deja su versión corregida y el motivo ([importes publicados y corregidos](#importes-publicados-y-corregidos-issue-22)).

Evalúa todas las entradas del parquet PLACSP tal como se publican: cada fila de resultado conserva `n_versiones` y `es_ultima_version`. Con `--solo-ultima-version` evalúa solo la versión más reciente de cada una de las **4,727,478 licitaciones**.

> ⚠️ El parquet publicado en `v2026.02` (`calidad_licitaciones_resultado.parquet`, score medio 88.3) se calculó sobre las 8,7M entradas de `licitaciones_espana.parquet` sin forma de distinguir la versión vigente de cada licitación, con `importe_sin_iva` = valor estimado y con las etiquetas de procedimiento desplazadas. La columna "Corregido" es el pipeline actual con `--solo-ultima-version` sobre ese mismo parquet (una evaluación por licitación, semántica corregida de importes y procedimientos, ver [correcciones PLACSP](placsp.md#correcciones-en-los-datos-placsp)): FIA-04 sigue usando la `fecha_publicacion` antigua y VAL-01 el valor estimado cuando no hay presupuesto, hasta que se reprocesen los ATOM.

## Resultados

| Indicador | % Fallo v2026.02 (8,7M entradas) | % Fallo corregido (última versión, 4,7M licitaciones) | Descripción |
|---|---|---|---|
| INT-VAL-01 | 22.6% | 0.8% | Importe de licitación en formato válido |
| INT-VAL-02 | 31.8% | 5.8% | Importe de adjudicación en formato válido |
| INT-VAL-07 | 39.2% | 12.3% | Fecha de adjudicación válida |
| INT-VAL-09 | 23.3% | 41.4% | Código CPV válido (sube porque los contratos menores, 60 % sin CPV, casi no tienen versiones: son el 38 % de las entradas pero el 68 % de las licitaciones) |
| INT-VAL-12 | 33.4% | 7.8% | NIF/CIF adjudicatario válido (checksum) |
| INT-VAL-13 | 1.3% | 0.5% | Contrato menor coherente con cuantía (LCSP art. 118); antes 162K derivados de acuerdo marco contaban como menores |
| INT-CONS-01 | 1.1% | 1.4% | Si adjudicado, nº ofertas ≥ 1 |
| INT-CONS-08 | 0.3% | 0.4% | Importe adjudicación ≤ licitación (+5%) |
| INT-CONS-18 | 51.3% | 52.4% | Adjudicatario existe en BORME (3.3M empresas) |
| INT-COM-31 | 77.5% | pendiente | Contrato SARA publicado en TED, es decir, formalización en el DOUE (requiere regenerar el cruce PLACSP↔TED). Se une por versión (`id` + `fecha_updated`): solo la versión evaluada recibe resultado, y un año sin avisos en el snapshot TED (p.ej. 2026 con TED hasta 2025) queda sin evaluar, no como fallo |
| INT-FIA-01 | 0.6% | 0.7% | Nº ofertas en rango razonable (P99 por CPV) |
| INT-FIA-04 | 21.3% | 21.8% | Plazo presentación ofertas razonable (0-365 días) |
| INT-FIA-08 | 0.4% | 0.0% | PBL no outlier (≤ 50M€); antes se evaluaba el valor estimado |
| INT-FIA-09 | 0.8% | 1.2% | PA plausible por segmento CPV (P1-P99) |
| PROPIO-FIA-01 | — | — | PBL y PA del mismo orden de magnitud (PA < 100 × PBL). Nuevo ([issue #22](https://github.com/BquantFinance/licitaciones-espana/issues/22)); en la regeneración de 2026-09 falla en 495 de 4,83M licitaciones (0,010 %) |

Los 6 indicadores restantes (formato numérico, no negativos, NUTS, trazabilidad) dan 0.0% de fallo — checks de sanidad que se aplican pero no revelan problemas.

Los indicadores se basan en el marco de calidad de PPDS, con contribuciones de Jaime Gómez-Obregón (umbrales LCSP) y OIRESCON (benchmarks PBL).

## Numeración de los indicadores

Los identificadores `INT-…` y sus nombres (los de `CATALOGO` en el script) son los del anexo digital 1 del *Sistema de Indicadores de Transparencia en Contratación Pública* de la Fundación Hay Derecho: el 1.a (criterio legal) y el 1.b (calidad del dato). `PROPIO-FIA-01` es un indicador del proyecto que no está en ese sistema. [`tests/test_calidad.py`](../../tests/test_calidad.py) comprueba la numeración contra una tabla de referencia.

Tres columnas llevaban antes el número de otro indicador del anexo:

| Antes | Ahora | Qué mide | En el anexo, el número anterior es |
|---|---|---|---|
| `INT-VAL-14` | `INT-VAL-13` | Clasificación correcta del procedimiento según cuantía (aquí, un contrato menor por encima del umbral de su tipo) | La identificación válida de la financiación del PRTR |
| `INT-CONS-20` | `INT-COM-31` | Publicación del anuncio de formalización en el DOUE de los contratos SARA (cruce con el TED; criterio legal, anexo 1.a) | La consistencia de la clasificación entre la PLACSP y el portal autonómico |
| `INT-FIA-12` | `PROPIO-FIA-01` | Presupuesto y adjudicación del mismo orden de magnitud (adjudicación < 100 × presupuesto). No está en el anexo: lo más cercano es INT-CONS-13 (coherencia PBL–VEC–PA), del que cubre un solo caso | La trazabilidad de las consultas preliminares de mercado |

Solo cambian los nombres: los valores, el orden de las columnas y el score son los mismos. El Parquet de la release (`calidad_licitaciones_resultado.zip`, el de v2026.02) es anterior y conserva los nombres de antes (`INT-VAL-14` e `INT-CONS-20`; no trae el de salto de escala). La salida del pipeline actual usa los nuevos. Para leer los dos con los mismos nombres:

```python
df = df.rename(columns={'INT-VAL-14': 'INT-VAL-13', 'INT-CONS-20': 'INT-COM-31', 'INT-FIA-12': 'PROPIO-FIA-01'})
```

## Menores vs Regulares

Recalculado con el pipeline corregido sobre la última versión de cada licitación (en `v2026.02` se comparaban 3.3M entradas de menores con 5.4M entradas "regulares", que corresponden a 1.5M licitaciones):

| Indicador | Menores (3.2M) | Regulares (1.5M) | Diferencia |
|---|---|---|---|
| NIF adjudicatario inválido | 2.7% | 18.6% | -16.0pp |
| Sin CPV | 60.6% | 1.2% | +59.3pp |
| Sin fecha adjudicación | 0.6% | 36.9% | -36.3pp |
| Sin importe licitación | 0.0% | 2.5% | -2.5pp |

## Importes publicados y corregidos (issue #22)

Los importes se sirven tal como los publica la administración, también cuando están mal: que se publicara un importe erróneo es en sí un dato para la auditoría ciudadana. Al lado, `calidad_licitaciones_resultado.parquet` lleva la versión corregida de cada importe y el motivo (`calidad/correcciones.py`), para que un buscador pueda enseñar las dos y los agregados (rankings, cuotas, HHI) y los modelos usen la corregida. Los indicadores y el score siguen evaluando lo publicado.

Para `importe_adjudicacion`, `importe_sin_iva` y `valor_estimado_contrato`:

| Columna | Contenido |
|---|---|
| `<campo>_corregido` | El importe que usar: el publicado si no hay corrección; vacío si el publicado no es fiable y no hay estimación |
| `correccion_<campo>` | Vacío, o el motivo de la corrección |

| Motivo | Regla | Última versión, regeneración 2026-09 |
|---|---|---:|
| `registro` | Error verificado a mano en [`calidad/errores_fuente.csv`](../../calidad/errores_fuente.csv), con su evidencia y el valor probable. Solo cambia las versiones que aún publican el valor erróneo | 1 adjudicación y 1 valor estimado |
| `escala_x100` / `escala_x1000` | Adjudicación de 100 veces el presupuesto o más que, dividida entre 100 o 1.000, vuelve a su orden (la coma decimal perdida): con las mismas cifras que el presupuesto (desde 1.000 €) o entre el 50 % y el 105 % de él (desde 10.000 €) | 24 / 6 |
| `inverosimil` | Adjudicación de 100 veces el presupuesto o más sin corrección fiable: la corregida queda vacía | 23 |
| `no_comparable` | Presupuesto de menos de 1.000 € frente a una adjudicación de 100 veces o más. Suele ser un precio unitario o simbólico (1 €): se vacía el presupuesto corregido y la adjudicación se mantiene | 441 |

En la última versión de cada licitación, la adjudicación de la tabla principal (sin IVA; en las de varios lotes, la del primero) suma unos 525.800 M€ publicada y unos 520.130 M€ corregida. Las 23 `inverosimil` (2.485,9 M€ publicados) quedan vacías y no suman. Para sumas y rankings hay que filtrar `es_ultima_version`, porque la salida trae todas las versiones.

El caso del issue #22 es CONSTRUCCIONES URDINBERRI, S.L. (entrada `PlataformasAgregadasSinMenores/15091104`):
- publica una adjudicación de 2.357.531.666 € para una obra de 2.518.819,27 €;
- la plataforma de origen (Euskadi) da 2.593.284,83 € con IVA del 10 %, es decir, 2.357.531,67 € sin IVA;
- su total adjudicado en la tabla principal pasa de 2.381,6 M€ publicados a 26,4 M€ corregidos; contando todos sus lotes (`_resultados`, con el registro aplicado), de 2.386,4 M€ a 31,2 M€.

**Para añadir un error verificado** al registro, hay que rellenar una fila con estos campos:
- `fuente`: `placsp`, o el dataset donde se ve;
- `id` de la entrada, `campo` y `valor_publicado`;
- `valor_probable`: vacío si no se sabe;
- `certeza`: `confirmado` si la propia fuente da la prueba, `probable` si no;
- `evidencia`, `referencia` (issue o URL) y la fecha en `verificado`.

La misma corrección se aplica al cargar los datos en otro sistema:

```python
from calidad import correcciones
df = df.join(correcciones.corregir_importes(df, correcciones.cargar_registro()))
```

En las tablas de detalle (`_resultados`, un importe por lote) no hay presupuesto por fila, así que ahí solo se aplica lo del registro. Las reglas de escala por lote (con el presupuesto de `_lotes`) están pendientes: en la última versión, `_resultados` suma 2,26 millones de M€ frente a los 525.800 M€ de la tabla principal.

## Archivos

```
calidad/
├── calidad_licitaciones.py                  # Pipeline
├── correcciones.py                          # Importes corregidos junto a los publicados
├── errores_fuente.csv                       # Errores de la fuente verificados, con su evidencia
└── calidad_licitaciones_resultado.parquet   # v2026.02: 8.7M filas × 70 cols (977 MB), pendiente de regenerar (en Releases: calidad_licitaciones_resultado.zip)
```

## Uso

`-i` es la tabla principal de la PLACSP: la salida de `nacional/licitaciones.py` o las 6 partes de la release juntas en un fichero ([cómo](ted.md#cruce-placsp--ted-cross-validation)).

```bash
# Solo indicadores base (19)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_completo.parquet

# Completo con TED + BORME (21 indicadores)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_completo.parquet \
  --ted ted/crossval_sara.parquet \
  --borme borme_empresas.parquet

# Una evaluación por licitación (versión más reciente)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_completo.parquet --solo-ultima-version

# Sin el registro de errores de la fuente (solo las reglas de escala)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_completo.parquet --errores-fuente ''
```

Sobre el Parquet de la release (`calidad_licitaciones_resultado.zip`, el de v2026.02): una fila por **entrada** de
v2026.02, es decir, todas las versiones de cada licitación y sin `es_ultima_version`, con 20 indicadores y los errores de
los datos nacionales de v2026.02. Sirve para ver qué falla, no para contar licitaciones. Son 8,7 M de filas y 70
columnas: lee solo las que uses (entero, con pandas, pasa de 10 GB de memoria):

```python
import pandas as pd
df = pd.read_parquet('calidad_licitaciones_resultado/v2026.02/calidad_licitaciones_resultado.parquet',
                     columns=['expediente', 'organo_contratante', 'importe_adjudicacion', 'INT-CONS-20', 'INT-VAL-14'])

# En este fichero, con la numeración anterior: INT-CONS-20 es hoy INT-COM-31, e INT-VAL-14, INT-VAL-13
# Contratos SARA no publicados en TED (entradas de v2026.02, no licitaciones)
df[df['INT-CONS-20'] == False].groupby('organo_contratante').size().nlargest(10)

# Contratos menores que superan umbral LCSP
df[df['INT-VAL-14'] == False][['expediente', 'organo_contratante', 'importe_adjudicacion']]
```

La salida del pipeline actual (`calidad/calidad_licitaciones_resultado.parquet`, al ejecutar `calidad_licitaciones.py`) trae
además `es_ultima_version` y los importes corregidos:

```python
import pandas as pd
df = pd.read_parquet('calidad/calidad_licitaciones_resultado.parquet')
ultima = df[df['es_ultima_version']]           # una evaluación por licitación

# Score medio por órgano
ultima.groupby('organo_contratante')['score_calidad'].mean().nlargest(20)

# Ranking de adjudicatarios con los importes corregidos, y lo que se ha corregido
ultima.groupby('adjudicatario')['importe_adjudicacion_corregido'].sum().nlargest(20)
ultima[ultima['correccion_importe_adjudicacion'].notna()][
    ['id', 'adjudicatario', 'importe_adjudicacion', 'importe_adjudicacion_corregido', 'correccion_importe_adjudicacion']]
```

## Scripts

| Script | Descripción |
|--------|-------------|
| `calidad/calidad_licitaciones.py` | 21 indicadores de calidad sobre PLACSP + TED + BORME |
| `calidad/correcciones.py` | Importes corregidos junto a los publicados (registro de errores de la fuente y saltos de escala) |

---

[Todas las fuentes](README.md) · [README](../../README.md)
