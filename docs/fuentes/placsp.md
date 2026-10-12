# PLACSP: Plataforma de Contratación del Sector Público

Licitaciones de la [Plataforma de Contratación del Sector Público](https://contrataciondelsectorpublico.gob.es/).

- **En la release:** los 9 ZIP `nacional_*.zip`: la tabla principal, partida en 6 Parquet repartidos en los 5 `nacional_licitaciones_*.zip`, y las tablas de detalle en `nacional_resultados.zip`, `nacional_criterios.zip`, `nacional_adjudicatarios.zip` y `nacional_lotes_y_otras_tablas.zip`.
- **Script:** [`nacional/licitaciones.py`](../../nacional/licitaciones.py) y, para corregir Parquet ya generados, [`nacional/normalizar_placsp.py`](../../nacional/normalizar_placsp.py).
- **Portal:** [contrataciondelsectorpublico.gob.es](https://contrataciondelsectorpublico.gob.es/), datos abiertos (ATOM/CODICE): licitaciones (643), agregación de las plataformas autonómicas (1044), contratos menores (1143), encargos a medios propios (1383) y consultas preliminares (1403).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#nacional-y-unión-europea).

## v2026.10

En v2026.10 (Releases) los datos salen del scraper corregido: 10.907.567 entradas publicadas de 5.241.440 licitaciones (`es_ultima_version`), con todas las versiones, la tabla de resultados por lote y las de adjudicatarios, lotes, criterios, modificaciones y borrados.

| ZIP | Fichero | Filas |
|---|---|--:|
| `nacional_licitaciones_hasta_2019.zip` | `licitaciones_completo/licitaciones_completo_sin_ano.parquet` (`ano IS NULL`) | 10.226 |
| `nacional_licitaciones_hasta_2019.zip` | `licitaciones_completo/licitaciones_completo_hasta_2019.parquet` (`ano < 2020`) | 1.607.195 |
| `nacional_licitaciones_2020_2021.zip` | `licitaciones_completo/licitaciones_completo_2020_2021.parquet` | 1.718.374 |
| `nacional_licitaciones_2022_2023.zip` | `licitaciones_completo/licitaciones_completo_2022_2023.parquet` | 2.564.085 |
| `nacional_licitaciones_2024_2025.zip` | `licitaciones_completo/licitaciones_completo_2024_2025.parquet` | 3.036.783 |
| `nacional_licitaciones_desde_2026.zip` | `licitaciones_completo/licitaciones_completo_desde_2026.parquet` (`ano >= 2026`) | 1.970.904 |
| `nacional_resultados.zip` | `licitaciones_completo_resultados.parquet` | 10.677.644 |
| `nacional_criterios.zip` | `licitaciones_completo_criterios.parquet` | 14.571.218 |
| `nacional_adjudicatarios.zip` | `licitaciones_completo_adjudicatarios.parquet` | 10.146.626 |
| `nacional_lotes_y_otras_tablas.zip` | `licitaciones_completo_lotes.parquet` | 5.007.443 |
| `nacional_lotes_y_otras_tablas.zip` | `licitaciones_completo_modificaciones.parquet` | 358.958 |
| `nacional_lotes_y_otras_tablas.zip` | `licitaciones_completo_borrados.parquet` | 498.491 |
| `nacional_lotes_y_otras_tablas.zip` | `licitaciones_completo_semilla_contenido.parquet` | 39.028 |

**La tabla principal va partida.** GitHub no admite ficheros de 2 GB o más, así que `licitaciones_completo.parquet` (10.907.567 filas, 80 columnas, 5,0 GB) se publica partida por `ano` (año del anuncio de licitación) en 6 Parquet repartidos en los 5 ZIP `nacional_licitaciones_*.zip`. Al descomprimirlos, las partes quedan en `licitaciones_completo/` y se leen como una sola tabla. Tienen las mismas columnas, tipos y valores que la tabla original: la suma de filas de las partes y una huella de contenido (suma del hash de cada fila y de cada columna) coinciden con las del original. Las tablas de detalle se cruzan con la principal por `id` + `fecha_updated`.

## Correcciones en los datos PLACSP

Los parquet nacionales publicados hasta `v2026.02` —y todo lo calculado sobre ellos: indicadores de calidad, cruce PLACSP↔TED, detector BORME×PLACSP— tienen errores de columnas que distorsionan cualquier suma o recuento. Cifras medidas sobre el propio `licitaciones_espana.parquet`:

| Problema | Efecto | Corrección |
|----------|--------|------------|
| **Versiones sin marcar.** La PLACSP publica una entrada nueva del ATOM por cada actualización de una licitación (anuncio, adjudicación, formalización...), y nada indicaba cuál es la vigente | 8.693.891 entradas para 4.727.478 licitaciones (hasta 15+ por licitación; 90.128 son copias exactas). Sumar importes sin filtrar cuenta la misma licitación varias veces: adjudicación ×4,7 (2.223,7 frente a 476,6 B€) e `importe_sin_iva` ×8 (11.764,7 frente a 1.476,1 B€). `licitaciones_completo_2012_2026.parquet` tiene una fila por licitación, pero en 23.911 casos (0,5 %) no es la versión más reciente | Se sirven **todas** las entradas tal como las publica la PLACSP, con `n_versiones` (versiones distintas del mismo `id`, es decir, valores distintos de `fecha_updated`), `entrada_repetida` (la misma versión publicada más de una vez: se conserva y se marca) y `es_ultima_version` (una fila por `id`: la de `fecha_updated` más reciente). Para contar o sumar licitaciones: `df[df.es_ultima_version]` |
| **`importe_sin_iva` era el valor estimado** ([#6](https://github.com/BquantFinance/licitaciones-espana/issues/6)): se guardaba `EstimatedOverallContractAmount` | En el conjunto `licitaciones`, el 27 % de las filas tiene `importe_sin_iva` > `importe_con_iva`, imposible para un presupuesto sin IVA | `valor_estimado_contrato` = EstimatedOverallContractAmount; `importe_sin_iva` = TaxExclusiveAmount |
| **Etiquetas de códigos desplazadas** | "Negociado sin publicidad" etiquetaba el código 4 (negociado *con* publicidad): 41.388 filas frente a 191.039 licitaciones reales. 162.392 derivados de acuerdo marco figuraban como "Contrato menor" y los 3,3M contratos menores como "Asociación innovación". Tipos 22/32 (concesiones LCSP) sin etiqueta y 40 (colaboración público-privada) como "Concesión Servicios" | Etiquetas recalculadas desde `procedimiento_code` / `tipo_contrato_code` |
| **CPV guardado como número** | 73.903 CPV sin el cero inicial (`9134100` en lugar de `09134100`) | CPV como texto de 8 dígitos |
| **`fecha_publicacion` (y `ano`) de un anuncio posterior.** Se tomaba el primer `ValidNoticeInfo` sin mirar su tipo | En las licitaciones adjudicadas o resueltas era casi siempre la fecha del anuncio de adjudicación/formalización: en el 63 % el plazo de presentación termina *antes* de esa "publicación" (0,2 % en las que siguen en plazo). Los recuentos por año usan en realidad el año de adjudicación | Fecha del anuncio de licitación (`DOC_CN`) o, si no lo hay, del primer anuncio publicado. Solo se corrige reprocesando los ATOM |

**v2026.10 ya trae los datos corregidos**, generados por el scraper actual. Medido en su última versión de cada licitación: 0 de 1.203.058 licitaciones con `importe_sin_iva` > `importe_con_iva` (frente al 27 %), el código 3 es "Negociado sin publicidad" (207.477) y el 4 "Negociado con publicidad" (14.360), y ningún CPV de 7 dígitos.

Para corregir un parquet de v2026.02 ya descargado sin volver a procesar los ATOM:

```bash
python nacional/normalizar_placsp.py -i nacional/licitaciones_espana.parquet \
    -o nacional/licitaciones_espana_normalizado.parquet    # ~1-2 min, ~4 GB de RAM
```

```python
import sys; sys.path.insert(0, '.')        # desde la raíz del repo
from nacional.licitaciones import leer_placsp
df = leer_placsp('nacional/licitaciones_espana.parquet')  # todas las entradas + n_versiones / es_ultima_version, semántica actual
ultimas = df[df['es_ultima_version']]                      # una fila por licitación, para sumar o contar
# o directamente: leer_placsp(ruta, solo_ultima_version=True)
```

Sobre los parquet de `v2026.02` la normalización mueve el antiguo `importe_sin_iva` a `valor_estimado_contrato` y deja `importe_sin_iva` vacío: el presupuesto sin IVA real solo se obtiene reprocesando los ZIP de la PLACSP:

```bash
# La salida, en una carpeta aparte: los parquet de nacional/ son la única copia de v2026.02
python nacional/licitaciones.py --data-dir <zips PLACSP> --output-dir <salida> --procesos 3 --sin-csv \
    --semilla nacional/licitaciones_espana.parquet --semilla nacional/licitaciones_completo_2012_2026.parquet
```

## Ejecución: memoria, histórico y semilla

- **Memoria acotada.** Los ZIP se leen sin extraerlos y las entradas se escriben por lotes (`--lote`, 50.000 por defecto): con agregación 2025 (250.652 entradas) el pico baja de 2,7 GB a 1,2 GB con la misma salida. `--procesos N` lee N ZIP a la vez y `--sin-csv` omite los CSV (con todos los conjuntos pasan de 10 GB).
- **Nada se machaca.** Un ZIP que cambia deja su versión anterior en `<data-dir>/<conjunto>/_historico/` y se leen todas las versiones: una entrada que la PLACSP deja de servir sigue en la salida con `_en_ultima_descarga=False`. La salida anterior de cada tabla pasa a `<salida>/_historico/`.
- **`--semilla`** incorpora un parquet publicado como la instantánea más antigua: solo añade, marcadas con `_origen='release v2026.02'` y `_en_ultima_descarga=False`, las filas cuya clave (`id`, `fecha_updated`) no está en la descarga, y solo de los conjuntos y años de ZIP leídos. Las 35.627 filas de `licitaciones_espana.parquet` sin `fecha_updated` (el código antiguo no leía los `atom:updated` sin milisegundos) se casan por contenido. Si coinciden con una fila de la descarga van a la tabla `_semilla_contenido`, para no perderlas ni duplicar la principal.
- **`textos_originales`** (JSON) guarda el texto publicado de cada importe o fecha que no se puede convertir (p.ej. `"fecha_adjudicacion": "0202-07-03"`): la columna queda vacía y el valor no se pierde.

## Archivos

```
nacional/
├── licitaciones.py                          # Scraper ATOM → Parquet/CSV (todas las entradas, con es_ultima_version)
├── normalizar_placsp.py                     # Corrige parquets ya generados (ver arriba)
├── licitaciones_espana.parquet              # v2026.02: 8,7M entradas = todas las versiones de 4,7M licitaciones (965 MB)
└── licitaciones_completo_2012_2026.parquet  # v2026.02: 4,7M filas, una por licitación, no siempre la más reciente (762 MB)
```

El scraper escribe `licitaciones_completo.parquet/.csv`, con una fila por entrada publicada en los ATOM (`n_versiones`, `es_ultima_version`), y `licitaciones_completo_resultados.parquet/.csv`, con una fila por resultado (`cac:TenderResult`, uno por lote) de cada entrada: las columnas de adjudicación de la tabla principal corresponden al **primer lote**. Además escribe `_adjudicatarios` (cada `WinningParty`), `_lotes`, `_criterios`, `_modificaciones`, `_borrados` (entradas `at:deleted-entry`) y, con `--semilla`, `_semilla_contenido`: se cruzan con la principal por `id` + `fecha_updated`. Con el rango completo (`--anos` del 2012 al año en curso, el de por defecto) el nombre es fijo; el nombre con años de cada año (`licitaciones_completo_2012_<año>...`, el de antes) es un enlace simbólico a cada tabla. La primera ejecución con el nombre fijo le pasa la salida con años más reciente, sea del año que sea, sin copiarla y con su historia de `_historico/`; las de otros años van a `_historico/` (no se borra nada). Con un rango parcial, `licitaciones_completo_{inicio}_{fin}`; si ese nombre es uno de los enlaces (p.ej. `--anos 2012-2026` lanzado en 2027), el script se para sin escribir nada: nunca escribe a través de un enlace.

**No leas la salida por patrón.** Los nombres con años son enlaces al mismo fichero: un glob lee cada tabla varias veces. En 2027, `*_resultados.parquet` casa con `licitaciones_completo_resultados`, `licitaciones_completo_2012_2026_resultados` y `licitaciones_completo_2012_2027_resultados`: el triple de filas (medido con DuckDB en una muestra: 180.309 filas frente a 60.103). Léela por el nombre fijo: `licitaciones_completo.parquet`, `licitaciones_completo_resultados.parquet`...

**Volver al código sin nombre fijo sin romper nada.** Ese código escribe `licitaciones_completo_2012_<año>` con `guardar_version`, que va por ruta y no sabe de enlaces: escribiría a través de ellos, los movería rotos a `_historico/` y dejaría ficheros reales solo en las tablas que cambian, mezclados con enlaces al fijo. Antes de desplegarlo, con este código y sin ninguna ejecución en marcha:

```bash
python nacional/licitaciones.py --output-dir <salida> --deshacer-nombre-fijo
```

Cada tabla vuelve a `licitaciones_completo_2012_<año en curso>`, sin copiarla y con su historia de `_historico/` (el código anterior sigue la misma cadena de versiones), y se quitan los enlaces con años. Si algo lee ya el nombre fijo (p. ej. un ETL), hay que volver a apuntarlo al nombre con años.

## Campos principales

| Categoría | Campos |
|-----------|--------|
| Identificación | id, expediente, objeto, url |
| Órgano | organo_contratante, nif_organo, dir3_organo, ciudad_organo, dependencia |
| Tipo | tipo_contrato(_code), subtipo_code, procedimiento(_code), estado(_code) |
| Importes | valor_estimado_contrato, importe_sin_iva, importe_con_iva, importe_adjudicacion, importe_adj_con_iva |
| Adjudicación (1er lote) | adjudicatario, nif_adjudicatario, num_ofertas, es_pyme, fecha_adjudicacion |
| Lotes | n_lotes, n_resultados (+ tabla `_resultados`: lote, resultado_code, adjudicatario, NIF, importes, ofertas, pyme) |
| Clasificación | cpv_principal, cpvs, ubicacion, nuts |
| Fechas | fecha_publicacion, fecha_limite, fecha_adjudicacion, fecha_updated |

| Columna | Elemento CODICE | Significado |
|---------|-----------------|-------------|
| `valor_estimado_contrato` | `ProcurementProject/BudgetAmount/EstimatedOverallContractAmount` | Valor estimado: todos los lotes, prórrogas y modificaciones previstas (base de los umbrales SARA) |
| `importe_sin_iva` | `ProcurementProject/BudgetAmount/TaxExclusiveAmount` | Presupuesto base de licitación sin impuestos |
| `importe_con_iva` | `ProcurementProject/BudgetAmount/TotalAmount` | Presupuesto base de licitación con impuestos |
| `importe_adjudicacion` | `TenderResult/AwardedTenderedProject/LegalMonetaryTotal/TaxExclusiveAmount` | Adjudicado sin impuestos (primer lote) |
| `importe_adj_con_iva` | `TenderResult/AwardedTenderedProject/LegalMonetaryTotal/PayableAmount` | Adjudicado con impuestos (primer lote) |

**Procedimiento** (`procedimiento_code`): 1 Abierto · 2 Restringido · 3 Negociado sin publicidad · 4 Negociado con publicidad · 5 Diálogo competitivo · 6 Contrato menor · 7 Derivado de acuerdo marco · 8 Concurso de proyectos · 9 Abierto simplificado · 10 Asociación para la innovación · 11 Derivado de asociación para la innovación · 12 Sistema dinámico de adquisición · 13 Licitación con negociación · 100 Normas internas · 999 Otros.

**Tipo de contrato** (`tipo_contrato_code`): 1 Suministros · 2 Servicios · 3 Obras · 7 Administrativo especial · 8 Privado · 21 Gestión de servicios públicos · 22 Concesión de servicios · 31 Concesión de obras públicas · 32 Concesión de obras · 40 Colaboración público-privada · 50 Patrimonial · 999 Otros.

## v2026.02

Los Parquet nacionales de v2026.02 (en el árbol del repositorio, en Git LFS, y en `nacional.zip` de la release v2026.02) tienen los errores de [Correcciones en los datos PLACSP](#correcciones-en-los-datos-placsp). No se repiten en v2026.10: sus 8.693.891 y 4.725.557 filas están en v2026.10 por (`id`, `fecha_updated`), y las que no tenían `fecha_updated`, por contenido en `_semilla_contenido`.

| Conjunto | Licitaciones únicas | Filas en `licitaciones_espana.parquet` (v2026.02) | Período |
|----------|--------------------:|-------------------------------------------------:|---------|
| Licitaciones | 1,08M | 3,65M | 2012-actualidad |
| Agregación CCAA | 434K | 1,74M | 2016-actualidad |
| Contratos menores | 3,20M | 3,29M | 2018-actualidad |
| Encargos medios propios | 12,5K | 14,7K | 2021-actualidad |
| Consultas preliminares | 1,9K | 3,7K | 2022-actualidad |
| **Total** | **4,73M** | **8,69M** | |

## Uso

Con los ZIP descomprimidos cada uno en su carpeta (`unzip X.zip -d X`):

```python
import glob

import duckdb
import pyarrow.dataset as ds

PLACSP = "nacional_licitaciones_*/licitaciones_completo/*.parquet"   # las 6 partes, una sola tabla
RESULTADOS = "nacional_resultados/licitaciones_completo_resultados.parquet"

# Una fila por versión publicada: para contar o sumar licitaciones, solo es_ultima_version
ultimas = (ds.dataset(sorted(glob.glob(PLACSP)))
           .to_table(columns=["id", "fecha_updated", "organo_contratante", "importe_adjudicacion", "ano"],
                     filter=ds.field("es_ultima_version"))
           .to_pandas())

# Todos los lotes de cada licitación: la tabla de resultados, con la misma clave id + fecha_updated
# (en la principal, las columnas de adjudicación son las del primer lote)
duckdb.sql(f"""
    SELECT l.ano, count(DISTINCT l.id) AS licitaciones, count(*) AS resultados
    FROM '{PLACSP}' l JOIN '{RESULTADOS}' r ON r.id = l.id AND r.fecha_updated = l.fecha_updated
    WHERE l.es_ultima_version AND r.es_ultima_version
    GROUP BY 1 ORDER BY 1
""").show()
```

- Los importes se sirven tal como se publican: para sumas y rankings, el importe corregido de [`calidad/correcciones.py`](calidad.md#importes-publicados-y-corregidos-issue-22).
- En un acuerdo marco cada adjudicataria lleva el tope entero del lote: [`docs/MEDIR.md`](../MEDIR.md#qué-cuenta-cada-consulta) explica cómo no contarlo varias veces (consulta `placsp_cruce_comunidad`).
- Más consultas, ya comprobadas sobre la release: las `placsp_*` de [`scripts/medir_consultas.py`](../../scripts/medir_consultas.py).

## Scripts

| Script | Descripción |
|--------|-------------|
| `nacional/licitaciones.py` | Extrae datos nacionales de ATOM/XML (todas las entradas publicadas, marcadas con `es_ultima_version`, + tabla de resultados por lote) |
| `nacional/normalizar_placsp.py` | Corrige parquets PLACSP ya generados sin eliminar filas: marca versiones, semántica de importes, etiquetas y CPV |

---

[Todas las fuentes](README.md) · [README](../../README.md)
