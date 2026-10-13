# Esquema: las marcas comunes

Los datos se sirven tal como los publica cada administración. Lo que añade este proyecto va en columnas aparte, casi
siempre con un nombre que empieza por `_`: de dónde sale cada fila, si la fuente la sigue sirviendo, si viene repetida
o mal formada en origen, o el importe corregido junto al publicado. Ninguna de estas columnas cambia un valor
publicado.

Esta página explica las marcas que se repiten en varias fuentes. Las columnas de cada fichero, con su tipo, están en
[`docs/esquema/`](esquema/README.md), generadas desde la propia release; las marcas propias de una sola fuente, en su
ficha de [`docs/fuentes/`](fuentes/README.md).

## Control de versiones

Las pone [`comun/historico.py`](../comun/historico.py) en las tablas que se construyen acumulando todas las descargas
(regla 3 de los [principios](PRINCIPIOS.md)). Un registro que la fuente retira o cambia no desaparece: la fila antigua
sigue, marcada.

| Marca | Qué significa | Cómo usarla |
|---|---|---|
| `_primera_descarga` | Fecha de la primera descarga en la que apareció la fila | |
| `_ultima_descarga` | Fecha de la última versión de los ficheros de origen que trae la fila | Para quedarte con la versión más reciente de un registro: ordena por esta fecha |
| `_en_ultima_descarga` | `True` si la fila está en la última descarga. `False`: la fuente la ha retirado, o la ha cambiado (entonces la versión nueva es otra fila, con `True`), o viene de una semilla | `df[df._en_ultima_descarga]` es lo que la fuente sirve hoy. Lo retirado sigue siendo un dato: no lo quites sin pensarlo |
| `_origen` | Nulo si la fila sale de la descarga. `'release v2026.02'`: la añadió la semilla (`--semilla`) porque la fuente ya no la sirve o no se ha vuelto a descargar; trae los valores y los errores de entonces | `df[df._origen.isna()]` deja solo lo descargado. En v2026.10, por ejemplo, 751.187 filas del RPC de Cataluña y 12.974 del TED vienen de v2026.02 |
| `_fecha_descarga` | Fecha de descarga del fichero de origen de la fila | No cuenta al comparar versiones: una descarga idéntica en otra fecha no crea filas nuevas |

Para contar registros sin contar dos veces uno que ha cambiado: quédate con la última versión de cada clave (la
clave de cada fuente está en su ficha) o, si solo te interesa lo vigente, con `_en_ultima_descarga`.

## Procedencia

De qué fichero, hoja y fila sale cada registro, para volver al original. Las añade el script de cada fuente.

| Marca | Qué significa |
|---|---|
| `_fuente`, `_url_origen` | URL descargada |
| `_dataset`, `_conjunto`, `_recurso`, `_recurso_id` | Conjunto de datos y recurso del portal (CKAN, OpenDataSoft…) |
| `_archivo_origen`, `_archivo_fuente` | El fichero de la capa cruda del que sale la fila, con su ruta relativa |
| `_fichero_publicado`, `_nombre_publicado` | Nombre del fichero en el portal |
| `_formato` | Formato del fichero de origen (CSV, XLSX, XLS…) |
| `_miembro` | Fichero dentro de un ZIP o un RAR |
| `_hoja` | Hoja de un Excel o de un ODS |
| `_fila_origen` | Número de registro en el fichero de origen |
| `_anio`, `_anio_fichero`, `_anio_recurso`, `_trimestre`, `_mes`, `_periodo`, `_parte` | Periodo que el portal asigna al fichero (no la fecha del contrato) |
| `_titulo`, `_pagina` | Texto del enlace y página del portal que lo enlaza |
| `_encabezado`, `_titulo_tabla`, `_titulo_hoja` | Filas de título que hay encima de la cabecera |
| `_estructura` | Estructura de columnas detectada (Ayuntamiento de Madrid) |

## Calidad del origen

Lo raro que trae el fichero publicado se conserva y se marca: nada se borra ni se arregla en silencio.

| Marca | Qué significa | Dónde |
|---|---|---|
| `_duplicado` | La fila repite otra anterior del mismo origen (se conserva, así se publicó) | País Vasco, Ayuntamiento de Madrid |
| `_repetido_en_csv` | Fila de un XLSX o XLS que se consolida porque falta su CSV, y que ya está, igual o casi igual, en un CSV publicado | Ayuntamiento de Madrid |
| `_repetido_de` | El listado anterior, aún publicado, que ya traía el mismo número de registro | Extremadura |
| `_columnas_corridas` | Fila con las columnas desplazadas en origen, recolocada | País Vasco |
| `_registro_irregular` | Fila que sale de un registro irregular del CSV (campos de más o de menos, saltos de línea) | TED |
| `_columna_extra_N` | Campos de más de un registro, guardados en lugar de descartarlos | Comunidad de Madrid |
| `_lineas_unidas` | Registro partido por un salto de línea sin comillas, vuelto a unir: cuántas líneas ocupaba | Andalucía, menores |
| `_<columna>_sin_cortes`, `_resto_json` | Texto sin los cortes de línea del exportador y restos de la lista JSON que no son de ningún campo; la columna original se sirve tal cual | Región de Murcia |
| `_fila_vacia` | Fila sin datos, conservada | Ayuntamiento de Madrid |
| `textos_originales` | JSON con el texto publicado de cada importe o fecha que no se puede convertir (la columna queda vacía y el valor no se pierde) | PLACSP: en la principal, `_borrados` y, el de cada fila, en las tablas de la [ampliación del CODICE](fuentes/placsp.md#ampliación-del-codice-periodo-prórrogas-garantías-solvencia-anuncios-y-documentos) |

## Versiones de la PLACSP

La PLACSP publica una entrada nueva cada vez que se actualiza una licitación. Se sirven todas
([ficha](fuentes/placsp.md#correcciones-en-los-datos-placsp)).

| Marca | Qué significa | Cómo usarla |
|---|---|---|
| `n_versiones` | Versiones distintas del mismo `id` (valores distintos de `fecha_updated`) | |
| `es_ultima_version` | `True` en una fila por `id`: la de `fecha_updated` más reciente | **Para contar o sumar licitaciones: `df[df.es_ultima_version]`.** En v2026.02, que no la tenía, sumar sin filtrar multiplicaba la adjudicación por 4,7 |
| `entrada_repetida` | La misma versión publicada más de una vez: se conserva y se marca | |

Las tablas de detalle (`_resultados`, `_adjudicatarios`, `_lotes`, `_criterios`, `_modificaciones` y las de la ampliación del CODICE: `_ampliado`, `_condiciones`, `_anuncios`, `_documentos`) llevan las mismas
marcas y se cruzan con la principal por `id` + `fecha_updated`.

## Importes corregidos

Los importes se sirven como se publican, también cuando están mal: un importe erróneo publicado es un dato. Al lado, la
versión corregida y el motivo ([ficha de calidad](fuentes/calidad.md#importes-publicados-y-corregidos-issue-22)):

| Marca | Qué significa |
|---|---|
| `<campo>_corregido` | El importe que usar: el publicado si no hay corrección; vacío si el publicado no es fiable y no hay estimación |
| `correccion_<campo>` | Vacío, o el motivo: `registro`, `escala_x100`, `escala_x1000`, `inverosimil` o `no_comparable` |

Para `importe_adjudicacion`, `importe_sin_iva` y `valor_estimado_contrato` de la PLACSP. Las calcula
[`calidad/correcciones.py`](../calidad/correcciones.py), que puedes aplicar tú mismo sobre la release (ver el
[uso rápido](../README.md#uso-rápido)).

## Valores convertidos

Algunas fuentes publican importes o fechas como texto. Se sirve el texto y, al lado, la conversión:

| Marca | Qué significa | Dónde |
|---|---|---|
| `_<campo>_num` | El importe publicado leído como número (nulo si el texto no es un número) | Andalucía, menores |
| `<campo>_texto` | El texto publicado de un importe o una fecha convertidos | Ayuntamiento de Madrid |

## TED

| Marca | Qué significa |
|---|---|
| `_xml_eforms` | Vacío: la fila sale del XML eForms del aviso. Si no, el motivo por el que solo lleva los datos del aviso (404 de TED o XML anterior a eForms) |
| `ganadora_por` | Cómo se identificó la oferta ganadora: `contrato` (la cita un contrato) o `contrato del resultado` |

Más columnas del TED (`win_platform_id`, `n_ofertas_descritas`, `lot_result_id`…) en su [ficha](fuentes/ted.md).

## Cómo se genera `docs/esquema/`

[`scripts/esquema_release.py`](../scripts/esquema_release.py) lee solo los pies de los Parquet de una release
descomprimida (cada ZIP en su carpeta, como la deja `unzip X.zip -d X`) y escribe una página por ZIP con sus ficheros,
filas, columnas y tipos, enlazando cada marca de esta página. Con `--manifest` comprueba además filas y columnas
contra el `MANIFEST.csv` de la release:

```bash
python scripts/esquema_release.py --datos CARPETA --release v2026.10 --manifest CARPETA/MANIFEST.csv
```

Las páginas de `docs/esquema/` son las de la release v2026.10: los 612 Parquet de sus 31 ZIP, también los de las
carpetas `v2026.02/`, con las filas y las columnas iguales que en su `MANIFEST.csv`.
