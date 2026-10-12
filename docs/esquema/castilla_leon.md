# `castilla_leon.zip`

Esquema de los Parquet de `castilla_leon.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/castilla_leon.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratos-agregado.parquet` | 3.116 | 13 |
| `contratos-agricultura-ecologica-sac.parquet` | 53 | 11 |
| `contratos-agroambiente-clima-sac.parquet` | 424 | 12 |
| `contratos-basados-en-acuerdo-marco-de-sacyl.parquet` | 7.049 | 21 |
| `contratos-basados-en-acuerdo-marco.parquet` | 2.290 | 21 |
| `contratos-de-emergencia.parquet` | 2.594 | 16 |
| `contratos-desistimiento-renuncia-desierto.parquet` | 4.416 | 15 |
| `contratos-menores-sacyl.parquet` | 15.450 | 21 |
| `contratos-menores.parquet` | 128.232 | 21 |
| `contratos-modificados-sacyl.parquet` | 1.479 | 20 |
| `contratos-modificados.parquet` | 439 | 20 |
| `contratos-ordinarios-sacyl.parquet` | 7.453 | 21 |
| `contratos-ordinarios.parquet` | 9.462 | 21 |
| `contratos-realizados-en-las-provincias-de-castilla-y-leon.parquet` | 2.600 | 17 |
| `licitacion-de-obras-publicas.parquet` | 1 | 12 |

## `contratos-agregado.parquet`

3.116 filas · 13 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `fecha` | `string` |  |
| 2 | `organo` | `string` |  |
| 3 | `tipo_de_contrato` | `string` |  |
| 4 | `contratos` | `string` |  |
| 5 | `tipo` | `string` |  |
| 6 | `importe_total` | `string` |  |
| 7 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 8 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 10 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 11 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-agricultura-ecologica-sac.parquet`

53 filas · 11 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `AÑO` | `string` |  |
| 2 | `PROVINCIA` | `string` |  |
| 3 | `SECCIÓN AGRARICA COMARCAL` | `string` |  |
| 4 | `NÚMERO DE CONTRATOS EN VIGOR` | `string` |  |
| 5 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 6 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 7 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 8 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 9 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 10 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 11 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-agroambiente-clima-sac.parquet`

424 filas · 12 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `AÑO` | `string` |  |
| 2 | `PROVINCIA` | `string` |  |
| 3 | `SECCIÓN AGRARICA COMARCAL` | `string` |  |
| 4 | `TIPO CONTRATO` | `string` |  |
| 5 | `CONTRATOS EN VIGOR` | `string` |  |
| 6 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 7 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 8 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 10 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 11 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-basados-en-acuerdo-marco-de-sacyl.parquet`

7.049 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código Expediente` | `string` |  |
| 2 | `Titulo` | `string` |  |
| 3 | `Organo` | `string` |  |
| 4 | `Procedimiento de adjudicación` | `string` |  |
| 5 | `Tipo de Contrato` | `string` |  |
| 6 | `Presupuesto de licitación (IVA 
Incluido)` | `string` |  |
| 7 | `Nº de invitaciones 
cursadas` | `string` |  |
| 8 | `Presupuesto de Adjudicación
(IVA incluido)` | `string` |  |
| 9 | `Fecha de Adjudicación` | `string` |  |
| 10 | `Identidad del Adjudicatario` | `string` |  |
| 11 | `NIF adjudicatario` | `string` |  |
| 12 | `Plazo ejecución 
(meses)` | `string` |  |
| 13 | `Plazo ejecución 
(días)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-basados-en-acuerdo-marco.parquet`

2.290 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código contrato` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Órgano` | `string` |  |
| 4 | `Procedimiento de adjudicación` | `string` |  |
| 5 | `Tipo de contrato` | `string` |  |
| 6 | `Presupuesto de Licitación (IVA incluido)` | `string` |  |
| 7 | `Nº de Invitaciones Cursadas` | `string` |  |
| 8 | `Presupuesto de Adjudicación (IVA incluido)` | `string` |  |
| 9 | `Fecha de Adjudicación` | `string` |  |
| 10 | `Identidad del adjudicatario` | `string` |  |
| 11 | `NIF Adjudicatario` | `string` |  |
| 12 | `Plazo de ejecución (meses)` | `string` |  |
| 13 | `Plazo de ejecución (días)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-de-emergencia.parquet`

2.594 filas · 16 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Objeto del contrato` | `string` |  |
| 2 | `Órgano de contratación` | `string` |  |
| 3 | `Procedimiento de adjudicación` | `string` |  |
| 4 | `Tipo de contrato` | `string` |  |
| 5 | `Presupuesto de adjudicación (IVA incluido)` | `string` |  |
| 6 | `Fecha de formalización` | `string` |  |
| 7 | `Identidad del adjudicatario` | `string` |  |
| 8 | `NIF del adjudicatario` | `string` |  |
| 9 | `Plazo de ejecución` | `string` |  |
| 10 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 14 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 15 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-desistimiento-renuncia-desierto.parquet`

4.416 filas · 15 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código Expediente` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Órgano` | `string` |  |
| 4 | `Procedimiento de adjudicación` | `string` |  |
| 5 | `Tipo de contrato` | `string` |  |
| 6 | `Presupuesto de Licitación (IVA incluido)` | `string` |  |
| 7 | `Fecha de Resolución` | `string` |  |
| 8 | `Enlace de Publicación` | `string` |  |
| 9 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 10 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 14 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 15 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-menores-sacyl.parquet`

15.450 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Codigo Expediente` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Organo` | `string` |  |
| 4 | `Procedimiento de 
Adjudicación` | `string` |  |
| 5 | `Tipo de Contrato` | `string` |  |
| 6 | `Presupuesto de licitación (IVA incluido)` | `string` |  |
| 7 | `Nº de petición de ofertas` | `string` |  |
| 8 | `Presupuesto de Adjudicación 
(IVA Incluido)` | `string` |  |
| 9 | `Fecha Aprobación de Gasto` | `string` |  |
| 10 | `Identidad del Adjudicatario` | `string` |  |
| 11 | `NIF Adjudicatario` | `string` |  |
| 12 | `Plazo ejecución
(meses)` | `string` |  |
| 13 | `Plazo ejecución
(dias)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-menores.parquet`

128.232 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código Expediente` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Órgano` | `string` |  |
| 4 | `Procedimiento de adjudicación` | `string` |  |
| 5 | `Tipo de contrato` | `string` |  |
| 6 | `Presupuesto de Licitación (IVA incluido)` | `string` |  |
| 7 | `Nº Petición de Ofertas` | `string` |  |
| 8 | `Presupuesto de Adjudicación (IVA incluido)` | `string` |  |
| 9 | `Fecha Aprobación de Gasto` | `string` |  |
| 10 | `Identidad del adjudicatario` | `string` |  |
| 11 | `NIF Adjudicatario` | `string` |  |
| 12 | `Plazo de ejecución (meses)` | `string` |  |
| 13 | `Plazo de ejecución (dias)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-modificados-sacyl.parquet`

1.479 filas · 20 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Codigo contrato` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Órgano` | `string` |  |
| 4 | `Presupuesto de adjudicación (IVA incluido)` | `string` |  |
| 5 | `Identidad del Adjudicatario` | `string` |  |
| 6 | `NIF Adjudicatario` | `string` |  |
| 7 | `Fecha Formalización` | `string` |  |
| 8 | `Codigo de Incidencia` | `string` |  |
| 9 | `Modificación de importe (IVA incluido)` | `string` |  |
| 10 | `Modificación de plazo (meses)` | `string` |  |
| 11 | `Modificación de plazo (dias)` | `string` |  |
| 12 | `Fecha de resolución modificación` | `string` |  |
| 13 | `Enlace de Publicación` | `string` |  |
| 14 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-modificados.parquet`

439 filas · 20 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código de contrato` | `string` |  |
| 2 | `Título` | `string` |  |
| 3 | `Órgano` | `string` |  |
| 4 | `Importe de adjudicación (IVA incluido)` | `string` |  |
| 5 | `Identidad del adjudicatario` | `string` |  |
| 6 | `NIF adjudicatario` | `string` |  |
| 7 | `Fecha formalización` | `string` |  |
| 8 | `Código de incidencia` | `string` |  |
| 9 | `Modificación de importe (IVA incluido)` | `string` |  |
| 10 | `Modificación de plazo (meses)` | `string` |  |
| 11 | `Modificación de plazo (días)` | `string` |  |
| 12 | `Fecha de resolución modificación` | `string` |  |
| 13 | `Enlace de publicación` | `string` |  |
| 14 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-ordinarios-sacyl.parquet`

7.453 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código Contrato` | `string` |  |
| 2 | `Titulo` | `string` |  |
| 3 | `Organo` | `string` |  |
| 4 | `Procedimiento de adjudicación` | `string` |  |
| 5 | `Tipo de Contrato` | `string` |  |
| 6 | `Presupuesto de licitación (IVA 
Incluido)` | `string` |  |
| 7 | `Nº ofertas 
presentadas` | `string` |  |
| 8 | `Presupuesto de Adjudicación
(IVA incluido)` | `string` |  |
| 9 | `Fecha Formalización` | `string` |  |
| 10 | `Identidad del Adjudicatario` | `string` |  |
| 11 | `NIF adjudicatario` | `string` |  |
| 12 | `Plazo ejecución
(meses)` | `string` |  |
| 13 | `Plazo ejecución
(días)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-ordinarios.parquet`

9.462 filas · 21 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Código Contrato` | `string` |  |
| 2 | `Fecha Formalización` | `string` |  |
| 3 | `Título` | `string` |  |
| 4 | `Órgano` | `string` |  |
| 5 | `Procedimiento de adjudicación` | `string` |  |
| 6 | `Tipo de contrato` | `string` |  |
| 7 | `Presupuesto de Licitación (IVA incluido)` | `string` |  |
| 8 | `Nº Ofertas Presentadas` | `string` |  |
| 9 | `Presupuesto de Adjudicación (IVA incluido)` | `string` |  |
| 10 | `Identidad del adjudicatario` | `string` |  |
| 11 | `NIF Adjudicatario` | `string` |  |
| 12 | `Plazo de ejecución (meses)` | `string` |  |
| 13 | `Plazo de ejecución (dias)` | `string` |  |
| 14 | `Enlace de publicación` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos-realizados-en-las-provincias-de-castilla-y-leon.parquet`

2.600 filas · 17 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `FECHA` | `string` |  |
| 2 | `CODIGO_TERRITORIO` | `string` |  |
| 3 | `NOMBRE_TERRITORIO` | `string` |  |
| 4 | `LONGITUD` | `string` |  |
| 5 | `LATITUD` | `string` |  |
| 6 | `TOTAL` | `string` |  |
| 7 | `INDEFINIDO` | `string` |  |
| 8 | `TEMPORAL` | `string` |  |
| 9 | `Posición` | `string` |  |
| 10 | `Provincia` | `string` |  |
| 11 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 15 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `licitacion-de-obras-publicas.parquet`

1 filas · 12 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Periodo` | `string` |  |
| 2 | `Por organismos - total (miles de €)` | `string` |  |
| 3 | `Edificación - vivienda (miles de €)` | `string` |  |
| 4 | `Edificación - total (miles de €)` | `string` |  |
| 5 | `Obra civil - total (miles de €)` | `string` |  |
| 6 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 7 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 8 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 10 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 11 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
