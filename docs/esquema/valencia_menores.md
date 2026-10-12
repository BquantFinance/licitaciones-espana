# `valencia_menores.zip`

Esquema de los Parquet de `valencia_menores.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/valencia_menores.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `ajuntament_valencia.parquet` | 19.258 | 36 |
| `diputacion_alicante.parquet` | 33.473 | 50 |
| `diputacion_alicante_va.parquet` | 2.009 | 25 |
| `universidad_alicante.parquet` | 97.638 | 28 |
| `universidad_miguel_hernandez.parquet` | 14.949 | 136 |
| `universitat_politecnica_valencia.parquet` | 618.731 | 24 |
| `universitat_valencia.parquet` | 762.281 | 155 |
| `universitat_valencia_va.parquet` | 333.752 | 116 |

## `ajuntament_valencia.parquet`

19.258 filas · 36 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Objeto del contrato` | `string` |  |
| 2 | `Objeto del contrato.1` | `string` |  |
| 3 | `Tipo` | `string` |  |
| 4 | `Fecha` | `string` |  |
| 5 | `Importe (sin IVA)` | `string` |  |
| 6 | `IVA` | `string` |  |
| 7 | `Número de expediente` | `string` |  |
| 8 | `Órgano de contratación` | `string` |  |
| 9 | `Unidad tramitadora` | `string` |  |
| 10 | `Número de propuesta` | `string` |  |
| 11 | `Número de ofertas` | `string` |  |
| 12 | `Adjudicatario` | `string` |  |
| 13 | `NIF/CIF/NIE adjudicatario` | `string` |  |
| 14 | `Estado` | `string` |  |
| 15 | `Fecha de inicio` | `string` |  |
| 16 | `Fecha de fin` | `string` |  |
| 17 | `Duración del contrato` | `string` |  |
| 18 | `Fecha de la factura` | `string` |  |
| 19 | `Fecha de pago` | `string` |  |
| 20 | `Centro de gastos` | `string` |  |
| 21 | `Aplicación presupuestaria` | `string` |  |
| 22 | `Objeto del contrato.1 (enlace)` | `string` |  |
| 23 | `_num_contrato` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 24 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_mes` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 29 | `_estado_consulta` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 30 | `_consulta_desde` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 31 | `_consulta_hasta` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 32 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 33 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 34 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 35 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 36 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `diputacion_alicante.parquet`

33.473 filas · 50 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `C.Gestor` | `string` |  |
| 2 | `Tipo Contrato` | `string` |  |
| 3 | `Fase` | `string` |  |
| 4 | `Fecha (*)` | `string` |  |
| 5 | `Importe` | `string` |  |
| 6 | `Nombre Ter.` | `string` |  |
| 7 | `Texto Libre` | `string` |  |
| 8 | `Unnamed: 7` | `string` |  |
| 9 | `C Gestor` | `string` |  |
| 10 | `Nombre Ter ` | `string` |  |
| 11 | `Fecha` | `string` |  |
| 12 | `Nombre Tercero` | `string` |  |
| 13 | `Concepto` | `string` |  |
| 14 | `Nº Operación` | `string` |  |
| 15 | `Fecha )*)` | `string` |  |
| 16 | ` ` | `string` |  |
| 17 | `Económica` | `string` |  |
| 18 | `Fecha  (*)` | `string` |  |
| 19 | `Duración` | `string` |  |
| 20 | `Objeto` | `string` |  |
| 21 | `Duración ` | `string` |  |
| 22 | `Unnamed: 0` | `string` |  |
| 23 | `TERCERO` | `string` |  |
| 24 | `TIPO CONTRATO` | `string` |  |
| 25 | `OBJETO` | `string` |  |
| 26 | `ADJUDICADO (sin IVA)` | `string` |  |
| 27 | `ADJUDICADO (con IVA)` | `string` |  |
| 28 | `FECHA INICIO` | `string` |  |
| 29 | `FECHA FIN` | `string` |  |
| 30 | `DURACIÓN` | `string` |  |
| 31 | `CENTRO GESTOR` | `string` |  |
| 32 | `REFERENCIA` | `string` |  |
| 33 | `FECHA ADJUDICACIÓN` | `string` |  |
| 34 | `SUBSANACIÓN` | `string` |  |
| 35 | `NOTAS ACLARATORIAS:` | `string` |  |
| 36 | `Unnamed: 1` | `string` |  |
| 37 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 38 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 39 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 40 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 41 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 42 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 43 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 44 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 45 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 46 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 47 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 48 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 49 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 50 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `diputacion_alicante_va.parquet`

2.009 filas · 25 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `TERCER` | `string` |  |
| 2 | `TIPUS CONTRACTE` | `string` |  |
| 3 | `OBJECTE` | `string` |  |
| 4 | `ADJUDICAT (sense IVA)` | `string` |  |
| 5 | `ADJUDICAT (amb IVA)` | `string` |  |
| 6 | `DATA INICI` | `string` |  |
| 7 | `DATA FI` | `string` |  |
| 8 | `DURADA` | `string` |  |
| 9 | `CENTRE GESTOR` | `string` |  |
| 10 | `REFERÈNCIA` | `string` |  |
| 11 | `DATA ADJUDICACIÓ` | `string` |  |
| 12 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 18 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 22 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 25 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `universidad_alicante.parquet`

97.638 filas · 28 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EXPEDIENTE` | `string` |  |
| 2 | `TIPO_CONTRATO` | `string` |  |
| 3 | `DESCRIPCION` | `string` |  |
| 4 | `IMPORTE` | `string` |  |
| 5 | `ADJUDICATARIO` | `string` |  |
| 6 | `FECHA_ADJUDICACION` | `string` |  |
| 7 | `NÚM. DE EXPEDIENTE` | `string` |  |
| 8 | `TIPO` | `string` |  |
| 9 | `OBJETO DEL CONTRATO` | `string` |  |
| 10 | `FECHA DE ADJUDICACIÓN` | `string` |  |
| 11 | `PROVEEDOR` | `string` |  |
| 12 | `IMPORTE EN EUROS
(SIN IVA)` | `string` |  |
| 13 | `NÚM.DE EXPEIENTE` | `string` |  |
| 14 | `IMPORTE EN EUROS           (SIN IVA)` | `string` |  |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 21 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 25 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 26 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 27 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 28 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `universidad_miguel_hernandez.parquet`

14.949 filas · 136 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EXPEDIENTE` | `string` |  |
| 2 | `TIPO CONTRATO` | `string` |  |
| 3 | `OBJETO` | `string` |  |
| 4 | `ADJUDICATARIO` | `string` |  |
| 5 | `CIF/NIF CONTRATISTA` | `string` |  |
| 6 | `FECHA ADJUDICACIÓN` | `string` |  |
| 7 | `VALOR ESTIMADO` | `string` |  |
| 8 | `Código del expediente` | `string` |  |
| 9 | `Descripción Ayuda` | `string` |  |
| 10 | `Referencia 
ayuda` | `string` |  |
| 11 | `Publicidad ayuda` | `string` |  |
| 12 | `Unnamed: 4` | `string` |  |
| 13 | `Objeto contrato` | `string` |  |
| 14 | `Plazo de duración` | `string` |  |
| 15 | `N.I.F del proveedor` | `string` |  |
| 16 | `Nombre del proveedor` | `string` |  |
| 17 | `Importe total` | `string` |  |
| 18 | `Investigación` | `string` |  |
| 19 | `Descripción de tipo contrato` | `string` |  |
| 20 | `Fecha adjudicación` | `string` |  |
| 21 | `Exclusiv.` | `string` |  |
| 22 | `Observaciones` | `string` |  |
| 23 | `Referencia ayuda` | `string` |  |
| 24 | `N.I.F. del proveedor` | `string` |  |
| 25 | `Exclusividad` | `string` |  |
| 26 | `Descripción ayuda` | `string` |  |
| 27 | `CPV` | `string` |  |
| 28 | `DESCRIPCION CPV` | `string` |  |
| 29 | `USUARIO` | `string` |  |
| 30 | `OBSERVACIONES` | `string` |  |
| 31 | `DURACIÓN DE CONTRATO` | `string` |  |
| 32 | `Leyenda publicidad ayuda` | `string` |  |
| 33 | `Plazo de Duración ` | `string` |  |
| 34 | `Descripción tipo contrato` | `string` |  |
| 35 | `5950` | `string` |  |
| 36 | `5950.1` | `string` |  |
| 37 | `True` | `string` |  |
| 38 | `2022-05-02` | `string` |  |
| 39 | `2022-05-02.1` | `string` |  |
| 40 | `True.1` | `string` |  |
| 41 | `DIAGENODE` | `string` |  |
| 42 | `DIAGENODE.1` | `string` |  |
| 43 | `True.2` | `string` |  |
| 44 | `SERVICIO ` | `string` |  |
| 45 | `SERVICIO .1` | `string` |  |
| 46 | `True.3` | `string` |  |
| 47 | `Columna1` | `string` |  |
| 48 | `Descripción tipo` | `string` |  |
| 49 | `CÓDIGO DEL EXPEDIENTE` | `string` |  |
| 50 | `DESCRIPCIÓN AYUDA` | `string` |  |
| 51 | `REFERENCIA AYUDA` | `string` |  |
| 52 | `LEYENDA PUBLICIDAD` | `string` |  |
| 53 | `PUBLICIDAD AYUDA` | `string` |  |
| 54 | `OBJETO CONTRATO` | `string` |  |
| 55 | `PLAZO DE DURACIÓN` | `string` |  |
| 56 | `NIF PROVEEDOR` | `string` |  |
| 57 | `NOMBRE DEL PROVEEDOR` | `string` |  |
| 58 | `IMPORTE TOTAL ` | `string` |  |
| 59 | `INVESTIGACIÓN` | `string` |  |
| 60 | `DESCRIPCIÓN TIPO` | `string` |  |
| 61 | `EXCLUSIVIDAD` | `string` |  |
| 62 | `Nº DE OFERTAS` | `string` |  |
| 63 | `NOMBRE PROVEEDOR1` | `string` |  |
| 64 | `IMPORTE NETO (SIN IVA)1` | `string` |  |
| 65 | `NOMBRE PROVEEDOR2` | `string` |  |
| 66 | `IMPORTE NETO (SIN IVA)2` | `string` |  |
| 67 | `NOMBRE PROVEEDOR3` | `string` |  |
| 68 | `IMPORTE NETO (SIN IVA)3` | `string` |  |
| 69 | `NOMBRE PROVEEDOR4` | `string` |  |
| 70 | `IMPORTE NETO (SIN IVA)4` | `string` |  |
| 71 | `Nº OFERTAS` | `string` |  |
| 72 | `NOMBRE PROVEEDOR 1` | `string` |  |
| 73 | `IMPORTE NETO 1 (SIN IVA)` | `string` |  |
| 74 | `NOMBRE PROVEEDOR 2` | `string` |  |
| 75 | `IMPORTE NETO 2 (SIN IVA)` | `string` |  |
| 76 | `NOMBRE PROVEEDOR 3` | `string` |  |
| 77 | `IMPORTE NETO 3 (SIN IVA)` | `string` |  |
| 78 | `NOMBRE PROVEEDOR 4` | `string` |  |
| 79 | `IMPORTE NETO 4 (SIN IVA)` | `string` |  |
| 80 | `Nº EXPEDIENTE` | `string` |  |
| 81 | `IMPORTE TOTAL ADJUDICACIÓN (CON IVA)` | `string` |  |
| 82 | `NOMBRE PROVEEDOR` | `string` |  |
| 83 | `IMPORTE NETO (SIN IVA)` | `string` |  |
| 84 | `TIPO DE CONTRATO` | `string` |  |
| 85 | `NOMBRE PROVEEDOR5` | `string` |  |
| 86 | `OBJETO DEL CONTRATO` | `string` |  |
| 87 | `05` | `string` |  |
| 88 | `FACULTAD DE FARMACIA` | `string` |  |
| 89 | `42` | `string` |  |
| 90 | `SECRETARÍA GENERAL` | `string` |  |
| 91 | `Unnamed: 7` | `string` |  |
| 92 | `Expediente` | `string` |  |
| 93 | `1` | `string` |  |
| 94 | `referencia FEDER ` | `string` |  |
| 95 | `Duración` | `string` |  |
| 96 | `NIF proveedor` | `string` |  |
| 97 | `Proveedor` | `string` |  |
| 98 | `Importe total contrato` | `string` |  |
| 99 | `Tipo contrato` | `string` |  |
| 100 | `Tipo de pago` | `string` |  |
| 101 | `Descripción FEDER` | `string` |  |
| 102 | `NIF del proveedor` | `string` |  |
| 103 | `Tipo de contrato` | `string` |  |
| 104 | `Cofinanciación Fondos FEDER` | `string` |  |
| 105 | `Unnamed: 0` | `string` |  |
| 106 | `01` | `string` |  |
| 107 | `FACULTAD DE MEDICINA` | `string` |  |
| 108 | `33` | `string` |  |
| 109 | `ESCUELA POLITÉCNICA SUPERIOR DE ELCHE` | `string` |  |
| 110 | `Descripción  FEDER` | `string` |  |
| 111 | `Plazo de duración.1` | `string` |  |
| 112 | `Descripción de la unidad de tiempo del plazo de duración` | `string` |  |
| 113 | `Unnamed: 15` | `string` |  |
| 114 | `Unnamed: 5` | `string` |  |
| 115 | `Código ` | `string` |  |
| 116 | `Código de tipo contrato` | `string` |  |
| 117 | `Descripción de tipo de pago` | `string` |  |
| 118 | `referencia FEDER` | `string` |  |
| 119 | `Columna4` | `string` |  |
| 120 | `VICERRECTORADO DE PLANIFICACIÓN (Click para volver)` | `string` |  |
| 121 | `VICERRECTORADO DE ESTUDIOS (Click para volver)` | `string` |  |
| 122 | `CODIGO DEL EXPEDIENTE` | `string` |  |
| 123 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 124 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 125 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 126 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 127 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 128 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 129 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 130 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 131 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 132 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 133 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 134 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 135 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 136 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `universitat_politecnica_valencia.parquet`

618.731 filas · 24 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `OBJETO` | `string` |  |
| 2 | `DURACION` | `string` |  |
| 3 | `PPTO_LICITA` | `string` |  |
| 4 | `IMP_ADJUDICA` | `string` |  |
| 5 | `TERCERO` | `string` |  |
| 6 | `CENTRO_DIRECTIVO` | `string` |  |
| 7 | `CLASIFICACION` | `string` |  |
| 8 | `EXPEDIENTE` | `string` |  |
| 9 | `IMPBASE_ADJ` | `string` |  |
| 10 | `CONCEPTO ECONÓMICO` | `string` |  |
| 11 | `IMPORTE` | `string` |  |
| 12 | `CENTRO DIRECTIVO` | `string` |  |
| 13 | ` IMPORTE ` | `string` |  |
| 14 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 20 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 22 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `universitat_valencia.parquet`

762.281 filas · 155 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `FECHAOP` | `string` |  |
| 2 | `DESCRIPCION` | `string` |  |
| 3 | `NIF TERCERO` | `string` |  |
| 4 | `NOMTERCERO` | `string` |  |
| 5 | `TOTALCONTRATO` | `string` |  |
| 6 | `AREAGESTORA` | `string` |  |
| 7 | `NIFTERCERO` | `string` |  |
| 8 | `C.I.F./N.I.F. contratista` | `string` |  |
| 9 | `Contratista Nombre` | `string` |  |
| 10 | `Identificador largo` | `string` |  |
| 11 | `Objeto` | `string` |  |
| 12 | `Fecha Adjudicación` | `string` |  |
| 13 | `Importe adjudicación (I.V.A.excluido)` | `string` |  |
| 14 | `Importe I.V.A.` | `string` |  |
| 15 | `Importe adjudicación (I.V.A. incluido)` | `string` |  |
| 16 | `Duración contrato` | `string` |  |
| 17 | `Unidad Funcional` | `string` |  |
| 18 | `Tipo de contrato` | `string` |  |
| 19 | `Exclusividad` | `string` |  |
| 20 | `NIF adjudicatario` | `string` |  |
| 21 | `Nombre adjudicatario` | `string` |  |
| 22 | `Importe adjudicación (sin IVA)` | `string` |  |
| 23 | `Tipo IVA` | `string` |  |
| 24 | `Importe IVA` | `string` |  |
| 25 | `Importe adjudicación (con IVA)` | `string` |  |
| 26 | `Plazo ejecución` | `string` |  |
| 27 | `Fecha adjudicación` | `string` |  |
| 28 | `Importe final (sin IVA)` | `string` |  |
| 29 | `¿Se trata de acceso a BBDD o plublicaciones?` | `string` |  |
| 30 | `DA 54 LCSP` | `string` |  |
| 31 | `Unidad funcional` | `string` |  |
| 32 | `Identificador` | `string` |  |
| 33 | `Tipo Contrato` | `string` |  |
| 34 | `NIF Adjudicatario` | `string` |  |
| 35 | `Nombre Adjudicatario` | `string` |  |
| 36 | `Tipo I.V.A.` | `string` |  |
| 37 | `Plazo Ejecución` | `string` |  |
| 38 | `Importe final (I.V.A.excluido)` | `string` |  |
| 39 | `¿Se trata de acceso BBDD o publicaciones?` | `string` |  |
| 40 | `¿Se trata DA54 LCSP?` | `string` |  |
| 41 | `Pagado por caja fija` | `string` |  |
| 42 | `Tipo contrato` | `string` |  |
| 43 | `Importe adjudicación (IVA excluido)` | `string` |  |
| 44 | `Importe adjudicación (IVA incluido)` | `string` |  |
| 45 | `Importe final contrato (IVA excluido)` | `string` |  |
| 46 | `¿Se trata de acceso a BBDD o publicaciones?` | `string` |  |
| 47 | `Tipo contrato ` | `string` |  |
| 48 | `Importe final contrato          (IVA excluido)` | `string` |  |
| 49 | `Importe final contrato    (IVA excluido)` | `string` |  |
| 50 | `AM del que deriva el contrato` | `string` |  |
| 51 | `UNIDAD FUNCIONAL` | `string` |  |
| 52 | `IMPORTES ADJUDICACIÓN (IVA INCLUIDO)` | `string` |  |
| 53 | `Nº` | `string` |  |
| 54 | `EXPEDIENTES OTROS GASTOS` | `string` |  |
| 55 | `UNIDADES FUNCIONALES` | `string` |  |
| 56 | `IMPORTE ADJUDICACIÓN (IVA incluido)` | `string` |  |
| 57 | `Unnamed: 0` | `string` |  |
| 58 | `Suma importes adjudicación (IVA incluido)` | `string` |  |
| 59 | `Suma Importe adjudicación (IVA incluido) ` | `string` |  |
| 60 | `Acceso BBDD o publicaciones` | `string` |  |
| 61 | ` IMPORTE ADJUDICACIÓN (IVA INCL.)` | `string` |  |
| 62 | `IDENTIFICADOR CONTRATO` | `string` |  |
| 63 | `OBJETO` | `string` |  |
| 64 | `TIPO CONTRATO` | `string` |  |
| 65 | `EXCLUSIVIDAD` | `string` |  |
| 66 | `NIF ADJUDICATARIO` | `string` |  |
| 67 | `NOMBRE ADJUDICATARIO` | `string` |  |
| 68 | `IMPORTE ADJUDICACIÓN (IVA EXCL.)` | `string` |  |
| 69 | `TIPO IMPUESTO` | `string` |  |
| 70 | `IMPORTE IMPUESTO` | `string` |  |
| 71 | `IMPORTE ADJUDICACIÓN (IVA INCL.)` | `string` |  |
| 72 | `PLAZO EJECUCIÓN` | `string` |  |
| 73 | `FECHA ADJUDICACIÓN` | `string` |  |
| 74 | `IMPORTE FINAL CONTRATO` | `string` |  |
| 75 | `DISP. ADICIONAL 9` | `string` |  |
| 76 | `DISP. ADICIONAL 54` | `string` |  |
| 77 | `CAJA FIJA` | `string` |  |
| 78 | `OBJETO DE CONTRATO / CIF / NOMBRE` | `string` |  |
| 79 | `Suma de Importe adjudicación (IVA excluido)` | `string` |  |
| 80 | `Subpoblación` | `string` |  |
| 81 | `Ref. Expediente` | `string` |  |
| 82 | `Regulación armonizada` | `string` |  |
| 83 | `Procedimiento` | `string` |  |
| 84 | `Objeto contrato` | `string` |  |
| 85 | `Duración (meses)` | `string` |  |
| 86 | `Adjudicatario  ` | `string` |  |
| 87 | `CIF adjudicatario` | `string` |  |
| 88 | `D.A. 54 LCSP` | `string` |  |
| 89 | `Cuenta de Identificador` | `string` |  |
| 90 | `Suma Importe adjudicación 
(IVA excluido)` | `string` |  |
| 91 | `Importe Adjudicación 
(IVA excluido)` | `string` |  |
| 92 | `Núm. Expediente Padre` | `string` |  |
| 93 | `Título Expediente Padre` | `string` |  |
| 94 | `Núm. Expediente` | `string` |  |
| 95 | `Objeto Contrato 1` | `string` |  |
| 96 | `Tipo de Contrato` | `string` |  |
| 97 | `Lotes Expediente` | `string` |  |
| 98 | `NIF/CIF Adjudicatario/Contratista` | `string` |  |
| 99 | `Razón Social Adjudicatario/Contratista` | `string` |  |
| 100 | `Importe Adjudicación Sin Impuestos` | `string` |  |
| 101 | `Importe Impuestos Adjudicación` | `string` |  |
| 102 | `Importe total Adjudicación` | `string` |  |
| 103 | `Fecha Adjudicación Contrato` | `string` |  |
| 104 | `Procedimiento de Contratación` | `string` |  |
| 105 | `Unitadad Funcional` | `string` |  |
| 106 | `Importe Adjudicación
(IVA excluido)` | `string` |  |
| 107 | `Suma de Importe de adjudicación
(IVA exclòs)` | `string` |  |
| 108 | `NIF/CIF Adjudicatario/ Contratista` | `string` |  |
| 109 | `Columna1` | `string` |  |
| 110 | `Columna2` | `string` |  |
| 111 | `TIPO IMPUESTO
(%)` | `string` |  |
| 112 | `Suma de Importe
 Adjudicación
 (IVA EXCL.)` | `string` |  |
| 113 | `TIPO IMPUESTO (%)` | `string` |  |
| 114 | `Importe adjudicación 
(IVA excluido)` | `string` |  |
| 115 | `BBDD/ publicaciones` | `string` |  |
| 116 | `Nº EXPEDIENTE
PADRE` | `string` |  |
| 117 | `TÍTULO EXPEDIENTE
PADRE` | `string` |  |
| 118 | `PLAZO EJECUCIÓN
(meses)` | `string` |  |
| 119 | `UNIDAD FUNCIONAL.1` | `string` |  |
| 120 | `IDENTIFICADOR CONTRATO.1` | `string` |  |
| 121 | `OBJETO.1` | `string` |  |
| 122 | `TIPO CONTRATO.1` | `string` |  |
| 123 | `EXCLUSIVIDAD.1` | `string` |  |
| 124 | `NIF ADJUDICATARIO.1` | `string` |  |
| 125 | `NOMBRE ADJUDICATARIO.1` | `string` |  |
| 126 | `IMPORTE ADJUDICACIÓN (IVA EXCL.).1` | `string` |  |
| 127 | `TIPO IMPUESTO.1` | `string` |  |
| 128 | `IMPORTE IMPUESTO.1` | `string` |  |
| 129 | `IMPORTE ADJUDICACIÓN (IVA INCL.).1` | `string` |  |
| 130 | `PLAZO EJECUCIÓN
(meses).1` | `string` |  |
| 131 | `FECHA ADJUDICACIÓN.1` | `string` |  |
| 132 | `IMPORTE FINAL CONTRATO.1` | `string` |  |
| 133 | `DISP. ADICIONAL 9.1` | `string` |  |
| 134 | `DISP. ADICIONAL 54.1` | `string` |  |
| 135 | `CAJA FIJA.1` | `string` |  |
| 136 | `Importe Adjudicación` | `string` |  |
| 137 | `Importe Adjudicación
(IVA Excluido)` | `string` |  |
| 138 | `FECHA OPERACIÓN ` | `string` |  |
| 139 | `NOMBRE TERCERO` | `string` |  |
| 140 | `TOTAL CONTRATO` | `string` |  |
| 141 | `AREA GESTORA` | `string` |  |
| 142 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 143 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 144 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 145 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 146 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 147 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 148 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 149 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 150 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 151 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 152 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 153 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 154 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 155 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `universitat_valencia_va.parquet`

333.752 filas · 116 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EXPEDIENTS ALTRES DESPESES` | `string` |  |
| 2 | `IMPORTS ADJUDICACIÓ (IVA INCLÓS)` | `string` |  |
| 3 | `Nº` | `string` |  |
| 4 | `AM del que deriva el contrato` | `string` |  |
| 5 | `Identificador` | `string` |  |
| 6 | `Objeto` | `string` |  |
| 7 | `Tipo contrato` | `string` |  |
| 8 | `NIF adjudicatario` | `string` |  |
| 9 | `Nombre adjudicatario` | `string` |  |
| 10 | `Importe adjudicación (IVA excluido)` | `string` |  |
| 11 | `Tipo IVA` | `string` |  |
| 12 | `Importe IVA` | `string` |  |
| 13 | `Importe adjudicación (IVA incluido)` | `string` |  |
| 14 | `Plazo ejecución` | `string` |  |
| 15 | `Fecha adjudicación` | `string` |  |
| 16 | `Importe final contrato (IVA excluido)` | `string` |  |
| 17 | `Unitat Funcional` | `string` |  |
| 18 | `Objecte` | `string` |  |
| 19 | `Tipus contracte` | `string` |  |
| 20 | `Exclusivitat` | `string` |  |
| 21 | `Adjudicatari` | `string` |  |
| 22 | `Nom adjudicatari` | `string` |  |
| 23 | `Import adjudicació (IVA exclòs)` | `string` |  |
| 24 | `Tipus IVA` | `string` |  |
| 25 | `Import IVA` | `string` |  |
| 26 | `Importe adjudicació (IVA inclòs` | `string` |  |
| 27 | `Termini adjudicació` | `string` |  |
| 28 | `Data adjudicació` | `string` |  |
| 29 | ` Import final (IVA exclòs)` | `string` |  |
| 30 | `BBDD/ publicacions` | `string` |  |
| 31 | `DA 54 LCSP` | `string` |  |
| 32 | `Caixa Fixa` | `string` |  |
| 33 | `UNITAT FUNCIONAL` | `string` |  |
| 34 | `IMPORTS ADJUDICACIÓ      (IVA INCLÓS)` | `string` |  |
| 35 | `Unidad funcional` | `string` |  |
| 36 | `Exclusividad` | `string` |  |
| 37 | `¿Se trata de acceso a BBDD o publicaciones?` | `string` |  |
| 38 | `Pagado por caja fija` | `string` |  |
| 39 | `Unnamed: 0` | `string` |  |
| 40 | `Suma imports d'adjudicació (IVA inclós)` | `string` |  |
| 41 | `UNITATS FUNCIONALS` | `string` |  |
| 42 | `IMPORT ADJUDICACIÓ (IVA inclòs)` | `string` |  |
| 43 | `Importe adjudicació (IVA inclòs)` | `string` |  |
| 44 | ` IMPORT ADJUDICACIÓ (IVA INCLÒS)` | `string` |  |
| 45 | `UNIDAD FUNCIONAL` | `string` |  |
| 46 | ` IMPORTE ADJUDICACIÓN (IVA INCLUIDO)` | `string` |  |
| 47 | `IDENTIFICADOR CONTRATO` | `string` |  |
| 48 | `OBJETO` | `string` |  |
| 49 | `TIPO CONTRATO` | `string` |  |
| 50 | `EXCLUSIVIDAD` | `string` |  |
| 51 | `NIF ADJUDICATARIO` | `string` |  |
| 52 | `NOMBRE ADJUDICATARIO` | `string` |  |
| 53 | `IMPORTE ADJUDICACIÓN (IVA EXCL.)` | `string` |  |
| 54 | `TIPO IMPUESTO` | `string` |  |
| 55 | `IMPORTE IMPUESTO` | `string` |  |
| 56 | `IMPORTE ADJUDICACIÓN (IVA INCL.)` | `string` |  |
| 57 | `PLAZO EJECUCIÓN` | `string` |  |
| 58 | `FECHA ADJUDICACIÓN` | `string` |  |
| 59 | `IMPORTE FINAL CONTRATO` | `string` |  |
| 60 | `DISP. ADICIONAL 9` | `string` |  |
| 61 | `DISP. ADICIONAL 54` | `string` |  |
| 62 | `CAJA FIJA` | `string` |  |
| 63 | `OBJECTE DE CONTRATE / CIF / NOM` | `string` |  |
| 64 | `Suma d'Import d'adjudicació (IVA exclòs)` | `string` |  |
| 65 | `Subpoblación` | `string` |  |
| 66 | `Ref. Expediente` | `string` |  |
| 67 | `Regulación armonizada` | `string` |  |
| 68 | `Procedimiento` | `string` |  |
| 69 | `Objeto contrato` | `string` |  |
| 70 | `Duración (meses)` | `string` |  |
| 71 | `Adjudicatario  ` | `string` |  |
| 72 | `CIF adjudicatario` | `string` |  |
| 73 | `Import Adjudicació 
(IVA exclòs)` | `string` |  |
| 74 | `Núm. Expediente Padre` | `string` |  |
| 75 | `Título Expediente Padre` | `string` |  |
| 76 | `Núm. Expediente` | `string` |  |
| 77 | `Objeto Contrato 1` | `string` |  |
| 78 | `Tipo de Contrato` | `string` |  |
| 79 | `Lotes Expediente` | `string` |  |
| 80 | `NIF/CIF Adjudicatario/Contratista` | `string` |  |
| 81 | `Razón Social Adjudicatario/Contratista` | `string` |  |
| 82 | `Importe Adjudicación Sin Impuestos` | `string` |  |
| 83 | `Importe Impuestos Adjudicación` | `string` |  |
| 84 | `Importe total Adjudicación` | `string` |  |
| 85 | `Plazo Ejecución` | `string` |  |
| 86 | `Fecha Adjudicación Contrato` | `string` |  |
| 87 | `Procedimiento de Contratación` | `string` |  |
| 88 | `Suma d'Import d'adjudicació 
(IVA exclòs)` | `string` |  |
| 89 | `Import Adjudicació
(IVA exclòs)` | `string` |  |
| 90 | `Import final (IVA exclòs)` | `string` |  |
| 91 | `NIF/CIF Adjudicatario/ Contratista` | `string` |  |
| 92 | `Columna1` | `string` |  |
| 93 | `Columna2` | `string` |  |
| 94 | `Suma d'Import
 Adjudicació
 (IVA EXCL.)` | `string` |  |
| 95 | `TIPO IMPUESTO (%)` | `string` |  |
| 96 | `Import Adjudicació
(IVA exclòs) ` | `string` |  |
| 97 | `Nº EXPEDIENTE
PADRE` | `string` |  |
| 98 | `TÍTULO EXPEDIENTE
PADRE` | `string` |  |
| 99 | `Import adjudicació
(IVA exclòs)` | `string` |  |
| 100 | `PLAZO EJECUCIÓN
(meses)` | `string` |  |
| 101 | `Import Adjudicació` | `string` |  |
| 102 | `Import Adjudicació
(IVA Exclòs)` | `string` |  |
| 103 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 104 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 105 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 106 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 107 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 108 | `_categoria` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 109 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 110 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 111 | `_titulo_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 112 | `_tabla_dinamica` | `string` | [de la fuente](../fuentes/valencia_menores.md) |
| 113 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 114 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 115 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 116 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
