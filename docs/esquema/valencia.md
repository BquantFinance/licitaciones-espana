# `valencia.zip`

Esquema de los Parquet de `valencia.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/valencia.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2013.parquet` | 2.833 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2014.parquet` | 7.299 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2015.parquet` | 8.799 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2016.parquet` | 8.838 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2017.parquet` | 10.918 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2018.parquet` | 24.716 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2019.parquet` | 29.457 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2020.parquet` | 28.863 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2021.parquet` | 31.065 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2022.parquet` | 34.272 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2023.parquet` | 23.854 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2024.parquet` | 37.432 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2025.parquet` | 32 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2026.parquet` | 26.820 | 75 |
| `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_DANA.parquet` | 850 | 79 |
| `convenios/Convenios_subscritos_por_la_GVA_2013.parquet` | 5 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2014.parquet` | 2.256 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2015.parquet` | 939 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2017.parquet` | 1.402 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2018.parquet` | 1.379 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2019.parquet` | 1.758 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2020.parquet` | 1.752 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2021.parquet` | 1.644 | 70 |
| `convenios/Convenios_subscritos_por_la_GVA_2022.parquet` | 1.500 | 70 |
| `entidades/Recursos_de_gobierno_abierto_de_las_entidades_locales_de_la_Comunitat_Valenciana.parquet` | 536 | 17 |
| `lobbies/ACTIVIDADES_DE_INFLUENCIA.parquet` | 2.499 | 8 |
| `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_Grupos_de_Interes_asistentes.parquet` | 2.655 | 7 |
| `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_altos_cargos_asistentes.parquet` | 3.718 | 5 |
| `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_funcionarios_asistentes.parquet` | 492 | 5 |
| `lobbies/Grupos_de_interés.parquet` | 2.182 | 43 |
| `lobbies/Grupos_de_interés_-_detalle_de_aportaciones.parquet` | 1.589 | 7 |
| `lobbies/Grupos_de_interés_-_detalle_de_clientes.parquet` | 238 | 5 |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2013.parquet`

2.833 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2014.parquet`

7.299 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2015.parquet`

8.799 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `int64` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2016.parquet`

8.838 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `int64` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2017.parquet`

10.918 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `int64` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2018.parquet`

24.716 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2019.parquet`

29.457 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2020.parquet`

28.863 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2021.parquet`

31.065 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `large_string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2022.parquet`

34.272 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `large_string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `large_string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2023.parquet`

23.854 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `large_string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2024.parquet`

37.432 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `large_string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2025.parquet`

32 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_2026.parquet`

26.820 filas · 75 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO` | `int64` |  |
| 2 | `NUMERO_DE_REGISTRO` | `int64` |  |
| 3 | `EXPEDIENTE` | `large_string` |  |
| 4 | `CLASE_DE_CONTRATO` | `large_string` |  |
| 5 | `NUMERO_DE_LOTES` | `int64` |  |
| 6 | `NUMERO_DE_UNIDADES` | `double` |  |
| 7 | `CONSELLERIA_ENT_ADJUD` | `large_string` |  |
| 8 | `UNIDAD   ` | `large_string` |  |
| 9 | `OBJETO` | `large_string` |  |
| 10 | `LUGAR_EJECUCION` | `large_string` |  |
| 11 | `TIPO` | `large_string` |  |
| 12 | `TIPO_TRAMITACION` | `large_string` |  |
| 13 | `INC_CLAU_RESPONS` | `large_string` |  |
| 14 | `CONTRATO_RESERVADO` | `large_string` |  |
| 15 | `ACUERDO_CONTRATO_MARCO` | `large_string` |  |
| 16 | `PLAZO_DE_EJECUCION` | `int64` |  |
| 17 | `UNIDADES_PLAZO_EJECUCION` | `large_string` |  |
| 18 | `PLAZO_MAX_CONTRATO` | `double` |  |
| 19 | `UNIDADES_PLAZO_MAX` | `large_string` |  |
| 20 | `NUM_ANUALIDADES_PREV` | `int64` |  |
| 21 | `FECHA_PREV_FINALIZACION` | `large_string` |  |
| 22 | `FECHA_PREV_SIN_GARANTIA` | `large_string` |  |
| 23 | `VALOR_ESTIMADO_SIN_IVA` | `large_string` |  |
| 24 | `IMPORTE_LICIT_SIN_IVA` | `large_string` |  |
| 25 | `IMPORTE_LICIT_IVA` | `large_string` |  |
| 26 | `IMPORTE_TOTAL_LICIT` | `large_string` |  |
| 27 | `SISTEMA_DE_PRECIOS` | `large_string` |  |
| 28 | `REVISION_DE_PRECIOS` | `large_string` |  |
| 29 | `FORMULAS_REV_DE_PRECIOS` | `large_string` |  |
| 30 | `FINAN_CON_FONDOS_EUROPEOS` | `large_string` |  |
| 31 | `FONDO_EUROPEO` | `large_string` |  |
| 32 | `FECHA_PUBL_LICIT_DOCV` | `string` |  |
| 33 | `FECHA_PUBL_LICIT_BOE` | `large_string` |  |
| 34 | `FECHA_PUBL_LICIT_DOUE` | `large_string` |  |
| 35 | `FECHA_PUBL_LICIT_PERFIL` | `large_string` |  |
| 36 | `FECHA_PUBL_ADJ_FORM_DOCV` | `large_string` |  |
| 37 | `FECHA_PUBL_ADJ_FORM_BOE` | `large_string` |  |
| 38 | `FECHA_PUBL_ADJ_FORM_DOUE` | `large_string` |  |
| 39 | `FECHA_PUBL_ADJ_FORM_PERFIL` | `large_string` |  |
| 40 | `PROCEDIMIENTO` | `large_string` |  |
| 41 | `EXP_ACUERDO_MARCO_O_SDA` | `large_string` |  |
| 42 | `FORMA_DE_ADJUDICACION` | `large_string` |  |
| 43 | `NUM_EMPRESAS_INVITADAS` | `double` |  |
| 44 | `NUM_LICITADORES` | `double` |  |
| 45 | `NACIONALIDAD_ADJUDICATARIO` | `large_string` |  |
| 46 | `FECHA_ADJUDICACION` | `large_string` |  |
| 47 | `FECHA_FORMALIZACION` | `large_string` |  |
| 48 | `CIF_NIF_ENMASCARADO` | `large_string` |  |
| 49 | `NOMBRE_O_RAZON_SOCIAL` | `large_string` |  |
| 50 | `UTE` | `large_string` |  |
| 51 | `PYME` | `large_string` |  |
| 52 | `IMP_ADJUD_SIN_IVA` | `large_string` |  |
| 53 | `IMP_ADJUD_IVA` | `large_string` |  |
| 54 | `IMP_TOTAL_ADJUD` | `large_string` |  |
| 55 | `IMP_DEFINITIVO_CONTRATO` | `large_string` |  |
| 56 | `CONCEPTO` | `large_string` |  |
| 57 | `CODIGO_CPV` | `large_string` |  |
| 58 | `CPV` | `large_string` |  |
| 59 | `IMP_UNITARIOS_SIN_IVA` | `large_string` |  |
| 60 | `IMPORTE_LOTE_SIN_IVA` | `large_string` |  |
| 61 | `IMPORTE_LOTE_IVA` | `large_string` |  |
| 62 | `IMPORTE_TOTAL_LOTE` | `large_string` |  |
| 63 | `IMPORTE_DEFINITIVO_LOTE` | `large_string` |  |
| 64 | `TIENE_INCIDENCIAS_IDE` | `large_string` |  |
| 65 | `FECHA_DE_INCIDENCIA_IDE` | `large_string` |  |
| 66 | `TIENE_INCIDENCIAS_MPI` | `large_string` |  |
| 67 | `FECHA_DE_INCIDENCIA_MPI` | `large_string` |  |
| 68 | `TIPO_DE_CONCLUSION` | `large_string` |  |
| 69 | `FECHA_DE_CONCLUSION` | `large_string` |  |
| 70 | `IMP_TOTAL_FINAL_CONTRATO` | `large_string` |  |
| 71 | `CAUSA_DE_RESOLUCION` | `large_string` |  |
| 72 | `URL_LICITACION` | `large_string` |  |
| 73 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 74 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 75 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_DANA.parquet`

850 filas · 79 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `acuerdo_contrato_marco` | `large_string` |  |
| 2 | `causa_de_resolucion` | `large_string` |  |
| 3 | `cif_nif_enmascarado` | `large_string` |  |
| 4 | `clase_de_contrato` | `large_string` |  |
| 5 | `codigo_cpv` | `large_string` |  |
| 6 | `concepto` | `large_string` |  |
| 7 | `conselleria_ent_adjud` | `large_string` |  |
| 8 | `contrato_reservado` | `large_string` |  |
| 9 | `cpv` | `large_string` |  |
| 10 | `ejercicio` | `int64` |  |
| 11 | `exp_acuerdo_marco_o_sda` | `large_string` |  |
| 12 | `expediente` | `large_string` |  |
| 13 | `fecha_adjudicacion` | `large_string` |  |
| 14 | `fecha_de_conclusion` | `large_string` |  |
| 15 | `fecha_de_incidencia_ide` | `large_string` |  |
| 16 | `fecha_de_incidencia_mpi` | `large_string` |  |
| 17 | `fecha_formalizacion` | `large_string` |  |
| 18 | `fecha_prev_finalizacion` | `large_string` |  |
| 19 | `fecha_prev_sin_garantia` | `large_string` |  |
| 20 | `fecha_publ_adj_form_boe` | `string` |  |
| 21 | `fecha_publ_adj_form_docv` | `string` |  |
| 22 | `fecha_publ_adj_form_doue` | `large_string` |  |
| 23 | `fecha_publ_adj_form_perfil` | `large_string` |  |
| 24 | `fecha_publ_licit_boe` | `string` |  |
| 25 | `fecha_publ_licit_docv` | `string` |  |
| 26 | `fecha_publ_licit_doue` | `large_string` |  |
| 27 | `fecha_publ_licit_perfil` | `large_string` |  |
| 28 | `finan_con_fondos_europeos` | `large_string` |  |
| 29 | `fondo_europeo` | `large_string` |  |
| 30 | `forma_de_adjudicacion` | `large_string` |  |
| 31 | `formulas_rev_de_precios` | `large_string` |  |
| 32 | `imp_adjud_iva` | `double` |  |
| 33 | `imp_adjud_sin_iva` | `double` |  |
| 34 | `imp_definitivo_contrato` | `double` |  |
| 35 | `imp_total_adjud` | `double` |  |
| 36 | `imp_total_final_contrato` | `double` |  |
| 37 | `imp_unitarios_sin_iva` | `double` |  |
| 38 | `importe_definitivo_lote` | `double` |  |
| 39 | `importe_licit_iva` | `double` |  |
| 40 | `importe_licit_sin_iva` | `double` |  |
| 41 | `importe_lote_iva` | `double` |  |
| 42 | `importe_lote_sin_iva` | `double` |  |
| 43 | `importe_total_licit` | `double` |  |
| 44 | `importe_total_lote` | `double` |  |
| 45 | `inc_clau_respons` | `large_string` |  |
| 46 | `lugar_ejecucion` | `large_string` |  |
| 47 | `nacionalidad_adjudicatario` | `large_string` |  |
| 48 | `nombre_o_razon_social` | `large_string` |  |
| 49 | `num_anualidades_prev` | `int64` |  |
| 50 | `num_empresas_invitadas` | `double` |  |
| 51 | `num_licitadores` | `double` |  |
| 52 | `numero_de_lotes` | `int64` |  |
| 53 | `numero_de_registro` | `int64` |  |
| 54 | `numero_de_unidades` | `double` |  |
| 55 | `objeto` | `large_string` |  |
| 56 | `plazo_de_ejecucion` | `int64` |  |
| 57 | `plazo_max_contrato` | `double` |  |
| 58 | `procedimiento` | `large_string` |  |
| 59 | `pyme` | `large_string` |  |
| 60 | `revision_de_precios` | `large_string` |  |
| 61 | `sistema_de_precios` | `large_string` |  |
| 62 | `tiene_incidencias_ide` | `large_string` |  |
| 63 | `tiene_incidencias_mpi` | `large_string` |  |
| 64 | `tipo` | `large_string` |  |
| 65 | `tipo_de_conclusion` | `large_string` |  |
| 66 | `tipo_tramitacion` | `large_string` |  |
| 67 | `unidad` | `large_string` |  |
| 68 | `unidades_plazo_ejecucion` | `large_string` |  |
| 69 | `unidades_plazo_max` | `large_string` |  |
| 70 | `url_licitacion` | `large_string` |  |
| 71 | `ute` | `large_string` |  |
| 72 | `valor_estimado_sin_iva` | `double` |  |
| 73 | `motivo_necesidad_id` | `int64` |  |
| 74 | `motivo_necesidad` | `large_string` |  |
| 75 | `fecha_emergencia` | `large_string` |  |
| 76 | `fecha_ini_ejecucion` | `large_string` |  |
| 77 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 78 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 79 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2013.parquet`

5 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `string` |  |
| 23 | `IMPORTE_CONV` | `int64` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `string` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `string` |  |
| 66 | `FECHA_FIRMA_PARTE` | `string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2014.parquet`

2.256 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2015.parquet`

939 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2017.parquet`

1.402 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `double` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2018.parquet`

1.379 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `large_string` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `large_string` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `string` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `double` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `large_string` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `large_string` |  |
| 48 | `COD_EJERCICIO_PARTE` | `large_string` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `large_string` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `large_string` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2019.parquet`

1.758 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2020.parquet`

1.752 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `double` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2021.parquet`

1.644 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `int64` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `double` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `convenios/Convenios_subscritos_por_la_GVA_2022.parquet`

1.500 filas · 70 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJERCICIO_CONV` | `int64` |  |
| 2 | `COD_CONVENIO` | `int64` |  |
| 3 | `COD_TIPO_ENTIDAD_CONV` | `int64` |  |
| 4 | `DESC_TIPO_ENTIDAD_C_CONV` | `large_string` |  |
| 5 | `DESC_TIPO_ENTIDAD_V_CONV` | `large_string` |  |
| 6 | `COD_ORGANO_CONV` | `double` |  |
| 7 | `DESC_ORGANO_C_CONV` | `large_string` |  |
| 8 | `DESC_ORGANO_V_CONV` | `large_string` |  |
| 9 | `INDICADOR_ACTIVO_CONV` | `large_string` |  |
| 10 | `DESC_CONVENIO_C` | `large_string` |  |
| 11 | `DESC_CONVENIO_V` | `large_string` |  |
| 12 | `FECHA_FIRMA_CONV` | `large_string` |  |
| 13 | `DESC_VIGENCIA_PLAZO_C_CONV` | `large_string` |  |
| 14 | `DESC_VIGENCIA_PLAZO_V_CONV` | `large_string` |  |
| 15 | `FECHA_FIN_VIGENCIA_MAX_CONV` | `large_string` |  |
| 16 | `FECHA_CIERRE_CONV` | `large_string` |  |
| 17 | `DESC_MOTIVOS_CIERRE_C_CONV` | `large_string` |  |
| 18 | `DESC_MOTIVOS_CIERRE_V_CONV` | `large_string` |  |
| 19 | `DESC_OBLIGACIONES_C_CONV` | `large_string` |  |
| 20 | `DESC_OBLIGACIONES_V_CONV` | `large_string` |  |
| 21 | `DESC_OBLIGACIONES_ECONOMICAS_C_CONV` | `large_string` |  |
| 22 | `DESC_OBLIGACIONES_ECONOMICAS_V_CONV` | `large_string` |  |
| 23 | `IMPORTE_CONV` | `double` |  |
| 24 | `DESC_MODIFICACIONES_C_CONV` | `large_string` |  |
| 25 | `DESC_MODIFICACIONES_V_CONV` | `large_string` |  |
| 26 | `DESC_NOMBRE_FICHERO_C_CONV` | `large_string` |  |
| 27 | `DESC_NOMBRE_FICHERO_V_CONV` | `large_string` |  |
| 28 | `DESC_FICHERO_C_CONV` | `large_string` |  |
| 29 | `DESC_FICHERO_V_CONV` | `large_string` |  |
| 30 | `COD_ADENDA_CONV` | `int64` |  |
| 31 | `DESC_ADENDA_C_CONV` | `large_string` |  |
| 32 | `DESC_ADENDA_V_CONV` | `large_string` |  |
| 33 | `COD_ENMARCADO_CONV` | `int64` |  |
| 34 | `DESC_ENMARCADO_C_CONV` | `large_string` |  |
| 35 | `DESC_ENMARCADO_V_DESC` | `large_string` |  |
| 36 | `FECHA_PUB_OFICIAL_CONV` | `large_string` |  |
| 37 | `DESC_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 38 | `DESC_URL_PUBLICACION_OFICIAL_CONV` | `large_string` |  |
| 39 | `DESC_ENTIDAD_EXTERNA_C_CONV` | `large_string` |  |
| 40 | `DESC_ENTIDAD_EXTERNA_V_CONV` | `large_string` |  |
| 41 | `COD_TIPODOCV_CONV` | `int64` |  |
| 42 | `DESC_TIPODOCV_C_CONV` | `large_string` |  |
| 43 | `DESC_TIPODOCV_V_CONV` | `large_string` |  |
| 44 | `NUMERO_REGISTRO_CONV` | `large_string` |  |
| 45 | `DESC_MATERIAS_C_CONV` | `large_string` |  |
| 46 | `DESC_MATERIAS_V_CONV` | `large_string` |  |
| 47 | `COD_PARTE` | `int64` |  |
| 48 | `COD_EJERCICIO_PARTE` | `double` |  |
| 49 | `COD_TIPO_ENTIDAD_PARTE` | `int64` |  |
| 50 | `DESC_TIPO_ENTIDAD_C_PARTE` | `large_string` |  |
| 51 | `DESC_TIPO_ENTIDAD_V_PARTE` | `large_string` |  |
| 52 | `COD_ORGANO_PARTE` | `int64` |  |
| 53 | `DESC_ORGANO_C_PARTE` | `large_string` |  |
| 54 | `DESC_ORGANO_V_PARTE` | `large_string` |  |
| 55 | `DESC_INDICADOR_ACTIVO_PARTE` | `large_string` |  |
| 56 | `COD_ENTIDAD_EXTERNA_PARTE` | `int64` |  |
| 57 | `DESC_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 58 | `DESC_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 59 | `DESC_TIPO_ENTIDAD_EXTERNA_C_PARTE` | `large_string` |  |
| 60 | `DESC_TIPO_ENTIDAD_EXTERNA_V_PARTE` | `large_string` |  |
| 61 | `DESC_OBLIGACIONES_C_PARTE` | `large_string` |  |
| 62 | `DESC_OBLIGACIONES_V_PARTE` | `large_string` |  |
| 63 | `DESC_OBLIGACIONES_ECONOMICAS_C_PARTE` | `large_string` |  |
| 64 | `DESC_OBLIGACIONES_ECONOMICAS_V_PARTE` | `large_string` |  |
| 65 | `IMPORTE_PARTE` | `double` |  |
| 66 | `FECHA_FIRMA_PARTE` | `large_string` |  |
| 67 | `DESC_LUGAR_FIRMA_PARTE` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `entidades/Recursos_de_gobierno_abierto_de_las_entidades_locales_de_la_Comunitat_Valenciana.parquet`

536 filas · 17 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `nombre` | `large_string` |  |
| 2 | `nombre_val` | `large_string` |  |
| 3 | `telefono` | `int64` |  |
| 4 | `fax` | `double` |  |
| 5 | `mail` | `large_string` |  |
| 6 | `direccion` | `large_string` |  |
| 7 | `cp` | `large_string` |  |
| 8 | `codigo_ine` | `large_string` |  |
| 9 | `url_portal_transparencia` | `large_string` |  |
| 10 | `url_portal_open_data` | `large_string` |  |
| 11 | `url_derechos_acceso` | `large_string` |  |
| 12 | `url_codigo_buen_gobierno` | `large_string` |  |
| 13 | `url_presupuestos_participativos` | `large_string` |  |
| 14 | `xarxa_governança_cv` | `large_string` |  |
| 15 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/ACTIVIDADES_DE_INFLUENCIA.parquet`

2.499 filas · 8 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id_reunion` | `int64` |  |
| 2 | `tipo` | `large_string` |  |
| 3 | `asuntos_tratados` | `large_string` |  |
| 4 | `fecha` | `large_string` |  |
| 5 | `huella` | `large_string` |  |
| 6 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 7 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 8 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_Grupos_de_Interes_asistentes.parquet`

2.655 filas · 7 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id_reunion` | `int64` |  |
| 2 | `nombre_gi` | `large_string` |  |
| 3 | `num_doc_identidad` | `large_string` |  |
| 4 | `num_regia` | `int64` |  |
| 5 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 6 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 7 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_altos_cargos_asistentes.parquet`

3.718 filas · 5 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id_reunion` | `int64` |  |
| 2 | `cargo` | `large_string` |  |
| 3 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 4 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 5 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_funcionarios_asistentes.parquet`

492 filas · 5 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id_reunion` | `int64` |  |
| 2 | `cargo_funcionario` | `large_string` |  |
| 3 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 4 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 5 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/Grupos_de_interés.parquet`

2.182 filas · 43 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `nombre_gi` | `large_string` |  |
| 2 | ` num_regia` | `int64` |  |
| 3 | ` nif` | `large_string` |  |
| 4 | ` fecha` | `large_string` |  |
| 5 | ` categoria` | `large_string` |  |
| 6 | ` subcategoria` | `large_string` |  |
| 7 | ` direccion` | `large_string` |  |
| 8 | ` email` | `large_string` |  |
| 9 | ` tipo_organizacion` | `large_string` |  |
| 10 | ` ambito_act1` | `large_string` |  |
| 11 | ` ambito_act2` | `large_string` |  |
| 12 | ` ambito_act3` | `large_string` |  |
| 13 | ` ambito_act4` | `large_string` |  |
| 14 | ` ambito_int1` | `large_string` |  |
| 15 | ` ambito_int2` | `large_string` |  |
| 16 | `ambito_int3` | `large_string` |  |
| 17 | ` ambito_int4` | `large_string` |  |
| 18 | ` ambito_int5` | `large_string` |  |
| 19 | ` ambito_int6` | `large_string` |  |
| 20 | ` ambito_int7` | `large_string` |  |
| 21 | `ambito_int8` | `large_string` |  |
| 22 | ` ambito_int9` | `large_string` |  |
| 23 | ` ambito_int10` | `large_string` |  |
| 24 | ` ambito_int11` | `large_string` |  |
| 25 | ` ambito_int12` | `large_string` |  |
| 26 | ` ambito_int13` | `large_string` |  |
| 27 | ` ambito_int14` | `large_string` |  |
| 28 | ` ambito_int15` | `large_string` |  |
| 29 | `ambito_int16` | `large_string` |  |
| 30 | ` ambito_int17` | `large_string` |  |
| 31 | ` ambito_int18` | `large_string` |  |
| 32 | ` ambito_int19` | `large_string` |  |
| 33 | ` ambito_int20` | `large_string` |  |
| 34 | ` ambito_int21` | `large_string` |  |
| 35 | ` ambito_int22` | `large_string` |  |
| 36 | ` ambito_int23` | `large_string` |  |
| 37 | ` ambito_int24` | `large_string` |  |
| 38 | ` ambito_int25` | `large_string` |  |
| 39 | ` ambito_int26` | `large_string` |  |
| 40 | ` ambito_int27` | `large_string` |  |
| 41 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 43 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/Grupos_de_interés_-_detalle_de_aportaciones.parquet`

1.589 filas · 7 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `nr_num_regia` | `int64` |  |
| 2 | `  in_tipo_aportacion` | `large_string` |  |
| 3 | `  ds_procedencia` | `large_string` |  |
| 4 | `  nr_importe_aportacion` | `double` |  |
| 5 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 6 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 7 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `lobbies/Grupos_de_interés_-_detalle_de_clientes.parquet`

238 filas · 5 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `cd_num_regia` | `int64` |  |
| 2 | `ds_nombre_cliente` | `large_string` |  |
| 3 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 4 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 5 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (436, 436 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/contratacion/Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_DANA.parquet` | 792 | 76 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-01.parquet` | 18.377 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-02.parquet` | 18.509 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-03.parquet` | 19.856 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-04.parquet` | 18.740 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-05.parquet` | 20.062 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-06.parquet` | 21.162 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-07.parquet` | 21.056 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-08.parquet` | 16.778 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-09.parquet` | 20.940 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-10.parquet` | 20.668 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-11.parquet` | 18.689 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2022-12.parquet` | 15.807 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-01.parquet` | 16.807 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-02.parquet` | 16.272 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-03.parquet` | 17.854 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-04.parquet` | 16.285 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-05.parquet` | 17.769 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-06.parquet` | 18.784 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-07.parquet` | 18.815 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-08.parquet` | 14.914 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-09.parquet` | 18.719 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-10.parquet` | 19.471 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-11.parquet` | 17.907 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2023-12.parquet` | 14.837 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-01.parquet` | 16.822 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-02.parquet` | 16.603 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-03.parquet` | 15.997 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-04.parquet` | 17.014 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-05.parquet` | 17.318 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-06.parquet` | 17.951 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-07.parquet` | 19.414 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-08.parquet` | 13.959 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-09.parquet` | 18.709 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-10.parquet` | 19.670 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-11.parquet` | 17.327 | 14 |
| `v2026.02/empleo/Contratos_por_Municipio-Sector-Grupo_Ocupacion-Genero-Edad_2024-12.parquet` | 15.495 | 14 |
| `v2026.02/empleo/Datos_ERE_y_ERTE_solicitados_y_resueltos_en_la_Comunitat_Valenciana_2024-12-31.parquet` | 116.979 | 43 |
| `v2026.02/empleo/Datos_ERE_y_ERTE_solicitados_y_resueltos_en_la_Comunitat_Valenciana_2025-12-28.parquet` | 118.055 | 43 |
| `v2026.02/empleo/Datos_de_ERTE_RESUELTOS_en_la_Comunitat_Valenciana_por_DANA_desde_el_29-10-2024.parquet` | 356 | 8 |
| `v2026.02/empleo/Descarga_recurso_en_formato_CSV.parquet` | 3 | 102 |
| `v2026.02/empleo/Expedientes_ERTE_en_la_Comunitat_Valenciana_por_DANA.parquet` | 3.662 | 41 |
| `v2026.02/empleo/Poblaciones_de_Centros_de_Trabajo_ERTE_en_la_Comunitat_Valenciana_por_DANA.parquet` | 3.989 | 18 |
| `v2026.02/entidades/Asociaciones_de_la_Comunitat_Valenciana.parquet` | 93.238 | 17 |
| `v2026.02/entidades/Recursos_de_gobierno_abierto_de_las_entidades_locales_de_la_Comunitat_Valenciana.parquet` | 536 | 14 |
| `v2026.02/lobbies/ACTIVIDADES_DE_INFLUENCIA.parquet` | 1.771 | 5 |
| `v2026.02/lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_Grupos_de_Interes_asistentes.parquet` | 1.890 | 4 |
| `v2026.02/lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_altos_cargos_asistentes.parquet` | 2.902 | 2 |
| `v2026.02/lobbies/ACTIVIDADES_DE_INFLUENCIA_-_detalle_de_funcionarios_asistentes.parquet` | 388 | 2 |
| `v2026.02/lobbies/Grupos_de_interés.parquet` | 1.946 | 40 |
| `v2026.02/lobbies/Grupos_de_interés_-_detalle_de_aportaciones.parquet` | 1.655 | 4 |
| `v2026.02/lobbies/Grupos_de_interés_-_detalle_de_clientes.parquet` | 254 | 2 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación.parquet` | 46.962 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2020.parquet` | 21.743 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-01.parquet` | 4.466 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-02.parquet` | 4.482 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-03.parquet` | 4.482 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-04.parquet` | 4.492 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-05.parquet` | 4.516 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-06.parquet` | 4.531 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-07.parquet` | 4.512 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-08.parquet` | 4.526 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-09.parquet` | 4.519 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-10.parquet` | 4.493 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-11.parquet` | 4.474 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2021-12.parquet` | 4.416 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-01.parquet` | 4.364 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-02.parquet` | 4.369 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-03.parquet` | 4.301 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-04.parquet` | 4.321 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-05.parquet` | 4.376 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-06.parquet` | 4.402 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-07.parquet` | 4.403 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-08.parquet` | 4.406 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-09.parquet` | 4.379 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-10.parquet` | 4.365 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-11.parquet` | 4.364 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2022-12.parquet` | 4.380 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-01.parquet` | 4.407 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-03.parquet` | 4.402 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-04.parquet` | 4.421 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-05.parquet` | 4.456 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-06.parquet` | 4.450 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-07.parquet` | 4.437 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-08.parquet` | 4.421 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-09.parquet` | 4.411 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-10.parquet` | 4.389 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-11.parquet` | 4.391 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2023-12.parquet` | 4.383 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-01.parquet` | 4.388 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-02.parquet` | 4.353 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-03.parquet` | 4.342 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-04.parquet` | 4.379 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-05.parquet` | 4.401 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-07.parquet` | 4.385 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-08.parquet` | 4.403 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-09.parquet` | 4.401 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-10.parquet` | 4.402 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_grupo_de_ocupación_2024-12.parquet` | 4.376 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género.parquet` | 5.937 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2020.parquet` | 6.499 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-01.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-02.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-03.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-04.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-05.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-06.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-07.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-08.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-09.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-10.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-11.parquet` | 543 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2021-12.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-01.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-02.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-03.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-04.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-05.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-06.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-07.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-08.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-09.parquet` | 538 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-10.parquet` | 539 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-11.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2022-12.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-01.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-03.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-04.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-05.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-06.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-07.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-08.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-09.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-10.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-11.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2023-12.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-01.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-02.parquet` | 539 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-03.parquet` | 540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-04.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-05.parquet` | 541 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-07.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-08.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-09.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-10.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_2024-12.parquet` | 542 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad.parquet` | 53.491 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2020.parquet` | 261.541 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-01.parquet` | 22.213 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-02.parquet` | 22.322 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-03.parquet` | 22.313 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-04.parquet` | 22.462 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-05.parquet` | 22.657 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-06.parquet` | 22.821 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-07.parquet` | 22.725 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-08.parquet` | 22.829 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-09.parquet` | 22.758 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-10.parquet` | 22.510 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-11.parquet` | 22.087 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2021-12.parquet` | 21.630 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-01.parquet` | 20.817 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-02.parquet` | 20.844 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-03.parquet` | 20.133 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-04.parquet` | 20.287 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-05.parquet` | 20.619 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-06.parquet` | 20.753 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-07.parquet` | 20.802 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-08.parquet` | 20.807 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-09.parquet` | 20.812 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-10.parquet` | 20.597 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-11.parquet` | 20.533 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2022-12.parquet` | 20.676 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-01.parquet` | 20.920 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-03.parquet` | 20.910 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-04.parquet` | 20.962 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-05.parquet` | 21.273 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-06.parquet` | 21.333 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-07.parquet` | 21.229 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-08.parquet` | 21.285 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-09.parquet` | 21.210 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-10.parquet` | 20.860 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-11.parquet` | 20.844 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2023-12.parquet` | 20.877 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-01.parquet` | 21.018 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-02.parquet` | 20.924 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-03.parquet` | 20.827 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-04.parquet` | 20.959 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-05.parquet` | 21.269 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-07.parquet` | 21.239 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-08.parquet` | 21.373 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-09.parquet` | 21.439 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-10.parquet` | 21.260 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_género_y_edad_2024-12.parquet` | 21.115 | 11 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo.parquet` | 63.749 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2020.parquet` | 30.335 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-01.parquet` | 6.624 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-02.parquet` | 6.615 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-03.parquet` | 6.610 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-04.parquet` | 6.616 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-05.parquet` | 6.633 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-06.parquet` | 6.644 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-07.parquet` | 6.695 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-08.parquet` | 6.745 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-09.parquet` | 6.645 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-10.parquet` | 6.586 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-11.parquet` | 6.497 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2021-12.parquet` | 6.384 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-01.parquet` | 6.116 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-02.parquet` | 6.135 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-03.parquet` | 5.835 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-04.parquet` | 5.848 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-05.parquet` | 5.904 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-06.parquet` | 5.962 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-07.parquet` | 6.067 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-08.parquet` | 6.111 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-09.parquet` | 6.015 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-10.parquet` | 5.957 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-11.parquet` | 5.961 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2022-12.parquet` | 5.975 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-01.parquet` | 6.024 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-03.parquet` | 5.979 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-04.parquet` | 5.964 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-05.parquet` | 6.016 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-06.parquet` | 6.094 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-07.parquet` | 6.195 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-08.parquet` | 6.247 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-09.parquet` | 6.131 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-10.parquet` | 6.025 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-11.parquet` | 6.019 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2023-12.parquet` | 6.032 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-01.parquet` | 6.049 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-02.parquet` | 5.944 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-03.parquet` | 5.896 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-04.parquet` | 5.905 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-05.parquet` | 5.974 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-07.parquet` | 6.116 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-08.parquet` | 6.158 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-09.parquet` | 6.048 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-10.parquet` | 6.029 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_nivel_formativo_2024-12.parquet` | 6.040 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica.parquet` | 86.262 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2020.parquet` | 40.515 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-01.parquet` | 8.540 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-02.parquet` | 8.551 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-03.parquet` | 8.549 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-04.parquet` | 8.587 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-05.parquet` | 8.586 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-06.parquet` | 8.565 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-07.parquet` | 8.558 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-08.parquet` | 8.590 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-09.parquet` | 8.567 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-10.parquet` | 8.483 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-11.parquet` | 8.413 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2021-12.parquet` | 8.289 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-01.parquet` | 8.069 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-02.parquet` | 8.051 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-03.parquet` | 7.861 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-04.parquet` | 7.873 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-05.parquet` | 7.920 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-06.parquet` | 7.974 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-07.parquet` | 7.977 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-08.parquet` | 8.003 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-09.parquet` | 8.008 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-10.parquet` | 7.976 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-11.parquet` | 7.955 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2022-12.parquet` | 7.961 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-01.parquet` | 8.011 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-03.parquet` | 7.982 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-04.parquet` | 7.986 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-05.parquet` | 8.017 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-06.parquet` | 8.045 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-07.parquet` | 8.067 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-08.parquet` | 8.084 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-09.parquet` | 8.067 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-10.parquet` | 7.966 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-11.parquet` | 8.008 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2023-12.parquet` | 8.009 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-01.parquet` | 8.055 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-02.parquet` | 8.032 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-03.parquet` | 8.007 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-04.parquet` | 8.029 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-05.parquet` | 8.056 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-07.parquet` | 8.096 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-08.parquet` | 8.111 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-09.parquet` | 8.125 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-10.parquet` | 8.137 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sección_de_la_actividad_económica_2024-12.parquet` | 8.096 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica.parquet` | 26.281 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2020.parquet` | 12.000 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-01.parquet` | 2.490 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-02.parquet` | 2.491 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-03.parquet` | 2.490 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-04.parquet` | 2.498 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-05.parquet` | 2.504 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-06.parquet` | 2.509 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-07.parquet` | 2.493 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-08.parquet` | 2.497 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-09.parquet` | 2.500 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-10.parquet` | 2.487 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-11.parquet` | 2.476 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2021-12.parquet` | 2.437 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-01.parquet` | 2.390 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-02.parquet` | 2.383 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-03.parquet` | 2.381 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-04.parquet` | 2.382 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-05.parquet` | 2.394 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-06.parquet` | 2.414 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-07.parquet` | 2.399 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-08.parquet` | 2.402 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-09.parquet` | 2.394 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-10.parquet` | 2.381 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-11.parquet` | 2.375 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2022-12.parquet` | 2.401 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-01.parquet` | 2.413 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-03.parquet` | 2.402 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-04.parquet` | 2.414 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-05.parquet` | 2.424 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-06.parquet` | 2.437 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-07.parquet` | 2.430 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-08.parquet` | 2.410 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-09.parquet` | 2.404 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-10.parquet` | 2.376 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-11.parquet` | 2.394 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2023-12.parquet` | 2.393 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-01.parquet` | 2.399 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-02.parquet` | 2.389 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-03.parquet` | 2.389 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-04.parquet` | 2.396 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-05.parquet` | 2.415 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-07.parquet` | 2.407 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-08.parquet` | 2.406 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-09.parquet` | 2.410 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-10.parquet` | 2.400 | 9 |
| `v2026.02/paro/Demandantes_activos_parados_por_sector_de_la_actividad_económica_2024-12.parquet` | 2.406 | 9 |
| `v2026.02/paro/Descarga_recurso_en_formato_CSV.parquet` | 3 | 102 |
| `v2026.02/patrimonio/Bienes_de_interés_cultural.parquet` | 1.236 | 10 |
| `v2026.02/patrimonio/Bienes_de_relevancia_local.parquet` | 5.347 | 10 |
| `v2026.02/patrimonio/Listado_de_bienes_inmuebles_de_la_Generalitat_Valenciana.parquet` | 2.700 | 28 |
| `v2026.02/presupuestos/Ejecución_Presupuestaria_de_GASTOS_de_la_Generalitat_2024.parquet` | 165.140 | 43 |
| `v2026.02/presupuestos/Ejecución_Presupuestaria_de_GASTOS_de_la_Generalitat_2025.parquet` | 167.978 | 43 |
| `v2026.02/presupuestos/Ejecución_Presupuestaria_de_INGRESOS_de_la_Generalitat_2024.parquet` | 6.553 | 24 |
| `v2026.02/presupuestos/Ejecución_Presupuestaria_de_INGRESOS_de_la_Generalitat_2025.parquet` | 6.321 | 24 |
| `v2026.02/sanidad/Centros_Sanitarios_2020.parquet` | 14.553 | 14 |
| `v2026.02/sanidad/Información_del_mapa_sanitario_correspondiente_a_la_Comunitat_Valenciana.parquet` | 168.830 | 1 |
| `v2026.02/sanidad/TAE_mortalidad_cardiopatía_isquémica_en_la_Comunitat_Valenciana.parquet` | 207 | 30 |
| `v2026.02/sanidad/TAE_mortalidad_cáncer_en_la_Comunitat_Valenciana.parquet` | 207 | 30 |
| `v2026.02/sanidad/TAE_mortalidad_enfermedad_cerebrovascular_en_la_Comunitat_Valenciana.parquet` | 207 | 30 |
| `v2026.02/sanidad/TAE_mortalidad_general_en_la_Comunitat_Valenciana.parquet` | 207 | 30 |
| `v2026.02/sanidad/TAE_mortalidad_suicidio_en_la_Comunitat_Valenciana.parquet` | 207 | 30 |
| `v2026.02/sanidad/Tasa_bruta_de_mortalidad_en_la_Comunitat_Valenciana.parquet` | 4.290 | 7 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2015.parquet` | 45.550 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2016.parquet` | 48.878 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2017.parquet` | 53.063 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2018.parquet` | 56.303 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2019.parquet` | 61.280 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2020.parquet` | 51.765 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2021.parquet` | 60.054 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2022.parquet` | 63.963 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2023.parquet` | 64.075 | 11 |
| `v2026.02/siniestralidad/Datos_de_accidentes_de_trabajo_en_la_Comunidad_Valenciana_2024.parquet` | 65.246 | 11 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-01.parquet` | 32.275 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-02.parquet` | 51.723 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-03.parquet` | 115.148 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-04.parquet` | 91.618 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-05.parquet` | 38.147 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-06.parquet` | 9.816 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-07.parquet` | 10.409 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-08.parquet` | 53.736 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-09.parquet` | 16.966 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-10.parquet` | 16.871 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-11.parquet` | 21.471 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2022-12.parquet` | 37.505 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-01.parquet` | 4.045 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-02.parquet` | 9.597 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-03.parquet` | 25.444 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-04.parquet` | 8.467 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-05.parquet` | 16.064 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-06.parquet` | 96.166 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-07.parquet` | 11.437 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-08.parquet` | 13.970 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-09.parquet` | 20.611 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-10.parquet` | 26.161 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-11.parquet` | 15.134 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2023-12.parquet` | 22.675 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-01.parquet` | 3.761 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-02.parquet` | 40.649 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-03.parquet` | 97.332 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-04.parquet` | 131.984 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-05.parquet` | 77.950 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-06.parquet` | 20.116 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-07.parquet` | 5.109 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-08.parquet` | 7.488 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-09.parquet` | 26.271 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-10.parquet` | 13.026 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-11.parquet` | 41.474 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2024-12.parquet` | 55.966 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-01.parquet` | 54.647 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-02.parquet` | 74.172 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-03.parquet` | 74.935 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-04.parquet` | 73.733 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-05.parquet` | 32.879 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-06.parquet` | 12.037 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-07.parquet` | 29.637 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-08.parquet` | 18.027 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-09.parquet` | 31.433 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-10.parquet` | 48.137 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-11.parquet` | 10.541 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_concedidas_por_la_Generalitat_Valenciana_2025-12.parquet` | 43.293 | 82 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_de_la_Generalitat_relacionadas_con_la_DANA.parquet` | 344.488 | 15 |
| `v2026.02/subvenciones/Ayudas_y_subvenciones_de_la_Generalitat_relacionadas_con_la_DANA_(no_incluye_tipo_beneficiario_'familias').parquet` | 56.280 | 16 |
| `v2026.02/subvenciones/Información_de_solicitudes_por_municipio.parquet` | 448 | 5 |
| `v2026.02/subvenciones/Periodo_medio_de_pago_de_ayudas_y_subvenciones_desde_2024.parquet` | 66 | 12 |
| `v2026.02/territorio/Centros_docentes_de_la_Comunitat_Valenciana.parquet` | 3.681 | 23 |
| `v2026.02/transporte/agency.parquet` | 51 | 5 |
| `v2026.02/transporte/calendar_dates.parquet` | 7.063 | 3 |
| `v2026.02/transporte/routes.parquet` | 356 | 5 |
| `v2026.02/transporte/shapes.parquet` | 785.346 | 5 |
| `v2026.02/transporte/stop_times.parquet` | 185.428 | 5 |
| `v2026.02/transporte/stops.parquet` | 5.133 | 4 |
| `v2026.02/transporte/trips.parquet` | 9.200 | 4 |
| `v2026.02/turismo/Datos_de_Turismo_Activo.parquet` | 534 | 16 |
| `v2026.02/turismo/Lista_de_Acampadas_en_Finca_Particular.parquet` | 11 | 17 |
| `v2026.02/turismo/Lista_de_Agencias_de_viajes.parquet` | 1.345 | 24 |
| `v2026.02/turismo/Lista_de_Albergues.parquet` | 106 | 22 |
| `v2026.02/turismo/Lista_de_Alojamientos_Rurales_(Histórico).parquet` | 1.853 | 30 |
| `v2026.02/turismo/Lista_de_Alojamientos_Rurales_Último_Período.parquet` | 1.012 | 30 |
| `v2026.02/turismo/Lista_de_Campings.parquet` | 159 | 26 |
| `v2026.02/turismo/Lista_de_Campings_(Histórico).parquet` | 232 | 32 |
| `v2026.02/turismo/Lista_de_Campings_Último_Período.parquet` | 146 | 32 |
| `v2026.02/turismo/Lista_de_Casas_Rurales.parquet` | 1.090 | 22 |
| `v2026.02/turismo/Lista_de_Hoteles_(Histórico).parquet` | 2.458 | 32 |
| `v2026.02/turismo/Lista_de_Hoteles_Último_Período.parquet` | 1.591 | 32 |
| `v2026.02/turismo/Lista_de_Viviendas_de_uso_turístico_(Histórico).parquet` | 182.893 | 33 |
| `v2026.02/turismo/Lista_de_Viviendas_de_uso_turístico_Último_Período.parquet` | 101.408 | 33 |
| `v2026.02/turismo/Lista_de_viviendas_turísticas.parquet` | 88.451 | 23 |
| `v2026.02/turismo/Lista_de_áreas_de_pernocta.parquet` | 11 | 22 |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
