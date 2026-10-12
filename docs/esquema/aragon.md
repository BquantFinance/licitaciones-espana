# `aragon.zip`

Esquema de los Parquet de `aragon.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/aragon.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `anuncios_perfil__anuncios_perfil_contratante_aragon.parquet` | 12.156 | 31 |
| `anuncios_perfil__previsualizacion_ultimos_anuncios_perfil_contratante_aragon.parquet` | 10 | 31 |
| `contratos_gobierno__contratos_gobierno_de_aragon.parquet` | 117.770 | 42 |
| `contratos_gobierno__contratos_menores_gobierno_de_aragon.parquet` | 90.804 | 38 |
| `contratos_gobierno__encargos_de_ejecucion_a_medios_propios_desde.parquet` | 2.093 | 26 |
| `registro_contratos__contratos_mayores.parquet` | 8.440 | 67 |
| `registro_contratos__contratos_menores.parquet` | 13.656 | 56 |
| `registro_contratos__encargos_a_medios_propios.parquet` | 397 | 52 |
| `registro_contratos__incidencias.parquet` | 357 | 28 |
| `zaragoza_ocds_awards.parquet` | 3.096 | 25 |
| `zaragoza_ocds_contracts.parquet` | 2.872 | 23 |
| `zaragoza_ocds_parties.parquet` | 12.336 | 13 |
| `zaragoza_ocds_releases.parquet` | 3.305 | 57 |

## `anuncios_perfil__anuncios_perfil_contratante_aragon.parquet`

12.156 filas · 31 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `idanuncio` | `string` |  |
| 2 | `fechapublicacion` | `string` |  |
| 3 | `organismcodegg` | `string` |  |
| 4 | `nombregg` | `string` |  |
| 5 | `idexpediente` | `string` |  |
| 6 | `idexpedienteplacsp` | `string` |  |
| 7 | `dir3` | `string` |  |
| 8 | `organismcodeoc` | `string` |  |
| 9 | `nombreoc` | `string` |  |
| 10 | `ejercicio` | `string` |  |
| 11 | `objetocontrato` | `string` |  |
| 12 | `tipoprocedimiento` | `string` |  |
| 13 | `tipocontrato` | `string` |  |
| 14 | `subtipocontrato` | `string` |  |
| 15 | `importesinimpuestos` | `string` |  |
| 16 | `importeconimpuestos` | `string` |  |
| 17 | `motivorectificacion` | `string` |  |
| 18 | `urlexpediente` | `string` |  |
| 19 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 29 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 30 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 31 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `anuncios_perfil__previsualizacion_ultimos_anuncios_perfil_contratante_aragon.parquet`

10 filas · 31 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `idanuncio` | `string` |  |
| 2 | `fechapublicacion` | `string` |  |
| 3 | `organismcodegg` | `string` |  |
| 4 | `nombregg` | `string` |  |
| 5 | `idexpediente` | `string` |  |
| 6 | `idexpedienteplacsp` | `string` |  |
| 7 | `dir3` | `string` |  |
| 8 | `organismcodeoc` | `string` |  |
| 9 | `nombreoc` | `string` |  |
| 10 | `ejercicio` | `string` |  |
| 11 | `objetocontrato` | `string` |  |
| 12 | `tipoprocedimiento` | `string` |  |
| 13 | `tipocontrato` | `string` |  |
| 14 | `subtipocontrato` | `string` |  |
| 15 | `importesinimpuestos` | `string` |  |
| 16 | `importeconimpuestos` | `string` |  |
| 17 | `motivorectificacion` | `string` |  |
| 18 | `urlexpediente` | `string` |  |
| 19 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 29 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 30 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 31 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_gobierno__contratos_gobierno_de_aragon.parquet`

117.770 filas · 42 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJER` | `string` |  |
| 2 | `ORGA` | `string` |  |
| 3 | `OBJC` | `string` |  |
| 4 | `TIPC` | `string` |  |
| 5 | `PROC` | `string` |  |
| 6 | `IDAD` | `string` |  |
| 7 | `IMPL` | `string` |  |
| 8 | `IMPA` | `string` |  |
| 9 | `INST` | `string` |  |
| 10 | `COEX` | `string` |  |
| 11 | `Unnamed: 10` | `string` |  |
| 12 | `Unnamed: 11` | `string` |  |
| 13 | `Unnamed: 12` | `string` |  |
| 14 | `Unnamed: 13` | `string` |  |
| 15 | `Unnamed: 14` | `string` |  |
| 16 | `Obj` | `string` |  |
| 17 | `Organo` | `string` |  |
| 18 | `Procedimiento de adjudicación` | `string` |  |
| 19 | `Tipo de contrato` | `string` |  |
| 20 | `Importe de licitación` | `string` |  |
| 21 | `Importe de adjudicación` | `string` |  |
| 22 | `Instrumento de publicación` | `string` |  |
| 23 | `Número de licitadores` | `string` |  |
| 24 | `Identidad del adjudicatario` | `string` |  |
| 25 | `Ejercicio` | `string` |  |
| 26 | `Código Expediente` | `string` |  |
| 27 | `M` | `string` |  |
| 28 | `Órgano` | `string` |  |
| 29 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 31 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 32 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 33 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 34 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 35 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 36 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 37 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 38 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 39 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 40 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 41 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_gobierno__contratos_menores_gobierno_de_aragon.parquet`

90.804 filas · 38 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `EJER` | `string` |  |
| 2 | `ORGA` | `string` |  |
| 3 | `PROC` | `string` |  |
| 4 | `OBJC` | `string` |  |
| 5 | `IDAD` | `string` |  |
| 6 | `IMPA` | `string` |  |
| 7 | `Unnamed: 6` | `string` |  |
| 8 | `Unnamed: 7` | `string` |  |
| 9 | `Unnamed: 8` | `string` |  |
| 10 | `Unnamed: 9` | `string` |  |
| 11 | `Unnamed: 10` | `string` |  |
| 12 | `Obj` | `string` |  |
| 13 | `Organo` | `string` |  |
| 14 | `Procedimiento de adjudicación` | `string` |  |
| 15 | `Tipo de contrato` | `string` |  |
| 16 | `Importe de licitación` | `string` |  |
| 17 | `Importe de adjudicación` | `string` |  |
| 18 | `Instrumento de publicación` | `string` |  |
| 19 | `Número de licitadores` | `string` |  |
| 20 | `Identidad del adjudicatario` | `string` |  |
| 21 | `Ejercicio` | `string` |  |
| 22 | `Código Expediente` | `string` |  |
| 23 | `M` | `string` |  |
| 24 | `Órgano` | `string` |  |
| 25 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 29 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 31 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 32 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 33 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 34 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 35 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 36 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 37 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_gobierno__encargos_de_ejecucion_a_medios_propios_desde.parquet`

2.093 filas · 26 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Obj` | `string` |  |
| 2 | `Órgano` | `string` |  |
| 3 | `Procedimiento de adjudicación` | `string` |  |
| 4 | `Tipo de contrato` | `string` |  |
| 5 | `Importe de licitación` | `string` |  |
| 6 | `Importe de adjudicación` | `string` |  |
| 7 | `Instrumento de publicación` | `string` |  |
| 8 | `Número de licitadores` | `string` |  |
| 9 | `Identidad del adjudicatario` | `string` |  |
| 10 | `Ejercicio` | `string` |  |
| 11 | `Código Expediente` | `string` |  |
| 12 | `M` | `string` |  |
| 13 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 25 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 26 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos__contratos_mayores.parquet`

8.440 filas · 67 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `anyo` | `string` |  |
| 2 | `trimestre` | `string` |  |
| 3 | `contrato_reservado` | `string` |  |
| 4 | `organo_contratacion` | `string` |  |
| 5 | `nif_organo_contratacion` | `string` |  |
| 6 | `numero_de_expediente` | `string` |  |
| 7 | `modalidad` | `string` |  |
| 8 | `tipo_de_contrato` | `string` |  |
| 9 | `prestacion_principal` | `string` |  |
| 10 | `prestacion_accesoria` | `string` |  |
| 11 | `procedim_adjudicacion` | `string` |  |
| 12 | `tramitacion` | `string` |  |
| 13 | `objeto_del_contrato` | `string` |  |
| 14 | `cpv` | `string` |  |
| 15 | `estado_del_expediente` | `string` |  |
| 16 | `es_un_lote` | `string` |  |
| 17 | `numero_de_lote` | `string` |  |
| 18 | `descripcion_del_lote` | `string` |  |
| 19 | `sujeto_regulacion_armonizada` | `string` |  |
| 20 | `plurianual` | `string` |  |
| 21 | `numero_de_anualidades` | `string` |  |
| 22 | `criterios` | `string` |  |
| 23 | `VALOR_ESTIMADO_(SIN_IVA)` | `string` |  |
| 24 | `PRESUP_BASE_LICIT_(SIN_IVA)` | `string` |  |
| 25 | `PRESUP_BASE_LICIT_(CON_IVA)` | `string` |  |
| 26 | `IMPORTE_ADJUDIC_(SIN_IVA)` | `string` |  |
| 27 | `IMPORTE_ADJUDIC_(CON_IVA)` | `string` |  |
| 28 | `es_precio_unitario` | `string` |  |
| 29 | `canon_o_participacion` | `string` |  |
| 30 | `prevista_revision_precios` | `string` |  |
| 31 | `cofinanciado` | `string` |  |
| 32 | `fecha_de_adjudicacion` | `string` |  |
| 33 | `fecha_de_formalizacion` | `string` |  |
| 34 | `fecha_inicio_ejecucion` | `string` |  |
| 35 | `fecha_prevista_fin_contrato` | `string` |  |
| 36 | `fecha_de_licitacion` | `string` |  |
| 37 | `duracion_tipo` | `string` |  |
| 38 | `duracion_cantidad` | `string` |  |
| 39 | `razon_social_adjudicatario` | `string` |  |
| 40 | `nif_adjudicatario` | `string` |  |
| 41 | `nacionalidad` | `string` |  |
| 42 | `tipo_de_entidad` | `string` |  |
| 43 | `numero_de_invitaciones` | `string` |  |
| 44 | `posibilidad_de_prorroga` | `string` |  |
| 45 | `prorroga_tipo` | `string` |  |
| 46 | `prorroga_cantidad` | `string` |  |
| 47 | `num_ofertas_recibidas` | `string` |  |
| 48 | `num_ofertas_valoradas` | `string` |  |
| 49 | `instrumentos_publicidad` | `string` |  |
| 50 | `subcontrataciones` | `string` |  |
| 51 | `anyo_ejecucion_reserva` | `string` |  |
| 52 | `trim_ejecucion_reserva` | `string` |  |
| 53 | `reserva_importe_ejecutado` | `string` |  |
| 54 | `_razon_social_es_pais` | `bool` | [de la fuente](../fuentes/aragon.md) |
| 55 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 56 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 57 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 58 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 59 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 60 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 61 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 62 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 63 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 64 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 65 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 66 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 67 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos__contratos_menores.parquet`

13.656 filas · 56 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `anyo` | `string` |  |
| 2 | `trimestre` | `string` |  |
| 3 | `contrato_reservado` | `string` |  |
| 4 | `organo_de_contratacion` | `string` |  |
| 5 | `nif_organo_de_contratacion` | `string` |  |
| 6 | `numero_de_expediente` | `string` |  |
| 7 | `modalidad` | `string` |  |
| 8 | `tipo_de_contrato` | `string` |  |
| 9 | `prestacion_principal` | `string` |  |
| 10 | `prestacion_accesoria` | `string` |  |
| 11 | `procedim_de_adjudicacion` | `string` |  |
| 12 | `tramitacion` | `string` |  |
| 13 | `objeto_del_contrato` | `string` |  |
| 14 | `cpv` | `string` |  |
| 15 | `estado_del_expediente` | `string` |  |
| 16 | `criterios` | `string` |  |
| 17 | `VALOR_ESTIMADO_(SIN_IVA)` | `string` |  |
| 18 | `PRESUP_BASE_LICIT_(SIN_IVA)` | `string` |  |
| 19 | `PRESUP_BASE_LICIT_(CON_IVA)` | `string` |  |
| 20 | `IMPORTE_ADJUDIC_(SIN_IVA)` | `string` |  |
| 21 | `IMPORTE_ADJUDIC_(CON_IVA)` | `string` |  |
| 22 | `es_precio_unitario` | `string` |  |
| 23 | `fecha_de_adjudicacion` | `string` |  |
| 24 | `fecha_de_formalizacion` | `string` |  |
| 25 | `fecha_inicio_de_ejecucion` | `string` |  |
| 26 | `fecha_prevista_fin_contrato` | `string` |  |
| 27 | `fecha_de_licitacion` | `string` |  |
| 28 | `duracion_tipo` | `string` |  |
| 29 | `duracion_cantidad` | `string` |  |
| 30 | `razon_social_adjudicatario` | `string` |  |
| 31 | `nif_adjudicatario` | `string` |  |
| 32 | `nacionalidad` | `string` |  |
| 33 | `tipo_de_entidad` | `string` |  |
| 34 | `numero_de_invitaciones` | `string` |  |
| 35 | `numero_de_ofertas_recibidas` | `string` |  |
| 36 | `numero_de_ofertas_valoradas` | `string` |  |
| 37 | `instrumentos_de_publicidad` | `string` |  |
| 38 | `num_licitadores_participantes` | `string` |  |
| 39 | `subcontrataciones` | `string` |  |
| 40 | `anyo_ejecucion_reserva` | `string` |  |
| 41 | `trim_ejecucion_reserva` | `string` |  |
| 42 | `reserva_importe_ejecutado` | `string` |  |
| 43 | `_razon_social_es_pais` | `bool` | [de la fuente](../fuentes/aragon.md) |
| 44 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 45 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 46 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 47 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 48 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 49 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 50 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 51 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 52 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 53 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 54 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 55 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 56 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos__encargos_a_medios_propios.parquet`

397 filas · 52 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `anyo` | `string` |  |
| 2 | `trimestre` | `string` |  |
| 3 | `contrato_reservado` | `string` |  |
| 4 | `organo_de_contratacion` | `string` |  |
| 5 | `nif_organo_de_contratacion` | `string` |  |
| 6 | `numero_de_expediente` | `string` |  |
| 7 | `modalidad` | `string` |  |
| 8 | `tipo_de_contrato` | `string` |  |
| 9 | `prestacion_principal` | `string` |  |
| 10 | `prestacion_accesoria` | `string` |  |
| 11 | `objeto_del_contrato` | `string` |  |
| 12 | `cpv` | `string` |  |
| 13 | `estado_del_expediente` | `string` |  |
| 14 | `plurianual` | `string` |  |
| 15 | `numero_de_anualidades` | `string` |  |
| 16 | `VALOR_ESTIMADO_(SIN_IVA)` | `string` |  |
| 17 | `PRESUP_BASE_LICIT_(SIN_IVA)` | `string` |  |
| 18 | `PRESUP_BASE_LICIT_(CON_IVA)` | `string` |  |
| 19 | `IMPORTE_ADJUDIC_(SIN_IVA)` | `string` |  |
| 20 | `IMPORTE_ADJUDIC_(CON_IVA)` | `string` |  |
| 21 | `es_precio_unitario` | `string` |  |
| 22 | `prevista_revision_de_precios` | `string` |  |
| 23 | `cofinanciado` | `string` |  |
| 24 | `fecha_de_adjudicacion` | `string` |  |
| 25 | `fecha_de_formalizacion` | `string` |  |
| 26 | `fecha_de_inicio_de_ejecucion` | `string` |  |
| 27 | `fecha_prevista_fin_contrato` | `string` |  |
| 28 | `fecha_de_licitacion` | `string` |  |
| 29 | `duracion_tipo` | `string` |  |
| 30 | `duracion_cantidad` | `string` |  |
| 31 | `razon_social_adjudicatario` | `string` |  |
| 32 | `nif_adjudicatario` | `string` |  |
| 33 | `nacionalidad` | `string` |  |
| 34 | `tipo_de_entidad` | `string` |  |
| 35 | `posibilidad_de_prorroga` | `string` |  |
| 36 | `prorroga_tipo` | `string` |  |
| 37 | `prorroga_cantidad` | `string` |  |
| 38 | `subcontrataciones` | `string` |  |
| 39 | `_razon_social_es_pais` | `bool` | [de la fuente](../fuentes/aragon.md) |
| 40 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 41 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 42 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 43 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 44 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 45 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 46 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 47 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 48 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 49 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 50 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 51 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 52 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos__incidencias.parquet`

357 filas · 28 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `numero_de_expediente` | `string` |  |
| 2 | `tipo_de_incidencia` | `string` |  |
| 3 | `subtipo_de_incidencia` | `string` |  |
| 4 | `fecha_incidencia` | `string` |  |
| 5 | `anyo` | `string` |  |
| 6 | `trimestre` | `string` |  |
| 7 | `duracion_anyos` | `string` |  |
| 8 | `duracion_meses` | `string` |  |
| 9 | `duracion_dias` | `string` |  |
| 10 | `razon_social_cesionario` | `string` |  |
| 11 | `nif_cesionario` | `string` |  |
| 12 | `IMPORTE_(SIN_IVA)` | `string` |  |
| 13 | `IMPORTE_(CON_IVA)` | `string` |  |
| 14 | `motivacion` | `string` |  |
| 15 | `modificacion_prevista` | `string` |  |
| 16 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_recurso_id` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_anio_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_fila_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 26 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 27 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 28 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `zaragoza_ocds_awards.parquet`

3.096 filas · 25 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ocid` | `string` |  |
| 2 | `release_id` | `string` |  |
| 3 | `id` | `string` |  |
| 4 | `date` | `string` |  |
| 5 | `items` | `string` |  |
| 6 | `title` | `string` |  |
| 7 | `value/amount` | `string` |  |
| 8 | `value/currency` | `string` |  |
| 9 | `status` | `string` |  |
| 10 | `documents` | `string` |  |
| 11 | `suppliers` | `string` |  |
| 12 | `description` | `string` |  |
| 13 | `contractPeriod/endDate` | `string` |  |
| 14 | `contractPeriod/startDate` | `string` |  |
| 15 | `contractPeriod/maxExtentDate` | `string` |  |
| 16 | `contractPeriod/durationInDays` | `string` |  |
| 17 | `relatedLots` | `string` |  |
| 18 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_linea` | `string` | [de la fuente](../fuentes/aragon.md) |
| 22 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 25 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `zaragoza_ocds_contracts.parquet`

2.872 filas · 23 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ocid` | `string` |  |
| 2 | `release_id` | `string` |  |
| 3 | `id` | `string` |  |
| 4 | `items` | `string` |  |
| 5 | `title` | `string` |  |
| 6 | `value/amount` | `string` |  |
| 7 | `value/currency` | `string` |  |
| 8 | `period/endDate` | `string` |  |
| 9 | `period/startDate` | `string` |  |
| 10 | `period/maxExtentDate` | `string` |  |
| 11 | `period/durationInDays` | `string` |  |
| 12 | `status` | `string` |  |
| 13 | `awardID` | `string` |  |
| 14 | `dateSigned` | `string` |  |
| 15 | `description` | `string` |  |
| 16 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_linea` | `string` | [de la fuente](../fuentes/aragon.md) |
| 20 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 22 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `zaragoza_ocds_parties.parquet`

12.336 filas · 13 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ocid` | `string` |  |
| 2 | `release_id` | `string` |  |
| 3 | `id` | `string` |  |
| 4 | `name` | `string` |  |
| 5 | `roles` | `string` |  |
| 6 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 7 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 8 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_linea` | `string` | [de la fuente](../fuentes/aragon.md) |
| 10 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 11 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `zaragoza_ocds_releases.parquet`

3.305 filas · 57 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `string` |  |
| 2 | `tag` | `string` |  |
| 3 | `date` | `string` |  |
| 4 | `ocid` | `string` |  |
| 5 | `tender/id` | `string` |  |
| 6 | `tender/items` | `string` |  |
| 7 | `tender/title` | `string` |  |
| 8 | `tender/value/amount` | `string` |  |
| 9 | `tender/value/currency` | `string` |  |
| 10 | `tender/status` | `string` |  |
| 11 | `tender/minValue/amount` | `string` |  |
| 12 | `tender/minValue/currency` | `string` |  |
| 13 | `tender/documents` | `string` |  |
| 14 | `tender/tenderers` | `string` |  |
| 15 | `tender/awardPeriod/endDate` | `string` |  |
| 16 | `tender/awardPeriod/startDate` | `string` |  |
| 17 | `tender/awardPeriod/maxExtentDate` | `string` |  |
| 18 | `tender/awardPeriod/durationInDays` | `string` |  |
| 19 | `tender/description` | `string` |  |
| 20 | `tender/hasEnquiries` | `string` |  |
| 21 | `tender/tenderPeriod/endDate` | `string` |  |
| 22 | `tender/tenderPeriod/startDate` | `string` |  |
| 23 | `tender/tenderPeriod/maxExtentDate` | `string` |  |
| 24 | `tender/tenderPeriod/durationInDays` | `string` |  |
| 25 | `tender/awardCriteria` | `string` |  |
| 26 | `tender/enquiryPeriod/endDate` | `string` |  |
| 27 | `tender/enquiryPeriod/startDate` | `string` |  |
| 28 | `tender/enquiryPeriod/maxExtentDate` | `string` |  |
| 29 | `tender/enquiryPeriod/durationInDays` | `string` |  |
| 30 | `tender/contractPeriod/endDate` | `string` |  |
| 31 | `tender/contractPeriod/startDate` | `string` |  |
| 32 | `tender/contractPeriod/maxExtentDate` | `string` |  |
| 33 | `tender/contractPeriod/durationInDays` | `string` |  |
| 34 | `tender/procuringEntity/id` | `string` |  |
| 35 | `tender/procuringEntity/name` | `string` |  |
| 36 | `tender/procuringEntity/roles` | `string` |  |
| 37 | `tender/procuringEntity/identifier/id` | `string` |  |
| 38 | `tender/procuringEntity/identifier/uri` | `string` |  |
| 39 | `tender/procuringEntity/identifier/scheme` | `string` |  |
| 40 | `tender/numberOfTenderers` | `string` |  |
| 41 | `tender/procurementMethod` | `string` |  |
| 42 | `tender/mainProcurementCategory` | `string` |  |
| 43 | `language` | `string` |  |
| 44 | `initiationType` | `string` |  |
| 45 | `buyer/id` | `string` |  |
| 46 | `buyer/name` | `string` |  |
| 47 | `tender/lots` | `string` |  |
| 48 | `relatedProces` | `string` |  |
| 49 | `tender/amendments` | `string` |  |
| 50 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 51 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 52 | `_url_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 53 | `_linea` | `string` | [de la fuente](../fuentes/aragon.md) |
| 54 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 55 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 56 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 57 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
