# `euskadi.zip`

Esquema de los Parquet de `euskadi.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/euskadi.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `api_anuncios.parquet` | 704.529 | 67 |
| `api_contratos.parquet` | 715.868 | 40 |
| `contratos_master.parquet` | 727.690 | 42 |
| `empresas_licitadoras.parquet` | 9.357 | 20 |
| `poderes_adjudicadores.parquet` | 938 | 18 |
| `revascon_historico.parquet` | 34.507 | 81 |
| `revascon_por_poder.parquet` | 557.580 | 135 |
| `ultimos_90d.parquet` | 474 | 24 |
| `vitoria_contratos.parquet` | 2.347 | 46 |

## `api_anuncios.parquet`

704.529 filas · 67 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `int64` |  |
| 2 | `code` | `large_string` |  |
| 3 | `object` | `large_string` |  |
| 4 | `firstPublicationDate` | `large_string` |  |
| 5 | `lastPublicationDate` | `large_string` |  |
| 6 | `deadlineDate` | `large_string` |  |
| 7 | `budgetWithoutVAT` | `double` |  |
| 8 | `financedEuropeanFunds` | `bool` |  |
| 9 | `recoveryPlan` | `bool` |  |
| 10 | `minorContract` | `bool` |  |
| 11 | `subjectAppeal` | `bool` |  |
| 12 | `sara` | `bool` |  |
| 13 | `mainEntityOfPage` | `large_string` |  |
| 14 | `entity_id` | `double` |  |
| 15 | `entity_name` | `large_string` |  |
| 16 | `entity_org_id` | `double` |  |
| 17 | `entity_org_name` | `large_string` |  |
| 18 | `entity_org__links_self_name` | `large_string` |  |
| 19 | `entity_org__links_self_href` | `large_string` |  |
| 20 | `entity__links_self_name` | `large_string` |  |
| 21 | `entity__links_self_href` | `large_string` |  |
| 22 | `contractingAuthority_id` | `double` |  |
| 23 | `contractingAuthority_identificationNumber` | `large_string` |  |
| 24 | `contractingAuthority_name` | `large_string` |  |
| 25 | `contractingAuthority_scope` | `large_string` |  |
| 26 | `contractingAuthority_address` | `large_string` |  |
| 27 | `contractingAuthority_webPage` | `large_string` |  |
| 28 | `contractingAuthority_publicationDate` | `large_string` |  |
| 29 | `contractingAuthority_mainActivity` | `large_string` |  |
| 30 | `contractingAuthority_codNUTS` | `large_string` |  |
| 31 | `contractingAuthority_mainEntityOfPage` | `large_string` |  |
| 32 | `contractingAuthority__links_self_name` | `large_string` |  |
| 33 | `contractingAuthority__links_self_href` | `large_string` |  |
| 34 | `contractType_id` | `double` |  |
| 35 | `contractType_name` | `large_string` |  |
| 36 | `contractType__links_self_name` | `large_string` |  |
| 37 | `contractType__links_self_href` | `large_string` |  |
| 38 | `contractProcedureStatus_id` | `double` |  |
| 39 | `contractProcedureStatus_name` | `large_string` |  |
| 40 | `contractProcedureStatus__links_self_name` | `large_string` |  |
| 41 | `contractProcedureStatus__links_self_href` | `large_string` |  |
| 42 | `contractProcedureType_id` | `double` |  |
| 43 | `contractProcedureType_name` | `large_string` |  |
| 44 | `contractProcedureType__links_self_name` | `large_string` |  |
| 45 | `contractProcedureType__links_self_href` | `large_string` |  |
| 46 | `_links_self_name` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 47 | `_links_self_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 48 | `_links_contracts_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 49 | `_ventana` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 50 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 51 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 52 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 53 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 54 | `webpagElectronicBidding` | `large_string` |  |
| 55 | `contractingAuthority_alternativeActivity` | `large_string` |  |
| 56 | `contractingAuthority_CPV` | `large_string` |  |
| 57 | `contractingAuthority__links_contractingAuthorities` | `large_string` |  |
| 58 | `openingDates` | `large_string` |  |
| 59 | `numberBidders` | `double` |  |
| 60 | `numberBiddersPyme` | `double` |  |
| 61 | `numberBiddersUE` | `double` |  |
| 62 | `numberBiddersNoUE` | `double` |  |
| 63 | `electronicTenders` | `double` |  |
| 64 | `codMMR` | `large_string` |  |
| 65 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 66 | `_id_repetido` | `bool` | [de la fuente](../fuentes/euskadi.md) |
| 67 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `api_contratos.parquet`

715.868 filas · 40 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `object` | `large_string` |  |
| 3 | `CIF` | `large_string` |  |
| 4 | `socialReason` | `large_string` |  |
| 5 | `awardDate` | `large_string` |  |
| 6 | `companySignatureDate` | `large_string` |  |
| 7 | `administrationSignatureDate` | `large_string` |  |
| 8 | `awardAmount` | `double` |  |
| 9 | `awardAmountWithoutVAT` | `double` |  |
| 10 | `monthsContractDuration` | `double` |  |
| 11 | `CPV` | `large_string` |  |
| 12 | `minorContract` | `bool` |  |
| 13 | `mainEntityOfPage` | `large_string` |  |
| 14 | `contractType_id` | `double` |  |
| 15 | `contractType_name` | `large_string` |  |
| 16 | `contractType__links_self_name` | `large_string` |  |
| 17 | `contractType__links_self_href` | `large_string` |  |
| 18 | `contractProcedureStatus_id` | `int64` |  |
| 19 | `contractProcedureStatus_name` | `large_string` |  |
| 20 | `contractProcedureStatus__links_self_name` | `large_string` |  |
| 21 | `contractProcedureStatus__links_self_href` | `large_string` |  |
| 22 | `contractProcedureType_id` | `int64` |  |
| 23 | `contractProcedureType_name` | `large_string` |  |
| 24 | `contractProcedureType__links_self_name` | `large_string` |  |
| 25 | `contractProcedureType__links_self_href` | `large_string` |  |
| 26 | `_links_self_name` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 27 | `_links_self_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 28 | `_links_contractingAuthority_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 29 | `_links_contractingNotice_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 30 | `_links_entity_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 31 | `_links_org_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 32 | `contractEndDate` | `large_string` |  |
| 33 | `_ventana` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 34 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 35 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 36 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 37 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 39 | `_id_repetido` | `bool` | [de la fuente](../fuentes/euskadi.md) |
| 40 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `contratos_master.parquet`

727.690 filas · 42 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `nombre` | `large_string` |  |
| 2 | `descripción` | `large_string` |  |
| 3 | `colección` | `large_string` |  |
| 4 | `titulo_del_contrato` | `large_string` |  |
| 5 | `objeto_del_contrato` | `large_string` |  |
| 6 | `tipo_de_anuncio` | `large_string` |  |
| 7 | `fecha_de_publicación_documento` | `timestamp[ns]` |  |
| 8 | `expediente` | `large_string` |  |
| 9 | `estado_de_la_tramitacion` | `large_string` |  |
| 10 | `contrato_menor` | `large_string` |  |
| 11 | `adjudicación` | `large_string` |  |
| 12 | `ámbito_geográfico_del_poder_adjudicador` | `large_string` |  |
| 13 | `url_del_logo_del_poder_adjudicador` | `large_string` |  |
| 14 | `título_del_logo_del_poder_adjudicador` | `large_string` |  |
| 15 | `entidad_que_impulsa_la_contratación` | `large_string` |  |
| 16 | `órgano_de_contratación` | `large_string` |  |
| 17 | `fecha_límite_de_presentación` | `timestamp[ns]` |  |
| 18 | `url_física` | `large_string` |  |
| 19 | `xml_datos` | `large_string` |  |
| 20 | `xml_metadatos` | `large_string` |  |
| 21 | `zip` | `large_string` |  |
| 22 | `fecha_de_creación` | `timestamp[ns]` |  |
| 23 | `id._institución` | `large_string` |  |
| 24 | `institución` | `large_string` |  |
| 25 | `id._departamento` | `large_string` |  |
| 26 | `departamento` | `large_string` |  |
| 27 | `id._órgano_gestor` | `large_string` |  |
| 28 | `órgano_gestor` | `large_string` |  |
| 29 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_year` | `int64` | [de la fuente](../fuentes/euskadi.md) |
| 31 | `subsanación` | `large_string` |  |
| 32 | `apertura_de_plicas` | `large_string` |  |
| 33 | `acuerdos_de_la_mesa_de_contratacion` | `large_string` |  |
| 34 | `url_amigable` | `large_string` |  |
| 35 | `yetrs89` | `large_string` |  |
| 36 | `xetrs89` | `large_string` |  |
| 37 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 39 | `_en_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 40 | `_columnas_corridas` | `large_string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 41 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 42 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `empresas_licitadoras.parquet`

9.357 filas · 20 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `name` | `large_string` |  |
| 2 | `registrationnumber` | `large_string` |  |
| 3 | `identificationnumber` | `large_string` |  |
| 4 | `address` | `large_string` |  |
| 5 | `economicactivities` | `large_string` |  |
| 6 | `certificatestartdate` | `large_string` |  |
| 7 | `certificateenddate` | `large_string` |  |
| 8 | `object` | `large_string` |  |
| 9 | `mainentityofpage` | `large_string` |  |
| 10 | `location_postalcode` | `large_string` |  |
| 11 | `location_municipality` | `large_string` |  |
| 12 | `location_county` | `large_string` |  |
| 13 | `location_municipalityid` | `large_string` |  |
| 14 | `location_countyid` | `large_string` |  |
| 15 | `_links_self_name` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 16 | `_links_self_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 17 | `classification` | `large_string` |  |
| 18 | `hiringprohibitions` | `large_string` |  |
| 19 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 20 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `poderes_adjudicadores.parquet`

938 filas · 18 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `int64` |  |
| 2 | `identificationnumber` | `large_string` |  |
| 3 | `name` | `large_string` |  |
| 4 | `scope` | `large_string` |  |
| 5 | `address` | `large_string` |  |
| 6 | `webpage` | `large_string` |  |
| 7 | `mainactivity` | `large_string` |  |
| 8 | `codnuts` | `large_string` |  |
| 9 | `entities` | `large_string` |  |
| 10 | `mainentityofpage` | `large_string` |  |
| 11 | `_links_self_name` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 12 | `_links_self_href` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 13 | `publicationdate` | `large_string` |  |
| 14 | `_links_contractingauthorities` | `large_string` | [de la fuente](../fuentes/euskadi.md) |
| 15 | `cpv` | `large_string` |  |
| 16 | `alternativeactivity` | `large_string` |  |
| 17 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 18 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `revascon_historico.parquet`

34.507 filas · 81 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `estado_contrato` | `large_string` |  |
| 2 | `poder_adjudicador` | `large_string` |  |
| 3 | `entidad_impulsora` | `large_string` |  |
| 4 | `código_identificador_del_contrato` | `large_string` |  |
| 5 | `título_de_contrato` | `large_string` |  |
| 6 | `tipo_de_contrato` | `large_string` |  |
| 7 | `adjudicatario` | `large_string` |  |
| 8 | `importe_de_adjudicación_con_iva` | `int64` |  |
| 9 | `fecha_de_formalización` | `large_string` |  |
| 10 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `año_del_contrato` | `double` |  |
| 12 | `contrato_menor` | `large_string` |  |
| 13 | `ámbito` | `large_string` |  |
| 14 | `tipo_de_administración` | `large_string` |  |
| 15 | `tipo_de_administración_local` | `large_string` |  |
| 16 | `órgano_contratante` | `large_string` |  |
| 17 | `entidad_tramitadora` | `large_string` |  |
| 18 | `mesa_de_contratación` | `large_string` |  |
| 19 | `objeto_del_contrato` | `large_string` |  |
| 20 | `lugar_de_ejecución` | `large_string` |  |
| 21 | `¿lugar_ejecución_principal_en_la_unión_europea?` | `large_string` |  |
| 22 | `nuts` | `large_string` |  |
| 23 | `países` | `large_string` |  |
| 24 | `código_cpv` | `large_string` |  |
| 25 | `¿contratación_por_lotes?` | `large_string` |  |
| 26 | `¿contratación_mixto?` | `large_string` |  |
| 27 | `¿acuerdo_marco?` | `large_string` |  |
| 28 | `¿contrato_complementario?` | `large_string` |  |
| 29 | `¿publicidad?` | `large_string` |  |
| 30 | `fecha_publicación_en_el_boe` | `large_string` |  |
| 31 | `fecha_publicación_en_el_bopv` | `large_string` |  |
| 32 | `fecha_publicación_en_el_boletín_de_territorio_histórico` | `large_string` |  |
| 33 | `fecha_publicación_en_el_doue` | `large_string` |  |
| 34 | `fecha_publicación_en_el_perfil_de_contratante` | `large_string` |  |
| 35 | `tramitación` | `large_string` |  |
| 36 | `procedimiento_de_adjudicación` | `large_string` |  |
| 37 | `criterios_de_valoración` | `large_string` |  |
| 38 | `importe_de_licitación_sin_iva` | `double` |  |
| 39 | `importe_de_licitación_con_iva` | `double` |  |
| 40 | `iva_del_importe_de_licitación` | `large_string` |  |
| 41 | `importe_de_adjudicación_sin_iva` | `double` |  |
| 42 | `iva_del_importe_de_adjudicación` | `large_string` |  |
| 43 | `plazo_de_ejecución` | `large_string` |  |
| 44 | `plazo_de_ejecución_(meses)` | `double` |  |
| 45 | `¿carácter_plurianual?` | `large_string` |  |
| 46 | `anualidad` | `large_string` |  |
| 47 | `¿revisión_de_precios_establecidos?` | `large_string` |  |
| 48 | `nº_de_licitadores_presentados` | `double` |  |
| 49 | `fecha_de_adjudicación` | `large_string` |  |
| 50 | `observación_de_la_adjudicación/_formalización_del_contrato_en_castellano` | `large_string` |  |
| 51 | `observación_de_la_adjudicación/_formalización_del_contrato_en_euskera` | `large_string` |  |
| 52 | `modalidad_de_contratación` | `large_string` |  |
| 53 | `duración` | `double` |  |
| 54 | `modalidad_importe` | `large_string` |  |
| 55 | `tipo_de_contrato_de_suministro` | `large_string` |  |
| 56 | `precios_unitarios` | `large_string` |  |
| 57 | `país_de_origen_de_los_productos_adquiridos` | `large_string` |  |
| 58 | `modalidad_de_determinación_del_precio` | `large_string` |  |
| 59 | `indicación_del_supuesto_de_aplicación_que_amparó_el_uso_del_procedimiento` | `large_string` |  |
| 60 | `número_de_invitaciones` | `double` |  |
| 61 | `¿indicador_de_ingreso?` | `large_string` |  |
| 62 | `modalidad_que_determina_el_importe_del_contrato` | `large_string` |  |
| 63 | `modificación_del_objeto` | `large_string` |  |
| 64 | `modificación_del_plazo` | `large_string` |  |
| 65 | `modificación_del_adjudicatario` | `large_string` |  |
| 66 | `concepto_castellano` | `large_string` |  |
| 67 | `concepto_euskera` | `large_string` |  |
| 68 | `fecha_acuerdo` | `large_string` |  |
| 69 | `importe` | `double` |  |
| 70 | `resolución_contrato` | `large_string` |  |
| 71 | `finalización_contrato` | `large_string` |  |
| 72 | `porcentaje` | `double` |  |
| 73 | `participantes` | `large_string` |  |
| 74 | `objeto_de_recurso` | `large_string` |  |
| 75 | `régimen_general` | `large_string` |  |
| 76 | `indemnización` | `large_string` |  |
| 77 | `importe.1` | `double` |  |
| 78 | `continuidad_del_contrato` | `large_string` |  |
| 79 | `_year` | `double` | [de la fuente](../fuentes/euskadi.md) |
| 80 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 81 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `revascon_por_poder.parquet`

557.580 filas · 135 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `objeto_del_contrato` | `string` |  |
| 2 | `código_del_contrato` | `string` |  |
| 3 | `cif/nif` | `string` |  |
| 4 | `razón_social` | `string` |  |
| 5 | `estado_contrato` | `string` |  |
| 6 | `tipo_de_contrato` | `string` |  |
| 7 | `estado_tramitacion` | `string` |  |
| 8 | `procedimiento_de_adjudicación` | `string` |  |
| 9 | `criterios_de_adjudicación` | `string` |  |
| 10 | `tipo_de_administración` | `string` |  |
| 11 | `tipo_de_administración_local` | `null` |  |
| 12 | `poder_adjudicador` | `string` |  |
| 13 | `entidad_impulsora` | `string` |  |
| 14 | `organo` | `string` |  |
| 15 | `entidad_tramitadora` | `string` |  |
| 16 | `mesa_de_contratación` | `string` |  |
| 17 | `¿lugar_ejecución_principal_en_la_unión_europea?` | `string` |  |
| 18 | `nuts` | `string` |  |
| 19 | `lugar_de_ejecución` | `string` |  |
| 20 | `fecha_de_adjudicación` | `string` |  |
| 21 | `fecha_firma_empresa_/_formalización` | `string` |  |
| 22 | `duración_del_contrato` | `string` |  |
| 23 | `fecha_fin_plazo` | `string` |  |
| 24 | `importe_de_adjudicación` | `string` |  |
| 25 | `importe_de_adjudicación_con_iva` | `string` |  |
| 26 | `fecha_fin_contrato` | `string` |  |
| 27 | `¿caracter_plurianual?` | `string` |  |
| 28 | `¿revisión_de_precios_establecidos?` | `string` |  |
| 29 | `observaciones` | `string` |  |
| 30 | `cláusulas_especiales` | `string` |  |
| 31 | `reservado_a_talleres_protegidos` | `string` |  |
| 32 | `a_ejecutar_en_un_programa_de_empleo_protegido` | `string` |  |
| 33 | `condiciones_de_ejecución_del_contrato` | `string` |  |
| 34 | `obligación_de_indicar_los_nombres_y_las_cualificaciones_profesionales_del_personal_asignado_a_la_ejecución_del_contrato` | `string` |  |
| 35 | `cpv` | `string` |  |
| 36 | `división_en_lotes` | `string` |  |
| 37 | `¿publicidad?` | `string` |  |
| 38 | `boe` | `string` |  |
| 39 | `bopv` | `string` |  |
| 40 | `boletín_del_territorio_histórico` | `string` |  |
| 41 | `doue` | `string` |  |
| 42 | `fecha_publicación` | `string` |  |
| 43 | `presupuesto_sin_iva` | `string` |  |
| 44 | `presupuesto_con_iva` | `string` |  |
| 45 | `iva(s)_del_presupuesto_del_contrato` | `string` |  |
| 46 | `anualidades` | `string` |  |
| 47 | `nº_de_licitadores_presentados` | `string` |  |
| 48 | `¿ute?` | `string` |  |
| 49 | `número_de_empresas_invitadas` | `string` |  |
| 50 | `modificaciones_del_objeto` | `string` |  |
| 51 | `prórrogas` | `string` |  |
| 52 | `modificaciones_del_plazo` | `string` |  |
| 53 | `modificacion_del_adjudicatario` | `string` |  |
| 54 | `incidencias_contractuales` | `string` |  |
| 55 | `datos_resolución_por_finalización_anómala_o_anticipada` | `string` |  |
| 56 | `datos_finalizacion` | `string` |  |
| 57 | `garantia` | `string` |  |
| 58 | `regimen_general` | `string` |  |
| 59 | `tipo_nulidad` | `string` |  |
| 60 | `supuestos_especiales` | `null` |  |
| 61 | `indemnización` | `null` |  |
| 62 | `importe_nulidad` | `null` |  |
| 63 | `continuidad_del_contrato` | `string` |  |
| 64 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 65 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 66 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 67 | `_poder_id` | `string` | [de la fuente](../fuentes/euskadi.md) |
| 68 | `_año` | `int64` | [de la fuente](../fuentes/euskadi.md) |
| 69 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 70 | `estado_tramitación` | `string` |  |
| 71 | `modificación_del_adjudicatario` | `string` |  |
| 72 | `régimen_general` | `null` |  |
| 73 | `tipo_contrato` | `string` |  |
| 74 | `kontratuaren_xedea` | `string` |  |
| 75 | `kontratuaren_kodea` | `string` |  |
| 76 | `ifz/ifk` | `string` |  |
| 77 | `sozietate-izena` | `string` |  |
| 78 | `kontratuaren_egoera` | `string` |  |
| 79 | `kontratu_mota` | `string` |  |
| 80 | `izapidetze-egoera` | `string` |  |
| 81 | `adjudikazio-prozedura` | `string` |  |
| 82 | `adjudikatzeko_irizpideak` | `string` |  |
| 83 | `administrazio_mota` | `string` |  |
| 84 | `tokiko_administrazio_mota` | `null` |  |
| 85 | `botere_adjudikatzailea` | `string` |  |
| 86 | `erakunde_sustatzailea` | `string` |  |
| 87 | `organoa` | `string` |  |
| 88 | `erakunde_izapidatzailea` | `null` |  |
| 89 | `kontratazio-mahaia` | `null` |  |
| 90 | `egikaritzeko_toki_nagusia_europar_batasunean?` | `string` |  |
| 91 | `betearazpen-lekua` | `null` |  |
| 92 | `adjudikazio-data` | `string` |  |
| 93 | `enpres._sina./formalizazio_data` | `string` |  |
| 94 | `kontratuaren_iraupena` | `string` |  |
| 95 | `epearen_amaiera-data` | `null` |  |
| 96 | `adjudikazioaren_zenbatekoa` | `string` |  |
| 97 | `adjudikazioaren_zenbatekoa_bez_barne` | `string` |  |
| 98 | `kontratuaren_amaiera-data` | `null` |  |
| 99 | `urte_bat_baino_gehiago?` | `string` |  |
| 100 | `ezarritako_prezioen_berrikuspena?` | `string` |  |
| 101 | `oharrak` | `null` |  |
| 102 | `klausula_bereziak` | `null` |  |
| 103 | `lantegi_babestuetarako_erreserbatua` | `string` |  |
| 104 | `enplegu_babestuko_programa_batean_egikaritzeko` | `string` |  |
| 105 | `kontratua_gauzatzean_bete_beharreko_baldintzak` | `null` |  |
| 106 | `nahitaezkoa_da_kontratua_betearazteko_adjudikatutako_langileen_izenak_eta_lanbide-kualifikazioak_ematea` | `string` |  |
| 107 | `sortaka_zatitzea` | `string` |  |
| 108 | `publizitatea?` | `string` |  |
| 109 | `ehaa` | `null` |  |
| 110 | `lurralde_historikoko_aldizkaria` | `null` |  |
| 111 | `ebao` | `null` |  |
| 112 | `argitaratze-data` | `string` |  |
| 113 | `aurrekontua,_bezik_gabe` | `string` |  |
| 114 | `aurrekontua,_beza_barne` | `string` |  |
| 115 | `kontratuko_aurrekontuaren_beza(k)` | `null` |  |
| 116 | `urterokoak` | `null` |  |
| 117 | `aurkeztutako_lizitatzaileen_kop.` | `string` |  |
| 118 | `aldi_baterako_enpresa-elkartea?` | `string` |  |
| 119 | `gonbidatutako_enpresen_kopurua` | `string` |  |
| 120 | `xede-aldaketak` | `null` |  |
| 121 | `luzapenak` | `null` |  |
| 122 | `epe-aldaketak` | `null` |  |
| 123 | `adjudikazioduna_aldatzea` | `null` |  |
| 124 | `kontratu-gorabeherak` | `null` |  |
| 125 | `bukaera_anomaloaren_edo_ebazpen_aurreratuaren_datuak` | `null` |  |
| 126 | `amaieraren_datuak` | `string` |  |
| 127 | `bermeak` | `null` |  |
| 128 | `araubide_orokorra` | `null` |  |
| 129 | `deuseztasun_mota` | `null` |  |
| 130 | `kasu_bereziak` | `null` |  |
| 131 | `kalte-ordaina` | `null` |  |
| 132 | `deuseztasunaren_zenbatekoa` | `null` |  |
| 133 | `kontratuaren_jarraipena` | `null` |  |
| 134 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 135 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |

## `ultimos_90d.parquet`

474 filas · 24 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `nombre` | `large_string` |  |
| 2 | `colección` | `large_string` |  |
| 3 | `titulo_del_contrato` | `large_string` |  |
| 4 | `objeto_del_contrato` | `large_string` |  |
| 5 | `fecha_de_publicación_documento` | `large_string` |  |
| 6 | `expediente` | `large_string` |  |
| 7 | `estado_de_la_tramitacion` | `large_string` |  |
| 8 | `contrato_menor` | `bool` |  |
| 9 | `url_del_logo_del_poder_adjudicador` | `large_string` |  |
| 10 | `título_del_logo_del_poder_adjudicador` | `large_string` |  |
| 11 | `entidad_que_impulsa_la_contratación` | `large_string` |  |
| 12 | `órgano_de_contratación` | `large_string` |  |
| 13 | `fecha_límite_de_presentación` | `large_string` |  |
| 14 | `url_amigable` | `large_string` |  |
| 15 | `url_física` | `large_string` |  |
| 16 | `xml_datos` | `large_string` |  |
| 17 | `xml_metadatos` | `large_string` |  |
| 18 | `fecha_última_publicación` | `large_string` |  |
| 19 | `id._institución` | `large_string` |  |
| 20 | `institución` | `large_string` |  |
| 21 | `id._departamento` | `large_string` |  |
| 22 | `departamento` | `large_string` |  |
| 23 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_fuente` | `large_string` | [marca](../ESQUEMA.md#procedencia) |

## `vitoria_contratos.parquet`

2.347 filas · 46 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `tipo_contrato` | `string` |  |
| 2 | `cif_empresa_adjudicataria` | `string` |  |
| 3 | `empresa_adjudicataria` | `string` |  |
| 4 | `número_licitadores` | `string` |  |
| 5 | `precio_actual_del_contrato` | `string` |  |
| 6 | `fecha_del_contrato` | `string` |  |
| 7 | `fecha_del_contrato.1` | `string` |  |
| 8 | `expediente` | `string` |  |
| 9 | `asunto` | `string` |  |
| 10 | `procedimiento_utilizado` | `string` |  |
| 11 | `importe_licitación` | `string` |  |
| 12 | `duración` | `string` |  |
| 13 | `importe_adjudicación` | `string` |  |
| 14 | `publicación_en_doue` | `string` |  |
| 15 | `publicación_en_botha` | `string` |  |
| 16 | `publicación_en_boe` | `string` |  |
| 17 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `empresa_ajudicataria` | `string` |  |
| 23 | `fecha_de_contrato` | `string` |  |
| 24 | `importe_de_adjudicación` | `string` |  |
| 25 | `nif_empresa
_adjudicataria` | `string` |  |
| 26 | `empresa_(i)_adjudicataria` | `string` |  |
| 27 | `número_
licitadores` | `string` |  |
| 28 | `precio_actual_
del_contrato` | `string` |  |
| 29 | `fecha_
de_contrato` | `string` |  |
| 30 | `asunto-1` | `string` |  |
| 31 | `asunto-2` | `string` |  |
| 32 | `asunto-3` | `string` |  |
| 33 | `importe_
licitación` | `string` |  |
| 34 | `d/s/m/a_
duración` | `string` |  |
| 35 | `importe_
adjudicación` | `string` |  |
| 36 | `publicación
_en_doue` | `string` |  |
| 37 | `publicación
_en_botha` | `string` |  |
| 38 | `publicación
_en_boe` | `string` |  |
| 39 | `f_adjudica` | `string` |  |
| 40 | `precio` | `string` |  |
| 41 | `empresa` | `string` |  |
| 42 | `nº_invitados` | `string` |  |
| 43 | `nº_licitadores` | `string` |  |
| 44 | `fecha_adjudicación` | `string` |  |
| 45 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 46 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (4, 3 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/bilbao_contratos.parquet` | 4.823 | 14 |
| `v2026.02/contratos_master.parquet` | 664.545 | 56 |
| `v2026.02/empresas_licitadoras.parquet` | 9.017 | 19 |
| `v2026.02/stats.json` | — | — |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
