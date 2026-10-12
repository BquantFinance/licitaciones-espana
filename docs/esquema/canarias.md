# `canarias.zip`

Esquema de los Parquet de `canarias.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/canarias.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `cabildo_tenerife.parquet` | 6.996 | 18 |
| `gobierno_contratos.parquet` | 91.550 | 31 |
| `gobierno_resumen.parquet` | 3.666 | 38 |
| `las_palmas_gc.parquet` | 10.702 | 22 |
| `scs_resumen.parquet` | 2.404 | 25 |

## `cabildo_tenerife.parquet`

6.996 filas · 18 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `string` |  |
| 2 | `fecha` | `string` |  |
| 3 | `ejercicio` | `string` |  |
| 4 | `denominacion` | `string` |  |
| 5 | `duracion` | `string` |  |
| 6 | `importelicitacion` | `string` |  |
| 7 | `importeadjudicacion` | `string` |  |
| 8 | `procedimientoutilizado` | `string` |  |
| 9 | `publicidad` | `string` |  |
| 10 | `licitadores` | `string` |  |
| 11 | `adjudicatario` | `string` |  |
| 12 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `gobierno_contratos.parquet`

91.550 filas · 31 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `objeto_contrato` | `string` |  |
| 2 | `entidad_adjudicadora` | `string` |  |
| 3 | `procedimiento_contratacion` | `string` |  |
| 4 | `tramitacion_tipo` | `string` |  |
| 5 | `lote` | `string` |  |
| 6 | `adjudicacion_fecha` | `string` |  |
| 7 | `adjudicataria_nif` | `string` |  |
| 8 | `importe_ofertado` | `string` |  |
| 9 | `expediente_numero` | `string` |  |
| 10 | `licitacion_enlace` | `string` |  |
| 11 | `licitacion_estado` | `string` |  |
| 12 | `clasificacion_cpv` | `string` |  |
| 13 | `contrato_tipo` | `string` |  |
| 14 | `publicacion_fecha` | `string` |  |
| 15 | `publicacion_medio` | `string` |  |
| 16 | `licitacion_resultado` | `string` |  |
| 17 | `pliego_enlace` | `string` |  |
| 18 | `perfil_contratante_enlace` | `string` |  |
| 19 | `financiacion_europea` | `string` |  |
| 20 | `fuente_financiacion_id` | `string` |  |
| 21 | `fuente_financiacion` | `string` |  |
| 22 | `localizador_programa_ue` | `string` |  |
| 23 | `formula_revision_precios` | `string` |  |
| 24 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_fichero_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 29 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 30 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 31 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `gobierno_resumen.parquet`

3.666 filas · 38 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Tipo contrato` | `string` |  |
| 2 | `Número` | `string` |  |
| 3 | `Importe (EUR)` | `string` |  |
| 4 | `% (1)` | `string` |  |
| 5 | `Importe (EUR) ` | `string` |  |
| 6 | `Unnamed: 2` | `string` |  |
| 7 | `Unnamed: 5` | `string` |  |
| 8 | `Tipo contrato ` | `string` |  |
| 9 | `Número ` | `string` |  |
| 10 | ` ` | `string` |  |
| 11 | `	% (1) 	 ` | `string` |  |
| 12 | `Tipo de contrato` | `string` |  |
| 13 | `Importe incluido IGIC` | `string` |  |
| 14 | `ÓRGANO DE CONTRATACIÓN` | `string` |  |
| 15 | `TIPO DE CONTRATO` | `string` |  |
| 16 | `NÚMERO` | `string` |  |
| 17 | `IMPORTE (euros)` | `string` |  |
| 18 | `Sección` | `string` |  |
| 19 | `CONSEJERÍA DE SANIDAD` | `string` |  |
| 20 | `Unnamed: 3` | `string` |  |
| 21 | `Unnamed: 4` | `string` |  |
| 22 | `_seccion` | `string` | [de la fuente](../fuentes/canarias.md) |
| 23 | `_tabla` | `string` | [de la fuente](../fuentes/canarias.md) |
| 24 | `_cabecera_tabla` | `string` | [de la fuente](../fuentes/canarias.md) |
| 25 | `_parrafo_previo` | `string` | [de la fuente](../fuentes/canarias.md) |
| 26 | `_pie` | `string` | [de la fuente](../fuentes/canarias.md) |
| 27 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 29 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 31 | `_departamento` | `string` | [de la fuente](../fuentes/canarias.md) |
| 32 | `_organismo` | `string` | [de la fuente](../fuentes/canarias.md) |
| 33 | `_fichero_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 34 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 35 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 36 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 37 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `las_palmas_gc.parquet`

10.702 filas · 22 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `importe` | `string` |  |
| 2 | `ejercicio` | `string` |  |
| 3 | `url_origen` | `string` |  |
| 4 | `denominacion` | `string` |  |
| 5 | `n_expediente` | `string` |  |
| 6 | `adjudicatario` | `string` |  |
| 7 | `tipo_contrato` | `string` |  |
| 8 | `cif_adjudicatario` | `string` |  |
| 9 | `organo_de_contratacion` | `string` |  |
| 10 | `_esExterno` | `string` | [de la fuente](../fuentes/canarias.md) |
| 11 | `_idLocal` | `string` | [de la fuente](../fuentes/canarias.md) |
| 12 | `_identidad` | `string` | [de la fuente](../fuentes/canarias.md) |
| 13 | `id` | `string` |  |
| 14 | `_idExterno` | `string` | [de la fuente](../fuentes/canarias.md) |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_obligacion` | `string` | [de la fuente](../fuentes/canarias.md) |
| 18 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 22 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `scs_resumen.parquet`

2.404 filas · 25 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Unnamed: 0` | `string` |  |
| 2 | `Tipo de Contrato` | `string` |  |
| 3 | `Número` | `string` |  |
| 4 | `Importe` | `string` |  |
| 5 | `%(1)` | `string` |  |
| 6 | `Unnamed: 6` | `string` |  |
| 7 | `Órgano de contratación` | `string` |  |
| 8 | `Importe (Eur)` | `string` |  |
| 9 | `% (1)` | `string` |  |
| 10 | `% ` | `string` |  |
| 11 | `%` | `string` |  |
| 12 | `órgano de contratación` | `string` |  |
| 13 | `Importe ` | `string` |  |
| 14 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_fichero_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 25 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
