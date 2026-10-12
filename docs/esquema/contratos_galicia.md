# `contratos_galicia.zip`

Esquema de los Parquet de `contratos_galicia.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/galicia.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratos_galicia.parquet` | 1.848.770 | 69 |
| `contratos_galicia_base.parquet` | 1.848.770 | 12 |

## `contratos_galicia.parquet`

1.848.770 filas · 69 columnas · 37 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `int64` |  |
| 2 | `objeto` | `string` |  |
| 3 | `importe` | `double` |  |
| 4 | `estado` | `double` |  |
| 5 | `estadoDesc` | `string` |  |
| 6 | `publicado` | `timestamp[ns]` |  |
| 7 | `modificado` | `timestamp[ns]` |  |
| 8 | `_organismo_id` | `int64` | [de la fuente](../fuentes/galicia.md) |
| 9 | `_tipo` | `string` | [de la fuente](../fuentes/galicia.md) |
| 10 | `nif` | `string` |  |
| 11 | `adjudicatario` | `string` |  |
| 12 | `duracion` | `string` |  |
| 13 | `detail_status` | `string` |  |
| 14 | `detail_attempts` | `double` |  |
| 15 | `detail_last_http_status` | `double` |  |
| 16 | `detail_last_error` | `double` |  |
| 17 | `detail_updated_at` | `double` |  |
| 18 | `detail_html_sha256` | `double` |  |
| 19 | `detail_page_title` | `double` |  |
| 20 | `detail_url` | `double` |  |
| 21 | `detail_referencia` | `string` |  |
| 22 | `detail_objeto` | `double` |  |
| 23 | `detail_tipo_tramitacion` | `double` |  |
| 24 | `detail_tipo_procedimiento` | `double` |  |
| 25 | `detail_tipo_contrato` | `double` |  |
| 26 | `detail_presupuesto_base_text` | `double` |  |
| 27 | `detail_presupuesto_base_eur` | `double` |  |
| 28 | `detail_valor_estimado_text` | `double` |  |
| 29 | `detail_valor_estimado_eur` | `double` |  |
| 30 | `detail_num_lotes_text` | `double` |  |
| 31 | `detail_num_lotes` | `double` |  |
| 32 | `detail_sistema_contratacion` | `double` |  |
| 33 | `detail_tipo_financiacion` | `double` |  |
| 34 | `detail_fecha_difusion_text` | `double` |  |
| 35 | `detail_fecha_difusion` | `timestamp[ns]` |  |
| 36 | `detail_fecha_formalizacion_text` | `double` |  |
| 37 | `detail_fecha_formalizacion` | `timestamp[ns]` |  |
| 38 | `detail_sello` | `double` |  |
| 39 | `detail_organo` | `double` |  |
| 40 | `detail_direccion` | `double` |  |
| 41 | `detail_localidad` | `double` |  |
| 42 | `detail_cp` | `string` |  |
| 43 | `detail_telefono` | `string` |  |
| 44 | `detail_fax` | `string` |  |
| 45 | `detail_correo_electronico` | `double` |  |
| 46 | `detail_contrato_sara` | `double` |  |
| 47 | `detail_contratacion_centralizada` | `double` |  |
| 48 | `detail_ley_nacional_aplicacion` | `double` |  |
| 49 | `detail_contrato_mixto` | `double` |  |
| 50 | `detail_subasta_electronica` | `double` |  |
| 51 | `detail_compra_publica_estrategica` | `double` |  |
| 52 | `detail_acuerdo_marco` | `double` |  |
| 53 | `detail_prorroga` | `double` |  |
| 54 | `detail_modificacion` | `double` |  |
| 55 | `detail_cpv_codes` | `double` |  |
| 56 | `detail_nuts_codes` | `double` |  |
| 57 | `detail_publicaciones_count` | `double` |  |
| 58 | `detail_documentos_count` | `double` |  |
| 59 | `detail_adjudicaciones_count` | `double` |  |
| 60 | `detail_cambios_count` | `double` |  |
| 61 | `detail_pairs_count` | `double` |  |
| 62 | `detail_tables_count` | `double` |  |
| 63 | `detail_adjudicaciones_json` | `double` |  |
| 64 | `detail_campos_extra_json` | `double` |  |
| 65 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 66 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 67 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 68 | `importe_semilla` | `double` |  |
| 69 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_galicia_base.parquet`

1.848.770 filas · 12 columnas · 37 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `int64` |  |
| 2 | `objeto` | `string` |  |
| 3 | `importe` | `double` |  |
| 4 | `estado` | `double` |  |
| 5 | `estadoDesc` | `string` |  |
| 6 | `publicado` | `timestamp[ns]` |  |
| 7 | `modificado` | `timestamp[ns]` |  |
| 8 | `_organismo_id` | `int64` | [de la fuente](../fuentes/galicia.md) |
| 9 | `_tipo` | `string` | [de la fuente](../fuentes/galicia.md) |
| 10 | `nif` | `string` |  |
| 11 | `adjudicatario` | `string` |  |
| 12 | `duracion` | `string` |  |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (1, 0 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/contratos_galicia_COMPLETO.csv` | — | — |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
