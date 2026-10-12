# `nacional_lotes_y_otras_tablas.zip`

Esquema de los Parquet de `nacional_lotes_y_otras_tablas.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/placsp.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_completo_borrados.parquet` | 498.491 | 9 |
| `licitaciones_completo_lotes.parquet` | 5.007.443 | 17 |
| `licitaciones_completo_modificaciones.parquet` | 358.958 | 17 |
| `licitaciones_completo_semilla_contenido.parquet` | 39.028 | 50 |

## `licitaciones_completo_borrados.parquet`

498.491 filas · 9 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `fecha_borrado` | `timestamp[us, tz=UTC]` |  |
| 3 | `motivo` | `large_string` |  |
| 4 | `comentario` | `null` |  |
| 5 | `conjunto` | `large_string` |  |
| 6 | `archivo_origen` | `large_string` |  |
| 7 | `zip_historico` | `large_string` |  |
| 8 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 9 | `textos_originales` | `null` | [marca](../ESQUEMA.md#calidad-del-origen) |

## `licitaciones_completo_lotes.parquet`

5.007.443 filas · 17 columnas · 5 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `conjunto` | `large_string` |  |
| 4 | `orden_lote` | `int64` |  |
| 5 | `lote` | `large_string` |  |
| 6 | `objeto_lote` | `large_string` |  |
| 7 | `valor_estimado_contrato` | `null` |  |
| 8 | `importe_sin_iva` | `double` |  |
| 9 | `importe_con_iva` | `double` |  |
| 10 | `cpv_principal` | `large_string` |  |
| 11 | `cpvs` | `large_string` |  |
| 12 | `ubicacion` | `large_string` |  |
| 13 | `nuts` | `large_string` |  |
| 14 | `programas_financiacion` | `null` |  |
| 15 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 16 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 17 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |

## `licitaciones_completo_modificaciones.parquet`

358.958 filas · 17 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `conjunto` | `large_string` |  |
| 4 | `orden_modificacion` | `int64` |  |
| 5 | `id_modificacion` | `large_string` |  |
| 6 | `id_contrato` | `large_string` |  |
| 7 | `nota` | `large_string` |  |
| 8 | `importe_modificacion_sin_iva` | `double` |  |
| 9 | `importe_final_sin_iva` | `double` |  |
| 10 | `duracion_modificacion` | `large_string` |  |
| 11 | `duracion_modificacion_unidad` | `large_string` |  |
| 12 | `duracion_final` | `large_string` |  |
| 13 | `duracion_final_unidad` | `large_string` |  |
| 14 | `detalle` | `large_string` |  |
| 15 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 16 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 17 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |

## `licitaciones_completo_semilla_contenido.parquet`

39.028 filas · 50 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `objeto` | `large_string` |  |
| 4 | `organo_contratante` | `large_string` |  |
| 5 | `nif_organo` | `large_string` |  |
| 6 | `dir3_organo` | `large_string` |  |
| 7 | `id_plataforma` | `large_string` |  |
| 8 | `ciudad_organo` | `large_string` |  |
| 9 | `dependencia` | `large_string` |  |
| 10 | `tipo_contrato_code` | `large_string` |  |
| 11 | `tipo_contrato` | `large_string` |  |
| 12 | `subtipo_code` | `large_string` |  |
| 13 | `procedimiento_code` | `large_string` |  |
| 14 | `procedimiento` | `large_string` |  |
| 15 | `estado_code` | `large_string` |  |
| 16 | `estado` | `large_string` |  |
| 17 | `valor_estimado_contrato` | `double` |  |
| 18 | `importe_sin_iva` | `double` |  |
| 19 | `importe_con_iva` | `double` |  |
| 20 | `importe_adjudicacion` | `double` |  |
| 21 | `importe_adj_con_iva` | `double` |  |
| 22 | `adjudicatario` | `large_string` |  |
| 23 | `nif_adjudicatario` | `large_string` |  |
| 24 | `num_ofertas` | `double` |  |
| 25 | `es_pyme` | `bool` |  |
| 26 | `cpv_principal` | `large_string` |  |
| 27 | `cpvs` | `large_string` |  |
| 28 | `ubicacion` | `large_string` |  |
| 29 | `nuts` | `large_string` |  |
| 30 | `duracion` | `large_string` |  |
| 31 | `duracion_unidad` | `large_string` |  |
| 32 | `financiacion_ue` | `large_string` |  |
| 33 | `urgencia` | `large_string` |  |
| 34 | `fecha_limite` | `timestamp[us]` |  |
| 35 | `hora_limite` | `large_string` |  |
| 36 | `fecha_adjudicacion` | `timestamp[us]` |  |
| 37 | `fecha_publicacion` | `timestamp[us]` |  |
| 38 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 39 | `url` | `large_string` |  |
| 40 | `conjunto` | `large_string` |  |
| 41 | `archivo_origen` | `large_string` |  |
| 42 | `ano` | `double` |  |
| 43 | `tipo_registro` | `large_string` |  |
| 44 | `id_consulta` | `large_string` |  |
| 45 | `nombre_consulta` | `large_string` |  |
| 46 | `condiciones` | `large_string` |  |
| 47 | `tipo_condicion` | `large_string` |  |
| 48 | `fecha_planificada` | `timestamp[us]` |  |
| 49 | `fecha_limite_respuestas` | `timestamp[us]` |  |
| 50 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
