# `nacional_licitaciones_hasta_2019.zip`

Esquema de los Parquet de `nacional_licitaciones_hasta_2019.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/placsp.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_completo/licitaciones_completo_hasta_2019.parquet` | 1.607.195 | 80 |
| `licitaciones_completo/licitaciones_completo_sin_ano.parquet` | 10.226 | 80 |

## `licitaciones_completo/licitaciones_completo_hasta_2019.parquet`

1.607.195 filas · 80 columnas · 4 grupos de filas

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
| 26 | `n_lotes` | `int64` |  |
| 27 | `n_resultados` | `int64` |  |
| 28 | `cpv_principal` | `large_string` |  |
| 29 | `cpvs` | `large_string` |  |
| 30 | `ubicacion` | `large_string` |  |
| 31 | `nuts` | `large_string` |  |
| 32 | `duracion` | `large_string` |  |
| 33 | `duracion_unidad` | `large_string` |  |
| 34 | `financiacion_ue` | `large_string` |  |
| 35 | `urgencia` | `large_string` |  |
| 36 | `fecha_limite` | `timestamp[us]` |  |
| 37 | `hora_limite` | `large_string` |  |
| 38 | `fecha_adjudicacion` | `timestamp[us]` |  |
| 39 | `fecha_publicacion` | `timestamp[us]` |  |
| 40 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 41 | `url` | `large_string` |  |
| 42 | `conjunto` | `large_string` |  |
| 43 | `archivo_origen` | `large_string` |  |
| 44 | `tipo_registro` | `large_string` |  |
| 45 | `ano` | `double` |  |
| 46 | `n_versiones` | `int64` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 47 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 48 | `id_consulta` | `large_string` |  |
| 49 | `nombre_consulta` | `large_string` |  |
| 50 | `condiciones` | `large_string` |  |
| 51 | `tipo_condicion` | `large_string` |  |
| 52 | `fecha_planificada` | `timestamp[us]` |  |
| 53 | `fecha_limite_respuestas` | `timestamp[us]` |  |
| 54 | `tipo_condicion_code` | `large_string` |  |
| 55 | `motivo_tipo_condicion` | `large_string` |  |
| 56 | `motivo_seleccion` | `large_string` |  |
| 57 | `adjunto_consulta_url` | `large_string` |  |
| 58 | `sara` | `bool` |  |
| 59 | `sistema_contratacion_code` | `large_string` |  |
| 60 | `forma_presentacion_code` | `large_string` |  |
| 61 | `fecha_inicio_presentacion` | `timestamp[ms]` |  |
| 62 | `fecha_limite_solicitudes` | `timestamp[us]` |  |
| 63 | `hora_limite_solicitudes` | `large_string` |  |
| 64 | `fecha_limite_pliegos` | `timestamp[us]` |  |
| 65 | `hora_limite_pliegos` | `large_string` |  |
| 66 | `programas_financiacion` | `large_string` |  |
| 67 | `pliego_administrativo` | `large_string` |  |
| 68 | `pliego_administrativo_url` | `large_string` |  |
| 69 | `pliego_tecnico` | `large_string` |  |
| 70 | `pliego_tecnico_url` | `large_string` |  |
| 71 | `otros_documentos` | `large_string` |  |
| 72 | `otros_documentos_url` | `large_string` |  |
| 73 | `documentos_generales` | `large_string` |  |
| 74 | `documentos_generales_url` | `large_string` |  |
| 75 | `n_modificaciones` | `int64` |  |
| 76 | `textos_originales` | `large_string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 77 | `zip_historico` | `large_string` |  |
| 78 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 79 | `_origen` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 80 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `licitaciones_completo/licitaciones_completo_sin_ano.parquet`

10.226 filas · 80 columnas · 1 grupo de filas

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
| 26 | `n_lotes` | `int64` |  |
| 27 | `n_resultados` | `int64` |  |
| 28 | `cpv_principal` | `large_string` |  |
| 29 | `cpvs` | `large_string` |  |
| 30 | `ubicacion` | `large_string` |  |
| 31 | `nuts` | `large_string` |  |
| 32 | `duracion` | `large_string` |  |
| 33 | `duracion_unidad` | `large_string` |  |
| 34 | `financiacion_ue` | `large_string` |  |
| 35 | `urgencia` | `large_string` |  |
| 36 | `fecha_limite` | `timestamp[us]` |  |
| 37 | `hora_limite` | `large_string` |  |
| 38 | `fecha_adjudicacion` | `timestamp[us]` |  |
| 39 | `fecha_publicacion` | `timestamp[us]` |  |
| 40 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 41 | `url` | `large_string` |  |
| 42 | `conjunto` | `large_string` |  |
| 43 | `archivo_origen` | `large_string` |  |
| 44 | `tipo_registro` | `large_string` |  |
| 45 | `ano` | `double` |  |
| 46 | `n_versiones` | `int64` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 47 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 48 | `id_consulta` | `large_string` |  |
| 49 | `nombre_consulta` | `large_string` |  |
| 50 | `condiciones` | `large_string` |  |
| 51 | `tipo_condicion` | `large_string` |  |
| 52 | `fecha_planificada` | `timestamp[us]` |  |
| 53 | `fecha_limite_respuestas` | `timestamp[us]` |  |
| 54 | `tipo_condicion_code` | `large_string` |  |
| 55 | `motivo_tipo_condicion` | `large_string` |  |
| 56 | `motivo_seleccion` | `large_string` |  |
| 57 | `adjunto_consulta_url` | `large_string` |  |
| 58 | `sara` | `bool` |  |
| 59 | `sistema_contratacion_code` | `large_string` |  |
| 60 | `forma_presentacion_code` | `large_string` |  |
| 61 | `fecha_inicio_presentacion` | `timestamp[ms]` |  |
| 62 | `fecha_limite_solicitudes` | `timestamp[us]` |  |
| 63 | `hora_limite_solicitudes` | `large_string` |  |
| 64 | `fecha_limite_pliegos` | `timestamp[us]` |  |
| 65 | `hora_limite_pliegos` | `large_string` |  |
| 66 | `programas_financiacion` | `large_string` |  |
| 67 | `pliego_administrativo` | `large_string` |  |
| 68 | `pliego_administrativo_url` | `large_string` |  |
| 69 | `pliego_tecnico` | `large_string` |  |
| 70 | `pliego_tecnico_url` | `large_string` |  |
| 71 | `otros_documentos` | `large_string` |  |
| 72 | `otros_documentos_url` | `large_string` |  |
| 73 | `documentos_generales` | `large_string` |  |
| 74 | `documentos_generales_url` | `large_string` |  |
| 75 | `n_modificaciones` | `int64` |  |
| 76 | `textos_originales` | `large_string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 77 | `zip_historico` | `large_string` |  |
| 78 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 79 | `_origen` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 80 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
