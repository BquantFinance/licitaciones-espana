# `nacional_resultados.zip`

Esquema de los Parquet de `nacional_resultados.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/placsp.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_completo_resultados.parquet` | 10.677.644 | 28 |

## `licitaciones_completo_resultados.parquet`

10.677.644 filas · 28 columnas · 11 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `conjunto` | `large_string` |  |
| 4 | `lote` | `large_string` |  |
| 5 | `resultado_code` | `large_string` |  |
| 6 | `adjudicatario` | `large_string` |  |
| 7 | `nif_adjudicatario` | `large_string` |  |
| 8 | `importe_adjudicacion` | `double` |  |
| 9 | `importe_adj_con_iva` | `double` |  |
| 10 | `fecha_adjudicacion` | `timestamp[us]` |  |
| 11 | `num_ofertas` | `double` |  |
| 12 | `es_pyme` | `bool` |  |
| 13 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 14 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 15 | `orden_resultado` | `int64` |  |
| 16 | `n_adjudicatarios` | `int64` |  |
| 17 | `adjudicatarios_todos` | `large_string` |  |
| 18 | `nifs_adjudicatarios_todos` | `large_string` |  |
| 19 | `descripcion_resultado` | `large_string` |  |
| 20 | `oferta_mas_baja` | `double` |  |
| 21 | `oferta_mas_alta` | `double` |  |
| 22 | `num_ofertas_pyme` | `double` |  |
| 23 | `contadores_ofertas` | `large_string` |  |
| 24 | `ofertas_anormalmente_bajas` | `bool` |  |
| 25 | `num_contrato` | `large_string` |  |
| 26 | `fecha_formalizacion` | `timestamp[us]` |  |
| 27 | `fecha_inicio_contrato` | `timestamp[us]` |  |
| 28 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
