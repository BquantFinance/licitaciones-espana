# `nacional_criterios.zip`

Esquema de los Parquet de `nacional_criterios.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/placsp.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_completo_criterios.parquet` | 14.571.218 | 13 |

## `licitaciones_completo_criterios.parquet`

14.571.218 filas · 13 columnas · 14 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `conjunto` | `large_string` |  |
| 4 | `lote` | `large_string` |  |
| 5 | `orden_criterio` | `int64` |  |
| 6 | `tipo_criterio_code` | `large_string` |  |
| 7 | `subtipo_criterio_code` | `large_string` |  |
| 8 | `descripcion` | `large_string` |  |
| 9 | `peso` | `double` |  |
| 10 | `nota` | `large_string` |  |
| 11 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 12 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 13 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
