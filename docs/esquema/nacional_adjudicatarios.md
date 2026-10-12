# `nacional_adjudicatarios.zip`

Esquema de los Parquet de `nacional_adjudicatarios.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/placsp.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_completo_adjudicatarios.parquet` | 10.146.626 | 13 |

## `licitaciones_completo_adjudicatarios.parquet`

10.146.626 filas · 13 columnas · 10 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `large_string` |  |
| 2 | `expediente` | `large_string` |  |
| 3 | `conjunto` | `large_string` |  |
| 4 | `lote` | `large_string` |  |
| 5 | `orden_resultado` | `int64` |  |
| 6 | `orden_adjudicatario` | `int64` |  |
| 7 | `adjudicatario` | `large_string` |  |
| 8 | `nif_adjudicatario` | `large_string` |  |
| 9 | `tipo_id_adjudicatario` | `large_string` |  |
| 10 | `ids_adjudicatario` | `large_string` |  |
| 11 | `fecha_updated` | `timestamp[us, tz=UTC]` |  |
| 12 | `es_ultima_version` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |
| 13 | `entrada_repetida` | `bool` | [marca](../ESQUEMA.md#versiones-de-la-placsp) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
