# `borme.zip`

Esquema de los Parquet de `borme.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/borme.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `borme_cargos_pub.parquet` | 17.774.035 | 14 |
| `borme_empresas_pub.parquet` | 9.610.713 | 21 |

## `borme_cargos_pub.parquet`

17.774.035 filas · 14 columnas · 17 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `fecha_borme` | `timestamp[ns]` |  |
| 2 | `num_entrada` | `large_string` |  |
| 3 | `empresa` | `large_string` |  |
| 4 | `empresa_norm` | `large_string` |  |
| 5 | `provincia` | `large_string` |  |
| 6 | `hoja_registral` | `large_string` |  |
| 7 | `tipo_acto` | `large_string` |  |
| 8 | `cargo` | `large_string` |  |
| 9 | `persona_hash` | `large_string` |  |
| 10 | `pdf_filename` | `large_string` |  |
| 11 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 14 | `_origen` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `borme_empresas_pub.parquet`

9.610.713 filas · 21 columnas · 10 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `fecha_borme` | `timestamp[ns]` |  |
| 2 | `num_borme` | `int64` |  |
| 3 | `num_entrada` | `large_string` |  |
| 4 | `empresa` | `large_string` |  |
| 5 | `empresa_norm` | `large_string` |  |
| 6 | `provincia` | `large_string` |  |
| 7 | `cod_provincia` | `large_string` |  |
| 8 | `tipo_borme` | `large_string` |  |
| 9 | `actos` | `large_string` |  |
| 10 | `domicilio` | `large_string` |  |
| 11 | `capital_euros` | `double` |  |
| 12 | `fecha_constitucion` | `large_string` |  |
| 13 | `hoja_registral` | `large_string` |  |
| 14 | `tomo` | `large_string` |  |
| 15 | `inscripcion` | `large_string` |  |
| 16 | `fecha_inscripcion` | `large_string` |  |
| 17 | `pdf_filename` | `large_string` |  |
| 18 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_origen` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
