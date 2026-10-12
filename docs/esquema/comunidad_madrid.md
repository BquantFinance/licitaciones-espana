# `comunidad_madrid.zip`

Esquema de los Parquet de `comunidad_madrid.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/comunidad_madrid.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratacion_comunidad_madrid_completo.parquet` | 4.906.068 | 22 |

## `contratacion_comunidad_madrid_completo.parquet`

4.906.068 filas · 22 columnas · 5 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Tipo de Publicación` | `string` |  |
| 2 | `Estado` | `string` |  |
| 3 | `Entidad Adjudicadora` | `string` |  |
| 4 | `Nº Expediente` | `string` |  |
| 5 | `Referencia` | `string` |  |
| 6 | `Título del contrato` | `string` |  |
| 7 | `Tipo de contrato` | `string` |  |
| 8 | `Procedimiento de adjudicación` | `string` |  |
| 9 | `Presupuesto de licitación` | `string` |  |
| 10 | `Nº de ofertas` | `string` |  |
| 11 | `Resultado` | `string` |  |
| 12 | `NIF del adjudicatario` | `string` |  |
| 13 | `Adjudicatario` | `string` |  |
| 14 | `Fecha del contrato` | `string` |  |
| 15 | `Importe de adjudicación` | `string` |  |
| 16 | `Importe de las modificaciones` | `string` |  |
| 17 | `Importe de las prórrogas` | `string` |  |
| 18 | `Importe de la liquidación` | `string` |  |
| 19 | `_archivo_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 22 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
