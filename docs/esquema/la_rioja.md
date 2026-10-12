# `la_rioja.zip`

Esquema de los Parquet de `la_rioja.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/la_rioja.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratos_menores.parquet` | 351.203 | 17 |

## `contratos_menores.parquet`

351.203 filas · 17 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `COD_CONTRATO` | `string` |  |
| 2 | `DEPARTAMENTO` | `string` |  |
| 3 | `TIPO_EXPEDIENTE` | `string` |  |
| 4 | `TERC_CIF` | `string` |  |
| 5 | `TERC_NOMBRE` | `string` |  |
| 6 | `CONCEPTO` | `string` |  |
| 7 | `FECHA` | `string` |  |
| 8 | `IMPORTE_EJERCICIO` | `string` |  |
| 9 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 10 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_anio_fichero` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 15 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
