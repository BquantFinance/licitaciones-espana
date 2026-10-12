# `andalucia_menores.zip`

Esquema de los Parquet de `andalucia_menores.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/andalucia_menores.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratos_menores.parquet` | 768.647 | 40 |

## `contratos_menores.parquet`

768.647 filas · 40 columnas · 9 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ID_EXPEDIENTE` | `string` |  |
| 2 | `ORGANO_CONTRATACION` | `string` |  |
| 3 | `NUM_EXPEDIENTE` | `string` |  |
| 4 | `TITULO` | `string` |  |
| 5 | `DESCRIPCION` | `string` |  |
| 6 | `TIPO_CONTRATO` | `string` |  |
| 7 | `PROCEDIMIENTO_ADJUDICACION` | `string` |  |
| 8 | `DURACION_CONTRATO` | `string` |  |
| 9 | `DURACION_MEDIDA` | `string` |  |
| 10 | `LUGAR_EJECUCION_CODIGO` | `string` |  |
| 11 | `LUGAR_EJECUCION_DENOMINACION` | `string` |  |
| 12 | `VALOR_ESTIMADO` | `string` |  |
| 13 | `IMPORTE_ADJUDICACION_SIN_IVA` | `string` |  |
| 14 | `IMPORTE_ADJUDICACION_CON_IVA` | `string` |  |
| 15 | `FINANCIACION_EUROPEA` | `string` |  |
| 16 | `FINANCIADO_POR` | `string` |  |
| 17 | `TASA_COFINANCIACION` | `string` |  |
| 18 | `NUM_LICITADORES_PRESENTADOS` | `string` |  |
| 19 | `NIF_ADJUDICATARIO` | `string` |  |
| 20 | `ADJUDICATARIO_DENOMINACION` | `string` |  |
| 21 | `FECHA_ADJUDICACION` | `string` |  |
| 22 | `FECHA_FORMALIZACION` | `string` |  |
| 23 | `ESTADO` | `string` |  |
| 24 | ` FECHA_ADJUDICACION` | `string` |  |
| 25 | ` FECHA_FORMALIZACION` | `string` |  |
| 26 | `_valor_estimado_num` | `double` | [marca](../ESQUEMA.md#valores-convertidos) |
| 27 | `_importe_adjudicacion_sin_iva_num` | `double` | [marca](../ESQUEMA.md#valores-convertidos) |
| 28 | `_importe_adjudicacion_con_iva_num` | `double` | [marca](../ESQUEMA.md#valores-convertidos) |
| 29 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_conjunto` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 31 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 32 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 33 | `_fichero_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 34 | `_miembro` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 35 | `_lineas_unidas` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 36 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 37 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 39 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 40 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
