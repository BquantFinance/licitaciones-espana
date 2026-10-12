# `andalucia.zip`

Esquema de los Parquet de `andalucia.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/andalucia.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `licitaciones_andalucia.parquet` | 900.931 | 42 |

## `licitaciones_andalucia.parquet`

900.931 filas · 42 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id_expediente` | `int64` |  |
| 2 | `numero_expediente` | `large_string` |  |
| 3 | `titulo` | `large_string` |  |
| 4 | `tipo_contrato` | `large_string` |  |
| 5 | `tipo_contrato_codigo` | `large_string` |  |
| 6 | `organo_contratacion` | `large_string` |  |
| 7 | `codigo_perfil` | `large_string` |  |
| 8 | `codigo_dir3` | `large_string` |  |
| 9 | `estado` | `large_string` |  |
| 10 | `estado_codigo` | `large_string` |  |
| 11 | `importe_licitacion` | `double` |  |
| 12 | `valor_estimado` | `double` |  |
| 13 | `importe_adjudicacion` | `double` |  |
| 14 | `importe_adjudicacion_iva` | `double` |  |
| 15 | `fecha_publicacion` | `large_string` |  |
| 16 | `fecha_limite_presentacion` | `large_string` |  |
| 17 | `anuncio_primera_fecha` | `large_string` |  |
| 18 | `anuncio_ultima_fecha` | `large_string` |  |
| 19 | `adjudicatario_nif` | `large_string` |  |
| 20 | `todos_adjudicatarios_nif` | `large_string` |  |
| 21 | `num_adjudicaciones` | `int64` |  |
| 22 | `codigo_procedimiento` | `large_string` |  |
| 23 | `codigo_tramitacion` | `large_string` |  |
| 24 | `codigo_normativa` | `large_string` |  |
| 25 | `forma_presentacion` | `large_string` |  |
| 26 | `cofinanciado_ue` | `large_string` |  |
| 27 | `subasta_electronica` | `large_string` |  |
| 28 | `sistema_racionalizacion` | `large_string` |  |
| 29 | `cpv` | `large_string` |  |
| 30 | `provincias_ejecucion` | `large_string` |  |
| 31 | `medios_publicacion` | `large_string` |  |
| 32 | `num_lotes` | `int64` |  |
| 33 | `num_anuncios` | `int64` |  |
| 34 | `url_detalle` | `large_string` |  |
| 35 | `adjudicaciones_json` | `large_string` |  |
| 36 | `lotes_json` | `large_string` |  |
| 37 | `anuncios_json` | `large_string` |  |
| 38 | `campos_extra_json` | `large_string` |  |
| 39 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 40 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 41 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_origen` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
