# `catalunya_menores.zip`

Esquema de los Parquet de `catalunya_menores.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/catalunya.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contractacio_menors.parquet` | 1.429.086 | 46 |

## `contractacio_menors.parquet`

1.429.086 filas · 46 columnas · 2 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `id` | `int64` |  |
| 2 | `titol` | `large_string` |  |
| 3 | `esPlacsp` | `bool` |  |
| 4 | `esAgregatContractes` | `bool` |  |
| 5 | `esAgregatEncarrecs` | `bool` |  |
| 6 | `nomPublicacioAgregada` | `large_string` |  |
| 7 | `descripcio` | `large_string` |  |
| 8 | `pressupostLicitacio` | `double` |  |
| 9 | `pressupostAdjudicacio` | `double` |  |
| 10 | `organ` | `large_string` |  |
| 11 | `idOrgan` | `int64` |  |
| 12 | `codiExpedient` | `large_string` |  |
| 13 | `expedientId` | `large_string` |  |
| 14 | `fasesVigents_ALERTA_FUTURA_lotsActius` | `double` |  |
| 15 | `fasesVigents_ALERTA_FUTURA_dataPublicacio` | `large_string` |  |
| 16 | `fasesVigents_ALERTA_FUTURA_idPublicacio` | `double` |  |
| 17 | `fasesVigents_CONSULTA_PRELIMINAR_MERCAT_lotsActius` | `double` |  |
| 18 | `fasesVigents_CONSULTA_PRELIMINAR_MERCAT_dataPublicacio` | `large_string` |  |
| 19 | `fasesVigents_CONSULTA_PRELIMINAR_MERCAT_idPublicacio` | `double` |  |
| 20 | `fasesVigents_ANUNCI_PREVI_lotsActius` | `double` |  |
| 21 | `fasesVigents_ANUNCI_PREVI_dataPublicacio` | `large_string` |  |
| 22 | `fasesVigents_ANUNCI_PREVI_idPublicacio` | `double` |  |
| 23 | `fasesVigents_ANUNCI_EN_TERMINI_lotsActius` | `double` |  |
| 24 | `fasesVigents_ANUNCI_EN_TERMINI_dataPublicacio` | `large_string` |  |
| 25 | `fasesVigents_ANUNCI_EN_TERMINI_idPublicacio` | `double` |  |
| 26 | `fasesVigents_AVALUACIO_lotsActius` | `double` |  |
| 27 | `fasesVigents_AVALUACIO_dataPublicacio` | `large_string` |  |
| 28 | `fasesVigents_AVALUACIO_idPublicacio` | `double` |  |
| 29 | `fasesVigents_ADJUDICACIO_lotsActius` | `double` |  |
| 30 | `fasesVigents_ADJUDICACIO_dataPublicacio` | `large_string` |  |
| 31 | `fasesVigents_ADJUDICACIO_idPublicacio` | `double` |  |
| 32 | `fasesVigents_ANULACIO_lotsActius` | `double` |  |
| 33 | `fasesVigents_ANULACIO_dataPublicacio` | `large_string` |  |
| 34 | `fasesVigents_ANULACIO_idPublicacio` | `double` |  |
| 35 | `fasesVigents_FORMALITZACIO_lotsActius` | `double` |  |
| 36 | `fasesVigents_FORMALITZACIO_dataPublicacio` | `large_string` |  |
| 37 | `fasesVigents_FORMALITZACIO_idPublicacio` | `double` |  |
| 38 | `fasesVigents_EXECUCIO_lotsActius` | `double` |  |
| 39 | `fasesVigents_EXECUCIO_dataPublicacio` | `large_string` |  |
| 40 | `fasesVigents_EXECUCIO_idPublicacio` | `double` |  |
| 41 | `fasesVigents_ANUNCI_EN_FORMALITZACIO_lotsActius` | `double` |  |
| 42 | `fasesVigents_ANUNCI_EN_FORMALITZACIO_dataPublicacio` | `large_string` |  |
| 43 | `fasesVigents_ANUNCI_EN_FORMALITZACIO_idPublicacio` | `double` |  |
| 44 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 45 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 46 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (1, 1 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/contratacion/contractacio_menors.parquet` | 3.023.802 | 43 |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
