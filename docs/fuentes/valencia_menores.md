# Comunitat Valenciana: contratos menores fuera del REGCON

- **En la release:** `valencia_menores.zip`: 8 Parquet, 1.882.091 filas (2015-2026).
- **Script:** [`scripts/ccaa_valencia_menores.py`](../../scripts/ccaa_valencia_menores.py). Su docstring es la referencia completa, con el detalle de cada portal.
- **Portal:** el de cada organismo (ver abajo).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (varias son restrictivas).

## Qué trae

Los contratos menores de órganos valencianos que no llegan (o llegan muy poco) a la PLACSP ni al Registro de Contratos de la Generalitat ([REGCON](valencia.md)):

| Fichero | Organismo y formato | Filas |
|---|---|--:|
| `universitat_valencia.parquet` | Universitat de València: relaciones trimestrales XLSX/XLS 2015-2026 | 762.281 |
| `universitat_valencia_va.parquet` | Universitat de València: lo que solo enlaza la página en valenciano | 333.752 |
| `universitat_politecnica_valencia.parquet` | Universitat Politècnica de València: CSV anuales del CKAN `upvtransparent` 2015-2026 | 618.731 |
| `universidad_alicante.parquet` | Universidad de Alicante: relaciones trimestrales XLSX 2018-2026 | 97.638 |
| `diputacion_alicante.parquet` | Diputación de Alicante: registro LIGATE, XLSX 2016-2026 | 33.473 |
| `diputacion_alicante_va.parquet` | Diputación de Alicante: versiones en valenciano | 2.009 |
| `ajuntament_valencia.parquet` | Ajuntament de València: buscador de contratos menores (HTML) 2017-2026 | 19.258 |
| `universidad_miguel_hernandez.parquet` | Universidad Miguel Hernández: relaciones trimestrales XLSX 2018-2026 | 14.949 |

## Avisos: para contar sin duplicados

- Quédate con `_en_ultima_descarga=True` y `_tabla_dinamica` distinto de `"resumen"`, y **deduplica por el identificador del contrato**:
  - en la UV hay ficheros acumulados (en 2024, 1r-2n, 1r-2n-3er y anual; desde el 3T de 2025, los de gastos menores y otros gastos acumulan el año): deduplica por «IDENTIFICADOR CONTRATO», «Identificador» o «Núm. Expediente»;
  - en València, una misma fila puede salir en dos consultas seguidas: `_num_contrato` la identifica.
- **Los Parquet `*_va`** solo tienen las versiones en valenciano de ficheros que ya están en castellano: no los sumes con los otros.
- **Tablas dinámicas de la UV (2025-2026):** la hoja visible es un resumen por unidad y adjudicatario (`_tabla_dinamica="resumen"`); los registros uno a uno, leídos de la caché de la tabla dinámica, llevan `_tabla_dinamica="registros"`.
- **NIF:** la UV lo trae en todos los años (97,9 % válido) y València en el 100 %, con las personas físicas enmascaradas; la Diputación, la UA y la UPV no lo traen ([`docs/COBERTURA.md`](../COBERTURA.md) §0.1).
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Columnas añadidas

`_fuente`, `_dataset`, `_anio`, `_trimestre`, `_mes`, `_periodo`, `_categoria`, `_estado_consulta`, `_consulta_desde` y `_consulta_hasta` (buscador de València; hasta exclusiva), `_archivo_origen`, `_hoja`, `_titulo_hoja`, `_tabla_dinamica`, `_fecha_descarga` y las 3 de control. En el buscador de València, además, `_num_contrato` y la columna «`<columna>` (enlace)» con la URL de la ficha.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_valencia_menores.py` | UV, Ajuntament de València, Diputación de Alicante, UA, UMH y UPV | Contratos menores valencianos fuera del REGCON |

---

[Todas las fuentes](README.md) · [README](../../README.md)
