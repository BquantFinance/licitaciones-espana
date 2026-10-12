# Castilla-La Mancha: Junta, SESCAM, sector público y UCLM

- **En la release:** `castilla_la_mancha.zip`: 6 Parquet, 6.945.081 filas (2015-2026).
- **Script:** [`scripts/ccaa_castilla_la_mancha.py`](../../scripts/ccaa_castilla_la_mancha.py). Su docstring es la referencia completa.
- **Portal:** los ficheros de transparencia de [contratacion.castillalamancha.es](https://contratacion.castillalamancha.es/) y los contratos menores de la [Universidad de Castilla-La Mancha](https://contratos.apps.uclm.es/).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (la de la UCLM es restrictiva).

## Qué trae

Los contratos menores que publica la propia comunidad fuera de la PLACSP. Un Parquet por conjunto; la columna `_unidad` dice qué es cada fila:

| Fichero | Contenido | Filas |
|---|---|--:|
| `menores_junta.parquet` | Junta (gestor PICOS): ficheros trimestrales y anuales desde 2019, una fila por contrato | 268.744 |
| `caja_pagadora.parquet` | Menores pagados por caja pagadora, desde 2026: una fila por factura, con NIF | 16.662 |
| `sector_publico.parquet` | «Sector público regional» 2015-2018, un fichero por consejería u organismo (unas 440 columnas en total) | 1.923.536 |
| `sescam.parquet` | Servicio de Salud (SESCAM), 2019-: **cada fila es una línea de factura, no un contrato**, y sin NIF | 4.162.351 |
| `informe_menores.parquet` | «Informe de Contratación Administrativa del Sector Público» 2019-2022: datos estadísticos | 319.658 |
| `uclm.parquet` | Universidad de Castilla-La Mancha: menores por ejercicio (2017-), con NIF | 254.130 |

## Avisos

- **Facturas no son contratos.** `sescam` (y los ficheros del SESCAM dentro de `sector_publico`) va por línea de factura: 4,16 M de los 6,95 M de filas. Sepáralo antes de contar contratos.
- **Solapes.** El fichero anual de la Junta se solapa con los trimestrales y no siempre es su suma (2023: 50.069 filas frente a 36.451): `_periodo` (1T…4T / anual) los distingue. Los de 2023 incluyen la UCLM, que también está en `uclm`.
- **El CIF** va en su columna desde 2022, dentro del texto del adjudicatario en 2020-2021 y no está en los tres primeros trimestres de 2019. Los nombres de columna cambian de un año a otro y se dejan como vienen.
- **RAR.** Algunos ficheros son RAR: hace falta `rarfile` con un programa (`unrar`, `unar`, `7z` o `bsdtar`) o `libarchive-c`. Un RAR que no se puede leer se conserva y queda pendiente (código de salida 1).
- **Una hoja .xls de 65.536 filas** (el máximo del formato) puede estar truncada en origen: le pasa al SESCAM de 2015, y se avisa.
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Columnas añadidas

`_fuente`, `_dataset`, `_titulo`, `_anio`, `_trimestre`, `_periodo`, `_unidad`, `_archivo_origen`, `_miembro` (fichero dentro del ZIP o RAR), `_hoja`, `_fecha_descarga` y las 3 de control.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_castilla_la_mancha.py` | Junta de Castilla-La Mancha + UCLM | Menores, caja pagadora, SESCAM por línea de factura, sector público 2015-2018 y contratos de la UCLM |

---

[Todas las fuentes](README.md) · [README](../../README.md)
