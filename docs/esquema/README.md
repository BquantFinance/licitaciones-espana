# Esquema de la release

Columnas, tipos y filas de cada Parquet de la release v2026.10, una página por ZIP. Se genera leyendo solo los pies de los Parquet con [`scripts/esquema_release.py`](../../scripts/esquema_release.py); las marcas `_…` que se repiten en varias fuentes se explican en [`docs/ESQUEMA.md`](../ESQUEMA.md).

31 ZIP, 142 Parquet y 115.160.695 filas, sin contar las carpetas `v2026.02/`.

| ZIP | Parquet | Filas | En `v2026.02/` |
|---|--:|--:|--:|
| [`andalucia.zip`](andalucia.md) | 1 | 900.931 | — |
| [`andalucia_menores.zip`](andalucia_menores.md) | 1 | 768.647 | — |
| [`aragon.zip`](aragon.md) | 13 | 267.292 | — |
| [`asturias.zip`](asturias.md) | 1 | 375.380 | — |
| [`borme.zip`](borme.md) | 2 | 27.384.748 | — |
| [`calidad_licitaciones_resultado.zip`](calidad_licitaciones_resultado.md) | 0 | 0 | 1 |
| [`canarias.zip`](canarias.md) | 5 | 115.318 | — |
| [`castilla_la_mancha.zip`](castilla_la_mancha.md) | 6 | 6.945.081 | — |
| [`castilla_leon.zip`](castilla_leon.md) | 15 | 185.058 | — |
| [`catalunya.zip`](catalunya.md) | 14 | 8.048.140 | 23 |
| [`catalunya_menores.zip`](catalunya_menores.md) | 1 | 1.429.086 | 1 |
| [`comunidad_madrid.zip`](comunidad_madrid.md) | 1 | 4.906.068 | — |
| [`contratos_galicia.zip`](contratos_galicia.md) | 2 | 3.697.540 | 1 |
| [`euskadi.zip`](euskadi.md) | 9 | 2.753.290 | 4 |
| [`extremadura.zip`](extremadura.md) | 3 | 221.749 | — |
| [`la_rioja.zip`](la_rioja.md) | 1 | 351.203 | — |
| [`madrid_ayuntamiento.zip`](madrid_ayuntamiento.md) | 2 | 264.174 | — |
| [`municipios.zip`](municipios.md) | 8 | 289.747 | — |
| [`murcia.zip`](murcia.md) | 3 | 1.008.568 | — |
| [`nacional_adjudicatarios.zip`](nacional_adjudicatarios.md) | 1 | 10.146.626 | — |
| [`nacional_criterios.zip`](nacional_criterios.md) | 1 | 14.571.218 | — |
| [`nacional_licitaciones_2020_2021.zip`](nacional_licitaciones_2020_2021.md) | 1 | 1.718.374 | — |
| [`nacional_licitaciones_2022_2023.zip`](nacional_licitaciones_2022_2023.md) | 1 | 2.564.085 | — |
| [`nacional_licitaciones_2024_2025.zip`](nacional_licitaciones_2024_2025.md) | 1 | 3.036.783 | — |
| [`nacional_licitaciones_desde_2026.zip`](nacional_licitaciones_desde_2026.md) | 1 | 1.970.904 | — |
| [`nacional_licitaciones_hasta_2019.zip`](nacional_licitaciones_hasta_2019.md) | 2 | 1.617.421 | — |
| [`nacional_lotes_y_otras_tablas.zip`](nacional_lotes_y_otras_tablas.md) | 4 | 5.903.920 | — |
| [`nacional_resultados.zip`](nacional_resultados.md) | 1 | 10.677.644 | — |
| [`ted.zip`](ted.md) | 1 | 857.017 | 7 |
| [`valencia.zip`](valencia.md) | 32 | 302.592 | 436 |
| [`valencia_menores.zip`](valencia_menores.md) | 8 | 1.882.091 | — |

Para regenerarlo con otra release, descomprimido cada ZIP en su carpeta (`unzip X.zip -d X`):

```bash
python scripts/esquema_release.py --datos CARPETA --release vAAAA.MM --manifest CARPETA/MANIFEST.csv
```
