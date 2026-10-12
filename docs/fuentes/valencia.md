# Comunitat Valenciana: REGCON y datos abiertos de la Generalitat

- **En la release:** `valencia.zip`: contratación (REGCON 2013-2026 y DANA, 276K filas), convenios, lobbies (REGIA) y entidades, 32 Parquet y 302.592 filas; y, en `v2026.02/`, las demás categorías y las tablas que han cambiado.
- **Script:** [`scripts/ccaa_valencia.py`](../../scripts/ccaa_valencia.py) (descarga) y [`scripts/ccaa_valencia_parquet.py`](../../scripts/ccaa_valencia_parquet.py) (Parquet).
- **Portal:** [dadesobertes.gva.es](https://dadesobertes.gva.es/) (CKAN).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

Los contratos menores de órganos valencianos que no llegan al REGCON (universidades, Ajuntament de València, Diputación de Alicante) son otra fuente: [Comunitat Valenciana, menores](valencia_menores.md).

Datos del portal [Dades Obertes GVA](https://dadesobertes.gva.es) (CKAN API).

`ccaa_valencia.py` y `ccaa_valencia_parquet.py` aceptan `--categorias contratacion,...` (por defecto, todas); la descarga de producción solo pide `contratacion`.

La tabla y el árbol siguientes son los de v2026.02 (los que estuvieron en Git LFS en el repositorio, ahora en la etiqueta `datos-lfs-v2026.02`), con todas las categorías:

| Categoría | Archivos | Registros | Contenido |
|-----------|----------|-----------|-----------|
| Contratación | 13 | 246K | REGCON 2014-2025 + DANA (2025 incompleto, ver nota) |
| Subvenciones | 52 | 2.2M | Ayudas 2022-2025 + DANA |
| Presupuestos | 4 | 346K | Ejecución 2024-2025 |
| Convenios | 5 | 8K | 2018-2022 |
| Lobbies (REGIA) | 7 | 11K | Único en España |
| Empleo | 42 | 888K | ERE/ERTE 2000-2025, DANA |
| Paro | 283 | 2.6M | Estadísticas LABORA |
| Siniestralidad | 10 | 570K | Accidentes 2015-2024 |
| Patrimonio | 3 | 9K | Inmuebles GVA |
| Entidades | 2 | 94K | Locales + Asociaciones |
| Territorio | 1 | 4K | Centros docentes |
| Turismo | 16 | 383K | Hoteles, VUT, campings... |
| Sanidad | 8 | 189K | Mapa sanitario |
| Transporte | 7 | 993K | Bus interurbano GTFS |

## Archivos

```
valencia/
├── contratacion/          # 13 archivos, 42 MB
├── subvenciones/          # 52 archivos, 26 MB
├── presupuestos/          # 4 archivos, 7 MB
├── convenios/             # 5 archivos, 2 MB
├── lobbies/               # 7 archivos, 0.4 MB  REGIA
├── empleo/                # 42 archivos, 13 MB
├── paro/                  # 283 archivos, 17 MB
├── siniestralidad/        # 10 archivos, 0.6 MB
├── patrimonio/            # 3 archivos, 0.4 MB
├── entidades/             # 2 archivos, 4 MB
├── territorio/            # 1 archivo, 0.4 MB
├── turismo/               # 16 archivos, 17 MB
├── sanidad/               # 8 archivos, 6 MB
└── transporte/            # 7 archivos, 21 MB
```

> ⚠️ Los contratos de 2025 de v2026.02 tienen solo 32 filas (formalizaciones de enero) frente a 37.432 en 2024: el script nunca volvía a descargar un fichero existente y se quedó la primera copia del año. Ahora vuelve a descargar los recursos que el portal ha actualizado (`last_modified`), descubre los años nuevos de cada serie y conserva los ceros a la izquierda de códigos postales, INE y centros (`03001`). En v2026.10 el fichero de 2025 sigue con 32 filas: es lo que sirve el recurso de 2025 del portal (descargado el 2026-09-28).
>
> v2026.10 (`valencia.zip`) solo trae contratación (REGCON 2013-2026 + DANA, 276K filas), convenios, lobbies y entidades; las demás categorías van tal cual, como en v2026.02, en la carpeta `v2026.02/` de `valencia.zip`.

## Datos únicos de Valencia

- **REGIA**: Registro de lobbies único en España (grupos de interés, actividades de influencia)
- **DANA**: Datasets específicos de la catástrofe (contratos, subvenciones, ERTE)
- **ERE/ERTE histórico**: 25 años de datos (2000-2025)
- **Siniestralidad laboral**: 10 años de accidentes de trabajo

En `v2026.02/` de `valencia.zip` van, tal cual, las categorías que no son contratación (subvenciones, presupuestos, empleo, paro, siniestralidad, patrimonio, territorio, turismo, sanidad y transporte), las asociaciones (no están en la descarga actual) y los lobbies, los recursos de gobierno abierto y el fichero de la DANA de v2026.02, cuyo contenido ha cambiado en el portal.

## Uso

- **Un Parquet por año** en `contratacion/` (`Contratos_inscritos_en_el_Registro_Oficial_de_Contratos_de_la_Generalitat_Valenciana_<año>.parquet`); el de la DANA tiene otro esquema (79 columnas frente a 75).
- **2025** tiene 32 filas: es lo que sirve el portal (ver arriba).

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `scripts/ccaa_valencia_parquet.py` | — | Convierte los CSV de Valencia a Parquet |

---

[Todas las fuentes](README.md) · [README](../../README.md)
