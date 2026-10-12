# Castilla y León: Junta y SACYL

- **En la release:** `castilla_leon.zip`: 15 Parquet, 185.058 filas (2005-2026).
- **Script:** [`scripts/ccaa_castilla_leon.py`](../../scripts/ccaa_castilla_leon.py). Su docstring es la referencia completa.
- **Portal:** el portal de análisis de datos abiertos de la Junta, [analisis.datosabiertos.jcyl.es](https://analisis.datosabiertos.jcyl.es) (OpenDataSoft, API Explore v2.1), y el histórico del perfil de contratante anterior al 17-4-2018 ([datosabiertos.jcyl.es](https://datosabiertos.jcyl.es/)).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## Qué trae

Todos los conjuntos de contratación del portal (los que tienen «contrat», «licitac» o «adjudica» en el id, el título o las palabras clave), un Parquet por conjunto con todas las columnas y filas publicadas, como texto:

| Fichero | Filas |
|---|--:|
| `contratos-menores.parquet` | 128.232 |
| `contratos-menores-sacyl.parquet` | 15.450 |
| `contratos-ordinarios.parquet` | 9.462 |
| `contratos-ordinarios-sacyl.parquet` | 7.453 |
| `contratos-basados-en-acuerdo-marco-de-sacyl.parquet` | 7.049 |
| `contratos-desistimiento-renuncia-desierto.parquet` | 4.416 |
| `contratos-agregado.parquet` | 3.116 |
| `contratos-realizados-en-las-provincias-de-castilla-y-leon.parquet` | 2.600 |
| `contratos-de-emergencia.parquet` | 2.594 |
| `contratos-basados-en-acuerdo-marco.parquet` | 2.290 |
| `contratos-modificados-sacyl.parquet` | 1.479 |
| `contratos-modificados.parquet` | 439 |
| `contratos-agroambiente-clima-sac.parquet` | 424 |
| `contratos-agricultura-ecologica-sac.parquet` | 53 |
| `licitacion-de-obras-publicas.parquet` | 1 |

## Cómo se descarga

- El catálogo se recorre entero; `DATASETS_CONOCIDOS` solo es un respaldo si no responde.
- Cada conjunto se exporta completo (`exports/csv`, sin límite de filas), con los mismos parámetros que el botón «Exportar» del portal (etiquetas publicadas, fechas en hora de Madrid). Solo se vuelve a pedir si el catálogo indica cambios desde la última comprobación, o con `--comprobar-todo`.
- Todo fichero pasa por `comun.historico.guardar_version`; los Parquet acumulan todas las versiones.

## Columnas añadidas

`_fuente` (URL descargada), `_dataset`, `_archivo_origen`, `_fecha_descarga` y las 3 de control. Los nombres técnicos de cada columna están en la ficha del conjunto (`raw/datasets/<id>.metadatos.json`).

## Avisos

- **Solape con la PLACSP:** los perfiles de la Junta están en la PLACSP desde el 17-4-2018. Entre el 94 % y el 98 % de los menores de la Junta de 2019-2025 ya está en el feed 1143; para cruzarlos, el `idEvl` de su «Enlace de publicación» es el mismo que el del `url` de la PLACSP ([`docs/COBERTURA.md`](../COBERTURA.md) §0).
- El SACYL publica unos 2.400 menores al año: probablemente solo los de caja fija (art. 63.4 LCSP).
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_castilla_leon.py` | Datos abiertos de la Junta de Castilla y León | Contratos ordinarios, menores, basados en acuerdo marco, modificados… de la Junta y del SACYL |

---

[Todas las fuentes](README.md) · [README](../../README.md)
