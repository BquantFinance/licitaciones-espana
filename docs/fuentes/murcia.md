# Región de Murcia: CARM y Servicio Murciano de Salud

- **En la release:** `murcia.zip`: 3 Parquet, 1.008.568 filas (2014-2025).
- **Script:** [`scripts/ccaa_murcia.py`](../../scripts/ccaa_murcia.py). Su docstring es la referencia completa.
- **Portal:** [datosabiertos.carm.es](https://datosabiertos.regiondemurcia.es/) (CSV anuales y catálogo CKAN) y el [portal de transparencia](https://transparencia.carm.es/) para el Servicio Murciano de Salud.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## Qué trae

| Fichero | Contenido | Filas |
|---|---|--:|
| `contratos_carm.parquet` | Contratos de la CARM inscritos en el registro, sin menores (`contratosOD{año}.csv`) | 15.399 |
| `contratos_menores_carm.parquet` | Contratos menores de la CARM (`CONTRA_ContratosMenores_{año}.csv`) | 244.185 |
| `contratos_menores_sms.parquet` | Contratos menores del Servicio Murciano de Salud, 2019-2025 (XLSX o XLS; cada año con otro nombre), con NIF | 748.984 |

## Cómo se descarga

- Cada serie se sondea año a año desde 2010 hasta el año en curso; el catálogo CKAN regional se guarda como inventario y añade los años que enlace.
- Se completan los años que faltan y se vuelven a pedir el año en curso y el anterior (el fichero de un año sigue creciendo en los primeros meses del siguiente); los antiguos, con `--comprobar-todo`. Si un fichero que se tenía pasa a dar 404, se marca como retirado y sus filas se conservan.

## Columnas añadidas

`_fuente`, `_dataset`, `_anio_fichero`, `_archivo_origen`, `_hoja`, `_fecha_descarga` y las 3 de control. En los CSV que el portal genera desde JSON, además, `_resto_json` (restos de la lista JSON que no son de ningún campo) y `_<columna>_sin_cortes` (el texto sin los cortes de línea cada 80 caracteres; la columna original se sirve tal cual).

## Avisos

- **Solape con la PLACSP:** la CARM publica sus menores en el feed 1143 desde 2022. Para sumarlos con la PLACSP hay que casar por contenido ([`docs/COBERTURA.md`](../COBERTURA.md) §0).
- **NIF de personas físicas** enmascarados.
- **Codificación:** los `contratosOD` de 2014-2018 están en cp850; el script la detecta.
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_murcia.py` | CARM + Servicio Murciano de Salud | Contratos y contratos menores de la CARM y menores del SMS |

---

[Todas las fuentes](README.md) · [README](../../README.md)
