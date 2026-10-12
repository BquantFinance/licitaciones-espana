# Aragón: Gobierno de Aragón y Ayuntamiento de Zaragoza

- **En la release:** `aragon.zip`: 13 Parquet, 267.292 filas (2009-2025).
- **Script:** [`scripts/ccaa_aragon.py`](../../scripts/ccaa_aragon.py). Su docstring es la referencia completa.
- **Portal:** [Aragón Open Data](https://opendata.aragon.es/) (CKAN) y el OCDS del Ayuntamiento de Zaragoza en el registro de Open Contracting Partnership ([zaragoza.es](https://www.zaragoza.es/)).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## Qué trae

Las licitaciones del Gobierno de Aragón publicadas desde 2018 ya llegan por la [PLACSP](placsp.md). Este script añade lo que la PLACSP no tiene: contratos menores, encargos a medios propios, el histórico anterior a 2018 y el Ayuntamiento de Zaragoza.

| Fichero | Contenido | Filas |
|---|---|--:|
| `contratos_gobierno__contratos_gobierno_de_aragon.parquet` | Contratos adjudicados del Gobierno, desde 2009 | 117.770 |
| `contratos_gobierno__contratos_menores_gobierno_de_aragon.parquet` | Contratos menores del Gobierno, 2014-2025 | 90.804 |
| `contratos_gobierno__encargos_de_ejecucion_a_medios_propios_desde.parquet` | Encargos a medios propios | 2.093 |
| `registro_contratos__contratos_menores.parquet` | Registro de Contratos (desde 2023): menores | 13.656 |
| `registro_contratos__contratos_mayores.parquet` | Registro de Contratos: mayores | 8.440 |
| `registro_contratos__encargos_a_medios_propios.parquet` | Registro de Contratos: encargos | 397 |
| `registro_contratos__incidencias.parquet` | Registro de Contratos: incidencias | 357 |
| `anuncios_perfil__anuncios_perfil_contratante_aragon.parquet` | Anuncios del perfil de contratante (hasta el 12-2-2021) | 12.156 |
| `anuncios_perfil__previsualizacion_ultimos_anuncios_perfil_contratante_aragon.parquet` | Vista previa de los últimos anuncios | 10 |
| `zaragoza_ocds_releases.parquet` | Zaragoza, OCDS: releases | 3.305 |
| `zaragoza_ocds_awards.parquet` | Zaragoza, OCDS: adjudicaciones | 3.096 |
| `zaragoza_ocds_contracts.parquet` | Zaragoza, OCDS: contratos | 2.872 |
| `zaragoza_ocds_parties.parquet` | Zaragoza, OCDS: partes | 12.336 |

## Cómo se descarga

- **Aragón Open Data:** `package_show` lista los recursos de cada conjunto y se descargan todos los años y series que publique, sin listas fijas de años. De cada serie y año se baja un formato (CSV > XLSX > XLS > ODS > JSON > XML). Algunos años son SpreadsheetML (`.xls.xml`), que pandas no lee: se parsean con `xml.etree`.
- **Zaragoza:** los JSONL.gz anuales del OCDS, tal cual, aplanados en releases, awards, contracts y parties (una fila por elemento; el resto de listas, como JSON). Con `--zaragoza-api`, también la API REST de contratación.
- Nunca se usa el DataStore de CKAN: se sirven los ficheros originales, con sus versiones en `_historico/`.

## Columnas

Todas las filas y columnas, todo como texto (sin perder ceros a la izquierda ni valores no numéricos). Si los ficheros de una serie tienen esquemas distintos se conservan todas las columnas. No se deduplican filas. Añadidas: `_fuente`, `_dataset`, `_recurso`, `_recurso_id`, `_formato`, `_anio_recurso`, `_archivo_origen`, `_url_origen`, `_hoja`, `_fila_origen`, `_encabezado`, `_fecha_descarga`, las 3 de control y, en el Registro de Contratos, `_razon_social_es_pais` (el portal publica el código de país en lugar del nombre del adjudicatario).

## Avisos

- **Solapes con la PLACSP:** antes de sumar con la PLACSP hay que deduplicar ([`docs/COBERTURA.md`](../COBERTURA.md) §0, «Para sumarlos o cruzarlos»).
- **NIF:** el Registro de Contratos lo trae en el 99,96 %; los contratos del Gobierno, no.
- **Los .xls del Gobierno de 2018-2025** son HTML con la cabecera en `<TH>` fuera de `<TR>`, y el euro escrito con el byte 0xA4: el script los lee bien y saca `€`.
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_aragon.py` | Aragón Open Data + Ayuntamiento de Zaragoza (OCDS) | Contratos del Gobierno de Aragón, Registro de Contratos, anuncios del perfil y OCDS de Zaragoza |

---

[Todas las fuentes](README.md) · [README](../../README.md)
