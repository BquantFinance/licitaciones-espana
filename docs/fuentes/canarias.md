# Canarias: Gobierno, Las Palmas de Gran Canaria, Cabildo de Tenerife y SCS

- **En la release:** `canarias.zip`: 5 Parquet, 115.318 filas (2016-2026).
- **Script:** [`scripts/ccaa_canarias.py`](../../scripts/ccaa_canarias.py). Su docstring es la referencia completa.
- **Portal:** [datos.canarias.es](https://datos.canarias.es/) (CKAN), el [portal de transparencia del Gobierno](https://www.gobiernodecanarias.org/transparencia/) y los de Las Palmas de Gran Canaria y el Cabildo de Tenerife.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (varía por serie; la de Las Palmas es restrictiva).

## Qué trae

Las fuentes de contratos menores de Canarias que no son la PLACSP ni Santa Cruz de Tenerife (que va en [`municipios.zip`](municipios.md)):

| Fichero | Contenido | Filas |
|---|---|--:|
| `gobierno_contratos.parquet` | Gobierno de Canarias: «Contratos adjudicados y formalizados», CSV del CKAN (desde 2020; todos los procedimientos, contrato a contrato) | 91.550 |
| `las_palmas_gc.parquet` | Ayuntamiento de Las Palmas de Gran Canaria: relación de contratos menores (API de transparencia, 2016-) | 10.702 |
| `cabildo_tenerife.parquet` | Cabildo Insular de Tenerife: relación de contratos menores (API de transparencia, 2019-; sin NIF del adjudicatario) | 6.996 |
| `scs_resumen.parquet` | Servicio Canario de la Salud: «Contratos menores trimestrales» (ODS, 2021-): **agregados** por órgano y tipo (número e importe) | 2.404 |
| `gobierno_resumen.parquet` | Departamentos y organismos del Gobierno, por trimestre (ODT, 3T 2023-): **agregados** por órgano y tipo | 3.666 |

## Avisos

- **El CSV del Gobierno es su PLACSP:** el 99,7 % de sus menores ya está en el feed 1143 de la [PLACSP](placsp.md) (por el `idEvl` de `licitacion_enlace`). Para no contarlos dos veces, crúzalos antes de sumar.
- **Los resúmenes no son contratos.** `scs_resumen` y `gobierno_resumen` son número e importe por órgano y tipo, con las filas de títulos, totales y notas tal como se publican. El SCS declara 89.340 menores en 2025 en sus propios resúmenes y publica unos 1.500 contrato a contrato: sirven de denominador, no de contratos ([`docs/COBERTURA.md`](../COBERTURA.md) §5.2).
- **Nada se limpia:** `_U` («no consta») y `_Z` («no aplica») del Gobierno se quedan como texto; cuando la cabecera cambia de un fichero a otro, el mismo dato queda en columnas distintas, como se publica. Los nombres que empiezan por `_` y no son del script son del portal (la API de Las Palmas trae `_esExterno`, `_idExterno`, `_idLocal` e `_identidad`).
- **Sin semilla:** ninguna de estas fuentes estaba en la release v2026.02.

## Columnas añadidas

- De origen: `_fuente`, `_archivo_origen`, `_fecha_descarga`, `_anio`, `_trimestre`, `_periodo`, `_departamento`, `_organismo`, `_recurso`, `_fichero_publicado`, `_obligacion` y `_hoja`.
- Del contenido de los agregados: `_titulo_tabla`, `_seccion`, `_tabla`, `_cabecera_tabla`, `_parrafo_previo` y `_pie`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_canarias.py` | datos.canarias.es y portales de transparencia de Canarias | Contratos menores de Canarias: CSV de contratos del Gobierno (2020-, copia de la PLACSP), API de Las Palmas de GC (2016-) y del Cabildo de Tenerife (2019-), y los resúmenes agregados del SCS (ODS) y de los departamentos (ODT); ficheros tal cual con versiones y un Parquet acumulado por serie |

---

[Todas las fuentes](README.md) · [README](../../README.md)
