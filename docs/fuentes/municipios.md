# Contratos menores de 8 ayuntamientos grandes

- **En la release:** `municipios.zip`: un Parquet por municipio, 289.747 filas (2015-2026).
- **Script:** [`scripts/municipios_menores.py`](../../scripts/municipios_menores.py). Su docstring es la referencia completa, con el detalle de cada portal.
- **Portal:** el portal de transparencia o de datos abiertos de cada ayuntamiento.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#ayuntamientos) (varias son restrictivas o no tienen licencia).

## Qué trae

Solo ~30 % de los municipios carga sus contratos menores en el feed 1143 de la [PLACSP](placsp.md). Este script descarga los que publican en su propio portal algunos ayuntamientos grandes con poco o nada en el 1143:

| Fichero | Municipio (código INE) | Filas | Notas |
|---|---|--:|---|
| `valladolid_menores.parquet` | Valladolid (47186) | 154.487 | Operaciones contables (SICALWIN) de todos los procedimientos; ficheros **acumulados** por año (ver avisos) |
| `gijon_menores.parquet` | Gijón (33024) | 64.187 | Ayuntamiento y organismos; CIF enmascarado en personas físicas |
| `fuenlabrada_menores.parquet` | Fuenlabrada (28058) | 34.080 | Con CIF; filas de subtotal intercaladas, como se publican |
| `malaga_menores.parquet` | Málaga (29067) | 16.501 | Ayuntamiento y CEMI; CIF solo en algunos trimestres |
| `vigo_menores.parquet` | Vigo (36057) | 12.392 | Sin NIF |
| `santa_cruz_tenerife_menores.parquet` | Santa Cruz de Tenerife (38038) | 3.485 | 2023-2024, sin adjudicatario ni NIF |
| `cordoba_menores.parquet` | Córdoba (14021) | 2.596 | |
| `leganes_menores.parquet` | Leganés (28074) | 2.019 | Con NIF |

## Cómo se descarga

- De cada municipio se lee la lista de ficheros que publica (página, CKAN o URL fija) y se bajan los que son tablas (CSV, JSON, XLS, XLSX, ODS). Los PDF, DOCX, ODT y ZIP de PDF no se extraen: se anotan en el inventario y en el resumen.
- Se vuelven a pedir los del año en curso y el anterior; los más antiguos, con `--comprobar-todo`. Lo que la lista deja de enlazar queda como retirado (sus filas se conservan), pero solo si la lista se ha podido leer entera.
- Los errores de origen permanentes (un enlace roto del portal que falla igual en cada intento) se anotan en `raw/_fallos_origen.json` y se siguen pidiendo.

## Columnas

Las publicadas, con el nombre que trae cada fichero: **no se unifican entre municipios ni entre años**. Añadidas: `_fuente`, `_municipio`, `_codigo_ine`, `_anio`, `_trimestre`, `_mes` (periodo que el portal asigna al fichero; puede ser un tramo, como `1-3` o `2021-2023`), `_titulo`, `_archivo_origen`, `_hoja`, `_titulo_tabla` (filas de encima de la cabecera), `_fecha_descarga` y las 3 de control.

## Avisos

- **Valladolid publica ficheros acumulados** (1T, 1T-2T, 1T-3T, año): `_trimestre` vale `1`, `1-2`, `1-3`, `1-4`. Para contar, quédate con el último de cada año. Son operaciones contables de todos los procedimientos (columna `PROCEDIMIENTO`), no solo menores.
- **Huecos de origen** en v2026.10: Leganés, agosto de 2026 (el portal devuelve una página HTML), y Málaga, 4.º trimestre de 2020 (404; solo está en PDF).
- **Solape con la PLACSP:** unos 2/3 de los menores de 2025 del Ayuntamiento de Málaga también están en el feed 1143 (mismo NIF e importe).
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/municipios_menores.py` | 8 ayuntamientos | Contratos menores de Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife |

---

[Todas las fuentes](README.md) · [README](../../README.md)
