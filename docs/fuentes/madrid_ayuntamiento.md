# Ayuntamiento de Madrid

- **En la release:** `madrid_ayuntamiento.zip`: tabla unificada (`actividad_contractual_madrid_completo.parquet`) y tabla fiel (`actividad_contractual_madrid_original.parquet`), 132.087 filas cada una. En v2026.02, `madrid_ayuntamiento.zip` era por error una copia de `comunidad_madrid.zip`: v2026.02 no tiene datos del Ayuntamiento.
- **Script:** [`comunidad_madrid/ccaa_madrid_ayuntamiento.py`](../../comunidad_madrid/ccaa_madrid_ayuntamiento.py).
- **Portal:** [datos.madrid.es](https://datos.madrid.es/) (CKAN).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#ayuntamientos).

Actividad contractual completa del [Ayuntamiento de Madrid](https://datos.madrid.es), unificando los ficheros CSV del portal, con 11 estructuras distintas, en un único dataset normalizado.

Recuento por categoría de una versión anterior del script (v2026.10, regenerado, tiene 132.087 filas):

| Categoría | Registros | Importe total |
|-----------|-----------|---------------|
| Contratos menores | 68,626 | 407M € |
| Contratos formalizados | 17,991 | 16,606M € |
| Acuerdo marco / sist. dinámico | 24,621 | 2,549M € |
| Prorrogados | 4,441 | 2,967M € |
| Modificados | 1,789 | 718M € |
| Cesiones | 30 | 80M € |
| Resoluciones | 225 | 62M € |
| Penalidades | 483 | 13M € |
| Homologación | 1,047 | 1M € |
| **Total** | **119,253** | **~23,400M €** |

> ⚠️ datos.madrid.es migró a CKAN y renumeró los recursos: el script descubre ahora los ficheros por la API CKAN (incluido 2026), ya no descarta en silencio ficheros con nombre repetido, vuelve a bajar el año en curso y el anterior, y decodifica los CSV en CP850 (antes "Descripci¢n", "A¤o"…).

## Archivos

El script `ccaa_madrid_ayuntamiento.py` genera, en `--output-dir`:

- `actividad_contractual_madrid_original.parquet` (**tabla fiel**, 207 columnas en v2026.10): todas las filas de los ficheros consolidados, con sus columnas, nombres de columna y texto originales, más `_dataset`, `_recurso`, `_descripcion`, `_categoria`, `_anio_fichero`, `_parte`, `_formato`, `_estructura`, `_archivo_origen`, `_fila_origen`, `_encabezado`, `_fecha_descarga`, `_duplicado` (idéntica a otra fila anterior del mismo recurso), `_repetido_en_csv` y las 3 de control.
- `actividad_contractual_madrid_completo.parquet` (**tabla unificada**, 113 columnas): las mismas filas, mapeadas a columnas comunes; importes a número y fechas a fecha, con el texto publicado en `<columna>_texto`; `tipo_contrato_normalizado` y `pyme_normalizado`; las filas vacías siguen, con `_fila_vacia=True`.
- `originales/` (los CSV, XLSX, XLS y PDF tal cual, con su `_manifiesto.json` y `_historico/`) e `informes/` (lectura de cada fichero, líneas fuera de tabla y comparación CSV frente a XLSX).

## Campos principales (70+ columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | n_registro_contrato, n_expediente, fuente_fichero, categoria |
| Organización | centro_seccion, organo_contratacion, organismo_contratante |
| Objeto | objeto_contrato, tipo_contrato, subtipo_contrato, codigo_cpv |
| Licitación | importe_licitacion_iva_inc, n_licitadores_participantes, n_lotes |
| Adjudicación | importe_adjudicacion_iva_inc, nif_adjudicatario, razon_social_adjudicatario, pyme |
| Fechas | fecha_adjudicacion, fecha_formalizacion, fecha_inicio, fecha_fin |
| Derivados (A.M.) | n_contrato_derivado, objeto_derivado, fecha_aprobacion_derivado |
| Incidencias | tipo_incidencia, importe_modificacion, importe_prorroga, importe_penalidad |
| Cesiones | adjudicatario_cedente, cesionario, importe_cedido |
| Resoluciones | causas_generales, causas_especificas, fecha_acuerdo_resolucion |
| Homologación | n_expediente_sh, objeto_sh, duracion_procedimiento |

## Estructuras detectadas

El script detecta y unifica automáticamente 11 estructuras de CSV distintas:

| Estructura | Período | Categorías |
|------------|---------|------------|
| A, B, C, D | 2015-2020 | Contratos menores |
| E, F | 2021-2025 | Contratos menores |
| AC_OLD | 2015-2020 | Formalizados, acuerdo marco |
| AC_OLD_MOD | 2015-2020 | Modificados |
| AC_HOMOLOGACION | 2022-2024 | Homologación |
| AC_NEW | 2021-2024 | Todas las categorías |
| AC_2025 | 2025 | Todas las categorías |

## Fuentes

El portal es un CKAN con dos conjuntos de datos, que el script lee con `package_show`:

- `300253-0-contratos-actividad-menores`: contratos menores, un fichero por año desde 2015 (2021 partido en «hasta febrero» y «desde marzo»).
- `216876-0-contratos-actividad`: registro de contratos, un fichero por categoría y año desde 2015.

Las URL antiguas del portal (`egob/…`) dan 404.

## Uso

- **Filas repetidas en origen:** se conservan, marcadas en `_duplicado` (idéntica a otra fila anterior del mismo recurso) y `_repetido_en_csv`. Para sumar importes, quítalas.
- **Importes:** con IVA (`importe_adjudicacion_iva_inc`) y sin IVA (`importe_adjudicacion_sin_iva`) en columnas distintas, según lo que publique cada estructura; el texto publicado, en `<columna>_texto`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | datos.madrid.es | Descarga y unifica los CSV de actividad contractual (9 categorías, 11 estructuras) |

---

[Todas las fuentes](README.md) · [README](../../README.md)
