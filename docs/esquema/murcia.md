# `murcia.zip`

Esquema de los Parquet de `murcia.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/murcia.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratos_carm.parquet` | 15.399 | 28 |
| `contratos_menores_carm.parquet` | 244.185 | 44 |
| `contratos_menores_sms.parquet` | 748.984 | 44 |

## `contratos_carm.parquet`

15.399 filas · 28 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ejercicio` | `string` |  |
| 2 | `codinscripcion` | `string` |  |
| 3 | `tipocontrato` | `string` |  |
| 4 | `procedimiento` | `string` |  |
| 5 | `objeto` | `string` |  |
| 6 | `importlicitacion` | `string` |  |
| 7 | `importadjudicacion` | `string` |  |
| 8 | `adjudicatariodescripcion` | `string` |  |
| 9 | `adjudicatariocodigo` | `string` |  |
| 10 | `fechaformalizacion` | `string` |  |
| 11 | `duracion` | `string` |  |
| 12 | `organo` | `string` |  |
| 13 | `cpvcodigo` | `string` |  |
| 14 | `cpvdescripcion` | `string` |  |
| 15 | `nummodificaciones` | `string` |  |
| 16 | `codigoOrgano` | `string` |  |
| 17 | `fechaInicio` | `string` |  |
| 18 | `_objeto_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 19 | `_adjudicatariodescripcion_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 20 | `_cpvdescripcion_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 21 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 22 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 23 | `_anio_fichero` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 26 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 27 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 28 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_menores_carm.parquet`

244.185 filas · 44 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `CONSEJERIA-OA-COD` | `string` |  |
| 2 | `CONSEJERIA-OA-DESCRIPCION` | `string` |  |
| 3 | `TIPO_CONTRATO` | `string` |  |
| 4 | `CPV_CODIGO` | `string` |  |
| 5 | `CPV_DESCRIPCION` | `string` |  |
| 6 | `ADJUDICATARIO_CIF` | `string` |  |
| 7 | `ADJUDICATARIO_DESCRIPCION` | `string` |  |
| 8 | `FECHA` | `string` |  |
| 9 | `IMPORTE(IVAINCLUIDO)` | `string` |  |
| 10 | `TRIMESTRE` | `string` |  |
| 11 | `CENTRO_COD` | `string` |  |
| 12 | `CENTRO_DESCRIPCION` | `string` |  |
| 13 | `EJERCICIO` | `string` |  |
| 14 | `TRIMESTRE_NUM` | `string` |  |
| 15 | `_resto_json` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 16 | `UNIDAD` | `string` |  |
| 17 | `UNIDADDESCRIPCION` | `string` |  |
| 18 | `TIPOCONTRATO` | `string` |  |
| 19 | `CPVCODIGO` | `string` |  |
| 20 | `CPVDESCRIPCION` | `string` |  |
| 21 | `ADJUDICATARIOCODIGO` | `string` |  |
| 22 | `ADJUDICATARIODESCRIPCION` | `string` |  |
| 23 | `FECHACONTABPAGO` | `string` |  |
| 24 | `VALORCONTABPAGO` | `string` |  |
| 25 | `CENTRO` | `string` |  |
| 26 | `CENTRODESCRIPCION` | `string` |  |
| 27 | `EJERCICIONUM` | `string` |  |
| 28 | `TRIMESTRENUM` | `string` |  |
| 29 | `_CPVDESCRIPCION_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 30 | `_ADJUDICATARIODESCRIPCION_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 31 | `CODEXPEDIENTE` | `string` |  |
| 32 | `OBJETO_CONTRATO_MENOR` | `string` |  |
| 33 | `UNIDADCODIGO` | `string` |  |
| 34 | `CENTROCODIGO` | `string` |  |
| 35 | `IMPORTECONTABPAGO` | `string` |  |
| 36 | `_OBJETO_CONTRATO_MENOR_sin_cortes` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 37 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 38 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 39 | `_anio_fichero` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 40 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 41 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 43 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 44 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratos_menores_sms.parquet`

748.984 filas · 44 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Centro` | `string` |  |
| 2 | `CENTRO DIRECTIVO` | `string` |  |
| 3 | `Ref. Contrato` | `string` |  |
| 4 | `Fecha Formalizacion` | `string` |  |
| 5 | `Objeto` | `string` |  |
| 6 | `Importe Adjudicacion` | `string` |  |
| 7 | `Adjudicatario NIF` | `string` |  |
| 8 | `Adjudicatario Nombre` | `string` |  |
| 9 | `Centro Directivo` | `string` |  |
| 10 | `Fecha Formalización` | `string` |  |
| 11 | `Descripción Material` | `string` |  |
| 12 | `Importe Adjudicación EUR` | `string` |  |
| 13 | `Importe Adjudicación` | `string` |  |
| 14 | `Doc.compras` | `string` |  |
| 15 | `C.DIRECTIVO CÓDIGO` | `string` |  |
| 16 | ` Valor neto` | `string` |  |
| 17 | `Texto breve` | `string` |  |
| 18 | `F.FORMALIZACIÓN` | `string` |  |
| 19 | `NIF Adj` | `string` |  |
| 20 | `Nombre del proveedor` | `string` |  |
| 21 | `CONSEJERÍA/OO.AA. CÓDIGO` | `string` |  |
| 22 | `CONSEJERIA/OO.AA. DESCRIPCION` | `string` |  |
| 23 | `C.DIRECTIVO DESCRIPCIÓN` | `string` |  |
| 24 | `TIPO` | `string` |  |
| 25 | `ADJUDICATARIO CÓDIGO OF` | `string` |  |
| 26 | `ADJUDICATARIO DESCRIPCION` | `string` |  |
| 27 | `IMPORTE (IVA INCLUIDO)` | `string` |  |
| 28 | `FECHA CONTABILIZACION` | `string` |  |
| 29 | `OBJETO` | `string` |  |
| 30 | `ADJUDICATARIO CÓDIGO` | `string` |  |
| 31 | `Doc.compr.` | `string` |  |
| 32 | `Texto descriptivo` | `string` |  |
| 33 | ` Cif` | `string` |  |
| 34 | `columna_1` | `string` |  |
| 35 | `columna_2` | `string` |  |
| 36 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 37 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 38 | `_anio_fichero` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 39 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 40 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 41 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 43 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 44 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
