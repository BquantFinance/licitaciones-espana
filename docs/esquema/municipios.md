# `municipios.zip`

Esquema de los Parquet de `municipios.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/municipios.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `cordoba_menores.parquet` | 2.596 | 91 |
| `fuenlabrada_menores.parquet` | 34.080 | 137 |
| `gijon_menores.parquet` | 64.187 | 29 |
| `leganes_menores.parquet` | 2.019 | 36 |
| `malaga_menores.parquet` | 16.501 | 82 |
| `santa_cruz_tenerife_menores.parquet` | 3.485 | 23 |
| `valladolid_menores.parquet` | 154.487 | 44 |
| `vigo_menores.parquet` | 12.392 | 18 |

## `cordoba_menores.parquet`

2.596 filas · 91 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Identificador` | `string` |  |
| 2 | `Link licitación` | `string` |  |
| 3 | `Fecha actualización` | `string` |  |
| 4 | `Vigente/Anulada/Archivada` | `string` |  |
| 5 | `Primera publicación` | `string` |  |
| 6 | `Estado` | `string` |  |
| 7 | `Número de expediente` | `string` |  |
| 8 | `Objeto del Contrato` | `string` |  |
| 9 | `Identificador único TED` | `string` |  |
| 10 | `Valor estimado del contrato` | `string` |  |
| 11 | `Presupuesto base sin impuestos` | `string` |  |
| 12 | `Presupuesto base con impuestos` | `string` |  |
| 13 | `CPV` | `string` |  |
| 14 | `Tipo de contrato` | `string` |  |
| 15 | `Contrato mixto` | `string` |  |
| 16 | `Lugar de ejecución` | `string` |  |
| 17 | `Órgano de Contratación` | `string` |  |
| 18 | `ID OC en PLACSP` | `string` |  |
| 19 | `NIF OC` | `string` |  |
| 20 | `DIR3` | `string` |  |
| 21 | `Enlace al Perfil de Contratante del OC` | `string` |  |
| 22 | `Tipo de Administración` | `string` |  |
| 23 | `Código Postal` | `string` |  |
| 24 | `Tipo de procedimiento` | `string` |  |
| 25 | `Sistema de contratación` | `string` |  |
| 26 | `Tramitación` | `string` |  |
| 27 | `Forma de presentación de la oferta` | `string` |  |
| 28 | `Fecha de presentación de ofertas` | `string` |  |
| 29 | `Fecha de presentación de solicitudes de participacion` | `string` |  |
| 30 | `Directiva de aplicación` | `string` |  |
| 31 | `Contrato SARA/Umbral` | `string` |  |
| 32 | `Financiación Europea y fuente` | `string` |  |
| 33 | `Descripción de la financiación europea` | `string` |  |
| 34 | `Subasta electrónica` | `string` |  |
| 35 | `Subcontratación permitida` | `string` |  |
| 36 | `Subcontratación permitida porcentaje` | `string` |  |
| 37 | `NIF` | `string` |  |
| 38 | `Adjudicatario` | `string` |  |
| 39 | `Objeto` | `string` |  |
| 40 | `Duración` | `string` |  |
| 41 | `Importe licitación sin IVA` | `string` |  |
| 42 | `Importe de adjudicac.` | `string` |  |
| 43 | `Importe de adjudicación IVA` | `string` |  |
| 44 | `Instrum. de public.` | `string` |  |
| 45 | `Nº licitad.` | `string` |  |
| 46 | `Fecha` | `string` |  |
| 47 | `Decreto` | `string` |  |
| 48 | `Delegación` | `string` |  |
| 49 | `Importe licitación
Sin IVA` | `string` |  |
| 50 | `Importe de adjudicación
Sin IVA` | `string` |  |
| 51 | `nid` | `string` |  |
| 52 | `Duracion` | `string` |  |
| 53 | `ImporteLicitacionSinIVA` | `string` |  |
| 54 | `ImporteDeAdjudicacionSinIVA` | `string` |  |
| 55 | `ImporteDeAdjudicacionIVA` | `string` |  |
| 56 | `InstrumDePublic` | `string` |  |
| 57 | `NLicitad` | `string` |  |
| 58 | `Delegacion` | `string` |  |
| 59 | `Número de expediente.1` | `string` |  |
| 60 | `Lote` | `string` |  |
| 61 | `Objeto licitación/lote` | `string` |  |
| 62 | `Presupuesto base con impuestos licitación/lote` | `string` |  |
| 63 | `Presupuesto base sin impuestos licitación/lote` | `string` |  |
| 64 | `CPV licitación/lote` | `string` |  |
| 65 | `Lugar ejecución licitación/lote` | `string` |  |
| 66 | `Resultado licitación/lote` | `string` |  |
| 67 | `Fecha del acuerdo licitación/lote` | `string` |  |
| 68 | `Número de ofertas recibidas por licitación/lote` | `string` |  |
| 69 | `Precio de la oferta más baja por licitación/lote` | `string` |  |
| 70 | `Precio de la oferta más alta por licitación/lote` | `string` |  |
| 71 | `Se han excluído ofertas por ser anormalmente bajas por licitación/lote` | `string` |  |
| 72 | `Número del contrato licitación/lote` | `string` |  |
| 73 | `Fecha formalización del contrato licitación/lote` | `string` |  |
| 74 | `Fecha entrada en vigor del contrato de licitación/lote` | `string` |  |
| 75 | `Adjudicatario licitación/lote` | `string` |  |
| 76 | `El adjudicatario es o no PYME de la licitación/lote` | `string` |  |
| 77 | `Importe adjudicación sin impuestos licitación/lote` | `string` |  |
| 78 | `Importe adjudicación con impuestos licitación/lote` | `string` |  |
| 79 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 80 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 81 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 82 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 83 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 84 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 85 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 86 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 87 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 88 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 89 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 90 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 91 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `fuenlabrada_menores.parquet`

34.080 filas · 137 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `ID` | `string` |  |
| 2 | `CONCEPTO` | `string` |  |
| 3 | `PLAZO 
EJECUCION` | `string` |  |
| 4 | `IMPORTE` | `string` |  |
| 5 | `Nº 
LICITADORES` | `string` |  |
| 6 | `ADJUDICATARIO` | `string` |  |
| 7 | `NIF` | `string` |  |
| 8 | `Nª 
LICITADORES` | `string` |  |
| 9 | `CO` | `string` |  |
| 10 | `Nº  LICITADORES` | `string` |  |
| 11 | `PLAZO   
EJECUCION` | `string` |  |
| 12 | `NUMERO
LICITADORES` | `string` |  |
| 13 | `NÚMERO EXPEDIENTE` | `string` |  |
| 14 | `TÍTULO` | `string` |  |
| 15 | `PLAZO 
EJECUCIÓN` | `string` |  |
| 16 | `Nº
 LICITADORES` | `string` |  |
| 17 | `CIF` | `string` |  |
| 18 | `PLAZO_EJECUCION` | `string` |  |
| 19 | `N_LICITADORES` | `string` |  |
| 20 | `PLAZO _ EJECUCIÓN` | `string` |  |
| 21 | `REFERENCIA` | `string` |  |
| 22 | `PLAZO EJECUCIÓN` | `string` |  |
| 23 | `Nº LICITADORES` | `string` |  |
| 24 | `NÚMERO DE EXPEDIENTE` | `string` |  |
| 25 | `PLAZO DE EJECUCIÓN` | `string` |  |
| 26 | `Nº DE LICITADORES` | `string` |  |
| 27 | `numero expediente` | `string` |  |
| 28 | `titulo` | `string` |  |
| 29 | `plazo_ejecucion` | `string` |  |
| 30 | `importe` | `string` |  |
| 31 | `n_licitadores` | `string` |  |
| 32 | `adjudicatario` | `string` |  |
| 33 | `cif` | `string` |  |
| 34 | `Num. Expe.` | `string` |  |
| 35 | `Título` | `string` |  |
| 36 | `Fecha Aprobación` | `string` |  |
| 37 | `Plazo Ejec.` | `string` |  |
| 38 | `Importe` | `string` |  |
| 39 | `Num. Licitadores` | `string` |  |
| 40 | `Adjudicatario` | `string` |  |
| 41 | `Unnamed: 9` | `string` |  |
| 42 | `Unnamed: 10` | `string` |  |
| 43 | `Unnamed: 11` | `string` |  |
| 44 | `Unnamed: 12` | `string` |  |
| 45 | `Unnamed: 13` | `string` |  |
| 46 | `Unnamed: 14` | `string` |  |
| 47 | `Unnamed: 15` | `string` |  |
| 48 | `Unnamed: 16` | `string` |  |
| 49 | `Unnamed: 17` | `string` |  |
| 50 | `Unnamed: 18` | `string` |  |
| 51 | `Unnamed: 19` | `string` |  |
| 52 | `Unnamed: 20` | `string` |  |
| 53 | `Unnamed: 21` | `string` |  |
| 54 | `Unnamed: 22` | `string` |  |
| 55 | `Unnamed: 23` | `string` |  |
| 56 | `Unnamed: 24` | `string` |  |
| 57 | `Unnamed: 25` | `string` |  |
| 58 | `Unnamed: 26` | `string` |  |
| 59 | `Unnamed: 27` | `string` |  |
| 60 | `Unnamed: 28` | `string` |  |
| 61 | `Unnamed: 29` | `string` |  |
| 62 | `Unnamed: 30` | `string` |  |
| 63 | `Unnamed: 31` | `string` |  |
| 64 | `Unnamed: 32` | `string` |  |
| 65 | `Unnamed: 33` | `string` |  |
| 66 | `Unnamed: 34` | `string` |  |
| 67 | `Unnamed: 35` | `string` |  |
| 68 | `Unnamed: 36` | `string` |  |
| 69 | `Unnamed: 37` | `string` |  |
| 70 | `Unnamed: 38` | `string` |  |
| 71 | `Unnamed: 39` | `string` |  |
| 72 | `Unnamed: 40` | `string` |  |
| 73 | `Unnamed: 41` | `string` |  |
| 74 | `Unnamed: 42` | `string` |  |
| 75 | `Unnamed: 43` | `string` |  |
| 76 | `Unnamed: 44` | `string` |  |
| 77 | `Unnamed: 45` | `string` |  |
| 78 | `Unnamed: 46` | `string` |  |
| 79 | `Unnamed: 47` | `string` |  |
| 80 | `Unnamed: 48` | `string` |  |
| 81 | `Unnamed: 49` | `string` |  |
| 82 | `Unnamed: 50` | `string` |  |
| 83 | `Órgano de contratación` | `string` |  |
| 84 | `Objeto del Contrato` | `string` |  |
| 85 | `Nombre del Adjudicatario` | `string` |  |
| 86 | `N.I.F. del adjudicatario` | `string` |  |
| 87 | `Fecha del contrato` | `string` |  |
| 88 | `Nº Referencia` | `string` |  |
| 89 | `NOMENTIDAD` | `string` |  |
| 90 | `OBJETO` | `string` |  |
| 91 | `PRECADJUDICACION` | `string` |  |
| 92 | `FECHAADJDEF` | `string` |  |
| 93 | `EXISTEPUBLICIDAD` | `string` |  |
| 94 | `tipoContrato` | `string` |  |
| 95 | `SistemaAdjudicacion` | `string` |  |
| 96 | `PLAZOCONCESION` | `string` |  |
| 97 | `Pais` | `string` |  |
| 98 | `EJERCICIOORIGINAL` | `string` |  |
| 99 | `REFERENCIAORIGINAL` | `string` |  |
| 100 | `LEY` | `string` |  |
| 101 | `IVAADJUDICACION` | `string` |  |
| 102 | `PETICIONOFERTAS` | `string` |  |
| 103 | `DURACION` | `string` |  |
| 104 | `Tipo de Contrato` | `string` |  |
| 105 | `Duración` | `string` |  |
| 106 | `Nº Licitadores` | `string` |  |
| 107 | `Tipo de contrato` | `string` |  |
| 108 | `Objeto` | `string` |  |
| 109 | `Nº Licitadores ` | `string` |  |
| 110 | `CIF Adjudicatario` | `string` |  |
| 111 | `Precio de adjudicación` | `string` |  |
| 112 | `Precio de adjudicación.1` | `string` |  |
| 113 | `Nº Licitadores Participantes` | `string` |  |
| 114 | `Organismo` | `string` |  |
| 115 | `Denominación` | `string` |  |
| 116 | `Subvención` | `string` |  |
| 117 | `Nº Expediente` | `string` |  |
| 118 | `Fecha Adj.` | `string` |  |
| 119 | `Plazo` | `string` |  |
| 120 | `Num. Expte.` | `string` |  |
| 121 | `Tipo de 
Contrato` | `string` |  |
| 122 | `Nº 
Licitadores` | `string` |  |
| 123 | `Unnamed: 8` | `string` |  |
| 124 | `Entidad` | `string` |  |
| 125 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 126 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 127 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 128 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 129 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 130 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 131 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 132 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 133 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 134 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 135 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 136 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 137 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `gijon_menores.parquet`

64.187 filas · 29 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `codigo_contrato` | `string` |  |
| 2 | `lote` | `string` |  |
| 3 | `numero_expediente` | `string` |  |
| 4 | `objeto_del_contrato` | `string` |  |
| 5 | `ejercicio` | `string` |  |
| 6 | `poder_adjudicador` | `string` |  |
| 7 | `organo_de_contrataciÓn` | `string` |  |
| 8 | `fase_del_contrato` | `string` |  |
| 9 | `duraciÓn_del_contrato` | `string` |  |
| 10 | `presupuesto_base_de_licitaciÓn` | `string` |  |
| 11 | `valor_estimado_sin_iva` | `string` |  |
| 12 | `precio_de_adjudicaciÓn` | `string` |  |
| 13 | `importe_del_iva` | `string` |  |
| 14 | `procedimiento_de_adjudicaciÓn` | `string` |  |
| 15 | `fecha_adjudicacion` | `string` |  |
| 16 | `tipo_de_contrato` | `string` |  |
| 17 | `cif_adjudicatario` | `string` |  |
| 18 | `identidad_del_adjudicatario` | `string` |  |
| 19 | `concurrencia` | `string` |  |
| 20 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 21 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 22 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 23 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 25 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 26 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 27 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 28 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 29 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `leganes_menores.parquet`

2.019 filas · 36 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `NIF/CIF Tercero` | `string` |  |
| 2 | `Razón Social Tercero` | `string` |  |
| 3 | `Procedimiento de Contratación` | `string` |  |
| 4 | `Num. Expediente` | `string` |  |
| 5 | `Titulo Expediente` | `string` |  |
| 6 | `Importe Total Presup.Licit.` | `string` |  |
| 7 | `Organismo` | `string` |  |
| 8 | `Nº EXPTE` | `string` |  |
| 9 | `IMPORTE ADJUDICAC` | `string` |  |
| 10 | `TITULO EXPTE` | `string` |  |
| 11 | `FECHA ADJUDICAC` | `string` |  |
| 12 | `ADJUDICATARIO` | `string` |  |
| 13 | `SERVICIO RESPONSABLE` | `string` |  |
| 14 | `NIF	` | `string` |  |
| 15 | `Adjudicatario	` | `string` |  |
| 16 | `Expediente	` | `string` |  |
| 17 | `Objeto del contrato	` | `string` |  |
| 18 | `Fecha contrato	` | `string` |  |
| 19 | `Precio sin impuestos	` | `string` |  |
| 20 | `Precio con impuestos` | `string` |  |
| 21 | `Duración contrato` | `string` |  |
| 22 | `Duración ` | `string` |  |
| 23 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 24 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 25 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 26 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 27 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 28 | `_mes` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 29 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 30 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 31 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 32 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 33 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 34 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 35 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 36 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `malaga_menores.parquet`

16.501 filas · 82 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Expte.` | `string` |  |
| 2 | `Descripción` | `string` |  |
| 3 | `Total` | `string` |  |
| 4 | `Tercero` | `string` |  |
| 5 | `EXPTE` | `string` |  |
| 6 | `DESCRIPCION` | `string` |  |
| 7 | `TOTAL` | `string` |  |
| 8 | `TERCERO` | `string` |  |
| 9 | `Area` | `string` |  |
| 10 | `Fecha` | `string` |  |
| 11 | `TC` | `string` |  |
| 12 | `B. Imp.` | `string` |  |
| 13 | `IVA` | `string` |  |
| 14 | `Partida presup.` | `string` |  |
| 15 | `CIF` | `string` |  |
| 16 | `nº contrato` | `string` |  |
| 17 | `Concepto` | `string` |  |
| 18 | `Importe` | `string` |  |
| 19 | `Adjudicatario` | `string` |  |
| 20 | `Fecha adj` | `string` |  |
| 21 | `Unnamed: 5` | `string` |  |
| 22 | `NIF ADJUDICATARIO` | `string` |  |
| 23 | `NOMBRE ADJUDICATARIO` | `string` |  |
| 24 | `FECHA DECRETO ADJUDICACIÓN` | `string` |  |
| 25 | `OBJETO` | `string` |  |
| 26 | `DURACIÓN` | `string` |  |
| 27 | `IMPORTE` | `string` |  |
| 28 | `TIPO DE CONTRATO` | `string` |  |
| 29 | `SUBTIPO` | `string` |  |
| 30 | `Unnamed: 6` | `string` |  |
| 31 | `Expte` | `string` |  |
| 32 | `Duración` | `string` |  |
| 33 | `Expte. ` | `string` |  |
| 34 | `CC` | `string` |  |
| 35 | `D` | `string` |  |
| 36 | `total adj.` | `string` |  |
| 37 | `columna_1` | `string` |  |
| 38 | `columna_2` | `string` |  |
| 39 | `Total Adjud.` | `string` |  |
| 40 | `tercero` | `string` |  |
| 41 | `FECHA RESOLUCIÓN ADJUDICACIÓN` | `string` |  |
| 42 | `DURACIÓN (meses)` | `string` |  |
| 43 | `D (meses)` | `string` |  |
| 44 | `Nº EXPEDIENTE` | `string` |  |
| 45 | `IMPORTE ADJUDICACIÓN` | `string` |  |
| 46 | `IDENTIDAD DEL ADJUDICATARIO` | `string` |  |
| 47 | `PYME` | `string` |  |
| 48 | `DURACIÓN ` | `string` |  |
| 49 | `Unnamed: 1` | `string` |  |
| 50 | `Unnamed: 3` | `string` |  |
| 51 | `Unnamed: 4` | `string` |  |
| 52 | `Decripción` | `string` |  |
| 53 | `B.Imp. Adj.` | `string` |  |
| 54 | `Aplic. Presup.` | `string` |  |
| 55 | `Nº de contrato` | `string` |  |
| 56 | `Objeto del contrato` | `string` |  |
| 57 | `Importe adjudicación  (IVA incluido)` | `string` |  |
| 58 | `Duración/ Plazo Ejecución` | `string` |  |
| 59 | `Unnamed: 13` | `string` |  |
| 60 | `Unnamed: 14` | `string` |  |
| 61 | `Unnamed: 15` | `string` |  |
| 62 | `Unnamed: 16` | `string` |  |
| 63 | `B. Imp. Lic.` | `string` |  |
| 64 | `B. imp. Adj.` | `string` |  |
| 65 | `IVA.1` | `string` |  |
| 66 | `importe` | `string` |  |
| 67 | `Descricpión` | `string` |  |
| 68 | `Importe adjudicación (IVA incluido)` | `string` |  |
| 69 | `DURACIÓN/ PLAZO EJECUCIÓN` | `string` |  |
| 70 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 71 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 72 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 73 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 74 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 75 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 76 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 77 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 78 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 79 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 80 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 81 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 82 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `santa_cruz_tenerife_menores.parquet`

3.485 filas · 23 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `CONTRATO` | `string` |  |
| 2 | `EXPEDIENTE` | `string` |  |
| 3 | `FECHA ALTA` | `string` |  |
| 4 | `UNIDAD DE CONTRATACIÓN` | `string` |  |
| 5 | `OBJETO DEL CONTRATO` | `string` |  |
| 6 | `TIPO` | `string` |  |
| 7 | `SUBTIPO` | `string` |  |
| 8 | `PROCEDIMIENTO` | `string` |  |
| 9 | `DESCRIPCION_ESTADO` | `string` |  |
| 10 | `PRESUPUESTO BASE SIN IMPUESTOS` | `string` |  |
| 11 | `IMPORTE DE ADJUDICACION` | `string` |  |
| 12 | `IMPORTE DE ADJ SIN IMPUESTOS` | `string` |  |
| 13 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 15 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 16 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 18 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 19 | `_titulo_tabla` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 20 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 21 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 22 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 23 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `valladolid_menores.parquet`

154.487 filas · 44 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `NOMBRE DE TERCERO` | `string` |  |
| 2 | `CONCEPTO` | `string` |  |
| 3 | `IMPORTE` | `string` |  |
| 4 | `Nº Operación` | `string` |  |
| 5 | `Fase` | `string` |  |
| 6 | `Fecha` | `string` |  |
| 7 | `Referencia` | `string` |  |
| 8 | `Aplicación` | `string` |  |
| 9 | `Importe` | `string` |  |
| 10 | `Tercero` | `string` |  |
| 11 | `Nombre Ter.` | `string` |  |
| 12 | `Texto Libre` | `string` |  |
| 13 | `Oper.` | `string` |  |
| 14 | `Provincia Tercero` | `string` |  |
| 15 | `Tipo Contrato` | `string` |  |
| 16 | `Procedim. Contrato` | `string` |  |
| 17 | `Oper. Coste` | `string` |  |
| 18 | `TRIMESTRE` | `string` |  |
| 19 | `PROCEDIMIENTO` | `string` |  |
| 20 | `AREA` | `string` |  |
| 21 | `AREA EN TEXTO` | `string` |  |
| 22 | `PROGRAMA` | `string` |  |
| 23 | `Programa en texto` | `string` |  |
| 24 | `CAPITULO` | `string` |  |
| 25 | `Capítulo en texto` | `string` |  |
| 26 | `ARTICULO` | `string` |  |
| 27 | `Artículo en texto` | `string` |  |
| 28 | `Criterio Contrato` | `string` |  |
| 29 | `CAPITULO EN TEXTO` | `string` |  |
| 30 | `PROGRAMA EN TEXTO` | `string` |  |
| 31 | `Localidad` | `string` |  |
| 32 | `Provincia` | `string` |  |
| 33 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 34 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 35 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 36 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 37 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 38 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 39 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 40 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 41 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 42 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 43 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 44 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `vigo_menores.parquet`

12.392 filas · 18 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `unidade` | `string` |  |
| 2 | `asunto` | `string` |  |
| 3 | `tipo` | `string` |  |
| 4 | `adxudicatario` | `string` |  |
| 5 | `importe` | `string` |  |
| 6 | `num_licitadores` | `string` |  |
| 7 | `fin_contrato` | `string` |  |
| 8 | `data` | `string` |  |
| 9 | `expediente` | `string` |  |
| 10 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_municipio` | `string` | [de la fuente](../fuentes/municipios.md) |
| 12 | `_codigo_ine` | `string` | [de la fuente](../fuentes/municipios.md) |
| 13 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 15 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 16 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
