# `castilla_la_mancha.zip`

Esquema de los Parquet de `castilla_la_mancha.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/castilla_la_mancha.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `caja_pagadora.parquet` | 16.662 | 20 |
| `informe_menores.parquet` | 319.658 | 63 |
| `menores_junta.parquet` | 268.744 | 58 |
| `sector_publico.parquet` | 1.923.536 | 443 |
| `sescam.parquet` | 4.162.351 | 176 |
| `uclm.parquet` | 254.130 | 19 |

## `caja_pagadora.parquet`

16.662 filas · 20 columnas · 2 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Organismo` | `string` |  |
| 2 | `Descripción` | `string` |  |
| 3 | `Importe IVA incluído` | `string` |  |
| 4 | `NIF Adjudicatario` | `string` |  |
| 5 | `Adjudicatario` | `string` |  |
| 6 | `Fecha Factura` | `string` |  |
| 7 | `Fecha de registro` | `string` |  |
| 8 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 10 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 15 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 17 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 20 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `informe_menores.parquet`

319.658 filas · 63 columnas · 4 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `NÚMERO` | `string` |  |
| 2 | `CG` | `string` |  |
| 3 | `ÓRGANO CONTRATACIÓN` | `string` |  |
| 4 | `IA` | `string` |  |
| 5 | `Objeto` | `string` |  |
| 6 | `CTO` | `string` |  |
| 7 | `CLASE CTO` | `string` |  |
| 8 | `Empresa` | `string` |  |
| 9 | `Proc. Adjudicación` | `string` |  |
| 10 | `IA /1000000` | `string` |  |
| 11 | `Unnamed: 4` | `string` |  |
| 12 | `Unnamed: 5` | `string` |  |
| 13 | `IA/1000000` | `string` |  |
| 14 | `TIPO CONTRATO` | `string` |  |
| 15 | `IA * 1000000` | `string` |  |
| 16 | `CONTADOR` | `string` |  |
| 17 | `Unnamed: 6` | `string` |  |
| 18 | `Unnamed: 7` | `string` |  |
| 19 | `Unnamed: 8` | `string` |  |
| 20 | `Unnamed: 9` | `string` |  |
| 21 | `Nombre departamento (sección)` | `string` |  |
| 22 | `Código departamento (sección)` | `string` |  |
| 23 | `Número de contrato (NRC)` | `string` |  |
| 24 | `Objeto del contrato` | `string` |  |
| 25 | `Tipo de contrato` | `string` |  |
| 26 | `Procedimiento de tramitación` | `string` |  |
| 27 | `Acuerdo Marco` | `string` |  |
| 28 | `IL` | `string` |  |
| 29 | `Artículo Negociado` | `string` |  |
| 30 | `Apartado Negociado` | `string` |  |
| 31 | `Subapartado Negociado` | `string` |  |
| 32 | `Supuesto aplicación Negociado` | `string` |  |
| 33 | `Nº expediente PICOS` | `string` |  |
| 34 | `IA * 1.000.000` | `string` |  |
| 35 | `contador` | `string` |  |
| 36 | `Contador` | `string` |  |
| 37 | `Etiquetas de fila` | `string` |  |
| 38 | `Suma de IA` | `string` |  |
| 39 | `Cuenta de Número de contrato (NRC)` | `string` |  |
| 40 | `Unnamed: 3` | `string` |  |
| 41 | `Unnamed: 1` | `string` |  |
| 42 | `Nº Contratos` | `string` |  |
| 43 | `% sobre el total de contratos formalizados` | `string` |  |
| 44 | `Importe *` | `string` |  |
| 45 | `% sobre el total del importe de adjudicación` | `string` |  |
| 46 | `Nª Contratos` | `string` |  |
| 47 | `% sobre el total de contratos formalizados.1` | `string` |  |
| 48 | `Importe *.1` | `string` |  |
| 49 | `% sobre el total del importe de adjudicación.1` | `string` |  |
| 50 | `Nº Contratos.1` | `string` |  |
| 51 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 52 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 53 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 54 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 55 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 56 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 57 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 58 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 59 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 60 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 61 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 62 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 63 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `menores_junta.parquet`

268.744 filas · 58 columnas · 36 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Adjudicatario(s)` | `string` |  |
| 2 | `Adjudicado Imp. sin Impuestos` | `string` |  |
| 3 | `Adjudicado Tipo IVA` | `string` |  |
| 4 | `Adjudicado Imp. Impuestos` | `string` |  |
| 5 | `Órgano de Contratación` | `string` |  |
| 6 | `Consejería/Organismo` | `string` |  |
| 7 | `Expediente PICOS` | `string` |  |
| 8 | `Descripción` | `string` |  |
| 9 | `Objeto de Gasto` | `string` |  |
| 10 | `Subobjeto de Gasto` | `string` |  |
| 11 | `F Adjudicación` | `string` |  |
| 12 | `F Pub Adjudicación` | `string` |  |
| 13 | `Plazo Ejecución/Entrega` | `string` |  |
| 14 | `Adjudicataria` | `string` |  |
| 15 | `Nº Expediente (@=lic elec)` | `string` |  |
| 16 | `Plazo Ejecución/ Entrega` | `string` |  |
| 17 | `Organismo` | `string` |  |
| 18 | `Adjudicataria(s)` | `string` |  |
| 19 | `Sección` | `string` |  |
| 20 | `Tipo de Adjudicación` | `string` |  |
| 21 | `Órgano` | `string` |  |
| 22 | `Nro. Expediente` | `string` |  |
| 23 | `Nº expediente PICOS` | `string` |  |
| 24 | `Número de contrato (NRC)` | `string` |  |
| 25 | `Organísmo` | `string` |  |
| 26 | `Importe adjudicación sin IVA` | `string` |  |
| 27 | `Importe IVA Adjudicación` | `string` |  |
| 28 | `Importe adjudicación IVA incluido` | `string` |  |
| 29 | `Nif adjudicataria(s)` | `string` |  |
| 30 | `Tipo de contrato` | `string` |  |
| 31 | `Plazo ejecución` | `string` |  |
| 32 | `Tipo Plazo` | `string` |  |
| 33 | `Número de Licitadores ` | `string` |  |
| 34 | `Fecha de adjudicación` | `string` |  |
| 35 | `Fecha Pub. Adjudicación PLACE` | `string` |  |
| 36 | `Fecha de formalización` | `string` |  |
| 37 | `Fecha de registro` | `string` |  |
| 38 | `Número de Licitadores` | `string` |  |
| 39 | `CIF Adjudicataria(s)` | `string` |  |
| 40 | `CIF Adjudicatario(s)` | `string` |  |
| 41 | `C.I.F. Adjudicatario(s)` | `string` |  |
| 42 | `CIF Adjudicatario(S)` | `string` |  |
| 43 | `CIFAdjudicatario(s)` | `string` |  |
| 44 | `Adjudicatario(os)` | `string` |  |
| 45 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 46 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 47 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 48 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 49 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 50 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 51 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 52 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 53 | `_miembro` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 54 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 55 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 56 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 57 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 58 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `sector_publico.parquet`

1.923.536 filas · 443 columnas · 14 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Descripción` | `string` |  |
| 2 | `Expediente UT` | `string` |  |
| 3 | `Expediente PICOS` | `string` |  |
| 4 | `Provincia` | `string` |  |
| 5 | `Provincia/NUTS` | `string` |  |
| 6 | `Objeto de Gasto` | `string` |  |
| 7 | `Subobjeto de Gasto` | `string` |  |
| 8 | `Plazo Ejecución/Entrega (MESES)` | `string` |  |
| 9 | `Plazo Ejecución/Entrega (DÍAS)` | `string` |  |
| 10 | `CIF/NIF Adjudicatario` | `string` |  |
| 11 | `Adjudicatario(s)` | `string` |  |
| 12 | `Adjudicado Importe sin IVA` | `string` |  |
| 13 | `Adjudicado Tipo IVA` | `string` |  |
| 14 | `Adjudicado Importe IVA` | `string` |  |
| 15 | `F Adjudicación` | `string` |  |
| 16 | `Unnamed: 15` | `string` |  |
| 17 | `DESCRIPCION` | `string` |  |
| 18 | `EXPEDIENTE UT` | `string` |  |
| 19 | `EXPEDIENTE PICOS` | `string` |  |
| 20 | `PROVINCIA` | `string` |  |
| 21 | `PROVINCIA/NUTS` | `string` |  |
| 22 | `OBJETO DE GASTO` | `string` |  |
| 23 | `SUBOBJETO DE GASTO` | `string` |  |
| 24 | `PLAZO EJECUCIÓN/ENTREGA (MESES)` | `string` |  |
| 25 | `PLAZO EJECUCION/ENTREGA (DIAS)` | `string` |  |
| 26 | `Adjudicado importe sin IVA` | `string` |  |
| 27 | `Adjudicado importe IVA` | `string` |  |
| 28 | `FECHA Adjudicación` | `string` |  |
| 29 | `TOTAL` | `string` |  |
| 30 | `DESCRIPCIÓN` | `string` |  |
| 31 | `NIF ADJUDICATARIO` | `string` |  |
| 32 | ` ADJUDICATARIO` | `string` |  |
| 33 | `IMPORTE` | `string` |  |
| 34 | `Plazo Ejecución/Entrega (DIAS)` | `string` |  |
| 35 | `Objeto del gasto` | `string` |  |
| 36 | `Subobjeto de gasto` | `string` |  |
| 37 | `Plazo de ejecución (meses)` | `string` |  |
| 38 | `Plazo de ejecución (días)` | `string` |  |
| 39 | `Fecha adjudicación` | `string` |  |
| 40 | `NIF` | `string` |  |
| 41 | `Nombre` | `string` |  |
| 42 | `Importe` | `string` |  |
| 43 | `Porcentaje IVA` | `string` |  |
| 44 | `Base imponible` | `string` |  |
| 45 | `Cuota IVA` | `string` |  |
| 46 | `051 Contrato de obras` | `string` |  |
| 47 | `OBSERVACIONES` | `string` |  |
| 48 | `Unnamed: 16` | `string` |  |
| 49 | `ORDEN MIO` | `string` |  |
| 50 | `OBJETO DEL GASTO` | `string` |  |
| 51 | `SUBOBJETO DEL GASTO` | `string` |  |
| 52 | `PLAZO EJECUCION/ENTREGA (MESES)` | `string` |  |
| 53 | `CIF/NIF ADJUDICATARIO` | `string` |  |
| 54 | `ADJUDICATARIO` | `string` |  |
| 55 | `ADJUDICADO IMPORTE SIN IVA` | `string` |  |
| 56 | `ADJUDICADO TIPO IVA` | `string` |  |
| 57 | `ADJUDICADO IMPORTE IVA` | `string` |  |
| 58 | `F.ADJUDICACION` | `string` |  |
| 59 | `Fe.contabilización` | `string` |  |
| 60 | `Número del Expediente` | `string` |  |
| 61 | `Posición presupuestaria` | `string` |  |
| 62 | `Centro gestor` | `string` |  |
| 63 | `Clase de documento` | `string` |  |
| 64 | `Doc.presupuestario` | `string` |  |
| 65 | `Acreedor` | `string` |  |
| 66 | `Status de pago` | `string` |  |
| 67 | `Status del Documento` | `string` |  |
| 68 | `Referencia` | `string` |  |
| 69 | `Elemento PEP` | `string` |  |
| 70 | `Fondo` | `string` |  |
| 71 | `Importe mon.EnCP` | `string` |  |
| 72 | `Texto` | `string` |  |
| 73 | `Nº ident.fis.1` | `string` |  |
| 74 | `Nombre 1` | `string` |  |
| 75 | `Adjudicado Importe con IVA` | `string` |  |
| 76 | `F Entrada Factura` | `string` |  |
| 77 | `Subobjeto de Gasto ` | `string` |  |
| 78 | `CIF/NIF/ ADJUDICATARIO` | `string` |  |
| 79 | `Plazo Ejecución/Entrega` | `string` |  |
| 80 | `Código` | `string` |  |
| 81 | `Fecha Adjudicación` | `string` |  |
| 82 | `Nº FACTURA INTERNO` | `string` |  |
| 83 | `CONTRATO` | `string` |  |
| 84 | `Tipo IVA` | `string` |  |
| 85 | `IMPORTE IVA` | `string` |  |
| 86 | `Retención IRPF` | `string` |  |
| 87 | `Importe total fra` | `string` |  |
| 88 | `Canal` | `string` |  |
| 89 | `FECHA APROBACION` | `string` |  |
| 90 | `Nº EXPTE. PICOS` | `string` |  |
| 91 | `OBJETO` | `string` |  |
| 92 | `IMPORTE SIN IVA` | `string` |  |
| 93 | `TIPO IVA` | `string` |  |
| 94 | `ADJUDICATARIO ` | `string` |  |
| 95 | `TIPO DE CONTRATO` | `string` |  |
| 96 | `Unnamed: 7` | `string` |  |
| 97 | `Unnamed: 8` | `string` |  |
| 98 | `FECHA ADJUDICACIÓN` | `string` |  |
| 99 | `ÓRGANO PROPONENTE` | `string` |  |
| 100 | `Tipo de Adjudicación` | `string` |  |
| 101 | `Localidad` | `string` |  |
| 102 | `F Fin de Plazo` | `string` |  |
| 103 | `Creación` | `string` |  |
| 104 | `Descripción del contrato` | `string` |  |
| 105 | `Tipo de contrato` | `string` |  |
| 106 | `Fecha de adjudicación` | `string` |  |
| 107 | `NIF adjudicatario` | `string` |  |
| 108 | `Nombre adjudicatario` | `string` |  |
| 109 | `Plazo Ejecución /entrega (MESES)` | `string` |  |
| 110 | `Plazo Ejecución /entrega (DIAS)` | `string` |  |
| 111 | `CIF / NIF Adjudicatario` | `string` |  |
| 112 | `ADJUDICADO INCLUIDO IVA` | `string` |  |
| 113 | `MI ORDEN` | `string` |  |
| 114 | `Programa de financiación` | `string` |  |
| 115 | `Bloqueo de pago` | `string` |  |
| 116 | `Vía de pago` | `string` |  |
| 117 | `Unnamed: 35` | `string` |  |
| 118 | `OBJETO DEL CONTRATO` | `string` |  |
| 119 | `OBJETO CONTRATO` | `string` |  |
| 120 | `FECHA` | `string` |  |
| 121 | `IMPORTE NETO` | `string` |  |
| 122 | `IVA` | `string` |  |
| 123 | `IMPORTE TOTAL` | `string` |  |
| 124 | `Gerencia` | `string` |  |
| 125 | `Tipo compra` | `string` |  |
| 126 | `Artículo` | `string` |  |
| 127 | `Proveedor` | `string` |  |
| 128 | `Nº de factura` | `string` |  |
| 129 | `Importe total con IVA` | `string` |  |
| 130 | `Cantidad ` | `string` |  |
| 131 | `Último precio Unitario` | `string` |  |
| 132 | `Importe total sin IVA` | `string` |  |
| 133 | `CIF/NIF Adjudicatario.1` | `string` |  |
| 134 | `Adjudicatario(s).1` | `string` |  |
| 135 | `NIF Adjudicatario` | `string` |  |
| 136 | `PROPONENTE` | `string` |  |
| 137 | `CONCEPTO` | `string` |  |
| 138 | `IMPORTE ADJUDICACIÓN` | `string` |  |
| 139 | `FECHA ADJUDICACIÓN/ FORMALICACIÓN` | `string` |  |
| 140 | `Plazo de ejecución/entrega` | `string` |  |
| 141 | `ÓRGANO 
CONTRATACIÓN` | `string` |  |
| 142 | `FECHA
ADJUDICACIÓN` | `string` |  |
| 143 | `IMPORTE
CONTRATO` | `string` |  |
| 144 | `IMPORTE
IVA` | `string` |  |
| 145 | `IMPORTE 
CONTRATO I.I.` | `string` |  |
| 146 | `GASTO Nº AS` | `string` |  |
| 147 | `Descripción Expediente` | `string` |  |
| 148 | `Objeto del Gasto` | `string` |  |
| 149 | `SubObjeto de Gasto` | `string` |  |
| 150 | `Plazo Ejecución` | `string` |  |
| 151 | `Tipo de Plazo` | `string` |  |
| 152 | `Importe Adjudicación Neto` | `string` |  |
| 153 | `Tipo IVA Adjudicación` | `string` |  |
| 154 | `Importe IVA Adjudicación` | `string` |  |
| 155 | `Descripción 
CONTRATOS MENORES 
GEACAM S.A. AÑO 2016` | `string` |  |
| 156 | `EXPEDIENTE
 UT` | `string` |  |
| 157 | `Nº EXPTE.` | `string` |  |
| 158 | `PLAZO EJECUCIÓN/
ENTREGA (MESES)` | `string` |  |
| 159 | `PLAZO EJECUCIÓN/
ENTREGA (DÍAS)` | `string` |  |
| 160 | `CIF/NIF 
ADJUDICATARIO` | `string` |  |
| 161 | `ADJUDICADO
 IMPORTE SIN IVA` | `string` |  |
| 162 | `ADJUDICADO
TIPO IVA` | `string` |  |
| 163 | `ADJUDICADO
IMPORTE CON IVA` | `string` |  |
| 164 | `FECHA 
ADJUDICACIÓN` | `string` |  |
| 165 | `Procedimiento` | `string` |  |
| 166 | `Objeto` | `string` |  |
| 167 | `Unnamed: 0` | `string` |  |
| 168 | `Contrato Privado` | `string` |  |
| 169 | `Campos:` | `string` |  |
| 170 | `Formato de Celda` | `string` |  |
| 171 | `Unnamed: 4` | `string` |  |
| 172 | `Unnamed: 10` | `string` |  |
| 173 | `REGISTRO` | `string` |  |
| 174 | `ENTIDAD ADJUDICADORA` | `string` |  |
| 175 | `FIRMA CONTRATO` | `string` |  |
| 176 | `PLAZO EJECUCIÓN DÍAS` | `string` |  |
| 177 | `TIPO CONTRATO` | `string` |  |
| 178 | `IMPORTE CON IVA` | `string` |  |
| 179 | `NUM EXPTE` | `string` |  |
| 180 | `CENTRO` | `string` |  |
| 181 | `EMPRESA SUMINISTRADORA` | `string` |  |
| 182 | `C.I.F.` | `string` |  |
| 183 | `PARTIDA PRESUPUESTARIA` | `string` |  |
| 184 | `EXPTE PICOS` | `string` |  |
| 185 | `Unnamed: 13` | `string` |  |
| 186 | `NOMBRE ADJUDICATARIO` | `string` |  |
| 187 | `Sociedad del Habilitado` | `string` |  |
| 188 | `Ce.gestor` | `string` |  |
| 189 | `PosPre` | `string` |  |
| 190 | `Situación Factura` | `string` |  |
| 191 | `Soc.` | `string` |  |
| 192 | `Año` | `string` |  |
| 193 | `Nº Factura` | `string` |  |
| 194 | `F. Entrada` | `string` |  |
| 195 | `F.Reposic.` | `string` |  |
| 196 | `    Importe` | `string` |  |
| 197 | `     Número Expte. Contable` | `string` |  |
| 198 | `Unnamed: 14` | `string` |  |
| 199 | `Fase de Expediente` | `string` |  |
| 200 | `Unidad Tramitadora` | `string` |  |
| 201 | `Unidad Proponente` | `string` |  |
| 202 | `Forma Tramitación` | `string` |  |
| 203 | `Tipo de Tramitación` | `string` |  |
| 204 | `Regulación Armonizada` | `string` |  |
| 205 | `Prórroga` | `string` |  |
| 206 | `Duración Prórroga` | `string` |  |
| 207 | `Precio Unitario` | `string` |  |
| 208 | `Suministro Informático` | `string` |  |
| 209 | `Dirección` | `string` |  |
| 210 | `Expediente Complementario` | `string` |  |
| 211 | `Adjudicación F Pub PC` | `string` |  |
| 212 | `PLAZO DE EJECUCIÓN` | `string` |  |
| 213 | `Importe total CON IVA` | `string` |  |
| 214 | `Importe IVA` | `string` |  |
| 215 | `Importe sin IVA` | `string` |  |
| 216 | `% IVA` | `string` |  |
| 217 | `Plazo Ejecución/Entrega ` | `string` |  |
| 218 | `Tipo de IVA` | `string` |  |
| 219 | `Unidad Solicitante` | `string` |  |
| 220 | `Ppto Contrato Original sin IVA` | `string` |  |
| 221 | `Mod Previstas sin IVA` | `string` |  |
| 222 | `Importe de Prórroga` | `string` |  |
| 223 | `Estimado Importe sin IVA` | `string` |  |
| 224 | `Estimado Tipo IVA` | `string` |  |
| 225 | `Estimado Importe IVA` | `string` |  |
| 226 | `Licitado Importe sin IVA` | `string` |  |
| 227 | `Licitado Tipo IVA` | `string` |  |
| 228 | `Licitado Importe IVA` | `string` |  |
| 229 | `Tipo de Negociado` | `string` |  |
| 230 | `Tipo Sistema Racionalización` | `string` |  |
| 231 | `Sist. Rac. Ejercicio Inicio` | `string` |  |
| 232 | `Sist. Rac. Ejercicio Fin` | `string` |  |
| 233 | `Sist. Rac. Exp Dependiente` | `string` |  |
| 234 | `Tipo Obra` | `string` |  |
| 235 | `Tipo Centro` | `string` |  |
| 236 | `Área de Atención` | `string` |  |
| 237 | `Empresa Pública` | `string` |  |
| 238 | `Licitación F Pub DOUE` | `string` |  |
| 239 | `Licitación Nro. Pub DOUE` | `string` |  |
| 240 | `Licitación F Pub PC` | `string` |  |
| 241 | `Licitación F Pub BOE` | `string` |  |
| 242 | `Licitación Nro. Pub BOE` | `string` |  |
| 243 | `Licitación F Pub DOCM` | `string` |  |
| 244 | `Licitación Nro. Pub DOCM` | `string` |  |
| 245 | `Adjudicación F Pub DOUE` | `string` |  |
| 246 | `Adjudicación Nro. Pub DOUE` | `string` |  |
| 247 | `Adjudicación F Pub BOE` | `string` |  |
| 248 | `Adjudicación Nro. Pub BOE` | `string` |  |
| 249 | `Adjudicación F Pub DOCM` | `string` |  |
| 250 | `Adjudicación Nro. Pub DOCM` | `string` |  |
| 251 | `Adjudicación Prov. F Pub PC` | `string` |  |
| 252 | `Formalización F Pub DOUE` | `string` |  |
| 253 | `Formalización Nro Pub DOUE` | `string` |  |
| 254 | `Formalización F Pub PC` | `string` |  |
| 255 | `Formalización F Pub BOE` | `string` |  |
| 256 | `Formalización Nro Pub BOE` | `string` |  |
| 257 | `Formalización F Pub DOCM` | `string` |  |
| 258 | `Formalización Nro Pub DOCM` | `string` |  |
| 259 | `F Informe Necesidad` | `string` |  |
| 260 | `F Memoria Económica` | `string` |  |
| 261 | `F Aprobación UT` | `string` |  |
| 262 | `F Aprobación Consejo G.` | `string` |  |
| 263 | `F Resolución Inicio` | `string` |  |
| 264 | `F Aprobación del Gasto` | `string` |  |
| 265 | `F Contabilización Doc. RC` | `string` |  |
| 266 | `F Aprobación PCAP` | `string` |  |
| 267 | `F Aprobación PPT` | `string` |  |
| 268 | `F Fiscalización Doc. A` | `string` |  |
| 269 | `F Contabilización Doc. A` | `string` |  |
| 270 | `F Convocatoria Mesa` | `string` |  |
| 271 | `Lugar de Presentación Ofertas` | `string` |  |
| 272 | `F Recepción Ofertas` | `string` |  |
| 273 | `Fecha Apertura Sobre 1` | `string` |  |
| 274 | `Fecha Apertura Sobre 2` | `string` |  |
| 275 | `Fecha Apertura Sobre 3` | `string` |  |
| 276 | `Fecha Apertura Sobre 4` | `string` |  |
| 277 | `F Apertura Oferta Técnica` | `string` |  |
| 278 | `F Oferta Eco. + Ventajosa` | `string` |  |
| 279 | `Fecha Adjudicación Prov.` | `string` |  |
| 280 | `Importe Anuncio BOE` | `string` |  |
| 281 | `Importe Anuncio DOCM` | `string` |  |
| 282 | `Nota Pública` | `string` |  |
| 283 | `Nota Interna` | `string` |  |
| 284 | `Importe Redacción Proyecto` | `string` |  |
| 285 | `Importe Dirección de Obra` | `string` |  |
| 286 | `F Prop. Gasto (C. Menor)` | `string` |  |
| 287 | `F Prop. Cont. Gasto (C. Menor)` | `string` |  |
| 288 | `Unnamed: 9` | `string` |  |
| 289 | `Unnamed: 11` | `string` |  |
| 290 | `Unnamed: 1` | `string` |  |
| 291 | `Unnamed: 6` | `string` |  |
| 292 | `Sub Objeto de Gasto` | `string` |  |
| 293 | `Importe/Otros gastos sin IVA` | `string` |  |
| 294 | `% ret IRPF` | `string` |  |
| 295 | `RETENCIÓN IRPF ` | `string` |  |
| 296 | `Adjudicatario/Proveedor` | `string` |  |
| 297 | `Nº Fac` | `string` |  |
| 298 | `CL` | `string` |  |
| 299 | `DIRECCIÓN` | `string` |  |
| 300 | `LOCALIDAD Y PROVINCIA` | `string` |  |
| 301 | `PROYECTO FUNDACION` | `string` |  |
| 302 | `Tfn` | `string` |  |
| 303 | `NO ESTAN LAS FACTURAS Y SE HA PAGADO` | `string` |  |
| 304 | `PAGO Nº AS` | `string` |  |
| 305 | `FECHA Y FORMA PAGO` | `string` |  |
| 306 | `CTA/IBAN` | `string` |  |
| 307 | `CERTIFICA` | `string` |  |
| 308 | `CORREO ELECTRÓNICO` | `string` |  |
| 309 | `columna_1` | `string` |  |
| 310 | `columna_2` | `string` |  |
| 311 | `columna_3` | `string` |  |
| 312 | `columna_4` | `string` |  |
| 313 | `columna_5` | `string` |  |
| 314 | `columna_6` | `string` |  |
| 315 | `columna_7` | `string` |  |
| 316 | `columna_8` | `string` |  |
| 317 | `columna_9` | `string` |  |
| 318 | `columna_10` | `string` |  |
| 319 | `columna_11` | `string` |  |
| 320 | `columna_12` | `string` |  |
| 321 | `columna_13` | `string` |  |
| 322 | `columna_14` | `string` |  |
| 323 | `columna_15` | `string` |  |
| 324 | `columna_16` | `string` |  |
| 325 | `columna_17` | `string` |  |
| 326 | `columna_18` | `string` |  |
| 327 | `columna_19` | `string` |  |
| 328 | `columna_20` | `string` |  |
| 329 | `columna_21` | `string` |  |
| 330 | `columna_22` | `string` |  |
| 331 | `columna_23` | `string` |  |
| 332 | `columna_24` | `string` |  |
| 333 | `columna_25` | `string` |  |
| 334 | `columna_26` | `string` |  |
| 335 | `columna_27` | `string` |  |
| 336 | `columna_28` | `string` |  |
| 337 | `columna_29` | `string` |  |
| 338 | `columna_30` | `string` |  |
| 339 | `columna_31` | `string` |  |
| 340 | ` Tipo IVA` | `string` |  |
| 341 | `Importe  total con IVA` | `string` |  |
| 342 | `Último precio unitario` | `string` |  |
| 343 | `N.I.F.ADJUDICATARIO` | `string` |  |
| 344 | `Adjudicado Imp. sin Impuestos` | `string` |  |
| 345 | `Adjudicado Imp. Impuestos` | `string` |  |
| 346 | `Adjudicatario` | `string` |  |
| 347 | `F Pub Adjudicación en Perfil de Contratante` | `string` |  |
| 348 | `GASTO Nº AS.` | `string` |  |
| 349 | `Importe Adjudicación Bruto` | `string` |  |
| 350 | `ORGANO CONTRATANTE` | `string` |  |
| 351 | `Texto cab.documento` | `string` |  |
| 352 | `Fecha documento` | `string` |  |
| 353 | `Contrato Menor Suministro` | `string` |  |
| 354 | `SECRETARIA GENERAL` | `string` |  |
| 355 | `UNITEL SISTEMA DE COMUNICACIONES DE` | `string` |  |
| 356 | `B45381217` | `string` |  |
| 357 | `100014238` | `string` |  |
| 358 | `Material informát no inv` | `string` |  |
| 359 | `SUM. MATERIAL INFORMATICO` | `string` |  |
| 360 | `340.02479338842977` | `string` |  |
| 361 | `15.12.2016` | `string` |  |
| 362 | `2102GU00` | `string` |  |
| 363 | `G/521B/22002` | `string` |  |
| 364 | `Tipo Contrato` | `string` |  |
| 365 | `Órgano Contratante` | `string` |  |
| 366 | `Importe €  sin IVA` | `string` |  |
| 367 | `Fecha` | `string` |  |
| 368 | `Unnamed: 3` | `string` |  |
| 369 | `Importe con IVA trimestre 4` | `string` |  |
| 370 | `Cantidad  trimestre 4` | `string` |  |
| 371 | `Último precio` | `string` |  |
| 372 | `Importe sin IVA  trimestre 4` | `string` |  |
| 373 | `Importe Total CON IVA` | `string` |  |
| 374 | `Cantidad Total` | `string` |  |
| 375 | `Último precio unitario CON  IVA` | `string` |  |
| 376 | `Importe total SIN IVA` | `string` |  |
| 377 | `Consejería/OOAA/Ente` | `string` |  |
| 378 | `Órgano de Contratación` | `string` |  |
| 379 | `Importe (sin Impuestos) *1` | `string` |  |
| 380 | `Importe (con Impuestos)` | `string` |  |
| 381 | `Duración (en días) * 2` | `string` |  |
| 382 | `Adjudicatario * 3` | `string` |  |
| 383 | `Consejería` | `string` |  |
| 384 | `Organo de contratación` | `string` |  |
| 385 | `Objeto del contrato` | `string` |  |
| 386 | `Importe (sin impuestos)` | `string` |  |
| 387 | `Importe (con impuestos)` | `string` |  |
| 388 | `Duración (en días)` | `string` |  |
| 389 | `Nº Expediente Gestor PICOS` | `string` |  |
| 390 | `Nº Expediente Tramitadora` | `string` |  |
| 391 | `F Pub Individual Adjudicación en Plataforma de Contratación` | `string` |  |
| 392 | `Plazo Ejecución / Entrega (en días)` | `string` |  |
| 393 | `Org.Gestor` | `string` |  |
| 394 | `Importe 3º Trimestre CON IVA` | `string` |  |
| 395 | `Cantidad 3º Trimestre` | `string` |  |
| 396 | `Último precio CON IVA` | `string` |  |
| 397 | `Importe 3º Trimestre SIN  IVA` | `string` |  |
| 398 | `Órgano de contratación` | `string` |  |
| 399 | `Importe 4º Trimestre con IVA` | `string` |  |
| 400 | `Cantidad 4º Trimestre` | `string` |  |
| 401 | `Último precio unitario con IVA` | `string` |  |
| 402 | `Importe 4º Trimestre sin IVA` | `string` |  |
| 403 | `Importe (sin Impuestos)` | `string` |  |
| 404 | `Adjudicatario ` | `string` |  |
| 405 | `Importe Total` | `string` |  |
| 406 | `TRIMESTRE I IMPORTE CON IVA` | `string` |  |
| 407 | `Último precio Unitario CON IVA` | `string` |  |
| 408 | `TRIMESTRE I IMPORTE SIN IVA` | `string` |  |
| 409 | `ÓRGANO CONTRATANTE` | `string` |  |
| 410 | `IMPORTE (SIN IVA)` | `string` |  |
| 411 | `Org.Contratante` | `string` |  |
| 412 | `Importe sin iva` | `string` |  |
| 413 | `Importe sin Iva` | `string` |  |
| 414 | `IMPORTE CON IVA TRIMESTRE II` | `string` |  |
| 415 | `CANTIDAD TRIMESTRE II` | `string` |  |
| 416 | `ULTIMO PRECIO UNITARIO CON IVA TRIMESTRE II` | `string` |  |
| 417 | `IMPORTE SIN IVA TRIMESTRE II` | `string` |  |
| 418 | `Cantidad total` | `string` |  |
| 419 | `Consejería/Organismo` | `string` |  |
| 420 | `F Pub Adjudicación` | `string` |  |
| 421 | `GERENCIA` | `string` |  |
| 422 | `TIPO_COMPRA` | `string` |  |
| 423 | `ARTICULO` | `string` |  |
| 424 | `PROVEEDOR` | `string` |  |
| 425 | `Nº FACTURA` | `string` |  |
| 426 | `IMPORTE TOTAL CON IVA` | `string` |  |
| 427 | `CANTIDAD TOTAL` | `string` |  |
| 428 | `ULTIMO PRECIO CON IVA` | `string` |  |
| 429 | `IMPORTE TOTAL SIN IVA` | `string` |  |
| 430 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 431 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 432 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 433 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 434 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 435 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 436 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 437 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 438 | `_miembro` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 439 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 440 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 441 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 442 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 443 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `sescam.parquet`

4.162.351 filas · 176 columnas · 30 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Gerencia` | `string` |  |
| 2 | `Tipo compra` | `string` |  |
| 3 | `Artículo` | `string` |  |
| 4 | `Proveedor` | `string` |  |
| 5 | `Nº de factura` | `string` |  |
| 6 | `Importe 4 TRI 2019` | `string` |  |
| 7 | `Cantidad 4 TRI 2019` | `string` |  |
| 8 | `Último precio 4 TRIM 2019` | `string` |  |
| 9 | `IVA` | `string` |  |
| 10 | `Importe sin IVA 4 TRIM 2019` | `string` |  |
| 11 | `Importe actual` | `string` |  |
| 12 | `Cantidad actual` | `string` |  |
| 13 | `Último precio` | `string` |  |
| 14 | `Importe sin IVA` | `string` |  |
| 15 | `Importe TRI 3 2019` | `string` |  |
| 16 | `Cantidad TRI 3 2019` | `string` |  |
| 17 | `Último precio TRI 3 2019` | `string` |  |
| 18 | `Importe sin IVA TRI 3 2019` | `string` |  |
| 19 | `GERENCIA` | `string` |  |
| 20 | `TIPO_COMPRA` | `string` |  |
| 21 | `ARTICULO` | `string` |  |
| 22 | `PROVEEDOR` | `string` |  |
| 23 | `N_FACTURA` | `string` |  |
| 24 | `IMPORTE CON IVA` | `string` |  |
| 25 | `CANTIDAD` | `string` |  |
| 26 | `ULTIMO PRECIO UNITARIO` | `string` |  |
| 27 | `% IVA` | `string` |  |
| 28 | `IMPORTE SIN IVA` | `string` |  |
| 29 | `Importe 4 TRIM 2020` | `string` |  |
| 30 | `Cantidad 4 TRIM 2020` | `string` |  |
| 31 | `Último precio 4 TRIM 2020` | `string` |  |
| 32 | `Importe sin IVA 4 TRIM 2020` | `string` |  |
| 33 | `OBSERVACIONES` | `string` |  |
| 34 | `Importe 2 TRIM 2020` | `string` |  |
| 35 | `Cantidad 2 TRIM 2020` | `string` |  |
| 36 | `Último precio       2 TRIM 2020` | `string` |  |
| 37 | `Importe sin IVA 2 TRIM 2020` | `string` |  |
| 38 | `Importe 3 TRIM 2020` | `string` |  |
| 39 | `Cantidad 3 TRIM 2020` | `string` |  |
| 40 | `Último precio 3 TRIM 2020` | `string` |  |
| 41 | `Importe sin IVA 3 TRIM 2020` | `string` |  |
| 42 | `Importe                           1 TRIM 2020` | `string` |  |
| 43 | `Cantidad       1 TRIM 2020` | `string` |  |
| 44 | `Último precio       1 TRIM 2020` | `string` |  |
| 45 | `Importe sin IVA                     1 TRIM 2020` | `string` |  |
| 46 | `Importe 4 TRIM 2021` | `string` |  |
| 47 | `Cantidad 4 TRIM 2021` | `string` |  |
| 48 | `Último precio 4 TRIM 2021` | `string` |  |
| 49 | `Importe sin IVA 4 TRIM 2021` | `string` |  |
| 50 | `Importe 1 TRIM 2021` | `string` |  |
| 51 | `Cantidad 1 TRIM 2021` | `string` |  |
| 52 | `Último precio 1 TRIM 2021` | `string` |  |
| 53 | `Importe sin IVA 1 TRIM 2021` | `string` |  |
| 54 | `Observaciones` | `string` |  |
| 55 | `Importe 2 TRIM 2021` | `string` |  |
| 56 | `Cantidad 2 TRIM 2021` | `string` |  |
| 57 | `Último precio 2 TRIM 2021` | `string` |  |
| 58 | `Importe sin IVA 2 TRIM 2021` | `string` |  |
| 59 | `Importe 3 TRIM 2021` | `string` |  |
| 60 | `Cantidad 3 TRIM 2021` | `string` |  |
| 61 | `Último precio 3 TRIM 2021` | `string` |  |
| 62 | `Importe sin IVA 3 TRIM 2021` | `string` |  |
| 63 | `Importe 3 TRIM 2022` | `string` |  |
| 64 | `Cantidad 3 TRIM 2022` | `string` |  |
| 65 | `Último precio 3 TRIM 2022` | `string` |  |
| 66 | `Importe sin IVA 3 TRIM 2022` | `string` |  |
| 67 | `Importe 2 TRIM 2022` | `string` |  |
| 68 | `Cantidad 2 TRIM 2022` | `string` |  |
| 69 | `Último precio 2 TRIM 2022` | `string` |  |
| 70 | `Importe sin IVA 2 TRIM 2022` | `string` |  |
| 71 | `Importe 1 TRIM 2022` | `string` |  |
| 72 | `Cantidad 1 TRIM 2022` | `string` |  |
| 73 | `Último precio 1 TRIM 2022` | `string` |  |
| 74 | `Importe sin IVA 1 TRIM 2022` | `string` |  |
| 75 | `Importe con IVA 4 TRIM 2023` | `string` |  |
| 76 | `Cantidad 4 TRIM 2023 ` | `string` |  |
| 77 | `PVL Precio unitario sin IVA` | `string` |  |
| 78 | `Importe sin IVA 4 TRIM 2023 (sin descuentos)` | `string` |  |
| 79 | `Descuento 1` | `string` |  |
| 80 | `Descuento 2` | `string` |  |
| 81 | `Importe sin IVA 4 TRIM 2023 (con descuento 1+2)` | `string` |  |
| 82 | `Precio unitario con IVA 4 TRIM 2023` | `string` |  |
| 83 | `Importe  sin IVA 4 TRIM 2023` | `string` |  |
| 84 | `Importe 1 TRIM 2023` | `string` |  |
| 85 | `Cantidad 1 TRIM 2023` | `string` |  |
| 86 | `Último precio 1 TRIM 2023` | `string` |  |
| 87 | `Importe sin IVA 1 TRIM 2023` | `string` |  |
| 88 | `Importe con IVA 2 TRIM 2023` | `string` |  |
| 89 | `Cantidad 2 TRIM 2023 ` | `string` |  |
| 90 | `Precio medio unitario con IVA 2 TRIM 2023` | `string` |  |
| 91 | `Importe  sin IVA 2 TRIM 2023` | `string` |  |
| 92 | `Importe con IVA 3 TRIM 2023` | `string` |  |
| 93 | `Cantidad 3 TRIM 2023 ` | `string` |  |
| 94 | `Importe sin IVA 3 TRIM 2023 (sin descuentos)` | `string` |  |
| 95 | `Importe sin IVA 3 TRIM 2023 (con descuento 1+2)` | `string` |  |
| 96 | `Precio unitario con IVA 3 TRIM 2023` | `string` |  |
| 97 | `Importe  sin IVA 3 TRIM 2023` | `string` |  |
| 98 | `Importe con IVA 4 TRIM 2024` | `string` |  |
| 99 | `Cantidad 4 TRIM 2024 ` | `string` |  |
| 100 | `Precio unitario con IVA 4 TRIM 2024` | `string` |  |
| 101 | `Importe  sin IVA 4 TRIM 2024` | `string` |  |
| 102 | `Importe sin IVA 4 TRIM 2024 (sin descuentos)` | `string` |  |
| 103 | `Importe sin IVA 4 TRIM 2024 (con descuento 1+2)` | `string` |  |
| 104 | `Importe con IVA 1 TRIM 2024` | `string` |  |
| 105 | `Cantidad 1 TRIM 2024 ` | `string` |  |
| 106 | `Importe sin IVA 1 TRIM 2024 (sin descuentos)` | `string` |  |
| 107 | `Importe sin IVA 1 TRIM 2024 (con descuento 1+2)` | `string` |  |
| 108 | `Precio unitario con IVA 1 TRIM 2024` | `string` |  |
| 109 | `Importe  sin IVA 1 TRIM 2024` | `string` |  |
| 110 | `Importe con IVA 2 TRIM 2024` | `string` |  |
| 111 | `Cantidad 2 TRIM 2024 ` | `string` |  |
| 112 | `Importe sin IVA 2 TRIM 2024 (sin descuentos)` | `string` |  |
| 113 | `Importe sin IVA 2 TRIM 2024 (con descuento 1+2)` | `string` |  |
| 114 | `Precio unitario con IVA 2 TRIM 2024` | `string` |  |
| 115 | `Importe  sin IVA 2 TRIM 2024` | `string` |  |
| 116 | `Importe con IVA 3 TRIM 2024` | `string` |  |
| 117 | `Cantidad 3 TRIM 2024 ` | `string` |  |
| 118 | `Precio unitario con IVA 3 TRIM 2024` | `string` |  |
| 119 | `Importe  sin IVA 3 TRIM 2024` | `string` |  |
| 120 | `PVL Precio unitario sin IVA ` | `string` |  |
| 121 | `Importe sin IVA 3 TRIM 2024 (sin descuentos)` | `string` |  |
| 122 | `Importe sin IVA 3 TRIM 2024 (con descuento 1+2)` | `string` |  |
| 123 | `Importe con IVA 4 TRIM 2025` | `string` |  |
| 124 | `Cantidad 4 TRIM 2025 ` | `string` |  |
| 125 | `Precio unitario con IVA 4 TRIM 2025` | `string` |  |
| 126 | `Importe  sin IVA 4 TRIM 2025` | `string` |  |
| 127 | `Importe sin IVA 4 TRIM 2025 (sin descuentos)` | `string` |  |
| 128 | `Importe sin IVA 4 TRIM 2025 (con descuento 1+2)` | `string` |  |
| 129 | `Importe con IVA 1 TRIM 2025` | `string` |  |
| 130 | `Cantidad 1 TRIM 2025 ` | `string` |  |
| 131 | `Importe sin IVA 1 TRIM 2025 (sin descuentos)` | `string` |  |
| 132 | `Importe sin IVA 1 TRIM 2025 (con descuento 1+2)` | `string` |  |
| 133 | `Precio unitario con IVA 1 TRIM 2025` | `string` |  |
| 134 | `Importe  sin IVA 1 TRIM 2025` | `string` |  |
| 135 | `Importe con IVA 2 TRIM 2025` | `string` |  |
| 136 | `Cantidad 2 TRIM 2025 ` | `string` |  |
| 137 | `Precio unitario con IVA 2 TRIM 2025` | `string` |  |
| 138 | `Importe  sin IVA 2 TRIM 2025` | `string` |  |
| 139 | `Importe sin IVA 2 TRIM 2025 (sin descuentos)` | `string` |  |
| 140 | `Importe sin IVA 2 TRIM 2025 (con descuento 1+2)` | `string` |  |
| 141 | `Importe con IVA 3 TRIM 2025` | `string` |  |
| 142 | `Cantidad 3 TRIM 2025 ` | `string` |  |
| 143 | `Precio unitario con IVA 3 TRIM 2025` | `string` |  |
| 144 | `Importe  sin IVA 3 TRIM 2025` | `string` |  |
| 145 | `Importe sin IVA 3 TRIM 2025 (sin descuentos)` | `string` |  |
| 146 | `Importe sin IVA 3 TRIM 2025 (con descuento 1+2)` | `string` |  |
| 147 | `Importe con IVA 1 TRIM 2026` | `string` |  |
| 148 | `Cantidad 1 TRIM 2026` | `string` |  |
| 149 | `Precio unitario con IVA 1 TRIM 2026` | `string` |  |
| 150 | `Importe  sin IVA 1 TRIM 2026` | `string` |  |
| 151 | `Importe sin IVA 1 TRIM 2026 (sin descuentos)` | `string` |  |
| 152 | `Importe sin IVA 1 TRIM 2026 (con descuento 1+2)` | `string` |  |
| 153 | `Importe con IVA 2 TRIM 2026` | `string` |  |
| 154 | `Cantidad 2 TRIM 2026` | `string` |  |
| 155 | `Precio unitario con IVA 2 TRIM 2026` | `string` |  |
| 156 | `Importe  sin IVA 2 TRIM 2026` | `string` |  |
| 157 | `Importe sin IVA 2 TRIM 2026 (sin descuentos)` | `string` |  |
| 158 | `Importe sin IVA 2 TRIM 2026 (con descuento 1+2)` | `string` |  |
| 159 | `Importe 4 TRIM 2022` | `string` |  |
| 160 | `Cantidad 4 TRIM 2022` | `string` |  |
| 161 | `Último precio 4 TRIM 2022` | `string` |  |
| 162 | `Importe sin IVA 4 TRIM 2022` | `string` |  |
| 163 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 164 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 165 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 166 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 167 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 168 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 169 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 170 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 171 | `_miembro` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 172 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 173 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 174 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 175 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 176 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `uclm.parquet`

254.130 filas · 19 columnas · 10 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Nif` | `string` |  |
| 2 | `Proveedor` | `string` |  |
| 3 | `Expediente` | `string` |  |
| 4 | `Objeto` | `string` |  |
| 5 | `Duración` | `string` |  |
| 6 | `Adjudicado` | `string` |  |
| 7 | `Un.Funcional` | `string` |  |
| 8 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 9 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 10 | `_titulo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 11 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 12 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 13 | `_periodo` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 14 | `_unidad` | `string` | [de la fuente](../fuentes/castilla_la_mancha.md) |
| 15 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 16 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 19 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
