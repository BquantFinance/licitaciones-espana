# `extremadura.zip`

Esquema de los Parquet de `extremadura.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/extremadura.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `registro_contratos_incidencias.parquet` | 3.508 | 100 |
| `registro_contratos_mayores.parquet` | 10.539 | 170 |
| `registro_contratos_menores.parquet` | 207.702 | 71 |

## `registro_contratos_incidencias.parquet`

3.508 filas · 100 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Nº Contrato` | `string` |  |
| 2 | `Nº Expediente` | `string` |  |
| 3 | `Consejería` | `string` |  |
| 4 | `Órgano de resolución` | `string` |  |
| 5 | `Tipo contrato` | `string` |  |
| 6 | `Objeto` | `string` |  |
| 7 | `CIF/NIF contratista` | `string` |  |
| 8 | `Denominación contratista` | `string` |  |
| 9 | `Pais` | `string` |  |
| 10 | `Condición de PYME` | `string` |  |
| 11 | `Nº Lotes` | `string` |  |
| 12 | `Nº Prorroga` | `string` |  |
| 13 | `Fecha de resolución` | `string` |  |
| 14 | `Año` | `string` |  |
| 15 | `Comunicante` | `string` |  |
| 16 | `Causas` | `string` |  |
| 17 | `Días de retraso` | `string` |  |
| 18 | `€/día` | `string` |  |
| 19 | `Total €` | `string` |  |
| 20 | `Duración` | `string` |  |
| 21 | `Fecha de petición de audiencia` | `string` |  |
| 22 | `Fecha informe` | `string` |  |
| 23 | `Fecha fin de plazo` | `string` |  |
| 24 | `Fecha de asiento` | `string` |  |
| 25 | `Fichero` | `string` |  |
| 26 | `Nº Modificación` | `string` |  |
| 27 | `Fecha de aprobación` | `string` |  |
| 28 | `Fecha de formalización` | `string` |  |
| 29 | `Plazo (meses)` | `string` |  |
| 30 | `Importe de adjudicación` | `string` |  |
| 31 | `Importe de adjudicación de modificación` | `string` |  |
| 32 | `% Variación` | `string` |  |
| 33 | `Importe de reajuste de garantía` | `string` |  |
| 34 | `Naturaleza` | `string` |  |
| 35 | `Causa` | `string` |  |
| 36 | `Fecha de fin` | `string` |  |
| 37 | `Importe de adjudicación de la prorroga` | `string` |  |
| 38 | `% incremento IPC` | `string` |  |
| 39 | `Importe de reajuste de la garantía` | `string` |  |
| 40 | `Nº Resolución` | `string` |  |
| 41 | `Fecha de acuerdo` | `string` |  |
| 42 | `Ley` | `string` |  |
| 43 | `Estado actual` | `string` |  |
| 44 | `Pérdida de garantía` | `string` |  |
| 45 | `Fecha incautación fianza` | `string` |  |
| 46 | `Importe incautación` | `string` |  |
| 47 | `Fecha valoración de daños` | `string` |  |
| 48 | `Importe valoración de daños` | `string` |  |
| 49 | `Fecha resarcimiento de daños` | `string` |  |
| 50 | `Importe resarcimiento de daños` | `string` |  |
| 51 | `Unnamed: 27` | `string` |  |
| 52 | `Ejercicio` | `string` |  |
| 53 | `Número de registro de contrato` | `string` |  |
| 54 | `Número de anotación` | `string` |  |
| 55 | `Código  expediente de origen` | `string` |  |
| 56 | `Órgano Gestor` | `string` |  |
| 57 | `Unnamed: 5` | `string` |  |
| 58 | `Órgano de contratación` | `string` |  |
| 59 | `Unnamed: 7` | `string` |  |
| 60 | `Tipo de Incidencia` | `string` |  |
| 61 | `Descripción` | `string` |  |
| 62 | `Fecha de Anotación` | `string` |  |
| 63 | `Título del expediente` | `string` |  |
| 64 | `Causa modificación` | `string` |  |
| 65 | `Número de modificación` | `string` |  |
| 66 | `Fecha de resolución de actuación` | `string` |  |
| 67 | `Causa de Resolución` | `string` |  |
| 68 | `Descripción.1` | `string` |  |
| 69 | `Tipo de contrato` | `string` |  |
| 70 | `Variación precio neto` | `string` |  |
| 71 | `Variación impuesto ` | `string` |  |
| 72 | `Variación total del precio ` | `string` |  |
| 73 | `Nueva fecha fin` | `string` |  |
| 74 | `Plazo de ejecución` | `string` |  |
| 75 | `Unidad de plazo` | `string` |  |
| 76 | `Descripción.2` | `string` |  |
| 77 | `Incautación de garantía` | `string` |  |
| 78 | `NIF` | `string` |  |
| 79 | `RECO-Órg.Gestor` | `string` |  |
| 80 | `Consejería/Organismo/Entidad` | `string` |  |
| 81 | `RECO-Órgano de contratación` | `string` |  |
| 82 | `Denominación órgano de contratación` | `string` |  |
| 83 | `Incidencia` | `string` |  |
| 84 | `Código del expediente` | `string` |  |
| 85 | `Objeto del contrato` | `string` |  |
| 86 | `Variación total del precio` | `string` |  |
| 87 | `Causa de resolución` | `string` |  |
| 88 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 89 | `_pagina` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 90 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 91 | `_tipo` | `string` | [de la fuente](../fuentes/extremadura.md) |
| 92 | `_nombre_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 93 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 94 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 95 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 96 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 97 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 98 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 99 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 100 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos_mayores.parquet`

10.539 filas · 170 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Nº Contrato` | `string` |  |
| 2 | `Nº Expediente` | `string` |  |
| 3 | `Consejería` | `string` |  |
| 4 | `Órgano` | `string` |  |
| 5 | `Tipo contrato` | `string` |  |
| 6 | `Objeto` | `string` |  |
| 7 | `Año` | `string` |  |
| 8 | `Remisión Trimestral TC` | `string` |  |
| 9 | `Modalidad GSP` | `string` |  |
| 10 | `Código CPV` | `string` |  |
| 11 | `Denominación CPV` | `string` |  |
| 12 | `Localización` | `string` |  |
| 13 | `Bien` | `string` |  |
| 14 | `Lotes` | `string` |  |
| 15 | `Nº lotes` | `string` |  |
| 16 | `Total lotes` | `string` |  |
| 17 | `Descripción de los lotes` | `string` |  |
| 18 | `Descripción de los lotes.1` | `string` |  |
| 19 | `Descripción de los lotes.2` | `string` |  |
| 20 | `Clasificaciones` | `string` |  |
| 21 | `Clasificaciones.1` | `string` |  |
| 22 | `Clasificaciones.2` | `string` |  |
| 23 | `Precios Unitarios` | `string` |  |
| 24 | `Precios Unitarios.1` | `string` |  |
| 25 | `Mixto` | `string` |  |
| 26 | `Marco` | `string` |  |
| 27 | `Complementario` | `string` |  |
| 28 | `Nº expediente primitivo` | `string` |  |
| 29 | `Tramitación anticipada` | `string` |  |
| 30 | `Usuario` | `string` |  |
| 31 | `Fecha asiento` | `string` |  |
| 32 | `Consejería Remisión` | `string` |  |
| 33 | `Órgano Remisión` | `string` |  |
| 34 | `Fichero` | `string` |  |
| 35 | `Observaciones órgano de contratación` | `string` |  |
| 36 | `Observaciones registro de contratos` | `string` |  |
| 37 | `Incluir en fichero RPC` | `string` |  |
| 38 | `Validado` | `string` |  |
| 39 | `Fecha validación` | `string` |  |
| 40 | `Contrato reservado` | `string` |  |
| 41 | `Tramitación` | `string` |  |
| 42 | `Criterio de adjudicación` | `string` |  |
| 43 | `Procedimiento negociado causa` | `string` |  |
| 44 | `Valor estimado (IVA excluido)` | `string` |  |
| 45 | `Nº invitaciones` | `string` |  |
| 46 | `Clasificaciones.3` | `string` |  |
| 47 | `Clasificaciones.4` | `string` |  |
| 48 | `Clasificaciones.5` | `string` |  |
| 49 | `Aplicaciones presupuestarias` | `string` |  |
| 50 | `Plazo previsto (meses)` | `string` |  |
| 51 | `Revisión de precios` | `string` |  |
| 52 | `Fórmula` | `string` |  |
| 53 | `Presupuesto licitación` | `string` |  |
| 54 | `Presupuesto licitación sin IVA` | `string` |  |
| 55 | `Contrato plurianual` | `string` |  |
| 56 | `Publicidad` | `string` |  |
| 57 | `Fecha licitación DOE` | `string` |  |
| 58 | `Fecha licitación BOE` | `string` |  |
| 59 | `Fecha licitación DOUE` | `string` |  |
| 60 | `Fecha anuncio perfil` | `string` |  |
| 61 | `Fecha licitación prensa` | `string` |  |
| 62 | `Fecha de adjudicación` | `string` |  |
| 63 | `CIF/NIF Contratista` | `string` |  |
| 64 | `Denominación contratista` | `string` |  |
| 65 | `Pais` | `string` |  |
| 66 | `Condición de PYME` | `string` |  |
| 67 | `Importe adjudicación` | `string` |  |
| 68 | `Porcentaje medio baja` | `string` |  |
| 69 | `Porcentaje baja adjudicatario` | `string` |  |
| 70 | `Modalidad importe` | `string` |  |
| 71 | `Valor modalidad` | `string` |  |
| 72 | `Unidad de medida` | `string` |  |
| 73 | `Aportaciones administración` | `string` |  |
| 74 | `Fecha formalización DOE` | `string` |  |
| 75 | `Fecha formalización BOE` | `string` |  |
| 76 | `Fecha formalización DOUE` | `string` |  |
| 77 | `Fecha formalización perfil contratante` | `string` |  |
| 78 | `Fecha formalización` | `string` |  |
| 79 | `Fecha prevista finalización` | `string` |  |
| 80 | `Importe fianza` | `string` |  |
| 81 | `Fecha inicio ejecución` | `string` |  |
| 82 | `Plazo ejecución (meses)` | `string` |  |
| 83 | `Origen producto` | `string` |  |
| 84 | `Importe adjudicado (IVA excluido)` | `string` |  |
| 85 | `SARA` | `string` |  |
| 86 | `Ingreso` | `string` |  |
| 87 | `Modalidad precio` | `string` |  |
| 88 | `Anexo III Incidencias TC` | `string` |  |
| 89 | `Fichero R.P.C.` | `string` |  |
| 90 | `Listado I Documentación TC` | `string` |  |
| 91 | `Listado I Relación Certificada TC` | `string` |  |
| 92 | `Nº Lote` | `string` |  |
| 93 | `Objeto del lote` | `string` |  |
| 94 | `Imp. licitación lote` | `string` |  |
| 95 | `Imp. adjudicación lote` | `string` |  |
| 96 | `Nº Precio Unitario` | `string` |  |
| 97 | `Precio Unitario` | `string` |  |
| 98 | `Código CPV.1` | `string` |  |
| 99 | `Denominación CPV.1` | `string` |  |
| 100 | `Grupo` | `string` |  |
| 101 | `Subgrupo` | `string` |  |
| 102 | `Categoría` | `string` |  |
| 103 | `Número de registro de contrato` | `string` |  |
| 104 | `Código del expediente` | `string` |  |
| 105 | `RECO-Órg.Gestor` | `string` |  |
| 106 | `Consejería/organismo/entidad` | `string` |  |
| 107 | `RECO-Órgano de contratación` | `string` |  |
| 108 | `Denominación órgano de contratación` | `string` |  |
| 109 | `Procedimiento de Adjudicación` | `string` |  |
| 110 | `Tipo de Contrato` | `string` |  |
| 111 | `Criterios de Adjudicación` | `string` |  |
| 112 | `Clase de tramitación` | `string` |  |
| 113 | `Indicador de plurianual` | `string` |  |
| 114 | `Indicador de lotes` | `string` |  |
| 115 | `Lote` | `string` |  |
| 116 | `Supuesto Negociado` | `string` |  |
| 117 | `Características bienes suministrados` | `string` |  |
| 118 | `Modalidad concesión servicio público` | `string` |  |
| 119 | `Contrato Complementario` | `string` |  |
| 120 | `Expediente de Referencia` | `string` |  |
| 121 | `Contrato Mixto` | `string` |  |
| 122 | `Ind.revisión de precios` | `string` |  |
| 123 | `Indicador de prórroga` | `string` |  |
| 124 | `Modificado` | `string` |  |
| 125 | `Determinación del Precio` | `string` |  |
| 126 | `Provincia de Ejecución` | `string` |  |
| 127 | `Valor estimado` | `string` |  |
| 128 | `Importe Licitación sin Impuesto` | `string` |  |
| 129 | `Importe del impuesto` | `string` |  |
| 130 | `Importe licitación con impuestos` | `string` |  |
| 131 | `Fecha de publicación` | `string` |  |
| 132 | `Fecha de publicación DOUE` | `string` |  |
| 133 | `Nº de ofertas recibidas` | `string` |  |
| 134 | `Fecha adjudicación` | `string` |  |
| 135 | `Fecha de publicación.1` | `string` |  |
| 136 | `Imp. Adjudicación sin Impuesto` | `string` |  |
| 137 | `Importe Adjudicación` | `string` |  |
| 138 | `Importe del impuesto.1` | `string` |  |
| 139 | `NIF` | `string` |  |
| 140 | `Fecha Formalización contrato` | `string` |  |
| 141 | `Fecha de publicación.2` | `string` |  |
| 142 | `Fecha inicio ejecución contrato` | `string` |  |
| 143 | `Fecha fin ejecución contrato` | `string` |  |
| 144 | `Plazo ejecución` | `string` |  |
| 145 | `Unidad de plazo` | `string` |  |
| 146 | `Consejería/Organismo/Entidad` | `string` |  |
| 147 | `Tipo de contrato` | `string` |  |
| 148 | `Provincia de ejecución` | `string` |  |
| 149 | `Objeto del contrato` | `string` |  |
| 150 | `Procedimiento de adjudicación` | `string` |  |
| 151 | `Valor estimado del contrato` | `string` |  |
| 152 | `Plazo de Ejecución (Meses)` | `string` |  |
| 153 | `Plurianual` | `string` |  |
| 154 | `Denominación Adjudicatario` | `string` |  |
| 155 | `Importe de adjudicación sin impuestos` | `string` |  |
| 156 | `Importe de adjudicación con impuestos` | `string` |  |
| 157 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 158 | `_pagina` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 159 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 160 | `_tipo` | `string` | [de la fuente](../fuentes/extremadura.md) |
| 161 | `_nombre_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 162 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 163 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 164 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 165 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 166 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 167 | `_repetido_de` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 168 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 169 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 170 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `registro_contratos_menores.parquet`

207.702 filas · 71 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Nº Contrato` | `string` |  |
| 2 | `Nº Expediente` | `string` |  |
| 3 | `Consejería` | `string` |  |
| 4 | `Órgano` | `string` |  |
| 5 | `Órgano delega` | `string` |  |
| 6 | `Tipo contrato` | `string` |  |
| 7 | `Objeto` | `string` |  |
| 8 | `Fecha de adjudicación` | `string` |  |
| 9 | `Año` | `string` |  |
| 10 | `CIF/NIF Contratista` | `string` |  |
| 11 | `Denominación contratista` | `string` |  |
| 12 | `Pais` | `string` |  |
| 13 | `Condición de PYME` | `string` |  |
| 14 | `Código CPV` | `string` |  |
| 15 | `Denominación CPV` | `string` |  |
| 16 | `Localización` | `string` |  |
| 17 | `Procedimiento de adjudicación` | `string` |  |
| 18 | `Importe contrato (con IVA)` | `string` |  |
| 19 | `Importe contrato (sin IVA)` | `string` |  |
| 20 | `Plazo ejecución (meses)` | `string` |  |
| 21 | `Fecha formalización` | `string` |  |
| 22 | `Usuario` | `string` |  |
| 23 | `Fecha asiento` | `string` |  |
| 24 | `Incluir en fichero RPC` | `string` |  |
| 25 | `Validado` | `string` |  |
| 26 | `Fecha validación` | `string` |  |
| 27 | `Contrato reservado` | `string` |  |
| 28 | `Condicón de PYME` | `string` |  |
| 29 | `Número de registro de contrato` | `string` |  |
| 30 | `Código del expediente` | `string` |  |
| 31 | `RECO-Órg.Gestor` | `string` |  |
| 32 | `Consejería/organismo/entidad` | `string` |  |
| 33 | `RECO-Órgano de contratación` | `string` |  |
| 34 | `Denominación órgano de contratación` | `string` |  |
| 35 | `Tipo de Contrato` | `string` |  |
| 36 | `Provincia de Ejecución` | `string` |  |
| 37 | `Fecha adjudicación` | `string` |  |
| 38 | `Imp. Adjudicación sin Impuesto` | `string` |  |
| 39 | `Importe Adjudicación` | `string` |  |
| 40 | `Importe del impuesto` | `string` |  |
| 41 | `NIF` | `string` |  |
| 42 | `Fecha Formalización contrato` | `string` |  |
| 43 | `Fecha inicio ejecución contrato` | `string` |  |
| 44 | `Fecha fin ejecución contrato` | `string` |  |
| 45 | `Plazo ejecución` | `string` |  |
| 46 | `Unidad de plazo` | `string` |  |
| 47 | `Consejería/Organismo/Entidad` | `string` |  |
| 48 | `Tipo de contrato` | `string` |  |
| 49 | `Provincia de ejecución` | `string` |  |
| 50 | `Objeto del contrato` | `string` |  |
| 51 | `Contrato Mixto` | `string` |  |
| 52 | `Criterio de adjudicación` | `string` |  |
| 53 | `Plazo de Ejecución (Meses)` | `string` |  |
| 54 | `Plurianual` | `string` |  |
| 55 | `Denominación Adjudicatario` | `string` |  |
| 56 | `Importe de adjudicación sin impuestos` | `string` |  |
| 57 | `Importe de adjudicación con impuestos` | `string` |  |
| 58 | `_fuente` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 59 | `_pagina` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 60 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 61 | `_tipo` | `string` | [de la fuente](../fuentes/extremadura.md) |
| 62 | `_nombre_publicado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 63 | `_anio` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 64 | `_trimestre` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 65 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 66 | `_hoja` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 67 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 68 | `_repetido_de` | `string` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 69 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 71 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
