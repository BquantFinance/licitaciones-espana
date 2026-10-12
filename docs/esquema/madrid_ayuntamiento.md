# `madrid_ayuntamiento.zip`

Esquema de los Parquet de `madrid_ayuntamiento.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/madrid_ayuntamiento.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `actividad_contractual_madrid_completo.parquet` | 132.087 | 113 |
| `actividad_contractual_madrid_original.parquet` | 132.087 | 207 |

## `actividad_contractual_madrid_completo.parquet`

132.087 filas · 113 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `fuente_fichero` | `string` |  |
| 2 | `categoria` | `string` |  |
| 3 | `estructura` | `string` |  |
| 4 | `n_registro_contrato` | `string` |  |
| 5 | `n_expediente` | `string` |  |
| 6 | `centro_seccion` | `string` |  |
| 7 | `organo_contratacion` | `string` |  |
| 8 | `organismo_contratante` | `string` |  |
| 9 | `organismo_promotor` | `string` |  |
| 10 | `objeto_contrato` | `string` |  |
| 11 | `tipo_contrato` | `string` |  |
| 12 | `subtipo_contrato` | `string` |  |
| 13 | `procedimiento_adjudicacion` | `string` |  |
| 14 | `criterios_adjudicacion` | `string` |  |
| 15 | `codigo_cpv` | `string` |  |
| 16 | `n_invitaciones_cursadas` | `string` |  |
| 17 | `invitados_presentar_oferta` | `string` |  |
| 18 | `importe_licitacion_sin_iva` | `double` |  |
| 19 | `importe_licitacion_iva_inc` | `double` |  |
| 20 | `n_licitadores_participantes` | `string` |  |
| 21 | `n_lotes` | `string` |  |
| 22 | `n_lote` | `string` |  |
| 23 | `nif_adjudicatario` | `string` |  |
| 24 | `razon_social_adjudicatario` | `string` |  |
| 25 | `pyme` | `string` |  |
| 26 | `importe_adjudicacion_sin_iva` | `double` |  |
| 27 | `importe_adjudicacion_iva_inc` | `double` |  |
| 28 | `fecha_adjudicacion` | `timestamp[us]` |  |
| 29 | `porcentaje_baja_adjudicacion` | `string` |  |
| 30 | `fecha_formalizacion` | `timestamp[us]` |  |
| 31 | `fecha_inicio` | `timestamp[us]` |  |
| 32 | `fecha_fin` | `timestamp[us]` |  |
| 33 | `plazo` | `string` |  |
| 34 | `fecha_inscripcion` | `timestamp[us]` |  |
| 35 | `fecha_inscripcion_contrato` | `timestamp[us]` |  |
| 36 | `valor_estimado` | `double` |  |
| 37 | `presupuesto_total_iva_inc` | `double` |  |
| 38 | `aplicacion_presupuestaria` | `string` |  |
| 39 | `acuerdo_marco_flag` | `string` |  |
| 40 | `ingreso_gasto` | `string` |  |
| 41 | `n_contrato_derivado` | `string` |  |
| 42 | `n_expediente_derivado` | `string` |  |
| 43 | `objeto_derivado` | `string` |  |
| 44 | `presupuesto_total_derivado` | `double` |  |
| 45 | `plazo_derivado` | `string` |  |
| 46 | `fecha_aprobacion_derivado` | `timestamp[us]` |  |
| 47 | `fecha_formalizacion_derivado` | `timestamp[us]` |  |
| 48 | `centro_seccion_incidencia` | `string` |  |
| 49 | `n_registro_incidencia` | `string` |  |
| 50 | `tipo_incidencia` | `string` |  |
| 51 | `importe_modificacion` | `double` |  |
| 52 | `fecha_formalizacion_incidencia` | `timestamp[us]` |  |
| 53 | `importe_prorroga` | `double` |  |
| 54 | `plazo_prorroga` | `string` |  |
| 55 | `importe_penalidad` | `double` |  |
| 56 | `fecha_acuerdo_penalidad` | `timestamp[us]` |  |
| 57 | `causa_penalidad` | `string` |  |
| 58 | `motivo` | `string` |  |
| 59 | `adjudicatario_cedente` | `string` |  |
| 60 | `cesionario` | `string` |  |
| 61 | `importe_cedido` | `double` |  |
| 62 | `fecha_autorizacion_cesion` | `timestamp[us]` |  |
| 63 | `fecha_peticion_cesion` | `timestamp[us]` |  |
| 64 | `causa_resolucion` | `string` |  |
| 65 | `causas_generales` | `string` |  |
| 66 | `otras_causas` | `string` |  |
| 67 | `causas_especificas` | `string` |  |
| 68 | `fecha_acuerdo_resolucion` | `timestamp[us]` |  |
| 69 | `n_expediente_sh` | `string` |  |
| 70 | `objeto_sh` | `string` |  |
| 71 | `duracion_procedimiento` | `string` |  |
| 72 | `fecha_fin_actualizada` | `timestamp[us]` |  |
| 73 | `titulo_expediente` | `string` |  |
| 74 | `anio` | `double` |  |
| 75 | `tipo_contrato_normalizado` | `string` |  |
| 76 | `pyme_normalizado` | `string` |  |
| 77 | `importe_licitacion_sin_iva_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 78 | `importe_licitacion_iva_inc_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 79 | `importe_adjudicacion_sin_iva_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 80 | `importe_adjudicacion_iva_inc_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 81 | `valor_estimado_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 82 | `presupuesto_total_iva_inc_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 83 | `presupuesto_total_derivado_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 84 | `importe_modificacion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 85 | `importe_prorroga_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 86 | `importe_penalidad_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 87 | `importe_cedido_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 88 | `fecha_adjudicacion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 89 | `fecha_formalizacion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 90 | `fecha_inicio_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 91 | `fecha_fin_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 92 | `fecha_inscripcion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 93 | `fecha_inscripcion_contrato_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 94 | `fecha_aprobacion_derivado_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 95 | `fecha_formalizacion_derivado_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 96 | `fecha_formalizacion_incidencia_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 97 | `fecha_acuerdo_penalidad_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 98 | `fecha_autorizacion_cesion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 99 | `fecha_peticion_cesion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 100 | `fecha_acuerdo_resolucion_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 101 | `fecha_fin_actualizada_texto` | `string` | [marca](../ESQUEMA.md#valores-convertidos) |
| 102 | `_fila_vacia` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 103 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 104 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 105 | `_anio_fichero` | `int64` | [marca](../ESQUEMA.md#procedencia) |
| 106 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 107 | `_fila_origen` | `int64` | [marca](../ESQUEMA.md#procedencia) |
| 108 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 109 | `_repetido_en_csv` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 110 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 111 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 112 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 113 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `actividad_contractual_madrid_original.parquet`

132.087 filas · 207 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Centro` | `string` |  |
| 2 | `Descripción` | `string` |  |
| 3 | `Título del expediente` | `string` |  |
| 4 | `Nº exped. Adm.` | `string` |  |
| 5 | `Importe` | `string` |  |
| 6 | `Fe.contab.` | `string` |  |
| 7 | `NIF` | `string` |  |
| 8 | `Tercero` | `string` |  |
| 9 | `T. expediente` | `string` |  |
| 10 | `Ce.gestor` | `string` |  |
| 11 | `Nº expediente` | `string` |  |
| 12 | `      Importe` | `string` |  |
| 13 | `Fech/ apro` | `string` |  |
| 14 | `Tipo de expediente` | `string` |  |
| 15 | `Fe/contab/` | `string` |  |
| 16 | `Documento 2015` | `string` |  |
| 17 | `Contratista` | `string` |  |
| 18 | `       Importe` | `string` |  |
| 19 | `Fech. apro` | `string` |  |
| 20 | `Documento 2016` | `string` |  |
| 21 | `NºRECON` | `string` |  |
| 22 | `NUMERO EXPEDIENTE` | `string` |  |
| 23 | `SECCION ` | `string` |  |
| 24 | `ORG.CONTRATACION` | `string` |  |
| 25 | `OBJETO DEL CONTRATO` | `string` |  |
| 26 | `TIPO DE CONTRATO` | `string` |  |
| 27 | `N.I.F` | `string` |  |
| 28 | `CONTRATISTA` | `string` |  |
| 29 | `IMPORTE` | `string` |  |
| 30 | `FECHA APROBACION` | `string` |  |
| 31 | `PLAZO` | `string` |  |
| 32 | `FCH.COMUNIC.REG` | `string` |  |
| 33 | `N. DE REGISTRO DE CONTRATO` | `string` |  |
| 34 | `N. DE EXPEDIENTE` | `string` |  |
| 35 | `CENTRO - SECCION` | `string` |  |
| 36 | `ORGANO DE CONTRATACION` | `string` |  |
| 37 | `N. DE INVITACIONES CURSADAS` | `string` |  |
| 38 | `INVITADOS A PRESENTAR OFERTA` | `string` |  |
| 39 | `IMPORTE LICITACION IVA INC.` | `string` |  |
| 40 | `N. LICITADORES PARTICIPANTES` | `string` |  |
| 41 | `NIF ADJUDICATARIO` | `string` |  |
| 42 | `RAZON SOCIAL ADJUDICATARIO` | `string` |  |
| 43 | `PYME` | `string` |  |
| 44 | `IMPORTE ADJUDICACION IVA INC.` | `string` |  |
| 45 | `FECHA DE ADJUDICACION` | `string` |  |
| 46 | `FECHA DE INSCRIPCION` | `string` |  |
| 47 | `CONTRATO` | `string` |  |
| 48 | `EXPEDIENTE` | `string` |  |
| 49 | `SECCIÓN` | `string` |  |
| 50 | `ORG_CONTRATACIÓN` | `string` |  |
| 51 | `OBJETO` | `string` |  |
| 52 | `TIPO_CONTRATO` | `string` |  |
| 53 | `CIF` | `string` |  |
| 54 | `RAZÓN_SOCIAL` | `string` |  |
| 55 | `F_APROBACIÓN` | `string` |  |
| 56 | `F_INSCRIPCION` | `string` |  |
| 57 | `ORGANISMO_CONTRATANTE` | `string` |  |
| 58 | `ORGANISMO_PROMOTOR` | `string` |  |
| 59 | `Mes` | `string` |  |
| 60 | `Año` | `string` |  |
| 61 | `Descripción Centro` | `string` |  |
| 62 | `Organismo` | `string` |  |
| 63 | `Número Contrato` | `string` |  |
| 64 | `Número Expediente` | `string` |  |
| 65 | `Descripción Contrato` | `string` |  |
| 66 | `Tipo Contrato` | `string` |  |
| 67 | `Procedimiento Adjudicación` | `string` |  |
| 68 | `Artículo` | `string` |  |
| 69 | `Apartado` | `string` |  |
| 70 | `Criterios Adjudicación` | `string` |  |
| 71 | `Plazo` | `string` |  |
| 72 | `Nombre/Razón Social` | `string` |  |
| 73 | `NIF/CIF Adjudicatario` | `string` |  |
| 74 | `Fecha Adjudicación` | `string` |  |
| 75 | `Fecha Formalización` | `string` |  |
| 76 | `Número Derivado` | `string` |  |
| 77 | `Objeto Derivado` | `string` |  |
| 78 | `Presupuesto Total             (IVA Incluido)` | `string` |  |
| 79 | `Importe Adjudicación   (IVA Incluido)` | `string` |  |
| 80 | `Plazo Derivado` | `string` |  |
| 81 | `Fecha Aprobación Derivado` | `string` |  |
| 82 | `Fecha Formalización Derivado` | `string` |  |
| 83 | `Presupuesto Total  IVA Incluido)` | `string` |  |
| 84 | `A±o` | `string` |  |
| 85 | `AÑO` | `string` |  |
| 86 | `MES` | `string` |  |
| 87 | `N. DE REGISTRO DE CONTRATO AM` | `string` |  |
| 88 | `N. DE EXPEDIENTE AM` | `string` |  |
| 89 | `PLAZO DE EJECUCION DEL AM` | `string` |  |
| 90 | `ADJUDICATARIO` | `string` |  |
| 91 | `FECHA DE FORMALIZACION DEL AM` | `string` |  |
| 92 | `N. DE CONTRATO DEL C.B.` | `string` |  |
| 93 | `N. DE EXPEDIENTE C.B.` | `string` |  |
| 94 | `OBJETO C.B.` | `string` |  |
| 95 | `CRITERIOS DE ADJUDICACION` | `string` |  |
| 96 | `PRESUPUESTO TOTAL IVA INC.` | `string` |  |
| 97 | `PLAZO C.B.` | `string` |  |
| 98 | `FECHA DE APROBACION C.B.` | `string` |  |
| 99 | `FECHA DE FORMALIZACION C.B.` | `string` |  |
| 100 | `N. DE EXPEDIENTE AM/SDA` | `string` |  |
| 101 | `PLAZO DE EJECUCION AM/SDA` | `string` |  |
| 102 | `FECHA DE FORMALIZACION AM` | `string` |  |
| 103 | `N. DE CONTRATO DEL CB/CESDA` | `string` |  |
| 104 | `N. DE EXPEDIENTE CB/CESDA` | `string` |  |
| 105 | `OBJETO CB/CESDA` | `string` |  |
| 106 | `PLAZO CB/CESDA` | `string` |  |
| 107 | `FECHA DE APROBACION CB/CESDA` | `string` |  |
| 108 | `FECHA DE FORMALIZACION CB/CESDA` | `string` |  |
| 109 | `Unnamed: 20` | `string` |  |
| 110 | `FECHA INSCRIPCION` | `string` |  |
| 111 | `TIPO DE INCIDENCIA` | `string` |  |
| 112 | `FECHA INSCRIPCION CONTRATO` | `string` |  |
| 113 | `CENTRO - SECCION DE CONTRATO` | `string` |  |
| 114 | `ADJUDICATARIO CEDENTE` | `string` |  |
| 115 | `FECHA DE FORMALIZACION` | `string` |  |
| 116 | `IMPORTE ADJUDICACION IVA INC` | `string` |  |
| 117 | `CENTRO - SECCION INC.` | `string` |  |
| 118 | `FECHA PETICION CESION` | `string` |  |
| 119 | `FECHA AUTORIZACION CESION` | `string` |  |
| 120 | `ADJ. DEL CONTRATO CESIONARIO` | `string` |  |
| 121 | `IMPORTE PRORROGA` | `string` |  |
| 122 | `N. REGISTRO INCIDENCIA` | `string` |  |
| 123 | `INGRESO/GASTO` | `string` |  |
| 124 | `Importe cedido IVA incluido` | `string` |  |
| 125 | `Presupuesto Total          (IVA Incluido)` | `string` |  |
| 126 | `Acuerdo Marco` | `string` |  |
| 127 | `Ingreso/Coste Cero` | `string` |  |
| 128 | `Observaciones` | `string` |  |
| 129 | `Presupuesto Total(IVA Incluido)` | `string` |  |
| 130 | `Importe Adjudicación (IVA Incluido)` | `string` |  |
| 131 | `F.P. ANUNCIO LICITACION DOUE` | `string` |  |
| 132 | `F.P. EN PERFIL DE CONTRATANTE` | `string` |  |
| 133 | `N. DE LICITADORES` | `string` |  |
| 134 | `PROCEDIMIENTO DE ADJUDICACION` | `string` |  |
| 135 | `ARTICULO` | `string` |  |
| 136 | `APARTADO` | `string` |  |
| 137 | `VALOR ESTIMADO` | `string` |  |
| 138 | `APLICACION PRESUPUESTARIA` | `string` |  |
| 139 | `PORCENTAJE BAJA ADJUDICACION` | `string` |  |
| 140 | `ACUERDO MARCO` | `string` |  |
| 141 | `PROCEDIMIENTO DE ADJUDICACION.1` | `string` |  |
| 142 | `N. EXPEDIENTE S.H.` | `string` |  |
| 143 | `OBJETO DEL S.H.` | `string` |  |
| 144 | `DURACION PROCEDIMIENTO (MESES)` | `string` |  |
| 145 | `FECHA DE FIN ACTUALIZADA` | `string` |  |
| 146 | `N. DE REGISTRO` | `string` |  |
| 147 | `TITULO DEL EXPEDIENTE` | `string` |  |
| 148 | `PLAZO DE EJECUCION` | `string` |  |
| 149 | `FECHA INSCRIPCIÓN` | `string` |  |
| 150 | `Nº CONTRATO` | `string` |  |
| 151 | `Nº EXPEDIENTE` | `string` |  |
| 152 | `GESTOR` | `string` |  |
| 153 | `FECHA FORMALIZACIÓN` | `string` |  |
| 154 | `IMPORTE ADJUDICACIÓN` | `string` |  |
| 155 | `FECJA FORMALIZACIÓN INCIDENCIA` | `string` |  |
| 156 | `IMPORTE DE LA MODIFICACIÓN` | `string` |  |
| 157 | `INGRESO / GASTO` | `string` |  |
| 158 | `NUM.CONTRATO` | `string` |  |
| 159 | `NUM.EXPEDIENTE` | `string` |  |
| 160 | `C.I.F` | `string` |  |
| 161 | `FECHA FORMALIZACION` | `string` |  |
| 162 | `IMPORTE ADJUDICACION` | `string` |  |
| 163 | `FECHA FORMALIZACION INCIDENCIA` | `string` |  |
| 164 | `IMPORTE MODIFICACION` | `string` |  |
| 165 | `TIPO INCID.` | `string` |  |
| 166 | `INCIDENCIA` | `string` |  |
| 167 | `FECHA INSCRIPCION CTO.` | `string` |  |
| 168 | `  -OBJETO CTO.- ` | `string` |  |
| 169 | `FCH. FORMALIZ/APROBAC.INCID.` | `string` |  |
| 170 | `MES INSCRIPCION` | `string` |  |
| 171 | `F_INSC_CONTRATO` | `string` |  |
| 172 | `OBJETO_CONTRATO` | `string` |  |
| 173 | `ADJUCICATARIO` | `string` |  |
| 174 | `F_FORMALIZACION` | `string` |  |
| 175 | `IMPORTE_ADJUDICACIÓN` | `string` |  |
| 176 | `F_FORM / F_APROB` | `string` |  |
| 177 | `IMPORTE_MODIFICACION` | `string` |  |
| 178 | `F_FORM_DERIVADO` | `string` |  |
| 179 | `OBJETO_DERIVADO` | `string` |  |
| 180 | `GASTO / INGRESO` | `string` |  |
| 181 | `INSCRIPCIÓN` | `string` |  |
| 182 | `FECHA DE FORMALIZACION INC.` | `string` |  |
| 183 | `R` | `string` |  |
| 184 | `FECHA ACUERDO PENALIDAD` | `string` |  |
| 185 | `IMPORTE DE LA PENALIDAD` | `string` |  |
| 186 | `CAUSA / CAUSAS` | `string` |  |
| 187 | `FECHA ACUERDO DE RESOLUCION` | `string` |  |
| 188 | `CAUSAS GENERALES` | `string` |  |
| 189 | `OTRAS CAUSAS` | `string` |  |
| 190 | `CAUSAS ESPECIFICAS` | `string` |  |
| 191 | `_dataset` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 192 | `_recurso` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 193 | `_descripcion` | `string` | [de la fuente](../fuentes/madrid_ayuntamiento.md) |
| 194 | `_categoria` | `string` | [de la fuente](../fuentes/madrid_ayuntamiento.md) |
| 195 | `_anio_fichero` | `int64` | [marca](../ESQUEMA.md#procedencia) |
| 196 | `_parte` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 197 | `_formato` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 198 | `_estructura` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 199 | `_archivo_origen` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 200 | `_fila_origen` | `int64` | [marca](../ESQUEMA.md#procedencia) |
| 201 | `_encabezado` | `string` | [marca](../ESQUEMA.md#procedencia) |
| 202 | `_fecha_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 203 | `_duplicado` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 204 | `_repetido_en_csv` | `bool` | [marca](../ESQUEMA.md#calidad-del-origen) |
| 205 | `_primera_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 206 | `_ultima_descarga` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 207 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
