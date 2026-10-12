# `asturias.zip`

Esquema de los Parquet de `asturias.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/asturias.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `asturias_contracts_ALL_YEARS.parquet` | 375.380 | 103 |

## `asturias_contracts_ALL_YEARS.parquet`

375.380 filas · 103 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Nº INSCRIPCION` | `large_string` |  |
| 2 | `ANO` | `double` |  |
| 3 | `CLASIFICACION GENERAL` | `large_string` |  |
| 4 | `Nº EXPEDIENTE ORGANO` | `string` |  |
| 5 | `F. DE ALTA` | `large_string` |  |
| 6 | `OBJETO` | `large_string` |  |
| 7 | `PRESUPUESTO` | `double` |  |
| 8 | `IVA` | `double` |  |
| 9 | `CARACTERISTICAS CONTRATO` | `large_string` |  |
| 10 | `ESTADO CONTRATO` | `large_string` |  |
| 11 | `ENTE CONTRATANTE` | `large_string` |  |
| 12 | `ORGANO CONTRATANTE` | `large_string` |  |
| 13 | `TIPO SOLICITUD` | `large_string` |  |
| 14 | `MODALIDAD` | `large_string` |  |
| 15 | `REGULACION` | `large_string` |  |
| 16 | `CODIGO CPV` | `large_string` |  |
| 17 | `MIXTO` | `large_string` |  |
| 18 | `MARCO` | `large_string` |  |
| 19 | `COMPLEMENTARIO` | `large_string` |  |
| 20 | `DETERMINACION PRECIO` | `large_string` |  |
| 21 | `CLASIFICACION EXIGIDA` | `large_string` |  |
| 22 | `GRUPO` | `large_string` |  |
| 23 | `SUBGRUPO` | `large_string` |  |
| 24 | `CATEGORIA` | `large_string` |  |
| 25 | `T. TRAMITACION` | `large_string` |  |
| 26 | `PLAZO EJECUCION` | `large_string` |  |
| 27 | `T. PLAZO` | `large_string` |  |
| 28 | `CODIGO FUNCION` | `large_string` |  |
| 29 | `DESCRIPCION FUNCION` | `large_string` |  |
| 30 | `FUNCION` | `large_string` |  |
| 31 | `F. BOPA` | `large_string` |  |
| 32 | `F. BOE` | `large_string` |  |
| 33 | `F. DOUE` | `large_string` |  |
| 34 | `P. CONTRATANTE` | `large_string` |  |
| 35 | `IMP. ADJ. (CON IVA)` | `double` |  |
| 36 | `IMP. ADJ. IMPUESTO` | `double` |  |
| 37 | `P. ORIGEN` | `double` |  |
| 38 | `P. SUMINISTRO` | `double` |  |
| 39 | `F. ADJ.` | `large_string` |  |
| 40 | `F. FORMALIZACION` | `large_string` |  |
| 41 | `IMP. ADJ. LOTE` | `double` |  |
| 42 | `MODALIDAD IMPORTE` | `large_string` |  |
| 43 | `APORTACION ADMINISTRACION` | `large_string` |  |
| 44 | `PROC. ADJUDICACION` | `large_string` |  |
| 45 | `FORMA ADJUDICACION` | `large_string` |  |
| 46 | `ARTICULO RDL` | `large_string` |  |
| 47 | `APARTADO RDL` | `large_string` |  |
| 48 | `INVITACIONES` | `large_string` |  |
| 49 | `C. BIENES` | `large_string` |  |
| 50 | `OBSERVACIONES` | `large_string` |  |
| 51 | `OBJETO.1` | `large_string` |  |
| 52 | `INGRESO` | `large_string` |  |
| 53 | `V. MODALIDAD` | `large_string` |  |
| 54 | `U. MODALIDAD` | `large_string` |  |
| 55 | `P. CONCESION` | `large_string` |  |
| 56 | `ESTADO LOTE` | `large_string` |  |
| 57 | `F. FIN EJECUCION` | `large_string` |  |
| 58 | `F. RECEPCION` | `large_string` |  |
| 59 | `DEFECTOS / REPARACIONES` | `large_string` |  |
| 60 | `T. MODIFICACION` | `large_string` |  |
| 61 | `Nº MODIFICACION` | `large_string` |  |
| 62 | `IMPORTE MODIFICACION` | `double` |  |
| 63 | `INC. PLAZO EJEC.` | `large_string` |  |
| 64 | `ART. RESOLUCION` | `large_string` |  |
| 65 | `APTDO. RESOLUCION` | `large_string` |  |
| 66 | `CAUSA RESOLUCION` | `double` |  |
| 67 | `FECHA RESOLUCION` | `double` |  |
| 68 | `PERDIDA GARANTIA RESOLUCION` | `large_string` |  |
| 69 | `FECHA` | `double` |  |
| 70 | `CAUSAS` | `large_string` |  |
| 71 | `CONTRATISTAS` | `large_string` |  |
| 72 | `ANUALIDADES` | `large_string` |  |
| 73 | `year` | `int64` |  |
| 74 | `source_file` | `large_string` |  |
| 75 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 76 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 77 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 78 | `ID. PLACE` | `large_string` |  |
| 79 | `ACUERDO MARCO DEL QUE DERIVA EL CONTRATO BASADO` | `large_string` |  |
| 80 | `CONTRATOS FINANCIADOS CON FONDOS EUROPEOS` | `large_string` |  |
| 81 | `TIPO DE FONDO EUROPEO` | `large_string` |  |
| 82 | `RESERVA CONFORME DISP. ADICIONAL` | `large_string` |  |
| 83 | `CONT. ESTRAT. C. SOCIALES` | `large_string` |  |
| 84 | `CONT. ESTRAT. C. MEDIOAMBIENTALES` | `large_string` |  |
| 85 | `CONT. ESTRAT. C. DE I+D` | `large_string` |  |
| 86 | `COND. ESP. EJEC. TIPO SOCIAL` | `large_string` |  |
| 87 | `COND. ESP. EJEC. TIPO MEDIOAMBIENTAL` | `large_string` |  |
| 88 | `COND. ESP. EJEC. TIPO I+D` | `large_string` |  |
| 89 | `AÑO` | `double` |  |
| 90 | `NIF/CIF CONTRATISTA RAZON SOCIAL CONTRATISTA ANUALIDADES` | `large_string` |  |
| 91 | `RAZON,  SOCIAL CONTRATISTA` | `large_string` |  |
| 92 | `ADJUDICADOS A PYMES` | `large_string` |  |
| 93 | `OBJETO DE RECURSO ESPECIAL EN MATERIA DE CONTRATACION` | `large_string` |  |
| 94 | `OBJETO DE RECURSO CONTENCIOSO ADMINISTRATIVO` | `large_string` |  |
| 95 | `CONTRATACION PUBLICA POR DEBAJO DE UMBRALES` | `large_string` |  |
| 96 | `OFERTAS INCURSAS EN PRESUNCION DE ANORMALIDAD` | `large_string` |  |
| 97 | `INDICADOR DE BAJA EN LA ADJUDICACION` | `large_string` |  |
| 98 | `PORCENTAJE DE BAJA EN LA ADJUDICACION` | `large_string` |  |
| 99 | `OBJETO_dup2` | `large_string` |  |
| 100 | `NIF/CIF CONTRATISTA` | `large_string` |  |
| 101 | `RAZON SOCIAL CONTRATISTA` | `large_string` |  |
| 102 | `V MODALIDAD` | `double` |  |
| 103 | `_origen` | `null` | [marca](../ESQUEMA.md#control-de-versiones) |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
