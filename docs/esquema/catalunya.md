# `catalunya.zip`

Esquema de los Parquet de `catalunya.zip` en la release v2026.10, leído de sus pies con [`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano). Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/catalunya.md). Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).

| Fichero | Filas | Columnas |
|---|--:|--:|
| `contratacion/adjudicaciones_generalitat.parquet` | 73.895 | 39 |
| `contratacion/contratacion_programada.parquet` | 37.641 | 15 |
| `contratacion/contratistas_bcn.parquet` | 154.471 | 16 |
| `contratacion/contratos_covid.parquet` | 5.567 | 18 |
| `contratacion/contratos_menores_autorizacion_bcn.parquet` | 8.246 | 9 |
| `contratacion/contratos_menores_bcn.parquet` | 177.679 | 14 |
| `contratacion/contratos_menores_generalitat.parquet` | 866.557 | 13 |
| `contratacion/contratos_registro.parquet` | 4.456.505 | 39 |
| `contratacion/fase_ejecucion.parquet` | 77.366 | 26 |
| `contratacion/modificaciones_bcn.parquet` | 3.171 | 52 |
| `contratacion/perfil_contratante_bcn.parquet` | 37.365 | 127 |
| `contratacion/publicaciones_pscp.parquet` | 2.065.860 | 71 |
| `contratacion/resoluciones_tribunal.parquet` | 1.220 | 9 |
| `contratacion/resumen_trimestral_bcn.parquet` | 82.597 | 9 |

## `contratacion/adjudicaciones_generalitat.parquet`

73.895 filas · 39 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Any` | `int64` |  |
| 2 | `Codi d'expedient` | `large_string` |  |
| 3 | `Departament d'adscripció` | `large_string` |  |
| 4 | `Òrgan de contractació` | `large_string` |  |
| 5 | `Àmbit subjectiu` | `large_string` |  |
| 6 | `Descripció del contracte` | `large_string` |  |
| 7 | `Tipus de contracte` | `large_string` |  |
| 8 | `Procediment d'adjudicació` | `large_string` |  |
| 9 | `Forma de tramitació` | `large_string` |  |
| 10 | `Nombre de lots` | `int64` |  |
| 11 | `Lot número` | `int64` |  |
| 12 | `Objecte del lot` | `large_string` |  |
| 13 | `Import licitat` | `double` |  |
| 14 | `Import adjudicat sense IVA` | `double` |  |
| 15 | `Tramitació amb mesures` | `large_string` |  |
| 16 | `Empresa adjudicatària` | `large_string` |  |
| 17 | `Data d'adjudicació` | `large_string` |  |
| 18 | `Clàusules ambientals` | `large_string` |  |
| 19 | `Responsable del contracte` | `large_string` |  |
| 20 | `Codi CPV` | `large_string` |  |
| 21 | `Clàusules socials` | `large_string` |  |
| 22 | `Nombre de clàusules socials` | `int64` |  |
| 23 | `Clàusules socials d'accessibilitat` | `large_string` |  |
| 24 | `Clàusules socials alimentàries` | `large_string` |  |
| 25 | `Clàusules socials altres` | `large_string` |  |
| 26 | `Clàusules socials de comerç just` | `large_string` |  |
| 27 | `Clàusules socials d'estabilitat laboral` | `large_string` |  |
| 28 | `Clàusules socials de foment de les PIME` | `large_string` |  |
| 29 | `Clàusules socials d'igualtat de gènere` | `large_string` |  |
| 30 | `Clàusules socials d'inserció laboral` | `large_string` |  |
| 31 | `Clàusules socials sense informació de tiologia` | `double` |  |
| 32 | `Clàusules socials de transparència fiscal` | `large_string` |  |
| 33 | `Clàusules socials de conciliació laboral i familiar` | `large_string` |  |
| 34 | `Clàusules socials de drets humans` | `large_string` |  |
| 35 | `Clàusules socials de formació de treballadors` | `large_string` |  |
| 36 | `Clàusules socials de millora condicions laborals` | `large_string` |  |
| 37 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 39 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/contratacion_programada.parquet`

37.641 filas · 15 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Any` | `int64` |  |
| 2 | `Trimestre` | `large_string` |  |
| 3 | `Departament/Ens` | `large_string` |  |
| 4 | `Descripció del contracte` | `large_string` |  |
| 5 | `Agrupació` | `large_string` |  |
| 6 | `Tipus de contracte` | `large_string` |  |
| 7 | `Procediment d'adjudicació` | `large_string` |  |
| 8 | `Import estimat VEC` | `large_string` |  |
| 9 | `Durada` | `large_string` |  |
| 10 | `Pròrroga prevista` | `large_string` |  |
| 11 | `Reserva social` | `large_string` |  |
| 12 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 14 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 15 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/contratistas_bcn.parquet`

154.471 filas · 16 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `NOM PROVEÏDOR` | `large_string` |  |
| 2 | `NIF` | `large_string` |  |
| 3 | ` IMPORT TOTAL ADJUDICAT ` | `large_string` |  |
| 4 | `_año` | `int64` | [de la fuente](../fuentes/catalunya.md) |
| 5 | `Nom Proveïdor` | `large_string` |  |
| 6 | ` Import total adjudicat ` | `large_string` |  |
| 7 | `Tipus ens` | `large_string` |  |
| 8 | `Òrgan contractant` | `large_string` |  |
| 9 | `Nombre de contractes` | `large_string` |  |
| 10 | `Import total adjudicat` | `large_string` |  |
| 11 | `Tipus_Ens` | `large_string` |  |
| 12 | `Organ_Contractant` | `large_string` |  |
| 13 | `Nom_Proveidor` | `large_string` |  |
| 14 | `Nombre_Contractes` | `double` |  |
| 15 | `Import_Total_Adjudicat` | `large_string` |  |
| 16 | `Unnamed: 6` | `large_string` |  |

## `contratacion/contratos_covid.parquet`

5.567 filas · 18 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Àmbit subjectiu` | `large_string` |  |
| 2 | `Òrgan de contractació` | `large_string` |  |
| 3 | `Codi d'expedient` | `large_string` |  |
| 4 | `Objecte del contracte` | `large_string` |  |
| 5 | `Descripció de la prestació` | `large_string` |  |
| 6 | `Tipus d'expedient` | `large_string` |  |
| 7 | `Tipus de contracte` | `large_string` |  |
| 8 | `Pressupost de licitació (sense IVA)` | `double` |  |
| 9 | `Valor estimat del contracte` | `double` |  |
| 10 | `Data de publicació` | `large_string` |  |
| 11 | `Empresa adjudicatària` | `large_string` |  |
| 12 | `Import d'adjudicació (sense IVA)` | `double` |  |
| 13 | `Enllaç a la publicació` | `large_string` |  |
| 14 | `Codi CPV` | `large_string` |  |
| 15 | `Data d'adjudicació` | `large_string` |  |
| 16 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 17 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 18 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/contratos_menores_autorizacion_bcn.parquet`

8.246 filas · 9 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Tipus ens` | `large_string` |  |
| 2 | `Òrgan contractant` | `large_string` |  |
| 3 | `Tipus contracte     ` | `large_string` |  |
| 4 | `Objecte contracte` | `large_string` |  |
| 5 | `Proveïdor` | `large_string` |  |
| 6 | `NIF` | `large_string` |  |
| 7 | `A genérica número` | `int64` |  |
| 8 | `Import adjudicat` | `double` |  |
| 9 | `_año` | `int64` | [de la fuente](../fuentes/catalunya.md) |

## `contratacion/contratos_menores_bcn.parquet`

177.679 filas · 14 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Tipus ens` | `large_string` |  |
| 2 | `Òrgan contractant` | `large_string` |  |
| 3 | `Tipus contracte     ` | `large_string` |  |
| 4 | `Data 
adjudicació` | `large_string` |  |
| 5 | `Proveïdor` | `large_string` |  |
| 6 | `NIF` | `large_string` |  |
| 7 | `Objecte del contracte` | `large_string` |  |
| 8 | `Import adjudicat` | `large_string` |  |
| 9 | `_año` | `int64` | [de la fuente](../fuentes/catalunya.md) |
| 10 | ` Import adjudicat ` | `large_string` |  |
| 11 | ` Mes ` | `large_string` |  |
| 12 | `Trimestre` | `large_string` |  |
| 13 | `Tipus Contracte     ` | `large_string` |  |
| 14 | `Durada` | `large_string` |  |

## `contratacion/contratos_menores_generalitat.parquet`

866.557 filas · 13 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Any` | `int64` |  |
| 2 | `Codi d'expedient` | `large_string` |  |
| 3 | `Departament d'adscripció` | `large_string` |  |
| 4 | `Òrgan de contractació` | `large_string` |  |
| 5 | `Àmbit subjectiu` | `large_string` |  |
| 6 | `Tipus de contracte` | `large_string` |  |
| 7 | `Procediment d'adjudicació` | `large_string` |  |
| 8 | `Objecte del contracte` | `large_string` |  |
| 9 | `Import adjudicat sense IVA` | `double` |  |
| 10 | `Empresa adjudicatària` | `large_string` |  |
| 11 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 12 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 13 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/contratos_registro.parquet`

4.456.505 filas · 39 columnas · 5 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Situació contractual` | `large_string` |  |
| 2 | `Exercici` | `int64` |  |
| 3 | `Àmbit organitzatiu` | `large_string` |  |
| 4 | `Identificador agrupació organisme` | `large_string` |  |
| 5 | `Agrupació organisme` | `large_string` |  |
| 6 | `Identificador organisme contractant` | `large_string` |  |
| 7 | `Organisme contractant` | `large_string` |  |
| 8 | `Codi de l’expedient` | `large_string` |  |
| 9 | `Procediment d’adjudicació` | `large_string` |  |
| 10 | `Tipus de contracte` | `large_string` |  |
| 11 | `Descripció de l’expedient` | `large_string` |  |
| 12 | `Número de lot` | `int64` |  |
| 13 | `Codi CPV` | `large_string` |  |
| 14 | `Adjudicatari` | `large_string` |  |
| 15 | `Import d’adjudicació` | `double` |  |
| 16 | `Data d’adjudicació` | `large_string` |  |
| 17 | `Descripció del lot` | `large_string` |  |
| 18 | `Lot desert` | `large_string` |  |
| 19 | `Durada dies` | `int64` |  |
| 20 | `Durada mesos` | `int64` |  |
| 21 | `Durada anys` | `int64` |  |
| 22 | `Número de pròrroga` | `double` |  |
| 23 | `Data inici pròrroga` | `large_string` |  |
| 24 | `Data fi pròrroga` | `large_string` |  |
| 25 | `Número de modificació` | `double` |  |
| 26 | `Tipus de modificació` | `large_string` |  |
| 27 | `Import de la modificació` | `double` |  |
| 28 | `Data aprovació modificació` | `large_string` |  |
| 29 | `Termini modificació anys` | `double` |  |
| 30 | `Termini modificació mesos` | `double` |  |
| 31 | `Termini modificació dies` | `double` |  |
| 32 | `Tipus de liquidació` | `large_string` |  |
| 33 | `Data de liquidació` | `large_string` |  |
| 34 | `Causa de resolució` | `large_string` |  |
| 35 | `Import de la liquidació` | `double` |  |
| 36 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 37 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 38 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 39 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/fase_ejecucion.parquet`

77.366 filas · 26 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Codi àmbit` | `int64` |  |
| 2 | `Nom àmbit` | `large_string` |  |
| 3 | `Codi departament ens` | `double` |  |
| 4 | `Nom departament ens` | `large_string` |  |
| 5 | `Codi òrgan` | `int64` |  |
| 6 | `Nom òrgan` | `large_string` |  |
| 7 | `Codi unitat` | `double` |  |
| 8 | `Nom unitat` | `large_string` |  |
| 9 | `Codi INE10` | `large_string` |  |
| 10 | `Codi DIR3` | `large_string` |  |
| 11 | `Codi expedient` | `large_string` |  |
| 12 | `Numero lot` | `large_string` |  |
| 13 | `Descripció lot` | `large_string` |  |
| 14 | `Tipus actuació execució` | `large_string` |  |
| 15 | `Denominació actuació` | `large_string` |  |
| 16 | `Data` | `large_string` |  |
| 17 | `Data fi` | `large_string` |  |
| 18 | `Import sense iva` | `double` |  |
| 19 | `Identificació` | `large_string` |  |
| 20 | `Denominació` | `large_string` |  |
| 21 | `Observacions` | `large_string` |  |
| 22 | `URL JSON` | `large_string` |  |
| 23 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 24 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 25 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 26 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/modificaciones_bcn.parquet`

3.171 filas · 52 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Tipus ens` | `large_string` |  |
| 2 | `Òrgan contractant` | `large_string` |  |
| 3 | `Objecte contracte` | `large_string` |  |
| 4 | `Data adjudicació` | `large_string` |  |
| 5 | `Import adjudicació` | `large_string` |  |
| 6 | `Obs import adjudicació` | `large_string` |  |
| 7 | `Modificació prevista` | `large_string` |  |
| 8 | `Modificació no prevista` | `large_string` |  |
| 9 | `Augment prestacio` | `large_string` |  |
| 10 | `Reducció prestació` | `large_string` |  |
| 11 | `increment preu` | `large_string` |  |
| 12 | `reducció preu` | `large_string` |  |
| 13 | `Import modificació` | `large_string` |  |
| 14 | `Percentatge variació sobre el preu` | `large_string` |  |
| 15 | `Observacions` | `large_string` |  |
| 16 | `Data aprovacio modificacio` | `large_string` |  |
| 17 | `_año` | `int64` | [de la fuente](../fuentes/catalunya.md) |
| 18 | `% variació sobre el preu` | `large_string` |  |
| 19 | `Unnamed: 16` | `large_string` |  |
| 20 | `Tipus contracte` | `large_string` |  |
| 21 | `Contracte núm.` | `large_string` |  |
| 22 | `Tipus de contracte` | `large_string` |  |
| 23 | `Data formalització` | `large_string` |  |
| 24 | `Preu contracte` | `large_string` |  |
| 25 | `Data formalització modificació` | `large_string` |  |
| 26 | `Modificació prevista en el plec` | `large_string` |  |
| 27 | `Modificació no prevista en el plec` | `large_string` |  |
| 28 | `% variació sobre el preu del contracte` | `large_string` |  |
| 29 | `Preu contracte (sense IVA)` | `large_string` |  |
| 30 | `Preu contracte (IVA inclòs)` | `large_string` |  |
| 31 | `Import modificació (sense IVA)` | `large_string` |  |
| 32 | `Import modificació (IVA inclòs)` | `large_string` |  |
| 33 | `Modificació prevista en el plec (art. 204 LCSP)` | `large_string` |  |
| 34 | `Modificació prevista en el plec (art. 205 LCSP)` | `large_string` |  |
| 35 | `% variació sobre el preu del contracte (sense IVA)` | `large_string` |  |
| 36 | `Nom adjudicatari` | `large_string` |  |
| 37 | `NIF adjudicatari` | `large_string` |  |
| 38 | `Organ_Contractant` | `large_string` |  |
| 39 | `Contracte_Num` | `large_string` |  |
| 40 | `Contracte_Tipus` | `large_string` |  |
| 41 | `Contracte_Objecte` | `large_string` |  |
| 42 | `Nom_Adjudicatari` | `large_string` |  |
| 43 | `Nif_Adjudicatari` | `large_string` |  |
| 44 | `Data_Formalitzacio` | `large_string` |  |
| 45 | `Preu_Contracte_Sense_IVA` | `large_string` |  |
| 46 | `Preu_Contracte_Inclos_IVA` | `large_string` |  |
| 47 | `Data_Formalitzacio_Modificacio` | `large_string` |  |
| 48 | `Import_Modificacio_Sense_IVA` | `large_string` |  |
| 49 | `Import_Modificacio_Inclos_IVA` | `large_string` |  |
| 50 | `Modificacio_Prevista_Al_Plec_(art. 204 LCSP)` | `large_string` |  |
| 51 | `Modificacio_Prevista_Al_Plec_(art. 205 LCSP)` | `large_string` |  |
| 52 | `Variacio_Sobre_Preu_Contracte_Sense_IVA` | `large_string` |  |

## `contratacion/perfil_contratante_bcn.parquet`

37.365 filas · 127 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `CODI_AMBIT` | `double` |  |
| 2 | `NOM_AMBIT` | `large_string` |  |
| 3 | `CODI_DEPARTAMENT_ENS` | `double` |  |
| 4 | `NOM_DEPARTAMENT_ENS` | `large_string` |  |
| 5 | `CODI_ORGAN` | `double` |  |
| 6 | `NOM_ORGAN` | `large_string` |  |
| 7 | `CODI_UNITAT` | `double` |  |
| 8 | `NOM_UNITAT` | `large_string` |  |
| 9 | `CODI_INE10` | `double` |  |
| 10 | `CODI_DIR3` | `large_string` |  |
| 11 | `CODI_EXPEDIENT` | `large_string` |  |
| 12 | `TIPUS_CONTRACTE` | `large_string` |  |
| 13 | `PROCEDIMENT` | `large_string` |  |
| 14 | `FASE_PUBLICACIO` | `large_string` |  |
| 15 | `DENOMINACIO` | `large_string` |  |
| 16 | `OBJECTE_CONTRACTE` | `large_string` |  |
| 17 | `VALOR_ESTIMAT_CONTRACTE` | `double` |  |
| 18 | `CODI_NUTS` | `large_string` |  |
| 19 | `LLOC_EXECUCIO` | `large_string` |  |
| 20 | `DURADA_CONTRACTE` | `large_string` |  |
| 21 | `TERMINI_PRESENTACIO_OFERTES` | `large_string` |  |
| 22 | `DATA_PUBLICACIO_FUTURA` | `large_string` |  |
| 23 | `DATA_PUBLICACIO_PREVI` | `large_string` |  |
| 24 | `DATA_PUBLICACIO_ANUNCI` | `large_string` |  |
| 25 | `DATA_PUBLICACIO_ADJUDICACIO` | `large_string` |  |
| 26 | `DATA_PUBLICACIO_FORMALITZACIO` | `large_string` |  |
| 27 | `DATA_PUBLICACIO_ANUL` | `large_string` |  |
| 28 | `NUMERO_LOT` | `large_string` |  |
| 29 | `DESCRIPCIO_LOT` | `large_string` |  |
| 30 | `CODI_CPV` | `large_string` |  |
| 31 | `TIPUS_IDENTIFICACIO_ADJUDICATARI` | `large_string` |  |
| 32 | `IDENTIFICACIO_ADJUDICATARI` | `large_string` |  |
| 33 | `DENOMINACIO_ADJUDICATARI` | `large_string` |  |
| 34 | `IMPORT_ADJUDICACIO_SENSE_IVA` | `double` |  |
| 35 | `IMPORT_ADJUDICACIO_AMB_IVA` | `double` |  |
| 36 | `IMPORT_ADJUDICACIO_SENSE_IVA_DESGLOSSAT` | `large_string` |  |
| 37 | `IMPORT_ADJUDICACIO_AMB_IVA_DESGLOSSAT` | `large_string` |  |
| 38 | `OFERTES_REBUDES` | `double` |  |
| 39 | `RESULTAT` | `large_string` |  |
| 40 | `ENLLAC_PUBLICACIO` | `large_string` |  |
| 41 | `ES_AGREGADA` | `large_string` |  |
| 42 | `DATA_ADJUDICACIO_CONTRACTE` | `large_string` |  |
| 43 | `DATA_FORMALITZACIO_CONTRACTE` | `large_string` |  |
| 44 | `TIPUS_TRAMITACIO` | `large_string` |  |
| 45 | `RACIONALITZACIO_CONTRACTACIO` | `large_string` |  |
| 46 | `TIPUS_FINANCAMENT` | `large_string` |  |
| 47 | `EINA_PRESENTACIO_ELECTRONICA` | `large_string` |  |
| 48 | `ALTRES_EINES_LICITACIO_ELECTRONICA` | `large_string` |  |
| 49 | `NO_ADMET_EINA_LICITACIO_ELECTRONICA` | `large_string` |  |
| 50 | `PRESSUPOST_LICITACIO_SENSE_IVA` | `double` |  |
| 51 | `PRESSUPOST_LICITACIO_SENSE_IVA_EXPEDIENT` | `double` |  |
| 52 | `PRESSUPOST_LICITACIO_AMB_IVA` | `double` |  |
| 53 | `PRESSUPOST_LICITACIO_AMB_IVA_EXPEDIENT` | `double` |  |
| 54 | `VALOR_ESTIMAT_EXPEDIENT` | `double` |  |
| 55 | `DATA_PUBLICACIO_CONSULTA_MERCAT` | `large_string` |  |
| 56 | `DATA_PUBLICACIO_AVALUACIO` | `large_string` |  |
| 57 | `DATA_PUBLICACIO_CONTRACTE_AGREGAT` | `large_string` |  |
| 58 | `DATA_PUBLICACIO_ENCARREC` | `double` |  |
| 59 | `TIPUS_EMPRESA` | `large_string` |  |
| 60 | `URL_JSON_FUTURA` | `large_string` |  |
| 61 | `URL_JSON_CPM` | `large_string` |  |
| 62 | `URL_JSON_PREVI` | `large_string` |  |
| 63 | `URL_JSON_LICITACIO` | `large_string` |  |
| 64 | `URL_JSON_AVALUACIO` | `large_string` |  |
| 65 | `URL_JSON_ADJUDICACIO` | `large_string` |  |
| 66 | `URL_JSON_FORMALITZACIO` | `large_string` |  |
| 67 | `URL_JSON_ANULACIO` | `large_string` |  |
| 68 | `URL_JSON_AGREGADA` | `large_string` |  |
| 69 | `_archivo_origen` | `large_string` | [marca](../ESQUEMA.md#procedencia) |
| 70 | `ORGANISME` | `large_string` |  |
| 71 | `EXPEDIENT` | `large_string` |  |
| 72 | `OBJECTE` | `large_string` |  |
| 73 | `TIPUS_DESC` | `large_string` |  |
| 74 | `PROCEDIMENT_DESC` | `large_string` |  |
| 75 | `CRITERIS_DESC` | `large_string` |  |
| 76 | `ESTAT_DESC` | `large_string` |  |
| 77 | `DINICI_OFERTES` | `large_string` |  |
| 78 | `DFINAL_OFERTES` | `large_string` |  |
| 79 | `DATA_OBERTURA_PLIQUES_1` | `large_string` |  |
| 80 | `LLOC_OBERTURA_PLIQUES_1` | `double` |  |
| 81 | `DATA_OBERTURA_PLIQUES_2` | `double` |  |
| 82 | `LLOC_OBERTURA_PLIQUES_2` | `double` |  |
| 83 | `TRAMITACIO` | `double` |  |
| 84 | `IMP_NET_LICITACIO` | `double` |  |
| 85 | `IMP_TOT_LICITACIO` | `double` |  |
| 86 | `IMP_IVA_LICITACIO` | `double` |  |
| 87 | `PRORROGA` | `double` |  |
| 88 | `VALOR_ESTIMAT` | `double` |  |
| 89 | `IMP_GARANT_PROV` | `double` |  |
| 90 | `NUM_OFERTES` | `double` |  |
| 91 | `DATA_ADJUDICACIO` | `large_string` |  |
| 92 | `ADJUDICATARI_NIF` | `large_string` |  |
| 93 | `ADJUDICATARI_RAO` | `large_string` |  |
| 94 | `PREU_CONTRACTE` | `large_string` |  |
| 95 | `IVA_CONTRACTE` | `large_string` |  |
| 96 | `TERMINI_FORM` | `large_string` |  |
| 97 | `DATA_FINALITZACIO` | `double` |  |
| 98 | `DATA_FORMALIT` | `double` |  |
| 99 | `URL` | `large_string` |  |
| 100 | `DATA_PUB_LICIT` | `large_string` |  |
| 101 | `DATA_PUB_ADJUDIC` | `large_string` |  |
| 102 | `DATA_PUB_FORMALIT` | `large_string` |  |
| 103 | `MITJA_PUBL_OF1` | `large_string` |  |
| 104 | `DATA_PUBL_OF1` | `large_string` |  |
| 105 | `MITJA_PUBL_OF2` | `large_string` |  |
| 106 | `DATA_PUBL_OF2` | `large_string` |  |
| 107 | `MITJA_PUBL_OF3` | `large_string` |  |
| 108 | `DATA_PUBL_OF3` | `large_string` |  |
| 109 | `MITJA_PUBL_OF4` | `double` |  |
| 110 | `DATA_PUBL_OF4` | `double` |  |
| 111 | `OBSERVACIONS` | `large_string` |  |
| 112 | `Unnamed: 42` | `large_string` |  |
| 113 | `Unnamed: 43` | `large_string` |  |
| 114 | `Unnamed: 44` | `large_string` |  |
| 115 | `Unnamed: 45` | `large_string` |  |
| 116 | `Unnamed: 46` | `large_string` |  |
| 117 | `Unnamed: 47` | `large_string` |  |
| 118 | `Unnamed: 48` | `large_string` |  |
| 119 | `Unnamed: 49` | `large_string` |  |
| 120 | `Unnamed: 50` | `large_string` |  |
| 121 | `Unnamed: 51` | `large_string` |  |
| 122 | `Unnamed: 52` | `large_string` |  |
| 123 | `Unnamed: 53` | `large_string` |  |
| 124 | `Unnamed: 54` | `large_string` |  |
| 125 | `Unnamed: 55` | `large_string` |  |
| 126 | `Unnamed: 56` | `large_string` |  |
| 127 | `Unnamed: 57` | `large_string` |  |

## `contratacion/publicaciones_pscp.parquet`

2.065.860 filas · 71 columnas · 2 grupos de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `codi_ambit` | `int64` |  |
| 2 | `nom_ambit` | `large_string` |  |
| 3 | `codi_departament_ens` | `double` |  |
| 4 | `nom_departament_ens` | `large_string` |  |
| 5 | `codi_organ` | `int64` |  |
| 6 | `nom_organ` | `large_string` |  |
| 7 | `codi_unitat` | `double` |  |
| 8 | `nom_unitat` | `large_string` |  |
| 9 | `codi_ine10` | `large_string` |  |
| 10 | `codi_dir3` | `large_string` |  |
| 11 | `codi_expedient` | `large_string` |  |
| 12 | `tipus_contracte` | `large_string` |  |
| 13 | `procediment` | `large_string` |  |
| 14 | `fase_publicacio` | `large_string` |  |
| 15 | `denominacio` | `large_string` |  |
| 16 | `objecte_contracte` | `large_string` |  |
| 17 | `valor_estimat_contracte` | `double` |  |
| 18 | `codi_nuts` | `large_string` |  |
| 19 | `lloc_execucio` | `large_string` |  |
| 20 | `durada_contracte` | `large_string` |  |
| 21 | `termini_presentacio_ofertes` | `large_string` |  |
| 22 | `data_publicacio_futura` | `large_string` |  |
| 23 | `data_publicacio_previ` | `large_string` |  |
| 24 | `data_publicacio_anunci` | `large_string` |  |
| 25 | `data_publicacio_adjudicacio` | `large_string` |  |
| 26 | `data_publicacio_formalitzacio` | `large_string` |  |
| 27 | `data_publicacio_anul` | `large_string` |  |
| 28 | `numero_lot` | `large_string` |  |
| 29 | `descripcio_lot` | `large_string` |  |
| 30 | `codi_cpv` | `large_string` |  |
| 31 | `tipus_identificacio_adjudicatari` | `large_string` |  |
| 32 | `identificacio_adjudicatari` | `large_string` |  |
| 33 | `denominacio_adjudicatari` | `large_string` |  |
| 34 | `import_adjudicacio_sense_iva` | `large_string` |  |
| 35 | `import_adjudicacio_amb_iva` | `large_string` |  |
| 36 | `ofertes_rebudes` | `double` |  |
| 37 | `resultat` | `large_string` |  |
| 38 | `enllac_publicacio` | `large_string` |  |
| 39 | `es_agregada` | `large_string` |  |
| 40 | `data_adjudicacio_contracte` | `large_string` |  |
| 41 | `data_formalitzacio_contracte` | `large_string` |  |
| 42 | `tipus_tramitacio` | `large_string` |  |
| 43 | `racionalitzacio_contractacio` | `large_string` |  |
| 44 | `tipus_financament` | `large_string` |  |
| 45 | `eina_presentacio_electronica` | `large_string` |  |
| 46 | `altres_eines_licitacio_electronica` | `large_string` |  |
| 47 | `no_admet_eina_licitacio_electronica` | `large_string` |  |
| 48 | `pressupost_licitacio_sense_iva` | `double` |  |
| 49 | `pressupost_licitacio_sense_iva_expedient` | `double` |  |
| 50 | `pressupost_licitacio_amb_iva` | `double` |  |
| 51 | `pressupost_licitacio_amb_iva_expedient` | `double` |  |
| 52 | `valor_estimat_expedient` | `double` |  |
| 53 | `data_publicacio_consulta_mercat` | `large_string` |  |
| 54 | `data_publicacio_avaluacio` | `large_string` |  |
| 55 | `data_publicacio_contracte_agregat` | `large_string` |  |
| 56 | `data_publicacio_encarrec` | `large_string` |  |
| 57 | `tipus_empresa` | `large_string` |  |
| 58 | `url_json_futura` | `large_string` |  |
| 59 | `url_json_cpm` | `large_string` |  |
| 60 | `url_json_previ` | `large_string` |  |
| 61 | `url_json_licitacio` | `large_string` |  |
| 62 | `url_json_avaluacio` | `large_string` |  |
| 63 | `url_json_adjudicacio` | `large_string` |  |
| 64 | `url_json_formalitzacio` | `large_string` |  |
| 65 | `url_json_anulacio` | `large_string` |  |
| 66 | `url_json_agregada` | `large_string` |  |
| 67 | `id_intern` | `large_string` |  |
| 68 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 69 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 70 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |
| 71 | `_origen` | `string` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/resoluciones_tribunal.parquet`

1.220 filas · 9 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Num Expedient` | `large_string` |  |
| 2 | `Num Resolució` | `large_string` |  |
| 3 | `Data resolució` | `large_string` |  |
| 4 | `Sentit` | `large_string` |  |
| 5 | `Veus` | `large_string` |  |
| 6 | `URL` | `large_string` |  |
| 7 | `_primera_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 8 | `_ultima_descarga` | `large_string` | [marca](../ESQUEMA.md#control-de-versiones) |
| 9 | `_en_ultima_descarga` | `bool` | [marca](../ESQUEMA.md#control-de-versiones) |

## `contratacion/resumen_trimestral_bcn.parquet`

82.597 filas · 9 columnas · 1 grupo de filas

| # | Columna | Tipo | |
|--:|---|---|---|
| 1 | `Trimestre` | `large_string` |  |
| 2 | `Tipus ens` | `large_string` |  |
| 3 | `Nom ens` | `large_string` |  |
| 4 | `Procediment` | `large_string` |  |
| 5 | `Nombre` | `large_string` |  |
| 6 | `Import (euros)` | `large_string` |  |
| 7 | `_año` | `int64` | [de la fuente](../fuentes/catalunya.md) |
| 8 | `Tipus de contracte` | `large_string` |  |
| 9 | `Import` | `large_string` |  |

## Carpeta `v2026.02/`

Ficheros de v2026.02 que van tal cual en esta release (23, 22 Parquet), porque la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del `MANIFEST.csv` del ZIP dice por qué está cada uno.

| Fichero | Filas | Columnas |
|---|--:|--:|
| `v2026.02/README.md` | — | — |
| `v2026.02/contratacion/perfil_contratante_bcn.parquet` | 25.766 | 127 |
| `v2026.02/convenios/convenios.parquet` | 23.433 | 25 |
| `v2026.02/entidades/ajuntaments.parquet` | 947 | 62 |
| `v2026.02/entidades/ajuntaments_lista.parquet` | 949 | 2 |
| `v2026.02/entidades/codigos_departamentos.parquet` | 234 | 10 |
| `v2026.02/entidades/composicio_plens.parquet` | 10.591 | 12 |
| `v2026.02/entidades/ens_locals.parquet` | 11.777 | 62 |
| `v2026.02/entidades/sector_publico_generalitat.parquet` | 474 | 48 |
| `v2026.02/presupuestos/ejecucion_gastos.parquet` | 1.551.254 | 44 |
| `v2026.02/presupuestos/ejecucion_ingresos.parquet` | 173.430 | 41 |
| `v2026.02/presupuestos/presupuestos_aprobados.parquet` | 124.012 | 31 |
| `v2026.02/rrhh/altos_cargos.parquet` | 401 | 10 |
| `v2026.02/rrhh/convocatorias_personal.parquet` | 199.444 | 10 |
| `v2026.02/rrhh/enunciats_examens.parquet` | 213 | 29 |
| `v2026.02/rrhh/retribuciones_funcionarios.parquet` | 1.167 | 12 |
| `v2026.02/rrhh/retribuciones_laboral.parquet` | 126 | 10 |
| `v2026.02/rrhh/taules_retributives.parquet` | 11 | 10 |
| `v2026.02/subvenciones/convocatorias_subvenciones.parquet` | 34.766 | 9 |
| `v2026.02/subvenciones/raisc_concesiones.parquet` | 9.630.023 | 42 |
| `v2026.02/subvenciones/raisc_convocatorias.parquet` | 64.245 | 62 |
| `v2026.02/territorio/municipis_catalunya.parquet` | 949 | 9 |
| `v2026.02/territorio/municipis_espanya.parquet` | 8.134 | 3 |

---

[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)
