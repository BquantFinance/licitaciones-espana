# TED: Diario Oficial de la UE (avisos de España)

- **En la release:** `ted.zip`: `ted_es_can.parquet` (857.017 filas de 355.195 avisos, 164 columnas) y, en `v2026.02/`, los ficheros del cruce PLACSP↔TED de v2026.02 (`crossval_*`, `missing_*`), calculados sobre los datos de v2026.02 y sin regenerar.
- **Script:** [`ted/ted_module.py`](../../ted/ted_module.py); el cruce, [`ted/run_ted_crossvalidation.py`](../../ted/run_ted_crossvalidation.py).
- **Portal:** [ted.europa.eu](https://ted.europa.eu/) (API v3: [documentación](https://ted.europa.eu/api/docs/)) y el CSV masivo de [data.europa.eu](https://data.europa.eu/data/datasets/ted-csv).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#nacional-y-unión-europea).

Contratos publicados en [Tenders Electronic Daily](https://ted.europa.eu/) correspondientes a España. Los contratos públicos que superan cierto importe (contratos SARA) deben publicarse obligatoriamente en el DOUE.

| Conjunto (v2026.10) | Registros | Período | Fuente |
|----------|-----------|---------|--------|
| CSV bulk | 642K | 2006-2023 | data.europa.eu |
| API v3 + XML eForms | 202K | 2023-2026 | ted.europa.eu/api |
| Filas de la API conservadas de v2026.02 (avisos que TED ya no sirve) | 13K | 2020-2023 | release v2026.02 |
| **Consolidado** | **857K filas, 355K avisos** | **2006-2026** | — |

> En v2026.02 (corregido en v2026.10): 2020-2023 (y ~87 % de 2023) venían de la API sin adjudicatario, importe, nº de ofertas ni fecha de adjudicación (anuncios anteriores a eForms), las filas de la API no tenían fecha de publicación, tipo de contrato ni procedimiento, y solo se descargaban 4 de los 7 tipos de anuncio de adjudicación (faltaban `veat`, `can-tran` y `compl`). v2026.10 está regenerado con el `ted_module.py` actual: 2006-2023 del CSV bulk de data.europa.eu con todas sus columnas, los 7 tipos de anuncio y, desde los avisos eForms de 2023, la API con el XML de cada aviso.

## Archivos

```
ted/
├── ted_module.py                    # Script de descarga TED
├── run_ted_crossvalidation.py       # Cross-validation PLACSP↔TED + matching avanzado
├── diagnostico_missing_ted.py       # Diagnóstico de missing
├── analisis_sector_salud.py         # Deep dive sector salud
├── ted_can_2006_ES.parquet          # 2006 (CSV bulk)
├── ...
├── ted_can_2023_ES.parquet          # 2023 (CSV bulk)
├── ted_can_2023_ES_eforms.parquet   # avisos eForms de los años del CSV (2022-2023), que el CSV bulk no trae
├── ted_can_2024_ES_api.parquet      # 2024 (API v3 + XML eForms)
├── ...
├── ted_can_<año en curso>_ES_api_en_curso.parquet  # año en curso: solo histórico, no se usa como caché
├── ted_can_<año>_registros_irregulares.csv  # registros irregulares del CSV bulk (solo si los hay)
├── xml/<año>/<número>.xml.gz        # XML eForms de cada aviso de la API (capa cruda: se pide una vez)
├── ted_resultados_lote_es.parquet   # una fila por resultado de lote: todas las estadísticas de ofertas (BT-759/760)
├── ted_licitadores_es.parquet       # una fila por oferta descrita en cada resultado de lote, con su licitador
├── _historico/                      # versiones anteriores de las cachés y del consolidado
└── ted_es_can.parquet               # Consolidado (v2026.10: 857K filas, 156 MB; en ted.zip, con el cruce de v2026.02 en v2026.02/)
```

Los `ted_can_*.parquet` que estuvieron en Git LFS en el repositorio, ahora en la etiqueta `datos-lfs-v2026.02`, son las cachés de v2026.02: CSV bulk de 2010 a 2019 y API de 2020 a 2025, con los errores de arriba.

**Sin sesgo del superviviente.**
- Las cachés por año y el consolidado se guardan con `guardar_version`: si TED cambia algo, la versión anterior pasa a `_historico/`.
- `ted_es_can.parquet` se reconstruye desde todas las versiones de cada año con `acumular`. Los avisos que TED retira o cambia quedan con `_en_ultima_descarga=False`, y un año que la ejecución no pide no se toca.
- La clave es el aviso normalizado a `número-año`: el CSV da `2020112` y la API `112-2020`, y son el mismo aviso.
- `--semilla ted_es_can.parquet` (el publicado en v2026.02) añade los avisos que ya no se sirven. **Úsalo en el primer refresco**: las cachés antiguas de la API tienen otro formato y no se comparan fila a fila.
- Las cachés de la API de un parser anterior tampoco se comparan fila a fila, pero no se pierde lo que solo está en ellas: los avisos que ya no trae la descarga (TED los ha retirado) se añaden con sus filas de entonces, `_en_ultima_descarga=False` y `_origen='caché de <año> del parser anterior'`.
- Para contar o cruzar, quédate con la última versión de cada aviso y sin los cancelados: `avisos_para_cruce()`. Ya lo hacen `cross_validate_ted`, `run_ted_crossvalidation.py` y los scripts de análisis.

**Filas de la API (2024 en adelante y los avisos eForms de 2023): resultado de lote → oferta ganadora → adjudicatario.**
- Una fila por oferta ganadora de cada resultado de lote, leída del XML eForms del aviso. Según el esquema de eForms, `efac:LotResult/efac:LotTender` son las ofertas **recibidas** que describe el aviso para ese lote; la ganadora es la que cita un contrato (`efac:SettledContract/efac:LotTender`, BT-3202). De la oferta salen su importe (BT-720) y, por `efac:TenderingParty`, sus organizaciones (nombre, NIF, país, tamaño); del contrato que la cita, su referencia (BT-150) y la fecha de adjudicación (BT-1451).
- Las ofertas que no ganan no son filas; siguen en el XML guardado y su número va en `n_ofertas_descritas`. En un lote sin adjudicar (`clos-nw`, `open-nw`) no hay adjudicataria, aunque un contrato cite una de sus ofertas.
- Un resultado sin oferta ganadora (desierto, sin adjudicar o sin contrato) es una fila sin adjudicatario, con su estado (BT-142) y su motivo (`non_award_justification`, BT-144); un aviso sin resultados, una fila con los datos del aviso.
- `ganadora_por`: `contrato` (la cita un contrato) o `contrato del resultado`. Lo segundo es un error medido del publicador (4 de 1.402 adjudicatarias en 525 avisos): un resultado `selec-w` describe una sola oferta y su propio contrato solo cita ofertas de otro lote, alguna de la misma empresa (536696-2026: el contrato del lote 8 cita la oferta del lote 5). Esa única oferta es la ganadora, con el contrato de su resultado. Si el contrato cita una oferta del mismo lote, la ganadora es esa (543120-2026: dos resultados del lote 3 citan el mismo contrato, que cita una sola oferta).
- `ganadora_por` = `oferta única del resultado`: un resultado `selec-w` (con ganador) que describe una sola oferta y ningún contrato que cite una oferta suya, porque no tiene contrato o porque su contrato no cita ninguna (BT-3202 vacío). Esa oferta es la ganadora, con el contrato de su resultado si lo tiene: no hay otra candidata ni una cita que la contradiga, y la API de TED (`winner-*`) también la cuenta. Medido en octubre de 2026: 2.275 resultados de 1.211 avisos de 2023-2025 quedaban sin adjudicataria por esto (700433-2023: el lote 2 describe una oferta y no tiene contrato; el lote 1 sí). Con dos o más ofertas descritas y sin contrato, o si el contrato del resultado cita la oferta de otro lote de otra empresa, no se sabe cuál ganó y el resultado sigue sin adjudicataria.
- `win_nationalid` es el identificador de la organización con forma de NIF o, si no lo hay, el primero que publica. Los números internos de la plataforma (`ID_PLATAFORMA`, `ID_UTE_TEMP_PLATAFORMA`: la PLACSP numera así las UTE sin NIF) van aparte en `win_platform_id` y nunca como NIF (538782-2026: `329082` en `win_platform_id` y los NIF de los socios, «B91251082 - B82387770», en `win_nationalid`).
- Hasta septiembre de 2026 las filas salían por posición de las listas de la API, que aplana cada campo en una lista del aviso entero y deduplica valores iguales: al acabarse una lista se repetían el último ganador y el primer importe (159.677 filas copia; 141548-2026 tenía 48.861 filas para 247 ofertas ganadoras de 226 empresas, y 646040-2026, 9 filas para 3). La API no da los enlaces entre resultado, oferta y organización; el XML sí. Sus campos `winner-*` cuentan como ganadoras todas las ofertas descritas de un lote `selec-w`, tengan contrato o no.
- Un grupo de empresas va en una fila con los miembros unidos por `---`, el líder primero, como en el CSV bulk.
- Cada importe en su columna y con su moneda en `<columna>_cur`: `tender_value` (la oferta ganadora de la fila), `estimated_value_lot` (valor estimado del lote, BT-27), `framework_max_lot` y `framework_est_value` (máximo y reestimado del acuerdo marco del lote, BT-709 y BT-660), `tender_value_lowest`/`_highest` (BT-710/711), `notice_value` (valor del aviso, BT-161), `notice_framework_max_value` y `notice_framework_approx_value` (BT-118, BT-1118). `total_value` es el campo de la API, que mezcla BT-161 y BT-118. Las filas de la API ya no llevan `value_euro`, que mezclaba la oferta, el máximo del acuerdo marco y el valor estimado del lote. `importe_ted` es la oferta en euros y, sin oferta, el valor del aviso en euros solo si el aviso es de una fila. Las filas sin XML eForms (`_xml_eforms` con motivo) siguen como antes: `total_value` de la API.
- Título y descripción: `title_proc` y `description_proc` (API, BT-21 y BT-24 del procedimiento), `title_lot` y `description_lot` (XML).
- Versiones de eForms: `notice_identifier` (BT-701), `notice_version` (BT-757), `changed_notice` (BT-758, el aviso que se cambia: la API da su número de publicación si lo conoce y, si no, el identificador y la versión) y `change_reason_code` (BT-140).
- Clave de la fila: aviso, `lot_result_id` y `tender_id`; `lot_index` es su posición en el aviso y `n_filas_aviso`, las filas del aviso. En los avisos que relacionan los contratos de un acuerdo marco o un SDA hay varios resultados por lote y la misma oferta sale en varios, cada uno con su contrato (542447-2026: 18 contratos de una oferta de 27,19 €): son contratos distintos, y `tender_value` es el de la oferta, repetido.
- `_xml_eforms` vacío: la fila sale del XML. Si no, el motivo por el que solo lleva los datos del aviso: 404 de TED, error, XML ilegible o de un esquema desconocido.
- **XML del esquema anterior a eForms (TED_EXPORT R2.0.9).** A principios de 2024 TED aún publicaba avisos con el esquema anterior: 1.523 de España (924 F03, 432 F20 de modificación, 147 F06 de sectores especiales, 6 F15, 4 F21, 3 F25, 1 F13 y 6 sin formulario). La API no da de ellos adjudicatario, importe, expediente ni título; su XML sí. Una fila por sección `AWARD_CONTRACT` (contrato o lote), con `lot_result_id` = `tender_id` = `AWARD_CONTRACT-<ITEM>` y `ganadora_por` = `contrato`: contratistas (un grupo en una fila, unidos por `---` en el orden publicado: el esquema no marca líder), importe (`VAL_TOTAL`) y estimado (`VAL_ESTIMATED_TOTAL`) con su moneda, ofertas recibidas, fecha de celebración del contrato (`dt_award` y `contract_conclusion_date`), nº de contrato (`contract_id`) y lote (`LOT_NO`). Una sección sin adjudicar (`NO_AWARDED_CONTRACT`) es una fila `clos-nw` con el motivo tal como se publica en `non_award_justification` (`PROCUREMENT_UNSUCCESSFUL`, `PROCUREMENT_DISCONTINUED`). Expediente (`REFERENCE_NUMBER`), título, descripción y valor del aviso salen del XML solo donde la API los deja vacíos. Hasta octubre de 2026 eran una fila con los datos del aviso y `_xml_eforms` = «XML del esquema anterior a eForms (TED_EXPORT): sin resultados por lote».
- **Relectura desde el XML guardado.** En la caché de un año cerrado, los avisos que el parser actual puede leer de otra forma (los TED_EXPORT con esa marca y los que tienen un resultado `selec-w` sin oferta ganadora) se vuelven a leer del XML guardado en cada ejecución, sin pedir nada a TED. Si cambia alguna fila, la caché se guarda con `guardar_version` (la anterior queda en `_historico/`) y el consolidado conserva las filas viejas con `_en_ultima_descarga=False`. No hace falta volver a descargar nada.
- Los XML se guardan comprimidos en `ted/xml/<año>/<número>.xml.gz` y solo se piden una vez: un aviso publicado no cambia (una corrección es otro aviso). Solo se guarda una respuesta que es el XML de un aviso (eForms o TED_EXPORT): una página HTML servida con 200 no. `--force` los vuelve a pedir, y uno igual no crea versión; uno dañado en disco se vuelve a pedir y queda en `_historico/`.
- Se piden con una `requests.Session` por hilo (8 hilos), a 5 por segundo como mucho. La primera ejecución pide unos 100.000 (≈6 h); después, solo los avisos nuevos.
- Un aviso sin XML (404 o error tras 5 intentos) va con los datos del aviso y el motivo en `_xml_eforms`, y se vuelve a pedir en cada ejecución, también si su año ya está cerrado. Unos pocos errores (20 más el 0,1 %) no impiden guardar el año, y un 404 (el aviso no tiene XML) no cuenta. Más errores (TED caído), o 404 en más de la mitad de los pedidos y al menos 50 (una URL que ha cambiado), dejan el año sin guardar y la ejecución sale con 1.
- Las esperas entre reintentos no pasan de 120 s, aunque TED pida más con `Retry-After`, ni del presupuesto de tiempo.
- Presupuesto de tiempo: a las 9,5 h se deja de pedir XML; el año no se guarda y la siguiente ejecución sigue donde se quedó. Así la ejecución no pasa de ~10,5 h y no hace esperar más de 12 h a nadie en el cerrojo global.
- El CSV bulk de 2023 no trae los avisos eForms (2.609 de España, 0 en el CSV): se piden a la API con `notice-subtype` y se guardan en `ted_can_2023_ES_eforms.parquet`. Si el CSV ya trae un aviso, se queda con sus filas del CSV.

**Licitadores y estadísticas de ofertas (desde octubre de 2026; aún no en la release).** Las filas solo llevan la oferta ganadora y, de las ofertas recibidas, `number_offers` (el tipo `tenders`). El XML eForms da más para cada resultado de lote, y `ted_module.py` lo guarda en dos tablas aparte, sin tocar las filas, las cachés ni el consolidado:
- `ted_resultados_lote_es.parquet`: una fila por resultado de lote (`efac:LotResult`), con todas sus estadísticas de ofertas recibidas (`efac:ReceivedSubmissionsStatistics`: BT-760 el tipo y BT-759 el número), una columna por código de la lista `received-submission-type` del SDK: `st_tenders` (ofertas; es `number_offers`), `st_t_sme` (de pymes), `st_t_micro`, `st_t_small`, `st_t_med` (por tamaño), `st_t_esubm` (por medios electrónicos), `st_t_oth_eea` (de otro país del EEE), `st_t_no_eea` (de fuera del EEE), `st_part_req` (solicitudes de participación), `st_t_verif_inad` y `st_t_verif_inad_low` (verificadas e inadmisibles; por anormalmente bajas) y `st_t_no_verif` (no verificadas). Un código fuera de la lista va a `st_otros` tal cual (`código=número;…`). Además, el estado (BT-142) y su motivo, `n_ofertas_descritas`, `n_ganadoras` y `ganadora_por`.
- `ted_licitadores_es.parquet`: una fila por oferta que describe cada resultado de lote (`efac:LotResult/efac:LotTender`, OPT-320), ganadora o no, en el orden del aviso (`orden`, desde 1): su importe (BT-720, `tender_value` y `tender_value_cur`), rango (BT-171), referencia, parte licitadora (`tendering_party_id`, `tendering_party_name`) y organizaciones (`tenderer_name`, `tenderer_nationalid`, `tenderer_platform_id`, `tenderer_country`, `tenderer_town`, `tenderer_size`; un grupo en una fila, `---` entre miembros y el líder primero, como `win_*`; `n_miembros`). `es_ganadora`: `true` para la ganadora, la misma que en las filas (`ganadora_por`); `false` para las demás ofertas de un resultado con ganadora o sin adjudicar (`clos-nw`, `open-nw`); vacío si el aviso no deja saber cuál ganó (varias ofertas descritas y ningún contrato que cite una).
- Las dos llevan la clave del aviso (`ted_notice_id`, `year`), su tipo y subtipo, la fecha de publicación y el país del comprador (`buyer_country`), y el resultado (`lot_result_id`, `lot_id`, `internal_id_lot`). La clave de una fila de licitadores es aviso + `lot_result_id` + `orden` (también + `tender_id`); la ganadora casa con su fila del consolidado por aviso + `lot_result_id` + `tender_id`.
- Salen de los XML ya guardados en `xml/<año>/`, sin pedir nada a TED: se rehacen enteras al final de cada `download` (unos 2 minutos para 100.000 XML) o con `python ted/ted_module.py licitadores`. Se guardan con `guardar_registros`: si no cambian no se tocan y, si cambian, la versión anterior queda en `_historico/`. Los XML del esquema anterior (TED_EXPORT) no entran: su número de ofertas ya está en `number_offers`. Si fallan, el consolidado ya está guardado y la ejecución sale con 1.
- Lo publicado, tal cual. El identificador de la organización es el que publica el aviso, con las reglas de `win_nationalid` (`_id_organizacion`): un NIF si lo hay; si no, el primero. Euskadi publicó en 2025 su número interno de licitador, sin esquema y como decimal (`103401.0`), y en 2026 el NIF. Un importe 0 de una oferta que no gana es lo que publica el aviso (Euskadi lo pone en ofertas que no se valoraron). La PLACSP describe a veces varias ofertas de la misma empresa en un resultado (4 ofertas, todas de la adjudicataria y tres a 0 €): son filas distintas, como en el aviso; para contar licitadores, cuenta organizaciones distintas.

Medido el 26-oct-2026 con los 101.593 XML guardados (2023-2026; la tabla de resultados tiene 185.022 filas y la de licitadores 209.527). En los avisos de adjudicación de compradores de España (`can-standard`, `can-social`, `can-desg`, `can-tran`; 82.274 avisos, 150.099 resultados de lote):

| Año | Resultados de lote | Con `st_tenders` | Con `st_t_sme` | Con `st_t_esubm` | De otro país (`st_t_oth_eea` o `st_t_no_eea`) | Ofertas descritas que no ganan | Resultados con 2 o más ofertas y todas descritas |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023 (solo eForms) | 4.232 | 80,6 % | 80,4 % | 24,0 % | 12,1 % | 250 | 24 de 2.161 |
| 2024 | 45.833 | 66,8 % | 55,5 % | 25,7 % | 8,7 % | 1.117 | 496 de 18.814 |
| 2025 | 55.190 | 80,5 % | 61,5 % | 35,5 % | 15,2 % | 6.958 | 2.261 de 25.402 |
| 2026 (descarga del 24-oct) | 44.844 | 94,0 % | 83,6 % | 50,7 % | 41,8 % | 9.047 | 2.782 de 24.903 |

- En los resultados adjudicados (`selec-w`), los perfiles de la PLACSP publican el número de ofertas y el de pymes en el 97-100 % desde 2023. Las demás plataformas, entre el 0 % (Euskadi en 2024) y el 78 % en 2024-2025, y entre el 84 y el 100 % en 2026.
- Las listas completas de licitadores son casi todas de Euskadi (1.644 de 1.679 resultados con 2 o más ofertas en 2025; 2.107 de 2.177 en 2026). La PLACSP describe todas las ofertas en el 1-3 % de sus resultados con 2 o más.
- Las 17.372 ofertas que no ganan descritas en 2023-2026 llevan nombre en el 99,5 %, identificador con forma de NIF de persona jurídica en el 72,6 % (los números internos de Euskadi de 2025 son el 23,1 %) e importe en el 95,0 % (mayor que 0 en el 48,2 %).

**Columnas que añade el consolidado** (no las cachés), sin tocar las publicadas:
- `expediente_sin_lote` y `expediente_lote`: el expediente (API: `internal_id_proc`, BT-22; CSV: `CONTRACT_NUMBER` salvo el ordinal del contrato en el aviso, `1`, `2`...) sin el sufijo de lote y el número de ese lote. Sufijo: separador y L, LT, LOT o LOTE con el número al final (`PSU/2020/0000098983 lote 4`, `0011-20PSU-L1`, `AM 01/22 L08`, `352/19 L.3`), LOTE pegado (`EC/40/19LOTE4`) o L0n pegado a una cifra (`PSS/2023/0000038741L01`). Sin separador ni la palabra LOTE no se quita (`1518L206`, `2023-PL120`). La PLACSP publica el expediente sin el sufijo: de 10.517 expedientes de adjudicaciones de TED de 2018-2026 con sufijo, 504 casan con un expediente del mismo órgano en la PLACSP tal cual y 7.293 sin el sufijo.
- `importe_ted_marca`: en una adjudicación (con adjudicatario o, en el XML, con oferta ganadora), `acuerdo_marco_simbolico` si `importe_ted` es 0 o simbólico (hasta 1 €) en un acuerdo marco o SDA (CSV: `is_framework`, `B_FRA_CONTRACT` o `B_DYN_PURCH_SYST` = `Y`; XML: `contract_framework` = `true` o un máximo del acuerdo marco del lote o del aviso), `simbolico` si lo es fuera de ellos y vacío si no. Es lo que se publica en los acuerdos marco sin importe por empresa o con precios unitarios (0 €, 0,01 €): `importe_ted` no cambia. Un lote sin adjudicar que el CSV publica con 0 no se marca.

**Lo publicado, tal cual.**
- Los avisos cancelados se conservan con `cancelled='1'` (TED usa `'0'`/`'1'`, no `'Y'`/`'N'`). Hasta septiembre de 2026 se eliminaban al descargar. Para contar adjudicaciones, exclúyelos **después** de quedarte con la última versión de cada aviso, como hace `avisos_para_cruce()`: si los quitas antes, un aviso cancelado en una descarga posterior vuelve con su versión anterior.
- **Fechas del CSV bulk.** `DT_DISPATCH` y `DT_AWARD` vienen como texto en dos formas: `21-DEC-18` (2006-2019) y `30/12/19` (2020-2023, día/mes/año). Las de la segunda forma se leen como día/mes/año con formato explícito. Hasta octubre de 2026 se leían como mes/día cuando el día era ≤ 12: en 2020-2023, 102.030 de 281.079 fechas de envío y 57.627 de 177.091 de adjudicación del consolidado tenían el día y el mes cambiados (el año, nunca). **El `ted_es_can.parquet` de v2026.10 tiene ese error**; las cachés por año guardan el texto, así que basta volver a construir el consolidado, sin descargar nada.
- El CSV bulk se lee registro a registro con el módulo `csv`. Textos como `NA`, `N/A` o `NULL` se conservan como texto; antes pandas los convertía en nulos. En 2021 eran 4 celdas y en 2019, ninguna. En los NIF limpios (`win_nif_clean`, `cae_nif_clean`) sí cuentan como vacíos.
- Los registros irregulares van a `ted_can_<año>_registros_irregulares.csv`, sean del país que sean. Se guardan con la línea en que empiezan, el motivo y todos sus campos, y con versiones en `_historico/`. Hay tres motivos:
  - `campos_de_mas`: más campos que la cabecera. No entran en la tabla.
  - `campos_de_menos`: menos campos. Si son de España, entran completados con vacíos, como con pandas.
  - `salto_de_linea`: un campo en varias líneas; una comilla sin cerrar se traga los registros que siguen.
  
  Antes se perdían sin guardarse. En los CSV de 2019 y 2021 no hay ninguno.
  - Las filas que entran en la tabla desde un registro irregular llevan el motivo en `_registro_irregular`.
  - Si el CSV de un año trae alguno, `download` guarda lo descargado y sale con código 1. Es un aviso único: el CSV de un año cerrado solo se lee una vez y después se usa la caché.

## Campos principales (de las 164 columnas del consolidado)

| Categoría | Campos |
|-----------|--------|
| Identificación | ted_notice_id, notice_type, year, source, lot_id, internal_id_proc; API: lot_result_id, tender_id, ganadora_por, notice_identifier, notice_version, changed_notice; desde oct-2026: expediente_sin_lote, expediente_lote |
| Comprador | cae_name, cae_nationalid, cae_type, cae_town, buyer_legal_type, iso_country |
| Contrato | cpv, type_of_contract, top_type, is_framework, lots_number; API: title_proc, title_lot, description_proc, contract_id |
| Importes | importe_ted, value_euro, award_value_euro (CSV bulk), total_value, estimated_value_proc; API: tender_value, estimated_value_lot, notice_value, framework_max_lot; desde oct-2026: importe_ted_marca |
| Adjudicación | win_name, win_nationalid, win_country, win_size (SME), dt_award |
| Competencia | number_offers, direct_award_justification, award_criterion_type |
| Duración | duration_lot |

## Cruce PLACSP ↔ TED (cross-validation)

Pipeline para validar si los contratos SARA españoles se publican efectivamente en el Diario Oficial de la UE.

### Resultados

> ⚠️ Calculado con `licitaciones_espana.parquet` de `v2026.02` contando cada entrada del ATOM como un contrato: cada versión adjudicada de una misma licitación contaba como un contrato SARA distinto, la suma de lotes por expediente sumaba versiones repetidas y "negociado sin publicidad" era en realidad el código de negociado *con* publicidad. `run_ted_crossvalidation.py` ahora cruza solo la versión más reciente de cada licitación (`es_ultima_version`; el parquet de entrada no se modifica); las cifras cambiarán al regenerar.

**Entradas.** `run_ted_crossvalidation.py` lee la PLACSP de `nacional/licitaciones_completo.parquet` (la tabla principal: la salida de `nacional/licitaciones.py` o las 6 partes de la release juntas en un fichero) y el TED de `ted/ted_es_can.parquet` (lo que escribe `ted_module.py`), las dos rutas relativas a la raíz del repositorio. Para juntar las partes de la release (34 s y 3,7 GB de memoria con DuckDB; leerla después con `leer_placsp` pide unos 12 GB):

```python
import duckdb

con = duckdb.connect()
con.execute("SET preserve_insertion_order = false")   # el orden de las filas no importa y así pide menos memoria
con.execute("""COPY (SELECT * FROM 'nacional_licitaciones_*/licitaciones_completo/*.parquet')
               TO 'licitaciones-espana/nacional/licitaciones_completo.parquet' (FORMAT parquet)""")
```

El cruce guarda `id`, `fecha_updated`, `_ano` y `_ted_anio_cubierto` en `crossval_sara.parquet`: calidad une el resultado por versión, y un SARA de un año sin avisos en el snapshot TED no cuenta como *missing* (ni en `_ted_missing` ni en `crossval_missing.parquet`) porque ese snapshot no puede decir si se publicó.

| Métrica | Valor |
|---------|-------|
| Contratos SARA identificados | 442,835 |
| Validados en TED | 177,892 (40.2%) |
| Missing | 257,258 |
| Missing alta confianza | 202,383 |

### Reglas SARA

Los umbrales de publicación obligatoria en TED no son un importe fijo — varían por **bienio**, **tipo de contrato** y **tipo de comprador**:

| Bienio | Obras | Servicios (AGE) | Servicios (resto) | Sectores especiales |
|--------|-------|------------------|---------------------|---------------------|
| 2016-2017 | 5,225,000€ | 135,000€ | 209,000€ | 418,000€ |
| 2018-2019 | 5,548,000€ | 144,000€ | 221,000€ | 443,000€ |
| 2020-2021 | 5,350,000€ | 139,000€ | 214,000€ | 428,000€ |
| 2022-2023 | 5,382,000€ | 140,000€ | 215,000€ | 431,000€ |
| 2024-2025 | 5,538,000€ | 143,000€ | 221,000€ | 443,000€ |

### Estrategias de matching

El matching se hace de forma secuencial — cada estrategia actúa solo sobre los registros que las anteriores no encontraron:

| # | Estrategia | Matches | % del total |
|---|-----------|---------|-------------|
| E1 | NIF adjudicatario + importe ±10% + año ±1 | 43,063 | 9.7% |
| E2 | Nº expediente + importe ±10% | 7,891 | 1.8% |
| E3 | NIF del órgano contratante + importe | 77,816 | 17.6% |
| E4 | Lotes agrupados (suma importes mismo órgano+año) | 31,365 | 7.1% |
| E5 | Nombre órgano normalizado + importe | 17,757 | 4.0% |

**Hallazgo clave**: E3 (NIF del órgano) es la estrategia más potente. TED registra el NIF del comprador; PLACSP, el del adjudicatario. Sin cruzar ambos se pierde el 17.6% de matches.

### Validación por año

```
Año     SARA    Match    %
2016   10,948    2,643  24.1%
2017   17,360    6,532  37.6%
2018   32,605   14,720  45.1%
2019   42,951   14,182  33.0%
2020   40,693    9,214  22.6%  ← COVID + baja cobertura TED
2021   47,971    7,472  15.6%
2022   56,649   22,250  39.3%
2023   60,518   31,829  52.6%
2024   59,114   38,216  64.6%  ← máximo
2025   48,276   26,920  55.8%
```

### Análisis sectorial: Salud

El sector salud representa el 17% de contratos SARA con una tasa de validación del 42.3%. El 38% del missing se explica por patrones de lotes (un anuncio TED = N adjudicaciones individuales en PLACSP). La cobertura real ajustada por lotes es ~54%.

Top órganos missing: Servicio Andaluz de Salud (4,833), FREMAP (2,410), IB-Salut (1,957), ICS (1,316), SERGAS (1,291).

### Scripts TED

| Script | Descripción |
|--------|-------------|
| `ted/ted_module.py` | Descarga TED: CSV bulk (2006-2023) + API v3, con el XML eForms de cada aviso (2024 en adelante y los avisos eForms de los años del CSV) |
| `ted/run_ted_crossvalidation.py` | Cross-validation PLACSP↔TED con reglas SARA + matching avanzado (9 estrategias: E1-E7, E2b, E3b) → `ted/crossval_*.parquet` |
| `ted/diagnostico_missing_ted.py` | Diagnóstico de missing: falsos positivos vs gaps reales |
| `ted/analisis_sector_salud.py` | Deep dive sector salud: lotes, acuerdos marco, CPV, CCAA |
| `ted/cross-validation_ted_placsp.py` | **Versión anterior** y más simple del cruce (un único umbral de 140.000 € y dos estrategias de matching, sin las reglas SARA por bienio, tipo de contrato y comprador). Se conserva, con su test, para repetir los resultados de entonces; la vigente es `run_ted_crossvalidation.py` |

## Uso

```python
import sys

import pandas as pd

sys.path.insert(0, "licitaciones-espana")   # el repositorio clonado
from ted.ted_module import avisos_para_cruce

ted = pd.read_parquet("ted/ted_es_can.parquet")   # 857.017 filas: una por oferta ganadora de cada lote
avisos = avisos_para_cruce(ted)                    # la última versión de cada aviso y, después, sin cancelados
avisos.groupby("year")["ted_notice_id"].nunique()  # 355.124 avisos de adjudicación en v2026.10
```

Contar filas (`df.groupby("year").size()`) cuenta lotes y ofertas, no avisos, y repite los avisos con varias versiones.

---

[Todas las fuentes](README.md) · [README](../../README.md)
