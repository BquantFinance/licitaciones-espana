# Cobertura de fuentes de contratación por comunidad autónoma

Qué publica cada administración, qué descargamos, qué falta y cómo atacarlo. Criterio del proyecto: servir los datos **tal como los publica la administración** (sin eliminar filas de origen, sin perder valores), guardando también los ficheros originales.

Estado a 2026-09. Confianza de cada fuente: **A** = confirmada en página oficial; **M** = confirmada a medias o por fuentes secundarias (código de terceros, catálogos); **B** = inferida. Este documento se redactó sin acceso a los portales oficiales. El 2026-09-27 se verificó en vivo parte de lo pendiente (ver §4.1); lo demás marcado M/B sigue sin verificar.

## 0. Contratos menores: qué tenemos (medición del 2026-09-27)

Objetivo del propietario: el 100 % de los contratos menores (LCSP art. 118 y 63.4) para un modelo antifraude.

**Método.**
- **PLACSP:** contratos menores distintos (un `id`) del feed 1143 en `licitaciones_espana.parquet` (v2026.02), con año de adjudicación 2018-2025.
  - Región: la del NUTS de ejecución, presente en el 100 % de las filas.
  - Tipo de órgano: el prefijo del DIR3 del órgano, presente en el 77,5 % (A = autonómica, L01 = ayuntamiento, L02/L03 = diputación, cabildo o consell, E = Estado, U = universidad).
- **Fuentes regionales:** contratos distintos por su clave en cada fuente.
- Scripts: `scratchpad/cobertura/medir_placsp.py` y `contar_regionales.py`, de la sesión del 2026-09-27.

**PLACSP 1143 por comunidad y tipo de órgano (2018-2025):**

| CCAA | Total | Autonómica | Ayuntamientos | Diput./Cabildo/Consell | Estado | Universidades | Sin DIR3 |
|---|---:|---:|---:|---:|---:|---:|---:|
| C. Valenciana | 499.558 | 26.171 | 276.508 | 22.821 | 21.269 | 36.338 | 99.769 |
| Castilla-La Mancha | 449.657 | 70.664 | 161.992 | 52.172 | 8.146 | 66 | 143.137 |
| Andalucía | 398.332 | **41** | 204.540 | 30.771 | 53.602 | 41.755 | 44.334 |
| Murcia | 351.311 | 61.457 | 90.115 | 5 | 11.768 | 138.768 | 49.190 |
| Castilla y León | 322.615 | 140.024 | 65.472 | 73.297 | 16.569 | 1 | 21.087 |
| Madrid | 271.983 | **230** | 54.837 | 23 | 157.902 | 1.757 | 46.167 |
| Canarias | 253.897 | 25.064 | 77.664 | 17.117 | 18.790 | 386 | 108.624 |
| Cantabria | 84.316 | 10.329 | 23.197 | 0 | 3.165 | 1 | 46.990 |
| Galicia | 83.525 | **10** | 41.129 | 7.294 | 15.938 | 1 | 16.651 |
| Aragón | 77.812 | 13.627 | 25.027 | 780 | 14.485 | 1.870 | 12.573 |
| Extremadura | 71.745 | 4.912 | 41.911 | 7.148 | 5.200 | 0 | 11.206 |
| País Vasco | 69.396 | **15** | **5.236** | 4 | 8.783 | 36.888 | 18.248 |
| Illes Balears | 56.341 | 13.296 | 10.923 | 4.434 | 3.627 | 0 | 22.433 |
| **Catalunya** | 31.267 | **49** | **66** | 85 | 13.986 | 0 | 16.958 |
| Asturias | 27.493 | 2.096 | 17.768 | 4 | 4.002 | 6 | 2.981 |
| Ceuta | 24.748 | 2.363 | 0 | 0 | 3.964 | 0 | 18.421 |
| La Rioja | 15.110 | **1** | 11.830 | 0 | 1.238 | 0 | 2.032 |
| Melilla | 3.294 | 1 | 0 | 0 | 2.413 | 0 | 880 |
| **Navarra** | 3.128 | **7** | **6** | 0 | 1.747 | 12 | 1.231 |
| (sin región) | 79.323 | 5.042 | 7.787 | 3.015 | 33.414 | 2.573 | 23.649 |

En negrita, lo que casi no está en PLACSP. Ahí los menores solo se publican en la plataforma propia de la comunidad o del ayuntamiento.

**Fuentes regionales que ya tenemos (contratos menores distintos por año):**

| CCAA | Fuente | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Catalunya | RPC (`contratos_registro`, Generalitat + locales + universidades; ventana móvil de 5 años) | 5 | 22 | 6.316 | 402.546 | 414.021 | 426.680 | 371.466 | 186.417 |
| Catalunya | PSCP (`contractacio_menors`) | 1.424 | 1.913 | 2.387 | 20.550 | 24.107 | 27.085 | 34.079 | 49.207 |
| Madrid | Comunidad (portal de contratación) | 206.629 | 174.992 | 171.760 | 164.473 | 320.026 | 439.322 | 384.677 | 215.353 |
| Madrid | Ayuntamiento (datos.madrid.es) | 7.857 | 8.772 | 6.152 | 6.753 | 6.566 | 5.719 | 5.535 | 4.730 |
| Galicia | Xunta (contratosdegalicia) | 156.608 | 186.075 | 181.151 | 204.315 | 200.331 | 191.457 | 221.827 | 251.968 |
| Andalucía | Junta (buscador; faltan ~41K del SAS) | 30.786 | 77.237 | 59.366 | 96.777 | 64.945 | 106.580 | 126.517 | 115.696 |
| Asturias | Principado (contratación centralizada) | – | 96.758 | 98.490 | 69.208 | 44.312 | 38.010 | 19.742 | – |
| Murcia | CARM (datosabiertos) | 20.223 | 24.633 | 17.892 | 18.810 | 19.906 | 21.313 | 21.452 | 20.987 |
| Murcia | SMS (**solo 2020**) | – | – | 155.085 | – | – | – | – | – |
| Castilla y León | Junta + SACYL (analisis.datosabiertos.jcyl.es) | 346 | 14.168 | 14.833 | 19.475 | 20.660 | 21.238 | 22.089 | 19.516 |
| Aragón | Gobierno (menores por año) + Registro (2023+) | 15.690 | 18.808 | 13.088 | 10.795 | 2.946 | 7.919 | 4.867 | 3.624 |
| C. Valenciana | REGCON (adjudicación directa; aproximado) | 5.806 | 5.351 | 5.038 | 6.184 | 6.354 | 5.217 | 10.169 | 108 |
| País Vasco | KontratazioA: **anuncios** de menores, sin importe ni adjudicatario | 4.728 | 59.854 | 63.172 | 59.144 | 91.149 | 99.620 | 92.883 | 86.809 |

**Primeras conclusiones:**
- **No hay cobertura del 100 %.**
- **Huecos totales:**
  - Navarra: ni en PLACSP ni en fuentes regionales.
  - Gobierno de La Rioja.
  - Menores del País Vasco con importe y adjudicatario: la API `/contracts` está escrita pero no se ha ejecutado entera.
  - Ayuntamientos vascos y catalanes anteriores a la ventana del RPC.
- **Huecos parciales:**
  - SMS de Murcia: solo 2020; los demás años no están en la misma URL (HTTP 404).
  - Aragón: 2024-2025 del Gobierno (el servidor da 403) y el Ayuntamiento de Zaragoza.
  - Andalucía: 41K del SAS.
  - Asturias: después de 2024.
  - Generalitat Valenciana: el REGCON parece traer pocos menores (~5-10K/año) frente al tamaño de la comunidad.
  - Organismos autonómicos que en PLACSP publican pocos menores en proporción (Extremadura 4.912, Asturias 2.096 y Canarias 25.064 en 8 años).
- **Solapes:** CyL, Murcia (CARM desde 2022) y Aragón publican parte de sus menores en PLACSP y también en su portal: hay que deduplicar antes de sumar, por NIF del órgano, expediente, adjudicatario, importe y fecha.
- **Pendiente:**
  - Contrastar con los totales oficiales (RCSP y OIRESCON) por CCAA para dar un % de cobertura.
  - El inventario de plataformas por CCAA (§2-§3, en curso).
  - Repetir la medición con la PLACSP regenerada, hasta septiembre de 2026.

## 1. Qué cubre ya PLACSP (`nacional/`)

| Feed | Contenido | Cobertura autonómica |
|------|-----------|----------------------|
| 643 Licitaciones | Perfiles alojados en PLACSP | Todas las CCAA que alojan sus perfiles en PLACSP (Aragón, Canarias, Cantabria, CyL, CLM, Extremadura, Murcia desde ~2020, Illes Balears, Asturias, C. Valenciana, Ceuta, Melilla) y casi todos los entes locales |
| 1044 Plataformas agregadas | Licitaciones de plataformas autonómicas propias, **sin menores** | Catalunya, Euskadi, Andalucía, Madrid, Galicia, Navarra, La Rioja (y Murcia 2016-2020) |
| 1143 Contratos menores | Menores **solo** de órganos que los cargan en su perfil de PLACSP | Muy desigual: muchos órganos publican los menores como XLS/PDF trimestral fuera del feed |
| 1383 Encargos, 1403 Consultas | Encargos a medios propios, consultas preliminares | Todas |

Consecuencias:
- Los **contratos menores de las plataformas autonómicas propias nunca llegan a PLACSP**: solo se obtienen de cada portal.
- Para las CCAA sin plataforma propia, lo que aportan las fuentes regionales es: menores estructurados, histórico anterior a 2018, registro de contratos (modificados, prórrogas, incidencias) y portales municipales propios.
- **Paso 0 (sin scraping):** una vista regional de PLACSP por DIR3 del órgano (A02 Aragón, A05 Canarias, A06 Cantabria, A07 CyL, A08 CLM, A11 Extremadura…; L01+INE para ayuntamientos), provincia del NIF y, en el 1044, host de la URL (`hacienda.navarra.es`, `larioja.org`, `carm.es`…). Mide qué hay ya por comunidad, conjunto y año, y sirve para no contar dos veces.

### Más información del CODICE que ya descargamos

Antes de raspar pliegos en HTML/PDF, el XML CODICE de cada entrada ATOM trae mucho más de lo que se extraía: todos los adjudicatarios de cada resultado (miembros de UTE), lotes con su presupuesto y CPV, modificaciones de contrato, criterios de adjudicación con su peso, indicador SARA, sistema de contratación, contadores de ofertas (pymes, UE, anormalmente bajas), número y fecha de formalización del contrato, programas de financiación y los enlaces a los pliegos (PCAP/PPT). También faltaban las consultas preliminares (el conjunto salía vacío) y las entradas borradas del feed. Ver `nacional/licitaciones.py`.

## 2. CCAA ya cubiertas: huecos y fuentes adicionales

| CCAA | Qué usamos | Huecos detectados | Fuentes a añadir (prioridad) |
|------|-----------|-------------------|------------------------------|
| **Euskadi** | XLSX anuales B1 (metadatos de anuncios), REVASCON 2013-2018, API (poderes, empresas), Bilbao | **Sin importes ni adjudicatario 2019-2026**: la API `/contracts` (655.518 contratos con importe, CIF, CPV) solo daba una muestra de 10 | API `/contracts` y `/contracting-notices` por ventanas de fecha (A); REVASCON por poder y año `contratos_poder{ID}_{AÑO}` 2018-2026 (A); Vitoria "Contratos formalizados" y "menores formalizados" (A); OpenDataBizkaia menores/no menores desde 2016 (A); Gipuzkoa Irekia (M); XML de detalle de cada anuncio (`xml_datos`) como plan B |
| **Catalunya** | Socrata (RPC `hb6v-jcbf`, PSCP `ybgg-dgi6`…), API de contractaciopublica.cat, Barcelona | Menores del portal incompletos frente a `ybgg-dgi6` (tope 20K por consulta en ICS, UPF, UAB…) | Socrata `qjue-2pk9` menores de la Generalitat con adjudicatario (A; importes en céntimos); AOC RPC local y histórico del perfil (A); `prorrogues-de-contractes` de Barcelona (M); detalle de publicación (`detall-publicacio-expedient`) con lotes y adjudicatario (M); instantáneas periódicas (el RPC es una ventana móvil de 5 años) |
| **Andalucía** | Buscador ES de la Junta (licitaciones y menores) | ~41K menores del SAS por encima del límite de 10K | CSV oficial anual "Contratación menor en {año}" del CKAN de la Junta, con NIF y nombre del adjudicatario (A); "Licitaciones publicadas {año}" como control (A); Registro de Contratos 2023+ (modificaciones y prórrogas) (M); menores municipales: Málaga (CKAN), Córdoba (CKAN), Diputación de Cádiz (A) |
| **Asturias** | CSV de contratación centralizada 2019+ | Años posteriores a 2024 (corregido); menores anteriores a 2019 | Menores 2016-2020 del dataset de datos.gob.es (M); relaciones trimestrales de menores por consejería 2024-2025 (A); Gijón datos abiertos (A); Oviedo perfil propio y BI (M) |
| **Galicia** | API de contratosdegalicia.gal (licitaciones 2007+, menores 2018+) | Licitaciones completas (99,95 % frente a PLACSP 1044); huecos puntuales de menores por errores HTTP del scraper antiguo | RSS de novedades para actualizaciones incrementales (A); menores de concellos y Deputacións vía 1143 filtrado por NUTS ES11 |
| **Madrid (Comunidad)** | Buscador del portal (CSV con CAPTCHA) | Menores de entidades históricas que ya no aparecen en el desplegable (consejerías de legislaturas anteriores) | Menores por ventanas de fecha sin entidad (M); feed Atom `feed/licitaciones2` para incrementales (A) |
| **Madrid (Ayuntamiento)** | CKAN datos.madrid.es | Solo ~3.400 de ~71.500 menores están en PLACSP | Ya cubierto por los dos datasets CKAN |
| **C. Valenciana** | CKAN dadesobertes.gva.es (REGCON) | 2025 congelado (corregido) | Comprobar si REGCON incluye menores comparando con el 1143 filtrado a la Generalitat; GVA Oberta (B); portal del Ajuntament de València (B) |

## 3. CCAA sin cubrir: plan

| CCAA | Situación en PLACSP | Fuentes regionales | Enfoque | Esfuerzo |
|------|---------------------|--------------------|---------|----------|
| **Castilla y León** | Perfiles en PLACSP desde 17-4-2018 | Portal OpenDataSoft `analisis.datosabiertos.jcyl.es`: contratos ordinarios, **menores** y modificados (2019+), acuerdo marco, emergencia, desiertos; SACYL menores (2018+) y ordinarios; CSV de licitaciones anterior a 2018 (A) | API Explore v2.1 (export completo) — `scripts/ccaa_castilla_leon.py` | ~1 día |
| **Región de Murcia** | 1044 hasta 2020; 643/1143 después (menores CARM solo desde 2022; SMS casi nada) | `datosabiertos.carm.es/odata/transparencia/contratosOD{AÑO}.csv` y `…/Hacienda/CONTRA_ContratosMenores_{AÑO}.csv` (A); menores del SMS en XLSX de transparencia.carm.es (M) | CSV directos — `scripts/ccaa_murcia.py` | 1-2 días |
| **Navarra** | Solo licitaciones por agregación desde 2018; **ningún menor** | CKAN `datosabiertos.navarra.es`: Registro de Contratos 2007-2024 y año vigente, anuncios y adjudicaciones del año en curso (A); SICP (modificados, prórrogas, encargos, menor cuantía agregada por contratista) (M) | CKAN + instantáneas — `scripts/ccaa_navarra.py`; SICP en 2ª fase | 2-4 días |
| **Aragón** | Perfiles en PLACSP desde 9-3-2018 | CKAN `opendata.aragon.es`: Registro de Contratos desde 2023 (mayores, menores, encargos con NIF y CPV), contratos 2009+ y menores 2014-2025 (SpreadsheetML `.xls.xml`), anuncios del perfil hasta 2021 (A); **Zaragoza en OCDS** (A) | CKAN + OCDS — `scripts/ccaa_aragon.py` | 1-2 días |
| **La Rioja** | CA por agregación (sin menores); ayuntamientos directos | Dato abierto opd-179 (licitaciones 2014-2025 a nivel de lote, NIF del adjudicatario) y menores por año (opd-979, opd-1151; 2021-2023 en datos.gob.es) (A) | Descarga directa — `scripts/ccaa_la_rioja.py` | 1-2 días |
| **Castilla-La Mancha** | Perfiles en PLACSP desde 19-6-2018 | XLS/XLSX trimestrales de menores de la JCCM desde 2016 (A); Registro de Contratos (buscador) (M) | Descarga de enlaces + unión de columnas — `scripts/ccaa_castilla_la_mancha.py` | 1-2 días |
| **Extremadura** | Perfiles en PLACSP desde 9-3-2018 | XLS trimestrales de la Intervención General (mayores, menores, incidencias; ~2016-2020), registro de contratos trimestral en juntaex.es (2022+; formato por confirmar); Cáceres CKAN (M) | Recorrido de páginas + XLS | 2-3 días |
| **Canarias** | Perfiles en PLACSP | Menores del Servicio Canario de la Salud (ODS/CSV trimestral 2021-2025) (A); Las Palmas de GC CSV (A); perfil antiguo "apipublica" (histórico) (M); menores de los departamentos del Gobierno solo agregados | ODS/CSV | 2-3 días |
| **Cantabria** | Perfiles en PLACSP desde 2018 | Transparencia "Consulta contratos Gobierno" desde 2015 con menores (buscador, sin descarga masiva) (A); CSV de contratosdecantabria.es (terceros, CC BY) (M) | Scraping del buscador; CSV de terceros como arranque | 2-3 días |
| **Illes Balears** | Govern e IB-Salut en PLACSP desde 2017-18 | Socrata `anss-9wx4` (derivado de PLACSP, con modificaciones y prórrogas) (M); menores del Ajuntament de Palma (B) | Solo lo que no está en PLACSP | 1-3 días |
| **Ceuta / Melilla** | Ceuta bien cubierta; Melilla sin menores | Relación de menores de Melilla (probablemente PDF) (B) | Prioridad baja | — |

Descartados: rendiciondecuentas.es y el Registro de Contratos del Sector Público de Hacienda (no publican contrato a contrato).

## 4. Verificación en vivo

### 4.1 Verificado el 2026-09-27 (sesión con red)

- **PLACSP.** `contrataciondelsectorpublico.gob.es` y `contrataciondelestado.es` sirven los ZIP (sin `Content-Length` ni `Range`; ~0,4-1 MB/s por conexión). Existen los anuales de 2025 y 2026 de los cinco conjuntos (el de 2026 se regenera a diario) y el mensual 202609. El 1403 (consultas) tiene ZIP 2022-2026 con dos ATOM cada uno; el de 2023 repite 345 entradas del de 2022, que salen como `entrada_repetida`. Las entradas CPM reales se parsean bien: 4.063 entradas y 2.262 consultas distintas en 2022-2026, frente a 3.681 filas en v2026.02.
- **TED.** CSV bulk: `https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-{año}.zip` redirige (301) a `/data-management/store/api/legacy/data/…zip/` y sirve el ZIP de 2006 a 2023 (2020: 88 MB, 2023: 111 MB). 2005 y 2024 dan 404. La API v3 acepta los 63 campos de `API_FIELDS` y los modos `PAGE_NUMBER` e `ITERATION`, que devuelve `iterationNextToken`. En una semana de 2025 no aparece nunca `winner-listed`, `buyer-contracting-entity`, `sme-part`, `subcontracting-value(-cur)`, `business-country` ni `business-identifier`.
- **BORME.** La API de sumarios responde en JSON y en XML, y `fetch_sumario_links` la parsea bien (secciones S, A, B y C). Solo cubre desde 2009. Días concretos:
  - 2012-09-07, 2012-09-11 y 2013-01-02: la API y el índice HTML coinciden, así que se pueden volver a descargar.
  - 2024-05-09 y 2024-05-10: solo tienen secciones S y C tanto en la API como en el índice oficial. No es un fallo de nuestra descarga.
  - 2001-2008: el índice HTML solo trae PDF de la sección C. La sección A en PDF por provincia empieza en 2009.
- **Euskadi (API KontratazioA).**
  - Volumen: `/contracts` tiene 715.569 contratos y `/contracting-notices` 717.076 anuncios.
  - Paginación: la API ya pagina, también con `?currentPage=N` a secas. `itemsOfPage` admite como máximo 50 (100 da HTTP 400). No hay tope de 10.000 por consulta: los 93.463 contratos de 2025 se paginan hasta la última página.
  - Filtros de fecha: `award-date.gt/.lt` aceptan AAAA-MM-DD, con `gt` estricto y `lt` inclusivo. El scraper pide `lt=hasta+1`, así que descarga un día de más que se descarta al consolidar. `/contracting-notices` filtra por `lastPublicationDate`.
  - Hay fechas erróneas en origen, desde `0001-01-03` hasta `2031-03-26`.
- **Catalunya (Socrata).** Existen `hb6v-jcbf`, `ybgg-dgi6` y `qjue-2pk9` (menores de la Generalitat, 10 columnas). `ydq4-xy5b` y `jxvs-kzbu` dan 404. `w2cu-rmuv`, `wwmk-zys7` y `nuym-4erw` son de presupuestos, no de contratación.
- **Madrid Ayuntamiento.** `datos.madrid.es` es un CKAN: `package_show` de `300253-0-contratos-actividad-menores` (29 recursos) y de `216876-0-contratos-actividad` (140), con CSV, XLSX, XLS y PDF de estructura. El año va en la descripción del recurso. Las URL antiguas `egob/catalogo/…` dan 404.
- **Scrapers nuevos.** Responden:
  - Castilla y León: API Explore v2.1.
  - Murcia: `contratosOD2024.csv` y `CONTRA_ContratosMenores_2024.csv`, y el CKAN de la Región.
  - Aragón: CKAN en `/api/3` y `/ckan/api/3`.
  - Excepciones: el CSV antiguo de licitaciones de CyL devuelve 92 bytes y el listado `transparencia.carm.es/…/SMS/Contratos_menores/` da 403.
- **Inalcanzables desde la nube de Claude Code.** No se pueden verificar ni regenerar desde allí:
  - `www.juntadeandalucia.es` (Andalucía) y `descargas.asturias.es` (Asturias): el túnel se corta en origen.
  - `www.zaragoza.es`: conexión reiniciada.
  - `datos.gob.es`: 403 de su cortafuegos (Imperva).

### 4.2 Pendiente

- **PLACSP.** Nº de entradas borradas por ZIP y hueco de `agregacion` del 17 al 22 de octubre de 2025. Salen del informe de procesado de la regeneración completa.
- **Andalucía.** Partición por mes de publicación (o `search_after`) para los segmentos del SAS de más de 10K y nº de documentos BRR. Requiere una máquina que llegue a la Junta.
- **Catalunya.** Filtro de fechas del portal de menores para segmentar ICS, UPF y UAB.
- **Madrid Comunidad.** Filtros de fecha para menores sin entidad.
- **Euskadi.** IDs de poder de REVASCON.
- **Scrapers nuevos.** Ejecución completa en vivo de Castilla y León, Murcia y Aragón.
