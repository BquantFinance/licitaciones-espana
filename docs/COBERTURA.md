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
| País Vasco | KontratazioA API `/contracts` (A1), con importe y CIF: **descargada el 2026-09-27, sin publicar** | 31.627 | 69.025 | 68.585 | 66.597 | 87.371 | 92.348 | 84.814 | 83.905 |

### 0.1 Veredicto (2026-09-27, noche)

**No hay cobertura del 100 %: estimamos el 55-65 % de los contratos menores publicados.**
- Recogemos ~2,0-2,2 millones de menores al año. En 2024:
  - PLACSP 1143: ~550 mil.
  - Catalunya (PSCP con NIF): ~350 mil, más los del RPC sin NIF.
  - Comunidad de Madrid: ~385 mil publicados; el portal tiene más.
  - Galicia: ~220 mil.
  - Junta de Andalucía: ~127 mil.
  - Murcia (CARM + SMS): ~120 mil.
  - Euskadi: ~85 mil (84.814 en 2024 en la API `/contracts`, descargada entera el 2026-09-27 y sin publicar todavía).
  - Resto: Asturias, CyL, Aragón, Valencia y Ayuntamiento de Madrid.
- **No hay un denominador oficial completo.**
  - La columna "Directo" del RCSP suma 1,16 M en 2024: 732 mil autonómicos, 233 mil locales, 174 mil de universidades y 18 mil de la AGE. Es solo una **cota inferior**: País Vasco, Navarra, La Rioja, Murcia, Ceuta y Melilla comunican 0 todos los años, y Madrid, Aragón y Cantabria casi 0.
  - La OIReScon concluye en su IAS 2025 que no es posible una imagen completa del volumen de contratación menor (§5.4).
- Donde el RCSP sí comunica, nuestras fuentes autonómicas cuadran con él en 2024:
  - Galicia: 222 mil frente a 219 mil.
  - Andalucía: 127 mil frente a 120 mil.
  - CyL: 22 mil frente a 21 mil.
  - Asturias: 20 mil frente a 16 mil.
  - Catalunya: la Generalitat en la PSCP frente a 141 mil.

**Por comunidad.** "Cobertura" es la estimación de los menores publicados que tenemos; "tras lo en curso" incluye los scrapers y descargas de esta sesión.

| CCAA | Cobertura hoy | Tras lo en curso | NIF | Huecos que quedan |
|---|---|---|---|---|
| Catalunya | ~95 % dentro de la ventana del RPC | = | PSCP 99,7-100 % desde el 2.º sem. 2022; RPC sin NIF | Antes de 2021 solo lo guardado. Faltan grandes ayuntamientos que solo publican documentos. El Parquet debe acumular versiones |
| Galicia | ~92 % | ~100 % (volver a ejecutar) | 99 % | Concellos fuera del 1143: Vigo (sin NIF), A Coruña (bloqueado desde la nube) |
| Andalucía | 75-85 % | = | 98 % | SAS ~41 mil (CSV del CKAN de la Junta, bloqueado desde la nube), 6 universidades, capitales y diputaciones de Granada y Huelva |
| Castilla y León | Junta ~100 % | = | 99 % | Valladolid, León, Salamanca, Ponferrada; diputaciones de Burgos y Ávila. SACYL publica ~2,4 mil/año (probable caja fija, art. 63.4) |
| Asturias | ~100 % del Principado hasta 2024 | = | 99,9 % combinando columnas | 2025-2026 bloqueado desde la nube; Gijón (64 mil con CIF desde 2018), Oviedo, Avilés |
| Madrid | ~52 % (Comunidad) | ~100 % (nueva descarga en curso) | 99 % | Ayuntamientos de Alcalá, Fuenlabrada, Móstoles, Leganés, Parla; universidades |
| Murcia | ~45 % | **~85-90 %** (SMS 2019-2025 hecho: +594 mil líneas) | 85-100 % (personas físicas enmascaradas) | UPCT, 14 ayuntamientos |
| País Vasco | ~11 % con importe y NIF | **~98 %** (API `/contracts` descargada: 643.462 menores 2014-2026; falta publicarla) | 100 % | Bilbao y Donostia (PDF), Barakaldo (nada desde 2021) |
| La Rioja | ~4 % | **~100 % del Gobierno** (CSV 2018-2026, scraper en curso) | Sí | Universidad, empresas públicas, Parlamento, ~120 municipios |
| Extremadura | ~30 % | **~90 %** (Registro de Contratos 1T 2022-2T 2026: `scripts/ccaa_extremadura.py`, 207.702 filas, verificado en vivo) | 99,997 % | 2016-2021 (Intervención General, 404), UEx, 173 ayuntamientos |
| Castilla-La Mancha | 50-65 % | ~85 % (UCLM, caja pagadora y ficheros de la JCCM, scraper en curso) | Sí salvo SESCAM | SESCAM (por factura, sin NIF), 469 ayuntamientos |
| Aragón | ~30 % | = (2024-2025 del Gobierno recuperados) | Registro 99,96 %; Gobierno sin NIF | Ayuntamiento de Zaragoza y DPZ (bloqueados desde la nube), SALUD pequeños, UZ |
| C. Valenciana | ~35 % | = | REGCON 93 % | Departamentos de salud, universidades (UV: 17 mil/año con NIF en XLSX), València, Elche, Diputación de Alicante |
| Canarias | ~30 % | = | — | **SCS: 89 mil/año sin fuente contrato a contrato** (solo totales; pedir por acceso a la información) |
| Illes Balears | ~15 % | = | — | Govern e IB-Salut (la CAIB publica ~2 mil/año, copia de PLACSP), Palma (bloqueado), UIB (PDF) |
| Cantabria | parcial | = | — | contratosdecantabria.es congelado en noviembre de 2023; cantabria.es bloqueado desde la nube |
| Navarra | ~0 % contrato a contrato | = | — | **La ley foral (art. 102.3 LFCP) solo obliga a publicar la menor cuantía agregada por empresa y trimestre**: 519 documentos en 2024, el 84 % PDF |
| Ceuta / Melilla | 1143 / ~0 | = | — | Melilla publica en PDF en la pestaña "Documentos" de su perfil |

**Hueco estructural: los entes locales.**
- Solo ~2.350 de los ~8.100 municipios publican menores en el 1143 (30 %). Municipio: el código INE del DIR3 `L01` o la ruta `dependencia` del órgano; publicado v2026.02:

  | CCAA | Municipios con menores en el 1143 (2024) | Municipios (INE) | % |
  |---|---:|---:|---:|
  | Asturias | 51 | 78 | 65 % |
  | Canarias | 54 | 88 | 61 % |
  | Murcia | 27 | 45 | 60 % |
  | C. Valenciana | 321 | 542 | 59 % |
  | Cantabria | 53 | 102 | 52 % |
  | Andalucía | 400 | 785 | 51 % |
  | Extremadura | 188 | 388 | 48 % |
  | Madrid | 84 | 179 | 47 % |
  | Aragón | 309 | 731 | 42 % |
  | Illes Balears | 27 | 67 | 40 % |
  | Galicia | 125 | 313 | 40 % |
  | Castilla-La Mancha | 350 | 919 | 38 % |
  | La Rioja | 30 | 174 | 17 % |
  | Castilla y León | 334 | 2248 | 15 % |
  | País Vasco | 1 | 251 | 0 % |
  | Catalunya | 0 | 947 | 0 % |
  | Navarra, Ceuta, Melilla | 0 | 274 | 0 % |

- Otros ~1.950 usan la PLACSP para licitar pero no cargan menores en el 1143. Los publican como listados en la pestaña "Documentos" (PDF o XLS) o no los publican.
- Catalunya y País Vasco los canalizan por la PSCP y KontratazioA. En País Vasco, 346 de 465 entes locales no publicaron ningún menor en la API en 2024.

**Para el modelo antifraude:**
1. Deduplicar los solapes antes de sumar (PLACSP frente a portal en CyL, Murcia desde 2022, Aragón y la UPV/EHU).
   - **CyL**: por el `idEvl` de su "Enlace de publicación", que es el mismo del `url` de la PLACSP. Lo trae el 96,5 % de los menores de la Junta y, en 2019-2025, entre el 94 % y el 98 % ya están en el 1143 (medido con v2026.02; 2026 casará con la PLACSP regenerada). El portal solo añade un 2-6 %.
   - **Resto de fuentes**: no llevan enlace ni identificador de la PLACSP. Hay que casar por contenido: NIF del órgano, expediente, adjudicatario, importe y fecha.
2. Armonizar el NIF, que cada fuente pone en su columna (Asturias, SMS…). Las personas físicas vienen enmascaradas (`***1234**`).
3. Separar los datasets por factura (SESCAM, relaciones de Navarra) de los que van por contrato.
4. Plan priorizado en `docs/CONTINUACION.md` §3.6. Inventario completo en §5.

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

## 5. Contratos menores: inventario de fuentes por comunidad (2026-09-27)

Inventario hecho con red completa desde la nube el 2026-09-27, en cuatro bloques independientes, cada uno con su método y su leyenda de confianza (**A** = verificada en vivo; **M** = fuente secundaria o portal inalcanzable desde la nube; **B** = inferida). "¿En 1143?" es cuántos menores de ese órgano trae el feed 1143 de la PLACSP. Las prioridades de cada bloque están resumidas y ordenadas en `docs/CONTINUACION.md` §3.6.

### 5.1 Catalunya, C. Valenciana, Illes Balears y Aragón

Verificado en vivo el 2026-09-27. Confianza: **A** = verificada en vivo; **M** = fuente secundaria o sitio inalcanzable desde la nube; **B** = inferida.

El 1143 se midió con los ZIP mensuales de marzo y junio de 2026 (45.612 y 44.359 entradas). Cada menor se asignó a su comunidad por el código postal del órgano. Más del 99 % traen NIF del adjudicatario e importe.

**PLACSP 1143: media mensual por comunidad y tipo de órgano**

| CCAA | Menores/mes | ≈ al año | Entidades locales | CA | Otras (universidades, fundaciones, consorcios) | AGE |
|---|---|---|---|---|---|---|
| Catalunya | 77 | 0,9 K | 0 | 0 | 26 (estatales) | 51 |
| C. Valenciana | 6.977 | 84 K | 4.947 | 530 | 1.421 | 80 |
| Illes Balears | 702 | 8,4 K | 444 | 96 | 148 | 15 |
| Aragón | 1.237 | 14,8 K | 956 | 68 | 79 | 135 |

Ningún órgano autonómico ni local catalán publica menores en el 1143. En Catalunya todo sale de la PSCP y del RPC.

#### Catalunya

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| PSCP en Socrata `ybgg-dgi6` (`procediment='Contracte menor'`) | Generalitat, entes locales, universidades | API Socrata (SoQL, CSV) | Estructurado desde el 2.º semestre de 2022 | 2024: 361 K. 2025: 381 K (locales 193 K, Generalitat 136 K, universidades 49 K). 100 % con NIF, nombre e importe | CSV completo (`ccaa_cataluna.py`); no se explota como tabla de menores | No | A | analisi.transparenciacatalunya.cat/resource/ybgg-dgi6.json |
| RPC `hb6v-jcbf` (`procediment_adjudicacio='Menor'`); su buscador `rpcac` cubre la misma ventana | Los mismos, más los menores < 5.000 € no publicados | API Socrata | Ventana móvil de 5 años (2021-2026) | 2023: 775 K. 2024: 655 K (89 % < 5.000 €). Nombre **sin NIF**; con CPV, importe y fecha | CSV completo | No | A | …/resource/hb6v-jcbf.json |
| `qjue-2pk9`, menores de la Generalitat | Generalitat y su sector público (entidades sanitarias 37 %) | API Socrata | 2020-2024 (5 años, cerrado a 1 de abril) | 150-210 K. Nombre sin NIF; importes en céntimos | **No**: el script pide `ydq4-xy5b`, que da 404 | No | A | …/resource/qjue-2pk9.json |
| API del portal PSCP (`cerca-avancada`) | Los mismos | JSON (tope de 10.000 por consulta) | 2018-2026; antes de 2022 muchas agregadas son 1 fila + documento | — | Sí (`ccaa_cataluna_contratosmenores.py`), sin NIF | No | A | contractaciopublica.cat/portal-api |
| CKAN del AOC | Espejos del RPC y la PSCP; menores de Rubí (2015-2026, con NIF), Sant Feliu, Manresa y Diputació de Tarragona | CKAN | Varios | 0,5-2,5 K por ente | No; redundante salvo Rubí | No | A | dadesobertes.seu-e.cat |
| Open Data BCN, `contractes-menors` | Ajuntament de Barcelona y entes municipales | CSV | 2014-2018; después, en PSCP y RPC | — | Sí. **Las descargas devuelven ahora un reto hCaptcha**; la API CKAN responde | No | A | opendata-ajuntament.barcelona.cat |

#### Comunitat Valenciana

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| PLACSP 1143 | Entes locales (Alicante ≈ 2 K/año, Castelló ≈ 1 K, Diputació de València), UJI, fundaciones y consorcios de la GVA, y una parte de consellerias y departamentos de salud | ATOM CODICE | 2018 en adelante | ≈ 84 K | Sí (`nacional/`) | — | A | …/sindicacion/sindicacion_1143/ |
| REGCON (`eco-gvo-contratos-{año}`) | GVA y sector público instrumental (73 órganos con menores en 2026) | CKAN, CSV con `;` | 2013-2026 | Menores ≥ 5.000 €: 4-6 K al año (2018-2024). En 2026 aparece la clase `Menor 5000`: 3,8 K, más 2,4 K de `Menor`, hasta septiembre. NIF en `CIF_NIF_ENMASCARADO` (las empresas salen completas) | Sí. El recurso de 2025 sigue congelado **en origen** (21-1-2025, 28 filas) | Parcial | A | dadesobertes.gva.es/dataset/eco-gvo-contratos-2026 |
| Buscador de menores del Ajuntament de València | Ajuntament | HTML (POST a un portlet Liferay, máximo 500 filas por consulta) | Probado 2025-2026 | ≈ 2 K (enero de 2025: 72; octubre de 2025: 236). NIF completo en ≈ 75 %; importe, unidad, nº de ofertas y aplicación presupuestaria | No | No (≤ 2 al mes) | A | valencia.es/cas/ayuntamiento/buscador-contratos-menores |
| Universitat de València | UV | XLSX o XLS trimestral, y PDF | 2016-2026 | ≈ 17 K (1T-2026: 4.413), con NIF | No | Parcial (70-90 al mes) | A | uv.es/contratacion/PORTALTRANSPARENCIA/menores/ |
| Diputación de Alicante (registro LIGATE) | Diputación | XLSX anual (trimestral en 2016-2018) | 2016-2026 | ≈ 1,5 K (2025: 1.494). Sin NIF | No | Parcial (≈ 10 al mes) | A | abierta.diputacionalicante.es/…/contratacion/ |
| UPV: "Contratos menores según Ley 1/2022" | UPV | Consulta HTML | — | — | No | No (0) | M | upv.es, portal de transparencia |
| UA, UMH, Elche, EMT, departamentos de salud | — | — | — | — | No | Casi nada | B | transparencia.elche.es da 403 |

#### Illes Balears

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| PLACSP 1143 | Consells (Eivissa, Mallorca, Formentera; el de Mallorca remite aquí sus menores), ayuntamientos salvo Palma, IB-Salut (≈ 50 al mes), Govern | ATOM | 2018 en adelante | ≈ 8,4 K | Sí | — | A | |
| Transparència de la CAIB, "Contractes menors" | Govern y sector público instrumental (IB-Salut 42 %, IBISEC 10 %) | CSV, XLS y ODS | 24-7-2017 a 30-6-2026 | 1,3-2,5 K (16.331 filas en total). CIF, enmascarado en el 11 % | No | Sí: se genera desde PLACSP | A | caib.es/sites/transparencia/ca/contractes_menors/ |
| Plataforma de contractació de la CAIB (menores 2008-2017) | Govern | HTML | 6-2008 a 23-7-2017 | — | No | No | A | **Retirada**: redirige a una URL rota |
| Ajuntament de Palma, relación trimestral | Ajuntament | XLSX trimestral | 2023-2026 | — | No | No (0) | M (el sitio corta la conexión desde la nube) | palma.es/es/contratos-menores |
| UIB | UIB | PDF trimestral | 2019-2023 o más | — | No | No (0) | M | transparencia.uib.cat |

#### Aragón

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| PLACSP 1143 | Comarcas y municipios (77 %), sectores de SALUD (Alcañiz, Calatayud, Zaragoza I), UZ (≥ 5.000 €), Huesca, Teruel, DPT | ATOM | 2018 en adelante | ≈ 14,8 K | Sí | — | A | |
| Registro de Contratos de la CA (GA_OD_Core 3133) | Departamentos, SALUD (38 %), organismos, empresas y fundaciones | CSV, JSON, XML y XLSX | 2023-2026 | ≈ 5 K (2023: 5.140; 2024: 4.868). NIF en el 99,96 %; CPV; nº de ofertas | Sí (`ccaa_aragon.py`) | Parcial | A | opendata.aragon.es/GA_OD_Core/download?resource_id=3133&formato=csv |
| "Contratos menores Gobierno de Aragón" (BRSCGI) | Los mismos | HTML servido como `.xls`, CSV y XML | 2014-2025 | ≈ 3,2 K (2024: 3.223; 2025: 3.139). Nombre sin NIF | Sí, pero **2024 y 2025 fallan**: el CKAN da URL `http://` y el proxy rechaza HTTP plano (403). Con `https://` responde 200 | Parcial | A | CKAN `contratos-gobierno-de-aragon` |
| Ayuntamiento de Zaragoza: API `contrato.json` y catálogo 147 | Ayuntamiento, sociedades y patronatos (según el catálogo, incluye menores) | JSON, XLS y SPARQL | — | — | Opcional (`--zaragoza-api`), sin verificar | No (0) | M (zaragoza.es bloqueado por el proxy) | zaragoza.es/sede/portal/datos-abiertos/servicio/catalogo/147 |
| OCDS de Zaragoza (OCP Data Registry) | Ayuntamiento | JSONL | 7-2016 a 3-2025; última recogida el 14-5-2025 | 0 menores (2024: 184 releases, solo `open` y `limited`) | Sí | — | A | data.open-contracting.org/en/publication/1 |
| DPZ: portal de contratación de la provincia, sección de menores | DPZ y municipios adheridos | HTML | — | — | No | No (0) | M (dpz.es corta la conexión) | dpz.es/ciudadano/perfil-de-contratante |
| UZ (Vicegerencia Económica) | UZ, menores de 0 a 15.000 € | CSV y XLSX | 2018-2020 | ≈ 1,3 K (2020), con NIF | No | Parcial | A | vgeconomica.unizar.es/datos-economicos/contratos-menores |

#### Huecos

Estimación de orden de magnitud (B). La referencia es la tasa per cápita de menores publicados en Catalunya (PSCP: ≈ 48 por 1.000 habitantes y año).

- **Catalunya.** No faltan fuentes: tenemos en crudo casi todo lo registrado dentro de la ventana. Los problemas son tres:
  - NIF: el 45 % de los menores solo trae el nombre. Son los del RPC que no están en la PSCP, casi todos por debajo de 5.000 €.
  - Histórico: el RPC y `qjue-2pk9` son ventanas de 5 años, y la PSCP solo es estructurada desde el 2.º semestre de 2022. De antes de 2021 solo queda lo que ya hayamos guardado.
  - Ayuntamientos grandes casi ausentes de RPC y PSCP (Badalona, L'Hospitalet, Cornellà, Mollet): publican pocos menores o solo documentos. Menos del 3 % (B).
- **C. Valenciana.** Tenemos ≈ 90 K al año de unos 250 K (≈ 35 %). Lo que falta se concentra en:
  - Sanidad: los departamentos de salud aportan ≈ 1,6 K al año en el 1143 y ≈ 0,6 K en REGCON.
  - Universidades: la UV publica ≈ 17 K en sus XLSX frente a ≈ 1 K en el 1143; UPV, UA y UMH no aparecen.
  - València, Elche y la Diputación de Alicante.
- **Illes Balears.** Tenemos ≈ 8,5 K al año de unos 55 K (≈ 15 %). Faltan el Govern e IB-Salut (la CAIB solo publica ≈ 2 K al año), Palma y la UIB.
- **Aragón.** Tenemos ≈ 20 K al año de unos 65 K (≈ 30 %), sumando 1143 y Registro con solapamiento. Faltan el Ayuntamiento de Zaragoza, los municipios del portal de la DPZ, los menores pequeños de SALUD y los de la UZ por debajo de 5.000 € desde 2021.

#### Prioridades (volumen × facilidad × valor antifraude)

| # | Acción | Filas/año | NIF | Esfuerzo |
|---|---|---|---|---|
| 1 | CAT: tabla de menores a partir de `ybgg-dgi6` (ya descargado). Cruzarla con el RPC por órgano, expediente, importe y fecha para añadir NIF. Guardar una instantánea mensual del RPC | 380 K + 270 K | Sí / por cruce | 1 día |
| 2 | CAT: cambiar `ydq4-xy5b` por `qjue-2pk9` (importes en céntimos) | 170 K (2020-2024) | No | 15 min |
| 3 | ARA: forzar `https://` en las URL BRSCGI; recupera 2024 y 2025 | 3,2 K | No | 15 min |
| 4 | VAL: XLSX trimestrales de la UV, 2016-2026 (sus enlaces `http://` también deben pasar a `https://`) | 17 K | Sí | 0,5 día |
| 5 | VAL: buscador de València, un POST por mes (< 500 filas) | 2 K | Sí | 0,5-1 día |
| 6 | VAL: registro LIGATE de la Diputación de Alicante, 2016-2026 | 1,5 K | No | 0,5 día |
| 7 | ARA: API `contrato.json` de Zaragoza, desde una red con acceso | 3-8 K (B) | Sí (M) | 1 día |
| 8 | BAL: XLSX trimestrales de Palma, desde una red con acceso | 2-5 K (B) | — | 0,5 día |
| 9 | ARA: menores del portal provincial de la DPZ | — | — | 1-2 días |
| 10 | VAL: UPV, UA, UMH y las relaciones trimestrales de los departamentos de salud adjuntas a su perfil en PLACSP | 10-40 K (B) | — | 2-4 días |
| 11 | BAL: PDF trimestrales de la UIB | — | — | 1-2 días |
| 12 | Controles: CSV de la CAIB frente al 1143, UZ 2018-2020, Rubí. BCN: sortear el hCaptcha con la API CKAN o conservar los ficheros del release | — | — | 0,5 día |

### 5.2 Andalucía, Murcia, Extremadura, Castilla-La Mancha, Canarias, Ceuta y Melilla

Verificado el 2026-09-27. Confianza: **A** = comprobada en vivo (código HTTP, formato y filas); **M** = catálogo o fuente secundaria; **B** = inferida.

"¿En 1143?" sale del ZIP anual 2025 del 1143: 542.191 menores distintos, clasificados por la jerarquía `ParentLocatedParty` del órgano. `www.juntadeandalucia.es`, `sevilla.org` y `web.archive.org` siguen cortando el TLS desde la nube.

#### Qué trae ya el 1143 (2025)

| Comunidad | Total | Autonómico | Universidades | Diputaciones / cabildos | Ayuntamientos (con menores / total) |
|---|---|---|---|---|---|
| Andalucía | 74.792 | 16 (la Junta, nada) | 14.982 (UMA 9.510, US 4.445) | 5.589 (Granada y Huelva: 0) | 48.438 (445 / 785) |
| Murcia | 59.830 | 21.112 (SMS: 4) | 20.232 (UMU; UPCT: 0) | — | 18.299 (31 / 45) |
| Extremadura | 13.194 | 4.052 (SES: 127) | 0 | 1.124 | 7.359 (215 / 388) |
| Castilla-La Mancha | 67.168 | 18.065 (SESCAM: 3.611) | 0 (UCLM) | 6.572 | 40.101 (450 / 919) |
| Canarias | 42.140 | 13.262 (SCS: ~1.700) | 252 | 5.053 | 17.673 (73 / 88) |
| Ceuta / Melilla | 2.429 / 116 | 2.370 / 116 | — | — | — |

El 99 % de las entradas trae NIF del adjudicatario e importe. Capitales: Sevilla 320, Granada 22, Cáceres 0, Guadalajara 0; Murcia 5.581 y Cartagena 3.255.

En las siete, los entes locales tienen el perfil en PLACSP. La Plataforma de la Junta de Andalucía no aloja entes locales.

#### Fuentes por comunidad

**Andalucía**

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Buscador ES de la Plataforma de la Junta | Junta, SAS, agencias, empresas. **Sin universidades, diputaciones ni ayuntamientos**: 0 filas en el publicado, contra lo que dice el README | JSON | 2016-2026 | 106-127K | Sí, `ccaa_andalucia.py` (faltan ~41K del SAS) | No | A (publicado) | `juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/` |
| CKAN "Contratación Menor en {año}" | Igual | CSV/JSON (2025: 224 MB) | 2018-2026 | Igual | No | No | M (data.europa.eu) | `…/datosabiertos/portal/dataset/00510697-…/download/menores_2025_v1_20260618.csv` |
| Pestaña "Documentos" del perfil en PLACSP | UGR (desde 2020), Diputación de Granada | PDF/XLS | 2019- | ? | No | No | A (UGR) | `scgp.ugr.es/pages/contratos-menores/contratos-menores` |
| Ayuntamiento de Sevilla | Ayuntamiento y organismos | PDF mensual | 2017- | ? | No | 320 | M | `sevilla.org/servicios/contratacion/contratos/{año}` |
| CKAN de Málaga y de Córdoba | Ayuntamientos | XLSX/XLS/ODS/PDF trimestral con CIF | 2016-2026 | ~600 / ~400 | No | Sí | A | `datosabiertos.malaga.eu`, `datosabiertos.cordoba.es` |

**Región de Murcia**

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| "Contratos menores CARM {año}" | Consejerías y organismos | CSV, una fila por pago, con NIF (2025: 20.987 filas, 12.491 expedientes) | 2014-2025 | 14-25K filas | Sí | Sí desde 2022 | A | `datosabiertos.carm.es/odata/Hacienda/CONTRA_ContratosMenores_{año}.csv` |
| SMS | Áreas de salud y hospitales | XLSX con NIF (el de personas físicas va enmascarado) | 2019-2025 | 65-79K contratos (95-155K líneas) | **Solo 2020**: cada año usa otro nombre (`SMS_Contratos_menores_2025`, `Contratos_Menores_SMS_2022-2024`, `SMS_Contratos_Menores_2021`, `PT_SMS_{1-4}T2019`) | No | A | `transparencia.carm.es/wres/transparencia/doc/Sector_Publico/SMS/Contratos_menores/` |
| Otros entes CARM (CEIS, ESAMUR, ICREF, INFO, ICA…) | Sector instrumental | XLS/CSV/PDF | 2015-2026 | Cientos | No | Parcial | A | `transparencia.carm.es/web/transparencia/contratos-y-convenios-del-sector-publico` |
| Ayuntamiento de Lorca | Ayuntamiento | CSV/XML (2013-2018 también en el CKAN regional) | 2013-2022 | ? | No | 40 | M | `datos.lorca.es/catalogo/contratos-menores-{año}/` |

**Extremadura**

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Registro de Contratos: listado trimestral de menores | Junta y SES (85-95 %) | XLSX/XLS con NIF, importe, CPV y órgano; tres esquemas | 1T 2022-2T 2026 | 30K (2025), 67K (2024) | **Sí**, `scripts/ccaa_extremadura.py` (2026-09-28) | ~4K (SES: 127) | A | `juntaex.es/w/registro-contratos-1t-2026` (resto, en el buscador) |
| Intervención General antigua | Igual | XLS | 2016-2021 | ? | No | Parcial | M (hoy 404) | `juntaex.es/ig/relacion-de-contratos-menores` |
| Ayuntamiento de Cáceres | Ayuntamiento y organismos | PDF anual | 2018-2024 | ? | No | 0 | A | `ayto-caceres.es/transparencia/…/contratos-menores-2/` |

**Castilla-La Mancha**

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Ficheros de transparencia (concepto 113), gestor PICOS | Junta; la UCLM hasta 2023 | XLS/XLSX con CIF, importe y fecha de publicación en PLACE | 2019-2026 | 16K (2025); 50K (2023, 31K de la UCLM) | No | 98,9 % (2025); 38,7 % (2023) | A | `contratacion.castillalamancha.es/ficheros-transparencia?concepto=113&year={año}` |
| Mismo portal: "caja pagadora" | Junta | XLSX con NIF | 2026- | ~33K | No | No | A | Mismo portal |
| Mismo portal: sector público regional | Junta, SESCAM y entes | ZIP (XLSX/RAR) | 2015-2018 | ? | No | No | A | Mismo portal |
| Mismo portal: SESCAM | Gerencias | XLSX por factura, sin NIF | 5 trimestres de 2022-2024 | ~145K líneas por trimestre | No | Parcial | A | Mismo portal |
| UCLM, "Menores anteriores" | UCLM | HTML (ASP.NET) con NIF | 2017-2026 | 26.907 expedientes (2025) | No | 0 | A | `contratos.apps.uclm.es/contratosMenoresAnteriores.aspx` |

**Canarias**

| Fuente | Órganos | Formato | Periodo | Volumen/año | ¿La tenemos? | ¿En 1143? | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Buscador de contratos adjudicados y formalizados | Gobierno | CSV (POST) | 2020- | 12.465 (2025) | No | Sí: es copia de PLACSP | A | `gobiernodecanarias.org/transparencia/…/actividad-contractual/formalizados/` |
| Contratos menores del SCS | SCS | ODS/PDF **agregado**, no contrato a contrato | 2021-2026 | **89.340** (2025) | No | ~1.700 | A | `www3.gobiernodecanarias.org/sanidad/scs/contenidoGenerico.jsp?idDocument=ecd71051-…` |
| Ayuntamiento de Santa Cruz de Tenerife | Ayuntamiento | CSV 2023-2024; PDF 2025 | 2020-2025 | ~1,2K | No | Sí | A | `santacruzdetenerife.es/gobiernoabierto/transparencia/contratos` |
| Ayuntamiento de Las Palmas de Gran Canaria | Ayuntamiento | CSV y perfil propio | ? | ? | No | ~2K | M (TLS) | `datosabiertos.laspalmasgc.es` |

**Melilla**: desde 2019 publica los menores en PDF en la pestaña "Documentos" del perfil de Hacienda en PLACSP, no en el 1143 (A, `melilla.es/…contenido=29689`). **Ceuta**: solo el 1143.

#### Huecos (estimación de lo que no tenemos hoy)

- **Canarias, ~65 %.** El SCS solo publica totales (89K al año). No hay fuente pública contrato a contrato.
- **Extremadura, ~70 %.** Sobre todo el SES (26-63K al año que solo están en el registro), más la UEx y 173 ayuntamientos.
- **Murcia, ~55 %.** Sobre todo el SMS de 2019 y 2021-2025, por el fallo de nombres del scraper; también la UPCT y 14 ayuntamientos.
- **Castilla-La Mancha, 35-50 %.** La UCLM (~27K), la caja pagadora (~33K), el SESCAM y 469 ayuntamientos.
- **Andalucía, 15-25 %.** Seis universidades, Sevilla y Granada capital, las diputaciones de Granada y Huelva, 340 ayuntamientos y los ~41K del SAS.
- **Melilla**: casi todo.
- **Patrón común:** muchos órganos cumplen el art. 63.4 con listados en la pestaña "Documentos" de PLACSP, que no llegan al 1143.

Correcciones: los menores del SCS son agregados; el registro de Extremadura es XLSX; la Plataforma de la Junta no incluye universidades ni diputaciones.

#### Prioridades

1. **SMS 2019-2025** en `ccaa_murcia.py`. La plantilla `Contratos_menores_SMS_{anio}` solo casa con 2020: hay que sacar las URL de la página de sector público y saltar las tres filas de cabecera de 2025. Son ~550K líneas con NIF. **2-4 h.**
2. ~~**Registro de Extremadura**~~: hecho (`scripts/ccaa_extremadura.py`). En vivo: 79 documentos y 207.702 filas de menores, un 99,997 % con NIF, con 1.132 M€ con IVA.
   - El listado de 4T 2023 vuelve a publicar 5.208 menores de trimestres anteriores. No se quita ninguno: la columna `_repetido_de` marca el listado anterior.
   - El trimestre del listado es el de inscripción, no el de adjudicación.
3. **UCLM 2017-2026**: un postback por año. **0,5-1 día.**
4. **Ficheros de la JCCM**: 57 enlaces en XLS, XLSX, ZIP y RAR (caja pagadora, 2015-2018, UCLM 2019-2023). **1 día.**
5. **CKAN de menores de la Junta de Andalucía**: cierra el hueco del SAS. Hay que ejecutarlo desde una máquina con acceso. **0,5 día.**
6. **Pestaña "Documentos" de PLACSP** (UGR, Diputación de Granada, Melilla): navegar el portal WPS y extraer tablas de PDF. **3-5 días.**
7. **Portales municipales**: Málaga, Córdoba, Santa Cruz de Tenerife, Lorca y Cáceres. Poco volumen, sirven para el histórico. **0,5 día cada uno.**
8. **SCS**: solicitud de acceso a la información.

### 5.3 Madrid, Castilla y León, Galicia, Asturias y Cantabria

**Método.** Se parseó el ZIP anual 2025 del 1143: 1.104 ATOM, 550.102 entradas y 542.191 id. Los órganos se clasificaron por `ParentLocatedParty`. Los portales se comprobaron con curl.

**Bloqueados desde la nube:**
- Cortan el TLS: `*.asturias.es` (descargas, sede, miprincipado), `astursalud.es` y `*.cantabria.es`.
- `share.coruna.gal`: 403 "Access denied for ASN 396982".
- `sede.oviedo.es`: 403.
- `sedeelectronica.aviles.es`: timeout.

Esas fuentes quedan en confianza M y hay que descargarlas desde una IP española no cloud.

**Leyenda.** 1143 = menores del órgano en el 1143 de 2025. Conf.: A = verificada en vivo; M = fuente secundaria o portal bloqueado; B = inferida.

#### PLACSP 1143 en 2025

| Comunidad | Autonómico | Local (entes) | Otros | Total | NIF adj. |
|---|---:|---:|---:|---:|---:|
| Madrid | 0 | 18.391 (113; Madrid capital 3.137) | 4.613 | 23.004 | 95 % |
| CyL | 20.309 | 24.788 (375) | 1.393 | 46.490 | 99,6 % |
| Galicia | 0 | 12.943 (141) | 1.050 | 13.993 | 99,7 % |
| Asturias | 345 | 4.089 (91) | 254 | 4.688 | 99,6 % |
| Cantabria | 1.758 | 4.591 (61) | 2.487 + UC 3.416 | 12.252 | 99,6 % |

- **Universidades:** ninguna de las cinco comunidades publica menores en el 1143, salvo la de Cantabria.
- **Ayuntamientos grandes con entre 0 y 80 menores en el 1143:**
  - Madrid: Alcalá, Fuenlabrada, Parla, Collado Villalba, Colmenar, Boadilla, Leganés, Coslada, Móstoles y San Sebastián de los Reyes.
  - CyL: Valladolid, León, Salamanca y Ponferrada, y las diputaciones de Burgos, Ávila y Soria.
  - Galicia: Vigo, A Coruña, Pontevedra, Ferrol, Oleiros, Arteixo y Carballo, y las deputacións de Lugo y A Coruña.
  - Asturias: Gijón, Oviedo y Avilés.
  - Cantabria: Torrelavega.

#### Comunidad de Madrid

| Fuente | Órganos | Formato | Periodo | Vol./año | ¿Tenemos? | 1143 | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Portal de Contratación CM | 125 entidades: consejerías, OOAA, SERMAS, Canal, Metro, RTVM, fundaciones | CSV de 18 col. con NIF (CAPTCHA) | ≤2017- | 340-440 mil (4.832.623 en total) | Parcial: el publicado tiene 2,53 M | No | A | contratos-publicos.comunidad.madrid/contratos |
| datos.madrid.es | Ayuntamiento y OOAA | CSV/XLSX | 2015- | ~7 mil | Sí | 3,1 mil | A | datos.madrid.es |
| PLACSP 1143 | 113 entes locales, fundaciones hospitalarias, empresas municipales | ATOM | 2018- | 23 mil | Sí (nacional) | — | A | sindicacion_1143 |
| Alcalá, CKAN | Ayuntamiento | XLSX trimestral: informe contable ADO pasado de PDF (408 hojas), con NIF | 2024 | ~4,5 mil operaciones | No | 0 | A | opendata.ayto-alcaladehenares.es/dataset/contratos-menores |
| Fuenlabrada | Ayuntamiento y OOAA | XLS/XLSX trimestral con CIF | 2022- | ~1 mil | No | 0 | A | transparencia.ayto-fuenlabrada.es/contratos/menores/ |
| Móstoles | Ayuntamiento y OOAA | PDF mensual con texto y NIF | 2018- | ~0,4 mil | No | 72 | A | mostoles.es (…/contratos-menores-mensuales-2025) |
| Leganés | Ayuntamiento | XLSX mensual con NIF | 2016- | ~0,2 mil | No | 49 | A | leganes.org/web/transparencia/contratos-menores |
| UPM | Universidad | XLSX anual (19 col.) y XML trimestral, sin NIF del adjudicatario | 2015- | ~0,9 mil | No | 0 | A | transparencia.upm.es/economico/contratos |
| UCM, UAM, UC3M, URJC, UAH | Universidades | Listas trimestrales | 2018- | 2-6 mil cada una | No | 0 | M/B | ucm.es/portaldetransparencia |
| Parla, Collado Villalba, Colmenar, Boadilla | Ayuntamientos | Sin localizar | — | — | No | 0-4 | B | — |

#### Castilla y León

| Fuente | Órganos | Formato | Periodo | Vol./año | ¿Tenemos? | 1143 | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| ODS `contratos-menores` | Consejerías, delegaciones, ITACyL, GSS, ECyL, ICE | API v2.1, 14 campos con NIF | 2019- | 17-20 mil | Sí (`ccaa_castilla_leon.py`) | Sí: el 97,8 % enlaza a PLACSP | A | analisis.datosabiertos.jcyl.es |
| ODS `contratos-menores-sacyl` | Gerencias de SACYL | Ídem | 2018- | 2,4 mil | Sí | Sí (99 %) | A | ídem |
| PLACSP 1143 | Junta y SACYL; diputaciones de Salamanca (5,2 mil), Valladolid (2,7 mil) y Zamora (2,2 mil); ayuntamientos de Zamora y Burgos | ATOM | 2018- | 46,5 mil | Sí (nacional) | — | A | sindicacion_1143 |
| Valladolid | Ayuntamiento y fundaciones municipales | XLSX trimestral acumulado de SICALWIN (24 col., sin NIF) | 2017- | ~2,5 mil (7.637 operaciones AD) | No | 36 | A | valladolid.gob.es/es/perfil-contratante/contratos-menores-volumen-contratacion-tipo-procedimiento |
| León | Ayuntamiento | HTML: listado y una ficha por contrato | — | — | No | 0 | A | sede.aytoleon.es/eAdmin/PerfilContratante.do?action=verContratos&tipo=menores |
| Salamanca | Ayuntamiento | Documento anual | 2017-2024 T1 | — | No | 4 | A | aytosalamanca.es/en/contratos-menores |
| Diputación de León | Diputación | XLS y PDF trimestrales | — | — | No | 122 | A | transparencia.dipuleon.es (08.02) |
| Diputaciones de Burgos y Ávila, Ponferrada, USAL, UVa, ULE, UBU | — | Relaciones trimestrales | — | — | No | 0 | B/M | — |

#### Galicia

| Fuente | Órganos | Formato | Periodo | Vol./año | ¿Tenemos? | 1143 | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| contratosdegalicia.gal, API de menores | 100 entes de la Xunta (SERGAS: 204 mil) | JSON DataTables con NIF | 2018- | 252 mil en 2025 (1.775.090 en total) | Sí (`scraper_galicia.py`); el publicado tiene 1,64 M | No | A | …/api/v1/organismos/{id}/contratosmenores/table |
| Misma plataforma: 263 concellos, deputacións, USC, UDC y UVigo | — | Solo licitaciones | — | 0 menores | — | — | A | ídem |
| PLACSP 1143 | 141 entes locales (Sanxenxo, Redondela, Lugo, Deputación de Ourense) | ATOM | 2018- | 14 mil | Sí (nacional) | — | A | sindicacion_1143 |
| Vigo | Concello | CSV/JSON/XLS anual, 9 col., sin NIF | 2019- | 1,4-2,1 mil | No | 4 | A | datos.vigo.org/data/sector-publico/contratos-menores-{AA}.csv |
| A Coruña | Concello | XLS/ODS trimestral | 2014-2025 | ~2,4 mil | No | 0 | A (listado; ficheros con 403 por ASN) | coruna.gal/transparencia (…/contratos-menores) |
| Deputación de Pontevedra | Deputación | DOCX/PDF trimestral, sin NIF | 2023- | ~0,4 mil | No | 119 | A | depo.gal/es/contratos-menores |
| Deputación da Coruña | Deputación | "Contratos e vales" | — | — | No | 19 | M | dacoruna.gal/contratacion/contratos-e-vales |
| Pontevedra, Ferrol, Deputación de Lugo, USC, UDC, UVigo | — | Sin localizar | — | — | No | 0-1 | B | — |

#### Asturias

| Fuente | Órganos | Formato | Periodo | Vol./año | ¿Tenemos? | 1143 | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| Contratación centralizada | Consejerías, OOAA, SESPA, empresas | CSV anual de 99 col. con NIF | 2019- | ~60 mil filas | Hasta 2024 (`ccaa_asturias.py`) | No (345) | M | descargas.asturias.es/asturias/opendata/SectorPublico/contratacion/ |
| Menores 2016-2020 | Ídem | XML | 2016-2020 | — | No | — | M | …/dataset-contratos-menores2016-2020.xml |
| Relaciones trimestrales | Consejerías y áreas del SESPA | XLSX/PDF | ~2020- | — | No | — | M | miprincipado.asturias.es/perfil-contratante/relaciones-trimestrales-contratos-menores |
| PLACSP 1143 | 91 entes locales | ATOM | 2018- | 4,7 mil | Sí (nacional) | — | A | sindicacion_1143 |
| Gijón | Ayuntamiento, Divertia, FMC, empresas municipales | CSV/JSON/XML de 19 col. con CIF | 2018- | 7-8,7 mil (63.978 en total) | No | 8 | A | opendata.gijon.es/descargar.php?id=725&tipo=JSON |
| Oviedo | Ayuntamiento y FMC | Buscador de la sede y BI Pentaho | — | — | No | 0 | M | sede.oviedo.es (…/contratos-menores-ayuntamiento-de-oviedo) |
| Avilés | Ayuntamiento | Publicaciones de la sede | — | — | No | 0 | M | sedeelectronica.aviles.es/Publicaciones.aspx?t=CM |
| Uniovi | Universidad | Relación trimestral | — | — | No | 0 | M | transparencia.uniovi.es/contratos |

#### Cantabria

| Fuente | Órganos | Formato | Periodo | Vol./año | ¿Tenemos? | 1143 | Conf. | URL |
|---|---|---|---|---|---|---|---|---|
| "Consulta contratos Gobierno" | Gobierno, OOAA, servicios centrales del SCS | Buscador sin descarga | 2015- | 3-4 mil | No | Parcial (1,8 mil) | M | transparencia.cantabria.es/consulta-contratos-gobierno |
| contratosdecantabria.es (terceros) | Ídem | CSV de 58 col., sin NIF | 2014 a nov. 2023 (congelado) | 3,2-4,4 mil | No | — | A | api.contratosdecantabria.es/v1/contracts/download |
| PLACSP 1143 | Gobierno, IDIVAL, HV Valdecilla, UC, 61 entes locales | ATOM | 2018- | 12,3 mil | Sí (nacional) | — | A | sindicacion_1143 |
| Santander | Ayuntamiento y TUS | Relaciones trimestrales por tipo | —-2026 | — | No | 102 | A | santander.es/servicios-empresas/perfil-contratante (tipo 905) |
| Hospitales del SCS y Torrelavega | — | Sin localizar | — | — | No | ~0 | B | — |

#### Verificación en vivo

- **CAM.**
  - `createddate` (desde) funciona sin entidad. Por año: 2018: 596 mil; 2019: 524 mil; 2020: 448 mil; 2021: 453 mil; 2022: 420 mil; 2023: 440 mil; 2024: 374 mil; 2025: 342 mil; 2026: 266 mil.
  - Con fecha hasta no devuelve resultados.
- **CyL ODS:**
  - 2019-2026: 128.119 filas de la Junta y 15.450 de SACYL.
  - 2025: 17.072 de la Junta y 2.444 de SACYL.
- **Galicia:** 429 organismos; 100 tienen menores. La API filtra por fecha en `recordsFiltered`.
- **Descargas con HTTP 200:**
  - Gijón: CSV de 23 MB. Los decimales con coma van sin comillas, así que es mejor usar el JSON.
  - Vigo: 2019, 2.067 filas; 2025, 1.436.
  - contratosdecantabria: 33.309 filas (25.302 menores).
  - UPM 2025: 904 filas.
  - Alcalá, 4º trimestre de 2024: 1.143 operaciones.

#### Huecos (estimación)

- **Madrid.**
  - Lo principal es regenerar la CM: faltan unos 2,3 M de filas (4,83 M en el portal frente a 2,53 M publicados).
  - Fuentes nuevas: ayuntamientos grandes (10-15 mil al año) y 6 universidades (10-20 mil al año).
- **Castilla y León.**
  - La Junta y SACYL están completos, porque el ODS coincide con el 1143.
  - Faltan 10-15 mil al año (20-25 %): ayuntamientos de Valladolid, León, Salamanca y Ponferrada, diputaciones de Burgos y Ávila, y 4 universidades.
  - SACYL publica unos 2,4 mil menores al año, frente a más de 200 mil del SERGAS. Probablemente usan la excepción del art. 63.4 (menos de 5.000 € por anticipo de caja fija), así que ese hueco no se puede recuperar.
- **Galicia.**
  - La Xunta está completa, pero hay que regenerar para recuperar 140 mil filas y corregir el importe ×10/×100.
  - Faltan 10-15 mil al año en grandes concellos, deputacións y 3 universidades.
- **Asturias.**
  - Del Principado faltan 2025 y siguientes, unos 60 mil al año; basta el scraper existente desde una IP española.
  - Localmente faltan 15-20 mil al año: Gijón, Oviedo, Avilés y Uniovi.
- **Cantabria.** Faltan unos 1,5-2 mil al año del Gobierno, el SCS (sin publicación localizada), Santander y Torrelavega.

#### Prioridades (volumen × facilidad × valor antifraude)

1. **Regenerar la CM**, por año con `createddate`: unos 2,3 M de filas con NIF. Esfuerzo: 1-2 días de ejecución.
2. **Regenerar Galicia.** Esfuerzo: 1 día.
3. **Asturias desde una IP española:** CSV de 2025-2026, XML de 2016-2020 y relaciones XLSX. Esfuerzo: 1 día.
4. **Gijón** (JSON con CIF). Esfuerzo: 0,5 días.
5. **Vigo y A Coruña** (esta, desde una IP no cloud). Esfuerzo: 1 día.
6. **Fuenlabrada, Leganés, Móstoles y Alcalá.** Esfuerzo: 2-3 días.
7. **Valladolid** (sin NIF: cruzar el nombre con el 1143 o el BORME) y **León**. Esfuerzo: 2 días.
8. **Universidades:** primero UPM, luego UCM y el resto. Esfuerzo: 3-5 días.
9. **Cantabria:** el buscador de transparencia, con contratosdecantabria como semilla. Esfuerzo: 2-3 días.
10. **Diputación de León y Deputación de Pontevedra.** Esfuerzo: 1 día.

**Paso 0:** vistas regionales del 1143 que ya descargamos, por `dependencia`, para no contar dos veces lo que se publica en los dos sitios.

### 5.4 País Vasco, Navarra, La Rioja y referencias nacionales

Verificado en vivo el 2026-09-27. Confianza: **A** = en vivo; **M** = secundaria o inalcanzable desde la nube; **B** = inferida. Cifras de referencia por comunidad y año en `docs/cobertura_referencias_menores.md`.

Tres situaciones distintas:
- **País Vasco.** La API de KontratazioA ya contiene los menores con importe, NIF y CPV: **643.463** contratos con `minorContract=true` (2014-2026) y el filtro `minor-contract=true`.
  - Serie anual: 69.025 (2019), 66.597 (2021), 92.348 (2023), 84.814 (2024), 83.905 (2025).
  - 2024, descargado entero por meses (84.814 ids = `totalItems`): 335,8 M€ con IVA. El NIF viene en el 100 %, el importe sin IVA solo en el 78 % y el CPV en el 34 %.
  - Publicado hoy: solo los metadatos de anuncio del B1, sin importe.
  - **Descarga completa del A1 hecha el 2026-09-27**, en el contenedor y sin publicar: 715.572 de los 715.574 registros de la API. Faltan 2 sin fecha que ninguna ventana devuelve.
    - La API sirve 217 filas dos veces, idénticas; quedan 715.357 contratos distintos.
    - **643.462 menores**, con el mismo recuento por año que el inventario `minor-contract=true`.
    - CIF en el 100 %. `awardAmount` (con IVA) en el 99,94 %, 2.651,8 M€ en total. Sin IVA en el 82,3 % y CPV en el 27,8 %.
    - 183 menores tienen el año de adjudicación mal escrito en origen (8, 201, 1201, 2027-2031…). Se sirven tal cual.
- **Navarra.** La LFCP 2/2018 (art. 102.3) solo obliga a publicar la menor cuantía **agregada por empresa y trimestre**. No existe fuente contrato a contrato.
- **La Rioja.** El Gobierno publica un CSV anual con **todos** sus menores (NIF incluido) desde 2018, y no lo usamos.

| Fuente | URL / API | Formato | Periodo | Órganos | Masiva | Menores/año | ¿Usamos? | ¿En 1143? | Conf. |
|---|---|---|---|---|---|---|---|---|---|
| **PV** API KontratazioA | `api.euskadi.eus/procurements/contracts?minor-contract=true` (+`award-date.gt/.lt`, `contracting-authority-id`) | JSON, 50/pág. | 2014-2026 | 441 de 938 poderes con algún menor (GV, DDFF, IFAS, EITB, Osakidetza; en 2024, 119 entes locales) | Sí | 84.814 (2024; 335,8 M€ con IVA) | A1 ejecutado el 2026-09-27 (643.462 menores); sin publicar | No | A |
| PV XLSX B1 `contrataciones_admin_{año}/opendata/contratos.xlsx` | opendata.euskadi.eus | XLSX | 2011-2026 | Los mismos | Sí | ~93 mil anuncios (2024) | Sí, sin importe ni NIF | No | A |
| PV REVASCON por poder (B4) `contratos_poder{ID}_{año}` | euskadi.eus | XLSX de 63 columnas | 2018-2026 | Por poder | Sí | Mismo código de contrato que la API; Bilbao sin menores | No (redundante) | No | A |
| PV Gardena, indicadores 49.2 (lista) y 49.1 (agregado) | gardena.euskadi.eus | XLSX | 2014-2025 | GV + Lanbide | Sí | 3.916 (2024) | No (sirve de control) | No | A |
| PV PLACSP 1143, órganos con sede en PV | feed 1143 | ATOM | 2018-2026 | 44: **UPV/EHU** (39.423 en total), AGE, mutuas, Ayto. Zalla, nanoGUNE | Sí | 11.161 (2024), 16.380 (2025) | Sí | Sí | A |
| PV Ayto. **Bilbao** "gasto menor" | bilbao.eus (BlobServer) | PDF de texto; adjudicatario, tipo, objeto, importe; sin NIF ni fecha | 2016-2026 T1 | Ayuntamiento | Sí (11 PDF) | ~1.850 (2024; 8,3 M€) | No | No | A |
| PV Ayto. **Donostia** | donostia.eus `/documents/d/asset-library-639343/contratos-menores-{año}-{trimestre}` | PDF trimestral (solo ≥5.000 €; con adjudicatario, importe sin IVA y fecha) | 2014-2025 T1 (para 2025 T2 dice "no hay") | Ayuntamiento | Sí | 376 (2024; 4,7 M€) | No | No | A |
| PV Ayto. Vitoria-Gasteiz | `vitoria-gasteiz.org/docs/j34/catalogo/00/09/` (ficha `app_j34_0009`) | ODS anual con NIF, importes y fecha (2013 PDF; 2014-15 CSV) | 2013-2026 T2 | Ayuntamiento | Sí | 490 (2024), ≈ API (471) | No: el C2 solo encuentra los CSV de 2014-15 que enlaza Open Data Euskadi, y su URL antigua da 404 | No | A |
| PV DF Álava (Araba Irekia) | irekia.araba.eus | XML/PDF anual | 2015-2022 | DFA | Sí | 260 (2022) | No | No | A |
| PV Ayto. Barakaldo | barakaldo.eus | PDF anual | 2011-2020 | Ayuntamiento | Sí | Nada en su web desde 2021 (API: 2-26/año) | No | No | A |
| PV OpenDataBizkaia "contratos menores"; Gipuzkoa Irekia "kontratu txikiak" | opendatabizkaia.eus; gipuzkoairekia.eus | CSV (sin verificar) | 2016+; hasta 2022 | DFB; DFG | ? | ? | No | No | M (conexión reiniciada; 503) |
| **Navarra** SICP "relaciones trimestrales de facturas / contratos menores" | `hacienda.navarra.es/sicpportal/mtoBuscadorFacturasTrimestrales.aspx` (POST ASP.NET por año) → `mtoGenerarDocumentoFacturaTrimestral.aspx?UID=` | 84 % PDF, 14 % XLSX, ODS, DOCX; por factura, contratista o contrato | 2018-2026 (documentos: 11 → 176 → 403 → 519 en 2018/19/21/24) | 150 entidades (2024): 12 departamentos (Salud con facturas del SNS-O), Pamplona, UPNA, Parlamento, 85 ayuntamientos o concejos | Semi (listado + UID) | 519 documentos (2024); Salud ~950-1.550 filas/trimestre (incluye facturas del HUN) | No | No | A |
| Navarra CKAN Registro de Contratos | datosabiertos.navarra.es | CSV | 2007-2026 | Todas | Sí | 0: no incluye menor cuantía | No | No | A |
| Navarra PLACSP 1143 | feed 1143 | ATOM | 2018-2026 | 13, todos del Estado o de la UNED (AENA, Guardia Civil…) | Sí | ~150-230 | Sí | Sí | A |
| **La Rioja** dato abierto "Contratos menores {año}" | `ias1.larioja.org/opendata/download?r=` + base64(`cd=N\|cf=03`); cd: 367, 379, 406, 866, 910, 963, 979, 1151, 1175 (2018→2026); cf 01 = XLS, 02 = XML, 03 = CSV, 04 = JSON | CSV `;` latin-1: COD_CONTRATO, DEPARTAMENTO, TIPO_EXPEDIENTE, TERC_CIF, TERC_NOMBRE, CONCEPTO, FECHA, IMPORTE_EJERCICIO | 2018-2026 | Administración general + **SERIS** (71 %) + IER | Sí | 50.343 (2024; 96,3 M€) | No | No | A |
| La Rioja PLACSP 1143 | feed 1143 | ATOM | 2018-2026 | 52 ayuntamientos (Haro, Arnedo, Calahorra, Alfaro, Logroño…) + AGE | Sí | 2.328 (2024) | Sí | Sí | A |
| La Rioja Ayto. Logroño | logrono.es/contratos-menores | XLS/PDF/CSV | 2017-2021 (950 filas); luego solo 1143 | Ayuntamiento | Sí | 96-170/año en el 1143 | No | Sí (2018+) | A |
| La Rioja "Consulta menores" de la plataforma; UR; Parlamento | larioja.org; unirioja.es | Excel trimestral por órgano | — | — | — | — | No | No (UR tampoco) | M (Cloudflare 403; 429) |

**Qué falta hoy** (estimaciones B):
- **País Vasco.** Lo publicado en 2024 suma unos 98 mil menores: API 84,8 mil + 1143 11,2 mil + Bilbao 1,85 mil + Donostia 0,4 mil. Con importe y NIF tenemos solo el ~11 % (el 1143); con el A1 pasaríamos al ~98 %.
  - Lo no publicado no se puede medir con fuentes abiertas:
    - En 2024, 346 de 465 entes locales no publicaron ningún menor en la API (Bilbao, Leioa, Sestao, Arrasate, Erandio…).
    - Osakidetza publica 2.387 menores (16,4 M€) en 2024, pero el TVCP halló **129,2 M€** de compras fraccionadas y tramitadas como menores en 2022 (M, prensa), frente a los 23,2 M€ publicados ese año.
- **Navarra.** De sus entes tenemos ~0 %. Lo máximo alcanzable son los documentos de unas 150 entidades, con granularidad heterogénea.
- **La Rioja.** Tenemos ~4 % de lo publicado (2,3 de 52,7 mil); con el CSV autonómico, ~100 %. Quedan fuera la UR, las empresas y fundaciones públicas, el Parlamento y unos 120 de los 174 municipios, sin menores en el 1143.

**Prioridades:**
1. ~~Ejecutar el A1 de Euskadi~~: hecho el 2026-09-27 con la API completa (643.462 menores; +67-92 mil/año con importe). Falta publicarlo desde la máquina del propietario (`Euskadi/ccaa_euskadi.py` y `consolidacion_euskadi.py`). Deduplicar la UPV/EHU de 2025-2026 (1.492 y 4.051 en la API, frente a 12.063 y 187 en el 1143).
2. `scripts/ccaa_la_rioja.py` con los 9 CSV (cd arriba): +32-50 mil/año.
3. Navarra SICP. Listado por año + descarga por UID + extracción, empezando por Salud (XLSX), Educación, Pamplona y la UPNA. Columna `_granularidad` (factura, contratista o contrato).
4. Fuentes pequeñas: Bilbao (PDF), Donostia (PDF), Vitoria 2013-2017 (anterior a la API), Álava XML 2015-2022, Barakaldo 2011-2020.
5. Verificar desde otra red OpenDataBizkaia, Gipuzkoa Irekia, la UR y "Consulta menores" de La Rioja.

#### Referencias oficiales para medir la cobertura

**Obligación legal (verificada en el BOE):**
- **Art. 346.3 LCSP.** Todos los menores se comunican al RCSP, salvo los de **menos de 5.000 € IVA incluido pagados por anticipo de caja fija**. Los demás de menos de 5.000 € van con datos mínimos (órgano, objeto, adjudicatario, código, importe). No es "solo los de 5.000 € o más".
- **Art. 63.4.** Publicación al menos trimestral, con la misma excepción (valor estimado).
- **Art. 335 e instrucciones del TCu de 28-6-2018** (BOE-A-2018-9585, local; BOE 28-7-2018, estatal y autonómico). Relación anual certificada al TCu/OCEX, menores incluidos, con la misma excepción.

**B1. RCSP, columna "Directo", sector autonómico, en miles de contratos.** "Directo" es la adjudicación directa: en la práctica, menores más emergencias. El RCSP no lo define; la equivalencia es una interpretación (B).
- Fuente: `https://www.hacienda.gob.es/DGPatrimonio/Junta%20Consultiva/Documentos%20txt%20registro%20de%20contratos/{AÑO}/Contratos{AÑO}-CCAA-Numero.txt` (e `-Importes-Porcentajes.txt`).
- Índice: página "Registro de Contratos del Sector Público" de la JCCPE. Corte de 2024: 17-12-2025.

| CCAA | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | M€ 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Total CCAA | 499,3 | 836,0 | 822,2 | 805,3 | 756,2 | 789,5 | 731,9 | 4.041 |
| Galicia | 190,1 | 194,3 | 178,9 | 203,7 | 202,3 | 202,3 | 218,8 | 1.187 |
| Canarias | 0,0 | 149,4 | 154,2 | 154,0 | 148,4 | 165,0 | 147,6 | 261 |
| Cataluña | 123,0 | 149,8 | 130,9 | 136,3 | 139,4 | 159,1 | 141,3 | 692 |
| Andalucía | 61,1 | 68,0 | 88,6 | 91,2 | 81,0 | 103,1 | 120,2 | 1.190 |
| Extremadura | 20,8 | 120,4 | 109,9 | 88,0 | 72,6 | 65,9 | 39,1 | 283 |
| Castilla y León | 0 | 13,2 | 13,6 | 16,9 | 18,9 | 19,6 | 20,7 | 96 |
| Asturias | 81,0 | 96,8 | 98,4 | 69,2 | 43,5 | 38,0 | 15,8 | 105 |
| C. Valenciana | 21,4 | 25,5 | 24,2 | 26,2 | 30,3 | 19,7 | 14,9 | 83 |
| Castilla-La Mancha | 0 | 17,1 | 16,8 | 17,9 | 18,2 | 15,4 | 11,6 | 54 |
| Illes Balears | 1,8 | 1,4 | 1,4 | 1,2 | 1,4 | 1,5 | 1,6 | 24 |
| Madrid | 0,0 | 0,0 | 5,0 | 0,7 | 0,1 | 0,0 | 0,0 | 65 |
| Cantabria | 0 | 0 | 0 | 0 | 0 | 0,0 | 0,1 | 3 |
| Aragón | 0,0 | 0,0 | 0,1 | 0,0 | 0,0 | 0,0 | 0,0 | 0 |
| **País Vasco, Navarra, La Rioja, Murcia, Ceuta, Melilla** | 0 | 0 | 0 (PV: 21) | 0 | 0 | 0 | 0 | 0 |

**B2. RCSP "Directo", total nacional por tipo de administración (miles de contratos y M€).**
- Ficheros `-Estado-`, `-EELL-`, `-Universidades-` y `-Mutuas-`.
- Las EELL solo vienen por tipo de entidad, **sin comunidad autónoma**.

| Tipo | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| AGE | 9,9 (366) | 10,7 (460) | 9,7 (323) | 11,6 (421) | 17,0 (483) | 17,9 (255) | 17,6 (315) |
| EELL | 76,6 (394) | 280,3 (620) | 253,3 (731) | 278,6 (756) | 267,7 (716) | 238,8 (696) | 233,3 (770) |
| Universidades | 65,5 (58) | 140,4 (124) | 118,1 (132) | 137,3 (143) | 172,2 (167) | 227,4 (250) | 173,6 (188) |

La UPV/EHU, la UPNA y la UR comunican **0** "Directo" todos los años.

**Limitaciones del RCSP como denominador:**
1. **Comunicación muy desigual.** Las seis comunidades en negrita comunican 0, y Madrid, Aragón y Cantabria casi 0, aunque publican decenas de miles (La Rioja 50 mil; PV 85 mil).
2. **Mezcla emergencias.** "Directo" incluye adjudicaciones de emergencia: Madrid 2020 declara 4.986 por 1.005,6 M€ (~200 mil € cada uno). En la AGE el importe medio es de 14-43 mil €, incompatible con solo menores. Hay que usar recuentos, no importes, y tratar 2020-2021 con cautela.
3. **Menores en "No consta".** Parte puede estar ahí (p. ej., Universidad de Murcia: 18.087 en 2024).
4. **Infraregistro también en la AGE.** El TCu (Informe nº 1.670, 26-2-2026, Cuadros 1-2) cuenta 4.590 menores (23,3 M€ sin IVA) solo en el área de gasto 2 de 7 ministerios en 2024. El RCSP da, para ministerios enteros, cifras menores (Derechos Sociales: 31 frente a 819; Juventud: 2 frente a 63).
5. Quedan fuera los menores de menos de 5.000 € pagados por anticipo de caja fija. Hay retraso (unos 12 meses) y los ficheros son instantáneas a una fecha de corte. No hay desglose por órgano.

**Otras fuentes evaluadas:**
- **OIReScon.** Ningún IAS revisado da recuentos de menores (2019-2021 completos; Módulo V de 2022-2025; Módulo I de 2026, que los excluye expresamente en la p. 22). El IAS 2025, Módulo V §18 (pp. 67-72), concluye que no es posible "una imagen fiel y completa del volumen de contratación menor".
- **TCu y OCEX.** Los informes del sector público local excluyen los menores de sus cuadros (nº 1.530, 2021). rendiciondecuentas.es solo informa de si cada entidad remitió.
- **CNMC.** E/CNMC/004/18 excluyó los menores por la calidad del dato.

**Recomendación:**
1. Medir la cobertura **fuente a fuente**: nuestras filas frente a lo que publica cada portal, que es un recuento verificable (tabla de §5.4 y `docs/cobertura_referencias_menores.md`).
2. Usar el RCSP solo como **cota inferior** y solo donde comunica de forma estable (Galicia, Canarias, Cataluña, Andalucía, Castilla y León, Castilla-La Mancha; Extremadura y Asturias con caídas fuertes), comparando recuentos del sector autonómico sin universidades.
3. Para PV, Navarra y La Rioja, el denominador es:
   - PV: la API, ~67-92 mil/año.
   - La Rioja: el CSV autonómico, 32-50 mil/año, más el 1143 local.
   - Navarra: los documentos del SICP; no hay recuento oficial.
4. Pedir por derecho de acceso (Ley 19/2013) a la JCCPE el RCSP "Directo" **por órgano (DIR3) y año**, y a TCu y OCEX los recuentos de menores de las relaciones anuales del art. 335. Serían los únicos denominadores oficiales completos, incluidas las EELL por comunidad.
