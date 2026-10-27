# Cambios

Las versiones son las releases de datos (`vAAAA.MM`). Cada una lleva su nota completa en [Releases](https://github.com/BquantFinance/licitaciones-espana/releases); aquí va el resumen.

## [Sin publicar]

### Datos personales (próxima versión de la release)

- **NIF de personas físicas enmascarado** con el formato de la disposición adicional séptima de la LOPDGDD (cifras cuarta a séptima: `***4567**`), en toda columna de texto, con la marca `_nif_enmascarado`. El nombre, como la fuente.
- **Supresión y oposición** (arts. 17 y 21 del RGPD): quien lo pide sale de los datos (`[persona física]`, sin NIF; marca `_suprimido_art21`). Contacto: bquantfinance@gmail.com.
- **BORME: `persona_hash` con clave secreta** (HMAC-SHA256): ya no se puede calcular a partir de un nombre ni enlazar con v2026.10. La v2026.10 publicada no cambia. [Detalle](DATA_LICENSE.md#datos-personales).

### Añadido

- **BOE, sección III «Otras disposiciones»** (`boe/boe_otras_disposiciones.py`): una fila por disposición, día a día, por la API de datos abiertos del BOE: los convenios de la Administración General del Estado (que desde la Ley 40/2015 solo son eficaces publicados aquí), sus adendas, protocolos, encargos y los convenios colectivos, con una marca `_tipo`. 29.477 disposiciones de 2024-01-01 a 2026-10-24. Los nombres de personas físicas del título se sustituyen por «[persona física]». [Ficha](docs/fuentes/boe_otras_disposiciones.md).
- **GLEIF: entidades españolas con LEI y sus grupos** (`gleif/gleif_espana.py`): de la Golden Copy de la GLEIF (CC0), las 195.434 entidades con domicilio, sede o jurisdicción en España, las entidades de cualquier país enlazadas con ellas por una relación de nivel 2 (matriz directa y última, sucursales, fondos) y las excepciones de matriz de las españolas. Es el único dato abierto de grupos empresariales con la matriz declarada. Sin empresarios individuales: ni sus filas ni los ZIP originales que las traen se guardan. [Ficha](docs/fuentes/gleif.md).
- **TED: licitadores y estadísticas de ofertas de cada lote** (`ted/ted_module.py`): dos tablas nuevas desde los XML eForms que ya se guardan, sin pedir nada a TED. `ted_resultados_lote_es.parquet`, una fila por resultado de lote con todas las estadísticas de ofertas recibidas (BT-759/760: de pymes, por tamaño, electrónicas, de otro país del EEE o de fuera, solicitudes de participación, inadmisibles), y `ted_licitadores_es.parquet`, una fila por oferta descrita, gane o no, con su importe, su rango y su licitador (nombre, identificador, país y tamaño). Se rehacen al final de cada `download` o con `python ted/ted_module.py licitadores`. Las filas, las cachés y el consolidado no cambian: con los 101.593 XML de 2023-2026, las filas salen iguales XML a XML. [Ficha](docs/fuentes/ted.md).
- **Palma** en `scripts/municipios_menores.py`: la relación trimestral de contratos menores del Ayuntamiento (XLS/XLSX 2017-2026, 33.674 filas; los PDF de 2013-2016 se anotan sin extraer), del bloque «Documentos» de su página de contratos menores. Ninguno está en el feed 1143 de la PLACSP. El resto de municipios no cambia. [Ficha](docs/fuentes/municipios.md).
- **Cantabria** (`scripts/ccaa_cantabria.py`): la base de datos de contratos del Portal de Transparencia del Gobierno de Cantabria (2015-2026, 39.661 contratos, menores incluidos) por su API, con la exportación CSV de cada mes tal cual (273 columnas) y un Parquet acumulado. Trae los invitados y las ofertas de los menores, con el NIF de cada licitador, que no publica la PLACSP. [Ficha](docs/fuentes/cantabria.md).
- Documentación por fuente en [`docs/fuentes/`](docs/fuentes/): una ficha por fuente con todo lo que estaba en el README (qué trae, cómo se descarga, columnas, avisos y errores conocidos de cada versión), ejemplos rehechos sobre la release v2026.10 y fichas nuevas para Aragón, Castilla y León, Castilla-La Mancha, Región de Murcia, Extremadura, La Rioja, Canarias, los menores valencianos, los menores de la Junta de Andalucía y los 8 ayuntamientos.
- [`docs/REPRODUCIR.md`](docs/REPRODUCIR.md): requisitos, cómo se ejecuta cada script con su semilla, tiempos orientativos y qué ZIP sale de cada uno.
- Logo del repositorio en [`docs/img/`](docs/img/), con versión para fondo oscuro, y la vista previa social (`docs/img/social-preview.png`).
- [`docs/ESQUEMA.md`](docs/ESQUEMA.md): las marcas `_…` comunes (control de versiones, procedencia, calidad del origen, versiones de la PLACSP, importes corregidos, valores convertidos y TED): qué significa cada una, quién la pone y cómo filtrar.
- [`docs/esquema/`](docs/esquema/README.md): columnas, tipos y filas de cada Parquet de la release v2026.10, una página por ZIP, generadas con `scripts/esquema_release.py`, que lee solo los pies de los Parquet y comprueba filas y columnas contra el `MANIFEST.csv`.
- `tests/test_enlaces_docs.py`: cada enlace relativo y cada ancla del README y de `docs/` tiene que existir.
- **PLACSP: ampliación del CODICE en cuatro tablas nuevas** (`nacional/licitaciones.py`): lo que el ATOM publica y el scraper no leía. `_ampliado` (una fila por entrada: periodo de ejecución «del … al …», prórrogas previstas, recursos, subasta, variantes, subcontratación, límites de lotes y tipo y actividad del órgano), `_condiciones` (garantías, solvencia, clasificación, requisitos y condiciones especiales de ejecución, del expediente o de cada lote), `_anuncios` (cada anuncio con su tipo, medio, fecha, envío al DOUE o al BOE y el documento que adjunta, como la resolución de adjudicación) y `_documentos` (el `DocumentTypeCode` y el hash de cada documento). Salen de los mismos ZIP, sin peticiones nuevas, y se cruzan con la principal por `id` + `fecha_updated`. **Las tablas de antes no cambian**: reprocesando todo el histórico, la principal y sus tablas de detalle salen idénticas byte a byte. [Ficha](docs/fuentes/placsp.md#ampliación-del-codice-periodo-prórrogas-garantías-solvencia-anuncios-y-documentos).
- **PLACSP: retención opcional del histórico de la salida** (`nacional/licitaciones.py --conservar-versiones N`, con `--simular-poda` y `--solo-podar`; la regla, en `comun.historico.podar`). Cada versión de la salida se archiva entera en `<salida>/_historico/`: hoy unos 10 GB por versión, y unos 15 GB con la ampliación. Se puede regenerar desde los ZIP, que se conservan siempre. Con la opción, tras publicar quedan las N versiones anteriores más recientes de cada tabla Parquet y se borran las demás, cada una anotada en el registro con su tamaño y su fecha. Nunca se tocan la vigente, los ZIP descargados ni sus versiones, los CSV ni las semillas. Sin la opción no se borra nada, y ningún otro scraper cambia. [Ficha](docs/fuentes/placsp.md#retención-del-histórico-de-la-salida).
- `LICENSE` (MIT) para el código, [`DATA_LICENSE.md`](DATA_LICENSE.md) con la licencia de cada fuente y CC BY 4.0 para lo que añade el proyecto, `CITATION.cff`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` y plantillas de issues y PR.

### Cambiado

- **README** corto: qué es, descarga de la release (los 31 ZIP y `sha256sum -c`), fuentes, cobertura por comunidad, uso rápido con los avisos dentro del código (`es_ultima_version`, `avisos_para_cruce()` y el importe corregido), medir, reproducir, calidad, licencias y cómo citar. Recomienda clonar con `GIT_LFS_SKIP_SMUDGE=1` y bajar los datos de las releases.
- Los ejemplos de análisis del README antiguo sumaban sobre los ficheros de v2026.02 lo que no se debe sumar (todas las versiones de la PLACSP, filas del TED en vez de avisos, las copias de los menores de Catalunya, el `importe` inflado de Galicia): se han rehecho sobre la release, comprobados con los datos, o se han quitado.
- Contradicciones del README antiguo corregidas: años del CSV masivo del TED (2006-2023), cifras del BORME de v2026.10, 11 estructuras del Ayuntamiento de Madrid, 9 tablas de Euskadi, tramos de `idExpediente` en Andalucía y 21 indicadores de calidad (19 base + TED + BORME). Se quita la tabla de frecuencias de actualización, que estaba desfasada.
- `ted/run_ted_crossvalidation.py`, `ted/cross-validation_ted_placsp.py` y `ted/analisis_sector_salud.py` leen por defecto la PLACSP de `nacional/licitaciones_completo.parquet` (la tabla del scraper actual), no de `nacional/licitaciones_espana.parquet` (v2026.02).
- Cataluña: `ccaa_cataluna_parquet.py` convierte cada versión a Parquet por trozos. Con las tres versiones de la PSCP, el pico de memoria baja de 15,2 a 3,6 GiB, con la misma salida fila a fila (#57).
- **Red común en [`comun/red.py`](comun/red.py)** (sin cambio de comportamiento): `pedir_json`, `pedir_texto`, `descargar`, `_espera`, `ErrorPortal` y las constantes de red (User-Agent, timeouts, intentos y esperas), que estaban copiados en 9 scrapers autonómicos y municipales. Cada script importa los mismos nombres; lo que no era idéntico se queda en su script. Las mismas peticiones con los portales simulados de los tests y los mismos Parquet, byte a byte, desde la capa cruda de la descarga de producción.
- **Lectura común en [`comun/lectura.py`](comun/lectura.py)** (sin cambio de comportamiento): CSV (codificación, separador y lectura tolerante), XLSX, XLS, JSON y el formato real de un fichero, que estaban copiados en los mismos 9 scrapers. Cada script importa lo que tenía idéntico y que solo llama a funciones también idénticas; lo demás se queda en su script (p. ej. `leer_tabla`, igual en 4 scripts pero cada uno con su variante de lo que llama). Las mismas peticiones con los portales simulados de los tests y los mismos Parquet, byte a byte, desde la capa cruda de la descarga de producción.
- **Salida común en [`comun/salida.py`](comun/salida.py)** (sin cambio de comportamiento): `guardar_json`, `escribir_parquet`, `acumular_fichero`, `construir_parquet`, `_como_texto` y las fechas de versión (`fecha_version`, `iso`), que estaban copiados en los mismos 9 scrapers. Lo que depende de las columnas o de la lectura de cada script (`METADATOS_ORIGEN`, `ordenar_columnas`, `leer_tabla`) lo recibe como argumento, y el script conserva una envoltura de una línea con los suyos. Las mismas peticiones con los portales simulados de los tests y los mismos Parquet, byte a byte, desde la capa cruda de la descarga de producción.
- **Calidad: numeración de los indicadores como en el anexo del sistema de indicadores de transparencia de la Fundación Hay Derecho.** Cambian de nombre tres columnas de la salida de `calidad/calidad_licitaciones.py`, que llevaban el número de otro indicador del anexo: `INT-VAL-14` → `INT-VAL-13` (clasificación del procedimiento según cuantía), `INT-CONS-20` → `INT-COM-31` (formalización en el DOUE de los contratos SARA, el cruce con el TED) e `INT-FIA-12` → `PROPIO-FIA-01` (adjudicación < 100 × presupuesto, que no está en el anexo). `calcular_cons20` pasa a llamarse `calcular_com31`, y los nombres de `CATALOGO` son los del anexo. Los valores no cambian: la misma salida con los nombres nuevos. El Parquet de la release, el de v2026.02, conserva los anteriores. `tests/test_calidad.py` comprueba la numeración contra una tabla de referencia. Equivalencia y cómo leer los dos ficheros con los mismos nombres: [ficha de calidad](docs/fuentes/calidad.md#numeración-de-los-indicadores).

### Corregido

- **Cataluña, menores de la PSCP** (`scripts/ccaa_cataluna_contratosmenores.py`): la descarga se quedaba corta por el tope de 10.000 resultados de la API. Las fases 900 y 1000 no tienen procedimiento y se leían en dos ventanas de 10.000; los órganos de más de 20.000, también; lo que tenía un ámbito, tipo o procedimiento fuera de las listas fijas se perdía sin aviso, y las líneas de una publicación agregada se repetían o faltaban entre páginas de 100. Ahora cada fase se lee por tramos de la fecha de la última publicación, partidos hasta que cada uno tiene menos de 10.000 y comprobados con su recuento exacto (un día que sigue en el tope se parte por ámbito, tipo, procedimiento y órgano), con páginas de 2.000. En un ensayo del 26-oct-2026 faltaban en la salida 453.944 de 1.481.458 líneas, sin ningún tramo incompleto, con unas 7.500 peticiones en vez de 91.383. Con todo leído entero, los grupos vuelven a entrar en el ámbito: lo que el portal ya no sirve queda con `_en_ultima_descarga=False` y la semilla v2026.02 añade lo que falta. [Ficha](docs/fuentes/catalunya.md#contratos-menores-de-la-pscp).
- **Menores valencianos, Diputación de Alicante** (`scripts/ccaa_valencia_menores.py`): un enlace sin texto ni imagen (no se ve en la página) que da 404 ya no es un aviso de «enlace roto»: va a «NO PUBLICADOS EN EL PORTAL» y se sigue pidiendo. Es el caso de `RP-922-Adjudic-a-31-12-2024-cas.xlsx`, cuatro anclas vacías en la sección de menores que dan 404 desde la primera descarga (28-sep-2026); 2024 entero ya viene en RP-1076. Los enlaces con texto que dan 404 siguen siendo avisos, y lo descargado no cambia (el inventario `_enlaces.json` tampoco).
- **Cataluña, datasets retirados en origen** (`scripts/ccaa_cataluna.py`): `a23c-d6vp` (licitaciones y adjudicaciones en curso) y `jxvs-kzbu` (adjudicaciones quincenales) dan HTTP 404 desde la primera descarga (28-sep-2026) y no están en el catálogo del portal ni en el Wayback Machine; nunca se habían descargado (tampoco en v2026.02). Cada semana salían como `❌ Error … 404` y `⚠️ Metadatos … HTTP 404`. Ahora constan en `SOCRATA_RETIRADOS`: con 404 se anotan como «Retirado en origen» (no cuentan como fallo) y se siguen pidiendo; si vuelven a responder, se descargan y se avisa. Un 404 de cualquier otro dataset sigue siendo un fallo. Ningún fichero descargado cambia.
- **Murcia, menores del SMS** (`scripts/ccaa_murcia.py`): el portal de transparencia sirve a veces la página de sector público sin el bloque de enlaces, y la serie se quedaba sin comprobar esa semana («no enlaza ningún fichero de la serie»). Ahora la página se lee hasta 3 veces; la que llega bien se guarda en `raw/contratos_menores_sms/_pagina.html` y la que no, en `_pagina_sin_enlaces.html`; si sigue sin enlaces, se usan los de la última página buena (o las URL del manifiesto) de los últimos 28 días, con aviso y sin retirar nada. Además, una URL del catálogo CKAN ya no puede hacer pasar por buena una página sin enlaces (antes se habría retirado el resto de la serie), y una copia retirada que la página vuelve a enlazar vuelve a constar como publicada.
- **Galicia:** `scope_from_report` construye el día con `pd.Timedelta(1, unit="D")` en lugar de `pd.Timedelta(days=1)`: el mismo valor y la misma comparación, sin los 6.928 avisos de numpy («unidad genérica») que daba en los tests con pandas 2.2 y numpy 2.5 y que un numpy futuro convertirá en error.
- **Calidad:** `calcular_score` rellena con `False` sin el FutureWarning de pandas 2.2; el resultado es el mismo en cada versión de pandas (Parquet de salida idéntico byte a byte).
- **Calidad:** `calcular_score` daba `ZeroDivisionError` con pandas 3 si una fila no tenía ningún indicador evaluado y alguna columna era `object` (la suma por fila quedaba en `object` y 0 / 0 se dividía con enteros de Python). Ahora esa fila queda sin score (NaN), como con pandas 2.2. En el pipeline no ocurría, porque siempre hay indicadores booleanos evaluados, y la salida no cambia: Parquet idéntico byte a byte con pandas 3.0.6 y 2.2.3, también sobre la PLACSP hasta 2019 de la release v2026.10.
- **Tests:** el fixture de Canarias llevaba el NIF real de una persona física; ahora uno inventado con su letra de control válida (`12345678Z`).
- `ted/cross-validation_ted_placsp.py` se marca como versión anterior del cruce PLACSP↔TED; la vigente es `run_ted_crossvalidation.py`.
- **Comentarios que citaban scripts que no existen:** las cabeceras de `ccaa_castilla_leon.py`, `ccaa_murcia.py`, `ccaa_extremadura.py` y `ccaa_la_rioja.py` decían compartir su bloque de utilidades con `ccaa_navarra.py`, que no se llegó a escribir. Ahora nombran los cuatro scripts que lo comparten y lo que cada uno tiene de más. También el «Uso» de `scripts/ccaa_cataluna_contratosmenores.py` y `ted/run_ted_crossvalidation.py`, que citaba scripts con otro nombre, y el plan de Navarra en `docs/COBERTURA.md`. Solo cambian comentarios y docstrings (`scripts/comprobar_ast.py`: árbol sintáctico idéntico), y `tests/test_referencias_scripts.py` comprueba que cada script citado en un comentario o un docstring exista.

### Eliminado

- **Los datos de v2026.02 que estaban en Git LFS salen del árbol** (519 ficheros, 6,77 GB, y `Euskadi/euskadi_parquet/stats.json`). Siguen en la etiqueta `datos-lfs-v2026.02`, en las releases y en la historia: no se borra ningún objeto. El clon baja de 6,9 GB con git-lfs a unos 160 MB. `.gitignore` excluye las carpetas de salida de los scrapers y los Parquet, para que una ejecución local no suba datos; `.gitattributes` queda con la regla de `*.parquet`, y se quita `Euskadi/.gitattributes`, que la repetía. `ccaa_Andalucia/` y `ccaa_asturias/` siguen en el árbol, solo con un `LEEME.md`, porque sus scripts las crean de entrada (al importarse y al instanciarse) y con el repositorio en solo lectura tienen que existir.
- `catalunya/README.md`: estadísticas de enero de 2026 sobre una carpeta que ya no existe. La misma información va en `v2026.02/README.md` de `catalunya.zip`.

## [v2026.10] - 2026-10-05

31 ZIP (9,92 GB) con 142 ficheros y 115.160.695 filas, los mismos datos que usa [buscalicitaciones.com](https://buscalicitaciones.com). Para no tener menos datos que v2026.02, cada ZIP lleva en una carpeta `v2026.02/` los ficheros de v2026.02 que la descarga actual no cubre (473 ficheros, 39.512.017 filas), tal cual.

### Añadido

- **Fuentes nuevas:**
  - Aragón, Castilla y León, Castilla-La Mancha, Región de Murcia, Extremadura, La Rioja y Canarias;
  - los menores valencianos fuera del REGCON (universidades, Ajuntament de València y Diputación de Alicante);
  - los menores de la Junta de Andalucía con el SAS entero (CKAN);
  - los menores de 8 ayuntamientos grandes: Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife.
- **PLACSP, tablas nuevas:** resultados por lote, adjudicatarios, lotes, criterios, modificaciones y borrados.
- **Verificación:** cada ZIP lleva `LEEME.txt` y `MANIFEST.csv` (filas, columnas, bytes y SHA-256 de cada fichero), y la release, `SHA256SUMS.txt` y un `MANIFEST.csv` global.

### Cambiado

- **PLACSP:**
  - una fila por entrada publicada, con `n_versiones`, `entrada_repetida` y `es_ultima_version` (10.907.567 entradas de 5.241.440 licitaciones). Para contar o sumar licitaciones hay que filtrar `es_ultima_version`;
  - la tabla principal va partida por año en 6 Parquet repartidos en 5 ZIP, porque GitHub no admite ficheros de 2 GB o más. Juntas se leen como una sola tabla, con la misma huella de contenido que el original.
- **TED:** CSV masivo de 2006 a 2023 con todas sus columnas y los 7 tipos de anuncio de adjudicación, y la API v3 con el XML eForms de cada aviso (una fila por oferta ganadora de cada resultado de lote). 857.017 filas de 355.195 avisos.
- **Catalunya:** los contratos menores de la PSCP tienen su propio ZIP.

### Corregido

- **`madrid_ayuntamiento.zip`** era una copia exacta de `comunidad_madrid.zip`. Ahora trae los datos del Ayuntamiento: 132.087 filas, en tabla unificada y en tabla fiel.
- **PLACSP:**
  - `importe_sin_iva` es el presupuesto sin IVA y `valor_estimado_contrato`, el valor estimado. En v2026.02, `importe_sin_iva` era el valor estimado;
  - las etiquetas de procedimiento se recalculan desde el código, y el CPV es texto de 8 dígitos.
- **TED:** en v2026.02, 2020-2023 venían de la API sin adjudicatario ni importe, y faltaban 3 de los 7 tipos de anuncio de adjudicación.
- **Catalunya, contratos menores:** de 3,0 M de filas, con 2,16 M de copias idénticas, a 1.429.086 filas distintas.
- **Comunidad de Madrid:** se conservan las filas de continuación (lotes, adjudicatarios, prórrogas y modificaciones) que v2026.02 perdía al deduplicar: 4.906.068 filas, frente a 2.563.527.
- **Galicia:** `importe` ya no está inflado ×10 o ×100.
- **Asturias:** el IVA de 2023 ya no está multiplicado por 10, y los importes son numéricos.
- **Euskadi:** consolidación corregida (REVASCON sin columnas `unnamed`, filas corridas recolocadas y duplicados marcados) y la API `/contracts` completa: 715.868 contratos con importe y adjudicatario.
- **Andalucía:** 900.931 filas, frente a 808.441, con el registro (`portalGestor`, `idExpediente`) y columnas JSON con todas las adjudicaciones, lotes y anuncios.
- **BORME:** actos hasta el 2026-10-01, y recuperados los boletines 2012 n.º 173 y 2013 n.º 1.

### Ficheros de v2026.02 que van tal cual

En una carpeta `v2026.02/` del ZIP de su fuente, con el mismo SHA-256 que en v2026.02, para que v2026.10 no tenga menos datos que v2026.02:

- **Euskadi — `bilbao_contratos.parquet`:** es la última versión publicada (v2026.02, febrero de 2026), porque el portal de datos abiertos de Bilbao responde 403 a la descarga actual. Se actualizará cuando vuelva a servirse. Lleva los errores conocidos de esa versión (importes divididos entre 1.000, día y mes invertidos en la fecha de adjudicación).
- **Euskadi — `contratos_master` y `empresas_licitadoras` de v2026.02:** traen filas que la descarga actual ya no sirve (`contratos_2025.xlsx` tenía 100.850 filas y ahora 96.570; 4 empresas que ya no están en el Registro de Licitadores).
- **Catalunya:** las categorías que no son contratación (subvenciones, presupuestos, convenios, RRHH, entidades y territorio), que no se han vuelto a descargar, y `perfil_contratante_bcn` de v2026.02, con 6.120 publicaciones que el portal ya no sirve.
- **Catalunya, menores de la PSCP:** el `contractacio_menors.parquet` de v2026.02, con 66.132 publicaciones (`id`, `expedientId`) que la descarga actual no trae.
- **Valencia:** las categorías que no son contratación (subvenciones, presupuestos, empleo, paro, siniestralidad, patrimonio, territorio, turismo, sanidad y transporte), las asociaciones (no están en la descarga actual) y los lobbies, los recursos de gobierno abierto y el fichero de la DANA de v2026.02, cuyo contenido ha cambiado en el portal.
- **Galicia:** el CSV de v2026.02, con 913 contratos menores que la descarga actual no trae (y el `importe` inflado de entonces).
- **TED:** los ficheros del cruce PLACSP↔TED de v2026.02 (`crossval_*`, `missing_*`), calculados sobre los datos de v2026.02 y sin regenerar.
- **Calidad:** `calidad_licitaciones_resultado.zip` lleva el Parquet que v2026.02 publicaba en `calidad_licitaciones_resultado.rar`, sin regenerar: está calculado sobre los datos nacionales de v2026.02, con sus errores.

### Lo que no se repite de v2026.02

Porque lo que contiene ya está en v2026.10:

- Los Parquet nacionales de v2026.02 (`licitaciones_espana`, `licitaciones_completo_2012_2026`): las 8.693.891 y 4.725.557 filas están en v2026.10 por (`id`, `fecha_updated`), y las que no tenían `fecha_updated`, por contenido en `_semilla_contenido`.
- El consolidado de TED y sus cachés por año: los 287.036 avisos de v2026.02 están en el consolidado de v2026.10.
- Los Parquet de Andalucía, Asturias, BORME y la Comunidad de Madrid de v2026.02, las demás tablas de Catalunya y Valencia que siguen con el mismo nombre, y `poderes_adjudicadores` y `revascon_historico` de Euskadi: sus registros (por su clave) están todos en v2026.10, aunque algunos valores hayan cambiado por correcciones del script o del portal. A `revascon_historico` solo le faltan las 16 filas de título y cabecera de los XLSX que v2026.02 leía como datos.
- Los CSV que repetían un Parquet (Andalucía, Comunidad de Madrid), las capas crudas (`csv_originales/` de la Comunidad de Madrid) y las cachés (progreso de Andalucía, cachés por año de TED). Los scripts están en el repositorio.

### Avisos

- Los actos del BORME hasta el 2026-02-17 vienen de v2026.02, con sus errores conocidos, salvo cuatro días de 2012 y 2013 descargados de nuevo. Siguen faltando la sección A de 2012 n.º 174 y de 2024 n.º 89-90.
- `calidad_licitaciones_resultado.zip` es el Parquet de v2026.02 sin regenerar: está calculado sobre los datos nacionales de v2026.02, con sus errores.
- Las filas con `_origen = 'release v2026.02'` vienen de la release anterior: la fuente ya no las sirve o no se han vuelto a descargar.

## [v2026.02] - 2026-02-13

Primera release de datos: un ZIP por fuente, sin Git LFS. `nacional.zip` (PLACSP), `ted.zip`, `borme.zip`, `andalucia.zip`, `asturias.zip`, `catalunya.zip`, `comunidad_madrid.zip`, `madrid_ayuntamiento.zip`, `contratos_galicia.zip`, `euskadi.zip`, `valencia.zip` y `calidad_licitaciones_resultado.rar`.

### Errores conocidos (corregidos en v2026.10)

- **PLACSP:** versiones de una licitación sin marcar (sumar todas las filas infla los importes), `importe_sin_iva` con el valor estimado y etiquetas de procedimiento desplazadas.
- **TED:** 2020-2023 sin adjudicatario ni importe, y 3 de los 7 tipos de anuncio de adjudicación sin descargar.
- **Catalunya, contratos menores:** 2,16 M de copias idénticas.
- **Comunidad de Madrid:** sin las filas de continuación (lotes, adjudicatarios, prórrogas y modificaciones).
- **Galicia:** `importe` inflado ×10 o ×100.
- **Asturias:** IVA de 2023 multiplicado por 10.
- **`madrid_ayuntamiento.zip`:** copia exacta de `comunidad_madrid.zip`.

[Sin publicar]: https://github.com/BquantFinance/licitaciones-espana/compare/v2026.10...HEAD
[v2026.10]: https://github.com/BquantFinance/licitaciones-espana/releases/tag/v2026.10
[v2026.02]: https://github.com/BquantFinance/licitaciones-espana/releases/tag/v2026.02
