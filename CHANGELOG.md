# Cambios

Las versiones son las releases de datos (`vAAAA.MM`). Cada una lleva su nota completa en [Releases](https://github.com/BquantFinance/licitaciones-espana/releases); aquí va el resumen.

## [Sin publicar]

### Añadido

- Documentación por fuente en [`docs/fuentes/`](docs/fuentes/): una ficha por fuente con todo lo que estaba en el README (qué trae, cómo se descarga, columnas, avisos y errores conocidos de cada versión), ejemplos rehechos sobre la release v2026.10 y fichas nuevas para Aragón, Castilla y León, Castilla-La Mancha, Región de Murcia, Extremadura, La Rioja, Canarias, los menores valencianos, los menores de la Junta de Andalucía y los 8 ayuntamientos.
- [`docs/REPRODUCIR.md`](docs/REPRODUCIR.md): requisitos, cómo se ejecuta cada script con su semilla, tiempos orientativos y qué ZIP sale de cada uno.
- Logo del repositorio en [`docs/img/`](docs/img/), con versión para fondo oscuro, y la vista previa social (`docs/img/social-preview.png`).
- `tests/test_enlaces_docs.py`: cada enlace relativo y cada ancla del README y de `docs/` tiene que existir.
- `LICENSE` (MIT) para el código, [`DATA_LICENSE.md`](DATA_LICENSE.md) con la licencia de cada fuente y CC BY 4.0 para lo que añade el proyecto, `CITATION.cff`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` y plantillas de issues y PR.

### Cambiado

- **README** corto: qué es, descarga de la release (los 31 ZIP y `sha256sum -c`), fuentes, cobertura por comunidad, uso rápido con los avisos dentro del código (`es_ultima_version`, `avisos_para_cruce()` y el importe corregido), medir, reproducir, calidad, licencias y cómo citar. Recomienda clonar con `GIT_LFS_SKIP_SMUDGE=1` y bajar los datos de las releases.
- Los ejemplos de análisis del README antiguo sumaban sobre los ficheros de v2026.02 lo que no se debe sumar (todas las versiones de la PLACSP, filas del TED en vez de avisos, las copias de los menores de Catalunya, el `importe` inflado de Galicia): se han rehecho sobre la release, comprobados con los datos, o se han quitado.
- Contradicciones del README antiguo corregidas: años del CSV masivo del TED (2006-2023), cifras del BORME de v2026.10, 11 estructuras del Ayuntamiento de Madrid, 9 tablas de Euskadi, tramos de `idExpediente` en Andalucía y 21 indicadores de calidad (19 base + TED + BORME). Se quita la tabla de frecuencias de actualización, que estaba desfasada.
- Cataluña: `ccaa_cataluna_parquet.py` convierte cada versión a Parquet por trozos. Con las tres versiones de la PSCP, el pico de memoria baja de 15,2 a 3,6 GiB, con la misma salida fila a fila (#57).

### Eliminado

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
