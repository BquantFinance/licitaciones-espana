# Continuación del trabajo (instrucciones para la próxima sesión de Claude Code)

Rama: `claude/determined-albattani-5ze2p8` · PR: [#23](https://github.com/BquantFinance/licitaciones-espana/pull/23) (no hacer merge hasta cerrar lo pendiente) · Presupuesto previsto: ~250 USD.

## 0. Prompt para pegar al empezar

> Continúa el trabajo de la PR #23 de BquantFinance/licitaciones-espana en la rama `claude/determined-albattani-5ze2p8`. Lee primero `docs/CONTINUACION.md`, `docs/COBERTURA.md` y `comun/historico.py`, y sigue el plan de la sección 3 en orden. Reglas innegociables en la sección 2. Cada bloque se cierra con doble verificación (sección 4) y se commitea y sube al terminarlo, sin esperar al final. Pon ultracode / usa workflows con verificación adversarial. Si tienes acceso de red a los portales oficiales, prioriza la sección 3.3 (verificación en vivo y regeneración de datos).

## 1. Estado

**Hecho y verificado** (commits hasta `0bfd7d1`):
- PLACSP (issue #6):
  - Semántica de importes: `valor_estimado_contrato` / `importe_sin_iva` / `importe_con_iva`.
  - Etiquetas de código corregidas, CPV como texto y `fecha_publicacion` del anuncio de licitación.
  - Todas las entradas se sirven con `n_versiones` / `es_ultima_version`.
  - `normalizar_placsp.py` corrige los parquet publicados sin re-descargar. Verificado con las 8.693.891 filas reales.
- TED: los 7 tipos de anuncio de adjudicación y el CSV 2020-2023. BORME: parser, API de sumarios y anonimización solo de nombres de personas.
- Euskadi: filas repetidas marcadas en `_duplicado` y recolocación de 2021 con `_columnas_corridas`. Verificado celda a celda contra los ficheros originales del commit 93fb9e6.
- Catalunya, Valencia, Andalucía, Asturias, Galicia y Madrid: completitud de descargas y conservación de datos (ver los mensajes de commit).
- `comun/historico.py`: módulo contra el sesgo del superviviente, con tests.
- README (estado real de cada fuente) y `docs/COBERTURA.md` (plan de plataformas por CCAA).
- Comprobado: los 519 objetos LFS del repo (6,77 GB) existen en GitHub con el tamaño correcto, y la PR no modifica ningún fichero de datos.

**A medias (WIP, sin verificar)**, commit `757d5eb` y el commit final de la sesión anterior (ver su mensaje: dice qué quedó en verde):
- `nacional/`: entradas CPM (el conjunto `consultas` salía vacío), `_borrados`, lectura de todas las versiones de cada ZIP y campos CODICE extra.
- `Euskadi/`: descarga completa de la API `/contracts`, con importes y adjudicatario de 655K contratos (hoy solo hay metadatos de anuncios).
- Scrapers nuevos con tests simulados: `scripts/ccaa_castilla_leon.py`, `ccaa_murcia.py` y `ccaa_aragon.py`.

## 2. Reglas innegociables

1. **Servir exactamente lo que publica la administración.** No limpiar valores ni eliminar filas de origen, aunque estén duplicadas: se marcan con columnas `_...`. Solo se descartan los artefactos de nuestra propia descarga, como consultas solapadas, y siempre documentado. El objetivo del proyecto es enseñar cómo publica la administración.
2. **Nunca perder datos.**
   - Leer como texto (`dtype=str`) cuando haya duda.
   - Conservar todas las columnas.
   - Ningún valor pasa a NaN en silencio.
   - Nada de `on_bad_lines='skip'` sin guardar lo descartado.
3. **Sesgo del superviviente: nunca re-descargar y machacar a ciegas; siempre completar lo que falta.** Se aplica con `comun/historico.py` (tres niveles, más la semilla del punto 4):
   - **Capa cruda:** toda escritura de una descarga que pueda existir pasa por `guardar_version`; la versión anterior va a `_historico/`.
   - **Salidas:** se construyen con el código actual desde **todas** las versiones de los crudos (`versiones()`), usando `acumular(anterior, nuevos, fecha, ambito=...)`. Los registros retirados o modificados se conservan con `_en_ultima_descarga=False`. Una descarga vacía o fallida no retira nada.
   - **Re-ejecuciones:** incrementales y reanudables.
4. **El release v2026.02 y el LFS del repo son la única copia histórica**, porque el propietario ya no tiene los datos en local.
   - No modificar ni borrar ficheros de datos.
   - Cada scraper debe aceptar `--semilla <parquet publicado>`. Añade, **por clave estable**, solo las claves que no aparecen en la nueva descarga, marcadas con `_origen='release v2026.02'` y `_en_ultima_descarga=False`. Nunca modifica ni duplica filas nuevas. Hay que documentar los errores conocidos del publicado de esa fuente (ver README).
   - Los parquet publicados se descargan de `https://media.githubusercontent.com/media/BquantFinance/licitaciones-espana/main/<ruta>`.
5. **Tests con pandas 3 y pandas 2.2**, en un venv aparte (`pip install "pandas<3"`). La suite completa en verde antes de cada commit.
6. **Git.**
   - No usar `git stash`, `checkout` ni `reset` con agentes trabajando en el mismo árbol.
   - Los agentes no commitean; commitea la sesión principal.
   - Commit y push al cerrar cada bloque.
   - `ted/*.py` y `scripts/ccaa_asturias.py` usan CRLF: hay que conservarlo.
7. **Verificar dos veces con datos reales siempre que se pueda.** El patrón probado para no perder nada:
   - Comparar, fichero a fichero, el número de filas y de celdas con valor entre el original y la salida.
   - Clasificar los valores por tipo (URL, fecha, id, texto) en cada columna para detectar columnas corridas.

## 3. Plan por prioridad

### 3.1 Cerrar el WIP (primero)
0. **Estado al cerrar la sesión anterior.**
   - Suite completa: 511 passed con pandas 3 y con 2.2.
   - `nacional/` ya tiene una revisión adversarial con 4 arreglos, cada uno con su test (`TestRevisionAdversarial`).
   - Hay que confirmarlo con el propietario: `n_versiones` pasa a contar versiones distintas (pares id / fecha_updated), y `entrada_repetida` marca las copias.
   - Las filas CPM (`consultas`) no se han verificado con datos reales.
1. **`nacional/`.**
   - Suite en verde y revisión adversarial del diff desde `0bfd7d1`.
   - Hallazgos de la auditoría que hay que cubrir:
     - P1: entradas CPM → `conjunto='consultas'`.
     - P2: `at:deleted-entry` → tabla `_borrados`.
     - P3: refrescar los ZIP anuales del año en curso con `guardar_version`.
     - P4: anual primero y, si da 404, mensuales, sin leer los dos a la vez.
     - P5/P6: informe de 404 y de entradas descartadas.
     - P9: `--anos` por defecto 2012 → año actual.
     - P11: `entrada_repetida` para (id, fecha_updated) publicadas dos veces (unas 86K filas), con `n_versiones` sobre versiones distintas.
     - P7: todos los `WinningParty` (UTE), `_lotes`, `_modificaciones`, criterios de adjudicación, `OverThresholdIndicator` (SARA), contadores de ofertas, `Contract/ID` e `IssueDate`, y enlaces a pliegos.
   - No cambiar las columnas existentes.
2. **`Euskadi/`: API `/contracts` y `/contracting-notices`.**
   - Parámetros según código de terceros de 2025-26: `currentPage`, `itemsOfPage=50`, `orderBy`, `orderType`, filtros `award-date.gt/.lt` y `publication-date.gt/.lt`.
   - Ventanas mensuales, comprobando en cada una que el nº de ids únicos coincide con `totalItems`.
   - Los tests de consolidación existentes deben seguir pasando: la lógica de `_duplicado` y la recolocación de 2021 ya están verificadas con datos reales.
3. **CCAA nuevas escritas** (Castilla y León, Murcia, Aragón). Ya revisados contra sobrescrituras, descargas vacías y valores convertidos en NaN, con los tests en verde. Pendiente:
   - Instalar `xlrd` y probar los `.xls` (hay un test de Murcia que se salta).
   - Aplicar a Castilla y León los dos arreglos de lectura de Excel de Murcia.
   - Que Aragón lea los `.xls` sin convertir las celdas de error en NaN.
   - Evitar que un año del SMS con `.xlsx` y `.xls` a la vez entre dos veces.
   - Verificar en vivo las URLs y los ids de sus docstrings.

### 3.2 Sesgo del superviviente en los scrapers existentes
Hoy varios **sobrescriben al refrescar**. Hay que aplicar la regla 3, con la semilla del release, a cada uno:

| Scraper | Qué hay que cambiar |
|---|---|
| `ted/ted_module.py` | Cachés por año y refresco del año en curso |
| `borme/scripts/borme_scraper.py`, `borme_batch_parser.py` | Los PDF son inmutables. El parse completo no debe perder lo ya parseado si faltan PDF en disco |
| `scripts/ccaa_valencia.py`, `ccaa_valencia_parquet.py` | `recurso_actualizado` y la reconversión sobrescriben |
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | `VIGENCIA_HORAS` sobrescribe los CSV. Las versiones de un mismo CSV no son "consultas solapadas". Respetar las filas de continuación |
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | `_sigue_cambiando` sobrescribe |
| `scripts/ccaa_asturias.py` | CSV anuales |
| `galicia/scraper_galicia.py` | Caché SQLite y merge final |
| `scripts/ccaa_andalucia.py` | El re-scrape completo sobrescribe la salida |
| `scripts/ccaa_cataluna_contratosmenores.py` | Acumular por registro. Al sembrar, quitar solo las 2,16M copias idénticas del publicado |
| `scripts/ccaa_cataluna.py`, `ccaa_cataluna_parquet.py` | Re-descarga por `rowsUpdatedAt` / `last_modified`. El RPC y `qjue-2pk9` son ventanas móviles de 5 años: lo que sale de la ventana debe conservarse |
| `Euskadi/ccaa_euskadi.py` | Refresco de los ficheros que "siguen cambiando" |

Un workflow razonable, ya probado en la sesión anterior aunque se paró por cuota:
- Una unidad por scraper, cada una solo con sus ficheros.
- Implementación, luego dos verificadores en paralelo:
  - Lente "re-ejecución y pérdidas": registro retirado o modificado, descarga vacía o interrumpida, `--resume`, semilla aplicada dos veces, ámbito mal elegido.
  - Lente "regresión y fidelidad": con una sola descarga la salida es idéntica más las 3 columnas meta, y los tests de mutación fallan si se quita la lógica.
- Corrección y nueva verificación hasta que no queden bloqueantes.

### 3.3 Verificación en vivo y regeneración de datos (si la sesión tiene red)
Desde la nube de Claude Code los portales oficiales devolvían 403 del proxy. En una máquina con acceso:
1. Ejecutar las comprobaciones de `docs/COBERTURA.md` §4:
   - URL del CSV de TED 2020-2023 y nombres de campo de la API.
   - API de sumarios del BORME.
   - Parámetros de la API de Euskadi.
   - Partición por mes del Elasticsearch de Andalucía (unos 41K menores del SAS por encima del límite de 10K).
   - Filtro de fechas de los menores de Catalunya (ICS, UPF, UAB).
   - Menores de Madrid sin entidad.
   - URLs de los scrapers nuevos.
2. Regenerar con `--semilla` en este orden:
   1. PLACSP. Primero `normalizar_placsp.py` sobre el publicado, que no necesita red. Después los ZIP completos para recuperar `importe_sin_iva` real y `fecha_publicacion`.
   2. TED.
   3. Galicia (importes ×10/×100).
   4. Asturias.
   5. Euskadi.
   6. Catalunya.
   7. Valencia.
   8. Madrid Comunidad.
   9. **Madrid Ayuntamiento**: no hay copia; el ZIP del release es una copia de la Comunidad.
   10. Andalucía.
   11. BORME: volver a descargar 2012-09-07/11, 2013-01-02 y 2024-05-09/10.
3. Volver a calcular calidad y el cruce PLACSP↔TED. Actualizar las cifras del README (hoy son las de v2026.02).
4. Publicar un release nuevo sin borrar v2026.02.

### 3.4 Huecos de fuentes en CCAA ya cubiertas (detalle en `docs/COBERTURA.md` §2)
- **Euskadi.**
  - REVASCON por poder y año: `contratos_poder{ID}_{AÑO}`, 2018-2026. Los IDs de poder se sacan del catálogo, no de la API.
  - Vitoria: "Contratos formalizados" y "menores formalizados".
  - OpenDataBizkaia y Gipuzkoa.
  - B3 `ultimas_contrataciones_admin`.
  - Bilbao sin filtros.
- **Catalunya.**
  - Socrata `qjue-2pk9`, con importes en céntimos que no hay que convertir.
  - AOC: RPC local e histórico.
  - Barcelona: `prorrogues-de-contractes`.
  - Comprobar si existen `ydq4-xy5b`, `jxvs-kzbu`, `w2cu-rmuv`, `wwmk-zys7` y `nuym-4erw`.
- **Andalucía.** CSV oficial de menores de la Junta (CKAN, separador `|`, con NIF y nombre del adjudicatario) y "Licitaciones publicadas {año}".
- **Asturias.** Menores 2016-2018 (datos.gob.es) y relaciones trimestrales de menores 2024-2025.
- **Madrid.** Menores de entidades históricas que ya no salen en el desplegable.
- **Galicia.** Paginar por id y publicar el detalle.

### 3.5 CCAA nuevas pendientes (detalle y URLs en `docs/COBERTURA.md` §3)
1. Navarra: CKAN Registro de Contratos 2007+ e instantáneas del año en curso.
2. La Rioja: opd-179 y menores por año.
3. Castilla-La Mancha: XLS trimestrales de menores.
4. Extremadura.
5. Canarias.
6. Cantabria.
7. Illes Balears: solo lo que no está en PLACSP.
8. Melilla.

Antes de todo esto, el "paso 0": una vista regional de PLACSP por DIR3, NIF y host (§1 de COBERTURA) para medir lo que ya tenemos.

### 3.6 Cierre
- README: actualizar las secciones de las CCAA nuevas, el uso de `--semilla` y `_historico/`.
- `requirements.txt`: añadir `odfpy` y `xlrd` si hacen falta.
- Reescribir la descripción de la PR y hacer merge cuando todo esté verificado.
- Issues relacionados: #2, #6 y #20, que se cierran con la PR.

## 4. Protocolo de verificación de cada bloque

1. Tests del bloque y suite completa con pandas 3 y 2.2: todo en verde.
2. Dos revisiones adversariales independientes, con las lentes de la sección 3.2. Un hallazgo es bloqueante solo si hay un escenario concreto reproducido.
3. Si hay datos reales o publicados disponibles:
   - Filas y celdas conservadas, fichero a fichero.
   - Tipos por columna.
   - Sumas con `es_ultima_version` / `_en_ultima_descarga` donde toque.
4. Commit con un mensaje que diga qué se verificó y cómo, y push.
