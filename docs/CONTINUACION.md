# Continuación del trabajo (instrucciones para la próxima sesión de Claude Code)

## Estado al 2026-09-29 (sesión del VPS)

**Fusionado en `main`:**
- #30 TED, regla 2;
- #31 Catalunya y Valencia por categorías, y semilla de Catalunya;
- #32 Murcia en CP850;
- #33 regla 3 en 7 scrapers;
- #34 semilla de la PSCP por uuid;
- #35 crones en `despliegue/vps`;
- #36 Galicia: la semilla conserva los organismos retirados enteros;
- #37 menores de la Junta de Andalucía desde el CKAN, con el SAS entero: 768.647 filas y +79.721 que no teníamos.

**VPS, primeras descargas verificadas:**
- Sin semilla: asturias, ayto_madrid, aragon, castilla_leon, murcia, extremadura, la_rioja, valencia_menores, castilla_la_mancha y valencia.
- ted: 1.029.593 filas.
- catalunya: semillas +751.187 en el RPC, +85.397 en la PSCP, +9.047 y +5.095.
- nacional: 9.719.224 entradas y 0 errores; la semilla no añade ninguna fila.
- municipios: rc=1 por dos errores de origen permanentes (ver abajo).

**VPS, crones semanales (01:00; el cerrojo global los pone en fila):**
- lunes: nacional;
- martes: catalunya y valencia;
- miércoles: asturias;
- viernes: ayto_madrid, aragon, castilla_leon, murcia, extremadura, la_rioja, valencia_menores y castilla_la_mancha;
- sábado: ted.
- La cola de las 00:30 sigue con andalucia, comunidad_madrid, galicia, catalunya_menores, borme, euskadi y andalucia_menores.

**Web (repo privado BquantFinance/buscalicitaciones, PR #1 fusionada):**
- ETL v2 (`etl/v2/`, decisiones en `docs/etl_v2/grupo1..5.md`): 59 tablas, 32,3 M filas, 24,7 M a la web y 0 errores.
  - PLACSP por resultado de lote, con el presupuesto del lote para las correcciones: cubre las «reglas de escala por lote» del punto 3 de abajo en la web.
  - TED, Catalunya, 11 fuentes nuevas.
  - Fase 1: las fuentes sin descarga nueva, desde la web actual.
- Despliegue azul/verde preparado; el cambio espera el OK del propietario.

**Siguiente, por orden (sustituye a la lista de abajo donde choquen):**
1. Cambio de la web (azul/verde) con el OK del propietario.
2. Fase 2 del ETL según terminen las descargas: Andalucía (buscador + menores del CKAN, deduplicando por id y nº de expediente), Comunidad de Madrid, Galicia, Euskadi y menores de Catalunya.
3. Arreglos de scrapers medidos por el ETL:
   - **TED:** el parser de la API rellena por posición (159.677 copias) y falta el título; hay que leer resultado de lote → oferta → ganador.
   - **Murcia:** `escapechar='\\'` (38 filas corridas), `\n` literal y restos de JSON.
   - **Aragón:** se pierde la 1.ª fila de cada .xls.
   - **Municipios:** errores de origen permanentes (Leganés da HTML en agosto de 2026 y Málaga 404 en 4T-2020) que no deben dar rc=1 cada semana, y Valladolid sin cabecera.
   - **Barcelona:** la consolidación no acumula versiones y lee CP1252 como latin-1.
   - **PLACSP:** nombre de fichero fijo; hoy lleva el año (`licitaciones_completo_2012_<año>`) y el ETL lo cita.
4. Menores alcanzables desde el VPS (§3.6.8; medido el 28-sep: responden todos salvo dpz.es):
   - Asturias 2016-2018 y 2024-2026;
   - Cantabria;
   - A Coruña, Oviedo, Avilés, Palma y Zaragoza;
   - Navarra (CKAN, 2007+).
5. Matriz de cobertura de menores por CCAA (canal API/CSV/HTML/PLACSP, campos y hueco medido), plan de la web y, después, el modelo.

## Relevo del 2026-09-28: una sola sesión, la del VPS, lleva el VPS y GitHub

Desde el 2026-09-28 hay **una sola sesión de trabajo**: la de Claude Code en el VPS (Remote Control, `/opt/apps/licitaciones-vps` y `/opt/apps/licitaciones-espana`). Lleva a la vez el VPS y este repo. La sesión en la nube (claude.ai/code, rama `claude/continue-previous-process-wsi03z`) se cerró tras las PR #25 y #26 y no deja trabajo a medias. Así no hay dos sesiones tocando lo mismo.

**Estado al cerrar la nube.**
- `main` tiene todo: la PR #25 (`8eb1540`, menores y sesgo del superviviente) y la PR #26 (`df75b4a`, importes corregidos del issue #22), más este relevo.
- **Datos:**
  - Lo que se descargó en la nube solo vivía en su contenedor y se pierde: la PLACSP regenerada del 2026-09-27, la API completa de Euskadi, los menores de la Comunidad de Madrid y la calidad regenerada.
  - El VPS lo regenera con `despliegue/vps/` (PR #27).
  - Para contrastar sus cifras: `docs/REGENERACION_ISSUE_6.md` y §1.
- **Abierto en GitHub:**
  - **PR #27** (la de la sesión del VPS): operación de los scrapers. Es suya.
  - **PR #24** (@686f6c61): regeneración independiente de PLACSP y calidad. Sirve como contraste de la nuestra; decisión del propietario.
  - **PR #21** (@elCanosail, autor del issue #22): QA entre datasets y loader DuckDB. Decisión del propietario.
  - **PR #12 a #15** (@686f6c61, marzo-abril de 2026): refactors anteriores a las PR #25 y #26, probablemente en conflicto con `main`. Decisión del propietario.
  - **Issue #22:** respondido el 2026-09-28. Se puede cerrar cuando salga el release con las columnas corregidas.
  - **Issue #6** (Elicita): espera el release en borrador.

**Siguiente, por orden.**
1. **PLACSP → cruce TED → calidad** (`--solo-ultima-version`; ya incluye INT-FIA-12 y los importes corregidos).
   - Contrastar con `docs/REGENERACION_ISSUE_6.md`.
   - Release en borrador **v2026.09**, sin tocar v2026.02. Notas en `herramientas/sesion_2026_09_27/regeneracion_placsp/notas_release.md`, con el commit usado.
2. **Web y modelo** (issue #22):
   - Enseñar el importe publicado y el corregido con su motivo.
   - Rankings, cuotas y modelo con `<campo>_corregido`, filtrando `es_ultima_version`.
3. **Reglas de escala por lote.**
   - `_resultados` suma 2,26 millones de M€ en la última versión, frente a 525.800 M€ en la tabla principal, y solo recibe el registro.
   - Hay que unir el presupuesto de cada lote (`_lotes`) y aplicar las mismas reglas antes de usar los lotes en rankings por empresa.
4. **Resto de la cola de primeras descargas** (`despliegue/vps/cola_primera.txt`).
   - Andalucía y las fuentes bloqueadas desde la nube (§3.6.8) se verifican por primera vez desde el VPS.
5. **`--salida` en Comunidad de Madrid y TED** (lo pide la PR #27).
6. **Decisiones del propietario pendientes:**
   - §3.2: cargos repetidos del BORME, retención de `_historico/`, segmentación y semilla de la PSCP y huecos de TED (los avisos cancelados ya se conservan, `b8c6709`).
   - §3.1.4: identificadores truncados del Ayuntamiento de Madrid.
   - §1: los 4,8 M de menores que anuncia el portal de la Comunidad de Madrid frente a los 2,8 M descargados.

### Sincronización con el VPS (buscalicitaciones.com)

La web (buscador) y el modelo antifraude se alimentan desde el VPS con el código de este repo. Para que no haya desfase:
- **El código vive solo en GitHub `main`.**
  - Antes de cada ejecución en el VPS: `git pull` en la copia del repo que usa, y anotar `git rev-parse --short HEAD` en el registro de la ejecución y en las notas del release.
- **Un cambio de código hecho en el VPS se sube a GitHub** (rama y PR a `main`) el mismo día; nada se queda solo en el VPS.
  - Si desde el VPS no se puede subir, se deja el parche (`git diff > cambio.patch`) y se avisa al propietario.
- **Los datos no están en el repo**: los genera el VPS.
  - Un cambio en `calidad/` no obliga a volver a descargar nada. Basta con repetir la calidad sobre la PLACSP ya generada, o aplicar `calidad.correcciones.corregir_importes` al cargar.
- **Importes (issue #22):**
  - La web enseña el importe publicado y, al lado, el corregido con su motivo (`<campo>_corregido`, `correccion_<campo>`).
  - Los rankings, las cuotas y el modelo usan el corregido.
  - Ver README, «Importes publicados y corregidos».

## 0. Prompt para pegar al empezar

> Eres la única sesión de trabajo de BquantFinance/licitaciones-espana y del VPS de buscalicitaciones.com. Lee primero el «Relevo del 2026-09-28» y la «Sincronización con el VPS» de `docs/CONTINUACION.md`, y después `despliegue/vps/README.md`, `docs/COBERTURA.md` y `comun/historico.py`. Sigue la lista «Siguiente, por orden» del relevo. Reglas innegociables en la sección 2. Cada bloque se cierra con doble verificación (sección 4), en una rama y una PR a `main`, sin esperar al final. Usa revisores adversariales en paralelo (dos lentes por bloque). Nada de código se queda solo en el VPS.

## 1. Estado

**Entorno de la nube (lecciones del 2026-09-27).**
- El entorno necesita acceso de red *Full*, que el propietario activa en la configuración del entorno desde la web.
- Con ese acceso, `pypi.org` va por el proxy, pero sigue en `NO_PROXY` y la conexión directa da 403. Para instalar: `env -u NO_PROXY -u no_proxy pip install --proxy "$HTTPS_PROXY" ...`.
- Entornos de trabajo: `/home/user/venv3` (pandas 3.0.6) y `/home/user/venv22` (pandas 2.2.3). Instala también `xlrd`, `xlwt` y `odfpy`: sin `xlwt` se salta un test.
- La API de GitHub responde con el token que inyecta el proxy (`-H "Authorization: Bearer $GITHUB_TOKEN"`, permiso de escritura), pero **crear releases está bloqueado para este tipo de sesión** (403: "Creating, editing, or deleting releases is not permitted for this session type", comprobado el 2026-09-27). Tampoco hay `git lfs` en el contenedor. **Desde la nube no se pueden sacar datos a GitHub**: los datos descargados solo viven en el contenedor y se pierden al acabar la sesión. Los releases los tiene que crear el propietario desde su máquina, con los scripts de `herramientas/sesion_2026_09_27/`.
- Recursos: 4 CPU, 15 GB de RAM y ~30 GB de disco. Hay que borrar las salidas de pruebas: los ZIP de toda la PLACSP ocupan ~11 GB.

**Hecho y verificado.**
- Hasta `627008b`: las correcciones de la PR (ver la descripción de la PR y los mensajes de commit).
- `5c3a82e`, PLACSP por lotes y `--semilla` (el WIP de la sesión anterior, cerrado):
  - Dos revisiones adversariales con 9 arreglos, cada uno con su test (`TestSegundaRevision`).
  - Comprobado con los ZIP reales de hoy frente al código anterior, celda a celda: consultas y encargos 2022-2026, licitaciones 2012 y agregación 2025 (250.652 entradas, pico de 1,2 GB frente a 2,7 GB).
  - Comprobado con la semilla real v2026.02, simulando además entradas retiradas: se recuperan exactamente las que faltan.
  - Novedades: la tabla `_semilla_contenido` (filas sin fecha del publicado que casan por contenido), la semilla en dos fases y el rechazo de sembrar desde una tabla de salida.
- `0263faf`, comillas literales (`comun/lectura_csv.py`):
  - En Castilla y León, 5 títulos que empiezan por comilla se tragaban 113 contratos menores. Lo usan CyL y Murcia.
  - Verificado en vivo: CyL, Murcia y Aragón (ver §3.1.3).
- `d2f0a1a`: `xlrd` y `odfpy` en `requirements.txt`.
- Suite completa: 631 passed con pandas 3.0.6 y con 2.2.3.

**Ayuntamiento de Madrid (el WIP de `a07b0d3`).**
- Con una sola descarga está verificado con los datos reales:
  - 169 recursos del CKAN y 82 ficheros consolidados.
  - Tabla fiel: 132.087 filas; en los 82 ficheros cuadran filas, celdas con valor y multiconjunto de textos.
  - Una segunda ejecución no cambia nada.
  - 79 de 81 CSV coinciden con el código anterior; las 2 diferencias son correcciones.
- La revisión de re-ejecuciones encontró duplicados sin marcar al retirarse un año, con fallos pasajeros o al retocar las descripciones: ver §3.1.4.

**Datos para Elicita (issue #6).** El propietario pidió regenerar la PLACSP y la calidad y publicarlas en un release en **borrador**, sin tocar v2026.02. Estado y cifras en §3.3.
- **Regeneración del 2026-09-27**: 45 ZIP (todos los actuales, 2012-2026) → 9.710.903 entradas de 5.218.753 licitaciones, 0 descartadas y 0 errores, en 59 min con un pico de 6,2 GB.
- La semilla v2026.02 no añade **ninguna** fila: 8.693.891 y 4.725.557 filas leídas, todas presentes (35.627 y 3.401 sin fecha, casadas por contenido). Todo lo publicado en v2026.02 sigue en los ZIP de hoy.
- Salida: principal 4,3 GB; resultados 0,9 GB; criterios 0,7 GB; adjudicatarios 0,4 GB; lotes 0,2 GB.
- **No se pudo subir** (releases bloqueados en la sesión): hay que repetirlo en la máquina del propietario con `herramientas/sesion_2026_09_27/regeneracion_placsp/`.
- **Resultados en `docs/REGENERACION_ISSUE_6.md`**: cruce TED, 20 indicadores, contraste por versión con `v2026.02` y con la PR #24, y hashes de los 45 ZIP.
  - Contraste con `v2026.02`: `importe_sin_iva` cambia en 4.314.328 de 8.494.308 versiones (50,8 %); el publicado es siempre el valor estimado. La PR #24 da 50,9 %.
  - TED: 153.110 de 257.637 SARA casados; 11.677 de 2026 sin evaluar. INT-CONS-20 = 37,8 %.
  - Calidad: score medio 93,3.

**Sesión del 2026-09-27 (noche).** Prioridad del propietario: *el 100 % de los contratos menores para un modelo antifraude* (§3.6 y `docs/COBERTURA.md` §0 y §5).
- Commits, todos con tests en pandas 3 y 2.2:
  - `8a1b8f2`, `ddaff3c` (Euskadi, API `/contracts`):
    - Un mes sin contratos llega sin `items` (`{totalItems: 0}`) y se tomaba por respuesta rota.
    - La API sirve algunas filas dos veces, idénticas (2020-01: 5.466 filas y 5.463 ids). Una pasada DESC completa distingue esas filas repetidas en origen de una paginación inestable. Antes se partía el mes hasta días sueltos y quedaba incompleto.
  - `9e945c2`: parche de @686f6c61 (PR #24) para TED y calidad:
    - `pd.NA` en `classify_buyer`, `normalize_name` y `clean_nif`.
    - Asignaciones escalares con `.at`.
    - Parquet de calidad en zstd.
  - `2b184a1`: INT-CONS-20 se une por versión (`id` + `fecha_updated`) y no por `expediente|nif`. Un SARA de un año sin avisos en el snapshot TED (2026) queda sin evaluar (`_ted_anio_cubierto`), no como missing.
  - `73d6e80`: Ayuntamiento de Madrid, bloque de re-ejecuciones cerrado (§3.1.4).
  - `8b4ccce`, `78d4f39` (Catalunya): los menores de la Generalitat se piden a `qjue-2pk9` (`ydq4-xy5b` da 404), y una descarga actualizada deja la anterior en `_historico/`.
  - `48e4d42` (Aragón): los enlaces `http://` se piden primero por `https://`; recupera las series de menores 2024-2025 del Gobierno.
  - `8791825` (Murcia): los menores del SMS se toman de los enlaces de la página de sector público. En vivo pasa de 155.085 filas (2020) a 748.984 (2019-2025), con NIF.
- **PR #24** (@686f6c61, externa): regenera la PLACSP y la calidad con el parser de `627008b` (sin los commits de esta rama), sobre el inventario de 78 ZIP de v2026.02 (hasta enero de 2026). Tiene un prerelease en su fork con hashes: 8.721.484 filas, 20 indicadores, verificación PASS.
  - Su parche de TED ya está aplicado aquí (`9e945c2`), y sus dos observaciones sobre CONS-20, corregidas en `2b184a1`.
  - Propuesta al propietario, pendiente de su decisión: usar su entrega como contraste independiente de la nuestra (filas, importes e indicadores sobre el mismo inventario) y no fusionar su cadena de scripts paralela.
- **Trabajo de la sesión del 2026-09-27/28.** Los datos descargados quedaron solo en el contenedor de la sesión y no se publicaron. Los scripts para repetirlo están en `herramientas/sesion_2026_09_27/`, con su README, y la puesta en producción se hace en el VPS:
  - **Euskadi `/contracts`: hecho el 2026-09-27**, pero solo en el contenedor, sin publicar.
    - 715.572 de 715.574 registros; faltan 2 sin fecha. La API sirve 217 filas dos veces; quedan 715.357 contratos.
    - **643.462 menores 2014-2026**, con el mismo recuento por año que `minor-contract=true`, CIF en el 100 % y 2.651,8 M€ con IVA. Detalle en `docs/COBERTURA.md` §5.4.
    - Para publicarlo, el propietario ejecuta `euskadi/contratos_api_tramo.py` por tramos y `euskadi/terminar.sh` (unas 2 h con 4 procesos). Las ventanas completas no se vuelven a bajar.
  - **PLACSP para Elicita** (§3.3): hecho en la sesión (`docs/REGENERACION_ISSUE_6.md`). Falta el release en borrador, que tiene que crear el propietario con `publicar_release.py`: desde la sesión da 403.
  - **Comunidad de Madrid: descarga hecha el 2026-09-28**, en el contenedor y sin publicar. Se hizo con una copia de `descarga_contratacion_comunidad_madrid_v1.py` fuera del repo (`python <copia> menores`: 126 entidades, subdividiendo por importe al llegar a 50.000 filas).
    - 343 CSV y 2.794.757 filas, 5.103 de ellas repetidas entre consultas. NIF en el 99,999 %.
    - Frente al publicado (2.563.527): 2015-2024 casi idénticos (difieren como mucho 135 filas al año; en 2020 hay 1 menos). 2025: 348.704 frente a 215.353. 2026: 127.001 nuevos.
    - Pendiente: el portal anuncia 4.832.623 en total. Hay que ver si la diferencia son otros tipos de publicación o menores sin entidad (§3.3).
    - La ruta `Entidad Adjudicadora` del publicado es la jerarquía completa (`Consejería de Sanidad··>SERMAS··>…>Hospital…`). Para comparar con las 125 entidades del desplegable hay que usar el primer nivel.
  - **Galicia**: la nueva descarga del listado de menores no terminó en la sesión. El portal tiene 1.775.090 frente a 1,64 M publicados, y a 100 filas por petición son unas 10 horas. Se hará en el VPS con el scraper ya cerrado: `python galicia/scraper_galicia.py base --skip-lic --output <datos>/galicia` y después `merge` con `--semilla`.
  - **Extremadura: cerrado** (`scripts/ccaa_extremadura.py`, 71 tests en los dos pandas, 39 mutaciones detectadas, verificado en vivo). Pendiente:
    - La serie 2016-2021 de la Intervención General da 404 en www.juntaex.es. Hay que verificar `instituciones.juntaex.es` desde otra red.
    - El solape con el 1143 está sin medir.
    - **Revisar `ccaa_murcia.py` y `ccaa_castilla_leon.py`**: usan el mismo bloque `_como_texto`. Con pandas 3, las columnas leídas del Parquet anterior son `str` y las del Excel `object`, así que cambian los metadatos sin que cambie ningún dato y se guarda una versión de más en `_historico/`. En Extremadura se corrigió escribiendo siempre `object`.
  - **La Rioja: cerrado** (`scripts/ccaa_la_rioja.py`, 29 tests en los dos pandas, 30 mutaciones detectadas, 350.967 filas en vivo).
  - **Valencia, menores fuera del REGCON: cerrado** (`scripts/ccaa_valencia_menores.py`, 34 tests en los dos pandas, 21 mutaciones detectadas).
    - En vivo, con las páginas en castellano: 1.425.763 filas de 6 fuentes. Hay otras 216.660 en las versiones que solo enlazan las páginas en valenciano (`*_va`), en su mayoría repetidas.
    - La UV publica ficheros acumulados que repiten filas: son unos 39.300 contratos distintos en 2024.
    - Pendiente: Elche, Castelló y la Diputació de València cortan la conexión desde la nube. Hay que verificarlo desde otra red.
  - **Castilla-La Mancha: cerrado** (`scripts/ccaa_castilla_la_mancha.py`, 48 tests en los dos pandas, unas 50 mutaciones detectadas, descarga completa en vivo: 555 MB y pico de 2,1 GB).
    - UCLM: 253.571 menores 2017-2026 con NIF. Junta: 14-50 mil al año. SESCAM: 4,16 M de líneas de factura sin NIF, que no son contratos (columna `_unidad`).
    - Solapes que se conservan tal cual: el anual de la Junta frente a sus trimestres (`_periodo`); 30.707 filas de la UCLM dentro de la Junta de 2023; ficheros repetidos en sector público.
    - En origen, el SESCAM de 2015 y del 2T de 2016 están cortados en 65.535 filas (el máximo de un .xls).
    - Para los RAR: `rarfile` con `unrar`, o `libarchive-c`; están en `requirements.txt`. Sin ellos, los RAR quedan pendientes y la ejecución sale con código 1.
  - **Municipios: cerrado** (`scripts/municipios_menores.py`: 29 tests en los dos pandas, 33 mutaciones detectadas). En vivo: 299 ficheros y 289.401 filas de 8 ayuntamientos.
    - Casi nada de esto está en el 1143. En 2025, Gijón tiene 8.157 menores en su portal frente a 24 en el 1143, y Fuenlabrada 1.352 frente a 0.
    - Fallos del portal, que se repiten en cada ejecución: Leganés sirve la portada en lugar del XLSX de agosto de 2026, y el XLSX de Málaga del 4T 2020 da 404.
    - Sin extraer, solo en PDF:
      - Córdoba: 35 PDF, 2025 incluido.
      - Santa Cruz de Tenerife: 2016-2022 y 2025.
      - Leganés: 19 PDF.
      - Valladolid: las fundaciones de 2024-2026.
    - El listado de Fuenlabrada pierde entradas al paginar por fecha. Por eso solo se retira un fichero si su URL da 404.
    - Valladolid publica ficheros acumulados: para contar, el último de cada año.
    - En Córdoba, 2021-2023 cuenta doble (CSV y XLS se solapan).
  - Los commits `WIP (copia de seguridad, sin revisar)` son copias automáticas del trabajo de los agentes. Lo que vale es lo que recoge el commit de cierre de cada scraper (tabla siguiente).
- **Issue #22 (2026-09-28, después de la PR #25): importes publicados y corregidos.**
  - URDINBERRI (entrada 15091104 de la agregación) publica 2.357.531.666 € de adjudicación para una obra de 2.518.819,27 €.
    - La plataforma de origen (Euskadi) da 2.593.284,83 € con IVA del 10 %: 2.357.531,67 € sin IVA, con la coma decimal perdida.
    - El valor estimado (25.188.819,27 €) tiene además un 8 repetido.
  - Lo publicado no se toca. `calidad/correcciones.py` añade `<campo>_corregido` y `correccion_<campo>` con dos fuentes de corrección:
    - el registro verificado `calidad/errores_fuente.csv`;
    - las reglas de escala.
  - Nuevo indicador INT-FIA-12. Cifras sobre la regeneración en `docs/REGENERACION_ISSUE_6.md` §5.
  - Pendiente del propietario: responder en el issue #22 (hay un borrador en la sesión).

### Estado de los scrapers (para el VPS y producción)

Solo se usan en producción los scrapers **cerrados**: revisados, con tests en pandas 3 y 2.2 y con el sesgo del superviviente cubierto. Para comprobar que no hay cambios sin revisar después del cierre, `git log --format='%h %s' <cierre>..HEAD -- <script>` no debe mostrar ningún commit `WIP`.

| Script | Estado | Commit de cierre | Verificado en vivo |
|---|---|---|---|
| `nacional/licitaciones.py`, `nacional/normalizar_placsp.py` | Cerrado | `de70485` | Sí (regeneración del 2026-09-27) |
| `calidad/calidad_licitaciones.py`, `calidad/correcciones.py` | Cerrado | `ba5a46e` | Sí (regeneración del 2026-09-27; URDINBERRI contra la API de Euskadi) |
| `ted/ted_module.py`, `ted/run_ted_crossvalidation.py` | Cerrado | `b8c6709` | Sí (y el lector nuevo del CSV, con los CSV reales de 2019 y 2021) |
| `scripts/ccaa_cataluna.py`, `scripts/ccaa_cataluna_parquet.py` | Cerrado (`--salida`, `--entrada`, `--categorias`, `--semilla`) | `511e88e` | Sí (la semilla, con la primera descarga del VPS; Barcelona: versiones, CP1252 y semilla del perfil, con los crudos del VPS del 29-sep) |
| `scripts/ccaa_valencia.py`, `scripts/ccaa_valencia_parquet.py` | Cerrado (`--salida`, `--entrada`, `--categorias`) | `95815b3` | Sí (primera descarga del VPS, 2026-09-28) |
| `Euskadi/ccaa_euskadi.py`, `Euskadi/consolidacion_euskadi.py` | Cerrado (`--salida`, `--entrada`) | `7953621` | Sí (API completa; con `--salida` el log va a la carpeta de salida, comprobado en el VPS) |
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | Cerrado | `73d6e80` | Sí |
| `scripts/ccaa_murcia.py` | Cerrado | `83e49b2` | Sí (codificación cp850 de contratosOD 2014-2018, con los ficheros del VPS) |
| `scripts/ccaa_aragon.py` | Cerrado | `48e4d42` | Sí |
| `scripts/ccaa_castilla_leon.py` | Cerrado | `0263faf` | Sí |
| `scripts/ccaa_extremadura.py` | Cerrado | `4163d85` | Sí |
| `scripts/ccaa_la_rioja.py`, `scripts/ccaa_valencia_menores.py` | Cerrado | `9190268` | Sí |
| `scripts/ccaa_castilla_la_mancha.py` | Cerrado | `d015194` | Sí |
| `scripts/municipios_menores.py` | Cerrado | `1361abd` | Sí |
| `scripts/ccaa_asturias.py` | Cerrado | `aefb659` | Sí, desde el VPS (2026-09-28): 2019-2024, 375.380 filas; la semilla no añade ninguna; 2025 y 2026 dan 404 en `dataset-contratacion-centralizada-<año>.csv` |
| `scripts/ccaa_andalucia.py` | Cerrado | `a367943` | **No**: el portal corta desde la nube |
| `scripts/ccaa_andalucia_menores.py` | Cerrado | `6f2f73c` | Sí, desde el VPS (2026-09-29): 9 CSV del CKAN de la Junta (2018-2026), 768.647 registros, 544.898 del SAS; la salida es idéntica al original celda a celda |
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | Cerrado | `699bf16` | Sí (descarga completa del 2026-09-28) |
| `galicia/scraper_galicia.py` | Cerrado | `699bf16` | Sí (4 organismos) |
| `scripts/ccaa_cataluna_contratosmenores.py` | Cerrado (sin segmentación por fecha: los órganos grandes quedan fuera del ámbito) | `1fa7f01` | Sí (dos fases) |
| `borme/scripts/*.py` | Cerrado | `38e72aa` | Sí (boe.es) |

Avisos de la sesión del VPS (2026-09-28):
- **Galicia:** segfault dentro de `to_numeric` (`csv_to_parquet`). Aquí no se reproduce (pandas 2.2.3, numpy 2.4.6, pyarrow 25.0.1). Ese código es anterior a esta sesión.
  - **Versiones del VPS** (medidas el 2026-09-28): el fallo era con **pandas 2.3.3**, numpy 2.5.3 y pyarrow 25.0.1, no con 2.2.3; la imagen se había construido con `pandas<3`.
  - En `db64717`, `tests/test_galicia.py` da segfault con 2.3.3 (código de salida 139) y pasa con 2.2.3.
  - En `main` (`8eb1540`) pasa 3 de 3 con las dos versiones: el arreglo de `csv_to_parquet` lo resuelve.
  - Sigue sin saberse qué valor lo dispara.
- **`test_ayto_a_corrupt_manifest_is_recovered_from_its_history`:** intermitente si dos ejecuciones caen en el mismo segundo, porque el manifiesto sale idéntico. El código es correcto; el test se hace determinista.

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
1. **`nacional/`: HECHO** (`5c3a82e`; ver §1).
   - Pendiente de confirmar con el propietario: `n_versiones` cuenta versiones distintas (pares id / fecha_updated) y `entrada_repetida` marca las copias.
   - Menores de la revisión que quedan:
     - Una interrupción durante `_publicar` puede dejar tablas de ejecuciones distintas: hay que calcular los hashes antes y hacer después solo los `os.replace`.
     - `--conjunto X` sustituye la salida de todos los conjuntos. La anterior queda en `_historico/` con aviso.
     - Instantes válidos del borde (`2262-04-11T20:00Z`) quedan nulos.
     - Formato CSV de fechas y números cuando se mezclan semillas y descarga en una columna de texto.
     - Mutantes que sobreviven: M17, M18, M19 y M08 (ver el informe del revisor de fidelidad en el mensaje de `5c3a82e`).
2. **`Euskadi/`: API `/contracts` y `/contracting-notices`.**
   - **Hecho y con tests:**
     - Descarga por ventanas de fecha que cuadra con `totalItems`. Aborta si la API repite página.
     - Re-ejecuciones sin machacar.
     - Consolidaciones A1, A2, B4 y C2.
   - **Verificado en vivo** (COBERTURA §4.1): paginación con `currentPage`, `itemsOfPage` de 50 como máximo, sin tope de 10.000, y filtros `.gt` estricto y `.lt` inclusivo.
   - **Pendiente:**
     - Terminar la descarga completa real (715.574 contratos a 50 por página, unas 14.300 peticiones), en curso el 2026-09-27 (§1). Arreglados los meses vacíos (`8a1b8f2`) y las filas repetidas por la API (`ddaff3c`).
     - Las URLs de Vitoria y Bilbao sin filtros.
3. **CCAA nuevas: verificadas en vivo el 2026-09-27** (`0263faf`).
   - **Castilla y León**: 16 conjuntos en 50 s.
     - Ids conocidos corregidos según el catálogo.
     - Los 113 contratos menores que tragaban las comillas literales se recuperan.
     - El aviso "No existen datos asociados" del histórico ya no se convierte en tabla.
     - `licitacion-de-obras-publicas` es una estadística agregada con una fila `</HTML>`, basura del portal: excluirla del descubrimiento o dejarla documentada.
   - **Murcia**: 15.399 contratos, 244.185 menores CARM y 748.984 líneas de menores del SMS 2019-2025 (`8791825`; antes solo 2020, con 155.085).
     - Los `contratosOD2019`-`2023.csv` tienen entre 3 y 10 filas con campos de más, que van a `_columna_extra_N` (columnas corridas por separadores sin comillas).
   - **Aragón**: 9 tablas (106.146 contratos y 84.436 menores del Gobierno, Registro de Contratos, encargos, anuncios).
     - ~~Los 4 `.xls` de 2024-2025 del Gobierno dan HTTP 403~~: el CKAN los da con `http://` y el proxy rechaza HTTP plano. Arreglado en `48e4d42` (primero `https://`): 3.139 filas de 2025.
     - Revisar los ZIP "Contratos del Sector Público de Aragón 2014/2015", que se omiten como no tabulares.
   - **Pendiente:**
     - Aplicar a Castilla y León los dos arreglos de lectura de Excel de Murcia.
     - Que Aragón lea los `.xls` sin convertir las celdas de error en NaN.
     - ~~Evitar que un año del SMS con `.xlsx` y `.xls` a la vez entre dos veces~~: el SMS ya no va por plantilla de año (`8791825`).
     - Revisión adversarial independiente de `comun/lectura_csv.py`: hasta ahora solo tiene tests, una prueba con 300 CSV aleatorios y el fichero real.
4. **Ayuntamiento de Madrid: HECHO** (`73d6e80`: 135 tests con pandas 3 y 2.2, y tablas idénticas byte a byte con `--solo-procesar` sobre la ejecución real). Queda la decisión de los identificadores truncados (abajo). Historia del bloque:
   - **Verificado con los datos reales** (una descarga completa, 169 recursos):
     - Tabla fiel y unificada: 132.087 filas cada una.
     - Por fichero cuadran filas, celdas con valor y multiconjunto de textos; en total, 2.143.400 celdas con valor, sin ningún error de posición.
     - La segunda ejecución da `sin_cambios`.
     - 79 de 81 CSV son idénticos al código anterior. En los otros dos, el nuevo recupera 43 registros que `on_bad_lines='skip'` descartaba y lee bien la cabecera de `modificados_2021`.
   - **Revisión de re-ejecuciones**, con arreglos en curso al escribir esto (ver el commit que cierre el bloque):
     - **BLOQUEANTE:** si el portal retira un año entero (CSV y XLSX), el XLSX se consolida y cada contrato retirado sale dos veces sin marca.
     - Si un CSV falta en un listado, o tiene un fallo pasajero (503, o solo cabecera a principios de año), su XLSX gemelo queda consolidado para siempre y duplica las filas.
     - Un retoque cosmético de la descripción en CKAN retira ficheros idénticos.
     - Menores: la descarga no se compara con el `size` de CKAN, la comprobación de "menos filas" solo avisa una vez y un manifiesto corrupto aborta la ejecución.
   - **Revisión de fidelidad:**
     - `acuerdo_marco_2025` trae 21 campos y solo 20 nombres de cabecera. El contratante queda corrido y el promotor está en `Unnamed: 20` (3.420 valores); pasaba igual con el código anterior.
     - Hacía falta un test de "cada fila se mapea con la cabecera de su versión".
     - `importe_excel` no distingue una celda de texto de una numérica.
   - **Decisión del propietario: identificadores truncados.**
     - El CSV publicado trae `N. DE EXPEDIENTE` y NIF en notación científica de Excel (`1,45202E+11`) en unas 3.083 celdas, sobre todo en menores 2021-2024. El XLSX del mismo recurso trae el valor completo (`145202100418`) y se guarda en la capa cruda, pero no llega a las tablas.
     - Propuesta: una columna `<col>_xlsx` en esas filas.

### 3.2 Sesgo del superviviente en los scrapers existentes
Hoy varios **sobrescriben al refrescar**. Hay que aplicar la regla 3, con la semilla del release, a cada uno.

**Estado a 2026-09-28** (comprobado con `grep` de `guardar_version`, `acumular` y `_en_ultima_descarga`):
- **Cubiertos:** PLACSP (`--semilla` y `_borrados`), Catalunya RPC y Generalitat, Valencia REGCON, Ayuntamiento de Madrid, Aragón, Castilla y León, Murcia, la tabla de la API de Euskadi, y los scrapers nuevos (Extremadura, La Rioja, menores valencianos, Castilla-La Mancha, municipios y menores de la Junta de Andalucía del CKAN).
- **Todas las fuentes están cubiertas desde el 2026-09-28.** Pendientes de las dos últimas:
  - **Comunidad de Madrid:**
    - **El histórico crudo crece mucho.** Los menores cambian a diario y cada ejecución guarda casi 1 GB en `_historico/`. Opciones: comprimir, refrescar con menos frecuencia o consolidar de forma incremental, como en Extremadura.
    - **Entidades que desaparecen del desplegable:** hoy sus menores quedan con `False`. La alternativa es dejarlos fuera del ámbito. Decisión del propietario.
    - **Primera ejecución:** usar como primera versión de `csv_originales/` los CSV del ZIP del release, que conservan las filas de continuación.
    - El modo `prueba` usa la entidad "38", que ya no es el Gregorio Marañón.
  - **Galicia:**
    - ~~**Organismos que el portal quita enteros:** sus filas de la semilla no se añaden nunca~~: **hecho** (`4bb101f`, decisión 4 del propietario). Se añaden, porque son la única copia, marcadas como las demás de la semilla (`_origen`, `_en_ultima_descarga=False`).
      - `base` guarda en el manifiesto la lista de organismos que lee `discover()` (`descubrimientos`: fecha, ids probados y CM/LIC que declara cada organismo). Es completa porque `discover()` para ante cualquier sonda fallida. Cada `--resume` añade la suya; con `--organismo` no hay lista.
      - En `merge`, un organismo de la semilla está retirado si su id se ha probado en todas las listas de la descarga, no está en ninguna y la descarga no lo ha leído. Uno que está en la lista y no se ha leído (un corte, un `--resume` a medias) no lo está: sus filas siguen fuera del ámbito.
      - No se da por retirado ninguno, con aviso en el log:
        - si la descarga no tiene lista (`--organismo` o una descarga del código anterior);
        - si una lista no trae ningún organismo con CM o ninguno con LIC (una sonda ha respondido vacío para todos);
        - si los organismos que añadirían filas son más de `--max-organismos-retirados` (20; con 0, nunca). El aviso los lista para revisarlo y repetir `merge` con un máximo mayor.
      - El informe de la semilla da las filas añadidas por organismo retirado, y el resumen del `merge`, el total.
      - **Primera descarga del VPS:** si se hace con el código anterior, no tiene lista y no añade ninguno. Entran al repetir `merge --semilla` tras una descarga `base` sin `--organismo` con este código, o tras `base --resume` sobre aquella: solo lee la lista (4.000 sondas: los ids 1-2000 en LIC y en CM) y no vuelve a pedir los organismos ya leídos.
      - Medido sin red con la semilla real, quitando de la descarga los organismos 283, 441 y 54: se añaden exactamente sus 44.830 filas, sin claves repetidas. Sin lista, ninguna.
      - Riesgo que queda: un organismo al que las dos sondas respondan vacío por error en todas las listas de la descarga se da por retirado. Si vuelve, sus filas de la descarga entran como altas junto a las de la semilla (el mismo contrato dos veces, una con `_origen`).
      - Pendiente de decidir: las filas de nuestras propias descargas de un organismo que el portal retira después siguen con `_en_ultima_descarga=True` (quedan fuera del ámbito), y un tipo (CM o LIC) que se queda a cero en un organismo que sigue en la lista no cuenta como retirado.
    - El importe de la semilla va a `importe_semilla`, sin corregir el ×10/×100.
    - `csv_to_parquet` ya no pasa a `to_numeric` textos que no son números normales. Algunos hashes parecen notación científica, y es la causa probable del segfault del VPS en pandas 2.2 (sin confirmar).
    - `csv_to_parquet` convierte en nulo el texto literal `NA`/`null` (código anterior; el publicado no tiene ninguno).
  - **`_ultima_descarga`** es la fecha de la última versión que trae la fila, no la de la última comprobación. Así una re-ejecución idéntica no reescribe la salida. Es igual en TED, Comunidad de Madrid y Galicia.
- **Catalunya (Socrata): semilla del release** (`95815b3`, 2026-09-28). `ccaa_cataluna_parquet.py` no la admitía.
  - Lo que la ventana móvil sacó antes de la primera descarga del VPS solo estaba en el release.
  - Medido: RPC +751.187 filas (2021, sobre todo), PSCP +85.397, fase de ejecución +9.047 y contratación programada +5.095.
  - La clave de la PSCP es el uuid del procedimiento en la URL. Con la URL entera eran +187.577: cambia con cada fase y entre `/ca/` y `/es/`, y se colaban 102.180 fases antiguas de procedimientos que siguen publicados.
  - ~~Pendiente: **Barcelona** (`consolidar_bcn`) no acumula versiones ni siembra~~: **hecho** (`511e88e`, medido con la primera descarga del VPS del 29-sep).
    - Cada recurso se construye con todas sus versiones (`acumular`, ámbito el recurso); una versión vacía o ilegible no retira nada.
    - Los CSV que no son UTF-8 se leen en CP1252: 37.721 celdas con '€' (36.728) y comillas o rayas que llegaban como controles C1.
    - Semilla del perfil de contratante por el uuid del procedimiento: +7.411 filas de 6.120 publicaciones que el portal ya no sirve. Las otras 4 tablas del release coinciden con la descarga.
- **Solo contratación en el VPS** (`--categorias contratacion`, `95815b3`), por decisión del propietario.
  - Las subvenciones, presupuestos, RRHH… de Catalunya y Valencia no se usan.
  - Las subvenciones se harán a nivel estatal.
- Los menores de la PSCP quedaron cubiertos el 2026-09-28. Pendientes que dejó su revisión:
  - **Cobertura:** `totalElements` nunca pasa de 10.000, así que los órganos grandes (ICS, UPF…) desbordan la ventana. Quedan fuera del ámbito (ni se retiran ni se siembran) hasta que haya **segmentación por fecha**. `recuperar_hueco` nunca llega a ejecutarse con la API real.
  - **Semilla:** con la clave por publicación no entran las versiones de febrero de 2026 de publicaciones que han cambiado desde entonces. La alternativa es tomar el publicado como primera descarga, comparando por contenido. Decisión del propietario.
  - **Disco:** cada ejecución con cambios guarda en `_historico/` el crudo, las fases y la salida anteriores, y el crudo repite las filas de las fases. Una opción es dejar de escribir el crudo.
  - **No usar el commit `d4aad7a`:** es una copia WIP que se hizo durante las pruebas de mutación y contiene un mutante. Los posteriores están bien.
- El BORME quedó cubierto el 2026-09-28. Pendientes que dejó su revisión:
  - **Contra la regla 1 de §2:** el parser descarta desde siempre los cargos repetidos dentro de un mismo acto (misma persona y cargo; 119 filas en 47 PDF, sobre todo en actos concursales). Hay que conservarlos y marcarlos (`_repetido`), y adaptar los consumidores.
  - **Retención de `_historico/`:** cada ejecución que cambia las tablas crudas guarda una copia entera (del orden de GB). Hay que decidir cuántas se conservan.
  - La semilla se aplica entera y no solo en el ámbito de lo parseado, porque el BORME no retira actos. En una máquina sin PDF, la tabla cruda queda con filas `_origen`.
  - `calidad/calidad_licitaciones.py` lee `empresa_norm` de todas las filas, versiones antiguas incluidas. No afecta a la pertenencia, pero conviene filtrar `_en_ultima_descarga`.
- TED quedó cubierto el 2026-09-28. Pendientes que dejó su revisión:
  - **Hueco de 2020-2023:** el CSV no trae los `can-modif` ni los `can-desg` (1.620 solo en 2020). Viene de `5827174`. Con `--semilla` se recuperan del publicado, pero no se vuelven a descargar. Decidir si se piden a la API.
  - El primer refresco real, con `--semilla` y el `ted_es_can.parquet` publicado.
  - **Coste:** cada ejecución acumula todas las versiones (unos 6 s por versión en un año de 125.000 filas). Para ejecuciones diarias haría falta un acumulado intermedio por año.
  - ~~`diagnostico_missing_ted.py`, `analisis_sector_salud.py` y `cross-validation_ted_placsp.py` leen el consolidado entero~~: **hecho** (`c0230cb`). Usan `avisos_para_cruce`: la última versión de cada aviso y, después, sin los cancelados.
  - ~~**Contra la regla 2 de §2:** el CSV se lee con `on_bad_lines='skip'` sin guardar lo descartado, y `_normalize_ted_data` elimina los avisos cancelados~~: **hecho** (`c0230cb` y `b8c6709`).
    - El CSV se lee registro a registro con el módulo `csv`.
    - Los registros irregulares van a `ted_can_<año>_registros_irregulares.csv`. Las filas que entran en la tabla desde uno de ellos llevan el motivo en `_registro_irregular`.
    - Si el CSV de un año trae alguno, `download` guarda lo descargado y sale con 1. Es un aviso único: el CSV de un año cerrado solo se lee una vez.
    - Los cancelados se conservan con `cancelled='1'`, y los cruces los excluyen después de quedarse con la última versión de cada aviso.
    - Verificado con los CSV reales de 2019 y 2021 (566 y 749 MB): sale la misma tabla, salvo 4 celdas de 2021 (`N/A` ×3 y `NA` ×1) que ahora se conservan como texto. No hay ningún registro irregular.
    - La web (`buscalicitaciones`, `etl/build_unified.py`) marcaba como cancelados los `'Y'`, pero TED usa `'1'`: lo corrige el ETL nuevo.
- Andalucía quedó cubierta el 2026-09-28, sin verificar en vivo.
- Asturias quedó cubierta el 2026-09-28, sin verificar en vivo porque su portal no responde desde la nube.

| Scraper | Qué hay que cambiar |
|---|---|
| ~~`ted/ted_module.py`~~ | **Hecho** (2026-09-28): cachés y consolidado con `guardar_version`, `acumular` por año con la clave del aviso normalizada (`número-año`), el año en curso guardado aparte, `--semilla` y `ultima_version_por_aviso` en el cruce. Verificado en vivo. Pendiente: ver la lista de TED más abajo |
| ~~`borme/scripts/borme_scraper.py`, `borme_batch_parser.py`~~ | **Hecho** (2026-09-28): PDF con `guardar_version` (`--comprobar`), parse incremental con `acumular` que conserva lo parseado aunque falten PDF, `--reprocesar` y `--semilla` por (`pdf_filename`, `num_entrada`). Verificado en vivo contra boe.es |
| ~~`scripts/ccaa_valencia.py`, `ccaa_valencia_parquet.py`~~ | **Hecho** (`31db07f`). Sin semilla: no hay clave estable y el release es incompatible |
| ~~`comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py`~~ | **Hecho** (2026-09-28): CSV con `guardar_version` y `_comprobaciones.json`, consolidación con `acumular` por bloque (registro + continuaciones), `--semilla` por `Referencia` + `Entidad Adjudicadora` y archivado de las consultas de entidades renumeradas. Verificado con los 773 CSV reales, sin pérdida de filas ni celdas |
| ~~`comunidad_madrid/ccaa_madrid_ayuntamiento.py`~~ | **Hecho** (§3.1.4, `73d6e80`): `guardar_version` y `acumular` |
| ~~`scripts/ccaa_asturias.py`~~ | **Hecho** (2026-09-28): CSV anuales en `raw/` con `guardar_version`, Parquet desde todas las versiones con `acumular` (comparando el texto publicado) y `--semilla` por (`year`, `Nº INSCRIPCION`). El portal no responde desde la nube: falta verificarlo en vivo (VPS) |
| ~~`galicia/scraper_galicia.py`~~ | **Hecho** (2026-09-28): base y final con `guardar_version`, caché SQLite que nunca se borra (`detail_cache_historico`), `acumular` con ámbito por organismo y ventana leídos completos, y `--semilla` por (`_tipo`, `id`). Verificado en vivo (organismos 190, 305, 47 y 441) y a escala con 1,69 M filas |
| ~~`scripts/ccaa_andalucia.py`~~ | **Hecho** (2026-09-28): `raw/` con `guardar_version`, `acumular` con ámbito (no se retira nada de consultas con el tope o incompletas), `--semilla` por `id_expediente`, reanudación y `procesar` sin red. Probado sin red con las 808.441 filas del publicado. El portal corta desde la nube: falta la prueba en vivo y la partición por mes del SAS |
| ~~`scripts/ccaa_cataluna_contratosmenores.py`~~ | **Hecho** (2026-09-28): fases, crudo y salida con `guardar_version`; `acumular` con ámbito por grupo (normales o agregadas) leído entero; `--semilla` por (`id`, `expedientId`), quitando solo las 2,16 M copias idénticas del publicado. Verificado en vivo (fases 500 y 1100). Pendiente: ver la lista de la PSCP más abajo |
| `scripts/ccaa_cataluna.py`, `ccaa_cataluna_parquet.py` | Re-descarga por `rowsUpdatedAt` / `last_modified`. El RPC y `qjue-2pk9` son ventanas móviles de 5 años: lo que sale de la ventana debe conservarse. **Hecho**: capa cruda (`78d4f39`, `guardar_version`) y Parquet con todas las versiones (`1e28560`, `acumular`) |
| `Euskadi/ccaa_euskadi.py` | La tabla de la API ya acumula (`consolidacion_euskadi.py`, `_en_ultima_descarga`). Falta revisar el refresco de los ficheros crudos que "siguen cambiando" |

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
   - Partición por mes del Elasticsearch de Andalucía (unos 41K menores del SAS por encima del límite de 10K). Para los menores ya no hace falta: el CKAN de la Junta trae el SAS entero (`scripts/ccaa_andalucia_menores.py`).
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
  - ~~Socrata `qjue-2pk9`, con importes en céntimos que no hay que convertir~~ (`8b4ccce`).
  - AOC: RPC local e histórico.
  - Barcelona: `prorrogues-de-contractes`.
  - Comprobar si existen `ydq4-xy5b`, `jxvs-kzbu`, `w2cu-rmuv`, `wwmk-zys7` y `nuym-4erw`.
- **Andalucía.** ~~CSV oficial de menores de la Junta (CKAN, separador `|`, con NIF y nombre del adjudicatario)~~: **hecho** (`scripts/ccaa_andalucia_menores.py`, 2026-09-29; `docs/COBERTURA.md` §5.2). Queda "Licitaciones publicadas {año}".
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

### 3.6 Cobertura de contratos menores (prioridad del propietario)
Inventario completo, veredicto por CCAA y referencias (RCSP, OIReScon) en `docs/COBERTURA.md` §0 y §5. Por orden de valor (filas con NIF) y esfuerzo:
1. **Comunidad de Madrid**:
   - El portal tiene 4.832.623 menores y el publicado 2,53 M. Volver a ejecutar `descarga_contratacion_comunidad_madrid_v1.py` sobre una **copia** del script fuera del repo, porque escribe en su propia carpeta, que es la de los datos LFS.
   - El filtro de fecha "desde" (`createddate`) funciona sin entidad y sirve para recuperar las entidades que ya no salen en el desplegable.
2. ~~**Euskadi `/contracts`**: terminar la descarga, consolidar y medir~~: hecho (643.462 menores con importe y CIF). Falta publicarlo.
3. **Galicia**: volver a ejecutar, porque el portal tiene 1.775.090 menores y el publicado 1,64 M.
4. **Extremadura** (Registro de Contratos, XLSX trimestrales 2022-2026, ~200K con NIF) y **Castilla-La Mancha** (UCLM, caja pagadora, ficheros de la JCCM): scrapers nuevos hechos por agentes; revisar, verificar y commitear.
5. **Catalunya**:
   - Tabla de menores desde `ybgg-dgi6`: 381K en 2025, todos con NIF.
   - Cruce con el RPC para dar NIF al 45 % que solo trae nombre.
   - ~~Que el Parquet acumule las versiones~~ (`1e28560`).
6. **C. Valenciana** (~35 % hoy):
   - XLSX trimestrales de la UV 2016-2026 (~17K/año con NIF).
   - Buscador del Ajuntament de València (~2K/año con NIF).
   - Registro LIGATE de la Diputación de Alicante.
7. **Municipios con fuente propia**: Gijón (64K desde 2018, con CIF), Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife.
8. **Bloqueados desde la nube**: ejecutar desde una IP española que no sea de la nube.
   - `*.asturias.es` (2025-2026), `*.cantabria.es`, A Coruña, Oviedo, Avilés, Palma, zaragoza.es y dpz.es.
   - ~~`www.juntaandalucia.es`: CSV de menores de la Junta en el CKAN, con los ~41K del SAS~~: **hecho** desde el VPS (2026-09-29): 41.022 menores del SAS 2019-2025 que el buscador no alcanzaba (`scripts/ccaa_andalucia_menores.py`, `docs/COBERTURA.md` §5.2). El CKAN no es un superconjunto del buscador: hay que usar los dos, deduplicados por (id_expediente, número de expediente).
9. **Sin fuente pública contrato a contrato**: menores del SCS (Canarias, 89K en 2025, solo totales). Hay que pedirlo por acceso a la información.
10. **Pestaña "Documentos" de la PLACSP**: UGR, Diputación de Granada, Melilla y unos 1.950 ayuntamientos con perfil en la PLACSP y sin menores en el 1143. Requiere extraer tablas de PDF.
11. **Navarra y La Rioja**: ver el inventario de §5 de COBERTURA.

Medir de nuevo cada fuente al incorporarla: filas por año, % con NIF válido, solape con el 1143 (deduplicar por NIF del órgano, expediente, adjudicatario, importe y fecha).

### 3.7 Cierre
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
