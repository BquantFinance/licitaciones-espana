# Estado de los scrapers

Qué scripts están **cerrados** (revisados, con tests en pandas 3 y 2.2 y con el sesgo del superviviente cubierto) y desde qué commit. Solo los cerrados se usan en producción: la comprobación automática del despliegue lee esta tabla y rechaza un script que no esté cerrado o que tenga commits WIP después de su cierre.

## Estado de los scrapers

Solo se usan en producción los scrapers **cerrados**: revisados, con tests en pandas 3 y 2.2 y con el sesgo del superviviente cubierto. Para comprobar que no hay cambios sin revisar después del cierre, `git log --format='%h %s' <cierre>..HEAD -- <script>` no debe mostrar ningún commit `WIP`.

| Script | Estado | Commit de cierre | Verificado en vivo |
|---|---|---|---|
| `nacional/licitaciones.py`, `nacional/normalizar_placsp.py` | Cerrado | `93b840d` | Sí (regeneración del 2026-09-27; nombre fijo con los ZIP reales de encargos del VPS: tablas iguales byte a byte y enlaces leídos con DuckDB 1.1.3; migración de la salida con años de cualquier año y nunca a través de un enlace, simulado en enero de 2027 con el ETL de la web igual) |
| `calidad/calidad_licitaciones.py`, `calidad/correcciones.py` | Cerrado | `ba5a46e` | Sí (regeneración del 2026-09-27; URDINBERRI contra la API de Euskadi) |
| `ted/ted_module.py`, `ted/run_ted_crossvalidation.py` | Cerrado (filas de la API desde el XML eForms; adjudicataria = oferta que cita un contrato) | `67d81dc` | Sí: ventana real de 3 días (493 avisos) y 525 XML, contrastados con los ganadores que da la API; sin descarga nueva, el consolidado sale idéntico byte a byte al de producción |
| `scripts/ccaa_cataluna.py`, `scripts/ccaa_cataluna_parquet.py` | Cerrado (`--salida`, `--entrada`, `--categorias`, `--semilla`) | `dcd6d54` | Sí (la semilla, con la primera descarga del VPS; Barcelona: versiones, CP1252 en las secuencias que no son UTF-8 y semilla del perfil, con los crudos del VPS del 29-sep; una cabecera cambiada es un caso a revisar, código 1) |
| `scripts/ccaa_valencia.py`, `scripts/ccaa_valencia_parquet.py` | Cerrado (`--salida`, `--entrada`, `--categorias`) | `95815b3` | Sí (primera descarga del VPS, 2026-09-28) |
| `Euskadi/ccaa_euskadi.py`, `Euskadi/consolidacion_euskadi.py` | Cerrado (`--salida`, `--entrada`) | `7953621` | Sí (API completa; con `--salida` el log va a la carpeta de salida, comprobado en el VPS) |
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | Cerrado | `73d6e80` | Sí |
| `scripts/ccaa_murcia.py` | Cerrado | `10078e9` | Sí, con los crudos del VPS (2026-09-29): CSV del exportador JSON (comillas `\"`, decididas solo por su presencia; cortes `\n` cada 80 caracteres en `_<columna>_sin_cortes`, con el contador del exportador, que no se reinicia en los saltos del texto; y restos de la lista en `_resto_json`), mismas filas y ningún texto perdido; y la codificación cp850 de contratosOD 2014-2018 |
| `scripts/ccaa_aragon.py` | Cerrado | `90978d2` | Sí (cabecera `<TH>` de los .xls del Gobierno, euro en 0xA4 y `_razon_social_es_pais` del Registro, con los crudos del VPS del 28-sep: +17 filas, 11 filas marcadas en mayores, 40 en menores y 19 en encargos, y el resto igual salvo 178 celdas por la codificación) |
| `scripts/ccaa_castilla_leon.py` | Cerrado | `0263faf` | Sí |
| `scripts/ccaa_extremadura.py` | Cerrado | `4163d85` | Sí |
| `scripts/ccaa_la_rioja.py`, `scripts/ccaa_valencia_menores.py` | Cerrado | `9190268` | Sí |
| `scripts/ccaa_castilla_la_mancha.py` | Cerrado | `d015194` | Sí |
| `scripts/municipios_menores.py` | Cerrado | `0f4f89e` | Sí (errores de origen permanentes en `raw/_fallos_origen.json`: Leganés y Málaga siguen fallando igual el 29-sep; con los datos del VPS, los 8 Parquet idénticos byte a byte con el código anterior; Valladolid ya leía su cabecera, los avisos eran de hojas auxiliares; una entrada del registro que no es un objeto se descarta con aviso) |
| `scripts/ccaa_asturias.py` | Cerrado | `aefb659` | Sí, desde el VPS (2026-09-28): 2019-2024, 375.380 filas; la semilla no añade ninguna; 2025 y 2026 dan 404 en `dataset-contratacion-centralizada-<año>.csv` |
| `scripts/ccaa_andalucia.py` | Cerrado (registro (`portalGestor`, `idExpediente`), tramos de id, semilla que mira antes si está) | `a8b13a7` | Sí, desde el VPS: primera descarga del 2026-09-29 (900.929 filas) y, con este cierre, OBRA/RES sin tramitación 6.072 de 6.072 y las tres consultas del SAS con tope en tramos de 9.874 como mucho (en vivo, 2026-09-29) |
| `scripts/ccaa_andalucia_menores.py` | Cerrado | `6f2f73c` | Sí, desde el VPS (2026-09-29): 9 CSV del CKAN de la Junta (2018-2026), 768.647 registros, 544.898 del SAS; la salida es idéntica al original celda a celda |
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | Cerrado (vía por fecha: menores sin entidad por ventanas de un mes de «Fecha del contrato», cada una contrastada con el recuento del portal; en la tabla solo entra lo que no trae la vía por entidad) | `242cac1` | Sí, por partes: la vía por entidad, con la descarga completa del 2026-09-28; la vía por fecha, medida en el portal el 2026-09-30 (recuentos por año, fronteras de las ventanas y un día exportado dos veces, idéntico) y con la capa cruda real: sin CSV por fecha, la tabla sale idéntica byte a byte a la del código anterior; con la exportación real del 15-5-2019, +1.251 menores de entidades históricas y ninguna clave presente dos veces. Falta su primera descarga completa (la hace `todo`) |
| `galicia/scraper_galicia.py` | Cerrado (listados por id, ventanas repetidas, `_organismo_nombre`; sin la fase `detail` en el VPS) | `c8d7806` | Sí, desde el VPS: primera descarga del 2026-09-29 (420 organismos) y, con este cierre, las 45 ventanas de CM que quedaron incompletas llegan completas (19.364 de 19.364, en vivo) |
| `scripts/ccaa_cataluna_contratosmenores.py` | Cerrado (sin segmentación por fecha: los órganos grandes quedan fuera del ámbito) | `1fa7f01` | Sí (dos fases) |
| `borme/scripts/*.py` | Cerrado | `38e72aa` | Sí (boe.es) |

Avisos de la sesión del VPS (2026-09-28):
- **Galicia:** segfault dentro de `to_numeric` (`csv_to_parquet`). Aquí no se reproduce (pandas 2.2.3, numpy 2.4.6, pyarrow 25.0.1). Ese código es anterior a esta sesión.
  - **Versiones del VPS** (medidas el 2026-09-28): el fallo era con **pandas 2.3.3**, numpy 2.5.3 y pyarrow 25.0.1, no con 2.2.3; la imagen se había construido con `pandas<3`.
  - En `db64717`, `tests/test_galicia.py` da segfault con 2.3.3 (código de salida 139) y pasa con 2.2.3.
  - En `main` (`8eb1540`) pasa 3 de 3 con las dos versiones: el arreglo de `csv_to_parquet` lo resuelve.
  - Sigue sin saberse qué valor lo dispara.
- **`test_ayto_a_corrupt_manifest_is_recovered_from_its_history`:** intermitente si dos ejecuciones caen en el mismo segundo, porque el manifiesto sale idéntico. El código es correcto; el test se hace determinista.
