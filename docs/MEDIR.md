# ¿Te basta tu ordenador? Tiempos de consulta sobre la release

Antes de bajarte los 10,7 GB de la release, mira cuánto tardan las consultas que más se hacen con estos datos y cuánta
memoria piden. `scripts/medir_consultas.py` lanza esas consultas sobre la release descomprimida (o sobre la salida de los
scripts) con DuckDB y, si se pide, con pandas, y guarda los tiempos en un JSON. Aquí están los de la release v2026.10 en
tres máquinas, y cómo medirlo en la tuya.

## Resultados con la release v2026.10

Medido el 7 de octubre de 2026. En resumen:

- **Con 2 CPU y 4 GB** (un portátil modesto), todas las consultas con DuckDB tardan menos de 7 s. Las de la PLACSP, sobre
  sus 10,9 M de filas, de 0,04 a 3,2 s. La más lenta es la de los menores de la Comunidad de Madrid (6,9 s): quitar los
  repetidos de 10 columnas de texto pide más que los 3 GiB de DuckDB (con 8 GB usa 5,5), que escribe en disco lo que no
  le cabe; con 8 GB tarda 2 s.
- **Con 4 CPU y 8 GB**, ninguna pasa de 2,1 s ni de 5,5 GB de memoria.
- **Con 8 CPU y 16 GB**, ninguna pasa de 1,4 s.
- **pandas** da las mismas cifras, pero tarda de 2 a 125 veces más y lo que lee tiene que caber en memoria: con 4 GB, el
  cruce de resultados y licitaciones no cabe. La del TED tarda unos 10 s en las tres máquinas porque
  `avisos_para_cruce()` agrupa texto con pandas, en un solo hilo.

La cifra principal es la mediana de 3 repeticiones; entre paréntesis, la primera vez en un proceso nuevo; después, el
pico de memoria del proceso.

**DuckDB**: repetida (primera) · pico de memoria

| Consulta | 2 CPU / 4 GB | 4 CPU / 8 GB | 8 CPU / 16 GB |
|---|---|---|---|
| Abrir toda la carpeta: ficheros y filas de cada Parquet | 0,02 s (0,02 s) · 0,14 GB | 0,01 s (0,02 s) · 0,14 GB | 0,01 s (0,02 s) · 0,14 GB |
| PLACSP: licitaciones por año, la última versión de cada una | 0,04 s (0,05 s) · 0,14 GB | 0,03 s (0,04 s) · 0,14 GB | 0,03 s (0,04 s) · 0,14 GB |
| PLACSP: la última versión de cada licitación, calculada sin la marca | 2,30 s (2,27 s) · 1,53 GB | 1,22 s (1,23 s) · 1,60 GB | 0,77 s (0,83 s) · 1,84 GB |
| PLACSP: licitaciones con una palabra en el objeto | 2,50 s (2,90 s) · 0,16 GB | 1,35 s (1,70 s) · 0,18 GB | 0,92 s (1,25 s) · 0,21 GB |
| PLACSP: adjudicaciones de un NIF | 0,52 s (0,88 s) · 0,18 GB | 0,32 s (0,65 s) · 0,20 GB | 0,25 s (0,57 s) · 0,24 GB |
| PLACSP: los 20 adjudicatarios con más licitaciones, sin duplicados | 3,19 s (3,22 s) · 2,18 GB | 1,76 s (1,88 s) · 2,15 GB | 1,38 s (1,48 s) · 2,48 GB |
| PLACSP: lo adjudicado por comunidad (cruce de resultados y licitaciones) | 1,93 s (2,04 s) · 1,93 GB | 1,00 s (1,09 s) · 2,02 GB | 0,78 s (0,87 s) · 2,28 GB |
| TED: avisos de adjudicación por año, cada aviso una vez | 0,12 s (0,14 s) · 0,30 GB | 0,11 s (0,12 s) · 0,38 GB | 0,08 s (0,10 s) · 0,39 GB |
| Comunidad de Madrid: contratos menores por año, sin contar dos veces el mismo | 6,88 s (7,39 s) · 3,44 GB | 2,04 s (2,20 s) · 5,50 GB | 1,36 s (1,46 s) · 6,14 GB |
| Catalunya (PSCP): los 20 adjudicatarios con más contratos menores | 1,17 s (1,17 s) · 0,21 GB | 1,15 s (1,15 s) · 0,33 GB | 1,14 s (1,11 s) · 0,40 GB |
| BORME: constituciones de sociedades por año | 0,08 s (0,08 s) · 0,14 GB | 0,05 s (0,06 s) · 0,14 GB | 0,04 s (0,04 s) · 0,14 GB |
| BORME: las 20 sociedades con más nombramientos de cargos | 1,99 s (2,07 s) · 1,38 GB | 1,02 s (1,11 s) · 1,72 GB | 0,69 s (0,74 s) · 1,95 GB |

**pandas + pyarrow**: repetida (primera) · pico de memoria

| Consulta | 2 CPU / 4 GB | 4 CPU / 8 GB | 8 CPU / 16 GB |
|---|---|---|---|
| PLACSP: licitaciones por año, la última versión de cada una | 0,11 s (0,28 s) · 0,40 GB | 0,09 s (0,26 s) · 0,54 GB | 0,07 s (0,25 s) · 0,64 GB |
| PLACSP: los 20 adjudicatarios con más licitaciones, sin duplicados | 7,61 s (14,1 s) · 3,34 GB | 7,67 s (11,6 s) · 3,45 GB | 7,44 s (12,2 s) · 3,77 GB |
| PLACSP: lo adjudicado por comunidad (cruce de resultados y licitaciones) | sin memoria | 11,7 s (15,8 s) · 4,75 GB | 12,1 s (16,2 s) · 4,99 GB |
| TED: avisos de adjudicación por año, cada aviso una vez | 10,1 s (10,4 s) · 0,64 GB | 10,4 s (10,4 s) · 0,68 GB | 10,6 s (10,3 s) · 0,67 GB |
| BORME: constituciones de sociedades por año | 1,29 s (1,70 s) · 1,68 GB | 1,40 s (3,53 s) · 1,67 GB | 1,24 s (1,49 s) · 1,70 GB |

## Cómo se midió

- **Datos.** Los 142 Parquet de v2026.10 (11,5 GB descomprimidos), es decir, todo lo actual de los 31 ZIP sin las
  carpetas `v2026.02/`, con la estructura que deja `unzip X.zip -d X` en cada ZIP. Antes de medir se comprobó el SHA-256
  de **cada** fichero contra el `MANIFEST.csv` de la release (cuyo SHA-256 es el de `SHA256SUMS.txt`): los 142 iguales.
- **Máquinas.** Una sola máquina (Intel Core i9-13900, 32 hilos, 62,6 GiB de memoria, Linux) con tres perfiles
  simulados con límites de Docker: `--cpus 2 -m 4g`, `--cpus 4 -m 8g` y `--cpus 8 -m 16g`, sin intercambio a disco
  (`--memory-swap` igual que `-m`). DuckDB, con tantos hilos como CPU y `memory_limit` del 75 % de la memoria (3, 6 y
  12 GiB), lo que el script hace solo al detectar el límite. La máquina estaba compartida con otros procesos: la carga
  media al empezar y al acabar cada perfil está en el JSON (`carga_al_empezar`, `carga_al_acabar`).
- **Versiones.** Python 3.12.13, DuckDB 1.5.6, pandas 3.0.6, pyarrow 25.0.1 y numpy 2.5.3.
- **Tiempos.** Cada consulta corre en un proceso nuevo. «Primera» es la primera vez en ese proceso (lee los metadatos y
  los datos de los Parquet); la cifra principal es la mediana de 3 repeticiones más en el mismo proceso. Tiempo de pared,
  contando traer el resultado a Python. Los ficheros estaban en la caché de disco del sistema (se leyeron enteros antes
  de cada perfil): desde un disco frío, la primera vez suma lo que tarde tu disco en leer las columnas que pide la
  consulta.
- **Memoria.** El pico de memoria residente del proceso de cada consulta. DuckDB se ajusta a su `memory_limit`: si no le
  basta, escribe en una carpeta temporal y tarda más (el pico del proceso puede pasar un poco de ese límite, que no
  cuenta todo lo que reserva). pandas carga en memoria lo que lee y, si no cabe, el sistema mata el proceso: la tabla lo
  dice como «sin memoria».
- **Resultados.** Los JSON completos están en [`docs/medidas/`](medidas/): versiones, CPU, memoria, configuración de
  DuckDB, filas y bytes de cada tabla y, de cada consulta, qué mide, su SQL, los tiempos, la memoria y el resultado
  entero (hasta 250 filas). Las cuentas son las mismas en las tres máquinas y, con pandas, las mismas que con DuckDB
  (`coincide_con_duckdb`).

## Qué cuenta cada consulta

Cada consulta cuenta como se debe con estos datos. Los atajos dan otra cifra, y lo que cuesta hacerlo bien es parte de lo
que se mide.

| Consulta | Qué hace | Para no contar dos veces |
|---|---|---|
| `todo` | Ficheros y filas de cada Parquet de la carpeta | Solo lee los metadatos. No entra en `v2026.02/` ni en `_historico/` |
| `placsp_ultima_version` | Licitaciones de la PLACSP por año | Una fila por versión publicada: filtra `es_ultima_version` (5.241.440 licitaciones en 10.907.567 filas) |
| `placsp_ultima_version_calculada` | Lo mismo, sin la marca: ventana por `id` | La `fecha_updated` más reciente y, si la misma entrada se publicó dos veces, la que no lleva `entrada_repetida`. Sin ese desempate, 15 licitaciones caen en otro año. Tiene que dar lo mismo que la anterior, y da lo mismo |
| `placsp_texto` | Licitaciones con una palabra en el objeto (`ILIKE`) | Última versión |
| `placsp_un_nif` | Lotes y licitaciones adjudicados a un NIF | Última versión y NIF normalizado: `B-12345678`, `b12345678` y `ESB12345678` son el mismo |
| `placsp_top_adjudicatarios` | Los 20 NIF con más licitaciones adjudicadas | Última versión y NIF normalizado, sin NIF enmascarados ni marcadores (`-`, `***`). En un acuerdo marco cada empresa lleva el tope entero del lote: se reparte entre ellas. El nombre, el más repetido de cada NIF |
| `placsp_cruce_comunidad` | Lo adjudicado por comunidad (NUTS de 4 caracteres): resultados × licitaciones por `id` y `fecha_updated` | Última versión en las dos tablas. El tope de un acuerdo marco, una vez por lote: sumarlo en cada empresa multiplica el total por 2,8 (2,26 billones de euros frente a 815.000 M€) |
| `ted_avisos` | Avisos de adjudicación del TED por año | La regla de `avisos_para_cruce()` (`ted/ted_module.py`): la última versión de cada aviso y, después, sin los cancelados. 355.124 avisos en 857.017 filas |
| `madrid_menores` | Contratos menores de la Comunidad de Madrid por año e importe | Fuera las versiones anteriores (misma Referencia y entidad en la última descarga) y el mismo menor publicado con otra Referencia. Importes y fechas en texto («1.234,56», «19 de mayo del 2023») |
| `pscp_top_menores` | Catalunya (PSCP): los 20 NIF con más contratos menores | Fuera las fases anteriores de una publicación que sigue en la descarga, las anulaciones, las filas con varios adjudicatarios (`\|\|`) y los NIF enmascarados |
| `borme_constituciones` | Constituciones de sociedades por año | «Constitución» como acto de la lista, no como texto suelto |
| `borme_nombramientos` | Las 20 sociedades con más nombramientos de cargos | Recuento de distintos sobre los 17,8 M de cargos (los códigos de persona están seudonimizados) |

Los importes son los publicados, sin las correcciones de `calidad/` (ver «Importes publicados y corregidos» en el
README). Las cinco consultas que también se hacen con pandas leen con pyarrow solo las columnas que hacen falta y usan la
misma regla; la del TED usa `avisos_para_cruce()` del repositorio, tal cual.

## Medir en tu máquina

```bash
pip install duckdb pandas pyarrow
# Solo el código: sin GIT_LFS_SKIP_SMUDGE=1, git-lfs bajaría los 6,8 GB de datos antiguos (v2026.02) del repositorio
GIT_LFS_SKIP_SMUDGE=1 git clone --depth 1 https://github.com/BquantFinance/licitaciones-espana
cd licitaciones-espana
python scripts/medir_consultas.py --datos CARPETA --motor ambos --json mi_maquina.json
```

`CARPETA` es donde has descomprimido los ZIP: cada uno en su carpeta (`unzip X.zip -d X`) o todos en la misma. Basta con
los que uses: una consulta cuya tabla no está se salta y lo dice. Para todas hacen falta los 5 `nacional_licitaciones_*.zip`,
`nacional_resultados.zip`, `ted.zip`, `comunidad_madrid.zip`, `catalunya.zip` y `borme.zip`.

Opciones:

- `--motor duckdb` (por defecto), `pandas` o `ambos`.
- `--hilos N` y `--memoria 6GB`: los de DuckDB (y los hilos de pyarrow). Por defecto, las CPU disponibles y el 75 % de la
  memoria disponible, también dentro de un contenedor con límites.
- `--repeticiones 3`, `--solo placsp_texto,ted_avisos` (ver `--listar`), `--nif` y `--palabra` (las de `placsp_un_nif` y
  `placsp_texto`), `--tiempo-maximo` (segundos por consulta), `--perfil` y `--etiqueta` (nombres para el JSON).
- `--tabla docs/medidas/*.json mi_maquina.json`: junta varios JSON en una tabla Markdown, una columna por máquina.

El JSON solo lleva nombres de fichero, sin rutas. En Windows no se mide la memoria.

## Pruebas

`tests/test_medir_consultas.py` construye una release diminuta con la estructura real (una carpeta por ZIP, las 6 partes
de la PLACSP en `licitaciones_completo/`, carpetas `v2026.02/` y `_historico/` que no se deben leer, y los nombres y tipos
de columna publicados) y con los casos que hacen contar mal: versiones anteriores, entradas repetidas con otro año, el
tope de un acuerdo marco en cada empresa, avisos del TED cancelados después, el mismo menor publicado dos veces, fases
anteriores de la PSCP y NIF enmascarados. Cada consulta tiene que dar el resultado contado a mano, con DuckDB y con
pandas, también con cero resultados (un NIF que no está, una palabra que no sale, un top sin ningún NIF válido); y las
tablas o columnas que faltan se saltan con su motivo. Se saltan si no está DuckDB.
