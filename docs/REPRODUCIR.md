# Reproducir la release

Todo lo que trae la release sale de los scripts de este repositorio, que descargan de los portales oficiales. Esta guía explica cómo volver a generarlo en tu máquina: requisitos, cómo se ejecuta cada script, cómo completar con la release anterior para no perder lo que la fuente ya no sirve y qué ZIP sale de cada uno.

## Requisitos

```bash
git clone https://github.com/BquantFinance/licitaciones-espana.git
cd licitaciones-espana
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt -c constraints.txt
python -m pytest -q -p no:cacheprovider     # sin red: comprueba que el entorno está bien
```

- **Python 3.12** y las versiones de [`constraints.txt`](../constraints.txt), que son las de los tests. La suite pasa también con pandas 2.2.3.
- **RAR** (Castilla-La Mancha): libarchive (la usa `libarchive-c`) o uno de `unar`, `unrar`, `bsdtar` o `7z`.
- **Memoria:** todas las descargas de la release v2026.10 se hicieron con 4 CPU y 10 GB de memoria; la PLACSP, con 12 GB.
- **Disco:** la primera descarga completa de todo ocupa unos 45 GB con las capas crudas (la PLACSP sola, unos 20 GB entre sus ZIP y su salida), y crece con cada versión que se guarda en `_historico/`. En la PLACSP, el histórico de la salida se puede acotar con `--conservar-versiones` ([ficha](fuentes/placsp.md#retención-del-histórico-de-la-salida)). La release, ya en Parquet, ocupa 11,5 GB descomprimida.
- **Red:** cada script pide a su portal con pausas y reintentos. Respeta esas pausas: algunos portales cortan a quien pide demasiado deprisa.

## Cómo funciona cada script

Todos siguen los [principios del proyecto](PRINCIPIOS.md), con [`comun/historico.py`](../comun/historico.py):

- **Capa cruda.** Cada fichero descargado se guarda tal cual. Si en otra ejecución la fuente lo cambia, la versión anterior pasa a `_historico/`: nunca se machaca nada.
- **Salida.** Los Parquet se construyen desde **todas** las versiones de la capa cruda. Lo que la fuente retira o cambia se conserva con `_en_ultima_descarga=False`.
- **Re-ejecuciones.** Incrementales: vuelven a pedir lo reciente (casi siempre el año en curso y el anterior) y completan lo que falta. `--comprobar-todo` vuelve a pedir también lo antiguo.
- **Semilla.** `--semilla <Parquet de la release v2026.02>` añade, por su clave, lo que la fuente ya no sirve, marcado con `_origen='release v2026.02'` y `_en_ultima_descarga=False`. Úsala en la primera ejecución: sin ella, lo que v2026.02 tenía y el portal ya ha retirado no estaría en tu salida.
- **Código de salida.** 0 si todo fue bien; 1 si algo que existe no se pudo descargar o leer (se reintenta en la siguiente ejecución, y no se retira nada por un fallo).

El docstring de cada script es su referencia completa: qué pide, qué guarda, sus columnas y sus casos raros.

## Las semillas: la release v2026.02

Las semillas son ficheros de la [release v2026.02](https://github.com/BquantFinance/licitaciones-espana/releases/tag/v2026.02). Descomprime cada ZIP en su carpeta, por ejemplo en `v2026.02/`:

```bash
for z in nacional ted andalucia asturias borme catalunya comunidad_madrid; do
    unzip -q "$z.zip" -d "v2026.02/$z"
done
```

La de Galicia es el Parquet que estuvo en Git LFS en el repositorio, del 26-feb-2026: el ZIP de v2026.02 solo trae el CSV, con 12 columnas. Sigue en la etiqueta `datos-lfs-v2026.02`:

```bash
mkdir -p v2026.02/galicia
curl -L -o v2026.02/galicia/contratos_galicia.parquet https://media.githubusercontent.com/media/BquantFinance/licitaciones-espana/datos-lfs-v2026.02/galicia/contratos_galicia.parquet
```
 Las fuentes nuevas de v2026.10 no tienen semilla: no estaban en v2026.02.

## Cada fuente

Los comandos son los de una primera descarga completa en `datos/<fuente>/`. Los tiempos son los de la primera descarga completa de la release v2026.10 (septiembre de 2026), con 4 CPU: dependen sobre todo de los portales, así que tómalos como orientación.

| Fuente | Comando | Semilla | Tiempo | ZIP |
|---|---|---|---|---|
| [PLACSP](fuentes/placsp.md) | ver abajo | `v2026.02/nacional/licitaciones_espana.parquet` y `licitaciones_completo_2012_2026.parquet` | 1 h 5 min | `nacional_*.zip` |
| [TED](fuentes/ted.md) | `python ted/ted_module.py download --semilla v2026.02/ted/ted_es_can.parquet` | `ted_es_can.parquet` | ~10 h la primera vez (ver abajo) | `ted.zip` |
| [BOE, sección III](fuentes/boe_otras_disposiciones.md) | `python boe/boe_otras_disposiciones.py --salida datos/boe_otras_disposiciones` | — | ~4,5 h desde 2016-10-02 (una petición por día, unas 3.100); ~5 min a la semana | — (aún no en la release) |
| [BORME](fuentes/borme.md) | ver abajo | `borme_empresas_pub.parquet` y `borme_cargos_pub.parquet` | horas (~126.000 PDF) | `borme.zip` |
| [Andalucía](fuentes/andalucia.md) | `python scripts/ccaa_andalucia.py scrape --salida datos/andalucia --semilla v2026.02/andalucia/licitaciones_andalucia.parquet` | `licitaciones_andalucia.parquet` | 2 h | `andalucia.zip` |
| [Andalucía, menores](fuentes/andalucia_menores.md) | `python scripts/ccaa_andalucia_menores.py --salida datos/andalucia_menores` | — | 6 min | `andalucia_menores.zip` |
| [Aragón](fuentes/aragon.md) | `python scripts/ccaa_aragon.py --salida datos/aragon` | — | 4 min | `aragon.zip` |
| [Asturias](fuentes/asturias.md) | `python scripts/ccaa_asturias.py --salida datos/asturias --semilla v2026.02/asturias/asturias_contracts_ALL_YEARS.parquet` | `asturias_contracts_ALL_YEARS.parquet` | 3 min | `asturias.zip` |
| [Canarias](fuentes/canarias.md) | `python scripts/ccaa_canarias.py --salida datos/canarias` | — | 9 min | `canarias.zip` |
| [Cantabria](fuentes/cantabria.md) | `python scripts/ccaa_cantabria.py --salida datos/cantabria` | — | ~15 min (unas 290 peticiones) | — (aún no en la release) |
| [Castilla-La Mancha](fuentes/castilla_la_mancha.md) | `python scripts/ccaa_castilla_la_mancha.py --salida datos/castilla_la_mancha` | — | ~1,5 h | `castilla_la_mancha.zip` |
| [Castilla y León](fuentes/castilla_leon.md) | `python scripts/ccaa_castilla_leon.py --salida datos/castilla_leon` | — | 1 min | `castilla_leon.zip` |
| [Cataluña](fuentes/catalunya.md) | ver abajo | la carpeta `v2026.02/catalunya` | — | `catalunya.zip` |
| [Cataluña, menores de la PSCP](fuentes/catalunya.md#contratos-menores-de-la-pscp) | `python scripts/ccaa_cataluna_contratosmenores.py --output datos/catalunya_menores/contractacio_menors.parquet --resume --semilla v2026.02/catalunya/contratacion/contractacio_menors.parquet` | `contractacio_menors.parquet` | ~3 h | `catalunya_menores.zip` |
| [C. Valenciana](fuentes/valencia.md) | ver abajo | — | ~20 min | `valencia.zip` |
| [C. Valenciana, menores](fuentes/valencia_menores.md) | `python scripts/ccaa_valencia_menores.py --salida datos/valencia_menores` | — | 12 min | `valencia_menores.zip` |
| [Extremadura](fuentes/extremadura.md) | `python scripts/ccaa_extremadura.py --salida datos/extremadura` | — | 2 min | `extremadura.zip` |
| [GLEIF](fuentes/gleif.md) | `python gleif/gleif_espana.py --salida datos/gleif_espana` | — | ~5 min (4 peticiones, 566 MB) | — (aún no en la release) |
| [Galicia](fuentes/galicia.md) | ver abajo | `v2026.02/galicia/contratos_galicia.parquet` (etiqueta `datos-lfs-v2026.02`) | 9 h | `contratos_galicia.zip` |
| [La Rioja](fuentes/la_rioja.md) | `python scripts/ccaa_la_rioja.py --salida datos/la_rioja` | — | 1 min | `la_rioja.zip` |
| [Comunidad de Madrid](fuentes/comunidad_madrid.md) | ver abajo | `contratacion_comunidad_madrid_completo.parquet` | 4 h 20 min | `comunidad_madrid.zip` |
| [Ayuntamiento de Madrid](fuentes/madrid_ayuntamiento.md) | `python comunidad_madrid/ccaa_madrid_ayuntamiento.py --output-dir datos/madrid_ayuntamiento` | — | 8 min | `madrid_ayuntamiento.zip` |
| [Región de Murcia](fuentes/murcia.md) | `python scripts/ccaa_murcia.py --salida datos/murcia` | — | 2 min | `murcia.zip` |
| [País Vasco](fuentes/euskadi.md) | `python Euskadi/ccaa_euskadi.py --salida datos/euskadi/crudo && python Euskadi/consolidacion_euskadi.py --entrada datos/euskadi/crudo --salida datos/euskadi/parquet` | — | 13 h 30 min | `euskadi.zip` |
| [8 ayuntamientos](fuentes/municipios.md) | `python scripts/municipios_menores.py --salida datos/municipios` | — | 7 min | `municipios.zip` |
| [Calidad](fuentes/calidad.md) | `python calidad/calidad_licitaciones.py -i <Parquet de la PLACSP> --ted <cruce SARA> --borme <empresas del BORME>` | — | — | `calidad_licitaciones_resultado.zip` |

«—» en el tiempo: la primera descarga no se hizo de una vez (Cataluña y la calidad), así que no hay una cifra limpia.

### PLACSP

Primero los ZIP de la PLACSP (la descarga va a ~0,4-1 MB/s por conexión, así que conviene repartirla por conjuntos y años, en paralelo) y después el procesado, con las dos semillas:

```bash
python nacional/licitaciones.py --solo-descargar --conjunto licitaciones --anos 2012-2026 --data-dir datos/nacional/zips --output-dir datos/nacional/tmp
# … lo mismo con --conjunto agregacion, menores, encargos y consultas
python nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-2026 --data-dir datos/nacional/zips \
    --output-dir datos/nacional/salida --procesos 3 --sin-csv \
    --semilla v2026.02/nacional/licitaciones_espana.parquet --semilla v2026.02/nacional/licitaciones_completo_2012_2026.parquet
```

`--procesos 3` lee 3 ZIP a la vez (unos 1,2 GB de memoria cada uno) y `--sin-csv` omite los CSV, que con todos los conjuntos pasan de 10 GB. La release parte después `licitaciones_completo.parquet` en 6 Parquet por año ([ficha](fuentes/placsp.md#v202610)).

### TED

`ted_module.py` escribe junto al script, en `ted/` (para escribir en otra carpeta, copia allí el script y pon la raíz del repositorio en `PYTHONPATH`). La primera vez pide unos 100.000 XML eForms, a 5 por segundo como mucho: a las 9,5 h deja de pedir, guarda lo hecho y la siguiente ejecución sigue donde se quedó. Después solo pide los avisos nuevos.

### BORME

```bash
python borme/scripts/borme_scraper.py --start 2009-01-01 --output datos/borme/pdfs        # ~126.000 PDF, ~6 GB
python borme/scripts/borme_batch_parser.py --input datos/borme/pdfs --output datos/borme/parse --workers 4 \
    --semilla v2026.02/borme/borme_empresas_pub.parquet --semilla v2026.02/borme/borme_cargos_pub.parquet
python borme/scripts/borme_anonymize.py --input datos/borme/parse --output datos/borme/pub
```

Con la semilla basta con bajar los PDF desde el último día de la release anterior (`--start 2026-02-18` para v2026.02): los actos anteriores salen de la semilla. Las tablas con nombres de `parse/` son privadas: solo se publican las de `pub/`, con las personas seudonimizadas ([ficha](fuentes/borme.md)).

### Cataluña

```bash
python scripts/ccaa_cataluna.py --salida datos/catalunya/crudo --categorias contratacion
python scripts/ccaa_cataluna_parquet.py --entrada datos/catalunya/crudo --salida datos/catalunya/parquet \
    --categorias contratacion --semilla v2026.02/catalunya
```

Sin `--categorias contratacion` se descargan también subvenciones, presupuestos y el resto: las concesiones RAISC pesan 19 GB por descarga. La conversión a Parquet va por trozos: con las tres versiones de la PSCP, el pico de memoria es de 3,6 GiB.

En los menores de la PSCP, `--resume` solo en la primera ejecución: el punto de control se conserva al terminar, y con `--resume` una ejecución posterior daría todas las fases por hechas. La consolidación de la primera descarga completa (4,8 M de filas) pide 6,4-6,8 GiB; con los tramos de fecha (26-oct-2026) se leen unos 5,6 M de filas, así que hay que contar con más (el ensayo no llegó a consolidar).

### Comunitat Valenciana

```bash
python scripts/ccaa_valencia.py --salida datos/valencia/crudo --categorias contratacion
python scripts/ccaa_valencia_parquet.py --entrada datos/valencia/crudo --salida datos/valencia/parquet --categorias contratacion
```

### Galicia

```bash
python galicia/scraper_galicia.py base --output datos/galicia --workers 3 --resume
python galicia/scraper_galicia.py merge --output datos/galicia --semilla v2026.02/galicia/contratos_galicia.parquet --origen-semilla "LFS 2026-02-26"
```

La release v2026.10 no ejecutó la fase `detail` (las columnas `detail_*` van vacías). Con `all` se ejecutan las tres fases: `base`, `detail` y `merge`.

### Comunidad de Madrid

El script escribe junto a sí mismo (`comunidad_madrid/`). La primera vez, usa como primera versión de `csv_originales/` los CSV del ZIP de v2026.02, con su fecha: conservan las filas de continuación que perdió el Parquet publicado.

```bash
cp -r --preserve=timestamps v2026.02/comunidad_madrid/csv_originales comunidad_madrid/
python comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py todo
python comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py unificar \
    --semilla v2026.02/comunidad_madrid/contratacion_comunidad_madrid_completo.parquet
```

## Comprobar el resultado

- **Tests:** `python -m pytest -q -p no:cacheprovider`, con pandas 3 y con pandas 2.2.3 ([`CONTRIBUTING.md`](../CONTRIBUTING.md#tests)).
- **Contra la release:** los `MANIFEST.csv` de la release dan filas, columnas y SHA-256 de cada fichero, y [`docs/esquema/`](esquema/README.md), las columnas y los tipos. Una descarga posterior tendrá más filas (lo nuevo) y nunca menos de lo que tenía la release: lo retirado sigue, con `_en_ultima_descarga=False`.
- **Tiempos de consulta:** [`docs/MEDIR.md`](MEDIR.md).

## Todos los scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `nacional/licitaciones.py` | PLACSP | Extrae datos nacionales de ATOM/XML (todas las entradas publicadas, marcadas con `es_ultima_version`, + tabla de resultados por lote) |
| `nacional/normalizar_placsp.py` | — | Corrige parquets PLACSP ya generados sin eliminar filas: marca versiones, semántica de importes, etiquetas y CPV |
| `scripts/ccaa_andalucia.py` | Junta de Andalucía | Scraper ES proxy con subdivisión 8D y tramos de `idExpediente` (multi-sort con 14 órdenes como reserva) + salida reproducible |
| `scripts/ccaa_andalucia_menores.py` | CKAN de datos abiertos de la Junta de Andalucía | Contratos menores «Contratación Menor en {año}» (2018-), con el SAS entero; CSV tal cual con versiones y Parquet acumulado |
| `scripts/ccaa_cantabria.py` | Portal de Transparencia de Cantabria (API) | Contratos del Gobierno de Cantabria y su sector público (2015-), menores incluidos, con invitados y ofertas: la exportación CSV de cada mes tal cual, con versiones, y un Parquet acumulado |
| `scripts/ccaa_canarias.py` | datos.canarias.es y portales de transparencia de Canarias | Contratos menores de Canarias: CSV de contratos del Gobierno (2020-, copia de la PLACSP), API de Las Palmas de GC (2016-) y del Cabildo de Tenerife (2019-), y los resúmenes agregados del SCS (ODS) y de los departamentos (ODT); ficheros tal cual con versiones y un Parquet acumulado por serie |
| `Euskadi/ccaa_euskadi.py` | KontratazioA + Open Data Euskadi | Scraper v4 (solo descarga): API REST + XLSX anuales + portales municipales |
| `Euskadi/consolidacion_euskadi.py` | — | Consolida JSON/XLSX/CSV → 9 tablas Parquet normalizadas |
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | contratos-publicos.comunidad.madrid | Web scraping con antibot bypass + subdivisión recursiva por importe |
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | datos.madrid.es | Descarga y unifica los CSV de actividad contractual (9 categorías, 11 estructuras) |
| `scripts/ccaa_cataluna_contratosmenores.py` | contractaciopublica.cat | Descarga contratos menores Catalunya (todas las fases, API del portal) |
| `boe/boe_otras_disposiciones.py` | API de datos abiertos del BOE (sumarios) | Sección III del BOE día a día: convenios, adendas, protocolos, encargos y convenios colectivos, sin nombres de personas físicas |
| `gleif/gleif_espana.py` | Golden Copy de GLEIF (API) | Entidades españolas con LEI, las enlazadas por relaciones de nivel 2 (matriz directa y última) y las excepciones de matriz; sin empresarios individuales |
| `galicia/scraper_galicia.py` | contratosdegalicia.gal | Pipeline base + detalle HTML + merge, con discovery automático, barrido CM 3 meses, caché SQLite y `--resume` |
| `scripts/ccaa_asturias.py` | Principado de Asturias | Descarga contratación centralizada Asturias → `ccaa_asturias/` |
| `scripts/ccaa_aragon.py` | Aragón Open Data + Ayuntamiento de Zaragoza (OCDS) | Contratos del Gobierno de Aragón, Registro de Contratos, anuncios del perfil y OCDS de Zaragoza |
| `scripts/ccaa_castilla_leon.py` | Datos abiertos de la Junta de Castilla y León | Contratos ordinarios, menores, basados en acuerdo marco, modificados… de la Junta y del SACYL |
| `scripts/ccaa_castilla_la_mancha.py` | Junta de Castilla-La Mancha + UCLM | Menores, caja pagadora, SESCAM por línea de factura, sector público 2015-2018 y contratos de la UCLM |
| `scripts/ccaa_murcia.py` | CARM + Servicio Murciano de Salud | Contratos y contratos menores de la CARM y menores del SMS |
| `scripts/ccaa_extremadura.py` | Junta de Extremadura | Registro de Contratos: menores, mayores e incidencias |
| `scripts/ccaa_la_rioja.py` | Gobierno de La Rioja | CSV anual de contratos menores |
| `scripts/ccaa_valencia_menores.py` | UV, Ajuntament de València, Diputación de Alicante, UA, UMH y UPV | Contratos menores valencianos fuera del REGCON |
| `scripts/municipios_menores.py` | 9 ayuntamientos | Contratos menores de Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba, Santa Cruz de Tenerife y Palma |
| `scripts/ccaa_cataluna.py` | Socrata + CKAN Barcelona | Descarga datos Catalunya |
| `scripts/ccaa_cataluna_parquet.py` | — | Convierte los CSV de Catalunya a Parquet |
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `scripts/ccaa_valencia_parquet.py` | — | Convierte los CSV de Valencia a Parquet |
| `ted/ted_module.py` | TED | Descarga CSV bulk (2006-2023) + API v3 con el XML eForms de cada aviso |
| `ted/run_ted_crossvalidation.py` | — | Cross-validation PLACSP↔TED + matching avanzado (9 estrategias) |
| `ted/diagnostico_missing_ted.py` | — | Diagnóstico de missing |
| `ted/analisis_sector_salud.py` | — | Deep dive sector salud |
| `ted/cross-validation_ted_placsp.py` | — | Versión anterior del cruce PLACSP↔TED (la vigente es `run_ted_crossvalidation.py`) |
| `borme/scripts/borme_scraper.py` | BOE/BORME | Descarga ~126K PDFs del Registro Mercantil |
| `borme/scripts/borme_batch_parser.py` | — | Parser de actos mercantiles (constituciones, cargos...) |
| `borme/scripts/borme_anonymize.py` | — | Genera las versiones públicas, con las personas seudonimizadas |
| `borme/scripts/borme_placsp_match.py` | — | Detector de anomalías BORME × PLACSP (5 flags) |
| `calidad/calidad_licitaciones.py` | — | 21 indicadores de calidad sobre PLACSP + TED + BORME |
| `calidad/correcciones.py` | — | Importes corregidos junto a los publicados (registro de errores de la fuente y saltos de escala) |
| `scripts/medir_consultas.py` | — | Tiempos de consulta sobre la release ([`docs/MEDIR.md`](MEDIR.md)) |
| `scripts/esquema_release.py` | — | Columnas, tipos y filas de cada Parquet de una release, leyendo solo sus pies ([`docs/ESQUEMA.md`](ESQUEMA.md#cómo-se-genera-docsesquema)) |
