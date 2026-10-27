<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/img/logo-oscuro.svg">
    <img src="docs/img/logo.svg" alt="licitaciones-espana" height="52">
  </picture>
</p>

<p align="center">La contratación pública de España, en datos abiertos: scrapers, ETL y releases en Parquet de la PLACSP, el TED, las comunidades autónomas, los ayuntamientos y el BORME, tal como lo publica cada fuente oficial.</p>

<p align="center">
  <a href="https://github.com/BquantFinance/licitaciones-espana/releases/latest"><img alt="release" src="https://img.shields.io/github/v/release/BquantFinance/licitaciones-espana?label=release&color=1f4068"></a>
  <a href="LICENSE"><img alt="licencia del código: MIT" src="https://img.shields.io/badge/c%C3%B3digo-MIT-1f4068"></a>
  <a href="https://github.com/BquantFinance/licitaciones-espana/actions/workflows/tests.yml"><img alt="tests" src="https://github.com/BquantFinance/licitaciones-espana/actions/workflows/tests.yml/badge.svg?branch=main"></a>
  <a href="#descargar"><img alt="datos: v2026.10, 31 ZIP, 10,7 GB en Parquet" src="https://img.shields.io/badge/datos-v2026.10%20%C2%B7%2010%2C7%20GB%20%C2%B7%20Parquet-2d5a3d"></a>
</p>

## Qué es

Un repositorio de datos abiertos de la contratación pública española, con el código que los descarga:

- **Datos.** Cada [release](https://github.com/BquantFinance/licitaciones-espana/releases) trae las tablas en Parquet, una por fuente oficial, con su `LEEME.txt` y su `MANIFEST.csv` (filas, columnas y SHA-256 de cada fichero). La actual, [v2026.10](https://github.com/BquantFinance/licitaciones-espana/releases/tag/v2026.10), tiene 115 millones de filas: 10,9 M de entradas de la PLACSP, 857.017 filas del TED, 14 comunidades autónomas, los contratos menores de 8 ayuntamientos grandes y 27,4 M de actos y cargos del BORME.
- **Código.** Un script por fuente, con tests sin red, que vuelve a descargar todo desde los portales oficiales.

**Tal como se publica.** No se limpian valores ni se borran filas: lo raro se marca con columnas `_…` (`_duplicado`, `_columnas_corridas`, `_en_ultima_descarga`…) y lo corregido va en una columna aparte, junto al valor publicado. Lo que una fuente retira se conserva, marcado: sin sesgo del superviviente ([principios](docs/PRINCIPIOS.md)).

Es para periodistas, investigadores, auditores y cualquiera que quiera analizar la contratación pública sin raspar veinte portales. [buscalicitaciones.com](https://buscalicitaciones.com) es un ejemplo de uso: un buscador hecho con estos mismos datos.

## Descargar

Los datos están en las [releases](https://github.com/BquantFinance/licitaciones-espana/releases), no en el árbol del repositorio. La [v2026.10](https://github.com/BquantFinance/licitaciones-espana/releases/tag/v2026.10) (5 de octubre de 2026) son 31 ZIP: 9,92 GB tal como los cuenta GitHub (10,7 GB en unidades decimales). Basta con bajar los que vayas a usar.

| ZIP | Contenido | Filas | Tamaño |
|---|---|--:|--:|
| `nacional_licitaciones_hasta_2019.zip` | PLACSP, tabla principal: año del anuncio hasta 2019 y entradas sin año | 1.617.421 | 406 MB |
| `nacional_licitaciones_2020_2021.zip` | PLACSP, tabla principal: 2020-2021 | 1.718.374 | 468 MB |
| `nacional_licitaciones_2022_2023.zip` | PLACSP, tabla principal: 2022-2023 | 2.564.085 | 759 MB |
| `nacional_licitaciones_2024_2025.zip` | PLACSP, tabla principal: 2024-2025 | 3.036.783 | 1,24 GB |
| `nacional_licitaciones_desde_2026.zip` | PLACSP, tabla principal: 2026 en adelante | 1.970.904 | 762 MB |
| `nacional_resultados.zip` | PLACSP, resultados por lote | 10.677.644 | 763 MB |
| `nacional_criterios.zip` | PLACSP, criterios de adjudicación | 14.571.218 | 663 MB |
| `nacional_adjudicatarios.zip` | PLACSP, adjudicatarios | 10.146.626 | 336 MB |
| `nacional_lotes_y_otras_tablas.zip` | PLACSP, lotes, modificaciones, borrados y semilla por contenido | 5.903.920 | 239 MB |
| `ted.zip` | TED, avisos de adjudicación de España + `v2026.02/` | 857.017 | 298 MB |
| `borme.zip` | BORME, actos mercantiles y cargos (personas seudonimizadas) | 27.384.748 | 775 MB |
| `andalucia.zip` | Andalucía, buscador de contratación de la Junta | 900.931 | 79 MB |
| `andalucia_menores.zip` | Andalucía, contratos menores del CKAN de la Junta, con el SAS | 768.647 | 57 MB |
| `aragon.zip` | Aragón: Gobierno y Ayuntamiento de Zaragoza (OCDS) | 267.292 | 13 MB |
| `asturias.zip` | Asturias, contratación centralizada del Principado | 375.380 | 17 MB |
| `canarias.zip` | Canarias: Gobierno, Las Palmas de GC, Cabildo de Tenerife y resúmenes del SCS | 115.318 | 11 MB |
| `castilla_la_mancha.zip` | Castilla-La Mancha: menores de la Junta, caja pagadora, SESCAM, sector público y UCLM | 6.945.081 | 168 MB |
| `castilla_leon.zip` | Castilla y León: Junta y SACYL | 185.058 | 11 MB |
| `catalunya.zip` | Cataluña, contratación (14 tablas) + `v2026.02/` | 8.048.140 | 1,08 GB |
| `catalunya_menores.zip` | Cataluña, contratos menores de la PSCP + `v2026.02/` | 1.429.086 | 307 MB |
| `valencia.zip` | C. Valenciana: REGCON, DANA, convenios, lobbies y entidades + `v2026.02/` | 302.592 | 124 MB |
| `valencia_menores.zip` | C. Valenciana: menores de universidades, Ajuntament de València y Diputación de Alicante | 1.882.091 | 57 MB |
| `extremadura.zip` | Extremadura, Registro de Contratos de la Junta | 221.749 | 8 MB |
| `contratos_galicia.zip` | Galicia, Contratos Públicos de Galicia + `v2026.02/` | 3.697.540 | 96 MB |
| `la_rioja.zip` | La Rioja, contratos menores del Gobierno | 351.203 | 7 MB |
| `comunidad_madrid.zip` | Comunidad de Madrid, portal de contratación | 4.906.068 | 142 MB |
| `madrid_ayuntamiento.zip` | Ayuntamiento de Madrid (tabla unificada y tabla fiel) | 264.174 | 27 MB |
| `murcia.zip` | Región de Murcia: CARM y Servicio Murciano de Salud | 1.008.568 | 19 MB |
| `euskadi.zip` | País Vasco: KontratazioA y Open Data Euskadi (9 tablas) + `v2026.02/` | 2.753.290 | 403 MB |
| `municipios.zip` | Contratos menores de Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife | 289.747 | 17 MB |
| `calidad_licitaciones_resultado.zip` | Indicadores de calidad sobre la PLACSP: el Parquet de v2026.02, sin regenerar | (8.693.891) | 753 MB |

Las filas son las de los ficheros de v2026.10 de cada ZIP, sin la carpeta `v2026.02/`; las de la última fila, entre paréntesis, son de v2026.02.

**Comprobar y descomprimir**, con los ZIP, `SHA256SUMS.txt` y `MANIFEST.csv` en la misma carpeta:

```bash
sha256sum -c --ignore-missing SHA256SUMS.txt          # macOS: shasum -a 256 -c SHA256SUMS.txt
for z in *.zip; do unzip -q "$z" -d "${z%.zip}"; done  # cada ZIP en su carpeta
```

- **La tabla principal de la PLACSP va partida.** GitHub no admite ficheros de 2 GB o más, así que `licitaciones_completo.parquet` (10.907.567 filas, 80 columnas) va partida por año en 6 Parquet, en la carpeta `licitaciones_completo/` de los 5 `nacional_licitaciones_*.zip`. Juntas se leen como una sola tabla (ver [uso rápido](#uso-rápido)), con las mismas filas, columnas y valores que el original.
- **Carpeta `v2026.02/`.** Para que v2026.10 no tenga menos datos que v2026.02, algunos ZIP llevan ahí, tal cual, los ficheros de v2026.02 que la descarga actual ya no cubre: 473 ficheros. La columna `nota` del `MANIFEST.csv` de cada ZIP dice por qué está cada uno. Detalle en el [CHANGELOG](CHANGELOG.md#v202610---2026-10-05) y en la ficha de cada fuente.
- En Windows: `Get-FileHash .\ted.zip -Algorithm SHA256` y compáralo con su línea de `SHA256SUMS.txt`.

**Solo el código:** el repositorio no lleva datos, y el clon, con toda su historia, ocupa unos 160 MB.

```bash
git clone https://github.com/BquantFinance/licitaciones-espana.git
```

Los Parquet de v2026.02 que estuvieron en Git LFS hasta octubre de 2026 (6,8 GB, con los errores que corrige v2026.10: [CHANGELOG](CHANGELOG.md#v202602---2026-02-13)) siguen en la etiqueta `datos-lfs-v2026.02`. Solo se bajan si haces checkout de esa etiqueta, o de un commit anterior, con git-lfs instalado; para clonar una versión anterior sin ellos, `GIT_LFS_SKIP_SMUDGE=1 git clone …`.

## Fuentes

Cada fuente tiene su ficha en [`docs/fuentes/`](docs/fuentes/): qué trae, cómo se descarga, sus columnas, los avisos y los errores conocidos de cada versión. La licencia de cada portal está en [`DATA_LICENSE.md`](DATA_LICENSE.md).

| Fuente | Portal oficial | Script | ZIP |
|---|---|---|---|
| [PLACSP](docs/fuentes/placsp.md) | [contrataciondelsectorpublico.gob.es](https://contrataciondelsectorpublico.gob.es/) | `nacional/licitaciones.py` | `nacional_*.zip` (9) |
| [TED](docs/fuentes/ted.md) (Unión Europea, avisos de España) | [ted.europa.eu](https://ted.europa.eu/), [data.europa.eu](https://data.europa.eu/data/datasets/ted-csv) | `ted/ted_module.py` | `ted.zip` |
| [BORME](docs/fuentes/borme.md) | [boe.es](https://www.boe.es/diario_borme/) | `borme/scripts/` | `borme.zip` |
| [BOE, sección III](docs/fuentes/boe_otras_disposiciones.md) (convenios) | [boe.es](https://www.boe.es/datosabiertos/) | `boe/boe_otras_disposiciones.py` | — (aún no en la release) |
| [GLEIF](docs/fuentes/gleif.md) (LEI y grupos empresariales) | [gleif.org](https://www.gleif.org/en/lei-data/gleif-golden-copy) | `gleif/gleif_espana.py` | — (aún no en la release) |
| [Andalucía](docs/fuentes/andalucia.md) | Buscador de contratación de la Junta | `scripts/ccaa_andalucia.py` | `andalucia.zip` |
| [Andalucía, menores](docs/fuentes/andalucia_menores.md) | [CKAN de la Junta](https://www.juntadeandalucia.es/datosabiertos/portal.html) | `scripts/ccaa_andalucia_menores.py` | `andalucia_menores.zip` |
| [Aragón](docs/fuentes/aragon.md) | [opendata.aragon.es](https://opendata.aragon.es/), OCDS de Zaragoza | `scripts/ccaa_aragon.py` | `aragon.zip` |
| [Asturias](docs/fuentes/asturias.md) | [sede.asturias.es](https://sede.asturias.es/) | `scripts/ccaa_asturias.py` | `asturias.zip` |
| [Canarias](docs/fuentes/canarias.md) | [datos.canarias.es](https://datos.canarias.es/) y portales de transparencia | `scripts/ccaa_canarias.py` | `canarias.zip` |
| [Cantabria](docs/fuentes/cantabria.md) | [transparencia.cantabria.es](https://transparencia.cantabria.es/contratacion-publica) | `scripts/ccaa_cantabria.py` | — (aún no en la release) |
| [Castilla-La Mancha](docs/fuentes/castilla_la_mancha.md) | [contratacion.castillalamancha.es](https://contratacion.castillalamancha.es/), UCLM | `scripts/ccaa_castilla_la_mancha.py` | `castilla_la_mancha.zip` |
| [Castilla y León](docs/fuentes/castilla_leon.md) | [datosabiertos.jcyl.es](https://datosabiertos.jcyl.es/) | `scripts/ccaa_castilla_leon.py` | `castilla_leon.zip` |
| [Cataluña](docs/fuentes/catalunya.md) (RPC, PSCP, Generalitat, Barcelona, COVID, Tribunal) | [analisi.transparenciacatalunya.cat](https://analisi.transparenciacatalunya.cat/), Open Data BCN | `scripts/ccaa_cataluna.py` + `_parquet.py` | `catalunya.zip` |
| [Cataluña, menores de la PSCP](docs/fuentes/catalunya.md#contratos-menores-de-la-pscp) | [contractaciopublica.cat](https://contractaciopublica.cat) | `scripts/ccaa_cataluna_contratosmenores.py` | `catalunya_menores.zip` |
| [Cataluña, RELIC](docs/fuentes/relic.md) (prohibiciones de contratar) | [analisi.transparenciacatalunya.cat](https://analisi.transparenciacatalunya.cat/d/t3wj-j4pu) | `relic/relic_cataluna.py` | — (aún no en la release) |
| [C. Valenciana](docs/fuentes/valencia.md) (REGCON) | [dadesobertes.gva.es](https://dadesobertes.gva.es/) | `scripts/ccaa_valencia.py` + `_parquet.py` | `valencia.zip` |
| [C. Valenciana, menores](docs/fuentes/valencia_menores.md) | UV, Ajuntament de València, Diputación de Alicante, UA, UMH y UPV | `scripts/ccaa_valencia_menores.py` | `valencia_menores.zip` |
| [C. Valenciana, REGIA](docs/fuentes/regia.md) (grupos de interés y reuniones) | [dadesobertes.gva.es](https://dadesobertes.gva.es/) | `regia/regia_valencia.py` | — (aún no en la release) |
| [Extremadura](docs/fuentes/extremadura.md) | [juntaex.es](https://www.juntaex.es/) | `scripts/ccaa_extremadura.py` | `extremadura.zip` |
| [Galicia](docs/fuentes/galicia.md) | [contratosdegalicia.gal](https://www.contratosdegalicia.gal/) | `galicia/scraper_galicia.py` | `contratos_galicia.zip` |
| [La Rioja](docs/fuentes/la_rioja.md) | [web.larioja.org/dato-abierto](https://web.larioja.org/dato-abierto) | `scripts/ccaa_la_rioja.py` | `la_rioja.zip` |
| [Comunidad de Madrid](docs/fuentes/comunidad_madrid.md) | [contratos-publicos.comunidad.madrid](https://contratos-publicos.comunidad.madrid/) | `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | `comunidad_madrid.zip` |
| [Ayuntamiento de Madrid](docs/fuentes/madrid_ayuntamiento.md) | [datos.madrid.es](https://datos.madrid.es/) | `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | `madrid_ayuntamiento.zip` |
| [Melilla](docs/fuentes/melilla.md) (menores de la Ciudad Autónoma) | [Perfil del contratante en la PLACSP](https://contrataciondelestado.es/wps/poc?uri=deeplink:perfilContratante&idBp=QOY179oppWwQK2TEfXGy%2BA%3D%3D), pestaña «Documentos» | `melilla/melilla_menores.py` | — (aún no en la release) |
| [Región de Murcia](docs/fuentes/murcia.md) | [datosabiertos.carm.es](https://datosabiertos.regiondemurcia.es/), [transparencia.carm.es](https://transparencia.carm.es/) | `scripts/ccaa_murcia.py` | `murcia.zip` |
| [País Vasco](docs/fuentes/euskadi.md) | [contratacion.euskadi.eus](https://www.contratacion.euskadi.eus/), [opendata.euskadi.eus](https://opendata.euskadi.eus/) | `Euskadi/ccaa_euskadi.py` + `consolidacion_euskadi.py` | `euskadi.zip` |
| [8 ayuntamientos](docs/fuentes/municipios.md) (menores) | Portal de cada ayuntamiento | `scripts/municipios_menores.py` | `municipios.zip` |
| [Calidad](docs/fuentes/calidad.md) (indicadores sobre la PLACSP) | Calculado por el proyecto | `calidad/calidad_licitaciones.py` | `calidad_licitaciones_resultado.zip` |

## Cobertura por comunidad

La PLACSP cubre a todas las administraciones que publican en ella, en todas las comunidades, desde 2012. Lo que añaden las fuentes autonómicas son, sobre todo, los **contratos menores**, que muchas comunidades y ayuntamientos publican solo en su portal. Columna «menores»: estimación de los menores publicados que recoge el repositorio, de [`docs/COBERTURA.md`](docs/COBERTURA.md#01-veredicto-2026-09-27) (septiembre de 2026).

| Comunidad | Fuente propia en la release | Años | Menores | Lo que falta |
|---|---|---|---|---|
| Andalucía | `andalucia.zip`, `andalucia_menores.zip`; Málaga y Córdoba en `municipios.zip` | 2008-2026 | Sí: Junta y SAS | Universidades, capitales y diputaciones de Granada y Huelva |
| Aragón | `aragon.zip` (Gobierno y Ayuntamiento de Zaragoza) | 2009-2025 | Parcial (~30 %) | Ayuntamiento de Zaragoza y DPZ, universidad |
| Asturias | `asturias.zip`; Gijón en `municipios.zip` | 2019-2024 | Sí: el Principado hasta 2024 | 2025-2026, que el Principado aún no publicaba (404) |
| Canarias | `canarias.zip`; Santa Cruz de Tenerife en `municipios.zip` | 2016-2026 | Parcial (~30-35 %) | El Servicio Canario de la Salud solo publica resúmenes |
| Cantabria | No: solo la PLACSP (la base de datos de contratos del Gobierno, `scripts/ccaa_cantabria.py`, aún no está en la release) | — | Parcial (la PLACSP: el 92-96 % de los menores del Gobierno) | Ayuntamientos que no cargan sus menores en la PLACSP; ofertas e invitados de los menores, que solo trae la base de datos del Gobierno |
| Castilla-La Mancha | `castilla_la_mancha.zip` | 2015-2026 | Parcial (~85 %) | SESCAM (por línea de factura, sin NIF), ayuntamientos |
| Castilla y León | `castilla_leon.zip`; Valladolid en `municipios.zip` | 2005-2026 | Sí: la Junta (~100 %) | León, Salamanca, Ponferrada, diputaciones de Burgos y Ávila |
| Cataluña | `catalunya.zip`, `catalunya_menores.zip` | 2014-2026 | Sí (~95 % dentro de la ventana de 5 años del RPC) | Antes de 2021, solo lo guardado; ayuntamientos que solo publican documentos |
| Comunidad de Madrid | `comunidad_madrid.zip`, `madrid_ayuntamiento.zip`; Fuenlabrada y Leganés en `municipios.zip` | 2011-2026 | Sí | Menores de entidades históricas; otros ayuntamientos y universidades |
| C. Valenciana | `valencia.zip`, `valencia_menores.zip` | 2013-2026 | Parcial (~50 %) | Departamentos de salud, Elche, Castelló, Diputació de València, UJI |
| Extremadura | `extremadura.zip` | 2022-2026 | Sí desde 2022 (~90 %) | 2016-2021, la universidad y los ayuntamientos |
| Galicia | `contratos_galicia.zip`; Vigo en `municipios.zip` | 2008-2026 | Sí (~92-100 %) | Concellos fuera de la PLACSP |
| Illes Balears | No: solo la PLACSP | — | Parcial (~15 %, la PLACSP) | Govern e IB-Salut, Palma, UIB |
| La Rioja | `la_rioja.zip` | 2018-2026 | Sí: el Gobierno (~100 %) | Universidad, empresas públicas, Parlamento y ayuntamientos |
| Navarra | No: solo la PLACSP | — | No | Su ley foral (art. 102.3) solo obliga a publicar los menores agregados por empresa y trimestre |
| País Vasco | `euskadi.zip` | 2011-2026 | Sí, con la API de KontratazioA (~98 %) | Bilbao y Donostia (PDF), Barakaldo |
| Región de Murcia | `murcia.zip` | 2014-2025 | Parcial (~85-90 %) | UPCT y ayuntamientos |
| Ceuta | No: solo la PLACSP | — | La PLACSP | — |
| Melilla | No: solo la PLACSP (las relaciones trimestrales de menores de la Ciudad, `melilla/melilla_menores.py`, aún no están en la release) | 2019-2026 | No (~0 %): la Ciudad publica sus menores como relaciones trimestrales en XLS (y PDF hasta 2021) en la pestaña «Documentos» de su perfil | Los entes y empresas de la Ciudad que no publican en esas relaciones |

**No hay cobertura del 100 % de los menores:** estimamos entre el 55 y el 65 % de los publicados en España, y no existe un denominador oficial completo. Veredicto, inventario de fuentes y plan por comunidad: [`docs/COBERTURA.md`](docs/COBERTURA.md).

## Esquema

- **Columnas y tipos de cada fichero:** [`docs/esquema/`](docs/esquema/README.md), una página por ZIP, generada desde los pies de los Parquet de la release con [`scripts/esquema_release.py`](scripts/esquema_release.py) y comprobada contra su `MANIFEST.csv`.
- **Marcas `_…`:** qué significa cada una, quién la pone y cómo filtrar, en [`docs/ESQUEMA.md`](docs/ESQUEMA.md). Las propias de una sola fuente, en su ficha de [`docs/fuentes/`](docs/fuentes/).

Las tres que más importan:

- **Control de versiones** ([`comun/historico.py`](comun/historico.py)): `_primera_descarga`, `_ultima_descarga` y `_en_ultima_descarga` (`False` = la fuente ya no la sirve o la ha cambiado). `_origen = 'release v2026.02'`: la fila viene de la release anterior ([más](docs/ESQUEMA.md#control-de-versiones)).
- **PLACSP**: `n_versiones`, `es_ultima_version` y `entrada_repetida` ([ficha](docs/fuentes/placsp.md#correcciones-en-los-datos-placsp)).
- **Importes corregidos**: `<campo>_corregido` y `correccion_<campo>`, junto al publicado ([calidad](docs/fuentes/calidad.md#importes-publicados-y-corregidos-issue-22)).

## Uso rápido

Con cada ZIP descomprimido en su carpeta, como arriba. Los avisos van en el propio código: son los tres errores más fáciles de cometer con estos datos.

```python
import glob
import sys

import duckdb
import pandas as pd
import pyarrow.dataset as ds

sys.path.insert(0, "licitaciones-espana")  # el repositorio clonado, para usar sus funciones
from calidad import correcciones
from ted.ted_module import avisos_para_cruce

PLACSP = "nacional_licitaciones_*/licitaciones_completo/*.parquet"  # las 6 partes son una sola tabla

# 1. PLACSP: una fila por cada versión publicada de una licitación (10,9 M filas de 5,2 M licitaciones).
#    Para contar o sumar licitaciones, quédate con la última versión: es_ultima_version.
duckdb.sql(f"""
    SELECT ano, count(*) AS licitaciones
    FROM '{PLACSP}'
    WHERE es_ultima_version
    GROUP BY ano ORDER BY ano
""").show()

# 2. Importes: se sirven tal como se publican, también los erróneos (una coma decimal perdida
#    multiplica por 100). Para sumas y rankings, usa el corregido, que va al lado con su motivo.
#    En la tabla principal, la adjudicación es la del primer lote (los demás, en nacional_resultados).
columnas = ["id", "organo_contratante", "importe_sin_iva", "importe_con_iva",
            "importe_adjudicacion", "importe_adj_con_iva", "valor_estimado_contrato"]
ultimas = (ds.dataset(sorted(glob.glob(PLACSP)))
           .to_table(columns=columnas, filter=ds.field("es_ultima_version"))
           .to_pandas())
ultimas = ultimas.join(correcciones.corregir_importes(ultimas, correcciones.cargar_registro()))
print(ultimas.groupby("organo_contratante")["importe_adjudicacion_corregido"].sum().nlargest(10))

# 3. TED: varias filas por aviso (una por oferta ganadora de cada lote) y varias versiones de un
#    mismo aviso. Para contarlos: la última versión de cada aviso y, después, sin los cancelados.
ted = pd.read_parquet("ted/ted_es_can.parquet")
print(avisos_para_cruce(ted).groupby("year")["ted_notice_id"].nunique())
```

Hace falta `pip install duckdb pandas pyarrow`. Más ejemplos, con los avisos de cada fuente, en su ficha de [`docs/fuentes/`](docs/fuentes/).

## Medir en tu máquina

¿Te basta tu ordenador? [`docs/MEDIR.md`](docs/MEDIR.md) da los tiempos y la memoria de las consultas típicas sobre toda la release en tres máquinas (con 2 CPU y 4 GB, todas bajan de 7 s con DuckDB) y cómo medirlo en la tuya con [`scripts/medir_consultas.py`](scripts/medir_consultas.py).

## Reproducir

Todo se puede volver a descargar desde los portales oficiales con los scripts del repositorio. [`docs/REPRODUCIR.md`](docs/REPRODUCIR.md) explica los requisitos, cómo se ejecuta cada script, cómo completar con la release anterior (`--semilla`) para no perder lo que la fuente ya no sirve, y qué ZIP sale de cada uno. Los scripts están cerrados y revisados: [`docs/ESTADO_SCRAPERS.md`](docs/ESTADO_SCRAPERS.md).

```bash
pip install -r requirements.txt -r requirements-dev.txt -c constraints.txt
python -m pytest -q -p no:cacheprovider     # sin red: los portales se simulan
```

## Calidad

[`calidad/calidad_licitaciones.py`](calidad/calidad_licitaciones.py) evalúa la PLACSP con **21 indicadores** de validez, consistencia y fiabilidad: 19 sobre la propia PLACSP, uno que la cruza con el TED (INT-COM-31, formalización de los contratos SARA en el DOUE) y otro con el BORME (INT-CONS-18, el adjudicatario existe en el Registro Mercantil). Los identificadores son los del sistema de indicadores de transparencia de la Fundación Hay Derecho, salvo PROPIO-FIA-01, que es del proyecto ([numeración](docs/fuentes/calidad.md#numeración-de-los-indicadores)). Al lado de cada importe deja su versión corregida y el motivo ([`calidad/correcciones.py`](calidad/correcciones.py) y el registro de errores verificados de la fuente, [`calidad/errores_fuente.csv`](calidad/errores_fuente.csv)).

`calidad_licitaciones_resultado.zip` es el Parquet de v2026.02 sin regenerar: 20 indicadores con la numeración anterior (el de salto de escala, hoy PROPIO-FIA-01, es posterior), calculados sobre los datos nacionales de v2026.02, con sus errores, y sin los importes corregidos. Resultados, reglas y ejemplos: [ficha de calidad](docs/fuentes/calidad.md).

## Licencias

- **Código:** [MIT](LICENSE).
- **Lo que añade este proyecto a los datos** (columnas `_…`, versiones de la PLACSP, valores `<campo>_corregido`, indicadores de calidad, `persona_hash`…): [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.es).
- **Los datos de origen** conservan la licencia o las condiciones de reutilización de cada portal, casi siempre con la obligación de citar la fuente. [`DATA_LICENSE.md`](DATA_LICENSE.md) las recoge fuente a fuente, con el enlace a cada una, e incluye los portales que no publican licencia para sus datos.
- **Datos personales.** Algunas fuentes publican nombres y NIF de personas físicas, y los cargos del BORME van **seudonimizados**, no anonimizados ([ficha del BORME](docs/fuentes/borme.md)). Su reutilización está sujeta al RGPD.

## Cómo citar

[`CITATION.cff`](CITATION.cff) (en GitHub, «Cite this repository»). Por ejemplo:

> Sánchez Vidal, G. (BQuant Finance). *licitaciones-espana: datos abiertos de la contratación pública española*, v2026.10, 2026. https://github.com/BquantFinance/licitaciones-espana

## Contribuir

[`CONTRIBUTING.md`](CONTRIBUTING.md) explica cómo preparar el entorno, pasar los tests con pandas 3 y 2.2, proponer una fuente nueva o avisar de un error en los datos. También están el [código de conducta](CODE_OF_CONDUCT.md) y cómo avisar de un problema de [seguridad](SECURITY.md) o de un dato personal publicado por error.

## Cambios

Los cambios de cada release, y los errores conocidos de v2026.02 que corrige v2026.10, están en el [CHANGELOG](CHANGELOG.md).

---

[@Gsnchez](https://twitter.com/Gsnchez) · [BQuant Finance](https://bquantfinance.com)
