# BORME: Registro Mercantil

- **En la release:** `borme.zip`: `borme_empresas_pub.parquet` (9.610.713 actos) y `borme_cargos_pub.parquet` (17.774.035 cargos, con la persona seudonimizada).
- **Script:** los de [`borme/scripts/`](../../borme/scripts/).
- **Portal:** [boe.es](https://www.boe.es/diario_borme/), PDF de la sección A y API de sumarios.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#nacional-y-unión-europea).

Datos del [Boletín Oficial del Registro Mercantil](https://www.boe.es/diario_borme/) parseados desde ~126.000 PDFs (2009-2026). Permite cruzar relaciones societarias con contratación pública para detectar anomalías.

| Conjunto | Registros | Contenido |
|----------|-----------|-----------|
| Empresas | 9.61M filas, 3.43M empresas (v2026.10) | Actos mercantiles: constituciones, disoluciones, fusiones, ampliaciones de capital... |
| Cargos | 17.8M filas, 3.92M códigos de persona (v2026.10) | Nombramientos, ceses, revocaciones — con la persona seudonimizada (`persona_hash`) |

> ⚠️ Los PDFs originales no se redistribuyen porque contienen nombres de personas físicas protegidos por el RGPD. Se publica el scraper para descargarlos directamente desde boe.es y los datos derivados con las personas **seudonimizadas**: solo se sustituye el nombre de cada persona de los cargos por un código, `persona_hash` (SHA-256 del nombre con una sal, truncado a 16 caracteres hexadecimales); empresa, domicilio social y actos se publican tal como aparecen en el BORME.
>
> **Seudonimizado no es anonimizado.**
> - El código es el mismo para el mismo nombre, en cualquier fecha y empresa: es lo que permite seguir a un administrador entre empresas.
> - Por eso **junta a los homónimos**: dos personas que se llaman igual comparten código y cuentan como una sola. Y al revés: una misma persona escrita de dos formas (con o sin tilde, en otro orden) tiene dos códigos. Los 3.92M códigos de persona de la tabla no son 3.92M personas.
> - No impide reconocer a una persona cuyo nombre ya se conoce. Trata `borme_cargos_pub` como datos personales.
>
> Los datos publicados solo contienen la sección A. `borme_scraper.py` ahora combina los PDF del índice HTML con la API oficial de sumarios (secciones A, B y C). En v2026.02 faltaban los boletines 2012 #173-174, 2013 #1 y 2024 #89-90; v2026.10 ya trae 2012 #173 y 2013 #1, y siguen faltando 2012 #174 y 2024 #89-90.
>
> En v2026.10, los actos del 2009-01-02 al 2026-02-17 vienen del release v2026.02 (`_origen='release v2026.02'`, con los errores conocidos de abajo), salvo los días 7, 10 y 11 de septiembre de 2012 y 2 de enero de 2013; los del 2026-02-18 en adelante salen de PDF nuevos.

## Archivos

```
borme/
├── data/
│   ├── borme_empresas_pub.parquet     # v2026.02 (etiqueta datos-lfs-v2026.02): 9,2 M actos; en v2026.10 (borme.zip), 9,61 M
│   └── borme_cargos_pub.parquet       # v2026.02 (etiqueta datos-lfs-v2026.02): 17 M cargos; en v2026.10, 17,8 M (persona_hash seudonimizado)
└── scripts/
    ├── borme_scraper.py               # Descarga PDFs desde boe.es
    ├── borme_batch_parser.py          # Extrae actos mercantiles de los PDFs
    ├── borme_validate.py              # Validación del parser
    ├── borme_anonymize.py             # Genera las versiones públicas (personas seudonimizadas)
    └── borme_placsp_match.py          # Cruza BORME × PLACSP → flags de anomalías
```

## El calificador de cada acto (`actos_detalle`)

`actos` es la lista de actos de cada anuncio («Disolución|Extinción»). El BORME dice además de qué tipo es cada uno, y la lista lo pierde: una extinción por fusión y una por liquidación se ven igual. `actos_detalle` lo guarda, con un elemento por acto, en el mismo orden y con el mismo separador que `actos`:

```
actos          Disolución|Extinción
actos_detalle  Disolución=Fusion|Extinción=
```

| Acto | Qué se guarda | Ejemplos |
|------|---------------|----------|
| Disolución | el tipo | `Voluntaria`, `Judicial`, `Fusion`, `Escision`, `Otras Causas`, `Cesión global de activo y pasivo` |
| Fusión por absorción, Escisión, Transformación de sociedad | las sociedades que nombra | `Sociedades absorbidas: X SL. Y SL` |
| Situación concursal | por cada resolución: tipo, calificador y fecha de la resolución, separadas por `; ` | `Auto de declaración de concurso. Voluntario (2026-05-04); Sentencia de aprobación del convenio (2026-07-01)` |
| Auto de declaración de concurso / de conclusión del concurso | el calificador | `Voluntario`, `Necesario`; `Insuficiencia de la masa activa`, `Cumplimiento de convenio` |
| Otros conceptos | solo las palabras clave que contiene, no el texto | `concurso,liquidación` |
| Los demás (cargos incluidos) | nada | `Nombramientos=` |

Sin datos de personas: no se guardan cargos, juzgados, jueces, administradores concursales ni el texto libre de «Resoluciones:», y un calificador con forma de nombre de persona (dos palabras seguidas en mayúsculas) se sustituye por `(omitido)`. `borme_anonymize.py` lo publica.

La columna sale de los PDF parseados con esta versión del parser. Las filas que vienen del release v2026.02 (`_origen`) no la tienen (nula): para rellenarla hacen falta los PDF de esos días.

## Datos registrales de cada anuncio

Cada anuncio termina con sus datos registrales: «Datos registrales. T 4343, L 3253, F 188, S 8, H MA 92480, I/A 5 (11.02.21).». El parser los separa en columnas, solo las que trae el anuncio:

| Columna | Dato | Ejemplos |
|---------|------|----------|
| `tomo` | T | `4343` |
| `libro` | L (solo en algunos registros) | `3253` |
| `folio` | F | `188` |
| `seccion` | S | `8`, `3` |
| `hoja_registral` | H: la hoja de la sociedad en su registro | `MA 92480`, `SE142429`, `BI 2950,A`, `VI 20755,BIS` |
| `inscripcion` | I/A: número de la inscripción o letra de la anotación | `5`, `6 D`, `N 2`, `A` |
| `fecha_inscripcion` | fecha de la inscripción, como la publica el BORME | `11.02.21`, `5.04.19`, `05.05.2025` |

- Desde 2024 en unos registros, y en todos desde 2025, el BORME publica el registro electrónico **sin tomo ni folio** («S 8 , H GC 63782, I/A 1 (13.01.25)»). El parser anterior exigía los dos y no leía la hoja de ningún anuncio desde 2025. Tampoco leía las inscripciones con letra («I/A 1 B», «I/A C»), las hojas con sufijo («H M 82294,BIS , I/ 46 .(…)»), las fechas tras una letra entre puntos («I/ 10 .M.) 2 11 18») ni el folio electrónico («I/A 5 Folio electrónico , Irus … ( 05/05/2025 )»).
- `libro`, `folio` y `seccion` son columnas nuevas. Las filas que vienen del release v2026.02 (`_origen`) traen los datos registrales que leyó aquel parser (la hoja, en el 8 % de las filas).
- La hoja identifica a la sociedad **dentro de su registro**: dos sociedades con el mismo nombre tienen hojas distintas, y una sociedad que cambia de provincia abre hoja nueva en el registro de destino.

Dos correcciones más sobre cómo se separan los anuncios:
- **Acto pegado al nombre.** En las provincias con varios registros el BORME añade «(R.M. <registro>)» al nombre, a veces sin punto detrás («HIERROS LOIS SL(R.M. A CORUÑA) Ceses/Dimisiones.»), y otras veces falta el punto tras la última palabra del nombre. El acto acababa dentro del nombre. Ahora el nombre de un acto en minúsculas tras el nombre en mayúsculas abre el cuerpo del anuncio.
- **Códigos CNAE a principio de línea.** Una línea del objeto social como «6820 - Alquiler de bienes inmobiliarios por cuenta propia» se tomaba por un anuncio nuevo: creaba una sociedad con ese texto por nombre, le pasaba los datos registrales y los cargos del anuncio de verdad y, en el primer BORME del año, hacía descartar los anuncios siguientes de 1 a 3 cifras. Ahora un número de 4 cifras o más que no continúa la numeración no abre anuncio si le sigue una palabra en minúsculas (los nombres van en mayúsculas), si la numeración continúa más adelante o si va hacia atrás.

## Detector de anomalías (BORME × PLACSP)

| Flag | Señal | Descripción |
|------|-------|-------------|
| 1 | Empresa recién creada | Constitución < 6 meses antes de adjudicación |
| 2 | Capital ridículo | Capital social < 10K€ ganando contratos > 100K€ |
| 3 | Administradores compartidos | Misma persona con cargo en varias empresas adjudicatarias. Agrupa por nombre (o por `persona_hash`, que sale del nombre): los homónimos cuentan como una sola persona |
| 4 | Disolución post-adjudicación | Disuelta < 12 meses después de cobrar |
| 5 | Adjudicación en concurso | Empresa en situación concursal recibiendo contratos |

## Pipeline

```bash
# 1. Descargar PDFs (~126K, ~6 GB)
python borme/scripts/borme_scraper.py --start 2009-01-01 --output ./borme_pdfs

# 2. Parsear → borme_empresas.parquet + borme_cargos.parquet (PRIVADOS)
python borme/scripts/borme_batch_parser.py --input ./borme_pdfs --workers 8

# 3. Seudonimizar → versiones públicas con persona_hash
python borme/scripts/borme_anonymize.py --input ./borme_pdfs --output borme/data

# 4. Detectar anomalías cruzando con PLACSP (cuenta cada licitación una vez: su versión más reciente)
python borme/scripts/borme_placsp_match.py --borme ./borme_pdfs --placsp nacional/licitaciones_completo.parquet --output ./anomalias
```

**Sin sesgo del superviviente.**
- `borme_batch_parser.py` es incremental: solo parsea los PDF nuevos o cambiados y acumula sobre la salida anterior con `acumular`.
  - Lo ya parseado no se pierde aunque falten PDF en disco.
  - Las tablas llevan `_primera_descarga`, `_ultima_descarga` y `_en_ultima_descarga`, y la versión anterior de cada tabla queda en `_historico/`.
- `--reprocesar` vuelve a parsear con el código actual todo lo que hay en disco, para aplicar correcciones del parser: datos registrales (también sin tomo ni folio), anuncios 1-999 de cada año, capital resultante, actos pegados al nombre, cortes por códigos CNAE.
- `--semilla <parquet publicado>` (empresas y cargos) añade los actos (`pdf_filename`, `num_entrada`) que no están en el parse local.
- `borme_scraper.py` nunca sobrescribe un PDF. Los que están en el manifiesto pero faltan en disco se vuelven a bajar, y `--comprobar` vuelve a pedir los ya descargados: si alguno cambia, el anterior pasa a `<día>/_historico/`.
- Errores conocidos del publicado, frente al parser actual (2.043 actos del 2024-01-04):
  - nombres con ". Sociedad unipersonal";
  - domicilios sin municipio;
  - capitales;
  - fechas de constitución con punto final;
  - datos registrales que faltan.

## Uso

```python
import duckdb

# Constituciones por año: «Constitución» como acto de la lista (actos va separado por |), no como texto suelto
duckdb.sql("""
    SELECT year(fecha_borme) AS anio, count(*) AS constituciones
    FROM 'borme/borme_empresas_pub.parquet'
    WHERE list_contains(string_split(actos, '|'), 'Constitución')
    GROUP BY 1 ORDER BY 1
""").show()

# Códigos de persona con nombramientos en más de una sociedad. persona_hash sale del nombre:
# junta a los homónimos (dos personas que se llaman igual cuentan como una) y es un dato personal.
duckdb.sql("""
    SELECT count(*) AS codigos_en_varias_sociedades FROM (
        SELECT persona_hash FROM 'borme/borme_cargos_pub.parquet'
        WHERE tipo_acto = 'nombramiento' AND empresa_norm IS NOT NULL
        GROUP BY 1 HAVING count(DISTINCT empresa_norm) > 1)
""").show()
```

## Scripts

| Script | Descripción |
|--------|-------------|
| `borme/scripts/borme_scraper.py` | Descarga ~126K PDFs del Registro Mercantil desde boe.es |
| `borme/scripts/borme_batch_parser.py` | Parser de actos mercantiles (constituciones, cargos...) |
| `borme/scripts/borme_validate.py` | Validación del parser |
| `borme/scripts/borme_anonymize.py` | Genera las versiones públicas, con las personas seudonimizadas |
| `borme/scripts/borme_placsp_match.py` | Detector de anomalías BORME × PLACSP (5 flags) |

---

[Todas las fuentes](README.md) · [README](../../README.md)
