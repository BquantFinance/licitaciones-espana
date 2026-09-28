# 🇪🇸 Datos Abiertos de Contratación Pública - España

Dataset completo de contratación pública española: nacional (PLACSP) + datos autonómicos (Andalucía, Asturias, Catalunya, Euskadi, Galicia, Valencia, Madrid) + cruce europeo (TED) + Registro Mercantil (BORME).

## 📊 Resumen de Datos

| Fuente | Registros | Período | Tamaño |
|--------|-----------|---------|--------|
| Nacional (PLACSP) | 8.7M entradas publicadas de 4.7M licitaciones (una por versión) | 2012-2026 | 780 MB |
| Andalucía | ~857K | 2016-2026 | 47 MB |
| Catalunya | 20.6M | 2014-2025 | ~180 MB |
| 🆕 Euskadi | 704K | 2005-2026 | ~160 MB |
| Valencia | 8.5M | 2000-2026 | 156 MB |
| Madrid – Comunidad | 2.56M | 2017-2025 | 90 MB |
| Madrid – Ayuntamiento | 119K | 2015-2025 | ~40 MB |
| 🆕 Galicia | 1.7M | 2007-2026 | 36 MB |
| 🆕 Asturias | 375K | 2019-2024 | 21 MB |
| TED (España) | 591K | 2010-2025 | 57 MB |
| 🆕 BORME (Registro Mercantil) | 9.2M empresas + 17M cargos | 2009-2026 | 750 MB |
| 🆕 Calidad (indicadores) | 8.7M filas × 20 indicadores (v2026.02, ver correcciones PLACSP) | 2012-2026 | 977 MB |
| **TOTAL** | **~40.4M + BORME** | **2000-2026** | **~2.3 GB** |

---

## 📥 Descarga de datos

> ⚠️ Los ficheros `.parquet` y `.csv` de este repo usan **Git LFS**. Si haces fork o descargas el ZIP del repo, solo obtendrás punteros (~130 bytes), no los datos reales.

### 👉 [Descarga directa (sin LFS) → GitHub Releases](https://github.com/BquantFinance/licitaciones-espana/releases/latest)

| ZIP | Contenido | Tamaño |
|-----|-----------|--------|
| `nacional.zip` | Licitaciones PLACSP | 1.34 GB |
| `catalunya.zip` | Datos Catalunya (contratación, subvenciones, RRHH...) | 1.06 GB |
| `ted.zip` | Tenders Electronic Daily — España | 217 MB |
| `valencia.zip` | Datos Valencia (14 categorías) | 120 MB |
| `andalucia.zip` | Contratación Junta de Andalucía | 114 MB |
| `euskadi.zip` | Contratación Euskadi | 109 MB |
| `comunidad_madrid.zip` | Contratación Comunidad de Madrid (CSV + Parquet + CSV originales) | 252 MB |
| `madrid_ayuntamiento.zip` | Actividad contractual Ayuntamiento de Madrid ⚠️ ver nota | 252 MB |
| `contratos_galicia.zip` | Contratación pública Xunta de Galicia (CM + LIC) | 34 MB |
| `asturias.zip` | Contratación centralizada Principado de Asturias | 17 MB |
| `borme.zip` | Registro Mercantil — actos mercantiles + cargos (anonimizado) | 750 MB |
| `calidad_licitaciones_resultado.rar` | Indicadores de calidad sobre PLACSP (RAR) | 690 MB |

> ⚠️ En el release `v2026.02`, `madrid_ayuntamiento.zip` es por error una copia exacta de `comunidad_madrid.zip` (mismo SHA-256): contiene los datos de la **Comunidad** de Madrid, no `actividad_contractual_madrid_completo.parquet`. Para obtener los datos del Ayuntamiento, ejecutar `comunidad_madrid/ccaa_madrid_ayuntamiento.py` hasta que se publique el ZIP correcto.
>
> ⚠️ Los datos nacionales (PLACSP) de `v2026.02` y los indicadores de calidad derivados tienen errores de columnas que afectan a cualquier suma o recuento: ver [Correcciones en los datos PLACSP](#correcciones-en-los-datos-placsp).

### Cómo obtener los datos

| Método | Instrucciones |
|--------|---------------|
| **Descarga directa** (recomendado) | Ir a [Releases](https://github.com/BquantFinance/licitaciones-espana/releases/latest) y descargar los ZIP |
| **Git clone + LFS** | `git clone` + `git lfs pull` (requiere [Git LFS](https://git-lfs.github.com/) instalado) |
| **Fork** | Tras hacer fork, ejecutar `git lfs pull` en tu copia, o descargar desde Releases |

---

## 🇪🇺 TED — Diario Oficial de la UE

Contratos publicados en [Tenders Electronic Daily](https://ted.europa.eu/) correspondientes a España. Los contratos públicos que superan cierto importe (contratos SARA) deben publicarse obligatoriamente en el DOUE.

| Conjunto | Registros | Período | Fuente |
|----------|-----------|---------|--------|
| CSV bulk | 339K | 2010-2019 | data.europa.eu |
| API v3 eForms | 252K | 2020-2025 | ted.europa.eu/api |
| **Consolidado** | **591K** | **2010-2025** | — |

> ⚠️ En los datos publicados, 2020-2023 (y ~87 % de 2023) vienen de la API sin adjudicatario, importe, nº de ofertas ni fecha de adjudicación (anuncios anteriores a eForms), las filas de la API no tienen fecha de publicación, tipo de contrato ni procedimiento, y solo se descargaban 4 de los 7 tipos de anuncio de adjudicación (faltaban `veat`, `can-tran` y `compl`). `ted_module.py` ya usa el CSV bulk de data.europa.eu para 2020-2023, conserva todas sus columnas, pide los 7 tipos y cubre de 2006 al año en curso; hay que regenerar los datos.

### Archivos

```
ted/
├── ted_module.py                    # Script de descarga TED
├── run_ted_crossvalidation.py       # Cross-validation PLACSP↔TED + matching avanzado
├── diagnostico_missing_ted.py       # Diagnóstico de missing
├── analisis_sector_salud.py         # Deep dive sector salud
├── ted_can_2010_ES.parquet          # 2010 (CSV bulk)
├── ted_can_2011_ES.parquet
├── ...
├── ted_can_2019_ES.parquet          # 2019 (CSV bulk)
├── ted_can_2020_ES_api.parquet      # 2020 (API v3 eForms)
├── ...
├── ted_can_2025_ES_api.parquet      # 2025 (API v3 eForms)
├── ted_can_2026_ES_api_en_curso.parquet  # año en curso: solo histórico, no se usa como caché
├── ted_can_<año>_registros_irregulares.csv  # registros irregulares del CSV bulk (solo si los hay)
├── _historico/                      # versiones anteriores de las cachés y del consolidado
└── ted_es_can.parquet               # Consolidado (591K, 31 MB)
```

**Sin sesgo del superviviente.**
- Las cachés por año y el consolidado se guardan con `guardar_version`: si TED cambia algo, la versión anterior pasa a `_historico/`.
- `ted_es_can.parquet` se reconstruye desde todas las versiones de cada año con `acumular`. Los avisos que TED retira o cambia quedan con `_en_ultima_descarga=False`, y un año que la ejecución no pide no se toca.
- La clave es el aviso normalizado a `número-año`: el CSV da `2020112` y la API `112-2020`, y son el mismo aviso.
- `--semilla ted_es_can.parquet` (el publicado en v2026.02) añade los avisos que ya no se sirven. **Úsalo en el primer refresco**: las cachés antiguas de la API tienen otro formato y no se comparan fila a fila.
- Para contar o cruzar, quédate con la última versión de cada aviso y sin los cancelados: `avisos_para_cruce()`. Ya lo hacen `cross_validate_ted`, `run_ted_crossvalidation.py` y los scripts de análisis.

**Lo publicado, tal cual.**
- Los avisos cancelados se conservan con `cancelled='1'` (TED usa `'0'`/`'1'`, no `'Y'`/`'N'`). Hasta septiembre de 2026 se eliminaban al descargar. Para contar adjudicaciones, exclúyelos **después** de quedarte con la última versión de cada aviso, como hace `avisos_para_cruce()`: si los quitas antes, un aviso cancelado en una descarga posterior vuelve con su versión anterior.
- El CSV bulk se lee registro a registro con el módulo `csv`. Textos como `NA`, `N/A` o `NULL` se conservan como texto; antes pandas los convertía en nulos. En 2021 eran 4 celdas y en 2019, ninguna. En los NIF limpios (`win_nif_clean`, `cae_nif_clean`) sí cuentan como vacíos.
- Los registros irregulares van a `ted_can_<año>_registros_irregulares.csv`, sean del país que sean. Se guardan con la línea en que empiezan, el motivo y todos sus campos, y con versiones en `_historico/`. Hay tres motivos:
  - `campos_de_mas`: más campos que la cabecera. No entran en la tabla.
  - `campos_de_menos`: menos campos. Si son de España, entran completados con vacíos, como con pandas.
  - `salto_de_linea`: un campo en varias líneas; una comilla sin cerrar se traga los registros que siguen.
  
  Antes se perdían sin guardarse. En los CSV de 2019 y 2021 no hay ninguno.
  - Las filas que entran en la tabla desde un registro irregular llevan el motivo en `_registro_irregular`.
  - Si el CSV de un año trae alguno, `download` guarda lo descargado y sale con código 1. Es un aviso único: el CSV de un año cerrado solo se lee una vez y después se usa la caché.

### Campos principales (57 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | ted_notice_id, notice_type, year, source, lot_id, internal_id_proc |
| Comprador | cae_name, cae_nationalid, cae_type, cae_town, buyer_legal_type, iso_country |
| Contrato | cpv, type_of_contract, top_type, is_framework, lots_number |
| Importes | importe_ted, value_euro, award_value_euro (CSV bulk), total_value, estimated_value_proc |
| Adjudicación | win_name, win_nationalid, win_country, win_size (SME), dt_award |
| Competencia | number_offers, direct_award_justification, award_criterion_type |
| Duración | duration_lot |

---

## 🔍 Cross-Validation PLACSP ↔ TED

Pipeline para validar si los contratos SARA españoles se publican efectivamente en el Diario Oficial de la UE.

### Resultados

> ⚠️ Calculado con `licitaciones_espana.parquet` de `v2026.02` contando cada entrada del ATOM como un contrato: cada versión adjudicada de una misma licitación contaba como un contrato SARA distinto, la suma de lotes por expediente sumaba versiones repetidas y "negociado sin publicidad" era en realidad el código de negociado *con* publicidad. `run_ted_crossvalidation.py` ahora cruza solo la versión más reciente de cada licitación (`es_ultima_version`; el parquet de entrada no se modifica); las cifras cambiarán al regenerar.

El cruce guarda `id`, `fecha_updated`, `_ano` y `_ted_anio_cubierto` en `crossval_sara.parquet`: calidad une el resultado por versión, y un SARA de un año sin avisos en el snapshot TED no cuenta como *missing* (ni en `_ted_missing` ni en `crossval_missing.parquet`) porque ese snapshot no puede decir si se publicó.

| Métrica | Valor |
|---------|-------|
| Contratos SARA identificados | 442,835 |
| Validados en TED | 177,892 (40.2%) |
| Missing | 257,258 |
| Missing alta confianza | 202,383 |

### Reglas SARA

Los umbrales de publicación obligatoria en TED no son un importe fijo — varían por **bienio**, **tipo de contrato** y **tipo de comprador**:

| Bienio | Obras | Servicios (AGE) | Servicios (resto) | Sectores especiales |
|--------|-------|------------------|---------------------|---------------------|
| 2016-2017 | 5,225,000€ | 135,000€ | 209,000€ | 418,000€ |
| 2018-2019 | 5,548,000€ | 144,000€ | 221,000€ | 443,000€ |
| 2020-2021 | 5,350,000€ | 139,000€ | 214,000€ | 428,000€ |
| 2022-2023 | 5,382,000€ | 140,000€ | 215,000€ | 431,000€ |
| 2024-2025 | 5,538,000€ | 143,000€ | 221,000€ | 443,000€ |

### Estrategias de matching

El matching se hace de forma secuencial — cada estrategia actúa solo sobre los registros que las anteriores no encontraron:

| # | Estrategia | Matches | % del total |
|---|-----------|---------|-------------|
| E1 | NIF adjudicatario + importe ±10% + año ±1 | 43,063 | 9.7% |
| E2 | Nº expediente + importe ±10% | 7,891 | 1.8% |
| E3 | NIF del órgano contratante + importe | 77,816 | 17.6% |
| E4 | Lotes agrupados (suma importes mismo órgano+año) | 31,365 | 7.1% |
| E5 | Nombre órgano normalizado + importe | 17,757 | 4.0% |

**Hallazgo clave**: E3 (NIF del órgano) es la estrategia más potente. TED registra el NIF del comprador; PLACSP, el del adjudicatario. Sin cruzar ambos se pierde el 17.6% de matches.

### Validación por año

```
Año     SARA    Match    %
2016   10,948    2,643  24.1%
2017   17,360    6,532  37.6%
2018   32,605   14,720  45.1%
2019   42,951   14,182  33.0%
2020   40,693    9,214  22.6%  ← COVID + baja cobertura TED
2021   47,971    7,472  15.6%
2022   56,649   22,250  39.3%
2023   60,518   31,829  52.6%
2024   59,114   38,216  64.6%  ← máximo
2025   48,276   26,920  55.8%
```

### Análisis sectorial: Salud

El sector salud representa el 17% de contratos SARA con una tasa de validación del 42.3%. El 38% del missing se explica por patrones de lotes (un anuncio TED = N adjudicaciones individuales en PLACSP). La cobertura real ajustada por lotes es ~54%.

Top órganos missing: Servicio Andaluz de Salud (4,833), FREMAP (2,410), IB-Salut (1,957), ICS (1,316), SERGAS (1,291).

### Scripts TED

| Script | Descripción |
|--------|-------------|
| `ted/ted_module.py` | Descarga TED: CSV bulk (2010-2019) + API v3 eForms (2020-2025) |
| `ted/run_ted_crossvalidation.py` | Cross-validation PLACSP↔TED con reglas SARA + matching avanzado (9 estrategias: E1-E7, E2b, E3b) → `ted/crossval_*.parquet` |
| `ted/diagnostico_missing_ted.py` | Diagnóstico de missing: falsos positivos vs gaps reales |
| `ted/analisis_sector_salud.py` | Deep dive sector salud: lotes, acuerdos marco, CPV, CCAA |

---

## 🏢 BORME — Registro Mercantil

Datos del [Boletín Oficial del Registro Mercantil](https://www.boe.es/diario_borme/) parseados desde ~126.000 PDFs (2009-2026). Permite cruzar relaciones societarias con contratación pública para detectar anomalías.

| Conjunto | Registros | Contenido |
|----------|-----------|-----------|
| Empresas | 9.2M filas, 3.3M únicas | Actos mercantiles: constituciones, disoluciones, fusiones, ampliaciones de capital... |
| Cargos | 17M filas, 3.8M personas | Nombramientos, ceses, revocaciones — con persona hasheada (SHA-256) |

> ⚠️ Los PDFs originales no se redistribuyen porque contienen nombres de personas físicas protegidos por RGPD. Se publica el scraper para descargarlos directamente desde boe.es y los datos derivados anonimizados: solo se sustituyen por un hash los nombres de las personas de los cargos; empresa, domicilio social y actos se publican tal como aparecen en el BORME.
>
> Los datos publicados solo contienen la sección A. `borme_scraper.py` ahora combina los PDF del índice HTML con la API oficial de sumarios (secciones A, B y C). Faltan los boletines 2012 #173-174, 2013 #1 y 2024 #89-90: hay que volver a descargar esos días.

### Archivos

```
borme/
├── data/
│   ├── borme_empresas_pub.parquet     # 9.2M actos mercantiles por empresa
│   └── borme_cargos_pub.parquet       # 17M cargos (persona_hash, no nombre real)
└── scripts/
    ├── borme_scraper.py               # Descarga PDFs desde boe.es
    ├── borme_batch_parser.py          # Extrae actos mercantiles de los PDFs
    ├── borme_validate.py              # Validación del parser
    ├── borme_anonymize.py             # Genera datasets públicos sin datos personales
    └── borme_placsp_match.py          # Cruza BORME × PLACSP → flags de anomalías
```

### Detector de anomalías (BORME × PLACSP)

| Flag | Señal | Descripción |
|------|-------|-------------|
| 1 | Empresa recién creada | Constitución < 6 meses antes de adjudicación |
| 2 | Capital ridículo | Capital social < 10K€ ganando contratos > 100K€ |
| 3 | Administradores compartidos | Misma persona con cargo en varias empresas adjudicatarias |
| 4 | Disolución post-adjudicación | Disuelta < 12 meses después de cobrar |
| 5 | Adjudicación en concurso | Empresa en situación concursal recibiendo contratos |

### Pipeline

```bash
# 1. Descargar PDFs (~126K, ~6 GB)
python borme/scripts/borme_scraper.py --start 2009-01-01 --output ./borme_pdfs

# 2. Parsear → borme_empresas.parquet + borme_cargos.parquet (PRIVADOS)
python borme/scripts/borme_batch_parser.py --input ./borme_pdfs --workers 8

# 3. Anonimizar → versiones públicas con persona_hash
python borme/scripts/borme_anonymize.py --input ./borme_pdfs --output borme/data

# 4. Detectar anomalías cruzando con PLACSP (cuenta cada licitación una vez: su versión más reciente)
python borme/scripts/borme_placsp_match.py --borme ./borme_pdfs --placsp nacional/licitaciones_espana.parquet --output ./anomalias
```

**Sin sesgo del superviviente.**
- `borme_batch_parser.py` es incremental: solo parsea los PDF nuevos o cambiados y acumula sobre la salida anterior con `acumular`.
  - Lo ya parseado no se pierde aunque falten PDF en disco.
  - Las tablas llevan `_primera_descarga`, `_ultima_descarga` y `_en_ultima_descarga`, y la versión anterior de cada tabla queda en `_historico/`.
- `--reprocesar` vuelve a parsear con el código actual todo lo que hay en disco, para aplicar correcciones del parser: datos registrales, anuncios 1-999 de cada año, capital resultante.
- `--semilla <parquet publicado>` (empresas y cargos) añade los actos (`pdf_filename`, `num_entrada`) que no están en el parse local.
- `borme_scraper.py` nunca sobrescribe un PDF. Los que están en el manifiesto pero faltan en disco se vuelven a bajar, y `--comprobar` vuelve a pedir los ya descargados: si alguno cambia, el anterior pasa a `<día>/_historico/`.
- Errores conocidos del publicado, frente al parser actual (2.043 actos del 2024-01-04):
  - nombres con ". Sociedad unipersonal";
  - domicilios sin municipio;
  - capitales;
  - fechas de constitución con punto final;
  - datos registrales que faltan.

---

## 🆕 Calidad de Datos — 21 Indicadores

Pipeline de calidad que aplica **21 indicadores** de validez, consistencia y fiabilidad sobre el dataset nacional (PLACSP), cruzando con TED y BORME. Al lado de cada importe publicado deja su versión corregida y el motivo ([importes publicados y corregidos](#importes-publicados-y-corregidos-issue-22)).

Evalúa todas las entradas del parquet PLACSP tal como se publican: cada fila de resultado conserva `n_versiones` y `es_ultima_version`. Con `--solo-ultima-version` evalúa solo la versión más reciente de cada una de las **4,727,478 licitaciones**.

> ⚠️ El parquet publicado en `v2026.02` (`calidad_licitaciones_resultado.parquet`, score medio 88.3) se calculó sobre las 8,7M entradas de `licitaciones_espana.parquet` sin forma de distinguir la versión vigente de cada licitación, con `importe_sin_iva` = valor estimado y con las etiquetas de procedimiento desplazadas. La columna "Corregido" es el pipeline actual con `--solo-ultima-version` sobre ese mismo parquet (una evaluación por licitación, semántica corregida de importes y procedimientos, ver [correcciones PLACSP](#correcciones-en-los-datos-placsp)): FIA-04 sigue usando la `fecha_publicacion` antigua y VAL-01 el valor estimado cuando no hay presupuesto, hasta que se reprocesen los ATOM.

### Resultados

| Indicador | % Fallo v2026.02 (8,7M entradas) | % Fallo corregido (última versión, 4,7M licitaciones) | Descripción |
|---|---|---|---|
| INT-VAL-01 | 22.6% | 0.8% | Importe de licitación en formato válido |
| INT-VAL-02 | 31.8% | 5.8% | Importe de adjudicación en formato válido |
| INT-VAL-07 | 39.2% | 12.3% | Fecha de adjudicación válida |
| INT-VAL-09 | 23.3% | 41.4% | Código CPV válido (sube porque los contratos menores, 60 % sin CPV, casi no tienen versiones: son el 38 % de las entradas pero el 68 % de las licitaciones) |
| INT-VAL-12 | 33.4% | 7.8% | NIF/CIF adjudicatario válido (checksum) |
| INT-VAL-14 | 1.3% | 0.5% | Contrato menor coherente con cuantía (LCSP art. 118); antes 162K derivados de acuerdo marco contaban como menores |
| INT-CONS-01 | 1.1% | 1.4% | Si adjudicado, nº ofertas ≥ 1 |
| INT-CONS-08 | 0.3% | 0.4% | Importe adjudicación ≤ licitación (+5%) |
| INT-CONS-18 | 51.3% | 52.4% | Adjudicatario existe en BORME (3.3M empresas) |
| INT-CONS-20 | 77.5% | pendiente | Contrato SARA publicado en TED (requiere regenerar el cruce PLACSP↔TED). Se une por versión (`id` + `fecha_updated`): solo la versión evaluada recibe resultado, y un año sin avisos en el snapshot TED (p.ej. 2026 con TED hasta 2025) queda sin evaluar, no como fallo |
| INT-FIA-01 | 0.6% | 0.7% | Nº ofertas en rango razonable (P99 por CPV) |
| INT-FIA-04 | 21.3% | 21.8% | Plazo presentación ofertas razonable (0-365 días) |
| INT-FIA-08 | 0.4% | 0.0% | PBL no outlier (≤ 50M€); antes se evaluaba el valor estimado |
| INT-FIA-09 | 0.8% | 1.2% | PA plausible por segmento CPV (P1-P99) |
| INT-FIA-12 | — | — | PBL y PA del mismo orden de magnitud (PA < 100 × PBL). Nuevo ([issue #22](https://github.com/BquantFinance/licitaciones-espana/issues/22)); en la regeneración de 2026-09 falla en 495 de 4,83M licitaciones (0,010 %) |

Los 6 indicadores restantes (formato numérico, no negativos, NUTS, trazabilidad) dan 0.0% de fallo — checks de sanidad que se aplican pero no revelan problemas.

Los indicadores se basan en el marco de calidad de PPDS, con contribuciones de Jaime Gómez-Obregón (umbrales LCSP) y OIRESCON (benchmarks PBL).

### Menores vs Regulares

Recalculado con el pipeline corregido sobre la última versión de cada licitación (en `v2026.02` se comparaban 3.3M entradas de menores con 5.4M entradas "regulares", que corresponden a 1.5M licitaciones):

| Indicador | Menores (3.2M) | Regulares (1.5M) | Diferencia |
|---|---|---|---|
| NIF adjudicatario inválido | 2.7% | 18.6% | -16.0pp |
| Sin CPV | 60.6% | 1.2% | +59.3pp |
| Sin fecha adjudicación | 0.6% | 36.9% | -36.3pp |
| Sin importe licitación | 0.0% | 2.5% | -2.5pp |

### Importes publicados y corregidos (issue #22)

Los importes se sirven tal como los publica la administración, también cuando están mal: que se publicara un importe erróneo es en sí un dato para la auditoría ciudadana. Al lado, `calidad_licitaciones_resultado.parquet` lleva la versión corregida de cada importe y el motivo (`calidad/correcciones.py`), para que un buscador pueda enseñar las dos y los agregados (rankings, cuotas, HHI) y los modelos usen la corregida. Los indicadores y el score siguen evaluando lo publicado.

Para `importe_adjudicacion`, `importe_sin_iva` y `valor_estimado_contrato`:

| Columna | Contenido |
|---|---|
| `<campo>_corregido` | El importe que usar: el publicado si no hay corrección; vacío si el publicado no es fiable y no hay estimación |
| `correccion_<campo>` | Vacío, o el motivo de la corrección |

| Motivo | Regla | Última versión, regeneración 2026-09 |
|---|---|---:|
| `registro` | Error verificado a mano en [`calidad/errores_fuente.csv`](calidad/errores_fuente.csv), con su evidencia y el valor probable. Solo cambia las versiones que aún publican el valor erróneo | 1 adjudicación y 1 valor estimado |
| `escala_x100` / `escala_x1000` | Adjudicación de 100 veces el presupuesto o más que, dividida entre 100 o 1.000, vuelve a su orden (la coma decimal perdida): con las mismas cifras que el presupuesto (desde 1.000 €) o entre el 50 % y el 105 % de él (desde 10.000 €) | 24 / 6 |
| `inverosimil` | Adjudicación de 100 veces el presupuesto o más sin corrección fiable: la corregida queda vacía | 23 |
| `no_comparable` | Presupuesto de menos de 1.000 € frente a una adjudicación de 100 veces o más. Suele ser un precio unitario o simbólico (1 €): se vacía el presupuesto corregido y la adjudicación se mantiene | 441 |

En la última versión de cada licitación, la adjudicación de la tabla principal (sin IVA; en las de varios lotes, la del primero) suma unos 525.800 M€ publicada y unos 520.130 M€ corregida. Las 23 `inverosimil` (2.485,9 M€ publicados) quedan vacías y no suman. Para sumas y rankings hay que filtrar `es_ultima_version`, porque la salida trae todas las versiones.

El caso del issue #22 es CONSTRUCCIONES URDINBERRI, S.L. (entrada `PlataformasAgregadasSinMenores/15091104`):
- publica una adjudicación de 2.357.531.666 € para una obra de 2.518.819,27 €;
- la plataforma de origen (Euskadi) da 2.593.284,83 € con IVA del 10 %, es decir, 2.357.531,67 € sin IVA;
- su total adjudicado en la tabla principal pasa de 2.381,6 M€ publicados a 26,4 M€ corregidos; contando todos sus lotes (`_resultados`, con el registro aplicado), de 2.386,4 M€ a 31,2 M€.

**Para añadir un error verificado** al registro, hay que rellenar una fila con estos campos:
- `fuente`: `placsp`, o el dataset donde se ve;
- `id` de la entrada, `campo` y `valor_publicado`;
- `valor_probable`: vacío si no se sabe;
- `certeza`: `confirmado` si la propia fuente da la prueba, `probable` si no;
- `evidencia`, `referencia` (issue o URL) y la fecha en `verificado`.

La misma corrección se aplica al cargar los datos en otro sistema:

```python
from calidad import correcciones
df = df.join(correcciones.corregir_importes(df, correcciones.cargar_registro()))
```

En las tablas de detalle (`_resultados`, un importe por lote) no hay presupuesto por fila, así que ahí solo se aplica lo del registro. Las reglas de escala por lote (con el presupuesto de `_lotes`) están pendientes: en la última versión, `_resultados` suma 2,26 millones de M€ frente a los 525.800 M€ de la tabla principal.

### Archivos

```
calidad/
├── calidad_licitaciones.py                  # Pipeline
├── correcciones.py                          # Importes corregidos junto a los publicados
├── errores_fuente.csv                       # Errores de la fuente verificados, con su evidencia
└── calidad_licitaciones_resultado.parquet   # v2026.02: 8.7M filas × 70 cols (977 MB), pendiente de regenerar
```

### Uso

```bash
# Solo indicadores base (19)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet

# Completo con TED + BORME (21 indicadores)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
  --ted ted/crossval_sara.parquet \
  --borme borme_empresas.parquet

# Una evaluación por licitación (versión más reciente)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet --solo-ultima-version

# Sin el registro de errores de la fuente (solo las reglas de escala)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet --errores-fuente ''
```

```python
import pandas as pd
df = pd.read_parquet('calidad/calidad_licitaciones_resultado.parquet')

# Contratos SARA no publicados en TED
df[df['INT-CONS-20'] == False].groupby('organo_contratante').size().nlargest(10)

# Contratos menores que superan umbral LCSP
df[df['INT-VAL-14'] == False][['expediente', 'organo_contratante', 'importe_adjudicacion']]

# Score medio por órgano
df.groupby('organo_contratante')['score_calidad'].mean().nlargest(20)

# Ranking de adjudicatarios con los importes corregidos, y lo que se ha corregido
ultima = df[df['es_ultima_version']]
ultima.groupby('adjudicatario')['importe_adjudicacion_corregido'].sum().nlargest(20)
ultima[ultima['correccion_importe_adjudicacion'].notna()][
    ['id', 'adjudicatario', 'importe_adjudicacion', 'importe_adjudicacion_corregido', 'correccion_importe_adjudicacion']]
```

---

## 🏛️ Nacional - PLACSP

Licitaciones de la [Plataforma de Contratación del Sector Público](https://contrataciondelsectorpublico.gob.es/).

| Conjunto | Licitaciones únicas | Filas en `licitaciones_espana.parquet` (v2026.02) | Período |
|----------|--------------------:|-------------------------------------------------:|---------|
| Licitaciones | 1,08M | 3,65M | 2012-actualidad |
| Agregación CCAA | 434K | 1,74M | 2016-actualidad |
| Contratos menores | 3,20M | 3,29M | 2018-actualidad |
| Encargos medios propios | 12,5K | 14,7K | 2021-actualidad |
| Consultas preliminares | 1,9K | 3,7K | 2022-actualidad |
| **Total** | **4,73M** | **8,69M** | |

### Correcciones en los datos PLACSP

Los parquet nacionales publicados hasta `v2026.02` —y todo lo calculado sobre ellos: indicadores de calidad, cruce PLACSP↔TED, detector BORME×PLACSP— tienen errores de columnas que distorsionan cualquier suma o recuento. Cifras medidas sobre el propio `licitaciones_espana.parquet`:

| Problema | Efecto | Corrección |
|----------|--------|------------|
| **Versiones sin marcar.** La PLACSP publica una entrada nueva del ATOM por cada actualización de una licitación (anuncio, adjudicación, formalización...), y nada indicaba cuál es la vigente | 8.693.891 entradas para 4.727.478 licitaciones (hasta 15+ por licitación; 90.128 son copias exactas). Sumar importes sin filtrar cuenta la misma licitación varias veces: adjudicación ×4,7 (2.223,7 frente a 476,6 B€) e `importe_sin_iva` ×8 (11.764,7 frente a 1.476,1 B€). `licitaciones_completo_2012_2026.parquet` tiene una fila por licitación, pero en 23.911 casos (0,5 %) no es la versión más reciente | Se sirven **todas** las entradas tal como las publica la PLACSP, con `n_versiones` (versiones distintas del mismo `id`, es decir, valores distintos de `fecha_updated`), `entrada_repetida` (la misma versión publicada más de una vez: se conserva y se marca) y `es_ultima_version` (una fila por `id`: la de `fecha_updated` más reciente). Para contar o sumar licitaciones: `df[df.es_ultima_version]` |
| **`importe_sin_iva` era el valor estimado** ([#6](https://github.com/BquantFinance/licitaciones-espana/issues/6)): se guardaba `EstimatedOverallContractAmount` | En el conjunto `licitaciones`, el 27 % de las filas tiene `importe_sin_iva` > `importe_con_iva`, imposible para un presupuesto sin IVA | `valor_estimado_contrato` = EstimatedOverallContractAmount; `importe_sin_iva` = TaxExclusiveAmount |
| **Etiquetas de códigos desplazadas** | "Negociado sin publicidad" etiquetaba el código 4 (negociado *con* publicidad): 41.388 filas frente a 191.039 licitaciones reales. 162.392 derivados de acuerdo marco figuraban como "Contrato menor" y los 3,3M contratos menores como "Asociación innovación". Tipos 22/32 (concesiones LCSP) sin etiqueta y 40 (colaboración público-privada) como "Concesión Servicios" | Etiquetas recalculadas desde `procedimiento_code` / `tipo_contrato_code` |
| **CPV guardado como número** | 73.903 CPV sin el cero inicial (`9134100` en lugar de `09134100`) | CPV como texto de 8 dígitos |
| **`fecha_publicacion` (y `ano`) de un anuncio posterior.** Se tomaba el primer `ValidNoticeInfo` sin mirar su tipo | En las licitaciones adjudicadas o resueltas era casi siempre la fecha del anuncio de adjudicación/formalización: en el 63 % el plazo de presentación termina *antes* de esa "publicación" (0,2 % en las que siguen en plazo). Los recuentos por año usan en realidad el año de adjudicación | Fecha del anuncio de licitación (`DOC_CN`) o, si no lo hay, del primer anuncio publicado. Solo se corrige reprocesando los ATOM |

El scraper ya genera los datos corregidos. Para corregir un parquet ya descargado sin volver a procesar los ATOM:

```bash
python nacional/normalizar_placsp.py -i nacional/licitaciones_espana.parquet \
    -o nacional/licitaciones_espana_normalizado.parquet    # ~1-2 min, ~4 GB de RAM
```

```python
import sys; sys.path.insert(0, '.')        # desde la raíz del repo
from nacional.licitaciones import leer_placsp
df = leer_placsp('nacional/licitaciones_espana.parquet')  # todas las entradas + n_versiones / es_ultima_version, semántica actual
ultimas = df[df['es_ultima_version']]                      # una fila por licitación, para sumar o contar
# o directamente: leer_placsp(ruta, solo_ultima_version=True)
```

Sobre los parquet de `v2026.02` la normalización mueve el antiguo `importe_sin_iva` a `valor_estimado_contrato` y deja `importe_sin_iva` vacío: el presupuesto sin IVA real solo se obtiene reprocesando los ZIP de la PLACSP:

```bash
# La salida, en una carpeta aparte: los parquet de nacional/ son la única copia de v2026.02
python nacional/licitaciones.py --data-dir <zips PLACSP> --output-dir <salida> --procesos 3 --sin-csv \
    --semilla nacional/licitaciones_espana.parquet --semilla nacional/licitaciones_completo_2012_2026.parquet
```

### Ejecución: memoria, histórico y semilla

- **Memoria acotada.** Los ZIP se leen sin extraerlos y las entradas se escriben por lotes (`--lote`, 50.000 por defecto): con agregación 2025 (250.652 entradas) el pico baja de 2,7 GB a 1,2 GB con la misma salida. `--procesos N` lee N ZIP a la vez y `--sin-csv` omite los CSV (con todos los conjuntos pasan de 10 GB).
- **Nada se machaca.** Un ZIP que cambia deja su versión anterior en `<data-dir>/<conjunto>/_historico/` y se leen todas las versiones: una entrada que la PLACSP deja de servir sigue en la salida con `_en_ultima_descarga=False`. La salida anterior de cada tabla pasa a `<salida>/_historico/`.
- **`--semilla`** incorpora un parquet publicado como la instantánea más antigua: solo añade, marcadas con `_origen='release v2026.02'` y `_en_ultima_descarga=False`, las filas cuya clave (`id`, `fecha_updated`) no está en la descarga, y solo de los conjuntos y años de ZIP leídos. Las 35.627 filas de `licitaciones_espana.parquet` sin `fecha_updated` (el código antiguo no leía los `atom:updated` sin milisegundos) se casan por contenido. Si coinciden con una fila de la descarga van a la tabla `_semilla_contenido`, para no perderlas ni duplicar la principal.
- **`textos_originales`** (JSON) guarda el texto publicado de cada importe o fecha que no se puede convertir (p.ej. `"fecha_adjudicacion": "0202-07-03"`): la columna queda vacía y el valor no se pierde.

### Archivos

```
nacional/
├── licitaciones.py                          # Scraper ATOM → Parquet/CSV (todas las entradas, con es_ultima_version)
├── normalizar_placsp.py                     # Corrige parquets ya generados (ver arriba)
├── licitaciones_espana.parquet              # v2026.02: 8,7M entradas = todas las versiones de 4,7M licitaciones (965 MB)
└── licitaciones_completo_2012_2026.parquet  # v2026.02: 4,7M filas, una por licitación, no siempre la más reciente (762 MB)
```

El scraper escribe `licitaciones_completo_{inicio}_{fin}.parquet/.csv`, con una fila por entrada publicada en los ATOM (`n_versiones`, `es_ultima_version`), y `licitaciones_completo_{inicio}_{fin}_resultados.parquet/.csv`, con una fila por resultado (`cac:TenderResult`, uno por lote) de cada entrada: las columnas de adjudicación de la tabla principal corresponden al **primer lote**. Además escribe `_adjudicatarios` (cada `WinningParty`), `_lotes`, `_criterios`, `_modificaciones`, `_borrados` (entradas `at:deleted-entry`) y, con `--semilla`, `_semilla_contenido`: se cruzan con la principal por `id` + `fecha_updated`.

### Campos principales

| Categoría | Campos |
|-----------|--------|
| Identificación | id, expediente, objeto, url |
| Órgano | organo_contratante, nif_organo, dir3_organo, ciudad_organo, dependencia |
| Tipo | tipo_contrato(_code), subtipo_code, procedimiento(_code), estado(_code) |
| Importes | valor_estimado_contrato, importe_sin_iva, importe_con_iva, importe_adjudicacion, importe_adj_con_iva |
| Adjudicación (1er lote) | adjudicatario, nif_adjudicatario, num_ofertas, es_pyme, fecha_adjudicacion |
| Lotes | n_lotes, n_resultados (+ tabla `_resultados`: lote, resultado_code, adjudicatario, NIF, importes, ofertas, pyme) |
| Clasificación | cpv_principal, cpvs, ubicacion, nuts |
| Fechas | fecha_publicacion, fecha_limite, fecha_adjudicacion, fecha_updated |

| Columna | Elemento CODICE | Significado |
|---------|-----------------|-------------|
| `valor_estimado_contrato` | `ProcurementProject/BudgetAmount/EstimatedOverallContractAmount` | Valor estimado: todos los lotes, prórrogas y modificaciones previstas (base de los umbrales SARA) |
| `importe_sin_iva` | `ProcurementProject/BudgetAmount/TaxExclusiveAmount` | Presupuesto base de licitación sin impuestos |
| `importe_con_iva` | `ProcurementProject/BudgetAmount/TotalAmount` | Presupuesto base de licitación con impuestos |
| `importe_adjudicacion` | `TenderResult/AwardedTenderedProject/LegalMonetaryTotal/TaxExclusiveAmount` | Adjudicado sin impuestos (primer lote) |
| `importe_adj_con_iva` | `TenderResult/AwardedTenderedProject/LegalMonetaryTotal/PayableAmount` | Adjudicado con impuestos (primer lote) |

**Procedimiento** (`procedimiento_code`): 1 Abierto · 2 Restringido · 3 Negociado sin publicidad · 4 Negociado con publicidad · 5 Diálogo competitivo · 6 Contrato menor · 7 Derivado de acuerdo marco · 8 Concurso de proyectos · 9 Abierto simplificado · 10 Asociación para la innovación · 11 Derivado de asociación para la innovación · 12 Sistema dinámico de adquisición · 13 Licitación con negociación · 100 Normas internas · 999 Otros.

**Tipo de contrato** (`tipo_contrato_code`): 1 Suministros · 2 Servicios · 3 Obras · 7 Administrativo especial · 8 Privado · 21 Gestión de servicios públicos · 22 Concesión de servicios · 31 Concesión de obras públicas · 32 Concesión de obras · 40 Colaboración público-privada · 50 Patrimonial · 999 Otros.

---

## 🏴 Catalunya

Datos del portal [Transparència Catalunya](https://analisi.transparenciacatalunya.cat) (Socrata API).

| Categoría | Registros | Período |
|-----------|-----------|---------|
| Subvenciones RAISC | 9.6M | 2014-2025 |
| **Contratación pública** | **4.3M** | 2014-2025 |
| ↳ Contratos regulares | 1.3M | 2014-2025 |
| ↳ Contratos menores 🆕 | 3.0M filas (868K distintas) | 2014-2025 |
| Presupuestos | 3.1M | 2014-2025 |
| Convenios | 62K | 2014-2025 |
| RRHH | 3.4M | 2014-2025 |
| Patrimonio | 112K | 2020-2025 |

### Archivos

```
catalunya/
├── contratacion/
│   ├── contractacio_menors.parquet          # 3.0M filas, 868K distintas (ver abajo) 🆕
│   ├── publicaciones_pscp.parquet           # Publicaciones PSCP (ciclo completo)
│   ├── adjudicaciones_generalitat.parquet
│   ├── contratos_registro.parquet, fase_ejecucion.parquet, contratacion_programada.parquet
│   ├── contratos_covid.parquet, resoluciones_tribunal.parquet
│   └── *_bcn.parquet                        # Ayuntamiento de Barcelona (menores, contratistas, perfil, modificaciones, resumen)
├── subvenciones/
│   ├── raisc_concesiones.parquet            # 9.6M registros
│   ├── raisc_convocatorias.parquet
│   └── convocatorias_subvenciones.parquet
├── presupuestos/                            # ejecución de gastos/ingresos, presupuestos aprobados
├── convenios/convenios.parquet
├── rrhh/                                    # altos cargos, retribuciones, convocatorias
├── entidades/                               # ayuntamientos, entes locales, sector público
└── territorio/                              # municipios
```

### 🆕 Contratos menores Catalunya

Dataset nuevo con **3.024.000 filas** de contratos menores del sector público catalán.

> ⚠️ En el parquet publicado solo 868.063 filas son distintas: las otras 2.155.739 son copias idénticas de la misma publicación devuelta por varias consultas de fase (artefacto de la descarga, hasta 7 copias). Además la descarga se quedaba corta frente al dataset PSCP de Socrata (`ybgg-dgi6`): p.ej. 301.614 contratos menores agregados de 2025 frente a 529.780, ICS o UPF muy por debajo, por el tope de 20.000 resultados por consulta. El script ahora solo quita filas idénticas.
- **Límite de la API:** `totalElements` nunca pasa de 10.000, así que el total no sirve para saber si falta algo. Los segmentos incompletos se detectan cuando los órdenes ascendente y descendente no se solapan.
- **Sin sesgo del superviviente:**
  - Las fases, el crudo y la salida limpia se guardan con `guardar_version`.
  - La salida limpia se acumula con `acumular`: lo retirado queda con `_en_ultima_descarga=False`, solo dentro del grupo (normales o agregadas) que se ha leído entero.
  - `--semilla` añade, por (`id`, `expedientId`), lo que ya no se sirve.
- **Pendiente:** los órganos más grandes (ICS, UPF…) desbordan la ventana y quedan fuera del ámbito hasta que haya segmentación por fecha. En los parquet publicados se perdían los ceros a la izquierda de códigos postales e INE (`08002` → 8002) y el dataset `contractes-menors-a-generica` de Barcelona no se descargaba (slug erróneo); ambos corregidos.


- **43 columnas**: `id`, `titol`, `descripcio`, `pressupostLicitacio`, `pressupostAdjudicacio`, `organ`, `idOrgan`, `codiExpedient`, `expedientId`, `esPlacsp`, `esAgregatContractes`, `esAgregatEncarrecs`, `nomPublicacioAgregada` y `fasesVigents_<FASE>_{lotsActius,dataPublicacio,idPublicacio}` para 10 fases (no incluye nombre ni NIF del adjudicatario)
- Una fila por publicación y contrato, con las fases vigentes de cada una en columnas (una publicación agregada contiene varios contratos, que se distinguen por `expedientId`)
- Extraído de la API del portal de contratación pública (`contractaciopublica.cat`, `portal-api`) mediante paginación con sub-segmentación automática (72K requests API)
- Fuente: [Plataforma de Serveis de Contractació Pública](https://contractaciopublica.cat)

**Menores con NIF del adjudicatario.** `publicaciones_pscp.parquet` (Socrata `ybgg-dgi6`) trae 1.048.149 filas con `procediment = 'Contracte menor'` y NIF en el 99,7-100 %: ~350.000 al año en 2023-2024 (locales, Generalitat y universidades), estructurados desde el 2.º semestre de 2022. El RPC (`contratos_registro.parquet`) trae también los de menos de 5.000 €, pero solo con el nombre del adjudicatario.

**Ventanas móviles.** El RPC (`hb6v-jcbf`) y los menores de la Generalitat (`qjue-2pk9`, 2020-2024, importes en céntimos tal como se publican; antes se pedía `ydq4-xy5b`, que da 404) solo sirven los últimos 5 años. `ccaa_cataluna.py` guarda la versión anterior de cada CSV en `_historico/` y `ccaa_cataluna_parquet.py` construye cada parquet con todas las versiones: lo que sale de la ventana sigue con `_en_ultima_descarga=False`.

---

## 🆕 Euskadi

Contratación pública del [País Vasco / Euskadi](https://www.contratacion.euskadi.eus/), combinando la API REST de KontratazioA con exports XLSX históricos de Open Data Euskadi y portales municipales independientes (Bilbao, Vitoria-Gasteiz). Arquitectura API-first con fallback a XLSX para series históricas.

| Dataset | Registros | Período | Fuente |
|---------|-----------|---------|--------|
| Anuncios de contratación (metadatos) | 664,545 | 2011-2026 | XLSX anual + JSON 2011-2013 |
| Poderes adjudicadores | 919 | Actual | API REST KontratazioA |
| Empresas licitadoras | 9,017 | Actual | API REST KontratazioA |
| REVASCON histórico (con importes) | 34,523 | 2013-2018 | CSV/XLSX agregado anual |
| Bilbao contratos | 4,823 | 2005-2026 | Portal municipal Bilbao |
| Vitoria contratos menores | — | Actual | Open Data Euskadi (no se consolida) |
| **Total** | **~704K** | **2005-2026** | — |

> ⚠️ Los parquet publicados de Euskadi tienen errores de consolidación ya corregidos en los scripts (hay que regenerarlos): en `revascon_historico` 31.191 de las 34.523 filas (REVASCON 2015-2018) salieron como columnas `unnamed:_N` porque el XLSX trae filas de título antes de la cabecera; en `bilbao_contratos` los importes están divididos entre 1.000 (`"52.990"` → 52,99) y la fecha de adjudicación tiene día y mes invertidos; `contratos_master` tiene los años 2011-2013 en columnas aparte y 18.826 filas de `contratos_2021.xlsx` con las columnas corridas 2-3 posiciones (URL en la fecha límite, expediente en la fecha de publicación…). La consolidación actual conserva todas las filas y celdas de los ficheros originales (verificado fichero a fichero): recoloca esas filas y lo indica en `_columnas_corridas`, y las filas repetidas (los JSON 2012-2013 repiten filas del de 2011; REVASCON repite contratos entre años; la API de empresas devuelve 25 empresas dos veces) se conservan marcadas en `_duplicado`. `contratos_master` son **metadatos de anuncios**: ninguna fuente de B1 incluye importes, adjudicatario, NIF, CPV ni procedimiento, así que para 2019-2026 no hay importes de adjudicación de Euskadi en los datos publicados. La API `/contracts` de KontratazioA tiene 655.518 contratos con importe y adjudicatario, pero el scraper solo obtenía una muestra de 10 (la API repetía la página 1); ver [docs/COBERTURA.md](docs/COBERTURA.md).

### Archivos

```
Euskadi/
├── euskadi_parquet/
│   ├── contratos_master.parquet         # 664K anuncios (138 MB)
│   ├── poderes_adjudicadores.parquet    # 919 poderes adjudicadores
│   ├── empresas_licitadoras.parquet     # 9K empresas del registro
│   ├── revascon_historico.parquet       # 34K registros 2013-2018
│   └── bilbao_contratos.parquet         # 4.8K contratos Bilbao
├── ccaa_euskadi.py                      # Scraper v4 (solo descarga → Euskadi/datos_euskadi_contratacion_v4/)
└── consolidacion_euskadi.py             # Consolidación → Parquet
```

### Campos principales (contratos_master)

| Categoría | Campos |
|-----------|--------|
| Anuncio | nombre, descripción, colección, titulo_del_contrato, objeto_del_contrato, tipo_de_anuncio, fecha_de_publicación_documento |
| Expediente | expediente, estado_de_la_tramitacion, contrato_menor, adjudicación, subsanación, apertura_de_plicas, acuerdos_de_la_mesa_de_contratacion |
| Órgano | ámbito_geográfico_del_poder_adjudicador, entidad_que_impulsa_la_contratación, órgano_de_contratación, institución, departamento, órgano_gestor |
| Enlaces | url_física, url_amigable, xml_datos, xml_metadatos, zip |
| Fechas | fecha_límite_de_presentación, fecha_de_creación |
| Origen | _archivo_origen, _year (año del fichero, no del contrato), _fuente |

Los importes y adjudicatarios de Euskadi están en `revascon_historico` (2013-2018: tipo_de_contrato, adjudicatario, importe_de_adjudicación_con_iva, fecha_de_formalización...) y `bilbao_contratos`.

### Arquitectura de fuentes

El scraper sigue una arquitectura **API-first** con múltiples capas de fallback:

**Módulo A — API REST KontratazioA** (fuente principal para catálogos)
- A1/A2: Contratos y anuncios (muestra — bulk inviable: 655K items × 10/pág = 65K peticiones ~27h). La API ignora `currentPage` en estos recursos: la "muestra 1K" publicada eran 100 copias de la página 1 (10 registros); el scraper ahora lo detecta y aborta
- A3: Poderes adjudicadores — 919 registros completos (92 páginas)
- A4: Empresas licitadoras — 9,042 registros descargados (905 páginas), 9,017 únicos
- Paginación: `?currentPage=N` (1-based, 10 items/pág fijo)

**Módulo B — XLSX/CSV Históricos** (fuente principal para contratos)
- B1: XLSX anuales 2011-2026 (655K registros) + JSON fallback 2011-2013 (9.5K registros de XLSX vacíos)
- B2: REVASCON agregado 2013-2018 (formato más rico que B1 para ese período)
- B3: Snapshot últimos 90 días (ventana móvil)

**Módulo C — Portales municipales** (datos no centralizados)
- C1: Bilbao — contratos adjudicados 2005-2026 (CSV por año + tipo)
- C2: Vitoria-Gasteiz — contratos menores formalizados

### Notas técnicas

- La API de KontratazioA usa `?currentPage=N` para paginación (no `page`, `_page`, ni HATEOAS). El parámetro `_pageSize` se ignora (fijo a 10).
- Los XLSX de 2011-2013 se publican vacíos (solo cabeceras), pero los JSON del mismo endpoint de Open Data sí contienen los datos completos (9,482 registros combinados).
- El consolidador convierte columnas con listas/dicts a JSON string antes de deduplicar, necesario para los campos anidados de la API (clasificaciones, categorías).

---

## 🍊 Valencia

Datos del portal [Dades Obertes GVA](https://dadesobertes.gva.es) (CKAN API).

| Categoría | Archivos | Registros | Contenido |
|-----------|----------|-----------|-----------|
| Contratación | 13 | 246K | REGCON 2014-2025 + DANA (2025 incompleto, ver nota) |
| Subvenciones | 52 | 2.2M | Ayudas 2022-2025 + DANA |
| Presupuestos | 4 | 346K | Ejecución 2024-2025 |
| Convenios | 5 | 8K | 2018-2022 |
| Lobbies (REGIA) | 7 | 11K | Único en España 🌟 |
| Empleo | 42 | 888K | ERE/ERTE 2000-2025, DANA |
| Paro | 283 | 2.6M | Estadísticas LABORA |
| Siniestralidad | 10 | 570K | Accidentes 2015-2024 |
| Patrimonio | 3 | 9K | Inmuebles GVA |
| Entidades | 2 | 94K | Locales + Asociaciones |
| Territorio | 1 | 4K | Centros docentes |
| Turismo | 16 | 383K | Hoteles, VUT, campings... |
| Sanidad | 8 | 189K | Mapa sanitario |
| Transporte | 7 | 993K | Bus interurbano GTFS |

### Archivos

```
valencia/
├── contratacion/          # 13 archivos, 42 MB
├── subvenciones/          # 52 archivos, 26 MB
├── presupuestos/          # 4 archivos, 7 MB
├── convenios/             # 5 archivos, 2 MB
├── lobbies/               # 7 archivos, 0.4 MB  🌟 REGIA
├── empleo/                # 42 archivos, 13 MB
├── paro/                  # 283 archivos, 17 MB
├── siniestralidad/        # 10 archivos, 0.6 MB
├── patrimonio/            # 3 archivos, 0.4 MB
├── entidades/             # 2 archivos, 4 MB
├── territorio/            # 1 archivo, 0.4 MB
├── turismo/               # 16 archivos, 17 MB
├── sanidad/               # 8 archivos, 6 MB
└── transporte/            # 7 archivos, 21 MB
```

> ⚠️ Los contratos de 2025 publicados tienen solo 32 filas (formalizaciones de enero) frente a 37.432 en 2024: el script nunca volvía a descargar un fichero existente y se quedó la primera copia del año. Ahora vuelve a descargar los recursos que el portal ha actualizado (`last_modified`), descubre los años nuevos de cada serie y conserva los ceros a la izquierda de códigos postales, INE y centros (`03001`).

### 🌟 Datos únicos de Valencia

- **REGIA**: Registro de lobbies único en España (grupos de interés, actividades de influencia)
- **DANA**: Datasets específicos de la catástrofe (contratos, subvenciones, ERTE)
- **ERE/ERTE histórico**: 25 años de datos (2000-2025)
- **Siniestralidad laboral**: 10 años de accidentes de trabajo

---

## 🆕 Andalucía

Contratación pública de la [Junta de Andalucía](https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/perfiles-licitaciones/buscador-general), incluyendo licitaciones regulares y contratos menores de todos los organismos y empresas públicas andaluzas. Extraído mediante ingeniería inversa del proxy Elasticsearch del portal, con estrategia de subdivisión recursiva en 8 dimensiones para superar el límite de 10K resultados por consulta.

Conteos observados en torno al 2026-03-23 consultando la API pública del portal:

| Tipo | Registros | Cobertura |
|------|-----------|-----------|
| Licitaciones regulares (estándar, sin BRR) | ~80.9K | Operativa |
| Contratos menores (sin BRR) | ~775.7K | Operativa |
| **Total (sin BRR)** | **~856.7K** | **Operativa** |

> ⚠️ El fichero publicado tiene 808.441 filas (hasta 2026-02-11): faltan unos 41K contratos menores. 15 segmentos del SAS superan el límite de 10.000 resultados tras las 8 dimensiones de subdivisión (273K registros); la solución prevista es partir por mes de publicación (pendiente de verificar en vivo). El script ahora incluye los códigos de estado/tipo/provincia presentes en los datos, años calculados en ejecución, descubrimiento completo de perfiles y 4 columnas JSON con todas las adjudicaciones, lotes, anuncios y campos no mapeados (19.765 expedientes con varias adjudicaciones perdían las siguientes).

### Archivos

```
ccaa_Andalucia/
├── licitaciones_andalucia.parquet          # ~857K registros (47 MB, snappy)
├── _historico/                             # versiones anteriores del Parquet
└── raw/                                    # cada descarga tal cual (jsonl.gz); anteriores en raw/_historico/

scripts/
└── ccaa_andalucia.py                       # Scraper ES proxy 8D + multi-sort + salida CSV/Parquet
```

**Sin sesgo del superviviente.**
- Cada descarga se guarda tal cual en `raw/` (`guardar_version`).
- El Parquet se construye con `acumular` sobre la salida anterior. Lo que el portal retira o cambia se conserva con `_en_ultima_descarga=False`. Solo se retira lo que seguro cae en una consulta completa: con el tope de 10.000 o una paginación cortada no se retira nada.
- `--semilla <parquet publicado>` añade los `id_expediente` que ya no se sirven (el identificador del portal; en v2026.02 es único en las 808.441 filas).
- `procesar` regenera las salidas sin red. `--perfil` y `--anio` descargan una parte, y solo esa parte puede marcar retiradas.
- Si la salida por defecto es aún un puntero Git LFS sin descargar, el script se niega a sobrescribirla. En ese caso usa `--salida` o baja antes el fichero.
- Los importes van siempre como `float64`.

### Campos principales (34 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | id_expediente, numero_expediente, titulo, url_detalle |
| Clasificación | tipo_contrato(_codigo), estado(_codigo), codigo_procedimiento (9 = contrato menor), codigo_tramitacion, codigo_normativa |
| Órgano | organo_contratacion, codigo_perfil, codigo_dir3, provincias_ejecucion |
| Importes | importe_licitacion, valor_estimado, importe_adjudicacion (sin IVA), importe_adjudicacion_iva (primera adjudicación) |
| Adjudicación | adjudicatario_nif, todos_adjudicatarios_nif, num_adjudicaciones |
| Fechas | fecha_publicacion, fecha_limite_presentacion, anuncio_primera_fecha, anuncio_ultima_fecha |
| Otros | forma_presentacion, cofinanciado_ue, subasta_electronica, sistema_racionalizacion, cpv, medios_publicacion, num_lotes, num_anuncios |

### Estrategia de descarga

El portal de la Junta de Andalucía usa un proxy frontend que limita a 10.000 resultados por consulta Elasticsearch. Con 850K registros totales, se requirió una estrategia de subdivisión recursiva en **8 dimensiones** + multi-sort para cobertura completa:

1. **codigoProcedimiento**: Estándar vs Menores
2. **tipoContrato.codigo**: 21 tipos (SERV, SUM, OBRA, PRIV...)
3. **estado.codigo**: 14 estados (RES, ADJ, PUB, EVA...)
4. **codigoTipoTramitacion**: 5 valores + null (295K registros sin tramitación)
5. **perfilContratante.codigo**: ~400 organismos
6. **provinciasEjecucion**: 8 provincias + null
7. **formaPresentacion**: 6 valores + null
8. **numeroExpediente (año)**: match por texto "2018"-"2026" + null

Para los chunks que aún superan 10K tras las 8 dimensiones (ej. SYBS03/Servicio Andaluz de Salud con 290K registros), se usa **multi-sort con 12 órdenes** distintas (idExpediente, importeLicitacion, numeroExpediente, titulo, fechaLimitePresentacion, adjudicaciones.importeAdjudicacion — cada una asc/desc) que acceden a ventanas diferentes de 10K registros con 0% de solapamiento.

### Perfiles incluidos (~400)

Todas las consejerías, agencias, hospitales del SAS, universidades, diputaciones provinciales, empresas públicas y fundaciones de la Junta de Andalucía, incluyendo:

- Servicio Andaluz de Salud — SYBS03 (290K contratos, mayor organismo)
- 8 Diputaciones provinciales
- 10 Universidades públicas
- Consejerías (Salud, Educación, Fomento, Economía, etc.)
- Agencias (IDEA, AEPSA, ADE, etc.)

---

## 🏛️ Madrid – Comunidad Autónoma

Contratación pública completa de la [Comunidad de Madrid](https://contratos-publicos.comunidad.madrid), incluyendo todas las consejerías, hospitales, organismos autónomos y empresas públicas. Extraído mediante web scraping del buscador avanzado con resolución del módulo antibot de Drupal.

| Tipo de publicación | Registros | Presupuesto licitación | Importe adjudicación |
|---------------------|-----------|----------------------|---------------------|
| Contratos menores | 2,529,049 | 487M € | 487M € |
| Convocatoria anunciada a licitación | 21,070 | 39,551M € | — |
| Contratos adjudicados sin publicidad | 10,035 | 8,466M € | — |
| Encargos a medios propios | 2,178 | 173M € | — |
| Anuncio de información previa | 1,166 | 327M € | — |
| Consultas preliminares del mercado | 28 | — | — |
| **Total** | **2,563,527** | **49,004M €** | **487M €** |

> ⚠️ El portal pone cada lote, adjudicatario, prórroga o modificación adicional en una **fila de continuación** sin tipo ni referencia justo después de su contrato (el 36 % de las filas en una muestra independiente de 30.907). El dataset publicado deduplicaba por expediente + referencia + entidad y las perdió todas (queda 1). El script ahora solo descarta bloques completos repetidos en CSV distintos (consultas solapadas) y conserva los duplicados de origen, descarga también los anuncios de 2014-2016 y los menores con presupuesto ≤0 o >50.000 € de las entidades subdivididas, y vuelve a descargar los CSV acumulativos (el publicado se corta el 2025-09-30). Pendiente: los menores de entidades históricas que ya no aparecen en el desplegable (p.ej. consejerías de legislaturas anteriores). Hay que regenerar los datos.

### Archivos

```
comunidad_madrid/
├── contratacion_comunidad_madrid_completo.parquet   # Dataset unificado (90 MB, snappy)
└── csv_originales/                                  # 765 CSVs individuales; versiones anteriores en _historico/
```

**Sin sesgo del superviviente.**
- Cada CSV descargado pasa por `guardar_version`. `csv_originales/_comprobaciones.json` anota la última comprobación de cada consulta, así que una ejecución cortada no vuelve a pedir lo que no ha cambiado.
- **Renumeración:** el número de entidad del desplegable cambia entre ejecuciones (Sanidad era la `ent028` en febrero y es la `ent060` en septiembre). Al acabar una descarga completa, los CSV de consultas que ya no existen pasan a `_historico/`.
- **Consolidación:**
  - Se hace con todas las versiones de cada CSV mediante `acumular`, comparando por bloque: el registro más sus filas de continuación.
  - Lo retirado queda con `_en_ultima_descarga=False`.
  - Los campos de más van a `_columna_extra_N` en lugar de descartarse.
- **Semilla:** `--semilla <parquet publicado>` añade, por `Referencia` + `Entidad Adjudicadora`, lo que el portal ya no sirve, dentro del ámbito de la descarga. Para la primera ejecución conviene usar como primera versión de `csv_originales/` los CSV del ZIP del release: conservan las filas de continuación que perdió el Parquet publicado.

### Campos principales (18 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | Nº Expediente, Referencia, Título del contrato |
| Clasificación | Tipo de Publicación, Estado, Tipo de contrato |
| Entidad | Entidad Adjudicadora |
| Proceso | Procedimiento de adjudicación, Presupuesto de licitación, Nº de ofertas |
| Adjudicación | Resultado, NIF del adjudicatario, Adjudicatario, Importe de adjudicación |
| Incidencias | Importe de las modificaciones, Importe de las prórrogas, Importe de la liquidación |
| Temporal | Fecha del contrato |

### Estrategia de descarga

El portal de la Comunidad de Madrid usa un módulo antibot de Drupal y tiene restricciones complejas en los filtros de búsqueda que requirieron ingeniería inversa:

- **Antibot key**: El JavaScript del portal transforma la clave de autenticación invirtiendo pares de 2 caracteres desde el final. El script replica esta transformación.
- **CAPTCHA matemático**: Cada descarga CSV requiere resolver una operación aritmética (ej. `3 + 8 =`).
- **Contratos menores** (~99% del volumen): El filtro `fecha_hasta` es incompatible con este tipo de publicación, y `fecha_desde` no funciona combinado con `entidad_adjudicadora`. Solución: descargar por **entidad adjudicadora** (125 entidades) sin filtro de fecha.
- **Subdivisión recursiva**: Las entidades con >50K registros (hospitales grandes) se subdividen automáticamente por **rango de presupuesto de licitación**, partiendo rangos por la mitad recursivamente hasta que cada segmento queda por debajo del límite de truncamiento.
- **Otros tipos** (licitaciones, adjudicaciones, etc.): Se descargan por **mes + tipo de publicación** con filtros de fecha, que sí funcionan para estos tipos.

### Entidades incluidas (125)

Todas las consejerías, organismos autónomos, empresas públicas y fundaciones de la CAM, incluyendo:

- 10 Consejerías (Sanidad, Educación, Digitalización, Economía, etc.)
- 30+ Hospitales del SERMAS (Gregorio Marañón, La Paz, 12 de Octubre, Ramón y Cajal, etc.)
- Canal de Isabel II y filiales
- Fundaciones IMDEA (7)
- Fundaciones de investigación biomédica (12)
- Consorcios urbanísticos, agencias y entes públicos

---

## 🏛️ Madrid – Ayuntamiento

Actividad contractual completa del [Ayuntamiento de Madrid](https://datos.madrid.es), unificando 67 ficheros CSV con 12 estructuras distintas en un único dataset normalizado.

| Categoría | Registros | Importe total |
|-----------|-----------|---------------|
| Contratos menores | 68,626 | 407M € |
| Contratos formalizados | 17,991 | 16,606M € |
| Acuerdo marco / sist. dinámico | 24,621 | 2,549M € |
| Prorrogados | 4,441 | 2,967M € |
| Modificados | 1,789 | 718M € |
| Cesiones | 30 | 80M € |
| Resoluciones | 225 | 62M € |
| Penalidades | 483 | 13M € |
| Homologación | 1,047 | 1M € |
| **Total** | **119,253** | **~23,400M €** |

> ⚠️ datos.madrid.es migró a CKAN y renumeró los recursos: el script descubre ahora los ficheros por la API CKAN (incluido 2026), ya no descarta en silencio ficheros con nombre repetido, vuelve a bajar el año en curso y el anterior, y decodifica los CSV en CP850 (antes "Descripci¢n", "A¤o"…).

### Archivos

El script `ccaa_madrid_ayuntamiento.py` genera:

### Campos principales (70+ columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | n_registro_contrato, n_expediente, fuente_fichero, categoria |
| Organización | centro_seccion, organo_contratacion, organismo_contratante |
| Objeto | objeto_contrato, tipo_contrato, subtipo_contrato, codigo_cpv |
| Licitación | importe_licitacion_iva_inc, n_licitadores_participantes, n_lotes |
| Adjudicación | importe_adjudicacion_iva_inc, nif_adjudicatario, razon_social_adjudicatario, pyme |
| Fechas | fecha_adjudicacion, fecha_formalizacion, fecha_inicio, fecha_fin |
| Derivados (A.M.) | n_contrato_derivado, objeto_derivado, fecha_aprobacion_derivado |
| Incidencias | tipo_incidencia, importe_modificacion, importe_prorroga, importe_penalidad |
| Cesiones | adjudicatario_cedente, cesionario, importe_cedido |
| Resoluciones | causas_generales, causas_especificas, fecha_acuerdo_resolucion |
| Homologación | n_expediente_sh, objeto_sh, duracion_procedimiento |

### Estructuras detectadas

El script detecta y unifica automáticamente 12 estructuras de CSV distintas:

| Estructura | Período | Categorías |
|------------|---------|------------|
| A, B, C, D | 2015-2020 | Contratos menores |
| E, F | 2021-2025 | Contratos menores |
| AC_OLD | 2015-2020 | Formalizados, acuerdo marco |
| AC_OLD_MOD | 2015-2020 | Modificados |
| AC_HOMOLOGACION | 2022-2024 | Homologación |
| AC_NEW | 2021-2024 | Todas las categorías |
| AC_2025 | 2025 | Todas las categorías |

### Fuentes

- [Contratos menores](https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=9e42c176aab90410VgnVCM1000000b205a0aRCRD) — 12 ficheros (2015-2025)
- [Actividad contractual](https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=7449f3b0a4699510VgnVCM1000001d4a900aRCRD) — 55 ficheros (2015-2025)

---

## 🆕 Galicia

Contratación pública completa de la [Xunta de Galicia](https://www.contratosdegalicia.gal) y todos sus organismos dependientes, extraída mediante ingeniería inversa de la API jQuery DataTables del portal. Incluye contratos menores (adjudicación directa, desde 2018) y licitaciones formales (desde 2007) de 418 organismos.

| Tipo | Registros | Período |
|------|-----------|---------|
| Contratos menores | 1,635,407 | 2018-2026 |
| Licitaciones | 50,382 | 2007-2026 |
| **Total** | **1,685,789** | **2007-2026** |

> ⚠️ En los datos publicados (`contratos_galicia.parquet` / `contratos_galicia.zip`) la columna `importe` está inflada ×10 o ×100: el scraper eliminaba el punto decimal de los importes de la API como si fuera separador de miles (674.78 → 67478). El 55 % de los contratos menores publicados supera 48.400 € (imposible por ley) y suman 547.600 M€. El scraper ya está corregido; hay que regenerar los datos. El fichero publicado solo tiene las 12 columnas base; con la fase de detalle son 64, incluidas `detail_adjudicaciones_json` (adjudicatarios e importes por lote, que para las licitaciones solo están en el detalle) y `detail_campos_extra_json`. El scraper compara lo descargado con los totales que declara el portal por organismo.

### Archivos

```
galicia/
├── contratos_galicia_base.csv            # Dataset base de tabla (12 columnas)
├── contratos_galicia_base.parquet        # Base en parquet
├── contratos_galicia_detail.sqlite3      # Caché incremental del detalle HTML
├── contratos_galicia.csv                 # Dataset final mergeado (62 columnas + 3 de control)
├── contratos_galicia.parquet             # Dataset final en parquet
├── _historico/                           # versiones anteriores de base y final
├── contratos_galicia_base_progress.json  # Checkpoint y ámbito leído de cada organismo
└── scraper_galicia.py                    # Pipeline base + detail + merge
```

### Campos principales

**Base (12 columnas)**: `id`, `objeto`, `importe`, `estado`, `estadoDesc`, `publicado`, `modificado`, `_organismo_id`, `_tipo`, `nif`, `adjudicatario`, `duracion`

**Detalle HTML enriquecido**: el dataset final añade >50 columnas derivadas de la ficha del portal, entre ellas:

- `detail_tipo_tramitacion`
- `detail_tipo_procedimiento`
- `detail_tipo_contrato`
- `detail_presupuesto_base_text` / `detail_presupuesto_base_eur`
- `detail_valor_estimado_text` / `detail_valor_estimado_eur`
- `detail_num_lotes`
- `detail_fecha_difusion`
- `detail_organo`
- `detail_correo_electronico`
- `detail_cpv_codes`
- `detail_nuts_codes`
- `detail_documentos_count`
- `detail_adjudicaciones_count`
- `detail_status`, `detail_attempts`, `detail_last_error`

Además, la caché SQLite puede guardar comprimidos los `pairs` y `tables` crudos de cada ficha HTML para no perder información aunque no todo se aplane a columnas.

### Estrategia de descarga

El portal usa jQuery DataTables con server-side processing y dos endpoints separados:

- **Licitaciones**: `/api/v1/organismos/{id}/licitaciones/table` — paginación estándar, sin restricciones temporales
- **Contratos menores**: `/api/v1/organismos/{id}/contratosmenores/table` — requiere header `Referer` dinámico por organismo y rechaza rangos de fecha >3 meses

**Discovery automático**: El scraper prueba IDs de organismo 1–2000 contra ambos endpoints (licitaciones en paralelo, CM secuencial por la restricción del `Referer`) para descubrir los organismos activos.

**Barrido temporal CM**: Ventanas de 3 meses desde la fecha actual hasta 2000-01-01. El servidor reporta `recordsTotal` global (ignorando el filtro de fecha), pero los datos devueltos sí están filtrados. Deduplicación por `(id, _tipo)` para eliminar solapamientos entre ventanas.

**Detalle HTML real**: El portal no expone un endpoint JSON útil para la ficha; los campos adicionales salen de `POST /licitacion`. El scraper hace un segundo paso de enriquecimiento HTML para `LIC` y `CM`.

**Sin sesgo del superviviente.**
- Una descarga base nueva no borra la anterior: pasa a `_historico/`.
- La caché SQLite de fichas nunca se borra. Una ficha ya descargada no se sustituye por un error ni por una vacía, y si el portal la cambia la anterior queda en `detail_cache_historico`.
- La tabla final se acumula con `acumular`: lo retirado queda con `_en_ultima_descarga=False`, pero solo en lo que se ha vuelto a leer completo.
  - LIC: si llegan exactamente `recordsTotal` ids.
  - Ventanas de CM: si traen exactamente `recordsFiltered` ids.
- `--semilla` añade por (`_tipo`, `id`) lo que ya no se sirve. El importe publicado (inflado ×10/×100) va a `importe_semilla` sin corregir.
- `detail` sin `--resume` ya no rehace todo: reintenta las fichas pendientes. Para rehacerlo todo, `--force-detail`.

**Pipeline incremental y reanudable**:

- `all`: ejecuta `base -> detail -> merge`
- `base`: reconstruye solo el dataset base de tabla
- `detail`: enriquece HTML sobre el base ya descargado
- `merge`: junta base + detalle en el dataset final

El progreso base se guarda por organismo en `contratos_galicia_base_progress.json` y el detalle se cachea en `contratos_galicia_detail.sqlite3`, de forma que si la ejecución se corta o el portal devuelve `403/429`, se puede retomar con `--resume`.

**Salida reproducible**: otra persona puede rehacer la descarga completa ejecutando el mismo pipeline y obteniendo base, detalle cacheado y merge final.

### Validación reciente

En una validación real sobre un organismo mixto (`33`), el scraper antiguo seguía sacando `152` filas y `12` columnas. El pipeline nuevo mantiene las `152` filas, genera un base estable de `12` columnas y un dataset final enriquecido de `62` columnas, con `152/152` fichas HTML resueltas y una segunda pasada `detail --resume` que no rehace trabajo (`0` procesados).

Este diseño está pensado para ejecutar el backfill histórico completo sin depender de memoria RAM ni de una corrida monolítica: si el portal corta la sesión o devuelve `403/429`, basta con relanzar la fase correspondiente con `--resume`.

---

## 🆕 Asturias

Contratación centralizada del [Principado de Asturias](https://sede.asturias.es/), incluyendo contratos menores, servicios, obras y suministros de todos los organismos y entes públicos del Principado.

| Métrica | Valor |
|---------|-------|
| Registros | 375,380 |
| Período | 2019-2024 |
| Columnas | 99 |
| Tamaño | 21 MB |

> ⚠️ En el parquet publicado la columna `IVA` de 2023 está multiplicada ×10 (210/100/40/50 en lugar de 21/10/4/5) y, con pandas 3, los importes quedaban como texto. El script ya está corregido y escribe en `ccaa_asturias/`; hay que regenerar los datos. Además: descarga todos los años hasta el actual (antes solo 2019-2024, y el fichero de 2025 ya existe), lee los CSV como Windows-1252 (5.099 caracteres corruptos), mantiene `Nº EXPEDIENTE ORGANO` como texto (67.440 valores pasaban a NaN) y guarda las líneas mal formadas en `lineas_descartadas_AAAA.csv` en vez de descartarlas.

### Archivos

```
ccaa_asturias/
├── asturias_contracts_ALL_YEARS.parquet   # 375K registros (21 MB, snappy)
├── _historico/                            # versiones anteriores del Parquet
└── raw/                                   # CSV anuales tal cual; versiones anteriores en raw/_historico/
```

**Sin sesgo del superviviente.**
- Cada CSV anual se guarda tal cual en `raw/`. Si el Principado lo cambia, la versión anterior pasa a `raw/_historico/`.
- El Parquet se construye desde todas las versiones. Las filas se comparan por el texto publicado.
  - Lo que se retira o cambia se conserva con `_en_ultima_descarga=False` (también llevan `_primera_descarga` y `_ultima_descarga`).
  - Un año que deja de servirse conserva sus filas.
- `--semilla <parquet publicado>` añade las inscripciones (`year`, `Nº INSCRIPCION`) que ya no se sirven, marcadas con `_origen`.

```bash
python scripts/ccaa_asturias.py --semilla asturias_contracts_ALL_YEARS.parquet   # el de v2026.02, la primera vez
```

### Campos principales (99 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | Nº INSCRIPCION, Nº EXPEDIENTE ORGANO, OBJETO |
| Clasificación | CLASIFICACION GENERAL, CARACTERISTICAS CONTRATO, REGULACION |
| Órgano | ENTE CONTRATANTE, ORGANO CONTRATANTE |
| Importes | PRESUPUESTO, IMP. ADJ. (CON IVA), IMP. ADJ. IMPUESTO, IMP. ADJ. LOTE |
| Proceso | PROC. ADJUDICACION, FORMA ADJUDICACION, T. TRAMITACION |
| Adjudicación | CONTRATISTAS, NIF/CIF CONTRATISTA, RAZON SOCIAL CONTRATISTA, ADJUDICADOS A PYMES |
| CPV | CODIGO CPV |
| Fechas | F. DE ALTA, F. ADJ., F. FORMALIZACION, F. FIN EJECUCION |
| Publicación | F. BOPA, F. BOE, F. DOUE |
| Fondos europeos | CONTRATOS FINANCIADOS CON FONDOS EUROPEOS, TIPO DE FONDO EUROPEO |
| Estrategia | CONT. ESTRAT. C. SOCIALES, C. MEDIOAMBIENTALES, C. DE I+D |
| Recursos | OBJETO DE RECURSO ESPECIAL EN MATERIA DE CONTRATACION |

---

## 📥 Uso

```python
import pandas as pd

# Nacional - PLACSP (una fila por licitación, semántica de importes corregida)
import sys; sys.path.insert(0, '.')
from nacional.licitaciones import leer_placsp
df_nacional = leer_placsp('nacional/licitaciones_espana.parquet')

# TED - España (consolidado)
df_ted = pd.read_parquet('ted/ted_es_can.parquet')

# Andalucía - Contratación completa
df_and = pd.read_parquet('ccaa_Andalucia/licitaciones_andalucia.parquet')

# Euskadi - Contratos sector público
df_eus = pd.read_parquet('Euskadi/euskadi_parquet/contratos_master.parquet')

# Euskadi - Poderes adjudicadores
df_poderes = pd.read_parquet('Euskadi/euskadi_parquet/poderes_adjudicadores.parquet')

# Euskadi - Empresas licitadoras
df_empresas = pd.read_parquet('Euskadi/euskadi_parquet/empresas_licitadoras.parquet')

# Comunidad de Madrid - Contratación completa
df_cam = pd.read_parquet('comunidad_madrid/contratacion_comunidad_madrid_completo.parquet')

# Madrid Ayuntamiento - Actividad contractual (generado por comunidad_madrid/ccaa_madrid_ayuntamiento.py)
df_madrid = pd.read_parquet('datos_madrid_contratacion_completa/actividad_contractual_madrid_completo.parquet')

# Catalunya - Contratos menores
df_cat_menors = pd.read_parquet('catalunya/contratacion/contractacio_menors.parquet')

# Catalunya - Subvenciones
df_cat_subv = pd.read_parquet('catalunya/subvenciones/raisc_concesiones.parquet')

# Valencia - Contratación (un fichero por año; el de la DANA tiene otro esquema)
import glob
df_val = pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('valencia/contratacion/*_20*.parquet'))])

# Valencia - Lobbies REGIA (la carpeta mezcla 7 tablas distintas)
df_lobbies = pd.read_parquet('valencia/lobbies/Grupos_de_interés.parquet')

# BORME - Actos mercantiles (anonimizado)
df_borme = pd.read_parquet('borme/data/borme_empresas_pub.parquet')

# BORME - Cargos con persona hasheada
df_cargos = pd.read_parquet('borme/data/borme_cargos_pub.parquet')

# Galicia - Contratación completa (CM + LIC)
df_gal = pd.read_parquet('galicia/contratos_galicia.parquet')

# Asturias - Contratación centralizada
df_ast = pd.read_parquet('ccaa_asturias/asturias_contracts_ALL_YEARS.parquet')
```

### Ejemplos de análisis

```python
# Top adjudicatarios nacional (importe adjudicado sin IVA, primer lote)
df_nacional.groupby('nif_adjudicatario')['importe_adjudicacion'].sum().nlargest(10)

# Contratos España publicados en TED por año
df_ted.groupby('year').size().plot(kind='bar', title='Contratos TED España')

# Andalucía: contratos menores por órgano de contratación
and_menores = df_and[df_and['codigo_procedimiento'].astype(str) == '9']
and_menores['organo_contratacion'].value_counts().head(20)

# Euskadi: importe adjudicado por año y tipo de contrato (REVASCON 2013-2018)
df_rev = pd.read_parquet('Euskadi/euskadi_parquet/revascon_historico.parquet')
anio_formalizacion = pd.to_datetime(df_rev['fecha_de_formalización'], errors='coerce', dayfirst=True).dt.year
df_rev.groupby([anio_formalizacion, 'tipo_de_contrato'])['importe_de_adjudicación_con_iva'].sum().unstack().plot()

# Euskadi: anuncios por tipo de anuncio
df_eus['tipo_de_anuncio'].value_counts().head(10)

# Euskadi: empresas del Registro de Licitadores
df_empresas['name'].value_counts().head(10)

# Comunidad de Madrid: contratos menores por hospital
cam_menores = df_cam[df_cam['Tipo de Publicación'] == 'Contratos menores']
cam_menores['Entidad Adjudicadora'].value_counts().head(20)

# Ayuntamiento Madrid: gasto por categoría y año
df_madrid.groupby(['categoria', 'anio'])['importe_adjudicacion_iva_inc'].sum().unstack(0).plot()

# Contratos SARA no publicados en TED
df_sara = pd.read_parquet('ted/crossval_sara.parquet')
missing = df_sara[df_sara['_ted_missing']]
missing.groupby('organo_contratante').size().nlargest(10)

# Contratos menores Catalunya por órgano
df_cat_menors.groupby('organ')['pressupostAdjudicacio'].sum().nlargest(10)

# Evolución ERE/ERTE Valencia (un único snapshot: los de 2024 y 2025 se solapan)
df_erte = pd.read_parquet('valencia/empleo/Datos_ERE_y_ERTE_solicitados_y_resueltos_en_la_Comunitat_Valenciana_2025-12-28.parquet')
df_erte.groupby(df_erte['FECHA_SOLICITUD'].str[:4]).size().plot()

# BORME: constituciones por año
df_borme = pd.read_parquet('borme/data/borme_empresas_pub.parquet')
constit = df_borme[df_borme['actos'].str.contains('Constitución', na=False)]
constit.groupby(constit['fecha_borme'].dt.year).size().plot(title='Constituciones/año')

# BORME: administradores compartidos entre empresas
df_cargos = pd.read_parquet('borme/data/borme_cargos_pub.parquet')
nombramientos = df_cargos[df_cargos['tipo_acto'] == 'nombramiento']
multi = nombramientos.groupby('persona_hash')['empresa_norm'].nunique()
print(f"Admins en >1 empresa: {(multi > 1).sum():,}")

# Galicia: top 10 adjudicatarios por importe (contratos menores)
df_gal_cm = df_gal[df_gal['_tipo'] == 'CM']
df_gal_cm.groupby('adjudicatario')['importe'].sum().nlargest(10)

# Galicia: evolución del gasto en contratos menores por año
df_gal_cm['año'] = df_gal_cm['publicado'].dt.year
df_gal_cm.groupby('año')['importe'].sum().plot(kind='bar', title='Contratos menores Galicia')

# Galicia: concentración — adjudicatarios que acumulan el 50% del gasto
top = df_gal_cm.groupby('nif')['importe'].sum().sort_values(ascending=False)
n_50 = (top.cumsum() / top.sum() <= 0.5).sum() + 1
print(f"{n_50} adjudicatarios concentran el 50% del gasto en CM Galicia")

# Asturias: gasto anual por tipo de contrato
df_ast = pd.read_parquet('ccaa_asturias/asturias_contracts_ALL_YEARS.parquet')
df_ast.groupby(['year', 'CARACTERISTICAS CONTRATO'])['IMP. ADJ. (CON IVA)'].sum().unstack().plot()

# Asturias: top entes contratantes por volumen
df_ast.groupby('ENTE CONTRATANTE')['IMP. ADJ. (CON IVA)'].sum().nlargest(10)

# Asturias: contratos menores por órgano
ast_menores = df_ast[df_ast['CLASIFICACION GENERAL'].isin(['MENOR', 'MENORES 5000'])]
ast_menores['ORGANO CONTRATANTE'].value_counts().head(20)
```

---

## 🔧 Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `nacional/licitaciones.py` | PLACSP | Extrae datos nacionales de ATOM/XML (todas las entradas publicadas, marcadas con `es_ultima_version`, + tabla de resultados por lote) |
| `nacional/normalizar_placsp.py` | — | Corrige parquets PLACSP ya generados sin eliminar filas: marca versiones, semántica de importes, etiquetas y CPV |
| `scripts/ccaa_andalucia.py` | Junta de Andalucía | Scraper ES proxy con subdivisión 8D + multi-sort 12x + salida reproducible |
| `Euskadi/ccaa_euskadi.py` | KontratazioA + Open Data Euskadi | Scraper v4 (solo descarga): API REST + XLSX anuales + portales municipales |
| `Euskadi/consolidacion_euskadi.py` | — | Consolida JSON/XLSX/CSV → 5 Parquets normalizados |
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | contratos-publicos.comunidad.madrid | Web scraping con antibot bypass + subdivisión recursiva por importe |
| `comunidad_madrid/ccaa_madrid_ayuntamiento.py` | datos.madrid.es | Descarga y unifica 67 CSVs (9 categorías, 12 estructuras) |
| `scripts/ccaa_cataluna_contratosmenores.py` | contractaciopublica.cat | Descarga contratos menores Catalunya (todas las fases, API del portal) |
| `galicia/scraper_galicia.py` | contratosdegalicia.gal | Pipeline base + detalle HTML + merge, con discovery automático, barrido CM 3 meses, caché SQLite y `--resume` |
| `scripts/ccaa_asturias.py` | Principado de Asturias | Descarga contratación centralizada Asturias → `ccaa_asturias/` |
| `scripts/ccaa_cataluna.py` | Socrata + CKAN Barcelona | Descarga datos Catalunya |
| `scripts/ccaa_cataluna_parquet.py` | — | Convierte los CSV de Catalunya a Parquet |
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `scripts/ccaa_valencia_parquet.py` | — | Convierte los CSV de Valencia a Parquet |
| `ted/ted_module.py` | TED | Descarga CSV bulk + API v3 eForms |
| `ted/run_ted_crossvalidation.py` | — | Cross-validation PLACSP↔TED + matching avanzado (9 estrategias) |
| `ted/diagnostico_missing_ted.py` | — | Diagnóstico de missing |
| `ted/analisis_sector_salud.py` | — | Deep dive sector salud |
| `borme/scripts/borme_scraper.py` | BOE/BORME | Descarga ~126K PDFs del Registro Mercantil |
| `borme/scripts/borme_batch_parser.py` | — | Parser de actos mercantiles (constituciones, cargos...) |
| `borme/scripts/borme_anonymize.py` | — | Genera datasets públicos sin datos personales |
| `borme/scripts/borme_placsp_match.py` | — | Detector de anomalías BORME × PLACSP (5 flags) |
| `calidad/calidad_licitaciones.py` | — | 21 indicadores de calidad sobre PLACSP + TED + BORME |
| `calidad/correcciones.py` | — | Importes corregidos junto a los publicados (registro de errores de la fuente y saltos de escala) |

---

## 🔄 Actualización

| Fuente | Frecuencia |
|--------|------------|
| PLACSP | Mensual |
| TED | Trimestral (API) / Anual (CSV bulk) |
| Andalucía | Trimestral (re-ejecutar script) |
| Euskadi | Trimestral (re-ejecutar ccaa_euskadi.py + consolidar) |
| Madrid – Comunidad | Trimestral (re-ejecutar script) |
| Madrid – Ayuntamiento | Anual (nuevos CSVs por año) |
| Catalunya | Variable (depende del dataset) |
| Valencia | Diaria/Mensual (depende del dataset) |
| Galicia | Trimestral (re-ejecutar scraper, ~8h) |
| Asturias | Anual (nuevos datasets por año) |
| BORME | Trimestral (re-ejecutar scraper + parser + anonymize) |

---

## 📋 Requisitos

```bash
pip install -r requirements.txt

# Tests (offline, sin acceso a los portales)
pip install pytest && python -m pytest
```

---

## 📄 Licencia

Datos públicos del Gobierno de España, Unión Europea y CCAA.

- España: [Licencia de Reutilización](https://datos.gob.es/es/aviso-legal)
- Galicia: [Ley 1/2016 de transparencia y buen gobierno de Galicia](https://www.contratosdegalicia.gal)
- Asturias: [Portal de Transparencia del Principado de Asturias](https://sede.asturias.es/)
- TED: [EU Open Data Licence](https://data.europa.eu/eli/dec_impl/2011/833/oj)
- BORME: [Condiciones de Reutilización BOE](https://www.boe.es/informacion/aviso_legal/index.php#reutilizacion) — Fuente: Agencia Estatal Boletín Oficial del Estado

---

## 🔗 Fuentes

| Portal | URL |
|--------|-----|
| PLACSP | https://contrataciondelsectorpublico.gob.es/ |
| TED | https://ted.europa.eu/ |
| TED API v3 | https://ted.europa.eu/api/docs/ |
| TED CSV Bulk | https://data.europa.eu/data/datasets/ted-csv |
| Andalucía | https://www.juntadeandalucia.es/contratacion/ |
| Euskadi — KontratazioA | https://www.contratacion.euskadi.eus/ |
| Euskadi — Open Data | https://opendata.euskadi.eus/ |
| Euskadi — API REST | https://api.euskadi.eus/procurements/ |
| Madrid – Comunidad | https://contratos-publicos.comunidad.madrid/ |
| Madrid – Ayuntamiento | https://datos.madrid.es/ |
| Catalunya | https://analisi.transparenciacatalunya.cat/ |
| Valencia | https://dadesobertes.gva.es/ |
| Galicia | https://www.contratosdegalicia.gal/ |
| Asturias | https://sede.asturias.es/ |
| BORME | https://www.boe.es/diario_borme/ |
| BQuant Finance | https://bquantfinance.com |

---

## 📈 Cobertura y próximas CCAA

- [x] Nacional (PLACSP), Andalucía, Asturias, Catalunya, Euskadi, Galicia, Madrid, Valencia
- [x] Castilla y León, Región de Murcia y Aragón: `scripts/ccaa_castilla_leon.py`, `ccaa_murcia.py` (incluye los menores del Servicio Murciano de Salud 2019-2025) y `ccaa_aragon.py`, verificados en vivo el 2026-09-27
- [x] Extremadura: `scripts/ccaa_extremadura.py`, el Registro de Contratos de la Junta (menores, mayores e incidencias, 1T 2022-2T 2026; 207.702 filas de menores con NIF), verificado en vivo el 2026-09-28
- [x] La Rioja: `scripts/ccaa_la_rioja.py`, el CSV anual de menores del Gobierno (2018-2026, 350.967 filas, con NIF salvo personas físicas enmascaradas), verificado en vivo el 2026-09-28
- [x] Menores valencianos fuera del REGCON: `scripts/ccaa_valencia_menores.py` (Universitat de València, Ajuntament de València, Diputación de Alicante, UA, UMH y UPV; 1,43 M filas 2015-2026), verificado en vivo el 2026-09-28
- [x] Menores de 8 ayuntamientos grandes con poco o nada en el 1143: `scripts/municipios_menores.py` (Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife; 289.401 filas 2015-2026), verificado en vivo el 2026-09-28
- [x] Castilla-La Mancha: `scripts/ccaa_castilla_la_mancha.py`, los ficheros de transparencia de la Junta (menores, caja pagadora, SESCAM por línea de factura, sector público 2015-2018) y la UCLM 2017-2026, verificado en vivo el 2026-09-28. Los RAR necesitan `rarfile` con `unrar` o `libarchive-c` (Universitat de València, Ajuntament de València, Diputación de Alicante): scrapers en desarrollo
- [ ] Canarias, Cantabria, Illes Balears, Ceuta y Melilla
- Navarra: su ley foral de contratos (art. 102.3) solo obliga a publicar la menor cuantía agregada por empresa y trimestre; no hay fuente contrato a contrato

**Contratos menores:** no hay cobertura del 100 %. Estimamos el 55-65 % de los menores publicados y no existe un denominador oficial completo (el RCSP es una cota inferior). Veredicto por comunidad, inventario de fuentes y plan: [docs/COBERTURA.md](docs/COBERTURA.md) (§0.1 y §5).

---

⭐ Si te resulta útil, dale una estrella al repo

[@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
