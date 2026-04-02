# 🇪🇸 Datos Abiertos de Contratación Pública - España

Dataset completo de contratación pública española: nacional (PLACSP) + datos autonómicos (Andalucía, Asturias, Catalunya, Euskadi, Galicia, Valencia, Madrid) + cruce europeo (TED) + Registro Mercantil (BORME).

## 📊 Resumen de Datos

| Fuente | Registros | Período | Tamaño |
|--------|-----------|---------|--------|
| Nacional (PLACSP) | 8.7M | 2012-2026 | 780 MB |
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
| 🆕 Calidad (indicadores) | 8.7M contratos × 20 indicadores | 2012-2026 | 977 MB |
| **TOTAL** | **~44.4M + BORME** | **2000-2026** | **~2.3 GB** |

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
| `comunidad_madrid.zip` | Contratación Comunidad de Madrid | ~90 MB |
| `madrid_ayuntamiento.zip` | Actividad contractual Ayuntamiento de Madrid | ~40 MB |
| `galicia.zip` | Contratación pública Xunta de Galicia (CM + LIC) | ~35 MB |
| `asturias.zip` | Contratación centralizada Principado de Asturias | ~21 MB |
| `borme.zip` | Registro Mercantil — actos mercantiles + cargos (anonimizado) | 750 MB |

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
└── ted_es_can.parquet               # Consolidado (591K, 31 MB)
```

### Campos principales (57 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | ted_notice_id, notice_type, year |
| Comprador | cae_name, cae_nationalid, buyer_legal_type, buyer_country |
| Contrato | cpv_code, type_of_contract, procedure_type |
| Importes | award_value, total_value, estimated_value |
| Adjudicación | win_name, win_nationalid, win_country, win_size (SME) |
| Competencia | number_offers, direct_award_justification, award_criterion_type |
| Duración | duration_lot, contract_start, contract_completion |

---

## 🔍 Cross-Validation PLACSP ↔ TED

Pipeline para validar si los contratos SARA españoles se publican efectivamente en el Diario Oficial de la UE.

### Resultados

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
| `ted/run_ted_crossvalidation.py` | Cross-validation PLACSP↔TED con reglas SARA + matching avanzado (5 estrategias) |
| `ted/diagnostico_missing_ted.py` | Diagnóstico de missing: falsos positivos vs gaps reales |
| `ted/analisis_sector_salud.py` | Deep dive sector salud: lotes, acuerdos marco, CPV, CCAA |

---

## 🏢 BORME — Registro Mercantil

Datos del [Boletín Oficial del Registro Mercantil](https://www.boe.es/diario_borme/) parseados desde ~126.000 PDFs (2009-2026). Permite cruzar relaciones societarias con contratación pública para detectar anomalías.

| Conjunto | Registros | Contenido |
|----------|-----------|-----------|
| Empresas | 9.2M filas, 3.3M únicas | Actos mercantiles: constituciones, disoluciones, fusiones, ampliaciones de capital... |
| Cargos | 17M filas, 3.8M personas | Nombramientos, ceses, revocaciones — con persona hasheada (SHA-256) |

> ⚠️ Los PDFs originales no se redistribuyen porque contienen nombres de personas físicas protegidos por RGPD. Se publica el scraper para descargarlos directamente desde boe.es y los datos derivados anonimizados.

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
| 3 | Administradores compartidos | Misma persona administrando empresas competidoras |
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

# 4. Detectar anomalías cruzando con PLACSP
python borme/scripts/borme_placsp_match.py --borme ./borme_pdfs --placsp nacional/licitaciones_espana.parquet --output ./anomalias
```

---

## 🆕 Calidad de Datos — 20 Indicadores

Pipeline de calidad que aplica **20 indicadores** de validez, consistencia y fiabilidad sobre el dataset nacional (PLACSP), cruzando con TED y BORME.

**8,693,891 contratos** evaluados | Score medio: **88.3** | Mediana: **89.5**

### Resultados

| Indicador | % Fallo | Descripción |
|---|---|---|
| INT-VAL-01 | 22.6% | Importe de licitación en formato válido |
| INT-VAL-02 | 31.8% | Importe de adjudicación en formato válido |
| INT-VAL-07 | 39.2% | Fecha de adjudicación válida |
| INT-VAL-09 | 23.3% | Código CPV válido |
| INT-VAL-12 | 33.4% | NIF/CIF adjudicatario válido (checksum) |
| INT-VAL-14 | 1.3% | Contrato menor coherente con cuantía (LCSP art. 118) |
| INT-CONS-01 | 1.1% | Si adjudicado, nº ofertas ≥ 1 |
| INT-CONS-08 | 0.3% | Importe adjudicación ≤ licitación (+5%) |
| INT-CONS-18 | 51.3% | Adjudicatario existe en BORME (3.3M empresas) |
| INT-CONS-20 | 77.5% | Contrato SARA publicado en TED (5 estrategias matching) |
| INT-FIA-01 | 0.6% | Nº ofertas en rango razonable (P99 por CPV) |
| INT-FIA-04 | 21.3% | Plazo presentación ofertas razonable (0-365 días) |
| INT-FIA-08 | 0.4% | PBL no outlier (≤ 50M€) |
| INT-FIA-09 | 0.8% | PA plausible por segmento CPV (P1-P99) |

Los 6 indicadores restantes (formato numérico, no negativos, NUTS, trazabilidad) dan 0.0% de fallo — checks de sanidad que se aplican pero no revelan problemas.

Los indicadores se basan en el marco de calidad de PPDS, con contribuciones de Jaime Gómez-Obregón (umbrales LCSP) y OIRESCON (benchmarks PBL).

### Menores vs Regulares

| Indicador | Menores (3.3M) | Regulares (5.4M) | Diferencia |
|---|---|---|---|
| NIF adjudicatario inválido | 2.7% | 52.1% | -49.5pp |
| Sin CPV | 60.3% | 0.7% | +59.6pp |
| Sin fecha adjudicación | 0.8% | 62.6% | -61.9pp |
| Sin importe licitación | 53.1% | 4.1% | +49.0pp |
| Score medio | 90.8 | 86.7 | +4.1 |

### Archivos

```
calidad/
├── calidad_licitaciones.py                  # Pipeline (429 líneas)
└── calidad_licitaciones_resultado.parquet   # 8.7M × 70 cols (977 MB)
```

### Uso

```bash
# Solo indicadores base (17)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet

# Completo con TED + BORME (20 indicadores)
python calidad/calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
  --ted ted/crossval_sara_v2.parquet \
  --borme borme_empresas.parquet
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
```

---

## 🏛️ Nacional - PLACSP

Licitaciones de la [Plataforma de Contratación del Sector Público](https://contrataciondelsectorpublico.gob.es/).

| Conjunto | Registros | Período |
|----------|-----------|---------|
| Licitaciones | 3.6M | 2012-actualidad |
| Agregación CCAA | 1.7M | 2016-actualidad |
| Contratos menores | 3.3M | 2018-actualidad |
| Encargos medios propios | 14.7K | 2021-actualidad |
| Consultas preliminares | 3.7K | 2022-actualidad |

### Archivos

```
nacional/
├── licitaciones_espana.parquet              # Última versión (641 MB)
└── licitaciones_completo_2012_2026.parquet  # Historial completo (780 MB)
```

### Campos principales (48 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | id, expediente, objeto, url |
| Órgano | organo_contratante, nif_organo, dir3_organo, ciudad_organo |
| Tipo | tipo_contrato, subtipo_code, procedimiento, estado |
| Importes | importe_sin_iva, importe_con_iva, importe_adjudicacion |
| Adjudicación | adjudicatario, nif_adjudicatario, num_ofertas, es_pyme |
| Clasificación | cpv_principal, cpvs, ubicacion, nuts |
| Fechas | fecha_publicacion, fecha_limite, fecha_adjudicacion |

---

## 🏴 Catalunya

Datos del portal [Transparència Catalunya](https://analisi.transparenciacatalunya.cat) (Socrata API).

| Categoría | Registros | Período |
|-----------|-----------|---------|
| Subvenciones RAISC | 9.6M | 2014-2025 |
| **Contratación pública** | **4.3M** | 2014-2025 |
| ↳ Contratos regulares | 1.3M | 2014-2025 |
| ↳ Contratos menores 🆕 | 3.0M | 2014-2025 |
| Presupuestos | 3.1M | 2014-2025 |
| Convenios | 62K | 2014-2025 |
| RRHH | 3.4M | 2014-2025 |
| Patrimonio | 112K | 2020-2025 |

### Archivos

```
catalunya/
├── contratacion/
│   ├── contractacio_publica.parquet         # 1.3M contratos regulares
│   ├── contractacio_menors.parquet          # 3.0M contratos menores 🆕
│   ├── contractacio_menors_detallado.parquet # contratos menores enriquecidos
│   └── contractacio_menors_detail.sqlite3   # cache incremental del detalle
├── subvenciones/
│   └── raisc_subvenciones.parquet           # 9.6M registros
├── pressupostos/
│   └── pressupostos_*.parquet
├── convenis/
│   └── convenis_*.parquet
├── rrhh/
│   └── rrhh_*.parquet
└── patrimoni/
    └── patrimoni_*.parquet
```

### 🆕 Contratos menores Catalunya

Dataset nuevo con **3.024.000 registros** de contratos menores del sector público catalán:

- **43 columnas** incluyendo: `id`, `descripcio`, `pressupostLicitacio`, `pressupostAdjudicacio`, `adjudicatariNom`, `adjudicatariNif`, `organContractant`, `fase`
- Incluye **histórico completo** con todas las actualizaciones de estado de cada contrato
- Extraído mediante paginación con sub-segmentación automática (72K requests API)
- Pipeline incremental con `checkpoint`, reanudación por ramas, ficheros intermedios por `faseVigent`, salida `raw` + `clean` y soporte `--resume`
- Genera además un resumen por rama para auditar qué estrategia de segmentación ha seguido cada bloque pesado
- Tiene además un segundo paso opcional de `detail`, con caché SQLite y merge final sobre el endpoint público `detall-publicacio-expedient/{id}`
- Fuente: [PSCP Catalunya - buscador avanzado](https://contractaciopublica.cat)

---

## 🆕 Euskadi

Contratación pública del [País Vasco / Euskadi](https://www.contratacion.euskadi.eus/), combinando la API REST de KontratazioA con exports XLSX históricos de Open Data Euskadi y portales municipales independientes (Bilbao, Vitoria-Gasteiz). Arquitectura API-first con fallback a XLSX para series históricas.

| Dataset | Registros | Período | Fuente |
|---------|-----------|---------|--------|
| Contratos sector público | 664,545 | 2011-2026 | XLSX anual + JSON 2011-2013 |
| Poderes adjudicadores | ~919 | Actual | API REST KontratazioA |
| Empresas licitadoras | ~9,042 | Actual | API REST KontratazioA |
| REVASCON histórico | 34,523 | 2013-2018 | CSV/XLSX agregado anual |
| Bilbao contratos | 4,823 | 2005-2026 | Portal municipal Bilbao |
| Vitoria contratos menores | — | Actual | Open Data Euskadi |
| **Total** | **~704K** | **2005-2026** | — |

### Archivos

```
euskadi_parquet/
├── contratos_master.parquet             # 664K contratos (138 MB)
├── poderes_adjudicadores.parquet        # 919 poderes adjudicadores
├── empresas_licitadoras.parquet         # 9K empresas del registro
├── revascon_historico.parquet           # 34K registros 2013-2018
└── bilbao_contratos.parquet            # 4.8K contratos Bilbao

ccaa_euskadi.py                          # Scraper principal v4 (descarga)
consolidar_euskadi_v4.py                 # Consolidación → Parquet
```

### Campos principales (56 columnas — contratos_master)

| Categoría | Campos |
|-----------|--------|
| Identificación | codigo_contrato, numero_expediente, objeto |
| Órgano | poder_adjudicador, codigo_organismo, ambito |
| Tipo | tipo_contrato, procedimiento, tramitacion |
| Importes | importe_adjudicacion, importe_licitacion, valor_estimado |
| Adjudicación | adjudicatario, nif_adjudicatario |
| Fechas | fecha_adjudicacion, fecha_formalizacion, duracion |
| CPV | codigo_cpv |

### Arquitectura de fuentes

El scraper sigue una arquitectura **API-first** con múltiples capas de fallback:

**Módulo A — API REST KontratazioA** (fuente principal para catálogos)
- A1/A2: Contratos y anuncios (muestra 1K registros — bulk inviable: 655K items × 10/pág = 65K peticiones ~27h)
- A3: Poderes adjudicadores — 919 registros completos (92 páginas)
- A4: Empresas licitadoras — 9,042 registros completos (905 páginas)
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
| Contratación | 13 | 246K | REGCON 2014-2025 + DANA |
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

### Archivos

```
ccaa_Andalucia/
└── licitaciones_andalucia.parquet          # ~857K registros (47 MB, snappy)

scripts/
└── ccaa_andalucia.py                       # Scraper ES proxy 8D + multi-sort + salida CSV/Parquet
```

### Campos principales (34 columnas)

| Categoría | Campos |
|-----------|--------|
| Identificación | id_expediente, numero_expediente, titulo |
| Clasificación | tipo_contrato, estado, procedimiento, tramitacion |
| Órgano | perfil_contratante, provincia |
| Importes | importe_licitacion, valor_estimado, importe_adjudicacion |
| Adjudicación | adjudicatario, nif_adjudicatario |
| Fechas | fecha_publicacion, fecha_limite_presentacion |
| Otros | forma_presentacion, clausulas_sociales, clausulas_ambientales |

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

### Archivos

```
comunidad_madrid/
├── contratacion_comunidad_madrid_completo.parquet   # Dataset unificado (90 MB, snappy)
└── csv_originales/                                  # 765 CSVs individuales
```

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

### Archivos

```
galicia/
├── contratos_galicia_base.csv            # Dataset base de tabla (12 columnas)
├── contratos_galicia_base.parquet        # Base en parquet
├── contratos_galicia_detail.sqlite3      # Caché incremental del detalle HTML
├── contratos_galicia.csv                 # Dataset final mergeado (62 columnas)
├── contratos_galicia.parquet             # Dataset final en parquet
├── contratos_galicia_base_progress.json  # Checkpoint de organismos completados
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

### Archivos

```
ccaa_asturias/
└── asturias_contracts_ALL_YEARS.parquet   # 375K registros (21 MB, snappy)
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

# Nacional - PLACSP
df_nacional = pd.read_parquet('nacional/licitaciones_espana.parquet')

# TED - España (consolidado)
df_ted = pd.read_parquet('ted/ted_es_can.parquet')

# Andalucía - Contratación completa
df_and = pd.read_parquet('ccaa_Andalucia/licitaciones_andalucia.parquet')

# Euskadi - Contratos sector público
df_eus = pd.read_parquet('euskadi_parquet/contratos_master.parquet')

# Euskadi - Poderes adjudicadores
df_poderes = pd.read_parquet('euskadi_parquet/poderes_adjudicadores.parquet')

# Euskadi - Empresas licitadoras
df_empresas = pd.read_parquet('euskadi_parquet/empresas_licitadoras.parquet')

# Comunidad de Madrid - Contratación completa
df_cam = pd.read_parquet('comunidad_madrid/contratacion_comunidad_madrid_completo.parquet')

# Madrid Ayuntamiento - Actividad contractual
df_madrid = pd.read_parquet('madrid/actividad_contractual_madrid_completo.parquet')

# Catalunya - Contratos menores
df_cat_menors = pd.read_parquet('catalunya/contratacion/contractacio_menors.parquet')

# Catalunya - Subvenciones
df_cat_subv = pd.read_parquet('catalunya/subvenciones/raisc_subvenciones.parquet')

# Valencia - Contratación
df_val = pd.read_parquet('valencia/contratacion/')

# Valencia - Lobbies REGIA
df_lobbies = pd.read_parquet('valencia/lobbies/')

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
# Top adjudicatarios nacional
df_nacional.groupby('adjudicatario')['importe_sin_iva'].sum().nlargest(10)

# Contratos España publicados en TED por año
df_ted.groupby('year').size().plot(kind='bar', title='Contratos TED España')

# Andalucía: contratos menores por perfil contratante
and_menores = df_and[df_and['procedimiento'] == 'Contrato menor']
and_menores['perfil_contratante'].value_counts().head(20)

# Euskadi: gasto anual por tipo de contrato
df_eus.groupby(['anio', 'tipo_contrato'])['importe_adjudicacion'].sum().unstack().plot()

# Euskadi: top poderes adjudicadores por volumen
df_eus.groupby('poder_adjudicador')['importe_adjudicacion'].sum().nlargest(20)

# Euskadi: empresas más activas en el Registro de Licitadores
df_empresas['officialname'].value_counts().head(10)

# Comunidad de Madrid: contratos menores por hospital
cam_menores = df_cam[df_cam['Tipo de Publicación'] == 'Contratos menores']
cam_menores['Entidad Adjudicadora'].value_counts().head(20)

# Ayuntamiento Madrid: gasto por categoría y año
df_madrid.groupby(['categoria', 'anio'])['importe_adjudicacion_iva_inc'].sum().unstack(0).plot()

# Contratos SARA no publicados en TED
df_sara = pd.read_parquet('ted/crossval_sara_v2.parquet')
missing = df_sara[df_sara['_ted_missing']]
missing.groupby('organo_contratante').size().nlargest(10)

# Contratos menores Catalunya por órgano
df_cat_menors.groupby('organContractant')['pressupostAdjudicacio'].sum().nlargest(10)

# Evolución ERE/ERTE Valencia (2000-2025)
df_erte = pd.read_parquet('valencia/empleo/')
df_erte.groupby('año')['expedientes'].sum().plot()

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
ast_menores = df_ast[df_ast['CLASIFICACION GENERAL'] == 'MENOR']
ast_menores['ORGANO CONTRATANTE'].value_counts().head(20)
```

---

## 🔧 Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `nacional/licitaciones.py` | PLACSP | Extrae datos nacionales de ATOM/XML |
| `scripts/ccaa_andalucia.py` | Junta de Andalucía | Scraper ES proxy con subdivisión 8D + multi-sort 12x + salida reproducible |
| `ccaa_euskadi.py` | KontratazioA + Open Data Euskadi | Scraper v4: API REST + XLSX anuales + portales municipales |
| `consolidar_euskadi_v4.py` | — | Consolida JSON/XLSX/CSV → 5 Parquets normalizados |
| `descarga_contratacion_comunidad_madrid_v1.py` | contratos-publicos.comunidad.madrid | Web scraping con antibot bypass + subdivisión recursiva por importe |
| `ccaa_madrid_ayuntamiento.py` | datos.madrid.es | Descarga y unifica 67 CSVs (9 categorías, 12 estructuras) |
| `scripts/ccaa_cataluna_contratosmenores.py` | PSCP Catalunya | Scraper incremental con segmentación 10K, `checkpoint`, `raw` + `clean`, reanudación por ramas y fallback fino por `organ` |
| `scripts/ccaa_cataluna_contratosmenores_detail.py` | PSCP Catalunya | Detalle incremental sobre `detall-publicacio-expedient/{id}` con caché SQLite, parser `modern`/`legacy`/`aggregated` y merge final |
| `galicia/scraper_galicia.py` | contratosdegalicia.gal | Pipeline base + detalle HTML + merge, con discovery automático, barrido CM 3 meses, caché SQLite y `--resume` |
| `ccaa_asturias.py` | Principado de Asturias | Descarga contratación centralizada Asturias |
| `scripts/ccaa_catalunya.py` | Socrata | Descarga datos Catalunya |
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `ted/ted_module.py` | TED | Descarga CSV bulk + API v3 eForms |
| `ted/run_ted_crossvalidation.py` | — | Cross-validation PLACSP↔TED + matching avanzado (5 estrategias) |
| `ted/diagnostico_missing_ted.py` | — | Diagnóstico de missing |
| `ted/analisis_sector_salud.py` | — | Deep dive sector salud |
| `borme/scripts/borme_scraper.py` | BOE/BORME | Descarga ~126K PDFs del Registro Mercantil |
| `borme/scripts/borme_batch_parser.py` | — | Parser de actos mercantiles (constituciones, cargos...) |
| `borme/scripts/borme_anonymize.py` | — | Genera datasets públicos sin datos personales |
| `borme/scripts/borme_placsp_match.py` | — | Detector de anomalías BORME × PLACSP (5 flags) |
| `calidad/calidad_licitaciones.py` | — | 20 indicadores de calidad sobre PLACSP + TED + BORME |

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
pip install pandas pyarrow requests beautifulsoup4 pdfplumber python-dateutil
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

## 📈 Próximas CCAA

- [x] Euskadi ✅
- [x] Andalucía ✅
- [x] Madrid ✅
- [x] Galicia ✅
- [x] Asturias ✅
- [ ] Castilla y León

---

⭐ Si te resulta útil, dale una estrella al repo

[@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
