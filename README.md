# 🇪🇸 Datos Abiertos de Contratación Pública - España

Dataset completo de contratación pública española: nacional (PLACSP) + datos autonómicos (Catalunya, Valencia) + cruce europeo (TED).

## 📊 Resumen de Datos

| Fuente | Registros | Período | Tamaño |
|--------|-----------|---------|--------|
| Nacional (PLACSP) | 8.7M | 2012-2026 | 780 MB |
| Catalunya | 20.6M | 2014-2025 | ~180 MB |
| Valencia | 8.5M | 2000-2026 | 156 MB |
| 🆕 TED (España) | 591K | 2010-2025 | 57 MB |
| **TOTAL** | **38.4M** | **2000-2026** | **~1.2 GB** |

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
├── run_ted_crossvalidation.py       # Cross-validation PLACSP↔TED
├── matching_avanzado_ted.py         # Matching avanzado (5 estrategias)
├── diagnostico_missing_ted.py       # Diagnóstico de missing
├── analisis_sector_salud.py         # Deep dive sector salud
├── cross-validation_ted_placsp.py   # Exploración inicial
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
| `ted/run_ted_crossvalidation.py` | Cross-validation con reglas SARA por bienio/tipo/comprador |
| `ted/matching_avanzado_ted.py` | Matching avanzado: 5 estrategias (E1-E5) |
| `ted/diagnostico_missing_ted.py` | Diagnóstico de missing: falsos positivos vs gaps reales |
| `ted/analisis_sector_salud.py` | Deep dive sector salud: lotes, acuerdos marco, CPV, CCAA |
| `ted/cross-validation_ted_placsp.py` | Script de exploración inicial |

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
│   └── contractacio_menors.parquet          # 3.0M contratos menores 🆕
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
- Fuente: [Transparència Catalunya - Contractació Pública](https://analisi.transparenciacatalunya.cat)

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

## 📥 Uso

```python
import pandas as pd

# Nacional - PLACSP
df_nacional = pd.read_parquet('nacional/licitaciones_espana.parquet')

# TED - España (consolidado)
df_ted = pd.read_parquet('ted/ted_es_can.parquet')

# Catalunya - Contratos menores
df_cat_menors = pd.read_parquet('catalunya/contratacion/contractacio_menors.parquet')

# Catalunya - Subvenciones
df_cat_subv = pd.read_parquet('catalunya/subvenciones/raisc_subvenciones.parquet')

# Valencia - Contratación
df_val = pd.read_parquet('valencia/contratacion/')

# Valencia - Lobbies REGIA
df_lobbies = pd.read_parquet('valencia/lobbies/')
```

### Ejemplos de análisis

```python
# Top adjudicatarios nacional
df_nacional.groupby('adjudicatario')['importe_sin_iva'].sum().nlargest(10)

# Contratos España publicados en TED por año
df_ted.groupby('year').size().plot(kind='bar', title='Contratos TED España')

# Contratos SARA no publicados en TED
df_sara = pd.read_parquet('ted/crossval_sara_v2.parquet')  # generado por el pipeline
missing = df_sara[df_sara['_ted_missing']]
missing.groupby('organo_contratante').size().nlargest(10)

# Contratos menores Catalunya por órgano
df_cat_menors.groupby('organContractant')['pressupostAdjudicacio'].sum().nlargest(10)

# Evolución ERE/ERTE Valencia (2000-2025)
df_erte = pd.read_parquet('valencia/empleo/')
df_erte.groupby('año')['expedientes'].sum().plot()

# Lobbies por sector
df_regia = pd.read_parquet('valencia/lobbies/')
df_regia['sector'].value_counts()
```

---

## 🔧 Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `nacional/licitaciones.py` | PLACSP | Extrae datos nacionales de ATOM/XML |
| `scripts/ccaa_cataluna_contratosmenores.py` | Socrata | Descarga contratos menores Catalunya |
| `scripts/ccaa_catalunya.py` | Socrata | Descarga datos Catalunya |
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `ted/ted_module.py` | TED | Descarga CSV bulk + API v3 eForms |
| `ted/run_ted_crossvalidation.py` | — | Cross-validation PLACSP↔TED (reglas SARA) |
| `ted/matching_avanzado_ted.py` | — | Matching avanzado (5 estrategias) |
| `ted/diagnostico_missing_ted.py` | — | Diagnóstico de missing |
| `ted/analisis_sector_salud.py` | — | Deep dive sector salud |

---

## 🔄 Actualización

| Fuente | Frecuencia |
|--------|------------|
| PLACSP | Mensual |
| TED | Trimestral (API) / Anual (CSV bulk) |
| Catalunya | Variable (depende del dataset) |
| Valencia | Diaria/Mensual (depende del dataset) |

---

## 📋 Requisitos

```bash
pip install pandas pyarrow requests
```

---

## 📄 Licencia

Datos públicos del Gobierno de España, Unión Europea y CCAA.

- España: [Licencia de Reutilización](https://datos.gob.es/es/aviso-legal)
- TED: [EU Open Data Licence](https://data.europa.eu/eli/dec_impl/2011/833/oj)

---

## 🔗 Fuentes

| Portal | URL |
|--------|-----|
| PLACSP | https://contrataciondelsectorpublico.gob.es/ |
| TED | https://ted.europa.eu/ |
| TED API v3 | https://ted.europa.eu/api/docs/ |
| TED CSV Bulk | https://data.europa.eu/data/datasets/ted-csv |
| Catalunya | https://analisi.transparenciacatalunya.cat/ |
| Valencia | https://dadesobertes.gva.es/ |
| BQuant Finance | https://bquantfinance.com |

---

## 📈 Próximas CCAA

- [ ] Euskadi
- [ ] Andalucía
- [ ] Madrid

---

⭐ Si te resulta útil, dale una estrella al repo

[@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
