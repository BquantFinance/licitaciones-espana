# 🇪🇸 Datos Abiertos de Contratación Pública - España

Dataset completo de contratación pública española: nacional (PLACSP) + datos autonómicos (Andalucía, Catalunya, Valencia, Madrid) + cruce europeo (TED).

## 📊 Resumen de Datos

| Fuente | Registros | Período | Tamaño |
|--------|-----------|---------|--------|
| Nacional (PLACSP) | 8.7M | 2012-2026 | 780 MB |
| 🆕 Andalucía | 808K | 2016-2026 | 47 MB |
| Catalunya | 20.6M | 2014-2025 | ~180 MB |
| Valencia | 8.5M | 2000-2026 | 156 MB |
| Madrid – Comunidad | 2.56M | 2017-2025 | 90 MB |
| Madrid – Ayuntamiento | 119K | 2015-2025 | ~40 MB |
| TED (España) | 591K | 2010-2025 | 57 MB |
| **TOTAL** | **~42M** | **2000-2026** | **~1.4 GB** |

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

## 🆕 Andalucía

Contratación pública de la [Junta de Andalucía](https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/perfiles-licitaciones/buscador-general), incluyendo licitaciones regulares y contratos menores de todos los organismos y empresas públicas andaluzas. Extraído mediante ingeniería inversa del proxy Elasticsearch del portal, con estrategia de subdivión recursiva en 8 dimensiones para superar el límite de 10K resultados por consulta.

| Tipo | Registros | Cobertura |
|------|-----------|-----------|
| Licitaciones regulares (estándar) | 72,165 | 92% |
| Contratos menores | 736,276 | 95% |
| **Total** | **808,441** | **95%** |

### Archivos

```
ccaa_Andalucia/
└── licitaciones_andalucia.parquet          # 808K registros (47 MB, snappy)

scripts/
└── ccaa_andalucia.py                       # Scraper ES proxy 8D + multi-sort
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
5. **perfilContratante.codigo**: 372 organismos
6. **provinciasEjecucion**: 8 provincias + null
7. **formaPresentacion**: 6 valores + null
8. **numeroExpediente (año)**: match por texto "2018"-"2026" + null

Para los chunks que aún superan 10K tras las 8 dimensiones (ej. SYBS03/Servicio Andaluz de Salud con 290K registros), se usa **multi-sort con 12 órdenes** distintas (idExpediente, importeLicitacion, numeroExpediente, titulo, fechaLimitePresentacion, adjudicaciones.importeAdjudicacion — cada una asc/desc) que acceden a ventanas diferentes de 10K registros con 0% de solapamiento.

### Perfiles incluidos (372)

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

## 📥 Uso

```python
import pandas as pd

# Nacional - PLACSP
df_nacional = pd.read_parquet('nacional/licitaciones_espana.parquet')

# TED - España (consolidado)
df_ted = pd.read_parquet('ted/ted_es_can.parquet')

# Andalucía - Contratación completa
df_and = pd.read_parquet('ccaa_Andalucia/licitaciones_andalucia.parquet')

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

# Andalucía: gasto del SAS por provincia
sas = df_and[df_and['perfil_contratante'] == 'SYBS03']
sas.groupby('provincia')['importe_licitacion'].sum().sort_values()

# Comunidad de Madrid: contratos menores por hospital
cam_menores = df_cam[df_cam['Tipo de Publicación'] == 'Contratos menores']
cam_menores['Entidad Adjudicadora'].value_counts().head(20)

# Comunidad de Madrid: gasto por tipo de publicación
df_cam.groupby('Tipo de Publicación')['Importe de adjudicación'].sum().sort_values()

# Ayuntamiento Madrid: gasto por categoría y año
df_madrid.groupby(['categoria', 'anio'])['importe_adjudicacion_iva_inc'].sum().unstack(0).plot()

# Ayuntamiento Madrid: top adjudicatarios en contratos formalizados
form = df_madrid[df_madrid['categoria'] == 'contratos_formalizados']
form.groupby('razon_social_adjudicatario')['importe_adjudicacion_iva_inc'].sum().nlargest(10)

# Contratos SARA no publicados en TED
df_sara = pd.read_parquet('ted/crossval_sara_v2.parquet')
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
| `scripts/ccaa_andalucia.py` | Junta de Andalucía | Scraper ES proxy con subdivisión 8D + multi-sort 12x |
| `descarga_contratacion_comunidad_madrid_v1.py` | contratos-publicos.comunidad.madrid | Web scraping con antibot bypass + subdivisión recursiva por importe |
| `ccaa_madrid_ayuntamiento.py` | datos.madrid.es | Descarga y unifica 67 CSVs (9 categorías, 12 estructuras) |
| `scripts/ccaa_cataluna_contratosmenores.py` | Socrata | Descarga contratos menores Catalunya |
| `scripts/ccaa_catalunya.py` | Socrata | Descarga datos Catalunya |
| `scripts/ccaa_valencia.py` | CKAN | Descarga datos Valencia |
| `ted/ted_module.py` | TED | Descarga CSV bulk + API v3 eForms |
| `ted/run_ted_crossvalidation.py` | — | Cross-validation PLACSP↔TED + matching avanzado (5 estrategias) |
| `ted/diagnostico_missing_ted.py` | — | Diagnóstico de missing |
| `ted/analisis_sector_salud.py` | — | Deep dive sector salud |

---

## 🔄 Actualización

| Fuente | Frecuencia |
|--------|------------|
| PLACSP | Mensual |
| TED | Trimestral (API) / Anual (CSV bulk) |
| Andalucía | Trimestral (re-ejecutar script) |
| Madrid – Comunidad | Trimestral (re-ejecutar script) |
| Madrid – Ayuntamiento | Anual (nuevos CSVs por año) |
| Catalunya | Variable (depende del dataset) |
| Valencia | Diaria/Mensual (depende del dataset) |

---

## 📋 Requisitos

```bash
pip install pandas pyarrow requests beautifulsoup4
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
| Andalucía | https://www.juntadeandalucia.es/contratacion/ |
| Madrid – Comunidad | https://contratos-publicos.comunidad.madrid/ |
| Madrid – Ayuntamiento | https://datos.madrid.es/ |
| Catalunya | https://analisi.transparenciacatalunya.cat/ |
| Valencia | https://dadesobertes.gva.es/ |
| BQuant Finance | https://bquantfinance.com |

---

## 📈 Próximas CCAA

- [ ] Euskadi
- [x] Andalucía ✅
- [x] Madrid ✅

---

⭐ Si te resulta útil, dale una estrella al repo

[@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
