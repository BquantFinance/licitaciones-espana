# Asturias

- **En la release:** `asturias.zip`: `asturias_contracts_ALL_YEARS.parquet` (375.380 filas, 103 columnas: las 99 publicadas, `_origen` y las 3 de control).
- **Script:** [`scripts/ccaa_asturias.py`](../../scripts/ccaa_asturias.py).
- **Portal:** [sede.asturias.es](https://sede.asturias.es/), contratación centralizada del Principado (CSV anuales).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

Gijón publica sus menores aparte: están en [`municipios.zip`](municipios.md).

Contratación centralizada del [Principado de Asturias](https://sede.asturias.es/), incluyendo contratos menores, servicios, obras y suministros de todos los organismos y entes públicos del Principado.

| Métrica | Valor |
|---------|-------|
| Registros | 375,380 |
| Período | 2019-2024 |
| Columnas | 99 |
| Tamaño | 21 MB |

> v2026.10 está regenerado: el IVA de 2023 ya es 21/10/4 y los importes son numéricos. Sigue siendo 2019-2024: el Principado no publicaba aún los ficheros de 2025 ni de 2026 (404 el 2026-09-28).
>
> ⚠️ En el parquet de v2026.02 la columna `IVA` de 2023 está multiplicada ×10 (210/100/40/50 en lugar de 21/10/4/5) y, con pandas 3, los importes quedaban como texto. El script ya está corregido y escribe en `ccaa_asturias/` (v2026.10 está regenerado). Además: descarga todos los años hasta el actual que publique el Principado (antes solo 2019-2024), lee los CSV como Windows-1252 (5.099 caracteres corruptos), mantiene `Nº EXPEDIENTE ORGANO` como texto (67.440 valores pasaban a NaN) y guarda las líneas mal formadas en `lineas_descartadas_AAAA.csv` en vez de descartarlas.

## Archivos

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

## Campos principales (99 columnas)

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

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_asturias.py` | Principado de Asturias | Descarga contratación centralizada Asturias → `ccaa_asturias/` |

---

[Todas las fuentes](README.md) · [README](../../README.md)
