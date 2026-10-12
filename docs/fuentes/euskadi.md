# País Vasco (Euskadi)

- **En la release:** `euskadi.zip` (9 tablas, 2.753.290 filas) y, en `v2026.02/`, `bilbao_contratos.parquet`, `contratos_master.parquet` y `empresas_licitadoras.parquet` de v2026.02.
- **Script:** [`Euskadi/ccaa_euskadi.py`](../../Euskadi/ccaa_euskadi.py) (descarga) y [`Euskadi/consolidacion_euskadi.py`](../../Euskadi/consolidacion_euskadi.py) (consolida JSON, XLSX y CSV en 9 tablas Parquet).
- **Portal:** [contratacion.euskadi.eus](https://www.contratacion.euskadi.eus/) (KontratazioA y su [API REST](https://api.euskadi.eus/procurements/)), [opendata.euskadi.eus](https://opendata.euskadi.eus/) y los portales de Bilbao y Vitoria-Gasteiz.
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## v2026.10: `euskadi.zip`

| Fichero | Contenido | Filas |
|---|---|--:|
| `contratos_master.parquet` | Anuncios de contratación (XLSX anuales y JSON 2011-2013): metadatos, sin importes | 727.690 |
| `api_contratos.parquet` | API `/contracts` de KontratazioA: contratos con importe y adjudicatario | 715.868 |
| `api_anuncios.parquet` | API `/contracting-notices` de KontratazioA | 704.529 |
| `revascon_por_poder.parquet` | REVASCON por poder adjudicador y año | 557.580 |
| `revascon_historico.parquet` | REVASCON agregado 2013-2018 (con importes) | 34.507 |
| `empresas_licitadoras.parquet` | Registro de Licitadores (API) | 9.357 |
| `vitoria_contratos.parquet` | Vitoria-Gasteiz, contratos | 2.347 |
| `poderes_adjudicadores.parquet` | Poderes adjudicadores (API) | 938 |
| `ultimos_90d.parquet` | Ventana de los últimos 90 días | 474 |

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

> **v2026.10** está regenerado con la consolidación actual (9 tablas, 2,75M filas), incluida la API `/contracts` completa (`api_contratos`, 715.868 contratos con importe y adjudicatario), y, en la carpeta `v2026.02/` de `euskadi.zip`, `bilbao_contratos.parquet`: es la última versión publicada (v2026.02, febrero de 2026), porque el portal de datos abiertos de Bilbao responde 403 a la descarga actual; se actualizará cuando vuelva a servirse. En esa carpeta van también `contratos_master` y `empresas_licitadoras` de v2026.02, que traen filas que ya no se sirven.
>
> ⚠️ Los parquet de Euskadi de v2026.02 (los que estuvieron en Git LFS en el repositorio, ahora en la etiqueta `datos-lfs-v2026.02`) tienen errores de consolidación ya corregidos: en `revascon_historico` 31.191 de las 34.523 filas (REVASCON 2015-2018) salieron como columnas `unnamed:_N` porque el XLSX trae filas de título antes de la cabecera; en `bilbao_contratos` los importes están divididos entre 1.000 (`"52.990"` → 52,99) y la fecha de adjudicación tiene día y mes invertidos; `contratos_master` tiene los años 2011-2013 en columnas aparte y 18.826 filas de `contratos_2021.xlsx` con las columnas corridas 2-3 posiciones (URL en la fecha límite, expediente en la fecha de publicación…). La consolidación actual conserva todas las filas y celdas de los ficheros originales (verificado fichero a fichero): recoloca esas filas y lo indica en `_columnas_corridas`, y las filas repetidas (los JSON 2012-2013 repiten filas del de 2011; REVASCON repite contratos entre años; la API de empresas devuelve 25 empresas dos veces) se conservan marcadas en `_duplicado`. `contratos_master` son **metadatos de anuncios**: ninguna fuente de B1 incluye importes, adjudicatario, NIF, CPV ni procedimiento, así que para 2019-2026 no hay importes de adjudicación de Euskadi en los datos publicados. La API `/contracts` de KontratazioA tiene 655.518 contratos con importe y adjudicatario, pero el scraper solo obtenía una muestra de 10 (la API repetía la página 1); ver [docs/COBERTURA.md](../COBERTURA.md).

## Archivos

Los Parquet de v2026.02 que estuvieron en el repositorio (Git LFS; ahora en la etiqueta `datos-lfs-v2026.02`):

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

## Campos principales (contratos_master)

| Categoría | Campos |
|-----------|--------|
| Anuncio | nombre, descripción, colección, titulo_del_contrato, objeto_del_contrato, tipo_de_anuncio, fecha_de_publicación_documento |
| Expediente | expediente, estado_de_la_tramitacion, contrato_menor, adjudicación, subsanación, apertura_de_plicas, acuerdos_de_la_mesa_de_contratacion |
| Órgano | ámbito_geográfico_del_poder_adjudicador, entidad_que_impulsa_la_contratación, órgano_de_contratación, institución, departamento, órgano_gestor |
| Enlaces | url_física, url_amigable, xml_datos, xml_metadatos, zip |
| Fechas | fecha_límite_de_presentación, fecha_de_creación |
| Origen | _archivo_origen, _year (año del fichero, no del contrato), _fuente |

Los importes y adjudicatarios de Euskadi están en `revascon_historico` (2013-2018: tipo_de_contrato, adjudicatario, importe_de_adjudicación_con_iva, fecha_de_formalización...) y `bilbao_contratos`.

## Arquitectura de fuentes

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

## Notas técnicas

- La API de KontratazioA usa `?currentPage=N` para paginación (no `page`, `_page`, ni HATEOAS). El parámetro `_pageSize` se ignora (fijo a 10).
- Los XLSX de 2011-2013 se publican vacíos (solo cabeceras), pero los JSON del mismo endpoint de Open Data sí contienen los datos completos (9,482 registros combinados).
- El consolidador convierte columnas con listas/dicts a JSON string antes de deduplicar, necesario para los campos anidados de la API (clasificaciones, categorías).

## Uso

- **Importes y adjudicatarios de 2019 en adelante:** `api_contratos.parquet`. `contratos_master` son metadatos de anuncios, sin importes (ver arriba).
- **Filas repetidas:** se conservan y se marcan en `_duplicado` (los JSON 2012-2013 repiten filas del de 2011; REVASCON repite contratos entre años). Para sumar, quítalas: `df[~df['_duplicado']]`. Las filas recolocadas llevan `_columnas_corridas`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `Euskadi/ccaa_euskadi.py` | KontratazioA + Open Data Euskadi | Scraper v4 (solo descarga): API REST + XLSX anuales + portales municipales |
| `Euskadi/consolidacion_euskadi.py` | — | Consolida JSON/XLSX/CSV → 9 tablas Parquet normalizadas |

---

[Todas las fuentes](README.md) · [README](../../README.md)
