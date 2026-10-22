# Galicia

- **En la release:** `contratos_galicia.zip`: tabla final `contratos_galicia.parquet` (1.848.770 filas, 69 columnas) y base `contratos_galicia_base.parquet` (las mismas filas, 12 columnas); y, en `v2026.02/`, el CSV de v2026.02 (`contratos_galicia_COMPLETO.csv`), con 913 contratos menores que la descarga actual no trae.
- **Script:** [`galicia/scraper_galicia.py`](../../galicia/scraper_galicia.py).
- **Portal:** [contratosdegalicia.gal](https://www.contratosdegalicia.gal/).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

Contratación pública completa de la [Xunta de Galicia](https://www.contratosdegalicia.gal) y todos sus organismos dependientes, extraída mediante ingeniería inversa de la API jQuery DataTables del portal. Incluye contratos menores (adjudicación directa, desde 2018) y licitaciones formales (desde 2007) de 418 organismos.

En v2026.02 (v2026.10, regenerado, tiene 1.848.770 filas):

| Tipo | Registros | Período |
|------|-----------|---------|
| Contratos menores | 1,635,407 | 2018-2026 |
| Licitaciones | 50,382 | 2007-2026 |
| **Total** | **1,685,789** | **2007-2026** |

> v2026.10 está regenerado: `importe` corregido (el 0,2 % de los contratos menores supera 48.400 €, frente al 55 % de v2026.02), 1.848.770 filas. Las columnas `detail_*` van vacías (`detail_status = missing`): esa descarga no ejecutó la fase de detalle. El CSV de v2026.02 va también, tal cual, en `v2026.02/` de `contratos_galicia.zip`: trae 913 contratos menores que la descarga actual no trae.
>
> ⚠️ En los datos de v2026.02 (`contratos_galicia.parquet` de la etiqueta `datos-lfs-v2026.02` / `contratos_galicia.zip`) la columna `importe` está inflada ×10 o ×100: el scraper eliminaba el punto decimal de los importes de la API como si fuera separador de miles (674.78 → 67478). El 55 % de los contratos menores publicados supera 48.400 € (imposible por ley) y suman 547.600 M€. El scraper ya está corregido (v2026.10 está regenerado). El fichero de v2026.02 solo tiene las 12 columnas base; con la fase de detalle son 64, incluidas `detail_adjudicaciones_json` (adjudicatarios e importes por lote, que para las licitaciones solo están en el detalle) y `detail_campos_extra_json`. El scraper compara lo descargado con los totales que declara el portal por organismo.

## Archivos

```
galicia/
├── contratos_galicia_base.csv            # Dataset base de tabla (12 columnas)
├── contratos_galicia_base.parquet        # Base en parquet
├── contratos_galicia_detail.sqlite3      # Caché incremental del detalle HTML
├── contratos_galicia.csv                 # Dataset final mergeado (62 columnas + nombre del organismo + 3 de control)
├── contratos_galicia.parquet             # Dataset final en parquet
├── licitadores_galicia.csv               # Relación de licitadores presentados (una fila por licitador)
├── licitadores_galicia.parquet           # La misma en parquet
├── _historico/                           # versiones anteriores de base y final
├── contratos_galicia_base_progress.json  # Checkpoint y ámbito leído de cada organismo
└── scraper_galicia.py                    # Pipeline base + detail + merge
```

## Campos principales

**Base (12 columnas)**: `id`, `objeto`, `importe`, `estado`, `estadoDesc`, `publicado`, `modificado`, `_organismo_id`, `_tipo`, `nif`, `adjudicatario`, `duracion`

**Nombre del organismo**: `_organismo_nombre`, en la tabla final detrás de las 12 columnas base. Es el de la página del organismo (`consultaOrganismo.jsp`), que el scraper visita para la sesión: el nombre actual, en todas las filas del organismo (también las retiradas). Los de cada descarga quedan en el manifiesto (`nombres`); si una descarga no lee un organismo, se conserva el de la tabla anterior.

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

## Estrategia de descarga

El portal usa jQuery DataTables con server-side processing y dos endpoints separados:

- **Licitaciones**: `/api/v1/organismos/{id}/licitaciones/table` — paginación estándar, sin restricciones temporales
- **Contratos menores**: `/api/v1/organismos/{id}/contratosmenores/table` — requiere header `Referer` dinámico por organismo y rechaza rangos de fecha >3 meses

**Discovery automático**: El scraper prueba IDs de organismo 1–2000 contra ambos endpoints (licitaciones en paralelo, CM secuencial por la restricción del `Referer`) para descubrir los organismos activos.

**Barrido temporal CM**: Ventanas de 3 meses desde la fecha actual hasta 2000-01-01. El servidor reporta `recordsTotal` global (ignorando el filtro de fecha), pero los datos devueltos sí están filtrados. Deduplicación por `(id, _tipo)` para eliminar solapamientos entre ventanas.

**Orden de las páginas**: por `id`, único en cada listado. Ordenadas por `publicado` (el orden del navegador), los empates salían en otro orden en cada página: la primera descarga de producción (29-sep-2026) dejó 45 ventanas de CM incompletas y 989 menores sin descargar. Con el orden por `id` las 45 llegan completas (19.364 de 19.364, medido en vivo). Una ventana o un listado de LIC que no llega completo se repite hasta dos veces antes de darlo por incompleto.

**Detalle HTML real**: El portal no expone un endpoint JSON útil para la ficha; los campos adicionales salen de `POST /licitacion`. El scraper hace un segundo paso de enriquecimiento HTML para `LIC` y `CM`.

**Sin sesgo del superviviente.**
- Una descarga base nueva no borra la anterior: pasa a `_historico/`.
- La caché SQLite de fichas nunca se borra. Una ficha ya descargada no se sustituye por un error ni por una vacía, y si el portal la cambia la anterior queda en `detail_cache_historico`.
- La tabla final se acumula con `acumular`: lo retirado queda con `_en_ultima_descarga=False`, pero solo en lo que se ha vuelto a leer completo.
  - LIC: si llegan exactamente `recordsTotal` ids.
  - Ventanas de CM: si traen exactamente `recordsFiltered` ids.
- `--semilla` añade por (`_tipo`, `id`) lo que ya no se sirve. El importe publicado (inflado ×10/×100) va a `importe_semilla` sin corregir.
  - También las filas de los organismos que el portal ha retirado enteros: los que no están en la lista de organismos que la descarga guarda en el manifiesto (`descubrimientos`) y la descarga no ha leído.
  - Sin esa lista (`--organismo` o una descarga del código anterior) no se da por retirado ninguno. Tampoco si serían más de 20 organismos (`--max-organismos-retirados`), por si la lista está mal leída. En los dos casos, con aviso.
- `detail` sin `--resume` ya no rehace todo: reintenta las fichas pendientes. Para rehacerlo todo, `--force-detail`.

**Pipeline incremental y reanudable**:

- `all`: ejecuta `base -> detail -> merge`
- `base`: reconstruye solo el dataset base de tabla
- `detail`: enriquece HTML sobre el base ya descargado
- `merge`: junta base + detalle en el dataset final y exporta la relación de licitadores
- `licitadores`: solo exporta la relación de licitadores de la caché (no pide nada al portal)

El progreso base se guarda por organismo en `contratos_galicia_base_progress.json` y el detalle se cachea en `contratos_galicia_detail.sqlite3`, de forma que si la ejecución se corta o el portal devuelve `403/429`, se puede retomar con `--resume`.

**Salida reproducible**: otra persona puede rehacer la descarga completa ejecutando el mismo pipeline y obteniendo base, detalle cacheado y merge final.

## Relación de licitadores

La ficha trae, en una ventana oculta que se abre desde la columna «Part.» (participantes) de la tabla de la resolución, la **relación de licitadores presentados**: una fila por licitador con `Lote`, `NIF` y `Nombre` (`Nome` en gallego). Ya va en el crudo de la caché (`tables` de `raw_gzip`), así que se extrae sin volver a pedir ninguna ficha.

- `detail` la escribe con cada lote de fichas que guarda en dos tablas de la caché: `licitadores` (una fila por licitador) y `licitadores_fichas` (las fichas leídas, con la versión de la que salen —`updated_at` y `html_sha256`— y si traían la ventana). Un fallo al leerla no para la descarga: esas fichas quedan pendientes.
- `merge` y `licitadores` leen antes las fichas pendientes (las descargadas por una versión anterior del scraper o una ficha que el portal ha cambiado: sus filas se sustituyen) y exportan `licitadores_galicia.csv` y `.parquet` con `guardar_version`. Es reanudable: se confirma cada 2.000 fichas y la siguiente vuelta sigue por donde iba. Unas 100.000 fichas se leen en ~35 s; sin pendientes, la exportación tarda segundos.
- Fichas descargadas con `--no-raw-detail` no tienen crudo: no se pueden leer.

Columnas de `licitadores_galicia.csv` (una fila por licitador y lote; sin ninguno, solo las cabeceras):

| Columna | Qué es |
|---|---|
| `_tipo`, `id`, `_organismo_id` | la ficha, con los nombres del CSV base (la tabla final se une por `_tipo` + `id`) |
| `lote` | el lote tal cual; vacío en una ficha sin lotes (`-` en la ventana) |
| `orden` | posición en la relación (1, 2…) |
| `nif`, `nombre` | tal cual. Una UTE puede venir con el NIF provisional `U00000000` y los NIF de sus socios en el nombre; si la celda trae varios nombres (lista), van unidos por un espacio |
| `importe_oferta`, `puntuacion` | **vacías**: la ficha no publica la oferta ni la puntuación de cada licitador (se mantienen para que el esquema sea el de otras fuentes que sí lo hacen) |
| `situacion` | `presentado` (lo que dice la ventana: no publica admitidos ni excluidos) |
| `es_adjudicatario`, `es_adjudicatario_via` | deducido de la propia ficha: `nif_formalizacion` (su NIF está en el contratista de la formalización de su lote) o `nombre_resolucion` (su nombre, sin tildes, signos ni espacios, es el adjudicatario de una resolución «Adjudicado» de su lote). `False` si su lote tiene resolución o formalización y no es él; vacío si la ficha no tiene ninguna (en plazo) |
| `detail_updated_at` | cuándo se descargó la ficha de la que sale |

Las filas sin NIF ni nombre no entran, y una fila repetida (mismo lote, NIF y nombre) entra una vez. La ventana marca sus filas con el lote y la resolución (`filaLic_<lote>_<resolución>`), pero el crudo no guarda esa marca: el lote sale de la columna `Lote`.

Medido en una muestra de 45 fichas de 2024 y 2025 (37 menores y 8 licitaciones): las 45 traen la ventana, con tantas filas como dice «Part.», y las 45 tienen exactamente un licitador adjudicatario. En los menores la relación tiene un solo licitador, el adjudicatario (37 de 37); las licitaciones, de 1 a 5 (media 2,5). Ninguna ficha de la muestra tenía lotes.

## Validación del pipeline

Nota de cuando se escribió el pipeline actual (antes de la fase de detalle con `_organismo_nombre`). En una validación real sobre un organismo mixto (`33`), el scraper antiguo seguía sacando `152` filas y `12` columnas. El pipeline nuevo mantiene las `152` filas, genera un base estable de `12` columnas y un dataset final enriquecido de `62` columnas, con `152/152` fichas HTML resueltas y una segunda pasada `detail --resume` que no rehace trabajo (`0` procesados).

Este diseño está pensado para ejecutar el backfill histórico completo sin depender de memoria RAM ni de una corrida monolítica: si el portal corta la sesión o devuelve `403/429`, basta con relanzar la fase correspondiente con `--resume`.

## Uso

```python
import pandas as pd

gal = pd.read_parquet("contratos_galicia/contratos_galicia.parquet",
                      columns=["id", "_tipo", "nif", "adjudicatario", "importe", "publicado", "_ultima_descarga"])

# Una fila por contrato (_tipo + id) y versión: si el portal cambia un contrato, la versión anterior se
# conserva con _en_ultima_descarga=False. En v2026.10 no hay ninguna, pero la regla vale para las siguientes.
gal = gal.sort_values("_ultima_descarga").drop_duplicates(["_tipo", "id"], keep="last")

# Contratos menores (CM) por año: número e importe (corregido en v2026.10; en v2026.02 estaba inflado)
cm = gal[gal["_tipo"] == "CM"]
cm.groupby(cm["publicado"].dt.year)["importe"].agg(["count", "sum"])
```

Las filas que vienen de la semilla de v2026.02 llevan el importe publicado entonces, inflado, en `importe_semilla`, e `importe` vacío: no suman.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `galicia/scraper_galicia.py` | contratosdegalicia.gal | Pipeline base + detalle HTML + merge, con discovery automático, barrido CM 3 meses, caché SQLite y `--resume` |

---

[Todas las fuentes](README.md) · [README](../../README.md)
