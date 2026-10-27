# Comunidad de Madrid

- **En la release:** `comunidad_madrid.zip`: `contratacion_comunidad_madrid_completo.parquet` (4.906.068 filas, 22 columnas).
- **Script:** [`comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py`](../../comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py).
- **Portal:** [contratos-publicos.comunidad.madrid](https://contratos-publicos.comunidad.madrid/).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

Contratación pública completa de la [Comunidad de Madrid](https://contratos-publicos.comunidad.madrid), incluyendo todas las consejerías, hospitales, organismos autónomos y empresas públicas. Extraído mediante web scraping del buscador avanzado con resolución del módulo antibot de Drupal.

Por tipo de publicación, en v2026.02 (v2026.10, regenerado, tiene 4.906.068 filas: ver el aviso):

| Tipo de publicación | Registros | Presupuesto licitación | Importe adjudicación |
|---------------------|-----------|----------------------|---------------------|
| Contratos menores | 2,529,049 | 487M € | 487M € |
| Convocatoria anunciada a licitación | 21,070 | 39,551M € | — |
| Contratos adjudicados sin publicidad | 10,035 | 8,466M € | — |
| Encargos a medios propios | 2,178 | 173M € | — |
| Anuncio de información previa | 1,166 | 327M € | — |
| Consultas preliminares del mercado | 28 | — | — |
| **Total** | **2,563,527** | **49,004M €** | **487M €** |

> ⚠️ El portal pone cada lote, adjudicatario, prórroga o modificación adicional en una **fila de continuación** sin tipo ni referencia justo después de su contrato (el 36 % de las filas en una muestra independiente de 30.907). El dataset de v2026.02 deduplicaba por expediente + referencia + entidad y las perdió todas (queda 1). El script ahora solo descarta bloques completos repetidos en CSV distintos (consultas solapadas) y conserva los duplicados de origen, descarga también los anuncios de 2014-2016 y los menores con presupuesto ≤0 o >50.000 € de las entidades subdivididas, y vuelve a descargar los CSV acumulativos (el publicado se corta el 2025-09-30). Pendiente: los menores de entidades históricas que ya no aparecen en el desplegable (p.ej. consejerías de legislaturas anteriores). v2026.10 está regenerado con el script actual: 4.906.068 filas (v2026.02: 2.563.527).

## Archivos

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

## Campos principales (18 columnas publicadas, más `_archivo_fuente` y las 3 de control)

| Categoría | Campos |
|-----------|--------|
| Identificación | Nº Expediente, Referencia, Título del contrato |
| Clasificación | Tipo de Publicación, Estado, Tipo de contrato |
| Entidad | Entidad Adjudicadora |
| Proceso | Procedimiento de adjudicación, Presupuesto de licitación, Nº de ofertas |
| Adjudicación | Resultado, NIF del adjudicatario, Adjudicatario, Importe de adjudicación |
| Incidencias | Importe de las modificaciones, Importe de las prórrogas, Importe de la liquidación |
| Temporal | Fecha del contrato |

## Estrategia de descarga

El portal de la Comunidad de Madrid usa un módulo antibot de Drupal y tiene restricciones complejas en los filtros de búsqueda que requirieron ingeniería inversa:

- **Antibot key**: El JavaScript del portal transforma la clave de autenticación invirtiendo pares de 2 caracteres desde el final. El script replica esta transformación.
- **CAPTCHA matemático**: Cada descarga CSV requiere resolver una operación aritmética (ej. `3 + 8 =`).
- **Contratos menores** (~99% del volumen): El filtro `fecha_hasta` es incompatible con este tipo de publicación, y `fecha_desde` no funciona combinado con `entidad_adjudicadora`. Solución: descargar por **entidad adjudicadora** (125 entidades) sin filtro de fecha.
- **Subdivisión recursiva**: Las entidades con >50K registros (hospitales grandes) se subdividen automáticamente por **rango de presupuesto de licitación**, partiendo rangos por la mitad recursivamente hasta que cada segmento queda por debajo del límite de truncamiento.
- **Otros tipos** (licitaciones, adjudicaciones, etc.): Se descargan por **mes + tipo de publicación** con filtros de fecha, que sí funcionan para estos tipos.

## Fichas de los menores (modo `fichas`, opcional)

El CSV del buscador solo trae el «Importe de adjudicación» **con IVA**. La ficha de cada menor (`/contrato/<Referencia>`, la que enlaza el buscador) trae además el **importe sin IVA**, el **código DIR3** de la entidad adjudicadora (no en todas) y la **duración**. `python descarga_contratacion_comunidad_madrid_v1.py fichas` las pide para los menores de la tabla consolidada:

- solo los presentes en la última descarga, con Referencia y con una Referencia que no comparte otra entidad (la URL de la ficha es la Referencia sola);
- filtros: `fichas 2019 2025` (años de «Fecha del contrato») y `--franjas`, que pide solo los de importe con IVA entre el umbral menos 500 € y el umbral con el 21 % (14.500-18.150, 39.000-48.400 y 49.500-60.500 €): los únicos en los que el importe sin IVA decide si el menor llega al umbral o cae justo por debajo;
- ritmo: `--pausa` segundos entre peticiones (1 por defecto) y `--hilos` (1); `--max-fichas N` para una pasada parcial. Para a la primera respuesta 403 o 429;
- caché `fichas_menores.sqlite3`: la ficha de cada Referencia con su HTML comprimido; una ficha buena nunca se sustituye por un error ni por una página sin importes y, si el portal la cambia, la anterior queda en `fichas_historico`. Una nueva pasada solo pide lo que falta (las que fallan, hasta 4 intentos; `--reintentar` para más);
- salida `fichas_menores.parquet` (con `guardar_version`): `Referencia`, `Entidad Adjudicadora` (las de la tabla), `estado`, los importes como se publican (`importe_sin_iva_texto`, `importe_con_iva_texto`) y en número, `codigo_entidad`, `duracion`, `num_ofertas_ficha`, `fecha_descarga` y, si la ficha trae varias adjudicaciones, `importes_json` con todas.

Ninguna otra fase la lanza: `todo` no la incluye.

## Entidades incluidas (125)

Todas las consejerías, organismos autónomos, empresas públicas y fundaciones de la CAM, incluyendo:

- 10 Consejerías (Sanidad, Educación, Digitalización, Economía, etc.)
- 30+ Hospitales del SERMAS (Gregorio Marañón, La Paz, 12 de Octubre, Ramón y Cajal, etc.)
- Canal de Isabel II y filiales
- Fundaciones IMDEA (7)
- Fundaciones de investigación biomédica (12)
- Consorcios urbanísticos, agencias y entes públicos

## Uso

- **Filas de continuación.** Una fila sin tipo ni referencia es un lote, adjudicatario, prórroga o modificación del contrato de la fila anterior (ver el aviso): no la quites.
- **Contar menores.** Hay que quitar las versiones anteriores (misma Referencia y entidad en la última descarga) y el mismo menor publicado con otra Referencia, y leer importes y fechas que vienen como texto («1.234,56», «19 de mayo del 2023»): es la consulta `madrid_menores` de [`scripts/medir_consultas.py`](../../scripts/medir_consultas.py), explicada en [`docs/MEDIR.md`](../MEDIR.md#qué-cuenta-cada-consulta).

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py` | contratos-publicos.comunidad.madrid | Web scraping con antibot bypass + subdivisión recursiva por importe |

---

[Todas las fuentes](README.md) · [README](../../README.md)
