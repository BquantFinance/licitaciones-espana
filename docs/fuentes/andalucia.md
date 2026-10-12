# Andalucía: buscador de contratación de la Junta

- **En la release:** `andalucia.zip`: `licitaciones_andalucia.parquet` (900.931 filas, 42 columnas).
- **Script:** [`scripts/ccaa_andalucia.py`](../../scripts/ccaa_andalucia.py).
- **Portal:** [buscador de perfiles y licitaciones de la Junta](https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/perfiles-licitaciones/buscador-general).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

Los contratos menores de la Junta con el SAS entero, sin el tope de 10.000 resultados del buscador, son otra fuente: [Andalucía, menores](andalucia_menores.md). Las dos se complementan.

Contratación pública de la [Junta de Andalucía](https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico/perfiles-licitaciones/buscador-general), incluyendo licitaciones regulares y contratos menores de todos los organismos y empresas públicas andaluzas. Extraído mediante ingeniería inversa del proxy Elasticsearch del portal, con estrategia de subdivisión recursiva en 8 dimensiones para superar el límite de 10K resultados por consulta.

Conteos observados en torno al 2026-03-23 consultando la API pública del portal:

| Tipo | Registros | Cobertura |
|------|-----------|-----------|
| Licitaciones regulares (estándar, sin BRR) | ~80.9K | Operativa |
| Contratos menores (sin BRR) | ~775.7K | Operativa |
| **Total (sin BRR)** | **~856.7K** | **Operativa** |

> v2026.10: 900.931 filas (descarga del 2026-09-29) y, aparte, los menores del CKAN de la Junta con el SAS entero en `andalucia_menores.zip` (768.647).
>
> ⚠️ El fichero de v2026.02 tiene 808.441 filas (hasta 2026-02-11): faltan unos 41K contratos menores (los del SAS están en el CKAN de la Junta: `scripts/ccaa_andalucia_menores.py`, 41.022 en 2019-2025). 15 segmentos del SAS superan el límite de 10.000 resultados tras las 8 dimensiones de subdivisión (273K registros); la solución prevista es partir por mes de publicación (pendiente de verificar en vivo). El script ahora incluye los códigos de estado/tipo/provincia presentes en los datos, años calculados en ejecución, descubrimiento completo de perfiles y 4 columnas JSON con todas las adjudicaciones, lotes, anuncios y campos no mapeados (19.765 expedientes con varias adjudicaciones perdían las siguientes).

## Archivos

```
ccaa_Andalucia/
├── licitaciones_andalucia.parquet          # v2026.10: 900.931 registros (115 MB, snappy)
├── _historico/                             # versiones anteriores del Parquet
└── raw/                                    # cada descarga tal cual (jsonl.gz); anteriores en raw/_historico/

scripts/
└── ccaa_andalucia.py                       # Scraper ES proxy 8D + tramos de idExpediente + salida CSV/Parquet
```

**Sin sesgo del superviviente.**
- Cada descarga se guarda tal cual en `raw/` (`guardar_version`).
- El Parquet se construye con `acumular` sobre la salida anterior. Lo que el portal retira o cambia se conserva con `_en_ultima_descarga=False`. Solo se retira lo que seguro cae en una consulta completa: con el tope de 10.000 o una paginación cortada no se retira nada.
- **Registro = (`portalGestor`, `idExpediente`).** El índice junta dos numeraciones que comparten ids (4.402-13.890 y 400.000-425.471, de 2021-2022); `portalGestor` va en `campos_extra_json`. Deduplicar por el id solo perdió 18.453 menores y 34 licitaciones en la primera descarga de producción (29-sep-2026) y dio por retiradas 5.030 licitaciones.
- `--semilla <parquet publicado>` añade las filas del publicado que no están en la salida y que una descarga de su alcance ha dejado de traer. Primero se mira si están y después el ámbito. Fuera de los ids compartidos basta el `id_expediente`; en ellos, el mismo id y nº de expediente, la misma fila, o el mismo id, perfil, título e importe (el nº cambia a veces). Una descarga del código anterior no cuenta como releída en los ids compartidos.
- `procesar` regenera las salidas sin red. `--perfil` y `--anio` descargan una parte, y solo esa parte puede marcar retiradas.
- Si la salida por defecto es aún un puntero Git LFS sin descargar, el script se niega a sobrescribirla. En ese caso usa `--salida` o baja antes el fichero.
- Los importes van siempre como `float64`.

## Campos principales (38 columnas, más las 3 de control)

| Categoría | Campos |
|-----------|--------|
| Identificación | id_expediente, numero_expediente, titulo, url_detalle |
| Clasificación | tipo_contrato(_codigo), estado(_codigo), codigo_procedimiento (9 = contrato menor), codigo_tramitacion, codigo_normativa |
| Órgano | organo_contratacion, codigo_perfil, codigo_dir3, provincias_ejecucion |
| Importes | importe_licitacion, valor_estimado, importe_adjudicacion (sin IVA), importe_adjudicacion_iva (primera adjudicación) |
| Adjudicación | adjudicatario_nif, todos_adjudicatarios_nif, num_adjudicaciones |
| Fechas | fecha_publicacion, fecha_limite_presentacion, anuncio_primera_fecha, anuncio_ultima_fecha |
| Otros | forma_presentacion, cofinanciado_ue, subasta_electronica, sistema_racionalizacion, cpv, medios_publicacion, num_lotes, num_anuncios |
| Detalle completo (JSON) | adjudicaciones_json, lotes_json, anuncios_json, campos_extra_json (con `portalGestor`) |

> **Columnas planas de la adjudicación.** `adjudicatario_nif`, `importe_adjudicacion` e `importe_adjudicacion_iva` son la **primera adjudicación de primer nivel tal como la sirve el portal**, sea cual sea su resultado (`codigoResultado` AWARD, NOAWA —desierta—, RESIGN, MISES; una no adjudicada suele traer 0 o el presupuesto), y no miran los lotes: en un expediente con lotes están vacías (sus adjudicaciones van en `lotes_json[].adjudicacion`). Para sumar lo adjudicado hay que leer `adjudicaciones_json` y `lotes_json` (resultado AWARD, sin la copia de formalización). `fecha_publicacion` es a menudo una publicación posterior: la primera es `anuncio_primera_fecha`. `url_detalle` lleva solo el `idExpediente`, que en los ids compartidos no dice de qué numeración es.

## Estrategia de descarga

El portal de la Junta de Andalucía usa un proxy frontend que limita a 10.000 resultados por consulta Elasticsearch. Con 850K registros totales, se requirió una estrategia de subdivisión recursiva en **8 dimensiones** y, después, tramos de `idExpediente` para cobertura completa:

1. **codigoProcedimiento**: Estándar vs Menores
2. **tipoContrato.codigo**: 21 tipos (SERV, SUM, OBRA, PRIV...)
3. **estado.codigo**: 14 estados (RES, ADJ, PUB, EVA...)
4. **codigoTipoTramitacion**: 5 valores + null (295K registros sin tramitación)
5. **perfilContratante.codigo**: ~400 organismos
6. **provinciasEjecucion**: 8 provincias + null
7. **formaPresentacion**: 6 valores + null
8. **numeroExpediente (año)**: match por texto "2018"-"2026" + null

Para los chunks que aún superan 10K tras las 8 dimensiones (los menores de suministros del SAS sin tramitación, forma de presentación ni año en el nº), se parte la consulta en **tramos de `idExpediente`** (`range`), contando cada mitad hasta que cabe en la ventana: medido el 2026-09-29, las tres consultas del SAS con tope (39.797, 33.259 y 38.550 documentos) quedan en 6, 4 y 6 tramos de 9.874 como mucho, que suman el total. Antes se usaba un **multi-sort con 14 órdenes**, que dejaba 4.901 menores sin descargar; sigue como reserva si el proxy rechaza `range`. Las páginas se piden ordenadas por (`idExpediente`, `portalGestor`) y una consulta a la que le faltan documentos se repite (en orden inverso y otra vez en el mismo) antes de darla por incompleta.

## Perfiles incluidos (~400)

Todas las consejerías, agencias, hospitales del SAS, universidades, diputaciones provinciales, empresas públicas y fundaciones de la Junta de Andalucía, incluyendo:

- Servicio Andaluz de Salud — SYBS03 (290K contratos, mayor organismo)
- 8 Diputaciones provinciales
- 10 Universidades públicas
- Consejerías (Salud, Educación, Fomento, Economía, etc.)
- Agencias (IDEA, AEPSA, ADE, etc.)

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_andalucia.py` | Junta de Andalucía | Scraper ES proxy con subdivisión 8D y tramos de `idExpediente` (multi-sort con 14 órdenes como reserva) + salida reproducible |

---

[Todas las fuentes](README.md) · [README](../../README.md)
