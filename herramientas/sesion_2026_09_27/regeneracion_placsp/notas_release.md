> ⚠️ **Borrador.** No sustituye a `v2026.02`, que se conserva tal cual. Se regeneró en la nube de Claude Code el 2026-09-27 a petición del propietario, para el issue #6.

## Qué es

Regeneración completa de la PLACSP (conjunto nacional) y de los 20 indicadores de calidad con el código corregido.
- **Código:** rama `claude/continue-previous-process-wsi03z`, commit `{COMMIT}`, que incluye todas las correcciones de la PR #23.
- **Datos:** todos los ZIP de la PLACSP descargados el 2026-09-27 (5 conjuntos, 2012-2026).
- **Semilla:** los dos parquet de `v2026.02` se incorporan como la instantánea más antigua (`--semilla`), para no perder lo que la PLACSP ya no publica.

## Correcciones respecto a v2026.02 (issue #6 y PR #23)

| Columna | v2026.02 | Ahora |
|---|---|---|
| `importe_sin_iva` | Valor estimado (`EstimatedOverallContractAmount`) | Presupuesto base sin IVA (`TaxExclusiveAmount`) |
| `valor_estimado_contrato` | — | `EstimatedOverallContractAmount` |
| `fecha_publicacion` / `ano` | Primer anuncio (a menudo el de adjudicación) | Anuncio de licitación (`DOC_CN`) |
| Etiquetas de procedimiento y tipo | Desplazadas (menores como "Asociación innovación"…) | Recalculadas desde los códigos |
| CPV | Número (sin el cero inicial) | Texto de 8 dígitos |
| Versiones | Sin marcar (8,7M entradas de 4,7M licitaciones) | `n_versiones`, `es_ultima_version` y `entrada_repetida` |
| Consultas preliminares | 3.681 filas sin `fecha_updated` | Todas las entradas CPM, con fecha |

**Para contar o sumar licitaciones:** filtrar `es_ultima_version`. Cada actualización de una licitación es una entrada más.

## Ficheros

{FICHEROS}

**Procedencia de cada fila:**
- `_origen` nulo: la fila se ha leído de los ZIP de hoy.
- `_origen = 'release v2026.02'`: fila que la PLACSP ya no publica, recuperada de v2026.02.
- `_en_ultima_descarga`: si la entrada sigue en la copia actual de algún ZIP.

`textos_originales` guarda el texto publicado de los importes y fechas que no se pueden convertir (p.ej. `"0202-07-03"`).

## Cifras

{CIFRAS}

## Cómo se generó

```bash
python nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-2026 \
    --data-dir <zips> --output-dir <salida> --procesos 3 --sin-csv \
    --semilla licitaciones_espana.parquet --semilla licitaciones_completo_2012_2026.parquet
python calidad/calidad_licitaciones.py -i <principal reducida> --ted crossval_sara.parquet \
    --borme borme/data/borme_empresas_pub.parquet
```

{LIMITACIONES}
