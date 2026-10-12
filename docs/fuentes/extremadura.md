# Extremadura: Registro de Contratos de la Junta

- **En la release:** `extremadura.zip`: 3 Parquet, 221.749 filas (1T 2022-2T 2026).
- **Script:** [`scripts/ccaa_extremadura.py`](../../scripts/ccaa_extremadura.py). Su docstring es la referencia completa.
- **Portal:** las páginas trimestrales «Contratos e incidencias inscritas en el Registro de Contratos» de [juntaex.es](https://www.juntaex.es/).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (aviso legal contradictorio).

## Qué trae

| Fichero | Contenido | Filas |
|---|---|--:|
| `registro_contratos_menores.parquet` | Contratos menores inscritos (casi todos del SES), con NIF en el 99,997 % | 207.702 |
| `registro_contratos_mayores.parquet` | Contratos no menores inscritos | 10.539 |
| `registro_contratos_incidencias.parquet` | Modificaciones, ampliaciones de plazo, prórrogas y resoluciones (la columna `_tipo` dice de qué documento sale cada fila) | 3.508 |

Los resúmenes estadísticos anuales (PDF) se guardan, pero no son tablas: no entran en ningún Parquet.

## Cómo se descarga

- Las páginas del registro salen del buscador del portal, de una lista de páginas conocidas (con los nombres irregulares) y de los nombres esperados de cada trimestre desde el 1T 2022.
- Se bajan todos los documentos que enlazan, identificados por su uuid del gestor documental. Se vuelven a pedir los del año en curso y el anterior, y los que el portal enlaza con otra versión; los demás, con `--comprobar-todo`.
- Un documento que ninguna página enlaza ya queda como retirado (sus filas se conservan con `_en_ultima_descarga=False`). Si el buscador o una página fallan, no se retira nada y la ejecución termina con código 1.

## Columnas añadidas

`_fuente`, `_pagina`, `_dataset`, `_tipo`, `_nombre_publicado`, `_anio` y `_trimestre` del listado (`3T`; `2T-3T` si cubre dos), `_archivo_origen`, `_hoja`, `_fecha_descarga`, las 3 de control y, en menores y mayores, `_repetido_de`: el listado anterior, aún publicado, que ya traía el mismo número de registro.

## Avisos

- **Contratos repetidos entre listados.** Un mismo número de registro puede salir en dos listados trimestrales. Los contratos vigentes sin repetir son `_en_ultima_descarga & _repetido_de` nulo.
- **2016-2021** no está: los XLS de la Intervención General dan 404 ([`docs/COBERTURA.md`](../COBERTURA.md) §0.1).
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_extremadura.py` | Junta de Extremadura | Registro de Contratos: menores, mayores e incidencias |

---

[Todas las fuentes](README.md) · [README](../../README.md)
