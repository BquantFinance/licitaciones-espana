# La Rioja: contratos menores del Gobierno

- **En la release:** `la_rioja.zip`: `contratos_menores.parquet` (351.203 filas, 17 columnas, 2018-2026).
- **Script:** [`scripts/ccaa_la_rioja.py`](../../scripts/ccaa_la_rioja.py). Su docstring es la referencia completa.
- **Portal:** los datos abiertos anuales «Contratos menores {año} Comunidad Autónoma de La Rioja» de [web.larioja.org/dato-abierto](https://web.larioja.org/dato-abierto) (fichas `opd-<código>`).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## Qué trae

Todos los contratos menores de la Administración general, el Servicio Riojano de Salud (SERIS) y el Instituto de Estudios Riojanos, con NIF salvo en las personas físicas, que van enmascaradas (91,9 % de NIF válidos y 8,1 % enmascarados).

Columnas del portal: `COD_CONTRATO`, `DEPARTAMENTO`, `TIPO_EXPEDIENTE`, `TERC_CIF`, `TERC_NOMBRE`, `CONCEPTO`, `FECHA`, `IMPORTE_EJERCICIO`. Añadidas: `_fuente`, `_dataset`, `_recurso` (ficha `opd-<N>`), `_anio_fichero`, `_archivo_origen`, `_fecha_descarga` y las 3 de control.

## Cómo se descarga

- Cada año es un dato abierto con su código. `CODIGOS_CONOCIDOS` es la lista verificada (2018-2026); los años nuevos se descubren sondeando el servidor de descargas, que da en cada código el nombre del fichero (`contratos_CAR_<año>.csv`).
- Antes de descargar se comprueba que el código sigue sirviendo el fichero de ese año. Se vuelven a pedir el año en curso y el anterior (se actualizan a diario); los antiguos, con `--comprobar-todo`.

## Avisos

- **Codificación.** El CSV está en ISO-8859-15, que no puede representar algunos caracteres: se pierden 3-5 valores por año (`´`, `¤`, `¨`, `¼`, `½` en `TERC_NOMBRE` o `CONCEPTO`), que el portal publica como `?`. El resto coincide fila a fila con el JSON del mismo dato.
- **Importes y fechas como texto:** `IMPORTE_EJERCICIO` con coma decimal y sin separador de miles (hay negativos); `FECHA` como `AAAA/MM/DD 00:00:00.000`.
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_la_rioja.py` | Gobierno de La Rioja | CSV anual de contratos menores |

---

[Todas las fuentes](README.md) · [README](../../README.md)
