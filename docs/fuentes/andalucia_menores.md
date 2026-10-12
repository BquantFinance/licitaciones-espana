# Andalucía: contratos menores de la Junta (CKAN)

- **En la release:** `andalucia_menores.zip`: `contratos_menores.parquet` (768.647 filas, 40 columnas, 2018-2026).
- **Script:** [`scripts/ccaa_andalucia_menores.py`](../../scripts/ccaa_andalucia_menores.py). Su docstring es la referencia completa.
- **Portal:** el CKAN de datos abiertos de la Junta de Andalucía, serie «Contratación Menor en {año} publicada en la Plataforma de Contratación de la Junta de Andalucía».
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## Qué trae

Los contratos menores (art. 118 LCSP) que se publican en la Plataforma de Contratación de la Junta: consejerías, delegaciones, agencias, empresas y fundaciones de la Junta y el **Servicio Andaluz de Salud (SAS)**, uno por fila, con órgano, nº de expediente, título, descripción, tipo, duración, NUTS, valor estimado, importes sin y con IVA, financiación europea, licitadores, NIF y nombre del adjudicatario, fechas de adjudicación y formalización y estado.

Es la misma plataforma que lee [`scripts/ccaa_andalucia.py`](andalucia.md) por su buscador, pero sin su tope de 10.000 resultados por consulta: aquí el SAS está entero (544.898 de los 768.647 registros; 98.976 de las 124.824 filas de 2025). Cubre los ~41K menores del SAS que el buscador no alcanza: las dos fuentes se complementan ([`docs/COBERTURA.md`](../COBERTURA.md) §5.2). No trae universidades, diputaciones ni ayuntamientos: no publican en esa plataforma. NIF válido en el 98,4 %.

## Cómo se descarga

- El CSV de cada año tal como se publica (2018-2023 van en ZIP), con todas sus versiones en `raw/<año>/_historico/`, y la ficha de CKAN de cada conjunto.
- `contratos_menores.parquet`: todos los años, con las columnas publicadas como texto, acumulado con `comun/historico.py` (lo que la Junta retira o cambia sigue con `_en_ultima_descarga=False`).
- Se vuelven a pedir el año en curso y el anterior; los cerrados, con `--comprobar-todo`.

## Columnas

Las publicadas, con su nombre y su valor tal cual, sin recortar. Añadidas por el script:

- `_valor_estimado_num`, `_importe_adjudicacion_sin_iva_num` e `_importe_adjudicacion_con_iva_num`: el importe publicado leído como número (nulo si el texto no es un número con un solo separador decimal).
- `_fuente`, `_conjunto`, `_recurso`, `_anio`, `_fichero_publicado`, `_miembro`, `_lineas_unidas` (en un registro partido por un salto de línea sin comillas, que se vuelve a unir, cuántas líneas ocupaba), `_archivo_origen`, `_fecha_descarga` y las 3 de control.

## Avisos

- **Formatos de cada año, tal cual.** Desde 2023 cada campo va relleno con espacios hasta un ancho fijo; importes con punto decimal salvo en 2025 (coma); fechas ISO con zona hasta 2024 y dd/mm/aaaa en 2025-2026; el NIF, a veces con `;` al final, con guion o enmascarado en personas físicas. Para importes, usa las columnas `_…_num`.
- **La cabecera de 2022** trae ` FECHA_ADJUDICACION` y ` FECHA_FORMALIZACION` con un espacio delante: son columnas aparte, nulas en los demás años.
- **`ID_EXPEDIENTE` no es único:** en 2021 y 2022 el SAS y la Junta comparten números, y en 2025 hay 35 contratos del IFAPA publicados dos veces con otro órgano. Se conservan todas las filas.
- **Sin semilla:** esta fuente no estaba en la release v2026.02.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_andalucia_menores.py` | CKAN de datos abiertos de la Junta de Andalucía | Contratos menores «Contratación Menor en {año}» (2018-), con el SAS entero; CSV tal cual con versiones y Parquet acumulado |

---

[Todas las fuentes](README.md) · [README](../../README.md)
