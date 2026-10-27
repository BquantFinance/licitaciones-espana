# BOE: sección III «Otras disposiciones» (convenios)

- **En la release:** todavía no (el script es nuevo).
- **Script:** [`boe/boe_otras_disposiciones.py`](../../boe/boe_otras_disposiciones.py). Su docstring es la referencia completa.
- **Portal:** [API de datos abiertos del BOE](https://www.boe.es/datosabiertos/api/api.php) (sumario de cada día).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#nacional-y-unión-europea) (licencia tipo de la Agencia Estatal BOE).

## Qué trae

Una fila por disposición de la sección III del BOE, día a día: identificador (`BOE-A-…`), departamento, epígrafe, título y enlaces al PDF, al HTML y al XML. Ahí se publican los **convenios** de la Administración General del Estado (desde la Ley 40/2015, art. 48.8, un convenio solo es eficaz cuando se inscribe en el REOICO y se publica en el BOE), sus adendas y prórrogas, los protocolos generales de actuación, los encargos, los convenios colectivos que registra la Dirección General de Trabajo, ayudas, premios y otras resoluciones.

| Fichero | Contenido | Filas |
|---|---|--:|
| `disposiciones.parquet` | Una fila por disposición de la sección III, con su día, su departamento y su título | 29.477 de 2024-01-01 a 2026-10-24 (2024: 10.036; 2025: 10.660; 2026: 8.781); mediana de 31 al día |

De esas 29.477: 9.848 con `_tipo` = `convenio`, 1.831 `acuerdo`, 1.679 `adenda`, 850 `convenio_colectivo`, 22 `encomienda` y 15 `protocolo`; el resto son ayudas, premios, nacionalidades, sanciones que se publican aquí (la CNMV publica en esta sección las de infracciones muy graves) y otras resoluciones. El título dice con quién: «Resolución de 26 de septiembre de 2025, de la Secretaría General Técnica, por la que se publica el Convenio entre la Embajada de España en Francia y Hotel Royal Alma, SAS, para la celebración de la Fiesta Nacional de España». Las partes no se separan aquí: van en el título tal cual.

## Avisos

- **`_tipo` es una marca para filtrar**, por la primera palabra clave del título: `convenio`, `adenda`, `protocolo`, `encargo`, `encomienda`, `acuerdo` o `convenio_colectivo` (laboral, entre una empresa o un sector y los sindicatos: no es un convenio de la Administración). Vacía en lo demás (ayudas, premios, normas técnicas…).
- **Nombres de personas físicas sustituidos** en el título por «[persona física]» (tratamiento «don», «doña», «D.», «Dña.» seguido de nombre y apellidos: premios, honores, convenios con particulares); `_personas_quitadas` dice cuántos. Las personas jurídicas no se tocan. El sumario completo no se guarda: solo su sha256 en el manifiesto.
- **Solo la sección III**: los nombramientos (II), la contratación (V-A, que ya trae la PLACSP) y los anuncios no entran.
- **Una disposición, un día**: si el BOE corrige el sumario de un día, la versión anterior del día queda en `raw/<año>/_historico/` y la fila retirada sigue en el Parquet con `_en_ultima_descarga=False`.

## Columnas añadidas

- `_tipo` y `_personas_quitadas` (ver arriba).
- De origen: `_archivo_origen` y `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `boe/boe_otras_disposiciones.py` | API de datos abiertos del BOE (sumarios) | Sección III del BOE día a día (convenios, adendas, protocolos, encargos…): el extracto de cada día, con versiones y sin nombres de personas físicas, y un Parquet acumulado |

---

[Todas las fuentes](README.md) · [README](../../README.md)
