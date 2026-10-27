# Cataluña

- **En la release:** `catalunya.zip` (contratación: 14 tablas, 8.048.140 filas; y, en `v2026.02/`, las demás categorías y el perfil del contratante de Barcelona de v2026.02) y `catalunya_menores.zip` (contratos menores de la PSCP, 1.429.086 filas; y, en `v2026.02/`, el fichero de v2026.02).
- **Script:** [`scripts/ccaa_cataluna.py`](../../scripts/ccaa_cataluna.py) (descarga) y [`scripts/ccaa_cataluna_parquet.py`](../../scripts/ccaa_cataluna_parquet.py) (Parquet); los menores de la PSCP, [`scripts/ccaa_cataluna_contratosmenores.py`](../../scripts/ccaa_cataluna_contratosmenores.py).
- **Portal:** [analisi.transparenciacatalunya.cat](https://analisi.transparenciacatalunya.cat/) (Socrata), Open Data BCN y [contractaciopublica.cat](https://contractaciopublica.cat).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas).

## v2026.10: `catalunya.zip`

| Fichero (en `contratacion/`) | Filas |
|---|--:|
| `contratos_registro.parquet` (RPC, `hb6v-jcbf`) | 4.456.505 |
| `publicaciones_pscp.parquet` (PSCP, `ybgg-dgi6`) | 2.065.860 |
| `contratos_menores_generalitat.parquet` (`qjue-2pk9`) | 866.557 |
| `contratos_menores_bcn.parquet` | 177.679 |
| `contratistas_bcn.parquet` | 154.471 |
| `resumen_trimestral_bcn.parquet` | 82.597 |
| `fase_ejecucion.parquet` | 77.366 |
| `adjudicaciones_generalitat.parquet` | 73.895 |
| `contratacion_programada.parquet` | 37.641 |
| `perfil_contratante_bcn.parquet` | 37.365 |
| `contratos_menores_autorizacion_bcn.parquet` | 8.246 |
| `contratos_covid.parquet` | 5.567 |
| `modificaciones_bcn.parquet` | 3.171 |
| `resoluciones_tribunal.parquet` | 1.220 |

En `v2026.02/` van, tal cual, las categorías que no son contratación (subvenciones, presupuestos, convenios, RRHH, entidades y territorio), que no se han vuelto a descargar, y `perfil_contratante_bcn` de v2026.02, con 6.120 publicaciones que el portal ya no sirve. En `v2026.02/` de `catalunya_menores.zip`, el `contractacio_menors.parquet` de v2026.02, con 66.132 publicaciones (`id`, `expedientId`) que la descarga actual no trae.

Datos del portal [Transparència Catalunya](https://analisi.transparenciacatalunya.cat) (Socrata API).

En v2026.10 solo se ha actualizado la contratación: `catalunya.zip` (14 tablas, 8,05M filas) y `catalunya_menores.zip` (contratos menores de la PSCP, 1,43M filas). La tabla y el árbol de abajo son los de v2026.02 (los que estuvieron en Git LFS en el repositorio, ahora en la etiqueta `datos-lfs-v2026.02`); las demás categorías van tal cual, como en v2026.02, en la carpeta `v2026.02/` de `catalunya.zip`.

| Categoría | Registros | Período |
|-----------|-----------|---------|
| Subvenciones RAISC | 9.6M | 2014-2025 |
| **Contratación pública** | **4.3M** | 2014-2025 |
| ↳ Contratos regulares | 1.3M | 2014-2025 |
| ↳ Contratos menores | 3.0M filas (868K distintas) | 2014-2025 |
| Presupuestos | 3.1M | 2014-2025 |
| Convenios | 62K | 2014-2025 |
| RRHH | 3.4M | 2014-2025 |
| Patrimonio | 112K | 2020-2025 |

## Archivos

```
catalunya/
├── contratacion/
│   ├── contractacio_menors.parquet          # 3.0M filas, 868K distintas (ver abajo)
│   ├── publicaciones_pscp.parquet           # Publicaciones PSCP (ciclo completo)
│   ├── adjudicaciones_generalitat.parquet
│   ├── contratos_registro.parquet, fase_ejecucion.parquet, contratacion_programada.parquet
│   ├── contratos_covid.parquet, resoluciones_tribunal.parquet
│   └── *_bcn.parquet                        # Ayuntamiento de Barcelona (menores, contratistas, perfil, modificaciones, resumen)
├── subvenciones/
│   ├── raisc_concesiones.parquet            # 9.6M registros
│   ├── raisc_convocatorias.parquet
│   └── convocatorias_subvenciones.parquet
├── presupuestos/                            # ejecución de gastos/ingresos, presupuestos aprobados
├── convenios/convenios.parquet
├── rrhh/                                    # altos cargos, retribuciones, convocatorias
├── entidades/                               # ayuntamientos, entes locales, sector público
└── territorio/                              # municipios
```

## Contratos menores de la PSCP

Contratos menores del sector público catalán publicados en la Plataforma de Serveis de Contractació Pública: `catalunya_menores.zip`, con 1.429.086 filas en v2026.10 (`contractacio_menors.parquet`, 46 columnas: las 43 de abajo y las 3 de control). En v2026.02 eran 3.024.000 filas, la mayoría copias (ver el aviso).

> ⚠️ En el parquet de v2026.02 solo 868.063 filas eran distintas (v2026.10: 1.429.086 filas, todas distintas; el fichero de v2026.02 va también, tal cual, en `v2026.02/` de `catalunya_menores.zip`, porque trae 66.132 publicaciones que la descarga actual no trae): las otras 2.155.739 son copias idénticas de la misma publicación devuelta por varias consultas de fase (artefacto de la descarga, hasta 7 copias). Además la descarga se quedaba corta frente al dataset PSCP de Socrata (`ybgg-dgi6`): p.ej. 301.614 contratos menores agregados de 2025 frente a 529.780, ICS o UPF muy por debajo, por el tope de 20.000 resultados por consulta. El script ahora solo quita filas idénticas.
- **Límite de la API:** cada consulta sirve como mucho 10.000 resultados, `totalElements` tampoco pasa de 10.000 y las páginas son de 2.000 como mucho. Desde el 26-oct-2026 cada fase se lee **por tramos de la fecha de la última publicación** (el filtro «Interval» de la búsqueda avanzada), partidos hasta que cada uno tiene menos de 10.000, con su recuento exacto como control; un día que sigue en el tope se parte por ámbito, tipo, procedimiento y órgano y, en el último nivel, por los dos órdenes (un segmento incompleto se detecta cuando no se solapan). `contractacio_menors_duplicate_analysis.json` trae `anunciados_por_fase`: lo que anuncia la API sumando los tramos.
- **Sin sesgo del superviviente:**
  - Las fases, el crudo y la salida limpia se guardan con `guardar_version`.
  - La salida limpia se acumula con `acumular`: lo retirado queda con `_en_ultima_descarga=False`, solo dentro del grupo (normales o agregadas) que se ha leído entero.
  - `--semilla` añade, por (`id`, `expedientId`), lo que ya no se sirve.
- **Hasta la descarga del 20-oct-2026 (la de v2026.10) la descarga se quedaba corta:** las fases 900 y 1000 (sin procedimiento) y los órganos de más de 20.000 (ICS, universidades, fundaciones de investigación…) se leían en dos ventanas, y lo que tenía un ámbito, tipo o procedimiento fuera de las listas fijas se perdía sin aviso; ningún grupo entraba en el ámbito. En un ensayo del 26-oct con los tramos de fecha, faltaban en la salida 453.944 líneas (`id`, `expedientId`) de 1.481.458 (2024: 162.053; 2025: 183.917; 2026: 77.241; 2023: 17.019; antes, 13.714); el 98 % tienen su expediente en `publicaciones_pscp` (Socrata). En los parquet publicados se perdían los ceros a la izquierda de códigos postales e INE (`08002` → 8002) y el dataset `contractes-menors-a-generica` de Barcelona no se descargaba (slug erróneo); ambos corregidos.

- **43 columnas**: `id`, `titol`, `descripcio`, `pressupostLicitacio`, `pressupostAdjudicacio`, `organ`, `idOrgan`, `codiExpedient`, `expedientId`, `esPlacsp`, `esAgregatContractes`, `esAgregatEncarrecs`, `nomPublicacioAgregada` y `fasesVigents_<FASE>_{lotsActius,dataPublicacio,idPublicacio}` para 10 fases (no incluye nombre ni NIF del adjudicatario)
- Una fila por publicación y contrato, con las fases vigentes de cada una en columnas (una publicación agregada contiene varios contratos, que se distinguen por `expedientId`)
- Extraído de la API del portal de contratación pública (`contractaciopublica.cat`, `portal-api`) por tramos de fecha (unas 7.500 peticiones en el ensayo del 26-oct-2026; antes, con sub-segmentación por listas, unas 90.000)
- Fuente: [Plataforma de Serveis de Contractació Pública](https://contractaciopublica.cat)

**Menores con NIF del adjudicatario.** `publicaciones_pscp.parquet` (Socrata `ybgg-dgi6`) trae 1.048.149 filas con `procediment = 'Contracte menor'` y NIF en el 99,7-100 %: ~350.000 al año en 2023-2024 (locales, Generalitat y universidades), estructurados desde el 2.º semestre de 2022. El RPC (`contratos_registro.parquet`) trae también los de menos de 5.000 €, pero solo con el nombre del adjudicatario.

**Datasets retirados en origen.** `a23c-d6vp` (licitaciones y adjudicaciones en curso) y `jxvs-kzbu` (adjudicaciones quincenales) dan HTTP 404 desde la primera descarga (28-sep-2026) y no están en el catálogo del portal: el script los sigue pidiendo y los anota como «Retirado en origen» en el log y en el resumen, sin contarlos como fallo (`SOCRATA_RETIRADOS`). Lo que traían está en `ybgg-dgi6` (publicaciones de la PSCP) y en `nn7v-4yxe` (adjudicaciones de la Generalitat).

**Ventanas móviles.** El RPC (`hb6v-jcbf`) y los menores de la Generalitat (`qjue-2pk9`, 2020-2024, importes en céntimos tal como se publican; antes se pedía `ydq4-xy5b`, que da 404) solo sirven los últimos 5 años. `ccaa_cataluna.py` guarda la versión anterior de cada CSV en `_historico/` y `ccaa_cataluna_parquet.py` construye cada parquet con todas las versiones: lo que sale de la ventana sigue con `_en_ultima_descarga=False`.

**Memoria.** `ccaa_cataluna_parquet.py` no carga los CSV enteros: lee cada versión por trozos y la guarda en una carpeta temporal junto al parquet (`.<parquet>.trozos`, que se borra al acabar), decide la acumulación con las huellas de las filas, infiere los tipos columna a columna y escribe el parquet por grupos de filas, en un fichero aparte que solo sustituye al anterior al terminar. Con las tres versiones de la PSCP (2,5 GB cada una) el pico baja de 15,2 a 3,6 GiB, con la misma salida fila a fila (`tests/test_ccaa_cataluna_trozos.py`).

**Semilla del release (`--semilla`).** Lo que salió de la ventana antes de la primera descarga solo está en el release v2026.02. `ccaa_cataluna_parquet.py --semilla <carpeta catalunya del release>` añade las filas del publicado cuya clave ya no está en la descarga, con `_origen='release v2026.02'` y `_en_ultima_descarga=False` (claves en `SEMILLAS`).
- Medido el 28-sep-2026 frente a la primera descarga de producción:
  - RPC: 751.187 filas, sobre todo menores y liquidaciones de 2021.
  - PSCP: 85.397, de procedimientos que ya no se publican. La clave es el uuid del procedimiento en la URL: la URL entera cambia con cada fase y entre `/ca/` y `/es/`.
  - Fase de ejecución: 9.047.
  - Contratación programada: 5.095.
- Adjudicaciones de la Generalitat, COVID y el Tribunal coinciden con el publicado.

**Solo algunas categorías (`--categorias`).** `ccaa_cataluna.py` y `ccaa_cataluna_parquet.py` aceptan `--categorias contratacion,subvenciones,...` (por defecto, todas). Open Data Barcelona va con `contratacion`. La descarga de producción solo usa `contratacion`: las concesiones RAISC pesan 19 GB por descarga.

## Uso

- **Menores con NIF:** `publicaciones_pscp.parquet` con `procediment = 'Contracte menor'` (ver arriba). Para un ranking de adjudicatarios hay que quitar las fases anteriores de una publicación que sigue en la descarga, las anulaciones, las filas con varios adjudicatarios (`||`) y los NIF enmascarados: es la consulta `pscp_top_menores` de [`scripts/medir_consultas.py`](../../scripts/medir_consultas.py), explicada en [`docs/MEDIR.md`](../MEDIR.md#qué-cuenta-cada-consulta).
- **Ventanas móviles:** en las tablas acumuladas, `_en_ultima_descarga=False` es lo que el portal ya no sirve (salió de la ventana de 5 años o se retiró), no un error.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_cataluna.py` | Socrata + CKAN Barcelona | Descarga datos Catalunya |
| `scripts/ccaa_cataluna_parquet.py` | — | Convierte los CSV de Catalunya a Parquet |
| `scripts/ccaa_cataluna_contratosmenores.py` | contractaciopublica.cat | Descarga contratos menores Catalunya (todas las fases, API del portal) |

---

[Todas las fuentes](README.md) · [README](../../README.md)
