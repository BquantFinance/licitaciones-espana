# Regeneración de la PLACSP y de la calidad para el issue #6 (2026-09-27)

Regeneración completa del conjunto nacional (PLACSP) con el código corregido de esta rama y con `--semilla` de `v2026.02`, más el cruce con TED y los 20 indicadores de calidad. La pidió el propietario para el issue #6, en un release en **borrador** que no toca `v2026.02`.

> **No se ha publicado.** Esta sesión de Claude Code en la nube no puede crear releases (403: *"Creating, editing, or deleting releases is not permitted for this session type"*) ni tiene `git lfs`. Los ficheros solo existieron en el contenedor. Para publicarlos hay que repetirlo en la máquina del propietario (§6). Las cifras de este documento sirven para verificar ese resultado.

## 1. Resumen

| | Cifra |
|---|---|
| ZIP leídos | 45, todos los que publica hoy la PLACSP (2012-2026, datos hasta el 2026-09-22) |
| Entradas | **9.710.903** de **5.218.753** licitaciones distintas; 0 descartadas y 0 errores |
| Filas añadidas por la semilla `v2026.02` | **0**: todo lo publicado en `v2026.02` sigue en los ZIP de hoy |
| Versiones comparables con `v2026.02` | 8.494.308 |
| `importe_sin_iva` cambiado | 4.314.328 (50,8 %). El publicado es el `valor_estimado_contrato` en el 100 % de los casos (0 diferencias) |
| `importe_con_iva` distinto | 0 |
| SARA en TED (última versión) | 153.110 de 257.637 (59,4 %); 11.677 de 2026 quedan sin evaluar |
| Calidad | Score medio 93,3 (mediana 94,7); con INT-FIA-12, añadido después, 93,6 (§5) |

Coincide con la regeneración independiente de la PR #24: 4.383.029 cambios de 8.612.333 comparables (50,9 %), también con 0 y 0 (§3).

## 2. Fuentes y procesado

`python nacional/licitaciones.py --solo-procesar --conjunto todos --anos 2012-2026 --procesos 3 --sin-csv --semilla licitaciones_espana.parquet --semilla licitaciones_completo_2012_2026.parquet`, con los ZIP descargados el 2026-09-27. Tardó 59 min, con un pico de 6,2 GB.

| Conjunto | ZIP | Entradas | Entradas borradas (`at:deleted-entry`) |
|---|---|---|---|
| Licitaciones (643) | 15 (2012-2026) | 4.145.655 | 86.506 |
| Agregación (1044) | 11 (2016-2026) | 1.912.642 | 351.939 |
| Menores (1143) | 9 (2018-2026) | 3.631.002 | 40.897 |
| Encargos a medios propios (1383) | 5 (2022-2026) | 17.541 | 96 |
| Consultas preliminares (1403) | 5 (2022-2026) | 4.063 | 26 |
| **Total** | **45** | **9.710.903** | **479.464** |

- **Versiones.** Cada actualización de una licitación es una entrada más. Hay 130.806 entradas que repiten una ya leída (`entrada_repetida`). Para contar o sumar licitaciones hay que filtrar `es_ultima_version`.
- **Semilla.** Se leyeron 8.693.891 filas de `licitaciones_espana.parquet` y 4.725.557 de `licitaciones_completo_2012_2026.parquet`. Todas casan: por clave (`id`, `fecha_updated`) 8.658.264 y 4.722.156; por contenido, las que no tienen fecha, 35.627 y 3.401. No se añade ninguna fila con `_origen = 'release v2026.02'`.
- **Salida.** Principal de 4,3 GB, con 55 columnas. Tablas de detalle: resultados 0,9 GB, criterios 0,7 GB, adjudicatarios 0,4 GB y lotes 0,2 GB, más `_borrados` y `_semilla_contenido`.
- Los hashes de los 45 ZIP están en el anexo.

### Sesgo del superviviente

- Lo que la PLACSP ya no publica se conserva con la semilla. Hoy no falta nada de `v2026.02`, pero la próxima regeneración debe sembrarse con este resultado.
- Las **entradas borradas** del ATOM no se descartan: van a la tabla `_borrados`, con 479.464 filas y 210.857 ids distintos.
  - Por motivo: `CERRADA` 389.204, `ANULADA` 16.883 y sin motivo 73.377.
  - En 470.057 de las 470.305 marcas no repetidas, la licitación tiene alguna versión en la tabla principal. De las otras 248 solo queda la marca.
  - La agregación de 2024 es un caso extremo: 227.716 marcas de borrado frente a 242.265 entradas.
  - Para el modelo antifraude, una licitación anulada o retirada es información, no ruido.

## 3. Contraste con `v2026.02` y con la PR #24

**Por fichero no sirve.** La PLACSP ha sustituido los ZIP mensuales de 2025 y de enero de 2026, que usó `v2026.02`, por ZIP anuales.
- De los 78 ficheros del inventario de `v2026.02`, 37 tienen hoy el mismo número de filas.
- Los 39 mensuales no existen ya: 0 filas frente a 16.543-70.554 cada uno.
- `CPM_SectorPublico_2026` y `EMP_SectorPublico_2026` han crecido: 548→930 y 317→3.163.

**Por versión** (`herramientas/.../comparar_por_version.py`). Se une por `id` + `fecha_updated` sin mirar el fichero, y solo con claves únicas y con fecha en los dos lados:

| | Esta regeneración | PR #24 (@686f6c61) |
|---|---|---|
| Inventario | 45 ZIP actuales (hasta 2026-09) | Los 78 ZIP de `v2026.02` (hasta 2026-01) |
| Filas | 9.710.903 | 8.721.484 |
| Versiones comparables | 8.494.308 | 8.612.333 |
| `importe_sin_iva` cambiado | 4.314.328 (**50,8 %**) | 4.383.029 (**50,9 %**) |
| `importe_sin_iva` publicado ≠ `valor_estimado_contrato` regenerado | **0** | **0** |
| `importe_con_iva` distinto | **0** | **0** |

- Hay 8.495.737 versiones únicas en el publicado. 1.429 no tienen pareja única porque en el regenerado esa clave aparece repetida (`entrada_repetida`). Ninguna falta.
- Los 0 de la fila central confirman el error del issue #6: en `v2026.02`, `importe_sin_iva` es siempre el valor estimado (`EstimatedOverallContractAmount`), no el presupuesto sin IVA (`TaxExclusiveAmount`).
- Dos regeneraciones independientes, con inventarios distintos, dan la misma proporción de cambios y ninguna diferencia en los otros dos importes.
- *Nota técnica:* la primera pasada dio 2.391.779 cambios por un error del script de contraste. Con dtypes nullable (pandas 3), `NaN == x` da `<NA>` y `.sum()` se saltaba las filas con el importe publicado vacío. Corregido en `30e74fb`.

## 4. Cruce con TED (INT-CONS-20)

`ted/run_ted_crossvalidation.py` con el parche de la PR #24 (`9e945c2`) y la cobertura por año (`2b184a1`), frente al `ted_es_can.parquet` del repo: 591.047 avisos CAN de 2010 a 2025. Pico de 7,2 GB.

- **Candidatos SARA** (última versión de cada licitación, sin menores, sobre umbral): **257.637**.
  - Por tipo: Servicios 152.259, Suministros 99.807, Obras 5.029 y Concesión de servicios 475.
  - La suma de lotes por expediente añade 73.
- **Casados en TED: 153.110 (59,4 %).**

  | Estrategia | Casados |
  |---|---|
  | E1: NIF adjudicatario + importe | 57.311 |
  | E3: NIF del órgano + importe | 45.025 |
  | E4: lotes agrupados | 18.461 |
  | E7: tokens + importe | 14.721 |
  | E5: nombre del órgano + importe | 11.194 |
  | E2: expediente + importe | 5.607 |
  | E3b: alias del órgano | 764 |
  | E6: propagación por expediente | 25 |
  | E2b: lotes de un expediente | 2 |

- **Sin publicar en TED: 71.004 (27,6 %)**, sin contar los negociados sin publicidad (24.461, un 9,5 %).
  - Alta confianza (órgano presente en TED, desde 2016 y ≥ 221.000 €): 32.785.
  - Por expediente: 104.270 de 257.261 expedientes SARA (40,5 %).
- **Sin evaluar: 11.677 (4,5 %)**, todos de 2026: el snapshot de TED llega a 2025.
  - Antes de `2b184a1` estos contratos contaban como no publicados. Por eso 2026 aparece con un 33,2 % casado y 0 missing.
- **En calidad** (unión por versión, `2b184a1`): se evalúan 245.960 SARA y 153.110 están en TED (62,2 %). **INT-CONS-20 = 37,8 %** (92.850, contando los negociados sin publicidad).

## 5. Calidad

`calidad/calidad_licitaciones.py` sobre la **última versión** de cada licitación: 5.218.753 contratos, de ellos 3.528.042 menores (67,6 %). Usa el BORME del repo (3.334.397 empresas) y el cruce TED del §4. Pico de 7,1 GB.

| Indicador | % con incidencia | Evaluados | Menores | Resto | Qué mide |
|---|---|---|---|---|---|
| INT-VAL-01 | 0,0 % | 5.218.753 | 0,0 % | 0,1 % | Importe de licitación en formato válido |
| INT-VAL-02 | 5,8 % | 5.218.753 | 0,3 % | 17,4 % | Importe de adjudicación en formato válido |
| INT-VAL-03 | 0,3 % | 5.218.753 | 0,1 % | 0,7 % | Importe mínimo plausible |
| INT-VAL-04 | 0,0 % | 5.218.753 | 0,0 % | 0,0 % | Número de licitadores entero |
| INT-VAL-05 | 0,0 % | 5.218.753 | 0,0 % | 0,0 % | Número de licitadores no negativo |
| INT-VAL-06 | 0,1 % | 5.218.753 | 0,0 % | 0,3 % | Fecha de publicación válida |
| INT-VAL-07 | 12,3 % | 5.218.753 | 0,7 % | 36,6 % | Fecha de adjudicación válida |
| INT-VAL-09 | 40,5 % | 5.218.753 | 59,3 % | 1,2 % | Código CPV válido |
| INT-VAL-10 | 0,2 % | 5.218.753 | 0,0 % | 0,6 % | Código territorial válido |
| INT-VAL-12 | 8,4 % | 5.218.753 | 3,3 % | 19,0 % | NIF/NIE del adjudicatario válido |
| INT-VAL-14 | 0,5 % | 5.218.753 | 0,8 % | 0,0 % | Procedimiento coherente con la cuantía |
| INT-CONS-01 | 2,2 % | 5.218.753 | 0,2 % | 6,3 % | Con adjudicación, al menos 1 oferta |
| INT-CONS-08 | 0,5 % | 5.218.753 | 0,3 % | 0,9 % | Importe de licitación y de adjudicación coherentes |
| INT-FIA-01 | 0,7 % | 5.218.753 | 0,3 % | 1,5 % | Número de ofertas en rango razonable |
| INT-FIA-04 | 10,1 % | 5.218.753 | 10,2 % | 9,8 % | Plazo de presentación razonable |
| INT-FIA-08 | 0,0 % | 5.218.753 | 0,0 % | 0,1 % | Presupuesto atípico (outlier) |
| INT-FIA-09 | 1,2 % | 5.218.753 | 0,8 % | 1,8 % | Importe adjudicado plausible por CPV |
| INT-FIA-11 | 0,0 % | 5.218.753 | 0,0 % | 0,1 % | Trazabilidad mínima del expediente |
| INT-CONS-20 | 37,8 % | 245.960 | — | — | Contrato SARA publicado en TED (§4) |
| INT-CONS-18 | 52,4 % | 4.095.232 | 54,1 % | 48,8 % | Adjudicatario presente en el BORME |

- **Score:** media 93,3 y mediana 94,7. En menores, media 93,7; en el resto, 92,5.
- **INT-VAL-09 (CPV).** Los 2.113.790 fallos son todos por CPV vacío; no hay ningún CPV mal formado.
  - 2.092.825 de esos fallos son menores. La LCSP (art. 63.4) no exige publicar el CPV de los menores.
  - Es lo que se publica, no un error del parser.
- **INT-CONS-18 (BORME).** Solo evalúa personas jurídicas: el NIF debe empezar por A-H, J-N, P-S, U, V o W. El cruce es por nombre normalizado, porque el BORME no publica el NIF.
  - No encontrados por letra del NIF: B (SL) 49,6 % y A (SA) 42,0 %.
  - En las entidades que no se inscriben en el Registro Mercantil pasa del 95 %: G (asociaciones y fundaciones), F (cooperativas), Q (organismos públicos), U (UTE), J (sociedades civiles), E (comunidades de bienes), N (entidades extranjeras) y V.
  - También D (comanditarias), con un 98,6 %, y W (establecimientos de no residentes), con un 71,7 %.
  - Para el modelo antifraude conviene leerlo solo en A y B. Aun así, la mitad sin encontrar apunta a nombres que no casan más que a empresas inexistentes.

### Añadido después: INT-FIA-12 e importes corregidos (issue #22)

La tabla anterior es anterior a los dos. Medidos después sobre la misma PLACSP regenerada y la misma salida de calidad:

- **INT-FIA-12** (presupuesto y adjudicación del mismo orden de magnitud, adjudicación < 100 × presupuesto): 495 fallos de 4.831.209 licitaciones evaluadas (0,010 %; en menores 0,003 %, en el resto 0,030 %). Con todas las versiones, 860 de 6.410.691.
  - Entra en el score: la media pasa de 93,30 a 93,58 y la mediana de 94,7 a 95,0 (menores 93,99; resto 92,74).
- **Importes corregidos** (`calidad/correcciones.py`, sin tocar los publicados), en la última versión:

  | Motivo | Campo | Licitaciones |
  |---|---|---:|
  | `no_comparable` | presupuesto (1 €, precios unitarios) | 441 |
  | `escala_x100` | adjudicación | 24 |
  | `escala_x1000` | adjudicación | 6 |
  | `inverosimil` | adjudicación | 23 |
  | `registro` | adjudicación y valor estimado | 1 y 1 (URDINBERRI) |

  - La adjudicación publicada suma 525.800 M€ y la corregida 520.130 M€.
  - CONSTRUCCIONES URDINBERRI, S.L. pasa de 2.381,6 M€ a 26,4 M€.
- **INT-CONS-20 de URDINBERRI.** Da fallo porque el cruce toma el valor estimado publicado (25.188.819,27 €, por encima del umbral SARA de obras), pero la plataforma de origen y la PLACSP declaran el contrato no sujeto a regulación armonizada. El valor estimado está en el registro de errores como probable errata.

## 6. Limitaciones y cómo reproducirlo

- La calidad y el cruce TED se calculan sobre la **última versión** de cada licitación, como hace el módulo de calidad. Las versiones anteriores están en la principal, marcadas con `es_ultima_version = False`.
- El snapshot de TED llega a 2025, y 2026 queda sin evaluar. El BORME es el del repo, así que las empresas inscritas después no cuentan.
- **Reproducir en la máquina del propietario** con `herramientas/sesion_2026_09_27/regeneracion_placsp/` (el README de `herramientas/sesion_2026_09_27/` explica las variables `TRABAJO`, `PY` y `REPO`):
  1. `descargar.sh`: los 45 ZIP. Hay que comprobar los hashes del anexo; si la PLACSP ha actualizado algún ZIP, cambian y hay más entradas.
  2. `procesar.sh`: `nacional/licitaciones.py` con `--semilla` de `v2026.02`. Pide unos 6 GB de RAM y ~15 GB de disco con los ZIP.
  3. `cadena2.sh`:
     - `reducir.py` y `subconjunto.py`, porque el cruce TED con las 55 columnas pasaba de 13 GB.
     - `cruce_ted.py` y la calidad.
     - `comparar_publicado.py` frente a `v2026.02`.
  4. `comparar_por_version.py`: el contraste del §3. Las cifras deben coincidir con las de este documento, o superarlas si hay ZIP nuevos.
  5. `publicar_release.py` con un token del propietario: release en borrador, con las notas de `notas_release.md`.

## Anexo: ZIP leídos (descargados el 2026-09-27)

| ZIP | Bytes | SHA-256 |
|---|---|---|
| `CPM_SectorPublico_2022.zip` | 258442 | `15c0020d31c97cd8f06d169ad271cdbad76e0ec9d348ab3dce593a3c99809aea` |
| `CPM_SectorPublico_2023.zip` | 612796 | `bd31cb07dad3ba890866dfd704fdbdaa479fbb8787a96252dc2f500f182071e8` |
| `CPM_SectorPublico_2024.zip` | 654808 | `4f3373e50505a510e963b917eb4c5a14e3c0f0f77ac8338fa79bcd6417948fef` |
| `CPM_SectorPublico_2025.zip` | 741012 | `707eb44ff659fc576f187b22f4fb89ab02f1a3bcee08b0f8f3b4d27050765591` |
| `CPM_SectorPublico_2026.zip` | 680949 | `3977a35600573ab69b661cac0ae193eebf3c929963d0898d9fc6f00e4b82fd82` |
| `EMP_SectorPublico_2022.zip` | 1642866 | `96c6473a86fd576a6dd1b447f1b1017a1c28fb74b86ca0faf579a7d7a99d9596` |
| `EMP_SectorPublico_2023.zip` | 1741424 | `cea787f522877444cb5b5789507b3d67d572e806114f75b9334f03c546cf4910` |
| `EMP_SectorPublico_2024.zip` | 1864957 | `53c0309592d8f179b79730b6092a6133306382d8a80c21c16028121987602dca` |
| `EMP_SectorPublico_2025.zip` | 2046491 | `423c5a01e69a4b109df69d8fcaec72325df1a12014d8327a0b08219c148dbd2c` |
| `EMP_SectorPublico_2026.zip` | 1606925 | `89cb2bf333f0b3f63b219b45b069fc1d3b6db8820346d8c0ab39be72993d9e9a` |
| `PlataformasAgregadasSinMenores_2016.zip` | 4089629 | `6ca8dbf3171b9fc33ee59d1aa2da66cb379048d09063a5c17c1d0d4acfd9ef97` |
| `PlataformasAgregadasSinMenores_2017.zip` | 9267272 | `5a2eeff3a1bd81ac0b05c76f08d8c23aa0f1f49217918b3c129a07a3d33f9553` |
| `PlataformasAgregadasSinMenores_2018.zip` | 54874064 | `e2bd3ab8c5201322af11cfd199f629da7aa57e2e8dbe016d3caee9bdf04fb2b5` |
| `PlataformasAgregadasSinMenores_2019.zip` | 68699311 | `c6917134c8031c9de15e5d0c989fadc4a39ae0844ad1e8abe62769116a2cf433` |
| `PlataformasAgregadasSinMenores_2020.zip` | 59059847 | `a028ad076684f37228c19d982b08b974187b727deb0f69b72fc10e89b05b680f` |
| `PlataformasAgregadasSinMenores_2021.zip` | 75570257 | `3acf7070b2021f75b47f4c8e699a6f12348839686691f325b733727b07750418` |
| `PlataformasAgregadasSinMenores_2022.zip` | 82657184 | `428b9ee5c38022e6c926ca118865c27f624634f89c57bafa034c670986c37600` |
| `PlataformasAgregadasSinMenores_2023.zip` | 88234635 | `bf14cc120432265b8b350fd01d8a93681b8df6892306326197bc78b1a87c8a9f` |
| `PlataformasAgregadasSinMenores_2024.zip` | 127929554 | `73fe8927142466ae26d4d9f9767af72720b8efbee3e4b2ec5496b56f6263aac2` |
| `PlataformasAgregadasSinMenores_2025.zip` | 139302233 | `51ab7fdb3e9224c3cfc091af943a8bfd922b51e30247e87718b4350ee9b46267` |
| `PlataformasAgregadasSinMenores_2026.zip` | 114921807 | `5c90119e451a0f9dbeba65c6b27b407d02276ac5f9260b4b2798dcc34145951c` |
| `contratosMenoresPerfilesContratantes_2018.zip` | 50297666 | `178f7d799741e01d6c34ccd5272111671d87336b44ed3a4e70d1e76cd607d495` |
| `contratosMenoresPerfilesContratantes_2019.zip` | 87178632 | `14a7a63e7f54ab33105915bbd808b3159a50454b26acf669f6dc17806137c54e` |
| `contratosMenoresPerfilesContratantes_2020.zip` | 84239231 | `aae936fb102b5d88457617d02e52a3ba999fc541406470da325bc384eba7eb05` |
| `contratosMenoresPerfilesContratantes_2021.zip` | 155396818 | `978b1c554b51ccb9f80f218d67b8c6c0c5f2b1bedd2194d54dbaac7f26bf52db` |
| `contratosMenoresPerfilesContratantes_2022.zip` | 206732416 | `16a197bf99c01fadccc97ef4eaf6e24b059f4ceb4daa729ae12603c3ccc94e32` |
| `contratosMenoresPerfilesContratantes_2023.zip` | 251245910 | `43b2a3238bb3bddffd3e28478b31622de9054cb397175ad050251e1166904464` |
| `contratosMenoresPerfilesContratantes_2024.zip` | 268657466 | `1e3ba0fc70c72800d8529cc55f8f181200934b6e698069f22b1b3f8876cae444` |
| `contratosMenoresPerfilesContratantes_2025.zip` | 297068701 | `586ed8b448f20273454b24edfcd723e8099d49e9d4630757494790111f1dec34` |
| `contratosMenoresPerfilesContratantes_2026.zip` | 220550333 | `5f01b5dd4e45f4c6e28ab922433364c47a77a3864b7ae50a20c38a8d9b8b84cd` |
| `licitacionesPerfilesContratanteCompleto3_2012.zip` | 16008909 | `de130e278de9877fe5840ba1263de75d1894949155989050edfad5cc76247763` |
| `licitacionesPerfilesContratanteCompleto3_2013.zip` | 20632011 | `1fbc453887a4923dedf9570e23aea10eea3a0bf1dbcb1311f8b43b0ec772c4e8` |
| `licitacionesPerfilesContratanteCompleto3_2014.zip` | 26239660 | `85c6b884dfa081067f843969ccaf03f6993dfa487f34c982386dae6d451c8222` |
| `licitacionesPerfilesContratanteCompleto3_2015.zip` | 37183295 | `4f8d6e29fcd7a831e58bf40ed35bf093ed8662ba979a6e58a361ccf85e59d4eb` |
| `licitacionesPerfilesContratanteCompleto3_2016.zip` | 58188149 | `ddeea4f453a72da195070f7e279dc0da4c5080eab39a438484f320a153fb8ecc` |
| `licitacionesPerfilesContratanteCompleto3_2017.zip` | 95910662 | `585b5ba2df1acd0fef1a5e6fa9fb624b93b0b203896fbd4162b305afde3d0936` |
| `licitacionesPerfilesContratanteCompleto3_2018.zip` | 276279363 | `6c027f8704b91678d7d2fec52a6bf5b2d540ac188f8b28159c0a01558698d16a` |
| `licitacionesPerfilesContratanteCompleto3_2019.zip` | 466498327 | `b3e4cc9c7d340ca5ba2807dc50574d68c4da338b88174afed4171bf917a7206b` |
| `licitacionesPerfilesContratanteCompleto3_2020.zip` | 445577831 | `d046552784468bb9a0db9bca00ba48229551bfed320c4ce66c8246604b7e6b46` |
| `licitacionesPerfilesContratanteCompleto3_2021.zip` | 605726559 | `1a77ca3f9a40d76f42e2ab883ca73d8e71c85410f9da10807a748a55a6de0287` |
| `licitacionesPerfilesContratanteCompleto3_2022.zip` | 947589492 | `c6a10117857a62c96d73c0dc412b2b0162861b171f99b4d6958e15e53a05b756` |
| `licitacionesPerfilesContratanteCompleto3_2023.zip` | 1668002789 | `39e7ab384113e8016e69196e0f3a5e20f28b0f19cc16b9e6c1ad6c56d609f493` |
| `licitacionesPerfilesContratanteCompleto3_2024.zip` | 1792750583 | `2b0bff734c49a423cf34c1d152863963f0ce329f7d8d4b224fa62cd510d6ba43` |
| `licitacionesPerfilesContratanteCompleto3_2025.zip` | 2172595976 | `3248179d8726dc8a592932c3e895eb2f7adff061a2cb4e217a6c057c02cde325` |
| `licitacionesPerfilesContratanteCompleto3_2026.zip` | 1714468263 | `f72564bce401fc1ac3b0d298168b9edc4ad7d17d62bd6667e71d7c70d8562ca9` |
