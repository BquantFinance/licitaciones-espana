# Operación de los scrapers en el VPS (buscalicitaciones.com)

Esta carpeta es la fuente de lo que corre en el VPS, desplegado en `/opt/apps/licitaciones-vps`
(ver `docs/CONTINUACION.md`, «Sincronización con el VPS»: el código vive en GitHub `main` y un
cambio hecho en el VPS se sube con rama y PR el mismo día). Al promover un commit, el vigía despliega
esta carpeta de ese commit: lo desplegado es siempre lo que hay en `main`.

Imagen: `docker build -f despliegue/vps/Dockerfile -t licitaciones-scrapers:vps .` desde la raíz del
repo, o desde `/opt/apps/licitaciones-vps` con su copia de `requirements.txt`. En producción la
construye el vigía (etiqueta `receta_sha` = huella del Dockerfile y de `requirements.txt`).

Ejecuta los scrapers del repo [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana)
en Docker, con los datos fuera del repo y el histórico completo (sin sesgo del superviviente).

## Piezas

| Ruta | Qué es |
|---|---|
| `Dockerfile` | Imagen `licitaciones-scrapers:vps` (python 3.12-slim + `requirements.txt` del repo; pandas 3.0.6; `unar` para los RAR que libarchive no lee) |
| `Dockerfile.pandas22`, `Dockerfile.pandas223` | Imágenes solo para tests (pandas 2.3.3 y 2.2.3), sobre la de producción |
| `runs/<commit>/` | Worktrees del repo, uno por commit revisado (no van en git) |
| `produccion` | Enlace al worktree que usa producción. Cambiarlo es «promover» otro commit |
| `bin/ejecutar_fuente.sh <fuente> [primera\|semanal\|prueba]` | Lanza una fuente. Códigos: 2 configuración, 3 disco < 100 GB, 4 ya en marcha, 5 cerrojo global ocupado, 6 preparación, 124 tiempo agotado, 11 semanal con avisos; otro, el del scraper |
| `bin/cola_primera_descarga.sh [--hasta-vaciar] [--parar-a HH:MM] [--saltar f1,f2]` | Primera descarga de cada fuente en el orden de `cola_primera.txt`. Sin opciones, una línea por ejecución. El cron de las 00:30 usa `--hasta-vaciar`: sigue con la línea siguiente hasta vaciar la cola, o hasta la hora de `--parar-a` |
| `fuentes/<fuente>.sh` | Comandos de cada fuente (`CMD_PRIMERA`, `CMD_SEMANAL`), límites y preparación (`PREPARAR_PRIMERA`, `PREPARAR_SEMANAL`) |
| `bin/vigia_codigo.sh` | Cron diario (23:00). Si `main` avanza: `git pull` de la copia del repo, comprueba que ningún fichero de código tenga como último cambio un commit WIP, la tabla «Estado de los scrapers» y la suite de tests con la imagen que usará producción, y promueve (enlace, `despliegue/vps` e imagen). `vigia_codigo.sh comprobar <ref>` solo comprueba |

## Datos (`/opt/data/licitaciones-historico/`)

| Ruta | Qué es |
|---|---|
| `<fuente>/` | Salida de cada scraper, con sus `_historico/`. Nunca se borra nada |
| `semillas/vps_20260503/` | Copia completa (sha256) de `/opt/data/buscalicitaciones` del 3-may-2026. Solo lectura |
| `semillas/release_v2026.02/` | Los 12 ficheros del release v2026.02 y `extraido/` (solo lectura, fechas del ZIP) |
| `logs/<fuente>/<fecha>_<modo>.log` | Log de cada ejecución, con el commit usado |
| `logs/ejecuciones.jsonl` | Una línea por ejecución (también las que no arrancan): fuente, modo, commit, duración, código, espacio |
| `logs/promocion.log` | Lo que hace el vigía cada noche |
| `cola/<fuente>.ok` / `.fallo` / `.espera` | Primera descarga hecha, fallida (se revisa a mano) o aparcada (no se ejecuta hasta borrar la marca) |
| `backups/` | Copias de `compose`, `.env` y crontab antes de cada cambio |

## Reglas

- Solo se usan scripts «Cerrado» de la tabla «Estado de los scrapers» de `docs/CONTINUACION.md`, sin
  commits WIP después de su cierre, con la suite de tests en verde.
- Nunca dos descargas a la vez (cerrojo global). Prioridad baja de CPU y disco. Abortan si quedan
  menos de 100 GB libres (se mira después de conseguir el cerrojo).
- Comunidad de Madrid y TED escriben junto a su script: se ejecuta una copia byte a byte idéntica
  dentro de su carpeta de datos (el log guarda el sha256 de las dos).
- PLACSP: la salida se llama `licitaciones_completo` (`.parquet`, `_resultados.parquet`...): nombre
  fijo, así que al cambiar de año la cadena de versiones de `_historico/` sigue y no queda ninguna salida
  congelada. El nombre con años de cada año (`licitaciones_completo_2012_<año>...`, el de antes) es un
  enlace simbólico a la vigente: lo que aún lo lee (el ETL de la web) sigue funcionando.
  - Quien lea la salida, por el nombre fijo y **nunca por patrón**: un glob lee también los enlaces (en
    2027, `*_resultados.parquet` daría el triple de filas).
  - El script nunca escribe a través de un enlace: un rango parcial cuyo nombre es uno de ellos (p.ej.
    `--anos 2012-2026` en 2027) se para sin tocar nada.
  - Para volver al código sin nombre fijo, antes de desplegarlo y con el cerrojo de la fuente (ver
    «Volver al código sin nombre fijo» en el README principal):
    `flock /opt/data/licitaciones-historico/nacional/.cerrojo docker run --rm --network none -v "$(readlink -f /opt/apps/licitaciones-vps/produccion)":/repo:ro -v /opt/data/licitaciones-historico/nacional:/datos licitaciones-scrapers:vps python /repo/nacional/licitaciones.py --output-dir /datos/salida --deshacer-nombre-fijo`
- El parse privado del BORME (`borme/parse/`) contiene nombres de personas: no se publica. Solo
  `borme/pub/`.

## Cron del VPS (usuario admin, hora de Madrid)

```
0 23 * * *  /opt/apps/licitaciones-vps/bin/vigia_codigo.sh
30 0 * * *  /opt/apps/licitaciones-vps/bin/cola_primera_descarga.sh --hasta-vaciar >> /opt/data/licitaciones-historico/logs/cola.log 2>&1
# Semanales (desde el 28/29-sep-2026), a la 01:00; si coinciden, el cerrojo global las pone en fila
0 1 * * 1   /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh nacional semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
0 1 * * 2   … catalunya y valencia (una línea por fuente)
0 1 * * 3   /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh asturias semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
0 1 * * 4   /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh andalucia semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
0 1 * * 5   … ayto_madrid, aragon, castilla_leon, murcia, extremadura, la_rioja, valencia_menores, castilla_la_mancha y municipios
0 1 * * 6   /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh ted semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
# Mensuales: Comunidad de Madrid (día 5; cada ejecución guarda ~1 GB en _historico/) y Galicia (día 15; ~9 h)
0 1 5 * *   /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh comunidad_madrid semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
0 1 15 * *  /opt/apps/licitaciones-vps/bin/ejecutar_fuente.sh galicia semanal >> /opt/data/licitaciones-historico/logs/semanal.log 2>&1
# Web (repo BquantFinance/buscalicitaciones, docs/PASO_A_PRODUCCION_V2.md §7): el domingo a las 07:00 construye y publica
# la web con lo bajado en la semana; su ETL toma el cerrojo global mientras lee
0 7 * * 0   /opt/apps/buscalicitaciones-v2/despliegue/actualizar_web.sh >> /opt/data/buscalicitaciones-v2/logs/cron.log 2>&1
```

Cada fuente entra en su cron semanal (`bin/ejecutar_fuente.sh <fuente> semanal`) cuando su primera
descarga está verificada. Andalucía (jueves), Galicia (día 15) y municipios (viernes) entraron el
30-sep-2026, tras las PR #46, #47 y #42. En municipios, un error de origen permanente (el XLSX de agosto de
2026 de Leganés o el del 4T-2020 de Málaga) solo da rc=1 la primera vez: esa ejecución lo anota en
`raw/_fallos_origen.json` y, desde el día siguiente, se avisa con rc=0. Calendario previsto para las que
faltan, cuando su primera descarga termine bien: jueves los menores de Andalucía del CKAN, Euskadi y BORME
(este y los menores de la PSCP murieron por memoria en su primera descarga el 29-sep); sábado los menores de
la PSCP. La calidad y el cruce TED de la PLACSP aún no van en cron. Con la
cola vacía, el cron de las 00:30 no hace nada.
