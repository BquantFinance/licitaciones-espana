# Operación de los scrapers en el VPS (buscalicitaciones.com)

Esta carpeta es la fuente de lo que corre en el VPS, desplegado en `/opt/apps/licitaciones-vps`
(ver `docs/CONTINUACION.md`, «Sincronización con el VPS»: el código vive en GitHub `main` y un
cambio hecho en el VPS se sube con rama y PR el mismo día).

Imagen: `docker build -f despliegue/vps/Dockerfile -t licitaciones-scrapers:vps .` desde la raíz del
repo, o desde `/opt/apps/licitaciones-vps` con su copia de `requirements.txt`.

Ejecuta los scrapers del repo [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana)
en Docker, con los datos fuera del repo y el histórico completo (sin sesgo del superviviente).

## Piezas

| Ruta | Qué es |
|---|---|
| `Dockerfile` | Imagen `licitaciones-scrapers:vps` (python 3.12-slim + `requirements.txt` del repo; pandas 3.0.6) |
| `Dockerfile.pandas22`, `Dockerfile.pandas223` | Imágenes solo para tests (pandas 2.3.3 y 2.2.3) |
| `runs/<commit>/` | Worktrees del repo, uno por commit revisado (no van en git) |
| `produccion` | Enlace al worktree que usa producción. Cambiarlo es «promover» otro commit |
| `bin/ejecutar_fuente.sh <fuente> [primera\|semanal\|prueba]` | Lanza una fuente. Ver la cabecera del script |
| `bin/cola_primera_descarga.sh` | Primera descarga de cada fuente, una línea de `cola_primera.txt` por noche |
| `fuentes/<fuente>.sh` | Comandos de cada fuente (`CMD_PRIMERA`, `CMD_SEMANAL`), límites y preparación |
| `bin/vigia_codigo.sh` | Cron diario (23:00): si `main` avanza, `git pull` de la copia del repo, comprueba la tabla «Estado de los scrapers» (sin WIP tras el cierre) y la suite de tests con pandas 3, y promueve el commit. `vigia_codigo.sh comprobar <ref>` solo comprueba |

## Datos (`/opt/data/licitaciones-historico/`)

| Ruta | Qué es |
|---|---|
| `<fuente>/` | Salida de cada scraper, con sus `_historico/`. Nunca se borra nada |
| `semillas/vps_20260503/` | Copia completa (sha256) de `/opt/data/buscalicitaciones` del 3-may-2026. Solo lectura |
| `semillas/release_v2026.02/` | Los 12 ficheros del release v2026.02 y `extraido/` (solo lectura, fechas del ZIP) |
| `logs/<fuente>/<fecha>_<modo>.log` | Log de cada ejecución |
| `logs/ejecuciones.jsonl` | Una línea por ejecución: fuente, modo, commit, duración, código de salida, espacio |
| `cola/<fuente>.ok` / `.fallo` | Estado de la primera descarga de cada fuente |
| `backups/` | Copias de `compose`, `.env` y crontab antes de cada cambio |

## Reglas

- Solo se usan scripts «Cerrado» de la tabla «Estado de los scrapers» de `docs/CONTINUACION.md`.
  Para promover un commit: `git log <cierre>..<commit> -- <script>` sin commits WIP y la suite de
  tests en verde con pandas 3.
- Nunca dos descargas pesadas a la vez (cerrojo global). Prioridad baja de CPU y disco.
  Abortan si quedan menos de 100 GB libres.
- Comunidad de Madrid y TED escriben junto a su script: se ejecuta una copia byte a byte idéntica
  dentro de su carpeta de datos (el log guarda el sha256 de las dos).
- El parse privado del BORME (`borme/parse/`) contiene nombres de personas: no se publica. Solo
  `borme/pub/`.

## Cron del VPS (usuario admin, hora de Madrid)

```
0 23 * * *  /opt/apps/licitaciones-vps/bin/vigia_codigo.sh
30 0 * * *  /opt/apps/licitaciones-vps/bin/cola_primera_descarga.sh >> /opt/data/licitaciones-historico/logs/cola.log 2>&1
```

Cuando termine la cola de primeras descargas, se sustituye por un cron semanal por fuente
(`bin/ejecutar_fuente.sh <fuente> semanal`).
