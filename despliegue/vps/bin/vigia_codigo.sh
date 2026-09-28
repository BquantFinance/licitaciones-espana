#!/bin/bash
# Vigía del código de producción. Si origin/main avanza, comprueba el commit nuevo y, si cumple las
# reglas, lo promueve: cambia el enlace `produccion`, despliega `despliegue/vps` de ese commit en
# /opt/apps/licitaciones-vps y, si cambió requirements.txt, pone en producción la imagen ya probada.
# Nunca toca ramas remotas ni hace push.
#
#   uso: vigia_codigo.sh                 # cron: fetch, git pull de main, comprobar y promover
#        vigia_codigo.sh comprobar <ref> # solo comprobar un commit: no cambia enlace, imagen ni despliegue
#
# Reglas:
#   1. Ningún fichero de código (fuera de docs/) cuyo último cambio en ACTUAL..NUEVO sea un commit WIP
#      (cubre también comun/ y despliegue/, que no están en la tabla).
#   2. Cada script que usa fuentes/*.sh está «Cerrado» en la tabla «Estado de los scrapers» de
#      docs/CONTINUACION.md y no tiene commits WIP entre su cierre y NUEVO.
#   3. La suite completa de tests pasa con la imagen que usará producción (pandas 3). Si cambia
#      requirements.txt, esa imagen se construye como candidata (`:cand-<commit>`) y solo pasa a
#      `licitaciones-scrapers:vps` al promover.
# Un trabajo en marcha no se ve afectado: resuelve `produccion` al empezar, los worktrees antiguos no
# se borran y los ficheros desplegados se sustituyen con mv (inodo nuevo).
set -uo pipefail
VPS=/opt/apps/licitaciones-vps
REPO=/opt/apps/licitaciones-espana
H=/opt/data/licitaciones-historico
IMAGEN=licitaciones-scrapers:vps
MODO="${1:-cron}"
mkdir -p "$H/logs"
[ "$MODO" = cron ] && exec >> "$H/logs/promocion.log" 2>&1
exec 6>"$H/logs/.cerrojo_vigia"
flock -n 6 || { echo "$(date -Is) ya hay un vigía en marcha: no se arranca otro"; exit 0; }
echo "=== $(date -Is) modo=$MODO"
cd "$REPO" || exit 2
timeout 5m git fetch -q origin || { echo "git fetch falló"; exit 1; }
# La copia del repo que usa el VPS va siempre al día con main (docs/CONTINUACION.md, «Sincronización
# con el VPS»). Solo fast-forward: si hubiera cambios locales, se para y se revisa a mano.
if [ "$MODO" = cron ]; then
  [ "$(git rev-parse --abbrev-ref HEAD)" = main ] && git merge -q --ff-only origin/main \
    || { echo "la copia local no está en main o no avanza en fast-forward: revisar a mano"; exit 1; }
  echo "copia local de main: $(git rev-parse --short HEAD)"
fi

ACTUAL=$(git -C "$(readlink -f "$VPS/produccion")" rev-parse HEAD)
if [ "$MODO" = comprobar ]; then
  NUEVO=$(git rev-parse "${2:?falta el commit}^{commit}") || exit 2
else
  NUEVO=$(git rev-parse origin/main)
  if [ "$ACTUAL" = "$NUEVO" ]; then echo "main sin cambios ($(git rev-parse --short "$NUEVO"))"; exit 0; fi
  git merge-base --is-ancestor "$ACTUAL" "$NUEVO" || { echo "origin/main no desciende de producción: revisar a mano"; exit 1; }
fi
CORTO=$(git rev-parse --short "$NUEVO")
W="$VPS/runs/$CORTO"
if [ -d "$W" ]; then
  [ "$(git -C "$W" rev-parse HEAD 2>/dev/null)" = "$NUEVO" ] && [ -z "$(git -C "$W" status --porcelain 2>/dev/null)" ] \
    || { echo "runs/$CORTO existe pero no es $CORTO o tiene cambios: revisar a mano"; exit 1; }
else
  git worktree add -q --detach "$W" "$NUEVO" || { echo "no se pudo crear el worktree"; exit 1; }
fi
echo "producción $(git rev-parse --short "$ACTUAL") -> candidato $CORTO ($(git log -1 --format=%s "$NUEVO" | cut -c1-90))"

# Regla 1: último cambio de cada fichero de código sin WIP
MAL=""
while IFS= read -r f; do
  [ -z "$f" ] && continue
  ultimo=$(git log -1 --format='%h %s' "$ACTUAL..$NUEVO" -- "$f")
  case "$ultimo" in *WIP*) MAL="$MAL\n  $f: último cambio en un commit WIP ($ultimo)";; esac
done < <(git diff --name-only "$ACTUAL" "$NUEVO" -- . ':!docs')
[ -z "$MAL" ] || { echo -e "RECHAZADO por commits WIP:$MAL"; exit 1; }

# Regla 2: tabla de estado frente a los scripts que usa producción
python3 - "$W" "$VPS/fuentes" "$NUEVO" <<'PY' || { echo "RECHAZADO por la tabla de estado"; exit 1; }
import re, subprocess, sys, pathlib
w, fuentes, nuevo = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
doc = (w / "docs/CONTINUACION.md").read_text(encoding="utf-8")
tabla, dentro = {}, False
for linea in doc.splitlines():
    if linea.startswith("### Estado de los scrapers"): dentro = True; continue
    if dentro and linea.startswith("#"): break
    if dentro and linea.startswith("| `"):
        celdas = [c.strip() for c in linea.strip("|").split("|")]
        scripts = re.findall(r"`([^`]+)`", celdas[0]); cierre = re.findall(r"`([0-9a-f]{7,40})`", celdas[2])
        for s in scripts: tabla[s] = (celdas[1], cierre[0] if cierre else None)
if not tabla: print("  no se encontró la tabla «Estado de los scrapers»"); sys.exit(1)
# Los scripts que usa producción: los de las fuentes desplegadas y, si el commit trae despliegue/vps, los suyos
usados = set()
for d in (fuentes, w / "despliegue/vps/fuentes"):
    for f in d.glob("*.sh"):
        usados |= set(re.findall(r"/repo/([\w./-]+\.py)", f.read_text(encoding="utf-8")))
mal = []
for s in sorted(usados):
    fila = tabla.get(s) or next((v for k, v in tabla.items() if "*" in k and pathlib.PurePosixPath(s).match(k)), None)
    if not fila: mal.append(f"{s}: no está en la tabla"); continue
    estado, cierre = fila
    if not estado.startswith("Cerrado") or not cierre: mal.append(f"{s}: estado «{estado}»"); continue
    if not (w / s).exists(): mal.append(f"{s}: no existe en el commit"); continue
    log = subprocess.run(["git", "-C", str(w), "log", "--format=%h %s", f"{cierre}..{nuevo}", "--", s],
                         capture_output=True, text=True)
    if log.returncode: mal.append(f"{s}: cierre {cierre} desconocido"); continue
    wip = [l for l in log.stdout.splitlines() if "WIP" in l]
    if wip: mal.append(f"{s}: commits WIP tras el cierre {cierre}: {wip[:3]}")
nuevos = sorted(k for k, (e, _) in tabla.items() if e.startswith("Cerrado") and "*" not in k and k not in usados and k.endswith(".py"))
print(f"  scripts usados por producción: {len(usados)}; en la tabla: {len(tabla)}")
if nuevos: print(f"  AVISO: cerrados en la tabla sin configurar en fuentes/ (revisar a mano): {nuevos}")
for m in mal: print("  " + m)
sys.exit(1 if mal else 0)
PY

# Imagen: huella de su receta (Dockerfile de despliegue/vps del commit, o el desplegado si no lo trae,
# más requirements.txt). Si no es la de la imagen de producción (etiqueta receta_sha), se construye y
# prueba una candidata; solo pasa a producción al promover.
CTX=$(mktemp -d)
if git cat-file -e "$NUEVO:despliegue/vps/Dockerfile" 2>/dev/null; then
  git show "$NUEVO:despliegue/vps/Dockerfile" > "$CTX/Dockerfile"
else
  cp "$VPS/Dockerfile" "$CTX/Dockerfile"
fi
git show "$NUEVO:requirements.txt" > "$CTX/requirements.txt"
RECETA_NUEVA=$(cat "$CTX/Dockerfile" "$CTX/requirements.txt" | sha256sum | cut -c1-16)
RECETA_PROD=$(docker image inspect "$IMAGEN" --format '{{index .Config.Labels "receta_sha"}}' 2>/dev/null)
IMAGEN_TEST="$IMAGEN"
if [ "$RECETA_NUEVA" != "$RECETA_PROD" ]; then
  IMAGEN_TEST="licitaciones-scrapers:cand-$CORTO"
  echo "receta de la imagen distinta (${RECETA_PROD:-sin etiqueta} -> $RECETA_NUEVA): imagen candidata $IMAGEN_TEST"
  timeout 60m nice docker build -q --label "receta_sha=$RECETA_NUEVA" -t "$IMAGEN_TEST" "$CTX" \
    || { rm -rf "$CTX"; echo "RECHAZADO: falló la imagen candidata"; exit 1; }
fi
rm -rf "$CTX"

# Regla 3: suite completa con la imagen que usará producción
TESTLOG="$H/logs/tests_${CORTO}_$(date +%Y%m%d_%H%M%S).log"
docker run --rm --network none --cpus 4 -m 8g -v "$W":/repo:ro "$IMAGEN_TEST" \
  timeout 50m sh -c 'python -c "import pandas,numpy,pyarrow;print(\"pandas\",pandas.__version__,\"numpy\",numpy.__version__,\"pyarrow\",pyarrow.__version__)"; python -m pytest -q -p no:cacheprovider --no-header' > "$TESTLOG" 2>&1
RC=$?
echo "  tests ($IMAGEN_TEST): rc=$RC $(tail -1 "$TESTLOG") ($TESTLOG)"
if [ $RC -ne 0 ] || grep -q -E 'Fatal Python error|Segmentation' "$TESTLOG"; then echo "RECHAZADO por los tests"; exit 1; fi

# Despliegue: lo que hay en despliegue/vps del commit frente a lo desplegado
DIFS=""
if [ -d "$W/despliegue/vps" ]; then
  while IFS= read -r rel; do
    cmp -s "$W/despliegue/vps/$rel" "$VPS/$rel" || DIFS="$DIFS $rel"
  done < <(cd "$W/despliegue/vps" && find . -type f | sed 's#^\./##' | sort)
fi
[ -n "$DIFS" ] && echo "  despliegue/vps distinto de lo desplegado en:$DIFS"

if [ "$MODO" = comprobar ]; then echo "APTO (modo comprobar: no se cambia enlace, imagen ni despliegue)"; exit 0; fi

# Promoción
for rel in $DIFS; do
  mkdir -p "$(dirname "$VPS/$rel")"
  install -m "$(stat -c %a "$W/despliegue/vps/$rel")" "$W/despliegue/vps/$rel" "$VPS/$rel.nuevo.$$" && mv -f "$VPS/$rel.nuevo.$$" "$VPS/$rel" \
    && echo "  desplegado $rel"
done
if [ "$IMAGEN_TEST" != "$IMAGEN" ]; then
  docker tag "$IMAGEN_TEST" "$IMAGEN" && git show "$NUEVO:requirements.txt" > "$VPS/requirements.txt" \
    && echo "  imagen $IMAGEN_TEST -> $IMAGEN"
fi
ln -sfn "runs/$CORTO" "$VPS/produccion.nuevo" && mv -T "$VPS/produccion.nuevo" "$VPS/produccion"
git -C "$VPS" add -A && git -C "$VPS" -c user.name="admin VPS" -c user.email=bquantfinance@gmail.com commit -q -m "Promovido licitaciones-espana $CORTO (vigía: WIP, tabla de estado y tests en verde)"
echo "PROMOVIDO: produccion -> runs/$CORTO"
