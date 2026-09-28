#!/bin/bash
# Vigía del código de producción. Si origin/main avanza, comprueba el commit nuevo y, si cumple las
# reglas, lo promueve (cambia el enlace `produccion`). Nunca toca ramas remotas ni hace push.
#
#   uso: vigia_codigo.sh                 # cron: fetch, comprobar y promover si procede
#        vigia_codigo.sh comprobar <ref> # solo comprobar un commit (no promueve)
#
# Reglas (docs/CONTINUACION.md, tabla «Estado de los scrapers»):
#   1. Cada script que usa fuentes/*.sh está en la tabla como «Cerrado», y entre su commit de cierre
#      y el commit nuevo no hay ningún commit WIP que lo toque.
#   2. La suite completa de tests pasa con la imagen de producción (pandas 3).
# Si cambia requirements.txt, primero se reconstruye la imagen. Un trabajo en marcha no se ve
# afectado: resuelve `produccion` al empezar y los worktrees antiguos no se borran.
set -uo pipefail
VPS=/opt/apps/licitaciones-vps
REPO=/opt/apps/licitaciones-espana
H=/opt/data/licitaciones-historico
MODO="${1:-cron}"
mkdir -p "$H/logs"
[ "$MODO" = cron ] && exec >> "$H/logs/promocion.log" 2>&1
echo "=== $(date -Is) modo=$MODO"
cd "$REPO" || exit 2
git fetch -q origin || { echo "git fetch falló"; exit 1; }
# La copia del repo que usa el VPS va siempre al día con main (docs/CONTINUACION.md, «Sincronización
# con el VPS»). Solo fast-forward: si hubiera cambios locales, se para y se revisa a mano.
if [ "$MODO" = cron ]; then
  [ "$(git rev-parse --abbrev-ref HEAD)" = main ] && git merge -q --ff-only origin/main \
    || { echo "la copia local no está en main o no avanza en fast-forward: revisar a mano"; exit 1; }
  echo "copia local de main: $(git rev-parse --short HEAD)"
fi

ACTUAL=$(git -C "$(readlink -f "$VPS/produccion")" rev-parse HEAD)
if [ "$MODO" = comprobar ]; then
  NUEVO=$(git rev-parse "${2:?falta el commit}") || exit 2
else
  NUEVO=$(git rev-parse origin/main)
  if [ "$ACTUAL" = "$NUEVO" ]; then echo "main sin cambios ($(git rev-parse --short "$NUEVO"))"; exit 0; fi
  git merge-base --is-ancestor "$ACTUAL" "$NUEVO" || { echo "origin/main no desciende de producción: revisar a mano"; exit 1; }
fi
CORTO=$(git rev-parse --short "$NUEVO")
W="$VPS/runs/$CORTO"
[ -d "$W" ] || git worktree add -q --detach "$W" "$NUEVO" || { echo "no se pudo crear el worktree"; exit 1; }
echo "producción $(git rev-parse --short "$ACTUAL") -> candidato $CORTO ($(git log -1 --format=%s "$NUEVO" | cut -c1-90))"

# Regla 1: tabla de estado frente a los scripts que usa producción
python3 - "$W" "$VPS/fuentes" "$NUEVO" <<'PY' || { echo "RECHAZADO por la tabla de estado"; exit 1; }
import re, subprocess, sys, glob, pathlib
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
usados = set()
for f in fuentes.glob("*.sh"):
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

# Si cambió requirements.txt, reconstruir la imagen antes de probar
if ! git diff --quiet "$ACTUAL" "$NUEVO" -- requirements.txt; then
  echo "requirements.txt cambió: se reconstruye licitaciones-scrapers:vps"
  cp "$W/requirements.txt" "$VPS/requirements.txt"
  nice docker build -q -t licitaciones-scrapers:vps "$VPS" || { echo "RECHAZADO: falló la imagen"; exit 1; }
fi

# Regla 2: suite completa con la imagen de producción
TESTLOG="$H/logs/tests_${CORTO}_$(date +%Y%m%d_%H%M%S).log"
docker run --rm --network none --cpus 4 -m 8g -v "$W":/repo:ro licitaciones-scrapers:vps \
  sh -c 'python -c "import pandas,numpy,pyarrow;print(\"pandas\",pandas.__version__,\"numpy\",numpy.__version__,\"pyarrow\",pyarrow.__version__)"; python -m pytest -q -p no:cacheprovider --no-header' > "$TESTLOG" 2>&1
RC=$?
RESUMEN=$(tail -1 "$TESTLOG")
echo "  tests: rc=$RC $RESUMEN ($TESTLOG)"
if [ $RC -ne 0 ] || grep -q -E 'Fatal Python error|Segmentation' "$TESTLOG"; then echo "RECHAZADO por los tests"; exit 1; fi

if [ "$MODO" = comprobar ]; then echo "APTO (no se promueve en modo comprobar)"; exit 0; fi
ln -sfn "runs/$CORTO" "$VPS/produccion.nuevo" && mv -T "$VPS/produccion.nuevo" "$VPS/produccion"
git -C "$VPS" add produccion requirements.txt && git -C "$VPS" -c user.name="admin VPS" -c user.email=bquantfinance@gmail.com commit -q -m "Promovido licitaciones-espana $CORTO (vigía: tabla de estado y tests en verde)"
echo "PROMOVIDO: produccion -> runs/$CORTO"
