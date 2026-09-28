#!/bin/bash
# Ejecuta un scraper de licitaciones-espana en Docker con el código fijado en
# /opt/apps/licitaciones-vps/produccion (worktree de un commit revisado, en solo lectura).
#
#   uso: ejecutar_fuente.sh <fuente> [primera|semanal|prueba]
#
# - Datos:   /opt/data/licitaciones-historico/<fuente>/   (nunca dentro del repo)
# - Semillas (solo lectura): /opt/data/licitaciones-historico/semillas -> /semillas
# - Log:     /opt/data/licitaciones-historico/logs/<fuente>/<fecha>_<modo>.log
# - Resumen: /opt/data/licitaciones-historico/logs/ejecuciones.jsonl (una línea por ejecución,
#            también las que no llegan a arrancar)
#
# Cerrojos: uno por fuente (no se solapan dos ejecuciones de la misma) y uno global
# (nunca dos descargas a la vez: la siguiente espera hasta 12 h).
# Códigos de salida propios: 2 configuración, 3 disco < 100 GB, 4 fuente ya en marcha,
# 5 cerrojo global ocupado, 6 falló la preparación. Otro código es el del scraper
# (124 = se agotó el tiempo máximo de la fuente; 11 = semanal con avisos, ver fuentes/nacional.sh).
set -uo pipefail

FUENTE="${1:?uso: ejecutar_fuente.sh <fuente> [primera|semanal|prueba]}"
MODO="${2:-semanal}"
VPS=/opt/apps/licitaciones-vps
H=/opt/data/licitaciones-historico
CONF="$VPS/fuentes/$FUENTE.sh"
INICIO=$(date +%Y%m%d_%H%M%S)
T0=$(date +%s); RC=2; COMMIT=""; TAM_ANTES=0; LOG=/dev/null
mkdir -p "$H/logs"

# Toda salida deja su línea en ejecuciones.jsonl (los campos numéricos se validan: nunca JSON roto)
resumen() {
  local t1 tam libre
  t1=$(date +%s)
  tam=$( [ -d "${DATOS:-/nonexistent}" ] && du -sb "$DATOS" 2>/dev/null | cut -f1 || echo 0 )
  libre=$(df -BG --output=avail "$H" 2>/dev/null | tail -1 | tr -dc 0-9)
  [[ "$tam" =~ ^[0-9]+$ ]] || tam=0; [[ "$libre" =~ ^[0-9]+$ ]] || libre=0
  [[ "$TAM_ANTES" =~ ^[0-9]+$ ]] || TAM_ANTES=0
  printf '{"fuente":"%s","modo":"%s","commit":"%s","inicio":"%s","duracion_s":%d,"rc":%d,"tam_antes":%s,"tam_despues":%s,"libre_gb_fin":%s,"log":"%s"}\n' \
    "$FUENTE" "$MODO" "$COMMIT" "$INICIO" "$((t1 - T0))" "$RC" "$TAM_ANTES" "$tam" "$libre" "$LOG" \
    >> "$H/logs/ejecuciones.jsonl"
}
salir() { RC=$1; [ -n "${2:-}" ] && echo "$2" | tee -a "$LOG" >&2; resumen; exit "$RC"; }

[ -f "$CONF" ] || salir 2 "no existe $CONF"
CODIGO=$(readlink -f "$VPS/produccion")
COMMIT=$(git -C "$CODIGO" rev-parse --short HEAD 2>/dev/null) || salir 2 "sin código fijado en $VPS/produccion"
DATOS="$H/$FUENTE"
mkdir -p "$DATOS" "$H/logs/$FUENTE"
LOG="$H/logs/$FUENTE/${INICIO}_${MODO}.log"
export H DATOS

# Valores por defecto. fuentes/<fuente>.sh define CMD_PRIMERA y CMD_SEMANAL y puede cambiar el resto.
CPUS=4; MEM=10g; LIMITE=6h; IMAGEN=licitaciones-scrapers:vps; MONTAJES=(); PREPARAR_PRIMERA=""; PREPARAR_SEMANAL=""
CMD_PRUEBA='python -c "import comun.historico, pandas, pyarrow; print(\"imports ok; pandas\", pandas.__version__, \"pyarrow\", pyarrow.__version__)" && ls /semillas && touch /datos/.prueba_escritura && rm /datos/.prueba_escritura && echo "escritura ok en $(pwd)"'
CMD_PRIMERA=""; CMD_SEMANAL=""
source "$CONF"
case "$MODO" in
  primera) CMD="$CMD_PRIMERA"; PREPARAR="$PREPARAR_PRIMERA";;
  semanal) CMD="$CMD_SEMANAL"; PREPARAR="$PREPARAR_SEMANAL";;
  prueba)  CMD="$CMD_PRUEBA";  PREPARAR="";;
  *) salir 2 "modo desconocido: $MODO";;
esac
[ -n "$CMD" ] || salir 2 "$CONF no define el comando del modo $MODO"

exec 8>"$DATOS/.cerrojo"
flock -n 8 || salir 4 "ya hay una ejecución de $FUENTE en marcha"
if [ "$MODO" != prueba ]; then
  exec 9>"$H/.cerrojo_global"
  flock -w 43200 9 || salir 5 "el cerrojo global lleva más de 12 h ocupado"
fi

# El disco se mira después de conseguir el cerrojo global: la espera puede ser de horas
LIBRE_GB=$(df -BG --output=avail "$H" | tail -1 | tr -dc 0-9)
[ "$LIBRE_GB" -ge 100 ] || salir 3 "ABORTA: solo ${LIBRE_GB} GB libres (mínimo 100)"

if [ -n "$PREPARAR" ]; then
  echo "preparación: $PREPARAR" >> "$LOG"
  bash -c "$PREPARAR" >> "$LOG" 2>&1 || salir 6 "falló la preparación"
fi

TAM_ANTES=$(du -sb "$DATOS" | cut -f1)
{
  echo "fuente=$FUENTE modo=$MODO commit=$COMMIT imagen=$IMAGEN limite=$LIMITE cpus=$CPUS mem=$MEM"
  echo "inicio=$(date -Is) libre=${LIBRE_GB}G tam_antes=$TAM_ANTES"
  echo "cmd: $CMD"
  echo "------------------------------------------------------------------------"
} >> "$LOG"

# Solo existe si una ejecución anterior murió sin que Docker la borrase
docker rm -f "lic-$FUENTE" >/dev/null 2>&1
docker run --rm --name "lic-$FUENTE" \
  --cpus "$CPUS" -m "$MEM" --memory-swap "$MEM" --cpu-shares 256 \
  -v "$CODIGO":/repo:ro -v "$DATOS":/datos -v "$H/semillas":/semillas:ro "${MONTAJES[@]}" \
  -e PYTHONPATH=/repo -e TZ=Europe/Madrid -e PYTHONUNBUFFERED=1 -w /datos "$IMAGEN" \
  timeout --kill-after=10m "$LIMITE" ionice -c3 nice -n 19 bash -c "$CMD" >> "$LOG" 2>&1
RC=$?

{
  echo "------------------------------------------------------------------------"
  echo "fin=$(date -Is) rc=$RC duracion_s=$(( $(date +%s) - T0 )) tam_despues=$(du -sb "$DATOS" | cut -f1) libre=$(df -BG --output=avail "$H" | tail -1 | tr -dc 0-9)G"
} >> "$LOG"
resumen
exit $RC
