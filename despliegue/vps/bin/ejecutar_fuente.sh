#!/bin/bash
# Ejecuta un scraper de licitaciones-espana en Docker con el código fijado en
# /opt/apps/licitaciones-vps/produccion (worktree de un commit revisado, en solo lectura).
#
#   uso: ejecutar_fuente.sh <fuente> [primera|semanal|prueba]
#
# - Datos:   /opt/data/licitaciones-historico/<fuente>/   (nunca dentro del repo)
# - Semillas (solo lectura): /opt/data/licitaciones-historico/semillas -> /semillas
# - Log:     /opt/data/licitaciones-historico/logs/<fuente>/<fecha>_<modo>.log
# - Resumen: /opt/data/licitaciones-historico/logs/ejecuciones.jsonl (una línea por ejecución)
#
# Cerrojos: uno por fuente (no se solapan dos ejecuciones de la misma) y uno global
# (nunca dos descargas a la vez: la siguiente espera hasta 12 h).
# Códigos de salida propios: 2 configuración, 3 disco < 100 GB, 4 fuente ya en marcha,
# 5 cerrojo global ocupado, 6 falló la preparación. Otro código es el del scraper
# (124 = se agotó el tiempo máximo de la fuente).
set -uo pipefail

FUENTE="${1:?uso: ejecutar_fuente.sh <fuente> [primera|semanal|prueba]}"
MODO="${2:-semanal}"
VPS=/opt/apps/licitaciones-vps
H=/opt/data/licitaciones-historico
CONF="$VPS/fuentes/$FUENTE.sh"
[ -f "$CONF" ] || { echo "no existe $CONF" >&2; exit 2; }

CODIGO=$(readlink -f "$VPS/produccion")
COMMIT=$(git -C "$CODIGO" rev-parse --short HEAD) || { echo "sin código fijado en $VPS/produccion" >&2; exit 2; }
DATOS="$H/$FUENTE"
mkdir -p "$DATOS" "$H/logs/$FUENTE"
INICIO=$(date +%Y%m%d_%H%M%S)
LOG="$H/logs/$FUENTE/${INICIO}_${MODO}.log"
export H DATOS

# Valores por defecto. fuentes/<fuente>.sh define CMD_PRIMERA y CMD_SEMANAL y puede cambiar el resto.
CPUS=4; MEM=10g; LIMITE=6h; IMAGEN=licitaciones-scrapers:vps; MONTAJES=(); PREPARAR_PRIMERA=""
CMD_PRUEBA='python -c "import comun.historico, pandas, pyarrow; print(\"imports ok; pandas\", pandas.__version__, \"pyarrow\", pyarrow.__version__)" && ls /semillas && touch /datos/.prueba_escritura && rm /datos/.prueba_escritura && echo "escritura ok en $(pwd)"'
CMD_PRIMERA=""; CMD_SEMANAL=""
source "$CONF"
case "$MODO" in
  primera) CMD="$CMD_PRIMERA";;
  semanal) CMD="$CMD_SEMANAL";;
  prueba)  CMD="$CMD_PRUEBA";;
  *) echo "modo desconocido: $MODO" >&2; exit 2;;
esac
[ -n "$CMD" ] || { echo "$CONF no define el comando del modo $MODO" >&2; exit 2; }

LIBRE_GB=$(df -BG --output=avail "$H" | tail -1 | tr -dc 0-9)
if [ "$LIBRE_GB" -lt 100 ]; then
  echo "ABORTA: solo ${LIBRE_GB} GB libres (mínimo 100)" | tee -a "$LOG"; exit 3
fi

exec 8>"$DATOS/.cerrojo"
flock -n 8 || { echo "ya hay una ejecución de $FUENTE en marcha" | tee -a "$LOG"; exit 4; }
if [ "$MODO" != prueba ]; then
  exec 9>"$H/.cerrojo_global"
  flock -w 43200 9 || { echo "el cerrojo global lleva más de 12 h ocupado" | tee -a "$LOG"; exit 5; }
fi

if [ "$MODO" = primera ] && [ -n "$PREPARAR_PRIMERA" ]; then
  echo "preparación: $PREPARAR_PRIMERA" >> "$LOG"
  bash -c "$PREPARAR_PRIMERA" >> "$LOG" 2>&1 || { echo "falló la preparación" | tee -a "$LOG"; exit 6; }
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
T0=$(date +%s)
docker run --rm --name "lic-$FUENTE" \
  --cpus "$CPUS" -m "$MEM" --memory-swap "$MEM" --cpu-shares 256 \
  -v "$CODIGO":/repo:ro -v "$DATOS":/datos -v "$H/semillas":/semillas:ro "${MONTAJES[@]}" \
  -e PYTHONPATH=/repo -e TZ=Europe/Madrid -e PYTHONUNBUFFERED=1 -w /datos "$IMAGEN" \
  timeout --kill-after=10m "$LIMITE" ionice -c3 nice -n 19 bash -c "$CMD" >> "$LOG" 2>&1
RC=$?
T1=$(date +%s)

TAM_DESPUES=$(du -sb "$DATOS" | cut -f1)
LIBRE_FIN=$(df -BG --output=avail "$H" | tail -1 | tr -dc 0-9)
{
  echo "------------------------------------------------------------------------"
  echo "fin=$(date -Is) rc=$RC duracion_s=$((T1 - T0)) tam_despues=$TAM_DESPUES libre=${LIBRE_FIN}G"
} >> "$LOG"
printf '{"fuente":"%s","modo":"%s","commit":"%s","inicio":"%s","duracion_s":%d,"rc":%d,"tam_antes":%s,"tam_despues":%s,"libre_gb_fin":%s,"log":"%s"}\n' \
  "$FUENTE" "$MODO" "$COMMIT" "$INICIO" "$((T1 - T0))" "$RC" "$TAM_ANTES" "$TAM_DESPUES" "$LIBRE_FIN" "$LOG" \
  >> "$H/logs/ejecuciones.jsonl"
exit $RC
