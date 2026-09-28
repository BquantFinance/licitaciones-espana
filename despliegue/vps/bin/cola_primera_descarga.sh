#!/bin/bash
# Primera descarga de cada fuente, en el orden de cola_primera.txt.
#
#   uso: cola_primera_descarga.sh [--hasta-vaciar] [--parar-a HH:MM] [--saltar f1,f2]
#
# Sin opciones (cron de las 00:30): ejecuta la primera línea con alguna fuente pendiente (sus fuentes,
# una detrás de otra) y termina. Con --hasta-vaciar sigue con las líneas siguientes hasta acabar la cola;
# con --parar-a no empieza ninguna fuente nueva a partir de esa hora (la que esté en marcha termina);
# con --saltar deja para otra ejecución las fuentes indicadas.
# Estado en /opt/data/licitaciones-historico/cola/:
#   <fuente>.ok      hecha        <fuente>.fallo  falló (se revisa a mano; no para la cola)
#   <fuente>.espera  aparcada: no se ejecuta hasta que se borre (p. ej. pendiente de un arreglo de código)
#   PARADA           cola parada por falta de disco
# Si la fuente no llegó a arrancar (4 = ya en marcha, 5 = cerrojo global ocupado) no se marca: se
# reintenta en la ejecución siguiente. Una sola instancia a la vez (cerrojo cola/.cerrojo_cola).
set -uo pipefail
VPS=/opt/apps/licitaciones-vps
H=/opt/data/licitaciones-historico
HASTA_VACIAR=0; PARAR_A=""; SALTAR=","
while [ $# -gt 0 ]; do
  case "$1" in
    --hasta-vaciar) HASTA_VACIAR=1;;
    --parar-a) PARAR_A="${2:?falta HH:MM}"; shift;;
    --saltar) SALTAR=",${2:?falta la lista},"; shift;;
    *) echo "opción desconocida: $1" >&2; exit 2;;
  esac
  shift
done
mkdir -p "$H/cola"
exec 7>"$H/cola/.cerrojo_cola"
flock -n 7 || { echo "$(date -Is) otra ejecución de la cola sigue en marcha: no se arranca otra"; exit 0; }
if [ -e "$H/cola/PARADA" ]; then echo "$(date -Is) cola parada: $(cat "$H/cola/PARADA")"; exit 0; fi

# Hora límite absoluta: la próxima vez que el reloj marque PARAR_A (hoy o, si ya pasó, mañana)
LIMITE_EPOCH=""
if [ -n "$PARAR_A" ]; then
  LIMITE_EPOCH=$(date -d "today $PARAR_A" +%s) || { echo "hora no válida: $PARAR_A" >&2; exit 2; }
  [ "$LIMITE_EPOCH" -le "$(date +%s)" ] && LIMITE_EPOCH=$(date -d "tomorrow $PARAR_A" +%s)
  echo "$(date -Is) no se empezará ninguna fuente a partir de $(date -d "@$LIMITE_EPOCH" -Is)"
fi
pasada_la_hora() { [ -n "$LIMITE_EPOCH" ] && [ "$(date +%s)" -ge "$LIMITE_EPOCH" ]; }

hizo_algo=0
while read -r linea; do
  linea="${linea%%#*}"
  [ -z "${linea// }" ] && continue
  pendientes=()
  for f in $linea; do
    [ -e "$H/cola/$f.ok" ] || [ -e "$H/cola/$f.fallo" ] || [ -e "$H/cola/$f.espera" ] && continue
    [[ "$SALTAR" == *",$f,"* ]] && continue
    pendientes+=("$f")
  done
  [ ${#pendientes[@]} -eq 0 ] && continue
  echo "$(date -Is) línea: ${pendientes[*]}"
  for f in "${pendientes[@]}"; do
    if pasada_la_hora; then echo "$(date -Is) pasadas las $PARAR_A: no se empieza $f"; exit 0; fi
    "$VPS/bin/ejecutar_fuente.sh" "$f" primera
    rc=$?
    echo "$(date -Is) $f rc=$rc"
    hizo_algo=1
    case $rc in
      0) date -Is > "$H/cola/$f.ok";;
      3) echo "disco insuficiente $(date -Is)" > "$H/cola/PARADA"; exit 1;;
      4|5) echo "  $f no llegó a arrancar: se reintenta en la ejecución siguiente";;
      *) echo "rc=$rc $(date -Is)" > "$H/cola/$f.fallo";;
    esac
  done
  [ "$HASTA_VACIAR" = 1 ] || exit 0
done < "$VPS/cola_primera.txt"
[ "$hizo_algo" = 1 ] && echo "$(date -Is) fin de esta ejecución de la cola" || echo "$(date -Is) nada pendiente en la cola (fuera de lo aparcado o saltado)"
