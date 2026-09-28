#!/bin/bash
# Primera descarga de cada fuente: cada noche se ejecuta la primera línea de
# cola_primera.txt que tenga alguna fuente pendiente (sus fuentes, una detrás de otra).
# Una fuente queda hecha con cola/<fuente>.ok o fallida con cola/<fuente>.fallo; los
# fallos no paran la cola (las fuentes son independientes) y se revisan a mano.
# Solo se para si falta disco (cola/PARADA). Cuando no queda nada, no hace nada.
set -uo pipefail
VPS=/opt/apps/licitaciones-vps
H=/opt/data/licitaciones-historico
mkdir -p "$H/cola"
if [ -e "$H/cola/PARADA" ]; then echo "$(date -Is) cola parada: $(cat "$H/cola/PARADA")"; exit 0; fi

while read -r linea; do
  linea="${linea%%#*}"
  [ -z "${linea// }" ] && continue
  pendientes=()
  for f in $linea; do
    [ -e "$H/cola/$f.ok" ] || [ -e "$H/cola/$f.fallo" ] || pendientes+=("$f")
  done
  [ ${#pendientes[@]} -eq 0 ] && continue
  echo "$(date -Is) esta noche: ${pendientes[*]}"
  for f in "${pendientes[@]}"; do
    "$VPS/bin/ejecutar_fuente.sh" "$f" primera
    rc=$?
    if [ $rc -eq 0 ]; then date -Is > "$H/cola/$f.ok"; else echo "rc=$rc $(date -Is)" > "$H/cola/$f.fallo"; fi
    echo "$(date -Is) $f rc=$rc"
    if [ $rc -eq 3 ]; then echo "disco insuficiente $(date -Is)" > "$H/cola/PARADA"; exit 1; fi
  done
  exit 0
done < "$VPS/cola_primera.txt"
echo "$(date -Is) cola terminada: no queda ninguna fuente pendiente"
