# Andalucía: contratos menores de la Junta y del SAS entero, del CKAN de datos abiertos («Contratación Menor
# en {año}», 2018-). Fuente nueva (no está en el release: sin semilla). Script cerrado según docs/CONTINUACION.md.
# Medido el 2026-09-29: 9 CSV (525 MB) en 6 min; el Parquet, en 30 s con 2,1 GB. Cada semanal vuelve a bajar el
# año en curso y el anterior (unos 290 MB) y los cerrados solo si CKAN los da por cambiados.
LIMITE=2h
CMD_PRIMERA='python /repo/scripts/ccaa_andalucia_menores.py --salida /datos'
CMD_SEMANAL="$CMD_PRIMERA"
