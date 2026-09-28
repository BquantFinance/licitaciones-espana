# Asturias: CSV anuales en raw/ con versiones; semilla por (year, Nº INSCRIPCION).
LIMITE=3h
CMD_PRIMERA='python /repo/scripts/ccaa_asturias.py --salida /datos --semilla /semillas/release_v2026.02/extraido/asturias/asturias_contracts_ALL_YEARS.parquet'
CMD_SEMANAL="$CMD_PRIMERA"
