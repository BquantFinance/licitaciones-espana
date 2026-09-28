# Contratos menores de la PSCP (contractaciopublica.cat); semilla por (id, expedientId).
# --resume solo en la primera: el checkpoint se conserva al terminar (se borra solo con --cleanup), y
# un semanal con --resume daría todas las fases por hechas y no descargaría nada. El semanal empieza
# una descarga nueva; la salida anterior queda en _historico/ y acumular conserva lo retirado.
LIMITE=20h
CMD_PRIMERA='python /repo/scripts/ccaa_cataluna_contratosmenores.py --output /datos/contractacio_menors.parquet --resume --semilla /semillas/release_v2026.02/extraido/catalunya/contratacion/contractacio_menors.parquet'
CMD_SEMANAL='python /repo/scripts/ccaa_cataluna_contratosmenores.py --output /datos/contractacio_menors.parquet --semilla /semillas/release_v2026.02/extraido/catalunya/contratacion/contractacio_menors.parquet'
