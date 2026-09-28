# Contratos menores de la PSCP (contractaciopublica.cat); semilla por (id, expedientId).
LIMITE=20h
CMD_PRIMERA='python /repo/scripts/ccaa_cataluna_contratosmenores.py --output /datos/contractacio_menors.parquet --resume --semilla /semillas/release_v2026.02/extraido/catalunya/contratacion/contractacio_menors.parquet'
CMD_SEMANAL="$CMD_PRIMERA"
