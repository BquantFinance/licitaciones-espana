# Andalucía: licitaciones estándar y menores (proxy Elasticsearch del portal). Registro (portalGestor, idExpediente);
# semilla por id_expediente (en los ids de las dos numeraciones, con el nº o el perfil, título e importe).
# Primera descarga (29-sep-2026): 2 h 3 min.
LIMITE=20h
CMD_PRIMERA='python /repo/scripts/ccaa_andalucia.py scrape --salida /datos --semilla /semillas/release_v2026.02/extraido/andalucia/licitaciones_andalucia.parquet'
CMD_SEMANAL="$CMD_PRIMERA"
