# Galicia: listado base (menores y licitaciones) y merge con semilla. El release solo trae un CSV:
# la semilla es el Parquet del LFS (26-feb-2026), idéntico a la copia del VPS. El detalle HTML, más adelante.
LIMITE=24h
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
python /repo/galicia/scraper_galicia.py base --output /datos --log-path /datos/scraper.log --workers 3 --resume
python /repo/galicia/scraper_galicia.py merge --output /datos --log-path /datos/scraper.log --semilla /semillas/vps_20260503/raw_preserved/GALICIA/contratos_galicia.parquet --origen-semilla "LFS 2026-02-26"
CMD
read -r -d '' CMD_SEMANAL <<'CMD'
set -e
python /repo/galicia/scraper_galicia.py base --output /datos --log-path /datos/scraper.log --workers 3
python /repo/galicia/scraper_galicia.py merge --output /datos --log-path /datos/scraper.log --semilla /semillas/vps_20260503/raw_preserved/GALICIA/contratos_galicia.parquet --origen-semilla "LFS 2026-02-26"
CMD
