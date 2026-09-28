# BORME: PDF (inmutables, con versiones), parse privado (con nombres de personas: NO se publica) y
# anonimizado (lo único que puede usar la web o un release). El release llega al 2026-02-18.
LIMITE=24h
read -r -d '' CMD_PRIMERA <<'CMD'
set -e
S=/semillas/release_v2026.02/extraido/borme
python /repo/borme/scripts/borme_scraper.py --start 2026-02-18 --output /datos/pdfs
python /repo/borme/scripts/borme_scraper.py --start 2012-09-07 --end 2012-09-11 --output /datos/pdfs
python /repo/borme/scripts/borme_scraper.py --start 2013-01-02 --end 2013-01-02 --output /datos/pdfs
python /repo/borme/scripts/borme_scraper.py --start 2024-05-09 --end 2024-05-10 --output /datos/pdfs
python /repo/borme/scripts/borme_batch_parser.py --input /datos/pdfs --output /datos/parse --workers 4 --semilla $S/borme_empresas_pub.parquet --semilla $S/borme_cargos_pub.parquet
python /repo/borme/scripts/borme_anonymize.py --input /datos/parse --output /datos/pub
CMD
read -r -d '' CMD_SEMANAL <<'CMD'
set -e
S=/semillas/release_v2026.02/extraido/borme
python /repo/borme/scripts/borme_scraper.py --start "$(date -d '-45 days' +%F)" --output /datos/pdfs
python /repo/borme/scripts/borme_batch_parser.py --input /datos/pdfs --output /datos/parse --workers 4 --semilla $S/borme_empresas_pub.parquet --semilla $S/borme_cargos_pub.parquet
python /repo/borme/scripts/borme_anonymize.py --input /datos/parse --output /datos/pub
CMD
