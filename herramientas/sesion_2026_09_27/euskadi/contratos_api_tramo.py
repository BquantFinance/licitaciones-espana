"""Descarga en paralelo un tramo de años de /contracts de KontratazioA.

Uso: python contratos_api_tramo.py <año_desde> <año_hasta>

Llama a _ventana_api del scraper (con su reanudación y su publicación por
ventana) solo para las ventanas mensuales del tramo. Deja sin tocar las que la
ejecución normal refresca siempre (los últimos meses y 'posteriores'): esas, el
resumen _estado.json y la búsqueda de registros sin fecha los hace después la
ejecución normal (contratos_api.py), que ya encuentra completas las de los tramos.
"""
import sys
from pathlib import Path
import time
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'Euskadi'))
import ccaa_euskadi as E  # noqa: E402

desde_anio, hasta_anio = map(int, sys.argv[1:3])
t = time.time()
E.setup_dirs()
cfg = E.API_COMPLETA['contracts']
api_url = E.API_BASE + cfg['ruta']
d = E.DIRS[cfg['dir']]
d.mkdir(parents=True, exist_ok=True)
data, _ = E._get_pagina_api(E._url_api(api_url, cfg, 1, 'DESC'), cfg['nombre'], 1)
total_global = int(data['totalItems'])
ventanas = E._ventanas_api(date.today())
refrescar = {'posteriores'} | {c for c, _, _ in ventanas[-1 - E.API_MESES_REFRESCO:-1]}
propias = [(c, a, b) for c, a, b in ventanas
           if c not in refrescar and c not in ('anteriores',) and desde_anio <= a.year <= hasta_anio]
print(f'tramo {desde_anio}-{hasta_anio}: {len(propias)} ventanas; total API {total_global}', flush=True)
n = 0
for clave, a, b in propias:
    n += len(E._ventana_api(api_url, cfg, d, clave, a, b, total_global, refrescar=False))
print(f'tramo {desde_anio}-{hasta_anio}: {n} ids en {time.time() - t:.0f} s; stats {E.stats}', flush=True)
