"""Descarga completa de /contracts de KontratazioA (importes y adjudicatario),
sin el resto de módulos de Euskadi/ccaa_euskadi.py."""
import sys
from pathlib import Path
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'Euskadi'))
import ccaa_euskadi as E  # noqa: E402

t = time.time()
E.setup_dirs()
urls = E._probe_api()
print('endpoints:', urls, flush=True)
E.dl_A_api_completa(urls, recursos=("contracts",))
print(f'terminado en {time.time() - t:.0f} s', flush=True)
