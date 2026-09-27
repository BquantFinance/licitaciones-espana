"""ted/run_ted_crossvalidation.py con otras rutas (el script las tiene fijas).
Uso: python cruce_ted.py <placsp.parquet> <ted_es_can.parquet> <dir_salida>"""
import importlib.util
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
spec = importlib.util.spec_from_file_location('cruce', REPO / 'ted' / 'run_ted_crossvalidation.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
m.OUTPUT_DIR = Path(sys.argv[3])
t = time.time()
df_placsp = m.load_placsp(Path(sys.argv[1]))
df_ted = m.load_ted(Path(sys.argv[2]))
matched_idx, match_data, n_e1, n_e2, n_e2b, e2b_matched_idx, consumidos = m.run_e1_e2(df_placsp, df_ted)
adv = m.run_advanced_matching(df_placsp, df_ted, matched_idx, match_data, consumidos)
df_placsp, df_missing_final, hc = m.apply_results_and_report(
    df_placsp, matched_idx, match_data, n_e1, n_e2, n_e2b, e2b_matched_idx, adv,
    anios_ted=m.anios_cubiertos(df_ted))
m.save_outputs(df_placsp, df_missing_final, hc)
print(f'cruce PLACSP-TED en {time.time() - t:.0f} s')
