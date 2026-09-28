"""Consolida A1 (/contracts) de Euskadi en el scratchpad (no en Euskadi/euskadi_parquet,
que tiene datos del LFS) y mide los menores por año, con importe y CIF."""
import json
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'Euskadi'))
import consolidacion_euskadi as cons  # noqa: E402

E = Path(os.environ['TRABAJO']) / 'euskadi'
cons.OUTPUT_DIR = E / 'consolidado'
cons.OUTPUT_DIR.mkdir(exist_ok=True)
info = cons.consolidar_A1_api_contratos()
print(json.dumps(info, ensure_ascii=False, indent=1, default=str))
df = pd.read_parquet(cons.OUTPUT_DIR / 'api_contratos.parquet')
df = df[df['_en_ultima_descarga'].astype(bool) & ~df['_duplicado'].astype(bool)]
menor = df['minorContract'].astype('string').str.lower().eq('true')
anio = pd.to_numeric(df['awardDate'].astype('string').str[:4], errors='coerce')
m = pd.DataFrame({'anio': anio[menor], 'cif': df.loc[menor, 'CIF'].notna(),
                  'importe': pd.to_numeric(df.loc[menor, 'awardAmount'], errors='coerce')})
t = m.groupby('anio').agg(menores=('cif', 'size'), con_cif=('cif', 'mean'), importe_meur=('importe', lambda s: s.sum() / 1e6))
print(f"{len(df):,} contratos vigentes; {int(menor.sum()):,} menores")
print(t[(t.index >= 2014) & (t.index <= 2026)].round(3).to_string())
print('Referencia de la API (inventario, minor-contract=true): 2019 69.025 | 2021 66.597 | 2023 92.348 | 2024 84.814 | 2025 83.905')
