"""
Diagnostico profundo de MISSING IN TED
========================================
Analiza los 384K contratos SARA que no se encontraron en TED para
separar falsos positivos de missing reales.

Estrategias:
  1. Match por nombre organo + importe (sin NIF)
  2. Match por NIF organo (cae_nationalid) + importe
  3. Deteccion de lotes (mismo organo, misma fecha, multiples adjudicatarios)
  4. Analisis de cobertura TED por organo (tienen OTROS contratos en TED?)
  5. Clasificacion final: falso positivo vs missing real

Uso:
    python diagnostico_missing_ted.py
"""

import pandas as pd
import numpy as np
import time
import re
from pathlib import Path
from collections import defaultdict

# ======================================================================
#  CONFIG
# ======================================================================

# Entradas = salidas de run_ted_crossvalidation.py (ted/crossval_*.parquet) y el
# TED consolidado (ted/ted_es_can.parquet); rutas relativas al repo, no al cwd.
# La salida se mantiene en data/ted para no pisar ted/missing_alta_confianza.parquet
_REPO_DIR = Path(__file__).resolve().parent.parent
MISSING_PATH = _REPO_DIR / "ted" / "crossval_missing.parquet"
SARA_PATH = _REPO_DIR / "ted" / "crossval_sara.parquet"
MATCHED_PATH = _REPO_DIR / "ted" / "crossval_matched.parquet"
TED_PATH = _REPO_DIR / "ted" / "ted_es_can.parquet"
OUTPUT_DIR = _REPO_DIR / "data" / "ted"

MATCH_TOLERANCE_PCT = 0.15   # Mas generoso para fuzzy
MATCH_TOLERANCE_ABS = 10_000


# ======================================================================
#  HELPERS
# ======================================================================

def normalize_name(name):
    """Normaliza nombre de organo/empresa para fuzzy matching."""
    if not name or pd.isna(name):
        return ''
    s = str(name).upper().strip()
    # Quitar acentos basicos
    for a, b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U'),
                 ('Ñ','N'),('Ü','U'),('Ç','C')]:
        s = s.replace(a, b)
    # Quitar puntuacion
    s = re.sub(r'[.,;:\-\/\\()\[\]"\'`]', ' ', s)
    # Quitar formas juridicas
    for pat in ['S.A.', 'SA ', 'S.L.', 'SL ', 'S.A.U.', 'SAU ',
                'S.M.E.', 'SME ', 'M.P.', 'S.COOP', 'S.L.U.']:
        s = s.replace(pat.upper(), ' ')
    # Colapsar espacios
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def extract_keywords(name, min_len=4):
    """Extrae palabras clave de un nombre (>= min_len chars)."""
    norm = normalize_name(name)
    words = norm.split()
    # Filtrar palabras comunes
    stopwords = {'DE', 'DEL', 'LA', 'LAS', 'LOS', 'EL', 'EN', 'POR',
                 'PARA', 'CON', 'SIN', 'UNA', 'UNO', 'QUE', 'MAS',
                 'GENERAL', 'NACIONAL', 'COMUNIDAD', 'AUTONOMA', 'ESTADO',
                 'SECTOR', 'PUBLICO', 'PUBLICA', 'SERVICIO', 'SERVICIOS',
                 'DIRECCION', 'SUBDIREC', 'AREA', 'UNIDAD', 'JEFATURA'}
    return [w for w in words if len(w) >= min_len and w not in stopwords]


# ======================================================================
#  1. CARGAR DATOS
# ======================================================================

print(f"\n{'='*70}")
print(f"  DIAGNOSTICO MISSING IN TED")
print(f"{'='*70}")

t0 = time.time()

# Missing
df_miss = pd.read_parquet(MISSING_PATH)
print(f"\n  Missing cargados: {len(df_miss):,}")

# SARA completo (para contexto)
df_sara = pd.read_parquet(SARA_PATH)
print(f"  SARA total: {len(df_sara):,}")

# Matched (para saber que organos SI tienen presencia TED)
df_matched = pd.read_parquet(MATCHED_PATH)
print(f"  Matched: {len(df_matched):,}")

# TED
df_ted = pd.read_parquet(TED_PATH)
for col in ['importe_ted', 'year', 'number_offers']:
    if col in df_ted.columns:
        df_ted[col] = pd.to_numeric(df_ted[col], errors='coerce')
if 'win_nif_clean' not in df_ted.columns:
    df_ted['win_nif_clean'] = df_ted.get('win_nationalid', pd.Series(dtype=str)) \
        .fillna('').astype(str).str.strip().str.upper()
print(f"  TED total: {len(df_ted):,}")

# Asegurar columnas
for col in ['importe_adjudicacion', 'ano']:
    if col in df_miss.columns:
        df_miss[col] = pd.to_numeric(df_miss[col], errors='coerce')

ted_valid = df_ted[
    (df_ted['importe_ted'].notna()) & (df_ted['importe_ted'] > 0)
].copy()


# ======================================================================
#  2. ANALISIS: Organos con presencia parcial en TED
# ======================================================================

print(f"\n{'='*70}")
print(f"  ANALISIS 1: Presencia de organos en TED")
print(f"{'='*70}")

# Organos que tienen AL MENOS un contrato matched en TED
organs_in_ted = set(df_matched['organo_contratante'].dropna().unique())
print(f"  Organos con al menos 1 match TED: {len(organs_in_ted):,}")

# Cuantos missing son de organos que SI tienen presencia TED
df_miss['_organ_in_ted'] = df_miss['organo_contratante'].isin(organs_in_ted)
n_organ_ted = df_miss['_organ_in_ted'].sum()
n_organ_no_ted = (~df_miss['_organ_in_ted']).sum()

print(f"\n  Missing de organos CON presencia TED:  {n_organ_ted:,} ({n_organ_ted/len(df_miss)*100:.1f}%)")
print(f"  Missing de organos SIN presencia TED:  {n_organ_no_ted:,} ({n_organ_no_ted/len(df_miss)*100:.1f}%)")
print(f"  (Los SIN presencia pueden ser clasificacion erronea de comprador)")


# ======================================================================
#  3. ANALISIS: Match por nombre de organo + importe (sin NIF adjudicatario)
# ======================================================================

print(f"\n{'='*70}")
print(f"  ANALISIS 2: Match por nombre organo + importe")
print(f"{'='*70}")

# Construir indice TED por nombre organo normalizado + ano
ted_by_buyer = defaultdict(list)
for _, row in ted_valid.iterrows():
    buyer_name = normalize_name(row.get('cae_name', ''))
    yr = row.get('year', np.nan)
    if buyer_name and len(buyer_name) > 5 and pd.notna(yr):
        ted_by_buyer[(buyer_name[:40], int(yr))].append({
            'importe': row['importe_ted'],
            'ted_id': str(row.get('ted_notice_id', '')),
            'win_nif': str(row.get('win_nif_clean', '')),
            'consumed': False,
        })

print(f"  Indice TED por buyer name: {len(ted_by_buyer):,} claves")

# Intentar match de missing por nombre organo
n_buyer_match = 0
buyer_match_reasons = defaultdict(int)

sample_size = min(len(df_miss), 200_000)  # Limitar para velocidad
df_miss_sample = df_miss.head(sample_size)

for idx, row in df_miss_sample.iterrows():
    organ = normalize_name(row.get('organo_contratante', ''))
    imp = pd.to_numeric(row.get('importe_adjudicacion', 0), errors='coerce')
    yr = pd.to_numeric(row.get('ano', 0), errors='coerce')
    
    if not organ or pd.isna(imp) or pd.isna(yr) or imp <= 0:
        continue
    
    yr = int(yr)
    tol = max(imp * MATCH_TOLERANCE_PCT, MATCH_TOLERANCE_ABS)
    
    # Buscar por nombre organo truncado
    row_matched = False  # por fila (antes se usaba el contador global y, tras el
                         # primer match, se cortaba en el 1er año con avisos)
    for yr_try in [yr, yr-1, yr+1]:
        key = (organ[:40], yr_try)
        entries = ted_by_buyer.get(key, [])
        for entry in entries:
            if entry['consumed']:
                continue
            diff = abs(entry['importe'] - imp)
            if diff <= tol:
                n_buyer_match += 1
                row_matched = True
                entry['consumed'] = True
                # Razon: NIF diferente?
                nif_placsp = str(row.get('nif_adjudicatario', '')).strip().upper()
                nif_ted = entry['win_nif']
                if nif_placsp and nif_ted and nif_placsp != nif_ted:
                    buyer_match_reasons['nif_diferente'] += 1
                elif not nif_ted or len(nif_ted) < 5:
                    buyer_match_reasons['ted_sin_nif'] += 1
                else:
                    buyer_match_reasons['otro'] += 1
                break
        if row_matched:
            break

pct_buyer = n_buyer_match / max(sample_size, 1) * 100
print(f"\n  Sample analizado: {sample_size:,} missing")
print(f"  Match por buyer name + importe: {n_buyer_match:,} ({pct_buyer:.1f}%)")
print(f"  Razones del no-match original:")
for reason, cnt in sorted(buyer_match_reasons.items(), key=lambda x: -x[1]):
    print(f"    {reason:<25}: {cnt:,}")

estimated_buyer_total = int(n_buyer_match / sample_size * len(df_miss))
print(f"\n  Estimacion total recuperable por buyer name: ~{estimated_buyer_total:,}")


# ======================================================================
#  4. ANALISIS: Deteccion de lotes
# ======================================================================

print(f"\n{'='*70}")
print(f"  ANALISIS 3: Deteccion de posibles lotes")
print(f"{'='*70}")

# Lotes: mismo organo + mismo ano + importes similares = probablemente un solo
# contrato en TED con multiples lotes
# Esto genera N registros en PLACSP pero solo 1 en TED

lot_groups = df_miss.groupby(
    ['organo_contratante', 'ano']
).agg(
    n_contratos=('expediente', 'count'),
    n_expedientes=('expediente', 'nunique'),
    imp_total=('importe_adjudicacion', 'sum'),
    imp_mean=('importe_adjudicacion', 'mean'),
).reset_index()

# Grupos con muchos contratos del mismo organo/ano = probable lotes
multi_lot = lot_groups[lot_groups['n_contratos'] >= 5]
n_records_in_lots = df_miss.merge(
    multi_lot[['organo_contratante', 'ano']], 
    on=['organo_contratante', 'ano'],
    how='inner'
).shape[0]

print(f"  Organo+ano con >=5 contratos missing: {len(multi_lot):,} grupos")
print(f"  Registros en esos grupos: {n_records_in_lots:,} ({n_records_in_lots/len(df_miss)*100:.1f}%)")
print(f"  (Muchos de estos pueden ser 1 contrato TED con N lotes)")


# ======================================================================
#  5. ANALISIS: Cobertura temporal
# ======================================================================

print(f"\n{'='*70}")
print(f"  ANALISIS 4: Cobertura temporal TED vs PLACSP")
print(f"{'='*70}")

# Para cada ano, que % de SARA esta en TED?
print(f"\n  {'Ano':>6} {'SARA':>8} {'Match':>8} {'Miss':>8} {'%Miss':>7} {'TED_ES':>8} {'Ratio':>7}")
print(f"  {'-'*58}")

for yr in sorted(df_sara['ano'].dropna().unique()):
    yr = int(yr)
    if yr < 2010 or yr > 2026:
        continue
    n_sara_yr = len(df_sara[df_sara['ano'] == yr])
    n_match_yr = len(df_matched[df_matched['ano'] == yr])
    n_miss_yr = len(df_miss[df_miss['ano'] == yr])
    n_ted_yr = len(df_ted[df_ted['year'] == yr])
    pct_miss = n_miss_yr / max(n_sara_yr, 1) * 100
    ratio = n_ted_yr / max(n_sara_yr, 1)
    print(f"  {yr:>6} {n_sara_yr:>8,} {n_match_yr:>8,} {n_miss_yr:>8,} "
          f"{pct_miss:>6.1f}% {n_ted_yr:>8,} {ratio:>6.2f}x")


# ======================================================================
#  6. ANALISIS: NIF organo vs TED buyer-identifier
# ======================================================================

print(f"\n{'='*70}")
print(f"  ANALISIS 5: Match por NIF organo contratante")
print(f"{'='*70}")

# Construir indice TED por buyer NIF + ano
ted_by_cae_nif = defaultdict(list)
for _, row in ted_valid.iterrows():
    cae_nif = str(row.get('cae_nationalid', '')).strip().upper()
    yr = row.get('year', np.nan)
    if cae_nif and len(cae_nif) >= 5 and pd.notna(yr):
        ted_by_cae_nif[(cae_nif, int(yr))].append({
            'importe': row['importe_ted'],
            'consumed': False,
        })

print(f"  Indice TED por buyer NIF: {len(ted_by_cae_nif):,} claves")

# Match missing por NIF organo
n_cae_match = 0
sample2 = df_miss.head(sample_size)

for idx, row in sample2.iterrows():
    nif_org = str(row.get('nif_organo', '')).strip().upper()
    imp = pd.to_numeric(row.get('importe_adjudicacion', 0), errors='coerce')
    yr = pd.to_numeric(row.get('ano', 0), errors='coerce')
    
    if not nif_org or len(nif_org) < 5 or pd.isna(imp) or pd.isna(yr):
        continue
    
    yr = int(yr)
    tol = max(imp * MATCH_TOLERANCE_PCT, MATCH_TOLERANCE_ABS)
    
    row_matched = False
    for yr_try in [yr, yr-1, yr+1]:
        key = (nif_org, yr_try)
        entries = ted_by_cae_nif.get(key, [])
        for entry in entries:
            if entry['consumed']:
                continue
            diff = abs(entry['importe'] - imp)
            if diff <= tol:
                n_cae_match += 1
                row_matched = True
                entry['consumed'] = True
                break
        if row_matched:
            break

pct_cae = n_cae_match / max(sample_size, 1) * 100
est_cae = int(n_cae_match / sample_size * len(df_miss))
print(f"\n  Match por NIF organo + importe: {n_cae_match:,} ({pct_cae:.1f}%)")
print(f"  Estimacion total: ~{est_cae:,}")


# ======================================================================
#  7. CLASIFICACION FINAL
# ======================================================================

print(f"\n{'='*70}")
print(f"  CLASIFICACION FINAL DE MISSING")
print(f"{'='*70}")

total_miss = len(df_miss)

# Categoria 1: Organos SIN ninguna presencia TED (probable error clasificacion)
cat1 = (~df_miss['_organ_in_ted']).sum()

# Categoria 2: Anos con cobertura TED < 5% (2010-2015, datos incompletos)
early_years = df_miss[df_miss['ano'].isin(range(2010, 2016))].shape[0]
cat2 = early_years

# Categoria 3: Zona gris importes (AGE tiene 140K, resto 215K)
# Muchos missing entre 140-221K pueden ser CCAA/EELL bien clasificados
zona_gris = df_miss[
    (df_miss['importe_adjudicacion'] >= 139_000) & 
    (df_miss['importe_adjudicacion'] < 221_000)
].shape[0]

# Categoria 4: Estimados recuperables por buyer name match
cat4 = estimated_buyer_total

# Categoria 5: Residual = probables missing reales
# No sumar categorias (se solapan), estimar por exclusion
missing_alta_confianza = df_miss[
    df_miss['_organ_in_ted'] &                              # Organo SI tiene presencia TED
    (df_miss['ano'] >= 2016) &                              # Desde que hay buena cobertura
    (df_miss['importe_adjudicacion'] >= 221_000)            # Sobre umbral incluso para resto
].shape[0]

print(f"\n  Total missing: {total_miss:,}")
print(f"")
print(f"  PROBABLES FALSOS POSITIVOS:")
print(f"    Organo sin presencia TED (clasif. erronea?):  {cat1:>8,} ({cat1/total_miss*100:.1f}%)")
print(f"    Anos 2010-2015 (datos TED incompletos):       {cat2:>8,} ({cat2/total_miss*100:.1f}%)")
print(f"    Zona gris 139-221K (umbral AGE vs resto):     {zona_gris:>8,} ({zona_gris/total_miss*100:.1f}%)")
print(f"    Recuperables por buyer name (NIF diferente):  ~{cat4:>8,} ({cat4/total_miss*100:.1f}%)")
print(f"")
print(f"  MISSING ALTA CONFIANZA:")
print(f"    Organo con presencia TED + >=2016 + >=221K:   {missing_alta_confianza:>8,} ({missing_alta_confianza/total_miss*100:.1f}%)")

# Guardar missing alta confianza
missing_hc = df_miss[
    df_miss['_organ_in_ted'] &
    (df_miss['ano'] >= 2016) &
    (df_miss['importe_adjudicacion'] >= 221_000)
]

if len(missing_hc) > 0:
    # Top organos missing alta confianza
    print(f"\n  Top 15 organos - MISSING ALTA CONFIANZA:")
    top_hc = missing_hc.groupby('organo_contratante').agg(
        n=('expediente', 'count'),
        imp_total=('importe_adjudicacion', 'sum'),
    ).sort_values('n', ascending=False).head(15)
    
    for organ, r in top_hc.iterrows():
        print(f"    {str(organ)[:55]:<55}: {int(r['n']):>5,} | {r['imp_total']/1e6:.0f}M")
    
    # Por ano
    print(f"\n  Missing alta confianza por ano:")
    for yr, cnt in missing_hc['ano'].value_counts().sort_index().items():
        yr_sara = len(df_sara[df_sara['ano'] == yr])
        pct = cnt / max(yr_sara, 1) * 100
        print(f"    {int(yr)}: {cnt:>6,} ({pct:.0f}% de SARA)")
    
    # Guardar
    save_cols = [c for c in missing_hc.columns if not c.startswith('_')]
    save_cols += ['_es_sara', '_ted_missing', '_is_age', '_is_sector']
    save_cols = [c for c in save_cols if c in missing_hc.columns]
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    missing_hc[save_cols].to_parquet(OUTPUT_DIR / "missing_alta_confianza.parquet", index=False)
    print(f"\n  Guardado: {OUTPUT_DIR / 'missing_alta_confianza.parquet'} ({len(missing_hc):,})")

elapsed = time.time() - t0
print(f"\n  Tiempo total: {elapsed:.0f}s")
print(f"  Diagnostico completado.")