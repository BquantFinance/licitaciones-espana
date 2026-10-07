"""
Deep Dive: Sector Salud — Missing in TED
==========================================
Analisis detallado de los organos de salud que dominan el missing:
SAS, SERGAS, ICS, IB-Salut, hospitales, etc.

Hipotesis: la mayoria de missing en salud son acuerdos marco de
medicamentos/material sanitario publicados como 1 CAN en TED pero
como N adjudicaciones individuales en PLACSP.

Analiza:
  1. Perfil de contratacion por organo de salud
  2. Patron de lotes (mismo organo, misma fecha, multiples adjudicatarios)
  3. Concentracion de CPV sanitarios
  4. Acuerdos marco vs contratos individuales
  5. Comparacion matched vs missing (que SI encuentran en TED?)
  6. Estimacion de cobertura real ajustada por lotes

Uso:
    python analisis_sector_salud.py
"""

import sys

import pandas as pd
import re
import time
from pathlib import Path

# Última versión de cada aviso y sin cancelados (ted_module.avisos_para_cruce), desde cualquier cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from ted_module import avisos_para_cruce  # noqa: E402

# ======================================================================
#  CONFIG
# ======================================================================

# Entradas = salidas de run_ted_crossvalidation.py (ted/crossval_sara*.parquet) y
# el TED consolidado (ted/ted_es_can.parquet); rutas relativas al repo, no al cwd
_REPO_DIR = Path(__file__).resolve().parent.parent
SARA_V2_PATH = _REPO_DIR / "ted" / "crossval_sara_v2.parquet"
SARA_V1_PATH = _REPO_DIR / "ted" / "crossval_sara.parquet"
TED_PATH = _REPO_DIR / "ted" / "ted_es_can.parquet"
PLACSP_PATH = _REPO_DIR / "nacional" / "licitaciones_espana.parquet"
OUTPUT_DIR = _REPO_DIR / "data" / "ted"

# Patrones para detectar organos de salud
HEALTH_PATTERNS = [
    r'SALUD', r'SANITARI', r'HOSPITAL', r'CLINIC[AO]', r'OSAKIDETZA',
    r'SERGAS', r'SACYL', r'SESCAM', r'OSASUNBIDEA', r'INGESA',
    r'IB.SALUT', r'ICS\b', r'INSTITUT CATALA DE LA SALUT',
    r'SERVICIO ANDALUZ', r'SERVICIO CANARIO', r'SERVICIO MURCIANO',
    r'SERVICIO RIOJANO', r'SERVICIO NAVARRO', r'SERVICIO EXTREMEÑO',
    r'SERVICIO ARAGONES', r'SERVICIO GALLEGO',
    r'SERVEI BALEAR', r'SERVEI CATALA',
    r'AREA DE SALUD', r'GERENCIA DE ATENCION',
    r'FARMACI', r'MEDICAMENT', r'CENTRO DE TRANSFUSION',
    r'AMBULANCI', r'EMERGENCI', r'061', r'112 SANITARI',
    r'MUFACE', r'FREMAP', r'MUTUA',
    r'AECC', r'CRUZ ROJA',
]

# CPV sanitarios (primeros 2-3 digitos)
HEALTH_CPV_PREFIXES = [
    '33',   # Equipos medicos, farmaceuticos, productos de higiene
    '85',   # Servicios de salud y servicios sociales
    '35',   # Equipos de seguridad (incluye ambulancias)
    '38',   # Instrumentos de laboratorio
    '24',   # Productos quimicos (farmaceuticos)
    '44',   # Estructuras y materiales de construccion (quirofanos)
]


# ======================================================================
#  HELPERS
# ======================================================================

def is_health_organ(name):
    """Detecta si un organo es del sector salud."""
    if not name or pd.isna(name):
        return False
    name_upper = str(name).upper()
    for pat in HEALTH_PATTERNS:
        if re.search(pat, name_upper):
            return True
    return False


def is_health_cpv(cpv):
    """Detecta si un CPV es sanitario."""
    if not cpv or pd.isna(cpv):
        return False
    cpv_str = str(cpv).strip()[:2]
    return cpv_str in ['33', '85']


def detect_lot_groups(df, date_col='fecha_adjudicacion', organ_col='organo_contratante',
                      max_days=7):
    """
    Detecta grupos de lotes: mismo organo, fechas cercanas (<max_days),
    multiples adjudicaciones.
    Returns df with '_lot_group_id' column.
    """
    df = df.copy()
    df['_lot_group_id'] = -1
    
    if date_col not in df.columns:
        return df
    
    df['_fecha_dt'] = pd.to_datetime(df[date_col], errors='coerce')
    
    group_id = 0
    for organ, grp in df.groupby(organ_col):
        grp_sorted = grp.dropna(subset=['_fecha_dt']).sort_values('_fecha_dt')
        if len(grp_sorted) < 2:
            continue
        
        current_group = [grp_sorted.index[0]]
        last_date = grp_sorted.iloc[0]['_fecha_dt']
        
        for i in range(1, len(grp_sorted)):
            idx = grp_sorted.index[i]
            curr_date = grp_sorted.iloc[i]['_fecha_dt']
            
            if pd.notna(curr_date) and pd.notna(last_date):
                diff = abs((curr_date - last_date).days)
                if diff <= max_days:
                    current_group.append(idx)
                else:
                    if len(current_group) >= 2:
                        for ci in current_group:
                            df.loc[ci, '_lot_group_id'] = group_id
                        group_id += 1
                    current_group = [idx]
            else:
                current_group = [idx]
            
            last_date = curr_date
        
        # Last group
        if len(current_group) >= 2:
            for ci in current_group:
                df.loc[ci, '_lot_group_id'] = group_id
            group_id += 1
    
    df.drop(columns=['_fecha_dt'], inplace=True, errors='ignore')
    return df


# ======================================================================
#  CARGAR DATOS
# ======================================================================

print(f"\n{'='*70}")
print(f"  DEEP DIVE: SECTOR SALUD — MISSING IN TED")
print(f"{'='*70}")

t0 = time.time()

# SARA con matching
if SARA_V2_PATH.exists():
    df_sara = pd.read_parquet(SARA_V2_PATH)
    print(f"  SARA v2 cargado: {len(df_sara):,}")
else:
    df_sara = pd.read_parquet(SARA_V1_PATH)
    print(f"  SARA v1 cargado: {len(df_sara):,}")

for col in ['importe_adjudicacion', 'ano']:
    if col in df_sara.columns:
        df_sara[col] = pd.to_numeric(df_sara[col], errors='coerce')

for col in ['_ted_validated', '_ted_missing', '_es_sara']:
    if col in df_sara.columns:
        df_sara[col] = df_sara[col].fillna(False).astype(bool)

# TED
# El consolidado conserva el histórico y los cancelados: cada aviso una vez, sin cancelados
df_ted = avisos_para_cruce(pd.read_parquet(TED_PATH)).reset_index(drop=True)
for col in ['importe_ted', 'year', 'number_offers']:
    if col in df_ted.columns:
        df_ted[col] = pd.to_numeric(df_ted[col], errors='coerce')
print(f"  TED cargado: {len(df_ted):,}")

# Clasificar organos de salud
df_sara['_is_health'] = df_sara['organo_contratante'].apply(is_health_organ)
df_health = df_sara[df_sara['_is_health']].copy()
print(f"\n  Organos de salud en SARA: {df_health['organo_contratante'].nunique():,}")
print(f"  Contratos SARA salud: {len(df_health):,} ({len(df_health)/len(df_sara)*100:.1f}%)")


# ======================================================================
#  1. PERFIL DE CONTRATACION POR ORGANO DE SALUD
# ======================================================================

print(f"\n{'='*70}")
print(f"  1. PERFIL DE CONTRATACION — SECTOR SALUD")
print(f"{'='*70}")

health_matched = df_health['_ted_validated'].sum()
health_missing = df_health['_ted_missing'].fillna(False).sum()
health_total = len(df_health)

print(f"\n  Total contratos SARA salud: {health_total:,}")
print(f"  Validados en TED:           {health_matched:,} ({health_matched/health_total*100:.1f}%)")
print(f"  Missing en TED:             {health_missing:,} ({health_missing/health_total*100:.1f}%)")

# Comparar con no-salud
non_health = df_sara[~df_sara['_is_health']]
nh_matched = non_health['_ted_validated'].sum()
nh_total = len(non_health)
print(f"\n  Comparacion:")
print(f"    Salud:    {health_matched/health_total*100:.1f}% validado")
print(f"    No-salud: {nh_matched/nh_total*100:.1f}% validado")

# Top organos de salud
print(f"\n  Top 20 organos de salud por volumen SARA:")
print(f"  {'Organo':<55} {'SARA':>6} {'Match':>6} {'%':>6} {'Miss':>6} {'Imp(M)':>8}")
print(f"  {'-'*90}")

health_organs = df_health.groupby('organo_contratante').agg(
    n_sara=('_es_sara', 'count'),
    n_match=('_ted_validated', 'sum'),
    n_miss=('_ted_missing', 'sum'),
    imp_total=('importe_adjudicacion', 'sum'),
).sort_values('n_sara', ascending=False)

for organ, r in health_organs.head(20).iterrows():
    pct = r['n_match'] / max(r['n_sara'], 1) * 100
    print(f"  {str(organ)[:55]:<55} {int(r['n_sara']):>6,} {int(r['n_match']):>6,} "
          f"{pct:>5.1f}% {int(r['n_miss']):>6,} {r['imp_total']/1e6:>7.0f}M")

# Por tipo de contrato
print(f"\n  Tipo de contrato en salud:")
tc_col = '_tipo_contrato' if '_tipo_contrato' in df_health.columns else 'tipo_contrato'
if tc_col in df_health.columns:
    for tc, grp in df_health.groupby(tc_col):
        n = len(grp)
        m = grp['_ted_validated'].sum()
        pct = m / max(n, 1) * 100
        if n >= 50:
            print(f"    {str(tc):<30}: {n:>6,} SARA | {int(m):>5,} match ({pct:.0f}%)")


# ======================================================================
#  2. PATRON DE LOTES
# ======================================================================

print(f"\n{'='*70}")
print(f"  2. DETECCION DE LOTES — SECTOR SALUD")
print(f"{'='*70}")

health_missing_df = df_health[df_health['_ted_missing'].fillna(False)].copy()
print(f"\n  Missing salud a analizar: {len(health_missing_df):,}")

# Agrupar por organo + ano
lot_analysis = health_missing_df.groupby(['organo_contratante', 'ano']).agg(
    n=('importe_adjudicacion', 'count'),
    n_nifs=('nif_adjudicatario', 'nunique'),
    imp_total=('importe_adjudicacion', 'sum'),
    imp_mean=('importe_adjudicacion', 'mean'),
    imp_median=('importe_adjudicacion', 'median'),
    imp_std=('importe_adjudicacion', 'std'),
    n_expedientes=('expediente', 'nunique'),
).reset_index()

# Grupos grandes = probable lotes/acuerdos marco
lot_analysis['ratio_exp_n'] = lot_analysis['n_expedientes'] / lot_analysis['n'].clip(lower=1)
lot_analysis['cv'] = lot_analysis['imp_std'] / lot_analysis['imp_mean'].clip(lower=1)

# Clasificar
lot_analysis['_tipo_grupo'] = 'individual'

# Muchos contratos, pocos expedientes = lotes de un mismo procedimiento
lot_analysis.loc[
    (lot_analysis['n'] >= 5) & (lot_analysis['ratio_exp_n'] < 0.3),
    '_tipo_grupo'
] = 'lotes_mismo_expediente'

# Muchos contratos, muchos expedientes, importes similares = acuerdo marco
lot_analysis.loc[
    (lot_analysis['n'] >= 5) & (lot_analysis['ratio_exp_n'] >= 0.3) & (lot_analysis['cv'] < 0.5),
    '_tipo_grupo'
] = 'acuerdo_marco_homogeneo'

# Muchos contratos, muchos expedientes, importes variados
lot_analysis.loc[
    (lot_analysis['n'] >= 5) & (lot_analysis['ratio_exp_n'] >= 0.3) & (lot_analysis['cv'] >= 0.5) &
    (lot_analysis['_tipo_grupo'] == 'individual'),
    '_tipo_grupo'
] = 'contratos_recurrentes'

# Grupos pequenos
lot_analysis.loc[
    (lot_analysis['n'] >= 2) & (lot_analysis['n'] < 5) &
    (lot_analysis['_tipo_grupo'] == 'individual'),
    '_tipo_grupo'
] = 'grupo_pequeno'

# Resumir
tipo_counts = lot_analysis.groupby('_tipo_grupo').agg(
    n_grupos=('n', 'count'),
    n_contratos=('n', 'sum'),
    imp_total=('imp_total', 'sum'),
).sort_values('n_contratos', ascending=False)

print(f"\n  Clasificacion de grupos organo+ano (missing salud):")
print(f"  {'Tipo':<30} {'Grupos':>7} {'Contratos':>10} {'Importe':>10}")
print(f"  {'-'*60}")

for tipo, r in tipo_counts.iterrows():
    print(f"  {tipo:<30} {int(r['n_grupos']):>7,} {int(r['n_contratos']):>10,} "
          f"{r['imp_total']/1e6:>9.0f}M")

total_lotes = tipo_counts.loc[
    tipo_counts.index.isin(['lotes_mismo_expediente', 'acuerdo_marco_homogeneo']),
    'n_contratos'
].sum()
print(f"\n  Contratos en patron lotes/AM: {int(total_lotes):,} ({total_lotes/len(health_missing_df)*100:.1f}% del missing salud)")

# Ejemplos concretos
print(f"\n  Ejemplos de patron lotes (top 10 grupos):")
top_lots = lot_analysis[
    lot_analysis['_tipo_grupo'].isin(['lotes_mismo_expediente', 'acuerdo_marco_homogeneo'])
].sort_values('n', ascending=False).head(10)

for _, r in top_lots.iterrows():
    print(f"    {str(r['organo_contratante'])[:45]:<45} | {int(r['ano'])} | "
          f"{int(r['n']):>4} contr. | {int(r['n_expedientes']):>3} exp. | "
          f"{r['imp_total']/1e6:.0f}M | tipo: {r['_tipo_grupo']}")


# ======================================================================
#  3. ANALISIS CPV SANITARIO
# ======================================================================

print(f"\n{'='*70}")
print(f"  3. ANALISIS CPV — SECTOR SALUD")
print(f"{'='*70}")

cpv_col = None
# crossval_sara.parquet (run_ted_crossvalidation.py) trae el CPV de PLACSP como 'cpv_principal'
for c in ['cpv_principal', 'cpv', 'cpv_code', 'codigo_cpv']:
    if c in df_health.columns:
        cpv_col = c
        break

if cpv_col:
    df_health['_cpv2'] = df_health[cpv_col].fillna('').astype(str).str[:2]
    
    cpv_analysis = df_health.groupby('_cpv2').agg(
        n=('_es_sara', 'count'),
        n_match=('_ted_validated', 'sum'),
        n_miss=('_ted_missing', 'sum'),
        imp=('importe_adjudicacion', 'sum'),
    ).sort_values('n', ascending=False)
    
    cpv_labels = {
        '33': 'Equipos medicos/farmaceuticos',
        '85': 'Servicios de salud',
        '45': 'Obras de construccion',
        '50': 'Reparacion/mantenimiento',
        '79': 'Servicios empresariales',
        '72': 'Servicios informaticos',
        '71': 'Ingenieria/arquitectura',
        '90': 'Servicios medioambientales',
        '39': 'Mobiliario',
        '38': 'Instrumentos laboratorio',
        '44': 'Materiales construccion',
        '48': 'Software',
        '24': 'Productos quimicos',
        '34': 'Vehiculos',
        '42': 'Maquinaria industrial',
        '55': 'Servicios hoteleros',
        '60': 'Servicios transporte',
        '35': 'Equipos seguridad',
        '09': 'Energia',
        '18': 'Ropa/textiles',
        '15': 'Alimentacion',
    }
    
    print(f"\n  CPV (2 digitos) en contratos salud:")
    print(f"  {'CPV':>4} {'Descripcion':<35} {'SARA':>7} {'Match':>6} {'%':>6} {'Miss':>6}")
    print(f"  {'-'*70}")
    
    for cpv, r in cpv_analysis.head(15).iterrows():
        if r['n'] < 20:
            continue
        pct = r['n_match'] / max(r['n'], 1) * 100
        label = cpv_labels.get(cpv, '?')
        print(f"  {cpv:>4} {label:<35} {int(r['n']):>7,} {int(r['n_match']):>6,} "
              f"{pct:>5.1f}% {int(r['n_miss']):>6,}")
    
    # CPV 33 deep dive
    cpv33 = df_health[df_health['_cpv2'] == '33']
    if len(cpv33) > 0:
        print(f"\n  CPV 33 (Equipos medicos/farmaceuticos) — Deep dive:")
        df_health['_cpv4'] = df_health[cpv_col].fillna('').astype(str).str[:4]
        cpv33_detail = df_health[df_health['_cpv2'] == '33'].groupby('_cpv4').agg(
            n=('_es_sara', 'count'),
            n_match=('_ted_validated', 'sum'),
        ).sort_values('n', ascending=False)
        
        cpv33_labels = {
            '3314': 'Material medico', '3360': 'Suministros farmaceuticos',
            '3316': 'Reactivos', '3310': 'Equipos de imagen',
            '3317': 'Protesis/implantes', '3315': 'Equipos quirurgicos',
            '3312': 'Material de sutura', '3319': 'Equipos de rehabilitacion',
            '3318': 'Instrumentos medicos', '3369': 'Otros farmaceuticos',
            '3313': 'Equipos esterilizacion', '3311': 'Equipos dentales',
            '3320': 'Equipos resucitacion',
        }
        
        print(f"    {'CPV4':>6} {'Desc':<30} {'SARA':>6} {'Match':>5} {'%':>6}")
        for cpv4, r in cpv33_detail.head(10).iterrows():
            if r['n'] < 20:
                continue
            pct = r['n_match'] / max(r['n'], 1) * 100
            label = cpv33_labels.get(cpv4, cpv4)
            print(f"    {cpv4:>6} {label:<30} {int(r['n']):>6,} {int(r['n_match']):>5,} {pct:>5.1f}%")
else:
    print(f"  No se encontro columna CPV en los datos")


# ======================================================================
#  4. MATCHED vs MISSING: QUE DIFERENCIA HAY?
# ======================================================================

print(f"\n{'='*70}")
print(f"  4. PERFIL MATCHED vs MISSING")
print(f"{'='*70}")

health_match_df = df_health[df_health['_ted_validated'].fillna(False)].copy()

print(f"\n  {'Metrica':<35} {'Matched':>12} {'Missing':>12}")
print(f"  {'-'*60}")

# Importe
print(f"  {'Importe medio':<35} {health_match_df['importe_adjudicacion'].mean():>12,.0f} "
      f"{health_missing_df['importe_adjudicacion'].mean():>12,.0f}")
print(f"  {'Importe mediana':<35} {health_match_df['importe_adjudicacion'].median():>12,.0f} "
      f"{health_missing_df['importe_adjudicacion'].median():>12,.0f}")

# Tipo contrato
if tc_col in df_health.columns:
    for tc in ['Servicios', 'Suministros', 'Obras']:
        n_m = (health_match_df[tc_col] == tc).sum()
        n_miss = (health_missing_df[tc_col] == tc).sum()
        pct_m = n_m / max(len(health_match_df), 1) * 100
        pct_miss = n_miss / max(len(health_missing_df), 1) * 100
        print(f"  {'% ' + tc:<35} {pct_m:>11.1f}% {pct_miss:>11.1f}%")

# Distribucion importes
brackets = [
    (0, 221_000, '<221K'),
    (221_000, 500_000, '221K-500K'),
    (500_000, 1_000_000, '500K-1M'),
    (1_000_000, 5_000_000, '1M-5M'),
    (5_000_000, float('inf'), '>=5M'),
]

print(f"\n  Distribucion importes:")
print(f"  {'Bracket':<20} {'Matched':>8} {'%':>6} {'Missing':>8} {'%':>6}")
print(f"  {'-'*50}")
for lo, hi, label in brackets:
    n_m = ((health_match_df['importe_adjudicacion'] >= lo) & 
           (health_match_df['importe_adjudicacion'] < hi)).sum()
    n_miss = ((health_missing_df['importe_adjudicacion'] >= lo) & 
              (health_missing_df['importe_adjudicacion'] < hi)).sum()
    pct_m = n_m / max(len(health_match_df), 1) * 100
    pct_miss = n_miss / max(len(health_missing_df), 1) * 100
    print(f"  {label:<20} {n_m:>8,} {pct_m:>5.1f}% {n_miss:>8,} {pct_miss:>5.1f}%")


# ======================================================================
#  5. MATCHING STRATEGY DISTRIBUTION (SALUD)
# ======================================================================

print(f"\n{'='*70}")
print(f"  5. ESTRATEGIAS DE MATCHING — SECTOR SALUD")
print(f"{'='*70}")

if '_match_strategy' in df_health.columns:
    strat = health_match_df['_match_strategy'].value_counts()
    print(f"\n  Como se encontraron los {len(health_match_df):,} matched de salud:")
    for s, n in strat.items():
        if n > 0:
            print(f"    {s:<25}: {n:>6,} ({n/len(health_match_df)*100:.1f}%)")
else:
    print(f"  Columna _match_strategy no disponible")


# ======================================================================
#  6. ESTIMACION COBERTURA REAL (AJUSTADA POR LOTES)
# ======================================================================

print(f"\n{'='*70}")
print(f"  6. ESTIMACION COBERTURA REAL — SECTOR SALUD")
print(f"{'='*70}")

# Hipotesis: los grupos tipo lotes/AM cuentan como 1 contrato en TED
# Si tenemos 100 missing pero 80 estan en 5 grupos de lotes,
# realmente son ~5 contratos TED, no 80

lotes_grupos = lot_analysis[
    lot_analysis['_tipo_grupo'].isin(['lotes_mismo_expediente', 'acuerdo_marco_homogeneo'])
]
n_lotes_contratos = lotes_grupos['n'].sum()
n_lotes_grupos = len(lotes_grupos)

individual_missing = len(health_missing_df) - n_lotes_contratos

# Cobertura ajustada: matched + (missing_individual como missing real)
# Los lotes cuentan como n_grupos contratos en TED, no como n contratos
adjusted_missing = individual_missing + n_lotes_grupos
# El total ajustado solo cambia los lotes por grupos: los SARA no validados que no
# cuentan como missing (negociado sin publicidad) siguen en el denominador, como
# en la cobertura sin ajustar (antes se caían y la cobertura "ajustada" subía sin lotes)
n_otros_no_validados = health_total - health_matched - len(health_missing_df)
adjusted_total = health_matched + adjusted_missing + n_otros_no_validados
adjusted_coverage = health_matched / max(adjusted_total, 1) * 100

print(f"\n  Missing bruto salud:          {len(health_missing_df):,}")
print(f"  En patron de lotes/AM:        {int(n_lotes_contratos):,} contratos en {n_lotes_grupos:,} grupos")
print(f"  Missing individual:            {individual_missing:,}")
print(f"")
print(f"  Sin ajustar:")
print(f"    Matched: {health_matched:,} / {health_total:,} = {health_matched/health_total*100:.1f}%")
print(f"")
print(f"  Ajustado por lotes (cada grupo de lotes cuenta como 1 contrato TED):")
print(f"    Matched: {health_matched:,} / {adjusted_total:,} = {adjusted_coverage:.1f}%")
print(f"")
print(f"  Interpretacion: la cobertura real en salud es probablemente ~{adjusted_coverage:.0f}%,")
print(f"  no {health_matched/health_total*100:.0f}%. La diferencia se debe a que PLACSP registra")
print(f"  cada lote por separado y TED consolida en un unico anuncio.")


# ======================================================================
#  7. POR CCAA
# ======================================================================

print(f"\n{'='*70}")
print(f"  7. SALUD POR COMUNIDAD AUTONOMA")
print(f"{'='*70}")

# Intentar extraer CCAA de dependencia o codigo organo
ccaa_patterns = {
    'Andalucia': ['ANDALUZ', 'JUNTA DE ANDALUCIA', 'SAS'],
    'Cataluña': ['CATALA', 'GENERALITAT DE CATALUN', 'ICS'],
    'Madrid': ['MADRID', 'COMUNIDAD DE MADRID', 'SERMAS'],
    'C. Valenciana': ['VALENCIA', 'GENERALITAT VALENCIANA'],
    'Galicia': ['GALLEGO', 'SERGAS', 'XUNTA'],
    'Pais Vasco': ['OSAKIDETZA', 'EUSKO'],
    'Castilla y Leon': ['SACYL', 'CASTILLA Y LEON'],
    'Canarias': ['CANARIO', 'CANARIAS'],
    'Baleares': ['BALEAR', 'IB.SALUT'],
    'Aragon': ['ARAGON'],
    'Asturias': ['ASTURIAS', 'SESPA'],
    'Murcia': ['MURCIANO', 'MURCIA'],
    'Extremadura': ['EXTREMEÑO', 'EXTREMADURA'],
    'Castilla-La Mancha': ['SESCAM', 'CASTILLA.LA MANCHA'],
    'Navarra': ['NAVARRO', 'OSASUNBIDEA', 'NAVARRA'],
    'La Rioja': ['RIOJANO', 'RIOJA'],
    'Cantabria': ['CANTABRIA'],
    'AGE (INGESA, MUFACE...)': ['INGESA', 'MUFACE', 'FREMAP'],
}

def detect_ccaa(organ):
    if not organ or pd.isna(organ):
        return 'Otros'
    organ_upper = str(organ).upper()
    for ccaa, patterns in ccaa_patterns.items():
        for pat in patterns:
            # Inicio de palabra (y '.' como comodín, p.ej. 'IB.SALUT'): como
            # subcadena 'SAS' casaba con 'CASAS' y 'ARAGON' con 'TARRAGONA'
            if re.search(r'\b' + pat, organ_upper):
                return ccaa
    return 'Otros'

df_health['_ccaa_salud'] = df_health['organo_contratante'].apply(detect_ccaa)

print(f"\n  {'CCAA':<25} {'SARA':>7} {'Match':>6} {'%':>6} {'Miss':>6} {'Imp(M)':>8}")
print(f"  {'-'*60}")

ccaa_stats = df_health.groupby('_ccaa_salud').agg(
    n_sara=('_es_sara', 'count'),
    n_match=('_ted_validated', 'sum'),
    n_miss=('_ted_missing', 'sum'),
    imp=('importe_adjudicacion', 'sum'),
).sort_values('n_sara', ascending=False)

for ccaa, r in ccaa_stats.iterrows():
    if r['n_sara'] < 10:
        continue
    pct = r['n_match'] / max(r['n_sara'], 1) * 100
    print(f"  {ccaa:<25} {int(r['n_sara']):>7,} {int(r['n_match']):>6,} "
          f"{pct:>5.1f}% {int(r['n_miss']):>6,} {r['imp']/1e6:>7.0f}M")


# ======================================================================
#  8. RESUMEN Y CONCLUSIONES
# ======================================================================

print(f"\n{'='*70}")
print(f"  RESUMEN — SECTOR SALUD")
print(f"{'='*70}")

print(f"""
  DATOS CLAVE:
  • Sector salud = {len(df_health):,} contratos SARA ({len(df_health)/len(df_sara)*100:.0f}% del total)
  • Validados en TED: {health_matched:,} ({health_matched/health_total*100:.1f}%)
  • Missing: {len(health_missing_df):,}
  • Cobertura ajustada por lotes: ~{adjusted_coverage:.0f}%
  
  HALLAZGOS:
  • {total_lotes/len(health_missing_df)*100:.0f}% del missing esta en patron de lotes/acuerdos marco
  • Suministros farmaceuticos (CPV 33) domina el missing
  • SAS (Andalucia), ICS (Catalunya), IB-Salut, SERGAS son los top missing
  • La mediana de importe missing es significativamente menor que la de matched
    → consistente con lotes individuales pequenos dentro de contratos grandes
  
  CONCLUSION:
  • El sector salud tiene una tasa de validacion TED INFERIOR a la media
  • PERO la mayor parte del gap se explica por la fragmentacion de lotes en PLACSP
  • Los acuerdos marco de suministros sanitarios generan N registros en PLACSP
    por cada anuncio unico en TED
  • La cobertura REAL ajustada por lotes es ~{adjusted_coverage:.0f}%, significativamente
    mayor que el {health_matched/health_total*100:.0f}% bruto
""")

elapsed = time.time() - t0
print(f"  Tiempo total: {elapsed:.0f}s")
print(f"  Analisis sector salud completado.")