"""
Cross-validation PLACSP <-> TED  v2.0 (SARA rules)
====================================================
Aplica correctamente las reglas SARA para determinar que contratos
deberian estar publicados en TED.

Reglas SARA (Sujeto a Regulacion Armonizada):
  - El umbral varia por BIENIO, TIPO DE CONTRATO y TIPO DE PODER ADJUDICADOR
  - Contratos menores: excluidos
  - Encargos a medios propios: excluidos
  - Negociados sin publicidad: pueden no aparecer (art. 168 LCSP)
  - Contratos privados/patrimoniales: NO son SARA
  - Importes SIN IVA

Uso:
    python run_ted_crossvalidation.py
"""

import pandas as pd
import numpy as np
import time
from pathlib import Path
from collections import defaultdict

# ======================================================================
#  CONFIG
# ======================================================================

PLACSP_PATH = Path("nacional/licitaciones_espana.parquet")
TED_PATH = Path("ted/ted_es_can.parquet")
OUTPUT_DIR = Path("ted")

# -- Umbrales SARA por bienio (sin IVA, en euros) --
SARA_THRESHOLDS = {
    (2010, 2011): {
        'obras': 4_845_000,
        'servicios_age': 125_000,
        'servicios_resto': 193_000,
        'sectores': 387_000,
    },
    (2012, 2013): {
        'obras': 5_000_000,
        'servicios_age': 130_000,
        'servicios_resto': 200_000,
        'sectores': 400_000,
    },
    (2014, 2015): {
        'obras': 5_186_000,
        'servicios_age': 134_000,
        'servicios_resto': 207_000,
        'sectores': 414_000,
    },
    (2016, 2017): {
        'obras': 5_225_000,
        'servicios_age': 135_000,
        'servicios_resto': 209_000,
        'sectores': 418_000,
    },
    (2018, 2019): {
        'obras': 5_548_000,
        'servicios_age': 144_000,
        'servicios_resto': 221_000,
        'sectores': 443_000,
    },
    (2020, 2021): {
        'obras': 5_350_000,
        'servicios_age': 139_000,
        'servicios_resto': 214_000,
        'sectores': 428_000,
    },
    (2022, 2023): {
        'obras': 5_382_000,
        'servicios_age': 140_000,
        'servicios_resto': 215_000,
        'sectores': 431_000,
    },
    (2024, 2025): {
        'obras': 5_538_000,
        'servicios_age': 143_000,
        'servicios_resto': 221_000,
        'sectores': 443_000,
    },
    (2026, 2027): {
        'obras': 5_404_000,
        'servicios_age': 140_000,
        'servicios_resto': 216_000,
        'sectores': 432_000,
    },
}

# Patrones para detectar AGE (Administracion General del Estado)
AGE_PATTERNS = [
    'ADMINISTRACION DEL ESTADO', 'ADMINISTRACION GENERAL DEL ESTADO',
    'Ministerio', 'TRAGSA', 'SEPI', 'AENA', 'RENFE', 'ADIF',
    'CORREOS', 'MUFACE', 'INGESA', 'TGSS', 'Tesoreria General',
    'Agencia Estatal', 'Agencia Tributaria', 'Seguridad Social',
    'Patrimonio Nacional', 'Instituto Nacional', 'CSIC',
    'AEMET', 'DGT', 'Guardia Civil', 'Ejercito', 'Armada',
    'BOE', 'FNMT', 'ICO ', 'ICEX', 'CDTI',
]

# Patrones para detectar sectores especiales
SECTORES_PATTERNS = [
    'AENA', 'RENFE', 'ADIF', 'CORREOS', 'Puertos del Estado',
    'Autoridad Portuaria', 'ENAGAS', 'Canal de Isabel II',
    'Aguas de', 'EMASA', 'EMASESA', 'ACUAES',
    'Metro de', 'Transports de Barcelona', 'TMB',
    'FGC', 'Ferrocarrils',
]

# Tolerancia matching
MATCH_TOLERANCE_PCT = 0.10
MATCH_TOLERANCE_ABS = 5_000
MATCH_YEAR_WINDOW = 1


# ======================================================================
#  FUNCIONES AUXILIARES
# ======================================================================

def get_sara_threshold(year, tipo_contrato, is_age, is_sector):
    """Devuelve el umbral SARA aplicable. None si no es candidato SARA."""
    if tipo_contrato in ('Privado', 'Patrimonial', 'Administrativo Especial',
                         'Gestion Servicios Publicos', '22', '32', '999', 'nan', ''):
        return None

    thresholds = None
    for (y_start, y_end), thr in SARA_THRESHOLDS.items():
        if y_start <= year <= y_end:
            thresholds = thr
            break
    if thresholds is None:
        thresholds = list(SARA_THRESHOLDS.values())[-1]

    if tipo_contrato == 'Obras':
        return thresholds['obras']
    elif tipo_contrato in ('Servicios', 'Suministros'):
        if is_sector:
            return thresholds['sectores']
        elif is_age:
            return thresholds['servicios_age']
        else:
            return thresholds['servicios_resto']
    else:
        return thresholds['servicios_resto']


def classify_buyer(dependencia):
    """Clasifica: (is_age, is_sector)."""
    if not dependencia or pd.isna(dependencia):
        return False, False
    dep_upper = str(dependencia).upper()
    is_sector = any(p.upper() in dep_upper for p in SECTORES_PATTERNS)
    is_age = any(p.upper() in dep_upper for p in AGE_PATTERNS) and not is_sector
    return is_age, is_sector


# ======================================================================
#  1. CARGAR DATOS
# ======================================================================

def load_placsp(path):
    """Carga PLACSP: solo adjudicaciones validas, excluye menores/encargos."""
    print(f"\n{'='*70}")
    print(f"  CARGA PLACSP")
    print(f"{'='*70}")
    df = pd.read_parquet(path)
    print(f"  Total registros: {len(df):,}")

    # Solo adjudicaciones reales (acepta si tiene importe_sin_iva O importe_adjudicacion)
    mask = (
        (df['tipo_registro'] == 'LICITACION') &
        (df['estado'].isin(['Resuelta', 'Adjudicada'])) &
        (df['nif_adjudicatario'].notna()) &
        (df['importe_adjudicacion'].notna() | df['importe_sin_iva'].notna())
    )
    df = df[mask].copy()
    print(f"  Adjudicaciones con NIF + importe: {len(df):,}")

    # Excluir menores
    n_menores = (df['conjunto'] == 'menores').sum()
    df = df[df['conjunto'] != 'menores'].copy()
    print(f"  Excluidos contratos menores: {n_menores:,}")

    # Excluir encargos
    n_enc = (df['conjunto'] == 'encargos').sum()
    df = df[df['conjunto'] != 'encargos'].copy()
    if n_enc > 0:
        print(f"  Excluidos encargos: {n_enc:,}")

    # Excluir consultas
    n_con = (df['conjunto'] == 'consultas').sum()
    df = df[df['conjunto'] != 'consultas'].copy()
    if n_con > 0:
        print(f"  Excluidas consultas: {n_con:,}")

    # Excluir tipos que NO son SARA (art. 25-27 LCSP)
    tipos_no_sara = ['Privado', 'Patrimonial', 'Administrativo Especial',
                     '22', '999', '32']
    _tc = df['tipo_contrato'].astype(str).replace('nan', '')
    mask_no_sara = _tc.isin(tipos_no_sara)
    n_no_sara = mask_no_sara.sum()
    df = df[~mask_no_sara].copy()
    if n_no_sara > 0:
        print(f"  Excluidos tipos no SARA (Privado/Patrimonial/otros): {n_no_sara:,}")

    print(f"  Registros tras exclusiones: {len(df):,}")

    # -- Campos auxiliares --
    df['_nif'] = df['nif_adjudicatario'].astype(str).str.strip().str.upper()
    df['_nif'] = df['_nif'].str.replace(r'^ES[-\s]*', '', regex=True)
    df.loc[df['_nif'].str.len() < 5, '_nif'] = ''

    df['_imp_adj'] = pd.to_numeric(df['importe_adjudicacion'].astype(str).replace('nan', ''), errors='coerce')
    df['_imp_sin_iva'] = pd.to_numeric(df['importe_sin_iva'].astype(str).replace('nan', ''), errors='coerce')
    
    # Para identificar SARA: usar importe_sin_iva (más cercano al VEC)
    # con fallback a importe_adjudicacion cuando sin_iva no existe
    df['_imp_sara'] = np.where(df['_imp_sin_iva'].notna(), df['_imp_sin_iva'], df['_imp_adj'])
    
    n_sin_iva = df['_imp_sin_iva'].notna().sum()
    n_fallback = (df['_imp_sin_iva'].isna() & df['_imp_adj'].notna()).sum()
    print(f"  Importe SARA: {n_sin_iva:,} usan importe_sin_iva, {n_fallback:,} fallback a importe_adjudicacion")
    
    df['_ano'] = pd.to_numeric(df['ano'], errors='coerce')
    df = df[(df['_ano'] >= 2010) & (df['_ano'] <= 2027)].copy()

    df['_expediente'] = df['expediente'].astype(str).replace('nan', '').str.strip()
    df['_fecha_adj'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce')
    df['_tipo_contrato'] = df['tipo_contrato'].astype(str).replace('nan', '').str.strip()
    df['_procedimiento'] = df['procedimiento'].astype(str).replace('nan', '').str.strip()

    # Clasificar comprador
    buyer_class = df['dependencia'].apply(classify_buyer)
    df['_is_age'] = buyer_class.apply(lambda x: x[0])
    df['_is_sector'] = buyer_class.apply(lambda x: x[1])

    # Umbral SARA por contrato
    df['_umbral_sara'] = df.apply(
        lambda r: get_sara_threshold(
            int(r['_ano']) if pd.notna(r['_ano']) else 2024,
            r['_tipo_contrato'],
            r['_is_age'],
            r['_is_sector']
        ), axis=1
    )

    # Candidato SARA? (compara importe_sin_iva / VEC proxy contra umbral)
    df['_es_sara'] = df['_umbral_sara'].notna() & (df['_imp_sara'] >= df['_umbral_sara'])

    # -- Suma de lotes por expediente (VEC = suma de todos los lotes) --
    # Si un procedimiento tiene N lotes individuales bajo el umbral pero
    # la suma supera el umbral, TODOS los lotes son SARA
    n_sara_before_lots = df['_es_sara'].sum()
    
    # Solo considerar contratos no-SARA con expediente válido
    non_sara = df[~df['_es_sara'] & (df['_expediente'].str.len() > 3)].copy()
    
    if len(non_sara) > 0:
        # Agrupar por expediente: sumar importes + tomar umbral del primer lote
        lot_sums = non_sara.groupby('_expediente').agg(
            imp_total=('_imp_sara', 'sum'),
            n_lotes=('_imp_sara', 'count'),
            umbral=('_umbral_sara', 'first'),
        )
        
        # Filtrar: ≥2 lotes y suma >= umbral
        lot_sums = lot_sums[
            (lot_sums['n_lotes'] >= 2) & 
            lot_sums['umbral'].notna() &
            (lot_sums['imp_total'] >= lot_sums['umbral'])
        ]
        
        # Marcar todos los lotes de esos expedientes como SARA
        sara_expedientes = set(lot_sums.index)
        lot_mask = (~df['_es_sara']) & (df['_expediente'].isin(sara_expedientes))
        df.loc[lot_mask, '_es_sara'] = True
        df.loc[lot_mask, '_sara_por_lotes'] = True
        
        n_sara_lots = lot_mask.sum()
        n_exp_lots = len(sara_expedientes)
        print(f"\n  Suma de lotes por expediente:")
        print(f"    Expedientes con lotes que suman >= umbral: {n_exp_lots:,}")
        print(f"    Contratos adicionales marcados SARA:       {n_sara_lots:,}")
        print(f"    SARA antes de lotes: {n_sara_before_lots:,} → después: {df['_es_sara'].sum():,}")
    
    if '_sara_por_lotes' not in df.columns:
        df['_sara_por_lotes'] = False
    df['_sara_por_lotes'] = df['_sara_por_lotes'].fillna(False)

    # Negociado sin publicidad
    df['_es_neg_sin_pub'] = df['_procedimiento'].str.contains(
        'Negociado sin publicidad', case=False, na=False
    )

    # -- Stats --
    n_sara = df['_es_sara'].sum()
    n_age = df['_is_age'].sum()
    n_sector = df['_is_sector'].sum()
    n_neg = df[df['_es_sara'] & df['_es_neg_sin_pub']].shape[0]

    print(f"\n  Clasificacion compradores:")
    print(f"    AGE:             {n_age:,}")
    print(f"    Sectores espec.: {n_sector:,}")
    print(f"    Resto:           {len(df) - n_age - n_sector:,}")
    print(f"\n  Candidatos SARA (sobre umbral): {n_sara:,}")
    print(f"  De los cuales neg. sin pub.:    {n_neg:,}")

    sara_df = df[df['_es_sara']]
    print(f"\n  SARA por tipo contrato:")
    for tc, cnt in sara_df['_tipo_contrato'].value_counts().items():
        print(f"    {tc:<25}: {cnt:>8,}")

    print(f"\n  SARA por ano (ultimos 10):")
    for yr, cnt in sara_df['_ano'].value_counts().sort_index().tail(10).items():
        print(f"    {int(yr)}: {cnt:>6,}")

    return df


def load_ted(path):
    """Carga datos TED normalizados."""
    print(f"\n{'='*70}")
    print(f"  CARGA TED")
    print(f"{'='*70}")
    df = pd.read_parquet(path)
    print(f"  Total registros: {len(df):,}")

    for col in ['importe_ted', 'year', 'number_offers']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'win_nif_clean' not in df.columns:
        df['win_nif_clean'] = df.get('win_nationalid', pd.Series(dtype=str)) \
            .fillna('').astype(str).str.strip().str.upper()
        df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)

    valid = df['importe_ted'].notna() & (df['importe_ted'] > 0)
    has_nif = df['win_nif_clean'].str.len() >= 5
    print(f"  Con importe: {valid.sum():,}")
    print(f"  Con NIF ganador: {(valid & has_nif).sum():,}")

    return df


# ======================================================================
#  2. CROSS-VALIDATION
# ======================================================================

def cross_validate(df_placsp, df_ted):
    """
    Cruza solo candidatos SARA contra TED.
    Negociados sin publicidad se cuentan aparte.
    """
    print(f"\n{'='*70}")
    print(f"  CROSS-VALIDATION PLACSP <-> TED")
    print(f"{'='*70}")

    t0 = time.time()

    # -- Indices TED --
    ted_valid = df_ted[
        (df_ted['importe_ted'].notna()) & (df_ted['importe_ted'] > 0)
    ].copy()

    ted_lookup = defaultdict(list)
    ted_lookup_exp = defaultdict(list)

    print(f"  Construyendo indices TED ({len(ted_valid):,} registros)...")
    for _, row in ted_valid.iterrows():
        nif = str(row.get('win_nif_clean', '')).strip()
        imp = row['importe_ted']
        yr = row.get('year', np.nan)

        entry = {
            'importe': imp,
            'ted_id': str(row.get('ted_notice_id', '')),
            'n_ofertas': row.get('number_offers', np.nan),
            'cpv_ted': str(row.get('cpv', '')),
            'cae_ted': str(row.get('cae_name', '')),
            'win_size': str(row.get('win_size', '')),
            'direct_award': str(row.get('direct_award_justification', '')),
            'sme_part': str(row.get('sme_participation', '')),
            'buyer_legal_type': str(row.get('buyer_legal_type', '')),
            'duration_lot': row.get('duration_lot', np.nan),
            'award_criterion_type': str(row.get('award_criterion_type', '')),
            'internal_id': str(row.get('internal_id_proc', '')),
            'consumed': False,
        }

        if nif and len(nif) >= 5 and pd.notna(yr):
            ted_lookup[(nif, int(yr))].append(entry)

        exp_id = str(row.get('internal_id_proc', '')).strip()
        if exp_id and len(exp_id) >= 4:
            ted_lookup_exp[exp_id.upper()].append(entry)

    print(f"  TED lookup NIF+ano: {len(ted_lookup):,} claves")
    print(f"  TED lookup expediente: {len(ted_lookup_exp):,} claves")

    # -- Solo matchear SARA con NIF valido --
    sara_valid = df_placsp[
        df_placsp['_es_sara'] &
        (df_placsp['_nif'].str.len() >= 5) &
        (df_placsp['_imp_adj'] > 0) &
        (df_placsp['_ano'].notna())
    ]
    print(f"\n  SARA candidatos para matching: {len(sara_valid):,}")

    # -- Matching loop --
    matched_idx = []
    match_data = {}
    n_match_nif = 0
    n_match_exp = 0

    total = len(sara_valid)
    for count, (idx, row) in enumerate(sara_valid.iterrows()):
        if count % 50_000 == 0 and count > 0:
            elapsed = time.time() - t0
            pct = count / total * 100
            rate = count / max(elapsed, 1)
            eta = (total - count) / max(rate, 1)
            print(f"    {count:,}/{total:,} ({pct:.0f}%) | "
                  f"{len(matched_idx):,} matches | {elapsed:.0f}s | ETA {eta:.0f}s")

        nif = row['_nif']
        imp = row['_imp_adj']
        yr = int(row['_ano'])
        tol = max(imp * MATCH_TOLERANCE_PCT, MATCH_TOLERANCE_ABS)

        best_match = None
        best_diff = float('inf')
        best_key = None
        best_i = None
        best_lookup = None

        # Estrategia 1: NIF + importe + ano
        for yr_offset in range(MATCH_YEAR_WINDOW + 1):
            for yr_try in ([yr + yr_offset, yr - yr_offset] if yr_offset > 0 else [yr]):
                key = (nif, yr_try)
                entries = ted_lookup.get(key, [])
                for i, entry in enumerate(entries):
                    if entry['consumed']:
                        continue
                    diff = abs(entry['importe'] - imp)
                    if diff <= tol and diff < best_diff:
                        best_match = entry
                        best_diff = diff
                        best_key = key
                        best_i = i
                        best_lookup = ted_lookup

        # Estrategia 2: expediente
        if best_match is None:
            exp_id = row['_expediente'].strip().upper()
            if exp_id and len(exp_id) >= 4:
                entries = ted_lookup_exp.get(exp_id, [])
                for i, entry in enumerate(entries):
                    if entry['consumed']:
                        continue
                    diff = abs(entry['importe'] - imp)
                    if diff <= tol and diff < best_diff:
                        best_match = entry
                        best_diff = diff
                        best_key = exp_id
                        best_i = i
                        best_lookup = ted_lookup_exp

        if best_match is not None:
            best_lookup[best_key][best_i]['consumed'] = True
            matched_idx.append(idx)
            if best_lookup is ted_lookup:
                n_match_nif += 1
            else:
                n_match_exp += 1
            match_data[idx] = {
                'ted_id': best_match['ted_id'],
                'ted_importe': best_match['importe'],
                'ted_n_ofertas': best_match['n_ofertas'],
                'ted_cpv': best_match['cpv_ted'],
                'ted_cae': best_match['cae_ted'],
                'ted_win_size': best_match['win_size'],
                'ted_direct_award': best_match['direct_award'],
                'ted_sme_part': best_match['sme_part'],
                'ted_buyer_legal_type': best_match['buyer_legal_type'],
                'ted_duration': best_match['duration_lot'],
                'ted_award_criterion': best_match['award_criterion_type'],
                'ted_internal_id': best_match['internal_id'],
                'match_diff_euros': best_diff,
            }

    elapsed = time.time() - t0

    # -- E2b: Match a nivel EXPEDIENTE para contratos SARA por lotes --
    # Si un expediente tiene lotes marcados como SARA por suma, intentar
    # matchear el expediente completo contra TED (sin filtro de importe)
    # Si el expediente aparece en TED → todos sus lotes se validan
    
    n_match_exp_lot = 0
    sara_lot_not_matched = df_placsp[
        df_placsp['_es_sara'] &
        df_placsp['_sara_por_lotes'] &
        ~df_placsp.index.isin(matched_idx)
    ]
    
    if len(sara_lot_not_matched) > 0:
        # Agrupar por expediente
        lot_expedientes = sara_lot_not_matched.groupby('_expediente').agg(
            indices=('_expediente', lambda x: list(x.index)),
            n_lotes=('_expediente', 'count'),
            imp_total=('_imp_sara', 'sum'),
            nif_organo=('nif_organo', 'first'),
            organo=('organo_contratante', 'first'),
            ano=('_ano', 'first'),
        )
        
        # Para cada expediente, buscar en TED por expediente (sin filtro importe)
        for exp_id, grp in lot_expedientes.iterrows():
            if not exp_id or len(exp_id) < 4:
                continue
            
            exp_upper = exp_id.strip().upper()
            entries = ted_lookup_exp.get(exp_upper, [])
            
            # Match sin filtro de importe — el expediente como unidad
            ted_match = None
            for i, entry in enumerate(entries):
                if not entry['consumed']:
                    ted_match = entry
                    ted_match_i = i
                    break
            
            if ted_match is not None:
                # Validar todos los lotes de este expediente
                ted_lookup_exp[exp_upper][ted_match_i]['consumed'] = True
                for lot_idx in grp['indices']:
                    if lot_idx not in matched_idx:
                        matched_idx.append(lot_idx)
                        df_placsp.loc[lot_idx, '_ted_validated'] = True
                        match_data[lot_idx] = {
                            'ted_id': ted_match['ted_id'],
                            'ted_importe': ted_match['importe'],
                            'ted_n_ofertas': ted_match['n_ofertas'],
                            'ted_cpv': ted_match['cpv_ted'],
                            'ted_cae': ted_match['cae_ted'],
                            'ted_win_size': ted_match['win_size'],
                            'ted_direct_award': ted_match['direct_award'],
                            'ted_sme_part': ted_match['sme_part'],
                            'ted_buyer_legal_type': ted_match['buyer_legal_type'],
                            'ted_duration': ted_match['duration_lot'],
                            'ted_award_criterion': ted_match['award_criterion_type'],
                            'ted_internal_id': ted_match['internal_id'],
                            'match_diff_euros': 0,
                        }
                        n_match_exp_lot += 1
        
        print(f"\n  E2b - Match expediente (lotes SARA): {n_match_exp_lot:,} lotes validados")

    elapsed = time.time() - t0
    df_placsp['_ted_validated'] = False
    df_placsp.loc[matched_idx, '_ted_validated'] = True

    enrich_fields = {
        '_ted_id': '', '_ted_n_ofertas': np.nan, '_ted_cpv': '',
        '_ted_win_size': '', '_ted_direct_award': '', '_ted_sme_part': '',
        '_ted_buyer_legal_type': '', '_ted_duration': np.nan,
        '_ted_award_criterion': '', '_ted_internal_id': '',
    }
    for col, default in enrich_fields.items():
        df_placsp[col] = default

    for idx, data in match_data.items():
        df_placsp.loc[idx, '_ted_id'] = data['ted_id']
        df_placsp.loc[idx, '_ted_n_ofertas'] = pd.to_numeric(data['ted_n_ofertas'], errors='coerce')
        df_placsp.loc[idx, '_ted_cpv'] = data['ted_cpv']
        df_placsp.loc[idx, '_ted_win_size'] = data['ted_win_size']
        df_placsp.loc[idx, '_ted_direct_award'] = data['ted_direct_award']
        df_placsp.loc[idx, '_ted_sme_part'] = data['ted_sme_part']
        df_placsp.loc[idx, '_ted_buyer_legal_type'] = data['ted_buyer_legal_type']
        df_placsp.loc[idx, '_ted_duration'] = pd.to_numeric(data['ted_duration'], errors='coerce')
        df_placsp.loc[idx, '_ted_award_criterion'] = data['ted_award_criterion']
        df_placsp.loc[idx, '_ted_internal_id'] = data['ted_internal_id']

    # -- Missing in TED --
    df_placsp['_ted_missing'] = (
        df_placsp['_es_sara'] &
        ~df_placsp['_ted_validated'] &
        ~df_placsp['_es_neg_sin_pub']
    )
    df_placsp['_ted_missing_incl_neg'] = (
        df_placsp['_es_sara'] &
        ~df_placsp['_ted_validated']
    )

    n_matched = len(matched_idx)
    n_sara = df_placsp['_es_sara'].sum()
    n_missing_strict = df_placsp['_ted_missing'].sum()
    n_missing_incl = df_placsp['_ted_missing_incl_neg'].sum()
    n_neg_sin_pub = df_placsp[df_placsp['_es_sara'] & df_placsp['_es_neg_sin_pub']].shape[0]

    # ==============================
    #  RESUMEN
    # ==============================
    print(f"\n{'='*70}")
    print(f"  RESULTADOS CROSS-VALIDATION")
    print(f"{'='*70}")
    print(f"  Tiempo: {elapsed:.0f}s")
    print(f"\n  CANDIDATOS SARA: {n_sara:,}")
    print(f"    Neg. sin publicidad: {n_neg_sin_pub:,}")

    print(f"\n  VALIDADOS POR TED: {n_matched:,} ({n_matched/max(n_sara,1)*100:.1f}%)")
    print(f"    Por NIF + importe (E1): {n_match_nif:,}")
    print(f"    Por expediente (E2):    {n_match_exp:,}")
    print(f"    Por expediente lotes (E2b): {n_match_exp_lot:,}")

    print(f"\n  MISSING IN TED (excl. neg s/p): {n_missing_strict:,} "
          f"({n_missing_strict/max(n_sara - n_neg_sin_pub,1)*100:.1f}%)")
    print(f"  MISSING IN TED (incl. neg s/p): {n_missing_incl:,} "
          f"({n_missing_incl/max(n_sara,1)*100:.1f}%)")

    # Por ano
    print(f"\n  {'Ano':>6} {'SARA':>8} {'Match':>8} {'%Match':>7} {'Missing':>8} {'NegS/P':>7}")
    print(f"  {'-'*52}")
    for yr in sorted(df_placsp[df_placsp['_es_sara']]['_ano'].unique()):
        yr = int(yr)
        yr_sara = df_placsp[(df_placsp['_es_sara']) & (df_placsp['_ano'] == yr)]
        yr_match = yr_sara['_ted_validated'].sum()
        yr_miss = yr_sara['_ted_missing'].sum()
        yr_neg = yr_sara['_es_neg_sin_pub'].sum()
        pct = yr_match / max(len(yr_sara), 1) * 100
        print(f"  {yr:>6} {len(yr_sara):>8,} {yr_match:>8,} {pct:>6.1f}% {yr_miss:>8,} {yr_neg:>7,}")

    # Por tipo contrato
    print(f"\n  Validacion por tipo contrato:")
    for tc in ['Obras', 'Servicios', 'Suministros']:
        tc_sara = df_placsp[(df_placsp['_es_sara']) & (df_placsp['_tipo_contrato'] == tc)]
        tc_match = tc_sara['_ted_validated'].sum()
        tc_miss = tc_sara['_ted_missing'].sum()
        pct = tc_match / max(len(tc_sara), 1) * 100
        print(f"    {tc:<15}: {len(tc_sara):>8,} SARA | "
              f"{tc_match:>6,} match ({pct:.0f}%) | {tc_miss:>6,} missing")

    # Top organos missing
    df_missing = df_placsp[df_placsp['_ted_missing']].copy()
    if len(df_missing) > 0:
        print(f"\n  Top 15 organos con mas missing:")
        top_org = df_missing.groupby('organo_contratante').agg(
            n=('_ted_missing', 'sum'),
            imp=('_imp_adj', 'sum'),
        ).sort_values('n', ascending=False).head(15)
        for organ, r in top_org.iterrows():
            print(f"    {str(organ)[:55]:<55}: {int(r['n']):>5,} | {r['imp']/1e6:.1f}M")

    # Distribucion importes missing
    if len(df_missing) > 0:
        print(f"\n  Distribucion importes missing:")
        for lo, hi, label in [
            (0, 221_000, "<221K (zona gris AGE/resto)"),
            (221_000, 500_000, "221K-500K"),
            (500_000, 1_000_000, "500K-1M"),
            (1_000_000, 5_000_000, "1M-5M"),
            (5_000_000, float('inf'), ">=5M (obras)"),
        ]:
            n = ((df_missing['_imp_adj'] >= lo) & (df_missing['_imp_adj'] < hi)).sum()
            print(f"    {label:<35}: {n:>6,}")
    
    # -- Missing a nivel EXPEDIENTE (no individual) --
    # Un expediente = 1 procedimiento en TED, independientemente de N lotes
    sara_all = df_placsp[df_placsp['_es_sara']].copy()
    exp_stats = sara_all.groupby('_expediente').agg(
        n_lotes=('_es_sara', 'count'),
        any_matched=('_ted_validated', 'any'),
        imp_total=('_imp_sara', 'sum'),
        es_lotes=('_sara_por_lotes', 'any'),
    )
    n_exp_total = len(exp_stats)
    n_exp_matched = exp_stats['any_matched'].sum()
    n_exp_missing = n_exp_total - n_exp_matched
    n_exp_lotes_missing = ((~exp_stats['any_matched']) & exp_stats['es_lotes']).sum()
    
    print(f"\n  MISSING A NIVEL EXPEDIENTE (1 expediente = 1 procedimiento TED):")
    print(f"    Expedientes SARA totales:    {n_exp_total:,}")
    print(f"    Expedientes con match TED:   {n_exp_matched:,} ({n_exp_matched/max(n_exp_total,1)*100:.1f}%)")
    print(f"    Expedientes missing:         {n_exp_missing:,} ({n_exp_missing/max(n_exp_total,1)*100:.1f}%)")
    print(f"    De los cuales SARA por lotes:{n_exp_lotes_missing:,}")

    return df_placsp, df_missing


# ======================================================================
#  3. MAIN
# ======================================================================

if __name__ == "__main__":
    df_placsp = load_placsp(PLACSP_PATH)
    df_ted = load_ted(TED_PATH)

    df_placsp, df_missing = cross_validate(df_placsp, df_ted)

    # -- Guardar --
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save_cols = [
        'expediente', 'organo_contratante', 'nif_organo', 'dependencia',
        'nif_adjudicatario', 'adjudicatario', 'importe_adjudicacion',
        'importe_sin_iva', 'ano', 'estado', 'conjunto', 'tipo_contrato',
        'procedimiento', 'cpv_principal', 'fecha_adjudicacion',
        '_es_sara', '_umbral_sara', '_imp_sara', '_sara_por_lotes', '_is_age', '_is_sector',
        '_es_neg_sin_pub', '_tipo_contrato', '_procedimiento',
        '_ted_validated', '_ted_missing', '_ted_missing_incl_neg',
        '_ted_id', '_ted_n_ofertas', '_ted_cpv', '_ted_win_size',
        '_ted_direct_award', '_ted_sme_part', '_ted_buyer_legal_type',
        '_ted_duration', '_ted_award_criterion', '_ted_internal_id',
    ]
    cols_exist = [c for c in save_cols if c in df_placsp.columns]

    # SARA completo
    sara_df = df_placsp[df_placsp['_es_sara']][cols_exist]
    sara_df.to_parquet(OUTPUT_DIR / "crossval_sara.parquet", index=False)
    print(f"\n  SARA completo: {OUTPUT_DIR / 'crossval_sara.parquet'} ({len(sara_df):,})")

    # Matched
    matched = df_placsp[df_placsp['_ted_validated']][cols_exist]
    matched.to_parquet(OUTPUT_DIR / "crossval_matched.parquet", index=False)
    print(f"  Matched: {OUTPUT_DIR / 'crossval_matched.parquet'} ({len(matched):,})")

    # Missing
    if len(df_missing) > 0:
        miss_cols = [c for c in cols_exist if c in df_missing.columns]
        df_missing[miss_cols].to_parquet(OUTPUT_DIR / "crossval_missing.parquet", index=False)
        print(f"  Missing: {OUTPUT_DIR / 'crossval_missing.parquet'} ({len(df_missing):,})")

    print(f"\n  Cross-validation completado.")