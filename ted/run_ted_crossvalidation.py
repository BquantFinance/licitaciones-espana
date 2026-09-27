"""
Cross-validation PLACSP <-> TED  v4.0 (Unificado)
====================================================
Pipeline completo de validacion: identifica contratos SARA en PLACSP
y los cruza contra el Diario Oficial de la UE (TED) con 9 estrategias.

Reglas SARA (Sujeto a Regulacion Armonizada):
  - Umbral varia por BIENIO, TIPO DE CONTRATO y TIPO DE PODER ADJUDICADOR
  - Se aplica sobre el Valor Estimado del Contrato (valor_estimado_contrato;
    si falta, presupuesto base sin IVA y en ultimo caso importe adjudicado)
  - Suma de lotes del mismo expediente (y organo) cuenta como un solo VEC
  - PLACSP se lee con nacional.licitaciones.leer_placsp: una fila por
    licitacion (version mas reciente) y semantica actual de columnas
  - Contratos menores, encargos, privados, patrimoniales: excluidos

Estrategias de matching:
  E1:  NIF adjudicatario + importe ±10% + año ±1
  E2:  Nº expediente + importe ±10%
  E2b: Expediente completo (lotes SARA, sin filtro importe)
  E3:  NIF organo contratante + importe ±10%
  E4:  Suma importes agrupados (lotes/acuerdos marco) vs total_value TED
  E5:  Nombre organo normalizado + importe ±10%
  E3b: Tabla alias organo (PLACSP→TED) + importe ±10%
  E7:  Fuzzy token overlap (>=60%) + importe ±10%
  E6:  Propagacion por expediente (si 1 lote matched -> todos matched)

Uso:
    python crossvalidation_ted.py

Outputs:
    ted/crossval_sara.parquet           - Todos los contratos SARA
    ted/crossval_matched.parquet        - SARA con match TED
    ted/crossval_missing.parquet        - SARA sin match TED
    ted/missing_alta_confianza.parquet  - Missing alta confianza
"""

import pandas as pd
import numpy as np
import re
import sys
import time
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nacional.licitaciones import leer_placsp  # noqa: E402


# ======================================================================
#  CONFIG
# ======================================================================

# Rutas relativas a la raíz del repo (no al cwd)
_REPO_DIR = Path(__file__).resolve().parent.parent
PLACSP_PATH = _REPO_DIR / "nacional" / "licitaciones_espana.parquet"
TED_PATH = _REPO_DIR / "ted" / "ted_es_can.parquet"
OUTPUT_DIR = _REPO_DIR / "ted"

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

# Tolerancias matching
MATCH_TOL_PCT = 0.10
MATCH_TOL_ABS = 5_000
MATCH_TOL_PCT_WIDE = 0.15       # Para lotes agrupados (E4)
MATCH_TOL_ABS_WIDE = 20_000
MATCH_YEAR_WINDOW = 1

# Tabla de equivalencias nombre PLACSP → nombre(s) TED (normalizados, truncados 40 chars)
# Construida a partir del diagnostico_missing_hc.py
ORGAN_ALIASES = {
    'ADIF PRESIDENCIA':
        ['ADMINISTRADOR DE INFRAESTRUCTURAS FERROV', 'ADIF'],
    'ADIF CONSEJO DE ADMINISTRACION':
        ['ADMINISTRADOR DE INFRAESTRUCTURAS FERROV', 'ADIF'],
    'ADIF ALTA VELOCIDAD PRESIDENCIA':
        ['ADIF ALTA VELOCIDAD', 'ADIF AV'],
    'ADIF ALTA VELOCIDAD CONSEJO DE ADMINISTR':
        ['ADIF ALTA VELOCIDAD', 'ADIF AV'],
    'EMPRESA PUBLICA DE METRO DE MADRID S A':
        ['METRO DE MADRID S A'],
    'INSTITUT CATALA DE LA SALUT ICS':
        ['INSTITUT CATALA DE LA SALUT', 'ICS'],
    'CONSELLERIA DE SANIDADE SERGAS':
        ['SERVIZO GALEGO DE SAUDE', 'SERGAS'],
    'HOSPITAL UNIVERSITARIO DOCE DE OCTUBRE':
        ['HOSPITAL UNIVERSITARIO 12 DE OCTUBRE', 'HOSPITAL 12 DE OCTUBRE'],
    'HOSPITAL LA PAZ':
        ['HOSPITAL UNIVERSITARIO LA PAZ'],
    'PARADORES DE TURISMO DE ESPANA S M E S A':
        ['PARADORES DE TURISMO DE ESPANA S A'],
    'AENA DIRECCION DEL AEROPUERTO ADOLFO SUA':
        ['AENA S M E S A', 'AENA CONSEJO DE ADMINISTRACION'],
    'AENA DIRECCION DE CONTRATACION':
        ['AENA S M E S A', 'AENA CONSEJO DE ADMINISTRACION'],
    'SERVEI CATALA DE LA SALUT CATSALUT':
        ['SERVEI CATALA DE LA SALUT', 'CATSALUT'],
    'COMITE CENTRAL DE COMPRAS DE NAVANTIA S':
        ['NAVANTIA S A S M E', 'NAVANTIA'],
    'HOSPITAL UNIVERSITARIO DE FUENLABRADA':
        ['HOSPITAL UNIVERSITARIO DE FUENLABRADA'],
    'DIRECCION GENERAL DE RACIONALIZACION Y C':
        ['DIRECCION GENERAL DE RACIONALIZACION Y C',
         'JUNTA DE CONTRATACION CENTRALIZADA'],
}


# ======================================================================
#  FUNCIONES AUXILIARES
# ======================================================================

def get_sara_threshold(year, tipo_contrato, is_age, is_sector):
    """Devuelve el umbral SARA aplicable. None si no es candidato SARA."""
    # Tipo desconocido: según la versión de pandas llega como NaN, None o 'None'
    if tipo_contrato is None or pd.isna(tipo_contrato):
        return None
    # Comparación sin acentos: el scraper nacional emite 'Gestión Servicios Públicos'
    tipo_norm = normalize_name(tipo_contrato)
    if tipo_norm in ('PRIVADO', 'PATRIMONIAL', 'ADMINISTRATIVO ESPECIAL',
                     'GESTION SERVICIOS PUBLICOS', '22', '32', '999', 'NAN', 'NONE', ''):
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
    elif tipo_norm.startswith('CONCESION'):
        # Concesiones de obras y de servicios: mismo umbral que obras
        # (art. 20 LCSP, Directiva 2014/23/UE; art. 14 TRLCSP)
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
    # Sin acentos y por palabra completa: como subcadena 'ICO ' casaba con
    # 'PÚBLICO ' (todo 'Sector Público > ...' salía AGE) y 'BOE' con 'BOECILLO'
    dep_norm = normalize_name(dependencia)
    is_sector = bool(_SECTORES_RE.search(dep_norm))
    is_age = bool(_AGE_RE.search(dep_norm)) and not is_sector
    return is_age, is_sector


def normalize_name(name):
    """Normaliza nombre organo para matching fuzzy."""
    if not name or pd.isna(name):
        return ''
    s = str(name).upper().strip()
    for a, b in [('Á','A'),('É','E'),('Í','I'),('Ó','O'),('Ú','U'),
                 ('Ñ','N'),('Ü','U'),('Ç','C')]:
        s = s.replace(a, b)
    s = re.sub(r'[.,;:\-\/\\()\[\]"\'`]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _patterns_regex(patterns):
    """Regex de patrones normalizados (sin acentos) que solo casa palabras completas."""
    alts = sorted({normalize_name(p) for p in patterns}, key=len, reverse=True)
    return re.compile(r'(?<!\w)(?:' + '|'.join(re.escape(a) for a in alts) + r')(?!\w)')


_AGE_RE = _patterns_regex(AGE_PATTERNS)
_SECTORES_RE = _patterns_regex(SECTORES_PATTERNS)


def _str_or_empty(val):
    """str(val), salvo nulos (None/NaN) → '' (evita 'nan'/'None' como texto)."""
    try:
        if pd.isna(val):
            return ''
    except (TypeError, ValueError):
        pass
    return str(val)


# Expedientes nulos convertidos a texto por astype(str) (pandas 2: 'None'/'nan')
_EXP_NULOS = frozenset(['NONE', 'NAN'])


def _clave_organo_expediente(df, exp_col):
    """Clave (órgano, expediente): el nº de expediente solo es único dentro de un órgano
    ('1/2024' existe en cientos de órganos). NaN si el expediente no es utilizable."""
    exp = df[exp_col].map(_str_or_empty).astype(str).str.strip()
    valido = (exp.str.len() > 3) & ~exp.str.upper().isin(_EXP_NULOS)
    clave = df['organo_contratante'].map(_str_or_empty).astype(str) + '||' + exp
    return clave.where(valido)


def clean_nif(nif):
    """Limpia NIF: mayusculas, sin prefijo ES."""
    if not nif or pd.isna(nif):
        return ''
    s = str(nif).strip().upper()
    s = re.sub(r'^ES[-\s]*', '', s)
    return s if len(s) >= 5 else ''


def tol(imp, pct=MATCH_TOL_PCT, abs_val=MATCH_TOL_ABS):
    """Tolerancia de matching para un importe."""
    return max(imp * pct, abs_val)


# Stopwords para token matching (articulos, preposiciones, formas juridicas)
_STOPWORDS = frozenset([
    'DE', 'DEL', 'LA', 'LAS', 'LOS', 'EL', 'EN', 'Y', 'E', 'A', 'AL',
    'S', 'SA', 'SL', 'SLU', 'SAU', 'SME', 'MP', 'EP',
    'CONSEJO', 'ADMINISTRACION', 'DIRECCION', 'GENERAL', 'JUNTA',
    'GOBIERNO', 'LOCAL', 'PROVINCIAL', 'GERENCIA', 'PRESIDENCIA',
    'DIRECTOR', 'GERENTE', 'PRESIDENTE', 'VICEPRESIDENTE',
])


def token_overlap(name_a, name_b, min_tokens=2):
    """Calcula solapamiento de tokens significativos entre dos nombres.
    Returns: (overlap_ratio, n_common) where ratio = common / min(len_a, len_b)."""
    if not name_a or not name_b:
        return 0.0, 0
    toks_a = {t for t in name_a.split() if len(t) > 2 and t not in _STOPWORDS}
    toks_b = {t for t in name_b.split() if len(t) > 2 and t not in _STOPWORDS}
    if len(toks_a) < min_tokens or len(toks_b) < min_tokens:
        return 0.0, 0
    common = toks_a & toks_b
    if len(common) < min_tokens:
        return 0.0, 0
    ratio = len(common) / min(len(toks_a), len(toks_b))
    return ratio, len(common)


# ======================================================================
#  1. CARGA PLACSP
# ======================================================================

def _num_col(df, col):
    """Columna numerica (NaN si no existe)."""
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return pd.to_numeric(df[col], errors='coerce')


def load_placsp(path):
    """Carga PLACSP: adjudicaciones validas, identifica SARA.

    leer_placsp deja una fila por licitacion: los parquet publicados hasta
    v2026.02 repiten cada licitacion en varias versiones y cada version
    adjudicada contaba como un contrato SARA distinto.
    """
    print(f"\n{'='*70}")
    print(f"  CARGA PLACSP")
    print(f"{'='*70}")
    df = leer_placsp(path)
    print(f"  Total licitaciones (una fila por id): {len(df):,}")

    # Solo adjudicaciones reales ('tipo_registro' solo existe en los parquet publicados)
    if 'tipo_registro' in df.columns:
        es_licitacion = df['tipo_registro'] == 'LICITACION'
    else:
        es_licitacion = df['conjunto'] != 'consultas'
    cols_importe = [c for c in ('importe_adjudicacion', 'valor_estimado_contrato', 'importe_sin_iva')
                    if c in df.columns]
    mask = (
        es_licitacion &
        (df['estado'].isin(['Resuelta', 'Adjudicada'])) &
        (df['nif_adjudicatario'].notna()) &
        df[cols_importe].notna().any(axis=1)
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

    # Excluir tipos no SARA (art. 25-27 LCSP). Las concesiones (22/31/32) si son
    # SARA, con el umbral de obras (get_sara_threshold)
    tipos_no_sara = ['Privado', 'Patrimonial', 'Administrativo Especial',
                     'Gestión Servicios Públicos', 'Otros']
    _tc = df['tipo_contrato'].astype(str).replace('nan', '')
    mask_no_sara = _tc.isin(tipos_no_sara)
    n_no_sara = mask_no_sara.sum()
    df = df[~mask_no_sara].copy()
    if n_no_sara > 0:
        print(f"  Excluidos tipos no SARA (Privado/Patrimonial/otros): {n_no_sara:,}")

    print(f"  Registros tras exclusiones: {len(df):,}")

    # -- Campos auxiliares --
    df['_nif'] = df['nif_adjudicatario'].apply(clean_nif)
    df['_imp_adj'] = _num_col(df, 'importe_adjudicacion')
    df['_imp_sin_iva'] = _num_col(df, 'importe_sin_iva')          # presupuesto base sin IVA
    df['_imp_vec'] = _num_col(df, 'valor_estimado_contrato')      # valor estimado del contrato

    # Para SARA: el umbral se aplica al valor estimado; si falta, presupuesto base
    # sin IVA (<= VEC, conservador) y en ultimo caso el importe adjudicado
    df['_imp_sara'] = df['_imp_vec'].fillna(df['_imp_sin_iva']).fillna(df['_imp_adj'])

    # Para matching contra TED: preferir imp_adj (comparable con award_value)
    df['_imp_match'] = (df['_imp_adj'].where(df['_imp_adj'] > 0)
                        .fillna(df['_imp_sin_iva']).fillna(df['_imp_vec']))

    n_vec = df['_imp_vec'].notna().sum()
    n_pbl = (df['_imp_vec'].isna() & df['_imp_sin_iva'].notna()).sum()
    n_fallback = (df['_imp_vec'].isna() & df['_imp_sin_iva'].isna() & df['_imp_adj'].notna()).sum()
    print(f"  Importe SARA: {n_vec:,} valor estimado, {n_pbl:,} presupuesto sin IVA, "
          f"{n_fallback:,} importe adjudicado")

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

    # Candidato SARA individual
    df['_es_sara'] = df['_umbral_sara'].notna() & (df['_imp_sara'] >= df['_umbral_sara'])

    # -- Suma de lotes por expediente (VEC = suma de todos los lotes) --
    # Clave (organo, expediente): el mismo n de expediente existe en muchos organos
    n_sara_before_lots = df['_es_sara'].sum()
    df['_sara_por_lotes'] = False
    clave_lote = _clave_organo_expediente(df, '_expediente')

    non_sara = df[~df['_es_sara'] & clave_lote.notna()].copy()
    if len(non_sara) > 0:
        non_sara['_clave_lote'] = clave_lote[non_sara.index]
        lot_sums = non_sara.groupby('_clave_lote').agg(
            imp_suma=('_imp_sara', 'sum'),
            vec_max=('_imp_vec', 'max'),
            vec_distintos=('_imp_vec', 'nunique'),
            vec_informados=('_imp_vec', 'count'),
            n_lotes=('_imp_sara', 'count'),
            umbral=('_umbral_sara', 'min'),
        )
        # Si todas las filas repiten el mismo valor estimado (el del expediente
        # completo) se cuenta una vez: sumarlo lo multiplicaria por el n de filas
        repetido = (lot_sums['vec_distintos'] == 1) & (lot_sums['vec_informados'] == lot_sums['n_lotes'])
        lot_sums['imp_total'] = lot_sums['imp_suma'].where(~repetido, lot_sums['vec_max'])
        lot_sums = lot_sums[
            (lot_sums['n_lotes'] >= 2) &
            lot_sums['umbral'].notna() &
            (lot_sums['imp_total'] >= lot_sums['umbral'])
        ]
        sara_expedientes = set(lot_sums.index)
        lot_mask = (~df['_es_sara']) & clave_lote.isin(sara_expedientes)
        df.loc[lot_mask, '_es_sara'] = True
        df.loc[lot_mask, '_sara_por_lotes'] = True

        n_sara_lots = lot_mask.sum()
        print(f"\n  Suma de lotes por expediente:")
        print(f"    Expedientes con lotes que suman >= umbral: {len(sara_expedientes):,}")
        print(f"    Contratos adicionales marcados SARA:       {n_sara_lots:,}")
        print(f"    SARA antes de lotes: {n_sara_before_lots:,} -> despues: {df['_es_sara'].sum():,}")

    df['_sara_por_lotes'] = df['_sara_por_lotes'].astype(bool)

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
        if cnt > 50:
            print(f"    {tc:<25}: {cnt:>8,}")

    print(f"\n  SARA por ano (ultimos 10):")
    for yr in sorted(sara_df['_ano'].unique()):
        yr = int(yr)
        if yr >= 2017:
            cnt = (sara_df['_ano'] == yr).sum()
            print(f"    {yr}: {cnt:,}")

    return df


# ======================================================================
#  2. CARGA TED
# ======================================================================

def load_ted(path):
    """Carga TED: limpia NIFs, normaliza importes."""
    print(f"\n{'='*70}")
    print(f"  CARGA TED")
    print(f"{'='*70}")
    df = pd.read_parquet(path)
    print(f"  Total registros: {len(df):,}")

    for col in ['year', 'number_offers']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Importe TED
    if 'importe_ted' not in df.columns:
        for col in ['award_value', 'value', 'total_value']:
            if col in df.columns:
                df['importe_ted'] = pd.to_numeric(df[col], errors='coerce')
                break

    df['importe_ted'] = pd.to_numeric(df.get('importe_ted', pd.Series(dtype=float)), errors='coerce')

    # NIF ganador limpio
    df['win_nif_clean'] = df.get('win_nationalid', pd.Series(dtype=str)) \
        .fillna('').astype(str).str.strip().str.upper()
    df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)

    valid = df['importe_ted'].notna() & (df['importe_ted'] > 0)
    has_nif = df['win_nif_clean'].str.len() >= 5
    print(f"  Con importe: {valid.sum():,}")
    print(f"  Con NIF ganador: {(valid & has_nif).sum():,}")

    return df


# ======================================================================
#  3. CROSS-VALIDATION (E1 + E2 + E2b)
# ======================================================================

def run_e1_e2(df_placsp, df_ted):
    """E1: NIF adj + importe. E2: expediente + importe. E2b: expediente lotes."""
    print(f"\n{'='*70}")
    print(f"  CROSS-VALIDATION E1 + E2 + E2b")
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

        # _str_or_empty: filas CSV bulk sin campos eForms -> '' (no 'nan'/'None')
        entry = {
            'importe': imp,
            'ted_id': _str_or_empty(row.get('ted_notice_id', '')),
            'n_ofertas': row.get('number_offers', np.nan),
            'cpv_ted': _str_or_empty(row.get('cpv', '')),
            'cae_ted': _str_or_empty(row.get('cae_name', '')),
            'win_size': _str_or_empty(row.get('win_size', '')),
            'direct_award': _str_or_empty(row.get('direct_award_justification', '')),
            'sme_part': _str_or_empty(row.get('sme_participation', '')),
            'buyer_legal_type': _str_or_empty(row.get('buyer_legal_type', '')),
            'duration_lot': row.get('duration_lot', np.nan),
            'award_criterion_type': _str_or_empty(row.get('award_criterion_type', '')),
            'internal_id': _str_or_empty(row.get('internal_id_proc', '')),
            'consumed': False,
        }

        if nif and len(nif) >= 5 and pd.notna(yr):
            ted_lookup[(nif, int(yr))].append(entry)

        # Nulos fuera: con pandas 2 str(None) = 'None' agrupaba todos los avisos
        # sin internal_id (todo el CSV bulk) bajo la clave 'NONE'
        exp_id = entry['internal_id'].strip()
        if exp_id and len(exp_id) >= 4:
            ted_lookup_exp[exp_id.upper()].append(entry)

    print(f"  TED lookup NIF+ano: {len(ted_lookup):,} claves")
    print(f"  TED lookup expediente: {len(ted_lookup_exp):,} claves")

    # -- Solo matchear SARA --
    # E1 needs NIF; E2 needs expediente. We run the loop on all SARA with
    # importe + año, and inside the loop E1 is skipped when NIF is short.
    sara_valid = df_placsp[
        df_placsp['_es_sara'] &
        (df_placsp['_imp_match'] > 0) &
        (df_placsp['_ano'].notna())
    ]
    print(f"\n  SARA candidatos para matching: {len(sara_valid):,}")

    # -- Matching loop E1 + E2 --
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
        imp = row['_imp_match']
        yr = int(row['_ano'])
        t = tol(imp)

        best_match = None
        best_diff = float('inf')
        best_key = None
        best_i = None
        best_lookup = None

        # E1: NIF + importe + año (only if NIF is valid)
        if nif and len(nif) >= 5:
            for yr_offset in range(MATCH_YEAR_WINDOW + 1):
                for yr_try in ([yr + yr_offset, yr - yr_offset] if yr_offset > 0 else [yr]):
                    key = (nif, yr_try)
                    entries = ted_lookup.get(key, [])
                    for i, entry in enumerate(entries):
                        if entry['consumed']:
                            continue
                        diff = abs(entry['importe'] - imp)
                        if diff <= t and diff < best_diff:
                            best_match = entry
                            best_diff = diff
                            best_key = key
                            best_i = i
                            best_lookup = ted_lookup

        # E2: expediente + importe (con pandas 3 el expediente nulo llega como NaN)
        if best_match is None:
            exp_id = _str_or_empty(row['_expediente']).strip().upper()
            if exp_id in _EXP_NULOS:
                exp_id = ''
            if exp_id and len(exp_id) >= 4:
                entries = ted_lookup_exp.get(exp_id, [])
                for i, entry in enumerate(entries):
                    if entry['consumed']:
                        continue
                    diff = abs(entry['importe'] - imp)
                    if diff <= t and diff < best_diff:
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
            match_data[idx] = best_match

    print(f"\n  E1 (NIF adj + importe): {n_match_nif:,}")
    print(f"  E2 (expediente + importe): {n_match_exp:,}")

    # -- E2b: Expediente completo para lotes SARA --
    n_match_exp_lot = 0
    e2b_matched_idx = set()  # Track E2b matches explicitly
    sara_lot_not_matched = df_placsp[
        df_placsp['_es_sara'] &
        df_placsp['_sara_por_lotes'] &
        ~df_placsp.index.isin(matched_idx)
    ]

    if len(sara_lot_not_matched) > 0:
        # Agrupar por (órgano, expediente): el mismo nº de expediente se repite en
        # órganos distintos y un aviso TED no debe validar los lotes de otro órgano
        lot_expedientes = sara_lot_not_matched.groupby(
            [sara_lot_not_matched['organo_contratante'].map(_str_or_empty), '_expediente']
        ).agg(
            indices=('_expediente', lambda x: list(x.index)),
            n_lotes=('_expediente', 'count'),
            imp_total=('_imp_sara', 'sum'),
            nif_organo=('nif_organo', 'first'),
            organo=('organo_contratante', 'first'),
            ano=('_ano', 'first'),
        )

        for (_organo, exp_id), grp in lot_expedientes.iterrows():
            exp_id = _str_or_empty(exp_id)
            if not exp_id or len(exp_id) < 4 or exp_id.strip().upper() in _EXP_NULOS:
                continue
            exp_upper = exp_id.strip().upper()
            entries = ted_lookup_exp.get(exp_upper, [])

            ted_match = None
            for i, entry in enumerate(entries):
                if not entry['consumed']:
                    ted_match = entry
                    ted_match_i = i
                    break

            if ted_match is not None:
                ted_lookup_exp[exp_upper][ted_match_i]['consumed'] = True
                for lot_idx in grp['indices']:
                    if lot_idx not in matched_idx:
                        matched_idx.append(lot_idx)
                        match_data[lot_idx] = ted_match
                        e2b_matched_idx.add(lot_idx)
                        n_match_exp_lot += 1

        print(f"  E2b (expediente lotes): {n_match_exp_lot:,}")

    elapsed = time.time() - t0
    print(f"  Tiempo E1+E2+E2b: {elapsed:.0f}s")
    print(f"  Total matched: {len(matched_idx):,}")

    # Collect consumed TED notice IDs to avoid re-matching in E3-E6
    consumed_ted_ids = set()
    for idx, data in match_data.items():
        tid = data.get('ted_id', '')
        if tid and tid != 'nan':
            consumed_ted_ids.add(tid)

    return matched_idx, match_data, n_match_nif, n_match_exp, n_match_exp_lot, e2b_matched_idx, consumed_ted_ids


# ======================================================================
#  4. MATCHING AVANZADO (E3 + E4 + E5 + E6)
# ======================================================================

def run_advanced_matching(df_sara, df_ted, matched_idx_prev, match_data_prev, consumed_ted_ids):
    """E3-E6: NIF organo, lotes agrupados, nombre organo, propagacion."""
    print(f"\n{'='*70}")
    print(f"  MATCHING AVANZADO (E3 + E3b + E4 + E5 + E7 + E6)")
    print(f"{'='*70}")

    t0 = time.time()

    # Separar matched vs missing
    matched_set = set(matched_idx_prev)
    df_missing = df_sara[
        df_sara['_es_sara'] &
        ~df_sara.index.isin(matched_set) &
        ~df_sara['_es_neg_sin_pub']
    ].copy()
    print(f"  Missing a analizar: {len(df_missing):,}")

    # TED valido — excluir los ya consumed en E1+E2
    ted_valid = df_ted[
        (df_ted['importe_ted'].notna()) & (df_ted['importe_ted'] > 0)
    ].copy()
    if consumed_ted_ids:
        ted_id_col = 'ted_notice_id' if 'ted_notice_id' in ted_valid.columns else None
        if ted_id_col:
            n_before = len(ted_valid)
            ted_valid = ted_valid[~ted_valid[ted_id_col].astype(str).isin(consumed_ted_ids)]
            print(f"  TED excluidos (ya matched E1+E2): {n_before - len(ted_valid):,}")
    ted_valid = ted_valid.reset_index(drop=True)
    ted_consumed = set()

    # ── E3: NIF organo contratante + importe ──
    print(f"\n  --- E3: NIF organo contratante + importe ---")

    ted_by_cae = defaultdict(list)
    for tidx, row in ted_valid.iterrows():
        cae_nif = clean_nif(row.get('cae_nationalid', ''))
        yr = row.get('year', np.nan)
        imp = row['importe_ted']
        if cae_nif and pd.notna(yr):
            ted_by_cae[(cae_nif, int(yr))].append((tidx, imp))

    print(f"  Indice TED por buyer NIF: {len(ted_by_cae):,} claves")

    e3_matched = []
    for idx, row in df_missing.iterrows():
        nif_org = clean_nif(row.get('nif_organo', ''))
        imp = row['_imp_match']
        yr = row['_ano'] if '_ano' in row.index else row['ano']

        if not nif_org or pd.isna(imp) or pd.isna(yr) or imp <= 0:
            continue

        yr = int(yr)
        t = tol(imp)
        best = None
        best_diff = float('inf')

        for yr_try in [yr, yr-1, yr+1]:
            for tidx, ted_imp in ted_by_cae.get((nif_org, yr_try), []):
                if tidx in ted_consumed:
                    continue
                diff = abs(ted_imp - imp)
                if diff <= t and diff < best_diff:
                    best = tidx
                    best_diff = diff

        if best is not None:
            ted_consumed.add(best)
            e3_matched.append((idx, best, best_diff))

    e3_idx = {m[0] for m in e3_matched}
    df_missing_after_e3 = df_missing[~df_missing.index.isin(e3_idx)]
    print(f"  E3 matches: {len(e3_matched):,}")
    print(f"  Missing restante: {len(df_missing_after_e3):,}")

    # ── E4: Lotes agrupados ──
    print(f"\n  --- E4: Lotes agrupados ---")

    ted_by_total = defaultdict(list)
    for tidx, row in ted_valid.iterrows():
        if tidx in ted_consumed:
            continue
        total_val = pd.to_numeric(row.get('total_value', np.nan), errors='coerce')
        est_val = pd.to_numeric(row.get('estimated_value_proc', np.nan), errors='coerce')
        lot_imp = row['importe_ted']
        yr = row.get('year', np.nan)
        cae_nif = clean_nif(row.get('cae_nationalid', ''))
        cae_name = normalize_name(row.get('cae_name', ''))[:40]

        if pd.notna(yr):
            yr = int(yr)
            if cae_nif:
                for val in [total_val, est_val, lot_imp]:
                    if pd.notna(val) and val > 0:
                        ted_by_total[(cae_nif, yr)].append((tidx, val))
            if cae_name and len(cae_name) > 5:
                for val in [total_val, est_val, lot_imp]:
                    if pd.notna(val) and val > 0:
                        ted_by_total[('name:' + cae_name, yr)].append((tidx, val))

    print(f"  Indice TED agrupado: {len(ted_by_total):,} claves")

    ano_col = '_ano' if '_ano' in df_missing_after_e3.columns else 'ano'
    groups = df_missing_after_e3.groupby(['organo_contratante', ano_col]).agg(
        indices=('_imp_match', lambda x: list(x.index)),
        n=('_imp_match', 'count'),
        imp_total=('_imp_match', 'sum'),
        nif_org=('nif_organo', 'first'),
    ).reset_index()
    groups = groups[groups['n'] >= 2].copy()
    print(f"  Grupos organo+ano con >=2 contratos: {len(groups):,}")

    e4_matched_groups = []
    e4_matched_idx = set()

    for _, grp in groups.iterrows():
        nif_org = clean_nif(grp['nif_org'])
        organ_name = normalize_name(grp['organo_contratante'])[:40]
        yr = int(grp[ano_col])
        imp_total = grp['imp_total']
        indices = grp['indices']

        if pd.isna(imp_total) or imp_total <= 0:
            continue

        t = tol(imp_total, MATCH_TOL_PCT_WIDE, MATCH_TOL_ABS_WIDE)
        best = None
        best_diff = float('inf')

        for yr_try in [yr, yr-1, yr+1]:
            if nif_org:
                for tidx, ted_val in ted_by_total.get((nif_org, yr_try), []):
                    if tidx in ted_consumed:
                        continue
                    diff = abs(ted_val - imp_total)
                    if diff <= t and diff < best_diff:
                        best = tidx
                        best_diff = diff
            if best is None and organ_name and len(organ_name) > 5:
                for tidx, ted_val in ted_by_total.get(('name:' + organ_name, yr_try), []):
                    if tidx in ted_consumed:
                        continue
                    diff = abs(ted_val - imp_total)
                    if diff <= t and diff < best_diff:
                        best = tidx
                        best_diff = diff

        if best is not None:
            ted_consumed.add(best)
            e4_matched_groups.append((indices, best, best_diff, imp_total))
            for i in indices:
                e4_matched_idx.add(i)

    df_missing_after_e4 = df_missing_after_e3[~df_missing_after_e3.index.isin(e4_matched_idx)]
    print(f"  E4 grupos matched: {len(e4_matched_groups):,}")
    print(f"  E4 registros cubiertos: {len(e4_matched_idx):,}")
    print(f"  Missing restante: {len(df_missing_after_e4):,}")

    # ── E5: Nombre organo + importe ──
    print(f"\n  --- E5: Nombre organo + importe ---")

    ted_by_name = defaultdict(list)
    for tidx, row in ted_valid.iterrows():
        if tidx in ted_consumed:
            continue
        name = normalize_name(row.get('cae_name', ''))[:40]
        yr = row.get('year', np.nan)
        imp = row['importe_ted']
        if name and len(name) > 5 and pd.notna(yr):
            ted_by_name[(name, int(yr))].append((tidx, imp))

    print(f"  Indice TED por buyer name: {len(ted_by_name):,} claves")

    e5_matched = []
    for idx, row in df_missing_after_e4.iterrows():
        organ = normalize_name(row.get('organo_contratante', ''))[:40]
        imp = row['_imp_match']
        yr = row[ano_col]

        if not organ or len(organ) < 6 or pd.isna(imp) or pd.isna(yr) or imp <= 0:
            continue

        yr = int(yr)
        t = tol(imp)
        best = None
        best_diff = float('inf')

        for yr_try in [yr, yr-1, yr+1]:
            for tidx, ted_imp in ted_by_name.get((organ, yr_try), []):
                if tidx in ted_consumed:
                    continue
                diff = abs(ted_imp - imp)
                if diff <= t and diff < best_diff:
                    best = tidx
                    best_diff = diff

        if best is not None:
            ted_consumed.add(best)
            e5_matched.append((idx, best, best_diff))

    e5_idx = {m[0] for m in e5_matched}
    df_missing_after_e5 = df_missing_after_e4[~df_missing_after_e4.index.isin(e5_idx)]
    print(f"  E5 matches: {len(e5_matched):,}")
    print(f"  Missing restante: {len(df_missing_after_e5):,}")

    # ── E3b: Alias nombre organo + importe ──
    # Para organos cuyo nombre en PLACSP difiere del de TED (ej. ADIF)
    print(f"\n  --- E3b: Alias nombre organo + importe ---")

    # Build alias lookup: para cada alias TED, indexar por (alias_norm[:40], year)
    # Reutilizamos ted_by_name construido para E5, pero necesitamos reconstruir
    # para incluir entradas consumed por E5 que podrian tener alias diferentes
    ted_by_name_full = defaultdict(list)
    for tidx, row in ted_valid.iterrows():
        if tidx in ted_consumed:
            continue
        name = normalize_name(row.get('cae_name', ''))
        yr = row.get('year', np.nan)
        imp = row['importe_ted']
        if name and len(name) > 5 and pd.notna(yr):
            # Index full name and first 40 chars
            ted_by_name_full[(name[:40], int(yr))].append((tidx, imp))

    # Build reverse alias: PLACSP norm name → list of TED norm name keys to try
    alias_lookup = {}
    for placsp_name, ted_names in ORGAN_ALIASES.items():
        alias_lookup[placsp_name[:40]] = [n[:40] for n in ted_names]

    e3b_matched = []
    for idx, row in df_missing_after_e5.iterrows():
        organ = normalize_name(row.get('organo_contratante', ''))[:40]
        imp = row['_imp_match']
        yr = row[ano_col]

        if not organ or pd.isna(imp) or pd.isna(yr) or imp <= 0:
            continue
        if organ not in alias_lookup:
            continue

        yr = int(yr)
        t = tol(imp)
        best = None
        best_diff = float('inf')

        for alias in alias_lookup[organ]:
            for yr_try in [yr, yr-1, yr+1]:
                for tidx, ted_imp in ted_by_name_full.get((alias, yr_try), []):
                    if tidx in ted_consumed:
                        continue
                    diff = abs(ted_imp - imp)
                    if diff <= t and diff < best_diff:
                        best = tidx
                        best_diff = diff

        if best is not None:
            ted_consumed.add(best)
            e3b_matched.append((idx, best, best_diff))

    e3b_idx = {m[0] for m in e3b_matched}
    df_missing_after_e3b = df_missing_after_e5[~df_missing_after_e5.index.isin(e3b_idx)]
    print(f"  Alias definidos: {len(ORGAN_ALIASES)}")
    print(f"  E3b matches: {len(e3b_matched):,}")
    print(f"  Missing restante: {len(df_missing_after_e3b):,}")

    # ── E7: Fuzzy token overlap + importe ──
    # Para organos con nombres parecidos pero no identicos despues de normalizar
    print(f"\n  --- E7: Fuzzy token overlap + importe ---")

    # Build TED name index (full normalized names, not truncated)
    ted_names_full = defaultdict(list)
    for tidx, row in ted_valid.iterrows():
        if tidx in ted_consumed:
            continue
        name = normalize_name(row.get('cae_name', ''))
        yr = row.get('year', np.nan)
        imp = row['importe_ted']
        if name and len(name) > 10 and pd.notna(yr) and pd.notna(imp) and imp > 0:
            ted_names_full[int(yr)].append((tidx, name, imp))

    # Extract unique significant tokens from TED names for fast pre-filtering
    # Build inverted index: token → set of years that have it
    ted_token_index = defaultdict(lambda: defaultdict(list))
    for yr, entries in ted_names_full.items():
        for tidx, name, imp in entries:
            toks = {t for t in name.split() if len(t) > 3 and t not in _STOPWORDS}
            for tok in toks:
                ted_token_index[tok][yr].append((tidx, name, imp))

    print(f"  TED token index: {len(ted_token_index):,} tokens unicos")
    print(f"  TED entries por año: {sum(len(v) for v in ted_names_full.values()):,}")

    e7_matched = []
    e7_checked = 0
    MIN_OVERLAP_RATIO = 0.6
    MIN_OVERLAP_TOKENS = 3

    for idx, row in df_missing_after_e3b.iterrows():
        organ_full = normalize_name(row.get('organo_contratante', ''))
        imp = row['_imp_match']
        yr = row[ano_col]

        if not organ_full or len(organ_full) < 12 or pd.isna(imp) or pd.isna(yr) or imp <= 0:
            continue

        yr = int(yr)
        t = tol(imp)
        organ_toks = {tok for tok in organ_full.split() if len(tok) > 3 and tok not in _STOPWORDS}

        if len(organ_toks) < MIN_OVERLAP_TOKENS:
            continue

        e7_checked += 1

        # Use inverted index: find TED entries that share at least one significant token
        candidate_tids = set()
        for tok in organ_toks:
            for yr_try in [yr, yr-1, yr+1]:
                for entry in ted_token_index[tok].get(yr_try, []):
                    candidate_tids.add(entry)

        best = None
        best_diff = float('inf')
        best_ratio = 0.0

        for tidx, ted_name, ted_imp in candidate_tids:
            if tidx in ted_consumed:
                continue
            diff = abs(ted_imp - imp)
            if diff > t:
                continue
            ratio, n_common = token_overlap(organ_full, ted_name, MIN_OVERLAP_TOKENS)
            if ratio >= MIN_OVERLAP_RATIO and diff < best_diff:
                best = tidx
                best_diff = diff
                best_ratio = ratio

        if best is not None:
            ted_consumed.add(best)
            e7_matched.append((idx, best, best_diff))

    e7_idx = {m[0] for m in e7_matched}
    df_missing_after_e7 = df_missing_after_e3b[~df_missing_after_e3b.index.isin(e7_idx)]
    print(f"  Candidatos analizados: {e7_checked:,}")
    print(f"  E7 matches: {len(e7_matched):,}")
    print(f"  Missing restante: {len(df_missing_after_e7):,}")

    # ── E6: Propagacion por expediente ──
    print(f"\n  --- E6: Propagacion por expediente ---")

    all_matched_idx_pre = matched_set | e3_idx | e4_matched_idx | e5_idx | e3b_idx | e7_idx
    exp_col = '_expediente' if '_expediente' in df_sara.columns else 'expediente'

    # Propagar solo dentro del MISMO órgano: clave (órgano, expediente)
    clave_sara = _clave_organo_expediente(df_sara, exp_col)
    matched_expedientes = set(
        clave_sara[df_sara.index.isin(all_matched_idx_pre)].dropna()
    )

    clave_missing = _clave_organo_expediente(df_missing_after_e7, exp_col)
    e6_candidates = df_missing_after_e7[
        clave_missing.isin(matched_expedientes) & clave_missing.notna()
    ]

    e6_matched_idx = set(e6_candidates.index)

    # Copiar ted_id del primer lote matched del mismo expediente
    e6_ted_ids = {}

    # Build lookup: sara_idx -> ted_id from ALL prior strategies
    all_ted_ids = {}
    # From E1+E2+E2b
    for sidx, mdata in match_data_prev.items():
        tid = mdata.get('ted_id', '')
        if tid and tid != 'nan':
            all_ted_ids[sidx] = tid
    # From E3/E4/E5
    for s_idx_e3, t_idx_e3, _ in e3_matched:
        all_ted_ids[s_idx_e3] = str(ted_valid.loc[t_idx_e3, 'ted_notice_id'])
    for indices_e4, t_idx_e4, _, _ in e4_matched_groups:
        tid_e4 = str(ted_valid.loc[t_idx_e4, 'ted_notice_id'])
        for s_idx_e4 in indices_e4:
            all_ted_ids[s_idx_e4] = tid_e4
    for s_idx_e5, t_idx_e5, _ in e5_matched:
        all_ted_ids[s_idx_e5] = str(ted_valid.loc[t_idx_e5, 'ted_notice_id'])
    for s_idx_e3b, t_idx_e3b, _ in e3b_matched:
        all_ted_ids[s_idx_e3b] = str(ted_valid.loc[t_idx_e3b, 'ted_notice_id'])
    for s_idx_e7, t_idx_e7, _ in e7_matched:
        all_ted_ids[s_idx_e7] = str(ted_valid.loc[t_idx_e7, 'ted_notice_id'])

    # Build lookup: (organo, expediente) -> ted_id (from matched rows)
    exp_to_tid = {}
    matched_claves = clave_sara[df_sara.index.isin(all_matched_idx_pre)].dropna()
    for sidx, exp in matched_claves.items():
        if exp not in exp_to_tid and sidx in all_ted_ids:
            exp_to_tid[exp] = all_ted_ids[sidx]

    if len(e6_matched_idx) > 0:
        for idx in e6_matched_idx:
            exp = clave_missing.loc[idx]
            if exp in exp_to_tid:
                e6_ted_ids[idx] = exp_to_tid[exp]

    df_missing_final = df_missing_after_e7[~df_missing_after_e7.index.isin(e6_matched_idx)]

    print(f"  Expedientes con match previo: {len(matched_expedientes):,}")
    print(f"  E6 lotes propagados: {len(e6_matched_idx):,}")
    print(f"  Missing final: {len(df_missing_final):,}")

    elapsed = time.time() - t0
    print(f"  Tiempo E3-E6: {elapsed:.0f}s")

    return {
        'e3_matched': e3_matched,
        'e3_idx': e3_idx,
        'e3b_matched': e3b_matched,
        'e3b_idx': e3b_idx,
        'e4_matched_groups': e4_matched_groups,
        'e4_matched_idx': e4_matched_idx,
        'e5_matched': e5_matched,
        'e5_idx': e5_idx,
        'e7_matched': e7_matched,
        'e7_idx': e7_idx,
        'e6_matched_idx': e6_matched_idx,
        'e6_ted_ids': e6_ted_ids,
        'df_missing_final': df_missing_final,
        'df_missing_after_e3': df_missing_after_e3,
        'df_missing_after_e4': df_missing_after_e4,
        'df_missing_after_e5': df_missing_after_e5,
        'df_missing_after_e3b': df_missing_after_e3b,
        'df_missing_after_e7': df_missing_after_e7,
        'ted_valid': ted_valid,
    }


# ======================================================================
#  5. APLICAR RESULTADOS + RESUMEN
# ======================================================================

def apply_results_and_report(df_placsp, matched_idx, match_data,
                              n_e1, n_e2, n_e2b, e2b_matched_idx, adv):
    """Marca matched/missing, genera resumen, guarda outputs."""
    print(f"\n{'='*70}")
    print(f"  RESUMEN CONSOLIDADO")
    print(f"{'='*70}")

    ted_valid = adv['ted_valid']

    # -- Marcar E1/E2/E2b --
    df_placsp['_ted_validated'] = False
    df_placsp['_match_strategy'] = ''

    # Enrich fields
    enrich_defaults = {
        '_ted_id': '', '_ted_cpv': '', '_ted_win_size': '',
        '_ted_direct_award': '', '_ted_sme_part': '',
        '_ted_buyer_legal_type': '', '_ted_award_criterion': '',
        '_ted_internal_id': '',
    }
    for col, default in enrich_defaults.items():
        df_placsp[col] = default
    df_placsp['_ted_n_ofertas'] = np.nan
    df_placsp['_ted_duration'] = np.nan

    for idx in matched_idx:
        df_placsp.loc[idx, '_ted_validated'] = True
        # Asignar estrategia correcta
        if idx in e2b_matched_idx:
            df_placsp.loc[idx, '_match_strategy'] = 'E2b_exp_lotes'
        else:
            df_placsp.loc[idx, '_match_strategy'] = 'E1_E2'
        if idx in match_data:
            m = match_data[idx]
            df_placsp.loc[idx, '_ted_id'] = m.get('ted_id', '')
            df_placsp.loc[idx, '_ted_n_ofertas'] = pd.to_numeric(m.get('n_ofertas', np.nan), errors='coerce')
            df_placsp.loc[idx, '_ted_cpv'] = m.get('cpv_ted', '')
            df_placsp.loc[idx, '_ted_win_size'] = m.get('win_size', '')
            df_placsp.loc[idx, '_ted_direct_award'] = m.get('direct_award', '')
            df_placsp.loc[idx, '_ted_sme_part'] = m.get('sme_part', '')
            df_placsp.loc[idx, '_ted_buyer_legal_type'] = m.get('buyer_legal_type', '')
            df_placsp.loc[idx, '_ted_duration'] = pd.to_numeric(m.get('duration_lot', np.nan), errors='coerce')
            df_placsp.loc[idx, '_ted_award_criterion'] = m.get('award_criterion_type', '')
            df_placsp.loc[idx, '_ted_internal_id'] = m.get('internal_id', '')

    # -- Marcar E3 --
    ted_enrich_cols = {
        '_ted_n_ofertas': 'number_offers',
        '_ted_cpv': 'cpv',
        '_ted_win_size': 'win_size',
        '_ted_direct_award': 'direct_award_justification',
        '_ted_sme_part': 'sme_participation',
        '_ted_buyer_legal_type': 'buyer_legal_type',
        '_ted_duration': 'duration_lot',
        '_ted_award_criterion': 'award_criterion_type',
        '_ted_internal_id': 'internal_id_proc',
    }

    def _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols):
        """Enrich PLACSP row with TED fields from ted_valid DataFrame."""
        df_placsp.loc[s_idx, '_ted_id'] = str(ted_valid.loc[t_idx, 'ted_notice_id'])
        for dest_col, src_col in ted_enrich_cols.items():
            if src_col in ted_valid.columns:
                val = ted_valid.loc[t_idx, src_col]
                if dest_col in ('_ted_n_ofertas', '_ted_duration'):
                    df_placsp.loc[s_idx, dest_col] = pd.to_numeric(val, errors='coerce')
                else:
                    df_placsp.loc[s_idx, dest_col] = str(val) if pd.notna(val) else ''

    for s_idx, t_idx, _ in adv['e3_matched']:
        df_placsp.loc[s_idx, '_match_strategy'] = 'E3_nif_org'
        df_placsp.loc[s_idx, '_ted_validated'] = True
        _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols)

    # -- Marcar E4 --
    for indices, t_idx, _, _ in adv['e4_matched_groups']:
        for s_idx in indices:
            df_placsp.loc[s_idx, '_match_strategy'] = 'E4_lotes'
            df_placsp.loc[s_idx, '_ted_validated'] = True
            _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols)

    # -- Marcar E5 --
    for s_idx, t_idx, _ in adv['e5_matched']:
        df_placsp.loc[s_idx, '_match_strategy'] = 'E5_nombre'
        df_placsp.loc[s_idx, '_ted_validated'] = True
        _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols)

    # -- Marcar E3b --
    for s_idx, t_idx, _ in adv['e3b_matched']:
        df_placsp.loc[s_idx, '_match_strategy'] = 'E3b_alias'
        df_placsp.loc[s_idx, '_ted_validated'] = True
        _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols)

    # -- Marcar E7 --
    for s_idx, t_idx, _ in adv['e7_matched']:
        df_placsp.loc[s_idx, '_match_strategy'] = 'E7_fuzzy'
        df_placsp.loc[s_idx, '_ted_validated'] = True
        _enrich_from_ted_valid(df_placsp, s_idx, t_idx, ted_valid, ted_enrich_cols)

    # -- Marcar E6 --
    for s_idx in adv['e6_matched_idx']:
        df_placsp.loc[s_idx, '_match_strategy'] = 'E6_propagacion'
        df_placsp.loc[s_idx, '_ted_validated'] = True
        if s_idx in adv['e6_ted_ids']:
            df_placsp.loc[s_idx, '_ted_id'] = adv['e6_ted_ids'][s_idx]

    # -- Missing flags --
    df_placsp['_ted_missing'] = (
        df_placsp['_es_sara'] &
        ~df_placsp['_ted_validated'] &
        ~df_placsp['_es_neg_sin_pub']
    )
    df_placsp['_ted_missing_incl_neg'] = (
        df_placsp['_es_sara'] &
        ~df_placsp['_ted_validated']
    )

    # -- Counts --
    n_sara = df_placsp['_es_sara'].sum()
    n_e3 = len(adv['e3_matched'])
    n_e3b = len(adv['e3b_matched'])
    n_e4 = len(adv['e4_matched_idx'])
    n_e5 = len(adv['e5_matched'])
    n_e7 = len(adv['e7_matched'])
    n_e6 = len(adv['e6_matched_idx'])
    n_total = n_e1 + n_e2 + n_e2b + n_e3 + n_e3b + n_e4 + n_e5 + n_e7 + n_e6
    n_missing = df_placsp['_ted_missing'].sum()
    n_missing_incl_neg = df_placsp['_ted_missing_incl_neg'].sum()
    n_neg = df_placsp[df_placsp['_es_sara'] & df_placsp['_es_neg_sin_pub'] & ~df_placsp['_ted_validated']].shape[0]

    print(f"\n  Total SARA: {n_sara:,}")
    print(f"")
    print(f"  E1 NIF adj. + importe:      {n_e1:>8,}  ({n_e1/n_sara*100:>5.1f}%)")
    print(f"  E2 Expediente + importe:    {n_e2:>8,}  ({n_e2/n_sara*100:>5.1f}%)")
    print(f"  E2b Expediente lotes:       {n_e2b:>8,}  ({n_e2b/n_sara*100:>5.1f}%)")
    print(f"  E3 NIF organo + importe:    {n_e3:>8,}  ({n_e3/n_sara*100:>5.1f}%)")
    print(f"  E3b Alias nombre + imp.:    {n_e3b:>8,}  ({n_e3b/n_sara*100:>5.1f}%)")
    print(f"  E4 Lotes agrupados:         {n_e4:>8,}  ({n_e4/n_sara*100:>5.1f}%)")
    print(f"  E5 Nombre organo + imp.:    {n_e5:>8,}  ({n_e5/n_sara*100:>5.1f}%)")
    print(f"  E7 Fuzzy token + imp.:      {n_e7:>8,}  ({n_e7/n_sara*100:>5.1f}%)")
    print(f"  E6 Propagacion expediente:  {n_e6:>8,}  ({n_e6/n_sara*100:>5.1f}%)")
    print(f"  {'─'*46}")
    print(f"  Total matched:              {n_total:>8,}  ({n_total/n_sara*100:>5.1f}%)")
    print(f"  Neg. sin pub. (no matched):     {n_neg:>8,}  ({n_neg/n_sara*100:>5.1f}%)")
    print(f"  Missing (excl. neg s/p):    {n_missing:>8,}  ({n_missing/n_sara*100:>5.1f}%)")
    print(f"  Missing (incl. neg s/p):    {n_missing_incl_neg:>8,}  ({n_missing_incl_neg/n_sara*100:>5.1f}%)")

    # -- Por año --
    df_missing_final = adv['df_missing_final']
    ano_col = '_ano' if '_ano' in df_placsp.columns else 'ano'

    print(f"\n  {'Ano':>6} {'SARA':>8} {'Match':>8} {'%':>6} {'Missing':>8}")
    print(f"  {'-'*42}")
    for yr in sorted(df_placsp[df_placsp['_es_sara']][ano_col].dropna().unique()):
        yr = int(yr)
        if yr < 2010 or yr > 2026:
            continue
        yr_sara = df_placsp[(df_placsp['_es_sara']) & (df_placsp[ano_col] == yr)]
        yr_match = yr_sara['_ted_validated'].sum()
        yr_miss = yr_sara['_ted_missing'].sum()
        pct = yr_match / max(len(yr_sara), 1) * 100
        print(f"  {yr:>6} {len(yr_sara):>8,} {yr_match:>8,} {pct:>5.1f}% {yr_miss:>8,}")

    # -- Por tipo contrato --
    print(f"\n  Matching por tipo contrato:")
    tc_col = '_tipo_contrato' if '_tipo_contrato' in df_placsp.columns else 'tipo_contrato'
    for tc in ['Obras', 'Servicios', 'Suministros']:
        tc_sara = df_placsp[(df_placsp['_es_sara']) & (df_placsp[tc_col] == tc)]
        tc_match = tc_sara['_ted_validated'].sum()
        tc_miss = tc_sara['_ted_missing'].sum()
        pct = tc_match / max(len(tc_sara), 1) * 100
        print(f"    {tc:<15}: {len(tc_sara):>8,} SARA | "
              f"{tc_match:>6,} match ({pct:.0f}%) | {tc_miss:>6,} miss")

    # -- Missing alta confianza --
    all_matched_organs = set(df_placsp[df_placsp['_ted_validated']]['organo_contratante'].dropna().unique())
    df_missing_final = df_missing_final.copy()
    df_missing_final['_organ_in_ted'] = df_missing_final['organo_contratante'].isin(all_matched_organs)

    hc = df_missing_final[
        df_missing_final['_organ_in_ted'] &
        (df_missing_final[ano_col] >= 2016) &
        (df_missing_final['_imp_match'] >= 221_000)
    ]
    lc = df_missing_final[~df_missing_final.index.isin(hc.index)]

    print(f"\n  MISSING - Clasificacion:")
    print(f"    Alta confianza (organo TED + >=2016 + >=221K): {len(hc):,}")
    print(f"    Baja confianza (resto):                        {len(lc):,}")

    # Distribucion importes missing
    print(f"\n  Distribucion importes missing:")
    for lo, hi, label in [
        (0, 221_000, "<221K (zona gris / lotes individuales)"),
        (221_000, 500_000, "221K-500K"),
        (500_000, 1_000_000, "500K-1M"),
        (1_000_000, 5_000_000, "1M-5M"),
        (5_000_000, float('inf'), ">=5M"),
    ]:
        n = ((df_missing_final['_imp_match'] >= lo) & (df_missing_final['_imp_match'] < hi)).sum()
        print(f"    {label:<40}: {n:>8,}")

    if len(hc) > 0:
        print(f"\n  Top 15 organos - MISSING ALTA CONFIANZA:")
        top_hc = hc.groupby('organo_contratante').agg(
            n=('_imp_match', 'count'),
            imp=('_imp_match', 'sum'),
        ).sort_values('n', ascending=False).head(15)
        for organ, r in top_hc.iterrows():
            print(f"    {str(organ)[:55]:<55}: {int(r['n']):>5,} | {r['imp']/1e6:.0f}M")

    # -- Missing a nivel expediente --
    exp_col = '_expediente' if '_expediente' in df_placsp.columns else 'expediente'
    sara_all = df_placsp[df_placsp['_es_sara']].copy()
    # Un expediente = (órgano, nº expediente): el mismo número se repite entre órganos
    exp_stats = sara_all.groupby(_clave_organo_expediente(sara_all, exp_col)).agg(
        n_lotes=('_es_sara', 'count'),
        any_matched=('_ted_validated', 'any'),
        es_lotes=('_sara_por_lotes', 'any'),
    )
    n_exp_total = len(exp_stats)
    n_exp_matched = exp_stats['any_matched'].sum()
    n_exp_missing = n_exp_total - n_exp_matched

    print(f"\n  MISSING A NIVEL EXPEDIENTE:")
    print(f"    Expedientes SARA totales:  {n_exp_total:,}")
    print(f"    Con match TED:             {n_exp_matched:,} ({n_exp_matched/max(n_exp_total,1)*100:.1f}%)")
    print(f"    Missing:                   {n_exp_missing:,} ({n_exp_missing/max(n_exp_total,1)*100:.1f}%)")

    return df_placsp, df_missing_final, hc


# ======================================================================
#  6. GUARDAR
# ======================================================================

def save_outputs(df_placsp, df_missing_final, hc):
    """Guarda parquets de resultados."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    save_cols = [
        'expediente', 'organo_contratante', 'nif_organo', 'dependencia',
        'nif_adjudicatario', 'adjudicatario', 'importe_adjudicacion',
        'valor_estimado_contrato', 'importe_sin_iva', 'ano', 'estado', 'conjunto', 'tipo_contrato',
        'procedimiento', 'cpv_principal', 'fecha_adjudicacion',
        '_es_sara', '_umbral_sara', '_imp_sara', '_imp_match',
        '_sara_por_lotes', '_is_age', '_is_sector',
        '_es_neg_sin_pub', '_tipo_contrato', '_procedimiento',
        '_ted_validated', '_ted_missing', '_ted_missing_incl_neg',
        '_match_strategy',
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
    miss_cols = [c for c in cols_exist if c in df_missing_final.columns]
    df_missing_final[miss_cols].to_parquet(OUTPUT_DIR / "crossval_missing.parquet", index=False)
    print(f"  Missing: {OUTPUT_DIR / 'crossval_missing.parquet'} ({len(df_missing_final):,})")

    # Missing alta confianza
    if len(hc) > 0:
        hc_cols = [c for c in miss_cols if c in hc.columns]
        hc[hc_cols].to_parquet(OUTPUT_DIR / "missing_alta_confianza.parquet", index=False)
        print(f"  Missing HC: {OUTPUT_DIR / 'missing_alta_confianza.parquet'} ({len(hc):,})")


# ======================================================================
#  MAIN
# ======================================================================

if __name__ == "__main__":
    t_start = time.time()

    # 1. Cargar datos
    df_placsp = load_placsp(PLACSP_PATH)
    df_ted = load_ted(TED_PATH)

    # 2. E1 + E2 + E2b
    matched_idx, match_data, n_e1, n_e2, n_e2b, e2b_matched_idx, consumed_ted_ids = run_e1_e2(df_placsp, df_ted)

    # 3. E3 + E4 + E5 + E6
    adv = run_advanced_matching(df_placsp, df_ted, matched_idx, match_data, consumed_ted_ids)

    # 4. Aplicar resultados + resumen
    df_placsp, df_missing_final, hc = apply_results_and_report(
        df_placsp, matched_idx, match_data, n_e1, n_e2, n_e2b, e2b_matched_idx, adv
    )

    # 5. Guardar
    save_outputs(df_placsp, df_missing_final, hc)

    elapsed = time.time() - t_start
    print(f"\n  Pipeline completo en {elapsed:.0f}s")
    print(f"  Cross-validation completado.")