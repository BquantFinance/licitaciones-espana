"""
═══════════════════════════════════════════════════════════════════════════════
  MÓDULO TED — v6.0
  
  Parte 1: Descargador de datos TED (Contract Award Notices) para España
  Parte 2: Cross-validation TED ↔ PLACSP/PSCP
  
  Fuentes de datos:
    - CSV bulk: data.europa.eu (2006-2023, formato legacy)
    - TED Search API v3: api.ted.europa.eu (2024+, eForms)
    - TED SPARQL: data.ted.europa.eu (alternativa)
  
  Cambios v6.0 respecto a v5.5:
    - API endpoint correcto: POST https://api.ted.europa.eu/v3/notices/search
    - Body params: "query" (no "q"), "page"/"limit" (no "pageNum"/"pageSize")
    - scope="ALL" (string, no int)
    - Query syntax eForms sin corchetes: notice-type IN (...) 
    - Fields eForms descubiertos: winner-identifier, tender-value, etc.
    - Parser adaptado a estructura real de respuesta API (multi-lot)
    - Eliminado endpoint legacy v3.0 (404 permanente)
    - Eliminada descarga CSV consolidado (404)
  
  Uso:
    1. Ejecutar download_ted_spain() para obtener ted_es_can.parquet
    2. Integrar cross_validate_ted() en el pipeline principal

  Histórico (sesgo del superviviente, comun/historico.py): las cachés por año
  y el consolidado no se machacan (la versión anterior va a _historico/) y
  ted_es_can.parquet conserva lo que TED retira o cambia con
  _en_ultima_descarga=False. --semilla <ted_es_can.parquet publicado> añade
  los avisos del release v2026.02 que ya no están en la descarga.
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import re
import sys
import time
import json
import math
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import pandas as pd
import numpy as np
import pyarrow.parquet as pq

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# comun/historico.py (sesgo del superviviente) desde la raíz del repo, con
# cualquier cwd
_RAIZ_REPO = str(Path(__file__).resolve().parents[1])
if _RAIZ_REPO not in sys.path:
    sys.path.insert(0, _RAIZ_REPO)
from comun.historico import (  # noqa: E402
    COLUMNAS_META, HISTORICO, acumular, guardar_registros, imprimir_informe_semilla, sembrar,
    versiones,
)

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════

class TEDConfig:
    """Configuración del módulo TED."""
    
    # ── Directorios ──
    # Carpeta ted/ del repo (junto a este script), como documenta el README y
    # donde lee run_ted_crossvalidation.py; no depende del cwd
    DATA_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = Path("output")
    
    # ── Filtro geográfico ──
    COUNTRY_CODE = "ES"  # España
    
    # ── CSV bulk ──
    # URLs del dataset CSV en data.europa.eu
    # Formato nuevo: "TED%202020/TED%20-%20Contract%20award%20notices%20{year}.csv"
    CSV_BASE_URL = "https://data.europa.eu/euodp/repository/ec/dg-grow/mapps"
    # Distribución actual del dataset "ted-csv" (un ZIP con un único CSV por año,
    # mismas columnas). Es la única que cubre 2020-2023: las URL de "TED 2020"
    # llegan a 2019 y esos años acababan en la API, que para los avisos
    # anteriores a eForms no devuelve adjudicatario, importe ni nº de ofertas
    # (0 % en 2020-2022 del ted_es_can.parquet publicado).
    CSV_HUB_URL = "https://data.europa.eu/api/hub/store/data/ted-contract-award-notices-{year}.zip"
    CSV_YEARS_AVAILABLE = range(2006, 2024)  # CSV llega hasta ~2023

    # ── TED Search API v3 (2024+, eForms) ──
    # Endpoint correcto (verificado feb 2026):
    #   POST https://api.ted.europa.eu/v3/notices/search
    #   Body JSON: { "query": "...", "fields": [...], "page": 1, "limit": 100, "scope": "ALL" }
    # Límites documentados: 250 avisos por página, 10.000 "campos por página"
    # (avisos x campos pedidos) y 15.000 avisos por consulta en modo PAGE_NUMBER.
    TED_API_SEARCH = "https://api.ted.europa.eu/v3/notices/search"
    TED_API_PAGE_SIZE = 100  # 100 x len(API_FIELDS) debe quedar < 10.000 campos por página
    TED_API_RATE_LIMIT = 1.0  # Segundos entre requests (0.5 causa 429)

    # ── Tipos de aviso (notice-type) ──
    # Todos los del tipo de documento CAN del eForms SDK (codelists/notice-type.gc
    # y notice-types.json): subtipos 25-28 veat (transparencia ex ante:
    # adjudicaciones sin licitación previa), 29-32 y E4 can-standard, 33-35
    # can-social, 36-37 can-desg, 38-40 y E6 can-modif, E5 compl (finalización)
    # y T02 can-tran (transporte de viajeros). Antes faltaban veat, can-tran y compl.
    API_NOTICE_TYPES = (
        "can-standard", "can-social", "can-modif", "can-desg",
        "can-tran", "veat", "compl",
    )

    # ── Campos eForms para la API ──
    # Descubiertos via error-mining del endpoint (feb 2026)
    # Solo can-standard/can-social devuelven winner/tender data
    # Un solo nombre desconocido hace fallar toda la consulta (HTTP 400)
    API_FIELDS = [
        "publication-number",
        "notice-type",
        # ── Aviso / procedimiento (sin ellos las filas de la API no tenían
        #    fecha de publicación, tipo de contrato ni procedimiento) ──
        "publication-date",
        "notice-subtype",
        "form-type",
        "procedure-type",
        "contract-nature-main-proc",
        "place-of-performance",
        # ── Importe ──
        "tender-value",
        "tender-value-cur",
        "tender-value-highest",
        "tender-value-lowest",
        "result-value-lot",
        "result-value-cur-lot",
        "result-value-notice",
        "result-value-cur-notice",
        "estimated-value-lot",
        "estimated-value-cur-lot",
        "estimated-value-proc",
        "estimated-value-cur-proc",
        "total-value",
        "total-value-cur",
        # ── Ganador ──
        "winner-name",
        "winner-identifier",
        "winner-country",
        "winner-decision-date",
        "winner-city",
        "winner-size",                  # PYME / grande
        "winner-listed",                # ¿Cotizada?
        "winner-owner-nationality",     # Nacionalidad propietario
        "winner-selection-status",      # Estado selección
        # ── Comprador ──
        "buyer-name",
        "buyer-identifier",
        "buyer-country",
        "buyer-city",
        "buyer-legal-type",             # Tipo jurídico (para umbrales UE)
        "buyer-contracting-entity",     # ¿Sectorial?
        "buyer-profile",                # URL perfil contratante
        # ── Clasificación ──
        "classification-cpv",
        # ── Ofertas ──
        "received-submissions-type-val",
        "received-submissions-type-code",
        # ── IDs / Linking ──
        "contract-identifier",
        "tender-identifier",
        "procedure-identifier",         # ID procedimiento TED
        "internal-identifier-proc",     # Nº expediente interno (!)
        "internal-identifier-lot",      # ID lote interno
        "identifier-lot",               # ID lote
        "result-lot-identifier",        # ID lote resultado
        "tender-lot-identifier",        # ID lote oferta
        "modification-previous-notice-identifier",  # Notice previa (modificados)
        # ── Procedimiento ──
        "direct-award-justification-proc",  # Justificación negociado s/p
        "direct-award-justification-text-proc",
        "non-award-justification",      # Justificación no-adjudicación
        "sme-part",                     # ¿Participación PYME?
        # ── Contrato ──
        "duration-period-value-lot",    # Duración contrato
        "subcontracting-value",         # Valor subcontratación
        "subcontracting-value-cur",
        # ── Criterios adjudicación ──
        "award-criterion-type-lot",     # Precio vs calidad
        "award-criterion-number-weight-lot",  # Peso criterio (%)
        # ── Framework ──
        "framework-estimated-value",
        "framework-maximum-value-lot",
        # ── Empresa ──
        "business-country",
        "business-identifier",
    ]
    
    # ── Campos a extraer del CSV bulk ──
    # Se guardan TODAS las columnas del CSV de las filas de España (título, nº de
    # contrato, URL del aviso, PYME, criterios, ofertas por tipo...). Con False,
    # solo las de CSV_COLUMNS_KEEP (lo que se hacía antes: 27 columnas).
    CSV_KEEP_ALL_COLUMNS = True
    CSV_COLUMNS_KEEP = [
        'ID_NOTICE_CAN', 'YEAR', 'ISO_COUNTRY_CODE',
        'CAE_NAME', 'CAE_NATIONALID', 'CAE_TYPE', 'CAE_TOWN',
        'TAL_LOCATION_NUTS', 'TYPE_OF_CONTRACT', 'CPV',
        'TOP_TYPE',
        'VALUE_EURO_FIN_1',
        'AWARD_VALUE_EURO_FIN_1',
        'WIN_NAME', 'WIN_NATIONALID', 'WIN_COUNTRY_CODE',
        'NUMBER_OFFERS', 'NUMBER_AWARDS',
        'DT_DISPATCH', 'DT_AWARD',
        'B_FRA_AGREEMENT', 'CANCELLED',
        'ID_AWARD', 'ID_LOT_AWARDED',
        'ADDITIONAL_CPV', 'LOTS_NUMBER',
        # Importes sin el sufijo _FIN_1 (CSV de data.europa.eu/api/hub): respaldo
        # de importe_ted si en algún año faltan las columnas *_FIN_1
        'VALUE_EURO', 'AWARD_VALUE_EURO',
    ]
    
    # ── Umbrales UE para España (2024, sin IVA) ──
    EU_THRESHOLD_WORKS = 5_382_000
    EU_THRESHOLD_SUPPLIES_CENTRAL = 140_000
    EU_THRESHOLD_SUPPLIES_SUB = 215_000
    EU_THRESHOLD_SERVICES_CENTRAL = 140_000
    EU_THRESHOLD_SERVICES_SUB = 215_000
    EU_THRESHOLD_UTILITIES = 431_000
    EU_THRESHOLD_MIN = 140_000
    
    # ── Cross-validation ──
    MATCH_TOLERANCE_PCT = 0.10  # ±10% del importe
    MATCH_TOLERANCE_ABS = 5_000  # O ±5000€
    MATCH_YEAR_WINDOW = 1  # ±1 año para matching temporal


# ═══════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════

log = logging.getLogger('ted_module')


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 1: DESCARGA DE DATOS TED
# ═══════════════════════════════════════════════════════════════════════════

def download_ted_spain(
    years=None,
    force_redownload=False,
    output_path=None,
    semillas=(),
):
    """
    Descarga y combina datos TED de España.
    
    Estrategia dual:
      - 2006-2023: CSV bulk (legacy format, columnas tipo WIN_NAME, CAE_NATIONALID)
      - 2024+: TED Search API v3 (eForms, campos tipo winner-identifier)

    Sin perder lo que TED retira o cambia (ver HISTÓRICO DE DESCARGAS): las
    cachés por año no se machacan y el consolidado se construye desde todas
    sus versiones, con _primera_descarga, _ultima_descarga y
    _en_ultima_descarga. Incluye también los años con caché de ejecuciones
    anteriores aunque ahora no se pidan (de ellos no se retira nada).

    semillas: parquets publicados (p.ej. el ted_es_can.parquet de v2026.02)
    de los que se añaden, por aviso, los que no están en la descarga (_origen).
    
    Returns:
        pd.DataFrame con todos los CAN de España
    """
    if not HAS_REQUESTS:
        log.error("Necesitas: pip install requests")
        return None
    
    TEDConfig.DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if output_path is None:
        output_path = TEDConfig.DATA_DIR / "ted_es_can.parquet"
    output_path = Path(output_path)

    if years is None:
        years = _default_years()

    faltan = [str(s) for s in semillas if not Path(s).is_file()]
    if faltan:
        log.error(f"No existe la semilla: {', '.join(faltan)}")
        return None

    if output_path.exists() and not force_redownload and not semillas:
        log.info(f"Cargando cache: {output_path}")
        cached = _read_cache(output_path)
        # Solo sirve si tiene todos los años pedidos y ninguno seguía abierto al
        # guardarla (antes se devolvía siempre: el año en curso quedaba congelado
        # y los años nuevos no se descargaban nunca). Con semillas se reconstruye.
        if cached is not None and _cache_covers_years(cached, output_path, years):
            return cached
        if cached is not None:
            log.info(f"  Cache {output_path.name} incompleta para {years[0]}-{years[-1]}: se reconstruye")

    descargas = []         # Lo obtenido en esta ejecución (solo se usa si queda incompleta)
    fuentes = {}           # Año → caché de la que sale en esta ejecución ('csv' o 'api')
    incomplete_years = []  # Años con descarga API cortada por errores/límite
    
    # ── CSV bulk para años disponibles, API para el resto ──
    csv_years = [y for y in years if y in TEDConfig.CSV_YEARS_AVAILABLE]
    api_years = [y for y in years if y not in TEDConfig.CSV_YEARS_AVAILABLE]
    
    # Años que fallan en CSV se reintentan por API
    csv_failed_years = []
    
    if csv_years:
        log.info(f"📥 Descargando CSV bulk para {csv_years[0]}-{csv_years[-1]}...")
        for year in csv_years:
            df_year = _download_csv_year(year, force_redownload)
            if df_year is not None and len(df_year) > 0:
                descargas.append(df_year)
                fuentes[year] = 'csv'
            else:
                csv_failed_years.append(year)
    
    # Años que no tienen CSV + años que fallaron en CSV → API
    api_years = sorted(set(api_years + csv_failed_years))
    
    if api_years:
        log.info(f"🌐 Consultando TED API para {api_years}...")
        for year in api_years:
            df_year = _download_api_year(year, force_redownload)
            if df_year is not None and df_year.attrs.get('descarga_incompleta'):
                incomplete_years.append(year)
            if df_year is not None and len(df_year) > 0:
                descargas.append(df_year)
                fuentes[year] = 'api'
    
    if not descargas:
        log.error("No se obtuvieron datos de ninguna fuente")
        return None
    
    if incomplete_years:
        # No se guarda nada: un consolidado truncado se reutilizaría como cache y
        # generaría falsos "missing in TED", y una descarga cortada no debe
        # retirar avisos. Se devuelve lo descargado, como antes
        df = pd.concat([_renombrar_csv(d) for d in descargas], ignore_index=True)
        log.info(f"Total registros brutos: {len(df):,}")
        df = _normalize_ted_data(df)
        log.error(f"⚠️ Descarga TED INCOMPLETA para {incomplete_years} (errores de la API): "
                  f"no se guarda {output_path}. Vuelve a ejecutar la descarga.")
        _print_ted_summary(df)
        return df

    # ── Consolidado desde todas las versiones de las cachés (y las semillas) ──
    df = _consolidar(fuentes, output_path, semillas)
    if df is None:
        log.error("Ninguna caché legible para construir el consolidado")
        return None
    estado = guardar_registros(df, output_path)   # la versión anterior queda en _historico/
    log.info(f"✅ Guardado ({estado}): {output_path} ({len(df):,} registros)")
    
    _print_ted_summary(df)
    
    return df


def _read_cache(path):
    """Lee un parquet de cache; None si no es legible (p.ej. puntero Git LFS sin descargar)."""
    try:
        return pd.read_parquet(path)
    except Exception as e:
        log.warning(f"  Cache ilegible {path} ({e}); se vuelve a descargar")
        return None


def _current_year():
    return datetime.now().year


def _default_years():
    """Todos los años que ofrecen las fuentes: primer año del CSV bulk → año en curso."""
    return list(range(TEDConfig.CSV_YEARS_AVAILABLE.start, _current_year() + 1))


def _cache_closed_for_year(path, year):
    """True si la cache se escribió después de terminar 'year'.

    TED publica avisos a diario: una cache guardada mientras el año seguía en
    curso solo tiene los avisos publicados hasta ese día.
    """
    try:
        return datetime.fromtimestamp(Path(path).stat().st_mtime).year > year
    except OSError:
        return False


def _cache_covers_years(df, path, years):
    """La cache consolidada sirve si contiene todos los años pedidos y ninguno
    seguía abierto cuando se guardó."""
    if not years or 'year' not in df.columns:
        return False
    cached_years = set(pd.to_numeric(df['year'], errors='coerce').dropna().astype(int))
    return set(years) <= cached_years and _cache_closed_for_year(path, max(years))


# ═══════════════════════════════════════════════════════════════════════════
#  HISTÓRICO DE DESCARGAS (sesgo del superviviente, comun/historico.py)
# ═══════════════════════════════════════════════════════════════════════════
#
# TED retira y corrige avisos. Antes, al volver a descargar un año (--force,
# caché de una versión anterior o guardada con el año abierto) su caché y el
# consolidado se sobrescribían y lo retirado desaparecía. Ahora:
#   - Cada caché por año se guarda con guardar_version: si trae las mismas
#     filas no se toca; si cambió, la anterior pasa a _historico/. Una
#     descarga incompleta, fallida o vacía no se guarda (no crea versión).
#   - El año en curso no se guarda como caché (se reutilizaría como si
#     estuviera completo) sino en ted_can_{año}_ES_api_en_curso.parquet, que
#     solo sirve de versión para el histórico.
#   - ted_es_can.parquet se construye con el código actual desde todas las
#     versiones de cada año (acumular): lo que TED ya no sirve queda con
#     _en_ultima_descarga=False. Cada año acumula solo sus versiones (ese es
#     su ámbito): un año que no se vuelve a descargar no retira nada.
#     _primera_descarga y _ultima_descarga son fechas de versión (el sello de
#     _historico/ o la fecha del fichero): una descarga idéntica no crea
#     versión ni cambia el consolidado.
#   - Semillas (--semilla): avisos del publicado que no están en la descarga,
#     por aviso (clave_aviso), con _origen='release v2026.02'.

# Columnas del CSV bulk (MAYÚSCULAS) → nombres de la API (minúsculas), para que
# las dos fuentes se fusionen al concatenar
_CSV_A_API = {
    'ID_NOTICE_CAN': 'ted_notice_id',
    'YEAR': 'year',
    'ISO_COUNTRY_CODE': 'iso_country',
    'CAE_NAME': 'cae_name',
    'CAE_NATIONALID': 'cae_nationalid',
    'CAE_TYPE': 'cae_type',
    'CAE_TOWN': 'cae_town',
    'TAL_LOCATION_NUTS': 'nuts',
    'TYPE_OF_CONTRACT': 'type_of_contract',
    'CPV': 'cpv',
    'ADDITIONAL_CPV': 'cpv_additional',
    'TOP_TYPE': 'top_type',
    'VALUE_EURO_FIN_1': 'value_euro',
    'AWARD_VALUE_EURO_FIN_1': 'award_value_euro',
    'WIN_NAME': 'win_name',
    'WIN_NATIONALID': 'win_nationalid',
    'WIN_COUNTRY_CODE': 'win_country',
    'NUMBER_OFFERS': 'number_offers',
    'NUMBER_AWARDS': 'number_awards',
    'DT_DISPATCH': 'dt_dispatch',
    'DT_AWARD': 'dt_award',
    'B_FRA_AGREEMENT': 'is_framework',
    'CANCELLED': 'cancelled',
    'ID_AWARD': 'ted_award_id',
    'ID_LOT_AWARDED': 'lot_id',
    'LOTS_NUMBER': 'lots_number',
}

# Ficheros de las cachés por año (actuales o en _historico/)
_RE_CACHE_ANUAL = re.compile(
    r"^ted_can_(\d{4})_ES(_api)?(?:_en_curso)?(?:__\d{8}T\d{6}Z(?:_\d+)?)?\.parquet$")
# Identificador de un aviso: publication-number de la API (22-2019) o
# ID_NOTICE_CAN del CSV bulk (año + número: 201922)
_RE_AVISO_API = re.compile(r"^0*(\d+)-(\d{4})$")
_RE_AVISO_CSV = re.compile(r"^((?:19|20)\d{2})0*(\d+)$")


def _renombrar_csv(df):
    """Columnas del CSV bulk con los nombres de la API y source='csv_bulk'."""
    rename = {k: v for k, v in _CSV_A_API.items() if k in df.columns}
    if rename:
        df = df.rename(columns=rename)
        if 'source' not in df.columns:
            df['source'] = 'csv_bulk'
    return df


def _ruta_cache(year, api):
    """Caché de un año: ted_can_{año}_ES.parquet (CSV) o ted_can_{año}_ES_api.parquet."""
    return TEDConfig.DATA_DIR / (f"ted_can_{year}_ES_api.parquet" if api else f"ted_can_{year}_ES.parquet")


def _ruta_en_curso(year):
    """Descargas completas del año en curso: versiones del histórico, nunca caché."""
    return TEDConfig.DATA_DIR / f"ted_can_{year}_ES_api_en_curso.parquet"


def _fecha_version(ruta):
    """Fecha de una versión de una caché: el sello que guardar_version pone en
    _historico/ (fecha de esa copia) o, para la copia actual, la fecha del
    fichero (una descarga con las mismas filas no la cambia)."""
    ruta = Path(ruta)
    if ruta.parent.name == HISTORICO:
        sellos = re.findall(r"__(\d{8}T\d{6}Z)", ruta.name)
        if sellos:
            return datetime.strptime(sellos[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
    return datetime.fromtimestamp(ruta.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")


def _es_puntero_lfs(ruta):
    try:
        with open(ruta, "rb") as f:
            return f.read(64).startswith(b"version https://git-lfs")
    except OSError:
        return False


def _leer_version(ruta):
    """Una versión de una caché, o None si no es legible. Un puntero Git LFS sin
    descargar (clon sin 'git lfs pull') no es un dato: guardar_version lo pasa
    a _historico/ al guardar la descarga y aquí se ignora sin avisar."""
    try:
        return pd.read_parquet(ruta)
    except Exception as e:
        if not _es_puntero_lfs(ruta):
            log.warning(f"  Versión ilegible {ruta} ({e}); no se usa")
        return None


def _huellas(df):
    """Huella de cada fila (valores como texto, nulo = nulo), ordenadas."""
    cols = sorted(df.columns, key=str)
    texto = pd.DataFrame({str(c): df[c].astype(object).where(df[c].notna(), "\x00").astype(str)
                          for c in cols}, index=df.index)
    return np.sort(pd.util.hash_pandas_object(texto, index=False).to_numpy())


def _mismas_filas(a, b):
    """Mismas columnas y mismas filas (como multiconjunto). Que la API sirva los
    avisos en otro orden, o que otra versión de pandas lea otro tipo o escriba
    otros bytes, no es una versión nueva."""
    return (sorted(map(str, a.columns)) == sorted(map(str, b.columns)) and len(a) == len(b)
            and np.array_equal(_huellas(a), _huellas(b)))


def _guardar_cache(df, ruta):
    """Guarda la descarga (completa) de un año sin perder la anterior: con las
    mismas filas no se toca (conserva la fecha de su versión); si cambió, la
    anterior pasa a _historico/ (guardar_version). Devuelve el estado."""
    ruta = Path(ruta)
    if ruta.exists():
        anterior = _leer_version(ruta)
        if anterior is not None and _mismas_filas(anterior, df):
            log.info(f"  {ruta.name}: sin cambios")
            return "sin_cambios"
    estado = guardar_registros(df, ruta)
    if estado == "actualizado":
        log.info(f"  {ruta.name}: versión nueva (la anterior queda en {HISTORICO}/)")
    return estado


def _anios_en_disco():
    """{año: {'csv', 'api'}} con alguna versión guardada (actual o en _historico/)."""
    anios = defaultdict(set)
    for carpeta in (TEDConfig.DATA_DIR, TEDConfig.DATA_DIR / HISTORICO):
        if carpeta.is_dir():
            for ruta in carpeta.glob("ted_can_*.parquet"):
                m = _RE_CACHE_ANUAL.match(ruta.name)
                if m:
                    anios[int(m.group(1))].add('api' if m.group(2) else 'csv')
    return anios


def _versiones_anio(year, fuente):
    """Versiones de las descargas de un año, de la más antigua a la actual: las
    de su caché del CSV o, de la API, las del año en curso seguidas de las de
    la caché del año cerrado."""
    if fuente == 'csv':
        return versiones(_ruta_cache(year, api=False))
    rutas = versiones(_ruta_en_curso(year)) + versiones(_ruta_cache(year, api=True))
    return sorted(rutas, key=_fecha_version)   # estable: con la misma fecha, antes el año en curso


def _formato_api(ruta):
    """Parser que generó una caché de la API (None si no es legible): 2 el
    actual (con notice_subtype) y 1 el anterior al 2026-09-27, el de las
    cachés publicadas en v2026.02. Sus valores no son comparables fila a fila
    con los actuales: cae_town como lista ("['Madrid']"), nº de ofertas sin
    distinguir el tipo, sin veat/can-tran/compl ni los campos de aviso."""
    try:
        columnas = pq.read_schema(ruta).names
    except Exception:
        return None
    return 2 if 'notice_subtype' in columnas else 1


def _tabla_anual(year, fuente):
    """Filas de un año desde todas las versiones de su caché, de la más antigua
    a la actual, con acumular(): lo que TED retira o cambia se conserva con
    _en_ultima_descarga=False (un aviso cambiado queda con su versión anterior
    y la nueva). Columnas del CSV renombradas como en la descarga.

    Solo se comparan fila a fila las versiones del mismo formato que la última:
    las de la API generadas por el parser anterior (_formato_api) se quedan en
    _historico/ y sus avisos se recuperan con --semilla. Cada versión se vuelve
    a acumular en cada ejecución (unos 6 s por versión en un año de 125.000
    filas). None si no hay ninguna versión legible."""
    rutas = _versiones_anio(year, fuente)
    if fuente == 'api':
        formatos = [_formato_api(r) for r in rutas]
        legibles = [f for f in formatos if f is not None]
        if legibles:
            omitidas = [r for r, f in zip(rutas, formatos) if f is not None and f != legibles[-1]]
            if omitidas:
                log.warning(f"  {year}: {len(omitidas)} versión(es) de la caché del parser anterior no se "
                            f"comparan fila a fila (siguen en {HISTORICO}/); sus avisos retirados se "
                            f"recuperan con --semilla <ted_es_can.parquet publicado>")
            rutas = [r for r, f in zip(rutas, formatos) if f == legibles[-1]]
    acumulado = None
    for ruta in rutas:
        df = _leer_version(ruta)
        if df is None or len(df) == 0:
            continue
        acumulado = acumular(acumulado, _renombrar_csv(df), _fecha_version(ruta))
    return acumulado


def _informar_historico(year, tabla):
    """Resumen de lo que TED ya no sirve en un año: avisos retirados (sin ninguna
    fila en la última versión) y filas de avisos que siguen con otra versión."""
    fuera = ~tabla['_en_ultima_descarga'].astype(bool)
    if not fuera.any() or 'ted_notice_id' not in tabla.columns:
        return
    avisos = tabla['ted_notice_id'].astype(str)
    cambiadas = fuera & avisos.isin(set(avisos[~fuera]))
    log.info(f"  {year}: {int(fuera.sum()):,} filas que TED ya no sirve "
             f"({avisos[fuera & ~cambiadas].nunique():,} avisos retirados; "
             f"{int(cambiadas.sum()):,} filas de versiones anteriores de avisos que siguen)")


def _consolidar(fuentes, output_path, semillas=()):
    """Consolidado desde todas las versiones de las cachés por año.

    fuentes: {año: 'csv' | 'api'} de esta ejecución. Se añaden los años con
    cachés de ejecuciones anteriores (la del CSV si la hay): no se han vuelto a
    descargar y quedan como en su última versión. Orden como en la descarga
    (años del CSV y después los de la API); columnas: las de siempre y al
    final _primera_descarga, _ultima_descarga, _en_ultima_descarga y, si hay
    semillas, _origen. None si no hay ninguna versión legible."""
    en_disco = _anios_en_disco()
    tablas = {}
    for year in sorted(set(fuentes) | set(en_disco)):
        opciones = [fuentes[year]] if year in fuentes else [f for f in ('csv', 'api') if f in en_disco[year]]
        for fuente in opciones:
            tabla = _tabla_anual(year, fuente)
            if tabla is not None and len(tabla) > 0:
                tablas[year] = (fuente, tabla)
                break
    orden = [y for f in ('csv', 'api') for y in sorted(tablas) if tablas[y][0] == f]
    if not orden:
        return None
    for year in orden:
        _informar_historico(year, tablas[year][1])
    df = pd.concat([tablas[y][1] for y in orden], ignore_index=True)
    fuera = int((~df['_en_ultima_descarga'].astype(bool)).sum())
    log.info(f"Total registros brutos: {len(df):,} ({fuera:,} que TED ya no sirve)")

    df = _normalize_ted_data(df)
    meta = [c for c in COLUMNAS_META if c in df.columns]
    df = df[[c for c in df.columns if c not in meta] + meta]
    return _aplicar_semillas(df, output_path, semillas, set(orden))


def clave_aviso(ids):
    """Clave estable de un aviso TED, igual en el CSV bulk y en la API:
    'número-año'. El CSV da ID_NOTICE_CAN como año + número (2020112) y la API
    el publication-number (112-2020): son el mismo aviso (el TED_NOTICE_URL del
    CSV lleva 'TED:NOTICE:112-2020'). Otros formatos quedan tal cual."""
    def una(v):
        if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NA:
            return None
        texto = str(v).strip()
        m = _RE_AVISO_API.match(texto)
        if m:
            return f"{int(m.group(1))}-{m.group(2)}"
        m = _RE_AVISO_CSV.match(texto)
        if m:
            return f"{int(m.group(2))}-{m.group(1)}"
        return texto
    serie = pd.Series(ids)
    return pd.Series([una(v) for v in serie.astype(object)], index=serie.index, dtype=object)


def _sembradas_antes(output_path):
    """Filas sembradas (con _origen) del consolidado anterior, o None: así se
    conservan aunque la ejecución no reciba --semilla."""
    try:
        if '_origen' not in pq.read_schema(output_path).names:
            return None
        origenes = pd.read_parquet(output_path, columns=['_origen'])['_origen'].dropna()
        valores = sorted(origenes.astype(str).unique())
        if not valores:
            return None
        return pd.read_parquet(output_path, filters=[('_origen', 'in', valores)])
    except Exception:
        return None   # no existe o no es legible (p.ej. puntero Git LFS)


def _aplicar_semillas(df, output_path, semillas, anios):
    """Añade de cada semilla (las filas ya sembradas del consolidado anterior y
    las de --semilla) los avisos que no están en la descarga, con
    _en_ultima_descarga=False y _origen (comun.historico.sembrar).

    Clave: el aviso (clave_aviso), no la fila. ted_notice_id no es único por
    fila (una fila por adjudicación o lote) y las filas de un aviso no se
    corresponden entre versiones del código: el publicado v2026.02 trae
    2020-2023 de la API (lot_index) y ahora salen del CSV (ID_AWARD). Así las
    filas de un aviso entran o no todas juntas. Solo se siembran los años que
    tiene el consolidado (anios): de un año sin ninguna descarga no se sabe si
    el aviso sigue publicado."""
    fuentes = []
    previas = _sembradas_antes(output_path)
    if previas is not None:
        fuentes.append(("salida anterior", previas))
    fuentes += [(str(ruta), pd.read_parquet(ruta)) for ruta in semillas]
    if not fuentes or 'ted_notice_id' not in df.columns:
        return df
    df = df.assign(_clave_aviso=clave_aviso(df['ted_notice_id']))
    for nombre, semilla in fuentes:
        if 'ted_notice_id' not in semilla.columns or 'year' not in semilla.columns:
            log.warning(f"  Semilla {nombre} sin ted_notice_id o year: no se usa")
            continue
        semilla = semilla.assign(_clave_aviso=clave_aviso(semilla['ted_notice_id']))
        en_ambito = pd.to_numeric(semilla['year'], errors='coerce').isin(anios).to_numpy()
        df, informe = sembrar(df, semilla, '_clave_aviso', en_ambito=en_ambito)
        informe['ruta'] = nombre
        imprimir_informe_semilla(informe)
    return df.drop(columns='_clave_aviso')


def _download_csv_year(year, force=False):
    """Descarga CSV de CAN para un año y filtra por España."""
    cache_path = _ruta_cache(year, api=False)
    
    if cache_path.exists() and not force:
        log.info(f"  {year}: usando cache {cache_path}")
        cached = _read_cache(cache_path)
        if cached is not None:
            return cached
    
    # URLs en orden de prioridad — espacios codificados como %20
    urls = [
        f"{TEDConfig.CSV_BASE_URL}/TED%202020/TED%20-%20Contract%20award%20notices%20{year}.csv",
        f"{TEDConfig.CSV_BASE_URL}/TED_CAN_{year}.csv",
        TEDConfig.CSV_HUB_URL.format(year=year),
    ]

    df = None
    for url in urls:
        try:
            log.info(f"  {year}: probando {url.split('/')[-1]}...")
            chunks = []
            for chunk in pd.read_csv(
                url,
                encoding='utf-8',
                encoding_errors='replace',  # un byte inválido no debe tirar el año entero a la API
                compression='zip' if url.endswith('.zip') else 'infer',
                low_memory=False,
                chunksize=50_000,
                dtype=str,
                on_bad_lines='skip',
            ):
                if 'ISO_COUNTRY_CODE' not in chunk.columns:
                    log.warning(f"  {year}: columna ISO_COUNTRY_CODE no encontrada")
                    break
                es_mask = chunk['ISO_COUNTRY_CODE'] == TEDConfig.COUNTRY_CODE
                if es_mask.any():
                    if TEDConfig.CSV_KEEP_ALL_COLUMNS:
                        keep_cols = list(chunk.columns)
                    else:
                        keep_cols = [c for c in TEDConfig.CSV_COLUMNS_KEEP if c in chunk.columns]
                    chunks.append(chunk.loc[es_mask, keep_cols])

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                log.info(f"  {year}: {len(df):,} registros España de CSV bulk")
                break
            # Respuesta sin filas de España (p.ej. una página HTML servida con
            # 200 o un CSV de otro formato): se prueba la siguiente URL en vez de
            # abandonar el CSV y caer a la API
            log.warning(f"  {year}: {url.split('/')[-1]} sin registros de España")
        except Exception as e:
            log.warning(f"  {year}: {url.split('/')[-1]} → {e}")
            continue
    
    if df is None:
        log.warning(f"  {year}: no se pudo descargar CSV")
        # Una descarga fallida no retira nada: si hay caché (--force) se sigue
        # usando en vez de pasar el año a la API
        cached = _read_cache(cache_path) if cache_path.exists() else None
        if cached is not None and len(cached) > 0:
            log.warning(f"  {year}: se mantiene la cache {cache_path.name}")
            return cached
    elif len(df) > 0:
        _guardar_cache(df, cache_path)   # la versión anterior queda en _historico/
    
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  TED SEARCH API v3 — eForms (2024+)
# ═══════════════════════════════════════════════════════════════════════════

def _download_api_year(year, force=False):
    """
    Descarga CAN de España para un año vía TED Search API v3.
    
    La API tiene un límite de ~15,000 resultados por query (150 páginas × 100).
    Si un periodo lo alcanza se divide (año → trimestres → meses → días) hasta
    que cada consulta quepa en el límite.
    """
    cache_path = _ruta_cache(year, api=True)

    if cache_path.exists() and not force:
        if _cache_closed_for_year(cache_path, year):
            log.info(f"  {year}: usando cache {cache_path}")
            cached = _read_cache(cache_path)
            if cached is not None and 'notice_subtype' in cached.columns:
                return cached
            if cached is not None:
                # Generada antes de pedir veat/can-tran/compl y los campos de
                # aviso/procedimiento: le faltan avisos y columnas
                log.info(f"  {year}: cache {cache_path.name} de una versión anterior; se vuelve a descargar")
        else:
            # Guardada con el año aún abierto: le faltan los avisos posteriores
            log.info(f"  {year}: cache {cache_path.name} guardada antes de cerrar el año; se actualiza")

    all_records, complete = _download_api_range(year, f"{year}0101", f"{year}1231", f"{year}")

    if not all_records:
        if not complete:
            # Fallo de la API, no "cero resultados": que download_ted_spain lo sepa
            log.warning(f"  {year}: sin resultados de API (descarga INCOMPLETA por errores)")
            df_vacio = pd.DataFrame()
            df_vacio.attrs['descarga_incompleta'] = True
            return df_vacio
        log.warning(f"  {year}: sin resultados de API")
        return None
    
    df = pd.DataFrame(all_records)
    
    # Deduplicar por ted_notice_id + lot_index (trimestres pueden solapar)
    if 'ted_notice_id' in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=['ted_notice_id', 'lot_index'], keep='first')
        dupes = before - len(df)
        if dupes > 0:
            log.info(f"  {year}: eliminados {dupes:,} duplicados")
    
    log.info(f"  {year}: {len(df):,} registros de API")
    
    if not complete:
        # No cachear: una descarga cortada se reutilizaría después como completa
        # (y sin versión nueva no retira ningún aviso del consolidado)
        log.warning(f"  {year}: descarga API INCOMPLETA (errores o límite de paginación); "
                    f"no se guarda la cache {cache_path.name}")
        df.attrs['descarga_incompleta'] = True
    elif year >= _current_year():
        # Año en curso: TED sigue publicando avisos; una cache ahora se
        # reutilizaría después como si el año estuviera completo. Se guarda
        # aparte, solo como versión del histórico (lo que TED retire durante
        # el año sigue en el consolidado)
        log.info(f"  {year}: año en curso, no se guarda la cache {cache_path.name}")
        if len(df) > 0:
            _guardar_cache(df, _ruta_en_curso(year))
    elif len(df) > 0:
        _guardar_cache(df, cache_path)   # la versión anterior queda en _historico/

    return df


def _subperiods(date_from, date_to):
    """Divide [date_from, date_to] (YYYYMMDD): año completo → trimestres,
    varios meses → meses, un mes → días. [] si es un solo día."""
    d0 = datetime.strptime(date_from, "%Y%m%d").date()
    d1 = datetime.strptime(date_to, "%Y%m%d").date()
    if d0 >= d1:
        return []
    fmt = "%Y%m%d"
    if d0.year == d1.year and (d0.month, d0.day, d1.month, d1.day) == (1, 1, 12, 31):
        y = d0.year
        return [(f"{y}0101", f"{y}0331", f"{y}-Q1"), (f"{y}0401", f"{y}0630", f"{y}-Q2"),
                (f"{y}0701", f"{y}0930", f"{y}-Q3"), (f"{y}1001", f"{y}1231", f"{y}-Q4")]
    out = []
    if (d0.year, d0.month) != (d1.year, d1.month):
        start = d0
        while start <= d1:
            nxt = start.replace(year=start.year + start.month // 12, month=start.month % 12 + 1, day=1)
            end = min(d1, nxt - timedelta(days=1))
            out.append((start.strftime(fmt), end.strftime(fmt), start.strftime("%Y-%m")))
            start = nxt
        return out
    day = d0
    while day <= d1:
        out.append((day.strftime(fmt), day.strftime(fmt), day.strftime("%Y-%m-%d")))
        day += timedelta(days=1)
    return out


def _download_api_range(year, date_from, date_to, period_label):
    """Descarga [date_from, date_to]; si la consulta alcanza el límite de
    paginación, la repite por subperiodos (recursivo).

    Returns: (records, complete). Antes solo se dividía una vez en trimestres:
    un trimestre con más de 15.000 avisos quedaba truncado.
    """
    records, hit_limit, complete = _download_api_period(year, date_from, date_to, period_label)
    if not hit_limit:
        return records, complete
    parts = _subperiods(date_from, date_to)
    if not parts:
        log.warning(f"  {period_label}: límite de paginación en un solo día; descarga INCOMPLETA")
        return records, False
    log.info(f"  {period_label}: límite paginación alcanzado, dividiendo en {len(parts)} periodos...")
    all_records, all_complete = [], True
    for sub_from, sub_to, sub_label in parts:
        sub_records, sub_complete = _download_api_range(year, sub_from, sub_to, sub_label)
        all_records.extend(sub_records)
        all_complete = all_complete and sub_complete
    return all_records, all_complete


def _download_api_period(year, date_from, date_to, period_label):
    """
    Descarga un periodo específico de la API.
    Returns: (records_list, hit_pagination_limit, complete)
      complete=False si la paginación se cortó (errores HTTP/red o límite de
      paginación) antes de recibir todos los avisos que anuncia la API.
    """
    query = (
        f"notice-type IN ({', '.join(TEDConfig.API_NOTICE_TYPES)}) "
        f"AND buyer-country=ESP "
        f"AND publication-date>={date_from} "
        f"AND publication-date<={date_to}"
    )
    
    records = []
    page = 1
    total_count = None
    total_pages = None
    consecutive_errors = 0
    max_errors = 3
    hit_limit = False
    failed = False   # Paginación abortada por errores (HTTP/red)
    n_notices = 0
    
    while True:
        try:
            body = {
                "query": query,
                "fields": TEDConfig.API_FIELDS,
                "page": page,
                "limit": TEDConfig.TED_API_PAGE_SIZE,
                "scope": "ALL",
                "checkQuerySyntax": False,
                "paginationMode": "PAGE_NUMBER",
            }
            
            log.debug(f"  POST {period_label} page={page}")
            resp = requests.post(
                TEDConfig.TED_API_SEARCH,
                json=body,
                timeout=60,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }
            )
            
            if resp.status_code == 429:
                log.warning(f"  Rate limited, esperando 15s...")
                time.sleep(15)
                continue
            
            if resp.status_code == 400:
                # Probable límite de paginación
                if page > 100:
                    log.warning(f"  {period_label}: HTTP 400 en página {page} "
                               f"(límite paginación, {len(records):,} registros obtenidos)")
                    hit_limit = True
                    break
                consecutive_errors += 1
                log.warning(f"  {period_label} page {page}: HTTP 400")
                if consecutive_errors >= max_errors:
                    hit_limit = (page > 50)  # Probable límite si pasamos de 50
                    failed = True
                    break
                time.sleep(2)
                continue
            
            if resp.status_code in (404, 405, 500, 502, 503):
                consecutive_errors += 1
                log.warning(f"  {period_label} page {page}: HTTP {resp.status_code}")
                if consecutive_errors >= max_errors:
                    failed = True
                    break
                time.sleep(2)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            consecutive_errors = 0
            
        except requests.exceptions.RequestException as e:
            consecutive_errors += 1
            log.warning(f"  {period_label} page {page}: {e}")
            if consecutive_errors >= max_errors:
                failed = True
                break
            time.sleep(2)
            continue
        
        # Parsear respuesta
        notices = data.get("notices", data.get("results", []))
        n_notices += len(notices)
        
        if total_count is None:
            total_count = data.get("total", data.get("totalNoticeCount", None))
            if total_count is not None:
                total_pages = math.ceil(total_count / TEDConfig.TED_API_PAGE_SIZE)
                log.info(f"  {period_label}: {total_count:,} resultados, ~{total_pages} páginas")
            else:
                log.info(f"  {period_label}: paginando ({len(notices)} resultados primera página)")
                total_pages = None
            
            if not notices:
                break
        
        if not notices:
            break
        
        for notice in notices:
            parsed = _parse_api_notice(notice)
            records.extend(parsed)
        
        # Paginación
        if total_pages is not None and page >= total_pages:
            break
        
        if len(notices) < TEDConfig.TED_API_PAGE_SIZE:
            break
        
        page += 1
        time.sleep(TEDConfig.TED_API_RATE_LIMIT)
        
        if page % 20 == 0:
            log.info(f"    {period_label} pág {page}: {len(records):,} registros...")
    
    # Sin error HTTP pero con menos avisos de los anunciados (páginas vacías o
    # cortas): límite de paginación alcanzado en silencio → resultado truncado
    if not failed and total_count is not None and n_notices < total_count:
        log.warning(f"  {period_label}: recibidos {n_notices:,} de {total_count:,} avisos "
                    f"(límite de paginación)")
        hit_limit = True
    if failed:
        log.warning(f"  {period_label}: paginación abortada por errores "
                    f"({n_notices:,} avisos recibidos de {total_count if total_count is not None else '?'})")

    complete = not failed and not hit_limit
    return records, hit_limit, complete


def _parse_api_notice(notice):
    """
    Parsea un resultado de la TED Search API v3 (eForms) a lista de dicts.
    
    La API devuelve listas para multi-lot notices:
      - winner-name: {'spa': ['EMPRESA A', 'EMPRESA B']}
      - winner-identifier: ['A12345678', 'B87654321']
      - tender-value: ['100000', '200000']
    
    Genera un registro por lot/winner. Para notices con un solo winner,
    genera un único registro.
    
    Returns:
        Lista de dicts (uno por lot/award)
    """
    try:
        pub_number = notice.get("publication-number", "")
        notice_type = notice.get("notice-type", "")
        
        # ── Comprador ──
        buyer_name = _extract_multilang_name(notice.get("buyer-name", {}))
        
        buyer_ids = _as_list(notice.get("buyer-identifier", []))
        # NIF español: 9 chars tipo A12345678 o P0400000F
        buyer_nif = _find_spanish_nif(buyer_ids)
        
        buyer_country = _first_of_list(notice.get("buyer-country", []), "ES")
        # Lista (['Madrid']) → primer valor; str(lista) dejaba "['Madrid']"
        buyer_city = _extract_multilang_name(notice.get("buyer-city", {})) \
            if isinstance(notice.get("buyer-city"), dict) else _first_of_list(notice.get("buyer-city", []))
        
        # ── CPV ──
        cpv_raw = _as_list(notice.get("classification-cpv", []))
        cpv = str(cpv_raw[0]) if cpv_raw else ""
        
        # ── Ganadores ──
        winner_names = _extract_multilang_list(notice.get("winner-name", {}))
        winner_ids = _as_list(notice.get("winner-identifier", []))
        winner_countries = _as_list(notice.get("winner-country", []))
        winner_dates = _as_list(notice.get("winner-decision-date", []))
        
        # ── Importes (prioridad: tender-value > result-value > estimated) ──
        tender_values = _as_list(notice.get("tender-value", []))
        result_values = _as_list(
            notice.get("result-value-lot", notice.get("result-value-notice", []))
        )
        estimated_values = _as_list(notice.get("estimated-value-lot", []))
        
        tender_cur = _first_of_list(notice.get("tender-value-cur", []), "EUR")
        
        # ── Ofertas recibidas ──
        # BT-760 se repite por tipo de estadística (BT-759: tenders, t-sme,
        # t-esubm...): si vienen los códigos, quedarse solo con 'tenders'
        offers_raw = _as_list(notice.get("received-submissions-type-val", []))
        offers_codes = _as_list(notice.get("received-submissions-type-code", []))
        if offers_codes and len(offers_codes) == len(offers_raw):
            offers_tenders = [v for v, c in zip(offers_raw, offers_codes) if str(c) == "tenders"]
            if offers_tenders:
                offers_raw = offers_tenders
        
        # ── Año de publicación (del publication-number: XXXXXX-YYYY) ──
        pub_year = pub_number.split("-")[-1] if "-" in pub_number else ""
        
        # ── Campos extra (scalar o primer valor) ──
        procedure_id = _first_of_list(notice.get("procedure-identifier", []))
        internal_id_proc = _first_of_list(notice.get("internal-identifier-proc", []))
        internal_id_lot_list = _as_list(notice.get("internal-identifier-lot", []))
        
        total_value = _first_of_list(notice.get("total-value", []))
        total_value_cur = _first_of_list(notice.get("total-value-cur", []), "EUR")
        estimated_value_proc = _first_of_list(notice.get("estimated-value-proc", []))
        
        winner_sizes = _as_list(notice.get("winner-size", []))
        
        buyer_legal_type = _first_of_list(notice.get("buyer-legal-type", []))
        buyer_contracting_entity = _first_of_list(notice.get("buyer-contracting-entity", []))
        buyer_profile = _first_of_list(notice.get("buyer-profile", []))
        
        direct_award_just = _first_of_list(notice.get("direct-award-justification-proc", []))
        direct_award_text = _extract_multilang_name(notice.get("direct-award-justification-text-proc", {}))
        non_award_just = _first_of_list(notice.get("non-award-justification", []))
        sme_part = _first_of_list(notice.get("sme-part", []))
        
        duration_lot_list = _as_list(notice.get("duration-period-value-lot", []))
        subcontracting_value = _first_of_list(notice.get("subcontracting-value", []))
        
        award_criterion_type_list = _as_list(notice.get("award-criterion-type-lot", []))
        award_criterion_weight_list = _as_list(notice.get("award-criterion-number-weight-lot", []))
        
        modification_prev = _first_of_list(notice.get("modification-previous-notice-identifier", []))
        
        lot_ids = _as_list(notice.get("identifier-lot", []))
        result_lot_ids = _as_list(notice.get("result-lot-identifier", []))
        
        framework_est_value = _first_of_list(notice.get("framework-estimated-value", []))
        framework_max_lot = _first_of_list(notice.get("framework-maximum-value-lot", []))

        # ── Aviso / procedimiento ──
        publication_date = _scalar(notice.get("publication-date", []))
        contract_nature = _scalar(notice.get("contract-nature-main-proc", []))
        place_of_performance = ";".join(
            _scalar(v) for v in _as_list(notice.get("place-of-performance", [])) if _scalar(v))

        # ── Determinar número de registros ──
        # Para CAN multi-lot: un registro por winner/value
        # Para non-award notices: un solo registro
        if notice_type in ("cn-standard", "cn-social", "pin-buyer", "pin-standard"):
            n_records = 1
        else:
            n_records = max(len(winner_ids), len(winner_names), len(tender_values), 1)
        
        records = []
        for i in range(n_records):
            value = _safe_index(tender_values, i) \
                 or _safe_index(result_values, i) \
                 or _safe_index(estimated_values, i) \
                 or _safe_index(tender_values, 0) \
                 or ""
            
            record = {
                "ted_notice_id": pub_number,
                "year": pub_year,
                "iso_country": buyer_country,
                "notice_type": notice_type,
                "notice_subtype": _scalar(notice.get("notice-subtype", [])),
                "form_type": _scalar(notice.get("form-type", [])),
                "publication_date": publication_date,
                "procedure_type": _scalar(notice.get("procedure-type", [])),
                "contract_nature": contract_nature,
                # Misma codificación que TYPE_OF_CONTRACT del CSV (W/U/S)
                "type_of_contract": _CONTRACT_NATURE_TO_CSV.get(contract_nature.lower(), ""),
                "place_of_performance": place_of_performance,
                # Comprador
                "cae_name": buyer_name,
                "cae_nationalid": buyer_nif,
                "cae_town": buyer_city,
                "buyer_legal_type": buyer_legal_type,
                "buyer_contracting_entity": buyer_contracting_entity,
                "buyer_profile": buyer_profile,
                # Ganador
                "win_name": _safe_index(winner_names, i) or _safe_index(winner_names, -1) or "",
                "win_nationalid": _safe_index(winner_ids, i) or _safe_index(winner_ids, -1) or "",
                "win_country": _safe_index(winner_countries, i) or "",
                "win_size": _safe_index(winner_sizes, i) or _safe_index(winner_sizes, 0) or "",
                # Importe
                "value_euro": value,
                "currency": tender_cur,
                "total_value": total_value,
                "total_value_cur": total_value_cur,
                "estimated_value_proc": estimated_value_proc,
                # Clasificación
                "cpv": cpv,
                # Ofertas
                "number_offers": _safe_index(offers_raw, i) or _safe_index(offers_raw, 0) or "",
                # Fechas
                "dt_award": _safe_index(winner_dates, i) or _safe_index(winner_dates, 0) or "",
                "dt_dispatch": "",
                # IDs / Linking
                "procedure_id": procedure_id,
                "internal_id_proc": internal_id_proc,
                "internal_id_lot": _safe_index(internal_id_lot_list, i) or "",
                "lot_id": _safe_index(lot_ids, i) or _safe_index(result_lot_ids, i) or "",
                "modification_prev_notice": modification_prev,
                # Procedimiento
                "direct_award_justification": direct_award_just,
                "direct_award_justification_text": direct_award_text,
                "non_award_justification": non_award_just,
                "sme_participation": sme_part,
                # Contrato
                "duration_lot": _safe_index(duration_lot_list, i) or "",
                "subcontracting_value": subcontracting_value,
                # Criterios
                "award_criterion_type": _safe_index(award_criterion_type_list, i) or "",
                "award_criterion_weight": _safe_index(award_criterion_weight_list, i) or "",
                # Framework
                "framework_est_value": framework_est_value,
                "framework_max_lot": framework_max_lot,
                # Meta
                "source": "api_v3",
                "lot_index": i if n_records > 1 else 0,
            }
            records.append(record)
        
        return records
    
    except Exception as e:
        log.debug(f"  Error parsing notice: {e}")
        return []


# ── Helpers para parseo eForms ──

def _as_list(val):
    """Asegura que val sea una lista."""
    if isinstance(val, list):
        return val
    if val is None or val == "":
        return []
    return [val]


def _first_of_list(val, default=""):
    """Primer elemento de lista o default."""
    lst = _as_list(val)
    return str(lst[0]) if lst else default


def _scalar(val, default=""):
    """Primer valor como texto; los dict multiidioma ({'spa': [...]}) por su nombre."""
    if isinstance(val, dict):
        return _extract_multilang_name(val) or default
    lst = _as_list(val)
    if not lst:
        return default
    first = lst[0]
    if isinstance(first, dict):
        return _extract_multilang_name(first) or default
    return str(first)


# contract-nature-main-proc (BT-23) → TYPE_OF_CONTRACT del CSV bulk
_CONTRACT_NATURE_TO_CSV = {"works": "W", "supplies": "U", "services": "S"}


def _str_or_empty(val):
    """str(val), salvo nulos (None/NaN/NaT) → '' (evita 'nan'/'None' como texto)."""
    try:
        if pd.isna(val):
            return ''
    except (TypeError, ValueError):
        pass
    return str(val)


def _safe_index(lst, i, default=None):
    """Acceso seguro a lista por índice."""
    if not lst:
        return default
    try:
        return lst[i]
    except (IndexError, TypeError):
        return default


def _extract_multilang_name(name_dict):
    """Extrae nombre de dict multiidioma {'spa': ['Nombre'], 'eng': ['Name']}."""
    if not isinstance(name_dict, dict):
        return str(name_dict) if name_dict else ""
    
    for lang in ('spa', 'SPA', 'eng', 'ENG'):
        names = name_dict.get(lang, [])
        if names:
            return names[0] if isinstance(names, list) else str(names)
    
    # Fallback: primer valor disponible
    for names in name_dict.values():
        if names:
            return names[0] if isinstance(names, list) else str(names)
    return ""


def _extract_multilang_list(name_dict):
    """Extrae lista de nombres de dict multiidioma."""
    if not isinstance(name_dict, dict):
        if isinstance(name_dict, list):
            return name_dict
        return [str(name_dict)] if name_dict else []
    
    for lang in ('spa', 'SPA', 'eng', 'ENG'):
        names = name_dict.get(lang, [])
        if names:
            return names if isinstance(names, list) else [str(names)]
    
    for names in name_dict.values():
        if names:
            return names if isinstance(names, list) else [str(names)]
    return []


def _find_spanish_nif(id_list):
    """
    Encuentra NIF/CIF español en lista de identificadores.
    NIF: letra + 8 dígitos, o 8 dígitos + letra (9 chars total).
    Si hay múltiples IDs, prioriza el que parece NIF.
    """
    if not id_list:
        return ""
    
    for bid in id_list:
        bid_str = str(bid).strip()
        if len(bid_str) == 9 and (bid_str[0].isalpha() or bid_str[-1].isalpha()):
            return bid_str
    
    # Fallback: último ID (suele ser el NIF en datos TED)
    return str(id_list[-1]).strip()


# ═══════════════════════════════════════════════════════════════════════════
#  NORMALIZACIÓN
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_ted_data(df):
    """Normaliza campos del DataFrame TED a formato uniforme."""
    
    # ── Mapeo de columnas CSV bulk → nombres normalizados ──
    col_map = {
        'ID_NOTICE_CAN': 'ted_notice_id',
        'YEAR': 'year',
        'ISO_COUNTRY_CODE': 'iso_country',
        'CAE_NAME': 'cae_name',
        'CAE_NATIONALID': 'cae_nationalid',
        'CAE_TYPE': 'cae_type',
        'CAE_TOWN': 'cae_town',
        'TAL_LOCATION_NUTS': 'nuts',
        'TYPE_OF_CONTRACT': 'type_of_contract',
        'CPV': 'cpv',
        'ADDITIONAL_CPV': 'cpv_additional',
        'TOP_TYPE': 'top_type',
        'VALUE_EURO_FIN_1': 'value_euro',
        'AWARD_VALUE_EURO_FIN_1': 'award_value_euro',
        'WIN_NAME': 'win_name',
        'WIN_NATIONALID': 'win_nationalid',
        'WIN_COUNTRY_CODE': 'win_country',
        'NUMBER_OFFERS': 'number_offers',
        'NUMBER_AWARDS': 'number_awards',
        'DT_DISPATCH': 'dt_dispatch',
        'DT_AWARD': 'dt_award',
        'B_FRA_AGREEMENT': 'is_framework',
        'CANCELLED': 'cancelled',
        'ID_AWARD': 'ted_award_id',
        'ID_LOT_AWARDED': 'lot_id',
        'LOTS_NUMBER': 'lots_number',
    }
    
    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    
    # ── Aplanar columnas que puedan contener listas (API v3 devuelve listas) ──
    # Primero deduplicar columnas (CSV + API pueden crear duplicados)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Con 500K+ filas, solo checar columnas object
    for col in df.columns:
        if df[col].dtype != object:
            continue
        # Check a larger sample for lists/dicts
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        # Check first, middle and last chunks
        check_idx = list(sample.index[:20]) + list(sample.index[-20:])
        has_complex = False
        for idx in check_idx:
            val = sample.loc[idx]
            if isinstance(val, (list, dict)):
                has_complex = True
                break
        
        if has_complex:
            def _flatten(x):
                if isinstance(x, list):
                    return x[0] if len(x) > 0 else None
                if isinstance(x, dict):
                    # Multilang dict: {'spa': ['val']}
                    for v in x.values():
                        if isinstance(v, list) and v:
                            return v[0]
                        if v:
                            return v
                    return str(x)
                return x
            df[col] = df[col].apply(_flatten)
    
    # ── Tipos numéricos ──
    numeric_cols = [
        'value_euro', 'award_value_euro', 'number_offers', 'year',
        'total_value', 'estimated_value_proc', 'subcontracting_value',
        'duration_lot', 'framework_est_value', 'framework_max_lot',
    ]
    for col in numeric_cols:
        if col in df.columns:
            # Force to string first, clean, then convert
            df[col] = df[col].apply(lambda x: 
                str(x[0]) if isinstance(x, list) and x else
                (str(x) if x is not None and not isinstance(x, float) else x)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # ── Mejor estimación del importe ──
    # Prioridad: award_value > value_euro (tender-value) > total_value > estimated
    if 'award_value_euro' in df.columns and 'value_euro' in df.columns:
        df['importe_ted'] = df['award_value_euro'].fillna(df['value_euro'])
    elif 'value_euro' in df.columns:
        df['importe_ted'] = pd.to_numeric(df['value_euro'], errors='coerce')
    elif 'award_value_euro' in df.columns:
        df['importe_ted'] = df['award_value_euro']
    else:
        df['importe_ted'] = np.nan
    
    # Fallback a total_value o estimated para registros sin importe
    if 'total_value' in df.columns:
        mask = df['importe_ted'].isna() & df['total_value'].notna()
        df.loc[mask, 'importe_ted'] = df.loc[mask, 'total_value']
    if 'estimated_value_proc' in df.columns:
        mask = df['importe_ted'].isna() & df['estimated_value_proc'].notna()
        df.loc[mask, 'importe_ted'] = df.loc[mask, 'estimated_value_proc']
    # CSV sin las columnas *_FIN_1: importes AWARD_VALUE_EURO / VALUE_EURO del CSV
    for raw_col in ('AWARD_VALUE_EURO', 'VALUE_EURO'):
        if raw_col in df.columns:
            mask = df['importe_ted'].isna()
            if mask.any():
                df.loc[mask, 'importe_ted'] = pd.to_numeric(df.loc[mask, raw_col], errors='coerce')

    # ── Limpiar NIF del ganador ──
    nif_col = 'win_nationalid' if 'win_nationalid' in df.columns else None
    if nif_col:
        df['win_nif_clean'] = df[nif_col].fillna('').astype(str).str.strip().str.upper()
        # Quitar prefijo país (ES, ES-, ESA, etc.) solo si va seguido del NIF
        df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*(?=[A-Z0-9])', '', regex=True)
        # Quitar strings vacías, "NONE", "NAN", etc.
        df.loc[df['win_nif_clean'].isin(['', 'NONE', 'NAN', 'N/A']), 'win_nif_clean'] = ''
        df.loc[df['win_nif_clean'].str.len() < 5, 'win_nif_clean'] = ''
    else:
        df['win_nif_clean'] = ''
    
    n_nif = (df['win_nif_clean'] != '').sum()
    log.info(f"  NIFs ganador limpios: {n_nif:,} ({n_nif/len(df)*100:.1f}%)")
    
    # ── Limpiar NIF del órgano ──
    if 'cae_nationalid' in df.columns:
        df['cae_nif_clean'] = df['cae_nationalid'].fillna('').astype(str).str.strip().str.upper()
        df['cae_nif_clean'] = df['cae_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)
    
    # ── Fechas ──
    for col in ['dt_dispatch', 'dt_award', 'publication_date']:
        if col in df.columns:
            # pandas 3: el texto tiene dtype 'str' (no object). Sin quitar la zona
            # horaria, to_datetime(errors='coerce') devuelve TODO NaT al mezclar
            # +01:00 (invierno) y +02:00 (verano)
            if df[col].dtype == object or pd.api.types.is_string_dtype(df[col].dtype):
                df[col] = df[col].astype(str).str.replace(r'(?:Z|[+-]\d{2}:\d{2})$', '', regex=True)
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
    
    # ── Año ──
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
    # Fallback: extraer de dt_award si year es NaN
    if 'year' in df.columns and 'dt_award' in df.columns:
        mask = df['year'].isna() & df['dt_award'].notna()
        if hasattr(df['dt_award'], 'dt'):
            df.loc[mask, 'year'] = df.loc[mask, 'dt_award'].dt.year
    if ('year' not in df.columns or df['year'].isna().all()) and 'dt_dispatch' in df.columns:
        df['year'] = df['dt_dispatch'].dt.year
    # Último fallback: extraer año del ted_notice_id (formato XXXXXX-YYYY)
    if 'year' in df.columns and 'ted_notice_id' in df.columns:
        mask = df['year'].isna()
        if mask.any():
            extracted = df.loc[mask, 'ted_notice_id'].astype(str).str.extract(r'-(\d{4})$')
            if len(extracted.columns) > 0:
                df.loc[mask, 'year'] = pd.to_numeric(extracted[0], errors='coerce')
    
    # ── Eliminar cancelados ──
    if 'cancelled' in df.columns:
        n_cancelled = (df['cancelled'].astype(str) == '1').sum()
        df = df[df['cancelled'].astype(str) != '1'].copy()
        if n_cancelled > 0:
            log.info(f"  Eliminados {n_cancelled:,} notices cancelados")
    
    # ── CPV limpio (primeros 2 dígitos) ──
    if 'cpv' in df.columns:
        df['cpv_2'] = df['cpv'].fillna('').astype(str).str[:2]
    
    # ── Tipo contrato legible ──
    type_map = {'W': 'obras', 'U': 'suministros', 'S': 'servicios'}
    if 'type_of_contract' in df.columns:
        df['tipo_contrato'] = df['type_of_contract'].map(type_map).fillna('otros')
    
    log.info(f"  Datos TED normalizados: {len(df):,} registros")
    
    return df


def _print_ted_summary(df):
    """Imprime resumen del dataset TED descargado."""
    print("\n" + "=" * 70)
    print("  RESUMEN DATOS TED — ESPAÑA")
    print("=" * 70)
    print(f"  Total registros (CAN): {len(df):,}")
    
    if 'year' in df.columns:
        year_counts = df['year'].dropna().value_counts().sort_index()
        if len(year_counts) > 0:
            print(f"  Rango temporal: {int(year_counts.index.min())}-{int(year_counts.index.max())}")
            print(f"\n  Registros por año:")
            for yr, cnt in year_counts.items():
                tag = " (API)" if yr >= 2024 else " (CSV)"
                print(f"    {int(yr)}: {cnt:>8,}{tag}")
    
    if 'source' in df.columns:
        print(f"\n  Por fuente:")
        for src, cnt in df['source'].value_counts().items():
            print(f"    {src}: {cnt:>8,}")
    
    if 'importe_ted' in df.columns:
        valid_imp = df['importe_ted'].dropna()
        if len(valid_imp) > 0:
            print(f"\n  Importes: media={valid_imp.mean():,.0f}€  mediana={valid_imp.median():,.0f}€")
        print(f"  Con importe: {len(valid_imp):,} ({len(valid_imp)/len(df)*100:.1f}%)")
    
    if 'win_nif_clean' in df.columns:
        n_with_nif = (df['win_nif_clean'].str.len() > 4).sum()
        print(f"  Con NIF ganador: {n_with_nif:,} ({n_with_nif/len(df)*100:.1f}%)")
    
    if 'number_offers' in df.columns:
        n_with_offers = df['number_offers'].notna().sum()
        print(f"  Con nº ofertas: {n_with_offers:,} ({n_with_offers/len(df)*100:.1f}%)")
    
    # ── Campos extra (solo API v3) ──
    api_rows = df[df.get('source', pd.Series()) == 'api_v3'] if 'source' in df.columns else pd.DataFrame()
    if len(api_rows) > 0:
        extras = {
            'internal_id_proc': 'Nº expediente interno',
            'win_size': 'Tamaño ganador (PYME)',
            'direct_award_justification': 'Justificación adj. directa',
            'sme_participation': 'Participación PYME',
            'duration_lot': 'Duración contrato',
            'subcontracting_value': 'Valor subcontratación',
            'award_criterion_type': 'Tipo criterio adjudicación',
            'buyer_legal_type': 'Tipo jurídico comprador',
            'modification_prev_notice': 'Notice previa (modificado)',
        }
        non_empty_extras = []
        for col, desc in extras.items():
            if col in api_rows.columns:
                filled = api_rows[col].notna() & (api_rows[col].astype(str).str.strip() != '')
                n = filled.sum()
                if n > 0:
                    non_empty_extras.append((desc, n, n/len(api_rows)*100))
        
        if non_empty_extras:
            print(f"\n  Campos extra (API v3, {len(api_rows):,} registros):")
            for desc, n, pct in sorted(non_empty_extras, key=lambda x: -x[1]):
                print(f"    {desc:<35}: {n:>6,} ({pct:.0f}%)")
    
    if 'tipo_contrato' in df.columns:
        print(f"\n  Por tipo de contrato:")
        for tipo, cnt in df['tipo_contrato'].value_counts().items():
            print(f"    {tipo:<15}: {cnt:>8,}")
    
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════════════
#  ALTERNATIVA: Descarga via SPARQL
# ═══════════════════════════════════════════════════════════════════════════

SPARQL_QUERY_TEMPLATE = """
PREFIX epo: <http://data.europa.eu/a4g/ontology#>
PREFIX org: <http://www.w3.org/ns/org#>
PREFIX cccev: <http://data.europa.eu/m8g/>

SELECT ?notice ?publishedDate ?buyerName ?buyerCountry 
       ?winnerName ?winnerId ?cpv ?procedureType ?value
WHERE {{
  ?notice a epo:Notice ;
          epo:hasPublicationDate ?publishedDate ;
          epo:refersToProcedure ?procedure .
  
  ?procedure epo:hasBuyer ?buyer ;
             epo:hasProcedureType ?procedureType .
  
  ?buyer org:identifier/org:notation ?buyerCountry .
  FILTER(?buyerCountry = "ES")
  
  OPTIONAL {{ ?buyer epo:hasLegalName ?buyerName }}
  OPTIONAL {{ ?procedure epo:isSubjectTo/epo:hasMainClassification ?cpv }}
  OPTIONAL {{
    ?procedure epo:hasLotAwardOutcome/epo:hasAwardedValue/epo:hasAmountValue ?value
  }}
  OPTIONAL {{
    ?procedure epo:hasLotAwardOutcome/epo:hasContractor ?winner .
    ?winner epo:hasLegalName ?winnerName .
    OPTIONAL {{ ?winner org:identifier/org:notation ?winnerId }}
  }}
  
  FILTER(YEAR(?publishedDate) = {year})
}}
LIMIT 10000
OFFSET {offset}
"""


def download_ted_spain_sparql(years=None):
    """Descarga datos TED vía SPARQL endpoint."""
    SPARQL_ENDPOINT = "https://data.ted.europa.eu/sparql"
    
    if years is None:
        years = list(range(2020, datetime.now().year + 1))
    
    all_records = []
    
    for year in years:
        log.info(f"  SPARQL {year}...")
        offset = 0
        
        while True:
            query = SPARQL_QUERY_TEMPLATE.format(year=year, offset=offset)
            
            try:
                resp = requests.get(
                    SPARQL_ENDPOINT,
                    params={"query": query, "format": "json"},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                
                bindings = data.get("results", {}).get("bindings", [])
                if not bindings:
                    break
                
                for b in bindings:
                    record = {
                        "ted_notice_id": b.get("notice", {}).get("value", ""),
                        "year": str(year),
                        "iso_country": "ES",
                        "cae_name": b.get("buyerName", {}).get("value", ""),
                        "cpv": b.get("cpv", {}).get("value", ""),
                        "value_euro": b.get("value", {}).get("value", ""),
                        "win_name": b.get("winnerName", {}).get("value", ""),
                        "win_nationalid": b.get("winnerId", {}).get("value", ""),
                        "source": "sparql",
                    }
                    all_records.append(record)
                
                if len(bindings) < 10000:
                    break
                offset += 10000
                
            except Exception as e:
                log.warning(f"  SPARQL {year}: {e}")
                break
    
    if not all_records:
        return None
    
    return pd.DataFrame(all_records)


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 2: CROSS-VALIDATION TED ↔ PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def ultima_version_por_aviso(df_ted):
    """Última versión de cada aviso, para cruzar TED con otras fuentes.

    El consolidado conserva el histórico: las filas con _en_ultima_descarga=False
    son versiones anteriores de avisos que TED ha cambiado, avisos retirados o
    avisos sembrados del release. En un cruce cada aviso cuenta una vez (si no,
    un aviso cambiado validaría dos contratos): sus filas vigentes o, si TED ya
    no lo sirve, las de la última descarga en que apareció (se publicó).
    Sin esas columnas devuelve la tabla tal cual."""
    if df_ted is None or '_en_ultima_descarga' not in df_ted.columns or 'ted_notice_id' not in df_ted.columns:
        return df_ted
    vigente = df_ted['_en_ultima_descarga'].astype('boolean').fillna(True).astype(bool)
    ids = df_ted['ted_notice_id'].astype(object)
    sin_id = pd.Series([f"\x00{i}" for i in range(len(df_ted))], index=df_ted.index)
    aviso = ids.where(ids.notna(), sin_id).astype(str)
    if '_ultima_descarga' in df_ted.columns:
        ultima = df_ted['_ultima_descarga'].astype(object)
        ultima = ultima.where(ultima.notna(), '').astype(str)
    else:
        ultima = pd.Series('', index=df_ted.index)
    con_vigente = aviso.isin(set(aviso[vigente]))
    ultima_del_aviso = ultima.groupby(aviso).transform('max')
    return df_ted[vigente | (~con_vigente & (ultima == ultima_del_aviso))]


def cross_validate_ted(df_pipeline, df_ted, src, R=None):
    """
    Cruza datos del pipeline (PLACSP/PSCP) contra TED para:
    
    1. Marcar contratos validados por TED (_ted_validated)
    2. Detectar contratos sobre umbral UE ausentes en TED (MISSING_TED)
    3. Enriquecer campos desde TED (n_ofertas, CPV)
    
    Args:
        df_pipeline: DataFrame del pipeline (con _nif, _imp_adj, _organ, etc.)
        df_ted: DataFrame de TED (output de download_ted_spain)
        src: 'CAT' o 'NAC'
        R: Reporter del pipeline (opcional)
    
    Returns:
        df_pipeline: Con columnas nuevas (_ted_validated, _ted_missing, etc.)
        df_missing: DataFrame con contratos que deberían estar en TED pero no están
    """
    if R:
        R.section(f"3A · CROSS-VALIDATION TED [{src}]")
    else:
        print(f"\n{'='*60}\n  CROSS-VALIDATION TED [{src}]\n{'='*60}")
    
    _log = R.log if R else print
    
    if df_ted is None or len(df_ted) == 0:
        _log("  ⚠️ Sin datos TED — saltando cross-validation")
        df_pipeline['_ted_validated'] = False
        df_pipeline['_ted_missing'] = False
        return df_pipeline, pd.DataFrame()
    
    # Histórico de ted_es_can.parquet: cada aviso una vez (su última versión)
    df_ted = ultima_version_por_aviso(df_ted)
    
    # ── 1. Preparar lookup de TED ──
    ted_valid = df_ted[
        (df_ted['importe_ted'].notna()) & 
        (df_ted['importe_ted'] > 0)
    ].copy()
    
    # Construir índice primario: (nif_limpio, año) → lista de importes
    ted_lookup = defaultdict(list)
    # Índice secundario: internal_id_proc (nº expediente) → lista
    ted_lookup_exp = defaultdict(list)
    
    for _, row in ted_valid.iterrows():
        nif = row.get('win_nif_clean', '')
        imp = row['importe_ted']
        yr = row.get('year', np.nan)
        
        entry = {
            'importe': imp,
            'ted_id': row.get('ted_notice_id', ''),
            'n_ofertas': row.get('number_offers', np.nan),
            'cpv_ted': row.get('cpv', ''),
            'cae_ted': row.get('cae_name', ''),
            'win_size': row.get('win_size', ''),
            'direct_award': row.get('direct_award_justification', ''),
            'sme_part': row.get('sme_participation', ''),
            'buyer_legal_type': row.get('buyer_legal_type', ''),
            'duration_lot': row.get('duration_lot', np.nan),
            'award_criterion_type': row.get('award_criterion_type', ''),
            'internal_id': row.get('internal_id_proc', ''),
            'consumed': False,
        }
        
        if nif and len(nif) >= 5 and pd.notna(yr):
            yr = int(yr)
            ted_lookup[(nif, yr)].append(entry)
        
        # Índice por nº expediente (matching directo sin NIF+importe).
        # Nulos fuera: con pandas 2 str(None) = 'None' agrupaba todos los avisos
        # sin internal_id (todo el CSV bulk) bajo la clave 'NONE'
        exp_id = _str_or_empty(row.get('internal_id_proc', '')).strip()
        if exp_id and len(exp_id) >= 4:
            ted_lookup_exp[exp_id.upper()].append(entry)
    
    _log(f"  TED lookup: {len(ted_lookup):,} claves (nif, año)")
    _log(f"  TED lookup expediente: {len(ted_lookup_exp):,} claves")
    _log(f"  TED registros con importe: {len(ted_valid):,}")
    
    # ── 2. Match pipeline → TED ──
    pipeline_valid = df_pipeline[
        df_pipeline['_nif'].notna() & 
        df_pipeline['_imp_adj'].notna() &
        (df_pipeline['_imp_adj'] > 0)
    ]
    
    matched_idx = []
    match_data = {}
    
    for idx, row in pipeline_valid.iterrows():
        nif = str(row['_nif']).strip().upper()
        imp = row['_imp_adj']
        yr = row.get('_año', np.nan)
        
        if pd.isna(yr):
            # Desde CSV la fecha llega como texto
            fecha = pd.to_datetime(row.get('_fecha_adj', pd.NaT), errors='coerce')
            if pd.notna(fecha):
                yr = fecha.year
            else:
                continue
        
        yr = int(yr)
        tol = max(imp * TEDConfig.MATCH_TOLERANCE_PCT, TEDConfig.MATCH_TOLERANCE_ABS)
        
        best_match = None
        best_diff = float('inf')
        best_key = None
        best_match_idx = None
        best_lookup = None  # Track which lookup dict
        
        # ── Estrategia 1: Match por NIF + importe + año ──
        for yr_offset in range(TEDConfig.MATCH_YEAR_WINDOW + 1):
            for yr_try in [yr + yr_offset, yr - yr_offset] if yr_offset > 0 else [yr]:
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
                        best_match_idx = i
                        best_lookup = ted_lookup
        
        # ── Estrategia 2: Match por nº expediente (si disponible) ──
        if best_match is None and '_expediente' in row.index:
            exp_id = str(row.get('_expediente', '')).strip().upper()
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
                        best_match_idx = i
                        best_lookup = ted_lookup_exp
        
        if best_match is not None:
            best_lookup[best_key][best_match_idx]['consumed'] = True
            matched_idx.append(idx)
            match_data[idx] = {
                'ted_id': best_match['ted_id'],
                'ted_importe': best_match['importe'],
                'ted_n_ofertas': best_match['n_ofertas'],
                'ted_cpv': best_match['cpv_ted'],
                'ted_cae': best_match['cae_ted'],
                'match_diff_euros': best_diff,
                # Campos nuevos v6.0
                'ted_win_size': best_match.get('win_size', ''),
                'ted_direct_award': best_match.get('direct_award', ''),
                'ted_sme_part': best_match.get('sme_part', ''),
                'ted_buyer_legal_type': best_match.get('buyer_legal_type', ''),
                'ted_duration': best_match.get('duration_lot', np.nan),
                'ted_award_criterion': best_match.get('award_criterion_type', ''),
                'ted_internal_id': best_match.get('internal_id', ''),
            }
    
    # ── 3. Aplicar resultados ──
    df_pipeline['_ted_validated'] = False
    df_pipeline.loc[matched_idx, '_ted_validated'] = True
    
    # Columnas de enriquecimiento
    enrich_cols = {
        '_ted_n_ofertas': np.nan,
        '_ted_cpv': '',
        '_ted_id': '',
        '_ted_win_size': '',
        '_ted_direct_award': '',
        '_ted_sme_part': '',
        '_ted_buyer_legal_type': '',
        '_ted_duration': np.nan,
        '_ted_award_criterion': '',
        '_ted_internal_id': '',
    }
    for col, default in enrich_cols.items():
        df_pipeline[col] = default
    
    for idx, data in match_data.items():
        df_pipeline.loc[idx, '_ted_n_ofertas'] = pd.to_numeric(
            data['ted_n_ofertas'], errors='coerce'
        )
        # _str_or_empty: los nulos (filas CSV bulk sin campos eForms) quedan '' y no 'nan'/'None'
        df_pipeline.loc[idx, '_ted_cpv'] = _str_or_empty(data['ted_cpv'])
        df_pipeline.loc[idx, '_ted_id'] = _str_or_empty(data['ted_id'])
        df_pipeline.loc[idx, '_ted_win_size'] = _str_or_empty(data.get('ted_win_size', ''))
        df_pipeline.loc[idx, '_ted_direct_award'] = _str_or_empty(data.get('ted_direct_award', ''))
        df_pipeline.loc[idx, '_ted_sme_part'] = _str_or_empty(data.get('ted_sme_part', ''))
        df_pipeline.loc[idx, '_ted_buyer_legal_type'] = _str_or_empty(data.get('ted_buyer_legal_type', ''))
        df_pipeline.loc[idx, '_ted_duration'] = pd.to_numeric(
            data.get('ted_duration', np.nan), errors='coerce'
        )
        df_pipeline.loc[idx, '_ted_award_criterion'] = _str_or_empty(data.get('ted_award_criterion', ''))
        df_pipeline.loc[idx, '_ted_internal_id'] = _str_or_empty(data.get('ted_internal_id', ''))
    
    n_matched = len(matched_idx)
    _log(f"  ✅ Contratos validados por TED: {n_matched:,}")
    
    # ── 4. Detectar MISSING IN TED ──
    above_threshold = df_pipeline[
        (df_pipeline['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN) &
        (~df_pipeline['_es_menor']) &
        (~df_pipeline['_ted_validated']) &
        (df_pipeline['_nif'].notna()) &
        (df_pipeline['_imp_adj'].notna())
    ].copy()
    
    df_pipeline['_ted_missing'] = False
    
    if len(above_threshold) > 0:
        missing_mask = (above_threshold['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN)
        
        if '_es_emergencia' in above_threshold.columns:
            missing_mask = missing_mask & (~above_threshold['_es_emergencia'])
        
        missing_idx = above_threshold.loc[missing_mask].index
        df_pipeline.loc[missing_idx, '_ted_missing'] = True
        
        n_missing = len(missing_idx)
        total_above = len(above_threshold)
        _log(f"  ⚠️ Contratos ≥{TEDConfig.EU_THRESHOLD_MIN:,}€ no-menores sin match TED: "
             f"{n_missing:,} de {total_above:,} ({n_missing/max(total_above,1)*100:.1f}%)")
        
        df_missing = df_pipeline.loc[missing_idx].copy()
        
        if len(df_missing) > 0:
            df_missing['umbral_ue_aplicable'] = TEDConfig.EU_THRESHOLD_MIN
            df_missing['exceso_sobre_umbral'] = df_missing['_imp_adj'] - TEDConfig.EU_THRESHOLD_MIN
            df_missing = df_missing.sort_values('_imp_adj', ascending=False)
            
            if R:
                R.subsection(f"TOP 30 MISSING IN TED [{src}]")
                R.log(f"  {'#':<3} {'Importe':>13} {'Órgano':<35} {'NIF':<12} {'Adj.':<25}")
                R.log(f"  {'-'*95}")
                for i, (_, r) in enumerate(df_missing.head(30).iterrows()):
                    R.log(f"  {i+1:<3} {r['_imp_adj']:>13,.0f}€ "
                          f"{str(r['_organ'])[:34]:<35} "
                          f"{str(r['_nif'])[:11]:<12} "
                          f"{str(r.get('_adj',''))[:24]:<25}")
    else:
        df_missing = pd.DataFrame()
        _log(f"  Sin contratos sobre umbral UE para verificar")
    
    # ── 5. Estadísticas de enriquecimiento ──
    if n_matched > 0:
        ted_ofertas = df_pipeline.loc[matched_idx, '_ted_n_ofertas'].dropna()
        if len(ted_ofertas) > 0:
            _log(f"\n  📊 Enriquecimiento desde TED:")
            _log(f"     Nº ofertas disponible para {len(ted_ofertas):,} contratos")
            _log(f"     Media ofertas (TED): {ted_ofertas.mean():.1f}")
            
            # '_ofertas' es opcional en el pipeline (p.ej. CSV de 'validate')
            both_mask = (
                df_pipeline['_ted_n_ofertas'].notna() & 
                df_pipeline['_ofertas'].notna()
            ) if '_ofertas' in df_pipeline.columns else pd.Series(False, index=df_pipeline.index)
            if both_mask.sum() > 0:
                pip_of = df_pipeline.loc[both_mask, '_ofertas']
                ted_of = df_pipeline.loc[both_mask, '_ted_n_ofertas']
                corr = pip_of.corr(ted_of)
                diff = (pip_of - ted_of).abs().mean()
                _log(f"     Correlación ofertas pipeline↔TED: {corr:.3f}")
                _log(f"     Diferencia media: {diff:.1f} ofertas")
    
    # ── 6. Resumen ──
    total = len(df_pipeline)
    n_validated = df_pipeline['_ted_validated'].sum()
    n_missing_flag = df_pipeline['_ted_missing'].sum()
    _log(f"\n  Resumen [{src}]:")
    _log(f"    Total contratos: {total:,}")
    _log(f"    TED validated: {n_validated:,} ({n_validated/total*100:.1f}%)")
    _log(f"    Missing in TED: {n_missing_flag:,} ({n_missing_flag/total*100:.1f}%)")
    _log(f"    Sin verificar (bajo umbral/sin NIF): "
         f"{total - n_validated - n_missing_flag:,}")
    
    return df_pipeline, df_missing


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 3: INTEGRACIÓN EN SCORING
# ═══════════════════════════════════════════════════════════════════════════

def integrate_ted_in_scoring(df_scored, entity_col, score_col='score_compuesto'):
    """
    Integra los resultados de cross-validation TED en el scoring.
    """
    if '_ted_missing' not in df_scored.columns:
        return df_scored
    
    if 'pct_missing_ted' not in df_scored.columns:
        return df_scored
    
    if 'pct_ted_validated' in df_scored.columns:
        high_quality = df_scored['pct_ted_validated'] > 50
        if high_quality.any():
            df_scored.loc[high_quality, '_ted_quality_flag'] = True
    
    return df_scored


def add_ted_indicators_to_organ_scoring(grp, ted_col_missing='_ted_missing',
                                         ted_col_validated='_ted_validated'):
    """Calcular indicadores TED para un órgano."""
    indicators = {}
    
    if ted_col_missing in grp.columns:
        n_above_threshold = (grp['_imp_adj'] >= TEDConfig.EU_THRESHOLD_MIN).sum()
        n_missing = grp[ted_col_missing].sum()
        
        if n_above_threshold >= 3:
            indicators['pct_missing_ted'] = n_missing / n_above_threshold * 100
            indicators['n_missing_ted'] = int(n_missing)
            indicators['n_above_eu_threshold'] = int(n_above_threshold)
    
    if ted_col_validated in grp.columns:
        n_validated = grp[ted_col_validated].sum()
        indicators['pct_ted_validated'] = n_validated / len(grp) * 100
        indicators['n_ted_validated'] = int(n_validated)
    
    return indicators


def add_ted_indicators_to_adj_scoring(grp, ted_col_missing='_ted_missing',
                                       ted_col_validated='_ted_validated'):
    """Calcular indicadores TED para un adjudicatario."""
    return add_ted_indicators_to_organ_scoring(grp, ted_col_missing, ted_col_validated)


# ═══════════════════════════════════════════════════════════════════════════
#  PARTE 4: SCRIPT DE EJECUCIÓN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """
    Uso:
        python ted_module.py download          # Solo descargar datos
        python ted_module.py validate FILE     # Validar contra pipeline
        python ted_module.py full              # Todo
        python ted_module.py download --semilla ted_es_can.parquet   # + avisos del release
    """
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    
    parser = argparse.ArgumentParser(description='Módulo TED para pipeline v6.0')
    parser.add_argument('command', choices=['download', 'validate', 'full'],
                       help='Comando a ejecutar')
    # Por defecto todo lo que ofrecen las fuentes (antes '2010-2025' fijo: dejaba
    # fuera 2006-2009 del CSV bulk y el año en curso)
    default_years = _default_years()
    parser.add_argument('--years', type=str, default=f'{default_years[0]}-{default_years[-1]}',
                       help=f'Rango de años (ej: 2015-2024; por defecto {default_years[0]}-{default_years[-1]})')
    parser.add_argument('--pipeline-file', type=str, default=None,
                       help='Archivo parquet/csv del pipeline para validar')
    parser.add_argument('--force', action='store_true',
                       help='Re-descargar aunque exista cache')
    parser.add_argument('--method', choices=['csv+api', 'sparql'], default='csv+api',
                       help='Método de descarga')
    parser.add_argument('--semilla', type=Path, action='append', default=[],
                       help='Parquet publicado (p.ej. ted_es_can.parquet de v2026.02): añade los avisos '
                            'que no están en la descarga, con _origen y _en_ultima_descarga=False')
    
    args = parser.parse_args()
    
    if '-' in args.years:
        y_start, y_end = args.years.split('-')
        years = list(range(int(y_start), int(y_end) + 1))
    else:
        years = [int(y) for y in args.years.split(',')]
    
    if args.command in ('download', 'full'):
        print("\n" + "=" * 70)
        print("  DESCARGA DATOS TED — ESPAÑA")
        print("=" * 70)
        
        if args.method == 'sparql':
            df_ted = download_ted_spain_sparql(years=years)
            if df_ted is not None:
                df_ted = _normalize_ted_data(df_ted)
                output_path = TEDConfig.DATA_DIR / "ted_es_can_sparql.parquet"
                # La versión anterior queda en _historico/. No se acumula: la
                # descarga SPARQL no sabe si un año quedó cortado (un error corta
                # la paginación) y retiraría avisos que TED sigue sirviendo
                guardar_registros(df_ted, output_path)
                _print_ted_summary(df_ted)
        else:
            df_ted = download_ted_spain(years=years, force_redownload=args.force,
                                        semillas=args.semilla)
    
    if args.command in ('validate', 'full'):
        if args.pipeline_file:
            print("\n" + "=" * 70)
            print("  CROSS-VALIDATION TED ↔ PIPELINE")
            print("=" * 70)
            
            ted_path = TEDConfig.DATA_DIR / "ted_es_can.parquet"
            if not ted_path.exists():
                print("❌ Primero ejecuta 'download' para obtener datos TED")
                return
            
            df_ted = pd.read_parquet(ted_path)
            
            if args.pipeline_file.endswith('.parquet'):
                df_pipeline = pd.read_parquet(args.pipeline_file)
            else:
                df_pipeline = pd.read_csv(args.pipeline_file)
            
            df_result, df_missing = cross_validate_ted(df_pipeline, df_ted, 'NAC')
            
            output_missing = TEDConfig.OUTPUT_DIR / "v6_0_missing_in_ted.csv"
            if len(df_missing) > 0:
                TEDConfig.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                cols_export = [
                    '_organ', '_nif', '_adj', '_imp_adj', '_fecha_adj',
                    '_cpv', '_es_menor', 'umbral_ue_aplicable',
                    'exceso_sobre_umbral'
                ]
                cols_export = [c for c in cols_export if c in df_missing.columns]
                df_missing[cols_export].to_csv(output_missing, index=False)
                print(f"\n✅ Missing in TED: {output_missing} ({len(df_missing):,} registros)")
            
            print(f"\n📊 Pipeline enriquecido: {df_result['_ted_validated'].sum():,} validados, "
                  f"{df_result['_ted_missing'].sum():,} missing")


if __name__ == '__main__':
    main()