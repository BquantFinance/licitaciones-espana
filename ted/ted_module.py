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

  Filas de la API (2024+ y los avisos eForms de 2023, que el CSV no trae): una
  por oferta ganadora de cada resultado de lote, leída del XML eForms de cada
  aviso (resultado de lote → oferta → parte licitadora → organización). Ver
  «XML eForms DE CADA AVISO».

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
import contextlib
import csv
import gzip
import io
import threading
import zipfile
import zlib
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict

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
    COLUMNAS_META, HISTORICO, acumular, archivar, guardar_registros, guardar_version, imprimir_informe_semilla,
    sembrar, versiones,
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

    # ── Campos de la API: solo los del aviso y del procedimiento ──
    # Un solo nombre desconocido hace fallar toda la consulta (HTTP 400; el
    # mensaje lista los admitidos). La API aplana cada campo en una lista del
    # aviso entero y deduplica los valores iguales: no dice qué ganador, importe
    # o fecha es de qué lote. Las filas (resultado de lote → oferta → ganador)
    # salen del XML eForms de cada aviso (_parse_eforms), no de estos campos.
    API_FIELDS = [
        "publication-number",
        "notice-type",
        "publication-date",
        "notice-subtype",
        "form-type",
        "procedure-type",
        "contract-nature-main-proc",
        "place-of-performance",
        # total-value mezcla el valor del aviso (BT-161) y el máximo de los acuerdos
        # marco (BT-118): se conserva por compatibilidad; el XML da cada uno aparte
        "total-value",
        "total-value-cur",
        "estimated-value-proc",
        "estimated-value-cur-proc",
        # ── Comprador ──
        "buyer-name",
        "buyer-identifier",
        "buyer-country",
        "buyer-city",
        "buyer-legal-type",             # Tipo jurídico (para umbrales UE)
        "buyer-contracting-entity",     # ¿Sectorial?
        "buyer-profile",                # URL perfil contratante
        # ── Clasificación e identificadores del procedimiento ──
        "classification-cpv",
        "procedure-identifier",         # ID procedimiento TED
        "internal-identifier-proc",     # Nº expediente interno (!)
        "modification-previous-notice-identifier",  # Notice previa (modificados)
        # ── Procedimiento ──
        "direct-award-justification-proc",  # Justificación negociado s/p
        "direct-award-justification-text-proc",
        "sme-part",                     # ¿Participación PYME?
        # ── Título y descripción del procedimiento (BT-21, BT-24) ──
        "title-proc",
        "description-proc",
        # ── Versiones: identificador del aviso (BT-701) y su versión (BT-757), el
        #    aviso que se cambia (BT-758: la API da su número de publicación si lo
        #    conoce, si no el identificador y la versión) y el motivo (BT-140) ──
        "notice-identifier",
        "notice-version",
        "change-notice-version-identifier",
        "change-reason-code",
    ]

    # ── XML eForms de cada aviso (resultados por lote, ofertas y ganadores) ──
    # Un fichero comprimido por aviso en <DATA_DIR>/xml/<año>/<número>.xml.gz. Un
    # aviso publicado no cambia (una corrección es otro aviso): solo se pide una vez
    TED_XML_URL = "https://ted.europa.eu/en/notice/{numero}/xml"
    XML_DIR = "xml"
    # Con 4 hilos se midieron 3,1 XML/s (493 en 160 s, ~1,3 s por petición): 8 hilos para llegar
    # al tope de 5/s. La primera ejecución pide unos 100.000 (2023-2026): ~6 h
    XML_WORKERS = 8            # peticiones a la vez
    XML_MAX_POR_SEGUNDO = 5.0  # ritmo máximo entre todas
    XML_REINTENTOS = 5
    # Subtipos de los avisos de adjudicación de eForms (notice-subtype, que solo
    # tienen los avisos eForms): 25-28 veat, 29-32 y E4 can-standard, 33-35
    # can-social, 36-37 can-desg, 38-40 y E6 can-modif, E5 compl y T02 can-tran
    EFORMS_CAN_SUBTYPES = ("25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
                           "36", "37", "38", "39", "40", "E4", "E5", "E6", "T02")
    # Años del CSV en que ya se publicaban avisos eForms (voluntarios desde nov-2022,
    # obligatorios desde el 25-oct-2023). El CSV no los trae (2.609 CAN de España en
    # 2023, 0 en 2022): se piden a la API y se guardan en ted_can_<año>_ES_eforms.parquet
    EFORMS_CSV_YEARS = range(2022, 2024)

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
    _borrar_temporales_viejos()
    
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
    irregulares = []       # Años cuyo CSV trae registros irregulares (main() sale con 1)
    
    # ── CSV bulk para años disponibles, API para el resto ──
    csv_years = [y for y in years if y in TEDConfig.CSV_YEARS_AVAILABLE]
    api_years = [y for y in years if y not in TEDConfig.CSV_YEARS_AVAILABLE]
    
    # Años que fallan en CSV se reintentan por API
    csv_failed_years = []
    
    if csv_years:
        log.info(f"📥 Descargando CSV bulk para {csv_years[0]}-{csv_years[-1]}...")
        for year in csv_years:
            df_year = _download_csv_year(year, force_redownload, irregulares=irregulares)
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
    
    # Años del CSV con avisos eForms, que el CSV no trae: se piden a la API (solo eForms)
    eforms_years = [y for y in csv_years if y in TEDConfig.EFORMS_CSV_YEARS and fuentes.get(y) == 'csv']
    if eforms_years:
        log.info(f"🌐 Avisos eForms de {eforms_years} (el CSV no los trae)...")
        for year in eforms_years:
            df_year = _download_api_year(year, force_redownload, solo_eforms=True)
            if df_year is not None and df_year.attrs.get('descarga_incompleta'):
                incomplete_years.append(year)
            if df_year is not None and len(df_year) > 0:
                descargas.append(df_year)

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
        df.attrs['sin_guardar'] = True   # main() sale con 1
        return df

    # ── Consolidado desde todas las versiones de las cachés (y las semillas) ──
    df = _consolidar(fuentes, output_path, semillas)
    if df is None:
        log.error("Ninguna caché legible para construir el consolidado")
        return None
    estado = guardar_registros(df, output_path)   # la versión anterior queda en _historico/
    log.info(f"✅ Guardado ({estado}): {output_path} ({len(df):,} registros)")
    
    _print_ted_summary(df)
    if irregulares:
        df.attrs['csv_irregular'] = irregulares   # main() sale con 1 (lo descargado ya está guardado)
    
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
    r"^ted_can_(\d{4})_ES(_api|_eforms)?(?:_en_curso)?(?:__\d{8}T\d{6}Z(?:_\d+)?)?\.parquet$")
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


def _ruta_cache_eforms(year):
    """Avisos eForms de un año del CSV (el CSV no los trae): ted_can_{año}_ES_eforms.parquet."""
    return TEDConfig.DATA_DIR / f"ted_can_{year}_ES_eforms.parquet"


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
    """{año: {'csv', 'api', 'eforms'}} con alguna versión guardada (actual o en _historico/)."""
    anios = defaultdict(set)
    for carpeta in (TEDConfig.DATA_DIR, TEDConfig.DATA_DIR / HISTORICO):
        if carpeta.is_dir():
            for ruta in carpeta.glob("ted_can_*.parquet"):
                m = _RE_CACHE_ANUAL.match(ruta.name)
                if m:
                    anios[int(m.group(1))].add({'_api': 'api', '_eforms': 'eforms'}.get(m.group(2), 'csv'))
    return anios


def _versiones_anio(year, fuente):
    """Versiones de las descargas de un año, de la más antigua a la actual: las
    de su caché del CSV o, de la API, las del año en curso seguidas de las de
    la caché del año cerrado."""
    if fuente == 'csv':
        return versiones(_ruta_cache(year, api=False))
    if fuente == 'eforms':
        return versiones(_ruta_cache_eforms(year))
    rutas = versiones(_ruta_en_curso(year)) + versiones(_ruta_cache(year, api=True))
    return sorted(rutas, key=_fecha_version)   # estable: con la misma fecha, antes el año en curso


def _formato_api(ruta):
    """Parser que generó una caché de la API (None si no es legible): 3 el actual
    (filas del XML eForms, con lot_result_id), 2 el de sept. 2026 (filas por
    posición de las listas de la API, con notice_subtype) y 1 el anterior al
    2026-09-27, el de las cachés publicadas en v2026.02. Sus filas no son
    comparables entre formatos: cambian la granularidad y las columnas."""
    try:
        columnas = pq.read_schema(ruta).names
    except Exception:
        return None
    return 3 if 'lot_result_id' in columnas else 2 if 'notice_subtype' in columnas else 1


def _acumular_rutas(rutas):
    """acumular() de las versiones `rutas` (de la más antigua a la actual), con las
    columnas del CSV renombradas; None si ninguna es legible y no vacía."""
    acumulado = None
    for ruta in rutas:
        df = _leer_version(ruta)
        if df is None or len(df) == 0:
            continue
        acumulado = acumular(acumulado, _renombrar_csv(df), _fecha_version(ruta))
    return acumulado


def _tabla_anual(year, fuente):
    """Filas de un año desde todas las versiones de su caché, de la más antigua
    a la actual, con acumular(): lo que TED retira o cambia se conserva con
    _en_ultima_descarga=False (un aviso cambiado queda con su versión anterior
    y la nueva). Columnas del CSV renombradas como en la descarga.

    En la API solo se comparan fila a fila las versiones del mismo formato que
    la última (_formato_api). De las de un parser anterior se añaden, con
    _en_ultima_descarga=False y _origen, los avisos que ya no están en las del
    formato actual (TED los ha retirado): sus filas son las de aquel parser,
    pero son la única copia (antes se quedaban solo en _historico/). Cada
    versión se vuelve a acumular en cada ejecución (unos 6 s por versión en un
    año de 125.000 filas). None si no hay ninguna versión legible."""
    rutas = _versiones_anio(year, fuente)
    anteriores = []
    if fuente in ('api', 'eforms'):
        formatos = [_formato_api(r) for r in rutas]
        legibles = [f for f in formatos if f is not None]
        if legibles:
            anteriores = [r for r, f in zip(rutas, formatos) if f is not None and f != legibles[-1]]
            rutas = [r for r, f in zip(rutas, formatos) if f == legibles[-1]]
    acumulado = _acumular_rutas(rutas)
    if not anteriores or acumulado is None or 'ted_notice_id' not in acumulado.columns:
        return acumulado if acumulado is not None else _acumular_rutas(anteriores)
    acumulado = acumulado.assign(_clave_aviso=clave_aviso(acumulado['ted_notice_id']))
    # Del formato más reciente al más antiguo: un aviso retirado entra con las filas de su último parser
    for formato in sorted({_formato_api(r) for r in anteriores}, reverse=True):
        de_formato = [r for r in anteriores if _formato_api(r) == formato]
        previo = _acumular_rutas(de_formato)
        if previo is None or 'ted_notice_id' not in previo.columns:
            continue
        previo = previo.assign(_clave_aviso=clave_aviso(previo['ted_notice_id']))
        acumulado, informe = sembrar(acumulado, previo, '_clave_aviso',
                                     origen=f"caché de {year} del parser anterior")
        informe['ruta'] = f"{len(de_formato)} versión(es) del parser anterior (formato {formato}) de {year}"
        imprimir_informe_semilla(informe)
    return acumulado.drop(columns='_clave_aviso')


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
    # Avisos eForms de los años del CSV (ted_can_<año>_ES_eforms.parquet), junto a su año. Un aviso
    # que el CSV ya trae se queda con sus filas del CSV (los avisos del CSV no cambian); si el año
    # salió de la API (el CSV falló), la API ya trae todos sus avisos
    eforms = {}
    for year in sorted(y for y, tipos in en_disco.items() if 'eforms' in tipos):
        if year in tablas and tablas[year][0] == 'api':
            continue
        tabla = _tabla_anual(year, 'eforms')
        if tabla is None or len(tabla) == 0:
            continue
        if year in tablas and 'ted_notice_id' in tablas[year][1].columns:
            en_csv = set(clave_aviso(tablas[year][1]['ted_notice_id']).dropna())
            repetidos = clave_aviso(tabla['ted_notice_id']).isin(en_csv).to_numpy()
            if repetidos.any():
                log.info(f"  {year}: {int(repetidos.sum()):,} filas eForms de avisos que ya trae el CSV "
                         f"(se quedan las del CSV)")
                tabla = tabla[~repetidos].reset_index(drop=True)
        if len(tabla) > 0:
            eforms[year] = tabla
    orden = [y for f in ('csv', 'api') for y in sorted(tablas) if tablas[y][0] == f]
    if not orden and not eforms:
        return None
    partes = []
    for year in orden:
        _informar_historico(year, tablas[year][1])
        partes.append(tablas[year][1])
        if year in eforms:
            _informar_historico(year, eforms[year])
            partes.append(eforms.pop(year))
    for year in sorted(eforms):   # años con avisos eForms y sin tabla del CSV
        _informar_historico(year, eforms[year])
        partes.append(eforms[year])
        orden.append(year)
    df = pd.concat(partes, ignore_index=True)
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


# Límite de tamaño de campo del módulo csv: 131.072 caracteres por defecto (pandas no tenía).
# El mayor que admite la plataforma (en Windows, un long de C de 32 bits)
_LIMITE_CAMPO_CSV = min(sys.maxsize, 2**31 - 1)


def _borrar_temporales_viejos(horas=12):
    """Borra los temporales de descarga que deja una ejecución que se mata (el límite de tiempo de la
    cola, un SIGKILL): hasta ~750 MB por año. Solo los de más de `horas`: los de una ejecución en
    marcha se escriben o se leen en minutos."""
    limite = time.time() - horas * 3600
    for patron in (".descarga_ted_can_*.tmp", ".ted_can_*_registros_irregulares.csv.*.tmp"):
        for ruta in TEDConfig.DATA_DIR.glob(patron):
            try:
                if ruta.stat().st_mtime < limite:
                    ruta.unlink()
                    log.warning(f"Temporal de una ejecución anterior borrado: {ruta.name}")
            except OSError:
                pass


def _descargar(url, destino):
    """Descarga `url` en `destino` por partes: el CSV de adjudicaciones de un año es de toda la UE.
    Lanza la excepción de requests si la respuesta no es 2xx."""
    with requests.get(url, stream=True, timeout=(30, 300)) as r:
        r.raise_for_status()
        with open(destino, 'wb') as f:
            for trozo in r.iter_content(1 << 20):
                f.write(trozo)


@contextlib.contextmanager
def _abrir_csv_texto(ruta, es_zip):
    """Texto del CSV descargado: el fichero o el CSV de dentro del ZIP (2020-2023 se sirven en ZIP).
    Como hacía pandas: utf-8, con o sin BOM, y un byte inválido se sustituye sin tirar el año. Un ZIP
    con varios CSV (o sin CSV y con varios ficheros) es un error, como lo era para pandas, en vez de
    leer solo uno. El ZIP se cierra al salir: en Windows no se puede borrar un fichero abierto."""
    if not es_zip:
        with open(ruta, encoding='utf-8-sig', errors='replace', newline='') as texto:
            yield texto
        return
    with zipfile.ZipFile(ruta) as zf:
        ficheros = [i.filename for i in zf.infolist() if not i.is_dir()]
        csvs = [n for n in ficheros if n.lower().endswith('.csv')] or ficheros
        if len(csvs) != 1:
            raise ValueError(f"el ZIP trae {len(csvs)} CSV ({', '.join(csvs[:5])}): se esperaba uno")
        otros = [n for n in ficheros if n not in csvs]
        if otros:
            log.warning(f"  ZIP: se lee {csvs[0]} y se ignoran {', '.join(otros[:5])}")
        with zf.open(csvs[0]) as bruto, \
                io.TextIOWrapper(bruto, encoding='utf-8-sig', errors='replace', newline='') as texto:
            yield texto


def _nombres_unicos(cabecera):
    """Nombres de columna como los ponía pandas al leer el CSV (medido con 2.2.3 y 3.0.6): una
    cabecera vacía es 'Unnamed: <posición>' y una repetida lleva .1, .2... saltándose los nombres que
    ya existen (X,X,X.1 da X,X.2,X.1); primero las columnas con nombre y después las vacías."""
    nombres = [n if n != '' else f"Unnamed: {i}" for i, n in enumerate(cabecera)]
    vacias = [i for i, n in enumerate(cabecera) if n == '']
    cuenta = {}
    for i in [i for i, n in enumerate(cabecera) if n != ''] + vacias:
        original = nombre = nombres[i]
        n = cuenta.get(nombre, 0)
        while n > 0:
            cuenta[original] = n + 1
            nombre = f"{original}.{n}"
            n = n + 1 if nombre in nombres else cuenta.get(nombre, 0)
        nombres[i] = nombre
        cuenta[nombre] = n + 1
    return nombres


class _Irregulares:
    """Registros irregulares del CSV de un año (ver _leer_csv_espana), escritos según se leen en un
    temporal que se crea con el primero: línea en que empieza, motivo, nº de campos (y los de la
    cabecera), URL y todos los campos en JSON."""

    def __init__(self, ruta, url):
        self.ruta, self.url = ruta, url
        self.n, self.n_es, self.motivos = 0, 0, Counter()
        self._f = self._w = None

    def __call__(self, linea, motivo, campos, n_cabecera):
        if self._f is None:
            self._f = open(self.ruta, 'w', encoding='utf-8', newline='')
            self._w = csv.writer(self._f, lineterminator='\n')
            self._w.writerow(['linea', 'motivo', 'n_campos', 'n_campos_cabecera', 'url', 'campos_json'])
        self._w.writerow([linea, motivo, len(campos), n_cabecera, self.url,
                          json.dumps(campos, ensure_ascii=False)])
        self.n += 1
        self.n_es += TEDConfig.COUNTRY_CODE in campos
        self.motivos[motivo] += 1

    def __enter__(self):
        return self

    def __exit__(self, *excepcion):
        if self._f is not None:
            self._f.close()


def _en_blanco(reg):
    """Línea en blanco para pandas: sin campos o un solo campo de espacios y tabuladores (medido con
    2.2.3 y 3.0.6: se salta, también antes de la cabecera; '  ,  ' sí es un registro)."""
    return not reg or (len(reg) == 1 and not reg[0].strip(' \t'))


def _leer_csv_espana(texto, irregular):
    """Lee el CSV de adjudicaciones registro a registro: (filas de España, cabecera, marcas, líneas
    leídas, líneas de los registros irregulares). `marcas` va en paralelo a las filas: el motivo si
    la fila sale de un registro irregular (columna _registro_irregular) y None si no.

    Antes se leía con pandas por trozos y on_bad_lines='skip': los registros con más campos que la
    cabecera se perdían sin guardarse (y con algunos patrones, como líneas malas y campos
    entrecomillados en varias líneas en el primer trozo, pandas 2.2.3 y 3.0.6 aceptaban en silencio
    líneas malas de trozos posteriores sin el campo sobrante). Ahora cada registro irregular pasa a
    `irregular(línea, motivo, campos, n_cabecera)` para guardarlo aparte, sea del país que sea (de un
    registro roto no se sabe el país):
      - 'campos_de_mas': no entra en la tabla.
      - 'campos_de_menos': entra completado con vacíos si es de España, como hacía pandas.
      - 'salto_de_linea': algún campo lleva saltos de línea. El CSV de TED no los trae (ninguno en
        2019 ni en 2021); una comilla sin cerrar se traga en su campo, que es el último del
        registro, los registros que siguen hasta la siguiente comilla o el final del fichero.
    Los que entran en la tabla llevan su motivo en `marcas`. Las líneas en blanco o de solo espacios
    se saltan, también antes de la cabecera, como en pandas. Sin la columna del país devuelve
    (None, cabecera, [], líneas leídas, 0)."""
    anterior = csv.field_size_limit(_LIMITE_CAMPO_CSV)
    try:
        lector = csv.reader(texto)
        cabecera = next((r for r in lector if not _en_blanco(r)), [])
        if 'ISO_COUNTRY_CODE' not in cabecera:
            return None, cabecera, [], lector.line_num, 0
        i_pais = cabecera.index('ISO_COUNTRY_CODE')
        n = len(cabecera)
        filas, marcas, lineas_irregulares = [], [], 0
        inicio = lector.line_num + 1
        for reg in lector:
            if not _en_blanco(reg):
                motivos = []
                if len(reg) > n:
                    motivos.append('campos_de_mas')
                elif len(reg) < n:
                    motivos.append('campos_de_menos')
                # Un campo con saltos de línea ocupa varias líneas del fichero, salvo el de una
                # comilla sin cerrar al final del fichero (siempre el último campo del registro)
                if lector.line_num > inicio or '\n' in reg[-1] or '\r' in reg[-1]:
                    motivos.append('salto_de_linea')
                motivo = '+'.join(motivos) or None
                if motivo:
                    irregular(inicio, motivo, reg, n)
                    lineas_irregulares += lector.line_num - inicio + 1
                if len(reg) <= n:
                    if len(reg) < n:
                        reg = reg + [''] * (n - len(reg))
                    if reg[i_pais] == TEDConfig.COUNTRY_CODE:
                        filas.append(reg)
                        marcas.append(motivo)
            inicio = lector.line_num + 1
        return filas, cabecera, marcas, lector.line_num, lineas_irregulares
    finally:
        csv.field_size_limit(anterior)


def _download_csv_year(year, force=False, irregulares=None):
    """Descarga CSV de CAN para un año y filtra por España.

    Los registros irregulares del CSV (ver _leer_csv_espana) se guardan en
    ted_can_<año>_registros_irregulares.csv, con versiones en _historico/ como las cachés, y las
    filas que salen de ellos llevan el motivo en _registro_irregular. Si el CSV que se usa trae
    alguno (o, sin ninguno bueno, alguno de los leídos), el año se añade a `irregulares`: el CSV de
    un año cerrado solo se lee una vez (después se usa la caché), así que es un aviso único."""
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
    ruta_irr = TEDConfig.DATA_DIR / f"ted_can_{year}_registros_irregulares.csv"

    df = None
    irregular_leido = False   # algún CSV leído (aunque no se use) traía registros irregulares
    for url in urls:
        nombre = url.split('/')[-1]
        # Temporales con el PID: dos ejecuciones a la vez no se pisan (los que deja una ejecución
        # que se mata los borra _borrar_temporales_viejos)
        tmp = TEDConfig.DATA_DIR / f".descarga_ted_can_{year}.{os.getpid()}.tmp"
        tmp_irr = TEDConfig.DATA_DIR / f".{ruta_irr.name}.{os.getpid()}.tmp"
        try:
            log.info(f"  {year}: probando {nombre}...")
            TEDConfig.DATA_DIR.mkdir(parents=True, exist_ok=True)
            _descargar(url, tmp)
            with _abrir_csv_texto(tmp, url.endswith('.zip')) as texto, _Irregulares(tmp_irr, url) as irr:
                filas, cabecera, marcas, lineas, lineas_irr = _leer_csv_espana(texto, irr)
            if irr.n:
                irregular_leido = True
                estado = guardar_version(ruta_irr, desde=tmp_irr)
                detalle = ', '.join(f"{v:,} {k}" for k, v in sorted(irr.motivos.items()))
                log.warning(f"  {year}: REGISTROS IRREGULARES en {nombre}: {irr.n:,} ({detalle}; "
                            f"{lineas_irr:,} de sus {lineas:,} líneas; {irr.n_es:,} con "
                            f"'{TEDConfig.COUNTRY_CODE}' en algún campo), guardados en {ruta_irr.name} ({estado})")
            if filas is None:
                log.warning(f"  {year}: columna ISO_COUNTRY_CODE no encontrada")
            elif filas:
                df = pd.DataFrame(filas, columns=_nombres_unicos(cabecera), dtype=str)
                df = df.mask(df == '')   # solo el vacío pasa a nulo: 'NA', 'NULL'... se conservan como texto
                if not TEDConfig.CSV_KEEP_ALL_COLUMNS:
                    df = df[[c for c in TEDConfig.CSV_COLUMNS_KEEP if c in df.columns]]
                if any(marcas):
                    # Filas que salen de un registro irregular: se marcan, no se limpian (regla 1)
                    df['_registro_irregular'] = pd.Series(marcas, index=df.index, dtype=object)
                log.info(f"  {year}: {len(df):,} registros España de CSV bulk")
                if irr.n:
                    if irregulares is not None:
                        irregulares.append(year)
                elif ruta_irr.exists():
                    archivar(ruta_irr)   # es de una descarga anterior: esta no trae registros irregulares
                break
            # Respuesta sin filas de España (p.ej. una página HTML servida con
            # 200 o un CSV de otro formato): se prueba la siguiente URL en vez de
            # abandonar el CSV y caer a la API
            log.warning(f"  {year}: {nombre} sin registros de España")
        except Exception as e:
            log.warning(f"  {year}: {nombre} → {e}")
            continue
        finally:
            tmp.unlink(missing_ok=True)
            tmp_irr.unlink(missing_ok=True)
    if df is None and irregular_leido and irregulares is not None:
        irregulares.append(year)   # ningún CSV bueno, y alguno de los leídos traía registros irregulares

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

def _download_api_year(year, force=False, solo_eforms=False):
    """
    Descarga CAN de España para un año vía TED Search API v3 y el XML eForms
    de cada aviso (las filas salen del XML: _filas_aviso).

    La API tiene un límite de ~15,000 resultados por query (150 páginas × 100).
    Si un periodo lo alcanza se divide (año → trimestres → meses → días) hasta
    que cada consulta quepa en el límite.

    solo_eforms: solo los avisos eForms (notice-subtype), para los años del CSV,
    que no los trae; se guardan en ted_can_<año>_ES_eforms.parquet.
    """
    cache_path = _ruta_cache_eforms(year) if solo_eforms else _ruta_cache(year, api=True)

    if cache_path.exists() and not force:
        if _cache_closed_for_year(cache_path, year):
            log.info(f"  {year}: usando cache {cache_path}")
            cached = _read_cache(cache_path)
            if cached is not None and 'lot_result_id' in cached.columns:
                return cached
            if cached is not None:
                # De un parser anterior (filas por posición de las listas de la API, sin XML)
                log.info(f"  {year}: cache {cache_path.name} de una versión anterior; se vuelve a descargar")
        else:
            # Guardada con el año aún abierto: le faltan los avisos posteriores
            log.info(f"  {year}: cache {cache_path.name} guardada antes de cerrar el año; se actualiza")

    filtro = f" AND notice-subtype IN ({', '.join(TEDConfig.EFORMS_CAN_SUBTYPES)})" if solo_eforms else ""
    etiqueta = f"{year}-eForms" if solo_eforms else f"{year}"
    avisos, complete = _download_api_range(year, f"{year}0101", f"{year}1231", etiqueta, filtro)

    if not avisos:
        if not complete:
            # Fallo de la API, no "cero resultados": que download_ted_spain lo sepa
            log.warning(f"  {year}: sin resultados de API (descarga INCOMPLETA por errores)")
            df_vacio = pd.DataFrame()
            df_vacio.attrs['descarga_incompleta'] = True
            return df_vacio
        log.warning(f"  {year}: sin resultados de API")
        return None

    # Un aviso una vez, el primero (los periodos pueden solapar)
    unicos, vistos = [], set()
    for aviso in avisos:
        if aviso['ted_notice_id'] and aviso['ted_notice_id'] in vistos:
            continue
        vistos.add(aviso['ted_notice_id'])
        unicos.append(aviso)
    if len(unicos) < len(avisos):
        log.info(f"  {year}: {len(avisos) - len(unicos):,} avisos repetidos entre periodos")
    estados, xml_completo = _xml_avisos([a['ted_notice_id'] for a in unicos], forzar=force)
    filas, ritmo = [], _Ritmo(TEDConfig.XML_MAX_POR_SEGUNDO)
    for aviso in unicos:
        numero = aviso['ted_notice_id']
        estado = estados.get(numero, 'error')
        contenido = _leer_xml(numero) if estado == 'ok' else None
        if estado == 'ok' and contenido is None:
            # En disco pero ilegible (fichero dañado): se vuelve a pedir y el dañado queda en _historico/
            log.warning(f"  XML {numero} ilegible en disco: se vuelve a pedir")
            estado = _obtener_xml(numero, ritmo, forzar=True)
            contenido = _leer_xml(numero) if estado == 'ok' else None
            if contenido is None:
                xml_completo = False
        filas.extend(_filas_aviso(aviso, contenido, estado))
    df = pd.DataFrame(filas, columns=_COLUMNAS_API)
    log.info(f"  {year}: {len(df):,} registros de {len(unicos):,} avisos "
             f"({int((df['_xml_eforms'] != '').sum()):,} filas sin resultados del XML eForms)")
    if not xml_completo:
        complete = False

    if not complete:
        # No cachear: una descarga cortada se reutilizaría después como completa
        # (y sin versión nueva no retira ningún aviso del consolidado)
        log.warning(f"  {year}: descarga API INCOMPLETA (errores, límite de paginación o XML sin "
                    f"descargar); no se guarda la cache {cache_path.name}")
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


def _download_api_range(year, date_from, date_to, period_label, filtro=""):
    """Descarga [date_from, date_to]; si la consulta alcanza el límite de
    paginación, la repite por subperiodos (recursivo).

    Returns: (avisos, complete), un dict por aviso (_aviso_api). Antes solo se
    dividía una vez en trimestres: un trimestre con más de 15.000 avisos
    quedaba truncado.
    """
    records, hit_limit, complete = _download_api_period(year, date_from, date_to, period_label, filtro)
    if not hit_limit:
        return records, complete
    parts = _subperiods(date_from, date_to)
    if not parts:
        log.warning(f"  {period_label}: límite de paginación en un solo día; descarga INCOMPLETA")
        return records, False
    log.info(f"  {period_label}: límite paginación alcanzado, dividiendo en {len(parts)} periodos...")
    all_records, all_complete = [], True
    for sub_from, sub_to, sub_label in parts:
        sub_records, sub_complete = _download_api_range(year, sub_from, sub_to, sub_label, filtro)
        all_records.extend(sub_records)
        all_complete = all_complete and sub_complete
    return all_records, all_complete


def _download_api_period(year, date_from, date_to, period_label, filtro=""):
    """
    Descarga un periodo específico de la API (filtro: condición que se añade a la consulta).
    Returns: (avisos, hit_pagination_limit, complete), un dict por aviso (_aviso_api)
      complete=False si la paginación se cortó (errores HTTP/red o límite de
      paginación) antes de recibir todos los avisos que anuncia la API.
    """
    query = (
        f"notice-type IN ({', '.join(TEDConfig.API_NOTICE_TYPES)}) "
        f"AND buyer-country=ESP "
        f"AND publication-date>={date_from} "
        f"AND publication-date<={date_to}"
        f"{filtro}"
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
            records.append(_aviso_api(notice))
        
        # Paginación
        if total_pages is not None and page >= total_pages:
            break
        
        if len(notices) < TEDConfig.TED_API_PAGE_SIZE:
            break
        
        page += 1
        time.sleep(TEDConfig.TED_API_RATE_LIMIT)
        
        if page % 20 == 0:
            log.info(f"    {period_label} pág {page}: {len(records):,} avisos...")
    
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


# ═══════════════════════════════════════════════════════════════════════════
#  XML eForms DE CADA AVISO: resultado de lote → oferta → ganador
# ═══════════════════════════════════════════════════════════════════════════
#
# La API de búsqueda aplana cada campo en una lista del aviso entero y además
# deduplica los valores iguales (tres contratos con la misma fecha dan una sola
# fecha). Hasta sept. 2026 el parser rellenaba la fila i con el elemento i de
# cada lista y, al acabarse una, repetía el último ganador y el primer importe:
# 159.677 filas copia (141548-2026: 48.861 filas para 951 adjudicaciones de 226
# empresas). La API no da los enlaces entre resultado, oferta, parte licitadora
# y organización (OPT-320, OPT-310, OPT-300, OPT-200), así que las filas salen
# del XML eForms del aviso:
#   efac:LotResult (lote, BT-142, BT-144, ofertas recibidas)
#     → efac:LotTender (BT-720 importe de la oferta) → efac:TenderingParty
#     → efac:Tenderer → efac:Organization (nombre, NIF, país, tamaño)
#   y el contrato (efac:SettledContract: BT-150, BT-1451, BT-145) que cita la oferta.
# Una fila por oferta ganadora de cada resultado de lote; un resultado sin
# oferta (lote desierto o sin adjudicar) es una fila sin ganador; un aviso sin
# resultados, una fila con los datos del aviso. Un grupo de empresas (UTE sin
# constituir) va en una fila con los miembros unidos por '---' (el líder
# primero), como en el CSV de 2006-2023.

_NS_EFORMS = {
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'efac': 'http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1',
    'efbc': 'http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1',
    'efext': 'http://data.europa.eu/p27/eforms-ubl-extensions/1',
    'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
}
_EXT_EFORMS = 'ext:UBLExtensions/ext:UBLExtension/ext:ExtensionContent/efext:EformsExtension'
# Raíz de un aviso eForms (UBL 2.3: ContractAwardNotice-2, ContractNotice-2...). Los avisos del
# esquema anterior (TED_EXPORT R2.0.9) aún se publicaban a principios de 2024 (1.523 CAN de España)
_RAIZ_EFORMS = '{urn:oasis:names:specification:ubl:schema:xsd:'

# Columnas de las filas de la API, siempre las mismas y en este orden (una caché con otras
# columnas sería otra versión aunque no cambie ningún dato)
_COLUMNAS_API = [
    # Aviso y procedimiento (API de búsqueda)
    'ted_notice_id', 'year', 'iso_country', 'notice_type', 'notice_subtype', 'form_type',
    'publication_date', 'procedure_type', 'contract_nature', 'type_of_contract', 'place_of_performance',
    'title_proc', 'description_proc',
    'cae_name', 'cae_nationalid', 'cae_town', 'buyer_legal_type', 'buyer_contracting_entity',
    'buyer_profile', 'cpv', 'total_value', 'total_value_cur', 'estimated_value_proc',
    'estimated_value_proc_cur', 'procedure_id', 'internal_id_proc', 'modification_prev_notice',
    'direct_award_justification', 'direct_award_justification_text', 'sme_participation',
    'notice_identifier', 'notice_version', 'changed_notice', 'change_reason_code', 'dt_dispatch',
    # Aviso (XML): valor del aviso (BT-161) y de los acuerdos marco (BT-118, BT-1118) por separado
    'notice_value', 'notice_value_cur', 'notice_framework_max_value', 'notice_framework_approx_value',
    # Fila: resultado de lote (XML)
    'lot_index', 'n_filas_aviso', 'lot_result_id', 'lot_id', 'internal_id_lot', 'title_lot',
    'description_lot', 'cpv_lot', 'estimated_value_lot', 'estimated_value_lot_cur', 'duration_lot',
    'duration_lot_unit', 'award_criterion_type', 'award_criterion_weight', 'winner_selection_status',
    'non_award_justification', 'number_offers', 'tender_value_lowest', 'tender_value_highest',
    'framework_max_lot', 'framework_est_value',
    # Oferta ganadora, su parte licitadora y sus organizaciones (XML)
    'tender_id', 'tender_reference', 'tender_rank', 'tender_value', 'tender_value_cur',
    'subcontracting_value', 'paid_amount', 'penalties_amount', 'tendering_party_name',
    'win_name', 'win_nationalid', 'win_country', 'win_town', 'win_size',
    # Contrato que cita la oferta (XML)
    'contract_id', 'dt_award', 'contract_award_dates', 'contract_conclusion_date', 'contract_title',
    'contract_framework',
    'source', '_xml_eforms',
]


def _txt(e, ruta):
    x = e.find(ruta, _NS_EFORMS) if e is not None else None
    return x.text.strip() if x is not None and x.text else ''


def _cantidad(e, ruta):
    """(importe, moneda) de un elemento con currencyID; ('', '') si no está."""
    x = e.find(ruta, _NS_EFORMS) if e is not None else None
    if x is None or not x.text or not x.text.strip():
        return '', ''
    return x.text.strip(), x.get('currencyID', '')


def _textos(e, ruta):
    return [x.text.strip() for x in e.findall(ruta, _NS_EFORMS) if x.text and x.text.strip()] if e is not None else []


def _en_castellano(e, ruta):
    """Texto multilingüe (languageID): castellano, si no inglés y si no el primero, como la API."""
    if e is None:
        return ''
    valores = [(x.get('languageID', '').upper(), x.text.strip())
               for x in e.findall(ruta, _NS_EFORMS) if x.text and x.text.strip()]
    for idioma in ('SPA', 'ENG'):
        for lengua, texto in valores:
            if lengua == idioma:
                return texto
    return valores[0][1] if valores else ''


def _id_organizacion(ids):
    """Identificador de una organización (BT-501, puede haber varios: 'ID_PLATAFORMA' y 'NIF'):
    el que tiene forma de NIF (9 caracteres con letra al principio o al final), si no el primero.
    Nunca el identificador técnico del aviso (ORG-0001)."""
    for v in ids:
        if len(v) == 9 and (v[0].isalpha() or v[-1].isalpha()):
            return v
    return ids[0] if ids else ''


def _parse_eforms(contenido):
    """Lee el XML eForms de un aviso: {'aviso': {...}, 'filas': [{...}, ...]}, o None si el XML no
    es eForms (esquema anterior). Lanza ET.ParseError si no es XML."""
    raiz = ET.fromstring(contenido)
    if not raiz.tag.startswith(_RAIZ_EFORMS):
        return None
    ext = _EXT_EFORMS
    orgs = {}
    for org in raiz.findall(ext + '/efac:Organizations/efac:Organization', _NS_EFORMS):
        emp = org.find('efac:Company', _NS_EFORMS)
        if emp is None:
            continue
        orgs[_txt(emp, 'cac:PartyIdentification/cbc:ID')] = {
            'name': _en_castellano(emp, 'cac:PartyName/cbc:Name'),
            'id': _id_organizacion(_textos(emp, 'cac:PartyLegalEntity/cbc:CompanyID')),
            'country': _txt(emp, 'cac:PostalAddress/cac:Country/cbc:IdentificationCode'),
            'town': _txt(emp, 'cac:PostalAddress/cbc:CityName'),
            'size': _txt(emp, 'efbc:CompanySizeCode'),
        }
    lotes = {}
    for lote in raiz.findall('cac:ProcurementProjectLot', _NS_EFORMS):
        pp = lote.find('cac:ProcurementProject', _NS_EFORMS)
        est, est_cur = _cantidad(pp, 'cac:RequestedTenderTotal/cbc:EstimatedOverallContractAmount')
        dur = pp.find('cac:PlannedPeriod/cbc:DurationMeasure', _NS_EFORMS) if pp is not None else None
        tipos, pesos = [], []
        for crit in lote.findall('cac:TenderingTerms/cac:AwardingTerms/cac:AwardingCriterion/'
                                 'cac:SubordinateAwardingCriterion', _NS_EFORMS):
            tipos.append(_txt(crit, 'cbc:AwardingCriterionTypeCode'))
            for par in crit.findall(ext + '/efac:AwardCriterionParameter', _NS_EFORMS):
                codigo = par.find('efbc:ParameterCode', _NS_EFORMS)
                if codigo is not None and codigo.get('listName') == 'number-weight':
                    pesos.append(_txt(par, 'efbc:ParameterNumeric'))
        lotes[_txt(lote, 'cbc:ID')] = {
            # BT-22: el esquema lo llama InternalID, pero la PLACSP usa 'ID_LOTE'
            'internal_id_lot': _txt(pp, 'cbc:ID'),
            'title_lot': _en_castellano(pp, 'cbc:Name'),
            'description_lot': _en_castellano(pp, 'cbc:Description'),
            'cpv_lot': _txt(pp, 'cac:MainCommodityClassification/cbc:ItemClassificationCode'),
            'estimated_value_lot': est, 'estimated_value_lot_cur': est_cur,
            'duration_lot': dur.text.strip() if dur is not None and dur.text else '',
            'duration_lot_unit': dur.get('unitCode', '') if dur is not None else '',
            'award_criterion_type': ';'.join(tipos),
            'award_criterion_weight': ';'.join(pesos),
        }
    aviso, filas = {}, []
    nr = raiz.find(ext + '/efac:NoticeResult', _NS_EFORMS)
    if nr is None:
        return {'aviso': aviso, 'filas': filas}
    aviso['notice_value'], aviso['notice_value_cur'] = _cantidad(nr, 'cbc:TotalAmount')
    aviso['notice_framework_max_value'] = _cantidad(nr, 'efbc:OverallMaximumFrameworkContractsAmount')[0]
    aviso['notice_framework_approx_value'] = _cantidad(nr, 'efbc:OverallApproximateFrameworkContractsAmount')[0]
    ofertas = {}
    for t in nr.findall('efac:LotTender', _NS_EFORMS):
        valor, moneda = _cantidad(t, 'cac:LegalMonetaryTotal/cbc:PayableAmount')
        ofertas[_txt(t, 'cbc:ID')] = {
            'tender_value': valor, 'tender_value_cur': moneda,
            'tender_rank': _txt(t, 'cbc:RankCode'),
            'tender_reference': _txt(t, 'efac:TenderReference/cbc:ID'),
            'subcontracting_value': _cantidad(t, 'efac:SubcontractingTerm/efbc:TermAmount')[0],
            'paid_amount': _cantidad(t, 'efac:AggregatedAmounts/cbc:PaidAmount')[0],
            'penalties_amount': _cantidad(t, 'efac:AggregatedAmounts/efbc:PenaltiesAmount')[0],
            '_parte': _txt(t, 'efac:TenderingParty/cbc:ID'),
        }
    partes = {}
    for p in nr.findall('efac:TenderingParty', _NS_EFORMS):
        miembros = sorted(
            (0 if _txt(ten, 'efbc:GroupLeadIndicator').lower() == 'true' else 1, i, _txt(ten, 'cbc:ID'))
            for i, ten in enumerate(p.findall('efac:Tenderer', _NS_EFORMS)))
        partes[_txt(p, 'cbc:ID')] = (_txt(p, 'cbc:Name'), [o for _, _, o in miembros])
    contratos, contratos_de_oferta = {}, defaultdict(list)
    for c in nr.findall('efac:SettledContract', _NS_EFORMS):
        cid = _txt(c, 'cbc:ID')
        contratos[cid] = {
            'contract_id': _txt(c, 'efac:ContractReference/cbc:ID'),
            'dt_award': _txt(c, 'cbc:AwardDate'),
            'contract_conclusion_date': _txt(c, 'cbc:IssueDate'),
            'contract_title': _en_castellano(c, 'cbc:Title'),
            'contract_framework': _txt(c, 'efbc:ContractFrameworkIndicator'),
        }
        for tid in _textos(c, 'efac:LotTender/cbc:ID'):
            contratos_de_oferta[tid].append(cid)
    for r in nr.findall('efac:LotResult', _NS_EFORMS):
        lote = _txt(r, 'efac:TenderLot/cbc:ID')
        estadisticas = {_txt(s, 'efbc:StatisticsCode'): _txt(s, 'efbc:StatisticsNumeric')
                        for s in r.findall('efac:ReceivedSubmissionsStatistics', _NS_EFORMS)}
        base = {
            'lot_result_id': _txt(r, 'cbc:ID'), 'lot_id': lote,
            'winner_selection_status': _txt(r, 'cbc:TenderResultCode'),
            'non_award_justification': _txt(r, 'efac:DecisionReason/efbc:DecisionReasonCode'),
            # BT-760 del tipo 'tenders' (BT-759): ofertas recibidas para este lote
            'number_offers': estadisticas.get('tenders', ''),
            'tender_value_lowest': _cantidad(r, 'cbc:LowerTenderAmount')[0],
            'tender_value_highest': _cantidad(r, 'cbc:HigherTenderAmount')[0],
            'framework_max_lot': _cantidad(r, 'efac:FrameworkAgreementValues/cbc:MaximumValueAmount')[0],
            'framework_est_value': _cantidad(r, 'efac:FrameworkAgreementValues/efbc:ReestimatedValueAmount')[0],
            **lotes.get(lote, {}),
        }
        del_resultado = list(dict.fromkeys(_textos(r, 'efac:SettledContract/cbc:ID')))
        ids_ofertas = list(dict.fromkeys(_textos(r, 'efac:LotTender/cbc:ID')))
        if not ids_ofertas:
            filas.append(base)   # resultado sin oferta ganadora: lote desierto, sin adjudicar...
            continue
        for tid in ids_ofertas:
            oferta = ofertas.get(tid, {})
            nombre_parte, miembros = partes.get(oferta.get('_parte', ''), ('', []))
            miembros = [orgs.get(o, {}) for o in miembros]
            # El contrato de la oferta: los del resultado que la citan (BT-3202); si ninguno, los que
            # la citan y si tampoco, los del resultado (un contrato puede citar ofertas de varios lotes)
            de_oferta = contratos_de_oferta.get(tid, [])
            cids = [c for c in del_resultado if c in de_oferta] or list(dict.fromkeys(de_oferta)) \
                or del_resultado
            cs = [contratos[c] for c in cids if c in contratos]
            fila = dict(base, tender_id=tid, tendering_party_name=nombre_parte,
                        **{k: v for k, v in oferta.items() if not k.startswith('_')})
            for campo, clave in (('win_name', 'name'), ('win_nationalid', 'id'), ('win_country', 'country'),
                                 ('win_town', 'town'), ('win_size', 'size')):
                fila[campo] = '---'.join(m.get(clave, '') for m in miembros)
            for campo in ('contract_id', 'contract_conclusion_date', 'contract_title', 'contract_framework'):
                fila[campo] = '---'.join(dict.fromkeys(c[campo] for c in cs if c[campo]))
            fechas = list(dict.fromkeys(c['dt_award'] for c in cs if c['dt_award']))
            # Varias fechas de adjudicación (contratos distintos de la oferta): la primera en dt_award
            # y todas en contract_award_dates
            fila['dt_award'] = min(fechas) if fechas else ''
            fila['contract_award_dates'] = '---'.join(fechas) if len(fechas) > 1 else ''
            filas.append(fila)
    return {'aviso': aviso, 'filas': filas}


def _aviso_api(notice):
    """Datos del aviso y del procedimiento de un resultado de la API de búsqueda (sin filas)."""
    pub_number = str(notice.get("publication-number", "") or "")
    contract_nature = _scalar(notice.get("contract-nature-main-proc", []))

    def todos(campo):
        return ";".join(_scalar(v) for v in _as_list(notice.get(campo, [])) if _scalar(v))

    return {
        "ted_notice_id": pub_number,
        # Año de publicación (del publication-number: XXXXXX-YYYY)
        "year": pub_number.split("-")[-1] if "-" in pub_number else "",
        "iso_country": _first_of_list(notice.get("buyer-country", []), "ES"),
        "notice_type": _scalar(notice.get("notice-type", "")),
        "notice_subtype": _scalar(notice.get("notice-subtype", [])),
        "form_type": _scalar(notice.get("form-type", [])),
        "publication_date": _scalar(notice.get("publication-date", [])),
        "procedure_type": _scalar(notice.get("procedure-type", [])),
        "contract_nature": contract_nature,
        # Misma codificación que TYPE_OF_CONTRACT del CSV (W/U/S)
        "type_of_contract": _CONTRACT_NATURE_TO_CSV.get(contract_nature.lower(), ""),
        "place_of_performance": todos("place-of-performance"),
        "title_proc": _extract_multilang_name(notice.get("title-proc", {})),
        "description_proc": _extract_multilang_name(notice.get("description-proc", {})),
        "cae_name": _extract_multilang_name(notice.get("buyer-name", {})),
        # NIF español: 9 chars tipo A12345678 o P0400000F
        "cae_nationalid": _find_spanish_nif(_as_list(notice.get("buyer-identifier", []))),
        # Lista (['Madrid']) → primer valor; str(lista) dejaba "['Madrid']"
        "cae_town": _extract_multilang_name(notice.get("buyer-city", {}))
        if isinstance(notice.get("buyer-city"), dict) else _first_of_list(notice.get("buyer-city", [])),
        "buyer_legal_type": _first_of_list(notice.get("buyer-legal-type", [])),
        "buyer_contracting_entity": _first_of_list(notice.get("buyer-contracting-entity", [])),
        "buyer_profile": _first_of_list(notice.get("buyer-profile", [])),
        "cpv": _first_of_list(notice.get("classification-cpv", [])),
        "total_value": _first_of_list(notice.get("total-value", [])),
        "total_value_cur": _first_of_list(notice.get("total-value-cur", []), "EUR"),
        "estimated_value_proc": _first_of_list(notice.get("estimated-value-proc", [])),
        "estimated_value_proc_cur": _first_of_list(notice.get("estimated-value-cur-proc", [])),
        "procedure_id": _first_of_list(notice.get("procedure-identifier", [])),
        "internal_id_proc": _first_of_list(notice.get("internal-identifier-proc", [])),
        "modification_prev_notice": _first_of_list(notice.get("modification-previous-notice-identifier", [])),
        "direct_award_justification": _first_of_list(notice.get("direct-award-justification-proc", [])),
        "direct_award_justification_text": _extract_multilang_name(
            notice.get("direct-award-justification-text-proc", {})),
        "sme_participation": _first_of_list(notice.get("sme-part", [])),
        "notice_identifier": _scalar(notice.get("notice-identifier", [])),
        "notice_version": _scalar(notice.get("notice-version", [])),
        "changed_notice": todos("change-notice-version-identifier"),
        "change_reason_code": todos("change-reason-code"),
        "dt_dispatch": "",
        "source": "api_v3",
    }


def _filas_aviso(aviso, contenido=None, estado='ok'):
    """Filas de un aviso: los datos del aviso de la API (_aviso_api) en cada fila del XML eForms
    (_parse_eforms). Sin XML legible en eForms, una fila con los datos del aviso y el motivo en
    _xml_eforms ('' si las filas salen del XML)."""
    marca, leido = '', None
    if contenido is None:
        marca = 'sin XML: TED responde 404' if estado == 'no_disponible' else 'sin XML'
    else:
        try:
            leido = _parse_eforms(contenido)
            if leido is None:
                marca = 'XML del esquema anterior a eForms (TED_EXPORT): sin resultados por lote'
        except ET.ParseError as e:
            marca = f'XML ilegible: {e}'
    filas = (leido or {}).get('filas') or [{}]
    datos_xml = (leido or {}).get('aviso', {})
    salida = []
    for i, fila in enumerate(filas):
        todo = {**aviso, **datos_xml, **fila}
        salida.append({c: todo.get(c, '') for c in _COLUMNAS_API})
        salida[-1].update(lot_index=i, n_filas_aviso=len(filas), _xml_eforms=marca,
                          source=aviso.get('source', 'api_v3'))
    return salida


def _parse_api_notice(notice, contenido=None):
    """Filas de un resultado de la API con su XML eForms (sin XML, una fila con los datos del aviso)."""
    return _filas_aviso(_aviso_api(notice), contenido)


def _ruta_xml(numero):
    """XML de un aviso: <DATA_DIR>/xml/<año de publicación>/<número>.xml.gz."""
    numero = str(numero)
    anio = numero.rsplit('-', 1)[-1] if re.fullmatch(r'\d+-\d{4}', numero) else 'otros'
    return TEDConfig.DATA_DIR / TEDConfig.XML_DIR / anio / f"{numero}.xml.gz"


def _leer_xml(numero):
    """Contenido del XML de un aviso guardado en disco, o None si no está o no se puede leer."""
    try:
        return gzip.decompress(_ruta_xml(numero).read_bytes())
    except (OSError, EOFError, zlib.error):
        return None


def _descargar_xml(url):
    """GET del XML de un aviso: (código HTTP, contenido, cabeceras). Aparte para simularlo."""
    r = requests.get(url, timeout=(30, 120), headers={"Accept": "application/xml"})
    return r.status_code, r.content, r.headers


class _Ritmo:
    """Ritmo máximo común a varios hilos: como mucho `por_segundo` peticiones por segundo."""

    def __init__(self, por_segundo):
        self.intervalo = 1.0 / por_segundo if por_segundo else 0.0
        self.siguiente = 0.0
        self.cerrojo = threading.Lock()

    def esperar(self):
        with self.cerrojo:
            ahora = time.monotonic()
            turno = max(ahora, self.siguiente)
            self.siguiente = turno + self.intervalo
        if turno > ahora:
            time.sleep(turno - ahora)


def _obtener_xml(numero, ritmo, forzar=False):
    """Descarga y guarda el XML de un aviso si no está en disco (o siempre, con forzar): 'ok',
    'no_disponible' (TED responde 404 dos veces) o 'error' (red, 5xx o 429 en todos los intentos).
    Con forzar, un XML que no ha cambiado no crea versión (guardar_version)."""
    ruta = _ruta_xml(numero)
    if not forzar and ruta.is_file() and ruta.stat().st_size > 0:
        return 'ok'
    url = TEDConfig.TED_XML_URL.format(numero=numero)
    veces_404 = 0
    for intento in range(TEDConfig.XML_REINTENTOS):
        ritmo.esperar()
        try:
            codigo, contenido, cabeceras = _descargar_xml(url)
        except requests.exceptions.RequestException as e:
            log.debug(f"  XML {numero}: {e}")
            time.sleep(5 * (intento + 1))
            continue
        if codigo == 200 and contenido and contenido.lstrip()[:1] == b'<':
            # Un aviso publicado no cambia: si ya hubiera otra copia, guardar_version la conserva
            guardar_version(ruta, gzip.compress(contenido, mtime=0))
            return 'ok'
        if codigo == 404:
            veces_404 += 1
            if veces_404 >= 2:
                return 'no_disponible'
        espera = 5 * (intento + 1)
        if codigo == 429:
            try:
                espera = max(espera, float((cabeceras or {}).get('Retry-After', 30)))
            except (TypeError, ValueError):
                espera = max(espera, 30)
        log.debug(f"  XML {numero}: HTTP {codigo}; nuevo intento en {espera:.0f} s")
        time.sleep(espera)
    return 'error'


def _xml_avisos(numeros, forzar=False):
    """XML eForms de los avisos: descarga en paralelo (TEDConfig.XML_WORKERS, como mucho
    XML_MAX_POR_SEGUNDO) los que no están en disco, o todos con forzar (--force). Devuelve
    ({número: estado}, completo):
    completo=False si alguno ha fallado por red o por el servidor (el año no se guarda y la
    siguiente ejecución continúa: lo ya descargado queda en disco)."""
    numeros = [n for n in dict.fromkeys(numeros) if n]
    estados, faltan = {}, []
    for n in numeros:
        ruta = _ruta_xml(n)
        if not forzar and ruta.is_file() and ruta.stat().st_size > 0:
            estados[n] = 'ok'
        else:
            faltan.append(n)
    if faltan:
        log.info(f"  XML eForms: {len(numeros) - len(faltan):,} en disco; se piden {len(faltan):,} "
                 f"({TEDConfig.XML_WORKERS} a la vez, ≤{TEDConfig.XML_MAX_POR_SEGUNDO:g}/s)")
        ritmo = _Ritmo(TEDConfig.XML_MAX_POR_SEGUNDO)
        with ThreadPoolExecutor(max_workers=TEDConfig.XML_WORKERS) as hilos:
            for i, (n, estado) in enumerate(zip(faltan, hilos.map(lambda x: _obtener_xml(x, ritmo, forzar), faltan)), 1):
                estados[n] = estado
                if i % 2000 == 0:
                    log.info(f"    XML {i:,}/{len(faltan):,}")
    cuenta = Counter(estados.values())
    if cuenta.get('no_disponible') or cuenta.get('error'):
        ejemplos = [n for n, e in estados.items() if e != 'ok'][:5]
        log.warning(f"  XML eForms: {cuenta.get('no_disponible', 0):,} avisos sin XML en TED (404) y "
                    f"{cuenta.get('error', 0):,} con error (p.ej. {', '.join(ejemplos)})")
    return estados, not cuenta.get('error')


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

# Textos que pandas lee como nulos por defecto (na_values; así se leía el CSV de TED hasta
# sept. 2026). El CSV se sirve tal cual, pero en un NIF 'N/A' o '#N/A N/A' no son un NIF
_TEXTOS_NULOS_PANDAS = ('', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND',
                        '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null')
_NIF_NULOS = frozenset(t.upper() for t in _TEXTOS_NULOS_PANDAS)


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
        # Filas del XML eForms: cada importe en su columna
        'tender_value', 'estimated_value_lot', 'notice_value', 'notice_framework_max_value',
        'notice_framework_approx_value', 'tender_value_lowest', 'tender_value_highest',
        'paid_amount', 'penalties_amount',
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
    # Filas de la API leídas del XML eForms (_xml_eforms no nulo): el importe de la fila es el de
    # su oferta ganadora (BT-720) y, sin oferta, el valor del aviso (BT-161) solo si el aviso es de
    # una sola fila; nunca un valor estimado ni el total del aviso repetido en cada fila. En otra
    # moneda, ninguno (importe_ted va en euros). Las filas del CSV no cambian
    if '_xml_eforms' in df.columns:
        del_xml = df['_xml_eforms'].notna()
        if del_xml.any():
            def _en_euros(col):
                return df[col].fillna('').astype(str).isin(['EUR', '']) if col in df.columns \
                    else pd.Series(True, index=df.index)
            oferta = df['tender_value'] if 'tender_value' in df.columns else pd.Series(np.nan, index=df.index)
            importe = oferta.where(_en_euros('tender_value_cur'))
            if 'notice_value' in df.columns and 'n_filas_aviso' in df.columns:
                una_fila = pd.to_numeric(df['n_filas_aviso'], errors='coerce').eq(1)
                usar_aviso = importe.isna() & oferta.isna() & una_fila & _en_euros('notice_value_cur')
                importe = importe.where(~usar_aviso, df['notice_value'])
            df.loc[del_xml, 'importe_ted'] = importe[del_xml]

    # ── Limpiar NIF del ganador ──
    nif_col = 'win_nationalid' if 'win_nationalid' in df.columns else None
    if nif_col:
        df['win_nif_clean'] = df[nif_col].fillna('').astype(str).str.strip().str.upper()
        # Quitar prefijo país (ES, ES-, ESA, etc.) solo si va seguido del NIF
        df['win_nif_clean'] = df['win_nif_clean'].str.replace(r'^ES[-\s]*(?=[A-Z0-9])', '', regex=True)
        # Quitar strings vacías y los textos que pandas leía como nulos ("N/A", "#N/A N/A"...)
        df.loc[df['win_nif_clean'].isin(_NIF_NULOS), 'win_nif_clean'] = ''
        df.loc[df['win_nif_clean'].str.len() < 5, 'win_nif_clean'] = ''
    else:
        df['win_nif_clean'] = ''
    
    n_nif = (df['win_nif_clean'] != '').sum()
    log.info(f"  NIFs ganador limpios: {n_nif:,} ({n_nif/len(df)*100:.1f}%)")
    
    # ── Limpiar NIF del órgano ──
    if 'cae_nationalid' in df.columns:
        df['cae_nif_clean'] = df['cae_nationalid'].fillna('').astype(str).str.strip().str.upper()
        df['cae_nif_clean'] = df['cae_nif_clean'].str.replace(r'^ES[-\s]*', '', regex=True)
        df.loc[df['cae_nif_clean'].isin(_NIF_NULOS), 'cae_nif_clean'] = ''
    
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
    
    # ── Cancelados: se conservan (reglas 1 y 2 de docs/CONTINUACION.md §2), con cancelled='1' ──
    # Antes se eliminaban aquí. Los cruces los excluyen después de quedarse con la última versión
    # de cada aviso (avisos_para_cruce, run_ted_crossvalidation.load_ted).
    if 'cancelled' in df.columns:
        n_cancelled = (df['cancelled'].astype(str) == '1').sum()
        if n_cancelled > 0:
            log.info(f"  {n_cancelled:,} avisos cancelados (se conservan con cancelled='1')")
    
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


def avisos_para_cruce(df_ted):
    """Avisos que cuentan en un cruce o un recuento: la última versión de cada aviso
    (ultima_version_por_aviso) sin los cancelados, que el consolidado conserva con cancelled='1'
    (TED usa '0'/'1'). El orden importa: un aviso que TED cancela en una descarga posterior queda
    fuera entero; quitando antes los cancelados, volvería con su versión anterior."""
    df_ted = ultima_version_por_aviso(df_ted)
    if df_ted is None or 'cancelled' not in df_ted.columns:
        return df_ted
    # Con tipos que admiten nulos (string, Int64) la comparación da <NA>: un nulo no es un cancelado
    cancelado = pd.to_numeric(df_ted['cancelled'], errors='coerce').eq(1).fillna(False).astype(bool)
    return df_ted[~cancelado.to_numpy()]


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
    
    # Histórico de ted_es_can.parquet: cada aviso una vez (su última versión), sin los cancelados
    df_ted = avisos_para_cruce(df_ted)
    
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
            if df_ted is None:
                log.error("La descarga SPARQL no ha devuelto datos: la ejecución sale con código 1")
                sys.exit(1)
            else:
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
            # Sin consolidado guardado la ejecución falla: un cron o una cola no deben darla
            # por buena (antes salía siempre con 0)
            if df_ted is None or df_ted.attrs.get('sin_guardar'):
                log.error("No se ha guardado ted_es_can.parquet: la ejecución sale con código 1")
                sys.exit(1)
            if df_ted.attrs.get('csv_irregular'):
                anios = ', '.join(map(str, df_ted.attrs['csv_irregular']))
                log.error(f"El CSV de {anios} trae registros irregulares (guardados en "
                          f"ted_can_<año>_registros_irregulares.csv; lo descargado sí se ha guardado): "
                          f"la ejecución sale con código 1")
                sys.exit(1)
    
    if args.command in ('validate', 'full'):
        if args.pipeline_file:
            print("\n" + "=" * 70)
            print("  CROSS-VALIDATION TED ↔ PIPELINE")
            print("=" * 70)
            
            ted_path = TEDConfig.DATA_DIR / "ted_es_can.parquet"
            if not ted_path.exists():
                print("❌ Primero ejecuta 'download' para obtener datos TED")
                return
            
            df_ted = pd.read_parquet(ted_path)   # cross_validate_ted deja fuera versiones viejas y cancelados
            
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