"""
Contractació Pública de Catalunya - Complete Scraper v4
Bypasses the 10k limit using multi-dimensional segmentation.

Changes in v4:
- Saves FULL JSON (pd.json_normalize) - no field filtering
- Does NOT auto-delete incremental files
- Optional cleanup with --cleanup flag

Usage:
    python contractacio_scraper_v4.py --output data.parquet
    python contractacio_scraper_v4.py --output data.parquet --resume
    python contractacio_scraper_v4.py --output data.parquet --cleanup  # Delete incremental files after
    python contractacio_scraper_v4.py --output data.parquet --incloure-placsp  # + publicaciones de PLACSP

Duplicados: una misma publicación sale en varias consultas (una por cada fase
vigente que tenga, ambos órdenes, recuperación de huecos) y esas copias son
idénticas. Solo se eliminan las filas idénticas en TODAS las columnas; antes se
deduplicaba por (id, descripcio) y se perdían los contratos distintos de una
misma publicación agregada con la misma descripción (en el parquet publicado,
1.781 grupos (id, descripcio) con expedientId distintos).
"""

import argparse
import asyncio
import aiohttp
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://contractaciopublica.cat/portal-api"

FASES_NORMAL = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 1200]
FASES_AGREGADAS = [800, 900, 1000, 1100]
FASES_ALL = FASES_NORMAL + FASES_AGREGADAS
TIPUS_CONTRACTE = [393, 394, 395, 396, 397, 398, 1000007, 1008217]
AMBITS = [1500001, 1500002, 1500003, 1500004, 1500005]
PROCEDIMENTS = [401, 419, 1000008, 402, 404, 421, 405, 1000010, 1000011, 403, 1000012, 1008211]
# Las listas de tipus/procediment son fijas y la fuente tiene más valores (en el
# dataset PSCP de Socrata ybgg-dgi6: "Privat d'Administració Pública", "Altra
# legislació sectorial", "Negociat amb publicitat", "Tramitació amb mesures de
# gestió eficient", "Específic de Sistema Dinàmic d'adquisició" y vacíos). Esos
# registros no salen en ningún sub-segmento: recuperar_hueco() los busca.

# La API solo sirve las primeras 10.000 posiciones de cada consulta (page * size)
VENTANA_API = 10000

# 'false' = sin las publicaciones de órganos que publican en PLACSP (todas las
# filas publicadas tienen esPlacsp=False). --incloure-placsp las incluye.
INCLOURE_PLACSP = 'false'

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'es',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


@dataclass
class ScraperStats:
    total_records: int = 0
    total_rows: int = 0
    segments_processed: int = 0
    segments_over_10k: int = 0
    requests_made: int = 0
    errors: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)


@dataclass
class Checkpoint:
    completed_fases: list = field(default_factory=list)
    total_records_so_far: int = 0
    requests_made: int = 0
    
    def save(self, path: Path):
        with open(path, 'w') as f:
            json.dump({
                'completed_fases': self.completed_fases,
                'total_records_so_far': self.total_records_so_far,
                'requests_made': self.requests_made,
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
    
    @classmethod
    def load(cls, path: Path) -> 'Checkpoint':
        if not path.exists():
            return cls()
        with open(path) as f:
            data = json.load(f)
            return cls(
                completed_fases=data.get('completed_fases', []),
                total_records_so_far=data.get('total_records_so_far', 0),
                requests_made=data.get('requests_made', 0)
            )


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict = None, stats: ScraperStats = None) -> dict:
    retryable_status_codes = {429, 500, 502, 503, 504}
    max_attempts = 5
    
    for attempt in range(max_attempts):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=30) as resp:
                if stats:
                    stats.requests_made += 1
                
                if resp.status == 200:
                    return await resp.json()
                
                elif resp.status in retryable_status_codes:
                    wait_time = min(10 * (2 ** attempt), 120)
                    logger.warning(f"HTTP {resp.status} (attempt {attempt+1}/{max_attempts}), waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                else:
                    logger.error(f"HTTP {resp.status} for {url} - not retrying")
                    if stats:
                        stats.errors.append({'url': url, 'status': resp.status})
                    return None
                    
        except asyncio.TimeoutError:
            wait_time = 5 * (attempt + 1)
            logger.warning(f"Timeout (attempt {attempt+1}/{max_attempts}), waiting {wait_time}s...")
            await asyncio.sleep(wait_time)
            
        except aiohttp.ClientError as e:
            wait_time = 5 * (attempt + 1)
            logger.warning(f"Connection error (attempt {attempt+1}/{max_attempts}): {e}, waiting {wait_time}s...")
            await asyncio.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if stats:
                stats.errors.append({'url': url, 'error': str(e)})
            await asyncio.sleep(5 * (attempt + 1))
    
    logger.error(f"All {max_attempts} attempts failed for {url}")
    return None


def clave_registro(r: dict) -> str:
    """Identidad de un registro de la API: su JSON completo.

    Las copias que devuelven consultas distintas (una por fase vigente, orden
    asc/desc...) son idénticas. (id, descripcio) no identifica un registro: una
    publicación agregada lista muchos contratos (expedientId distintos) que
    pueden compartir descripción.
    """
    return json.dumps(r, sort_keys=True, ensure_ascii=False, default=str)


async def get_count(session: aiohttp.ClientSession, params: dict, stats: ScraperStats) -> int:
    query_params = {**params, 'page': 0, 'size': 1, 'inclourePublicacionsPlacsp': INCLOURE_PLACSP,
                    'sortField': 'dataUltimaPublicacio', 'sortOrder': 'desc'}
    data = await fetch_json(session, f"{BASE_URL}/cerca-avancada", params=query_params, stats=stats)
    # A failed request is NOT "0 results": abort so the fase is not checkpointed (resume with --resume)
    if not data or data.get('errorData'):
        raise RuntimeError(f"Count request failed for {params}")
    return data.get('totalElements', 0)


async def scrape_segment(session: aiohttp.ClientSession, params: dict, stats: ScraperStats, 
                         desc: str = "", max_pages: int = 100, both_orders: bool = False) -> list:
    records = []
    seen_keys = set()
    
    orders = ['desc', 'asc'] if both_orders else ['desc']
    
    for order in orders:
        page = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while page < max_pages:
            query_params = {
                **params,
                'page': page,
                'size': 100,
                'inclourePublicacionsPlacsp': INCLOURE_PLACSP,
                'sortField': 'dataUltimaPublicacio',
                'sortOrder': order
            }
            
            data = await fetch_json(session, f"{BASE_URL}/cerca-avancada", params=query_params, stats=stats)
            
            if not data or 'content' not in data or data.get('errorData'):
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures, stopping segment")
                    # Do not return a truncated segment as if it were complete
                    raise RuntimeError(f"Too many consecutive failures at page {page} for {params}")
                await asyncio.sleep(5)
                continue
            
            consecutive_failures = 0
            content = data['content']
            
            if not content:
                break
            
            for r in content:
                key = clave_registro(r)
                if key not in seen_keys:
                    seen_keys.add(key)
                    records.append(r)
            
            if len(content) < 100:
                break
                
            page += 1
            
            if page % 10 == 0:
                await asyncio.sleep(0.3)
    
    return records


async def get_organs_for_ambit(session: aiohttp.ClientSession, ambit_id: int, stats: ScraperStats) -> list:
    organs = []
    page = 0
    while True:
        data = await fetch_json(
            session,
            f"{BASE_URL}/organs/noms",
            params={'page': page, 'size': 1000, 'ambitId': ambit_id},
            stats=stats
        )
        if data is None:
            # A partial/empty organ list would silently drop whole >10k segments
            raise RuntimeError(f"Could not fetch organs for ambit={ambit_id} (page {page})")
        if not data:
            break
        organs.extend(data)
        if len(data) < 1000:
            break
        page += 1
    return organs


async def scrape_with_segmentation(session: aiohttp.ClientSession, base_params: dict, 
                                   stats: ScraperStats, depth: int = 0, 
                                   organs_cache: dict = None) -> list:
    if organs_cache is None:
        organs_cache = {}
    
    count = await get_count(session, base_params, stats)
    
    if count == 0:
        return []
    
    if count < 10000:
        desc = f"depth={depth}, count={count}"
        return await scrape_segment(session, base_params, stats, desc)
    
    stats.segments_over_10k += 1
    all_records = []
    dimension = None  # filtro por el que se ha segmentado este nivel
    
    if 'faseVigent' not in base_params:
        logger.info(f"Segmenting by faseVigent (count={count})")
        dimension = 'faseVigent'
        for fase in FASES_ALL:
            params = {**base_params, 'faseVigent': fase}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache)
            all_records.extend(records)
            
    elif 'ambit' not in base_params:
        logger.debug(f"Segmenting by ambit for fase={base_params.get('faseVigent')}")
        dimension = 'ambit'
        for ambit in AMBITS:
            params = {**base_params, 'ambit': ambit}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache)
            all_records.extend(records)
            
    elif 'tipusContracte' not in base_params:
        logger.debug(f"Segmenting by tipusContracte for ambit={base_params.get('ambit')}")
        dimension = 'tipusContracte'
        for tipus in TIPUS_CONTRACTE:
            params = {**base_params, 'tipusContracte': tipus}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache)
            all_records.extend(records)
            
    elif 'procedimentAdjudicacio' not in base_params:
        logger.debug(f"Segmenting by procediment")
        dimension = 'procedimentAdjudicacio'
        for proc in PROCEDIMENTS:
            params = {**base_params, 'procedimentAdjudicacio': proc}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache)
            all_records.extend(records)
            
    elif 'organ' not in base_params:
        ambit_id = base_params.get('ambit')
        if ambit_id:
            logger.info(f"Segmenting by organ for ambit={ambit_id} (deepest level)")
            dimension = 'organ'
            
            if ambit_id not in organs_cache:
                organs_cache[ambit_id] = await get_organs_for_ambit(session, ambit_id, stats)
            
            for organ in organs_cache[ambit_id]:
                params = {**base_params, 'organ': organ['id']}
                records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache)
                all_records.extend(records)
        else:
            logger.warning(f"⚠️ Segment at 10k without ambit: {base_params}")
            all_records = await scrape_segment(session, base_params, stats, "no_ambit")
    else:
        logger.warning(f"⚠️ Segment at 10k after ALL segmentation - using both sort orders: {base_params}")
        dimension = 'ambos_ordenes'
        all_records = await scrape_segment(session, base_params, stats, "max_segmented", both_orders=True)
    
    # Records with values outside the hard-coded segment lists are not reachable: make the gap visible
    if len(all_records) < count:
        logger.warning(f"⚠️ Segmentation coverage gap: got {len(all_records)} of {count} records for {base_params}")
        all_records = await recuperar_hueco(session, base_params, stats, all_records, count, depth,
                                            organs_cache, dimension)
    
    return all_records


async def recuperar_hueco(session: aiohttp.ClientSession, base_params: dict, stats: ScraperStats,
                          records: list, count: int, depth: int, organs_cache: dict,
                          dimension: Optional[str]) -> list:
    """Busca los registros de un segmento que no salieron en sus sub-segmentos.

    Son los que tienen un valor que no está en las listas fijas (tipus o
    procediment vacío o nuevo, órgano que /organs/noms no lista...):
    - si el segmento cabe en dos ventanas de la API (<= 20.000), se pide entero
      en orden descendente y ascendente;
    - si no y se había segmentado por una lista fija (tipus/procediment), se
      vuelve a pedir segmentando por órgano (lista que da la propia API).
    Solo se añaden los registros que faltaban (sin copias).
    """
    extra = []
    if count <= 2 * VENTANA_API and dimension != 'ambos_ordenes':
        extra = await scrape_segment(session, base_params, stats, "gap", both_orders=True)
    elif (dimension in ('tipusContracte', 'procedimentAdjudicacio')
          and base_params.get('ambit') and 'organ' not in base_params):
        ambit_id = base_params['ambit']
        logger.info(f"   Gap recovery by organ for {base_params}")
        if ambit_id not in organs_cache:
            organs_cache[ambit_id] = await get_organs_for_ambit(session, ambit_id, stats)
        for organ in organs_cache[ambit_id]:
            params = {**base_params, 'organ': organ['id']}
            extra.extend(await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache))

    vistos = {clave_registro(r) for r in records}
    nuevos = []
    for r in extra:
        clave = clave_registro(r)
        if clave not in vistos:
            vistos.add(clave)
            nuevos.append(r)
    if nuevos:
        logger.info(f"   ✅ Gap recovery: +{len(nuevos)} records for {base_params}")
    completos = records + nuevos
    if len(completos) < count:
        logger.warning(f"⚠️ Coverage gap remains: {len(completos)} of {count} records for {base_params}")
    return completos


def save_incremental_full_json(records: list, output_path: Path, fase: int):
    """Save FULL JSON records using json_normalize - no field filtering."""
    fase_file = output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"
    if not records:
        # Remove a stale file from a previous run, otherwise it would be merged again
        if fase_file.exists():
            fase_file.unlink()
        return
    
    # FULL JSON - flatten everything
    df = pd.json_normalize(records, sep='_')
    
    df.to_parquet(fase_file, index=False, compression='snappy')
    logger.info(f"💾 Saved {len(df)} records ({len(df.columns)} columns) for fase {fase}")


def load_all_incremental(output_path: Path, completed_fases: list) -> pd.DataFrame:
    dfs = []
    for fase in completed_fases:
        fase_file = output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"
        if fase_file.exists():
            df = pd.read_parquet(fase_file)
            dfs.append(df)
            logger.info(f"📂 Loaded {len(df)} records ({len(df.columns)} cols) from {fase_file.name}")
    
    if dfs:
        # Concat with uniform columns (some fases may have different nested fields)
        return pd.concat(dfs, ignore_index=True, sort=False)
    return pd.DataFrame()


def cleanup_incremental_files(output_path: Path, fases: list):
    for fase in fases:
        fase_file = output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"
        if fase_file.exists():
            fase_file.unlink()
            logger.debug(f"🗑️ Removed {fase_file}")


def analyze_duplicates(df: pd.DataFrame, key_cols: list) -> dict:
    dupes_mask = df.duplicated(subset=key_cols, keep=False)
    dupes = df[dupes_mask].copy()
    
    if len(dupes) == 0:
        return {'duplicate_rows': 0, 'duplicate_groups': 0, 'differing_columns': []}
    
    n_dupe_rows = len(dupes)
    n_dupe_groups = dupes.groupby(key_cols).ngroups
    
    differing_cols = []
    non_key_cols = [c for c in df.columns if c not in key_cols]
    
    for col in non_key_cols:
        try:
            nunique = dupes.groupby(key_cols)[col].nunique()
            if (nunique > 1).any():
                n_groups_differ = (nunique > 1).sum()
                differing_cols.append({
                    'column': col,
                    'groups_with_differences': int(n_groups_differ),
                    'pct_groups': float(n_groups_differ / n_dupe_groups * 100)
                })
        except Exception:
            pass  # Skip columns that can't be compared
    
    differing_cols.sort(key=lambda x: x['groups_with_differences'], reverse=True)
    
    return {
        'duplicate_rows': int(n_dupe_rows),
        'duplicate_groups': int(n_dupe_groups),
        'differing_columns': differing_cols
    }


def _valores_comparables(df: pd.DataFrame) -> pd.DataFrame:
    """Copia de df donde listas/dicts/arrays (campos anidados de la API, que al leer
    el parquet llegan como numpy arrays) pasan a JSON para poder comparar filas."""
    def a_texto(v):
        if isinstance(v, np.ndarray):
            v = v.tolist()
        return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
    
    out = df
    for col in df.columns:
        if df[col].dtype != object:
            continue
        anidado = df[col].map(lambda v: isinstance(v, (list, dict, np.ndarray)))
        if anidado.any():
            if out is df:
                out = df.copy()
            out[col] = df[col].where(~anidado, df[col].map(a_texto))
    return out
    
    
def quitar_copias_identicas(df: pd.DataFrame) -> pd.DataFrame:
    """Quita solo las filas idénticas en todas las columnas (copias del mismo
    registro devueltas por varias consultas). No usa claves parciales."""
    copias = _valores_comparables(df).duplicated(keep='first')
    return df[~copias].reset_index(drop=True)


async def main(output_path: str, output_format: str = 'parquet', include_agregadas: bool = True, 
               resume: bool = False, cleanup: bool = False):
    stats = ScraperStats()
    
    output_file = Path(output_path)
    raw_file = output_file.with_stem(output_file.stem + '_raw')
    checkpoint_file = output_file.with_stem(output_file.stem + '_checkpoint').with_suffix('.json')
    
    checkpoint = Checkpoint()
    if resume and checkpoint_file.exists():
        checkpoint = Checkpoint.load(checkpoint_file)
        stats.requests_made = checkpoint.requests_made
        logger.info(f"🔄 Resuming from checkpoint:")
        logger.info(f"   Completed fases: {checkpoint.completed_fases}")
        logger.info(f"   Records so far: {checkpoint.total_records_so_far}")
    
    fases_to_scrape = FASES_ALL if include_agregadas else FASES_NORMAL
    remaining_fases = [f for f in fases_to_scrape if f not in checkpoint.completed_fases]
    
    if not remaining_fases:
        logger.info("✅ All fases already completed!")
    else:
        logger.info(f"📊 Starting Contractació Pública scraper v4 (FULL JSON)...")
        logger.info(f"   Include agregadas: {include_agregadas}")
        logger.info(f"   Fases to scrape: {len(remaining_fases)} remaining")
        logger.info(f"   Auto-cleanup: {cleanup}")
    
    total_api = None
    async with aiohttp.ClientSession() as session:
        # Total que anuncia la API sin filtro de fase: control de cobertura final
        # (registros cuya fase vigente no está en FASES_ALL no salen en ninguna consulta)
        try:
            total_api = await get_count(session, {}, stats)
            logger.info(f"   API total without phase filter: {total_api:,}")
        except RuntimeError as e:
            logger.warning(f"⚠️ Could not get the API total without phase filter: {e}")

        for fase in tqdm(remaining_fases, desc="Fases"):
            logger.info(f"\n{'='*60}")
            logger.info(f"📁 Processing faseVigent={fase}")
            
            try:
                records = await scrape_with_segmentation(session, {'faseVigent': fase}, stats)
                
                # Save FULL JSON
                save_incremental_full_json(records, output_file, fase)
                
                checkpoint.completed_fases.append(fase)
                checkpoint.total_records_so_far += len(records)
                checkpoint.requests_made = stats.requests_made
                checkpoint.save(checkpoint_file)
                
                stats.segments_processed += 1
                logger.info(f"   ✅ Fase {fase}: {len(records)} rows, total so far: {checkpoint.total_records_so_far}")
                
            except Exception as e:
                logger.error(f"❌ Error processing fase {fase}: {e}")
                logger.info(f"   Progress saved. Resume with --resume flag.")
                raise
    
    # Merge
    logger.info(f"\n📦 Merging all incremental files...")
    df_raw = load_all_incremental(output_file, checkpoint.completed_fases)
    stats.total_rows = len(df_raw)
    
    if len(df_raw) == 0:
        logger.warning("No records found!")
        return
    
    logger.info(f"📊 Total columns in raw data: {len(df_raw.columns)}")
    
    # Save RAW
    logger.info(f"💾 Saving RAW data ({len(df_raw)} rows, {len(df_raw.columns)} cols) to {raw_file}...")
    if output_format == 'parquet':
        df_raw.to_parquet(raw_file.with_suffix('.parquet'), index=False, compression='snappy')
    elif output_format == 'csv':
        df_raw.to_csv(raw_file.with_suffix('.csv'), index=False, encoding='utf-8-sig')
    else:
        df_raw.to_excel(raw_file.with_suffix('.xlsx'), index=False)
    logger.info(f"✅ Raw data saved!")
    
    # Copies of the same record returned by several queries (one per current phase,
    # both sort orders...): only rows identical in ALL columns are removed
    logger.info(f"\n🧹 Removing identical copies...")
    df_clean = quitar_copias_identicas(df_raw)

    removed_count = len(df_raw) - len(df_clean)
    logger.info(f"   Removed {removed_count:,} identical rows")
    logger.info(f"   Clean dataset: {len(df_clean):,} rows, {len(df_clean.columns)} cols")

    # Analyze what is left with the same identifier (informative: nothing else is removed)
    key_cols = ['id', 'expedientId'] if 'expedientId' in df_raw.columns else ['id', 'descripcio']
    logger.info(f"\n🔍 Analyzing duplicates (key: {key_cols})...")
    
    analysis = analyze_duplicates(df_raw, key_cols)
    analysis['identical_rows_removed'] = int(removed_count)
    analysis['rows_clean'] = int(len(df_clean))
    analysis['clean_rows_sharing_key'] = int(df_clean.duplicated(subset=key_cols, keep=False).sum())
    analysis['api_total_without_phase_filter'] = total_api
    
    logger.info(f"   Duplicate rows: {analysis['duplicate_rows']:,}")
    logger.info(f"   Duplicate groups: {analysis['duplicate_groups']:,}")
    logger.info(f"   Clean rows sharing {key_cols} (different content): {analysis['clean_rows_sharing_key']:,}")
    
    if analysis['differing_columns']:
        logger.info(f"   Columns that differ within duplicates (top 10):")
        for col_info in analysis['differing_columns'][:10]:
            logger.info(f"      - {col_info['column']}: differs in {col_info['groups_with_differences']:,} groups ({col_info['pct_groups']:.1f}%)")
    
    if total_api is not None and len(df_clean) < total_api:
        logger.warning(f"⚠️ The API reports {total_api:,} records without phase filter; "
                       f"got {len(df_clean):,} (phases outside FASES_ALL or coverage gaps)")

    analysis_file = output_file.with_stem(output_file.stem + '_duplicate_analysis').with_suffix('.json')
    with open(analysis_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    stats.total_records = len(df_clean)
    
    # Save clean (same extension rule as the raw file, e.g. -f csv must not write CSV into *.parquet)
    clean_file = output_file.with_suffix({'parquet': '.parquet', 'csv': '.csv'}.get(output_format, '.xlsx'))
    logger.info(f"💾 Saving CLEAN data to {clean_file}...")
    if output_format == 'parquet':
        df_clean.to_parquet(clean_file, index=False, compression='snappy')
    elif output_format == 'csv':
        df_clean.to_csv(clean_file, index=False, encoding='utf-8-sig')
    else:
        df_clean.to_excel(clean_file, index=False)
    
    # Cleanup only if requested
    if cleanup:
        logger.info(f"\n🧹 Cleaning up incremental files (--cleanup flag)...")
        cleanup_incremental_files(output_file, checkpoint.completed_fases)
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.info(f"   Removed checkpoint file")
    else:
        logger.info(f"\n📁 Incremental files KEPT (use --cleanup to remove)")
    
    # Stats
    elapsed = time.time() - stats.start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ COMPLETED")
    logger.info(f"   Total rows scraped: {stats.total_rows:,}")
    logger.info(f"   Total columns: {len(df_raw.columns)}")
    logger.info(f"   Unique records: {stats.total_records:,}")
    logger.info(f"   Segments processed: {stats.segments_processed}")
    logger.info(f"   Segments requiring sub-segmentation: {stats.segments_over_10k}")
    logger.info(f"   API requests: {stats.requests_made:,}")
    logger.info(f"   Time: {elapsed/60:.1f} minutes")
    logger.info(f"   Raw output: {raw_file}")
    logger.info(f"   Clean output: {clean_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Contractació Pública de Catalunya (FULL JSON)')
    parser.add_argument('--output', '-o', default='contractacio_publica.parquet', help='Output file path')
    parser.add_argument('--format', '-f', choices=['parquet', 'csv', 'xlsx'], default='parquet', help='Output format')
    parser.add_argument('--no-agregadas', action='store_true', help='Skip aggregated phases')
    parser.add_argument('--resume', '-r', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--cleanup', action='store_true', help='Delete incremental files after completion')
    parser.add_argument('--incloure-placsp', action='store_true',
                        help='Include publications of bodies that publish on PLACSP (inclourePublicacionsPlacsp=true)')
    args = parser.parse_args()

    if args.incloure_placsp:
        INCLOURE_PLACSP = 'true'
    
    asyncio.run(main(args.output, args.format, include_agregadas=not args.no_agregadas, 
                     resume=args.resume, cleanup=args.cleanup))
