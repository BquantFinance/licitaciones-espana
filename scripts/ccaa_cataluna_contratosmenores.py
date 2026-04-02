"""
Contractació Pública de Catalunya - contratos menores

Scraper incremental del buscador avanzado de la PSCP catalana. Guarda el JSON
completo por fase, reanuda desde checkpoint y genera una salida raw + clean a
partir de los incrementales.
"""

import argparse
import asyncio
import json
import logging
import random
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
import certifi
import pandas as pd
from tqdm import tqdm

LOGGER_NAME = "ccaa_cataluna_contratosmenores"
logger = logging.getLogger(LOGGER_NAME)

BASE_URL = "https://contractaciopublica.cat/portal-api"
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_OUTPUT_PATH = REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors.parquet"
DEFAULT_LOG_PATH = SCRIPT_DIR / "ccaa_cataluna_contratosmenores.log"

FASES_NORMAL = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 1200]
FASES_AGREGADAS = [800, 900, 1000, 1100]
FASES_ALL = FASES_NORMAL + FASES_AGREGADAS
FASES_ORGAN_FIRST = [900, 1000]
TIPUS_CONTRACTE = [393, 394, 395, 396, 397, 398, 1000007, 1008217]
AMBITS = [1500001, 1500002, 1500003, 1500004, 1500005]
PROCEDIMENTS = [401, 419, 1000008, 402, 404, 421, 405, 1000010, 1000011, 403, 1000012, 1008211]

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
    count_cache_hits: int = 0
    organ_cache_hits: int = 0
    errors: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)


@dataclass
class Checkpoint:
    completed_fases: list = field(default_factory=list)
    completed_branches: list = field(default_factory=list)
    total_records_so_far: int = 0
    requests_made: int = 0
    selected_fases: list = field(default_factory=list)

    def save(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "completed_fases": self.completed_fases,
                    "completed_branches": self.completed_branches,
                    "total_records_so_far": self.total_records_so_far,
                    "requests_made": self.requests_made,
                    "selected_fases": self.selected_fases,
                    "last_updated": datetime.now().isoformat(),
                },
                f,
                indent=2,
            )

    @classmethod
    def load(cls, path: Path) -> "Checkpoint":
        if not path.exists():
            return cls()
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
            return cls(
                completed_fases=data.get("completed_fases", []),
                completed_branches=data.get("completed_branches", []),
                total_records_so_far=data.get("total_records_so_far", 0),
                requests_made=data.get("requests_made", 0),
                selected_fases=data.get("selected_fases", []),
            )


@dataclass
class BranchPlanEntry:
    fase: int
    params: dict
    root_count: int
    strategy: str
    key: str

def build_logger(log_path: Path) -> logging.Logger:
    log_path = log_path.resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    configured = logging.getLogger(LOGGER_NAME)
    configured.setLevel(logging.INFO)
    configured.propagate = False

    for handler in list(configured.handlers):
        handler.close()
        configured.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    configured.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    configured.addHandler(stream_handler)
    return configured


def build_ssl_context(verify_ssl: bool = True):
    if not verify_ssl:
        return False

    # `aiohttp` puede fallar en macOS con el trust store del sistema; usar
    # certifi hace el scraper más portable sin desactivar verificación.
    return ssl.create_default_context(cafile=certifi.where())


def parse_fases_arg(raw_fases: Optional[str]) -> Optional[list]:
    if not raw_fases:
        return None
    fases = []
    for chunk in raw_fases.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        fases.append(int(chunk))
    return fases


def resolve_selected_fases(include_agregadas: bool = True, raw_fases: Optional[str] = None) -> list:
    fases = parse_fases_arg(raw_fases)
    if fases is not None:
        invalid = [fase for fase in fases if fase not in FASES_ALL]
        if invalid:
            raise ValueError(f"Fases no válidas: {invalid}")
        return fases
    return list(FASES_ALL if include_agregadas else FASES_NORMAL)


def build_output_artifact_paths(output_path: str | Path) -> dict:
    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    branches_dir = output_file.parent / f"{output_file.stem}_branches"
    return {
        "output": output_file,
        "raw": output_file.with_stem(output_file.stem + "_raw"),
        "checkpoint": output_file.with_stem(output_file.stem + "_checkpoint").with_suffix(".json"),
        "analysis": output_file.with_stem(output_file.stem + "_duplicate_analysis").with_suffix(".json"),
        "summary": output_file.with_stem(output_file.stem + "_summary").with_suffix(".json"),
        "branch_summary": output_file.with_stem(output_file.stem + "_branch_summary").with_suffix(".json"),
        "branches_dir": branches_dir,
    }


def write_run_summary(summary_path: Path, payload: dict):
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json_file(path: Path, default):
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def build_params_key(params: dict) -> tuple:
    return tuple(sorted((str(k), str(v)) for k, v in params.items()))


def build_branch_key(params: dict) -> str:
    preferred_order = ["faseVigent", "ambit", "tipusContracte", "procedimentAdjudicacio", "organ"]
    ordered_keys = [key for key in preferred_order if key in params]
    ordered_keys.extend(sorted(key for key in params if key not in preferred_order))
    parts = []
    for key in ordered_keys:
        value = params[key]
        normalized_key = str(key).replace("_", "-")
        normalized_value = str(value).replace("/", "-").replace(" ", "_").replace(";", "")
        parts.append(f"{normalized_key}-{normalized_value}")
    return "__".join(parts)


def branch_key_belongs_to_phase(branch_key: str, fase: int) -> bool:
    return f"faseVigent-{fase}" in branch_key


def build_branch_file_path(output_path: Path, branches_dir: Path, branch_key: str) -> Path:
    branches_dir.mkdir(parents=True, exist_ok=True)
    return branches_dir / f"{output_path.stem}_{branch_key}.parquet"


def upsert_branch_summary(branch_summaries: list, summary: dict):
    for idx, existing in enumerate(branch_summaries):
        if existing.get("branch_key") == summary.get("branch_key"):
            branch_summaries[idx] = summary
            return
    branch_summaries.append(summary)


def should_segment_by_organ_first(base_params: dict) -> bool:
    """Some aggregated phases misbehave when segmented by procedure first."""
    return (
        base_params.get("faseVigent") in FASES_ORGAN_FIRST
        and base_params.get("ambit") is not None
        and base_params.get("tipusContracte") is not None
        and "organ" not in base_params
    )


async def build_branch_plan(
    session: aiohttp.ClientSession,
    fase: int,
    stats: ScraperStats,
    count_cache: Optional[dict] = None,
) -> list[BranchPlanEntry]:
    """Plan the coarse branch split for one phase before scraping rows.

    The goal is to resume safely at branch granularity and to avoid exploring
    the full segmentation tree from scratch on every retry.
    """
    base_params = {"faseVigent": fase}
    phase_count = await get_count(session, base_params, stats, count_cache=count_cache)
    if phase_count == 0:
        return []
    if phase_count < 10000:
        return [
            BranchPlanEntry(
                fase=fase,
                params=base_params,
                root_count=phase_count,
                strategy="phase",
                key=build_branch_key(base_params),
            )
        ]

    branches = []
    for ambit in AMBITS:
        ambit_params = {**base_params, "ambit": ambit}
        ambit_count = await get_count(session, ambit_params, stats, count_cache=count_cache)
        if ambit_count == 0:
            continue
        if ambit_count < 10000:
            branches.append(
                BranchPlanEntry(
                    fase=fase,
                    params=ambit_params,
                    root_count=ambit_count,
                    strategy="ambit",
                    key=build_branch_key(ambit_params),
                )
            )
            continue

        branches_before = len(branches)
        for tipus in TIPUS_CONTRACTE:
            tipus_params = {**ambit_params, "tipusContracte": tipus}
            tipus_count = await get_count(session, tipus_params, stats, count_cache=count_cache)
            if tipus_count == 0:
                continue
            strategy = "tipus"
            if should_segment_by_organ_first(tipus_params):
                strategy = "organ-first"
            branches.append(
                BranchPlanEntry(
                    fase=fase,
                    params=tipus_params,
                    root_count=tipus_count,
                    strategy=strategy,
                    key=build_branch_key(tipus_params),
                )
            )

        if len(branches) == branches_before:
            branches.append(
                BranchPlanEntry(
                    fase=fase,
                    params=ambit_params,
                    root_count=ambit_count,
                    strategy="ambit-fallback",
                    key=build_branch_key(ambit_params),
                )
            )

    if not branches:
        branches.append(
            BranchPlanEntry(
                fase=fase,
                params=base_params,
                root_count=phase_count,
                strategy="phase-fallback",
                key=build_branch_key(base_params),
            )
        )
    return branches


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
                    wait_time = min(10 * (2 ** attempt), 120) * random.uniform(0.85, 1.15)
                    logger.warning(f"HTTP {resp.status} (attempt {attempt+1}/{max_attempts}), waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue

                else:
                    logger.error(f"HTTP {resp.status} for {url} - not retrying")
                    if stats:
                        stats.errors.append({'url': url, 'status': resp.status})
                    return None

        except asyncio.TimeoutError:
            wait_time = 5 * (attempt + 1) * random.uniform(0.85, 1.15)
            logger.warning(f"Timeout (attempt {attempt+1}/{max_attempts}), waiting {wait_time}s...")
            await asyncio.sleep(wait_time)

        except aiohttp.ClientError as e:
            wait_time = 5 * (attempt + 1) * random.uniform(0.85, 1.15)
            logger.warning(f"Connection error (attempt {attempt+1}/{max_attempts}): {e}, waiting {wait_time}s...")
            await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if stats:
                stats.errors.append({'url': url, 'error': str(e)})
            await asyncio.sleep(5 * (attempt + 1) * random.uniform(0.85, 1.15))

    logger.error(f"All {max_attempts} attempts failed for {url}")
    return None


async def get_count(
    session: aiohttp.ClientSession,
    params: dict,
    stats: ScraperStats,
    count_cache: Optional[dict] = None,
) -> int:
    query_params = {
        **params,
        'page': 0,
        'size': 1,
        'inclourePublicacionsPlacsp': 'false',
        'sortField': 'dataUltimaPublicacio',
        'sortOrder': 'desc',
    }
    cache_key = build_params_key(query_params)
    if count_cache is not None and cache_key in count_cache:
        stats.count_cache_hits += 1
        return count_cache[cache_key]
    data = await fetch_json(session, f"{BASE_URL}/cerca-avancada", params=query_params, stats=stats)
    count = data.get('totalElements', 0) if data else 0
    if count_cache is not None:
        count_cache[cache_key] = count
    return count


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
                'inclourePublicacionsPlacsp': 'false',
                'sortField': 'dataUltimaPublicacio',
                'sortOrder': order
            }
            
            data = await fetch_json(session, f"{BASE_URL}/cerca-avancada", params=query_params, stats=stats)
            
            if not data or 'content' not in data or data.get('errorData'):
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error("Too many consecutive failures, stopping segment")
                    break
                await asyncio.sleep(5)
                continue
            
            consecutive_failures = 0
            content = data['content']
            
            if not content:
                break
            
            for r in content:
                key = f"{r.get('id')}_{r.get('descripcio', '')}"
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
        if not data:
            break
        organs.extend(data)
        if len(data) < 1000:
            break
        page += 1
    return organs


async def scrape_by_organ(
    session: aiohttp.ClientSession,
    base_params: dict,
    stats: ScraperStats,
    depth: int,
    organs_cache: dict,
    count_cache: Optional[dict] = None,
    skip_procediment: bool = False,
) -> list:
    """Expand one branch by `organ` using the cached organ catalog per ambit."""
    ambit_id = base_params.get("ambit")
    if not ambit_id:
        logger.warning(f"⚠️ Segment at 10k without ambit: {base_params}")
        return await scrape_segment(session, base_params, stats, "no_ambit")

    if ambit_id not in organs_cache:
        organs_cache[ambit_id] = await get_organs_for_ambit(session, ambit_id, stats)
    else:
        stats.organ_cache_hits += 1

    all_records = []
    for organ in organs_cache[ambit_id]:
        params = {**base_params, "organ": organ["id"]}
        records = await scrape_with_segmentation(
            session,
            params,
            stats,
            depth + 1,
            organs_cache,
            count_cache=count_cache,
            skip_procediment=skip_procediment,
        )
        all_records.extend(records)
    return all_records


async def scrape_with_segmentation(
    session: aiohttp.ClientSession,
    base_params: dict,
    stats: ScraperStats,
    depth: int = 0,
    organs_cache: dict = None,
    count_cache: Optional[dict] = None,
    skip_procediment: bool = False,
) -> list:
    """Recursively scrape one branch until it drops below the 10k API cap.

    Catalunya does not fail uniformly by phase. In particular, 900/1000 behave
    better by splitting on `organ` before `procedimentAdjudicacio`, while 800
    still benefits from the normal procedure-first path.
    """
    if organs_cache is None:
        organs_cache = {}
    
    count = await get_count(session, base_params, stats, count_cache=count_cache)
    
    if count == 0:
        return []
    
    if count < 10000:
        desc = f"depth={depth}, count={count}"
        return await scrape_segment(session, base_params, stats, desc)
    
    stats.segments_over_10k += 1
    all_records = []
    
    if 'faseVigent' not in base_params:
        logger.info(f"Segmenting by faseVigent (count={count})")
        for fase in FASES_ALL:
            params = {**base_params, 'faseVigent': fase}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache, count_cache=count_cache)
            all_records.extend(records)
            
    elif 'ambit' not in base_params:
        logger.debug(f"Segmenting by ambit for fase={base_params.get('faseVigent')}")
        for ambit in AMBITS:
            params = {**base_params, 'ambit': ambit}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache, count_cache=count_cache)
            all_records.extend(records)
            
    elif 'tipusContracte' not in base_params:
        logger.debug(f"Segmenting by tipusContracte for ambit={base_params.get('ambit')}")
        for tipus in TIPUS_CONTRACTE:
            params = {**base_params, 'tipusContracte': tipus}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache, count_cache=count_cache)
            all_records.extend(records)

    elif should_segment_by_organ_first(base_params):
        logger.debug(
            "Segmenting by organ first for aggregated fase=%s ambit=%s tipus=%s",
            base_params.get("faseVigent"),
            base_params.get("ambit"),
            base_params.get("tipusContracte"),
        )
        all_records = await scrape_by_organ(
            session,
            base_params,
            stats,
            depth,
            organs_cache,
            count_cache=count_cache,
        )

    elif 'procedimentAdjudicacio' not in base_params and not skip_procediment:
        logger.debug("Segmenting by procediment")
        for proc in PROCEDIMENTS:
            params = {**base_params, 'procedimentAdjudicacio': proc}
            records = await scrape_with_segmentation(session, params, stats, depth + 1, organs_cache, count_cache=count_cache)
            all_records.extend(records)

        if not all_records and "organ" in base_params:
            logger.warning(
                "Procedure segmentation returned 0 rows for organ-level branch %s; "
                "retrying the organ branch with both sort orders",
                base_params,
            )
            all_records = await scrape_segment(
                session,
                base_params,
                stats,
                "organ_no_procedure",
                both_orders=True,
            )
        elif not all_records and base_params.get("ambit") is not None:
            logger.warning(
                "Procedure segmentation returned 0 rows for %s; falling back to organ segmentation",
                base_params,
            )
            all_records = await scrape_by_organ(
                session,
                base_params,
                stats,
                depth,
                organs_cache,
                count_cache=count_cache,
                skip_procediment=True,
            )
            
    elif 'organ' not in base_params:
        ambit_id = base_params.get('ambit')
        if ambit_id:
            logger.info(f"Segmenting by organ for ambit={ambit_id} (deepest level)")
        all_records = await scrape_by_organ(
            session,
            base_params,
            stats,
            depth,
            organs_cache,
            count_cache=count_cache,
        )
    else:
        logger.warning(f"⚠️ Segment at 10k after ALL segmentation - using both sort orders: {base_params}")
        all_records = await scrape_segment(session, base_params, stats, "max_segmented", both_orders=True)
    
    return all_records


def save_incremental_full_json(records: list, output_path: Path, fase: int):
    """Save FULL JSON records using json_normalize - no field filtering."""
    if not records:
        return
    
    # FULL JSON - flatten everything
    df = pd.json_normalize(records, sep='_')
    
    fase_file = output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"
    df.to_parquet(fase_file, index=False, compression='snappy')
    logger.info(f"💾 Saved {len(df)} records ({len(df.columns)} columns) for fase {fase}")


def save_branch_records(records: list, branch_file: Path):
    if not records:
        pd.DataFrame().to_parquet(branch_file, index=False, compression='snappy')
        return
    df = pd.json_normalize(records, sep='_')
    df.to_parquet(branch_file, index=False, compression='snappy')


def merge_phase_branches(output_path: Path, fase: int, branch_files: list[Path]) -> int:
    dfs = []
    for branch_file in branch_files:
        if branch_file.exists():
            dfs.append(pd.read_parquet(branch_file))
    if not dfs:
        return 0
    df = pd.concat(dfs, ignore_index=True, sort=False)
    fase_file = output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"
    df.to_parquet(fase_file, index=False, compression='snappy')
    logger.info(f"💾 Saved {len(df)} records ({len(df.columns)} columns) for fase {fase}")
    return len(df)


def cleanup_branch_files(branch_files: list[Path]):
    for branch_file in branch_files:
        if branch_file.exists():
            branch_file.unlink()


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


def smart_deduplicate(df: pd.DataFrame, key_cols: list, prefer_cols: list = None) -> pd.DataFrame:
    if prefer_cols is None:
        prefer_cols = []
    
    # Filter to existing columns
    prefer_cols = [c for c in prefer_cols if c in df.columns]
    
    df = df.copy()
    df['_completeness'] = df.notna().sum(axis=1)
    
    sort_cols = key_cols + ['_completeness'] + prefer_cols
    sort_ascending = [True] * len(key_cols) + [False] * (1 + len(prefer_cols))
    
    df_sorted = df.sort_values(sort_cols, ascending=sort_ascending)
    df_deduped = df_sorted.drop_duplicates(subset=key_cols, keep='first')
    df_deduped = df_deduped.drop(columns=['_completeness'])
    
    return df_deduped


async def main(
    output_path: str | Path,
    output_format: str = "parquet",
    include_agregadas: bool = True,
    resume: bool = False,
    cleanup: bool = False,
    raw_fases: Optional[str] = None,
    log_path: str | Path = DEFAULT_LOG_PATH,
    verify_ssl: bool = True,
):
    stats = ScraperStats()
    build_logger(Path(log_path))

    paths = build_output_artifact_paths(output_path)
    output_file = paths["output"]
    raw_file = paths["raw"]
    checkpoint_file = paths["checkpoint"]
    analysis_file = paths["analysis"]
    summary_file = paths["summary"]
    branch_summary_file = paths["branch_summary"]
    branches_dir = paths["branches_dir"]
    selected_fases = resolve_selected_fases(include_agregadas=include_agregadas, raw_fases=raw_fases)
    branch_summaries = load_json_file(branch_summary_file, []) if resume else []
    count_cache = {}
    organs_cache = {}

    checkpoint = Checkpoint()
    if resume and checkpoint_file.exists():
        checkpoint = Checkpoint.load(checkpoint_file)
        if checkpoint.selected_fases and checkpoint.selected_fases != selected_fases:
            raise ValueError(
                f"El checkpoint fue creado con fases {checkpoint.selected_fases} y "
                f"ahora intentas continuar con {selected_fases}"
            )
        stats.requests_made = checkpoint.requests_made
        logger.info("🔄 Resuming from checkpoint:")
        logger.info(f"   Completed fases: {checkpoint.completed_fases}")
        logger.info(f"   Completed branches: {len(checkpoint.completed_branches)}")
        logger.info(f"   Records so far: {checkpoint.total_records_so_far}")

    remaining_fases = [f for f in selected_fases if f not in checkpoint.completed_fases]

    if not remaining_fases:
        logger.info("✅ All fases already completed!")
    else:
        logger.info("📊 Starting Contractació Pública scraper v4 (FULL JSON)...")
        logger.info(f"   Include agregadas: {include_agregadas}")
        logger.info(f"   Selected fases: {selected_fases}")
        logger.info(f"   Fases to scrape: {len(remaining_fases)} remaining")
        logger.info(f"   Auto-cleanup: {cleanup}")

    connector = aiohttp.TCPConnector(ssl=build_ssl_context(verify_ssl=verify_ssl))
    async with aiohttp.ClientSession(connector=connector) as session:
        for fase in tqdm(remaining_fases, desc="Fases"):
            logger.info(f"\n{'='*60}")
            logger.info(f"📁 Processing faseVigent={fase}")

            try:
                branch_plan = await build_branch_plan(session, fase, stats, count_cache=count_cache)
                if not branch_plan:
                    if fase not in checkpoint.completed_fases:
                        checkpoint.completed_fases.append(fase)
                    checkpoint.requests_made = stats.requests_made
                    checkpoint.selected_fases = selected_fases
                    checkpoint.save(checkpoint_file)
                    stats.segments_processed += 1
                    logger.info(f"   ✅ Fase {fase}: 0 rows, total so far: {checkpoint.total_records_so_far}")
                    continue

                branch_files = []
                completed_branch_keys = set(checkpoint.completed_branches)
                logger.info(f"   Branches planned: {len(branch_plan)}")

                for branch in branch_plan:
                    branch_file = build_branch_file_path(output_file, branches_dir, branch.key)
                    branch_files.append(branch_file)

                    if branch.key in completed_branch_keys and branch_file.exists():
                        logger.info(f"   ↪️ Branch already completed: {branch.key}")
                        continue

                    branch_start = time.time()
                    requests_before = stats.requests_made
                    records = await scrape_with_segmentation(
                        session,
                        branch.params,
                        stats,
                        organs_cache=organs_cache,
                        count_cache=count_cache,
                    )
                    save_branch_records(records, branch_file)

                    branch_summary = {
                        "fase": fase,
                        "branch_key": branch.key,
                        "params": branch.params,
                        "strategy": branch.strategy,
                        "root_count": branch.root_count,
                        "rows": len(records),
                        "elapsed_seconds": time.time() - branch_start,
                        "requests_delta": stats.requests_made - requests_before,
                    }
                    upsert_branch_summary(branch_summaries, branch_summary)
                    write_run_summary(branch_summary_file, branch_summaries)

                    if branch.key not in completed_branch_keys:
                        checkpoint.completed_branches.append(branch.key)
                        completed_branch_keys.add(branch.key)
                    checkpoint.total_records_so_far += len(records)
                    checkpoint.requests_made = stats.requests_made
                    checkpoint.selected_fases = selected_fases
                    checkpoint.save(checkpoint_file)

                phase_rows = merge_phase_branches(output_file, fase, branch_files)
                cleanup_branch_files(branch_files)
                stats.segments_processed += 1

                if fase not in checkpoint.completed_fases:
                    checkpoint.completed_fases.append(fase)
                checkpoint.completed_branches = [
                    branch_key
                    for branch_key in checkpoint.completed_branches
                    if not branch_key_belongs_to_phase(branch_key, fase)
                ]
                checkpoint.requests_made = stats.requests_made
                checkpoint.selected_fases = selected_fases
                checkpoint.save(checkpoint_file)
                if branches_dir.exists() and not any(branches_dir.iterdir()):
                    branches_dir.rmdir()

                logger.info(f"   ✅ Fase {fase}: {phase_rows} rows, total so far: {checkpoint.total_records_so_far}")

            except Exception as e:
                logger.error(f"❌ Error processing fase {fase}: {e}")
                logger.info("   Progress saved. Resume with --resume flag.")
                raise

    # Merge
    logger.info("\n📦 Merging all incremental files...")
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
    logger.info("✅ Raw data saved!")
    
    # Analyze duplicates
    key_cols = ['id', 'descripcio']
    logger.info(f"\n🔍 Analyzing duplicates (key: {key_cols})...")
    
    analysis = analyze_duplicates(df_raw, key_cols)
    
    logger.info(f"   Duplicate rows: {analysis['duplicate_rows']:,}")
    logger.info(f"   Duplicate groups: {analysis['duplicate_groups']:,}")
    
    if analysis['differing_columns']:
        logger.info("   Columns that differ within duplicates (top 10):")
        for col_info in analysis['differing_columns'][:10]:
            logger.info(f"      - {col_info['column']}: differs in {col_info['groups_with_differences']:,} groups ({col_info['pct_groups']:.1f}%)")
    
    with analysis_file.open('w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    # Smart deduplication
    logger.info("\n🧹 Smart deduplication...")
    date_cols = [c for c in df_raw.columns if 'dataPublicacio' in c.lower() or 'data' in c.lower()]
    
    df_clean = smart_deduplicate(df_raw, key_cols, prefer_cols=date_cols)
    
    removed_count = len(df_raw) - len(df_clean)
    logger.info(f"   Removed {removed_count:,} duplicate rows")
    logger.info(f"   Clean dataset: {len(df_clean):,} rows, {len(df_clean.columns)} cols")
    
    stats.total_records = len(df_clean)
    
    # Save clean
    logger.info(f"💾 Saving CLEAN data to {output_file}...")
    if output_format == 'parquet':
        df_clean.to_parquet(output_file, index=False, compression='snappy')
    elif output_format == 'csv':
        df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
    else:
        df_clean.to_excel(output_file, index=False)
    # Cleanup only if requested
    if cleanup:
        logger.info("\n🧹 Cleaning up incremental files (--cleanup flag)...")
        cleanup_incremental_files(output_file, checkpoint.completed_fases)
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.info("   Removed checkpoint file")
    else:
        logger.info("\n📁 Incremental files KEPT (use --cleanup to remove)")
    # Stats
    elapsed = time.time() - stats.start_time
    write_run_summary(
        summary_file,
        {
            "generated_at": datetime.now().isoformat(),
            "selected_fases": selected_fases,
            "raw_rows": stats.total_rows,
            "unique_records": stats.total_records,
            "columns": len(df_raw.columns),
            "segments_processed": stats.segments_processed,
            "segments_over_10k": stats.segments_over_10k,
            "api_requests": stats.requests_made,
            "count_cache_hits": stats.count_cache_hits,
            "organ_cache_hits": stats.organ_cache_hits,
            "elapsed_seconds": elapsed,
            "output_format": output_format,
            "output_file": str(output_file),
            "raw_output_file": str(raw_file.with_suffix(f'.{output_format}')),
            "duplicate_analysis_file": str(analysis_file),
            "branch_summary_file": str(branch_summary_file),
        },
    )

    logger.info(f"\n{'='*60}")
    logger.info("✅ COMPLETED")
    logger.info(f"   Total rows scraped: {stats.total_rows:,}")
    logger.info(f"   Total columns: {len(df_raw.columns)}")
    logger.info(f"   Unique records: {stats.total_records:,}")
    logger.info(f"   Segments processed: {stats.segments_processed}")
    logger.info(f"   Segments requiring sub-segmentation: {stats.segments_over_10k}")
    logger.info(f"   API requests: {stats.requests_made:,}")
    logger.info(f"   Count cache hits: {stats.count_cache_hits:,}")
    logger.info(f"   Organ cache hits: {stats.organ_cache_hits:,}")
    logger.info(f"   Time: {elapsed/60:.1f} minutes")
    logger.info(f"   Raw output: {raw_file}")
    logger.info(f"   Clean output: {output_file}")
    logger.info(f"   Summary: {summary_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Contractació Pública de Catalunya (FULL JSON)')
    parser.add_argument('--output', '-o', default=str(DEFAULT_OUTPUT_PATH), help='Output file path')
    parser.add_argument('--format', '-f', choices=['parquet', 'csv', 'xlsx'], default='parquet', help='Output format')
    parser.add_argument('--no-agregadas', action='store_true', help='Skip aggregated phases')
    parser.add_argument('--fases', help='Comma-separated faseVigent values (example: 0,40,1100)')
    parser.add_argument('--resume', '-r', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--cleanup', action='store_true', help='Delete incremental files after completion')
    parser.add_argument('--log-path', default=str(DEFAULT_LOG_PATH), help='Log file path')
    parser.add_argument('--no-verify-ssl', action='store_true', help='Disable SSL verification as emergency fallback')
    args = parser.parse_args()

    asyncio.run(
        main(
            args.output,
            args.format,
            include_agregadas=not args.no_agregadas,
            resume=args.resume,
            cleanup=args.cleanup,
            raw_fases=args.fases,
            log_path=args.log_path,
            verify_ssl=not args.no_verify_ssl,
        )
    )
