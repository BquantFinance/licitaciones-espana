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

Sesgo del superviviente (comun/historico.py; docs/CONTINUACION.md, regla 3):
- Capa cruda: el fichero de cada fase (_fase_<n>), el crudo (_raw) y el
  análisis se escriben con guardar_version: si cambian, la versión anterior
  pasa a _historico/. Una fase que ya no devuelve nada no borra su fichero:
  pasa a _historico/.
- Salida limpia: acumular() de la descarga (sin copias idénticas) sobre la
  salida anterior, por publicación (fila entera, como clave_registro()). Lo que
  el portal retira o cambia se conserva con _en_ultima_descarga=False (una
  publicación cambiada queda como la versión antigua y la nueva). Fecha de la
  descarga: la de la versión del crudo (sin cambios, la misma fecha: una
  re-ejecución sin cambios no crea ninguna versión).
- Ámbito: solo se marca como no servido lo que esta descarga ha vuelto a leer
  entero. Las consultas de FASES_AGREGADAS devuelven las publicaciones
  agregadas (esAgregatContractes o esAgregatEncarrecs) y las de FASES_NORMAL
  las demás (se comprueba en cada ejecución); un grupo está en el ámbito si se
  han leído todas sus fases sin huecos: --no-agregadas no toca las agregadas,
  --resume solo cuenta las fases leídas, y un segmento que no se ha podido leer
  entero (la API sirve 10.000 resultados por consulta y su total también se
  queda en 10.000) deja fuera su órgano o, si no es de un solo órgano, el
  grupo entero. Una fase que vuelve vacía cuando la descarga anterior tenía
  filas también deja fuera su grupo. Una descarga vacía o fallida no retira
  nada (no se toca ninguna salida).
- Semilla (--semilla, p.ej. contractacio_menors.parquet del release v2026.02):
  se le quitan solo sus copias idénticas (2.155.739 de 3.023.802 filas en
  v2026.02) y se añaden, por la clave estable CLAVE_SEMILLA, las publicaciones
  del ámbito que no están en la salida, con _origen='release v2026.02' y
  _en_ultima_descarga=False (comun.historico.sembrar).
"""

import argparse
import asyncio
import aiohttp
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (  # noqa: E402
    ORIGEN_SEMILLA, acumular, archivar, guardar_version, imprimir_informe_semilla, sembrar,
)

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

# La API solo sirve las primeras 10.000 posiciones de cada consulta (page * size), y
# totalElements tampoco pasa de 10.000 (comprobado en vivo el 2026-09-28: sin filtros
# da 10.000): un recuento de 10.000 significa "10.000 o más"
VENTANA_API = 10000
TAMANO_PAGINA = 100

# Clave estable de una publicación para la semilla (sembrar): id de la publicación y
# expedientId (en las agregadas, "<uuid>;<n>" distingue cada contrato de la relación).
# En el publicado v2026.02 (868.063 filas distintas) nunca es nula y solo se repite en
# 2 publicaciones que cambiaron de fase durante aquella descarga (dos versiones de cada
# una; las dos se conservan). id solo no basta (707.006 filas lo comparten: relaciones
# de contratos) y la fila entera (clave_registro) no es estable: cambia con cada fase.
CLAVE_SEMILLA = ['id', 'expedientId']

# Grupos de publicaciones para el ámbito de acumular(): las consultas de
# FASES_AGREGADAS solo devuelven publicaciones agregadas (esAgregatContractes o
# esAgregatEncarrecs) y las de FASES_NORMAL solo las demás (en vivo el 2026-09-28;
# se comprueba en cada ejecución). El nombre de la fase no sirve: las fases 800, 900 y
# 1000 salen como ADJUDICACIO en fasesVigents y las 90 y 200-1200 mezclan varias.
GRUPO_NORMAL = 'normales'
GRUPO_AGREGADAS = 'agregadas'

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
    # Segmentos que no se han podido leer enteros: {'params': filtros, 'motivo': texto}
    huecos: list = field(default_factory=list)
    start_time: float = field(default_factory=time.time)


@dataclass
class Checkpoint:
    completed_fases: list = field(default_factory=list)
    total_records_so_far: int = 0
    requests_made: int = 0
    # Por fase completada (clave: str(fase)), sus segmentos incompletos (lista vacía si se
    # leyó entera): con --resume el ámbito de acumular() sale de aquí. No guarda datos,
    # solo el estado de la ejecución: no pasa por guardar_version.
    huecos: dict = field(default_factory=dict)

    def save(self, path: Path):
        # Escritura atómica: un corte a medias no deja un checkpoint ilegible
        tmp = path.with_name(f".{path.name}.nuevo")
        with open(tmp, 'w') as f:
            json.dump({
                'completed_fases': self.completed_fases,
                'total_records_so_far': self.total_records_so_far,
                'requests_made': self.requests_made,
                'huecos': self.huecos,
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: Path) -> 'Checkpoint':
        if not path.exists():
            return cls()
        with open(path) as f:
            data = json.load(f)
            return cls(
                completed_fases=data.get('completed_fases', []),
                total_records_so_far=data.get('total_records_so_far', 0),
                requests_made=data.get('requests_made', 0),
                # Un checkpoint sin 'huecos' (código anterior): esas fases cuentan como no
                # leídas enteras (calcular_ambito)
                huecos=data.get('huecos', {})
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
                         desc: str = "", max_pages: Optional[int] = None, both_orders: bool = False) -> list:
    """Registros de un segmento (sin copias). Si ningún orden llega al final de los
    resultados (se para en el tope de la ventana de la API) y los dos órdenes no se
    solapan, lo que queda entre las dos ventanas no se ha leído: el segmento se anota
    en stats.huecos (y no se retira nada de él, ver calcular_ambito)."""
    if max_pages is None:
        max_pages = VENTANA_API // TAMANO_PAGINA
    records = []
    seen_keys = set()
    claves_por_orden = {}
    agotado = False   # algún orden ha llegado al final de los resultados

    orders = ['desc', 'asc'] if both_orders else ['desc']

    for order in orders:
        page = 0
        consecutive_failures = 0
        max_consecutive_failures = 3
        claves_orden = claves_por_orden[order] = set()

        while page < max_pages:
            query_params = {
                **params,
                'page': page,
                'size': TAMANO_PAGINA,
                'inclourePublicacionsPlacsp': INCLOURE_PLACSP,
                'sortField': 'dataUltimaPublicacio',
                'sortOrder': order
            }
            
            data = await fetch_json(session, f"{BASE_URL}/cerca-avancada", params=query_params, stats=stats)
            
            if not data or 'content' not in data or data.get('errorData'):
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error("Too many consecutive failures, stopping segment")
                    # Do not return a truncated segment as if it were complete
                    raise RuntimeError(f"Too many consecutive failures at page {page} for {params}")
                await asyncio.sleep(5)
                continue
            
            consecutive_failures = 0
            content = data['content']

            if not content:
                agotado = True
                break

            for r in content:
                key = clave_registro(r)
                claves_orden.add(key)
                if key not in seen_keys:
                    seen_keys.add(key)
                    records.append(r)

            if len(content) < TAMANO_PAGINA:
                agotado = True
                break

            page += 1

            if page % 10 == 0:
                await asyncio.sleep(0.3)

    solapan = both_orders and bool(claves_por_orden['desc'] & claves_por_orden['asc'])
    if not agotado and not solapan:
        logger.warning(f"⚠️ Segmento más grande que la ventana de la API ({len(records)} registros leídos, "
                       f"{'sin solape entre los dos órdenes' if both_orders else 'un solo orden'}): {params}")
        if stats is not None:
            stats.huecos.append({'params': dict(params),
                                 'motivo': f'ventana de la API: {len(records)} registros leídos'})
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
    
    if count < VENTANA_API:
        desc = f"depth={depth}, count={count}"
        records = await scrape_segment(session, base_params, stats, desc)
        # Un segmento que cabe en la ventana y devuelve menos registros de los que anuncia
        # (páginas que se desplazan durante la descarga...) no está entero: se vuelve a pedir
        # en los dos órdenes y, si sigue incompleto, queda anotado en stats.huecos
        if len(records) < count:
            logger.warning(f"⚠️ Segment returned {len(records)} of {count} records: {base_params}")
            records = await recuperar_hueco(session, base_params, stats, records, count, depth,
                                            organs_cache, None)
        return records
    
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
        logger.debug("Segmenting by procediment")
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
    Solo se añaden los registros que faltaban (sin copias). Si aun así faltan, el
    segmento queda anotado en stats.huecos: no se retira nada de él.
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
        stats.huecos.append({'params': dict(base_params),
                             'motivo': f'faltan {count - len(completos)} de {count} registros'})
    return completos


def fichero_fase(output_path: Path, fase: int) -> Path:
    return output_path.parent / f"{output_path.stem}_fase_{fase}.parquet"


def save_incremental_full_json(records: list, output_path: Path, fase: int) -> str:
    """Save FULL JSON records using json_normalize - no field filtering.

    Pasa por guardar_version: si la fase ya tenía fichero y ha cambiado, la versión
    anterior queda en _historico/ (el fichero de una fase es la única copia de su
    descarga hasta que se une al crudo). Sin registros no se deja fichero (si no, se
    volvería a unir), pero el de una ejecución anterior pasa a _historico/ en vez de
    borrarse. Devuelve 'nuevo', 'actualizado', 'sin_cambios', 'archivado' o 'vacía'."""
    fase_file = fichero_fase(output_path, fase)
    if not records:
        if fase_file.exists():
            archivo = archivar(fase_file)
            logger.info(f"🗄️ Fase {fase} sin registros: el fichero anterior pasa a {archivo}")
            return 'archivado'
        return 'vacía'

    # FULL JSON - flatten everything
    df = pd.json_normalize(records, sep='_')

    estado = guardar_tabla(df, fase_file, 'parquet')
    logger.info(f"💾 Saved {len(df)} records ({len(df.columns)} columns) for fase {fase} ({estado})")
    return estado


def load_all_incremental(output_path: Path, completed_fases: list, columna_fase: Optional[str] = None) -> pd.DataFrame:
    """Une los ficheros de las fases completadas. Con columna_fase, cada fila lleva en
    esa columna la fase de la consulta que la devolvió (columna temporal)."""
    dfs = []
    for fase in completed_fases:
        fase_file = fichero_fase(output_path, fase)
        if fase_file.exists():
            df = pd.read_parquet(fase_file)
            if columna_fase:
                df[columna_fase] = fase
            dfs.append(df)
            logger.info(f"📂 Loaded {len(df)} records ({len(df.columns)} cols) from {fase_file.name}")

    if dfs:
        # Concat with uniform columns (some fases may have different nested fields)
        return pd.concat(dfs, ignore_index=True, sort=False)
    return pd.DataFrame()


def cleanup_incremental_files(output_path: Path, fases: list):
    """Borra los ficheros de fase actuales (su contenido ya está en el crudo, que pasa por
    guardar_version); sus versiones anteriores en _historico/ no se tocan."""
    for fase in fases:
        fase_file = fichero_fase(output_path, fase)
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


# =============================================================================
# Salidas sin perder versiones (comun/historico.py)
# =============================================================================

# Tipo con el que esta versión de pandas lee el texto de un parquet ('str' en pandas 3)
DTYPE_TEXTO = pd.Series(['a']).dtype


def _es_texto_o_nulo(v) -> bool:
    return v is None or v is pd.NA or isinstance(v, str) or (isinstance(v, float) and v != v)


def _es_numero_o_nulo(v) -> bool:
    return (v is None or v is pd.NA or (isinstance(v, (int, float, np.integer, np.floating))
                                        and not isinstance(v, (bool, np.bool_))))


def _tipos_estables(df: pd.DataFrame) -> pd.DataFrame:
    """Las columnas object con solo texto (o solo números) y nulos, con el tipo con el que
    pandas las vuelve a leer del parquet. Si no, con pandas 3 una columna de texto que
    llega como object (p.ej. _origen, de sembrar) se lee después como 'str' y los
    metadatos del parquet cambiarían sin que cambie ningún dato: una re-ejecución sin
    cambios guardaría otra versión."""
    cambios = {}
    for columna in df.columns:
        serie = df[columna]
        if serie.dtype != object or not serie.notna().any():
            continue
        if DTYPE_TEXTO != object and serie.map(_es_texto_o_nulo).all():
            cambios[columna] = DTYPE_TEXTO
        elif serie.map(_es_numero_o_nulo).all():
            cambios[columna] = 'float64'
    return df.astype(cambios) if cambios else df


def guardar_tabla(df: pd.DataFrame, destino: Path, formato: str = 'parquet') -> str:
    """Escribe df en `destino` con guardar_version: si ya existía y ha cambiado, la versión
    anterior pasa a _historico/; si es idéntica no se toca. Devuelve el estado
    ('nuevo', 'actualizado' o 'sin_cambios')."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        if formato == 'parquet':
            _tipos_estables(df).to_parquet(tmp, index=False, compression='snappy')
        elif formato == 'csv':
            df.to_csv(tmp, index=False, encoding='utf-8-sig')
        else:
            df.to_excel(tmp, index=False, engine='openpyxl')
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def leer_salida(ruta: Path, formato: str = 'parquet') -> Optional[pd.DataFrame]:
    """Salida anterior (None si no existe). De un CSV o un Excel se lee el texto tal cual
    se escribió: huellas_contenido lo compara igual que el valor con su tipo."""
    ruta = Path(ruta)
    if not ruta.exists():
        return None
    if formato == 'parquet':
        df = pd.read_parquet(ruta)
    elif formato == 'csv':
        df = pd.read_csv(ruta, dtype=str, keep_default_na=False, na_values=[''], encoding='utf-8-sig')
    else:
        df = pd.read_excel(ruta, dtype=str)
    if '_en_ultima_descarga' in df.columns and df['_en_ultima_descarga'].dtype != bool:
        df['_en_ultima_descarga'] = df['_en_ultima_descarga'].map(lambda v: str(v) == 'True').astype(bool)
    return df


def fecha_version(ruta: Path) -> str:
    """Fecha (UTC, ISO, con microsegundos) de la versión actual de un fichero: su fecha
    de modificación. guardar_version no toca un fichero sin cambios: una descarga
    idéntica a la anterior conserva su fecha."""
    return datetime.fromtimestamp(Path(ruta).stat().st_mtime, timezone.utc).isoformat(timespec='microseconds')


def _filas_parquet(ruta: Path) -> int:
    try:
        return pq.ParquetFile(ruta).metadata.num_rows
    except Exception:
        return 0


def _leer_json(ruta: Path) -> dict:
    try:
        with open(ruta, encoding='utf-8') as f:
            datos = json.load(f)
        return datos if isinstance(datos, dict) else {}
    except (OSError, ValueError):
        return {}


# Texto con que se escribe un número (repr de un float o un entero)
_NUMERO = re.compile(r"-?\d+(?:\.\d+)?(?:e[+-]\d+)?")


def _numero_texto(x: float) -> str:
    texto = repr(x)
    return texto[:-2] if texto.endswith('.0') else texto


def _canonico(v):
    """Texto de un valor para comparar publicaciones entre descargas (None si es nulo).
    El mismo número con otro tipo da el mismo texto (6242426, 6242426.0 o '6242426.0'
    leído de un CSV), los booleanos 'True'/'False' y los campos anidados su JSON. De un
    texto solo se normaliza la forma en que se escribe un número: '08002' o '1.50' se
    quedan como están."""
    if isinstance(v, np.ndarray):
        v = v.tolist()
    if isinstance(v, (list, dict, tuple)):
        return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, (bool, np.bool_)):
        return 'True' if v else 'False'
    if isinstance(v, (int, np.integer)):
        return str(int(v))
    if isinstance(v, (float, np.floating)):
        return None if v != v else _numero_texto(float(v))
    if isinstance(v, str):
        if v and v[0] in '-0123456789' and _NUMERO.fullmatch(v):
            numero = float(v)
            if v == repr(numero):
                return _numero_texto(numero)
        return v
    return str(v)


def _texto_comparable(serie) -> np.ndarray:
    """_canonico de cada valor de una columna (array object), vectorizado para los tipos
    booleanos y numéricos de numpy."""
    s = pd.Series(serie)
    tipo = s.dtype
    if tipo == bool:
        return np.where(s.to_numpy(), 'True', 'False').astype(object)
    if isinstance(tipo, np.dtype) and tipo.kind in 'iu':
        return s.to_numpy().astype(str).astype(object)
    if isinstance(tipo, np.dtype) and tipo.kind == 'f':
        valores = s.to_numpy()
        texto = pd.Series(valores.astype(str), dtype=object)   # astype(str) == repr en float64
        entero = texto.str.endswith('.0').to_numpy(dtype=bool)
        texto[entero] = texto[entero].str[:-2]
        texto[np.isnan(valores)] = None
        return texto.to_numpy()
    return np.array([_canonico(v) for v in s.astype(object).to_numpy()], dtype=object)


def huellas_contenido(*tablas) -> list:
    """Huella (hash de 64 bits) del contenido de cada fila de cada tabla, comparable entre
    ellas: todas las columnas de datos (no las de control, que empiezan por '_') en orden
    alfabético, con _canonico. Una columna que falta en una tabla cuenta como nula:
    json_normalize solo crea las columnas de las fases que salen en cada descarga."""
    columnas = sorted({str(c) for t in tablas for c in t.columns if not str(c).startswith('_')})
    huellas = []
    for tabla in tablas:
        h = np.full(len(tabla), 0x345678, dtype=np.uint64)
        mult = np.uint64(1000003)
        for i, columna in enumerate(columnas):
            if columna in tabla.columns:
                texto = _texto_comparable(tabla[columna])
            else:
                texto = np.full(len(tabla), None, dtype=object)
            # hash_array (con categorize) distingue el nulo del texto 'None'
            h = (h ^ pd.util.hash_array(texto)) * mult
            mult = np.uint64(int(mult) + 82520 + 2 * (len(columnas) - i))
        huellas.append(h)
    return huellas


def grupo_fase(fase: int) -> Optional[str]:
    if fase in FASES_AGREGADAS:
        return GRUPO_AGREGADAS
    if fase in FASES_NORMAL:
        return GRUPO_NORMAL
    return None


def grupo_publicacion(df: pd.DataFrame) -> np.ndarray:
    """Grupo de cada publicación por su contenido: agregada si esAgregatContractes o
    esAgregatEncarrecs es True."""
    agregada = np.zeros(len(df), dtype=bool)
    for columna in ('esAgregatContractes', 'esAgregatEncarrecs'):
        if columna in df.columns:
            agregada |= _texto_comparable(df[columna]) == 'True'
    return np.where(agregada, GRUPO_AGREGADAS, GRUPO_NORMAL)


def calcular_ambito(fases_leidas, huecos: dict, particion_ok: bool = True) -> dict:
    """Parte de lo acumulado que esta descarga ha vuelto a leer entera: el ámbito de
    acumular() (lo de fuera no se marca como retirado) y de la semilla.

    Devuelve {grupo: None si queda fuera, o el conjunto de órganos (texto) excluidos}.
    Un grupo está dentro si se han leído todas sus fases y ninguna tiene un hueco que no
    sea de un solo órgano (segmento sin 'organ' incompleto, fase que vuelve vacía cuando
    antes tenía filas o fase sin información de huecos). Si la partición por grupos no se
    ha cumplido (una fase devolvió publicaciones del otro grupo), cuentan como uno solo."""
    leidas = {int(f) for f in fases_leidas}
    ambito = {}
    for grupo, fases in ((GRUPO_NORMAL, FASES_NORMAL), (GRUPO_AGREGADAS, FASES_AGREGADAS)):
        excluidos = set() if fases and set(fases) <= leidas else None
        for fase in (fases if excluidos is not None else ()):
            for hueco in huecos.get(str(fase), [{'motivo': 'sin información de huecos'}]):
                organ = (hueco.get('params') or {}).get('organ')
                if organ is None:
                    excluidos = None
                    break
                excluidos.add(_canonico(organ))
            if excluidos is None:
                break
        ambito[grupo] = excluidos
    if not particion_ok:
        if any(v is None for v in ambito.values()):
            ambito = {g: None for g in ambito}
        else:
            union = set().union(*ambito.values())
            ambito = {g: set(union) for g in ambito}
    return ambito


def filas_en_ambito(df: pd.DataFrame, ambito: dict) -> np.ndarray:
    """Filas (booleano) dentro del ámbito: de un grupo que está dentro y de un órgano que
    no está excluido (sin órgano, solo si el grupo no excluye ninguno)."""
    grupo = grupo_publicacion(df)
    if 'idOrgan' in df.columns:
        organo = pd.Series(_texto_comparable(df['idOrgan']), dtype=object)
    else:
        organo = pd.Series([None] * len(df), dtype=object)
    dentro = np.zeros(len(df), dtype=bool)
    for nombre, excluidos in ambito.items():
        if excluidos is None:
            continue
        filas = grupo == nombre
        if excluidos:
            filas &= (organo.notna() & ~organo.isin(excluidos)).to_numpy(dtype=bool)
        dentro |= filas
    return dentro


def describir_ambito(ambito: dict) -> dict:
    return {g: ('fuera' if v is None else {'organos_excluidos': sorted(v)}) for g, v in ambito.items()}


def acumular_salida(anterior: Optional[pd.DataFrame], nuevos: pd.DataFrame, fecha: str,
                    ambito: dict) -> pd.DataFrame:
    """acumular() de la descarga limpia `nuevos` sobre la salida anterior, por publicación.

    Dos filas son la misma publicación si coincide todo su contenido (como
    clave_registro): se comparan por huellas_contenido, para que un cambio de tipo entre
    descargas (idOrgan entero o decimal según haya nulos, el texto de un CSV...) no
    parezca un cambio. Ninguna fila anterior se elimina; las de `ambito` que no vuelven a
    salir quedan con _en_ultima_descarga=False y las de fuera no cambian."""
    if anterior is None or len(anterior) == 0:
        return acumular(None, nuevos, fecha)
    h_anterior, h_nuevos = huellas_contenido(anterior, nuevos)
    ant = anterior.assign(_huella=h_anterior,
                          _ambito=np.where(filas_en_ambito(anterior, ambito), 'dentro', 'fuera'))
    nue = nuevos.assign(_huella=h_nuevos, _ambito='dentro')
    # Se compara solo la huella: el resto de columnas se ignoran en acumular()
    ignorar = [c for c in dict.fromkeys(list(ant.columns) + list(nue.columns)) if c != '_huella']
    salida = acumular(ant, nue, fecha, ambito=['_ambito'], ignorar=ignorar)
    return salida.drop(columns=['_huella', '_ambito'])


# Segunda clave (16 bytes) de hash_pandas_object: huella de 128 bits por fila de la semilla
CLAVE_HASH_2 = 'pscp-semilla-v02'


def leer_semilla(ruta: Path):
    """El parquet publicado sin sus copias idénticas: (DataFrame, filas leídas, copias).

    El publicado repite la misma publicación en filas idénticas, una por cada consulta
    que la devolvió (en v2026.02, 2.155.739 de 3.023.802 filas, hasta 7 copias): es un
    artefacto de aquella descarga. Solo se quitan las filas idénticas en todas las
    columnas, con sus tipos (como quitar_copias_identicas), y se conserva la primera.
    Para no tener el publicado en pandas dos veces se compara una huella de 128 bits de
    cada fila, calculada columna a columna sobre la tabla de Arrow."""
    tabla = pq.read_table(ruta)
    n = tabla.num_rows
    h1 = np.full(n, 0x345678, dtype=np.uint64)
    h2 = h1.copy()
    mult = np.uint64(1000003)
    for i, nombre in enumerate(tabla.column_names):
        columna = tabla.column(nombre).to_pandas()
        if pa.types.is_nested(tabla.schema.field(nombre).type):
            columna = _valores_comparables(columna.to_frame(nombre))[nombre]
        h1 = (h1 ^ pd.util.hash_pandas_object(columna, index=False).to_numpy()) * mult
        h2 = (h2 ^ pd.util.hash_pandas_object(columna, index=False, hash_key=CLAVE_HASH_2).to_numpy()) * mult
        mult = np.uint64(int(mult) + 82520 + 2 * (tabla.num_columns - i))
        del columna
    copia = pd.DataFrame({'a': h1, 'b': h2}).duplicated().to_numpy()
    semilla = tabla.filter(pa.array(~copia)).to_pandas()
    return semilla, n, int(copia.sum())


async def main(output_path: str, output_format: str = 'parquet', include_agregadas: bool = True, 
               resume: bool = False, cleanup: bool = False, semillas=()):
    stats = ScraperStats()
    
    output_file = Path(output_path)
    raw_file = output_file.with_stem(output_file.stem + '_raw')
    checkpoint_file = output_file.with_stem(output_file.stem + '_checkpoint').with_suffix('.json')
    analysis_file = output_file.with_stem(output_file.stem + '_duplicate_analysis').with_suffix('.json')
    # Same extension rule for raw and clean (e.g. -f csv must not write CSV into *.parquet)
    extension = {'parquet': '.parquet', 'csv': '.csv'}.get(output_format, '.xlsx')
    raw_path = raw_file.with_suffix(extension)
    clean_file = output_file.with_suffix(extension)
    
    # Una semilla inservible se detecta antes de descargar, no después de horas
    semillas = [Path(s) for s in semillas]
    for semilla in semillas:
        if not semilla.is_file():
            raise FileNotFoundError(f"No existe la semilla {semilla}")
        faltan = [c for c in CLAVE_SEMILLA if c not in pq.ParquetFile(semilla).schema_arrow.names]
        if faltan:
            raise ValueError(f"La semilla {semilla} no tiene las columnas {faltan} de la clave {CLAVE_SEMILLA}")
    # Filas de cada fase la última vez que se leyó (para detectar una fase que vuelve vacía)
    filas_previas = _leer_json(analysis_file).get('filas_por_fase') or {}
    
    checkpoint = Checkpoint()
    if resume and checkpoint_file.exists():
        checkpoint = Checkpoint.load(checkpoint_file)
        stats.requests_made = checkpoint.requests_made
        logger.info("🔄 Resuming from checkpoint:")
        logger.info(f"   Completed fases: {checkpoint.completed_fases}")
        logger.info(f"   Records so far: {checkpoint.total_records_so_far}")
    
    fases_to_scrape = FASES_ALL if include_agregadas else FASES_NORMAL
    remaining_fases = [f for f in fases_to_scrape if f not in checkpoint.completed_fases]
    
    if not remaining_fases:
        logger.info("✅ All fases already completed!")
    else:
        logger.info("📊 Starting Contractació Pública scraper v4 (FULL JSON)...")
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
                inicio_huecos = len(stats.huecos)
                records = await scrape_with_segmentation(session, {'faseVigent': fase}, stats)
                huecos_fase = stats.huecos[inicio_huecos:]
                if not records:
                    # Una fase que vuelve vacía cuando la última descarga tenía filas puede ser
                    # un fallo del portal: en esta ejecución no se retira nada de su grupo
                    previas = (_filas_parquet(fichero_fase(output_file, fase))
                               or int(filas_previas.get(str(fase)) or 0))
                    if previas:
                        logger.warning(f"⚠️ Fase {fase} sin registros; la última descarga tenía {previas:,}: "
                                       f"no se retira nada de su grupo en esta ejecución")
                        huecos_fase = huecos_fase + [{'params': {'faseVigent': fase},
                                                      'motivo': f'vacía: la última descarga tenía {previas} filas'}]
                
                # Save FULL JSON
                save_incremental_full_json(records, output_file, fase)
                
                checkpoint.completed_fases.append(fase)
                checkpoint.huecos[str(fase)] = huecos_fase
                checkpoint.total_records_so_far += len(records)
                checkpoint.requests_made = stats.requests_made
                checkpoint.save(checkpoint_file)
                
                stats.segments_processed += 1
                logger.info(f"   ✅ Fase {fase}: {len(records)} rows, total so far: {checkpoint.total_records_so_far}")
                if huecos_fase:
                    logger.warning(f"   ⚠️ Fase {fase}: {len(huecos_fase)} segmentos sin leer enteros "
                                   f"(no se retira nada de ellos)")
                
            except Exception as e:
                logger.error(f"❌ Error processing fase {fase}: {e}")
                logger.info("   Progress saved. Resume with --resume flag.")
                raise
    
    # Merge, en el orden de FASES_ALL (el crudo no depende del orden en que se completaron)
    fases_leidas = ([f for f in FASES_ALL if f in checkpoint.completed_fases]
                    + [f for f in checkpoint.completed_fases if f not in FASES_ALL])
    logger.info("\n📦 Merging all incremental files...")
    df_raw = load_all_incremental(output_file, fases_leidas, columna_fase='_fase')
    stats.total_rows = len(df_raw)
    
    if len(df_raw) == 0:
        # Una descarga vacía es casi siempre un fallo: no se toca ninguna salida
        logger.warning("No records found! No se toca ninguna salida (no se marca nada como retirado)")
        return
    
    # Cada fase debe devolver solo publicaciones de su grupo (base del ámbito de acumular)
    esperado = df_raw['_fase'].map({f: grupo_fase(f) for f in fases_leidas}).to_numpy(dtype=object)
    fuera_de_grupo = esperado != grupo_publicacion(df_raw)
    particion_ok = not fuera_de_grupo.any()
    if not particion_ok:
        fases_mal = sorted({int(f) for f in df_raw.loc[fuera_de_grupo, '_fase']})
        logger.warning(f"⚠️ {int(fuera_de_grupo.sum()):,} filas de las fases {fases_mal} no son de su grupo "
                       f"(normales/agregadas): el ámbito trata los dos grupos como uno")
    conteo = df_raw['_fase'].value_counts()
    # Filas de cada fase la última vez que se leyó (las no leídas ahora, de la anterior)
    filas_por_fase = {**{str(k): int(v) for k, v in filas_previas.items()},
                      **{str(f): int(conteo.get(f, 0)) for f in fases_leidas}}
    filas_por_fase = dict(sorted(filas_por_fase.items(), key=lambda kv: int(kv[0])))
    df_raw = df_raw.drop(columns='_fase')
    
    logger.info(f"📊 Total columns in raw data: {len(df_raw.columns)}")
    
    # Save RAW (guardar_version: si cambia, la versión anterior pasa a _historico/)
    logger.info(f"💾 Saving RAW data ({len(df_raw)} rows, {len(df_raw.columns)} cols) to {raw_path}...")
    estado_raw = guardar_tabla(df_raw, raw_path, output_format)
    # Fecha de esta descarga: la de la versión del crudo (si no ha cambiado, la de antes)
    fecha = fecha_version(raw_path)
    logger.info(f"✅ Raw data saved! ({estado_raw}; versión del {fecha})")
    
    # Copies of the same record returned by several queries (one per current phase,
    # both sort orders...): only rows identical in ALL columns are removed
    logger.info("\n🧹 Removing identical copies...")
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
        logger.info("   Columns that differ within duplicates (top 10):")
        for col_info in analysis['differing_columns'][:10]:
            logger.info(f"      - {col_info['column']}: differs in {col_info['groups_with_differences']:,} groups ({col_info['pct_groups']:.1f}%)")
    
    if total_api is not None and len(df_clean) < total_api:
        logger.warning(f"⚠️ The API reports {total_api:,} records without phase filter; "
                       f"got {len(df_clean):,} (phases outside FASES_ALL or coverage gaps)")
    
    stats.total_records = len(df_clean)
    
    # Salida limpia: la descarga acumulada sobre la salida anterior (sesgo del superviviente)
    ambito = calcular_ambito(fases_leidas, checkpoint.huecos, particion_ok)
    for grupo, excluidos in ambito.items():
        if excluidos is None:
            logger.warning(f"⚠️ Publicaciones {grupo}: fuera del ámbito (fases sin leer o sin leer enteras): "
                           f"no se marca ninguna como retirada")
        elif excluidos:
            logger.warning(f"⚠️ Publicaciones {grupo}: {len(excluidos)} órganos con segmentos sin leer enteros "
                           f"quedan fuera del ámbito: {sorted(excluidos)[:10]}")
    anterior = leer_salida(clean_file, output_format)
    if anterior is not None and len(anterior) and '_en_ultima_descarga' not in anterior.columns:
        fecha_anterior = fecha_version(clean_file)
        logger.warning(f"⚠️ {clean_file} no tiene las columnas de control (salida del código anterior): se toma "
                       f"como la primera descarga ({fecha_anterior}), sin sus copias idénticas")
        anterior = acumular(None, quitar_copias_identicas(anterior), fecha_anterior)
    salida = acumular_salida(anterior, df_clean, fecha, ambito)
    if anterior is not None and len(anterior):
        antes = anterior['_en_ultima_descarga'].to_numpy(dtype=bool)
        despues = salida['_en_ultima_descarga'].to_numpy(dtype=bool)[:len(anterior)]
        logger.info(f"   Salida anterior: {len(anterior):,} filas; {len(salida) - len(anterior):,} nuevas; "
                    f"{int((antes & ~despues).sum()):,} dejan de estar en la descarga (retiradas o cambiadas, se "
                    f"conservan con _en_ultima_descarga=False); {int((~antes & despues).sum()):,} vuelven a estar; "
                    f"{int((~filas_en_ambito(anterior, ambito)).sum()):,} fuera del ámbito")
    
    for ruta in semillas:
        semilla, leidas, copias = leer_semilla(ruta)
        logger.info(f"🌱 Semilla {ruta}: {leidas:,} filas; se quitan {copias:,} copias idénticas "
                    f"→ {len(semilla):,} filas distintas")
        faltan = [c for c in CLAVE_SEMILLA if c not in salida.columns]
        if faltan:
            raise ValueError(f"La descarga no tiene las columnas {faltan} de la clave {CLAVE_SEMILLA}: "
                             f"no se puede sembrar")
        sembrada, informe = sembrar(salida, semilla, CLAVE_SEMILLA, en_ambito=filas_en_ambito(semilla, ambito))
        informe['ruta'] = str(ruta)
        imprimir_informe_semilla(informe)
        if informe['anadidas']:   # sin nada que añadir la salida no cambia (ni sus columnas)
            salida = sembrada
        del semilla, sembrada
    
    # Save clean (guardar_version: la versión anterior pasa a _historico/)
    logger.info(f"💾 Saving CLEAN data to {clean_file}...")
    estado_limpio = guardar_tabla(salida, clean_file, output_format)
    en_ultima = int(salida['_en_ultima_descarga'].sum())
    logger.info(f"   {estado_limpio}: {len(salida):,} filas, {en_ultima:,} en la última descarga")
    
    analysis['fecha_descarga'] = fecha
    analysis['filas_por_fase'] = filas_por_fase
    analysis['fases_incompletas'] = {
        str(f): checkpoint.huecos.get(str(f), [{'motivo': 'sin información de huecos'}])
        for f in fases_leidas if checkpoint.huecos.get(str(f)) != []}
    analysis['particion_por_grupo'] = bool(particion_ok)
    analysis['ambito'] = describir_ambito(ambito)
    analysis['salida'] = {'filas': int(len(salida)), 'en_ultima_descarga': en_ultima,
                          'no_en_ultima_descarga': int(len(salida) - en_ultima)}
    guardar_version(analysis_file, json.dumps(analysis, indent=2, ensure_ascii=False).encode('utf-8'))
    
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
    logger.info(f"\n{'='*60}")
    logger.info("✅ COMPLETED")
    logger.info(f"   Total rows scraped: {stats.total_rows:,}")
    logger.info(f"   Total columns: {len(df_raw.columns)}")
    logger.info(f"   Unique records: {stats.total_records:,}")
    logger.info(f"   Output rows (accumulated): {len(salida):,} ({en_ultima:,} in the last download)")
    logger.info(f"   Segments processed: {stats.segments_processed}")
    logger.info(f"   Segments requiring sub-segmentation: {stats.segments_over_10k}")
    logger.info(f"   API requests: {stats.requests_made:,}")
    logger.info(f"   Time: {elapsed/60:.1f} minutes")
    logger.info(f"   Raw output: {raw_path}")
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
    parser.add_argument('--semilla', type=Path, action='append', default=[],
                        help=f"parquet publicado (p.ej. contractacio_menors.parquet de v2026.02): añade, por "
                             f"{'+'.join(CLAVE_SEMILLA)}, las publicaciones del ámbito que ya no se sirven, con "
                             f"_origen='{ORIGEN_SEMILLA}' (repetible)")
    args = parser.parse_args()

    if args.incloure_placsp:
        INCLOURE_PLACSP = 'true'
    
    asyncio.run(main(args.output, args.format, include_agregadas=not args.no_agregadas, 
                     resume=args.resume, cleanup=args.cleanup, semillas=args.semilla))
