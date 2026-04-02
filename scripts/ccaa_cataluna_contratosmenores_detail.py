"""
Contractacio Publica de Catalunya - detalle de contratos menores

Pipeline incremental sobre el endpoint publico `detall-publicacio-expedient`.
Lee el parquet base de contratos menores, cachea el detalle en SQLite y genera
un parquet enriquecido sin rehacer siempre el historico.
"""

import argparse
import asyncio
import json
import logging
import random
import sqlite3
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import certifi
import pandas as pd

LOGGER_NAME = "ccaa_cataluna_contratosmenores_detail"
logger = logging.getLogger(LOGGER_NAME)

BASE_URL = "https://contractaciopublica.cat/portal-api"
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_PATH = REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors.parquet"
DEFAULT_CACHE_PATH = REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors_detail.sqlite3"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors_detallado.parquet"
DEFAULT_LOG_PATH = SCRIPT_DIR / "ccaa_cataluna_contratosmenores_detail.log"
DEFAULT_WORKERS = 16
DEFAULT_BATCH_SIZE = 200

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ca",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}


@dataclass
class DetailStats:
    total_candidates: int = 0
    pending_candidates: int = 0
    api_requests: int = 0
    done: int = 0
    not_found: int = 0
    failed: int = 0
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)


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
    return ssl.create_default_context(cafile=certifi.where())


def build_artifact_paths(
    input_path: str | Path,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    cache_path: str | Path = DEFAULT_CACHE_PATH,
) -> dict:
    input_file = Path(input_path).expanduser().resolve()
    output_file = Path(output_path).expanduser().resolve()
    cache_file = Path(cache_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file = output_file.with_stem(output_file.stem + "_detail_summary").with_suffix(".json")
    return {
        "input": input_file,
        "output": output_file,
        "cache": cache_file,
        "summary": summary_file,
    }


def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def normalize_publicacio_id(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text


def get_nested(data: dict, *keys, default=None):
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def extract_text(value):
    if value is None:
        return None
    if isinstance(value, dict):
        if "text" in value:
            return value.get("text")
        if "nom" in value:
            return value.get("nom")
        if "titol" in value:
            return value.get("titol")
        if "codi" in value:
            return value.get("codi")
        return None
    return value


def extract_code(value):
    if isinstance(value, dict):
        return value.get("codi") or value.get("id")
    return value


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def join_unique(values: list) -> Optional[str]:
    clean = []
    for value in values:
        text = extract_text(value)
        if text is None:
            continue
        text = str(text).strip()
        if text:
            clean.append(text)
    if not clean:
        return None
    return " | ".join(sorted(dict.fromkeys(clean)))


def count_documents(node) -> int:
    if node is None:
        return 0
    if isinstance(node, list):
        return sum(count_documents(item) for item in node)
    if isinstance(node, dict):
        if isinstance(node.get("docs"), list):
            return len(node["docs"])
        if "id" in node and "titol" in node:
            return 1
        return sum(count_documents(value) for value in node.values())
    return 0


def summarize_contractes_agregada(contracts: list) -> dict:
    award_without_vat = 0.0
    award_with_vat = 0.0
    estimated_total = 0.0
    offers_total = 0
    offers_pime_total = 0
    dates = []
    start_dates = []
    end_dates = []
    cpv_codes = []
    adjudicatarios = []
    expedientes = []

    for contract in contracts:
        award_without_vat += to_float(contract.get("importAdjudicacioSenseIva")) or 0.0
        award_with_vat += to_float(contract.get("importAdjudicacioAmbIva")) or 0.0
        estimated_total += to_float(contract.get("vec")) or 0.0
        offers_total += to_int(contract.get("numOfertesRebudes")) or 0
        offers_pime_total += to_int(contract.get("numOfertesPime")) or 0
        if contract.get("dataAdjudicacio"):
            dates.append(contract["dataAdjudicacio"])
        if contract.get("dataIniciExecucio"):
            start_dates.append(contract["dataIniciExecucio"])
        if contract.get("dataFiExecucio"):
            end_dates.append(contract["dataFiExecucio"])
        cpv_codes.append(get_nested(contract, "codiCpv", "codi"))
        adjudicatarios.append(contract.get("nomAdjudicatari"))
        expedientes.append(contract.get("expedient"))

    return {
        "detail_contractes_agregada_count": len(contracts),
        "detail_contractes_agregada_importe_total_sin_iva": award_without_vat or None,
        "detail_contractes_agregada_importe_total_con_iva": award_with_vat or None,
        "detail_contractes_agregada_valor_estimado_total": estimated_total or None,
        "detail_contractes_agregada_ofertas_total": offers_total or None,
        "detail_contractes_agregada_ofertas_pime_total": offers_pime_total or None,
        "detail_contractes_agregada_fecha_adjudicacion_min": min(dates) if dates else None,
        "detail_contractes_agregada_fecha_adjudicacion_max": max(dates) if dates else None,
        "detail_contractes_agregada_fecha_inicio_min": min(start_dates) if start_dates else None,
        "detail_contractes_agregada_fecha_fin_max": max(end_dates) if end_dates else None,
        "detail_contractes_agregada_cpv_codes": join_unique(cpv_codes),
        "detail_contractes_agregada_adjudicatarios": join_unique(adjudicatarios),
        "detail_contractes_agregada_expedientes": join_unique(expedientes),
    }


def parse_modern_detail(payload: dict, root: dict) -> dict:
    publicacio = root["publicacio"]
    basics = publicacio.get("dadesBasiquesPublicacio", {})
    expedition = publicacio.get("dadesExpedient", {})
    publication = publicacio.get("dadesPublicacio", {})
    lots = publicacio.get("dadesPublicacioLot") or []
    aggregated_contracts = publication.get("contractesAgregada") or []

    detail = {
        "detail_source_kind": "aggregated" if aggregated_contracts else "modern",
        "detail_codi_expediente": basics.get("codiExpedient"),
        "detail_titulo": get_nested(basics, "denominacio", "text") or payload.get("titol"),
        "detail_descripcion": get_nested(basics, "descripcio", "text"),
        "detail_fase_publicacion": extract_text(basics.get("fasePublicacio")),
        "detail_tipo_publicacion_expediente": extract_text(basics.get("tipusPublicacioExpedient")),
        "detail_tipo_tramitacion": extract_text(basics.get("tipusTramitacio")),
        "detail_tipo_contrato": extract_text(basics.get("tipusContracte")),
        "detail_procedimiento_adjudicacion": extract_text(basics.get("procedimentAdjudicacio")),
        "detail_tipo_oferta_electronica": extract_text(basics.get("tipusOfertaElectronica")),
        "detail_normativa_aplicable": extract_text(basics.get("normativaAplicable")),
        "detail_unidad_contratacion": extract_text(basics.get("unitatContractacio")),
        "detail_publicacion_agregada": basics.get("publicacioAgregada"),
        "detail_ley_asociada": expedition.get("lleiAssociada"),
        "detail_es_publicacion_antigua": expedition.get("esPublicacioAntiga"),
        "detail_no_admite_preguntas": expedition.get("noAdmetrePreguntes"),
        "detail_presupuesto_licitacion": to_float(publication.get("pressupostLicitacio")),
        "detail_lot_count": len(lots) if lots else None,
    }

    if aggregated_contracts:
        detail.update(summarize_contractes_agregada(aggregated_contracts))
        detail["detail_document_count"] = None
        detail["detail_lot_ofertas_total"] = None
        detail["detail_lot_empresas"] = None
    else:
        empresas = []
        total_offers = 0
        document_count = 0
        for lot in lots:
            total_offers += to_int(lot.get("totalOfertesRebudes")) or 0
            document_count += count_documents(lot)
            for empresa in lot.get("identitatEmpresa") or []:
                empresas.append(empresa.get("empresa"))
        detail["detail_lot_ofertas_total"] = total_offers or None
        detail["detail_lot_empresas"] = join_unique(empresas)
        detail["detail_document_count"] = document_count or None

    return detail


def parse_legacy_detail(payload: dict, root: dict) -> dict:
    basics = root.get("dadesBasiques", {})
    contract = root.get("dadesContracte", {})
    duration = contract.get("durada") or {}
    documents = root.get("documentacio") or {}
    lots = root.get("lots") or []

    return {
        "detail_source_kind": "legacy",
        "detail_codi_expediente": basics.get("codiExpedient"),
        "detail_titulo": basics.get("denominacio") or payload.get("titol"),
        "detail_descripcion": contract.get("descripcioPrestacio"),
        "detail_fase_publicacion": basics.get("nomFaseCpp"),
        "detail_tipo_publicacion_expediente": basics.get("tipusPublicacio"),
        "detail_tipo_tramitacion": basics.get("tipusTramitacio"),
        "detail_tipo_contrato": basics.get("tipusContracte"),
        "detail_subtipo_contrato": basics.get("subtipusContracte"),
        "detail_procedimiento_adjudicacion": basics.get("procedimentAdjudicacio"),
        "detail_unidad_contratacion": basics.get("nomUnitat"),
        "detail_organ_name": basics.get("nomOrgan"),
        "detail_presupuesto_licitacion_sin_iva": to_float(contract.get("pressupostLicitacioSenseIva")),
        "detail_presupuesto_licitacion_con_iva": to_float(contract.get("pressupostLicitacioAmbIva")),
        "detail_valor_estimado_contrato": to_float(contract.get("valorEstimatContracte")),
        "detail_iva": to_float(contract.get("iva")),
        "detail_ambito_geografico": contract.get("ambitGeografic"),
        "detail_fecha_limite_ofertas": contract.get("terminiPresentacioOfertes"),
        "detail_duracion_inicio": duration.get("iniciTermini"),
        "detail_duracion_fin": duration.get("fiTermini"),
        "detail_duracion_observaciones": duration.get("observacions"),
        "detail_lot_count": len(lots) if lots else None,
        "detail_document_count": count_documents(documents) or None,
    }


def parse_detail_payload(payload: dict) -> dict:
    """Normalize one PSCP detail payload into a flat `detail_*` record.

    The endpoint currently serves three shapes: legacy top-level payloads,
    modern payloads under `dades.publicacio`, and aggregated publications with
    `contractesAgregada`.
    """
    root = payload.get("dades", {})
    organ = root.get("organ") or {}
    detail = {
        "detail_publicacio_id": normalize_publicacio_id(payload.get("publicacioId")),
        "detail_expedient_id": payload.get("expedientId"),
        "detail_fase": payload.get("fase"),
        "detail_titulo_portal": payload.get("titol"),
        "detail_publicacion_antigua": payload.get("publicacioAntiga"),
        "detail_fecha_publicacion_real": root.get("dataPublicacioReal") or root.get("dataPublicacio"),
        "detail_fecha_publicacion_planificada": root.get("dataPublicacioPlanificada"),
        "detail_organ_name": organ.get("nom"),
        "detail_organ_nif": organ.get("nif"),
        "detail_organ_web": organ.get("web"),
        "detail_organ_email": organ.get("email"),
        "detail_organ_nuts": organ.get("nuts"),
        "detail_organ_contactos": len(organ.get("personesContacte") or []),
        "detail_source_url": f"{BASE_URL}/detall-publicacio-expedient/{payload.get('publicacioId')}",
    }

    if "publicacio" in root:
        detail.update(parse_modern_detail(payload, root))
    else:
        detail.update(parse_legacy_detail(payload, root))
    return detail


def ensure_cache_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detalles (
            record_key TEXT PRIMARY KEY,
            publicacio_id TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            source_kind TEXT,
            detail_json TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_detalles_status ON detalles(status)")
    conn.commit()


def load_base_candidates(input_path: Path, limit: Optional[int] = None) -> tuple[pd.DataFrame, list[str]]:
    df = pd.read_parquet(input_path)
    if limit is not None:
        df = df.head(limit).copy()
    if "id" not in df.columns:
        raise ValueError(f"El parquet base no contiene la columna 'id': {input_path}")
    df["_detail_record_key"] = df["id"].map(normalize_publicacio_id)
    df = df[df["_detail_record_key"].notna()].copy()
    candidate_ids = list(dict.fromkeys(df["_detail_record_key"].tolist()))
    return df, candidate_ids


def select_pending_candidates(
    conn: sqlite3.Connection,
    candidate_ids: list[str],
    retry_not_found: bool = False,
    retry_failed: bool = False,
) -> tuple[list[str], int]:
    if not candidate_ids:
        return [], 0

    cached_status = {}
    chunk_size = 500
    for start in range(0, len(candidate_ids), chunk_size):
        chunk = candidate_ids[start : start + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT record_key, status FROM detalles WHERE record_key IN ({placeholders})",
            chunk,
        ).fetchall()
        cached_status.update({row[0]: row[1] for row in rows})

    pending = []
    cache_hits = 0
    for record_key in candidate_ids:
        status = cached_status.get(record_key)
        if status is None:
            pending.append(record_key)
            continue
        if status == "done":
            cache_hits += 1
            continue
        if status == "not_found" and not retry_not_found:
            cache_hits += 1
            continue
        if status == "failed" and not retry_failed:
            cache_hits += 1
            continue
        pending.append(record_key)
    return pending, cache_hits


async def fetch_detail_json(
    session: aiohttp.ClientSession,
    publicacio_id: str,
    stats: DetailStats,
) -> tuple[str, Optional[dict], Optional[str], str]:
    retryable_status_codes = {429, 500, 502, 503, 504}
    url = f"{BASE_URL}/detall-publicacio-expedient/{publicacio_id}"

    for attempt in range(1, 6):
        try:
            async with session.get(url, headers=HEADERS, timeout=30) as resp:
                stats.api_requests += 1
                if resp.status == 200:
                    return "done", await resp.json(), None, "done"
                if resp.status == 404:
                    return "not_found", None, "404", "not_found"
                if resp.status in retryable_status_codes:
                    wait_time = min(10 * (2 ** (attempt - 1)), 60) * random.uniform(0.85, 1.15)
                    logger.warning(
                        "HTTP %s para %s (intento %s/5), espero %.1fs",
                        resp.status,
                        publicacio_id,
                        attempt,
                        wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                text = await resp.text()
                return "failed", None, f"HTTP {resp.status}: {text[:180]}", "failed"
        except asyncio.TimeoutError:
            wait_time = min(5 * attempt, 30) * random.uniform(0.85, 1.15)
            logger.warning("Timeout en %s (intento %s/5), espero %.1fs", publicacio_id, attempt, wait_time)
            await asyncio.sleep(wait_time)
        except aiohttp.ClientError as exc:
            wait_time = min(5 * attempt, 30) * random.uniform(0.85, 1.15)
            logger.warning(
                "Error de red en %s (intento %s/5): %s, espero %.1fs",
                publicacio_id,
                attempt,
                exc,
                wait_time,
            )
            await asyncio.sleep(wait_time)
        except Exception as exc:
            return "failed", None, str(exc), "failed"

    return "failed", None, "Max retries exceeded", "failed"


async def fetch_detail_record(
    session: aiohttp.ClientSession,
    publicacio_id: str,
    stats: DetailStats,
    semaphore: asyncio.Semaphore,
) -> dict:
    async with semaphore:
        status, payload, last_error, final_status = await fetch_detail_json(session, publicacio_id, stats)
        parsed = parse_detail_payload(payload) if payload else None
        return {
            "record_key": publicacio_id,
            "publicacio_id": publicacio_id,
            "status": final_status,
            "attempts": 1,
            "last_error": last_error,
            "source_kind": parsed.get("detail_source_kind") if parsed else None,
            "detail_json": json.dumps(parsed, ensure_ascii=False) if parsed else None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }


def upsert_detail_rows(conn: sqlite3.Connection, rows: list[dict], stats: DetailStats):
    for row in rows:
        conn.execute(
            """
            INSERT INTO detalles (
                record_key, publicacio_id, status, attempts, last_error,
                source_kind, detail_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_key) DO UPDATE SET
                publicacio_id=excluded.publicacio_id,
                status=excluded.status,
                attempts=detalles.attempts + excluded.attempts,
                last_error=excluded.last_error,
                source_kind=excluded.source_kind,
                detail_json=excluded.detail_json,
                updated_at=excluded.updated_at
            """,
            (
                row["record_key"],
                row["publicacio_id"],
                row["status"],
                row["attempts"],
                row["last_error"],
                row["source_kind"],
                row["detail_json"],
                row["updated_at"],
            ),
        )
        if row["status"] == "done":
            stats.done += 1
        elif row["status"] == "not_found":
            stats.not_found += 1
        else:
            stats.failed += 1
    conn.commit()


def load_detail_frame(cache_path: Path) -> pd.DataFrame:
    conn = sqlite3.connect(cache_path)
    rows = conn.execute(
        """
        SELECT record_key, publicacio_id, status, attempts, last_error,
               source_kind, detail_json, updated_at
        FROM detalles
        """
    ).fetchall()
    conn.close()

    records = []
    for record_key, publicacio_id, status, attempts, last_error, source_kind, detail_json, updated_at in rows:
        record = {
            "_detail_record_key": record_key,
            "detail_status": status,
            "detail_attempts": attempts,
            "detail_last_error": last_error,
            "detail_source_kind": source_kind,
            "detail_fetched_at": updated_at,
        }
        if detail_json:
            record.update(json.loads(detail_json))
        records.append(record)

    if not records:
        return pd.DataFrame(columns=["_detail_record_key"])
    return pd.DataFrame(records)


def coerce_object_columns_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in df.columns:
        if df[column].dtype != "object":
            continue
        non_null = df[column].dropna()
        if non_null.empty:
            continue
        value_types = {type(value) for value in non_null}
        if value_types <= {str}:
            continue
        if any(isinstance(value, (dict, list, tuple, set)) for value in non_null):
            df[column] = df[column].map(
                lambda value: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (dict, list, tuple, set))
                else value
            )
            non_null = df[column].dropna()
            value_types = {type(value) for value in non_null}
        if len(value_types) > 1 or value_types != {str}:
            df[column] = df[column].map(lambda value: None if pd.isna(value) else str(value))
            df[column] = df[column].astype("string")
    return df


async def run_detail_fetch(
    input_path: Path,
    cache_path: Path,
    workers: int,
    batch_size: int,
    retry_not_found: bool,
    retry_failed: bool,
    limit: Optional[int],
    verify_ssl: bool,
) -> DetailStats:
    """Fetch detail JSON incrementally and persist normalized rows in SQLite."""
    stats = DetailStats()
    base_df, candidate_ids = load_base_candidates(input_path, limit=limit)
    stats.total_candidates = len(candidate_ids)

    conn = sqlite3.connect(cache_path)
    ensure_cache_schema(conn)
    pending_ids, cache_hits = select_pending_candidates(
        conn,
        candidate_ids,
        retry_not_found=retry_not_found,
        retry_failed=retry_failed,
    )
    stats.pending_candidates = len(pending_ids)
    stats.cache_hits = cache_hits

    logger.info("📦 Base rows con id: %s", len(base_df))
    logger.info("🧾 Publicaciones unicas candidatas: %s", len(candidate_ids))
    logger.info("♻️ Cache hits: %s", cache_hits)
    logger.info("🚀 Pendientes de detalle: %s", len(pending_ids))

    if not pending_ids:
        conn.close()
        return stats

    connector = aiohttp.TCPConnector(ssl=build_ssl_context(verify_ssl=verify_ssl))
    semaphore = asyncio.Semaphore(workers)
    async with aiohttp.ClientSession(connector=connector) as session:
        for start in range(0, len(pending_ids), batch_size):
            chunk = pending_ids[start : start + batch_size]
            results = await asyncio.gather(
                *(fetch_detail_record(session, publicacio_id, stats, semaphore) for publicacio_id in chunk)
            )
            upsert_detail_rows(conn, results, stats)
            if (start // batch_size) % 10 == 0:
                logger.info(
                    "   Progreso detail: %s/%s pendientes, done=%s, not_found=%s, failed=%s",
                    min(start + len(chunk), len(pending_ids)),
                    len(pending_ids),
                    stats.done,
                    stats.not_found,
                    stats.failed,
                )

    conn.close()
    return stats


def run_merge(input_path: Path, cache_path: Path, output_path: Path) -> dict:
    """Merge the base parquet with cached `detail_*` columns into one output."""
    base_df, _candidate_ids = load_base_candidates(input_path)
    detail_df = load_detail_frame(cache_path)
    if detail_df.empty:
        raise ValueError(f"No hay detalles cacheados en {cache_path}")

    merged = base_df.merge(detail_df, on="_detail_record_key", how="left")
    merged = merged.drop(columns=["_detail_record_key"])
    merged = coerce_object_columns_for_parquet(merged)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, index=False, compression="snappy")

    return {
        "base_rows": len(base_df),
        "merged_rows": len(merged),
        "columns": len(merged.columns),
        "done_rows": int((merged["detail_status"] == "done").sum()) if "detail_status" in merged.columns else 0,
        "not_found_rows": int((merged["detail_status"] == "not_found").sum()) if "detail_status" in merged.columns else 0,
        "failed_rows": int((merged["detail_status"] == "failed").sum()) if "detail_status" in merged.columns else 0,
    }


async def main():
    parser = argparse.ArgumentParser(description="Detalle incremental para contratos menores de Catalunya")
    parser.add_argument("--input", default=str(DEFAULT_INPUT_PATH), help="Parquet base de contratos menores")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Parquet final enriquecido")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE_PATH), help="SQLite incremental del detalle")
    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH), help="Ruta del log")
    parser.add_argument("--mode", choices=["detail", "merge", "full"], default="full", help="Fase a ejecutar")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrencia del detalle")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Tamano del lote async")
    parser.add_argument("--limit", type=int, help="Limitar publicaciones a procesar")
    parser.add_argument("--retry-not-found", action="store_true", help="Reintentar registros not_found")
    parser.add_argument("--retry-failed", action="store_true", help="Reintentar registros failed")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Desactivar verificacion SSL")
    args = parser.parse_args()

    build_logger(Path(args.log_path))
    paths = build_artifact_paths(args.input, args.output, args.cache)

    detail_stats = None
    if args.mode in {"detail", "full"}:
        detail_stats = await run_detail_fetch(
            input_path=paths["input"],
            cache_path=paths["cache"],
            workers=args.workers,
            batch_size=args.batch_size,
            retry_not_found=args.retry_not_found,
            retry_failed=args.retry_failed,
            limit=args.limit,
            verify_ssl=not args.no_verify_ssl,
        )
        logger.info(
            "✅ Detail terminado: done=%s, not_found=%s, failed=%s, requests=%s",
            detail_stats.done,
            detail_stats.not_found,
            detail_stats.failed,
            detail_stats.api_requests,
        )

    merge_stats = None
    if args.mode in {"merge", "full"}:
        merge_stats = run_merge(paths["input"], paths["cache"], paths["output"])
        logger.info(
            "✅ Merge terminado: rows=%s, cols=%s, done=%s, not_found=%s, failed=%s",
            merge_stats["merged_rows"],
            merge_stats["columns"],
            merge_stats["done_rows"],
            merge_stats["not_found_rows"],
            merge_stats["failed_rows"],
        )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_file": str(paths["input"]),
        "output_file": str(paths["output"]),
        "cache_file": str(paths["cache"]),
        "mode": args.mode,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "limit": args.limit,
        "retry_not_found": args.retry_not_found,
        "retry_failed": args.retry_failed,
    }
    if detail_stats:
        summary["detail"] = {
            "total_candidates": detail_stats.total_candidates,
            "pending_candidates": detail_stats.pending_candidates,
            "cache_hits": detail_stats.cache_hits,
            "done": detail_stats.done,
            "not_found": detail_stats.not_found,
            "failed": detail_stats.failed,
            "api_requests": detail_stats.api_requests,
            "elapsed_seconds": time.time() - detail_stats.start_time,
        }
    if merge_stats:
        summary["merge"] = merge_stats
    write_json(paths["summary"], summary)


if __name__ == "__main__":
    asyncio.run(main())
