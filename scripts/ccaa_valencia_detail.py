#!/usr/bin/env python3
"""
Detalle incremental para la contratacion de Valencia.

Enriquecimiento sobre los parquets base de `scripts/ccaa_valencia_parquet.py`.
Usa la API publica de CONREG como fuente principal de detalle a partir de la
clave `EJERCICIO + NUMERO_DE_REGISTRO`, cachea respuestas en SQLite y genera
un parquet enriquecido sin rehacer siempre todo el historico.
"""

import argparse
import base64
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_PATH = REPO_ROOT / "valencia" / "contratacion"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "valencia" / "contratacion" / "contratacion_detallada.parquet"
DEFAULT_CACHE_PATH = REPO_ROOT / "valencia" / "contratacion" / "contratacion_detail.sqlite3"
DEFAULT_LOG_PATH = SCRIPT_DIR / "ccaa_valencia_detail.log"
DEFAULT_WORKERS = 16
DEFAULT_BATCH_SIZE = 500
REQUEST_TIMEOUT = (20, 60)
LOGGER_NAME = "ccaa_valencia_detail"
DETAIL_API_BASE = "https://conreg.gva.es/conreg/api/consulta-contratos-inscritos"
DETAIL_FRONTEND_BASE = "https://conreg.gva.es/regcon-conreg-frontend/contratos-menor-nomenor"
DETAIL_SEARCH_URL = f"{DETAIL_API_BASE}/filtrar?page=0&size=20"
CONREG_AUTH_USER = os.getenv("CONREG_AUTH_USER", "admin")
CONREG_AUTH_PASSWORD = os.getenv("CONREG_AUTH_PASSWORD", "1234")
_THREAD_LOCAL = threading.local()

logger = logging.getLogger(LOGGER_NAME)


@dataclass(frozen=True)
class DetailCandidate:
    detail_key: str
    ejercicio: str
    numero_registro: str
    expediente: Optional[str]


def build_logger(log_path: Path) -> logging.Logger:
    log_path = log_path.expanduser().resolve()
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


def build_artifact_paths(
    input_path: str | Path,
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    cache_path: str | Path = DEFAULT_CACHE_PATH,
) -> dict[str, Path]:
    source = Path(input_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    cache = Path(cache_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    cache.parent.mkdir(parents=True, exist_ok=True)
    summary = output.with_stem(output.stem + "_detail_summary").with_suffix(".json")
    return {"input": source, "output": output, "cache": cache, "summary": summary}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def normalize_column_name(name: str) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    return text.strip("_")


def strip_html_text(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    text = str(value)
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"^[:\-–]+\s*", "", cleaned)
    return cleaned or value


def normalize_year(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        text = text[:-2]
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return None


def normalize_registro(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0"):
        text = text[:-2]
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def parse_euro_amount(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace("€", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and not pd.isna(value):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_bool_text(value) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text or text == "nan":
        return None
    if text in {"si", "sí", "s", "y", "yes", "true"}:
        return True
    if text in {"no", "n", "false"}:
        return False
    return None


def parse_date_es(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_date_flexible(value) -> Optional[str]:
    parsed = parse_date_es(value)
    if parsed:
        return parsed
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    return timestamp.date().isoformat()


def build_detail_key(ejercicio, numero_registro) -> Optional[str]:
    year = normalize_year(ejercicio)
    registro = normalize_registro(numero_registro)
    if not year or not registro:
        return None
    return f"{year}:{registro}"


def build_source_url(ejercicio: str, numero_registro: str) -> str:
    return f"{DETAIL_FRONTEND_BASE}/{ejercicio}/{numero_registro}/detail"


def build_conreg_headers() -> dict[str, str]:
    basic = base64.b64encode(f"{CONREG_AUTH_USER}:{CONREG_AUTH_PASSWORD}".encode("utf-8")).decode("ascii")
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Basic {basic}",
        "User-Agent": "licitaciones-espana/valencia-detail",
        "aplicacion": "",
        "x-api-key": "",
    }


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(build_conreg_headers())
    return session


def get_thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        _THREAD_LOCAL.session = session
    return session


def ensure_cache_schema(cache_path: Path) -> None:
    with sqlite3.connect(cache_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS valencia_detail (
                detail_key TEXT PRIMARY KEY,
                ejercicio TEXT,
                numero_registro TEXT,
                expediente TEXT,
                source_url TEXT,
                source_kind TEXT,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                fetched_at TEXT,
                payload_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_valencia_detail_status
            ON valencia_detail(status)
            """
        )
        conn.commit()


def load_cached_rows(cache_path: Path) -> dict[str, dict]:
    with sqlite3.connect(cache_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT detail_key, status, attempts, last_error, source_url, source_kind,
                   fetched_at, payload_json
            FROM valencia_detail
            """
        ).fetchall()
    return {row["detail_key"]: dict(row) for row in rows}


def upsert_cache_rows(cache_path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with sqlite3.connect(cache_path) as conn:
        conn.executemany(
            """
            INSERT INTO valencia_detail (
                detail_key, ejercicio, numero_registro, expediente, source_url,
                source_kind, status, attempts, last_error, fetched_at, payload_json
            ) VALUES (
                :detail_key, :ejercicio, :numero_registro, :expediente, :source_url,
                :source_kind, :status, :attempts, :last_error, :fetched_at, :payload_json
            )
            ON CONFLICT(detail_key) DO UPDATE SET
                ejercicio=excluded.ejercicio,
                numero_registro=excluded.numero_registro,
                expediente=excluded.expediente,
                source_url=excluded.source_url,
                source_kind=excluded.source_kind,
                status=excluded.status,
                attempts=excluded.attempts,
                last_error=excluded.last_error,
                fetched_at=excluded.fetched_at,
                payload_json=excluded.payload_json
            """,
            rows,
        )
        conn.commit()


def load_base_dataframe(input_path: Path) -> pd.DataFrame:
    if input_path.is_file():
        files = [input_path]
    elif input_path.is_dir():
        files = [
            parquet_path
            for parquet_path in sorted(input_path.glob("*.parquet"))
            if "detallad" not in parquet_path.stem.lower() and "detail" not in parquet_path.stem.lower()
        ]
    else:
        raise FileNotFoundError(f"No existe la entrada: {input_path}")

    if not files:
        raise FileNotFoundError(f"No hay parquets de contratacion en {input_path}")

    frames = []
    for parquet_path in files:
        dataframe = pd.read_parquet(parquet_path)
        dataframe.columns = [normalize_column_name(column) for column in dataframe.columns]
        for text_column in ("objeto", "concepto"):
            if text_column in dataframe.columns:
                dataframe[text_column] = dataframe[text_column].map(strip_html_text)
        dataframe["source_file"] = parquet_path.name
        frames.append(dataframe)
    merged = pd.concat(frames, ignore_index=True)

    for required in ("ejercicio", "numero_de_registro", "expediente"):
        if required not in merged.columns:
            raise ValueError(f"Falta columna base requerida: {required}")

    merged["detail_key"] = merged.apply(
        lambda row: build_detail_key(row.get("ejercicio"), row.get("numero_de_registro")),
        axis=1,
    )
    return merged


def coerce_object_columns_for_parquet(dataframe: pd.DataFrame) -> pd.DataFrame:
    converted = dataframe.copy()
    for column in converted.columns:
        if converted[column].dtype == "object":
            converted[column] = converted[column].astype("string")
    return converted


def build_candidates(dataframe: pd.DataFrame) -> tuple[list[DetailCandidate], int]:
    rows = dataframe[["detail_key", "ejercicio", "numero_de_registro", "expediente"]].drop_duplicates()
    candidates = []
    missing_key_rows = 0
    for row in rows.itertuples(index=False):
        detail_key = row.detail_key if row.detail_key is not None and not pd.isna(row.detail_key) else None
        if not detail_key:
            detail_key = build_detail_key(row.ejercicio, row.numero_de_registro)
        if not detail_key:
            missing_key_rows += 1
            continue
        candidates.append(
            DetailCandidate(
                detail_key=detail_key,
                ejercicio=normalize_year(row.ejercicio),
                numero_registro=normalize_registro(row.numero_de_registro),
                expediente=str(row.expediente).strip() if row.expediente is not None else None,
            )
        )
    return candidates, missing_key_rows


def first_non_empty(series: pd.Series):
    for value in series:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        if pd.isna(value):
            continue
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "sin determinar"}:
            continue
        return value
    return None


def build_cpv_principal(code, description) -> Optional[str]:
    code_text = first_non_empty(pd.Series([code]))
    desc_text = first_non_empty(pd.Series([description]))
    if code_text and desc_text:
        return f"{code_text} - {desc_text}"
    if code_text:
        return str(code_text)
    if desc_text:
        return str(desc_text)
    return None


def build_base_fallback_dataframe(base_df: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame:
    """
    Rescata detail parcial desde la base abierta cuando CONREG no resuelve la clave.
    """
    if detail_df.empty or "detail_status" not in detail_df.columns:
        return pd.DataFrame()

    not_found_keys = detail_df.loc[detail_df["detail_status"] == "not_found", "detail_key"].dropna().unique().tolist()
    if not not_found_keys:
        return pd.DataFrame()

    fallback_source = base_df[base_df["detail_key"].isin(not_found_keys)].copy()
    if fallback_source.empty:
        return pd.DataFrame()

    rows = []
    for detail_key, group in fallback_source.groupby("detail_key", dropna=True):
        class_text = first_non_empty(group.get("clase_de_contrato", pd.Series(dtype=object)))
        url_placsp = first_non_empty(group.get("url_licitacion", pd.Series(dtype=object)))
        if url_placsp and str(url_placsp).strip().lower() == "no encontrada":
            url_placsp = None

        payload = {
            "detail_key": detail_key,
            "detail_anyo": parse_int(first_non_empty(group.get("ejercicio", pd.Series(dtype=object)))),
            "detail_numreg": parse_int(first_non_empty(group.get("numero_de_registro", pd.Series(dtype=object)))),
            "detail_numversion": None,
            "detail_expediente": first_non_empty(group.get("expediente", pd.Series(dtype=object))),
            "detail_entidad_adjudicadora": first_non_empty(group.get("conselleria_ent_adj", pd.Series(dtype=object))),
            "detail_unidad": first_non_empty(group.get("unidad", pd.Series(dtype=object))),
            "detail_objeto": first_non_empty(group.get("objeto", pd.Series(dtype=object)))
            or first_non_empty(group.get("concepto", pd.Series(dtype=object))),
            "detail_tipo_contrato": first_non_empty(group.get("tipo", pd.Series(dtype=object))),
            "detail_procedimiento": first_non_empty(group.get("procedimiento", pd.Series(dtype=object))),
            "detail_tramitacion": first_non_empty(group.get("tipo_tramitacion", pd.Series(dtype=object))),
            "detail_tipo_menor": class_text,
            "detail_es_menor": "menor" in str(class_text).lower() if class_text else None,
            "detail_lugar_ejecucion": first_non_empty(group.get("lugar_ejecucion", pd.Series(dtype=object))),
            "detail_adjudicatario": first_non_empty(group.get("nombre_o_razon_social", pd.Series(dtype=object))),
            "detail_nif_adjudicatario": first_non_empty(group.get("cif_nif_enmascarado", pd.Series(dtype=object))),
            "detail_pais_adjudicatario": first_non_empty(group.get("nacionalidad_adjudicatario", pd.Series(dtype=object))),
            "detail_importe_adjudicacion_total": parse_euro_amount(first_non_empty(group.get("imp_total_adjud", pd.Series(dtype=object)))),
            "detail_importe_adjudicacion_iva": parse_euro_amount(first_non_empty(group.get("imp_adjud_iva", pd.Series(dtype=object)))),
            "detail_importe_adjudicacion_sin_iva": parse_euro_amount(first_non_empty(group.get("imp_adjud_sin_iva", pd.Series(dtype=object)))),
            "detail_importe_licitacion_total": parse_euro_amount(first_non_empty(group.get("importe_total_licit", pd.Series(dtype=object)))),
            "detail_importe_licitacion_iva": parse_euro_amount(first_non_empty(group.get("importe_licit_iva", pd.Series(dtype=object)))),
            "detail_importe_licitacion_sin_iva": parse_euro_amount(first_non_empty(group.get("importe_licit_sin_iva", pd.Series(dtype=object)))),
            "detail_importe_definitivo": parse_euro_amount(first_non_empty(group.get("imp_definitivo_contrato", pd.Series(dtype=object)))),
            "detail_valor_estimado": parse_euro_amount(first_non_empty(group.get("valor_estimado_sin_iva", pd.Series(dtype=object)))),
            "detail_fecha_adjudicacion": parse_date_flexible(first_non_empty(group.get("fecha_adjudicacion", pd.Series(dtype=object)))),
            "detail_fecha_formalizacion": parse_date_flexible(first_non_empty(group.get("fecha_formalizacion", pd.Series(dtype=object)))),
            "detail_fecha_prev_fin": parse_date_flexible(first_non_empty(group.get("fecha_prev_finalizacion", pd.Series(dtype=object)))),
            "detail_fecha_prevista_sin_garantia": parse_date_flexible(first_non_empty(group.get("fecha_prev_sin_garantia", pd.Series(dtype=object)))),
            "detail_plazo_ejecucion": parse_int(first_non_empty(group.get("plazo_de_ejecucion", pd.Series(dtype=object)))),
            "detail_unidad_plazo_ejecucion": first_non_empty(group.get("unidades_plazo_ejecucion", pd.Series(dtype=object))),
            "detail_plazo_max_contrato": parse_int(first_non_empty(group.get("plazo_max_contrato", pd.Series(dtype=object)))),
            "detail_unidad_plazo_max": first_non_empty(group.get("unidades_plazo_max", pd.Series(dtype=object))),
            "detail_num_anualidades": parse_int(first_non_empty(group.get("num_anualidades_prev", pd.Series(dtype=object)))),
            "detail_num_lotes": parse_int(first_non_empty(group.get("numero_de_lotes", pd.Series(dtype=object)))),
            "detail_num_invitadas": parse_int(first_non_empty(group.get("num_empresas_invitadas", pd.Series(dtype=object)))),
            "detail_num_licitaciones": parse_int(first_non_empty(group.get("num_licitadores", pd.Series(dtype=object)))),
            "detail_acuerdo_marco": parse_bool_text(first_non_empty(group.get("acuerdo_contrato_marco", pd.Series(dtype=object)))),
            "detail_exp_sistema_marco": first_non_empty(group.get("exp_acuerdo_marco_o_sda", pd.Series(dtype=object))),
            "detail_sistema_precios": first_non_empty(group.get("sistema_de_precios", pd.Series(dtype=object))),
            "detail_forma_adjudicacion": first_non_empty(group.get("forma_de_adjudicacion", pd.Series(dtype=object))),
            "detail_financiado_eu": parse_bool_text(first_non_empty(group.get("finan_con_fondos_europeos", pd.Series(dtype=object)))),
            "detail_fondo_europeo": first_non_empty(group.get("fondo_europeo", pd.Series(dtype=object))),
            "detail_pyme": parse_bool_text(first_non_empty(group.get("pyme", pd.Series(dtype=object)))),
            "detail_clausulas_sociales": parse_bool_text(first_non_empty(group.get("inc_clau_respons", pd.Series(dtype=object)))),
            "detail_reservado": parse_bool_text(first_non_empty(group.get("contrato_reservado", pd.Series(dtype=object)))),
            "detail_cpv_principal": build_cpv_principal(
                first_non_empty(group.get("codigo_cpv", pd.Series(dtype=object))),
                first_non_empty(group.get("cpv", pd.Series(dtype=object))),
            ),
            "detail_url_placsp": url_placsp,
            "detail_motivo_necesidad": None,
            "detail_clasificaciones_count": 0,
            "detail_status": "done_base_fallback",
            "detail_attempts": 0,
            "detail_last_error": None,
            "detail_source_url": url_placsp,
            "detail_source_kind": "base_row_fallback",
            "detail_fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        rows.append(payload)
    return pd.DataFrame(rows)


def select_pending_candidates(
    candidates: list[DetailCandidate],
    cache_rows: dict[str, dict],
    resume: bool,
    retry_not_found: bool,
    limit: Optional[int],
) -> tuple[list[DetailCandidate], int]:
    candidate_pool = candidates[:limit] if limit is not None else candidates
    pending = []
    cache_hits = 0
    for candidate in candidate_pool:
        cached = cache_rows.get(candidate.detail_key)
        if cached and resume:
            status = cached.get("status")
            if status == "done":
                cache_hits += 1
                continue
            if status == "not_found" and not retry_not_found:
                cache_hits += 1
                continue
        pending.append(candidate)
    return pending, cache_hits


def parse_detail_payload(payload: dict) -> dict:
    """
    Normaliza la respuesta JSON de CONREG a un esquema detail_* estable.
    """
    payload_id = payload.get("id") or {}
    return {
        "detail_anyo": payload_id.get("anyo"),
        "detail_numreg": payload_id.get("numreg"),
        "detail_numversion": payload_id.get("numversion"),
        "detail_expediente": payload.get("expediente"),
        "detail_entidad_adjudicadora": payload.get("deconentiadj"),
        "detail_unidad": payload.get("deunidad"),
        "detail_objeto": payload.get("objeto"),
        "detail_tipo_contrato": payload.get("desctipocont"),
        "detail_procedimiento": payload.get("descproced"),
        "detail_tramitacion": payload.get("descTramitacion"),
        "detail_tipo_menor": payload.get("descmenor"),
        "detail_es_menor": (
            True
            if payload.get("descmenor") == "Menor"
            else parse_bool_text(payload.get("menor"))
        ),
        "detail_lugar_ejecucion": payload.get("desclugareje"),
        "detail_adjudicatario": payload.get("nomcontratista") or payload.get("nombre"),
        "detail_nif_adjudicatario": payload.get("nifMascara"),
        "detail_pais_adjudicatario": payload.get("descPais"),
        "detail_importe_adjudicacion_total": parse_euro_amount(payload.get("imadjudicacion")),
        "detail_importe_adjudicacion_iva": parse_euro_amount(payload.get("imadjudicacioniva")),
        "detail_importe_adjudicacion_sin_iva": parse_euro_amount(payload.get("imadjudicacionsiniva")),
        "detail_importe_licitacion_total": parse_euro_amount(payload.get("impLicitacion")),
        "detail_importe_licitacion_iva": parse_euro_amount(payload.get("impLicitacionIva")),
        "detail_importe_licitacion_sin_iva": parse_euro_amount(payload.get("impLicitacionSinIva")),
        "detail_importe_definitivo": parse_euro_amount(payload.get("importeDefinitivo")),
        "detail_valor_estimado": parse_euro_amount(payload.get("valorEstimado")),
        "detail_fecha_adjudicacion": parse_date_es(payload.get("feadjudicacion")),
        "detail_fecha_formalizacion": parse_date_es(payload.get("feformalizacion")),
        "detail_fecha_prev_fin": parse_date_es(payload.get("fechaPrevFin")),
        "detail_fecha_prevista_sin_garantia": parse_date_es(payload.get("fechaPrevistaSinGarantia")),
        "detail_fecha_emergencia": parse_date_es(payload.get("fechaEmergencia")),
        "detail_plazo_ejecucion": parse_int(payload.get("plazoEjecucion")),
        "detail_unidad_plazo_ejecucion": payload.get("descUniPlazoEjecucion"),
        "detail_plazo_max_contrato": parse_int(payload.get("plazoMaxContrato")),
        "detail_unidad_plazo_max": payload.get("descUniMaxContrato"),
        "detail_num_anualidades": parse_int(payload.get("numAnualidades")),
        "detail_num_lotes": parse_int(payload.get("numLotes")),
        "detail_num_invitadas": parse_int(payload.get("numInvitadas")),
        "detail_num_licitaciones": parse_int(payload.get("numLicitaciones")),
        "detail_acuerdo_marco": parse_bool_text(payload.get("amarco")),
        "detail_exp_sistema_marco": payload.get("expSistemaMarco"),
        "detail_sistema_precios": payload.get("descSistemaPrecios"),
        "detail_forma_adjudicacion": payload.get("descFormAdj"),
        "detail_financiado_eu": parse_bool_text(payload.get("financiadoEu")),
        "detail_fondo_europeo": payload.get("fondoEuropeo"),
        "detail_pyme": parse_bool_text(payload.get("pyme")),
        "detail_clausulas_sociales": parse_bool_text(payload.get("incluClausulas")),
        "detail_reservado": parse_bool_text(payload.get("reservado")),
        "detail_cpv_principal": payload.get("cpvPrincipal"),
        "detail_url_placsp": payload.get("urlplacsp"),
        "detail_motivo_necesidad": payload.get("motivoNecesidad"),
        "detail_clasificaciones_count": len(payload.get("clasificaciones") or []),
    }


def get_response_json(session: requests.Session, ejercicio: str, numero_registro: str) -> tuple[int, Optional[dict]]:
    response = session.get(f"{DETAIL_API_BASE}/{ejercicio}&{numero_registro}", timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        return 404, None
    response.raise_for_status()
    return response.status_code, response.json()


def search_candidates_by_expediente(session: requests.Session, expediente: str) -> list[dict]:
    response = session.post(
        DETAIL_SEARCH_URL,
        timeout=REQUEST_TIMEOUT,
        json={"expediente": expediente},
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("content") or []


def select_search_match(
    search_results: list[dict],
    expected_year: str,
    expected_expediente: Optional[str] = None,
) -> tuple[Optional[tuple[str, str]], str]:
    unique_pairs = []
    seen = set()
    exact_same_year_pairs = []
    expediente_norm = (expected_expediente or "").strip().lower()

    for item in search_results:
        item_id = item.get("id") or {}
        year = normalize_year(item_id.get("anyo"))
        registro = normalize_registro(item_id.get("numreg"))
        if not year or not registro:
            continue
        pair = (year, registro)
        if pair in seen:
            continue
        seen.add(pair)
        unique_pairs.append(pair)
        if expediente_norm and year == expected_year:
            item_expediente = (item.get("expediente") or "").strip().lower()
            if item_expediente == expediente_norm:
                exact_same_year_pairs.append(pair)

    if len(exact_same_year_pairs) == 1:
        return exact_same_year_pairs[0], f"search_exact_year_1:{expected_year}"

    if not unique_pairs:
        return None, "search_0"
    if len(unique_pairs) == 1:
        return unique_pairs[0], "search_1"

    year_matches = [pair for pair in unique_pairs if pair[0] == expected_year]
    if len(year_matches) == 1:
        return year_matches[0], f"search_year_1:{expected_year}"

    return None, f"search_ambiguous:{len(unique_pairs)}"


def fetch_candidate(candidate: DetailCandidate) -> dict:
    session = get_thread_session()
    source_url = build_source_url(candidate.ejercicio, candidate.numero_registro)
    fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    attempts = 1

    try:
        source_kind = "conreg_registro"
        status_code, payload = get_response_json(session, candidate.ejercicio, candidate.numero_registro)
        if status_code == 404:
            not_found_reason = "404"
            if candidate.expediente:
                search_results = search_candidates_by_expediente(session, candidate.expediente)
                search_match, not_found_reason = select_search_match(
                    search_results,
                    candidate.ejercicio,
                    candidate.expediente,
                )
                attempts += 1
                if search_match is not None:
                    alt_year, alt_registro = search_match
                    source_kind = "conreg_expediente_search"
                    status_code, payload = get_response_json(session, alt_year, alt_registro)
                    attempts += 1
                    if status_code != 404:
                        detail_data = parse_detail_payload(payload)
                        return {
                            "detail_key": candidate.detail_key,
                            "ejercicio": candidate.ejercicio,
                            "numero_registro": candidate.numero_registro,
                            "expediente": candidate.expediente,
                            "source_url": build_source_url(alt_year, alt_registro),
                            "source_kind": source_kind,
                            "status": "done",
                            "attempts": attempts,
                            "last_error": None,
                            "fetched_at": fetched_at,
                            "payload_json": json.dumps(detail_data, ensure_ascii=False),
                            "detail_data": detail_data,
                        }
                    not_found_reason = f"{not_found_reason};alt_404"
            return {
                "detail_key": candidate.detail_key,
                "ejercicio": candidate.ejercicio,
                "numero_registro": candidate.numero_registro,
                "expediente": candidate.expediente,
                "source_url": source_url,
                "source_kind": source_kind,
                "status": "not_found",
                "attempts": attempts,
                "last_error": not_found_reason,
                "fetched_at": fetched_at,
                "payload_json": None,
                "detail_data": {},
            }
        detail_data = parse_detail_payload(payload)
        return {
            "detail_key": candidate.detail_key,
            "ejercicio": candidate.ejercicio,
            "numero_registro": candidate.numero_registro,
            "expediente": candidate.expediente,
            "source_url": source_url,
            "source_kind": source_kind,
            "status": "done",
            "attempts": attempts,
            "last_error": None,
            "fetched_at": fetched_at,
            "payload_json": json.dumps(detail_data, ensure_ascii=False),
            "detail_data": detail_data,
        }
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        return {
            "detail_key": candidate.detail_key,
            "ejercicio": candidate.ejercicio,
            "numero_registro": candidate.numero_registro,
            "expediente": candidate.expediente,
            "source_url": source_url,
            "source_kind": "conreg_registro",
            "status": "failed",
            "attempts": 1,
            "last_error": str(status_code or exc)[:200],
            "fetched_at": fetched_at,
            "payload_json": None,
            "detail_data": {},
        }
    except Exception as exc:
        return {
            "detail_key": candidate.detail_key,
            "ejercicio": candidate.ejercicio,
            "numero_registro": candidate.numero_registro,
            "expediente": candidate.expediente,
            "source_url": source_url,
            "source_kind": "conreg_registro",
            "status": "failed",
            "attempts": 1,
            "last_error": str(exc)[:200],
            "fetched_at": fetched_at,
            "payload_json": None,
            "detail_data": {},
        }


def fetch_details(
    pending: list[DetailCandidate],
    cache_path: Path,
    workers: int,
    batch_size: int,
) -> dict:
    stats = {
        "pending_candidates": len(pending),
        "api_requests": 0,
        "done": 0,
        "not_found": 0,
        "failed": 0,
    }
    if not pending:
        return stats

    max_workers = max(1, min(workers, len(pending)))
    for start in range(0, len(pending), batch_size):
        chunk = pending[start:start + batch_size]
        rows_to_persist = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(fetch_candidate, candidate): candidate for candidate in chunk}
            for future in as_completed(future_map):
                result = future.result()
                stats["api_requests"] += 1
                stats[result["status"]] += 1
                rows_to_persist.append({key: value for key, value in result.items() if key != "detail_data"})
        upsert_cache_rows(cache_path, rows_to_persist)
    return stats


def build_detail_dataframe(cache_path: Path) -> pd.DataFrame:
    with sqlite3.connect(cache_path) as conn:
        dataframe = pd.read_sql_query(
            """
            SELECT detail_key, status, attempts, last_error, source_url, source_kind,
                   fetched_at, payload_json
            FROM valencia_detail
            """,
            conn,
        )
    if dataframe.empty:
        return dataframe

    detail_rows = []
    for row in dataframe.itertuples(index=False):
        if row.payload_json is None or pd.isna(row.payload_json):
            payload = {}
        else:
            payload = json.loads(row.payload_json)
        payload.update(
            {
                "detail_key": row.detail_key,
                "detail_status": row.status,
                "detail_attempts": row.attempts,
                "detail_last_error": row.last_error,
                "detail_source_url": row.source_url,
                "detail_source_kind": row.source_kind,
                "detail_fetched_at": row.fetched_at,
            }
        )
        detail_rows.append(payload)
    return pd.DataFrame(detail_rows)


def run_merge(base_df: pd.DataFrame, cache_path: Path, output_path: Path) -> dict:
    """
    Une la base con la cache SQLite y deja un parquet detallado reproducible.
    """
    detail_df = build_detail_dataframe(cache_path)
    fallback_count = 0
    if not detail_df.empty:
        fallback_df = build_base_fallback_dataframe(base_df, detail_df)
        if not fallback_df.empty:
            fallback_count = len(fallback_df)
            detail_df = detail_df.set_index("detail_key")
            fallback_df = fallback_df.set_index("detail_key")
            replace_keys = detail_df.index.intersection(fallback_df.index)
            detail_df = detail_df.drop(index=replace_keys)
            detail_df = pd.concat([detail_df, fallback_df], axis=0, sort=False).reset_index()
    if detail_df.empty:
        merged = base_df.copy()
    else:
        merged = base_df.merge(detail_df, how="left", on="detail_key")

    merged = coerce_object_columns_for_parquet(merged)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, index=False)
    return {
        "rows": len(merged),
        "columns": len(merged.columns),
        "output_path": str(output_path),
        "base_fallback_done": fallback_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detalle incremental de contratacion de Valencia.")
    parser.add_argument(
        "--input-path",
        default=str(DEFAULT_INPUT_PATH),
        help=f"Directorio o parquet base de contratacion (default: {DEFAULT_INPUT_PATH}).",
    )
    parser.add_argument(
        "--output-path",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"Parquet enriquecido de salida (default: {DEFAULT_OUTPUT_PATH}).",
    )
    parser.add_argument(
        "--cache-path",
        default=str(DEFAULT_CACHE_PATH),
        help=f"SQLite de cache incremental (default: {DEFAULT_CACHE_PATH}).",
    )
    parser.add_argument(
        "--log-path",
        default=str(DEFAULT_LOG_PATH),
        help=f"Ruta del log (default: {DEFAULT_LOG_PATH}).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Workers para peticiones detail (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Candidatos por lote de escritura en cache (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limita el numero de candidatos a procesar.")
    parser.add_argument("--resume", action="store_true", help="Reutiliza done/not_found previos de la cache.")
    parser.add_argument(
        "--retry-not-found",
        action="store_true",
        help="Con --resume, vuelve a intentar los not_found previos.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configured_logger = build_logger(Path(args.log_path))
    configured_logger.info("Inicio Valencia detail")

    artifacts = build_artifact_paths(args.input_path, args.output_path, args.cache_path)
    ensure_cache_schema(artifacts["cache"])

    start_time = time.perf_counter()
    base_df = load_base_dataframe(artifacts["input"])
    candidates, missing_key_rows = build_candidates(base_df)
    cached_rows = load_cached_rows(artifacts["cache"])
    pending, cache_hits = select_pending_candidates(
        candidates=candidates,
        cache_rows=cached_rows,
        resume=args.resume,
        retry_not_found=args.retry_not_found,
        limit=args.limit,
    )

    configured_logger.info("Base cargada: %s filas, %s candidatos, %s sin clave", len(base_df), len(candidates), missing_key_rows)
    configured_logger.info("Pendientes: %s | cache hits: %s", len(pending), cache_hits)

    fetch_stats = fetch_details(
        pending=pending,
        cache_path=artifacts["cache"],
        workers=args.workers,
        batch_size=max(1, args.batch_size),
    )
    merge_stats = run_merge(base_df, artifacts["cache"], artifacts["output"])
    duration = round(time.perf_counter() - start_time, 2)

    summary = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "input_path": str(artifacts["input"]),
        "output_path": str(artifacts["output"]),
        "cache_path": str(artifacts["cache"]),
        "workers": args.workers,
        "batch_size": args.batch_size,
        "resume": args.resume,
        "retry_not_found": args.retry_not_found,
        "limit": args.limit,
        "base_rows": len(base_df),
        "total_candidates": len(candidates),
        "missing_key_rows": missing_key_rows,
        "cache_hits": cache_hits,
        **fetch_stats,
        **merge_stats,
        "duration_seconds": duration,
    }
    write_json(artifacts["summary"], summary)

    configured_logger.info(
        "Detail completado: %s done, %s not_found, %s failed en %.2fs",
        fetch_stats["done"],
        fetch_stats["not_found"],
        fetch_stats["failed"],
        duration,
    )
    configured_logger.info("Output: %s", artifacts["output"])
    configured_logger.info("Resumen: %s", artifacts["summary"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
