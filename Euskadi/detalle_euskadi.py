#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 DETALLE XML — CONTRATACIÓN PÚBLICA DE EUSKADI
═══════════════════════════════════════════════════════════════════════════════
 Enriquece contratos_master.parquet con detalle público enlazado por Open Data
 Euskadi / KontratazioA, resolviendo varias rutas posibles por expediente.

 Flujo:
   1. Lee contratos_master.parquet y construye una clave estable (`detail_key`)
      a partir de xml_datos/xml_metadatos/url_amigable/url_física.
   2. Descarga y parsea detalle incremental desde XML moderno, XML legacy,
      XML índice (`r01Index`) y fallback HTML.
   3. Guarda caché reanudable en SQLite con estado, fuente y payload comprimido.
   4. Genera contratos_master_detallado.parquet con merge sobre `detail_key`.
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import logging
import re
import sqlite3
import threading
import zlib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from bs4 import BeautifulSoup


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MASTER_PATH = SCRIPT_DIR / "euskadi_parquet" / "contratos_master.parquet"
DEFAULT_CACHE_PATH = SCRIPT_DIR / "euskadi_parquet" / "contratos_master_detail.sqlite3"
DEFAULT_OUTPUT_PATH = (
    SCRIPT_DIR / "euskadi_parquet" / "contratos_master_detallado.parquet"
)
DEFAULT_LOG_PATH = SCRIPT_DIR / "detalle_euskadi.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (investigacion-academica) contratacion-euskadi/4.0",
    "Accept": "application/xml, text/xml, */*",
}
TIMEOUT = 60
WRITE_BATCH_SIZE = 100
DEFAULT_WORKERS = 24
IN_FLIGHT_MULTIPLIER = 4
TERMINAL_STATUSES = {"done", "not_found"}
DETAIL_SOURCE_COLUMNS = [
    "xml_datos",
    "xml_metadatos",
    "url_amigable",
    "friendlyurl",
    "url_física",
    "physicalurl",
]
HTML_DETAIL_PREFIX_RE = re.compile(
    r"https?://[^/]+/(?:datos/)?contenidos/anuncio_contratacion/([^/]+)/",
    re.IGNORECASE,
)

log = logging.getLogger("euskadi.detalle")
log.addHandler(logging.NullHandler())
_thread_local = threading.local()


def build_logger(log_path: Path | None = None) -> logging.Logger:
    logger = logging.getLogger("euskadi.detalle")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def configure_runtime(log_path: Path | None = None) -> None:
    global LOG_PATH, log
    LOG_PATH = (log_path or DEFAULT_LOG_PATH).resolve()
    log = build_logger(LOG_PATH)


LOG_PATH = DEFAULT_LOG_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enriquece contratos_master.parquet con detalle XML de Euskadi",
    )
    parser.add_argument(
        "--master-path",
        type=Path,
        default=DEFAULT_MASTER_PATH,
        help="Ruta del contratos_master.parquet base.",
    )
    parser.add_argument(
        "--cache-path",
        type=Path,
        default=DEFAULT_CACHE_PATH,
        help="Ruta del SQLite incremental del detalle.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Ruta del Parquet enriquecido de salida.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Ruta del fichero de log del detalle.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Número de workers para descargar XML de detalle.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita el número de XMLs a procesar para smoke tests.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reanuda saltando xml_datos ya resueltos en la cache.",
    )
    parser.add_argument(
        "--retry-not-found",
        action="store_true",
        help="En resume, vuelve a intentar los casos marcados como not_found.",
    )
    parser.add_argument(
        "--detail-only",
        action="store_true",
        help="Solo descarga y actualiza la cache; no genera merge final.",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="No descarga detalle; solo genera el Parquet final desde la cache.",
    )
    return parser


def text_value(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value or None


def child_text(parent: ET.Element | None, tag: str) -> str | None:
    if parent is None:
        return None
    return text_value(parent.find(tag))


def str_to_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "sí", "si", "yes", "bai", "1"}:
        return True
    if normalized in {"false", "no", "ez", "0"}:
        return False
    return None


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        _thread_local.session = session
    return session


def normalize_value(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip()
    if not normalized or normalized.lower() in {"none", "nan", "nat", "<na>"}:
        return None
    return normalized


def is_http_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def looks_like_xml_url(value: str | None) -> bool:
    if not is_http_url(value):
        return False
    lowered = value.lower()
    if lowered.endswith(".zip"):
        return False
    return (
        lowered.endswith(".xml")
        or "/r01index/" in lowered
        or "/es_doc/data/" in lowered
    )


def looks_like_html_url(value: str | None) -> bool:
    if not is_http_url(value):
        return False
    lowered = value.lower()
    return (
        "/es_doc/" in lowered
        or "index.html" in lowered
        or "/es_arch_" in lowered
        or "/anuncio_contratacion/" in lowered
    )


def extract_detail_slug(value: str | None) -> str | None:
    if not value:
        return None
    match = HTML_DETAIL_PREFIX_RE.search(value)
    if match:
        return match.group(1)
    return None


def add_source_candidate(
    candidates: list[dict[str, str]],
    seen: set[str],
    url: str | None,
    source_kind: str,
) -> None:
    normalized = normalize_value(url)
    if not normalized or normalized in seen:
        return
    seen.add(normalized)
    candidates.append({"url": normalized, "kind": source_kind})


def derive_html_roots(row: dict[str, object]) -> list[str]:
    slugs = []
    for column in DETAIL_SOURCE_COLUMNS:
        slug = extract_detail_slug(normalize_value(row.get(column)))
        if slug and slug not in slugs:
            slugs.append(slug)

    roots = []
    for slug in slugs:
        roots.extend(
            [
                f"https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/{slug}/es_doc/",
                f"https://opendata.euskadi.eus/contenidos/anuncio_contratacion/{slug}/es_doc/",
            ]
        )
    return roots


def build_source_candidates(row: dict[str, object]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()

    raw_values = {column: normalize_value(row.get(column)) for column in DETAIL_SOURCE_COLUMNS}

    xml_priority = [
        raw_values.get("xml_datos"),
        raw_values.get("url_amigable"),
        raw_values.get("xml_metadatos"),
        raw_values.get("url_física"),
        raw_values.get("physicalurl"),
        raw_values.get("friendlyurl"),
    ]
    for value in xml_priority:
        if looks_like_xml_url(value):
            add_source_candidate(candidates, seen, value, "xml")

    html_priority = [
        raw_values.get("url_física"),
        raw_values.get("physicalurl"),
        raw_values.get("url_amigable"),
        raw_values.get("friendlyurl"),
    ]
    for value in html_priority:
        if looks_like_html_url(value):
            add_source_candidate(candidates, seen, value, "html")

    for value in derive_html_roots(row):
        add_source_candidate(candidates, seen, value, "html")

    return candidates


def build_detail_key(row: dict[str, object]) -> str | None:
    candidates = build_source_candidates(row)
    if not candidates:
        return None
    return candidates[0]["url"]


def ensure_cache(cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detail_cache (
            xml_url TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at TEXT NOT NULL,
            source_url TEXT,
            source_kind TEXT,
            mapped_json TEXT,
            raw_zlib BLOB
        )
        """
    )
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(detail_cache)").fetchall()
    }
    if "source_url" not in existing_columns:
        conn.execute("ALTER TABLE detail_cache ADD COLUMN source_url TEXT")
    if "source_kind" not in existing_columns:
        conn.execute("ALTER TABLE detail_cache ADD COLUMN source_kind TEXT")
    conn.commit()
    conn.close()


def load_existing_records(cache_path: Path) -> dict[str, tuple[str, int]]:
    if not cache_path.exists():
        return {}

    conn = sqlite3.connect(cache_path)
    rows = conn.execute("SELECT xml_url, status, attempts FROM detail_cache").fetchall()
    conn.close()
    return {xml_url: (status, attempts) for xml_url, status, attempts in rows}


def write_cache_rows(cache_path: Path, rows: list[dict]) -> None:
    if not rows:
        return

    conn = sqlite3.connect(cache_path)
    conn.executemany(
        """
        INSERT INTO detail_cache (
            xml_url,
            status,
            attempts,
            last_error,
            updated_at,
            source_url,
            source_kind,
            mapped_json,
            raw_zlib
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(xml_url) DO UPDATE SET
            status=excluded.status,
            attempts=excluded.attempts,
            last_error=excluded.last_error,
            updated_at=excluded.updated_at,
            source_url=excluded.source_url,
            source_kind=excluded.source_kind,
            mapped_json=excluded.mapped_json,
            raw_zlib=excluded.raw_zlib
        """,
        [
            (
                row["xml_url"],
                row["status"],
                row["attempts"],
                row["last_error"],
                row["updated_at"],
                row.get("source_url"),
                row.get("source_kind"),
                row["mapped_json"],
                row["raw_zlib"],
            )
            for row in rows
        ],
    )
    conn.commit()
    conn.close()


def parse_cpvs(contracting: ET.Element) -> dict[str, str | int | None]:
    cpvs = contracting.find("cpvs")
    if cpvs is None:
        return {
            "detail_cpv_count": 0,
            "detail_cpv_codes": None,
            "detail_cpv_names": None,
        }

    codes = []
    names = []
    for cpv in cpvs.findall("cpv"):
        code = cpv.attrib.get("id")
        name = child_text(cpv, "name")
        if code:
            codes.append(code)
        if name:
            names.append(name)

    return {
        "detail_cpv_count": len(codes),
        "detail_cpv_codes": "|".join(codes) or None,
        "detail_cpv_names": "|".join(names) or None,
    }


def parse_nuts(contracting: ET.Element) -> dict[str, str | int | None]:
    nuts_parent = contracting.find("placeExecutionNUTS")
    if nuts_parent is None:
        return {
            "detail_nuts_count": 0,
            "detail_nuts_codes": None,
            "detail_nuts_names": None,
        }

    codes = []
    names = []
    for place in nuts_parent.findall("placeExecution"):
        code = child_text(place, "code")
        name = child_text(place, "name")
        if code:
            codes.append(code)
        if name:
            names.append(name)

    return {
        "detail_nuts_count": len(codes),
        "detail_nuts_codes": "|".join(codes) or None,
        "detail_nuts_names": "|".join(names) or None,
    }


def parse_flags(contracting: ET.Element) -> dict[str, bool | int | None]:
    flags = {
        flag.attrib.get("id"): str_to_bool(text_value(flag))
        for flag in contracting.findall("flags/flag")
    }
    return {
        "detail_flag_count": len(flags),
        "detail_flag_contrato_menor": flags.get("contrato_menor"),
        "detail_flag_sara": flags.get("sara"),
        "detail_flag_licitacion_electronica": flags.get("licitacion_electronica"),
        "detail_flag_division_lotes": flags.get("division_lotes"),
        "detail_flag_prorroga": flags.get("prorroga"),
        "detail_flag_recurso": flags.get("recurso"),
    }


def parse_resolution(contracting: ET.Element) -> dict[str, str | int | None]:
    resolutions = contracting.findall("resolutions/resolution")
    first = resolutions[0] if resolutions else None
    adj_info = first.find("adjInfo") if first is not None else None
    licitor = adj_info.find("licitorCompanies") if adj_info is not None else None

    return {
        "detail_resolution_count": len(resolutions),
        "detail_award_price_without_vat": child_text(first, "priceWithoutVAT"),
        "detail_award_price_with_vat": child_text(first, "priceWithVAT"),
        "detail_award_date": child_text(adj_info, "date"),
        "detail_award_company_name": child_text(licitor, "name"),
        "detail_award_company_id": child_text(licitor, "cif"),
    }


def parse_formalization(contracting: ET.Element) -> dict[str, str | int | None]:
    formalizations = contracting.findall("formalizations/formalization")
    first = formalizations[0] if formalizations else None

    return {
        "detail_formalization_count": len(formalizations),
        "detail_contract_code": child_text(first, "contractCode"),
        "detail_contract_state": child_text(first, "state"),
        "detail_formalization_company_id": child_text(first, "id"),
        "detail_formalization_company_name": child_text(first, "businessName"),
        "detail_formalization_date": child_text(first, "dateCompanySignature"),
    }


def parse_offer_management(contracting: ET.Element) -> dict[str, str | int | None]:
    offers = contracting.findall("offersManagement/offerManagement")
    first = offers[0] if offers else None

    return {
        "detail_offer_management_count": len(offers),
        "detail_offer_company_name": child_text(first, "name"),
        "detail_offer_company_id": child_text(first, "cif"),
        "detail_offer_company_province": child_text(first, "provincia"),
    }


def collect_html_pairs(soup: BeautifulSoup) -> dict[str, str]:
    pairs: dict[str, str] = {}

    def store(label: str | None, value: str | None) -> None:
        label_text = normalize_value(label)
        value_text = normalize_value(value)
        if not label_text or not value_text:
            return
        pairs.setdefault(label_text, value_text)

    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        store(dt.get_text(" ", strip=True), dd.get_text(" ", strip=True) if dd else None)

    for row in soup.select("div.row"):
        direct_children = row.find_all("div", recursive=False)
        values = [
            normalize_value(child.get_text(" ", strip=True))
            for child in direct_children
        ]
        values = [value for value in values if value]
        if len(values) >= 2:
            store(values[0], " ".join(values[1:]))

    for tr in soup.select("tr"):
        cells = [
            normalize_value(cell.get_text(" ", strip=True))
            for cell in tr.find_all(["th", "td"], recursive=False)
        ]
        cells = [cell for cell in cells if cell]
        if len(cells) >= 2:
            store(cells[0], " ".join(cells[1:]))

    return pairs


def looks_like_not_found_body(content: bytes) -> bool:
    lowered = content.lower()
    return b"recurso dentro de" in lowered and b"no encontrado" in lowered


def parse_contracting_html(content: bytes) -> dict[str, object]:
    soup = BeautifulSoup(content, "html.parser")
    pairs = collect_html_pairs(soup)
    title = normalize_value(soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else None)

    if not pairs and not title:
        if looks_like_not_found_body(content):
            raise FileNotFoundError("Detalle HTML no encontrado")
        raise ValueError("HTML sin pares de detalle")

    return {
        "detail_html_title": title,
        "detail_html_pairs_count": len(pairs),
        "detail_cod_exp": pairs.get("Expediente") or title,
        "detail_subject": pairs.get("Objeto del contrato"),
        "detail_first_publication_date": pairs.get("Fecha de la primera publicación"),
        "detail_last_publication_date": pairs.get("Fecha de la última publicación"),
        "detail_contracting_type": pairs.get("Tipo de contrato"),
        "detail_processing_status": pairs.get("Estado de la tramitación"),
        "detail_contracting_authority_name": pairs.get("Poder adjudicador"),
        "detail_entity_driving_name": pairs.get("Entidad impulsora"),
        "detail_contracting_body_name": pairs.get("Órgano de contratación"),
        "detail_adjudication_procedure": pairs.get("Procedimiento de adjudicación"),
        "detail_contracting_authority_web": pairs.get("Dirección web de Licitación electrónica"),
        "detail_budget_without_vat": pairs.get("Presupuesto del contrato sin IVA"),
        "detail_budget_with_vat": pairs.get("Presupuesto del contrato con IVA"),
        "detail_flag_contrato_menor": str_to_bool(pairs.get("Contrato menor")),
        "detail_flag_sara": str_to_bool(
            pairs.get("Contrato sujeto a regulación armonizada")
        ),
    }


def pick_preferred_document(root: ET.Element) -> ET.Element | None:
    documents = root.findall("./documents/document")
    if not documents:
        return None

    for language in ("es", "eu"):
        for document in documents:
            if child_text(document, "language") == language:
                return document

    return documents[0]


def parse_index_metadata(document: ET.Element | None) -> dict[str, str]:
    if document is None:
        return {}

    data_files = document.findall(".//dataFile")
    if not data_files:
        return {}

    preferred_data_file = None
    for data_file in data_files:
        if data_file.attrib.get("mainDataFile", "").lower() == "true":
            preferred_data_file = data_file
            break
    if preferred_data_file is None:
        preferred_data_file = data_files[0]

    metadata: dict[str, str] = {}
    for meta in preferred_data_file.findall("./metaDataList/metaData"):
        oid = meta.attrib.get("metaDataOid")
        value = normalize_value(meta.findtext("metaDataValue"))
        if oid and value and oid not in metadata:
            metadata[oid] = value

    return metadata


def term_texts(node: ET.Element) -> dict[str, str]:
    terms: dict[str, str] = {}
    for term in node.findall("./terms/term"):
        lang = normalize_value(term.attrib.get("lang"))
        text = normalize_value(term.findtext("termText"))
        if lang and text:
            terms[lang] = text
    return terms


def choose_catalog_term(
    root: ET.Element,
    *,
    role: str | None = None,
    preferred_languages: tuple[str, ...] = ("es", "eu", "en"),
    exclude_values: set[str] | None = None,
) -> str | None:
    exclude_values = exclude_values or set()
    for catalog in root.findall("./structureCatalogs/structureCatalog"):
        if role is not None and catalog.attrib.get("labelRoleInStructure") != role:
            continue
        terms = term_texts(catalog)
        for language in preferred_languages:
            value = normalize_value(terms.get(language))
            if value and value not in exclude_values:
                return value
    return None


def parse_sparse_index_xml(root: ET.Element) -> dict[str, object]:
    raw_name = normalize_value(root.findtext("name"))
    internal_name = normalize_value(root.findtext("internalName"))
    subject = raw_name if raw_name and raw_name != internal_name else None

    authority = choose_catalog_term(root, role="2")
    body = choose_catalog_term(root, role="3", exclude_values={authority} if authority else set())
    excluded_type_values = {
        "Bizkaia",
        "Araba/Álava",
        "Gipuzkoa",
        "Euskadi",
        "País Vasco",
    }
    if authority:
        excluded_type_values.add(authority)
    if body:
        excluded_type_values.add(body)
    contract_type = choose_catalog_term(
        root,
        exclude_values=excluded_type_values,
    )

    return {
        "detail_xml_id": root.attrib.get("oid"),
        "detail_language": None,
        "detail_id_exp_origen": None,
        "detail_cod_exp": internal_name or raw_name,
        "detail_first_publication_date": normalize_value(root.findtext("createDate")),
        "detail_last_publication_date": None,
        "detail_contract_period": None,
        "detail_contract_period_type": None,
        "detail_processing_status": None,
        "detail_contracting_authority_name": authority,
        "detail_contracting_authority_nuts": None,
        "detail_contracting_authority_address": None,
        "detail_contracting_authority_web": None,
        "detail_entity_driving_name": authority,
        "detail_contracting_body_name": body or authority,
        "detail_contracting_type": contract_type,
        "detail_subject": subject,
        "detail_processing": None,
        "detail_adjudication_procedure": None,
        "detail_bidders_number": None,
        "detail_pyme_num_offers": None,
        "detail_other_estates_num_offers": None,
        "detail_third_countries_num_offers": None,
        "detail_electronically_num_offers": None,
        "detail_cpv_count": 0,
        "detail_cpv_codes": None,
        "detail_cpv_names": None,
        "detail_nuts_count": 0,
        "detail_nuts_codes": None,
        "detail_nuts_names": None,
        "detail_flag_count": 0,
        "detail_flag_contrato_menor": None,
        "detail_flag_sara": None,
        "detail_flag_licitacion_electronica": None,
        "detail_flag_division_lotes": None,
        "detail_flag_prorroga": None,
        "detail_flag_recurso": None,
        "detail_offer_management_count": 0,
        "detail_offer_company_name": None,
        "detail_offer_company_id": None,
        "detail_offer_company_province": None,
        "detail_formalization_count": 0,
        "detail_contract_code": None,
        "detail_contract_state": None,
        "detail_formalization_company_id": None,
        "detail_formalization_company_name": None,
        "detail_formalization_date": None,
        "detail_resolution_count": 0,
        "detail_award_price_without_vat": None,
        "detail_award_price_with_vat": None,
        "detail_award_date": None,
        "detail_award_company_name": None,
        "detail_award_company_id": None,
        "detail_budget_without_vat": None,
        "detail_budget_with_vat": None,
        "detail_index_metadata_count": 0,
        "detail_index_partial": True,
    }


def parse_content_index_xml(root: ET.Element) -> dict[str, object]:
    document = pick_preferred_document(root)
    metadata = parse_index_metadata(document)
    if not metadata:
        return parse_sparse_index_xml(root)

    publish_date = normalize_value(root.findtext(".//publishDate"))
    authority_web = metadata.get("contratacion_poder_adjudicador_url")
    if authority_web and authority_web.startswith("/"):
        authority_web = f"https://www.contratacion.euskadi.eus{authority_web}"

    return {
        "detail_xml_id": root.attrib.get("oid"),
        "detail_language": child_text(document, "language"),
        "detail_id_exp_origen": metadata.get("contratacion_id_peticion"),
        "detail_cod_exp": metadata.get("contratacion_expediente"),
        "detail_first_publication_date": metadata.get(
            "contratacion_fecha_primera_publicacion"
        )
        or publish_date,
        "detail_last_publication_date": metadata.get(
            "contratacion_fecha_de_publicacion_documento"
        )
        or publish_date,
        "detail_contract_period": metadata.get(
            "contratacion_duracion_contrato_plazo_ejecucion"
        ),
        "detail_contract_period_type": metadata.get(
            "contratacion_duracion_contrato_tipo"
        ),
        "detail_processing_status": metadata.get("contratacion_estado_tramitacion"),
        "detail_contracting_authority_name": metadata.get(
            "contratacion_poder_adjudicador_titulo"
        ),
        "detail_contracting_authority_nuts": metadata.get(
            "contratacion_codigo_nuts"
        ),
        "detail_contracting_authority_address": metadata.get(
            "contratacion_poder_adjudicador_direccion"
        ),
        "detail_contracting_authority_web": authority_web,
        "detail_entity_driving_name": metadata.get("contratacion_entidad_impulsora"),
        "detail_contracting_body_name": metadata.get("contratacion_organo_contratacion"),
        "detail_contracting_type": metadata.get("contratacion_tipo_contrato"),
        "detail_subject": metadata.get("contratacion_objeto_contrato")
        or metadata.get("contratacion_titulo_contrato"),
        "detail_processing": metadata.get("contratacion_tramitacion"),
        "detail_adjudication_procedure": metadata.get("contratacion_procedimiento"),
        "detail_bidders_number": metadata.get("contratacion_num_licitadores"),
        "detail_pyme_num_offers": None,
        "detail_other_estates_num_offers": None,
        "detail_third_countries_num_offers": None,
        "detail_electronically_num_offers": None,
        "detail_cpv_count": 1 if metadata.get("contratacion_codigo_cpv") else 0,
        "detail_cpv_codes": metadata.get("contratacion_codigo_cpv"),
        "detail_cpv_names": metadata.get("contratacion_desc_cpv"),
        "detail_nuts_count": 1 if metadata.get("contratacion_codigo_nuts") else 0,
        "detail_nuts_codes": metadata.get("contratacion_codigo_nuts"),
        "detail_nuts_names": metadata.get("contratacion_desc_nuts"),
        "detail_flag_count": 2,
        "detail_flag_contrato_menor": str_to_bool(
            metadata.get("contratacion_contrato_menor")
        ),
        "detail_flag_sara": str_to_bool(metadata.get("contratacion_sara")),
        "detail_flag_licitacion_electronica": str_to_bool(
            metadata.get("contratacion_licitacion_electronica")
        ),
        "detail_flag_division_lotes": str_to_bool(
            metadata.get("contratacion_division_lotes")
        ),
        "detail_flag_prorroga": str_to_bool(metadata.get("contratacion_prorroga")),
        "detail_flag_recurso": str_to_bool(metadata.get("contratacion_recurso")),
        "detail_offer_management_count": 0,
        "detail_offer_company_name": None,
        "detail_offer_company_id": None,
        "detail_offer_company_province": None,
        "detail_formalization_count": 0,
        "detail_contract_code": None,
        "detail_contract_state": None,
        "detail_formalization_company_id": None,
        "detail_formalization_company_name": None,
        "detail_formalization_date": None,
        "detail_resolution_count": 0,
        "detail_award_price_without_vat": metadata.get(
            "contratacion_precio_sin_iva"
        )
        or metadata.get("contratacion_presupuesto_sin_iva"),
        "detail_award_price_with_vat": metadata.get("contratacion_precio_con_iva")
        or metadata.get("contratacion_presupuesto_con_iva"),
        "detail_award_date": metadata.get("contratacion_fecha_adjudicacion"),
        "detail_award_company_name": metadata.get("contratacion_adjudicacion"),
        "detail_award_company_id": metadata.get("contratacion_adjudicacion_cif"),
        "detail_budget_without_vat": metadata.get("contratacion_presupuesto_sin_iva"),
        "detail_budget_with_vat": metadata.get("contratacion_presupuesto_con_iva"),
        "detail_index_metadata_count": len(metadata),
    }


def parse_contracting_xml(content: bytes) -> dict[str, object]:
    root = ET.fromstring(content)
    if root.tag == "record":
        return parse_legacy_record_xml(root)
    if root.tag == "content":
        return parse_content_index_xml(root)

    contracting = root.find("contracting")
    if contracting is None:
        raise ValueError("XML sin nodo <contracting>")

    authority = contracting.find("contractingAuthority")
    entity_driving = contracting.find("entityDriving")
    body = contracting.find("contractingBody")

    result = {
        "detail_xml_id": root.attrib.get("id"),
        "detail_language": child_text(root, "language"),
        "detail_id_exp_origen": child_text(contracting, "idExpOrigen"),
        "detail_cod_exp": child_text(contracting, "codExp"),
        "detail_first_publication_date": child_text(
            contracting, "firstPublicationDate"
        ),
        "detail_last_publication_date": child_text(contracting, "lastPublicationDate"),
        "detail_contract_period": child_text(contracting, "contractPeriod"),
        "detail_contract_period_type": child_text(contracting, "contractPeriodType"),
        "detail_processing_status": child_text(contracting, "processingStatus"),
        "detail_contracting_authority_name": child_text(authority, "name"),
        "detail_contracting_authority_nuts": child_text(authority, "nutsCode"),
        "detail_contracting_authority_address": child_text(authority, "address"),
        "detail_contracting_authority_web": child_text(authority, "webPage"),
        "detail_entity_driving_name": child_text(entity_driving, "name"),
        "detail_contracting_body_name": child_text(body, "name"),
        "detail_contracting_type": child_text(contracting, "contractingType"),
        "detail_subject": child_text(contracting, "subject"),
        "detail_processing": child_text(contracting, "processing"),
        "detail_adjudication_procedure": child_text(
            contracting, "adjudicationProcedure"
        ),
        "detail_bidders_number": child_text(contracting, "biddersNumber"),
        "detail_pyme_num_offers": child_text(contracting, "pymeNumOffers"),
        "detail_other_estates_num_offers": child_text(
            contracting, "otherEstatesNumOffers"
        ),
        "detail_third_countries_num_offers": child_text(
            contracting, "thirdCountriesNumOffers"
        ),
        "detail_electronically_num_offers": child_text(
            contracting, "electronicallyNumOffers"
        ),
    }

    result.update(parse_cpvs(contracting))
    result.update(parse_nuts(contracting))
    result.update(parse_flags(contracting))
    result.update(parse_offer_management(contracting))
    result.update(parse_formalization(contracting))
    result.update(parse_resolution(contracting))
    return result


def flatten_legacy_items(node: ET.Element, prefix: str = "") -> dict[str, str]:
    data: dict[str, str] = {}
    for item in node.findall("item"):
        name = item.attrib.get("name", "item")
        key = f"{prefix}__{name}" if prefix else name
        value = item.find("value")
        if value is None:
            continue

        child_items = value.findall("item")
        if child_items:
            data.update(flatten_legacy_items(value, key))
            continue

        text = "".join(value.itertext()).strip()
        if text:
            data[key] = text
    return data


def count_legacy_items(flat: dict[str, str], prefix: str, suffix: str) -> int:
    pattern = re.compile(rf"^{re.escape(prefix)}__(\d+)__{re.escape(suffix)}$")
    indexes = {int(match.group(1)) for key in flat if (match := pattern.match(key))}
    return len(indexes)


def collect_legacy_series(
    flat: dict[str, str],
    code_template: str,
    name_template: str,
) -> tuple[list[str], list[str]]:
    codes = []
    names = []
    index = 0
    while True:
        code = flat.get(code_template.format(index=index))
        name = flat.get(name_template.format(index=index))
        if code is None and name is None:
            break
        if code:
            codes.append(code)
        if name:
            names.append(name)
        index += 1
    return codes, names


def parse_legacy_record_xml(root: ET.Element) -> dict[str, object]:
    flat = flatten_legacy_items(root)

    cpv_codes, cpv_names = collect_legacy_series(
        flat,
        "contratacion__contratacion_codigo_cpv__{index}__contratacion_cpv",
        "contratacion__contratacion_codigo_cpv__{index}__contratacion_desc_cpv",
    )

    offer_prefix = "contratacion__contratacion_empresas_licitadoras"
    formalization_prefix = "contratacion__contratacion_formalizacion_contrato"
    resolution_prefix = "contratacion__contratacion_informe_adjudicacion_definitiva"

    return {
        "detail_xml_id": flat.get("v79_idRec"),
        "detail_language": flat.get("v79_idioma"),
        "detail_id_exp_origen": flat.get("contratacion__contratacion_id_peticion"),
        "detail_cod_exp": flat.get("contratacion__contratacion_expediente"),
        "detail_first_publication_date": flat.get(
            "contratacion__contratacion_fecha_primera_publicacion"
        ),
        "detail_last_publication_date": flat.get(
            "contratacion__contratacion_fecha_de_publicacion_documento"
        ),
        "detail_contract_period": flat.get(
            "contratacion__contratacion_duracion_contrato_plazo_ejecucion"
        ),
        "detail_contract_period_type": None,
        "detail_processing_status": flat.get(
            "contratacion__contratacion_estado_tramitacion__valor"
        ),
        "detail_contracting_authority_name": flat.get(
            "contratacion__contratacion_poder_adjudicador__valor"
        ),
        "detail_contracting_authority_nuts": flat.get(
            "contratacion__contratacion_poder_adjudicador__contratacion_codigo_nuts"
        ),
        "detail_contracting_authority_address": flat.get(
            "contratacion__contratacion_poder_adjudicador__direccion"
        ),
        "detail_contracting_authority_web": flat.get(
            "contratacion__contratacion_poder_adjudicador__direccion_web"
        ),
        "detail_entity_driving_name": flat.get(
            "contratacion__contratacion_entidad_impulsora__valor"
        ),
        "detail_contracting_body_name": flat.get(
            "contratacion__contratacion_entidad_tramitadora__valor"
        ),
        "detail_contracting_type": flat.get(
            "contratacion__contratacion_tipo_contrato__valor"
        ),
        "detail_subject": flat.get("contratacion__contratacion_titulo_contrato"),
        "detail_processing": flat.get("contratacion__contratacion_tramitacion__valor"),
        "detail_adjudication_procedure": flat.get(
            "contratacion__contratacion_procedimiento__valor"
        ),
        "detail_bidders_number": flat.get("contratacion__contratacion_num_licitadores"),
        "detail_pyme_num_offers": None,
        "detail_other_estates_num_offers": None,
        "detail_third_countries_num_offers": None,
        "detail_electronically_num_offers": None,
        "detail_cpv_count": len(cpv_codes),
        "detail_cpv_codes": "|".join(cpv_codes) or None,
        "detail_cpv_names": "|".join(cpv_names) or None,
        "detail_nuts_count": 1
        if flat.get(
            "contratacion__contratacion_poder_adjudicador__contratacion_codigo_nuts"
        )
        else 0,
        "detail_nuts_codes": flat.get(
            "contratacion__contratacion_poder_adjudicador__contratacion_codigo_nuts"
        ),
        "detail_nuts_names": None,
        "detail_flag_count": 1
        if flat.get("contratacion__contratacion_contrato_menor") is not None
        else 0,
        "detail_flag_contrato_menor": str_to_bool(
            flat.get("contratacion__contratacion_contrato_menor")
        ),
        "detail_flag_sara": None,
        "detail_flag_licitacion_electronica": None,
        "detail_flag_division_lotes": str_to_bool(
            flat.get("contratacion__contratacion_division_lotes")
        ),
        "detail_flag_prorroga": None,
        "detail_flag_recurso": None,
        "detail_offer_management_count": count_legacy_items(
            flat,
            offer_prefix,
            "contratacion_empresa_licitadora_razon_social",
        ),
        "detail_offer_company_name": flat.get(
            f"{offer_prefix}__0__contratacion_empresa_licitadora_razon_social"
        ),
        "detail_offer_company_id": flat.get(
            f"{offer_prefix}__0__contratacion_empresa_licitadora_cif"
        ),
        "detail_offer_company_province": flat.get(
            f"{offer_prefix}__0__contratacion_empresa_licitadora_provincia"
        ),
        "detail_formalization_count": count_legacy_items(
            flat, formalization_prefix, "empresa"
        ),
        "detail_contract_code": None,
        "detail_contract_state": None,
        "detail_formalization_company_id": None,
        "detail_formalization_company_name": flat.get(
            f"{formalization_prefix}__0__empresa"
        ),
        "detail_formalization_date": flat.get(
            f"{formalization_prefix}__0__fecha_firma"
        ),
        "detail_resolution_count": count_legacy_items(
            flat, resolution_prefix, "empresa"
        ),
        "detail_award_price_without_vat": flat.get(f"{resolution_prefix}__0__precio"),
        "detail_award_price_with_vat": flat.get(f"{resolution_prefix}__0__precioIVA"),
        "detail_award_date": flat.get(
            "contratacion__contratacion_fecha_adjudicacion_definitiva"
        ),
        "detail_award_company_name": flat.get(f"{resolution_prefix}__0__empresa"),
        "detail_award_company_id": None,
    }


def build_candidates(
    master_path: Path,
    cache_path: Path,
    resume: bool,
    retry_not_found: bool,
    limit: int | None = None,
) -> tuple[list[dict[str, object]], int]:
    df = pd.read_parquet(master_path, columns=DETAIL_SOURCE_COLUMNS)

    candidates_by_key: dict[str, dict[str, object]] = {}
    for row in df.to_dict(orient="records"):
        detail_key = build_detail_key(row)
        if not detail_key or detail_key in candidates_by_key:
            continue

        source_candidates = build_source_candidates(row)
        if not source_candidates:
            continue

        candidates_by_key[detail_key] = {
            "detail_key": detail_key,
            "source_candidates": source_candidates,
        }

    candidates = list(candidates_by_key.values())
    total_available = len(candidates)

    if resume:
        existing = load_existing_records(cache_path)
        skipped_statuses = {"done"} if retry_not_found else TERMINAL_STATUSES
        candidates = [
            candidate
            for candidate in candidates
            if existing.get(candidate["detail_key"], ("", 0))[0] not in skipped_statuses
        ]

    if limit is not None:
        candidates = candidates[:limit]

    return candidates, total_available


def fetch_one(detail_key: str, source_candidates: list[dict[str, str]], attempts: int) -> dict:
    timestamp = datetime.now().isoformat()
    last_missing_error = None
    last_failure_error = None

    for candidate in source_candidates:
        source_url = candidate["url"]
        source_kind = candidate["kind"]
        try:
            response = get_session().get(source_url, timeout=TIMEOUT)
            if response.status_code == 404:
                last_missing_error = "404"
                continue
            if response.status_code == 401:
                last_failure_error = f"401 Client Error: Unauthorized for url: {source_url}"
                continue

            response.raise_for_status()
            if source_kind == "html":
                mapped = parse_contracting_html(response.content)
            else:
                mapped = parse_contracting_xml(response.content)

            mapped["detail_source_url"] = source_url
            mapped["detail_source_kind"] = source_kind
            return {
                "xml_url": detail_key,
                "status": "done",
                "attempts": attempts + 1,
                "last_error": None,
                "updated_at": timestamp,
                "source_url": source_url,
                "source_kind": source_kind,
                "mapped_json": json.dumps(mapped, ensure_ascii=False),
                "raw_zlib": sqlite3.Binary(zlib.compress(response.content)),
            }
        except FileNotFoundError as exc:
            last_missing_error = str(exc)
        except Exception as exc:  # noqa: BLE001 - serializamos el último error útil
            last_failure_error = str(exc)

    if last_failure_error:
        return {
            "xml_url": detail_key,
            "status": "failed",
            "attempts": attempts + 1,
            "last_error": last_failure_error,
            "updated_at": timestamp,
            "source_url": None,
            "source_kind": None,
            "mapped_json": None,
            "raw_zlib": None,
        }
    return {
        "xml_url": detail_key,
        "status": "not_found",
        "attempts": attempts + 1,
        "last_error": last_missing_error or "404",
        "updated_at": timestamp,
        "source_url": None,
        "source_kind": None,
        "mapped_json": None,
        "raw_zlib": None,
    }


def iter_detail_results(
    candidates: list[dict[str, object]],
    existing: dict[str, tuple[str, int]],
    workers: int,
):
    """Mantiene un número acotado de futures en vuelo para históricos grandes."""

    inflight_limit = max(workers * IN_FLIGHT_MULTIPLIER, workers)
    candidate_iter = iter(candidates)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {}

        def submit_next() -> bool:
            try:
                candidate = next(candidate_iter)
            except StopIteration:
                return False

            detail_key = candidate["detail_key"]
            attempt = existing.get(detail_key, ("", 0))[1]
            future = executor.submit(
                fetch_one,
                detail_key,
                candidate["source_candidates"],
                attempt,
            )
            futures[future] = detail_key
            return True

        while len(futures) < inflight_limit and submit_next():
            pass

        while futures:
            done_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done_futures:
                futures.pop(future, None)
                yield future.result()

                while len(futures) < inflight_limit and submit_next():
                    pass


def run_detail(
    master_path: Path,
    cache_path: Path,
    workers: int,
    resume: bool,
    retry_not_found: bool,
    limit: int | None = None,
) -> dict[str, int]:
    ensure_cache(cache_path)
    existing = load_existing_records(cache_path)
    candidates, total_available = build_candidates(
        master_path=master_path,
        cache_path=cache_path,
        resume=resume,
        retry_not_found=retry_not_found,
        limit=limit,
    )

    if not candidates:
        log.info("No hay XMLs pendientes de detalle.")
        return {
            "available": total_available,
            "processed": 0,
            "done": 0,
            "not_found": 0,
            "failed": 0,
        }

    log.info(
        "Detalle XML Euskadi: %d candidatos de %d disponibles (workers=%d)",
        len(candidates),
        total_available,
        workers,
    )

    batch = []
    done = 0
    not_found = 0
    failed = 0

    for index, row in enumerate(
        iter_detail_results(
            candidates=candidates,
            existing=existing,
            workers=workers,
        ),
        start=1,
    ):
        batch.append(row)
        if row["status"] == "done":
            done += 1
        elif row["status"] == "not_found":
            not_found += 1
        else:
            failed += 1

        if len(batch) >= WRITE_BATCH_SIZE:
            write_cache_rows(cache_path, batch)
            batch.clear()

        if index % 100 == 0 or index == len(candidates):
            log.info(
                "  Detail: %d/%d procesados · done=%d · not_found=%d · failed=%d",
                index,
                len(candidates),
                done,
                not_found,
                failed,
            )

    if batch:
        write_cache_rows(cache_path, batch)

    return {
        "available": total_available,
        "processed": len(candidates),
        "done": done,
        "not_found": not_found,
        "failed": failed,
    }


def merge_detail(
    master_path: Path, cache_path: Path, output_path: Path
) -> dict[str, int]:
    master_df = pd.read_parquet(master_path)
    master_df["detail_key"] = [
        build_detail_key(row)
        for row in master_df[DETAIL_SOURCE_COLUMNS].to_dict(orient="records")
    ]

    if not cache_path.exists():
        raise FileNotFoundError(f"Cache SQLite no encontrada: {cache_path}")

    conn = sqlite3.connect(cache_path)
    rows = conn.execute(
        """
        SELECT xml_url, status, attempts, last_error, source_url, source_kind, mapped_json
        FROM detail_cache
        """
    ).fetchall()
    conn.close()

    detail_records = []
    for xml_url, status, attempts, last_error, source_url, source_kind, mapped_json in rows:
        record = {
            "detail_key": xml_url,
            "detail_status": status,
            "detail_attempts": attempts,
            "detail_last_error": last_error,
            "detail_source_url": source_url,
            "detail_source_kind": source_kind,
        }
        if mapped_json:
            record.update(json.loads(mapped_json))
        detail_records.append(record)

    detail_df = pd.DataFrame(detail_records)
    if not detail_df.empty:
        detail_df = detail_df.drop_duplicates(subset=["detail_key"])

    merged = master_df.merge(detail_df, on="detail_key", how="left")
    if "detail_status" not in merged.columns:
        merged["detail_status"] = None

    merged["detail_status"] = merged["detail_status"].fillna(
        merged["detail_key"].notna().map({True: "pending", False: "no_locator"})
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(output_path, index=False, engine="pyarrow")

    status_counts = merged["detail_status"].value_counts(dropna=False).to_dict()
    log.info(
        "  ✓ contratos_master_detallado: %d filas × %d cols",
        len(merged),
        len(merged.columns),
    )
    return {
        "rows": len(merged),
        "columns": len(merged.columns),
        **{f"status_{key}": int(value) for key, value in status_counts.items()},
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_runtime(log_path=args.log_path)

    if args.detail_only and args.merge_only:
        raise SystemExit("No puedes usar --detail-only y --merge-only a la vez.")

    if not args.master_path.exists():
        log.error("Master parquet no encontrado: %s", args.master_path)
        return 1

    detail_info = None
    merge_info = None

    if not args.merge_only:
        detail_info = run_detail(
            master_path=args.master_path,
            cache_path=args.cache_path,
            workers=args.workers,
            resume=args.resume,
            retry_not_found=args.retry_not_found,
            limit=args.limit,
        )

    if not args.detail_only:
        merge_info = merge_detail(
            master_path=args.master_path,
            cache_path=args.cache_path,
            output_path=args.output_path,
        )

    if detail_info is not None:
        log.info("Resumen detalle: %s", detail_info)
    if merge_info is not None:
        log.info("Resumen merge: %s", merge_info)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
