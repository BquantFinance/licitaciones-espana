"""
=============================================================================
DESCARGA DE CONTRATACION PUBLICA - COMUNIDAD DE MADRID
=============================================================================
Portal: https://contratos-publicos.comunidad.madrid
Metodo base: Buscador avanzado -> Exportar CSV
Enriquecimiento: fichas HTML publicas para no-menores y menores

Pipeline actual:
  1. Descarga base CSV por entidad / mes, igual que el flujo historico.
  2. Unificacion reproducible en CSV + Parquet base.
  3. Enriquecimiento incremental del detalle HTML en SQLite.
  4. Merge final reproducible en CSV + Parquet.

Nota importante:
  - Los tipos no-menores exponen detalle en rutas /contrato-publico/...
  - Los contratos menores exponen detalle en rutas /contrato/<Referencia>
=============================================================================
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import random
import re
import sqlite3
import threading
import time
from urllib.parse import quote
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


BASE_URL = "https://contratos-publicos.comunidad.madrid"
BUSCAR_URL = f"{BASE_URL}/contratos"
CSV_URL = f"{BASE_URL}/buscador-contratos/csv"

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR
DEFAULT_LOG_PATH = DEFAULT_OUTPUT_DIR / "descarga_comunidad_madrid.log"
BASE_CSV_NAME = "contratacion_comunidad_madrid_base.csv"
BASE_PARQUET_NAME = "contratacion_comunidad_madrid_base.parquet"
DETAIL_DB_NAME = "contratacion_comunidad_madrid_detail.sqlite3"
FINAL_CSV_NAME = "contratacion_comunidad_madrid_completo.csv"
FINAL_PARQUET_NAME = "contratacion_comunidad_madrid_completo.parquet"
DETAIL_WORKERS = 8
DETAIL_BATCH_SIZE = 100
DETAIL_MAX_ATTEMPTS = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
}

TIPOS_NO_MENORES = [
    "Convocatoria anunciada a licitación",
    "Contratos adjudicados por procedimientos sin publicidad",
    "Encargos a medios propios",
    "Anuncio de información previa",
    "Consultas preliminares del mercado",
]
TIPO_PUBLICACION_MENORES = "Contratos Menores"

DETAIL_SUPPORTED_TYPES = set(TIPOS_NO_MENORES + [TIPO_PUBLICACION_MENORES])

UMBRAL_TRUNCADO = 50000
MAX_REINTENTOS = 3
PAUSA_BASE = 5.0

RANGOS_IMPORTE = [
    ("0", "10"),
    ("10", "20"),
    ("20", "30"),
    ("30", "50"),
    ("50", "75"),
    ("75", "100"),
    ("100", "150"),
    ("150", "200"),
    ("200", "300"),
    ("300", "500"),
    ("500", "1000"),
    ("1000", "3000"),
    ("3000", "5000"),
    ("5000", "10000"),
    ("10000", "15000"),
    ("15000", "50000"),
]

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

DETAIL_FIELD_MAP = {
    "tipo de publicación": "detail_tipo_publicacion",
    "situación": "detail_situacion",
    "estado o situación": "detail_situacion",
    "número de expediente": "detail_numero_expediente",
    "numero de expediente": "detail_numero_expediente",
    "referencia": "detail_referencia",
    "código de la entidad adjudicadora (dir3)": "detail_dir3",
    "codigo de la entidad adjudicadora (dir3)": "detail_dir3",
    "código de la entidad adjudicadora": "detail_codigo_entidad_adjudicadora",
    "codigo de la entidad adjudicadora": "detail_codigo_entidad_adjudicadora",
    "entidad adjudicadora": "detail_entidad_adjudicadora",
    "objeto del contrato": "detail_objeto_contrato",
    "tipo de contrato": "detail_tipo_contrato",
    "tipo de resolución": "detail_tipo_resolucion",
    "tipo de resolucion": "detail_tipo_resolucion",
    "contrato mixto": "detail_contrato_mixto",
    "código cpv": "detail_codigo_cpv",
    "codigo cpv": "detail_codigo_cpv",
    "contrato/lote reservado": "detail_contrato_lote_reservado",
    "legislación nacional aplicable": "detail_legislacion_nacional",
    "legislacion nacional aplicable": "detail_legislacion_nacional",
    "sujeto a regulación armonizada": "detail_regulacion_armonizada",
    "sujeto a regulacion armonizada": "detail_regulacion_armonizada",
    "sistema de contratación": "detail_sistema_contratacion",
    "sistema de contratacion": "detail_sistema_contratacion",
    "código nuts": "detail_codigo_nuts",
    "codigo nuts": "detail_codigo_nuts",
    "compra pública de innovación": "detail_compra_publica_innovacion",
    "compra publica de innovacion": "detail_compra_publica_innovacion",
    "financiación de la unión europea": "detail_financiacion_ue",
    "financiacion de la union europea": "detail_financiacion_ue",
    "procedimiento de adjudicación": "detail_procedimiento_adjudicacion",
    "procedimiento de adjudicacion": "detail_procedimiento_adjudicacion",
    "tipo de tramitación": "detail_tipo_tramitacion",
    "tipo de tramitacion": "detail_tipo_tramitacion",
    "nº ofertas": "detail_num_ofertas",
    "n ofertas": "detail_num_ofertas",
    "número de ofertas": "detail_num_ofertas",
    "numero de ofertas": "detail_num_ofertas",
    "nif del adjudicatario": "detail_nif_adjudicatario",
    "nombre o razón social del adjudicatario": "detail_nombre_razon_social_adjudicatario",
    "nombre o razon social del adjudicatario": "detail_nombre_razon_social_adjudicatario",
    "método de presentación de ofertas": "detail_metodo_presentacion",
    "metodo de presentacion de ofertas": "detail_metodo_presentacion",
    "subasta electrónica": "detail_subasta_electronica",
    "subasta electronica": "detail_subasta_electronica",
    "presupuesto base licitación sin impuestos": "detail_presupuesto_base_sin_impuestos",
    "presupuesto base licitacion sin impuestos": "detail_presupuesto_base_sin_impuestos",
    "importe adjudicación sin iva": "detail_importe_adjudicacion_sin_iva",
    "importe adjudicacion sin iva": "detail_importe_adjudicacion_sin_iva",
    "importe adjudicación con iva": "detail_importe_adjudicacion_con_iva",
    "importe adjudicacion con iva": "detail_importe_adjudicacion_con_iva",
    "duración del contrato": "detail_duracion_contrato",
    "duracion del contrato": "detail_duracion_contrato",
    "fecha del contrato": "detail_fecha_contrato",
    "fecha y hora límite de presentación de ofertas o solicitudes de participación": (
        "detail_fecha_limite_presentacion"
    ),
    "fecha y hora limite de presentacion de ofertas o solicitudes de participacion": (
        "detail_fecha_limite_presentacion"
    ),
    "fecha y hora de publicación en el portal": "detail_fecha_publicacion_portal",
    "fecha y hora de publicacion en el portal": "detail_fecha_publicacion_portal",
    "fecha y hora de la última actualización": "detail_fecha_ultima_actualizacion",
    "fecha y hora de la ultima actualizacion": "detail_fecha_ultima_actualizacion",
}

DETAIL_NUMERIC_FIELDS = {
    "detail_presupuesto_base_sin_impuestos",
    "detail_importe_adjudicacion_sin_iva",
    "detail_importe_adjudicacion_con_iva",
    "detail_num_ofertas",
}

DETAIL_DATETIME_FIELDS = {
    "detail_fecha_contrato",
    "detail_fecha_limite_presentacion",
    "detail_fecha_publicacion_portal",
    "detail_fecha_ultima_actualizacion",
}

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

_thread_local = threading.local()


class ScraperError(RuntimeError):
    """Error controlado del scraper."""


@dataclass
class OutputPaths:
    output_dir: Path
    csv_dir: Path
    log_path: Path
    base_csv: Path
    base_parquet: Path
    detail_db: Path
    final_csv: Path
    final_parquet: Path


def build_paths(output_dir: Path, log_path: Optional[Path] = None) -> OutputPaths:
    output_dir = Path(output_dir).resolve()
    return OutputPaths(
        output_dir=output_dir,
        csv_dir=output_dir / "csv_originales",
        log_path=Path(log_path).resolve()
        if log_path
        else output_dir / DEFAULT_LOG_PATH.name,
        base_csv=output_dir / BASE_CSV_NAME,
        base_parquet=output_dir / BASE_PARQUET_NAME,
        detail_db=output_dir / DETAIL_DB_NAME,
        final_csv=output_dir / FINAL_CSV_NAME,
        final_parquet=output_dir / FINAL_PARQUET_NAME,
    )


def ensure_dirs(paths: OutputPaths) -> None:
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    paths.csv_dir.mkdir(parents=True, exist_ok=True)


def build_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger(f"comunidad_madrid.{log_path}")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(LOG_FORMAT)
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream)
    logger.addHandler(file_handler)
    return logger


def normalize_ws(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def slugify(value: str, max_length: int = 40) -> str:
    text = normalize_ws(value).lower().strip(" -")
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")[:max_length]


def strip_html(value: object) -> str:
    if value is None:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    return normalize_ws(text)


def normalize_label(value: str) -> str:
    text = normalize_ws(value).rstrip(":")
    replacements = str.maketrans(
        {
            "Á": "A",
            "É": "E",
            "Í": "I",
            "Ó": "O",
            "Ú": "U",
            "á": "a",
            "é": "e",
            "í": "i",
            "ó": "o",
            "ú": "u",
            "Ñ": "N",
            "ñ": "n",
        }
    )
    text = text.translate(replacements)
    text = re.sub(r"[()/_.,;]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_money(value: object) -> Optional[float]:
    text = normalize_ws(value)
    if not text:
        return None
    match = re.search(r"(-?\d[\d\.\s]*,\d+|-?\d[\d\.\s]*)", text.replace("\xa0", " "))
    if not match:
        return None
    number = match.group(1).replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return float(number)
    except ValueError:
        return None


def parse_portal_datetime(value: object) -> Optional[str]:
    text = normalize_ws(value)
    if not text:
        return None

    match = re.search(
        r"(\d{1,2})\s+de\s+([A-Za-záéíóúñ]+)\s+del?\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?",
        text,
        re.IGNORECASE,
    )
    if match:
        day = int(match.group(1))
        month_name = normalize_label(match.group(2))
        month = SPANISH_MONTHS.get(month_name)
        year = int(match.group(3))
        hour = int(match.group(4) or 0)
        minute = int(match.group(5) or 0)
        if month:
            return datetime(year, month, day, hour, minute).isoformat()

    match = re.search(r"(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}))?", text)
    if match:
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        hour = int(match.group(4) or 0)
        minute = int(match.group(5) or 0)
        return datetime(year, month, day, hour, minute).isoformat()

    return None


def resolver_captcha(text: str) -> Optional[int]:
    match = re.search(r"(\d+)\s*([+\-*/])\s*(\d+)\s*=", text)
    if not match:
        return None
    left = int(match.group(1))
    operator = match.group(2)
    right = int(match.group(3))
    ops = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a // b if b else None,
    }
    func = ops.get(operator)
    return func(left, right) if func else None


def transformar_antibot_key(key: str) -> str:
    result = ""
    for i in range(len(key) - 1, -1, -2):
        if i - 1 >= 0:
            result += key[i - 1] + key[i]
        else:
            result += key[i]
    return result


def nombre_csv_entidad(entidad_idx: int, entidad_nombre: str) -> str:
    return f"menores_ent{entidad_idx:03d}_{slugify(entidad_nombre, 40)}.csv"


def nombre_csv_entidad_rango(
    entidad_idx: int,
    entidad_nombre: str,
    importe_desde: str,
    importe_hasta: str,
) -> str:
    return f"menores_ent{entidad_idx:03d}_{slugify(entidad_nombre, 30)}_imp{importe_desde}-{importe_hasta}.csv"


def nombre_csv_mes(anio: int, mes: int, tipo_pub: str) -> str:
    return f"{anio}_{mes:02d}_{slugify(tipo_pub, 25)}.csv"


def generar_segmentos_mensuales(
    anio_inicio: int, anio_fin: int
) -> List[Tuple[str, str, int, int]]:
    hoy = datetime.now()
    segmentos = []
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if anio == hoy.year and mes > hoy.month:
                break
            _, ultimo_dia = monthrange(anio, mes)
            segmentos.append(
                (
                    f"01-{mes:02d}-{anio}",
                    f"{ultimo_dia:02d}-{mes:02d}-{anio}",
                    anio,
                    mes,
                )
            )
    return segmentos


def build_record_key(record: Dict[str, object]) -> str:
    # La clave mezcla identidad funcional del expediente y metadatos visibles del portal.
    # Así evitamos depender del nombre del CSV o del orden de descarga.
    parts = [
        normalize_ws(record.get("Tipo de Publicación")),
        normalize_ws(record.get("Entidad Adjudicadora")),
        normalize_ws(record.get("Nº Expediente")),
        normalize_ws(record.get("Referencia")),
        normalize_ws(record.get("Título del contrato")),
    ]
    return hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()


def _thread_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update(HEADERS)
        _thread_local.session = session
    return _thread_local.session


class ComunidadMadridScraper:
    def __init__(
        self,
        paths: OutputPaths,
        logger: logging.Logger,
        pausa_base: float = PAUSA_BASE,
        max_reintentos: int = MAX_REINTENTOS,
    ) -> None:
        self.paths = paths
        self.log = logger
        self.pausa_base = pausa_base
        self.max_reintentos = max_reintentos
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.antibot_key: Optional[str] = None
        self.entidades: List[Tuple[str, str]] = []
        self.stats = {
            "ok": 0,
            "error": 0,
            "skip_existe": 0,
            "skip_vacio": 0,
            "filas": 0,
            "bytes": 0,
            "archivos": [],
        }
        self.t_inicio: Optional[float] = None

    def _obtener_antibot_key(self, forzar: bool = False) -> str:
        if self.antibot_key and not forzar:
            return self.antibot_key

        self.log.info("  [antibot] Obteniendo key...")
        resp = self.session.get(BUSCAR_URL, timeout=60)
        resp.raise_for_status()

        match = re.search(
            r'<script[^>]*data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
            resp.text,
            re.DOTALL,
        )
        if match:
            try:
                settings = json.loads(match.group(1))
                forms = settings.get("antibot", {}).get("forms", {})
                for form_data in forms.values():
                    if "key" in form_data:
                        self.antibot_key = transformar_antibot_key(form_data["key"])
                        self.log.info("  [antibot] OK: %s...", self.antibot_key[:20])
                        break
            except (json.JSONDecodeError, KeyError):
                pass

        if not self.antibot_key:
            match = re.search(r'"key"\s*:\s*"([A-Za-z0-9_\-]+)"', resp.text)
            if match:
                self.antibot_key = transformar_antibot_key(match.group(1))
                self.log.info("  [antibot] OK (fallback): %s...", self.antibot_key[:20])

        if not self.antibot_key:
            raise ScraperError("No se encontro la antibot_key del portal")

        if not self.entidades:
            soup = BeautifulSoup(resp.text, "html.parser")
            select = soup.find("select", {"name": "entidad_adjudicadora"})
            if select:
                for option in select.find_all("option"):
                    value = option.get("value", "")
                    text = option.get_text(strip=True)
                    if value and value != "All":
                        self.entidades.append((value, text))
                self.log.info("  [entidades] %s encontradas", len(self.entidades))

        return self.antibot_key

    def _reset_sesion(self) -> None:
        self.session.cookies.clear()
        self.antibot_key = None

    def _buscar(
        self,
        fecha_desde: str = "",
        fecha_hasta: str = "",
        tipo_pub: Optional[str] = None,
        entidad: Optional[str] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> str:
        antibot_key = self._obtener_antibot_key()
        params = {
            "t": "",
            "tipo_publicacion": "All",
            "createddate": fecha_desde,
            "createddate_1": fecha_hasta,
            "fin_presentacion": "",
            "fin_presentacion_1": "",
            "ss_buscador_estado_situacion": "All",
            "numero_expediente": "",
            "referencia": "",
            "ss_identificador_ted": "",
            "entidad_adjudicadora": entidad or "All",
            "tipo_contrato": "All",
            "codigo_cpv": "",
            "ss_field_contrato_lote_reservado": "All",
            "bs_regulacion_armonizada": "All",
            "ss_sist_de_contratacion": "All",
            "modalidad_compra_publica": "All",
            "ss_financiacion_ue": "All",
            "ss_field_pcon_codigo_referencia": "",
            "procedimiento_adjudicacion": "All",
            "ss_tipo_de_tramitacion": "All",
            "ss_metodo_presentacion": "All",
            "bs_subasta_electronica": "All",
            "presupuesto_base_licitacion_total": "",
            "presupuesto_base_licitacion_total_1": "",
            "ds_field_pcon_fecha_desierto": "",
            "ds_field_pcon_fecha_desierto_1": "",
            "nif_adjudicatario": "",
            "nombre_adjudicatario": "",
            "importacion_adjudicacion_con_impuestos": "",
            "importacion_adjudicacion_con_impuestos_1": "",
            "ds_fecha_encargo": "",
            "ds_fecha_encargo_1": "",
            "ds_field_pcon_fecha_publi_anun_form": "",
            "ds_field_pcon_fecha_publi_anun_form_1": "",
            "antibot_key": antibot_key,
        }
        if tipo_pub:
            params["f[0]"] = f"tipo_publicacion:{tipo_pub}"
        if extra_params:
            params.update(extra_params)

        resp = self.session.get(BUSCAR_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _obtener_form_captcha(self) -> Dict[str, object]:
        resp = self.session.get(CSV_URL, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find(
            "form", {"id": "pcon-contratos-menores-export-results-form"}
        ) or soup.find("form", action="/buscador-contratos/csv")
        if not form:
            raise ScraperError("No se encontro el formulario de exportacion CSV")

        data = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                data[name] = inp.get("value", "")

        captcha = resolver_captcha(str(form))
        if captcha is None:
            raise ScraperError("No se pudo resolver el captcha matematico")
        data["captcha_response"] = str(captcha)

        action = form.get("action", "/buscador-contratos/csv")
        if action.startswith("/"):
            action = BASE_URL + action
        return {"action": action, "data": data}

    def _post_completion(self, form: BeautifulSoup) -> Optional[bytes]:
        data = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if name:
                data[name] = inp.get("value", "")

        action = form.get("action", "/buscador-contratos/csv/completion")
        if action.startswith("/"):
            action = BASE_URL + action

        resp = self.session.post(action, data=data, timeout=300)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        disposition = resp.headers.get("Content-Disposition", "")
        if "csv" in content_type or "octet-stream" in content_type or disposition:
            return resp.content

        debug_path = self.paths.output_dir / "debug_completion.html"
        debug_path.write_text(resp.text[:10000], encoding="utf-8")
        return None

    def _post_captcha(self, form_info: Dict[str, object]) -> Optional[bytes]:
        resp = self.session.post(
            form_info["action"], data=form_info["data"], timeout=120
        )
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        disposition = resp.headers.get("Content-Disposition", "")

        if "csv" in content_type or "octet-stream" in content_type or disposition:
            return resp.content

        if "text/html" in content_type:
            soup = BeautifulSoup(resp.text, "html.parser")
            form = soup.find(
                "form", {"id": "pcon-contratos-menores-export-results-completion-form"}
            ) or soup.find("form", action=re.compile(r"(completion|execute)"))
            if form:
                return self._post_completion(form)

            debug_path = self.paths.output_dir / "debug_post_captcha.html"
            debug_path.write_text(resp.text[:10000], encoding="utf-8")
        return None

    def _descargar_csv(
        self,
        fecha_desde: str = "",
        fecha_hasta: str = "",
        tipo_pub: Optional[str] = None,
        entidad: Optional[str] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> Optional[bytes]:
        self._buscar(fecha_desde, fecha_hasta, tipo_pub, entidad, extra_params)
        time.sleep(self.pausa_base)
        form_info = self._obtener_form_captcha()
        time.sleep(1)
        return self._post_captcha(form_info)

    def _guardar(self, csv_data: bytes, filepath: Path) -> int:
        filepath.write_bytes(csv_data)
        n_filas = csv_data.count(b"\n") - 1
        size_mb = len(csv_data) / (1024 * 1024)
        self.log.info(
            "  ✓ %s (%s filas, %.1f MB)", filepath.name, f"{n_filas:,}", size_mb
        )
        self.stats["ok"] += 1
        self.stats["filas"] += max(n_filas, 0)
        self.stats["bytes"] += len(csv_data)
        self.stats["archivos"].append(str(filepath))
        return n_filas

    def _descargar_con_reintentos(
        self,
        filepath: Path,
        label: str,
        fecha_desde: str = "",
        fecha_hasta: str = "",
        tipo_pub: Optional[str] = None,
        entidad: Optional[str] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, int]:
        if filepath.exists() and filepath.stat().st_size > 100:
            self.log.info("    Ya existe: %s, skip", filepath.name)
            self.stats["skip_existe"] += 1
            return True, 0

        for intento in range(self.max_reintentos):
            try:
                csv_data = self._descargar_csv(
                    fecha_desde=fecha_desde,
                    fecha_hasta=fecha_hasta,
                    tipo_pub=tipo_pub,
                    entidad=entidad,
                    extra_params=extra_params,
                )
                if csv_data:
                    n_filas = csv_data.count(b"\n") - 1
                    if n_filas <= 0:
                        self.log.info("    0 filas, skip")
                        self.stats["skip_vacio"] += 1
                        return True, 0
                    if n_filas >= UMBRAL_TRUNCADO:
                        self.log.warning(
                            "    ⚠ %s filas — posible truncamiento para: %s",
                            f"{n_filas:,}",
                            label,
                        )
                    self._guardar(csv_data, filepath)
                    return True, n_filas

                self.log.warning(
                    "    Intento %s/%s sin CSV", intento + 1, self.max_reintentos
                )
            except Exception as exc:  # pragma: no cover - validado en smokes reales
                self.log.error("    Error intento %s: %s", intento + 1, exc)

            time.sleep(5 * (intento + 1))
            self._reset_sesion()

        self.stats["error"] += 1
        return False, 0

    def descargar_menores(self) -> None:
        self._obtener_antibot_key()
        if not self.entidades:
            raise ScraperError("No se pudieron obtener las entidades adjudicadoras")

        total = len(self.entidades)
        self.log.info("%s", "=" * 65)
        self.log.info("CONTRATOS MENORES — Por entidad adjudicadora")
        self.log.info("  Entidades: %s", total)
        self.log.info("  Subdivision automatica por rango de importe si >50K")
        self.log.info("%s", "=" * 65)

        for index, (value, nombre) in enumerate(self.entidades, start=1):
            filepath = self.paths.csv_dir / nombre_csv_entidad(int(value), nombre)
            self.log.info("\n  [%s/%s] %s", index, total, nombre[:50])
            slug = slugify(nombre, 30)
            if any(
                path.name.startswith(f"menores_ent{int(value):03d}_{slug}_imp")
                for path in self.paths.csv_dir.glob(
                    f"menores_ent{int(value):03d}_*_imp*.csv"
                )
            ):
                self.log.info("    Ya subdividido por importe, skip")
                self.stats["skip_existe"] += 1
                continue

            ok, n_filas = self._descargar_con_reintentos(
                filepath,
                nombre[:50],
                tipo_pub="Contratos Menores",
                entidad=value,
            )
            if ok and n_filas >= UMBRAL_TRUNCADO:
                self.log.info("    → Subdividiendo recursivamente por importe...")
                self._revert_file_stats(filepath, n_filas)
                self._descargar_menores_por_importe(value, nombre)
            time.sleep(self.pausa_base)

    def _revert_file_stats(self, filepath: Path, n_filas: int) -> None:
        if not filepath.exists():
            return
        size = filepath.stat().st_size
        filepath.unlink()
        self.stats["ok"] -= 1
        self.stats["filas"] -= n_filas
        self.stats["bytes"] -= size
        if self.stats["archivos"]:
            try:
                self.stats["archivos"].remove(str(filepath))
            except ValueError:
                pass

    def _descargar_menores_por_importe(
        self,
        entidad_val: str,
        entidad_nombre: str,
        rangos: Optional[Sequence[Tuple[str, str]]] = None,
        depth: int = 0,
    ) -> None:
        rangos = list(rangos or RANGOS_IMPORTE)
        if depth > 5:
            self.log.error(
                "      ⚠ Profundidad maxima alcanzada, abortando subdivision"
            )
            return

        indent = "      " + "  " * depth
        for desde, hasta in rangos:
            filepath = self.paths.csv_dir / nombre_csv_entidad_rango(
                int(entidad_val), entidad_nombre, desde, hasta
            )
            self.log.info("%s→ Importe %s-%s€", indent, desde, hasta)
            ok, n_filas = self._descargar_con_reintentos(
                filepath,
                f"{entidad_nombre[:30]} imp {desde}-{hasta}",
                tipo_pub="Contratos Menores",
                entidad=entidad_val,
                extra_params={
                    "presupuesto_base_licitacion_total": desde,
                    "presupuesto_base_licitacion_total_1": hasta,
                },
            )
            if ok and n_filas >= UMBRAL_TRUNCADO:
                low = int(desde)
                high = int(hasta)
                mid = (low + high) // 2
                if mid <= low or mid >= high:
                    self.log.warning(
                        "%s  ⚠ Rango %s-%s no se puede subdividir mas (%s filas)",
                        indent,
                        desde,
                        hasta,
                        f"{n_filas:,}",
                    )
                    continue
                self.log.info(
                    "%s  → Re-subdividiendo %s-%s en %s-%s y %s-%s",
                    indent,
                    desde,
                    hasta,
                    low,
                    mid,
                    mid,
                    high,
                )
                self._revert_file_stats(filepath, n_filas)
                self._descargar_menores_por_importe(
                    entidad_val,
                    entidad_nombre,
                    [(str(low), str(mid)), (str(mid), str(high))],
                    depth + 1,
                )
            time.sleep(self.pausa_base)

    def descargar_otros(
        self, anio_inicio: int = 2017, anio_fin: int = datetime.now().year
    ) -> None:
        segmentos = generar_segmentos_mensuales(anio_inicio, anio_fin)
        total_meses = len(segmentos)
        total_descargas = total_meses * len(TIPOS_NO_MENORES)

        self.log.info("%s", "=" * 65)
        self.log.info("OTROS TIPOS — Por mes + tipo publicacion")
        self.log.info("  Periodo: %s-%s (%s meses)", anio_inicio, anio_fin, total_meses)
        self.log.info("  Tipos: %s", len(TIPOS_NO_MENORES))
        self.log.info("  Descargas estimadas: ~%s", total_descargas)
        self.log.info("%s", "=" * 65)

        for index, (desde, hasta, anio, mes) in enumerate(segmentos, start=1):
            self.log.info("\n  [%s/%s] %02d/%s", index, total_meses, mes, anio)
            for tipo_pub in TIPOS_NO_MENORES:
                filepath = self.paths.csv_dir / nombre_csv_mes(anio, mes, tipo_pub)
                self.log.info("    → %s", tipo_pub[:45])
                self._descargar_con_reintentos(
                    filepath,
                    f"{mes:02d}/{anio} {tipo_pub[:30]}",
                    fecha_desde=desde,
                    fecha_hasta=hasta,
                    tipo_pub=tipo_pub,
                )
                time.sleep(self.pausa_base)

    def descargar_prueba(self) -> None:
        self.t_inicio = time.time()
        self._obtener_antibot_key()
        self.log.info("%s", "=" * 65)
        self.log.info("PRUEBA — Subdivision recursiva por importe")
        self.log.info("%s", "=" * 65)

        value, nombre = "38", "Hospital General Universitario Gregorio Maranon"
        for current_value, current_name in self.entidades:
            if current_value == value:
                nombre = current_name
                break

        filepath = self.paths.csv_dir / nombre_csv_entidad(int(value), nombre)
        self.log.info("\n  [MENORES] %s", nombre[:50])
        ok, n_filas = self._descargar_con_reintentos(
            filepath,
            nombre[:50],
            tipo_pub="Contratos Menores",
            entidad=value,
        )
        if ok and n_filas >= UMBRAL_TRUNCADO:
            self.log.info("    → Subdividiendo recursivamente por importe...")
            self._revert_file_stats(filepath, n_filas)
            self._descargar_menores_por_importe(value, nombre)
        self._resumen()

    def descargar_todo(
        self, anio_inicio: int = 2017, anio_fin: int = datetime.now().year
    ) -> None:
        self.t_inicio = time.time()
        self.log.info("%s", "=" * 65)
        self.log.info("DESCARGA CONTRATACION PUBLICA - COMUNIDAD DE MADRID")
        self.log.info("  Portal: %s", BASE_URL)
        self.log.info("  Directorio: %s", self.paths.csv_dir)
        self.log.info("%s", "=" * 65)
        self.descargar_menores()
        self.descargar_otros(anio_inicio, anio_fin)
        self._resumen()

    def _resumen(self) -> None:
        elapsed = time.time() - self.t_inicio if self.t_inicio else 0
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        size_mb = self.stats["bytes"] / (1024 * 1024)
        self.log.info("\n%s", "=" * 65)
        self.log.info("RESUMEN")
        self.log.info("  Descargas OK:    %s", self.stats["ok"])
        self.log.info("  Errores:         %s", self.stats["error"])
        self.log.info("  Ya existian:     %s", self.stats["skip_existe"])
        self.log.info("  Vacios:          %s", self.stats["skip_vacio"])
        self.log.info("  Filas totales:   %s", f"{self.stats['filas']:,}")
        self.log.info("  Tamano total:    %.1f MB", size_mb)
        self.log.info("  Archivos:        %s", len(self.stats["archivos"]))
        self.log.info("  Tiempo:          %sm %ss", mins, secs)
        self.log.info("%s", "=" * 65)


def read_csv_with_fallbacks(
    filepath: Path, skiprows: int = 0, header: object = "infer"
) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(filepath, "r", encoding=encoding) as handle:
                lines = handle.readlines()
            idx = skiprows if len(lines) > skiprows else 0
            first_line = lines[idx] if lines else ""
            separator = ";" if first_line.count(";") >= first_line.count(",") else ","
            df = pd.read_csv(
                filepath,
                sep=separator,
                encoding=encoding,
                dtype=str,
                on_bad_lines="skip",
                skiprows=skiprows,
                header=header,
                quotechar='"',
            )
            if len(df) > 0 and len(df.columns) > 2:
                if header == "infer":
                    df.columns = [str(col).strip() for col in df.columns]
                return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return pd.read_csv(
        filepath,
        sep=";",
        encoding="latin-1",
        dtype=str,
        on_bad_lines="skip",
        skiprows=skiprows,
        header=header,
    )


def save_base_outputs(
    output_dir: Path, logger: logging.Logger
) -> Tuple[pd.DataFrame, Path, Optional[Path]]:
    paths = build_paths(output_dir)
    csv_paths = sorted(paths.csv_dir.glob("*.csv"))
    if not csv_paths:
        raise ScraperError("No hay CSVs descargados para unificar")

    dataframes = []
    for csv_path in csv_paths:
        try:
            df = read_csv_with_fallbacks(csv_path)
            if len(df) > 0:
                df["_archivo_fuente"] = csv_path.name
                dataframes.append(df)
                logger.info("  %s: %s filas", csv_path.name, f"{len(df):,}")
        except Exception as exc:
            logger.error("  Error leyendo %s: %s", csv_path.name, exc)

    if not dataframes:
        raise ScraperError("No se pudo cargar ningun CSV base")

    base_df = pd.concat(dataframes, ignore_index=True)
    original_cols = [col for col in base_df.columns if col != "_archivo_fuente"]
    before = len(base_df)
    base_df = base_df.drop_duplicates(subset=original_cols).reset_index(drop=True)
    removed = before - len(base_df)
    if removed:
        logger.info("  Eliminados %s duplicados exactos", f"{removed:,}")

    base_df.to_csv(paths.base_csv, index=False, sep=";", encoding="utf-8-sig")
    parquet_path: Optional[Path] = None
    if HAS_PYARROW:
        base_df.to_parquet(paths.base_parquet, index=False)
        parquet_path = paths.base_parquet

    logger.info("✓ Base CSV: %s (%s filas)", paths.base_csv, f"{len(base_df):,}")
    if parquet_path:
        logger.info("✓ Base Parquet: %s", parquet_path)
    return base_df, paths.base_csv, parquet_path


def init_detail_db(output_dir: Path) -> sqlite3.Connection:
    paths = build_paths(output_dir)
    ensure_dirs(paths)
    conn = sqlite3.connect(paths.detail_db)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS detail_cache (
            record_key TEXT PRIMARY KEY,
            tipo_publicacion TEXT,
            referencia TEXT,
            numero_expediente TEXT,
            entidad_adjudicadora TEXT,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_http_status INTEGER,
            detail_url TEXT,
            page_title TEXT,
            updated_at TEXT NOT NULL,
            search_summary_json TEXT,
            mapped_json TEXT
        )
        """
    )
    conn.commit()
    return conn


def query_detail_rows(
    conn: sqlite3.Connection,
    record_keys: Sequence[str],
    chunk_size: int = 900,
) -> Dict[str, Dict[str, object]]:
    rows: Dict[str, Dict[str, object]] = {}
    if not record_keys:
        return rows
    for start in range(0, len(record_keys), chunk_size):
        chunk = record_keys[start : start + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        cursor = conn.execute(
            f"""
            SELECT record_key, tipo_publicacion, referencia, numero_expediente,
                   entidad_adjudicadora, status, attempts, last_error,
                   last_http_status, detail_url, page_title, updated_at,
                   search_summary_json, mapped_json
            FROM detail_cache
            WHERE record_key IN ({placeholders})
            """,
            tuple(chunk),
        )
        for row in cursor.fetchall():
            rows[row[0]] = {
                "record_key": row[0],
                "tipo_publicacion": row[1],
                "referencia": row[2],
                "numero_expediente": row[3],
                "entidad_adjudicadora": row[4],
                "status": row[5],
                "attempts": row[6],
                "last_error": row[7],
                "last_http_status": row[8],
                "detail_url": row[9],
                "page_title": row[10],
                "updated_at": row[11],
                "search_summary_json": row[12],
                "mapped_json": row[13],
            }
    return rows


def persist_detail_results(
    conn: sqlite3.Connection, results: Sequence[Dict[str, object]]
) -> None:
    if not results:
        return
    conn.executemany(
        """
        INSERT INTO detail_cache (
            record_key, tipo_publicacion, referencia, numero_expediente,
            entidad_adjudicadora, status, attempts, last_error, last_http_status,
            detail_url, page_title, updated_at, search_summary_json, mapped_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(record_key) DO UPDATE SET
            tipo_publicacion=excluded.tipo_publicacion,
            referencia=excluded.referencia,
            numero_expediente=excluded.numero_expediente,
            entidad_adjudicadora=excluded.entidad_adjudicadora,
            status=excluded.status,
            attempts=excluded.attempts,
            last_error=excluded.last_error,
            last_http_status=excluded.last_http_status,
            detail_url=excluded.detail_url,
            page_title=excluded.page_title,
            updated_at=excluded.updated_at,
            search_summary_json=excluded.search_summary_json,
            mapped_json=excluded.mapped_json
        """,
        [
            (
                result["record_key"],
                result["tipo_publicacion"],
                result["referencia"],
                result["numero_expediente"],
                result["entidad_adjudicadora"],
                result["status"],
                result["attempts"],
                result.get("last_error"),
                result.get("last_http_status"),
                result.get("detail_url"),
                result.get("page_title"),
                result["updated_at"],
                result.get("search_summary_json"),
                result.get("mapped_json"),
            )
            for result in results
        ],
    )
    conn.commit()


def parse_search_result_html(html: str) -> Optional[Dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    item = soup.select_one("div.contratos-result ul li")
    if not item:
        return None

    detail_anchor = item.find("a", href=re.compile(r"^/(contrato-publico|contrato)/"))
    title = (
        normalize_ws(detail_anchor.get_text(" ", strip=True)) if detail_anchor else None
    )

    summary = {}
    for field in item.select(".views-field"):
        label = field.select_one(".views-label")
        value = field.select_one(".field-content")
        if label and value:
            summary[normalize_label(label.get_text(" ", strip=True))] = normalize_ws(
                value.get_text(" ", strip=True)
            )

    if not title:
        text_nodes = [normalize_ws(text) for text in item.stripped_strings]
        if text_nodes:
            title = text_nodes[0]

    return {
        "title": title,
        "detail_url": BASE_URL + detail_anchor["href"] if detail_anchor else None,
        "summary": summary,
    }


def build_minor_detail_url(reference: object) -> Optional[str]:
    reference_text = normalize_ws(reference)
    if not reference_text:
        return None
    return f"{BASE_URL}/contrato/{quote(reference_text, safe='')}"


def parse_detail_html(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    page_title = soup.title.get_text(" ", strip=True) if soup.title else None
    mapped: Dict[str, object] = {}

    # No-menores y menores usan plantillas distintas, pero ambas repiten
    # la pareja field__label / field__item dentro de .node__content.
    for label in soup.select(".node__content .field__label"):
        parent = label.parent
        if not parent:
            continue
        raw_label = normalize_ws(label.get_text(" ", strip=True))
        items = parent.select(".field__item")
        if not items:
            sibling = label.find_next_sibling(class_="field__item")
            items = [sibling] if sibling else []
        if not items:
            continue
        key = DETAIL_FIELD_MAP.get(normalize_label(raw_label))
        raw_value = " | ".join(
            normalize_ws(item.get_text(" ", strip=True)) for item in items
        )
        if not key:
            continue
        value: object = raw_value
        if key in DETAIL_NUMERIC_FIELDS:
            value = parse_money(raw_value)
        elif key in DETAIL_DATETIME_FIELDS:
            value = parse_portal_datetime(raw_value) or raw_value
        mapped[key] = value

    document_links = soup.select("#pcon-pliego-de-condiciones .archivos a[href]")
    if document_links:
        mapped["detail_documentos_count"] = len(document_links)

    return {"page_title": page_title, "mapped": mapped}


def make_detail_result(
    row: Dict[str, object],
    attempts: int,
    status: str,
    last_error: Optional[str] = None,
    last_http_status: Optional[int] = None,
    detail_url: Optional[str] = None,
    page_title: Optional[str] = None,
    search_summary: Optional[Dict[str, object]] = None,
    mapped: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    return {
        "record_key": row["record_key"],
        "tipo_publicacion": normalize_ws(row.get("Tipo de Publicación")),
        "referencia": normalize_ws(row.get("Referencia")),
        "numero_expediente": normalize_ws(row.get("Nº Expediente")),
        "entidad_adjudicadora": normalize_ws(row.get("Entidad Adjudicadora")),
        "status": status,
        "attempts": attempts,
        "last_error": last_error,
        "last_http_status": last_http_status,
        "detail_url": detail_url,
        "page_title": page_title,
        "updated_at": datetime.utcnow().replace(microsecond=0).isoformat(),
        "search_summary_json": json.dumps(search_summary or {}, ensure_ascii=False)
        if search_summary
        else None,
        "mapped_json": json.dumps(mapped or {}, ensure_ascii=False) if mapped else None,
    }


def fetch_detail_for_row(
    row: Dict[str, object],
    attempts_done: int = 0,
    delay: float = 0.2,
    jitter: float = 0.1,
) -> Dict[str, object]:
    session = _thread_session()
    time.sleep(max(0.0, delay + random.uniform(0.0, jitter)))

    tipo_publicacion = normalize_ws(row.get("Tipo de Publicación"))
    search_params_base: Dict[str, str] = {}
    if tipo_publicacion and tipo_publicacion in DETAIL_SUPPORTED_TYPES:
        search_params_base["f[0]"] = f"tipo_publicacion:{tipo_publicacion}"

    referencia = normalize_ws(row.get("Referencia"))
    numero_expediente = normalize_ws(row.get("Nº Expediente"))
    search_candidates = []
    if referencia:
        search_candidates.append(("referencia", referencia))
    if numero_expediente and numero_expediente != referencia:
        search_candidates.append(("numero_expediente", numero_expediente))

    direct_detail_url = None
    # En menores la ficha pública estable cuelga de /contrato/<Referencia>.
    # Se usa como apoyo por si el listado del buscador no trae el href esperado.
    if tipo_publicacion == TIPO_PUBLICACION_MENORES and referencia:
        direct_detail_url = build_minor_detail_url(referencia)

    for field, value in search_candidates:
        try:
            params = dict(search_params_base)
            params[field] = value
            resp = session.get(BUSCAR_URL, params=params, timeout=60)
            resp.raise_for_status()
            parsed_search = parse_search_result_html(resp.text)
            if not parsed_search:
                continue
            detail_url = parsed_search.get("detail_url") or direct_detail_url
            if not detail_url:
                continue

            time.sleep(max(0.0, delay + random.uniform(0.0, jitter)))
            detail_resp = session.get(detail_url, timeout=60)
            detail_resp.raise_for_status()
            parsed_detail = parse_detail_html(detail_resp.text)
            return make_detail_result(
                row,
                attempts_done + 1,
                "done",
                detail_url=detail_url,
                page_title=parsed_detail.get("page_title")
                or parsed_search.get("title"),
                search_summary=parsed_search["summary"],
                mapped=parsed_detail["mapped"],
            )
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            return make_detail_result(
                row,
                attempts_done + 1,
                "retryable",
                last_error=str(exc),
                last_http_status=status_code,
            )
        except Exception as exc:  # pragma: no cover - protegido con tests unitarios
            return make_detail_result(
                row,
                attempts_done + 1,
                "retryable",
                last_error=str(exc),
            )

    if direct_detail_url:
        try:
            time.sleep(max(0.0, delay + random.uniform(0.0, jitter)))
            detail_resp = session.get(direct_detail_url, timeout=60)
            detail_resp.raise_for_status()
            parsed_detail = parse_detail_html(detail_resp.text)
            return make_detail_result(
                row,
                attempts_done + 1,
                "done",
                detail_url=direct_detail_url,
                page_title=parsed_detail.get("page_title"),
                mapped=parsed_detail["mapped"],
            )
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            return make_detail_result(
                row,
                attempts_done + 1,
                "retryable",
                last_error=str(exc),
                last_http_status=status_code,
            )
        except Exception as exc:  # pragma: no cover - protegido con tests unitarios
            return make_detail_result(
                row,
                attempts_done + 1,
                "retryable",
                last_error=str(exc),
            )

    return make_detail_result(
        row,
        attempts_done + 1,
        "not_found",
        last_error="Sin resultados en el buscador para referencia/expediente",
    )


def iter_detail_batches(
    base_csv_path: Path,
    conn: sqlite3.Connection,
    batch_size: int = DETAIL_BATCH_SIZE,
    force: bool = False,
    include_menores: bool = True,
    retryable_only: bool = False,
    retryable_ignore_max_attempts: bool = False,
) -> Iterator[List[Dict[str, object]]]:
    base_df = pd.read_csv(base_csv_path, sep=";", dtype=str, keep_default_na=False)
    records = []
    for record in base_df.to_dict(orient="records"):
        tipo = normalize_ws(record.get("Tipo de Publicación"))
        if not include_menores and tipo == "Contratos Menores":
            continue
        if tipo and tipo not in DETAIL_SUPPORTED_TYPES and not include_menores:
            continue
        record["record_key"] = build_record_key(record)
        records.append(record)

    cached = query_detail_rows(conn, [record["record_key"] for record in records])
    pending = []
    for record in records:
        row = cached.get(record["record_key"])
        if force:
            pending.append(record)
            continue
        if retryable_only:
            if not row or row["status"] != "retryable":
                continue
            if (
                row["attempts"] >= DETAIL_MAX_ATTEMPTS
                and not retryable_ignore_max_attempts
            ):
                continue
            record["_attempts_done"] = row["attempts"]
            pending.append(record)
            continue
        if row and row["status"] == "done":
            continue
        if row:
            record["_attempts_done"] = row["attempts"]
        pending.append(record)

    for start in range(0, len(pending), batch_size):
        yield pending[start : start + batch_size]


def run_detail(
    output_dir: Path,
    logger: logging.Logger,
    detail_workers: int = DETAIL_WORKERS,
    detail_batch_size: int = DETAIL_BATCH_SIZE,
    detail_delay: float = 0.2,
    detail_jitter: float = 0.1,
    force: bool = False,
    include_menores: bool = True,
    retryable_only: bool = False,
    retryable_ignore_max_attempts: bool = False,
) -> sqlite3.Connection:
    paths = build_paths(output_dir)
    if not paths.base_csv.exists():
        raise ScraperError("No existe el CSV base. Ejecuta primero 'unificar'.")

    conn = init_detail_db(output_dir)
    total_done = total_retryable = total_not_found = 0

    for batch in iter_detail_batches(
        paths.base_csv,
        conn,
        batch_size=detail_batch_size,
        force=force,
        include_menores=include_menores,
        retryable_only=retryable_only,
        retryable_ignore_max_attempts=retryable_ignore_max_attempts,
    ):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=detail_workers
        ) as executor:
            futures = [
                executor.submit(
                    fetch_detail_for_row,
                    row,
                    attempts_done=int(row.get("_attempts_done", 0)),
                    delay=detail_delay,
                    jitter=detail_jitter,
                )
                for row in batch
            ]
            results = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]

        persist_detail_results(conn, results)
        total_done += sum(1 for result in results if result["status"] == "done")
        total_retryable += sum(
            1 for result in results if result["status"] == "retryable"
        )
        total_not_found += sum(
            1 for result in results if result["status"] == "not_found"
        )
        logger.info(
            "[detail] batch=%s done=%s retryable=%s not_found=%s",
            len(results),
            total_done,
            total_retryable,
            total_not_found,
        )

    return conn


def merge_base_and_detail(
    output_dir: Path, logger: Optional[logging.Logger] = None
) -> Tuple[Path, Optional[Path]]:
    paths = build_paths(output_dir)
    if not paths.base_csv.exists():
        raise ScraperError("No existe el CSV base. Ejecuta primero 'unificar'.")

    base_df = pd.read_csv(paths.base_csv, sep=";", dtype=str, keep_default_na=False)
    base_df["_record_key"] = [
        build_record_key(record) for record in base_df.to_dict(orient="records")
    ]

    conn = init_detail_db(output_dir)
    cache_rows = query_detail_rows(conn, base_df["_record_key"].tolist())

    meta_rows = []
    mapped_rows = []
    for record_key in base_df["_record_key"]:
        detail = cache_rows.get(record_key, {})
        mapped = {}
        if detail.get("mapped_json"):
            mapped = json.loads(detail["mapped_json"])
        meta_rows.append(
            {
                "_record_key": record_key,
                "detail_status": detail.get("status"),
                "detail_attempts": detail.get("attempts"),
                "detail_last_error": detail.get("last_error"),
                "detail_last_http_status": detail.get("last_http_status"),
                "detail_url": detail.get("detail_url"),
                "detail_page_title": detail.get("page_title"),
            }
        )
        mapped["_record_key"] = record_key
        mapped_rows.append(mapped)

    meta_df = pd.DataFrame(meta_rows)
    mapped_df = pd.DataFrame(mapped_rows).drop_duplicates(subset=["_record_key"])
    final_df = base_df.merge(meta_df, on="_record_key", how="left").merge(
        mapped_df, on="_record_key", how="left"
    )
    final_df = final_df.drop(columns=["_record_key"])

    final_df.to_csv(paths.final_csv, index=False, sep=";", encoding="utf-8-sig")
    parquet_path: Optional[Path] = None
    if HAS_PYARROW:
        final_df.to_parquet(paths.final_parquet, index=False)
        parquet_path = paths.final_parquet
    if logger:
        logger.info("✓ Final CSV: %s (%s filas)", paths.final_csv, f"{len(final_df):,}")
        if parquet_path:
            logger.info("✓ Final Parquet: %s", parquet_path)
    return paths.final_csv, parquet_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Descarga y enriquece la contratacion publica de la Comunidad de Madrid."
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default="help",
        choices=[
            "help",
            "prueba",
            "menores",
            "otros",
            "todo",
            "unificar",
            "detail",
            "merge",
            "full",
        ],
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--anio-inicio", type=int, default=2017)
    parser.add_argument("--anio-fin", type=int, default=datetime.now().year)
    parser.add_argument("--pause-base", type=float, default=PAUSA_BASE)
    parser.add_argument("--detail-workers", type=int, default=DETAIL_WORKERS)
    parser.add_argument("--detail-batch-size", type=int, default=DETAIL_BATCH_SIZE)
    parser.add_argument("--detail-delay", type=float, default=0.2)
    parser.add_argument("--detail-jitter", type=float, default=0.1)
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--include-menores",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--retryable-only", action="store_true")
    parser.add_argument("--retryable-ignore-max-attempts", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.mode == "help":
        parser.print_help()
        return 0

    paths = build_paths(Path(args.output), Path(args.log_path))
    ensure_dirs(paths)
    logger = build_logger(paths.log_path)

    if args.mode in {"prueba", "menores", "otros", "todo", "full"}:
        downloader = ComunidadMadridScraper(paths, logger, pausa_base=args.pause_base)
        if args.mode == "prueba":
            downloader.descargar_prueba()
            return 0
        if args.mode == "menores":
            downloader.t_inicio = time.time()
            downloader.descargar_menores()
            downloader._resumen()
            return 0
        if args.mode == "otros":
            downloader.t_inicio = time.time()
            downloader.descargar_otros(args.anio_inicio, args.anio_fin)
            downloader._resumen()
            return 0
        if args.mode == "todo":
            downloader.descargar_todo(args.anio_inicio, args.anio_fin)
            return 0
        if args.mode == "full":
            downloader.descargar_todo(args.anio_inicio, args.anio_fin)
            save_base_outputs(paths.output_dir, logger)
            run_detail(
                paths.output_dir,
                logger,
                detail_workers=args.detail_workers,
                detail_batch_size=args.detail_batch_size,
                detail_delay=args.detail_delay,
                detail_jitter=args.detail_jitter,
                force=args.force,
                include_menores=args.include_menores,
                retryable_only=args.retryable_only,
                retryable_ignore_max_attempts=args.retryable_ignore_max_attempts,
            )
            merge_base_and_detail(paths.output_dir, logger)
            return 0

    if args.mode == "unificar":
        save_base_outputs(paths.output_dir, logger)
        return 0
    if args.mode == "detail":
        run_detail(
            paths.output_dir,
            logger,
            detail_workers=args.detail_workers,
            detail_batch_size=args.detail_batch_size,
            detail_delay=args.detail_delay,
            detail_jitter=args.detail_jitter,
            force=args.force,
            include_menores=args.include_menores,
            retryable_only=args.retryable_only,
            retryable_ignore_max_attempts=args.retryable_ignore_max_attempts,
        )
        return 0
    if args.mode == "merge":
        merge_base_and_detail(paths.output_dir, logger)
        return 0

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
        raise SystemExit(130)
