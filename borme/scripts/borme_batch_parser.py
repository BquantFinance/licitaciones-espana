#!/usr/bin/env python3
"""
BORME Batch Parser v2 - Parquet
================================
Procesa todos los BORME-A-*.pdf (2009-2026) y genera:
  1. borme_empresas.parquet - una fila por empresa x acto (matching con licitaciones)
  2. borme_cargos.parquet   - una fila por cargo (red de personas)

Uso:
  python borme_batch_parser.py --input D:/Licitaciones/borme_pdfs --workers 8
  python borme_batch_parser.py --input D:/Licitaciones/borme_pdfs --workers 16 --resume
  python borme_batch_parser.py --input D:/Licitaciones/borme_pdfs --reprocesar
  python borme_batch_parser.py --input D:/Licitaciones/borme_pdfs \\
      --semilla borme/data/borme_empresas_pub.parquet --semilla borme/data/borme_cargos_pub.parquet

Sin perder lo ya parseado (sesgo del superviviente, comun/historico.py; detalle
en la sección BATCH RUNNER):
  - Cada ejecución parsea solo los PDF nuevos o cambiados y acumula el resultado
    sobre las tablas anteriores (acumular). Las filas de los PDF que ya no están
    en disco se conservan; si un PDF da otro resultado al volver a parsearlo
    (--reprocesar, tras cambiar el parser), las filas anteriores se conservan con
    _en_ultima_descarga=False. Columnas de control: _primera_descarga,
    _ultima_descarga y _en_ultima_descarga (y _origen en las de la semilla).
  - Las tablas se escriben con guardar_registros: la anterior pasa a _historico/.
  - --semilla <parquet publicado>: añade los actos del release que no salen del
    parse, marcados con _origen='release v2026.02'.

Basado en datos de la Agencia Estatal Boletin Oficial del Estado (https://www.boe.es)
"""

import os
import re
import sys
import json
import shutil
import logging
import argparse
import pdfplumber
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pip install pandas pyarrow")
    raise

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from comun.historico import (  # noqa: E402
    COLUMNAS_META, HISTORICO, ORIGEN_SEMILLA, acumular, guardar_registros,
    imprimir_informe_semilla, leer_registros, sembrar, versiones,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# =====================================================================
# CONSTANTES
# =====================================================================

PROVINCIAS = {
    "ALBACETE","ALICANTE","ALICANTE/ALACANT","ALMERIA","ALMERÍA",
    "ARABA/ÁLAVA","ASTURIAS","AVILA","ÁVILA","BADAJOZ",
    "BARCELONA","BIZKAIA","BURGOS","CACERES","CÁCERES",
    "CADIZ","CÁDIZ","CANTABRIA","CASTELLON","CASTELLÓN","CASTELLÓN/CASTELLÓ",
    "CIUDAD REAL","CORDOBA","CÓRDOBA","A CORUÑA","CUENCA",
    "GIPUZKOA","GIRONA","GRANADA","GUADALAJARA","HUELVA",
    "HUESCA","ILLES BALEARS","JAEN","JAÉN","LEON","LEÓN",
    "LLEIDA","LUGO","MADRID","MALAGA","MÁLAGA","MELILLA",
    "MURCIA","NAVARRA","OURENSE","PALENCIA","LAS PALMAS",
    "PONTEVEDRA","LA RIOJA","SALAMANCA","SEGOVIA","SEVILLA",
    "SORIA","TARRAGONA","SANTA CRUZ DE TENERIFE","TERUEL",
    "TOLEDO","VALENCIA","VALENCIA/VALÈNCIA","VALLADOLID",
    "ZAMORA","ZARAGOZA","CEUTA",
}

COD_PROVINCIA = {
    "01":"ARABA/ÁLAVA","02":"ALBACETE","03":"ALICANTE","04":"ALMERÍA",
    "05":"ÁVILA","06":"BADAJOZ","07":"ILLES BALEARS","08":"BARCELONA",
    "09":"BURGOS","10":"CÁCERES","11":"CÁDIZ","12":"CASTELLÓN",
    "13":"CIUDAD REAL","14":"CÓRDOBA","15":"A CORUÑA","16":"CUENCA",
    "17":"GIRONA","18":"GRANADA","19":"GUADALAJARA","20":"GIPUZKOA",
    "21":"HUELVA","22":"HUESCA","23":"JAÉN","24":"LEÓN","25":"LLEIDA",
    "26":"LA RIOJA","27":"LUGO","28":"MADRID","29":"MÁLAGA","30":"MURCIA",
    "31":"NAVARRA","32":"OURENSE","33":"ASTURIAS","34":"PALENCIA",
    "35":"LAS PALMAS","36":"PONTEVEDRA","37":"SALAMANCA",
    "38":"SANTA CRUZ DE TENERIFE","39":"CANTABRIA","40":"SEGOVIA",
    "41":"SEVILLA","42":"SORIA","43":"TARRAGONA","44":"TERUEL",
    "45":"TOLEDO","46":"VALENCIA","47":"VALLADOLID","48":"BIZKAIA",
    "49":"ZAMORA","50":"ZARAGOZA","51":"CEUTA","52":"MELILLA",
    "99":"REGISTROS MERCANTILES CENTRALES",
}

ACTOS = [
    # Constitución y extinción
    "Constitución","Disolución","Extinción",
    # Cargos
    "Ceses/Dimisiones","Nombramientos","Revocaciones","Reelecciones",
    "Cancelaciones de oficio de nombramientos",
    "Nombramiento de administradores",
    # Modificaciones
    "Modificaciones estatutarias",
    "Cambio de domicilio social","Cambio de objeto social",
    "Cambio de denominación social",
    "Ampliación del objeto social","Ampliacion del objeto social",
    "Ampliación de capital","Reducción de capital",
    # Operaciones societarias
    "Fusión por absorción","Fusión","Escisión","Transformación de sociedad",
    "Transformación",
    # Concursal
    "Situación concursal",
    "Auto de declaración de concurso",
    "Auto de apertura de la fase de liquidación",
    "Auto de conclusión del concurso",
    "Revocación de administradores concursales",
    "Crédito incobrable",
    # Hoja registral
    "Cierre provisional hoja registral",
    "Reapertura hoja registral",
    # Unipersonalidad
    "Declaración de unipersonalidad",
    "Pérdida del carácter de unipersonalidad",
    "Pérdida del caracter de unipersonalidad",
    # Otros
    "Otros conceptos","Fe de erratas","Depósito de cuentas anuales",
    "Empresario Individual","Sociedad unipersonal",
]

# =====================================================================
# REGEX
# =====================================================================

# Inicio de anuncio "57315 - EMPRESA SL.". La numeración se reinicia cada año,
# así que el primer BORME del año trae anuncios de 1-3 cifras (ver _entry_starts)
ENTRY_START_RE = re.compile(r'^(\d{1,7})\s*-\s*', re.MULTILINE)

# Generic body start detector (approach B):
# Company names are ALL CAPS. Body starts at first mixed-case word after ". "
# that is NOT a legal form word (Sociedad, Limitada, etc.)
_BODY_START_RE = re.compile(r'(?<=\.)\s+(?=[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{2,})')
_FE_ERRATAS_RE = re.compile(r'\.\s+Fe de erratas')  # special case: "Fe" too short

_LEGAL_FORMS = frozenset({
    'Sociedad', 'Limitada', 'Anónima', 'Anonima', 'Cooperativa', 'Profesional',
    'Laboral', 'Deportiva', 'Unipersonal', 'Civil', 'Comanditaria', 'Colectiva',
    'Comandita', 'Agrupación', 'Agrupacion', 'Europea', 'Responsabilidad',
    'Sucursal', 'Nueva', 'Empresa',
})

# ACTOS still used for detecting which actos are present in body text
_ACTO_KW_RE = re.compile(
    r'(?:' + '|'.join(re.escape(a) for a in ACTOS) + r'|Datos registrales)[.:]')

# Cargo generico: captura CUALQUIER abreviatura seguida de : y NOMBRES EN MAYUSCULAS
CARGO_RE = re.compile(
    r'(?<=\.\s)'
    r'([A-Z][A-Za-záéíóúñ.\s/\-\d=]{0,25}?)'
    r':\s*'
    r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\d\s;,.\-]+?)'
    r'(?:\.\s|$)'
)

# Excluir falsos positivos del regex de cargos
CARGO_EXCLUDE = frozenset({
    # Field labels
    'Objeto social', 'Domicilio', 'Capital', 'Datos registrales',
    'ACTIVIDAD PRINCIPAL', 'Comienzo de operaciones',
    'Artículo de los estatutos', 'ARTICULO',
    # Acto/section names that match cargo pattern but aren't cargos
    'Otros conceptos', 'Sociedades absorbidas', 'Resoluciones',
    'Denominación y forma adoptada',
})
CARGO_EXCLUDE_RE = re.compile(
    r'^(?:ART(?:ICULO|S)?[\s.\d,]+|CNAE\s|ACTIVIDAD)', re.IGNORECASE)

# Secciones de cargos -> tipo_acto
SECTION_MAP = {
    'Nombramientos': 'nombramiento',
    'Ceses/Dimisiones': 'cese',
    'Revocaciones': 'revocacion',
    'Reelecciones': 'reeleccion',
    'Cancelaciones de oficio de nombramientos': 'cancelacion',
}

# Variantes reales: "T 856, L 683, F 96, S 8, H CC 11959, I/A 2 (29.01.15)",
# "T 16030 , F 160, S 8, H M 271304, I/A 6 ( 2.02.15)" (sin Libro, espacio antes
# de la coma y tras el paréntesis), "H NA004126, I/A00037"
DATOS_REG_RE = re.compile(
    r'Datos registrales[.:]\s*T\s*(\d+)\s*,\s*(?:L\s*(\d+)\s*,\s*)?F\s*(\d+)\s*,\s*S\s*(\d+)\s*,'
    r'\s*H\s*([A-Z\s]*\d+)\s*,\s*I/A\s*(\d+)\s*\(\s*(\d{1,2}\.\d{2}\.\d{2,4})\s*\)')
# El domicilio termina en "(MUNICIPIO)." si no le sigue "Capital:"
DOMICILIO_RE = re.compile(
    r'Domicilio[.:]\s*(.+?)(?:[.]\s*Capital[.:]|(?<=\))\.(?=\s|$)|$)', re.DOTALL)
CAPITAL_RE = re.compile(r'Capital[.:]\s*([\d.,]+)\s*Euros')
# Ampliación/reducción: "Capital: <importe ampliado> Euros. Resultante Suscrito: <capital> Euros"
RESULTANTE_RE = re.compile(r'Resultante Suscrito[.:]\s*([\d.,]+)\s*Euros')
OBJETO_RE = re.compile(r'Objeto social[.:]\s*(.+?)(?:[.]\s*Domicilio[.:]|$)', re.DOTALL)
COMIENZO_RE = re.compile(r'Comienzo de operaciones[.:]\s*(\d(?:[\d.]*\d)?)')

_SKIP_LINES = frozenset([
    "SECCIÓN PRIMERA","Empresarios","Actos inscritos",
    "SECCIÓN SEGUNDA","Anuncios y avisos legales",
    "Otros actos publicados en el Registro Mercantil",
])

# =====================================================================
# FUNCIONES AUXILIARES
# =====================================================================

def _clean(raw: str) -> str:
    lines = []
    for line in raw.split("\n"):
        s = line.strip()
        if s.startswith("BOLETÍN OFICIAL DEL REGISTRO"): continue
        if s.startswith("Núm.") and "Pág." in s: continue
        if re.match(r'^[\d-]+[A-Z]-EMROB$', s): continue
        if s == ":evc": continue
        if s.startswith("http://www.boe.es"): continue
        if "D.L.: M-5188" in s: continue
        if s in _SKIP_LINES: continue
        lines.append(line)
    return "\n".join(lines)


def _entry_starts(text: str) -> list:
    """Inicios de anuncio. Un número de 1-3 cifras (primer BORME del año) solo se
    acepta con formato "N - " y si es el primero o continúa la numeración, para
    no cortar en líneas que empiezan por p.ej. "12 - 2º B" dentro de un domicilio."""
    starts = []
    for m in ENTRY_START_RE.finditer(text):
        num = m.group(1)
        if len(num) < 4 and (
                num.startswith("0") or not m.group(0).startswith(num + " ")
                or (starts and int(num) != int(starts[-1].group(1)) + 1)):
            continue
        starts.append(m)
    return starts


def _normalize_empresa(name: str) -> str:
    n = name.upper().strip()
    for suffix in [
        " SOCIEDAD ANONIMA DEPORTIVA", " SOCIEDAD ANONIMA",
        " SOCIEDAD LIMITADA PROFESIONAL", " SOCIEDAD LIMITADA LABORAL",
        " SOCIEDAD LIMITADA NUEVA EMPRESA", " SOCIEDAD LIMITADA",
        " SOCIEDAD COOPERATIVA ANDALUZA", " SOCIEDAD COOPERATIVA",
        " SOCIEDAD CIVIL PROFESIONAL", " SOCIEDAD CIVIL",
        " AGRUPACION DE INTERES ECONOMICO",
        " SAU", " SLU", " SAD", " SLL", " SLP", " SLNE",
        " SA SME", " SA", " SL", " SC", " SCA", " SCCL", " SCOOP",
        " SE", " SRL", " AIE",
    ]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
            break
    n = re.sub(r'[.,;]+$', '', n).strip()
    return n


def _extract_cargo_and_tipo(raw_cargo: str, body: str, match_start: int) -> Tuple[str, str]:
    c = raw_cargo.strip()
    # 1) El match incluye prefijo de seccion?
    for prefix, tipo in SECTION_MAP.items():
        if c.startswith(prefix + '. ') or c.startswith(prefix + '.'):
            cargo = c[len(prefix):].strip().lstrip('. ').strip()
            return cargo, tipo
    # 2) Buscar seccion mas cercana antes del match
    pre = body[:match_start]
    positions = {}
    for kw, tipo in SECTION_MAP.items():
        pos = pre.rfind(kw)
        if pos != -1:
            positions[tipo] = pos
    if positions:
        return c, max(positions, key=positions.get)
    return c, "nombramiento"


# =====================================================================
# PARSER PRINCIPAL
# =====================================================================

def parse_single_pdf(pdf_path: str) -> Tuple[List[Dict], List[Dict]]:
    path = Path(pdf_path)
    # Una versión anterior guardada en <día>/_historico/ lleva el nombre del PDF
    fname = _nombre_pdf(path)

    bm = re.match(r'BORME-([A-Z])-(\d{4})-(\d+)-(\d+)', fname)
    if not bm:
        return [], []

    tipo = bm.group(1)
    year = int(bm.group(2))
    numero = int(bm.group(3))
    cod_prov = bm.group(4)
    provincia_filename = COD_PROVINCIA.get(cod_prov, "")

    # Fecha de la ruta (.../YYYY/MM/DD/<pdf>, estructura del scraper). Se busca
    # desde el final y anclada al año del fichero: un directorio base tipo
    # "D:/2026/borme_pdfs" no debe tomarse como la fecha
    parts = path.parts
    fecha_borme = f"{year}-01-01"
    for idx in range(len(parts) - 4, -1, -1):
        if parts[idx] == str(year) and parts[idx + 1].isdigit() and parts[idx + 2].isdigit():
            fecha_borme = f"{parts[idx]}-{parts[idx+1].zfill(2)}-{parts[idx+2].zfill(2)}"
            break

    # Un PDF ilegible/truncado propaga la excepción: _process_one lo cuenta como
    # error (y --resume lo reintenta) en vez de darlo por procesado sin filas
    with pdfplumber.open(pdf_path) as pdf:
        raw = "\n".join(p.extract_text() or "" for p in pdf.pages)

    text = _clean(raw)
    splits = _entry_starts(text)
    if not splits:
        return [], []

    empresas_rows = []
    cargos_rows = []
    provincia_actual = provincia_filename

    for i, match in enumerate(splits):
        entry_num = match.group(1)
        start = match.end()
        end = splits[i + 1].start() if i + 1 < len(splits) else len(text)

        block = text[start:end].replace('\n', ' ')
        block = re.sub(r'\s{2,}', ' ', block).strip()

        # Nombre empresa (approach B: first mixed-case word = body start)
        body_pos = len(block)

        # Special case: "Fe de erratas" (too short for main regex)
        fe = _FE_ERRATAS_RE.search(block)
        if fe:
            body_pos = fe.start() + 1

        for bm in _BODY_START_RE.finditer(block):
            if bm.start() + 1 >= body_pos:
                break
            rest = block[bm.end():bm.end() + 40]
            first_word = rest.split('.')[0].split(':')[0].split(' ')[0].strip()
            # "Sociedad unipersonal." es un acto, no parte de la forma jurídica
            if first_word in _LEGAL_FORMS and not _ACTO_KW_RE.match(rest):
                continue
            body_pos = bm.start() + 1
            break

        empresa = block[:body_pos].strip().rstrip('.')
        body = ". " + block[body_pos:].strip() if body_pos < len(block) else ""

        # Provincia
        gap_start = splits[i - 1].start() if i > 0 else 0
        for line in text[gap_start:match.start()].split('\n'):
            if line.strip() in PROVINCIAS:
                provincia_actual = line.strip()

        empresa_norm = _normalize_empresa(empresa)

        # Row empresa
        row = {
            "fecha_borme": fecha_borme,
            "num_borme": numero,
            "num_entrada": entry_num,
            "empresa": empresa,
            "empresa_norm": empresa_norm,
            "provincia": provincia_actual,
            "cod_provincia": cod_prov,
            "tipo_borme": tipo,
            "pdf_filename": fname,
        }

        # Actos
        actos = []
        if "Constitución." in body or "Constitución:" in body:
            actos.append("Constitución")
            m = COMIENZO_RE.search(body)
            if m:
                row["fecha_constitucion"] = m.group(1)
            m = OBJETO_RE.search(body)
            if m:
                row["objeto_social"] = m.group(1).strip()[:500]

        m = DOMICILIO_RE.search(body)
        if m:
            row["domicilio"] = m.group(1).strip()[:300]
        elif "Cambio de domicilio social." in body:
            actos.append("Cambio de domicilio")
            dm = re.search(
                r'Cambio de domicilio social[.:]\s*(.+?)'
                r'(?:[.]\s*Datos registrales|(?<=\))\.(?=\s|$)|$)', body)
            if dm:
                row["domicilio"] = dm.group(1).strip()[:300]

        # Capital social tras el acto: en ampliaciones "Capital:" es el importe
        # ampliado y el capital resultante es el último "Resultante Suscrito:"
        # (en reducciones solo aparece este)
        resultantes = RESULTANTE_RE.findall(body)
        m = CAPITAL_RE.search(body)
        capital_txt = resultantes[-1] if resultantes else (m.group(1) if m else None)
        if capital_txt:
            try:
                row["capital_euros"] = float(capital_txt.replace(".", "").replace(",", "."))
            except ValueError:
                pass

        for acto in ACTOS:
            if (acto + "." in body or acto + ":" in body) and acto != "Constitución" and acto not in actos:
                actos.append(acto)

        row["actos"] = "|".join(actos)

        m = DATOS_REG_RE.search(body)
        if m:
            row["hoja_registral"] = m.group(5).strip()
            row["tomo"] = m.group(1)
            row["inscripcion"] = m.group(6)
            row["fecha_inscripcion"] = m.group(7)

        empresas_rows.append(row)

        # Cargos (regex generico)
        for cm in CARGO_RE.finditer(body):
            raw_cargo = cm.group(1).strip()
            personas_raw = cm.group(2).strip()

            if raw_cargo in CARGO_EXCLUDE:
                continue
            if CARGO_EXCLUDE_RE.match(raw_cargo):
                continue

            words = [w for w in personas_raw.replace(';', ' ').replace('.', ' ').split()
                     if len(w) > 1]
            if len(words) < 2:
                continue

            cargo, tipo_acto = _extract_cargo_and_tipo(raw_cargo, body, cm.start())
            if not cargo or cargo in CARGO_EXCLUDE or CARGO_EXCLUDE_RE.match(cargo):
                continue

            personas = [p.strip() for p in personas_raw.split(";") if p.strip()]
            for persona in personas:
                cargos_rows.append({
                    "fecha_borme": fecha_borme,
                    "num_entrada": entry_num,
                    "empresa": empresa,
                    "empresa_norm": empresa_norm,
                    "provincia": provincia_actual,
                    "hoja_registral": row.get("hoja_registral", ""),
                    "tipo_acto": tipo_acto,
                    "cargo": cargo,
                    "persona": persona,
                    "pdf_filename": fname,
                })

    return empresas_rows, cargos_rows


# =====================================================================
# BATCH RUNNER
# =====================================================================

def find_borme_a_pdfs(base_dir: Path) -> List[Path]:
    return sorted(base_dir.rglob("BORME-A-*.pdf"))


def _process_one(pdf_path_str: str) -> Tuple[List[Dict], List[Dict], str, bool]:
    try:
        e_rows, c_rows = parse_single_pdf(pdf_path_str)
        return e_rows, c_rows, pdf_path_str, True
    except Exception as e:
        log.warning(f"   Error procesando {pdf_path_str}: {e}")
        return [], [], pdf_path_str, False


def _pdfs_guardados(parts_dir: Path, empresas_parquet: Path) -> set:
    """Nombres de PDF con filas ya guardadas (parciales por batch o salida previa)."""
    fuentes = sorted(parts_dir.glob("empresas_*.parquet"))
    if empresas_parquet.exists():
        fuentes.append(empresas_parquet)
    guardados = set()
    for p in fuentes:
        guardados.update(pd.read_parquet(p, columns=["pdf_filename"])["pdf_filename"])
    return guardados


def _consolidar(parts_dir: Path, prefijo: str, previo: Path = None) -> pd.DataFrame:
    """Une las filas guardadas por batch con la salida previa (--resume).
    Si un PDF está en ambas, mandan las filas nuevas."""
    frames = [pd.read_parquet(p) for p in sorted(parts_dir.glob(f"{prefijo}_*.parquet"))]
    if previo is not None and previo.exists():
        nuevos = set()
        for f in frames:
            nuevos.update(f["pdf_filename"])
        base = pd.read_parquet(previo)
        frames.insert(0, base[~base["pdf_filename"].isin(nuevos)])
    frames = [f.assign(fecha_borme=pd.to_datetime(f["fecha_borme"], errors="coerce"))
              for f in frames if len(f) > 0]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_batch(base_dir: Path, output_dir: Path, workers: int = 8,
              resume: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)
    empresas_parquet = output_dir / "borme_empresas.parquet"
    cargos_parquet = output_dir / "borme_cargos.parquet"
    progress_file = output_dir / "borme_parse_progress.json"
    # Filas de cada batch, guardadas antes de marcar sus PDFs como hechos: así
    # --resume no pierde lo procesado en ejecuciones anteriores
    parts_dir = output_dir / "borme_parse_parts"

    log.info(f"Buscando BORME-A PDFs en {base_dir}...")
    all_pdfs = find_borme_a_pdfs(base_dir)
    log.info(f"   Encontrados: {len(all_pdfs):,} PDFs")

    done_set = set()
    usar_previo = resume and progress_file.exists()
    if usar_previo:
        with open(progress_file) as f:
            done_set = set(json.load(f).get("done", []))
        # Solo cuenta como procesado lo que tiene filas guardadas
        guardados = _pdfs_guardados(parts_dir, empresas_parquet)
        sin_filas = {p for p in done_set if Path(p).name not in guardados}
        if sin_filas:
            log.warning(f"   {len(sin_filas):,} PDFs marcados como procesados sin filas guardadas: se reprocesan")
            done_set -= sin_filas
        log.info(f"   Resumiendo: {len(done_set):,} ya procesados")
    elif parts_dir.exists():
        shutil.rmtree(parts_dir)  # ejecución completa: descartar parciales antiguos

    pending = [p for p in all_pdfs if str(p) not in done_set]
    log.info(f"   Pendientes: {len(pending):,}")

    if not pending and not any(parts_dir.glob("*.parquet")):
        log.info("Nada que procesar.")
        return

    parts_dir.mkdir(parents=True, exist_ok=True)
    BATCH_SIZE = 5000
    n_empresas = 0
    n_cargos = 0
    errors = []
    processed = len(done_set)
    total = len(all_pdfs)
    t0 = datetime.now()

    for batch_start in range(0, len(pending), BATCH_SIZE):
        batch = pending[batch_start:batch_start + BATCH_SIZE]
        batch_empresas = []
        batch_cargos = []

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one, str(p)): p for p in batch}
            for future in as_completed(futures):
                e_rows, c_rows, path_str, ok = future.result()
                processed += 1
                if ok:
                    batch_empresas.extend(e_rows)
                    batch_cargos.extend(c_rows)
                    done_set.add(path_str)
                else:
                    errors.append(path_str)

                if processed % 500 == 0:
                    elapsed = (datetime.now() - t0).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / rate if rate > 0 else 0
                    log.info(
                        f"   {processed:,}/{total:,} "
                        f"({processed / total * 100:.1f}%) "
                        f"| {rate:.0f} PDFs/s "
                        f"| ETA: {eta / 60:.0f}min "
                        f"| empresas: {n_empresas + len(batch_empresas):,} "
                        f"| cargos: {n_cargos + len(batch_cargos):,}"
                    )

        # Guardar las filas del batch ANTES de marcar sus PDFs como hechos
        tag = f"{t0:%Y%m%d%H%M%S}_{batch_start:07d}"
        if batch_empresas:
            pd.DataFrame(batch_empresas).to_parquet(
                parts_dir / f"empresas_{tag}.parquet", index=False, engine="pyarrow")
        if batch_cargos:
            pd.DataFrame(batch_cargos).to_parquet(
                parts_dir / f"cargos_{tag}.parquet", index=False, engine="pyarrow")
        n_empresas += len(batch_empresas)
        n_cargos += len(batch_cargos)

        with open(progress_file, "w") as f:
            json.dump({"done": list(done_set), "errors": errors}, f)
        log.info(f"   Batch guardado ({batch_start + len(batch):,} procesados)")

    # DataFrames: salida previa (--resume) + filas de los batches
    log.info("Construyendo DataFrames...")

    df_empresas = _consolidar(parts_dir, "empresas", empresas_parquet if usar_previo else None)
    df_cargos = _consolidar(parts_dir, "cargos", cargos_parquet if usar_previo else None)

    if len(df_empresas) > 0:
        df_empresas["fecha_borme"] = pd.to_datetime(df_empresas["fecha_borme"], errors="coerce")
        if "capital_euros" in df_empresas.columns:
            df_empresas["capital_euros"] = pd.to_numeric(df_empresas["capital_euros"], errors="coerce")

        before = len(df_empresas)
        df_empresas = df_empresas.drop_duplicates(
            subset=["fecha_borme", "num_entrada", "empresa_norm"], keep="first"
        )
        log.info(f"   Empresas: {before:,} -> {len(df_empresas):,} (dedup)")
        df_empresas.to_parquet(empresas_parquet, index=False, engine="pyarrow")
        log.info(f"   {empresas_parquet} ({empresas_parquet.stat().st_size / 1e6:.1f} MB)")

    if len(df_cargos) > 0:
        df_cargos["fecha_borme"] = pd.to_datetime(df_cargos["fecha_borme"], errors="coerce")

        before = len(df_cargos)
        df_cargos = df_cargos.drop_duplicates(
            subset=["fecha_borme", "num_entrada", "cargo", "persona", "tipo_acto"],
            keep="first"
        )
        log.info(f"   Cargos: {before:,} -> {len(df_cargos):,} (dedup)")
        df_cargos.to_parquet(cargos_parquet, index=False, engine="pyarrow")
        log.info(f"   {cargos_parquet} ({cargos_parquet.stat().st_size / 1e6:.1f} MB)")

    # Las salidas finales ya contienen todas las filas: los parciales sobran
    shutil.rmtree(parts_dir, ignore_errors=True)

    # Resumen
    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"\n{'=' * 60}")
    log.info(f"COMPLETADO en {elapsed / 60:.1f} minutos")
    log.info(f"   PDFs procesados: {processed:,}")
    log.info(f"   Errores: {len(errors):,}")
    log.info(f"   Empresas (filas): {len(df_empresas):,}")
    log.info(f"   Cargos (filas): {len(df_cargos):,}")
    if len(df_empresas) > 0:
        log.info(f"   Empresas unicas: {df_empresas['empresa_norm'].nunique():,}")
        log.info(f"   Provincias: {df_empresas['provincia'].nunique()}")
        log.info(f"   Rango fechas: {df_empresas['fecha_borme'].min()} -> {df_empresas['fecha_borme'].max()}")
        constit = df_empresas[df_empresas["actos"].str.contains("Constitución", na=False)]
        log.info(f"   Constituciones: {len(constit):,}")
        if "capital_euros" in df_empresas.columns:
            with_capital = df_empresas["capital_euros"].notna().sum()
            log.info(f"   Con capital: {with_capital:,}")
    if len(df_cargos) > 0:
        log.info(f"   Cargos unicos (tipos): {df_cargos['cargo'].nunique()}")
        log.info(f"   Personas unicas: {df_cargos['persona'].nunique():,}")
        for tipo, n in df_cargos['tipo_acto'].value_counts().items():
            log.info(f"      {tipo}: {n:,}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BORME Batch Parser v2")
    parser.add_argument("--input", required=True, help="Carpeta raiz de borme_pdfs")
    parser.add_argument("--output", default=None, help="Carpeta de salida (default: input)")
    parser.add_argument("--workers", type=int, default=8, help="Procesos paralelos")
    parser.add_argument("--resume", action="store_true", help="Continuar desde ultimo progreso")
    args = parser.parse_args()

    base = Path(args.input)
    out = Path(args.output) if args.output else base
    run_batch(base, out, workers=args.workers, resume=args.resume)
