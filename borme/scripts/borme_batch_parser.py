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
    import pyarrow.compute as pc
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
#
# Sesgo del superviviente (comun/historico.py). Los PDF del BORME no cambian,
# pero las tablas solo pueden salir de los PDF que hay en disco: un parse
# completo que las reescribiera perdería todo lo que salió de los que faltan
# (otra máquina sin el archivo completo, una carpeta borrada, una descarga
# parcial). Por eso:
# - Parse incremental: solo se parsean las versiones de PDF que no constan en el
#   registro de borme_parse_progress.json ("versiones": ruta relativa -> tamaño
#   y fecha de modificación; "filas": empresas y cargos que dio), es decir, las
#   nuevas o cambiadas. Solo cuenta como parseado lo que tiene filas en las
#   tablas: si se borran o se sustituyen por una copia anterior, lo que falta se
#   vuelve a parsear. --reprocesar vuelve a parsear todas las que hay en disco
#   con el código actual (p.ej. tras corregir el parser).
# - Las tablas se acumulan sobre la salida anterior con acumular(). Ámbito: los
#   PDF parseados en esta ejecución con alguna fila; un parse vacío o fallido no
#   retira nada. Las filas de los PDF que no se han vuelto a parsear (p.ej.
#   porque ya no están en disco) se quedan como estaban; las de un PDF que al
#   volver a parsearlo da otro resultado se conservan con _en_ultima_descarga=False.
# - Las versiones anteriores de un PDF (<día>/_historico/, borme_scraper.py
#   --comprobar) se parsean de la más antigua a la actual, cada una como una
#   descarga distinta: lo vigente es lo de la última.
# - Las tablas se escriben con guardar_registros (la anterior pasa a _historico/).
# - --semilla: filas publicadas (release v2026.02) de los actos que no salen del
#   parse, por CLAVE_SEMILLA (ver _sembrar).

EMPRESAS_PARQUET = "borme_empresas.parquet"
CARGOS_PARQUET = "borme_cargos.parquet"
PROGRESO = "borme_parse_progress.json"
PARTES = "borme_parse_parts"
BATCH_SIZE = 5000
# Anuncios repetidos dentro de un mismo parse que se descartan, con las claves de
# siempre (así una sola ejecución da la salida de antes más las columnas de control)
DEDUP_EMPRESAS = ["fecha_borme", "num_entrada", "empresa_norm"]
DEDUP_CARGOS = ["fecha_borme", "num_entrada", "cargo", "persona", "tipo_acto"]
# Clave estable de la semilla: el acto del BORME (número de anuncio dentro del PDF
# de cada boletín y provincia), en las dos tablas. No depende de cómo se extraen el
# nombre de la empresa ni los cargos, que cambian entre versiones del parser. En
# borme_empresas_pub.parquet (v2026.02) nunca es nula y es única salvo 663 anuncios
# que el parser parte en dos filas con el mismo número (p.ej. "CAJA DE AHORROS DE
# SALAMANCA Y SORIA," y "AGREDA" en BORME-A-2009-11-42.pdf; el código actual los
# parte igual): esas filas se añaden o se descartan juntas.
CLAVE_SEMILLA = ["pdf_filename", "num_entrada"]
# Columnas temporales: versión de PDF de la que sale cada fila de las partes y
# posición de cada fila en la semilla
COLUMNA_VERSION = "_version_pdf"
COLUMNA_POSICION = "_posicion_semilla"
# Sello de las copias de _historico/ (guardar_version; archivar() añade _N si dos
# coinciden): BORME-A-2024-3-28__20260928T101010Z.pdf
_SELLO_RE = re.compile(r"__\d{8}T\d{6}Z(?:_\d+)?$")


def _ahora() -> str:
    """Fecha de esta ejecución para acumular() (UTC, ISO 8601)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _nombre_pdf(path: Path) -> str:
    """Nombre del PDF; el de una copia de _historico/, sin el sello."""
    if path.parent.name == HISTORICO:
        return _SELLO_RE.sub("", path.stem) + path.suffix
    return path.name


def find_borme_a_pdfs(base_dir: Path) -> List[Path]:
    """PDFs BORME-A actuales (sin las versiones anteriores de _historico/)."""
    return sorted(p for p in base_dir.rglob("BORME-A-*.pdf") if p.parent.name != HISTORICO)


def _versiones_pdf(base_dir: Path) -> Dict[Path, List[Path]]:
    """{PDF: sus versiones de la más antigua a la actual} con versiones(): las
    copias de <día>/_historico/ y la actual (si sigue en disco)."""
    destinos = set()
    for p in base_dir.rglob("BORME-A-*.pdf"):
        destinos.add(p.parent.parent / _nombre_pdf(p) if p.parent.name == HISTORICO else p)
    return {d: versiones(d) for d in sorted(destinos)}


def _relativa(path: Path, base_dir: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        return path.as_posix()


def _firma(path: Path) -> list:
    """Tamaño y fecha de modificación de una versión: guardar_version solo
    reescribe un PDF si cambia y al pasarlo a _historico/ conserva su fecha."""
    st = path.stat()
    return [st.st_size, st.st_mtime_ns]


def _leer_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        log.warning(f"   {path} ilegible ({e}): se ignora (lo que registraba se vuelve a parsear)")
        return {}


def _escribir_json(path: Path, datos: dict):
    tmp = path.with_name(f".{path.name}.nuevo")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(datos, f)
    os.replace(tmp, path)


def _pdfs_en_salida(ruta: Path) -> set:
    """Nombres de PDF con alguna fila en una tabla de salida."""
    if not ruta.exists():
        return set()
    return set(pc.unique(pq.read_table(ruta, columns=["pdf_filename"]).column(0)).to_pylist())


def _registro_en_salida(registro: dict, filas: dict, base_dir: Path, tablas: List[Path]) -> dict:
    """Versiones del registro cuyas filas siguen en las tablas: su PDF tiene
    filas en cada tabla en la que dio alguna ("filas"; sin ese dato, en la de
    empresas). Si las tablas se han borrado o sustituido por una copia anterior,
    las demás se vuelven a parsear (y si su PDF ya no está en disco, se avisa:
    esas filas solo se pueden recuperar de una copia o con --semilla)."""
    if not registro:
        return registro
    presentes = [_pdfs_en_salida(t) for t in tablas]
    validas = {rel: firma for rel, firma in registro.items()
               if all(not n or _nombre_pdf(Path(rel)) in p for n, p in zip(filas.get(rel, [1, 0]), presentes))}
    if len(validas) < len(registro):
        faltan = [rel for rel in registro if rel not in validas]
        en_disco = sum((base_dir / rel).exists() for rel in faltan)
        log.warning(f"   {len(faltan):,} PDFs registrados como parseados no tienen filas en la salida: "
                    f"{en_disco:,} se vuelven a parsear y {len(faltan) - en_disco:,} ya no están en disco")
    return validas


def _pendientes(pdfs: Dict[Path, List[Path]], base_dir: Path, registro: dict,
                reprocesar: bool) -> Tuple[Dict[Path, List[Path]], dict]:
    """Versiones que hay que acumular en esta ejecución: {PDF: [versiones]} desde
    la primera que no consta en el registro hasta la actual (si falta una antigua,
    las posteriores se vuelven a acumular detrás de ella para que la vigente siga
    siendo la última; con reprocesar, todas), y {versión: (firma, PDF)} de las
    copias de _historico/ que ya se parsearon cuando eran la actual."""
    pendientes, movidas = {}, {}
    for destino, lista in pdfs.items():
        inicio = 0 if reprocesar else len(lista)
        if not reprocesar:
            for i, v in enumerate(lista):
                rel, firma = _relativa(v, base_dir), _firma(v)
                if registro.get(rel) == firma:
                    continue
                # guardar_version pasa la copia anterior a _historico/ sin cambiar su
                # fecha: si coincide con la registrada del PDF, ya se parseó como él
                if v.parent.name == HISTORICO and registro.get(_relativa(destino, base_dir)) == firma:
                    movidas[rel] = (firma, _relativa(destino, base_dir))
                    continue
                inicio = i
                break
        if inicio < len(lista):
            pendientes[destino] = lista[inicio:]
    return pendientes, movidas


def _process_one(pdf_path_str: str) -> Tuple[List[Dict], List[Dict], str, bool]:
    try:
        e_rows, c_rows = parse_single_pdf(pdf_path_str)
        return e_rows, c_rows, pdf_path_str, True
    except Exception as e:
        log.warning(f"   Error procesando {pdf_path_str}: {e}")
        return [], [], pdf_path_str, False


def _preparar(filas: pd.DataFrame, versiones_ronda: set, subset: List[str], nombre: str) -> pd.DataFrame:
    """Filas nuevas de las versiones de una ronda, con los tipos de siempre y sin
    los anuncios repetidos dentro de este parse (como hasta ahora)."""
    df = filas[filas[COLUMNA_VERSION].isin(versiones_ronda)].drop(columns=COLUMNA_VERSION)
    if len(df) == 0:
        return df.reset_index(drop=True)
    df = df.assign(fecha_borme=pd.to_datetime(df["fecha_borme"], errors="coerce"))
    if "capital_euros" in df.columns:
        df["capital_euros"] = pd.to_numeric(df["capital_euros"], errors="coerce")
    antes = len(df)
    df = df.drop_duplicates(subset=subset, keep="first").reset_index(drop=True)
    log.info(f"   {nombre}: {antes:,} -> {len(df):,} (dedup)")
    return df


def _con_meta(df: pd.DataFrame) -> pd.DataFrame:
    """Salida de una versión anterior del parser, sin columnas de control: sus
    filas son las del último parse (vigentes), de fecha desconocida."""
    if all(c in df.columns for c in COLUMNAS_META):
        return df
    df = df.copy()
    for c in ("_primera_descarga", "_ultima_descarga"):
        if c not in df.columns:
            df[c] = pd.Series([None] * len(df), index=df.index, dtype=object)
    if "_en_ultima_descarga" not in df.columns:
        df["_en_ultima_descarga"] = True
    return df


def _acumular_pdfs(anterior, nuevos: pd.DataFrame, fecha: str, pdfs: set):
    """acumular() con ámbito = los PDF `pdfs` (parseados en esta ronda con alguna
    fila). Solo se compara con las filas anteriores de esos PDF: las que el parse
    nuevo ya no da quedan con _en_ultima_descarga=False (también los cargos de un
    PDF que ahora da empresas pero ningún cargo) y las de los demás PDF no se
    tocan. Así una ejecución incremental no recorre las tablas enteras."""
    if anterior is None or len(anterior) == 0:
        return acumular(None, nuevos, fecha) if len(nuevos) else anterior
    anterior = _con_meta(anterior)
    dentro = anterior["pdf_filename"].isin(pdfs).to_numpy()
    acumuladas = acumular(anterior[dentro], nuevos, fecha, permitir_vacio=True)
    return pd.concat([anterior[~dentro], acumuladas], ignore_index=True, sort=False)


def _tabla_semilla(ruta: Path) -> str:
    """'empresas' o 'cargos' según las columnas del parquet publicado."""
    columnas = set(pq.read_schema(ruta).names)
    faltan = [c for c in CLAVE_SEMILLA if c not in columnas]
    if faltan:
        raise ValueError(f"{ruta}: la semilla no tiene las columnas de la clave {faltan}")
    return "cargos" if {"cargo", "tipo_acto"} <= columnas else "empresas"


def _leer_filas(ruta: Path, posiciones: np.ndarray) -> pd.DataFrame:
    """Filas `posiciones` (ordenadas) de un parquet, leído por grupos de filas."""
    archivo = pq.ParquetFile(ruta)
    partes, inicio = [], 0
    for i in range(archivo.metadata.num_row_groups):
        n = archivo.metadata.row_group(i).num_rows
        sel = posiciones[(posiciones >= inicio) & (posiciones < inicio + n)] - inicio
        if len(sel):
            partes.append(archivo.read_row_group(i).take(pa.array(sel)))
        inicio += n
    return pa.concat_tables(partes).to_pandas()


def _sembrar(salida, ruta: Path, origen: str = ORIGEN_SEMILLA):
    """Añade las filas de la semilla `ruta` (un parquet publicado de empresas o
    de cargos) cuyo acto (CLAVE_SEMILLA) no está en `salida` con
    comun.historico.sembrar: _origen=origen y _en_ultima_descarga=False. Nunca
    modifica ni duplica una fila de la salida, y sembrar dos veces no añade nada
    (la salida ya incluye lo sembrado antes).

    Ámbito: toda la semilla. En otras fuentes solo se siembra lo que se ha vuelto
    a descargar porque fuera de ello no se sabe si la administración lo sigue
    publicando; el BORME nunca se retira, así que un acto del release que no está
    es un PDF que falta en disco (o que el parser actual no lee igual), y es
    justo lo que hay que conservar.

    A sembrar() se le pasa solo la clave (con la posición de cada fila) y luego
    se leen del parquet solo las filas que añade: las tablas publicadas tienen
    9,2M y 17M filas. Las de cargos traen persona_hash en vez de persona
    (borme_anonymize.py lo conserva) y las de empresas no traen objeto_social."""
    columnas = pq.read_schema(ruta).names
    claves = pd.read_parquet(ruta, columns=CLAVE_SEMILLA + [c for c in ("_origen",) if c in columnas])
    claves[COLUMNA_POSICION] = np.arange(len(claves))
    base = (salida[CLAVE_SEMILLA] if salida is not None and len(salida)
            else pd.DataFrame({c: pd.Series(dtype=object) for c in CLAVE_SEMILLA}))
    resultado, informe = sembrar(base, claves, CLAVE_SEMILLA, origen=origen, contenido=[])
    marcas = resultado.iloc[len(base):]
    if not len(marcas):
        return salida, informe
    nuevas = _leer_filas(ruta, marcas[COLUMNA_POSICION].to_numpy(dtype="int64"))
    nuevas["_origen"] = marcas["_origen"].to_numpy()
    nuevas["_en_ultima_descarga"] = False
    if salida is None or len(salida) == 0:
        return nuevas, informe
    if "_origen" not in salida.columns:
        salida = salida.assign(_origen=pd.Series([None] * len(salida), index=salida.index, dtype=object))
    orden = list(salida.columns) + [c for c in nuevas.columns if c not in salida.columns]
    out = pd.concat([salida, nuevas], ignore_index=True, sort=False)[orden]
    out["_en_ultima_descarga"] = out["_en_ultima_descarga"].astype(bool)
    return out, informe


def run_batch(base_dir: Path, output_dir: Path, workers: int = 8,
              resume: bool = False, reprocesar: bool = False, semillas=()):
    base_dir, output_dir = Path(base_dir), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    empresas_parquet = output_dir / EMPRESAS_PARQUET
    cargos_parquet = output_dir / CARGOS_PARQUET
    progress_file = output_dir / PROGRESO
    # Filas de cada batch, guardadas antes de su lote: así --resume no pierde lo
    # procesado en una ejecución interrumpida
    parts_dir = output_dir / PARTES

    log.info(f"Buscando BORME-A PDFs en {base_dir}...")
    pdfs = _versiones_pdf(base_dir)
    n_actuales = sum(d.exists() for d in pdfs)
    n_copias = sum(len(v) for v in pdfs.values()) - n_actuales
    log.info(f"   Encontrados: {n_actuales:,} PDFs"
             + (f" y {n_copias:,} versiones anteriores en {HISTORICO}/" if n_copias else ""))

    progreso = _leer_json(progress_file)
    registro = dict(progreso.get("versiones") or {})
    filas_registro = dict(progreso.get("filas") or {})
    validas = {} if reprocesar else _registro_en_salida(
        registro, filas_registro, base_dir, [empresas_parquet, cargos_parquet])
    pendientes, movidas = _pendientes(pdfs, base_dir, validas, reprocesar)
    # Versiones que se acumulan en esta ejecución: ruta relativa -> (ronda, ruta,
    # firma). La ronda es su posición entre las pendientes de su PDF: la ronda 0
    # se acumula antes que la 1 (versiones de _historico/), etc.
    plan = {_relativa(v, base_dir): (ronda, v, _firma(v))
            for lista in pendientes.values() for ronda, v in enumerate(lista)}

    # Versiones ya parseadas: las de los lotes de una ejecución interrumpida
    # (--resume) y luego las de esta
    parseadas = {}
    if parts_dir.exists():
        if resume:
            for lote in sorted(parts_dir.glob("lote_*.json")):
                for rel, info in (_leer_json(lote).get("versiones") or {}).items():
                    if rel in plan and info.get("firma") == plan[rel][2]:
                        parseadas[rel] = info
        else:
            shutil.rmtree(parts_dir)  # sin --resume: descartar parciales antiguos

    tareas = sorted((ronda, v) for rel, (ronda, v, _) in plan.items() if rel not in parseadas)
    log.info(f"   Pendientes: {len(tareas):,}"
             + (f" (y {len(parseadas):,} ya parseados antes de una interrupción)" if parseadas else ""))
    if not tareas and not parseadas and not semillas:
        log.info("Nada que procesar.")
        return

    parts_dir.mkdir(parents=True, exist_ok=True)
    t0 = datetime.now()
    fecha = _ahora()
    errores = []
    n_empresas = n_cargos = 0
    total = len(tareas)

    for batch_start in range(0, total, BATCH_SIZE):
        batch = tareas[batch_start:batch_start + BATCH_SIZE]
        resultados = {}

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one, str(p)): p for _, p in batch}
            for processed, future in enumerate(as_completed(futures), batch_start + 1):
                e_rows, c_rows, _, ok = future.result()
                if ok:
                    resultados[futures[future]] = (e_rows, c_rows)
                else:
                    errores.append(_relativa(futures[future], base_dir))

                if processed % 500 == 0:
                    elapsed = (datetime.now() - t0).total_seconds()
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / rate if rate > 0 else 0
                    log.info(
                        f"   {processed:,}/{total:,} "
                        f"({processed / total * 100:.1f}%) "
                        f"| {rate:.0f} PDFs/s "
                        f"| ETA: {eta / 60:.0f}min"
                    )

        # Filas del batch en el orden de los PDF (el de siempre con un worker) y,
        # después, su lote: un lote sin su JSON no cuenta como parseado. El tag no
        # puede coincidir con el de un lote que se está reutilizando (--resume)
        tag = f"{t0:%Y%m%d%H%M%S%f}_{batch_start:07d}"
        while any(parts_dir.glob(f"*_{tag}.*")):
            tag += "b"
        filas = {"empresas": ([], []), "cargos": ([], [])}
        lote = {}
        for _, p in batch:
            if p not in resultados:
                continue
            rel = _relativa(p, base_dir)
            for nombre, rows in zip(filas, resultados[p]):
                filas[nombre][0].extend(rows)
                filas[nombre][1].extend([rel] * len(rows))
            lote[rel] = {"firma": plan[rel][2], "pdf": _nombre_pdf(p), "empresas": len(resultados[p][0]),
                         "cargos": len(resultados[p][1]), "tag": tag}
        for nombre, (rows, vers) in filas.items():
            if rows:
                df = pd.DataFrame(rows)
                df[COLUMNA_VERSION] = vers
                df.to_parquet(parts_dir / f"{nombre}_{tag}.parquet", index=False, engine="pyarrow")
        _escribir_json(parts_dir / f"lote_{tag}.json", {"versiones": lote})
        parseadas.update(lote)
        n_empresas += len(filas["empresas"][0])
        n_cargos += len(filas["cargos"][0])
        log.info(f"   Batch guardado ({batch_start + len(batch):,} procesados "
                 f"| empresas: {n_empresas:,} | cargos: {n_cargos:,})")

    # Por PDF, las versiones parseadas en orden hasta la primera que ha fallado:
    # las posteriores esperan a la siguiente ejecución (acumularlas antes dejaría
    # vigente una versión antigua)
    acumuladas = {}
    for lista in pendientes.values():
        for ronda, v in enumerate(lista):
            rel = _relativa(v, base_dir)
            if rel not in parseadas:
                break
            acumuladas[rel] = (ronda, parseadas[rel])

    log.info("Construyendo DataFrames...")
    orden = {rel: i for i, rel in enumerate(sorted(acumuladas, key=lambda r: (acumuladas[r][0], plan[r][1])))}

    def filas_partes(prefijo):
        """Filas de las partes de las versiones que se acumulan, en el orden de los PDF."""
        por_tag = {}
        for rel, (_, info) in acumuladas.items():
            por_tag.setdefault(info["tag"], set()).add(rel)
        frames = []
        for tag, rels in sorted(por_tag.items()):
            ruta = parts_dir / f"{prefijo}_{tag}.parquet"
            if ruta.exists():
                df = pd.read_parquet(ruta)
                df = df[df[COLUMNA_VERSION].isin(rels)]
                if len(df):
                    frames.append(df)
        if not frames:
            return pd.DataFrame({COLUMNA_VERSION: pd.Series(dtype=object)})
        df = pd.concat(frames, ignore_index=True)
        posicion = df[COLUMNA_VERSION].map(orden).to_numpy()
        return df.iloc[np.argsort(posicion, kind="stable")].reset_index(drop=True)

    df_empresas = leer_registros(empresas_parquet)
    df_cargos = leer_registros(cargos_parquet)
    cambio = {"empresas": False, "cargos": False}
    empresas, cargos = filas_partes("empresas"), filas_partes("cargos")
    for ronda in sorted({r for r, _ in acumuladas.values()}):
        rels = {rel for rel, (r, _) in acumuladas.items() if r == ronda}
        # Ámbito: PDF de la ronda con alguna fila (un parse vacío no retira nada)
        ambito = {info["pdf"] for rel, (r, info) in acumuladas.items() if r == ronda and info["empresas"]}
        nuevos_emp = _preparar(empresas, rels, DEDUP_EMPRESAS, "Empresas")
        nuevos_car = _preparar(cargos, rels, DEDUP_CARGOS, "Cargos")
        if len(nuevos_emp) or ambito:
            df_empresas = _acumular_pdfs(df_empresas, nuevos_emp, fecha, ambito)
            cambio["empresas"] = True
        if len(nuevos_car) or ambito:
            df_cargos = _acumular_pdfs(df_cargos, nuevos_car, fecha, ambito)
            cambio["cargos"] = True
    del empresas, cargos

    for ruta in semillas:
        ruta = Path(ruta)
        tabla = _tabla_semilla(ruta)
        if tabla == "cargos":
            df_cargos, informe = _sembrar(df_cargos, ruta)
        else:
            df_empresas, informe = _sembrar(df_empresas, ruta)
        cambio[tabla] |= informe["anadidas"] > 0
        informe["ruta"] = f"{ruta} ({tabla})"
        imprimir_informe_semilla(informe)

    for nombre, df, ruta in (("empresas", df_empresas, empresas_parquet), ("cargos", df_cargos, cargos_parquet)):
        if cambio[nombre] and df is not None and len(df) > 0:
            estado = guardar_registros(df, ruta)
            log.info(f"   {ruta} ({ruta.stat().st_size / 1e6:.1f} MB): {estado}")

    # Registro de lo acumulado, después de escribir las tablas: si se corta antes,
    # la siguiente ejecución lo vuelve a parsear y acumular (no se duplica nada)
    for rel, (firma, rel_pdf) in movidas.items():
        registro[rel] = firma
        if rel_pdf in filas_registro:
            filas_registro[rel] = filas_registro[rel_pdf]
    for rel, (_, info) in acumuladas.items():
        registro[rel] = info["firma"]
        filas_registro[rel] = [info["empresas"], info.get("cargos", 0)]
    _escribir_json(progress_file, {"done": sorted(registro), "errors": sorted(errores),
                                   "versiones": registro, "filas": filas_registro})
    # Las tablas ya contienen todas las filas: los parciales sobran
    shutil.rmtree(parts_dir, ignore_errors=True)

    # Resumen
    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"\n{'=' * 60}")
    log.info(f"COMPLETADO en {elapsed / 60:.1f} minutos")
    log.info(f"   PDFs procesados: {total:,}")
    log.info(f"   Errores: {len(errores):,}")
    for nombre, df in (("Empresas", df_empresas), ("Cargos", df_cargos)):
        if df is None or len(df) == 0:
            continue
        log.info(f"   {nombre} (filas): {len(df):,}")
        if "_en_ultima_descarga" in df.columns:
            log.info(f"      del último parse de su PDF: {int(df['_en_ultima_descarga'].sum()):,}")
        if "_origen" in df.columns:
            log.info(f"      de la semilla: {int(df['_origen'].notna().sum()):,}")
    if df_empresas is not None and len(df_empresas) > 0:
        log.info(f"   Empresas unicas: {df_empresas['empresa_norm'].nunique():,}")
        log.info(f"   Provincias: {df_empresas['provincia'].nunique()}")
        log.info(f"   Rango fechas: {df_empresas['fecha_borme'].min()} -> {df_empresas['fecha_borme'].max()}")
        constit = df_empresas[df_empresas["actos"].str.contains("Constitución", na=False)]
        log.info(f"   Constituciones: {len(constit):,}")
        if "capital_euros" in df_empresas.columns:
            with_capital = df_empresas["capital_euros"].notna().sum()
            log.info(f"   Con capital: {with_capital:,}")
    if df_cargos is not None and len(df_cargos) > 0:
        log.info(f"   Cargos unicos (tipos): {df_cargos['cargo'].nunique()}")
        if "persona" in df_cargos.columns:
            log.info(f"   Personas unicas: {df_cargos['persona'].nunique():,}")
        for tipo, n in df_cargos['tipo_acto'].value_counts().items():
            log.info(f"      {tipo}: {n:,}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BORME Batch Parser v2")
    parser.add_argument("--input", required=True, help="Carpeta raiz de borme_pdfs")
    parser.add_argument("--output", default=None, help="Carpeta de salida (default: input)")
    parser.add_argument("--workers", type=int, default=8, help="Procesos paralelos")
    parser.add_argument("--resume", action="store_true",
                        help="Aprovechar lo ya parseado por una ejecución interrumpida (borme_parse_parts/)")
    parser.add_argument("--reprocesar", action="store_true",
                        help=("Volver a parsear todos los PDF en disco con el código actual (p.ej. tras "
                              "corregir el parser). Lo que cambie queda como versión anterior "
                              "(_en_ultima_descarga=False); lo de los PDF que no están en disco se conserva. "
                              "Si se interrumpe, repetirlo con --reprocesar --resume"))
    parser.add_argument("--semilla", type=Path, action="append", default=[],
                        help=("Parquet publicado (borme_empresas_pub.parquet o borme_cargos_pub.parquet del "
                              "release v2026.02): añade los actos (pdf_filename, num_entrada) que no salen "
                              "del parse, con _origen. Se puede repetir"))
    args = parser.parse_args()

    base = Path(args.input)
    out = Path(args.output) if args.output else base
    run_batch(base, out, workers=args.workers, resume=args.resume,
              reprocesar=args.reprocesar, semillas=args.semilla)
