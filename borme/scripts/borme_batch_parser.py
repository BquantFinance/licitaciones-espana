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

Basado en datos de la Agencia Estatal Boletin Oficial del Estado (https://www.boe.es)
"""

import re
import json
import logging
import argparse
import pdfplumber
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    import pandas as pd
except ImportError:
    print("pip install pandas pyarrow")
    raise

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

ENTRY_START_RE = re.compile(r'^(\d{4,7})\s*-\s*', re.MULTILINE)

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

DATOS_REG_RE = re.compile(
    r'Datos registrales[.:]\s*T\s*(\d+),\s*L\s*(\d+),\s*F\s*(\d+),\s*S\s*(\d+),'
    r'\s*H\s*([A-Z\s]*\d+),\s*I/A\s*(\d+)\s*\((\d{1,2}\.\d{2}\.\d{2,4})\)')
DOMICILIO_RE = re.compile(r'Domicilio[.:]\s*(.+?)(?:[.]\s*Capital[.:]|$)', re.DOTALL)
CAPITAL_RE = re.compile(r'Capital[.:]\s*([\d.,]+)\s*Euros')
OBJETO_RE = re.compile(r'Objeto social[.:]\s*(.+?)(?:[.]\s*Domicilio[.:]|$)', re.DOTALL)
COMIENZO_RE = re.compile(r'Comienzo de operaciones[.:]\s*([\d.]+)')

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
    fname = path.name

    bm = re.match(r'BORME-([A-Z])-(\d{4})-(\d+)-(\d+)', fname)
    if not bm:
        return [], []

    tipo = bm.group(1)
    year = int(bm.group(2))
    numero = int(bm.group(3))
    cod_prov = bm.group(4)
    provincia_filename = COD_PROVINCIA.get(cod_prov, "")

    # Fecha de la ruta
    parts = path.parts
    fecha_borme = f"{year}-01-01"
    try:
        for idx, p in enumerate(parts):
            if p.isdigit() and len(p) == 4 and 2000 <= int(p) <= 2030:
                if idx + 2 < len(parts):
                    fecha_borme = f"{parts[idx]}-{parts[idx+1].zfill(2)}-{parts[idx+2].zfill(2)}"
                break
    except:
        pass

    try:
        with pdfplumber.open(pdf_path) as pdf:
            raw = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception:
        return [], []

    text = _clean(raw)
    splits = list(ENTRY_START_RE.finditer(text))
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
            if first_word in _LEGAL_FORMS:
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
                r'Cambio de domicilio social[.:]\s*(.+?)(?:[.]\s*Datos registrales|$)', body)
            if dm:
                row["domicilio"] = dm.group(1).strip()[:300]

        m = CAPITAL_RE.search(body)
        if m:
            try:
                row["capital_euros"] = float(m.group(1).replace(".", "").replace(",", "."))
            except:
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
    except Exception:
        return [], [], pdf_path_str, False


def run_batch(base_dir: Path, output_dir: Path, workers: int = 8,
              resume: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)
    empresas_parquet = output_dir / "borme_empresas.parquet"
    cargos_parquet = output_dir / "borme_cargos.parquet"
    progress_file = output_dir / "borme_parse_progress.json"

    log.info(f"Buscando BORME-A PDFs en {base_dir}...")
    all_pdfs = find_borme_a_pdfs(base_dir)
    log.info(f"   Encontrados: {len(all_pdfs):,} PDFs")

    done_set = set()
    if resume and progress_file.exists():
        with open(progress_file) as f:
            done_set = set(json.load(f).get("done", []))
        log.info(f"   Resumiendo: {len(done_set):,} ya procesados")

    pending = [p for p in all_pdfs if str(p) not in done_set]
    log.info(f"   Pendientes: {len(pending):,}")

    if not pending:
        log.info("Nada que procesar.")
        return

    BATCH_SIZE = 5000
    all_empresas = []
    all_cargos = []
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
                        f"| empresas: {len(all_empresas) + len(batch_empresas):,} "
                        f"| cargos: {len(all_cargos) + len(batch_cargos):,}"
                    )

        all_empresas.extend(batch_empresas)
        all_cargos.extend(batch_cargos)

        with open(progress_file, "w") as f:
            json.dump({"done": list(done_set), "errors": errors}, f)
        log.info(f"   Batch guardado ({batch_start + len(batch):,} procesados)")

    # DataFrames
    log.info("Construyendo DataFrames...")

    df_empresas = pd.DataFrame(all_empresas)
    df_cargos = pd.DataFrame(all_cargos)

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
