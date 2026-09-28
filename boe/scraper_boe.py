#!/usr/bin/env python3
"""
Conector BOE – Boletín Oficial del Estado
==========================================
Sección V.A: "Anuncios - A. Contratación del Sector Público"

Adaptación a Python del conector BOE de Elicita (ingesta/connectors/boe.js),
que lleva funcionando en producción desde 2026.

Estrategia de ingesta (optimizada):
  1. API sumario diario:
     https://www.boe.es/datosabiertos/api/boe/sumario/YYYYMMDD
     → JSON con estructura diario/sección/departamento/item
  2. Filtramos sección código "5A" (V. Anuncios - A. Contratación del Sector Público)
  3. Descarga XML de cada item:
     https://www.boe.es/diario_boe/xml.php?id=<identificador>
  4. Parseo de los campos del contrato (organismo, expediente, objeto,
     importes, adjudicatario, NIF, CPVs, fechas, estado...)
  5. Resolución de NIFs enmascarados (ej: "****3443*") contra los
     adjudicatarios del propio dataset nacional (nombre + validación del
     fragmento visible del NIF)
  6. Cache: skip si el id ya está en el CSV de salida (reanudable)

Rate limit: ~1 req/segundo para no sobrecargar boe.es

    pip install requests pandas pyarrow
    python boe/scraper_boe.py --dias 7
    python boe/scraper_boe.py --dias 30 --salida contratos_boe
"""
import argparse
import json
import re
import time
from pathlib import Path

import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "boe"

BOE_API_BASE = "https://www.boe.es/datosabiertos/api/boe/sumario"
BOE_XML_BASE = "https://www.boe.es/diario_boe/xml.php?id="

DIAS_DEFAULT = 7          # días hábiles a cubrir (margen para ejecuciones cada 4h)
BATCH_DELAY = 1.0         # s entre lotes (rate limit ~1 req/s)
PARALLEL_BATCH = 3        # XMLs en paralelo
XML_TIMEOUT = 15          # s por request XML
MAX_RETRIES = 2

FINAL_CSV_NAME = "contratos_boe.csv"
FINAL_PARQUET_NAME = "contratos_boe.parquet"
DEFAULT_LOG_PATH = DATA_DIR / "scraper_boe.log"

UA = "BquantScraper/1.0 (+https://github.com/BquantFinance/licitaciones-espana)"

# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

_log_file = None

def log(msg):
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    if _log_file:
        try:
            with open(_log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def fetch_url(url, accept="application/json", timeout_s=15, retries=MAX_RETRIES):
    """GET con reintentos y seguimiento de redirects."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": UA, "Accept": accept},
                             timeout=timeout_s, allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 * attempt)
    raise last_err


def get_recent_workdays(days: int):
    """Los últimos `days` días hábiles (el BOE no publica sábado/domingo)."""
    dates = []
    d = pd.Timestamp.today().normalize()
    while len(dates) < days:
        if d.dayofweek < 5:
            dates.append(d.strftime("%Y%m%d"))
        d -= pd.Timedelta(days=1)
    return dates


def clean_text(s):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    for a, b in (("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"), ("&nbsp;", " ")):
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


def extract_field(xml, pattern):
    """Extrae el texto del siguiente <dd> tras un <dt> que matchea fieldPattern."""
    if not xml:
        return None
    flags_int = re.DOTALL | (re.IGNORECASE if pattern.flags & re.IGNORECASE else 0)
    m = re.search(
        r"<dt>\s*(?:<[^>]+\>\s*)?" + pattern.pattern + "(?::\\s*)?(?:\\s*</[^>]+>)?\\s*</dt>\\s*<dd>([\\s\\S]*?)</dd>",
        xml, flags_int)
    return clean_text(m.group(1)) if m else None


def extract_nested_field(xml, pattern, sub_pattern):
    """Extrae un campo anidado a partir de un <dt> contenedor con sub <dl>."""
    if not xml:
        return None
    flags_int = re.DOTALL | (re.IGNORECASE if pattern.flags & re.IGNORECASE else 0)
    m = re.search(
        r"<dt>\s*(?:<[^>]+\>\s*)?" + pattern.pattern + "(?::\\s*)?(?:\\s*</[^>]+>)?\\s*</dt>\\s*<dd>\\s*<dl>[\\s\\S]*?<dt>\\s*(?:<[^>]+\>\s*)?"
        + sub_pattern.pattern + "(?::\\s*)?(?:\\s*</[^>]+>)?\\s*</dt>\\s*<dd>(.*?)</dd>",
        xml, flags_int)
    return clean_text(m.group(1)) if m else None


def parse_spanish_money(text):
    """'7.864.767,00 euros' → 7864767.0"""
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d,]", "", text).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_fecha(str_):
    if not str_:
        return None
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", str_)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", str_):
        return str_[:10]
    if re.match(r"^\d{8}$", str_):
        return f"{str_[:4]}-{str_[4:6]}-{str_[6:8]}"
    return None


def parse_spanish_date(text):
    """'26 de junio de 2026' → '2026-06-26'"""
    if not text:
        return None
    meses = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
             "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
             "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"}
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text, re.I)
    if m:
        return f"{m.group(3)}-{meses.get(m.group(2).lower(), '01')}-{m.group(1).zfill(2)}"
    return None


def extract_cpvs_from_text(text):
    """'72500000 (Servicios informáticos)' → ['72500000']"""
    if not text:
        return []
    return list(dict.fromkeys(re.findall(r"\b(\d{8})\b", text)))

# ─────────────────────────────────────────────────────────────────────────────
# SUMARIO DIARIO
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sumario_items(fecha):
    """Los items de sección 5A del sumario del día (identificador, titulo, url)."""
    url = f"{BOE_API_BASE}/{fecha}"
    try:
        raw = fetch_url(url, accept="application/json", timeout_s=15)
        data = json.loads(raw)
    except Exception as e:
        log(f"  ✗ Sumario {fecha}: {e}")
        return []

    if (data.get("status") or {}).get("code") != "200":
        log(f"  ✗ Sumario {fecha}: status {data.get('status', {}).get('code')}")
        return []

    items = []
    diarios = data.get("data", {}).get("sumario", {}).get("diario") or []
    if not isinstance(diarios, list):
        diarios = [diarios]

    for diario in diarios:
        secciones = diario.get("seccion") or []
        if not isinstance(secciones, list):
            secciones = [secciones]
        for seccion in secciones:
            if seccion.get("codigo") != "5A":
                continue
            deps = seccion.get("departamento") or []
            if not isinstance(deps, list):
                deps = [deps]
            for dep in deps:
                all_items = []
                direct = dep.get("item")
                all_items += direct if isinstance(direct, list) else ([direct] if direct else [])
                for ep in dep.get("epigrafe") or []:
                    if not isinstance(ep, dict):
                        continue
                    ep_items = ep.get("item")
                    all_items += ep_items if isinstance(ep_items, list) else ([ep_items] if ep_items else [])
                for item in all_items:
                    if not item.get("identificador"):
                        continue
                    items.append({
                        "identificador": item["identificador"],
                        "titulo": item.get("titulo") or "",
                        "url_html": item.get("url_html") or f"https://www.boe.es/diario_boe/txt.php?id={item['identificador']}",
                        "fecha": fecha,
                    })
    return items

# ─────────────────────────────────────────────────────────────────────────────
# PARSEO DEL XML DEL CONTRATO
# ─────────────────────────────────────────────────────────────────────────────

def fetch_and_parse_item(item):
    xml_url = f"{BOE_XML_BASE}{item['identificador']}"
    try:
        xml = fetch_url(xml_url, accept="text/xml", timeout_s=XML_TIMEOUT, retries=1)
    except Exception as e:
        log(f"    ✗ XML {item['identificador']}: {e}")
        return None

    titulo = clean_text(item.get("titulo") or "")

    # Metadata <analisis>
    cpv_raw = (re.search(r"<materias_cpv>([\s\S]*?)</materias_cpv>", xml) or [None, ""])[1]
    modal_m = re.search(r'<modalidad codigo="([^"]*)">(.*?)</modalidad>', xml)
    modalidad_codigo = modal_m.group(1) if modal_m else ""
    modalidad_nombre = clean_text(modal_m.group(2)) if modal_m else ""
    tipo_m = re.search(r'<tipo codigo="([^"]*)">(.*?)</tipo>', xml)
    tipo_contrato = clean_text(tipo_m.group(2)) if tipo_m else ""

    # CPVs: del texto primero (más completo), fallback a <materias_cpv>
    cpv_field_codes = []
    cpv_text = extract_field(xml, re.compile(r"\d+\.\s*C[oó]digos?\s+CPV", re.I)) or cpv_raw
    if cpv_text:
        cpv_field_codes = extract_cpvs_from_text(cpv_text)
    if not cpv_field_codes and cpv_raw:
        cpv_field_codes = extract_cpvs_from_text(cpv_raw)

    # Organismo: preferir bloque 1.1) Nombre en el XML, fallback a <departamento>
    organismo = extract_field(xml, re.compile(r"1\.1\)\s*Nombre", re.I))
    if not organismo:
        org_m = re.search(r"<departamento[^>]*>(.*?)</departamento>", xml)
        organismo = clean_text(org_m.group(1)) if org_m else ""

    # Fecha de publicación
    fecha_pub_m = re.search(r"<fecha_publicacion>(\d{8})</fecha_publicacion>", xml)
    fecha_publicacion = parse_fecha(fecha_pub_m.group(1)) if fecha_pub_m else \
        re.sub(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", item.get("fecha", ""))

    # Expediente: del título (después de "Expediente: ")
    expediente = None
    exp_m = re.search(r"\bExpediente:\s*(.+?)(?:\s+\.|\.$|$)", titulo, re.I)
    if exp_m:
        expediente = exp_m.group(1).strip()
    else:
        texto_m = re.search(r"<texto>([\s\S]*?)</texto>", xml)
        exp_text = extract_field(texto_m.group(1) if texto_m else None, re.compile(r"\d+\.?\s*Expediente", re.I))
        if exp_text:
            expediente = exp_text

    # Objeto/descripción
    objeto = ""
    if modalidad_codigo == "L":
        desc = extract_field(xml, re.compile(r"7\.\s*Descripci[oó]n\s+de\s+la\s+licitaci[oó]n", re.I)) \
            or extract_field(xml, re.compile(r"6\.1\)\s*Descripci[oó]n\s+gen[eé]rica", re.I))
        if desc:
            objeto = desc
    elif modalidad_codigo == "F":
        obj_m = re.search(r"\bObjeto:\s*([^\.]+)", titulo, re.I)
        if obj_m:
            objeto = obj_m.group(1).strip()
    if not objeto:
        desc = extract_field(xml, re.compile(r"(?:6|7)\.\s*Descripci[oó]n\s+(?:gen[eé]rica\s+de\s+la\s+licitaci[oó]n|de\s+la\s+licitaci[oó]n|gen[eé]rica)", re.I))
        if desc:
            objeto = desc

    # Importe licitación (solo licitaciones)
    importe_licitacion = 0.0
    if modalidad_codigo == "L":
        valor_estimado = extract_field(xml, re.compile(r"8\.\s*Valor\s+estimado", re.I))
        if valor_estimado:
            importe_licitacion = parse_spanish_money(valor_estimado)

    # Importe adjudicación + adjudicatario (solo formalizaciones)
    importe_adjudicacion = 0.0
    adjudicatario = None
    nif_adjudicatario = None
    fecha_adjudicacion = None
    num_ofertas = None

    if modalidad_codigo == "F":
        oferta = extract_nested_field(xml, re.compile(r"13\.\s*Valor\s+de\s+las\s+ofertas", re.I),
                                      re.compile(r"13\.1\.1\)\s*Valor\s+de\s+la\s+oferta\s+seleccionada", re.I)) \
            or extract_field(xml, re.compile(r"13\.1\.1\)\s*Valor\s+de\s+la\s+oferta\s+seleccionada", re.I))
        if oferta:
            importe_adjudicacion = parse_spanish_money(oferta)

        adjudicatario = extract_nested_field(xml, re.compile(r"12\.\s*Adjudicatarios", re.I),
                                             re.compile(r"12\.1\.1\)\s*Nombre", re.I)) \
            or extract_field(xml, re.compile(r"12\.1\.1\)\s*Nombre", re.I))

        nif_adjudicatario = extract_nested_field(xml, re.compile(r"12\.\s*Adjudicatarios", re.I),
                                                 re.compile(r"12\.1\.2\)\s*N[uú]mero\s+de\s+identificaci[oó]n\s+fiscal", re.I)) \
            or extract_field(xml, re.compile(r"12\.1\.2\)\s*N[uú]mero\s+de\s+identificaci[oó]n\s+fiscal", re.I))

        fa = extract_field(xml, re.compile(r"10\.\s*Fecha\s+de\s+adjudicaci[oó]n", re.I))
        if fa:
            fecha_adjudicacion = parse_spanish_date(fa)

        ofertas_text = extract_nested_field(xml, re.compile(r"11\.\s*Ofertas\s+recibidas", re.I),
                                            re.compile(r"11\.1\.1\)\s*N[uú]mero\s+de\s+ofertas\s+recibidas", re.I)) \
            or extract_field(xml, re.compile(r"11\.1\.1\)\s*N[uú]mero\s+de\s+ofertas\s+recibidas", re.I))
        if ofertas_text:
            try:
                num_ofertas = int(re.sub(r"\D", "", ofertas_text))
            except ValueError:
                num_ofertas = None

    # Fecha fin presentación (solo licitaciones)
    fecha_fin = None
    if modalidad_codigo == "L":
        plazo = extract_field(xml, re.compile(r"19\.\s*Plazo\s+para\s+la\s+recepci[oó]n(?:\s+de\s+ofertas)?(?:\s+o\s+solicitudes\s+de\s+participaci[oó]n)?", re.I))
        if plazo:
            fecha_fin = parse_spanish_date(plazo)
            if not fecha_fin:
                m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", plazo)
                if m:
                    fecha_fin = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"

    # Estado
    estado = "abierta"
    if modalidad_codigo == "F":
        estado = "formalizada"
    elif modalidad_codigo == "P":
        estado = "preparacion"

    return {
        "id": f"BOE:{item['identificador']}",
        "fuente": "BOE",
        "expediente": expediente or None,
        "titulo": titulo,
        "organismo": organismo,
        "objeto": objeto or titulo,
        "cpvs": list(dict.fromkeys(cpv_field_codes)),
        "importe_licitacion": importe_licitacion,
        "importe_adjudicacion": importe_adjudicacion,
        "fecha_publicacion": fecha_publicacion,
        "fecha_fin": fecha_fin,
        "fecha_adjudicacion": fecha_adjudicacion,
        "estado": estado,
        "url_fuente": item.get("url_html"),
        "adjudicatario": adjudicatario,
        "nif_adjudicatario": nif_adjudicatario,
        "_nif_boe_raw": nif_adjudicatario,
        "_tipo_contrato": tipo_contrato,
        "_modalidad": modalidad_nombre or modalidad_codigo,
        "_num_ofertas": num_ofertas,
        "_cpv_boe_raw": cpv_raw or None,
    }

# ─────────────────────────────────────────────────────────────────────────────
# RESOLUCIÓN DE NIFs ENMASCARADOS (contra el dataset nacional del propio repo)
# ─────────────────────────────────────────────────────────────────────────────

def normalize_name(name):
    if not name:
        return ""
    s = re.sub(r"[,.]", "", name)
    for pat in (r"\bS\.?L\.?U\.?\b", r"\bS\.?A\.?U\.?\b", r"\bS\.?L\.?\b",
                r"\bS\.?A\.?\b", r"\bS\.?C\.?\b", r"\bS\.?L\.?P\.?\b"):
        s = re.sub(pat, "", s, flags=re.I)
    return re.sub(r"\s+", " ", s.upper()).strip()


def load_resolver_table(nacional_parquet):
    """Los adjudicatarios+NIFs del dataset nacional (la tabla de resolución)."""
    df = pd.read_parquet(nacional_parquet, columns=["adjudicatario", "nif_adjudicatario"])
    df = df.dropna()
    df = df[df["nif_adjudicatario"].astype(str).str.len() >= 9]
    df["norm"] = df["adjudicatario"].map(normalize_name)
    return df.drop_duplicates("norm")[["norm", "nif_adjudicatario", "adjudicatario"]]


def resolve_masked_nif(masked_nif, adjudicatario_name, resolver):
    """Resuelve '****3443*' contra el dataset nacional por nombre + fragmento visible."""
    if not masked_nif or "*" not in masked_nif:
        return masked_nif, False
    if not adjudicatario_name:
        return masked_nif, False
    norm = normalize_name(adjudicatario_name)
    if len(norm) < 3:
        return masked_nif, False

    candidates = resolver[resolver["norm"] == norm]
    if candidates.empty:
        candidates = resolver[resolver["norm"].str.contains(norm, regex=False, na=False)]
    if candidates.empty:
        return masked_nif, False

    clean_masked = re.sub(r"[^*A-Z0-9]", "", masked_nif).upper()
    for _, row in candidates.iterrows():
        canonical = re.sub(r"[^A-Z0-9]", "", str(row["nif_adjudicatario"])).upper()
        if len(canonical) != len(clean_masked):
            continue
        if all(m == "*" or m == c for m, c in zip(clean_masked, canonical)):
            return row["nif_adjudicatario"], True
    return masked_nif, False

# ─────────────────────────────────────────────────────────────────────────────
# BUCLE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def process_chunk(items):
    return [c for c in (fetch_and_parse_item(i) for i in items) if c]


def main():
    global _log_file

    ap = argparse.ArgumentParser()
    ap.add_argument("--dias", type=int, default=DIAS_DEFAULT, help="días hábiles a cubrir hacia atrás")
    ap.add_argument("--salida", default=str(DATA_DIR / FINAL_CSV_NAME), help="CSV de salida")
    ap.add_argument("--sin-nif-resolver", action="store_true", help="no resolver NIFs enmascarados")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    _log_file = DEFAULT_LOG_PATH
    log("=== Inicio ingesta BOE ===")

    ruta_salida = Path(args.salida)
    ruta_parquet = ruta_salida.with_suffix(".parquet")

    # Cache de ids ya procesados (reanudable)
    cached_ids = set()
    cached_contratos = []
    if ruta_salida.exists():
        try:
            existing = pd.read_csv(ruta_salida)
            cached_contratos = existing.to_dict("records")
            cached_ids = set(existing["id"].astype(str))
            log(f"Cache: {cached_ids.size if hasattr(cached_ids, '__len__') else len(cached_ids)} contratos ya procesados")
        except Exception as e:
            log(f"Cache: no legible ({e})")

    # Tabla de resolución de NIFs (del dataset nacional del propio repo)
    resolver = None
    if not args.sin_nif_resolver:
        nac = REPO_ROOT / "nacional" / "licitaciones_espana.parquet"
        if nac.exists():
            log("Cargando tabla de resolución de NIFs (nacional)...")
            resolver = load_resolver_table(nac)
            log(f"  {len(resolver):,} adjudicatarios únicos")

    workdays = get_recent_workdays(args.dias)
    log(f"Procesando {len(workdays)} días hábiles (últimos {args.dias} días)")

    nuevos = []
    vistos = set()

    for fecha in workdays:
        log(f"→ Sumario {fecha}")
        items = fetch_sumario_items(fecha)
        log(f"  5A items: {len(items)}")
        if not items:
            time.sleep(0.5)
            continue

        filtrados = []
        for item in items:
            ident = item["identificador"]
            if ident in vistos or f"BOE:{ident}" in cached_ids:
                vistos.add(ident)
                continue
            vistos.add(ident)
            filtrados.append(item)

        log(f"  Nuevos a procesar: {len(filtrados)} de {len(items)}")
        if not filtrados:
            time.sleep(0.5)
            continue

        for i in range(0, len(filtrados), PARALLEL_BATCH):
            chunk = filtrados[i:i + PARALLEL_BATCH]
            for c in process_chunk(chunk):
                if resolver is not None:
                    c["nif_adjudicatario"], resolved = resolve_masked_nif(
                        c["nif_adjudicatario"], c["adjudicatario"], resolver)
                nuevos.append(c)
                log(f"  ✓ {c['id'].replace('BOE:', '')} | CPV: {','.join(c['cpvs'])} | {c['titulo'][:60]}")
            if i + PARALLEL_BATCH < len(filtrados):
                time.sleep(BATCH_DELAY)

        time.sleep(0.5)

    log(f"Nuevos contratos encontrados: {len(nuevos)}")

    # Merge: nuevos + cache (los nuevos tienen prioridad por si actualizan)
    if cached_contratos:
        df_cached = pd.DataFrame(cached_contratos)
        df_new = pd.DataFrame(nuevos)
        df = pd.concat([df_new, df_cached]).drop_duplicates("id", keep="first")
    else:
        df = pd.DataFrame(nuevos)

    df.to_csv(ruta_salida, index=False)
    df.to_parquet(ruta_parquet, index=False)
    log(f"=== Fin ingesta BOE: {len(df)} contratos → {ruta_salida.name} + {ruta_parquet.name} ===")


if __name__ == "__main__":
    main()