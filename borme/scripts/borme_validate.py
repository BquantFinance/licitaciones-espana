#!/usr/bin/env python3
"""
Validación del parser BORME v2 (regex genérico + approach B)
Ejecutar ANTES de relanzar el batch.

Uso: python borme_validate.py --input D:\Licitaciones\borme_pdfs --sample 300
"""

import re, glob, random, pdfplumber, argparse
from collections import Counter
from pathlib import Path

# ─── REGEX (idéntico al batch parser) ───
ENTRY_START_RE = re.compile(r'^(\d{4,7})\s*-\s*', re.MULTILINE)

_BODY_START_RE = re.compile(r'(?<=\.)\s+(?=[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{2,})')
_FE_ERRATAS_RE = re.compile(r'\.\s+Fe de erratas')

_LEGAL_FORMS = frozenset({
    'Sociedad', 'Limitada', 'Anónima', 'Anonima', 'Cooperativa', 'Profesional',
    'Laboral', 'Deportiva', 'Unipersonal', 'Civil', 'Comanditaria', 'Colectiva',
    'Comandita', 'Agrupación', 'Agrupacion', 'Europea', 'Responsabilidad',
    'Sucursal', 'Nueva', 'Empresa',
})

CARGO_RE = re.compile(
    r'(?<=\.\s)'
    r'([A-Z][A-Za-záéíóúñ.\s/\-\d=]{0,25}?)'
    r':\s*'
    r'([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\d\s;,.\-]+?)'
    r'(?:\.\s|$)'
)

CARGO_EXCLUDE = frozenset({
    'Objeto social', 'Domicilio', 'Capital', 'Datos registrales',
    'ACTIVIDAD PRINCIPAL', 'Comienzo de operaciones',
    'Artículo de los estatutos', 'ARTICULO',
    'Otros conceptos', 'Sociedades absorbidas', 'Resoluciones',
    'Denominación y forma adoptada',
})
CARGO_EXCLUDE_RE = re.compile(
    r'^(?:ART(?:ICULO|S)?[\s.\d,]+|CNAE\s|ACTIVIDAD)', re.IGNORECASE)

SECTION_MAP = {
    'Nombramientos': 'nombramiento',
    'Ceses/Dimisiones': 'cese',
    'Revocaciones': 'revocacion',
    'Reelecciones': 'reeleccion',
    'Cancelaciones de oficio de nombramientos': 'cancelacion',
}

_SKIP_LINES = frozenset([
    "SECCIÓN PRIMERA","Empresarios","Actos inscritos",
    "SECCIÓN SEGUNDA","Anuncios y avisos legales",
    "Otros actos publicados en el Registro Mercantil",
])


def clean(raw):
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


def extract_cargo_and_tipo(raw_cargo, body, match_start):
    c = raw_cargo.strip()
    for prefix, tipo in SECTION_MAP.items():
        if c.startswith(prefix + '. ') or c.startswith(prefix + '.'):
            cargo = c[len(prefix):].strip().lstrip('. ').strip()
            return cargo, tipo
    pre = body[:match_start]
    positions = {}
    for kw, tipo in SECTION_MAP.items():
        pos = pre.rfind(kw)
        if pos != -1:
            positions[tipo] = pos
    if positions:
        return c, max(positions, key=positions.get)
    return c, "nombramiento"


def find_empresa(block):
    """Approach B: empresa = ALL CAPS text before first mixed-case body word."""
    body_pos = len(block)

    fe = _FE_ERRATAS_RE.search(block)
    if fe:
        body_pos = fe.start() + 1

    for m in _BODY_START_RE.finditer(block):
        if m.start() + 1 >= body_pos:
            break
        rest = block[m.end():m.end() + 40]
        first_word = rest.split('.')[0].split(':')[0].split(' ')[0].strip()
        if first_word in _LEGAL_FORMS:
            continue
        body_pos = m.start() + 1
        break

    empresa = block[:body_pos].strip().rstrip('.')
    body = ". " + block[body_pos:].strip() if body_pos < len(block) else ""
    return empresa, body


def validate_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            raw = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except:
        return {"error": True, "path": str(pdf_path)}

    text = clean(raw)
    splits = list(ENTRY_START_RE.finditer(text))

    stats = {
        "path": str(pdf_path),
        "entries": len(splits),
        "cargos_found": 0,
        "cargo_types": Counter(),
        "tipo_actos": Counter(),
        "long_empresa_names": [],
        "unknown_cargo_patterns": [],
        "body_start_words": Counter(),
    }

    for i, match in enumerate(splits):
        start = match.end()
        end = splits[i+1].start() if i+1 < len(splits) else len(text)
        block = text[start:end].replace('\n', ' ')
        block = re.sub(r'\s{2,}', ' ', block).strip()

        empresa, body = find_empresa(block)

        # Track what word starts the body (for validation)
        if body and len(body) > 2:
            first_body_word = body[2:].split('.')[0].split(':')[0].split(' ')[0].strip()
            stats["body_start_words"][first_body_word] += 1

        if len(empresa) > 150:
            stats["long_empresa_names"].append(empresa[:200])

        # Extract cargos
        for m in CARGO_RE.finditer(body):
            raw_cargo = m.group(1).strip()
            personas_raw = m.group(2).strip()
            if raw_cargo in CARGO_EXCLUDE: continue
            if CARGO_EXCLUDE_RE.match(raw_cargo): continue
            words = [w for w in personas_raw.replace(';',' ').replace('.',' ').split() if len(w)>1]
            if len(words) < 2: continue
            cargo, tipo = extract_cargo_and_tipo(raw_cargo, body, m.start())
            if not cargo or cargo in CARGO_EXCLUDE or CARGO_EXCLUDE_RE.match(cargo): continue
            personas = [p.strip() for p in personas_raw.split(";") if p.strip()]
            stats["cargos_found"] += len(personas)
            stats["cargo_types"][cargo] += len(personas)
            stats["tipo_actos"][tipo] += len(personas)

        # Detect possible missed cargos
        POSSIBLE_CARGO = re.compile(r'(?<=\.\s)([^:]{1,30}?):\s*([A-ZÁÉÍÓÚÑ]{2,})')
        for pm in POSSIBLE_CARGO.finditer(body):
            candidate = pm.group(1).strip()
            if candidate in CARGO_EXCLUDE: continue
            if not CARGO_RE.search('. ' + pm.group(0) + '. '):
                stats["unknown_cargo_patterns"].append(candidate[:50])

    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--sample", type=int, default=300)
    args = parser.parse_args()

    pdfs = sorted(Path(args.input).rglob("BORME-A-*.pdf"))
    print(f"Total BORME-A PDFs: {len(pdfs):,}")

    sample = random.sample(pdfs, min(args.sample, len(pdfs)))
    print(f"Muestra: {len(sample)} PDFs\n")

    all_cargo_types = Counter()
    all_tipo_actos = Counter()
    all_unknown = Counter()
    all_body_words = Counter()
    total_entries = 0
    total_cargos = 0
    long_names = []
    errors = 0

    for i, pdf in enumerate(sample):
        stats = validate_pdf(pdf)
        if stats.get("error"):
            errors += 1
            continue
        total_entries += stats["entries"]
        total_cargos += stats["cargos_found"]
        all_cargo_types.update(stats["cargo_types"])
        all_tipo_actos.update(stats["tipo_actos"])
        long_names.extend(stats["long_empresa_names"])
        all_unknown.update(stats["unknown_cargo_patterns"])
        all_body_words.update(stats["body_start_words"])

        if (i+1) % 50 == 0:
            print(f"  {i+1}/{len(sample)}...")

    print(f"\n{'='*60}")
    print(f"RESULTADOS VALIDACIÓN v2")
    print(f"{'='*60}")
    print(f"PDFs procesados: {len(sample) - errors}")
    print(f"Errores: {errors}")
    print(f"Entradas totales: {total_entries:,}")
    print(f"Cargos capturados: {total_cargos:,}")

    print(f"\n--- BODY START WORDS (top 30) ---")
    for word, n in all_body_words.most_common(30):
        print(f"  {n:6,}x  {word}")

    print(f"\n--- TIPOS DE CARGO ({len(all_cargo_types)}) ---")
    for cargo, n in all_cargo_types.most_common(50):
        print(f"  {n:6,}x  {cargo}")

    print(f"\n--- DISTRIBUCIÓN TIPO ACTO ---")
    for tipo, n in all_tipo_actos.most_common():
        print(f"  {n:6,}x  {tipo}")

    if long_names:
        print(f"\n--- ⚠️  NOMBRES EMPRESA >150 CHARS ({len(long_names)}) ---")
        for name in long_names[:10]:
            print(f"  {name[:150]}...")
    else:
        print(f"\n--- ✅ NOMBRES EMPRESA: todos <150 chars ---")

    if all_unknown:
        print(f"\n--- ⚠️  CARGOS NO CAPTURADOS ({len(all_unknown)}) ---")
        for pattern, n in all_unknown.most_common(30):
            print(f"  {n:4}x  {pattern}")
    else:
        print(f"\n--- ✅ CARGOS: no se detectan patrones perdidos ---")

    if not long_names and not all_unknown:
        print(f"\n✅ VALIDACIÓN OK — listo para relanzar batch")
    else:
        print(f"\n⚠️  Revisar antes de relanzar")


if __name__ == "__main__":
    main()
