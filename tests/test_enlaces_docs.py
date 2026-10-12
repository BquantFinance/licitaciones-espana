"""Cada enlace relativo y cada ancla de la documentación tiene que existir.

Revisa el README, los .md de la raíz (CONTRIBUTING, CHANGELOG...) y todos los de docs/:

- enlaces Markdown `[texto](destino)` e imágenes, definiciones `[ref]: destino` y los atributos `href`, `src` y
  `srcset` del HTML que llevan (el `<picture>` del logo);
- un destino relativo tiene que existir en el árbol (fichero o carpeta);
- un ancla (`#…`, sola o tras un .md) tiene que ser un encabezado del fichero de destino, con el mismo
  identificador que le da GitHub, o un `id`/`name` explícito.

Los enlaces externos (http, https, mailto) no se comprueban: los tests no usan la red. Lo que va dentro de un
bloque de código o de un `código` en línea no es un enlace.
"""

import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import unquote

import pytest

RAIZ = Path(__file__).resolve().parent.parent
DOCUMENTOS = sorted({*RAIZ.glob("*.md"), *(RAIZ / "docs").rglob("*.md")})

VALLA = re.compile(r"^ {0,3}(`{3,}|~{3,})")
ENCABEZADO = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.*?)(?:[ \t]+#+)?[ \t]*$")
ENLACE = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^)\s>]*)>?(?:\s+[\"'(][^)]*)?\s*\)")
DEFINICION = re.compile(r"^ {0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")
ATRIBUTO = re.compile(r"""\b(href|src|srcset)\s*=\s*["']([^"']+)["']""")
ID_HTML = re.compile(r"""<a\s[^>]*\b(?:id|name)\s*=\s*["']([^"']+)["']""")
CODIGO_EN_LINEA = re.compile(r"(`+)(?:(?!\1).)+?\1")
EXTERNO = re.compile(r"^[a-z][a-z0-9+.-]*:", re.I)


def lineas_sin_codigo(texto, quitar_codigo_en_linea=True):
    """(número de línea, línea) fuera de los bloques de código; por defecto, sin el código en línea."""
    valla = None
    for n, linea in enumerate(texto.splitlines(), 1):
        m = VALLA.match(linea)
        if valla:
            if m and m.group(1)[0] == valla[0] and len(m.group(1)) >= len(valla):
                valla = None
            continue
        if m:
            valla = m.group(1)
            continue
        if linea.startswith(("    ", "\t")) and not linea.strip().startswith(("-", "*", "|", "<")):
            continue  # bloque de código sangrado
        yield n, CODIGO_EN_LINEA.sub("", linea) if quitar_codigo_en_linea else linea


def texto_visible(titulo):
    """El texto de un encabezado tal como lo muestra GitHub: sin marcas de Markdown ni HTML. El código en
    línea se queda tal cual, y un '_' dentro de una palabra (snake_case) no es énfasis."""
    codigos = []

    def guardar(m):
        codigos.append(m.group(2))
        return f"\x00{len(codigos) - 1}\x00"

    t = re.sub(r"(`+)(.+?)\1", guardar, titulo)
    t = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", t)   # [texto](url) -> texto
    t = re.sub(r"\[([^\]]*)\](?:\[[^\]]*\])?", r"\1", t)  # [texto][ref] o [texto] -> texto
    t = re.sub(r"<[^>]+>", "", t)
    t = re.sub(r"(\*\*|\*|~~)(?=\S)(.+?)(?<=\S)\1", r"\2", t)
    t = re.sub(r"(?<!\w)(__|_)(?=\S)(.+?)(?<=\S)\1(?!\w)", r"\2", t)
    return re.sub(r"\x00(\d+)\x00", lambda m: codigos[int(m.group(1))].strip(), t)


def ancla_github(titulo):
    """El id que pone GitHub a un encabezado: minúsculas, fuera la puntuación y los símbolos (emojis
    incluidos; se quedan letras, números, marcas, '_', '-' y espacios) y cada espacio, un guion."""
    t = texto_visible(titulo).strip().lower()
    t = "".join(c for c in t if c in " -_" or unicodedata.category(c)[0] in "LNM")
    return t.replace(" ", "-")


def anclas(ruta):
    """Identificadores de los encabezados de un .md (con los sufijos -1, -2... de los repetidos) y los id HTML."""
    vistos, resultado = {}, set()
    for _, linea in lineas_sin_codigo(ruta.read_text(encoding="utf-8"), quitar_codigo_en_linea=False):
        resultado.update(ID_HTML.findall(CODIGO_EN_LINEA.sub("", linea)))
        m = ENCABEZADO.match(linea)
        if not m:
            continue
        base = ancla_github(m.group(2))
        n = vistos.get(base, 0)
        vistos[base] = n + 1
        resultado.add(base if n == 0 else f"{base}-{n}")
    return resultado


def destinos(ruta):
    """(línea, destino) de cada enlace, imagen, definición o atributo href/src/srcset del documento."""
    for n, linea in lineas_sin_codigo(ruta.read_text(encoding="utf-8")):
        for d in ENLACE.findall(linea):
            yield n, d
        m = DEFINICION.match(linea)
        if m:
            yield n, m.group(1)
        for atributo, valor in ATRIBUTO.findall(linea):
            for parte in valor.split(",") if atributo == "srcset" else [valor]:
                parte = parte.strip().split()[0] if parte.strip() else ""
                if parte:
                    yield n, parte


def problemas(ruta):
    """Lista de enlaces rotos de un documento: 'línea: destino (motivo)'."""
    malos, cache = [], {}
    for n, destino in destinos(ruta):
        if not destino or EXTERNO.match(destino) or destino.startswith("//"):
            continue
        camino, _, ancla = destino.partition("#")
        objetivo = (ruta.parent / unquote(camino)).resolve() if camino else ruta
        if camino:
            try:
                objetivo.relative_to(RAIZ)
            except ValueError:
                malos.append(f"{n}: {destino} (sale del repositorio)")
                continue
            if not objetivo.exists():
                malos.append(f"{n}: {destino} (no existe)")
                continue
        if ancla:
            if objetivo.suffix.lower() != ".md" or objetivo.is_dir():
                malos.append(f"{n}: {destino} (ancla en algo que no es un .md)")
                continue
            if objetivo not in cache:
                cache[objetivo] = anclas(objetivo)
            if unquote(ancla).lower() not in cache[objetivo]:
                malos.append(f"{n}: {destino} (no hay ese encabezado)")
    return malos


def test_hay_documentos():
    nombres = {p.relative_to(RAIZ).as_posix() for p in DOCUMENTOS}
    assert {"README.md", "CHANGELOG.md", "docs/REPRODUCIR.md", "docs/fuentes/README.md"} <= nombres


@pytest.mark.parametrize("ruta", DOCUMENTOS, ids=lambda p: p.relative_to(RAIZ).as_posix())
def test_enlaces_relativos_y_anclas_existen(ruta):
    malos = problemas(ruta)
    assert not malos, f"{ruta.relative_to(RAIZ)}: enlaces rotos\n" + "\n".join(malos)


# ── El comprobador, sobre casos conocidos ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("titulo, esperado", [
    # identificadores comprobados con el render de GitHub (API /markdown, modo gfm)
    ("Uso rápido", "uso-rápido"),
    ("Importes publicados y corregidos (issue #22)", "importes-publicados-y-corregidos-issue-22"),
    ("[v2026.10] - 2026-10-05", "v202610---2026-10-05"),
    ("0.1 Veredicto (2026-09-27)", "01-veredicto-2026-09-27"),
    ("🇪🇺 TED — Diario Oficial de la UE", "-ted--diario-oficial-de-la-ue"),
    ("Campos principales (18 columnas publicadas, más `_archivo_fuente` y las 3 de control)",
     "campos-principales-18-columnas-publicadas-más-_archivo_fuente-y-las-3-de-control"),
    ("Cruce PLACSP ↔ TED (cross-validation)", "cruce-placsp--ted-cross-validation"),
    ("Nacional y Unión Europea", "nacional-y-unión-europea"),
    ("v2026.10: `catalunya.zip`", "v202610-catalunyazip"),
    ("3. Contraste con `v2026.02` y con la PR #24", "3-contraste-con-v202602-y-con-la-pr-24"),
    ("Marcas es_ultima_version y _origen", "marcas-es_ultima_version-y-_origen"),
    ("Lo **importante** y lo *otro*", "lo-importante-y-lo-otro"),
])
def test_ancla_github(titulo, esperado):
    assert ancla_github(titulo) == esperado


def test_detecta_enlaces_rotos_y_respeta_el_codigo(tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "RAIZ", tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "otro.md").write_text("# Título\n\n## Sección uno\n\n## Sección uno\n", encoding="utf-8")
    doc = tmp_path / "LEEME.md"
    doc.write_text(
        "# Arriba\n\n"
        "[bien](docs/otro.md) [bien](docs/otro.md#sección-uno) [repetido](docs/otro.md#sección-uno-1)\n"
        "[bien](#arriba) [carpeta](docs/) <img src=\"docs/otro.md\">\n"
        "[externo](https://example.org/nada) [correo](mailto:a@b.c)\n"
        "[roto](docs/falta.md)\n"
        "[ancla rota](docs/otro.md#no-existe)\n"
        "<picture><source srcset=\"docs/falta.svg\"></picture>\n"
        "[fuera](../fuera.md)\n"
        "`[en código](docs/falta.md)`\n"
        "```\n[en un bloque](docs/falta.md)\n# no es un encabezado\n```\n"
        "[ref]: docs/tampoco.md\n",
        encoding="utf-8")
    malos = problemas(doc)
    assert [m.split(" (")[0] for m in malos] == [
        "6: docs/falta.md", "7: docs/otro.md#no-existe", "8: docs/falta.svg", "9: ../fuera.md",
        "15: docs/tampoco.md"]


def test_cero_enlaces(tmp_path, monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "RAIZ", tmp_path)
    doc = tmp_path / "vacio.md"
    doc.write_text("Sin enlaces.\n", encoding="utf-8")
    assert problemas(doc) == []
    assert anclas(doc) == set()
