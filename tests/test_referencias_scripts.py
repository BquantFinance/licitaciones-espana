"""Los comentarios y los docstrings de los .py no citan scripts que no existen.

Las cabeceras de cuatro scrapers autonómicos decían compartir su bloque de utilidades con un script
de Navarra que no se llegó a escribir, y el «Uso» de otros dos citaba scripts con otro nombre que
no están en el repositorio. Cada script citado en un comentario o en un docstring tiene que
estar en el repositorio (en cualquier carpeta). El código no se revisa: ahí un nombre de fichero es
un dato.
"""

import ast
import io
import re
import tokenize
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
PY = sorted(
    p for p in RAIZ.rglob("*.py")
    # fuera .git/, cachés de herramientas (.pytest_cache, .ruff_cache...) y entornos virtuales
    if not any(d.startswith(".") or "venv" in d for d in p.relative_to(RAIZ).parts[:-1])
)
EXISTENTES = {p.name for p in PY}
# Nombres de ejemplo, no de scripts: «python scripts/comprobar_ast.py main a.py b.py»
EJEMPLOS = {"a.py", "b.py"}
CITA = re.compile(r"(?<![\w.-])([A-Za-z_][\w-]*\.py)\b")
CON_DOCSTRING = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def textos_libres(codigo):
    """(línea, texto) de cada comentario y de cada docstring de un módulo."""
    for tok in tokenize.generate_tokens(io.StringIO(codigo).readline):
        if tok.type == tokenize.COMMENT:
            yield tok.start[0], tok.string
    for nodo in ast.walk(ast.parse(codigo)):
        if isinstance(nodo, CON_DOCSTRING) and nodo.body:
            primero = nodo.body[0]
            if (isinstance(primero, ast.Expr) and isinstance(primero.value, ast.Constant)
                    and isinstance(primero.value.value, str)):
                for i, linea in enumerate(primero.value.value.splitlines()):
                    yield primero.lineno + i, linea


def citas_rotas(codigo):
    """(línea, nombre) de los .py citados en comentarios y docstrings que no están en el repositorio."""
    return [(n, m.group(1)) for n, texto in textos_libres(codigo) for m in CITA.finditer(texto)
            if m.group(1) not in EXISTENTES | EJEMPLOS]


def test_detecta_una_cita_rota():
    codigo = ('"""Uso: python scripts/ccaa_murcia.py"""\n'
              "# mismo bloque en ccaa_navarra.py\n"
              "RUTA = 'no_es_un_comentario.py'\n")
    assert citas_rotas(codigo) == [(2, "ccaa_navarra.py")]


def test_comentarios_y_docstrings_no_citan_scripts_que_no_existen():
    rotas = [f"{p.relative_to(RAIZ)}:{n}: {nombre}"
             for p in PY for n, nombre in citas_rotas(p.read_text(encoding="utf-8"))]
    assert not rotas, "Citan un .py que no existe:\n" + "\n".join(rotas)
