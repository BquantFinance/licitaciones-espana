"""scripts/comprobar_ast.py: un cambio solo de comentarios o docstrings da el mismo AST;
cualquier cambio de código, no."""

import importlib.util
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("comprobar_ast", RAIZ / "scripts" / "comprobar_ast.py")
ca = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ca)

BASE = '''"""Módulo."""
import hashlib

SAL = "x"  # comentario


class C:
    """Clase."""

    def m(self):
        """Método."""
        return 1


def f(nombre, salt=SAL):
    """Código de un nombre."""
    # comentario
    return hashlib.sha256(f"{salt}:{nombre}".encode()).hexdigest()[:16]


async def g():
    """Corrutina."""
    return None
'''


def test_comentarios_y_docstrings_no_cuentan():
    otro = (BASE.replace("# comentario", "# otro comentario")
                .replace('"""Código de un nombre."""', '"""Otro docstring,\n    en dos líneas."""')
                .replace('"""Módulo."""', '"""Otro módulo."""')
                .replace('"""Clase."""', '"""Otra clase."""')
                .replace('"""Método."""', '"""Otro método."""')
                .replace('"""Corrutina."""', '"""Otra corrutina."""'))
    assert ca.huella(BASE) == ca.huella(otro)


def test_lineas_en_blanco_no_cuentan():
    assert ca.huella(BASE) == ca.huella(BASE.replace("\n\n\nclass", "\n\nclass"))


def test_quitar_o_anadir_un_docstring_no_cuenta():
    assert ca.huella(BASE) == ca.huella(BASE.replace('    """Código de un nombre."""\n', ""))


def test_cualquier_cambio_de_codigo_cuenta():
    for viejo, nuevo in [('SAL = "x"', 'SAL = "y"'),                # una constante
                         ("[:16]", "[:15]"),                        # un índice
                         ("return 1", "return 2"),                  # un valor devuelto
                         ("salt=SAL", "salt=None"),                 # un valor por defecto
                         ("import hashlib", "import hashlib, os")]:  # un import
        assert ca.huella(BASE) != ca.huella(BASE.replace(viejo, nuevo)), viejo


def test_una_cadena_que_no_es_docstring_cuenta():
    con_cadena = BASE.replace("    # comentario\n", '    "no es docstring"\n')
    assert ca.huella(BASE) != ca.huella(con_cadena)
