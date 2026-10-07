#!/usr/bin/env python3
"""Comprueba que un cambio en los .py solo toca comentarios y docstrings.

Compara el árbol sintáctico (``ast.dump``) de cada .py que cambia entre una
referencia de git y el árbol de trabajo, sin docstrings. Los comentarios no
están en el árbol sintáctico y los docstrings se quitan antes de comparar: si
sale idéntico, el código que se ejecuta es el mismo, así que el cambio no puede
alterar lo que descarga o escribe ningún script.

Uso:
    python scripts/comprobar_ast.py                # .py cambiados frente a origin/main
    python scripts/comprobar_ast.py main           # frente a otra referencia
    python scripts/comprobar_ast.py main a.py b.py # solo esos ficheros

Sale con 1 si algún fichero cambia de árbol o se borra, y con 0 si no. Un
fichero nuevo se lista aparte: no tiene versión anterior con la que comparar.
"""

import argparse
import ast
import subprocess
import sys
from pathlib import Path

CON_DOCSTRING = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def sin_docstrings(arbol: ast.AST) -> ast.AST:
    """Quita el docstring de cada módulo, clase y función (in situ)."""
    for nodo in ast.walk(arbol):
        if isinstance(nodo, CON_DOCSTRING) and nodo.body:
            primero = nodo.body[0]
            if (isinstance(primero, ast.Expr) and isinstance(primero.value, ast.Constant)
                    and isinstance(primero.value.value, str)):
                nodo.body = nodo.body[1:]
    return arbol


def huella(codigo: str, nombre: str = "<codigo>") -> str:
    """ast.dump del código sin docstrings (sin posiciones: no cuentan las líneas)."""
    return ast.dump(sin_docstrings(ast.parse(codigo, filename=nombre)))


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def comparar(ref: str, rutas: list[str]) -> dict[str, str]:
    """Estado de cada ruta: 'idéntico', 'DISTINTO', 'nuevo' o 'BORRADO'."""
    resultado = {}
    for ruta in rutas:
        antes = subprocess.run(["git", "show", f"{ref}:{ruta}"], capture_output=True, text=True)
        existe_antes = antes.returncode == 0
        existe_ahora = Path(ruta).is_file()
        if not existe_antes:
            resultado[ruta] = "nuevo" if existe_ahora else "BORRADO"
        elif not existe_ahora:
            resultado[ruta] = "BORRADO"
        else:
            ahora = Path(ruta).read_text(encoding="utf-8")
            iguales = huella(antes.stdout, ruta) == huella(ahora, ruta)
            resultado[ruta] = "idéntico" if iguales else "DISTINTO"
    return resultado


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Comprueba que los .py cambiados tienen el mismo AST sin docstrings.")
    parser.add_argument("ref", nargs="?", default="origin/main", help="referencia de git (por defecto, origin/main)")
    parser.add_argument("rutas", nargs="*", help="ficheros .py (por defecto, los que cambian frente a ref)")
    args = parser.parse_args(argv)

    rutas = args.rutas or sorted(
        set(_git("diff", "--name-only", args.ref, "--", "*.py").split())
        | set(_git("ls-files", "--others", "--exclude-standard", "--", "*.py").split()))
    if not rutas:
        print(f"Ningún .py cambia frente a {args.ref}.")
        return 0
    resultado = comparar(args.ref, rutas)
    ancho = max(len(r) for r in resultado)
    for ruta, estado in resultado.items():
        print(f"{ruta:<{ancho}}  {estado}")
    malos = [r for r, e in resultado.items() if e in ("DISTINTO", "BORRADO")]
    iguales = sum(e == "idéntico" for e in resultado.values())
    print(f"\n{iguales} idénticos, {len(malos)} distintos o borrados, "
          f"{sum(e == 'nuevo' for e in resultado.values())} nuevos (frente a {args.ref}).")
    return 1 if malos else 0


if __name__ == "__main__":
    sys.exit(main())
