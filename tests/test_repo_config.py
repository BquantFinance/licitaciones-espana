"""Los ficheros de configuración de git deben ser UTF-8 sin BOM (issue #2).

Guardados desde algunos editores/PowerShell de Windows acababan en UTF-16 o con
BOM, y git en macOS/Linux dejaba de aplicar las reglas (LFS para los parquet,
exclusiones de .gitignore).
"""

from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
CONFIG = sorted(
    p for patron in (".gitattributes", ".gitignore")
    for p in RAIZ.rglob(patron)
    # fuera .git/, cachés de herramientas (.pytest_cache, .ruff_cache...) y entornos virtuales
    if not any(d.startswith(".") or "venv" in d for d in p.relative_to(RAIZ).parts[:-1])
)


@pytest.mark.parametrize("path", CONFIG, ids=lambda p: str(p.relative_to(RAIZ)))
def test_config_git_es_utf8_sin_bom(path):
    datos = path.read_bytes()
    assert not datos.startswith(b"\xef\xbb\xbf"), "BOM UTF-8"
    assert not datos.startswith((b"\xff\xfe", b"\xfe\xff")), "UTF-16"
    assert b"\x00" not in datos, "bytes NUL (UTF-16)"
    assert b"\r" not in datos, "finales de línea CRLF"
    datos.decode("utf-8")


def test_parquet_en_lfs():
    reglas = (RAIZ / ".gitattributes").read_text(encoding="utf-8").splitlines()
    assert "*.parquet filter=lfs diff=lfs merge=lfs -text" in reglas
