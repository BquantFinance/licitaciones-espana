"""Configuración común de los tests.

Sin ficheros .pyc, como con PYTHONDONTWRITEBYTECODE=1: algunos tests copian un
script a una carpeta temporal, lo importan y comprueban que no se escribe nada
junto a él (p. ej. el log de Euskadi con el script en una carpeta de solo
lectura). Con la caché de bytecode, la importación dejaría ahí un __pycache__.
"""

import sys

sys.dont_write_bytecode = True
