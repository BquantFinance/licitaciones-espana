## Qué cambia

<!-- Qué hace la PR y por qué. Si cierra un issue: «Cierra #N». -->

## Cómo se ha comprobado

<!-- Tests nuevos, la suite, y la salida antes y después si cambia lo que escribe un scraper. -->

## Lista de comprobación

- [ ] La suite pasa con pandas 3 y con pandas 2.2.3 (`python -m pytest -q -p no:cacheprovider`).
- [ ] Se sirve lo que publica la fuente: no se limpian valores ni se borran filas de origen; lo raro se marca con columnas `_…`.
- [ ] No se pierden datos: nada pasa a nulo en silencio y lo que la fuente retira se conserva (`comun/historico.py`).
- [ ] Si cambia lo que descarga o escribe un scraper, la PR lo explica y lo demuestra; si no debería cambiar, se ha comprobado (p. ej. `python scripts/comprobar_ast.py origin/main` para cambios de comentarios).
- [ ] Si toca un scraper cerrado, `docs/ESTADO_SCRAPERS.md` tiene el nuevo commit de cierre.
- [ ] Sin rutas absolutas, credenciales ni datos personales en el código, los tests o los fixtures.
