# Cómo contribuir

Gracias por ayudar. Este repositorio descarga y publica la contratación pública española **tal como la publica cada administración**. Antes de cambiar nada, lee los [principios del proyecto](docs/PRINCIPIOS.md). En resumen:

1. **Servir lo publicado.** No se limpian valores ni se eliminan filas de origen. Lo raro se marca con columnas `_…` (`_duplicado`, `_columnas_corridas`, `_registro_irregular`…) y lo corregido va en una columna aparte (`<campo>_corregido`), junto al valor publicado.
2. **No perder datos.** Leer como texto cuando haya duda, conservar todas las columnas y no pasar nada a nulo en silencio.
3. **Sin sesgo del superviviente.** Las descargas pasan por [`comun/historico.py`](comun/historico.py) (`guardar_version`, `acumular`, `sembrar`): lo que la fuente retira se conserva, marcado con `_en_ultima_descarga=False`.
4. **Ningún cambio altera en silencio lo que descarga o escribe un scraper.** Si lo cambia, la PR lo dice y lo demuestra con tests y, si se puede, con la salida de antes y la de después.

## Preparar el entorno

```bash
# Solo el código: los datos actuales están en las releases, no en Git LFS
GIT_LFS_SKIP_SMUDGE=1 git clone https://github.com/BquantFinance/licitaciones-espana.git
cd licitaciones-espana
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt -c constraints.txt
```

- `requirements.txt` lleva rangos; `constraints.txt`, las versiones exactas con las que pasa la suite.
- Castilla-La Mancha publica algunos ficheros en RAR. Para leerlos hace falta libarchive (la usa `libarchive-c`) o una de `unar`, `unrar`, `bsdtar` o `7z` en el sistema.

## Tests

La suite no usa la red (los portales se simulan) y tarda unos minutos:

```bash
python -m pytest -q -p no:cacheprovider
```

- Tiene que pasar con **pandas 3 y con pandas 2.2.3**. Para la segunda, usa un venv aparte con `pip install "pandas==2.2.3"`: `pandas<3` instala la 2.3, que no es la que se prueba.
- La CI de GitHub Actions corre las dos versiones en cada PR.
- Lint mínimo, solo errores: `ruff check .`. No se reformatea el código: un formateador cambiaría todas las líneas de los scrapers.

## Cambios en un scraper

- Un cambio por rama y por PR, con su test. El test reproduce el formato real del portal (un fixture recortado) y prueba también el caso de cero resultados y el de un fallo de red.
- Los scrapers de la tabla de [estado](docs/ESTADO_SCRAPERS.md) están **cerrados**. Un cambio en uno de ellos actualiza su commit de cierre en esa tabla.
- Un cambio que solo toca comentarios o docstrings se demuestra con `python scripts/comprobar_ast.py origin/main` (el AST sin docstrings tiene que salir idéntico).
- `ted/*.py` y `scripts/ccaa_asturias.py` usan finales de línea CRLF: hay que conservarlos.
- Nada de rutas absolutas, nombres de máquinas, credenciales ni datos personales en el código, los tests o los fixtures. Si un fixture necesita un NIF, que sea inventado.

## Proponer una fuente nueva

Abre un issue con la plantilla **«Fuente nueva»**: portal, URL de descarga o API, formato, periodo, condiciones de reutilización del portal y si esos contratos ya están en la PLACSP. [`docs/COBERTURA.md`](docs/COBERTURA.md) recoge lo que ya se ha revisado por comunidad.

## Avisar de un error en los datos

Abre un issue con la plantilla **«Error en los datos»**.

- **Si el dato está mal en la fuente** (por ejemplo, un importe con la coma decimal perdida), el publicado no se toca. Una vez verificado, el error entra en [`calidad/errores_fuente.csv`](calidad/errores_fuente.csv) con su evidencia (`fuente`, `id`, `campo`, `valor_publicado`, `valor_probable`, `certeza`, `evidencia`, `referencia`, `verificado`), y [`calidad/correcciones.py`](calidad/correcciones.py) añade el valor corregido junto al publicado.
- **Si el error es nuestro** (el scraper lee mal un campo), la misma plantilla sirve: se trata como un bug del scraper.

## Licencia de las contribuciones

Al contribuir aceptas que tu código se publique con la [licencia MIT](LICENSE) del repositorio, y las columnas o correcciones de datos que añadas, con CC BY 4.0 (ver [`DATA_LICENSE.md`](DATA_LICENSE.md)).

## Conducta y seguridad

- Este proyecto sigue el [código de conducta](CODE_OF_CONDUCT.md) de Contributor Covenant.
- Una vulnerabilidad o un dato personal publicado por error no se avisa en un issue público: ver [`SECURITY.md`](SECURITY.md).
