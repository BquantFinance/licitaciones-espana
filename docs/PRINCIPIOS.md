# Principios del proyecto

Reglas que cumple todo scraper y todo cambio del repositorio. El código las cita por su número (p. ej. «docs/PRINCIPIOS.md, regla 3»).

1. **Servir exactamente lo que publica la administración.** No limpiar valores ni eliminar filas de origen, aunque estén duplicadas: se marcan con columnas `_...`. Solo se descartan los artefactos de nuestra propia descarga, como consultas solapadas, y siempre documentado. El objetivo del proyecto es enseñar cómo publica la administración.
2. **Nunca perder datos.**
   - Leer como texto (`dtype=str`) cuando haya duda.
   - Conservar todas las columnas.
   - Ningún valor pasa a NaN en silencio.
   - Nada de `on_bad_lines='skip'` sin guardar lo descartado.
3. **Sesgo del superviviente: nunca re-descargar y machacar a ciegas; siempre completar lo que falta.** Se aplica con `comun/historico.py` (tres niveles, más la semilla del punto 4):
   - **Capa cruda:** toda escritura de una descarga que pueda existir pasa por `guardar_version`; la versión anterior va a `_historico/`.
   - **Salidas:** se construyen con el código actual desde **todas** las versiones de los crudos (`versiones()`), usando `acumular(anterior, nuevos, fecha, ambito=...)`. Los registros retirados o modificados se conservan con `_en_ultima_descarga=False`. Una descarga vacía o fallida no retira nada.
   - **Re-ejecuciones:** incrementales y reanudables.
4. **El release v2026.02 y el LFS del repo son la única copia histórica**.
   - No modificar ni borrar ficheros de datos.
   - Cada scraper debe aceptar `--semilla <parquet publicado>`. Añade, **por clave estable**, solo las claves que no aparecen en la nueva descarga, marcadas con `_origen='release v2026.02'` y `_en_ultima_descarga=False`. Nunca modifica ni duplica filas nuevas. Hay que documentar los errores conocidos del publicado de esa fuente (ver README).
   - Los parquet publicados se descargan de `https://media.githubusercontent.com/media/BquantFinance/licitaciones-espana/main/<ruta>`.
5. **Tests con pandas 3 y pandas 2.2**, en un venv aparte (`pip install "pandas<3"`). La suite completa en verde antes de cada commit.
6. **Git.**
   - No usar `git stash`, `checkout` ni `reset` con agentes trabajando en el mismo árbol.
   - Los agentes no commitean; commitea la sesión principal.
   - Commit y push al cerrar cada bloque.
   - `ted/*.py` y `scripts/ccaa_asturias.py` usan CRLF: hay que conservarlo.
7. **Verificar dos veces con datos reales siempre que se pueda.** El patrón probado para no perder nada:
   - Comparar, fichero a fichero, el número de filas y de celdas con valor entre el original y la salida.
   - Clasificar los valores por tipo (URL, fecha, id, texto) en cada columna para detectar columnas corridas.
