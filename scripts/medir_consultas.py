#!/usr/bin/env python3
"""
=============================================================================
MEDIR CONSULTAS - ¿te basta tu ordenador para trabajar con estos datos?
=============================================================================
Lanza sobre la release descomprimida (o sobre la salida de los scripts de este
repo) las consultas que más se hacen con estos datos y mide cuánto tarda cada
una y cuánta memoria usa, con DuckDB y, si se pide, con pandas (leyendo con
pyarrow). Cada consulta cuenta como se debe: la última versión de cada
licitación de la PLACSP, cada aviso del TED una vez, sin sumar duplicados.

Ejecutar:  pip install duckdb pandas pyarrow
           python scripts/medir_consultas.py --datos CARPETA
               [--motor duckdb|pandas|ambos] [--hilos N] [--memoria 6GB]
               [--repeticiones 3] [--solo ID,ID] [--perfil "4 CPU / 8 GB"]
               [--nif A28541639] [--palabra ambulancia] [--json resultado.json]
           python scripts/medir_consultas.py --listar
           python scripts/medir_consultas.py --tabla docs/medidas/*.json

CARPETA es donde se han descomprimido los ZIP de la release, cada uno en su
carpeta (unzip X.zip -d X) o todos en la misma, o la carpeta de salida de los
scripts. Las tablas se buscan por nombre de fichero dentro de ella:

  licitaciones    licitaciones_completo/*.parquet (las 6 partes de los 5 ZIP
                  nacional_licitaciones_*.zip, juntas en una tabla) o, en la
                  salida del scraper, licitaciones_completo.parquet
  resultados      licitaciones_completo_resultados.parquet (nacional_resultados.zip)
  ted             ted_es_can.parquet (ted.zip)
  madrid          contratacion_comunidad_madrid_completo.parquet (comunidad_madrid.zip)
  pscp            publicaciones_pscp.parquet (catalunya.zip)
  borme_empresas  borme_empresas_pub.parquet (borme.zip)
  borme_cargos    borme_cargos_pub.parquet (borme.zip)

No se entra en carpetas que empiezan por «_» o «.» (como _historico/, las
versiones anteriores que guardan los scripts) ni en las vAAAA.MM/ que traen
algunos ZIP (ficheros de una release anterior, tal cual: sumarlos con los
actuales contaría dos veces). Una consulta cuya tabla no está, o a la que le
faltan columnas, se salta y se dice por qué.

Cómo se mide:
- Cada consulta corre en un proceso nuevo: «primera» es la primera vez en ese
  proceso (lee los metadatos y los datos del Parquet) y «repetida», la mediana
  de --repeticiones veces más en el mismo proceso. Tiempo de pared
  (time.perf_counter), contando traer el resultado a Python.
- Si el sistema operativo ya tiene los ficheros en su caché de disco, la
  primera vez no lee del disco. Para medir en frío, reinicia la máquina (o vacía
  la caché) antes de lanzarlo.
- «memoria_pico_mb» es el máximo de memoria residente del proceso de esa
  consulta (en Windows no se mide). Si el sistema mata el proceso por falta de
  memoria, o pasa de --tiempo-maximo, se apunta y se sigue con la siguiente.
- Por defecto, DuckDB usa tantos hilos como CPU tiene disponibles el proceso
  (también dentro de un contenedor con límite) y el 75 % de la memoria
  disponible; --hilos y --memoria los fijan. Con pandas, pyarrow lee con los
  mismos hilos.
- Con --motor ambos, cada resultado de pandas se compara con el de DuckDB
  («coincide_con_duckdb»): las dos formas de contar tienen que dar lo mismo.

Con --json se guarda todo: versiones (Python, DuckDB, pandas, pyarrow,
numpy), CPU, núcleos y memoria de la máquina y los disponibles, la
configuración de DuckDB, cada tabla (ficheros, filas y bytes) y cada consulta
(qué mide, SQL, tiempos, memoria y el resultado, hasta 250 filas).
Solo nombres de fichero, sin rutas. --tabla junta varios JSON en una tabla
Markdown (una columna por perfil de máquina).

Nada se escribe en la carpeta de datos: DuckDB lee los Parquet tal cual, sin
cargarlos enteros en memoria (si le falta, usa una carpeta temporal del
sistema). Los importes son los publicados, sin las correcciones de calidad/
(ver «Importes publicados y corregidos» en el README antes de sumar en serio).
"""
from __future__ import annotations

import argparse
import datetime as dt
import decimal
import glob
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

FORMATO = 1
MAX_FILAS_RESULTADO = 250   # el resultado entero en el JSON, salvo que pase de estas filas
RAIZ_REPO = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------------------------------------------------------------
# Tablas: cómo se encuentran en la carpeta de datos
# --------------------------------------------------------------------------------------------------------------------
CARPETA_PARTES = "licitaciones_completo"          # las 6 partes de la tabla principal de la PLACSP en la release
FICHERO_UNICO = "licitaciones_completo.parquet"   # la misma tabla en un fichero (salida del scraper)
NOMBRES = {
    "resultados": "licitaciones_completo_resultados.parquet",
    "ted": "ted_es_can.parquet",
    "madrid": "contratacion_comunidad_madrid_completo.parquet",
    "pscp": "publicaciones_pscp.parquet",
    "borme_empresas": "borme_empresas_pub.parquet",
    "borme_cargos": "borme_cargos_pub.parquet",
}
DESCRIPCION_TABLAS = {
    "licitaciones": "PLACSP, tabla principal: una fila por versión publicada de cada licitación",
    "resultados": "PLACSP, resultados por lote: una fila por resultado de cada versión",
    "ted": "TED, consolidado de España: una fila por resultado de lote de cada versión de cada aviso",
    "madrid": "Comunidad de Madrid: buscador de contratos públicos, tal como lo exporta",
    "pscp": "Catalunya, publicaciones de la PSCP (Socrata ybgg-dgi6)",
    "borme_empresas": "BORME, actos inscritos (una fila por inscripción)",
    "borme_cargos": "BORME, cargos (seudonimizados)",
}
RE_CARPETA_RELEASE = re.compile(r"v\d{4}\.\d{2}")   # vAAAA.MM/: ficheros de una release anterior dentro de un ZIP


@dataclass
class Datos:
    tablas: dict = field(default_factory=dict)      # clave -> [Path] (las partes, o un fichero)
    todos: list = field(default_factory=list)       # todos los Parquet (para «todo»)
    avisos: list = field(default_factory=list)
    saltadas: list = field(default_factory=list)    # carpetas en las que no se ha entrado (relativas)


def buscar_tablas(datos: Path) -> Datos:
    """Recorre `datos` y devuelve cada tabla con sus ficheros. Sin entrar en _*/, .*/ ni vAAAA.MM/."""
    d = Datos()
    partes: dict[str, list[Path]] = {}
    unico: list[Path] = []
    cand: dict[str, list[Path]] = {c: [] for c in NOMBRES}
    for raiz, carpetas, ficheros in os.walk(datos):
        quedan = []
        for c in sorted(carpetas):
            if c.startswith(("_", ".")) or RE_CARPETA_RELEASE.fullmatch(c):
                d.saltadas.append(str((Path(raiz) / c).relative_to(datos)))
            else:
                quedan.append(c)
        carpetas[:] = quedan
        for nombre in sorted(ficheros):
            if not nombre.endswith(".parquet"):
                continue
            p = Path(raiz) / nombre
            d.todos.append(p)
            if Path(raiz).name == CARPETA_PARTES:
                partes.setdefault(nombre, []).append(p)
            elif nombre == FICHERO_UNICO:
                unico.append(p)
            for clave, n in NOMBRES.items():
                if nombre == n:
                    cand[clave].append(p)

    def preferida(rutas: list[Path], que: str) -> Path:
        rutas = sorted(rutas, key=lambda p: (len(p.relative_to(datos).parts), str(p)))
        if len(rutas) > 1:
            d.avisos.append(f"{que}: {len(rutas)} copias; se usa {rutas[0].relative_to(datos)}")
        return rutas[0]

    if partes:
        d.tablas["licitaciones"] = [preferida(r, n) for n, r in sorted(partes.items())]
        if unico:
            d.avisos.append(f"licitaciones: hay partes en {CARPETA_PARTES}/ y también {FICHERO_UNICO}; "
                            "se usan las partes")
    elif unico:
        d.tablas["licitaciones"] = [preferida(unico, FICHERO_UNICO)]
    for clave, rutas in cand.items():
        if rutas:
            d.tablas[clave] = [preferida(rutas, NOMBRES[clave])]
    return d


def lista_sql(rutas) -> str:
    return "[" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in rutas) + "]"


def tabla_sql(rutas) -> str:
    return f"read_parquet({lista_sql(rutas)})"


# --------------------------------------------------------------------------------------------------------------------
# Las consultas
# --------------------------------------------------------------------------------------------------------------------
def nif_sql(col: str) -> str:
    """NIF normalizado: solo letras y cifras, en mayúsculas y sin el prefijo de país ES (ESA08023145 → A08023145)."""
    return (f"regexp_replace(upper(regexp_replace({col}, '[^0-9A-Za-z]', '', 'g')), "
            f"'^ES([0-9A-Z]{{9}})$', '\\1')")


def nif_py(v: str) -> str:
    return re.sub(r"^ES([0-9A-Z]{9})$", r"\1", re.sub(r"[^0-9A-Za-z]", "", v).upper())


def nif_valido_sql(n: str) -> str:
    """Un NIF que identifica: al menos 8 caracteres y 5 cifras seguidas. Fuera los vacíos, los enmascarados de
    personas físicas ('*** 9265 **'), las listas de varios ('A1||B2') y los marcadores ('-', '.', 'U/K')."""
    return f"length({n}) >= 8 AND regexp_matches({n}, '[0-9]{{5}}')"


def sustituir(sql: str, tablas: dict) -> str:
    """{tabla} → read_parquet([...]) (o la lista de ficheros, en {todo}). Sin str.format: las llaves de las
    expresiones regulares ([0-9]{4}) se quedan como están."""
    for k, v in tablas.items():
        sql = sql.replace("{" + k + "}", lista_sql(v) if k == "todo" else tabla_sql(v))
    return sql

SQL_TED_AVISOS = """
-- avisos_para_cruce() de ted/ted_module.py en SQL: la última versión de cada aviso (sus filas vigentes o, si TED ya
-- no lo sirve, las de la última descarga en que salió) y, DESPUÉS, fuera los cancelados
WITH t AS (
    SELECT ted_notice_id, "year", cancelled, coalesce(_en_ultima_descarga, true) AS vigente,
           coalesce(_ultima_descarga, '') AS ultima
    FROM {ted}),
a AS (
    SELECT ted_notice_id, bool_or(vigente) AS con_vigente, max(ultima) AS ultima_max
    FROM t WHERE ted_notice_id IS NOT NULL GROUP BY ted_notice_id),
u AS (
    SELECT t.* FROM t LEFT JOIN a USING (ted_notice_id)
    WHERE t.vigente OR t.ted_notice_id IS NULL OR (NOT a.con_vigente AND t.ultima = a.ultima_max))
SELECT "year" AS anio,
       count(DISTINCT ted_notice_id) + count(*) FILTER (WHERE ted_notice_id IS NULL) AS avisos,
       count(*) AS filas
FROM u
WHERE NOT coalesce(TRY_CAST(cancelled AS DOUBLE) = 1, false)
GROUP BY ALL ORDER BY anio NULLS FIRST
"""

SQL_MADRID = """
-- Contratos menores. Una fila que ya no está en la última descarga y cuya Referencia y entidad sí están es una
-- versión anterior: fuera. Un menor publicado dos veces con otra Referencia (mismo contenido) cuenta una vez
WITH m AS (
    SELECT * FROM {madrid} WHERE "Tipo de Publicación" = 'Contratos menores'),
k AS (
    SELECT DISTINCT "Referencia", "Entidad Adjudicadora" FROM m WHERE coalesce(_en_ultima_descarga, true)),
v AS (
    SELECT m.* FROM m
    WHERE coalesce(m._en_ultima_descarga, true)
       OR NOT EXISTS (SELECT 1 FROM k WHERE k."Referencia" IS NOT DISTINCT FROM m."Referencia"
                                        AND k."Entidad Adjudicadora" IS NOT DISTINCT FROM m."Entidad Adjudicadora")),
d AS (
    SELECT DISTINCT "Entidad Adjudicadora", "Nº Expediente", "Título del contrato", "Tipo de contrato",
           "Presupuesto de licitación", "Nº de ofertas", "NIF del adjudicatario", "Adjudicatario",
           "Fecha del contrato", "Importe de adjudicación"
    FROM v)
SELECT TRY_CAST(regexp_extract("Fecha del contrato", '([0-9]{4})\\s*$', 1) AS INTEGER) AS anio,
       count(*) AS menores,
       sum(TRY_CAST(replace(replace(trim("Importe de adjudicación"), '.', ''), ',', '.') AS DOUBLE)) AS importe_con_iva
FROM d
GROUP BY ALL ORDER BY anio NULLS FIRST
"""

SQL_PSCP = """
-- Contratos menores de la PSCP. Una fila que ya no está en la última descarga y cuya publicación (el uuid de la URL,
-- que no cambia de una fase a otra) sí está es una fase anterior: fuera. Fuera también las anulaciones y las filas con
-- varios adjudicatarios ('||'), cuyo importe no se puede repartir. El nombre, el más repetido de cada NIF
WITH p AS (
    SELECT identificacio_adjudicatari, denominacio_adjudicatari, import_adjudicacio_sense_iva, fase_publicacio,
           coalesce(nullif(regexp_extract(enllac_publicacio, 'detall-publicacio/([^/]+)/', 1), ''),
                    enllac_publicacio) AS publicacion,
           coalesce(_en_ultima_descarga, true) AS en_ultima, coalesce(_ultima_descarga, '') AS ultima
    FROM {pscp} WHERE procediment = 'Contracte menor'),
v AS (
    SELECT *, en_ultima OR (NOT bool_or(en_ultima) OVER w AND ultima = max(ultima) OVER w) AS vigente
    FROM p WINDOW w AS (PARTITION BY publicacion)),
n AS (
    SELECT __NIF__ AS nif, denominacio_adjudicatari AS nombre,
           TRY_CAST(import_adjudicacio_sense_iva AS DOUBLE) AS importe
    FROM v
    WHERE vigente AND coalesce(fase_publicacio, '') <> 'Anul·lació'
      AND NOT contains(coalesce(identificacio_adjudicatari, ''), '||')),
t AS (
    SELECT nif, count(*) AS contratos, sum(importe) AS importe_sin_iva
    FROM n WHERE __VALIDO__
    GROUP BY nif ORDER BY contratos DESC, nif LIMIT 20),
m AS (
    SELECT nif, nombre FROM n WHERE nombre IS NOT NULL AND nif IN (SELECT nif FROM t)
    GROUP BY nif, nombre QUALIFY row_number() OVER (PARTITION BY nif ORDER BY count(*) DESC, nombre) = 1)
SELECT t.nif, m.nombre, t.contratos, t.importe_sin_iva
FROM t LEFT JOIN m USING (nif)
ORDER BY t.contratos DESC, t.nif
""".replace("__NIF__", nif_sql("identificacio_adjudicatari")).replace("__VALIDO__", nif_valido_sql("nif"))


SQL_TOP = """
-- La última versión de cada licitación (es_ultima_version). En un acuerdo marco cada empresa adjudicataria lleva en
-- su fila el mismo importe (el tope del lote): ese importe se reparte entre ellas para no sumarlo varias veces.
-- El nombre, el más repetido de cada NIF
WITH r AS (
    SELECT __NIF__ AS nif, adjudicatario, id,
           importe_adjudicacion / count(*) OVER (PARTITION BY id, lote, importe_adjudicacion) AS importe
    FROM {resultados} WHERE es_ultima_version AND nif_adjudicatario IS NOT NULL),
v AS (SELECT * FROM r WHERE __VALIDO__),
t AS (
    SELECT nif, count(DISTINCT id) AS licitaciones, count(*) AS lotes, sum(importe) AS importe_sin_iva
    FROM v GROUP BY nif ORDER BY licitaciones DESC, nif LIMIT 20),
n AS (
    SELECT nif, adjudicatario FROM v WHERE adjudicatario IS NOT NULL AND nif IN (SELECT nif FROM t)
    GROUP BY nif, adjudicatario QUALIFY row_number() OVER (PARTITION BY nif ORDER BY count(*) DESC, adjudicatario) = 1)
SELECT t.nif, n.adjudicatario AS nombre, t.licitaciones, t.lotes, t.importe_sin_iva
FROM t LEFT JOIN n USING (nif)
ORDER BY t.licitaciones DESC, t.nif
""".replace("__NIF__", nif_sql("nif_adjudicatario")).replace("__VALIDO__", nif_valido_sql("nif"))

SQL_CRUCE = """
-- Resultados (lotes) de la última versión con la comunidad (NUTS de 4 caracteres) de su licitación, cruzados por id y
-- fecha_updated. Un importe que se repite en el mismo lote (el tope de un acuerdo marco, en la fila de cada empresa)
-- cuenta una vez: sumarlo en cada fila multiplica el total por 2,8 (v2026.10)
WITH x AS (
    SELECT substr(l.nuts, 1, 4) AS nuts2, r.id, r.lote, r.importe_adjudicacion, count(*) AS resultados
    FROM {resultados} r JOIN {licitaciones} l ON l.id = r.id AND l.fecha_updated = r.fecha_updated
    WHERE r.es_ultima_version AND l.es_ultima_version AND r.nif_adjudicatario IS NOT NULL
    GROUP BY ALL)
SELECT nuts2, sum(resultados) AS adjudicaciones, count(DISTINCT id) AS licitaciones,
       sum(importe_adjudicacion) AS importe_sin_iva
FROM x
GROUP BY ALL ORDER BY importe_sin_iva DESC NULLS LAST, nuts2 NULLS LAST
"""


@dataclass(frozen=True)
class Consulta:
    id: str
    titulo: str
    mide: str
    columnas: dict          # tabla -> columnas que necesita
    sql: str                # {tabla} se sustituye por read_parquet([...]); $nif y $palabra son parámetros
    parametros: tuple = ()
    igual_que: str = ""     # otra consulta que tiene que dar el mismo resultado (comprobación)


CONSULTAS = (
    Consulta(
        "todo", "Abrir toda la carpeta: ficheros y filas de cada Parquet",
        "Solo metadatos: el pie de cada Parquet, sin leer datos",
        {"todo": ()},
        "SELECT count(*) AS ficheros, sum(num_rows) AS filas FROM parquet_file_metadata({todo})"),
    Consulta(
        "placsp_ultima_version", "PLACSP: licitaciones por año, la última versión de cada una",
        "Filtro por la marca es_ultima_version y recuento: 2 columnas de 10,9 M de filas",
        {"licitaciones": ("ano", "es_ultima_version")},
        "SELECT CAST(ano AS INTEGER) AS anio, count(*) AS licitaciones FROM {licitaciones}\n"
        "WHERE es_ultima_version GROUP BY ALL ORDER BY anio NULLS FIRST"),
    Consulta(
        "placsp_ultima_version_calculada", "PLACSP: la última versión de cada licitación, calculada sin la marca",
        "Ventana por id sobre 10,9 M de filas (ordenar y memoria): tiene que dar lo mismo que la anterior",
        {"licitaciones": ("id", "fecha_updated", "entrada_repetida", "ano")},
        "-- La versión con la fecha_updated más reciente. Si la misma entrada se publicó dos veces (mismo id y fecha,\n"
        "-- a veces con otro contenido), gana la primera leída: la que no lleva entrada_repetida. Sin ese desempate\n"
        "-- salen 15 licitaciones en otro año (v2026.10). Las filas sin id cuentan todas\n"
        "SELECT CAST(ano AS INTEGER) AS anio, count(*) AS licitaciones FROM (\n"
        "    SELECT ano FROM {licitaciones}\n"
        "    QUALIFY id IS NULL OR row_number() OVER (\n"
        "        PARTITION BY id ORDER BY fecha_updated DESC NULLS LAST, entrada_repetida) = 1)\n"
        "GROUP BY ALL ORDER BY anio NULLS FIRST",
        igual_que="placsp_ultima_version"),
    Consulta(
        "placsp_texto", "PLACSP: licitaciones con una palabra en el objeto",
        "ILIKE sobre el texto del objeto: la columna más pesada de la tabla",
        {"licitaciones": ("objeto", "es_ultima_version")},
        "SELECT count(*) AS licitaciones FROM {licitaciones}\n"
        "WHERE es_ultima_version AND objeto ILIKE '%' || $palabra || '%'",
        ("palabra",)),
    Consulta(
        "placsp_un_nif", "PLACSP: adjudicaciones de un NIF",
        "Búsqueda de un valor sin índice: recorre la columna entera de 10,7 M de filas",
        {"resultados": ("id", "nif_adjudicatario", "fecha_adjudicacion", "es_ultima_version")},
        "SELECT count(*) AS lotes, count(DISTINCT id) AS licitaciones,\n"
        "       min(fecha_adjudicacion) AS primera, max(fecha_adjudicacion) AS ultima\n"
        "FROM {resultados}\n"
        f"WHERE es_ultima_version AND {nif_sql('nif_adjudicatario')} = $nif",
        ("nif",)),
    Consulta(
        "placsp_top_adjudicatarios", "PLACSP: los 20 adjudicatarios con más licitaciones, sin duplicados",
        "Ventana y agrupación por NIF normalizado de 5,9 M de lotes (última versión, topes repartidos)",
        {"resultados": ("id", "lote", "nif_adjudicatario", "adjudicatario", "importe_adjudicacion",
                        "es_ultima_version")},
        SQL_TOP.strip()),
    Consulta(
        "placsp_cruce_comunidad", "PLACSP: lo adjudicado por comunidad (cruce de resultados y licitaciones)",
        "JOIN de 5,9 M de lotes con 5,2 M de licitaciones por id y fecha_updated, y dos agregaciones",
        {"resultados": ("id", "lote", "fecha_updated", "importe_adjudicacion", "nif_adjudicatario",
                        "es_ultima_version"),
         "licitaciones": ("id", "fecha_updated", "nuts", "es_ultima_version")},
        SQL_CRUCE.strip()),
    Consulta(
        "ted_avisos", "TED: avisos de adjudicación por año, cada aviso una vez",
        "Última versión de cada aviso sin los cancelados (avisos_para_cruce) sobre 0,86 M de filas",
        {"ted": ("ted_notice_id", "year", "cancelled", "_en_ultima_descarga", "_ultima_descarga")},
        SQL_TED_AVISOS.strip()),
    Consulta(
        "madrid_menores", "Comunidad de Madrid: contratos menores por año, sin contar dos veces el mismo",
        "Texto a número y a año, quitar versiones anteriores y repetidos (DISTINCT de 10 columnas) en 4,9 M de filas",
        {"madrid": ("Tipo de Publicación", "Referencia", "Entidad Adjudicadora", "Nº Expediente",
                    "Título del contrato", "Tipo de contrato", "Presupuesto de licitación", "Nº de ofertas",
                    "NIF del adjudicatario", "Adjudicatario", "Fecha del contrato", "Importe de adjudicación",
                    "_en_ultima_descarga")},
        SQL_MADRID.strip()),
    Consulta(
        "pscp_top_menores", "Catalunya (PSCP): los 20 adjudicatarios con más contratos menores",
        "Ventana por publicación (fase vigente) y agregación por NIF en 2,1 M de filas",
        {"pscp": ("procediment", "fase_publicacio", "identificacio_adjudicatari", "denominacio_adjudicatari",
                  "import_adjudicacio_sense_iva", "enllac_publicacio", "_en_ultima_descarga", "_ultima_descarga")},
        SQL_PSCP.strip()),
    Consulta(
        "borme_constituciones", "BORME: constituciones de sociedades por año",
        "Partir la lista de actos de 9,6 M de inscripciones y agregar",
        {"borme_empresas": ("fecha_borme", "actos", "capital_euros")},
        "SELECT year(fecha_borme) AS anio, count(*) AS constituciones, sum(capital_euros) AS capital_euros\n"
        "FROM {borme_empresas} WHERE list_contains(string_split(actos, '|'), 'Constitución')\n"
        "GROUP BY ALL ORDER BY anio NULLS FIRST"),
    Consulta(
        "borme_nombramientos", "BORME: las 20 sociedades con más nombramientos de cargos",
        "Agrupación con recuento de distintos sobre 17,8 M de cargos",
        {"borme_cargos": ("empresa_norm", "tipo_acto", "persona_hash")},
        "SELECT empresa_norm AS empresa, count(*) AS nombramientos, count(DISTINCT persona_hash) AS personas\n"
        "FROM {borme_cargos} WHERE tipo_acto = 'nombramiento' AND empresa_norm IS NOT NULL\n"
        "GROUP BY 1 ORDER BY nombramientos DESC, empresa LIMIT 20"),
)
POR_ID = {c.id: c for c in CONSULTAS}


# --------------------------------------------------------------------------------------------------------------------
# Las mismas consultas con pandas (leyendo con pyarrow): solo las que se escriben igual de fácil
# --------------------------------------------------------------------------------------------------------------------
def _leer(rutas, columnas, filtro=None):
    import pyarrow.dataset as ds
    return ds.dataset([str(p) for p in rutas], format="parquet").to_table(columns=list(columnas),
                                                                          filter=filtro).to_pandas()


def _nif_pd(s):
    return (s.str.replace(r"[^0-9A-Za-z]", "", regex=True).str.upper()
            .str.replace(r"^ES([0-9A-Z]{9})$", r"\1", regex=True))


def _nif_valido_pd(n):
    return (n.str.len() >= 8) & n.str.contains(r"[0-9]{5}", regex=True)


def _filas(df):
    return [tuple(None if _es_nulo(v) else v for v in fila) for fila in df.itertuples(index=False, name=None)]


def _es_nulo(v):
    try:
        import pandas as pd
        return v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and math.isnan(v))
    except ImportError:
        return v is None


def _suma(g, df, por, col):
    """Suma por grupo como en SQL: un grupo sin ningún valor da nulo, no 0."""
    x = df.groupby(por, dropna=False)[col].agg(["sum", "count"])
    return x["sum"].where(x["count"] > 0).reindex(g.index)


def pd_placsp_ultima_version(t, p):
    import pyarrow.dataset as ds
    df = _leer(t["licitaciones"], ["ano"], ds.field("es_ultima_version") == True)  # noqa: E712
    n = df["ano"].astype("Int64").value_counts(dropna=False)
    return sorted(((None if _es_nulo(a) else int(a), int(c)) for a, c in n.items()),
                  key=lambda x: (x[0] is not None, x[0] or 0))


def _nombre_mas_repetido(df, clave, nombre, claves):
    """El nombre más repetido de cada clave (en un empate, el primero en orden alfabético)."""
    sub = df[df[clave].isin(claves) & df[nombre].notna()]
    c = sub.groupby([clave, nombre]).size().reset_index(name="_n")
    c = c.sort_values([clave, "_n", nombre], ascending=[True, False, True]).drop_duplicates(clave)
    return dict(zip(c[clave], c[nombre]))


def pd_placsp_top_adjudicatarios(t, p):
    import pyarrow.dataset as ds
    df = _leer(t["resultados"], ["id", "lote", "nif_adjudicatario", "adjudicatario", "importe_adjudicacion"],
               (ds.field("es_ultima_version") == True) & ds.field("nif_adjudicatario").is_valid())  # noqa: E712
    n = df.groupby(["id", "lote", "importe_adjudicacion"], dropna=False)["id"].transform("size")
    df["importe"] = df["importe_adjudicacion"] / n
    df["nif"] = _nif_pd(df["nif_adjudicatario"])
    df = df[_nif_valido_pd(df["nif"])]
    g = df.groupby("nif").agg(licitaciones=("id", "nunique"), lotes=("id", "size"))
    g["importe_sin_iva"] = _suma(g, df, "nif", "importe")
    g = g.reset_index().sort_values(["licitaciones", "nif"], ascending=[False, True]).head(20)
    g["nombre"] = g["nif"].map(_nombre_mas_repetido(df, "nif", "adjudicatario", g["nif"]))
    return _filas(g[["nif", "nombre", "licitaciones", "lotes", "importe_sin_iva"]])


def pd_placsp_cruce_comunidad(t, p):
    import pyarrow.dataset as ds
    r = _leer(t["resultados"], ["id", "lote", "fecha_updated", "importe_adjudicacion"],
              (ds.field("es_ultima_version") == True) & ds.field("nif_adjudicatario").is_valid())  # noqa: E712
    lic = _leer(t["licitaciones"], ["id", "fecha_updated", "nuts"], ds.field("es_ultima_version") == True)  # noqa: E712
    m = r.merge(lic, on=["id", "fecha_updated"], how="inner")
    m["nuts2"] = m["nuts"].str[:4]
    x = m.groupby(["nuts2", "id", "lote", "importe_adjudicacion"], dropna=False).size().reset_index(name="resultados")
    g = x.groupby("nuts2", dropna=False).agg(adjudicaciones=("resultados", "sum"), licitaciones=("id", "nunique"))
    g["importe_sin_iva"] = _suma(g, x, "nuts2", "importe_adjudicacion")
    g = g.reset_index().sort_values(["importe_sin_iva", "nuts2"], ascending=[False, True], na_position="last")
    return _filas(g[["nuts2", "adjudicaciones", "licitaciones", "importe_sin_iva"]])


def pd_ted_avisos(t, p):
    if str(RAIZ_REPO) not in sys.path:
        sys.path.insert(0, str(RAIZ_REPO))
    from ted.ted_module import avisos_para_cruce   # la función del repo, tal cual
    df = _leer(t["ted"], ["ted_notice_id", "year", "cancelled", "_en_ultima_descarga", "_ultima_descarga"])
    a = avisos_para_cruce(df)
    sin_id = a["ted_notice_id"].isna()
    g = a.groupby("year", dropna=False).agg(avisos=("ted_notice_id", "nunique"), filas=("ted_notice_id", "size"))
    g["avisos"] += sin_id.groupby(a["year"], dropna=False).sum().reindex(g.index, fill_value=0)
    g = g.reset_index().sort_values("year", na_position="first")
    return _filas(g[["year", "avisos", "filas"]])


def pd_borme_constituciones(t, p):
    df = _leer(t["borme_empresas"], ["fecha_borme", "actos", "capital_euros"])
    df = df[("|" + df["actos"] + "|").str.contains("|Constitución|", regex=False).fillna(False).astype(bool)]
    df = df.assign(anio=df["fecha_borme"].dt.year)
    g = df.groupby("anio", dropna=False).agg(constituciones=("actos", "size"))
    g["capital_euros"] = _suma(g, df, "anio", "capital_euros")
    g = g.reset_index().sort_values("anio", na_position="first")
    return _filas(g[["anio", "constituciones", "capital_euros"]])


# Las de pandas que usan una función del repositorio (no se pueden medir con el script suelto)
NECESITA_REPO = {"ted_avisos": "ted/ted_module.py"}

PANDAS = {
    "placsp_ultima_version": pd_placsp_ultima_version,
    "placsp_top_adjudicatarios": pd_placsp_top_adjudicatarios,
    "placsp_cruce_comunidad": pd_placsp_cruce_comunidad,
    "ted_avisos": pd_ted_avisos,
    "borme_constituciones": pd_borme_constituciones,
}


# --------------------------------------------------------------------------------------------------------------------
# El entorno: versiones, CPU y memoria (también con los límites de un contenedor)
# --------------------------------------------------------------------------------------------------------------------
def _leer_texto(p):
    try:
        return Path(p).read_text().strip()
    except OSError:
        return None


def cpus_disponibles() -> float:
    n = float(os.cpu_count() or 1)
    if hasattr(os, "sched_getaffinity"):
        n = min(n, float(len(os.sched_getaffinity(0))))
    v2 = _leer_texto("/sys/fs/cgroup/cpu.max")                        # cgroup v2: "200000 100000" o "max 100000"
    if v2 and not v2.startswith("max"):
        cuota, periodo = v2.split()[:2]
        n = min(n, int(cuota) / int(periodo))
    cuota, periodo = _leer_texto("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"), _leer_texto("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if cuota and periodo and int(cuota) > 0:                          # cgroup v1
        n = min(n, int(cuota) / int(periodo))
    return n


def memoria_maquina() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        pass
    try:   # Windows
        import ctypes

        class _M(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
        m = _M()
        m.dwLength = ctypes.sizeof(_M)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
        return int(m.ullTotalPhys)
    except Exception:
        return None


def memoria_disponible() -> int | None:
    total = memoria_maquina()
    for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        v = _leer_texto(p)
        if v and v.isdigit() and (total is None or int(v) < total):
            total = int(v)
    return total


def modelo_cpu() -> str:
    t = _leer_texto("/proc/cpuinfo")
    if t:
        m = re.search(r"^model name\s*:\s*(.+)$", t, re.M)
        if m:
            return m.group(1).strip()
    if sys.platform == "darwin":
        try:
            return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True,
                                  timeout=5).stdout.strip() or platform.machine()
        except Exception:
            pass
    return platform.processor() or platform.machine()


def carga():
    """Carga media de la máquina en el último minuto (en Linux y macOS): si es alta, otros procesos compiten por la
    CPU y los tiempos salen peores."""
    try:
        return round(os.getloadavg()[0], 1)
    except (AttributeError, OSError):
        return None


def versiones() -> dict:
    v = {"python": platform.python_version()}
    for m in ("duckdb", "pandas", "pyarrow", "numpy"):
        try:
            v[m] = __import__(m).__version__
        except ImportError:
            v[m] = None
    return v


def gib(n: int) -> str:
    return f"{n / 2**30:.1f} GiB"


# --------------------------------------------------------------------------------------------------------------------
# Medir: cada consulta en un proceso nuevo
# --------------------------------------------------------------------------------------------------------------------
def _json(v):
    """Un valor del resultado para el JSON: números como números (también DECIMAL y HUGEINT), fechas en ISO."""
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return None if math.isnan(v) else v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if hasattr(v, "item"):          # numpy
        return _json(v.item())
    if hasattr(v, "isoformat"):     # pandas.Timestamp
        return v.isoformat()
    return str(v)


def memoria_pico_mb():
    try:
        import resource
    except ImportError:
        return None
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / 2**20 if sys.platform == "darwin" else r / 1024, 1)


def hijo() -> int:
    """Proceso de una consulta: lee la tarea de stdin, la mide y escribe una línea JSON en stdout."""
    tarea = json.loads(sys.stdin.read())
    c = POR_ID[tarea["consulta"]]
    tablas = {k: [Path(x) for x in v] for k, v in tarea["tablas"].items()}
    params = tarea["parametros"]
    if tarea["motor"] == "duckdb":
        import duckdb
        tmp = tempfile.mkdtemp(prefix="medir_duckdb_")
        con = duckdb.connect(config={"threads": tarea["hilos"], "memory_limit": tarea["memoria"],
                                     "temp_directory": tmp})
        sql = sustituir(c.sql, tablas)
        args = {k: params[k] for k in c.parametros}

        def una():
            return con.execute(sql, args).fetchall() if args else con.execute(sql).fetchall()
    else:
        import pyarrow
        pyarrow.set_cpu_count(tarea["hilos"])
        pyarrow.set_io_thread_count(max(tarea["hilos"], 2))
        tmp = None

        def una():
            return PANDAS[c.id](tablas, params)
    t0 = time.perf_counter()
    filas = una()
    primera = time.perf_counter() - t0
    tiempos = []
    for _ in range(tarea["repeticiones"]):
        t0 = time.perf_counter()
        una()
        tiempos.append(time.perf_counter() - t0)
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)
    print(json.dumps({"primera_s": round(primera, 4),
                      "repetida_s": round(statistics.median(tiempos), 4) if tiempos else None,
                      "repeticiones_s": [round(t, 4) for t in tiempos],
                      "memoria_pico_mb": memoria_pico_mb(),
                      "filas_resultado": len(filas),
                      "resultado": [[_json(v) for v in f] for f in filas[:MAX_FILAS_RESULTADO]],
                      "resultado_recortado": len(filas) > MAX_FILAS_RESULTADO}, ensure_ascii=False))
    return 0


def iguales(a, b) -> bool:
    """Dos resultados (listas de filas del JSON) iguales, con tolerancia en las sumas de importes."""
    if a is None or b is None or len(a) != len(b):
        return False
    for fa, fb in zip(a, b):
        if len(fa) != len(fb):
            return False
        for x, y in zip(fa, fb):
            if isinstance(x, (int, float)) and isinstance(y, (int, float)) and not isinstance(x, bool):
                if not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-6):
                    return False
            elif x != y:
                return False
    return True


def lanzar(tarea: dict, tiempo_maximo: int, datos: Path | None) -> dict:
    """Mide una consulta en un proceso aparte. Si el proceso muere (sin memoria) o se pasa de tiempo, lo dice."""
    try:
        r = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--_hijo"], input=json.dumps(tarea),
                           capture_output=True, text=True, timeout=tiempo_maximo)
    except subprocess.TimeoutExpired:
        return {"error": f"más de {tiempo_maximo} s: se ha parado"}
    if r.returncode != 0:
        if r.returncode in (-9, 137):
            return {"error": "el sistema ha terminado el proceso (casi siempre, por falta de memoria)"}
        lineas = r.stderr.strip().splitlines() or ["(sin mensaje)"]
        ultima = next((x for x in reversed(lineas) if re.search(r"Error|Exception", x)), lineas[-1]).strip()
        if datos is not None:
            ultima = ultima.replace(str(datos), "<datos>")
        return {"error": f"código {r.returncode}: {ultima[:400]}"}
    return json.loads(r.stdout.strip().splitlines()[-1])


def columnas_que_faltan(con, tablas: dict, c: Consulta) -> list:
    faltan = []
    for t, cols in c.columnas.items():
        if t == "todo" or not cols:
            continue
        hay = {f[0] for f in con.execute(f"DESCRIBE SELECT * FROM {tabla_sql(tablas[t])}").fetchall()}
        faltan += [f"{t}.{x}" for x in cols if x not in hay]
    return faltan


def medir(a) -> dict:
    import duckdb
    datos = a.datos.resolve()
    if not datos.is_dir():
        raise SystemExit(f"--datos {a.datos}: no es una carpeta")
    d = buscar_tablas(datos)
    cpus, mem = cpus_disponibles(), memoria_disponible()
    hilos = a.hilos or max(1, int(cpus))
    memoria = a.memoria or (f"{int(mem * 0.75 / 2**20)}MiB" if mem else None)
    motores = ["duckdb", "pandas"] if a.motor == "ambos" else [a.motor]
    v = versiones()
    if "pandas" in motores and (v["pandas"] is None or v["pyarrow"] is None):
        raise SystemExit("Falta pandas o pyarrow: pip install pandas pyarrow")
    con = duckdb.connect(config={"threads": hilos, **({"memory_limit": memoria} if memoria else {})})
    hilos_db, memoria_db = con.execute("SELECT current_setting('threads'), current_setting('memory_limit')").fetchone()
    params = {"nif": nif_py(a.nif), "palabra": a.palabra}
    res = {
        "formato": FORMATO,
        "fecha": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "perfil": a.perfil,
        "datos": a.etiqueta,
        "entorno": {**v, "sistema": f"{platform.system()} {platform.release()}", "cpu": modelo_cpu(),
                    "nucleos_maquina": os.cpu_count(), "cpus_disponibles": round(cpus, 2),
                    "memoria_maquina_gb": round(memoria_maquina() / 2**30, 1) if memoria_maquina() else None,
                    "memoria_disponible_gb": round(mem / 2**30, 1) if mem else None,
                    "carga_al_empezar": carga()},
        "duckdb": {"threads": int(hilos_db), "memory_limit": memoria_db},
        "pandas": {"hilos_pyarrow": hilos} if "pandas" in motores else None,
        "repeticiones": a.repeticiones,
        "parametros": {"nif": params["nif"], "palabra": a.palabra},
        "tablas": {},
        "carpetas_saltadas": len(d.saltadas),
        "avisos": [],
        "consultas": [],
    }
    print(f"DuckDB {v['duckdb']} · pandas {v['pandas']} · pyarrow {v['pyarrow']} · Python {v['python']}")
    print(f"{modelo_cpu()} · {cpus:g} CPU disponibles · {gib(mem) if mem else '¿?'} disponibles · "
          f"DuckDB con {hilos_db} hilos y {memoria_db}")
    if d.saltadas:
        print(f"Sin entrar en {len(d.saltadas)} carpetas (_*/, .*/ y vAAAA.MM/): "
              + ", ".join(d.saltadas[:5]) + (" …" if len(d.saltadas) > 5 else ""))
    for x in d.avisos:
        print("Aviso:", x)
    res["avisos"] = [x.replace(str(datos), "<datos>") for x in d.avisos]
    for clave in ("licitaciones", *NOMBRES):
        rutas = d.tablas.get(clave)
        if not rutas:
            print(f"  {clave:15} no encontrada")
            continue
        filas = con.execute(f"SELECT sum(num_rows) FROM parquet_file_metadata({lista_sql(rutas)})").fetchone()[0]
        tam = sum(p.stat().st_size for p in rutas)
        res["tablas"][clave] = {"descripcion": DESCRIPCION_TABLAS[clave], "ficheros": [p.name for p in rutas],
                                "filas": int(filas), "bytes": tam}
        print(f"  {clave:15} {len(rutas)} fichero(s)  {tam / 1e9:6.2f} GB  {int(filas):>12,} filas".replace(",", "."))
    if d.todos:
        res["tablas"]["todo"] = {"descripcion": "todos los Parquet de la carpeta", "n_ficheros": len(d.todos),
                                 "bytes": sum(p.stat().st_size for p in d.todos)}
    tablas_ok = {**d.tablas, **({"todo": d.todos} if d.todos else {})}
    elegidas = [POR_ID[x] for x in a.solo.split(",")] if a.solo else list(CONSULTAS)

    print(f"\n{'consulta':32} {'motor':7} {'primera':>9} {'repetida':>9} {'memoria':>9} {'filas':>6}")
    for c in elegidas:
        faltan = [t for t in c.columnas if t not in tablas_ok]
        cols = [] if faltan else columnas_que_faltan(con, tablas_ok, c)
        resultado_duckdb = None
        for motor in motores:
            base = {"id": c.id, "titulo": c.titulo, "mide": c.mide, "motor": motor,
                    "tablas": sorted(c.columnas), "sql": c.sql if motor == "duckdb" else None,
                    "parametros": {k: params[k] for k in c.parametros}}
            if motor == "pandas" and c.id not in PANDAS:
                continue
            sin_modulo = motor == "pandas" and c.id in NECESITA_REPO and not (RAIZ_REPO / NECESITA_REPO[c.id]).is_file()
            if faltan or cols or sin_modulo:
                motivo = (f"falta la tabla {', '.join(faltan)}" if faltan else f"faltan columnas: {', '.join(cols)}"
                          if cols else f"hace falta el repositorio entero ({NECESITA_REPO[c.id]}), no solo este script")
                res["consultas"].append({**base, "saltada": motivo})
                print(f"{c.id:32} {motor:7} {'—':>9} {'—':>9} {'—':>9} {'—':>6}  ({motivo})")
                continue
            tarea = {"consulta": c.id, "motor": motor, "tablas": {k: [str(p) for p in tablas_ok[k]]
                                                                  for k in c.columnas},
                     "hilos": hilos, "memoria": memoria or memoria_db, "repeticiones": a.repeticiones,
                     "parametros": params}
            m = lanzar(tarea, a.tiempo_maximo, datos)
            fila = {**base, **m}
            if "error" in m:
                print(f"{c.id:32} {motor:7} {'—':>9} {'—':>9} {'—':>9} {'—':>6}  ({m['error']})")
            else:
                if motor == "duckdb":
                    resultado_duckdb = m["resultado"]
                elif resultado_duckdb is not None:
                    fila["coincide_con_duckdb"] = iguales(resultado_duckdb, m["resultado"])
                rep = f"{m['repetida_s']:9.2f}" if m["repetida_s"] is not None else f"{'—':>9}"
                mem_txt = f"{m['memoria_pico_mb'] / 1024:7.2f}GB" if m["memoria_pico_mb"] else f"{'—':>9}"
                extra = "" if fila.get("coincide_con_duckdb", True) else "  ¡NO COINCIDE CON DUCKDB!"
                print(f"{c.id:32} {motor:7} {m['primera_s']:9.2f} {rep} {mem_txt} {m['filas_resultado']:6}{extra}",
                      flush=True)
            res["consultas"].append(fila)
    # Comprobaciones: dos formas de contar lo mismo tienen que dar lo mismo
    hechas = {(x["id"], x["motor"]): x for x in res["consultas"] if "resultado" in x}
    for c in elegidas:
        for motor in motores:
            x, y = hechas.get((c.id, motor)), hechas.get((c.igual_que, motor))
            if c.igual_que and x and y:
                x["coincide_con_" + c.igual_que] = iguales(x["resultado"], y["resultado"])
                if not x["coincide_con_" + c.igual_que]:
                    print(f"¡{c.id} ({motor}) NO COINCIDE CON {c.igual_que}!")
    res["entorno"]["carga_al_acabar"] = carga()
    print("\nSegundos. «primera»: la primera vez en un proceso nuevo; «repetida»: la mediana de las repeticiones. "
          "«memoria»: pico de memoria residente del proceso.")
    return res


# --------------------------------------------------------------------------------------------------------------------
# Tabla Markdown con varios JSON (un perfil de máquina por columna)
# --------------------------------------------------------------------------------------------------------------------
def _s(x):
    if x is None:
        return "—"
    return (f"{x:.2f}" if x < 10 else f"{x:.1f}" if x < 100 else f"{x:.0f}").replace(".", ",")


def tabla_markdown(ficheros: list[Path]) -> str:
    perfiles = [json.loads(Path(f).read_text(encoding="utf-8")) for f in ficheros]
    perfiles.sort(key=lambda r: (r["entorno"].get("cpus_disponibles") or 0, r["entorno"].get("memoria_disponible_gb") or 0))
    nombres = [r.get("perfil") or f"{r['entorno']['cpus_disponibles']:g} CPU / {r['entorno']['memoria_disponible_gb']:g} GB"
               for r in perfiles]
    salida = []
    for motor in ("duckdb", "pandas"):
        filas = []
        for c in CONSULTAS:
            celdas = []
            for r in perfiles:
                m = next((x for x in r["consultas"] if x["id"] == c.id and x["motor"] == motor), None)
                if m is None:
                    celdas.append(None)
                elif "saltada" in m or "error" in m:
                    celdas.append("sin memoria" if "memoria" in m.get("error", "") else "—")
                else:
                    gb = m.get("memoria_pico_mb")
                    celdas.append(f"{_s(m['repetida_s'])} s ({_s(m['primera_s'])} s)"
                                  + (f" · {_s(gb / 1024)} GB" if gb else ""))
            if any(x is not None for x in celdas):
                filas.append(f"| {c.titulo} | " + " | ".join(x or "—" for x in celdas) + " |")
        if filas:
            salida.append(f"**{'DuckDB' if motor == 'duckdb' else 'pandas + pyarrow'}**: repetida (primera) · "
                          "pico de memoria\n")
            salida.append("| Consulta | " + " | ".join(nombres) + " |")
            salida.append("|---|" + "---|" * len(nombres))
            salida += filas
            salida.append("")
    return "\n".join(salida)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--_hijo"]:
        return hijo()
    ap = argparse.ArgumentParser(description="Mide consultas típicas sobre los Parquet publicados (DuckDB y pandas)")
    ap.add_argument("--datos", type=Path, help="carpeta con la release descomprimida o la salida de los scripts")
    ap.add_argument("--motor", choices=("duckdb", "pandas", "ambos"), default="duckdb")
    ap.add_argument("--repeticiones", type=int, default=3, help="veces que se repite cada consulta (por defecto, 3)")
    ap.add_argument("--hilos", type=int, help="hilos de DuckDB y de pyarrow (por defecto, las CPU disponibles)")
    ap.add_argument("--memoria", help="límite de memoria de DuckDB, p. ej. 6GB (por defecto, el 75 %% de la disponible)")
    ap.add_argument("--solo", help="solo estas consultas, separadas por comas (ver --listar)")
    ap.add_argument("--nif", default="A28541639", help="NIF que se busca en placsp_un_nif")
    ap.add_argument("--palabra", default="ambulancia", help="palabra que se busca en el objeto (placsp_texto)")
    ap.add_argument("--perfil", help="nombre del perfil de máquina para el JSON y la tabla, p. ej. '4 CPU / 8 GB'")
    ap.add_argument("--etiqueta", help="qué datos son, para el JSON, p. ej. 'release v2026.10'")
    ap.add_argument("--tiempo-maximo", type=int, default=1800, help="segundos por consulta antes de pararla")
    ap.add_argument("--json", type=Path, help="guarda aquí el resultado")
    ap.add_argument("--listar", action="store_true", help="lista las consultas y sale")
    ap.add_argument("--tabla", nargs="+", type=Path, metavar="JSON", help="tabla Markdown con varios JSON y sale")
    a = ap.parse_args(argv)
    if a.listar:
        for c in CONSULTAS:
            pd_txt = " (también con pandas)" if c.id in PANDAS else ""
            print(f"{c.id:32} {c.titulo}{pd_txt}\n{'':32} mide: {c.mide}; tablas: {', '.join(sorted(c.columnas))}")
        return 0
    if a.tabla:
        print(tabla_markdown([Path(p) for f in a.tabla for p in (glob.glob(str(f)) or [f])]))
        return 0
    if a.datos is None:
        ap.error("di dónde están los datos: --datos CARPETA")
    if a.solo:
        malas = [x for x in a.solo.split(",") if x not in POR_ID]
        if malas:
            ap.error(f"--solo: no existen {', '.join(malas)} (ver --listar)")
    try:
        import duckdb  # noqa: F401
    except ImportError:
        print("Falta DuckDB: pip install duckdb", file=sys.stderr)
        return 2
    res = medir(a)
    if a.json:
        a.json.parent.mkdir(parents=True, exist_ok=True)
        a.json.write_text(json.dumps(res, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print(f"Resultado en {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
