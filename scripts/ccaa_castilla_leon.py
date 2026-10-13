#!/usr/bin/env python3
"""
=============================================================================
CASTILLA Y LEÓN - CONTRATACIÓN PÚBLICA (Junta de Castilla y León)
=============================================================================
Descarga TODOS los datasets de contratación del portal de análisis de datos
abiertos de la Junta (OpenDataSoft) y el histórico del perfil de contratante
anterior a 2018, guarda los ficheros originales tal cual (con todas sus
versiones) y genera un Parquet por dataset con todas las columnas y filas
publicadas, como texto.

Ejecutar:  python scripts/ccaa_castilla_leon.py [--salida DIR] [--solo-descarga]
           [--solo-parquet] [--comprobar-todo]

Salida (por defecto <repo>/ccaa_castilla_leon/):
    raw/datasets/<id>.csv              export CSV completo de cada dataset, tal cual
    raw/datasets/<id>.metadatos.json   ficha del dataset en el catálogo (campos, fechas...)
    raw/historico/1284165771488.csv    histórico del perfil de contratante (< 17-4-2018)
    raw/catalogo.json                  catálogo completo del portal
    raw/**/_historico/                 versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json               URL, fecha de descarga y última comprobación de cada fichero
    raw/descarga_log.txt               resumen de cada ejecución (se añade al final)
    <id>.parquet                       registros de cada dataset (todas las columnas, texto)
    licitaciones_perfil_historico.parquet
    _historico/                        versiones anteriores de los Parquet

Columnas añadidas a cada Parquet: _fuente (URL descargada), _dataset,
_archivo_origen (ruta en raw/), _fecha_descarga (de la versión del fichero de la
que sale la fila) y, de comun/historico.py, _primera_descarga, _ultima_descarga
y _en_ultima_descarga. Un registro que el portal retira o modifica NO desaparece:
sigue en el Parquet con _en_ultima_descarga=False (control del sesgo del
superviviente; ver comun/historico.py).

Qué se descarga:
- Catálogo: se recorre ENTERO (paginado) y se eligen los datasets cuyo id,
  título o palabras clave contienen "contrat", "licitac" o "adjudica". La lista
  DATASETS_CONOCIDOS solo es un respaldo (se pide aunque no salga en el
  catálogo y es lo único que se pide si el catálogo no responde). Los datasets
  descubiertos que no estaban en la lista se anotan en el resumen.
- Cada dataset se exporta completo (exports/csv no tiene límite de filas). Solo
  se vuelve a pedir si el catálogo indica cambios (metas.default.data_processed
  / modified) posteriores a la última comprobación, o con --comprobar-todo.
- Todo fichero pasa por comun.historico.guardar_version: si el contenido no
  cambia no se toca; si cambia, la copia anterior queda en _historico/.

FUENTES
-------
Confirmado en páginas oficiales (investigación previa, confianza A):
  - Portal de análisis OpenDataSoft: https://analisis.datosabiertos.jcyl.es
  - Datasets de contratación (contratos ordinarios, menores, modificados desde
    2019, basados en acuerdo marco, desistidos/renunciados/desiertos, emergencia;
    SACYL: menores desde 2018, ordinarios, acuerdo marco).
  - Histórico del perfil anterior al 17-4-2018 (URL_HISTORICO).
Documentado por OpenDataSoft (API Explore v2.1), a comprobar contra este portal:
  - GET /api/explore/v2.1/catalog/datasets?limit=100&offset=N&order_by=dataset_id
    -> {"total_count", "results": [{"dataset_id", "metas": {"default": {...}}}]}
    (límite de la API: offset + limit <= 10000).
  - GET /api/explore/v2.1/catalog/datasets/{id}/exports/csv con
    lang=es, timezone=Europe/Madrid, use_labels=true, delimiter=; (los mismos
    parámetros que usa el botón "Exportar" del portal: cabeceras con las
    etiquetas publicadas y fechas en hora de Madrid).
VERIFICAR EN VIVO (el sandbox donde se escribió no llega a los portales):
  - Los ids reales de DATASETS_CONOCIDOS (los de acuerdo marco y
    desistidos/renunciados/desiertos son suposiciones).
  - Que el catálogo devuelve metas.default.title/keyword/modified/data_processed.
  - Que use_labels=true no produce etiquetas repetidas (pandas las renombraría
    a "X.1"; los nombres técnicos están en <id>.metadatos.json).
  - Si jcyl.opendatasoft.com sirve lo mismo, por si el dominio propio falla.
=============================================================================
"""

import argparse
import json
import math
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones  # noqa: E402, F401
# Lectura común: los mismos nombres que tenía este script, aunque alguno ya solo lo lea comun.lectura
from comun.lectura import (  # noqa: E402, F401
    formato_contenido, _detectar_codificacion, _detectar_separador, _lineas_de_datos, _nombres_columnas,
    _leer_csv_tolerante, leer_csv, _celda_texto, _buscar_registros, _leer_json)
from comun.lectura_csv import registros_csv  # noqa: E402, F401
# Red común: los mismos nombres que tenía este script, aunque alguno ya solo lo lea comun.red
from comun.red import (  # noqa: E402, F401
    CABECERAS, TIMEOUT_API, TIMEOUT_DESCARGA, INTENTOS, ESPERA_BASE, ESPERA_MAXIMA, CODIGOS_REINTENTABLES,
    ERRORES_RED, ErrorPortal, _espera, pedir_json)
from comun import red  # noqa: E402
# Salida común: los mismos nombres que tenía este script, aunque alguno ya solo lo lea comun.salida
from comun.salida import (  # noqa: E402, F401
    iso, fecha_version, guardar_json, _como_texto)
import comun.salida  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

BASE_URL = "https://analisis.datosabiertos.jcyl.es"
API_URL = f"{BASE_URL}/api/explore/v2.1"
URL_CATALOGO = f"{API_URL}/catalog/datasets"
URL_EXPORT = API_URL + "/catalog/datasets/{id}/exports/csv"
PARAMS_EXPORT = {"lang": "es", "timezone": "Europe/Madrid", "use_labels": "true", "delimiter": ";"}
LIMITE_PAGINA = 100
MAX_RESULTADOS_API = 10_000   # offset + limit que admite la API v2.1

# Histórico del perfil de contratante anterior al 17-4-2018 (fichero estático)
URL_HISTORICO = "https://datosabiertos.jcyl.es/web/jcyl/risp/es/sector-publico/licitaciones/1284165771488.csv"
PARQUET_HISTORICO = "licitaciones_perfil_historico.parquet"
# Aviso que sirve el portal en vez del fichero cuando un dataset no tiene datos
SIN_DATOS = "No existen datos asociados a este dataset"
MOTIVO_SIN_DATOS = "el portal indica que no hay datos"

# Datasets de contratación que se buscan en el catálogo
PATRON_CONTRATACION = re.compile(r"contrat|licitac|adjudica", re.IGNORECASE)

# Respaldo: se piden siempre, aunque el catálogo no los liste. Verificados en
# vivo el 2026-09-27 contra el catálogo (los demás que lista se descubren solos)
DATASETS_CONOCIDOS = {
    "contratos-ordinarios": "Contratos ordinarios",
    "contratos-menores": "Contratos menores",
    "contratos-modificados": "Contratos modificados (desde 2019)",
    "contratos-basados-en-acuerdo-marco": "Contratos basados en acuerdo marco",
    "contratos-desistimiento-renuncia-desierto": "Contratos: desistimiento, renuncia, desierto",
    "contratos-de-emergencia": "Contratos de emergencia",
    "contratos-menores-sacyl": "Contratos menores SACYL (desde 2018)",
    "contratos-ordinarios-sacyl": "Contratos ordinarios SACYL",
    "contratos-basados-en-acuerdo-marco-de-sacyl": "Contratos basados en acuerdo marco de SACYL",
}

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_castilla_leon"
TITULO = "CASTILLA Y LEÓN - CONTRATACIÓN PÚBLICA"


# ============================================================================
# UTILIDADES COMUNES (la misma base en ccaa_castilla_leon.py, ccaa_murcia.py,
# ccaa_extremadura.py y ccaa_la_rioja.py): descarga con reintentos, versiones,
# lectura como texto, acumulación de registros y Parquet. Aquí además:
# validar_contenido descarta el aviso de «sin datos» que sirve el portal
# (SIN_DATOS), y los XLS y XLSX se leen con un _hoja_a_df propio, que no
# detecta las hojas sin fila de cabecera como el de comun/lectura.py.
# Lo que era idéntico en varios scripts está en comun/red.py, comun/lectura.py y
# comun/salida.py: se importa con el mismo nombre o se envuelve aquí.
# ============================================================================

# En comun/red.py, con los mismos valores: CABECERAS, TIMEOUT_API, TIMEOUT_DESCARGA, INTENTOS,
# ESPERA_BASE, ESPERA_MAXIMA, CODIGOS_REINTENTABLES y ERRORES_RED
PAUSA = 0.3                  # entre peticiones, para no cargar el portal
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL u otra hoja sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_dataset", "_recurso", "_anio_fichero", "_archivo_origen", "_hoja", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


def a_epoch(valor):
    """Fecha ISO de CKAN/OpenDataSoft (sin zona = UTC) -> epoch, o None."""
    if not valor:
        return None
    try:
        fecha = datetime.fromisoformat(str(valor).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.timestamp()


def url_completa(url, params=None):
    return requests.Request("GET", url, params=params).prepare().url


def sin_acentos(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto)) if not unicodedata.combining(c))


def validar_contenido(ruta, tipo):
    """Motivo por el que la descarga no es el fichero esperado (p.ej. una página
    HTML de error servida con 200), o None si es válida."""
    if tipo is None:
        return None
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía"
    formato = formato_contenido(cabeza)
    if formato in ("html", "xml"):
        return f"la respuesta es {formato.upper()}, no {tipo.upper()}"
    if SIN_DATOS.encode() in cabeza and os.path.getsize(ruta) < 4096:
        # El portal sirve un aviso en lugar del fichero (verificado en vivo el
        # 2026-09-27 con el histórico del perfil): no es una tabla de 1 fila
        return f"{MOTIVO_SIN_DATOS}: '{SIN_DATOS}'"
    if tipo in ("xlsx", "xls"):
        return None if formato in ("xlsx", "xls") else f"se esperaba una hoja de cálculo y llegó {formato}"
    return None if formato == tipo else f"se esperaba {tipo.upper()} y llegó {formato}"


def descargar(url, destino, params=None, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior
    (comun.red.descargar, con el validar_contenido de este script).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
    """
    return red.descargar(url, destino, params, tipo, validar_contenido=validar_contenido)


class Manifiesto:
    """raw/_manifiesto.json: de dónde sale cada fichero crudo, cuándo se descargó
    su versión actual (fecha_descarga), cuándo se comprobó por última vez
    (comprobado) y si el portal lo sigue publicando."""

    def __init__(self, raw):
        self.raw = Path(raw)
        self.ruta = self.raw / "_manifiesto.json"
        try:
            self.datos = json.loads(self.ruta.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            self.datos = {}

    def rel(self, ruta):
        return Path(ruta).relative_to(self.raw).as_posix()

    def get(self, rel):
        return self.datos.get(rel, {})

    def registrar(self, ruta, url, estado, **extra):
        entrada = self.datos.setdefault(self.rel(ruta), {})
        entrada.update(extra)
        entrada["url"] = url
        entrada["publicado"] = True
        entrada.pop("retirado_desde", None)
        if estado == "sin_cambios" and entrada.get("fecha_descarga"):
            entrada["comprobado"] = iso(ahora())
        else:
            entrada["fecha_descarga"] = entrada["comprobado"] = fecha_version(ruta)
        self.guardar()

    def retirar(self, ruta, detalle):
        entrada = self.datos.setdefault(self.rel(ruta), {})
        entrada["publicado"] = False
        entrada.setdefault("retirado_desde", iso(ahora()))
        entrada["comprobado"] = iso(ahora())
        entrada["detalle"] = detalle
        self.guardar()

    def guardar(self):
        self.raw.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_name(f".{self.ruta.name}.tmp")
        tmp.write_text(json.dumps(self.datos, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.ruta)


class Resumen:
    """Lo descargado, lo que no existe (404 por año...), lo retirado y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = []
        self.no_publicados = {}
        self.retirados = []
        self.avisos = []
        self.fallidos = []
        self.parquets = []

    def descarga(self, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios.append(etiqueta)
        else:
            self.descargados.append(f"{etiqueta} ({estado})")

    def no_publicado(self, grupo, valor):
        self.no_publicados.setdefault(grupo, []).append(valor)

    @staticmethod
    def _rangos(valores):
        if not all(isinstance(v, int) for v in valores):
            return ", ".join(str(v) for v in valores)
        valores, tramos = sorted(set(valores)), []
        for v in valores:
            if tramos and v == tramos[-1][1] + 1:
                tramos[-1][1] = v
            else:
                tramos.append([v, v])
        return ", ".join(str(a) if a == b else f"{a}-{b}" for a, b in tramos)

    def texto(self):
        lineas = ["=" * 70]
        if self.fallidos:
            lineas.append(f"⚠️ {self.titulo}: COMPLETADO CON ERRORES ({len(self.fallidos)})")
        else:
            lineas.append(f"✅ {self.titulo}: COMPLETADO")
        lineas += ["=" * 70, f"Inicio: {iso(self.inicio)}  Fin: {iso(ahora())}"]

        def bloque(titulo, elementos):
            if elementos:
                lineas.append(f"\n{titulo} ({len(elementos)}):")
                lineas.extend(f"   - {e}" for e in elementos)

        bloque("DESCARGADOS (versión nueva)", self.descargados)
        bloque("SIN CAMBIOS", self.sin_cambios)
        if self.no_publicados:
            lineas.append("\nNO PUBLICADOS EN EL PORTAL (404 / sin fichero):")
            lineas.extend(f"   - {g}: {self._rangos(v)}" for g, v in self.no_publicados.items())
        bloque("RETIRADOS POR EL PORTAL (se conservan con _en_ultima_descarga=False)", self.retirados)
        bloque("PARQUET", [f"{n}: {f:,} filas x {c} columnas ({r:,} ya no publicadas)"
                           for n, f, c, r in self.parquets])
        bloque("AVISOS", self.avisos)
        bloque("ERRORES - vuelve a ejecutar el script para reintentarlos", self.fallidos)
        return "\n".join(lineas)

    def cerrar(self, raw):
        texto = self.texto()
        print("\n" + texto)
        raw = Path(raw)
        raw.mkdir(parents=True, exist_ok=True)
        with open(raw / "descarga_log.txt", "a", encoding="utf-8") as f:
            f.write(texto + "\n\n")
        return 1 if self.fallidos else 0


# ----------------------------------------------------------------------------
# Lectura de ficheros como texto (todas las filas y columnas, sin convertir nada)
# ----------------------------------------------------------------------------

def _hoja_a_df(filas, nombre, hoja, avisos):
    """Tabla de una hoja: detecta la fila de cabecera (las filas de título de
    encima se anotan en los avisos) y conserva todas las filas con algún valor."""
    filas = [list(f) for f in filas if any(v is not None for v in f)]
    if not filas:
        return None
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    if pos:
        titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
        avisos.append(f"{nombre} [{hoja}]: {pos} filas antes de la cabecera (no son datos): {titulo[:200]}")
    ancho = max(len(f) for f in filas)
    cabecera = filas[pos] + [None] * (ancho - len(filas[pos]))
    datos = [f + [None] * (ancho - len(f)) for f in filas[pos + 1:]]
    df = pd.DataFrame(datos, columns=_nombres_columnas(cabecera), dtype=object)
    # Columnas sin nombre y sin ningún valor: restos del rango usado de Excel
    vacias = [c for c, v in zip(df.columns, cabecera) if v is None and df[c].isna().all()]
    df = df.drop(columns=vacias)
    df["_hoja"] = hoja
    return df


def _leer_xlsx(ruta):
    import openpyxl

    avisos, partes = [], []
    libro = openpyxl.load_workbook(ruta, read_only=True, data_only=True)
    try:
        for hoja in libro.worksheets:
            hoja.reset_dimensions()   # no fiarse de la dimensión declarada en el fichero
            filas = ([_celda_texto(v) for v in fila] for fila in hoja.iter_rows(values_only=True))
            df = _hoja_a_df(filas, Path(ruta).name, hoja.title, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.close()
    return partes, avisos


def _leer_xls(ruta):
    try:
        import xlrd
    except ImportError:
        raise RuntimeError("hace falta el paquete xlrd para leer .xls (pip install 'xlrd>=2.0.1')") from None

    def texto(celda, modo_fecha):
        if celda.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
            return None
        if celda.ctype == xlrd.XL_CELL_DATE:
            try:
                return _celda_texto(xlrd.xldate_as_datetime(celda.value, modo_fecha))
            except Exception:
                return _celda_texto(celda.value)
        if celda.ctype == xlrd.XL_CELL_BOOLEAN:
            return str(bool(celda.value))
        if celda.ctype == xlrd.XL_CELL_ERROR:
            return xlrd.error_text_from_code.get(celda.value, "#ERROR")
        return _celda_texto(celda.value)

    avisos, partes = [], []
    libro = xlrd.open_workbook(str(ruta), on_demand=True)
    try:
        for hoja in libro.sheets():
            filas = ([texto(c, libro.datemode) for c in hoja.row(i)] for i in range(hoja.nrows))
            df = _hoja_a_df(filas, Path(ruta).name, hoja.name, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.release_resources()
    return partes, avisos


def leer_tabla(ruta):
    """Lee un fichero tabular (CSV, XLSX, XLS o JSON, según su contenido real)
    con todas sus filas y columnas como texto. Devuelve (DataFrame, avisos)."""
    ruta = Path(ruta)
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato == "csv":
        return leer_csv(ruta)
    if formato in ("xlsx", "xls"):
        partes, avisos = (_leer_xlsx if formato == "xlsx" else _leer_xls)(ruta)
        if len(partes) > 1:
            avisos.append(f"{ruta.name}: {len(partes)} hojas con datos; se unen (columna _hoja)")
        if not partes:
            return pd.DataFrame(columns=["_hoja"]), avisos
        return pd.concat(partes, ignore_index=True, sort=False), avisos
    if formato == "json":
        return _leer_json(ruta), []
    raise ValueError(f"formato {formato.upper()} no tabular")


# ----------------------------------------------------------------------------
# Registros acumulados (comun/historico.py) y Parquet
# ----------------------------------------------------------------------------

def ordenar_columnas(df):
    """Columnas del portal (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def escribir_parquet(df, destino):
    """Parquet con todas las columnas como texto (_en_ultima_descarga booleana). Se escribe en un
    temporal y pasa por guardar_version: la versión anterior del Parquet queda en _historico/
    (comun.salida.escribir_parquet, con el ordenar_columnas de este script)."""
    return comun.salida.escribir_parquet(df, destino, ordenar_columnas=ordenar_columnas)


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen):
    """Registros de un fichero crudo a lo largo de todas sus versiones
    (comun.salida.acumular_fichero, con METADATOS_ORIGEN y leer_tabla de este script)."""
    return comun.salida.acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen,
                                         metadatos_origen=METADATOS_ORIGEN, leer_tabla=leer_tabla)


def construir_parquet(destino, ficheros, raw, manifiesto, resumen):
    """Genera `destino` con los registros acumulados de `ficheros` (lista de (ruta_actual, rel,
    metadatos)), cada uno desde todas sus versiones en raw/ con el código actual
    (comun.salida.construir_parquet, con METADATOS_ORIGEN, acumular_fichero y escribir_parquet de
    este script)."""
    return comun.salida.construir_parquet(destino, ficheros, raw, manifiesto, resumen,
                                          metadatos_origen=METADATOS_ORIGEN,
                                          acumular_fichero=acumular_fichero,
                                          escribir_parquet=escribir_parquet)


# ============================================================================
# CASTILLA Y LEÓN: CATÁLOGO OPENDATASOFT Y DESCARGAS
# ============================================================================

def listar_catalogo():
    """Todas las fichas del catálogo (paginado). Devuelve (fichas, total_count)."""
    fichas, total, offset = {}, None, 0
    while True:
        datos = pedir_json(URL_CATALOGO, params={"limit": LIMITE_PAGINA, "offset": offset, "order_by": "dataset_id"})
        if "results" in datos:
            pagina = datos.get("results") or []
        else:                                   # formato de la API v2.0
            pagina = [d.get("dataset", d) for d in datos.get("datasets") or []]
        total = datos.get("total_count", total)
        for ficha in pagina:
            if ficha.get("dataset_id"):
                fichas[ficha["dataset_id"]] = ficha      # páginas solapadas: una vez
        offset += len(pagina)
        if not pagina or (total is not None and offset >= total):
            break
        if offset + LIMITE_PAGINA > MAX_RESULTADOS_API:
            break
        time.sleep(PAUSA)
    return list(fichas.values()), total


def _metas(ficha):
    return ((ficha or {}).get("metas") or {}).get("default") or {}


def es_de_contratacion(ficha):
    metas = _metas(ficha)
    palabras = metas.get("keyword") or []
    if isinstance(palabras, str):
        palabras = [palabras]
    texto = " ".join([ficha.get("dataset_id") or "", metas.get("title") or ""] + [str(p) for p in palabras])
    return bool(PATRON_CONTRATACION.search(sin_acentos(texto)))


def fecha_portal(ficha):
    """Última modificación de los datos según el catálogo (epoch) o None."""
    metas = _metas(ficha)
    fechas = [a_epoch(metas.get(c)) for c in ("data_processed", "modified")]
    fechas = [f for f in fechas if f is not None]
    return max(fechas) if fechas else None


def procesar_dataset(ds, ficha, raw, manifiesto, resumen, comprobar_todo=False):
    destino = raw / "datasets" / f"{ds}.csv"
    rel = manifiesto.rel(destino)
    info = manifiesto.get(rel)
    if ficha is not None:
        guardar_json(raw / "datasets" / f"{ds}.metadatos.json", ficha)
    modificado = fecha_portal(ficha)
    comprobado = a_epoch(info.get("comprobado"))
    if (destino.exists() and not comprobar_todo and info.get("publicado", True)
            and modificado is not None and comprobado is not None and modificado <= comprobado):
        resumen.sin_cambios.append(f"{ds} (el catálogo no indica cambios desde {info['comprobado']})")
        return
    url = URL_EXPORT.format(id=ds)
    print(f"  ⬇️ {ds}...", end=" ", flush=True)
    estado, detalle = descargar(url, destino, params=PARAMS_EXPORT, tipo="csv")
    print(estado if not detalle else f"{estado}: {detalle}")
    if estado in ESTADOS_OK:
        manifiesto.registrar(destino, url_completa(url, PARAMS_EXPORT), estado, dataset=ds)
        resumen.descarga(ds, estado)
    elif estado == "no_existe" and destino.exists():
        manifiesto.retirar(destino, detalle)
        resumen.retirados.append(f"{ds}: la exportación da {detalle}; se conservan sus filas")
    elif estado == "no_existe" and ficha is None:
        resumen.no_publicado("ids conocidos que no existen en el portal", ds)
    else:
        resumen.fallidos.append(f"{ds}: {detalle} ({url})")
    time.sleep(PAUSA)


def procesar_historico(raw, manifiesto, resumen, comprobar_todo=False):
    destino = raw / "historico" / Path(urlparse(URL_HISTORICO).path).name
    if destino.exists() and not comprobar_todo:
        resumen.sin_cambios.append("histórico del perfil de contratante (fichero estático; "
                                   "--comprobar-todo para volver a pedirlo)")
        return
    print("  ⬇️ histórico del perfil de contratante...", end=" ", flush=True)
    estado, detalle = descargar(URL_HISTORICO, destino, tipo="csv")
    print(estado if not detalle else f"{estado}: {detalle}")
    if estado in ESTADOS_OK:
        manifiesto.registrar(destino, URL_HISTORICO, estado, dataset="historico-perfil-contratante")
        resumen.descarga("histórico del perfil de contratante", estado)
    elif estado == "no_existe" and destino.exists():
        manifiesto.retirar(destino, detalle)
        resumen.retirados.append(f"histórico del perfil de contratante: {detalle}; se conservan sus filas")
    elif estado == "invalido" and detalle.startswith(MOTIVO_SIN_DATOS):
        resumen.no_publicado("sin datos en el portal (no se crea tabla)", "histórico del perfil de contratante")
    else:
        resumen.fallidos.append(f"histórico del perfil de contratante: {detalle} ({URL_HISTORICO})")


def descargar_todo(raw, manifiesto, resumen, comprobar_todo=False):
    print("\n🔎 Catálogo del portal de análisis...")
    catalogo = None
    try:
        fichas, total = listar_catalogo()
        catalogo = {f["dataset_id"]: f for f in fichas}
        guardar_json(raw / "catalogo.json", fichas)
        print(f"   {len(catalogo)} datasets en el catálogo")
        if total is not None and len(catalogo) < total:
            resumen.fallidos.append(f"catálogo incompleto: {len(catalogo)} de {total} datasets")
    except ErrorPortal as e:
        resumen.fallidos.append(f"catálogo ({URL_CATALOGO}): {e}; solo se piden los datasets conocidos")

    seleccion = {}
    for ds, ficha in (catalogo or {}).items():
        if es_de_contratacion(ficha):
            seleccion[ds] = ficha
    descubiertos = sorted(ds for ds in seleccion if ds not in DATASETS_CONOCIDOS)
    if descubiertos:
        resumen.avisos.append("datasets de contratación descubiertos en el catálogo (no estaban en la lista conocida): "
                              + ", ".join(f"{ds} ({_metas(seleccion[ds]).get('title', '')})" for ds in descubiertos))
    for ds in DATASETS_CONOCIDOS:
        seleccion.setdefault(ds, (catalogo or {}).get(ds))
        if catalogo is not None and ds not in catalogo:
            resumen.avisos.append(f"{ds}: id conocido que no aparece en el catálogo")
    # Los que ya se tenían, aunque hayan desaparecido del catálogo: si el portal
    # ya no los sirve se marcan como retirados (sus filas se conservan)
    for previo in sorted((raw / "datasets").glob("*.csv")):
        seleccion.setdefault(previo.stem, (catalogo or {}).get(previo.stem))

    print(f"\n📦 {len(seleccion)} datasets de contratación")
    for ds in sorted(seleccion):
        procesar_dataset(ds, seleccion[ds], raw, manifiesto, resumen, comprobar_todo)
    procesar_historico(raw, manifiesto, resumen, comprobar_todo)


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    for actual in sorted((raw / "datasets").glob("*.csv")):
        ds, rel = actual.stem, manifiesto.rel(actual)
        fuente = manifiesto.get(rel).get("url") or url_completa(URL_EXPORT.format(id=ds), PARAMS_EXPORT)
        metadatos = {"_fuente": fuente, "_dataset": ds, "_archivo_origen": rel}
        construir_parquet(salida / f"{ds}.parquet", [(actual, rel, metadatos)], raw, manifiesto, resumen)
    historico = raw / "historico" / Path(urlparse(URL_HISTORICO).path).name
    if historico.exists():
        rel = manifiesto.rel(historico)
        metadatos = {"_fuente": manifiesto.get(rel).get("url") or URL_HISTORICO,
                     "_dataset": "historico-perfil-contratante", "_archivo_origen": rel}
        construir_parquet(salida / PARQUET_HISTORICO, [(historico, rel, metadatos)], raw, manifiesto, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga la contratación pública de Castilla y León")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar los Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también lo que el portal no marca como cambiado")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Portal: {BASE_URL}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
