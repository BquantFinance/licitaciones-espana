#!/usr/bin/env python3
"""
=============================================================================
REGIÓN DE MURCIA - CONTRATACIÓN PÚBLICA (CARM y Servicio Murciano de Salud)
=============================================================================
Descarga los ficheros anuales de contratación de la Comunidad Autónoma de la
Región de Murcia tal cual los publica (con todas sus versiones) y genera un
Parquet por serie con todas las columnas y filas de todos los años, como texto.

Ejecutar:  python scripts/ccaa_murcia.py [--salida DIR] [--desde AÑO] [--hasta AÑO]
           [--solo-descarga] [--solo-parquet] [--comprobar-todo]

Series (un fichero por año):
    contratos_carm          Contratos de la CARM inscritos en el registro (sin menores), CSV
    contratos_menores_carm  Contratos menores de la CARM, CSV
    contratos_menores_sms   Contratos menores del Servicio Murciano de Salud, XLSX (o XLS)

Salida (por defecto <repo>/ccaa_murcia/):
    raw/<serie>/<fichero original>     p.ej. raw/contratos_carm/contratosOD2019.csv
    raw/<serie>/_historico/            versiones anteriores de cada fichero (nunca se borran)
    raw/catalogo_ckan.json             inventario del catálogo CKAN regional (q=contrat)
    raw/_manifiesto.json               URL, fecha de descarga y última comprobación de cada fichero
    raw/descarga_log.txt               resumen de cada ejecución (se añade al final)
    <serie>.parquet                    todos los años de la serie (todas las columnas, texto)
    _historico/                        versiones anteriores de los Parquet

Columnas añadidas: _fuente (URL descargada), _dataset (serie), _anio_fichero,
_archivo_origen (ruta en raw/), _hoja (solo Excel), _fecha_descarga y, de
comun/historico.py, _primera_descarga, _ultima_descarga y _en_ultima_descarga.
Un registro que el portal retira o modifica NO desaparece: sigue en el Parquet
con _en_ultima_descarga=False (control del sesgo del superviviente).

Qué se descarga:
- Cada serie se sondea año a año desde --desde (2010 por defecto) hasta el año
  en curso. Un 404 en un año no confirmado se anota como "no publicado"; en un
  año confirmado (ver SERIES) es un error.
- El catálogo CKAN regional (package_search?q=contrat) se guarda como
  inventario: los recursos que casan con el nombre de fichero de una serie
  añaden su año y su URL (aunque esté fuera del rango sondeado); los demás se
  listan en el resumen para revisarlos.
- Se completan los años que faltan y se vuelven a pedir el año en curso y el
  anterior (el fichero de un año sigue creciendo en los primeros meses del
  siguiente); los años más antiguos solo con --comprobar-todo. Todo fichero
  pasa por comun.historico.guardar_version: si no cambia no se toca y si
  cambia la copia anterior queda en _historico/. Si un fichero que se tenía
  pasa a dar 404 se marca como retirado (sus filas se conservan).

FUENTES
-------
Confirmado en páginas oficiales (investigación previa, confianza A):
  - https://datosabiertos.carm.es/odata/transparencia/contratosOD{AÑO}.csv  (2019-2023)
  - https://datosabiertos.carm.es/odata/Hacienda/CONTRA_ContratosMenores_{AÑO}.csv  (2022-2025)
  - https://transparencia.carm.es/wres/transparencia/doc/Sector_Publico/SMS/Contratos_menores/
    Contratos_menores_SMS_{AÑO}.xlsx  (2020)
  - Catálogo CKAN: https://datosabiertos.regiondemurcia.es/api/3/action/package_search?q=contrat
VERIFICAR EN VIVO (el sandbox donde se escribió no llega a los portales):
  - Qué otros años existen en cada serie (el script los sondea y lo anota).
  - Si los años del SMS distintos de 2020 usan .xlsx o .xls y el mismo nombre.
  - Mayúsculas/minúsculas de las rutas (odata/transparencia frente a
    odata/Transparencia) y si los ficheros inexistentes dan 404 o una página
    HTML con 200 (el script rechaza el HTML y lo trata como "no publicado").
  - Codificación y separador de los CSV (se detectan: UTF-8/cp1252; ; , tab |).
  - Que package_search devuelve result.count/result.results[].resources[].url.
=============================================================================
"""

import argparse
import codecs
import datetime as dt
import json
import math
import os
import re
import sys
import time
import unicodedata
import warnings
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

ANIO_MINIMO = 2010   # primer año que se sondea por defecto

URL_SMS = ("https://transparencia.carm.es/wres/transparencia/doc/Sector_Publico/SMS/"
           "Contratos_menores/Contratos_menores_SMS_{anio}")

# plantillas: URL con {anio}, en orden de preferencia. El nombre local de cada
# año es el de la primera plantilla (con la extensión que se haya descargado).
# confirmados: años que constan publicados; si faltan, es un error.
SERIES = {
    "contratos_carm": {
        "descripcion": "Contratos de la CARM inscritos en el registro (sin menores)",
        "plantillas": ["https://datosabiertos.carm.es/odata/transparencia/contratosOD{anio}.csv"],
        "confirmados": range(2019, 2024),
    },
    "contratos_menores_carm": {
        "descripcion": "Contratos menores de la CARM",
        "plantillas": ["https://datosabiertos.carm.es/odata/Hacienda/CONTRA_ContratosMenores_{anio}.csv"],
        "confirmados": range(2022, 2026),
    },
    "contratos_menores_sms": {
        "descripcion": "Contratos menores del Servicio Murciano de Salud",
        "plantillas": [URL_SMS + ".xlsx", URL_SMS + ".xls"],
        "confirmados": range(2020, 2021),
    },
}

URL_CKAN = "https://datosabiertos.regiondemurcia.es/api/3/action/package_search"
CONSULTA_CKAN = "contrat"
FILAS_CKAN = 100

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_murcia"
TITULO = "REGIÓN DE MURCIA - CONTRATACIÓN PÚBLICA"


# ============================================================================
# UTILIDADES COMUNES (mismo bloque en ccaa_castilla_leon.py, ccaa_murcia.py y
# ccaa_navarra.py): descarga con reintentos, versiones, lectura como texto,
# acumulación de registros y Parquet.
# ============================================================================

CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 60
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
PAUSA = 0.3                  # entre peticiones, para no cargar el portal
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones: el mismo
# registro servido desde otra URL u otra hoja sigue siendo el mismo.
METADATOS_ORIGEN = ("_fuente", "_dataset", "_recurso", "_anio_fichero", "_archivo_origen", "_hoja", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


def iso(momento):
    """Fecha ISO 8601 en UTC al segundo: '2026-09-27T14:43:00Z' (ordenable como texto)."""
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def fecha_version(ruta):
    """Fecha de una versión de un fichero crudo: el sello que guardar_version pone
    en _historico/ (fecha de esa copia) o, para la copia actual, su fecha de
    modificación. Al pasar a _historico/ la copia conserva la misma fecha."""
    ruta = Path(ruta)
    if ruta.parent.name == HISTORICO:
        sellos = re.findall(r"__(\d{8}T\d{6}Z)", ruta.name)
        if sellos:
            return iso(datetime.strptime(sellos[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc))
    return iso(datetime.fromtimestamp(ruta.stat().st_mtime, timezone.utc))


def url_completa(url, params=None):
    return requests.Request("GET", url, params=params).prepare().url


def sin_acentos(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto)) if not unicodedata.combining(c))


class ErrorPortal(Exception):
    def __init__(self, mensaje, codigo=None):
        super().__init__(mensaje)
        self.codigo = codigo


def _espera(intento, respuesta=None):
    """Backoff exponencial, o lo que pida el servidor en Retry-After."""
    valor = (getattr(respuesta, "headers", None) or {}).get("Retry-After") if respuesta is not None else None
    if valor and str(valor).strip().isdigit():
        return min(float(valor), ESPERA_MAXIMA)
    return min(ESPERA_BASE * 2 ** (intento - 1), ESPERA_MAXIMA)


def pedir_json(url, params=None):
    """GET a una API JSON con reintentos (red, 429, 5xx, respuesta no JSON).
    Un 4xx no se reintenta: se lanza ErrorPortal con el código."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.json()
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except ValueError as e:
            detalle = f"respuesta no JSON ({str(e)[:80]})"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes."""
    if cabeza.startswith(b"PK\x03\x04"):
        return "xlsx"
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "csv"                                      # texto UTF-16
    texto = cabeza.lstrip(b"\xef\xbb\xbf").lstrip()
    if texto[:1] in (b"{", b"["):
        return "json"
    if texto[:1] == b"<":
        return "html" if b"html" in texto[:1024].lower() else "xml"
    return "csv"


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
    if tipo in ("xlsx", "xls"):
        return None if formato in ("xlsx", "xls") else f"se esperaba una hoja de cálculo y llegó {formato}"
    return None if formato == tipo else f"se esperaba {tipo.upper()} y llegó {formato}"


def descargar(url, destino, params=None, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, comprueba que es el tipo de fichero esperado y lo
    entrega a guardar_version(): si el contenido no cambió no se toca nada y si
    cambió la copia previa pasa a _historico/. Reintenta con backoff los fallos
    de red, 429 y 5xx (también cortes a mitad de descarga).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    detalle = ""
    try:
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                with requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_DESCARGA,
                                  stream=True) as respuesta:
                    codigo = respuesta.status_code
                    if codigo in (404, 410):
                        return "no_existe", f"HTTP {codigo}"
                    if codigo not in CODIGOS_REINTENTABLES:
                        if codigo >= 400:
                            return "error", f"HTTP {codigo}"
                        with open(tmp, "wb") as f:
                            for trozo in respuesta.iter_content(chunk_size=1 << 16):
                                if trozo:
                                    f.write(trozo)
                        motivo = validar_contenido(tmp, tipo)
                        if motivo:
                            return "invalido", motivo
                        return guardar_version(destino, desde=tmp), ""
                    detalle = f"HTTP {codigo}"
            except ERRORES_RED as e:
                detalle = f"{type(e).__name__}: {str(e)[:150]}"
            except requests.exceptions.RequestException as e:
                return "error", f"{type(e).__name__}: {str(e)[:150]}"
            if intento < INTENTOS:
                time.sleep(_espera(intento, respuesta))
        return "error", f"{detalle} (tras {INTENTOS} intentos)"
    finally:
        if tmp.exists():
            tmp.unlink()


def guardar_json(destino, datos):
    """Guarda metadatos del portal (catálogo, fichas) conservando sus versiones."""
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    return guardar_version(destino, contenido)


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

def _detectar_codificacion(ruta):
    """Primera codificación capaz de decodificar el fichero COMPLETO. cp1252 va
    antes que latin-1 (latin-1 acepta cualquier byte y dejaría '€', '’'... como
    caracteres de control)."""
    with open(ruta, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    candidatas = ["utf-8-sig"] if inicio.startswith(b"\xef\xbb\xbf") else ["utf-8"]
    for codificacion in candidatas + ["cp1252", "latin-1"]:
        decodificador = codecs.getincrementaldecoder(codificacion)()
        try:
            with open(ruta, "rb") as f:
                while bloque := f.read(1 << 20):
                    decodificador.decode(bloque)
            decodificador.decode(b"", final=True)
            return codificacion
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detectar_separador(ruta, codificacion):
    """Separador más frecuente (fuera de comillas) en la primera línea con texto."""
    with open(ruta, encoding=codificacion, newline="") as f:
        primera = next((linea for linea in f if linea.strip()), "")
    cuentas = dict.fromkeys([";", ",", "\t", "|"], 0)
    entre_comillas = False
    for caracter in primera:
        if caracter == '"':
            entre_comillas = not entre_comillas
        elif not entre_comillas and caracter in cuentas:
            cuentas[caracter] += 1
    mejor = max(cuentas, key=cuentas.get)
    return mejor if cuentas[mejor] else ","


def _lineas_de_datos(ruta):
    """Líneas no vacías del fichero menos la cabecera (para avisar si el
    analizador junta líneas: comillas desparejadas, saltos de línea en campos)."""
    with open(ruta, "rb") as f:
        return max(sum(1 for linea in f if linea.strip()) - 1, 0)


def _nombres_columnas(cabecera):
    """Nombres únicos como los pone pandas: vacíos -> 'Unnamed: i', repetidos -> 'X.1'."""
    nombres, vistos = [], set()
    for i, valor in enumerate(cabecera):
        nombre = "" if valor is None else str(valor)
        if nombre == "":
            nombre = f"Unnamed: {i}"
        if nombre in vistos:
            k = 1
            while f"{nombre}.{k}" in vistos:
                k += 1
            nombre = f"{nombre}.{k}"
        vistos.add(nombre)
        nombres.append(nombre)
    return nombres


def _leer_csv_tolerante(ruta, sep, codificacion):
    """Lectura que no descarta nada: como el módulo csv, pero una comilla
    literal al principio de un campo no se traga los registros siguientes
    (comun.lectura_csv); los campos que sobran respecto a la cabecera van a
    columnas _columna_extra_N. Devuelve (df, filas con campos de más,
    comillas literales)."""
    with open(ruta, encoding=codificacion, newline="") as f:
        filas, literales = registros_csv(f.read(), sep)
    if not filas:
        return pd.DataFrame(), 0, 0
    nombres = _nombres_columnas(filas[0])
    ancho = max(len(fila) for fila in filas)
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    datos = [[v if v != "" else None for v in fila] + [None] * (ancho - len(fila)) for fila in filas[1:]]
    df = pd.DataFrame(datos, columns=nombres, dtype=object)
    extra = [c for c in nombres if c.startswith("_columna_extra_")]
    con_extra = int(df[extra].notna().any(axis=1).sum()) if extra else 0
    vacias = [c for c in extra if df[c].isna().all()]
    return df.drop(columns=vacias), con_extra, literales


def leer_csv(ruta):
    """CSV como texto: dtype=str, sin convertir 'NA', 'N/A', 'NULL'... en nulos
    (solo el campo vacío es nulo) y sin perder filas ni campos."""
    ruta = Path(ruta)
    avisos = []
    codificacion = _detectar_codificacion(ruta)
    sep = _detectar_separador(ruta, codificacion)
    try:
        with warnings.catch_warnings():
            # "Length of header or names does not match": pandas perdería campos
            warnings.simplefilter("error", pd.errors.ParserWarning)
            df = pd.read_csv(ruta, sep=sep, encoding=codificacion, dtype=str, keep_default_na=False,
                             na_values=[""], index_col=False, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), [f"{ruta.name}: fichero sin cabecera ni filas"]
    except (pd.errors.ParserError, pd.errors.ParserWarning):
        df, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
        if con_extra:
            avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; "
                          "los campos de más se conservan en columnas _columna_extra_N")
        if literales:
            avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo "
                          "(se conservan en el texto; sin ellas se tragarían los registros siguientes)")
    if codificacion != "utf-16":
        lineas = _lineas_de_datos(ruta)
        if lineas != len(df):
            # Puede ser una comilla literal que se traga registros enteros:
            # se vuelve a leer sin que lo haga (comun.lectura_csv)
            tolerante, con_extra, literales = _leer_csv_tolerante(ruta, sep, codificacion)
            if literales and len(tolerante) > len(df):
                avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo se "
                              f"tragaban {len(tolerante) - len(df):,} registros; se leen sin tragárselos "
                              "(la comilla se conserva en el texto)")
                if con_extra:
                    avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; "
                                  "los campos de más se conservan en columnas _columna_extra_N")
                df = tolerante
        if lineas != len(df):
            avisos.append(f"{ruta.name}: {len(df):,} filas leídas de {lineas:,} líneas de datos "
                          "(campos entrecomillados con saltos de línea o comillas desparejadas): revisar")
    return df, avisos


def _celda_texto(valor):
    """Valor de una celda de Excel como texto (None si está vacía)."""
    if valor is None:
        return None
    if isinstance(valor, str):
        return valor if valor != "" else None
    if isinstance(valor, bool):
        return str(valor)
    if isinstance(valor, float):
        if math.isnan(valor):
            return None
        return str(int(valor)) if valor.is_integer() else repr(valor)
    if isinstance(valor, datetime):
        if valor.tzinfo is None and valor.time() == dt.time(0):
            return valor.date().isoformat()
        return valor.isoformat(sep=" ")
    if isinstance(valor, (dt.date, dt.time)):
        return valor.isoformat()
    return str(valor)


def _hoja_a_df(filas, nombre, hoja, avisos):
    """Tabla de una hoja: detecta la fila de cabecera (las filas de título de
    encima se anotan en los avisos) y conserva todas las filas con algún valor."""
    # Número de fila original de cada fila con algún valor (para el aviso)
    numeradas = [(i, list(f)) for i, f in enumerate(filas) if any(v is not None for v in f)]
    if not numeradas:
        return None
    filas = [f for _, f in numeradas]
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    if pos:
        titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
        avisos.append(f"{nombre} [{hoja}]: {numeradas[pos][0]} filas antes de la cabecera "
                      f"(no son datos): {titulo[:200]}")
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
    # Se abre como fichero: openpyxl rechaza por la extensión un .xlsx publicado
    # con nombre .xls, y el formato ya se ha decidido por el contenido.
    fichero = open(ruta, "rb")
    try:
        libro = openpyxl.load_workbook(fichero, read_only=True, data_only=True)
    except BaseException:
        fichero.close()
        raise
    try:
        for hoja in libro.worksheets:
            hoja.reset_dimensions()   # no fiarse de la dimensión declarada en el fichero
            filas = ([_celda_texto(v) for v in fila] for fila in hoja.iter_rows(values_only=True))
            df = _hoja_a_df(filas, Path(ruta).name, hoja.title, avisos)
            if df is not None:
                partes.append(df)
    finally:
        libro.close()
        fichero.close()
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


def _buscar_registros(datos, profundidad=0):
    """Primera lista de objetos de un JSON (la raíz, result.records, data...)."""
    if isinstance(datos, list) and all(isinstance(x, dict) for x in datos):
        return datos
    if isinstance(datos, dict) and profundidad < 4:
        claves = ["records", "result", "results", "data", "items"]
        for clave in claves + [k for k in datos if k not in claves]:
            encontrado = _buscar_registros(datos.get(clave), profundidad + 1)
            if encontrado is not None:
                return encontrado
    return None


def _leer_json(ruta):
    with open(ruta, encoding=_detectar_codificacion(ruta)) as f:
        registros = _buscar_registros(json.load(f))
    if registros is None:
        raise ValueError("el JSON no contiene una lista de registros")

    def texto(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v if v != "" else None
        return json.dumps(v, ensure_ascii=False)

    columnas = list(dict.fromkeys(k for r in registros for k in r))
    return pd.DataFrame([[texto(r.get(c)) for c in columnas] for r in registros], columns=columnas, dtype=object)


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

def _como_texto(serie):
    """Serie de texto (None = nulo) para escribirla como string en Parquet."""
    if isinstance(serie.dtype, pd.StringDtype):
        return serie
    valores = serie.astype(object)
    if pd.api.types.infer_dtype(valores, skipna=True) in ("string", "empty"):
        return valores
    return valores.map(lambda v: v if isinstance(v, str) else (None if pd.isna(v) else str(v)))


def ordenar_columnas(df):
    """Columnas del portal (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def escribir_parquet(df, destino):
    """Parquet con todas las columnas como texto (_en_ultima_descarga booleana).
    Se escribe en un temporal y pasa por guardar_version: la versión anterior
    del Parquet queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df = ordenar_columnas(df).copy()
    campos = []
    for columna in df.columns:
        if columna == "_en_ultima_descarga":
            df[columna] = df[columna].astype(bool)
            campos.append(pa.field(columna, pa.bool_()))
        else:
            df[columna] = _como_texto(df[columna])
            campos.append(pa.field(str(columna), pa.string()))
    tabla = pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen):
    """Registros de un fichero crudo a lo largo de todas sus versiones.

    anterior: filas de este fichero en el Parquet anterior (o None). Solo se
    aplican las versiones posteriores a su _ultima_descarga, en orden
    cronológico, con acumular(): nada de lo visto se pierde; lo que el portal
    retira o cambia queda con _en_ultima_descarga=False. Después se aplica la
    última comprobación: sin cambios (actualiza _ultima_descarga) o retirado
    por el portal (todo el fichero pasa a _en_ultima_descarga=False).
    """
    info = manifiesto.get(rel)
    corte = None
    if anterior is not None and len(anterior) and "_ultima_descarga" in anterior.columns:
        corte = anterior["_ultima_descarga"].dropna().astype(str).max()
    lista = versiones(actual)
    comprobado = info.get("comprobado")
    posterior = comprobado is not None and (corte is None or comprobado > corte)
    retirado = info.get("publicado") is False and posterior
    reconfirmado = (posterior and not retirado and bool(lista) and lista[-1] == Path(actual)
                    and comprobado > fecha_version(actual))
    ignorar = METADATOS_ORIGEN
    ultima = None
    for version in lista:
        fecha = fecha_version(version)
        nueva = corte is None or fecha > corte
        if not nueva and not (reconfirmado and version == lista[-1]):
            continue
        try:
            df, avisos = leer_tabla(version)
        except Exception as e:
            resumen.fallidos.append(f"{rel}: no se pudo leer la versión {version.name}: {e}")
            continue
        resumen.avisos.extend(avisos)
        df = df.copy()
        for columna, valor in metadatos.items():
            df[columna] = valor
        df["_fecha_descarga"] = fecha
        ultima = df
        if not nueva:
            continue
        try:
            anterior = acumular(anterior, df, fecha, ignorar=ignorar)
        except ValueError:
            resumen.avisos.append(f"{rel}: la versión del {fecha} no tiene filas; no se marca nada como retirado")
    if anterior is None or not len(anterior):
        return anterior
    if retirado:
        anterior = acumular(anterior, pd.DataFrame(), comprobado, ignorar=ignorar, permitir_vacio=True)
    elif reconfirmado and ultima is not None and len(ultima):
        anterior = acumular(anterior, ultima, comprobado, ignorar=ignorar)
    return anterior


def construir_parquet(destino, ficheros, raw, manifiesto, resumen):
    """Genera `destino` con los registros acumulados de `ficheros`
    (lista de (ruta_actual, rel, metadatos)) partiendo del Parquet anterior.

    Las filas de ficheros que ya no se procesan (retirados, otro formato...) se
    conservan: si el fichero sigue en raw/ se vuelve a procesar con sus
    versiones; si no, se copian tal cual del Parquet anterior.
    """
    destino = Path(destino)
    try:
        previo = leer_registros(destino)
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    por_fichero = {}
    if previo is not None and "_archivo_origen" in previo.columns:
        por_fichero = {str(k): g for k, g in previo.groupby("_archivo_origen", sort=False)}
    ficheros = list(ficheros)
    procesados = {rel for _, rel, _ in ficheros}
    for rel, grupo in por_fichero.items():
        if rel not in procesados and (Path(raw) / rel).exists():
            metadatos = {c: grupo[c].iloc[0] for c in METADATOS_ORIGEN
                         if c in grupo.columns and c not in ("_hoja", "_fecha_descarga")}
            ficheros.append((Path(raw) / rel, rel, metadatos))
            procesados.add(rel)
    partes = []
    for actual, rel, metadatos in ficheros:
        registros = acumular_fichero(actual, rel, por_fichero.pop(rel, None), metadatos, manifiesto, resumen)
        if registros is not None and len(registros):
            partes.append(registros)
    for rel, grupo in por_fichero.items():
        resumen.avisos.append(f"{destino.name}: {rel} ya no está en raw/; se conservan sus {len(grupo):,} filas")
        partes.append(grupo)
    if not partes:
        return None
    df = pd.concat(partes, ignore_index=True, sort=False)
    estado = escribir_parquet(df, destino)
    retiradas = int((~df["_en_ultima_descarga"].astype(bool)).sum())
    resumen.parquets.append((destino.name, len(df), len(df.columns), retiradas))
    print(f"  💾 {destino.name}: {len(df):,} filas ({estado})")
    return df


# ============================================================================
# REGIÓN DE MURCIA: SERIES ANUALES
# ============================================================================

def _tipo(url):
    extension = Path(urlparse(url).path).suffix.lower().lstrip(".")
    return extension if extension in ("csv", "xlsx", "xls", "json") else None


def _nombre_local(serie, anio):
    """Nombre (sin extensión) del fichero de un año: el de la primera plantilla."""
    return Path(urlparse(serie["plantillas"][0].format(anio=anio)).path).stem


def _patron(texto, extension=True):
    """Regex de un nombre de fichero con '{anio}' como grupo del año."""
    antes, despues = texto.split("{anio}")
    cola = re.escape(despues) + (r"\.(\w+)" if extension else "")
    return re.compile(re.escape(antes) + r"((?:19|20)\d{2})" + cola, re.IGNORECASE)


def candidatos(serie, anio, extra=()):
    """(url, nombre_local, tipo) a probar para un año, en orden: las plantillas
    y después las URL de ese año encontradas en el catálogo CKAN."""
    lista, vistas = [], set()
    for url in [p.format(anio=anio) for p in serie["plantillas"]] + list(extra):
        if url in vistas:
            continue
        vistas.add(url)
        tipo = _tipo(url) or _tipo(serie["plantillas"][0])
        lista.append((url, f"{_nombre_local(serie, anio)}.{tipo}", tipo))
    return lista


def archivos_locales(dir_serie, serie):
    """{año: [ruta, ...]} de las copias actuales de una serie; si un año tiene
    varias (p.ej. .xlsx y .xls), primero la del formato preferido."""
    patron = _patron(_nombre_local(serie, "{anio}"))
    orden = [_tipo(p) for p in serie["plantillas"]]
    por_anio = {}
    if Path(dir_serie).is_dir():
        for ruta in Path(dir_serie).iterdir():
            m = patron.fullmatch(ruta.name) if ruta.is_file() else None
            if m:
                por_anio.setdefault(int(m.group(1)), []).append(ruta)
    for rutas in por_anio.values():
        rutas.sort(key=lambda r: (orden.index(r.suffix.lower().lstrip("."))
                                  if r.suffix.lower().lstrip(".") in orden else len(orden), r.name))
    return dict(sorted(por_anio.items()))


def inventario_ckan(raw, resumen):
    """Catálogo CKAN regional (q=contrat). Devuelve ({(serie, año): [url]}, otros)."""
    print("\n🔎 Catálogo CKAN regional...")
    paquetes, inicio, total = {}, 0, None
    try:
        while True:
            datos = pedir_json(URL_CKAN, params={"q": CONSULTA_CKAN, "rows": FILAS_CKAN, "start": inicio})
            resultado = datos.get("result") or {}
            lote = resultado.get("results") or []
            total = resultado.get("count", total)
            for paquete in lote:
                paquetes[paquete.get("id") or paquete.get("name")] = paquete   # páginas solapadas: una vez
            inicio += len(lote)
            if not lote or (total is not None and inicio >= total):
                break
            time.sleep(PAUSA)
    except ErrorPortal as e:
        resumen.avisos.append(f"catálogo CKAN regional ({URL_CKAN}): {e}; se sigue con las URL conocidas")
        return {}, []
    paquetes = list(paquetes.values())
    guardar_json(raw / "catalogo_ckan.json", paquetes)
    print(f"   {len(paquetes)} datasets con '{CONSULTA_CKAN}'")

    patrones = {clave: [_patron(Path(urlparse(p).path).name, extension=False) for p in serie["plantillas"]]
                for clave, serie in SERIES.items()}
    encontrados, otros = {}, []
    for paquete in paquetes:
        for recurso in paquete.get("resources") or []:
            url = (recurso.get("url") or "").strip()
            if not url:
                continue
            nombre = Path(unquote(urlparse(url).path)).name
            casa = next(((clave, int(m.group(1))) for clave, lista in patrones.items()
                         for patron in lista for m in [patron.fullmatch(nombre)] if m), None)
            if casa:
                encontrados.setdefault(casa, []).append(url)
            else:
                otros.append(f"{paquete.get('name')}: {recurso.get('name') or nombre} ({url})")
    return encontrados, otros


def descargar_serie(clave, serie, anios, raw, manifiesto, resumen, comprobar_todo=False, extra=None):
    dir_serie = raw / clave
    anio_actual = ahora().year
    extra = extra or {}
    print(f"\n📦 {clave}: {serie['descripcion']}")
    for anio in anios:
        lista = candidatos(serie, anio, extra.get((clave, anio), ()))
        existentes = [dir_serie / nombre for _, nombre, _ in lista if (dir_serie / nombre).exists()]
        if existentes and not comprobar_todo and anio < anio_actual - 1:
            resumen.sin_cambios.append(f"{clave} {anio} (ya descargado; --comprobar-todo para volver a pedirlo)")
            continue
        fallos = []
        for url, nombre, tipo in lista:
            destino = dir_serie / nombre
            estado, detalle = descargar(url, destino, tipo=tipo)
            time.sleep(PAUSA)
            if estado in ESTADOS_OK:
                manifiesto.registrar(destino, url, estado, dataset=clave, anio=anio)
                resumen.descarga(f"{clave} {anio}", estado)
                print(f"  ✅ {anio}: {estado}")
                break
            fallos.append((url, estado, detalle))
        else:
            # Un error (red, 5xx) o una respuesta rara teniendo copia no permite
            # saber si el portal lo ha retirado: se conserva todo y se reintenta
            errores = [f for f in fallos if f[1] == "error" or (f[1] == "invalido" and existentes)]
            if errores:
                url, _, detalle = errores[0]
                resumen.fallidos.append(f"{clave} {anio}: {detalle} ({url})")
                print(f"  ❌ {anio}: {detalle}")
            elif existentes:
                for destino in existentes:
                    manifiesto.retirar(destino, fallos[0][2])
                resumen.retirados.append(f"{clave} {anio}: el portal ya no lo sirve ({fallos[0][2]}); "
                                         "se conservan sus filas")
                print(f"  🗑️ {anio}: retirado por el portal")
            elif anio in serie["confirmados"]:
                resumen.fallidos.append(f"{clave} {anio}: año publicado según las fuentes y ahora no "
                                        f"disponible ({fallos[0][2]}; {fallos[0][0]})")
                print(f"  ❌ {anio}: {fallos[0][2]}")
            else:
                resumen.no_publicado(clave, anio)


def descargar_todo(raw, manifiesto, resumen, desde, hasta, comprobar_todo=False):
    extra, otros = inventario_ckan(raw, resumen)
    if otros:
        resumen.avisos.append(f"{len(otros)} recursos del catálogo CKAN con '{CONSULTA_CKAN}' no son de las "
                              "series descargadas (revisar):\n      " + "\n      ".join(otros))
    for clave, serie in SERIES.items():
        anios = set(range(desde, hasta + 1)) | set(serie["confirmados"])
        anios |= {anio for (c, anio) in extra if c == clave}
        anios |= set(archivos_locales(raw / clave, serie))
        descargar_serie(clave, serie, sorted(anios), raw, manifiesto, resumen, comprobar_todo, extra)


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    for clave, serie in SERIES.items():
        ficheros = []
        for anio, rutas in archivos_locales(raw / clave, serie).items():
            ruta = rutas[0]
            if len(rutas) > 1:
                resumen.avisos.append(f"{clave} {anio}: se usa {ruta.name} (también hay "
                                      + ", ".join(r.name for r in rutas[1:]) + ")")
            rel = manifiesto.rel(ruta)
            metadatos = {"_fuente": manifiesto.get(rel).get("url") or serie["plantillas"][0].format(anio=anio),
                         "_dataset": clave, "_anio_fichero": str(anio), "_archivo_origen": rel}
            ficheros.append((ruta, rel, metadatos))
        destino = salida / f"{clave}.parquet"
        if ficheros or destino.exists():
            construir_parquet(destino, ficheros, raw, manifiesto, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga la contratación pública de la Región de Murcia")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--desde", type=int, default=ANIO_MINIMO, help="primer año que se sondea")
    parser.add_argument("--hasta", type=int, default=None, help="último año que se sondea (por defecto el actual)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar los Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años antiguos ya descargados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    hasta = args.hasta or ahora().year
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Años sondeados: {args.desde}-{hasta}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, args.desde, hasta, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
