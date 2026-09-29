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
En los CSV que el portal genera desde JSON (ver «CSV del exportador JSON»), además:
_resto_json (restos de la lista JSON que no son de ningún campo) y
_<columna>_sin_cortes (el texto sin los cortes de línea cada 80 caracteres; la
columna original se sirve tal cual).

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
  - Contratos menores del SMS: los ficheros que enlaza
    https://transparencia.carm.es/web/transparencia/contratos-y-convenios-del-sector-publico
    en .../Sector_Publico/SMS/Contratos_menores/, cada año con otro nombre
    (verificado el 2026-09-27: PT_SMS_{1-4}T2019, Contratos_menores_SMS_2020,
    SMS_Contratos_Menores_2021, Contratos_Menores_SMS_{2022-2024},
    SMS_Contratos_menores_2025; 748.984 líneas con NIF). Se guardan con su
    nombre publicado y el año del nombre; lo que la página deja de enlazar
    queda como retirado.
  - Catálogo CKAN: https://datosabiertos.regiondemurcia.es/api/3/action/package_search?q=contrat
VERIFICAR EN VIVO (el sandbox donde se escribió no llega a los portales):
  - Qué otros años existen en cada serie (el script los sondea y lo anota).
  - Mayúsculas/minúsculas de las rutas (odata/transparencia frente a
    odata/Transparencia) y si los ficheros inexistentes dan 404 o una página
    HTML con 200 (el script rechaza el HTML y lo trata como "no publicado").
  - Codificación y separador de los CSV (se detectan: UTF-8/cp1252/latin-1/cp850; ; , tab |).
  - Que package_search devuelve result.count/result.results[].resources[].url.
=============================================================================
"""

import argparse
import codecs
import datetime as dt
import html
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
from urllib.parse import unquote, urljoin, urlparse

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
URL_SECTOR_PUBLICO = "https://transparencia.carm.es/web/transparencia/contratos-y-convenios-del-sector-publico"

# plantillas: URL con {anio}, en orden de preferencia. El nombre local de cada
# año es el de la primera plantilla (con la extensión que se haya descargado).
# confirmados: años que constan publicados; si faltan, es un error.
# pagina/enlaces: serie sin nombre fijo; sus ficheros son los que enlaza la
# página (ruta que casa con la regex `enlaces`), cada uno con su nombre
# publicado y el año que trae en el nombre (descargar_enlazados).
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
        # Cada año con otro nombre (PT_SMS_1T2019…4T2019 por trimestres,
        # Contratos_menores_SMS_2020, SMS_Contratos_Menores_2021,
        # Contratos_Menores_SMS_2022…2024, SMS_Contratos_menores_2025): con la
        # plantilla solo se encontraba 2020
        "pagina": URL_SECTOR_PUBLICO,
        "enlaces": r"/Sector_Publico/SMS/Contratos_menores/[^/]+\.(?:xlsx|xls|csv)$",
        "plantillas": [URL_SMS + ".xlsx", URL_SMS + ".xls"],   # catálogo CKAN
        "confirmados": range(2019, 2026),
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


def pedir_texto(url):
    """GET de una página HTML con reintentos (red, 429, 5xx); un 4xx lanza
    ErrorPortal."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.text
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
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

# Letras del castellano que no son ASCII: deciden entre las codificaciones de un byte
_LETRAS_ES = re.compile("[ÁÉÍÓÚÑÜáéíóúñü]")
# De un byte, por preferencia a igualdad de letras: cp1252 antes que latin-1 (latin-1 dejaría
# '€', '’'... como caracteres de control) y cp850 la última
_UN_BYTE = ("cp1252", "latin-1", "cp850")


def _detectar_codificacion(ruta):
    """UTF-8 (o UTF-16) si decodifica el fichero COMPLETO. Si no, la codificación de un byte que da
    más letras del castellano (á, Ñ...): cp1252, latin-1 y cp850 aceptan casi cualquier byte, y la
    equivocada no falla, cambia las letras. Los contratosOD de 2014-2018 de la CARM vienen en cp850
    (la 'Ó' es el byte 0xE0): leídos como cp1252 daban 'NEGOCIACIàN' y 'µREA'."""
    with open(ruta, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    utf8 = "utf-8-sig" if inicio.startswith(b"\xef\xbb\xbf") else "utf-8"
    letras = {}
    for codificacion in (utf8,) + _UN_BYTE:
        decodificador = codecs.getincrementaldecoder(codificacion)()
        n = 0
        try:
            with open(ruta, "rb") as f:
                while bloque := f.read(1 << 20):
                    n += len(_LETRAS_ES.findall(decodificador.decode(bloque)))
            decodificador.decode(b"", final=True)
        except UnicodeDecodeError:
            continue
        if codificacion == utf8:
            return utf8
        letras[codificacion] = n
    return max(letras, key=lambda c: (letras[c], -_UN_BYTE.index(c)))


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


# ----------------------------------------------------------------------------
# CSV del exportador JSON de datosabiertos.carm.es/odata
# ----------------------------------------------------------------------------
# Medido en los crudos del VPS (descarga del 2026-09-28):
# - contratosOD 2019-2023 y CONTRA_ContratosMenores 2021-2022 escapan las comillas de dentro de un campo
#   entrecomillado con una barra, como una cadena JSON ('"IES \"MENARGUEZ COSTA\", CEIP..."'). Un lector
#   CSV normal cierra el campo en esa comilla: 44 contratos corridos, 38 a la derecha hasta
#   _columna_extra_N (1262/2019: '273928.75' como adjudicatario) y 6 a la izquierda sin ninguna marca
#   (649/2019: el adjudicatario en importadjudicacion), y 207 textos cambiados ('ASOC ... \ASPRODES\""').
#   El mismo nombre llega bien en los ficheros de 2020 y 2023, que doblan la comilla ('""'): \" es la
#   sintaxis de la comilla y en el valor queda '"', como con '""'.
# - Esos ficheros y los menores de 2023 traen el resto de secuencias de escape de JSON sin interpretar
#   (\n, \t, \uXXXX); todas las barras son de una secuencia válida (ninguna '\\'). Se sirven tal cual.
#   \n es un corte de línea cada 80 caracteres (se ve en objeto, adjudicatario y CPV): de 35.128 cortes
#   medidos, ningún trozo pasa de 80 caracteres (contando cada secuencia como uno), 23.309 tienen 80 (el
#   corte cae donde cae: 'TEMPORAD\nA 2019-2020') y 4.016 tienen 79: el carácter 80 era un espacio y se
#   recortó ('TRANSPORTE\nY SERVICIOS'); los demás son saltos de línea del texto original. Quitar la
#   secuencia juntaría dos palabras en esos casos y cambiarla por un espacio partiría las otras: el texto
#   sin cortes va en _<columna>_sin_cortes (texto_sin_cortes) y la columna original no se toca.
# - Los menores de 2017 y 2019 llevan cada línea rodeada de espacios (' "14",...,"4" '): un lector normal
#   leía el código de consejería como ' "14"' (con las comillas) y el trimestre como '4 '. Los de 2018 y
#   2022 acaban en '"4"]' y una línea ']', y los de 2019 en '"4"","': restos de la lista JSON. No son de
#   ningún campo y van a _resto_json; la línea ']' se conserva como fila (con solo _resto_json), como
#   estaba en el Parquet, para no hacer desaparecer ninguna fila ya descargada.

# Secuencias de escape de una cadena JSON; en el texto, cada una es un carácter
_ESCAPE_JSON = re.compile(r'\\(?:u[0-9a-fA-F]{4}|["\\/bfnrt])')
_ESCAPE_CUALQUIERA = re.compile(r'\\(?:u[0-9a-fA-F]{4}|.)', re.DOTALL)
# Final de fichero con restos de la lista JSON tras la comilla de cierre del último campo:
# '"4"]\n]' (menores 2018 y 2022) o '"4"","\n ' (2019). Grupo 1: lo pegado al último campo; grupo 2: lo que
# sigue (espacios y, en su caso, la línea ']')
_FINAL_JSON = re.compile(r'"(\]|",")([ \t\r\n]*(?:\][ \t\r\n]*)?)\Z')
# Primera línea de datos con espacios antes de la comilla del primer campo (menores 2017 y 2019)
_RELLENO_JSON = re.compile(r'[ \t]+"')
# Largo de línea del exportador: los cortes caen cada 80 caracteres (ver arriba)
LARGO_CORTE = 80


def _es_texto_json(texto):
    """¿Trae el texto secuencias de escape de JSON sin interpretar? Sí si tiene alguna barra y todas
    empiezan una secuencia válida (\\" \\n \\t \\uXXXX...). Los menores de 2024-2025 traen barras sueltas
    ('RD 390\\2021', 'Conversor\\es'): no lo son y se leen como siempre."""
    return "\\" in texto and "\\" not in _ESCAPE_JSON.sub("", texto)


def _formato_json(texto):
    """Rasgos del exportador JSON que un lector CSV normal lee mal, o None si el fichero no tiene
    ninguno: barra (comillas escapadas con \\"), relleno (espacios alrededor de cada campo
    entrecomillado desde la primera línea de datos) y final (restos de la lista al final)."""
    barra = '\\"' in texto and _es_texto_json(texto)
    salto = texto.find("\n")
    relleno = salto >= 0 and _RELLENO_JSON.match(texto, salto + 1) is not None
    final = _FINAL_JSON.search(texto, max(0, len(texto) - 200))    # solo el final del fichero
    if not (barra or relleno or final):
        return None
    return {"barra": barra, "relleno": relleno, "final": final}


def _fin_de_linea(texto, i):
    """Posición del salto de línea (o del final) de la línea que empieza en `i`. El '\\r' se busca solo
    hasta el '\\n': buscarlo en todo el texto en cada registro sería cuadrático (ficheros de 7 MB)."""
    fin = texto.find("\n", i)
    fin = len(texto) if fin < 0 else fin
    retorno = texto.find("\r", i, fin)
    return fin if retorno < 0 else retorno


def _registros_json(texto, sep, barra, relleno):
    """Registros de un CSV del exportador JSON, como el módulo csv salvo:
    - barra: dentro de un campo entrecomillado, una barra va con el carácter que la sigue (\\" no cierra
      el campo). En el valor, \\" queda como '"' (es la sintaxis de la comilla, como '""') y el resto de
      secuencias (\\n, \\t, \\uXXXX) quedan tal cual, como las publica el portal;
    - relleno: los espacios y tabuladores entre el separador (o el principio de la línea) y la comilla
      que abre un campo, y entre la que lo cierra y el separador (o el final), no son del valor;
    - una comilla que no se cierra nunca es literal (como en comun.lectura_csv), y
    - las líneas en blanco o de solo espacios no son registros (como en pandas).
    Devuelve (registros, comillas literales)."""
    contenido = re.compile(r'((?:[^"\\]|\\.|"")*)"' if barra else r'((?:[^"]|"")*)"', re.DOTALL)
    desescapar = re.compile(r'\\"|""|\\.', re.DOTALL) if barra else None
    registros, fila, literales = [], [], 0
    i, n = 0, len(texto)
    while i < n:
        if not fila:
            fin = _fin_de_linea(texto, i)
            if not texto[i:fin].strip():
                i = fin + (2 if texto.startswith("\r\n", fin) else 1)
                continue
        j = i
        if relleno:
            while j < n and texto[j] in " \t":
                j += 1
            if j >= n or texto[j] != '"':
                j = i                   # campo sin comillas: los espacios son del valor
        m = contenido.match(texto, j + 1) if j < n and texto[j] == '"' else None
        if m:
            valor = m.group(1)
            valor = (desescapar.sub(lambda e: '"' if e.group() in ('\\"', '""') else e.group(), valor)
                     if barra else valor.replace('""', '"'))
            fin = k = m.end()
            while fin < n and texto[fin] not in (sep, "\r", "\n"):
                fin += 1
            cola = texto[k:fin]
            if not (relleno and not cola.strip(" \t")):
                valor += cola           # como el módulo csv: lo pegado tras la comilla es del campo
        else:
            if j < n and texto[j] == '"':
                literales += 1          # comilla que no se cierra: literal
            fin = i
            while fin < n and texto[fin] not in (sep, "\r", "\n"):
                fin += 1
            valor = texto[i:fin]
        i = fin
        fila.append(valor)
        if i < n and texto[i] == sep:
            i += 1
            if i == n:                  # separador al final del texto: un último campo vacío
                fila.append("")
            continue
        if i < n and texto[i] == "\r":
            i += 1
        if i < n and texto[i] == "\n":
            i += 1
        registros.append(fila)
        fila = []
    if fila:
        registros.append(fila)
    return registros, literales


def _leer_csv_json(texto, sep, formato, nombre):
    """DataFrame (todo texto, '' = nulo) de un CSV del exportador JSON (_formato_json), con las filas y
    los campos de más en _columna_extra_N como _leer_csv_tolerante. Los restos de la lista JSON del
    final van a _resto_json: lo pegado al último campo, en la última fila, y la línea ']', en una fila
    propia sin ningún otro valor. Devuelve (df, avisos)."""
    avisos, restos = [], {}
    corchete = False
    final = formato["final"]
    if final:
        texto = texto[:final.start() + 1]          # hasta la comilla que cierra el último campo
        corchete = "]" in final.group(2)
    filas, literales = _registros_json(texto, sep, formato["barra"], formato["relleno"])
    if not filas:
        return pd.DataFrame(), avisos
    nombres = _nombres_columnas(filas[0])
    datos = filas[1:]
    if final and datos:
        restos[len(datos) - 1] = final.group(1)
    if corchete:
        restos[len(datos)] = "]"
        datos = datos + [[]]
    ancho = max([len(nombres)] + [len(fila) for fila in datos])
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    valores = [[v if v != "" else None for v in fila] + [None] * (ancho - len(fila)) for fila in datos]
    df = pd.DataFrame(valores, columns=nombres, dtype=object)
    extra = [c for c in nombres if c.startswith("_columna_extra_")]
    con_extra = int(df[extra].notna().any(axis=1).sum()) if extra else 0
    df = df.drop(columns=[c for c in extra if df[c].isna().all()])
    if restos:
        df["_resto_json"] = pd.Series([restos.get(i) for i in range(len(df))], index=df.index, dtype=object)
        avisos.append(f"{nombre}: acaba con restos de la lista JSON ({final.group(1)!r}"
                      + (" y una línea ']'" if corchete else "") + "): van a _resto_json"
                      + (" (la línea ']', como fila sin ningún otro valor)" if corchete else ""))
    if formato["barra"]:
        avisos.append(f"{nombre}: {texto.count(chr(92) + chr(34)):,} comillas escapadas con barra (\\\") "
                      "leídas como comillas del texto")
    if formato["relleno"]:
        avisos.append(f"{nombre}: líneas con espacios alrededor de los campos entrecomillados (restos del "
                      "JSON): no son parte de los valores")
    if con_extra:
        avisos.append(f"{nombre}: {con_extra:,} filas con más campos que la cabecera; "
                      "los campos de más se conservan en columnas _columna_extra_N")
    if literales:
        avisos.append(f"{nombre}: {literales:,} comillas que no se cierran (se conservan en el texto)")
    return df, avisos


def texto_sin_cortes(valor):
    """Texto de una celda del exportador JSON sin los cortes de línea (\\n escrito como barra y n): el
    exportador parte el texto en líneas de 80 caracteres y recorta los espacios del final de cada una.
    Un trozo de 80 caracteres (contando cada secuencia de escape como uno) se une al siguiente tal cual;
    uno más corto se une con un espacio: el recortado (79) o un salto de línea del texto original. Las
    demás secuencias (\\t, \\uXXXX) se dejan como están. None si no hay ningún corte."""
    if not isinstance(valor, str) or "\\n" not in valor:
        return None
    trozos, inicio = [], 0
    for m in _ESCAPE_CUALQUIERA.finditer(valor):
        if m.group() == "\\n":
            trozos.append(valor[inicio:m.start()])
            inicio = m.end()
    trozos.append(valor[inicio:])
    if len(trozos) == 1:
        return None
    texto = trozos[0]
    for previo, trozo in zip(trozos, trozos[1:]):
        largo = len(previo) - sum(len(e.group()) - 1 for e in _ESCAPE_CUALQUIERA.finditer(previo))
        union = "" if largo == LARGO_CORTE or not texto or not trozo else " "
        texto += union + trozo
    return texto


def _anadir_sin_cortes(df, texto, nombre, avisos):
    """En un CSV con escapes de JSON sin interpretar (_es_texto_json), cada columna del portal con cortes
    de línea (\\n) tiene al lado _<columna>_sin_cortes con texto_sin_cortes (nulo en las filas sin
    cortes). La columna original no cambia."""
    if len(df) == 0 or not _es_texto_json(texto):
        return df
    for columna in [c for c in df.columns if not str(c).startswith("_")]:
        valores = df[columna].astype(object)
        con_cortes = valores.map(lambda v: isinstance(v, str) and "\\n" in v)
        if con_cortes.any():
            df[f"_{columna}_sin_cortes"] = valores.map(texto_sin_cortes).astype(object)
            avisos.append(f"{nombre}: {int(con_cortes.sum()):,} valores de {columna} con cortes de línea "
                          f"escritos como \\n; el texto sin cortes va en _{columna}_sin_cortes")
    return df


def leer_csv(ruta):
    """CSV como texto: dtype=str, sin convertir 'NA', 'N/A', 'NULL'... en nulos
    (solo el campo vacío es nulo) y sin perder filas ni campos."""
    ruta = Path(ruta)
    avisos = []
    codificacion = _detectar_codificacion(ruta)
    sep = _detectar_separador(ruta, codificacion)
    texto = None
    if codificacion != "utf-16":
        with open(ruta, encoding=codificacion, newline="") as f:
            texto = f.read()
        formato = _formato_json(texto)
        if formato:
            df, avisos = _leer_csv_json(texto, sep, formato, ruta.name)
            lineas = _lineas_de_datos(ruta)
            if lineas != len(df):
                avisos.append(f"{ruta.name}: {len(df):,} filas leídas de {lineas:,} líneas de datos "
                              "(campos entrecomillados con saltos de línea o comillas desparejadas): revisar")
            return _anadir_sin_cortes(df, texto, ruta.name, avisos), avisos
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
    if texto is not None:
        # Los menores de 2023: escapes de JSON sin interpretar, pero las comillas se doblan ('""')
        df = _anadir_sin_cortes(df, texto, ruta.name, avisos)
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


PATRON_DATO = re.compile(r"-?\d+(?:[.,]\d+)*|\d{4}-\d{2}-\d{2}(?:[ T][\d:.]+)?|[A-Z]?\d{7,8}[A-Z]?|[A-Z]\d{7}[A-Z0-9]",
                         re.IGNORECASE)


def _parece_registro(fila):
    """¿La fila detectada como cabecera son datos? Lo son si la mayoría de sus
    celdas con valor son números, fechas o NIF (una cabecera son rótulos)."""
    valores = [str(v).strip() for v in fila if v is not None and str(v).strip()]
    return bool(valores) and sum(bool(PATRON_DATO.fullmatch(v)) for v in valores) * 2 > len(valores)


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
    if _parece_registro(cabecera):
        # Hoja sin cabecera (p.ej. una tabla auxiliar de códigos y NIF): su
        # primera fila es un registro, no los nombres de las columnas
        avisos.append(f"{nombre} [{hoja}]: sin fila de cabecera (la primera fila son datos: "
                      f"{' | '.join(str(v) for v in cabecera if v is not None)[:120]}); columnas columna_1…")
        cabecera = [f"columna_{i}" for i in range(1, ancho + 1)]
        pos -= 1
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
    (lista de (ruta_actual, rel, metadatos)), cada uno desde todas sus
    versiones en raw/ con el código actual.

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
        # Con el código actual desde todas las versiones del crudo (regla 3): si solo se aplicaran
        # las versiones posteriores al Parquet anterior, un arreglo de lectura (p.ej. la
        # codificación cp850 de 2014-2018) no llegaría nunca a las filas ya guardadas
        por_fichero.pop(rel, None)
        registros = acumular_fichero(actual, rel, None, metadatos, manifiesto, resumen)
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


PATRON_HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
PATRON_ANIO_NOMBRE = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
EXTENSIONES_TABLA = (".csv", ".xlsx", ".xls", ".json")


def anio_de_nombre(nombre):
    """Año de un fichero por su nombre ('PT_SMS_1T2019.xlsx' → 2019), o None si
    no trae exactamente uno."""
    anios = set(PATRON_ANIO_NOMBRE.findall(Path(nombre).stem))
    return int(anios.pop()) if len(anios) == 1 else None


def enlaces_serie(serie, extra=()):
    """{nombre publicado: url} de los ficheros de la serie que enlaza su página
    (y de las URL del catálogo CKAN que casen, si la página no los enlaza).
    Lanza ErrorPortal si no se puede leer la página."""
    patron = re.compile(serie["enlaces"], re.IGNORECASE)
    urls = [urljoin(serie["pagina"], html.unescape(h).strip())
            for h in PATRON_HREF.findall(pedir_texto(serie["pagina"]))] + list(extra)
    enlaces = {}
    for url in urls:
        ruta = unquote(urlparse(url).path)
        if patron.search(ruta):
            enlaces.setdefault(Path(ruta).name, url)
    return enlaces


def archivos_enlazados(dir_serie):
    """Copias locales de una serie con página (nombre publicado con un año)."""
    if not Path(dir_serie).is_dir():
        return []
    return sorted(r for r in Path(dir_serie).iterdir()
                  if r.is_file() and not r.name.startswith(".") and r.suffix.lower() in EXTENSIONES_TABLA
                  and anio_de_nombre(r.name) is not None)


def descargar_enlazados(clave, serie, raw, manifiesto, resumen, comprobar_todo=False, extra=()):
    """Serie sin nombre fijo: se descargan los ficheros que enlaza su página,
    cada uno con su nombre publicado. Como en descargar_serie, los de años
    cerrados que ya se tienen solo se vuelven a pedir con --comprobar-todo.
    Una copia que la página deja de enlazar queda como retirada (sus filas se
    conservan); si la página no se puede leer o no enlaza ninguno, no se
    retira nada (más probable un fallo del portal)."""
    dir_serie = raw / clave
    anio_actual = ahora().year
    print(f"\n📦 {clave}: {serie['descripcion']}")
    try:
        enlaces = enlaces_serie(serie, extra)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{clave}: no se pudo leer {serie['pagina']} ({e}); se conservan las copias")
        return
    if not enlaces:
        resumen.fallidos.append(f"{clave}: {serie['pagina']} no enlaza ningún fichero de la serie; "
                                "no se retira nada")
        return
    anios = set()
    for nombre, url in sorted(enlaces.items()):
        anio = anio_de_nombre(nombre)
        if anio is None or not nombre.lower().endswith(EXTENSIONES_TABLA):
            resumen.avisos.append(f"{clave}: {nombre} no trae un año en el nombre; no se descarga ({url})")
            continue
        anios.add(anio)
        destino = dir_serie / nombre
        if destino.exists() and not comprobar_todo and anio < anio_actual - 1:
            resumen.sin_cambios.append(f"{clave} {nombre} (ya descargado; --comprobar-todo para volver a pedirlo)")
            continue
        estado, detalle = descargar(url, destino, tipo=_tipo(url))
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, url, estado, dataset=clave, anio=anio)
            resumen.descarga(f"{clave} {nombre}", estado)
            print(f"  ✅ {nombre}: {estado}")
        else:
            # Enlazado y no se puede bajar: se conserva la copia y se reintenta
            resumen.fallidos.append(f"{clave} {nombre}: {detalle or estado} ({url})")
            print(f"  ❌ {nombre}: {detalle or estado}")
    for ruta in archivos_enlazados(dir_serie):
        if ruta.name not in enlaces and manifiesto.get(manifiesto.rel(ruta)).get("publicado", True):
            manifiesto.retirar(ruta, f"{serie['pagina']} ya no lo enlaza")
            resumen.retirados.append(f"{clave} {ruta.name}: la página ya no lo enlaza; se conservan sus filas")
            print(f"  🗑️ {ruta.name}: retirado por el portal")
    for anio in serie["confirmados"]:
        if anio not in anios:
            resumen.fallidos.append(f"{clave} {anio}: año publicado según las fuentes y la página no lo enlaza")


def descargar_todo(raw, manifiesto, resumen, desde, hasta, comprobar_todo=False):
    extra, otros = inventario_ckan(raw, resumen)
    if otros:
        resumen.avisos.append(f"{len(otros)} recursos del catálogo CKAN con '{CONSULTA_CKAN}' no son de las "
                              "series descargadas (revisar):\n      " + "\n      ".join(otros))
    for clave, serie in SERIES.items():
        if "pagina" in serie:
            urls = [u for (c, _), lista in extra.items() if c == clave for u in lista]
            descargar_enlazados(clave, serie, raw, manifiesto, resumen, comprobar_todo, urls)
            continue
        anios = set(range(desde, hasta + 1)) | set(serie["confirmados"])
        anios |= {anio for (c, anio) in extra if c == clave}
        anios |= set(archivos_locales(raw / clave, serie))
        descargar_serie(clave, serie, sorted(anios), raw, manifiesto, resumen, comprobar_todo, extra)


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    for clave, serie in SERIES.items():
        ficheros = []
        if "pagina" in serie:
            for ruta in archivos_enlazados(raw / clave):
                rel = manifiesto.rel(ruta)
                ficheros.append((ruta, rel, {"_fuente": manifiesto.get(rel).get("url") or serie["pagina"],
                                             "_dataset": clave, "_anio_fichero": str(anio_de_nombre(ruta.name)),
                                             "_archivo_origen": rel}))
        for anio, rutas in ([] if "pagina" in serie else archivos_locales(raw / clave, serie).items()):
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
