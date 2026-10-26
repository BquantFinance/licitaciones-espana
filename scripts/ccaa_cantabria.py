#!/usr/bin/env python3
"""
=============================================================================
CANTABRIA - CONTRATOS DEL GOBIERNO (base de datos de contratos de Transparencia)
=============================================================================
Descarga, tal como la exporta (con todas sus versiones), la «Base de datos de
contratos» del Portal de Transparencia del Gobierno de Cantabria: todos los
contratos adjudicados por la Administración de la Comunidad Autónoma y su
sector público desde 2015, también los menores, contrato a contrato. Genera
un Parquet con todas las filas y columnas exportadas, como texto.

Ejecutar:  python scripts/ccaa_cantabria.py [--salida DIR] [--desde AAAA-MM]
           [--hasta AAAA-MM] [--solo-descarga] [--solo-parquet] [--comprobar-todo]

Salida (por defecto <repo>/ccaa_cantabria/):
    raw/contratos/<año>/<año>-<mes>.csv    la exportación CSV de cada mes (por fecha de
                                           formalización), tal cual; una ventana de más de
                                           TOPE_EXPORTACION contratos se parte en tramos de
                                           días: <AAAA-MM-DD>_<AAAA-MM-DD>.csv
    raw/contratos/<año>/_historico/        versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json                   URL, ventana, contratos que cuenta la API, fecha de
                                           descarga, última comprobación y si se sigue publicando
    raw/descarga_log.txt                   resumen de cada ejecución (se añade al final)
    contratos.parquet, _historico/         registros acumulados de todas las ventanas

Columnas: las 273 de la exportación, con su nombre y su valor tal cual (texto:
'7.825,00 €', '11-02-2026', 'X' en las casillas marcadas). La exportación
repite nombres de columna en cada bloque (contrato menor, contrato general,
basado en acuerdo marco, en sistema dinámico, otros...): el segundo 'Sin lotes'
es 'Sin lotes.1', como los pone pandas. Lo que viene vacío es nulo. Las
columnas de detalle ('Invitados (detalle)', 'Ofertas (detalle)', 'Lotes
(detalle)'...) son texto con los elementos separados por '; ' («1- cif/nif:
... razon: ... sin_iva: ... iva: ...»): no se separan aquí.
Añadidas por el script:
- De origen (no cuentan al comparar versiones): _fuente (URL de la
  exportación), _ventana_desde y _ventana_hasta (fechas de formalización
  pedidas, AAAA-MM-DD), _anio (año de la ventana), _archivo_origen (ruta en
  raw/) y _fecha_descarga.
- De comun/historico.py: _primera_descarga, _ultima_descarga y
  _en_ultima_descarga.

Sesgo del superviviente (docs/PRINCIPIOS.md, regla 3; comun/historico.py):
- Capa cruda: cada exportación pasa por guardar_version; si no cambia no se
  toca y si cambia la copia anterior va a _historico/.
- Parquet: se construye con el código actual desde TODAS las versiones de
  raw/, con acumular() por fichero: lo que la API deja de dar o cambia sigue
  con _en_ultima_descarga=False. Las filas de un fichero que ya no está en raw/
  se copian del Parquet anterior.
- Retirado: un mes que tenía fichero y para el que la API cuenta 0 contratos
  pasa a _en_ultima_descarga=False con todas sus filas. Si la API falla, o si
  de golpe se quedarían sin contratos más de la mitad de los meses con fichero
  que se han comprobado (y más de UMBRAL_RETIRADA), no se retira nada y es un
  error. Un mes que se parte en tramos (o vuelve a caber en una exportación)
  retira el fichero de la ventana anterior cuando la nueva se ha bajado entera:
  sus filas quedan como no vigentes y las mismas, vigentes, en los ficheros
  nuevos.
- Una exportación que no trae tantos registros como cuenta la API para su
  ventana (o que no es el CSV de la exportación) no se guarda: es un error y la
  versión anterior se conserva.

Qué se pide en cada ejecución (una petición cada vez, con PAUSA entre ellas):
- El total de contratos de la API (1 petición) y, de cada mes que se descarga,
  cuántos contratos cuenta la API (1) y la exportación (1, o una por tramo).
- Se completan los meses que faltan y se vuelven a pedir los del año en curso
  y el anterior; los más antiguos que ya se tienen, solo con --comprobar-todo.
  La primera descarga completa (2015 hasta hoy) son unas 290 peticiones; la
  semanal, unas 45.
Sale con 1 si algo que existe no se pudo descargar o leer.

FUENTES
-------
Verificado en vivo el 2026-10-26 (confianza A):
  - Portal: https://aplicacionesweb.cantabria.es/transparencia/actuaciones/
    contratos-sector-publico (aplicación Angular, enlazada desde
    transparencia.cantabria.es, «Contratación pública» > «Base de datos de
    contratos»; fuente: Sistema de Suministro de Información TRANSPARENCIA).
    El listado es público: no hace falta iniciar sesión.
  - API: https://aplicacionesweb.cantabria.es/transparencia-back/webapi/
    actuaciones/contratos-sector-publico
      POST /filter  (JSON) -> {"pageInfo": {"totalElements": N, ...}, "content": [...]}:
                    listado con id, numExpediente, órgano, objeto, adjudicatario,
                    fechaFormalizacion e importeContrato; la página no pasa de 10.
      POST /export  (JSON, Accept: text/csv) -> la misma consulta como CSV con
                    las 273 columnas del contrato, HASTA 1.000 registros (con
                    "length" mayor también da 1.000).
    Cuerpo: {"page": 0, "start": 0, "length": L, "columns": [{"name": "id"}],
    "order": [{"column": "0", "dir": "asc"}], "fechaFormalizacionFrom":
    "AAAA-MM-DD", "fechaFormalizacionTo": "AAAA-MM-DD"} (ambas incluidas).
  - CSV: primera línea 'sep=;' (para Excel), cabecera de 273 columnas con
    nombres repetidos, ';', todos los campos entre comillas, CRLF. Se sirve como
    'text/csv;charset=ISO-8859-1', pero el euro va en 0x80: es Windows-1252
    ('7.825,00 €'). Fechas DD-MM-AAAA; importes con punto de miles, coma decimal
    y ' €'. NIF de personas físicas enmascarado ('***8328**').
  - Contratos por año de formalización: 2015: 1.513; 2016: 2.336; 2017: 4.924;
    2018: 4.533; 2019: 4.201; 2020: 4.143; 2021: 4.070; 2022: 3.640;
    2023: 3.127; 2024: 3.123; 2025: 2.412; 2026: 1.414 (hasta el 26-oct);
    ninguno antes de 2015. Total de la API: 39.661 (225 sin fecha de
    formalización: no se pueden pedir por ventana y el script lo avisa).
    El mes con más contratos de la muestra: octubre de 2019, 488.
  - Muestra de tres trimestres (4T 2019, 2T 2022, 1T 2024; 2.710 contratos):
    el 81-83 % son menores ('Contrato menor' = 'X'). De los menores, el
    28-39 % trae la relación de ofertas con el NIF de cada licitador
    ('Ofertas (detalle)'; con dos o más ofertas, el 19-29 %) y casi todos el
    número de invitados. Contra el feed 1143 de la PLACSP (nacional/), el
    92-96 % de esos menores ya está, por NIF e importe; lo nuevo de esta fuente
    son las ofertas, los invitados, las prórrogas, modificaciones,
    penalizaciones y subcontrataciones, y el 4-8 % de menores que no está.
  - Los contratos migrados del sistema anterior lo dicen en 'Observaciones
    públicas' («Los datos de este contrato han sido migrados desde la base de
    datos...»).
=============================================================================
"""

import argparse
import calendar
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, guardar_version, versiones  # noqa: E402, F401
from comun.lectura import _nombres_columnas  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402
# Red común: los mismos valores que el resto de scripts (la petición es POST: va aquí)
from comun.red import (  # noqa: E402, F401
    CABECERAS, TIMEOUT_API, TIMEOUT_DESCARGA, INTENTOS, CODIGOS_REINTENTABLES, ERRORES_RED,
    ErrorPortal, _espera)
from comun.salida import iso, fecha_version  # noqa: E402, F401
import comun.salida  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

URL_API = ("https://aplicacionesweb.cantabria.es/transparencia-back/webapi/actuaciones/"
           "contratos-sector-publico")
URL_FILTRO = URL_API + "/filter"
URL_EXPORTACION = URL_API + "/export"
URL_PORTAL = "https://aplicacionesweb.cantabria.es/transparencia/actuaciones/contratos-sector-publico"

SERIE = "contratos"
DESCRIPCION = "Contratos del Gobierno de Cantabria y su sector público (base de datos de Transparencia)"
PRIMER_MES = (2015, 1)            # primer mes con contratos (ninguno antes de 2015)
TOPE_EXPORTACION = 1000           # registros como mucho en una exportación
# Columnas que tiene que traer una exportación (si no, es otra cosa: un mensaje del portal)
IDENTIFICADORES = ("Consejería", "Órgano directivo", "Objeto del contrato", "Contrato menor")
UMBRAL_RETIRADA = 3               # meses que pueden quedarse sin contratos de golpe sin que sea un error
PAUSA = 1.0                       # entre peticiones, para no cargar el portal
ESTADOS_OK = ("nuevo", "actualizado", "sin_cambios")
# El CSV dice ISO-8859-1, pero el euro va en 0x80 (Windows-1252); latin-1, si algún byte no lo es
CODIFICACIONES = ("cp1252", "latin-1")

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_cantabria"
TITULO = "CANTABRIA - CONTRATOS DEL GOBIERNO (TRANSPARENCIA)"

# Columnas que añade el script, en el orden en que quedan al final del Parquet.
# Las de origen no cuentan al comparar registros entre versiones.
METADATOS_ORIGEN = ("_fuente", "_ventana_desde", "_ventana_hasta", "_anio", "_archivo_origen", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


# ============================================================================
# UTILIDADES (manifiesto y resumen: la misma base que ccaa_la_rioja.py)
# ============================================================================

class Manifiesto:
    """raw/_manifiesto.json: de dónde sale cada fichero crudo, cuándo se descargó
    su versión actual (fecha_descarga), cuándo se comprobó por última vez
    (comprobado) y si la API lo sigue dando."""

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
        entrada.pop("detalle", None)
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
    """Lo descargado, lo retirado, los avisos y lo que falló."""

    def __init__(self, titulo):
        self.titulo = titulo
        self.inicio = ahora()
        self.descargados = []
        self.sin_cambios = []
        self.sin_contratos = []
        self.retirados = []
        self.avisos = []
        self.fallidos = []
        self.parquets = []

    def descarga(self, etiqueta, estado):
        if estado == "sin_cambios":
            self.sin_cambios.append(etiqueta)
        else:
            self.descargados.append(f"{etiqueta} ({estado})")

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
        bloque("MESES SIN CONTRATOS EN LA API", self.sin_contratos)
        bloque("RETIRADOS (se conservan con _en_ultima_descarga=False)", self.retirados)
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


# ============================================================================
# API: peticiones POST con reintentos
# ============================================================================

def cuerpo(desde=None, hasta=None, length=1):
    """Cuerpo JSON de /filter y /export: una ventana de fechas de formalización
    (AAAA-MM-DD, ambas incluidas; sin ellas, todos los contratos), ordenada por id."""
    datos = {"page": 0, "start": 0, "length": length, "columns": [{"name": "id"}],
             "order": [{"column": "0", "dir": "asc"}]}
    if desde is not None:
        datos["fechaFormalizacionFrom"] = desde.isoformat()
        datos["fechaFormalizacionTo"] = hasta.isoformat()
    return datos


def pedir_post(url, datos, acepta, timeout=TIMEOUT_API):
    """POST JSON con reintentos (red, 429, 5xx); un 4xx lanza ErrorPortal con el
    código, sin reintentar. Devuelve el cuerpo de la respuesta en bytes."""
    cabeceras = dict(CABECERAS, Accept=acepta)
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.post(url, json=datos, headers=cabeceras, timeout=timeout)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.content
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


def contar(desde=None, hasta=None):
    """Contratos que cuenta la API en una ventana (o en total, sin ventana)."""
    contenido = pedir_post(URL_FILTRO, cuerpo(desde, hasta, 1), "application/json")
    try:
        total = json.loads(contenido.decode("utf-8"))["pageInfo"]["totalElements"]
    except (ValueError, KeyError, TypeError, UnicodeDecodeError) as e:
        raise ErrorPortal(f"respuesta de /filter sin pageInfo.totalElements ({type(e).__name__})") from e
    if not isinstance(total, int) or total < 0:
        raise ErrorPortal(f"pageInfo.totalElements no es un número de contratos: {total!r}")
    return total


# ============================================================================
# LECTURA DE LA EXPORTACIÓN (todas las filas y columnas, como texto)
# ============================================================================

def decodificar(contenido):
    """(texto, codificación) del CSV: Windows-1252 y, si algún byte no lo es, latin-1."""
    for codificacion in CODIFICACIONES:
        try:
            return contenido.decode(codificacion), codificacion
        except UnicodeDecodeError:
            continue
    raise ValueError("no se puede decodificar")


def registros_exportacion(contenido):
    """(cabecera, filas, avisos) de una exportación: sin la línea 'sep=;' de
    Excel, con el separador que dice (';' si no la trae)."""
    texto, codificacion = decodificar(contenido)
    avisos = []
    if codificacion != CODIFICACIONES[0]:
        avisos.append(f"bytes que no son {CODIFICACIONES[0]}: leído como {codificacion}")
    if texto.startswith("﻿"):
        texto = texto[1:]
    sep = ";"
    m = re.match(r"sep=(.)\r?\n", texto)
    if m:
        sep, texto = m.group(1), texto[m.end():]
    filas, _ = registros_csv(texto, sep)
    filas = [f for f in filas if any(v != "" for v in f)]
    if not filas:
        return [], [], avisos
    return filas[0], filas[1:], avisos


def problema_exportacion(contenido, esperados):
    """Motivo por el que una respuesta no es la exportación de una ventana con
    `esperados` contratos (o None si lo es)."""
    cabeza = contenido[:2048].lstrip().lower()
    if not cabeza:
        return "respuesta vacía"
    if cabeza.startswith((b"<", b"{", b"[")):
        return "la respuesta no es un CSV (HTML, XML o JSON)"
    try:
        cabecera, filas, _ = registros_exportacion(contenido)
    except ValueError as e:
        return f"CSV ilegible ({e})"
    faltan = [c for c in IDENTIFICADORES if c not in cabecera]
    if faltan:
        return f"la cabecera no trae {', '.join(faltan)}"
    if len(filas) != esperados:
        return f"la exportación trae {len(filas):,} registros y la API cuenta {esperados:,}"
    return None


def leer_tabla(ruta):
    """Exportación de raw/ como DataFrame de texto (nulo = vacío). Devuelve (df, avisos)."""
    ruta = Path(ruta)
    cabecera, filas, avisos = registros_exportacion(ruta.read_bytes())
    avisos = [f"{ruta.name}: {a}" for a in avisos]
    if not cabecera:
        return pd.DataFrame(), avisos
    nombres = _nombres_columnas(cabecera)
    ancho = max([len(nombres)] + [len(f) for f in filas])
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    datos = [[v if v != "" else None for v in f] + [None] * (ancho - len(f)) for f in filas]
    df = pd.DataFrame(datos, columns=nombres, dtype=object)
    extra = [c for c in nombres if c.startswith("_columna_extra_")]
    if extra:
        con_extra = int(df[extra].notna().any(axis=1).sum())
        if con_extra:
            avisos.append(f"{ruta.name}: {con_extra:,} filas con más campos que la cabecera; "
                          "los campos de más se conservan en columnas _columna_extra_N")
        df = df.drop(columns=[c for c in extra if df[c].isna().all()])
    return df, avisos


# ============================================================================
# REGISTROS ACUMULADOS (comun/historico.py) Y PARQUET
# ============================================================================

def ordenar_columnas(df):
    """Columnas de la exportación (en su orden) y después las añadidas por el script."""
    propias = [c for c in ORDEN_METADATOS if c in df.columns]
    return df[[c for c in df.columns if c not in ORDEN_METADATOS] + propias]


def escribir_parquet(df, destino):
    """comun.salida.escribir_parquet con el ordenar_columnas de este script."""
    return comun.salida.escribir_parquet(df, destino, ordenar_columnas=ordenar_columnas)


def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen):
    """comun.salida.acumular_fichero con METADATOS_ORIGEN y leer_tabla de este script."""
    return comun.salida.acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen,
                                         metadatos_origen=METADATOS_ORIGEN, leer_tabla=leer_tabla)


def construir_parquet(destino, ficheros, raw, manifiesto, resumen):
    """comun.salida.construir_parquet con METADATOS_ORIGEN, acumular_fichero y escribir_parquet
    de este script."""
    return comun.salida.construir_parquet(destino, ficheros, raw, manifiesto, resumen,
                                          metadatos_origen=METADATOS_ORIGEN,
                                          acumular_fichero=acumular_fichero,
                                          escribir_parquet=escribir_parquet)


# ============================================================================
# VENTANAS Y DESCARGA
# ============================================================================

PATRON_MES = re.compile(r"(\d{4})-(\d{2})\.csv")
PATRON_TRAMO = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.csv")


def leer_mes(texto):
    """'AAAA-MM' -> (año, mes); ValueError si no lo es."""
    m = re.fullmatch(r"(\d{4})-(\d{2})", str(texto))
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise ValueError(f"mes no válido (AAAA-MM): {texto!r}")
    return int(m.group(1)), int(m.group(2))


def meses(desde, hasta):
    """[(año, mes)] de `desde` a `hasta`, ambos incluidos."""
    (a, m), lista = desde, []
    while (a, m) <= hasta:
        lista.append((a, m))
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    return lista


def limites_mes(anio, mes):
    return date(anio, mes, 1), date(anio, mes, calendar.monthrange(anio, mes)[1])


def nombre_ventana(desde, hasta):
    """Nombre del fichero de una ventana: el mes entero (AAAA-MM.csv) o el tramo de días."""
    if desde.day == 1 and (desde.year, desde.month) == (hasta.year, hasta.month) and \
            hasta.day == calendar.monthrange(hasta.year, hasta.month)[1]:
        return f"{desde.year}-{desde.month:02d}.csv"
    return f"{desde.isoformat()}_{hasta.isoformat()}.csv"


def ventana_de_nombre(nombre):
    """(desde, hasta) de un fichero de raw/, o None si no es una ventana."""
    m = PATRON_MES.fullmatch(nombre)
    if m:
        return limites_mes(int(m.group(1)), int(m.group(2)))
    m = PATRON_TRAMO.fullmatch(nombre)
    if m:
        return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))
    return None


def ficheros_mes(raw, anio, mes):
    """Copias actuales (sin _historico/) de las ventanas que caen en un mes."""
    carpeta = Path(raw) / SERIE / str(anio)
    if not carpeta.is_dir():
        return []
    inicio, fin = limites_mes(anio, mes)
    return sorted(r for r in carpeta.iterdir() if r.is_file() and (v := ventana_de_nombre(r.name))
                  and inicio <= v[0] and v[1] <= fin)


def vigentes(rutas, manifiesto):
    return [r for r in rutas if manifiesto.get(manifiesto.rel(r)).get("publicado", True)]


def partir(desde, hasta, resumen, cuentas=None):
    """[(desde, hasta, contratos)] que cubren la ventana con como mucho
    TOPE_EXPORTACION contratos cada una (partiendo por la mitad en días).
    Lanza ErrorPortal si un solo día pasa del tope."""
    n = contar(desde, hasta) if cuentas is None else cuentas
    time.sleep(PAUSA)
    if n <= TOPE_EXPORTACION:
        return [(desde, hasta, n)]
    if desde == hasta:
        raise ErrorPortal(f"el {desde.isoformat()} tiene {n:,} contratos: más de los {TOPE_EXPORTACION:,} "
                          "que da una exportación")
    medio = desde + timedelta(days=(hasta - desde).days // 2)
    return partir(desde, medio, resumen) + partir(medio + timedelta(days=1), hasta, resumen)


def exportar(desde, hasta, esperados, destino):
    """Exportación de una ventana en `destino` (comun.historico.guardar_version).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'invalido' (no es la exportación esperada) o 'error'."""
    try:
        contenido = pedir_post(URL_EXPORTACION, cuerpo(desde, hasta, TOPE_EXPORTACION), "text/csv",
                               timeout=TIMEOUT_DESCARGA)
    except ErrorPortal as e:
        return "error", str(e)
    motivo = problema_exportacion(contenido, esperados)
    if motivo:
        return "invalido", motivo
    return guardar_version(destino, contenido), ""


def descargar_mes(anio, mes, raw, manifiesto, resumen):
    """Descarga las ventanas de un mes. Devuelve (contratos de la API, retirados por
    quedarse sin contratos: [ruta]) o None si falló (no se retira nada)."""
    inicio, fin = limites_mes(anio, mes)
    etiqueta = f"{SERIE} {anio}-{mes:02d}"
    previos = vigentes(ficheros_mes(raw, anio, mes), manifiesto)
    try:
        n = contar(inicio, fin)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{etiqueta}: no se pudo contar ({e}); no se descarga ni se retira nada")
        print(f"  ❌ {anio}-{mes:02d}: {e}")
        return None
    time.sleep(PAUSA)
    if n == 0:
        if previos:
            return 0, previos
        resumen.sin_contratos.append(f"{anio}-{mes:02d}")
        return 0, []
    try:
        tramos = partir(inicio, fin, resumen, cuentas=n) if n > TOPE_EXPORTACION else [(inicio, fin, n)]
    except ErrorPortal as e:
        resumen.fallidos.append(f"{etiqueta}: {e}; no se descarga ni se retira nada")
        print(f"  ❌ {anio}-{mes:02d}: {e}")
        return None
    if sum(t[2] for t in tramos) != n:
        resumen.fallidos.append(f"{etiqueta}: los tramos suman {sum(t[2] for t in tramos):,} contratos y el mes "
                                f"{n:,}; no se descarga ni se retira nada")
        return None
    nuevos, bien = [], True
    for desde, hasta, esperados in tramos:
        destino = Path(raw) / SERIE / str(anio) / nombre_ventana(desde, hasta)
        estado, detalle = exportar(desde, hasta, esperados, destino)
        time.sleep(PAUSA)
        rotulo = f"{etiqueta} {destino.name} ({esperados:,} contratos)"
        if estado not in ESTADOS_OK:
            resumen.fallidos.append(f"{rotulo}: {detalle}; se conserva la copia anterior")
            print(f"  ❌ {destino.name}: {detalle}")
            bien = False
            continue
        manifiesto.registrar(destino, URL_EXPORTACION, estado, dataset=SERIE, ventana_desde=desde.isoformat(),
                             ventana_hasta=hasta.isoformat(), contratos_api=esperados)
        resumen.descarga(rotulo, estado)
        print(f"  ✅ {destino.name}: {estado} ({esperados:,} contratos)")
        nuevos.append(destino)
    if bien:
        for ruta in previos:
            if ruta not in nuevos:
                manifiesto.retirar(ruta, "la ventana se ha vuelto a partir de otra forma; sus contratos están "
                                         "en los ficheros nuevos del mismo mes")
                resumen.retirados.append(f"{etiqueta} {ruta.name}: sustituido por "
                                         f"{', '.join(r.name for r in nuevos)}")
    return n, []


def descargar_todo(raw, manifiesto, resumen, desde=None, hasta=None, comprobar_todo=False):
    """Meses de `desde` a `hasta`: los que faltan, el año en curso y el anterior
    (los antiguos ya descargados, con --comprobar-todo). Al final, los meses que
    se han quedado sin contratos se retiran (salvo retirada masiva)."""
    hoy = ahora().date()
    desde = desde or PRIMER_MES
    hasta = hasta or (hoy.year, hoy.month)
    print(f"\n📦 {SERIE}: {DESCRIPCION}")
    try:
        total = contar()
    except ErrorPortal as e:
        resumen.fallidos.append(f"{SERIE}: no se pudo leer el total de la API ({e}); no se descarga nada")
        print(f"  ❌ total: {e}")
        return
    time.sleep(PAUSA)
    print(f"  La API cuenta {total:,} contratos")
    if total == 0:
        resumen.fallidos.append(f"{SERIE}: la API no da ningún contrato; no se descarga ni se retira nada")
        return
    comprobados, vacios = 0, []
    for anio, mes in meses(desde, hasta):
        tiene = vigentes(ficheros_mes(raw, anio, mes), manifiesto)
        if tiene and not comprobar_todo and anio < hoy.year - 1:
            resumen.sin_cambios.append(f"{SERIE} {anio}-{mes:02d} (ya descargado; --comprobar-todo para "
                                       "volver a pedirlo)")
            continue
        resultado = descargar_mes(anio, mes, raw, manifiesto, resumen)
        if resultado is None:
            continue
        if tiene:
            comprobados += 1
        vacios.extend(resultado[1])
    if vacios:
        if len(vacios) > UMBRAL_RETIRADA and len(vacios) > comprobados / 2:
            resumen.fallidos.append(f"{SERIE}: {len(vacios)} de {comprobados} ficheros comprobados se quedarían "
                                    "sin contratos de golpe (¿la API falla?); no se retira nada")
        else:
            for ruta in vacios:
                manifiesto.retirar(ruta, "la API ya no cuenta contratos en esa ventana")
                resumen.retirados.append(f"{SERIE} {ruta.name}: la API ya no cuenta contratos en esa ventana; "
                                         "se conservan sus filas")
                print(f"  🗑️ {ruta.name}: retirado")
    contados = sum(manifiesto.get(manifiesto.rel(r)).get("contratos_api", 0)
                   for r in archivos_serie(raw) if manifiesto.get(manifiesto.rel(r)).get("publicado", True))
    if contados and contados != total:
        resumen.avisos.append(f"{SERIE}: la API cuenta {total:,} contratos y las ventanas descargadas, {contados:,}"
                              f" ({total - contados:+,}): los que no tienen fecha de formalización no se pueden "
                              "pedir por ventana, y los meses no descargados en esta ejecución cuentan lo que "
                              "contaban la última vez")


def archivos_serie(raw):
    """Copias actuales de todas las ventanas de raw/, por fecha."""
    carpeta = Path(raw) / SERIE
    if not carpeta.is_dir():
        return []
    return sorted((r for d in carpeta.iterdir() if d.is_dir() and d.name != HISTORICO
                   for r in d.iterdir() if r.is_file() and ventana_de_nombre(r.name)),
                  key=lambda r: (ventana_de_nombre(r.name), r.name))


def generar_parquet(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    ficheros = []
    for ruta in archivos_serie(raw):
        rel = manifiesto.rel(ruta)
        desde, hasta = ventana_de_nombre(ruta.name)
        ficheros.append((ruta, rel, {"_fuente": manifiesto.get(rel).get("url") or URL_EXPORTACION,
                                     "_ventana_desde": desde.isoformat(), "_ventana_hasta": hasta.isoformat(),
                                     "_anio": str(desde.year), "_archivo_origen": rel}))
    destino = Path(salida) / f"{SERIE}.parquet"
    if ficheros or destino.exists():
        construir_parquet(destino, ficheros, raw, manifiesto, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga la base de datos de contratos del Gobierno de Cantabria")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--desde", type=leer_mes, default=None,
                        help=f"primer mes (AAAA-MM; por defecto {PRIMER_MES[0]}-{PRIMER_MES[1]:02d})")
    parser.add_argument("--hasta", type=leer_mes, default=None, help="último mes (AAAA-MM; por defecto, el actual)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar el Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar el Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los meses antiguos ya descargados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"API: {URL_API}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, args.desde, args.hasta, args.comprobar_todo)
    if not args.solo_descarga:
        generar_parquet(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
