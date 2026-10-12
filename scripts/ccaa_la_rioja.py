#!/usr/bin/env python3
"""
=============================================================================
LA RIOJA - CONTRATOS MENORES (Gobierno de La Rioja)
=============================================================================
Descarga los datos abiertos anuales "Contratos menores {año} Comunidad
Autónoma de La Rioja" tal cual los publica el Gobierno de La Rioja (con todas
sus versiones) y genera un Parquet con todas las columnas y filas de todos los
años, como texto. Son todos los contratos menores de la Administración general,
el Servicio Riojano de Salud (SERIS) y el Instituto de Estudios Riojanos.

Ejecutar:  python scripts/ccaa_la_rioja.py [--salida DIR] [--solo-descarga]
           [--solo-parquet] [--comprobar-todo] [--sin-sondeo]

Salida (por defecto <repo>/ccaa_la_rioja/):
    raw/contratos_menores/cd<N>/contratos_CAR_<año>.csv
                                       CSV del código N tal cual (p.ej. cd979/contratos_CAR_2024.csv)
    raw/contratos_menores/cd<N>/_historico/
                                       versiones anteriores de cada fichero (nunca se borran)
    raw/sondeo_codigos.json            códigos del servidor de descargas que ha encontrado el sondeo
    raw/_manifiesto.json               URL, código, año, fecha de descarga y última comprobación de cada fichero
    raw/descarga_log.txt               resumen de cada ejecución (se añade al final)
    contratos_menores.parquet          todos los años (todas las columnas, texto)
    _historico/                        versiones anteriores del Parquet

Columnas del portal: COD_CONTRATO, DEPARTAMENTO, TIPO_EXPEDIENTE, TERC_CIF,
TERC_NOMBRE, CONCEPTO, FECHA, IMPORTE_EJERCICIO. Añadidas: _fuente (URL
descargada), _dataset, _recurso (ficha del catálogo: opd-<N>), _anio_fichero
(año del nombre publicado), _archivo_origen (ruta en raw/), _fecha_descarga y,
de comun/historico.py, _primera_descarga, _ultima_descarga y
_en_ultima_descarga. Un registro que el portal retira o modifica NO
desaparece: sigue en el Parquet con _en_ultima_descarga=False (control del
sesgo del superviviente).

Qué se descarga:
- Cada año es un dato abierto con su código (cd). CODIGOS_CONOCIDOS es la
  lista verificada y sirve de respaldo; los años nuevos se descubren con un
  sondeo del servidor de descargas (sondear_codigos): el nombre del fichero
  que sirve cada código (contratos_CAR_{año}.csv) identifica la serie y el
  año. Otros ficheros de contratación que aparezcan se anotan para revisarlos.
- Antes de descargar se comprueba con HEAD que el código sigue sirviendo el
  fichero de ese año: si sirve otro, es un error y no se toca nada.
- Se completan los años que faltan y se vuelven a pedir el año en curso y el
  anterior (el fichero de un año se actualiza a diario); los antiguos solo
  con --comprobar-todo. Todo fichero pasa por comun.historico.guardar_version:
  si no cambia no se toca y si cambia la copia anterior queda en _historico/.
  Si un código que se tenía pasa a dar 404 se marca como retirado (sus filas
  se conservan); si el portal falla (red, 5xx, una página HTML) no se retira
  nada y la ejecución termina con error para reintentarlo.

FUENTES
-------
Verificado en vivo el 2026-09-27:
  - Descarga: https://ias1.larioja.org/opendata/download?r=<base64("cd=N|cf=FF")>,
    con cf 01 = XLS, 02 = XML, 03 = CSV y 04 = JSON (las mismas URL que
    enlazan el catálogo y datos.gob.es). Un código inexistente da 404 y uno
    no público, 403. HEAD devuelve en Content-Disposition el nombre del
    fichero sin descargarlo; Range no se atiende.
  - Códigos de "Contratos menores {año}": 367, 379, 406, 866, 910, 963, 979,
    1151 y 1175 para 2018-2026 (ficha en
    https://web.larioja.org/dato-abierto/datoabierto?n=opd-<cd>). Barrido de
    los códigos 1-1300 con HEAD: el portal crea cada enero seguidos los datos
    abiertos del año (movimientos, detalles, aplicaciones, contratos, pesca,
    licencias), el último existente era el 1183 y entre un año y otro hay
    saltos de hasta 381 códigos sin nada (471-853; 990-1148). No hay otra
    serie de contratos menores (ADER solo publica movimientos, detalles y
    aplicaciones); el 179 es "contratacion_electronica.csv" (licitaciones).
  - CSV en ISO-8859-15, no en cp1252 ni latin-1: el JSON del mismo dato
    (UTF-8) trae '´' donde el CSV trae '?', que es lo que hace un codificador
    latin-9 con lo que no puede representar ('€' sería 0xA4, que cp1252
    leería como '¤'). Así se pierden 3-5 valores por año ('´', '¤', '¨', '¼',
    '½' en TERC_NOMBRE o CONCEPTO, 2021-2026); el resto del CSV coincide
    fila a fila con el JSON, que en 2018-2020 viene roto (cada registro
    partido en 8 objetos de un campo). Separador ';', fin de línea CRLF,
    campos con ';' o comillas entre comillas y con las comillas dobladas, sin
    saltos de línea dentro de los campos. IMPORTE_EJERCICIO con coma decimal
    y sin separador de miles (hay negativos); FECHA 'AAAA/MM/DD 00:00:00.000';
    TERC_CIF enmascarado en las personas físicas ('***6651**'). El fichero de
    un año trae contratos de años anteriores con importe en ese ejercicio.
NO ACCESIBLE desde el entorno de desarrollo (no se ha podido usar para descubrir los códigos):
  - El catálogo https://web.larioja.org/dato-abierto (conexión reiniciada) y
    www.larioja.org (reto de Cloudflare, 403).
  - datos.gob.es, que federa el catálogo
    (a17002943-contratos-menores-{año}-comunidad-autonoma-de-la-rioja):
    Incapsula responde 403 a casi todas las peticiones.
=============================================================================
"""

import argparse
import base64
import codecs
import json
import os
import re
import sys
import time
import unicodedata
import warnings
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones  # noqa: E402
# Lectura común: los mismos nombres que tenía este script, aunque alguno ya solo lo lea comun.lectura
from comun.lectura import (  # noqa: E402, F401
    formato_contenido, _detectar_separador, _lineas_de_datos, _nombres_columnas, _leer_csv_tolerante,
    _celda_texto, PATRON_DATO, _parece_registro, _hoja_a_df, _leer_xlsx, _leer_xls, _buscar_registros)
from comun.lectura_csv import registros_csv  # noqa: E402, F401
# Red común: los mismos nombres que tenía este script, aunque alguno ya solo lo lea comun.red
from comun.red import (  # noqa: E402, F401
    CABECERAS, TIMEOUT_API, TIMEOUT_DESCARGA, INTENTOS, ESPERA_BASE, ESPERA_MAXIMA, CODIGOS_REINTENTABLES,
    ERRORES_RED, ErrorPortal, _espera, pedir_json, pedir_texto)
from comun import red  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

URL_DESCARGA = "https://ias1.larioja.org/opendata/download"
URL_FICHA = "https://web.larioja.org/dato-abierto/datoabierto?n=opd-{cd}"
FORMATO_CSV = "03"               # cf: 01 XLS, 02 XML, 03 CSV, 04 JSON

SERIE = "contratos_menores"
DESCRIPCION = "Contratos menores de la Comunidad Autónoma de La Rioja"
COLUMNAS = ("COD_CONTRATO", "DEPARTAMENTO", "TIPO_EXPEDIENTE", "TERC_CIF", "TERC_NOMBRE",
            "CONCEPTO", "FECHA", "IMPORTE_EJERCICIO")
# Nombre con que el servidor sirve el fichero de cada año (Content-Disposition)
PATRON_FICHERO = re.compile(r"contratos_CAR_((?:19|20)\d{2})\.csv", re.IGNORECASE)
# Otros ficheros que encuentre el sondeo y que se anotan para revisarlos
PATRON_CONTRATACION = re.compile(r"contrat|licita|adjudic|menor", re.IGNORECASE)

# Código (cd) de cada año, verificado en vivo el 2026-09-27. Es el respaldo:
# los años nuevos (2027...) los encuentra el sondeo.
CODIGOS_CONOCIDOS = {2018: 367, 2019: 379, 2020: 406, 2021: 866, 2022: 910,
                     2023: 963, 2024: 979, 2025: 1151, 2026: 1175}

# Sondeo de códigos nuevos (sondear_codigos). Solo se hace si falta algún año
# hasta el actual (en la práctica, cada enero) o con --comprobar-todo: uno a uno
# hasta SONDEO_SEGUIDOS códigos seguidos sin nada, y después uno de cada
# SONDEO_PASO hasta SONDEO_ALCANCE por encima del último código existente. Los
# saltos entre un año y otro han llegado a 381 códigos y los datos abiertos de
# cada año ocupan una docena de códigos seguidos, con algún hueco suelto pero
# siempre con 4 o más seguidos. Unas 350 peticiones HEAD (unos 7 minutos);
# SONDEO_MAXIMO corta un sondeo desbocado.
SONDEO_SEGUIDOS = 30
SONDEO_PASO = 3
SONDEO_ALCANCE = 1000
SONDEO_MAXIMO = 3000
# Desde este mes, que no se encuentre el fichero del año en curso es un error
# (el portal lo crea el 2 de enero: 2024 y 2025 en datos.gob.es)
MES_LIMITE_ANIO_NUEVO = 2

# Codificación de 8 bits de los CSV (ver FUENTES): se prueba después de UTF-8
CODIFICACIONES_8_BITS = ("iso-8859-15",)

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_la_rioja"
TITULO = "LA RIOJA - CONTRATOS MENORES"


# ============================================================================
# UTILIDADES COMUNES (mismo bloque en ccaa_castilla_leon.py, ccaa_murcia.py y
# ccaa_navarra.py): descarga con reintentos, versiones, lectura como texto,
# acumulación de registros y Parquet. Única diferencia: la codificación de 8
# bits de los CSV sale de CODIFICACIONES_8_BITS (aquí ISO-8859-15).
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
    """Descarga `url` en `destino` sin perder nunca la versión anterior
    (comun.red.descargar, con el validar_contenido de este script).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
    """
    return red.descargar(url, destino, params, tipo, validar_contenido=validar_contenido)


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
    """Primera codificación capaz de decodificar el fichero COMPLETO: UTF-8 y,
    si no, las de CODIFICACIONES_8_BITS (en los otros scripts, cp1252 antes que
    latin-1; aquí la del portal, ISO-8859-15, que acepta cualquier byte)."""
    with open(ruta, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    candidatas = ["utf-8-sig"] if inicio.startswith(b"\xef\xbb\xbf") else ["utf-8"]
    for codificacion in candidatas + list(CODIFICACIONES_8_BITS):
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
    (lista de (ruta_actual, rel, metadatos)), cada uno desde todas sus versiones en
    raw/ con el código actual.

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
        # las versiones posteriores al Parquet anterior, un arreglo de lectura no llegaría nunca a
        # las filas ya guardadas
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
# LA RIOJA: CÓDIGOS DE LOS DATOS ABIERTOS, SONDEO Y DESCARGA
# ============================================================================

def url_codigo(cd, formato=FORMATO_CSV):
    """URL de descarga del dato abierto `cd` en el formato `formato`:
    r = base64('cd=N|cf=FF'), como las que enlazan el catálogo y datos.gob.es."""
    r = base64.b64encode(f"cd={cd}|cf={formato}".encode("ascii")).decode("ascii")
    return f"{URL_DESCARGA}?r={r}"


def nombre_publicado(cabeceras):
    """Nombre del fichero en la cabecera Content-Disposition, o None."""
    valor = next((str(v) for k, v in (cabeceras or {}).items() if str(k).lower() == "content-disposition"), "")
    extendido = re.search(r"filename\*\s*=\s*(?:[\w-]+'[\w-]*')?([^;]+)", valor, re.IGNORECASE)
    if extendido:
        return unquote(extendido.group(1).strip().strip('"')) or None
    simple = re.search(r'filename\s*=\s*(?:"([^"]*)"|([^;]*))', valor, re.IGNORECASE)
    if simple:
        return (simple.group(1) if simple.group(1) is not None else simple.group(2)).strip() or None
    return None


def cabecera_fichero(url):
    """HEAD de una descarga: (código HTTP, nombre publicado o None). Reintenta
    los fallos de red, 429 y 5xx; si no hay respuesta lanza ErrorPortal."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.head(url, headers=CABECERAS, timeout=TIMEOUT_API, allow_redirects=False)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                return codigo, nombre_publicado(respuesta.headers)
            detalle = f"HTTP {codigo}"
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def ficheros_codigo(dir_cd):
    """Copias actuales (sin _historico/) de los ficheros de un código."""
    dir_cd = Path(dir_cd)
    if not dir_cd.is_dir():
        return []
    return sorted(r for r in dir_cd.iterdir() if r.is_file() and PATRON_FICHERO.fullmatch(r.name))


def archivos_serie(raw):
    """[(año, cd, ruta)] de las copias actuales de la serie, por año y código."""
    carpeta = Path(raw) / SERIE
    lista = []
    for dir_cd in (carpeta.iterdir() if carpeta.is_dir() else []):
        m = re.fullmatch(r"cd(\d+)", dir_cd.name)
        if m and dir_cd.is_dir():
            lista += [(int(PATRON_FICHERO.fullmatch(r.name).group(1)), int(m.group(1)), r)
                      for r in ficheros_codigo(dir_cd)]
    return sorted(lista)


def leer_sondeo(ruta):
    """{cd: nombre publicado o 'HTTP 403'} de los códigos existentes que
    encontraron los sondeos anteriores."""
    try:
        datos = json.loads(Path(ruta).read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}
    return {int(cd): str(nombre) for cd, nombre in (datos.get("existentes") or {}).items()}


def guardar_sondeo(ruta, existentes, sondeados):
    ruta = Path(ruta)
    datos = {"comprobado": iso(ahora()), "codigos_sondeados": sondeados,
             "existentes": {str(cd): nombre for cd, nombre in sorted(existentes.items())}}
    tmp = ruta.with_name(f".{ruta.name}.tmp")
    tmp.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, ruta)


def serie_sondeada(existentes):
    """{cd: año} de los códigos sondeados que sirven contratos_CAR_<año>.csv."""
    return {cd: int(m.group(1)) for cd, nombre in existentes.items() if (m := PATRON_FICHERO.fullmatch(nombre))}


def sondear_codigos(raw, resumen, buscar=()):
    """Busca en el servidor de descargas los códigos creados después de los de
    CODIGOS_CONOCIDOS. Devuelve {cd: año} de los ficheros de contratos menores
    encontrados (en esta ejecución o en las anteriores). buscar: años que
    faltan; el sondeo para en cuanto los encuentra (sin ellos, llega hasta
    SONDEO_ALCANCE).

    Los códigos son correlativos y el portal crea seguidos los datos abiertos
    de cada año, pero entre un año y otro hay saltos de cientos de códigos sin
    nada (404). Por encima del último código existente se sondea uno a uno
    hasta SONDEO_SEGUIDOS 404 seguidos y después uno de cada SONDEO_PASO hasta
    SONDEO_ALCANCE; si alguno existe, se sondean uno a uno los de alrededor.
    Los que existen pero no son públicos (403) se vuelven a sondear en cada
    ejecución. Lo encontrado se guarda en raw/sondeo_codigos.json, también si
    el sondeo se corta. Un fallo del portal es un error (sin sondeo no se sabe
    si hay un año nuevo), pero se sigue con los códigos conocidos."""
    ruta = Path(raw) / "sondeo_codigos.json"
    inicio = max(CODIGOS_CONOCIDOS.values()) + 1
    existentes = {cd: nombre for cd, nombre in leer_sondeo(ruta).items() if cd >= inicio}
    previos = dict(existentes)
    existe = {}                          # lo sondeado en esta ejecución: cd -> existe

    def probar(cd):
        """Sondea un código (una vez por ejecución); True si existe, aunque no sea público."""
        if cd not in existe:
            if len(existe) >= SONDEO_MAXIMO:
                raise ErrorPortal(f"{SONDEO_MAXIMO} códigos sondeados sin llegar al final")
            codigo, nombre = cabecera_fichero(url_codigo(cd))
            time.sleep(PAUSA)
            if codigo in (404, 410):
                existentes.pop(cd, None)
            elif codigo in (401, 403):
                existentes[cd] = f"HTTP {codigo}"
            elif 200 <= codigo < 300:
                existentes[cd] = nombre or ""
            else:
                raise ErrorPortal(f"código {cd}: HTTP {codigo}", codigo)
            existe[cd] = codigo not in (404, 410)
        return existe[cd]

    def tramo(desde, paso=1):
        """Uno a uno desde `desde`, hacia arriba (paso=1) o hacia abajo (paso=-1),
        hasta SONDEO_SEGUIDOS códigos seguidos sin nada; devuelve el último mirado."""
        cd, seguidos = desde, 0
        while seguidos < SONDEO_SEGUIDOS and cd >= inicio:
            seguidos = 0 if probar(cd) else seguidos + 1
            cd += paso
        return cd - paso

    def ultimo():
        return max(existentes, default=inicio - 1)

    def completo():
        return bool(buscar) and set(buscar) <= set(serie_sondeada(existentes).values())

    try:
        for cd in sorted(c for c, nombre in previos.items() if nombre.startswith("HTTP ")):
            probar(cd)
        cd = tramo(ultimo() + 1) + SONDEO_PASO
        while cd <= ultimo() + SONDEO_ALCANCE and not completo():
            if probar(cd):
                tramo(cd - 1, -1)        # los datos abiertos del año pueden empezar antes
                cd = tramo(cd + 1)
            cd += SONDEO_PASO
    except ErrorPortal as e:
        resumen.fallidos.append(f"sondeo de códigos nuevos ({URL_DESCARGA}): {e}; se sigue con los conocidos")
        print(f"   ❌ sondeo: {e}")
    finally:
        guardar_sondeo(ruta, existentes, len(existe))

    serie = serie_sondeada(existentes)
    otros = [f"cd={cd}: {nombre} ({url_codigo(cd)})" for cd, nombre in sorted(existentes.items())
             if cd not in serie and PATRON_CONTRATACION.search(nombre) and previos.get(cd) != nombre]
    if otros:
        resumen.avisos.append("el sondeo ha encontrado otros ficheros de contratación (no se descargan; "
                              "revisar):\n      " + "\n      ".join(otros))
    nuevos = sum(1 for cd, nombre in existentes.items() if previos.get(cd) != nombre)
    encontrados = ", ".join(f"{anio} (cd={cd})" for cd, anio in sorted(serie.items())) or "ninguno"
    print(f"   {len(existe)} códigos sondeados, {nuevos} nuevos; contratos menores: {encontrados}")
    return serie


def comprobar_columnas(ruta, resumen):
    """Aviso si la cabecera del CSV no es la conocida (se conservan todas las
    columnas igualmente)."""
    with open(ruta, "rb") as f:
        linea = f.readline()
    try:
        cabecera = linea.decode("utf-8-sig")
    except UnicodeDecodeError:
        cabecera = linea.decode(CODIFICACIONES_8_BITS[0], "replace")
    cabecera = cabecera.strip()
    if tuple(cabecera.split(";")) != COLUMNAS:
        resumen.avisos.append(f"{Path(ruta).name}: columnas distintas de las conocidas ({cabecera[:200]}); "
                              "se conservan todas, revisar")


def no_disponible(cd, anio, locales, detalle, manifiesto, resumen):
    """El código da 404: lo que se tenía de él queda como retirado (sus filas
    se conservan); si no se tenía nada y es un código conocido, es un error."""
    etiqueta = f"{SERIE} {anio} (cd={cd})"
    publicados = [r for r in locales if manifiesto.get(manifiesto.rel(r)).get("publicado", True)]
    for ruta in publicados:
        manifiesto.retirar(ruta, detalle)
    if publicados:
        resumen.retirados.append(f"{etiqueta}: el portal ya no lo sirve ({detalle}); se conservan sus filas")
        print(f"  🗑️ {anio} (cd={cd}): retirado por el portal")
    elif locales:
        resumen.sin_cambios.append(f"{etiqueta} (sigue retirado: {detalle})")
    elif CODIGOS_CONOCIDOS.get(anio) == cd:
        resumen.fallidos.append(f"{etiqueta}: publicado según las fuentes y ahora no disponible "
                                f"({detalle}; {url_codigo(cd)})")
        print(f"  ❌ {anio} (cd={cd}): {detalle}")
    else:
        resumen.avisos.append(f"{etiqueta}: lo encontró el sondeo y ahora da {detalle}")


def descargar_codigo(cd, anio, raw, manifiesto, resumen, comprobar_todo=False):
    """Descarga el CSV del año `anio` que publica el código `cd` en
    raw/contratos_menores/cd<N>/contratos_CAR_<anio>.csv. Los años cerrados
    que ya se tienen solo se vuelven a pedir con --comprobar-todo. Antes se
    comprueba con HEAD que el código sirve contratos_CAR_<anio>.csv: si sirve
    otra cosa (o no responde) es un error y no se descarga ni se retira nada."""
    dir_cd = Path(raw) / SERIE / f"cd{cd}"
    locales = ficheros_codigo(dir_cd)
    etiqueta = f"{anio} (cd={cd})"
    if locales and not comprobar_todo and anio < ahora().year - 1:
        resumen.sin_cambios.append(f"{SERIE} {etiqueta} (ya descargado; --comprobar-todo para volver a pedirlo)")
        return
    url = url_codigo(cd)
    try:
        codigo, nombre = cabecera_fichero(url)
    except ErrorPortal as e:
        resumen.fallidos.append(f"{SERIE} {etiqueta}: {e} ({url})")
        print(f"  ❌ {etiqueta}: {e}")
        return
    time.sleep(PAUSA)
    if codigo in (404, 410):
        no_disponible(cd, anio, locales, f"HTTP {codigo}", manifiesto, resumen)
        return
    m = PATRON_FICHERO.fullmatch(nombre or "")
    if not 200 <= codigo < 300 or not m or int(m.group(1)) != anio:
        detalle = f"HTTP {codigo}" if not 200 <= codigo < 300 else f"el servidor lo sirve como {nombre!r}"
        resumen.fallidos.append(f"{SERIE} {etiqueta}: {detalle}, no como contratos_CAR_{anio}.csv; "
                                f"no se descarga ni se retira nada ({url})")
        print(f"  ❌ {etiqueta}: {detalle}")
        return
    # Siempre con el mismo nombre (el publicado, salvo mayúsculas, que van al
    # manifiesto): un código es un año y un fichero
    destino = dir_cd / f"contratos_CAR_{anio}.csv"
    estado, detalle = descargar(url, destino, tipo="csv")
    time.sleep(PAUSA)
    if estado == "no_existe":
        no_disponible(cd, anio, locales, detalle, manifiesto, resumen)
        return
    if estado not in ESTADOS_OK:
        resumen.fallidos.append(f"{SERIE} {etiqueta}: {detalle} ({url})")
        print(f"  ❌ {etiqueta}: {detalle}")
        return
    manifiesto.registrar(destino, url, estado, dataset=SERIE, anio=anio, cd=cd, fichero_publicado=nombre,
                         ficha=URL_FICHA.format(cd=cd))
    resumen.descarga(f"{SERIE} {etiqueta}", estado)
    print(f"  ✅ {etiqueta}: {estado}")
    if estado != "sin_cambios":
        comprobar_columnas(destino, resumen)


def anios_sin_codigo(codigos):
    """Años desde el primero conocido hasta el actual sin ningún código."""
    return [anio for anio in range(min(CODIGOS_CONOCIDOS), ahora().year + 1) if anio not in codigos.values()]


def descargar_todo(raw, manifiesto, resumen, comprobar_todo=False, sondeo=True):
    """Códigos de la serie: los conocidos, los que encontraron los sondeos
    anteriores y los ya descargados (aunque ya no estén en la lista ni en el
    sondeo: si el portal ya no los sirve quedan como retirados). Se sondean
    códigos nuevos si falta algún año hasta el actual o con --comprobar-todo."""
    codigos = {cd: anio for anio, cd in CODIGOS_CONOCIDOS.items()}
    for cd, anio in serie_sondeada(leer_sondeo(Path(raw) / "sondeo_codigos.json")).items():
        codigos.setdefault(cd, anio)
    for anio, cd, _ in archivos_serie(raw):
        codigos.setdefault(cd, anio)
    faltan = anios_sin_codigo(codigos)
    if sondeo and (faltan or comprobar_todo):
        print(f"\n🔎 Sondeo de códigos nuevos en el servidor de descargas"
              f"{' (sin código: ' + ', '.join(map(str, faltan)) + ')' if faltan else ''}...")
        for cd, anio in sondear_codigos(raw, resumen, faltan).items():
            codigos.setdefault(cd, anio)
    anios = {}
    for cd, anio in codigos.items():
        anios.setdefault(anio, []).append(cd)
    for anio, cds in sorted(anios.items()):
        if len(cds) > 1:
            lista = ", ".join(str(cd) for cd in sorted(cds))
            resumen.avisos.append(f"{SERIE} {anio}: publicado con más de un código ({lista}); "
                                  "se descargan todos (revisar si repiten filas)")
    print(f"\n📦 {SERIE}: {DESCRIPCION}")
    for cd, anio in sorted(codigos.items(), key=lambda par: (par[1], par[0])):
        descargar_codigo(cd, anio, raw, manifiesto, resumen, comprobar_todo)
    for anio in anios_sin_codigo(codigos):
        if anio == ahora().year and ahora().month >= MES_LIMITE_ANIO_NUEVO:
            resumen.fallidos.append(f"{SERIE} {anio}: no se encuentra el fichero del año en curso (ni en "
                                    "CODIGOS_CONOCIDOS ni en el sondeo); si ya está publicado, añadir su "
                                    "código a CODIGOS_CONOCIDOS")
        else:
            resumen.no_publicado(SERIE, anio)


def generar_parquets(salida, raw, manifiesto, resumen):
    print("\n🧱 Generando Parquet...")
    ficheros = []
    for anio, cd, ruta in archivos_serie(raw):
        rel = manifiesto.rel(ruta)
        ficheros.append((ruta, rel, {"_fuente": manifiesto.get(rel).get("url") or url_codigo(cd),
                                     "_dataset": SERIE, "_recurso": f"opd-{cd}",
                                     "_anio_fichero": str(anio), "_archivo_origen": rel}))
    destino = Path(salida) / f"{SERIE}.parquet"
    if ficheros or destino.exists():
        construir_parquet(destino, ficheros, raw, manifiesto, resumen)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores del Gobierno de La Rioja")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar el Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar el Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años antiguos ya descargados y sondear "
                             "códigos nuevos aunque no falte ningún año")
    parser.add_argument("--sin-sondeo", action="store_true",
                        help="no buscar códigos nuevos: solo los conocidos, los que encontraron los "
                             "sondeos anteriores y los ya descargados")
    args = parser.parse_args(argv)

    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Servidor: {URL_DESCARGA}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, args.comprobar_todo, sondeo=not args.sin_sondeo)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
