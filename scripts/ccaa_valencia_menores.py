#!/usr/bin/env python3
"""
=============================================================================
COMUNITAT VALENCIANA - CONTRATOS MENORES FUERA DE LA PLACSP Y DEL REGCON
=============================================================================
Descarga, tal como los publican, los contratos menores de órganos valencianos
que no llegan (o llegan muy poco) a la PLACSP (sindicación 1143) ni al Registro
de Contratos de la Generalitat (REGCON, scripts/ccaa_valencia.py), y genera un
Parquet por fuente con todas sus filas y columnas, como texto.

Ejecutar:  python scripts/ccaa_valencia_menores.py [--salida DIR] [--fuentes uv,valencia,...]
           [--desde AÑO] [--hasta AÑO] [--solo-descarga] [--solo-parquet]
           [--comprobar-todo] [--con-pdf]

Fuentes (--fuentes, por defecto todas; cada una da uno o dos Parquet):
    uv         Universitat de València: relaciones trimestrales XLSX/XLS 2015-2026
               -> universitat_valencia(.parquet) y universitat_valencia_va
    valencia   Ajuntament de València: buscador de contratos menores (HTML) 2017-2026
               -> ajuntament_valencia
    alicante   Diputación de Alicante: registro LIGATE, XLSX 2016-2026
               -> diputacion_alicante y diputacion_alicante_va
    ua         Universidad de Alicante: relaciones trimestrales XLSX 2018-2026
               -> universidad_alicante
    umh        Universidad Miguel Hernández: relaciones trimestrales XLSX 2018-2026
               -> universidad_miguel_hernandez
    upv        Universitat Politècnica de València: CSV anuales del CKAN upvtransparent
               2015-2026 -> universitat_politecnica_valencia

Salida (por defecto <repo>/ccaa_valencia_menores/):
    raw/<serie>/<ruta publicada>          ficheros tal cual (con la ruta que tienen en el portal)
    raw/<serie>/_enlaces.json, _pagina.html   inventario de la página índice y la página
                                          (solo se guarda una versión nueva si cambian los enlaces)
    raw/ajuntament_valencia/<ESTADO>/<AAAA>/<AAAA-MM>.html   respuesta (HTML) de cada consulta
    raw/ajuntament_valencia/_troceadas/   respuestas con 500 filas (se piden de nuevo por días)
    raw/universitat_politecnica_valencia/<paquete>/_paquete.json   ficha CKAN de cada paquete
    raw/.../_historico/                   versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json                  URL, metadatos, fecha de descarga y comprobación de cada fichero
    raw/descarga_log.txt                  resumen de cada ejecución (se añade al final)
    <serie>.parquet, _historico/          registros acumulados (todas las columnas, texto)

Columnas añadidas: _fuente (URL), _dataset (serie), _anio, _trimestre, _mes,
_periodo (texto con el que la página describe el fichero), _categoria,
_estado_consulta, _consulta_desde y _consulta_hasta (buscador de València;
hasta exclusiva), _archivo_origen (ruta en raw/), _hoja, _titulo_hoja (filas de
título que hay encima de la cabecera), _tabla_dinamica, _fecha_descarga y, de
comun/historico.py, _primera_descarga, _ultima_descarga y _en_ultima_descarga.
En el buscador de València además _num_contrato (del enlace a la ficha) y la
columna "<columna> (enlace)" con la URL de la ficha. Un registro que el portal
retira o modifica NO desaparece: sigue en el Parquet con _en_ultima_descarga=False
(sesgo del superviviente). Si una fuente falla no se retira nada.

Para contar contratos sin duplicados (ver FUENTES): _en_ultima_descarga=True,
_tabla_dinamica distinto de "resumen" y, además, deduplicar por el identificador
del contrato: en la UV hay ficheros acumulados (2024: 1r-2n, 1r-2n-3er y anual;
desde el 3T de 2025, los de gastos menores y otros gastos acumulan el año) y en
València una misma fila puede salir en dos consultas (_num_contrato). Los Parquet
*_va solo tienen las versiones en valenciano de ficheros que ya están en castellano.

Qué se descarga y cuándo se vuelve a pedir:
- Ficheros enlazados (UV, Diputación, UA, UMH): los que enlaza la página índice
  (para UMH y la Diputación, solo los de la sección de contratos menores). Los de
  años cerrados que ya se tienen solo se vuelven a pedir con --comprobar-todo; el
  año en curso y el anterior, siempre. Los PDF no se descargan (salvo --con-pdf):
  en todas estas páginas cada PDF repite una hoja de cálculo del mismo nombre; los
  que no la tienen se listan en los avisos. Un fichero que la página deja de
  enlazar o que pasa a dar 404 queda como retirado (sus filas se conservan).
- Buscador de València: una consulta por estado y mes (ADJUDICADOS) o año
  (MODIFICADOS, RESUELTOS); si devuelve 500 filas (el máximo) se trocea en meses y
  días. Se vuelven a pedir el año en curso y el anterior; el resto, --comprobar-todo.
- UPV: los recursos CSV de los dos paquetes CKAN.

FUENTES (verificado en vivo el 2026-09-27)
-------
1. Universitat de València (UV)
   Página índice: https://www.uv.es/uvweb/transparencia-uv/es/economia-presupuesto/contratacion/contratos-menores-1285948376930.html
   (en valenciano: .../transparencia-uv/ca/economia-pressupost/contractacio-/contractes-menors-1285948376930.html;
   la inglesa enlaza lo mismo que la valenciana). Ficheros en
   https://www.uv.es/contratacion/PORTALTRANSPARENCIA/menores/ (el directorio da 403).
   - Una sección "EJERCICIO AAAA" por año, de 2015 (3T y 4T) a 2026 (1T y 2T).
     2025-2026: tabla con tres grupos (rowspan): 1. Contratos menores, 2. Gastos
     menores y 3. Otros gastos (contratos basados en acuerdos marco). Hasta 2024:
     párrafos con el texto del enlace. Los trimestres aún no publicados están en
     comentarios HTML (no se piden).
   - Muchos enlaces son http:// (el proxy da 403): se piden por https://. La página
     valenciana tiene un enlace roto a una unidad local (http://Z:\\contratacion\\...).
   - Contratos menores de 2025-2026: solo los de 5.000 EUR o más (1T-2026: 205 filas;
     las 4.413 del inventario eran filas vacías con formato). Los gastos menores y
     otros gastos de 2025 (2T-4T) y 2026 son "tablas dinámicas": la hoja visible
     es un resumen por unidad, NIF y adjudicatario (importe y nº), y los registros
     uno a uno solo están en la caché de la tabla dinámica (la hoja de origen es
     un libro interno de la UV). Se leen las dos cosas: la hoja con
     _tabla_dinamica="resumen" y los registros con _tabla_dinamica="registros"
     (GM 1T-2026: 4.141 registros con NIF, fecha, objeto e importes).
   - Ficheros acumulados: en 2024, además de los trimestrales, 1r-2n, 1r-2n-3er y
     "Año 2024"; desde el 3T de 2025 los de gastos menores y otros gastos traen
     todo el año hasta ese trimestre (gastos menores 2025: 6.533, 6.393, 17.588 y
     24.341 registros por trimestre, 24.637 identificadores distintos). Se conservan
     todos tal cual: para contar, deduplicar por "IDENTIFICADOR CONTRATO",
     "Identificador" o "Núm. Expediente" (otros gastos).
   - Castellano y valenciano: hasta 2023 las dos páginas enlazan los mismos
     ficheros; en 2024-2026 cada una enlaza su versión (cabeceras traducidas y, a
     veces, valores revisados: "Contratos_menores1_2024 revisado.xlsx" frente a
     "Contractes_menors1_2024.xlsx"). universitat_valencia tiene lo que enlaza la
     página en castellano; universitat_valencia_va, lo que solo enlaza la valenciana.
   - NIF del adjudicatario en todos los años (personas físicas enmascaradas: 189****1*).
     Los PDF repiten los XLS/XLSX del mismo nombre.
2. Ajuntament de València: buscador de contratos menores
   https://www.valencia.es/cas/ayuntamiento/buscador-contratos-menores (portlet Liferay)
   - Se abre la página (cookie JSESSIONID y acción del formulario con p_auth) y se
     hace un POST multipart con el formulario: estado (ADJUDICADOS, MODIFICADOS o
     RESUELTOS; no hay "todos": 77 de los 78 modificados salen también como
     adjudicados, los resueltos no), fechas desde/hasta y nº máximo de resultados
     (el formulario ofrece hasta 500; el servidor acepta más, p.ej. 1000, pero no
     se usa).
   - El filtro compara fecha y hora: "hasta" cuenta como ese día a las 00:00. Los
     contratos de ese día grabados con hora quedan fuera (marzo de 2026: 01/03-31/03
     da 157 filas y 01/03-01/04, 167) y los grabados a las 00:00 entran. Por eso cada
     mes se pide del día 1 al día 1 del mes siguiente, y los de fecha día 1 a las
     00:00 salen en dos meses seguidos (243 de 19.054 adjudicados en 2017-2026): son
     la misma fila en dos respuestas; _num_contrato la identifica.
   - La página dice "desde el 1 de enero de 2018", pero hay 119 de 2017 (enero,
     octubre-diciembre) y ninguno anterior (se comprueba con una consulta
     2000-2016). 18.883 contratos distintos en 2017-2026, unos 2.000 al año;
     MODIFICADOS: 78 y RESUELTOS: 72 filas en total.
   - Columnas: objeto, tipo, fecha, importe sin IVA, IVA, expediente, órgano,
     unidad tramitadora, nº de propuesta, nº de ofertas, adjudicatario, NIF
     (personas físicas enmascaradas: *****607X), estado, fechas de inicio, fin,
     factura y pago, centro de gastos y aplicación presupuestaria. La ficha de cada
     contrato (enlace) no añade datos (solo el importe con IVA, suma de los dos).
   - Se valida cada respuesta: tabla presente, fechas y estado de la consulta
     devueltos en el formulario y sin el aviso "servicio no disponible". Si no, se
     reintenta con una sesión nueva; si sigue fallando, error (no se retira nada).
3. Diputación de Alicante: registro LIGATE
   https://abierta.diputacionalicante.es/informacion-economica-presupuestaria-y-estadistica/contratacion/
   (sección "CONTRATOS MENORES"; en valenciano .../ca/informacio-economica-pressupostaria-i-estadistica/contractacio/,
   sección "CONTRACTES MENORS").
   - 2016-1T 2018: XLSX trimestrales (1T-2018: una hoja por departamento) y PDF
     iguales. Desde el 1 de junio de 2018, relaciones del registro LIGATE: anuales
     2018-2022, 2023 partido el 20 de julio, 2024 y 2025 anuales y 2026 trimestral
     (el de "a 30 de junio" trae solo "Contratos registrados del 01/04/2026 a
     30/06/2026"). ~1.500 al año (2025: 1.494).
   - Sin NIF: solo el nombre del tercero. El título de cada hoja (versión, fuente,
     cobertura) se guarda en _titulo_hoja.
   - La sección enlaza varias veces, sin texto, RP-922-Adjudic-a-31-12-2024-cas.xlsx,
     que da 404 (enlace roto: aviso). La página valenciana enlaza las versiones "val"
     de 2025 y 2026 (diputacion_alicante_va) y el resto en castellano.
4. Universidad de Alicante (UA)
   https://sctr.ua.es/es/01-presentacion/7-transparencia/contratos-menores/contratos-menores.html
   - XLSX trimestrales (un h3 por año, 2018-2026). Sin NIF (proveedor, objeto,
     tipo, fecha e importe sin IVA). 1T-2026: 1.609 filas. No está en el 1143.
5. Universidad Miguel Hernández (UMH)
   https://seguimientocontratacion.umh.es/transparencia/ (sección "Contratos Menores")
   - XLSX trimestrales desde el 3T de 2021; 2018-2021 y 4T-2023 son documentos de la
     PLACSP (docAccCmpnt, sin extensión: el formato se detecta al descargar; los de
     2018-2019 tienen una hoja por departamento) y hay resúmenes anuales 2018-2020
     que repiten esas relaciones. Con NIF del proveedor. Los "Contratos basados en
     acuerdos marco" (otra sección) no se descargan. Los menores de 2014-2018 están
     en otra página (https://sicgef.umh.es/contratos-menores-transparencia-sicgef/),
     no incorporada.
6. Universitat Politècnica de València (UPV): CKAN https://upvtransparent.upv.es
   - Paquetes contratos-menores (CSV y XLSX anuales 2015-2023; se descarga el CSV,
     que en 2023 es además la versión más reciente) y contratos-menores-ley-1-2022
     (CSV 2024, 2025 y el acumulado de 2026). ~54.000 filas al año. Sin NIF (solo
     el nombre del tercero). En la página de transparencia de la UPV solo hay dos
     botones que llevan a estos paquetes. No está en el 1143.

Otras fuentes valencianas revisadas (2026-09-27), no incorporadas:
  - Ayuntamiento de Elche: transparencia.elche.es corta la conexión desde el entorno de desarrollo;
    www.elche.es solo remite al perfil del contratante de su sede electrónica.
  - Ajuntament de Castelló: www.castello.es corta la conexión;
    transparencia.castello.es (Govern Obert) solo tiene fichas de taxonomía.
  - Diputació de València (www.dival.es corta la conexión) y de Castelló
    (www.dipcas.es, 403): en el 1143 según el inventario.
  - UJI, Ayuntamiento de Alicante y departamentos de salud: publican en la PLACSP
    (1143) o en el REGCON; no se ha encontrado una relación propia contrato a contrato.
  - Páginas en valenciano de la UA y la UMH: no revisadas.
=============================================================================
"""

import argparse
import codecs
import datetime as dt
import io
import json
import math
import os
import posixpath
import re
import sys
import time
import unicodedata
import warnings
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse, urlunparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup, NavigableString

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones  # noqa: E402
from comun.lectura_csv import registros_csv  # noqa: E402

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

SALIDA = Path(__file__).resolve().parent.parent / "ccaa_valencia_menores"
TITULO = "COMUNITAT VALENCIANA - CONTRATOS MENORES FUERA DE LA PLACSP Y DEL REGCON"

UV_PAGINA = ("https://www.uv.es/uvweb/transparencia-uv/es/economia-presupuesto/contratacion/"
             "contratos-menores-1285948376930.html")
UV_PAGINA_VA = ("https://www.uv.es/uvweb/transparencia-uv/ca/economia-pressupost/contractacio-/"
                "contractes-menors-1285948376930.html")
DIPA_PAGINA = "https://abierta.diputacionalicante.es/informacion-economica-presupuestaria-y-estadistica/contratacion/"
DIPA_PAGINA_VA = ("https://abierta.diputacionalicante.es/ca/informacio-economica-pressupostaria-i-estadistica/"
                  "contractacio/")
UA_PAGINA = "https://sctr.ua.es/es/01-presentacion/7-transparencia/contratos-menores/contratos-menores.html"
UMH_PAGINA = "https://seguimientocontratacion.umh.es/transparencia/"

# Series de ficheros enlazados desde una página índice:
#   pagina     página que enlaza los ficheros
#   seccion    (opcional) regex del título (h1-h6) de la sección: solo cuentan los
#              enlaces que hay entre ese título y el siguiente del mismo nivel o superior
#   admitir    regex de las URL (ya normalizadas) de los ficheros de la serie
#   base       parte de la ruta de la URL que no se repite en la ruta local
#   https      hosts cuyos enlaces http:// se piden por https:// (HTTP plano: 403)
#   principal  serie en castellano: aquí solo van los ficheros que ella no enlaza
SERIES_PAGINA = {
    "universitat_valencia": {
        "descripcion": "Universitat de València: contratos menores, gastos menores y otros gastos",
        "pagina": UV_PAGINA,
        "admitir": r"^https://www\.uv\.es/contratacion/PORTALTRANSPARENCIA/menores/",
        "base": "/contratacion/PORTALTRANSPARENCIA/menores/",
        "https": ("www.uv.es", "uv.es"),
    },
    "universitat_valencia_va": {
        "descripcion": "Universitat de València: ficheros que solo enlaza la página en valenciano",
        "pagina": UV_PAGINA_VA,
        "admitir": r"^https://www\.uv\.es/contratacion/PORTALTRANSPARENCIA/menores/",
        "base": "/contratacion/PORTALTRANSPARENCIA/menores/",
        "https": ("www.uv.es", "uv.es"),
        "principal": "universitat_valencia",
    },
    "diputacion_alicante": {
        "descripcion": "Diputación de Alicante: contratos menores (registro LIGATE desde junio de 2018)",
        "pagina": DIPA_PAGINA,
        "seccion": r"^CONTRATOS MENORES$",
        "admitir": r"^https://abierta\.diputacionalicante\.es/wp-content/uploads/",
        "base": "/wp-content/uploads/",
        "https": ("abierta.diputacionalicante.es",),
    },
    "diputacion_alicante_va": {
        "descripcion": "Diputación de Alicante: ficheros que solo enlaza la página en valenciano",
        "pagina": DIPA_PAGINA_VA,
        "seccion": r"^CONTRACTES MENORS$",
        "admitir": r"^https://abierta\.diputacionalicante\.es/wp-content/uploads/",
        "base": "/wp-content/uploads/",
        "https": ("abierta.diputacionalicante.es",),
        "principal": "diputacion_alicante",
    },
    "universidad_alicante": {
        "descripcion": "Universidad de Alicante: contratos menores",
        "pagina": UA_PAGINA,
        "admitir": r"^https://sctr\.ua\.es/es/01-presentacion/7-transparencia/contratos-menores/",
        "base": "/es/01-presentacion/7-transparencia/contratos-menores/",
        "https": ("sctr.ua.es",),
    },
    "universidad_miguel_hernandez": {
        "descripcion": "Universidad Miguel Hernández: contratos menores",
        "pagina": UMH_PAGINA,
        "seccion": r"^Contratos Menores$",
        "admitir": r"^https://(seguimientocontratacion\.umh\.es/files/|contrataciondelestado\.es/wps/)",
        "base": "/files/",
        "https": ("seguimientocontratacion.umh.es", "contrataciondelestado.es"),
    },
}

# Buscador de contratos menores del Ajuntament de València
VLC_SERIE = "ajuntament_valencia"
VLC_PAGINA = "https://www.valencia.es/cas/ayuntamiento/buscador-contratos-menores"
# Estado de la consulta -> ventana con la que se empieza ('mes' o 'anio')
VLC_ESTADOS = {"ADJUDICADOS": "mes", "MODIFICADOS": "anio", "RESUELTOS": "anio"}
VLC_MAX_FILAS = 500          # máximo del formulario: una consulta con 500 filas se trocea
VLC_ANIO_INICIO = 2017       # primeros registros: enero de 2017
VLC_PREVIAS_DESDE = date(2000, 1, 1)   # consulta de control de lo anterior a --desde
PAUSA_BUSCADOR = 1.5         # segundos entre consultas al buscador
TIMEOUT_BUSCADOR = 180

# UPV: CKAN upvtransparent
UPV_SERIE = "universitat_politecnica_valencia"
UPV_CKAN = "https://upvtransparent.upv.es/api/3/action/package_show"
UPV_PAQUETES = ("contratos-menores", "contratos-menores-ley-1-2022")

# --fuentes -> series (en este orden: la serie en castellano antes que la valenciana)
FUENTES = {
    "uv": ("universitat_valencia", "universitat_valencia_va"),
    "valencia": (VLC_SERIE,),
    "alicante": ("diputacion_alicante", "diputacion_alicante_va"),
    "ua": ("universidad_alicante",),
    "umh": ("universidad_miguel_hernandez",),
    "upv": (UPV_SERIE,),
}


# ============================================================================
# UTILIDADES COMUNES (mismo bloque que en ccaa_castilla_leon.py y ccaa_murcia.py,
# con estos añadidos: lectura del HTML del buscador, registros de la caché de
# las tablas dinámicas, _titulo_hoja y detección de PDF): descarga con
# reintentos, versiones, lectura como texto, acumulación de registros y Parquet.
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
METADATOS_ORIGEN = ("_fuente", "_dataset", "_anio", "_trimestre", "_mes", "_periodo", "_categoria",
                    "_estado_consulta", "_consulta_desde", "_consulta_hasta", "_archivo_origen", "_hoja",
                    "_titulo_hoja", "_tabla_dinamica", "_fecha_descarga")
ORDEN_METADATOS = METADATOS_ORIGEN + ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


def ahora():
    return datetime.now(timezone.utc)


def iso(momento):
    """Fecha ISO 8601 en UTC al segundo: '2026-09-27T14:43:00Z' (ordenable como texto)."""
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    ErrorPortal. Devuelve los bytes de la página."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, headers=CABECERAS, timeout=TIMEOUT_API)
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


def formato_contenido(cabeza):
    """Formato real de un fichero por sus primeros bytes."""
    if cabeza.startswith(b"PK\x03\x04"):
        return "xlsx"
    if cabeza.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "xls"
    if cabeza.startswith(b"%PDF"):
        return "pdf"
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
    HTML de error servida con 200), o None si es válida. tipo None: cualquier
    fichero que no sea una página HTML o XML."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if not cabeza.strip():
        return "respuesta vacía"
    formato = formato_contenido(cabeza)
    if formato in ("html", "xml"):
        return f"la respuesta es {formato.upper()}" + (f", no {tipo.upper()}" if tipo else "")
    if tipo is None:
        return None
    if tipo in ("xlsx", "xls"):
        return None if formato in ("xlsx", "xls") else f"se esperaba una hoja de cálculo y llegó {formato}"
    return None if formato == tipo else f"se esperaba {tipo.upper()} y llegó {formato}"


def bajar(url, tmp, params=None, tipo=None):
    """Descarga `url` en el temporal `tmp` y comprueba que es el tipo de fichero
    esperado. Reintenta con backoff los fallos de red, 429 y 5xx (también cortes
    a mitad de descarga). Devuelve (estado, detalle): 'ok', 'no_existe'
    (404/410), 'invalido' (no es el fichero esperado) o 'error'."""
    detalle = ""
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
                    return ("invalido", motivo) if motivo else ("ok", "")
                detalle = f"HTTP {codigo}"
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except requests.exceptions.RequestException as e:
            return "error", f"{type(e).__name__}: {str(e)[:150]}"
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    return "error", f"{detalle} (tras {INTENTOS} intentos)"


def descargar(url, destino, params=None, tipo=None):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal (bajar) y lo entrega a guardar_version(): si el
    contenido no cambió no se toca nada y si cambió la copia previa pasa a
    _historico/. Devuelve (estado, detalle): 'nuevo' | 'actualizado' |
    'sin_cambios', 'no_existe' (404/410), 'invalido' (no es el fichero
    esperado) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    try:
        estado, detalle = bajar(url, tmp, params, tipo)
        if estado != "ok":
            return estado, detalle
        return guardar_version(destino, desde=tmp), ""
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

    def entradas(self, dataset):
        """(rel, entrada) de los ficheros de una serie, por ruta."""
        return sorted((rel, e) for rel, e in self.datos.items() if e.get("dataset") == dataset)

    def guardar(self):
        self.raw.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_name(f".{self.ruta.name}.tmp")
        tmp.write_text(json.dumps(self.datos, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.ruta)


class Resumen:
    """Lo descargado, lo que no existe, lo retirado, lo que falló y las filas por año."""

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
        self.estadisticas = []

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
        bloque("FILAS VIGENTES POR AÑO (_en_ultima_descarga=True)", self.estadisticas)
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


PATRON_DATO = re.compile(r"-?\d+(?:[.,]\d+)*|\d{4}-\d{2}-\d{2}(?:[ T][\d:.]+)?|[A-Z]?\d{7,8}[A-Z]?|[A-Z]\d{7}[A-Z0-9]",
                         re.IGNORECASE)


def _parece_registro(fila):
    """¿La fila detectada como cabecera son datos? Lo son si la mayoría de sus
    celdas con valor son números, fechas o NIF (una cabecera son rótulos)."""
    valores = [str(v).strip() for v in fila if v is not None and str(v).strip()]
    return bool(valores) and sum(bool(PATRON_DATO.fullmatch(v)) for v in valores) * 2 > len(valores)


def _hoja_a_df(filas, nombre, hoja, avisos):
    """Tabla de una hoja: detecta la fila de cabecera (las filas de título de
    encima se anotan en los avisos y en _titulo_hoja) y conserva todas las filas
    con algún valor."""
    # Número de fila original de cada fila con algún valor (para el aviso)
    numeradas = [(i, list(f)) for i, f in enumerate(filas) if any(v is not None for v in f)]
    if not numeradas:
        return None
    filas = [f for _, f in numeradas]
    llenas = [sum(v is not None for v in f) for f in filas[:50]]
    maximo = max(llenas)
    umbral = 1 if maximo < 2 else max(2, math.ceil(0.6 * maximo))
    pos = next((i for i, n in enumerate(llenas[:30]) if n >= umbral), 0)
    titulo = None
    if pos:
        titulo = " | ".join(" ".join(str(v) for v in f if v is not None) for f in filas[:pos])
        avisos.append(f"{nombre} [{hoja}]: {numeradas[pos][0]} filas antes de la cabecera "
                      f"(no son datos; quedan en _titulo_hoja): {titulo[:200]}")
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
    df["_titulo_hoja"] = titulo
    return df


# Tablas dinámicas de un XLSX: la hoja visible es un resumen y los registros de
# origen solo están en la caché (xl/pivotCache/pivotCacheRecords*.xml) cuando
# la hoja de origen no está en el libro (UV, gastos menores 2025-2026).
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_NS_RELS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
_NS_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_ESCAPE_OOXML = re.compile(r"_x([0-9A-Fa-f]{4})_")


def _desescapar(texto):
    """Texto de un atributo OOXML: '_x000a_' -> salto de línea."""
    return _ESCAPE_OOXML.sub(lambda m: chr(int(m.group(1), 16)), texto)


def _relaciones(libro, parte):
    """{Id: (tipo, destino)} de las relaciones de una parte del paquete (destino
    como nombre dentro del zip, o tal cual si es externo)."""
    carpeta, nombre = posixpath.split(parte)
    ruta = posixpath.join(carpeta, "_rels", nombre + ".rels")
    if ruta not in libro.namelist():
        return {}
    relaciones = {}
    for rel in ET.fromstring(libro.read(ruta)).iter(_NS_RELS + "Relationship"):
        destino = rel.get("Target", "")
        if rel.get("TargetMode") != "External":
            destino = destino.lstrip("/") if destino.startswith("/") else posixpath.normpath(
                posixpath.join(carpeta, destino))
        relaciones[rel.get("Id")] = (rel.get("Type", ""), destino)
    return relaciones


def _valor_cache(elemento):
    """Valor de un elemento de la caché (s, n, d, b, e, m) como texto."""
    etiqueta = elemento.tag.replace(_NS, "")
    valor = elemento.get("v")
    if etiqueta == "m" or valor is None:
        return None
    if etiqueta == "s":
        return _desescapar(valor) if valor != "" else None
    if etiqueta == "n":
        return _celda_texto(float(valor))
    if etiqueta == "d":
        return _celda_texto(datetime.fromisoformat(valor))
    if etiqueta == "b":
        return str(valor in ("1", "true"))
    return valor                                             # e: error de Excel (#N/A...)


def _tablas_dinamicas(ruta, avisos):
    """(hojas con una tabla dinámica, [DataFrame con los registros de cada caché]).
    Si la hoja de origen de una caché está en el libro, sus registros ya están en
    esa hoja y la caché no se lee (se avisa)."""
    nombre = Path(ruta).name
    with zipfile.ZipFile(ruta) as libro:
        partes = set(libro.namelist())
        definiciones = sorted((n for n in partes if re.fullmatch(r"xl/pivotCache/pivotCacheDefinition\d+\.xml", n)),
                              key=lambda n: int(re.search(r"(\d+)\.xml$", n).group(1)))
        if not definiciones or "xl/workbook.xml" not in partes:
            return set(), []
        rels_libro = _relaciones(libro, "xl/workbook.xml")
        hojas, con_tabla = [], set()
        for hoja in ET.fromstring(libro.read("xl/workbook.xml")).iter(_NS + "sheet"):
            hojas.append(hoja.get("name"))
            destino = rels_libro.get(hoja.get(_NS_R + "id"), ("", ""))[1]
            if any(tipo.endswith("/pivotTable") for tipo, _ in _relaciones(libro, destino).values()):
                con_tabla.add(hoja.get("name"))
        tablas = []
        for definicion in definiciones:
            raiz = ET.fromstring(libro.read(definicion))
            fuente = raiz.find(f"{_NS}cacheSource/{_NS}worksheetSource")
            hoja_origen = fuente.get("sheet") if fuente is not None else None
            origen = (f"{hoja_origen or (fuente.get('name') or '')}!{fuente.get('ref') or ''}"
                      if fuente is not None else "")
            parte = Path(definicion).stem
            if hoja_origen in hojas and hoja_origen not in con_tabla:
                avisos.append(f"{nombre}: la caché {parte} viene de la hoja '{hoja_origen}' del propio libro; "
                              "sus registros ya están en esa hoja y no se leen de la caché")
                continue
            campos = []                   # (nombre, valores compartidos) de los campos con registros
            for campo in raiz.iter(_NS + "cacheField"):
                if campo.get("databaseField", "1") in ("0", "false"):
                    continue              # campo calculado o agrupación: no está en los registros
                compartidos = campo.find(_NS + "sharedItems")
                campos.append((_desescapar(campo.get("name", "")),
                               [] if compartidos is None else [_valor_cache(x) for x in compartidos]))
            registros = next((d for t, d in _relaciones(libro, definicion).values()
                              if t.endswith("/pivotCacheRecords")), None)
            if registros is None or registros not in partes:
                avisos.append(f"{nombre}: la tabla dinámica {parte} ({origen}) no guarda sus registros")
                continue
            filas = []
            for _, elemento in ET.iterparse(io.BytesIO(libro.read(registros))):
                if elemento.tag != _NS + "r":
                    continue
                fila = []
                for i, hijo in enumerate(elemento):
                    if i >= len(campos):
                        raise ValueError(f"{nombre}: un registro de {parte} tiene más valores que campos")
                    if hijo.tag == _NS + "x":
                        fila.append(campos[i][1][int(hijo.get("v"))])
                    else:
                        fila.append(_valor_cache(hijo))
                filas.append(fila + [None] * (len(campos) - len(fila)))
                elemento.clear()
            df = pd.DataFrame(filas, columns=_nombres_columnas([c[0] for c in campos]), dtype=object)
            df["_hoja"] = f"{parte} (origen: {origen})"
            df["_titulo_hoja"] = None
            df["_tabla_dinamica"] = "registros"
            tablas.append(df)
            avisos.append(f"{nombre}: {len(df):,} registros en la caché de la tabla dinámica ({origen}); "
                          "se leen con _tabla_dinamica='registros'")
    return con_tabla, tablas


def _leer_xlsx(ruta):
    import openpyxl

    avisos, partes = [], []
    con_tabla, caches = _tablas_dinamicas(ruta, avisos)
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
                df["_tabla_dinamica"] = "resumen" if hoja.title in con_tabla else None
                partes.append(df)
    finally:
        libro.close()
        fichero.close()
    return partes + caches, avisos


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
    """Lee un fichero tabular (CSV, XLSX, XLS o la página de resultados del
    buscador de València, según su contenido real) con todas sus filas y
    columnas como texto. Devuelve (DataFrame, avisos)."""
    ruta = Path(ruta)
    with open(ruta, "rb") as f:
        formato = formato_contenido(f.read(4096))
    if formato == "csv":
        return leer_csv(ruta)
    if formato in ("xlsx", "xls"):
        partes, avisos = (_leer_xlsx if formato == "xlsx" else _leer_xls)(ruta)
        hojas = sum(1 for p in partes if "_tabla_dinamica" not in p.columns or p["_tabla_dinamica"].ne("registros").all())
        if hojas > 1:
            avisos.append(f"{ruta.name}: {hojas} hojas con datos; se unen (columna _hoja)")
        if not partes:
            return pd.DataFrame(columns=["_hoja"]), avisos
        return pd.concat(partes, ignore_index=True, sort=False), avisos
    if formato == "html":
        return leer_html_buscador(ruta), []
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
        if not len(df):
            # Una consulta o un fichero sin filas: si antes las tenía no se marca
            # nada como retirado (casi siempre es un fallo, no una retirada)
            if anterior is not None and len(anterior):
                resumen.avisos.append(f"{rel}: la versión del {fecha} no tiene filas; "
                                      "no se marca nada como retirado")
            continue
        anterior = acumular(anterior, df, fecha, ignorar=ignorar)
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

    Las filas de ficheros que ya no se procesan se conservan: si el fichero
    sigue en raw/ se vuelve a procesar con sus versiones; si no, se copian tal
    cual del Parquet anterior.
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
                         if c in grupo.columns and c not in ("_hoja", "_titulo_hoja", "_tabla_dinamica",
                                                             "_fecha_descarga")}
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
# FICHEROS ENLAZADOS DESDE UNA PÁGINA ÍNDICE (UV, DIPUTACIÓN, UA, UMH)
# ============================================================================

EXT_TABLA = (".xlsx", ".xls", ".csv")
EXT_DOCUMENTO = (".pdf",)
EXT_OTROS_DATOS = (".ods", ".odt", ".doc", ".docx", ".zip", ".rar", ".7z", ".json", ".xml", ".txt")
FORMATOS_TABLA = ("xlsx", "xls", "csv")
PATRON_ANIO = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
# Texto de enlace que no describe el fichero ("XSLX", "PDF", "(pdf)", vacío...)
TEXTO_GENERICO = re.compile(r"^[\W_]*(?:xslx|xlsx|xls|csv|pdf|ods|descargar|descarregar|download)?[\W_]*$",
                            re.IGNORECASE)
CATEGORIAS = (
    ("otros gastos", r"otros\s+gastos|altres\s+(?:despeses|menors)|menores\s+otros|other\s+expenses"
                     r"|(?<![a-z])(?:cb|sda)(?![a-z])"),
    ("gastos menores", r"gastos\s+menores|despeses\s+menors|minor\s+expenses|(?<![a-z])gm(?![a-z])"),
    ("contratos menores", r"contratos?\s+menores|contractes?\s+menors|minor\s+contracts|(?<![a-z])cm(?![a-z])"),
)
ORDINALES = {"primer": 1, "primero": 1, "segundo": 2, "segon": 2, "tercer": 3, "tercero": 3, "cuarto": 4,
             "quart": 4, "first": 1, "second": 2, "third": 3, "fourth": 4}


def _texto(nodo):
    return " ".join(nodo.get_text(" ", strip=True).split())


def _es_texto(cadena):
    """Texto visible (no comentarios, scripts ni estilos)."""
    return type(cadena) is NavigableString and cadena.parent is not None \
        and cadena.parent.name not in ("script", "style")


def _texto_fuera_de_enlaces(nodo):
    """Texto de `nodo` sin el de los enlaces que contiene."""
    trozos = []
    for cadena in nodo.find_all(string=True):
        if not _es_texto(cadena):
            continue
        enlace = cadena.find_parent("a")
        if enlace is not None and (enlace is nodo or nodo in enlace.parents):
            continue
        trozos.append(str(cadena))
    return " ".join(" ".join(trozos).split())


def _celdas_extendidas(tr):
    """Textos de las celdas de filas anteriores que llegan a `tr` con rowspan (en
    la UV, el grupo "1. Contratos menores" solo está en la primera fila)."""
    tabla = tr.find_parent("table")
    if tabla is None:
        return []
    activas = []                      # [texto, filas siguientes que aún cubre]
    for fila in tabla.find_all("tr"):
        if fila.find_parent("table") is not tabla:
            continue
        if fila is tr:
            return [t for t, _ in activas if t]
        activas = [[t, n - 1] for t, n in activas if n > 1]
        for celda in fila.find_all(["td", "th"], recursive=False):
            try:
                n = int(celda.get("rowspan", 1))
            except (TypeError, ValueError):
                n = 1
            if n > 1:
                activas.append([_texto(celda), n - 1])
    return []


def _anio_unico(texto):
    anios = set(PATRON_ANIO.findall(texto or ""))
    return int(anios.pop()) if len(anios) == 1 else None


def _anio_encabezado(enlace, limite=None):
    """Año del título más cercano por encima del enlace ("EJERCICIO 2026", un h3
    "2026"...): el texto corto anterior (fuera de enlaces) con un solo año."""
    for elemento in enlace.previous_elements:
        if elemento is limite:
            return None
        if not _es_texto(elemento) or elemento.find_parent("a") is not None:
            continue
        texto = " ".join(str(elemento).split())
        if texto and len(texto) <= 40:
            anio = _anio_unico(texto)
            if anio:
                return anio
    return None


def trimestre_de(*textos):
    """Trimestre ('1'..'4') que nombra el primer texto que habla de trimestres,
    o None si no nombra ninguno o nombra varios ("1r - 2n - 3er trimestre")."""
    for texto in textos:
        t = sin_acentos(texto or "").lower().replace("_", " ")
        if "trim" not in t:
            continue
        # "1er", "2n", "4º" (-> "4o"), "3" seguidos de "trimestre", de un guion o de
        # otra palabra; no "2." (numeración de la lista) ni los dígitos de un año
        numeros = {int(m.group(1)) for m in re.finditer(
            r"(?<![0-9a-z.,])([1-4])\s*(?:er|r|n|t|o|a|ro|do|to)?(?=\s*(?:-|trim|$)|\s+[^0-9\s])", t)}
        numeros |= {int(m.group(1)) for m in re.finditer(r"trim\w*\s*([1-4])(?![0-9])", t)}
        numeros |= {n for palabra, n in ORDINALES.items() if re.search(rf"(?<![a-z]){palabra}(?![a-z])", t)}
        return str(numeros.pop()) if len(numeros) == 1 else None
    return None


def categoria_de(*textos):
    """Contratos menores, gastos menores u otros gastos, según el texto."""
    for texto in textos:
        t = sin_acentos(texto or "").lower().replace("_", " ")
        for categoria, patron in CATEGORIAS:
            if re.search(patron, t):
                return categoria
    return None


def normalizar_url(url, hosts_https=()):
    """URL absoluta comparable: http:// -> https:// en los hosts indicados y la
    ruta con un único escapado ('4%c2%ba' y '4º' son la misma)."""
    partes = urlparse(url.strip())
    esquema, host = partes.scheme.lower(), partes.netloc.lower()
    if esquema == "http" and host in hosts_https:
        esquema = "https"
    ruta = quote(unquote(partes.path), safe="/:@!$&'()*+,;=-._~")
    return urlunparse((esquema, host, ruta, partes.params, partes.query, ""))


def ruta_local(url, base):
    """Ruta en raw/<serie>/ de un fichero: su ruta publicada sin `base`; un
    documento de la PLACSP (sin nombre) se guarda por su DocumentIdParam."""
    partes = urlparse(url)
    ruta = unquote(partes.path)
    documento = parse_qs(partes.query).get("DocumentIdParam")
    if documento:
        rel = f"{partes.netloc}/{documento[0]}"
    elif base and ruta.startswith(base):
        rel = ruta[len(base):]
    else:
        rel = partes.netloc + ruta
    componentes = [c for c in PurePosixPath(rel).parts if c not in ("", ".", "..", "/")]
    return "/".join(componentes)


def _seccion(sopa, patron):
    """(título, siguiente título del mismo nivel o superior) de la sección."""
    for titulo in sopa.find_all(re.compile(r"^h[1-6]$")):
        if re.search(patron, _texto(titulo), re.IGNORECASE):
            nivel = int(titulo.name[1])
            fin = titulo.find_next(lambda t: t.name in {f"h{i}" for i in range(1, nivel + 1)})
            return titulo, fin
    return None, None


def extraer_enlaces(serie, contenido):
    """Ficheros que enlaza la página de una serie: {url: enlace} (url normalizada)
    y los avisos (enlaces mal formados, sección que falta...). Cada enlace lleva
    el texto que lo describe (el suyo o el de su fila o párrafo), el año, el
    trimestre y la categoría que se deducen de ese texto."""
    sopa = BeautifulSoup(contenido, "html.parser")
    avisos = []
    inicio = fin = None
    if serie.get("seccion"):
        inicio, fin = _seccion(sopa, serie["seccion"])
        if inicio is None:
            return {}, [f"la página no tiene la sección '{serie['seccion']}'"]
        anclas = []
        for elemento in inicio.find_all_next():
            if elemento is fin:
                break
            if elemento.name == "a" and elemento.get("href"):
                anclas.append(elemento)
    else:
        anclas = sopa.find_all("a", href=True)
    admitir = re.compile(serie["admitir"], re.IGNORECASE)
    enlaces = {}
    for ancla in anclas:
        href = ancla["href"].strip()
        if "\\" in href:
            if re.search(r"\.(xlsx?|csv|pdf)$", href, re.IGNORECASE):
                avisos.append(f"enlace mal formado (no se puede pedir): {href}")
            continue
        url = normalizar_url(urljoin(serie["pagina"], href), serie.get("https", ()))
        if not admitir.search(url):
            continue
        extension = PurePosixPath(unquote(urlparse(url).path)).suffix.lower()
        if extension in EXT_TABLA:
            tipo = "tabla"
        elif extension in EXT_DOCUMENTO:
            tipo = "pdf"
        elif not extension or "docAccCmpnt" in url:
            tipo = "desconocido"          # p.ej. documentos de la PLACSP: se detecta al bajarlo
        else:
            if extension in EXT_OTROS_DATOS:
                avisos.append(f"enlace a un fichero no tabular ({extension}): {url}")
            continue                      # páginas (.html, .php...) y otros ficheros
        propio = _texto(ancla)
        propio = "" if TEXTO_GENERICO.match(propio) else propio
        contexto = ancla.find_parent(["tr", "li", "p"])
        fila = ""
        if contexto is not None:
            fila = _texto_fuera_de_enlaces(contexto)
            if contexto.name == "tr":
                fila = " ".join(_celdas_extendidas(contexto) + ([fila] if fila else []))
        nombre = unquote(PurePosixPath(urlparse(url).path).name)
        descripcion = propio or fila or Path(nombre).stem
        anio = (_anio_unico(propio) or _anio_unico(fila) or _anio_unico(Path(nombre).stem)
                or _anio_encabezado(ancla, inicio))
        enlace = {"url": url, "tipo": tipo, "rel": ruta_local(url, serie.get("base")),
                  "descripcion": descripcion, "anio": anio,
                  "trimestre": trimestre_de(propio, fila, nombre),
                  "categoria": categoria_de(propio, fila, nombre) or "contratos menores"}
        previo = enlaces.get(url)
        if previo is None or (propio and previo["descripcion"] == Path(nombre).stem):
            enlaces[url] = enlace          # el mismo fichero enlazado dos veces: el que tiene texto
    return enlaces, avisos


def _metadatos_enlace(enlace):
    return {"_anio": None if enlace["anio"] is None else str(enlace["anio"]),
            "_trimestre": enlace["trimestre"], "_periodo": enlace["descripcion"],
            "_categoria": enlace["categoria"]}


def descargar_enlace(url, destino, tipo):
    """Descarga un fichero enlazado. tipo 'tabla' o 'pdf': se valida el contenido
    (una página HTML servida con 200 es 'invalido'); 'desconocido': se acepta lo
    que no sea HTML y, si la ruta no tiene extensión, se le pone la del formato
    detectado. Devuelve (estado, detalle, ruta final, formato)."""
    destino = Path(destino)
    esperado = {"tabla": "xlsx", "pdf": "pdf"}.get(tipo)
    if tipo == "tabla" and destino.suffix.lower() == ".csv":
        esperado = "csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    try:
        estado, detalle = bajar(url, tmp, tipo=esperado)
        if estado != "ok":
            return estado, detalle, destino, None
        with open(tmp, "rb") as f:
            formato = formato_contenido(f.read(4096))
        if destino.suffix.lower() not in EXT_TABLA + EXT_DOCUMENTO:
            destino = destino.with_name(f"{destino.name}.{formato}")
        return guardar_version(destino, desde=tmp), "", destino, formato
    finally:
        if tmp.exists():
            tmp.unlink()


def _por_url(manifiesto, clave):
    """{url: (rel, entrada)} de los ficheros descargados de una serie."""
    return {e.get("url"): (rel, e) for rel, e in manifiesto.entradas(clave)}


def descargar_enlazados(clave, serie, raw, manifiesto, resumen, comprobar_todo=False, con_pdf=False,
                        excluir=None):
    """Descarga los ficheros que enlaza la página de una serie. Devuelve las URL
    enlazadas (para excluirlas de la serie en valenciano) o None si no se pudo
    leer la página. Con `excluir` solo se descargan los enlaces que no estén ahí.

    Los de años cerrados que ya se tienen solo se vuelven a pedir con
    --comprobar-todo. Lo que la página deja de enlazar (o da 404) queda como
    retirado; si la página no se puede leer o no enlaza ningún fichero de la
    serie, no se retira nada."""
    print(f"\n📦 {clave}: {serie['descripcion']}")
    anio_actual = ahora().year
    try:
        contenido = pedir_texto(serie["pagina"])
    except ErrorPortal as e:
        resumen.fallidos.append(f"{clave}: no se pudo leer {serie['pagina']} ({e}); no se retira nada")
        return None
    enlaces, avisos = extraer_enlaces(serie, contenido)
    resumen.avisos.extend(f"{clave}: {a}" for a in avisos)
    todos = set(enlaces)
    # Hojas de cálculo de la página (también las de la serie en castellano): un
    # PDF con el mismo nombre (o con "-1" detrás) repite su tabla
    tablas = {Path(e["rel"]).with_suffix("").as_posix().lower() for e in enlaces.values() if e["tipo"] != "pdf"}
    if not any(e["tipo"] != "pdf" for e in enlaces.values()):
        resumen.fallidos.append(f"{clave}: {serie['pagina']} no enlaza ningún fichero de la serie "
                                "(¿ha cambiado la página?); no se retira nada")
        return None
    if excluir is not None:
        enlaces = {u: e for u, e in enlaces.items() if u not in excluir}
    dir_serie = raw / clave
    inventario = {u: {k: e[k] for k in ("tipo", "rel", "descripcion", "anio", "trimestre", "categoria")}
                  for u, e in sorted(enlaces.items())}
    if guardar_json(dir_serie / "_enlaces.json", {"pagina": serie["pagina"], "enlaces": inventario}) != "sin_cambios" \
            or not (dir_serie / "_pagina.html").exists():
        guardar_version(dir_serie / "_pagina.html", contenido)
    descargados = _por_url(manifiesto, clave)
    omitidos_pdf, cerrados = 0, 0
    for url, enlace in sorted(enlaces.items(), key=lambda x: x[1]["rel"]):
        if enlace["tipo"] == "pdf" and not con_pdf:
            tronco = Path(enlace["rel"]).with_suffix("").as_posix().lower()
            if tronco in tablas or re.sub(r"[-_ ]\d$", "", tronco) in tablas:
                omitidos_pdf += 1
            else:
                resumen.avisos.append(f"{clave}: PDF sin hoja de cálculo del mismo nombre (no se descarga; "
                                      f"usa --con-pdf): {enlace['descripcion']} ({url})")
            continue
        previo = descargados.get(url)
        if (previo and not comprobar_todo and enlace["anio"] and enlace["anio"] < anio_actual - 1
                and previo[1].get("publicado", True) and (raw / previo[0]).exists()):
            cerrados += 1
            continue
        destino = raw / previo[0] if previo else dir_serie / enlace["rel"]
        estado, detalle, destino, formato = descargar_enlace(url, destino, enlace["tipo"])
        time.sleep(PAUSA)
        if estado in ESTADOS_OK:
            manifiesto.registrar(destino, url, estado, dataset=clave, formato=formato,
                                 tabla=formato in FORMATOS_TABLA, metadatos=_metadatos_enlace(enlace))
            resumen.descarga(f"{clave} {manifiesto.rel(destino)}", estado)
            print(f"  ✅ {enlace['rel']}: {estado}")
        elif estado == "no_existe":
            if previo and (raw / previo[0]).exists():
                manifiesto.retirar(raw / previo[0], f"la página lo enlaza pero da {detalle}")
                resumen.retirados.append(f"{clave} {previo[0]}: la página lo enlaza pero da {detalle}; "
                                         "se conservan sus filas")
                print(f"  🗑️ {enlace['rel']}: {detalle}")
            else:
                resumen.avisos.append(f"{clave}: enlace roto ({detalle}): {enlace['descripcion']} ({url})")
                print(f"  ⚠️ {enlace['rel']}: enlace roto ({detalle})")
        else:
            # Enlazado y no se puede bajar: se conserva la copia y se reintenta
            resumen.fallidos.append(f"{clave} {enlace['rel']}: {detalle or estado} ({url})")
            print(f"  ❌ {enlace['rel']}: {detalle or estado}")
    if omitidos_pdf:
        resumen.avisos.append(f"{clave}: {omitidos_pdf} PDF no descargados: repiten la hoja de cálculo del mismo "
                              "nombre (--con-pdf para bajarlos)")
    if cerrados:
        resumen.sin_cambios.append(f"{clave}: {cerrados} ficheros de años cerrados ya descargados "
                                   "(--comprobar-todo para volver a pedirlos)")
    for url, (rel, entrada) in descargados.items():
        if url not in enlaces and entrada.get("publicado", True) and (raw / rel).exists():
            motivo = ("ahora lo enlaza también la página en castellano" if url in todos
                      else f"{serie['pagina']} ya no lo enlaza")
            manifiesto.retirar(raw / rel, motivo)
            resumen.retirados.append(f"{clave} {rel}: {motivo}; se conservan sus filas")
            print(f"  🗑️ {rel}: {motivo}")
    return todos


# ============================================================================
# AJUNTAMENT DE VALÈNCIA: BUSCADOR DE CONTRATOS MENORES
# ============================================================================

class RespuestaInvalida(Exception):
    """El buscador respondió, pero no con el resultado de la consulta pedida."""


def _texto_celda(celda):
    texto = celda.get_text().strip()
    return texto or None


def tabla_buscador(contenido):
    """(cabecera, filas) de la tabla de resultados del buscador; cada fila es una
    lista de (texto, enlace) por celda. RespuestaInvalida si no hay tabla."""
    sopa = contenido if isinstance(contenido, BeautifulSoup) else BeautifulSoup(contenido, "html.parser")
    tabla = sopa.find("table", id="tablaContratos")
    if tabla is None:
        raise RespuestaInvalida("la respuesta no trae la tabla de resultados")
    cabeza = tabla.find("thead")
    cabecera = [_texto_celda(th) for th in cabeza.find_all("th")] if cabeza else []
    filas = []
    for tr in tabla.find_all("tr"):
        if tr.find_parent("table") is not tabla or tr.find_parent("thead") is not None:
            continue
        celdas = tr.find_all(["td", "th"], recursive=False)
        fila = []
        for celda in celdas:
            ancla = celda.find("a", href=True)
            fila.append((_texto_celda(celda), urljoin(VLC_PAGINA, ancla["href"]) if ancla else None))
        filas.append(fila)
    return cabecera, filas


def _num_contrato(enlace):
    for clave, valores in parse_qs(urlparse(enlace).query).items():
        if clave.endswith("numContrato") and valores:
            return valores[0]
    return None


def leer_html_buscador(ruta):
    """Resultados guardados de una consulta al buscador como DataFrame de texto:
    una columna por columna de la tabla ("Objeto del contrato" va dos veces: la
    segunda es el texto recortado del enlace), "<columna> (enlace)" con la URL de
    la ficha y _num_contrato (número de contrato de ese enlace)."""
    cabecera, filas = tabla_buscador(Path(ruta).read_bytes())
    ancho = max([len(cabecera)] + [len(f) for f in filas])
    nombres = _nombres_columnas(cabecera)
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    con_enlace = sorted({i for f in filas for i, (_, enlace) in enumerate(f) if enlace})
    registros = []
    for fila in filas:
        fila = fila + [(None, None)] * (ancho - len(fila))
        registro = {nombres[i]: texto for i, (texto, _) in enumerate(fila)}
        for i in con_enlace:
            registro[f"{nombres[i]} (enlace)"] = fila[i][1]
        numeros = [_num_contrato(fila[i][1]) for i in con_enlace if fila[i][1]]
        registro["_num_contrato"] = next((n for n in numeros if n), None)
        registros.append(registro)
    columnas = nombres + [f"{nombres[i]} (enlace)" for i in con_enlace] + ["_num_contrato"]
    return pd.DataFrame(registros, columns=columnas, dtype=object)


class Ventana:
    """Periodo de una consulta: de 'desde' a 'hasta' a las 00:00 (el buscador
    compara fecha y hora: de 'hasta' solo entran los grabados a las 00:00, que
    salen también en la ventana siguiente)."""

    def __init__(self, desde, hasta, nivel):
        self.desde, self.hasta, self.nivel = desde, hasta, nivel     # nivel: previas, anio, mes, dia

    def __repr__(self):
        return f"Ventana({self.desde}, {self.hasta}, {self.nivel})"

    @property
    def etiqueta(self):
        return {"anio": f"{self.desde:%Y}", "mes": f"{self.desde:%Y-%m}", "dia": f"{self.desde:%Y-%m-%d}"}.get(
            self.nivel, f"anteriores-{self.hasta:%Y}")

    def ruta(self, raw, estado):
        base = Path(raw) / VLC_SERIE / estado
        if self.nivel in ("anio", "previas"):
            return base / f"{self.etiqueta}.html"
        if self.nivel == "mes":
            return base / f"{self.desde:%Y}" / f"{self.etiqueta}.html"
        return base / f"{self.desde:%Y}" / f"{self.desde:%Y-%m}" / f"{self.etiqueta}.html"

    def ruta_troceada(self, raw, estado):
        return Path(raw) / VLC_SERIE / "_troceadas" / self.ruta(raw, estado).relative_to(Path(raw) / VLC_SERIE)

    def carpeta_hijas(self, raw, estado):
        """Carpeta de las consultas más pequeñas en que se trocea esta."""
        base = Path(raw) / VLC_SERIE / estado
        if self.nivel == "anio":
            return base / f"{self.desde:%Y}"
        if self.nivel == "mes":
            return base / f"{self.desde:%Y}" / f"{self.etiqueta}"
        return None

    def hijas(self, hoy):
        """Meses de un año o días de un mes (hasta hoy)."""
        if self.nivel == "anio":
            meses = [date(self.desde.year, m, 1) for m in range(1, 13)]
            return [Ventana(d, _mes_siguiente(d), "mes") for d in meses if d <= hoy]
        if self.nivel == "mes":
            dias, d = [], self.desde
            while d < self.hasta and d <= hoy:
                dias.append(Ventana(d, d + dt.timedelta(days=1), "dia"))
                d += dt.timedelta(days=1)
            return dias
        return []

    def metadatos(self, estado):
        return {"_anio": None if self.nivel == "previas" else f"{self.desde:%Y}",
                "_mes": f"{self.desde:%m}" if self.nivel in ("mes", "dia") else None,
                "_periodo": self.etiqueta, "_estado_consulta": estado,
                "_consulta_desde": self.desde.isoformat(), "_consulta_hasta": self.hasta.isoformat()}


def _mes_siguiente(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def ventanas_iniciales(granularidad, desde_anio, hasta_anio, hoy):
    """Consultas con las que se empieza un estado: una de control con todo lo
    anterior a --desde (solo si --desde no es posterior al primer año con
    datos) y después meses o años hasta hoy."""
    ventanas = []
    if desde_anio <= VLC_ANIO_INICIO:
        ventanas.append(Ventana(VLC_PREVIAS_DESDE, date(desde_anio, 1, 1), "previas"))
    for anio in range(desde_anio, hasta_anio + 1):
        if granularidad == "anio":
            if date(anio, 1, 1) <= hoy:
                ventanas.append(Ventana(date(anio, 1, 1), date(anio + 1, 1, 1), "anio"))
        else:
            ventanas.extend(Ventana(date(anio, m, 1), _mes_siguiente(date(anio, m, 1)), "mes")
                            for m in range(1, 13) if date(anio, m, 1) <= hoy)
    return ventanas


class Buscador:
    """Sesión con el buscador: cookie de la página y acción del formulario (con
    su p_auth). Si una respuesta no es válida se abre una sesión nueva."""

    def __init__(self):
        self.cookies = {}
        self.accion = None
        self.espacio = ""
        self.fecha_formulario = ""
        self.consultas = 0

    def abrir(self):
        respuesta = requests.get(VLC_PAGINA, headers=CABECERAS, timeout=TIMEOUT_API)
        codigo = respuesta.status_code
        if codigo in CODIGOS_REINTENTABLES:
            raise RespuestaInvalida(f"HTTP {codigo} al abrir el buscador")
        if codigo >= 400:
            raise ErrorPortal(f"HTTP {codigo} al abrir el buscador", codigo)
        sopa = BeautifulSoup(respuesta.content, "html.parser")
        formulario = sopa.find("form", action=re.compile(r"javax\.portlet\.action=buscarContrato"))
        if formulario is None:
            raise RespuestaInvalida("la página no trae el formulario del buscador")
        self.espacio = formulario.get("data-fm-namespace") or ""
        campo = formulario.find("input", attrs={"name": self.espacio + "formDate"})
        self.fecha_formulario = campo.get("value", "") if campo is not None else ""
        self.accion = urljoin(VLC_PAGINA, formulario["action"])
        self.cookies = dict(getattr(respuesta, "cookies", None) or {})

    def campos(self, estado, ventana):
        valores = [("formDate", self.fecha_formulario), ("objeto", ""), ("numExpediente", ""),
                   ("nifAdjudicatario", ""), ("importeMin", ""), ("importeMax", ""), ("selectTipo", "todos"),
                   ("selectEstado", estado), ("fechaInicio", f"{ventana.desde:%d/%m/%Y}"),
                   ("fechaFin", f"{ventana.hasta:%d/%m/%Y}"), ("maxResultados", str(VLC_MAX_FILAS))]
        return [(self.espacio + nombre, (None, valor)) for nombre, valor in valores]

    def consultar(self, estado, ventana):
        """(bytes de la respuesta, (cabecera, filas)) de una consulta válida."""
        detalle = ""
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                if self.accion is None:
                    self.abrir()
                respuesta = requests.post(self.accion, files=self.campos(estado, ventana), cookies=self.cookies,
                                          headers=CABECERAS, timeout=TIMEOUT_BUSCADOR)
                self.consultas += 1
                codigo = respuesta.status_code
                if codigo in CODIGOS_REINTENTABLES:
                    detalle = f"HTTP {codigo}"
                elif codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                else:
                    tabla = validar_respuesta(respuesta.content, estado, ventana)
                    self.cookies.update(dict(getattr(respuesta, "cookies", None) or {}))
                    return respuesta.content, tabla
            except ErrorPortal:
                raise
            except RespuestaInvalida as e:
                detalle = str(e)
                self.accion = None            # sesión nueva en el siguiente intento
            except ERRORES_RED as e:
                detalle = f"{type(e).__name__}: {str(e)[:150]}"
            except requests.exceptions.RequestException as e:
                raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
            finally:
                time.sleep(PAUSA_BUSCADOR)
            if intento < INTENTOS:
                time.sleep(_espera(intento, respuesta))
        raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def validar_respuesta(contenido, estado, ventana):
    """Comprueba que la respuesta es el resultado de la consulta pedida: sin el
    aviso de servicio no disponible, con las fechas y el estado pedidos en el
    formulario y con la tabla de resultados. Devuelve (cabecera, filas)."""
    aviso = re.search(rb'var\s+mostrarError\s*=\s*"(\w*)"', contenido)
    if aviso and aviso.group(1) == b"mostrar":
        raise RespuestaInvalida("el buscador avisa de que el servicio no está disponible")
    sopa = BeautifulSoup(contenido, "html.parser")
    for campo, esperado in (("fechaInicio", f"{ventana.desde:%d/%m/%Y}"), ("fechaFin", f"{ventana.hasta:%d/%m/%Y}")):
        entrada = sopa.find("input", attrs={"name": re.compile(rf"_{campo}$")})
        if entrada is None or entrada.get("value") != esperado:
            raise RespuestaInvalida(f"la respuesta no es la de la consulta ({campo}="
                                    f"{None if entrada is None else entrada.get('value')}, se pidió {esperado})")
    situacion = re.search(rb'var\s+situacion\s*=\s*"(\w*)"', contenido)
    if situacion and situacion.group(1).decode() != estado:
        raise RespuestaInvalida(f"la respuesta es del estado {situacion.group(1).decode()}, se pidió {estado}")
    return tabla_buscador(sopa)


def guardar_consulta(destino, contenido, tabla):
    """Guarda la respuesta de una consulta. Si su tabla es igual a la de la copia
    actual no se toca nada ('sin_cambios'): el resto de la página cambia en cada
    petición (p_auth, fecha del formulario, nodo) y no son datos."""
    destino = Path(destino)
    if destino.exists():
        try:
            if tabla_buscador(destino.read_bytes()) == tabla:
                return "sin_cambios"
        except RespuestaInvalida:
            pass
    return guardar_version(destino, contenido)


class ConsultasValencia:
    """Descarga del buscador: consultas por estado y ventana, troceadas cuando
    llegan al máximo de filas."""

    def __init__(self, raw, manifiesto, resumen, comprobar_todo=False):
        self.raw, self.manifiesto, self.resumen = Path(raw), manifiesto, resumen
        self.comprobar_todo = comprobar_todo
        self.buscador = Buscador()
        self.hoy = ahora().date()
        self.cuentas = dict.fromkeys(("nuevo", "actualizado", "sin_cambios", "cerradas", "troceadas", "filas"), 0)

    def _vigente(self, ruta):
        rel = self.manifiesto.rel(ruta)
        return ruta.exists() and self.manifiesto.get(rel).get("publicado", True)

    def _retirar_hijas(self, estado, ventana):
        carpeta = ventana.carpeta_hijas(self.raw, estado)
        if carpeta is None or not carpeta.is_dir():
            return
        for ruta in sorted(carpeta.rglob("*.html")):
            if HISTORICO in ruta.parts or not self._vigente(ruta):
                continue
            self.manifiesto.retirar(ruta, f"la consulta {ventana.etiqueta} ya no se trocea: sus filas están en "
                                          f"{self.manifiesto.rel(ventana.ruta(self.raw, estado))}")

    def procesar(self, estado, ventana):
        hoja = ventana.ruta(self.raw, estado)
        troceada = ventana.ruta_troceada(self.raw, estado)
        reciente = ventana.nivel != "previas" and ventana.desde.year >= self.hoy.year - 1
        if not self.comprobar_todo and not reciente:
            if self._vigente(hoja):
                self.cuentas["cerradas"] += 1
                return
            if troceada.exists() and self.manifiesto.get(self.manifiesto.rel(troceada)).get("troceada"):
                for hija in ventana.hijas(self.hoy):
                    self.procesar(estado, hija)
                return
        try:
            contenido, tabla = self.buscador.consultar(estado, ventana)
        except ErrorPortal as e:
            self.resumen.fallidos.append(f"{VLC_SERIE} {estado} {ventana.etiqueta}: {e}; no se retira nada")
            print(f"  ❌ {estado} {ventana.etiqueta}: {e}")
            return
        filas = len(tabla[1])
        if filas >= VLC_MAX_FILAS and ventana.hijas(self.hoy):
            # Respuesta cortada en el máximo: se guarda (original) y se pide por partes
            guardado = guardar_consulta(troceada, contenido, tabla)
            self.manifiesto.registrar(troceada, VLC_PAGINA, guardado, dataset=VLC_SERIE, tabla=False, troceada=True,
                                      filas=filas, metadatos=ventana.metadatos(estado))
            if self._vigente(hoja):
                self.manifiesto.retirar(hoja, f"la consulta llega a {VLC_MAX_FILAS} filas y se pide por partes")
            self.cuentas["troceadas"] += 1
            print(f"  ✂️ {estado} {ventana.etiqueta}: {filas} filas (máximo): se trocea")
            for hija in ventana.hijas(self.hoy):
                self.procesar(estado, hija)
            return
        if filas >= VLC_MAX_FILAS:
            self.resumen.fallidos.append(f"{VLC_SERIE} {estado} {ventana.etiqueta}: {filas} filas, el máximo del "
                                         "buscador, en una consulta que no se puede trocear: puede haber más")
        guardado = guardar_consulta(hoja, contenido, tabla)
        self.manifiesto.registrar(hoja, VLC_PAGINA, guardado, dataset=VLC_SERIE, tabla=True, filas=filas,
                                  metadatos=ventana.metadatos(estado))
        self._retirar_hijas(estado, ventana)
        self.cuentas[guardado] += 1
        self.cuentas["filas"] += filas
        if guardado != "sin_cambios":
            print(f"  ✅ {estado} {ventana.etiqueta}: {filas} filas ({guardado})")

    def ejecutar(self, desde_anio, hasta_anio):
        print(f"\n📦 {VLC_SERIE}: buscador de contratos menores del Ajuntament de València")
        for estado, granularidad in VLC_ESTADOS.items():
            for ventana in ventanas_iniciales(granularidad, desde_anio, hasta_anio, self.hoy):
                self.procesar(estado, ventana)
        c = self.cuentas
        self.resumen.descargados.append(
            f"{VLC_SERIE}: {self.buscador.consultas} consultas; {c['nuevo']} nuevas, {c['actualizado']} "
            f"actualizadas, {c['sin_cambios']} sin cambios, {c['troceadas']} troceadas por llegar a "
            f"{VLC_MAX_FILAS} filas; {c['filas']:,} filas en las consultas hechas")
        if c["cerradas"]:
            self.resumen.sin_cambios.append(f"{VLC_SERIE}: {c['cerradas']} consultas de años cerrados ya "
                                            "descargadas (--comprobar-todo para volver a pedirlas)")


# ============================================================================
# UPV: PAQUETES CKAN DE upvtransparent.upv.es
# ============================================================================

def descargar_upv(raw, manifiesto, resumen, comprobar_todo=False):
    """Recursos CSV de los paquetes de contratos menores de la UPV. Lo que un
    paquete deja de tener queda como retirado (solo si se pudo leer el paquete)."""
    print(f"\n📦 {UPV_SERIE}: CKAN de la Universitat Politècnica de València")
    anio_actual = ahora().year
    descargados = _por_url(manifiesto, UPV_SERIE)
    for paquete in UPV_PAQUETES:
        try:
            datos = pedir_json(UPV_CKAN, params={"id": paquete})
        except ErrorPortal as e:
            resumen.fallidos.append(f"{UPV_SERIE} {paquete}: no se pudo leer el paquete ({e}); no se retira nada")
            continue
        resultado = (datos or {}).get("result") if isinstance(datos, dict) else None
        if not isinstance(resultado, dict) or not datos.get("success", True):
            resumen.fallidos.append(f"{UPV_SERIE} {paquete}: respuesta de CKAN sin 'result'; no se retira nada")
            continue
        carpeta = raw / UPV_SERIE / paquete
        guardar_json(carpeta / "_paquete.json", resultado)
        recursos = [r for r in resultado.get("resources") or []
                    if (r.get("format") or "").strip().upper() == "CSV" and r.get("url")]
        if not recursos:
            resumen.fallidos.append(f"{UPV_SERIE} {paquete}: el paquete no tiene recursos CSV; no se retira nada")
            continue
        otros = [r.get("name") or r.get("url") for r in resultado.get("resources") or [] if r not in recursos]
        if otros:
            resumen.avisos.append(f"{UPV_SERIE} {paquete}: {len(otros)} recursos que no son CSV no se descargan "
                                  "(el XLSX anual repite el CSV; el diccionario de datos es una página)")
        vigentes = set()
        for recurso in recursos:
            url = normalizar_url(recurso["url"])
            vigentes.add(url)
            anio = _anio_unico(str(recurso.get("year") or "")) or _anio_unico(recurso.get("name") or "")
            metadatos = {"_anio": None if anio is None else str(anio), "_trimestre": None,
                         "_periodo": recurso.get("name"), "_categoria": "contratos menores"}
            previo = descargados.get(url)
            if (previo and not comprobar_todo and anio and anio < anio_actual - 1
                    and previo[1].get("publicado", True) and (raw / previo[0]).exists()):
                resumen.sin_cambios.append(f"{UPV_SERIE} {previo[0]} (ya descargado; --comprobar-todo para "
                                           "volver a pedirlo)")
                continue
            nombre = unquote(PurePosixPath(urlparse(url).path).name) or f"{recurso.get('id')}.csv"
            destino = raw / previo[0] if previo else carpeta / nombre
            estado, detalle = descargar(url, destino, tipo="csv")
            time.sleep(PAUSA)
            if estado in ESTADOS_OK:
                manifiesto.registrar(destino, url, estado, dataset=UPV_SERIE, paquete=paquete, formato="csv",
                                     tabla=True, metadatos=metadatos)
                resumen.descarga(f"{UPV_SERIE} {manifiesto.rel(destino)}", estado)
                print(f"  ✅ {paquete}/{nombre}: {estado}")
            elif estado == "no_existe" and not previo:
                resumen.avisos.append(f"{UPV_SERIE} {paquete}: recurso roto ({detalle}): {url}")
            elif estado == "no_existe":
                manifiesto.retirar(raw / previo[0], f"el recurso da {detalle}")
                resumen.retirados.append(f"{UPV_SERIE} {previo[0]}: el recurso da {detalle}; se conservan sus filas")
            else:
                resumen.fallidos.append(f"{UPV_SERIE} {paquete}/{nombre}: {detalle or estado} ({url})")
                print(f"  ❌ {paquete}/{nombre}: {detalle or estado}")
        for url, (rel, entrada) in descargados.items():
            if entrada.get("paquete") == paquete and url not in vigentes and entrada.get("publicado", True):
                manifiesto.retirar(raw / rel, f"el paquete {paquete} ya no lo tiene")
                resumen.retirados.append(f"{UPV_SERIE} {rel}: el paquete ya no lo tiene; se conservan sus filas")
                print(f"  🗑️ {rel}: retirado del paquete")


# ============================================================================
# EJECUCIÓN
# ============================================================================

def descargar_todo(raw, manifiesto, resumen, fuentes, desde, hasta, comprobar_todo=False, con_pdf=False):
    enlazadas = {}
    for fuente in fuentes:
        if fuente == "valencia":
            ConsultasValencia(raw, manifiesto, resumen, comprobar_todo).ejecutar(desde, hasta)
            continue
        if fuente == "upv":
            descargar_upv(raw, manifiesto, resumen, comprobar_todo)
            continue
        for clave in FUENTES[fuente]:
            serie = SERIES_PAGINA[clave]
            excluir = None
            if serie.get("principal"):
                excluir = enlazadas.get(serie["principal"])
                if excluir is None:
                    resumen.fallidos.append(f"{clave}: sin la página en castellano no se puede saber qué ficheros "
                                            "enlaza solo la valenciana; no se descarga ni se retira nada")
                    continue
            urls = descargar_enlazados(clave, serie, raw, manifiesto, resumen, comprobar_todo, con_pdf, excluir)
            if urls is not None:
                enlazadas[clave] = urls


def generar_parquets(salida, raw, manifiesto, resumen, fuentes):
    print("\n🧱 Generando Parquet...")
    for fuente in fuentes:
        for clave in FUENTES[fuente]:
            ficheros = []
            for rel, entrada in manifiesto.entradas(clave):
                if not entrada.get("tabla") or not (raw / rel).exists():
                    continue
                metadatos = {"_fuente": entrada.get("url"), "_dataset": clave, **(entrada.get("metadatos") or {}),
                             "_archivo_origen": rel}
                ficheros.append((raw / rel, rel, metadatos))
            destino = salida / f"{clave}.parquet"
            if not ficheros and not destino.exists():
                continue
            df = construir_parquet(destino, ficheros, raw, manifiesto, resumen)
            if df is not None:
                vigentes = df[df["_en_ultima_descarga"].astype(bool)]
                anios = vigentes["_anio"].fillna("sin año").astype(str).value_counts().sort_index() \
                    if "_anio" in vigentes else pd.Series(dtype=int)
                resumen.estadisticas.append(f"{clave}: {len(vigentes):,} filas; "
                                            + ", ".join(f"{a}: {n:,}" for a, n in anios.items()))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Descarga los contratos menores valencianos que no están en la "
                                                 "PLACSP ni en el REGCON")
    parser.add_argument("--salida", type=Path, default=SALIDA, help=f"carpeta de salida (por defecto {SALIDA})")
    parser.add_argument("--fuentes", default=",".join(FUENTES),
                        help=f"fuentes separadas por comas (por defecto todas: {','.join(FUENTES)})")
    parser.add_argument("--desde", type=int, default=VLC_ANIO_INICIO,
                        help=f"buscador de València: primer año que se consulta (por defecto {VLC_ANIO_INICIO})")
    parser.add_argument("--hasta", type=int, default=None,
                        help="buscador de València: último año que se consulta (por defecto el actual)")
    parser.add_argument("--solo-descarga", action="store_true", help="no generar los Parquet")
    parser.add_argument("--solo-parquet", action="store_true", help="no descargar; solo generar los Parquet")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también lo de años cerrados ya descargado")
    parser.add_argument("--con-pdf", action="store_true", help="descargar también los PDF enlazados")
    args = parser.parse_args(argv)

    fuentes = [f.strip() for f in args.fuentes.split(",") if f.strip()]
    desconocidas = [f for f in fuentes if f not in FUENTES]
    if desconocidas:
        parser.error(f"fuentes desconocidas: {', '.join(desconocidas)} (hay: {', '.join(FUENTES)})")
    salida = Path(args.salida)
    raw = salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    hasta = args.hasta or ahora().year
    print("=" * 70)
    print(TITULO)
    print("=" * 70)
    print(f"Fuentes: {', '.join(fuentes)}\nDestino: {salida.resolve()}")
    manifiesto = Manifiesto(raw)
    resumen = Resumen(TITULO)
    if not args.solo_parquet:
        descargar_todo(raw, manifiesto, resumen, fuentes, args.desde, hasta, args.comprobar_todo, args.con_pdf)
    if not args.solo_descarga:
        generar_parquets(salida, raw, manifiesto, resumen, fuentes)
    return resumen.cerrar(raw)


if __name__ == "__main__":
    sys.exit(main())
