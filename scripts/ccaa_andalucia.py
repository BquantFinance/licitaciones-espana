#!/usr/bin/env python3
"""
===========================================================================
Scraper de licitaciones de la Junta de Andalucia
===========================================================================

Extrae licitaciones regulares y contratos menores desde el proxy
Elasticsearch del portal de perfiles del contratante.

Uso:
  python scripts/ccaa_andalucia.py scrape-std    Licitaciones regulares
  python scripts/ccaa_andalucia.py scrape-men    Contratos menores
  python scripts/ccaa_andalucia.py scrape        Las dos
  python scripts/ccaa_andalucia.py procesar      Solo regenera las salidas desde raw/ (sin red)
Opciones: --salida DIR (por defecto ccaa_Andalucia/), --perfil CODIGO y --anio AAAA
(descarga parcial: un perfil del contratante o el ano del numero de expediente, como la
dimension de particion), --semilla PARQUET (repetible) y --origen-semilla TEXTO.

Salida (en --salida):
  raw/<alcance>.jsonl.gz           cada descarga tal como la sirve el portal: una linea de
                                   cabecera (alcance, recuentos y consultas incompletas) y
                                   despues un _source por linea, en el orden de descarga.
                                   <alcance> es std o menores, con __perfil-<codigo> y
                                   __anio-<aaaa> en las descargas parciales
  raw/_historico/                  versiones anteriores de cada descarga (nunca se borran)
  raw/_en_curso/<alcance>/         bloques ya descargados de una ejecucion sin terminar
  licitaciones_andalucia.parquet   todas las filas acumuladas (+ _historico/)
  licitaciones_{std,menores,all}.csv  la misma tabla, partida por codigo_procedimiento
  perfiles_cache.json, scraper.log

Sesgo del superviviente (docs/PRINCIPIOS.md, regla 3; comun/historico.py)
---------------------------------------------------------------------------
- Capa cruda: toda descarga pasa por guardar_version. Si el contenido no cambio no se
  toca nada (se compara sin comprimir) y si cambio la version anterior va a
  raw/_historico/.
- Salidas: se construyen con el codigo actual desde la salida anterior y las versiones de
  raw/ que aun no tiene (la fecha de la ultima incorporada de cada alcance va en los
  metadatos del parquet), en orden, con acumular(): lo que el portal retira o cambia
  sigue en la tabla con _en_ultima_descarga=False. Con una sola descarga la tabla es la de
  antes mas _primera_descarga, _ultima_descarga y _en_ultima_descarga. El parquet anterior
  pasa a _historico/ (guardar_version); los CSV se sustituyen: tienen los mismos datos.
- Ambito de cada descarga (lo que se da por releido y puede quedar retirado): las filas
  cuyo registro (portalGestor, idExpediente) vuelve en ella y las que SEGURO caen dentro
  de su alcance y no PUEDEN caer en ninguna consulta incompleta: tope de 10.000 aun con
  las 8 dimensiones y los tramos de id (o el multi-sort), paginacion que se corta tras
  los reintentos, rama sin valor omitida por MAX_EXCLUSIONS o recuentos que no cubren el
  total. Se decide con los valores de la fila (_Coincidencias, con los 'match' de las
  dimensiones y los 'range' de idExpediente de los tramos).
  Asi una descarga parcial (--perfil, --anio, scrape-std o scrape-men solos) no retira
  nada fuera de su alcance. Una descarga vacia no se guarda y una que falla (el portal da
  error tras los reintentos) tampoco: no retiran nada y la ejecucion termina con codigo 1.
- Reanudable: cada bloque de primer nivel (un procedimiento en std, un tipo de contrato en
  menores) se guarda al terminar en raw/_en_curso/<alcance>/. Si la ejecucion se corta,
  la siguiente con el mismo alcance sigue desde el primer bloque que falta (para empezar
  de cero, borrar esa carpeta).
- Sin cambios en el portal no se escribe nada: la descarga queda sin_cambios y las salidas
  no se regeneran.
- Limite conocido: una descarga no es una foto instantanea. Si un expediente cambia de
  valor en una dimension de particion (p. ej. de ADJ a RES) mientras se descarga y pasa a
  una rama ya recorrida, esa descarga no lo trae y su version anterior queda como
  retirada hasta la siguiente.
- Memoria, medida con pandas 3.0.6 y la capa cruda real del 29-sep-2026 (900.929
  expedientes): 4,1 GB al generar la salida desde las dos descargas, 5,5 GB al incorporar
  una descarga nueva sobre ella y 3,5 GB al sembrar el publicado v2026.02 (el VPS da 10 GB).

Registro: (portalGestor, idExpediente), no el idExpediente solo
---------------------------------------------------------------
El indice junta dos numeraciones de idExpediente que se solapan (ids 4.402-13.890 y
400.000-425.471, de 2021-2022): la del gestor de expedientes (portalGestor=true) y la
anterior (false; el SAS con n.o '+6.…' y la Junta con 'CONTR …'). portalGestor viene en
todos los documentos (medido el 2026-09-29: 554.489 true + 370.648 false = los 925.137
sin BRR) y el _id del indice es el id en la primera y el id con 12 cifras en la segunda
('425471' / '000000425471'; 400 de 400 muestras): la pareja es el registro. Antes se
deduplicaba por idExpediente: la primera descarga del VPS (29-sep) descarto sin avisar
18.453 menores y 34 licitaciones (todas en esos rangos; dos hojas quedaron como
«paginacion incompleta» por lo mismo) y dio por retiradas 5.030 licitaciones al
incorporar los menores (el mismo id, en la otra numeracion). Ahora se deduplica por la
pareja al paginar, al juntar consultas y al reanudar (clave_documento), y en la
salida se da por releida una fila solo si vuelve su pareja (_claves_tabla, que lee
portalGestor de campos_extra_json: no hay columna propia para no cambiar las de siempre).
Las paginas se piden ordenadas por (idExpediente, portalGestor): un orden total.

Consultas que no caben en la ventana de 10.000
----------------------------------------------
- Una consulta de hasta 10.000 documentos se pagina y, si no llegan todos (el indice
  cambia mientras se pagina), se repite hasta REINTENTOS_PAGINACION veces (en orden
  inverso y luego en el mismo), juntando lo nuevo; si aun faltan, queda incompleta.
- Si tras las 8 dimensiones una consulta sigue por encima de 10.000 (los menores de
  suministros del SAS sin tramitacion, forma de presentacion ni ano en el n.o: 4.901
  documentos sin descargar el 29-sep), se parte por tramos de idExpediente ('range':
  el proxy lo admite, medido el 2026-09-29) contando cada mitad hasta que cabe; como
  mucho hay dos documentos por id. Si el proxy rechazara 'range' (HTTP 400) o no lo
  aplicara (las dos mitades con el total), se vuelve al multi-sort de antes, que puede
  quedarse corto (tope).

Columnas planas de la adjudicacion (sin cambios: las lee asi el ETL de la web)
-------------------------------------------------------------------------------
adjudicatario_nif, importe_adjudicacion e importe_adjudicacion_iva son la PRIMERA
adjudicacion de primer nivel tal como la sirve el portal, sea cual sea su resultado
(codigoResultado AWARD, NOAWA -desierta-, RESIGN, MISES: una no adjudicada suele traer
0 o el presupuesto) y sin mirar los lotes: en un expediente con lotes estan vacias (sus
adjudicaciones van en lotes_json[].adjudicacion). todos_adjudicatarios_nif junta los NIF
de primer nivel. Todas las adjudicaciones, con su lote, resultado, fechas y copia de
formalizacion, van completas en adjudicaciones_json y lotes_json: para sumar lo
adjudicado hay que leerlas (lo hace el ETL de la web).
fecha_publicacion es el fechaPublicacion del indice, que a menudo es una publicacion
posterior: la primera es anuncio_primera_fecha. url_detalle lleva solo el idExpediente,
que en los ids compartidos no dice de que numeracion es.

Semilla (--semilla; docs/PRINCIPIOS.md, regla 4)
--------------------------------------------------
El publicado v2026.02 (andalucia.zip, licitaciones_andalucia.parquet) no trae
portalGestor, y su id_expediente es unico porque el codigo que lo genero tambien
deduplicaba por el id. Una fila de la semilla ya esta en la salida (que incluye lo
retirado y las semillas anteriores; _semilla_presente) si alguna fila tiene su
id_expediente, salvo en los ids que comparten las dos numeraciones (IDS_COMPARTIDOS): ahi,
si coincide su registro (con portalGestor: una salida de este script), su id y n.o de
expediente o la fila entera (clave presente), su id, perfil, titulo e importe (contenido
presente: el n.o cambia a veces, y el publicado dejo 'nan' donde el portal pone 'N/A'), o
si la salida tiene ese id en las dos numeraciones. Primero se mira si esta y despues el
ambito: de las que faltan solo se anaden las que una descarga de raw/ que cubria su
alcance ya no trae (el mismo ambito que al retirar; una descarga del codigo anterior no
cubre los ids compartidos), marcadas con _origen='release v2026.02' y
_en_ultima_descarga=False; las demas se cuentan como fuera del ambito (antes se contaban
asi tambien las presentes: el 29-sep, 95.258 de las que 95.255 estaban). Asi entra un
expediente del publicado que la descarga no tiene aunque otro de la otra numeracion ocupe
su id. Nunca se modifica ni se duplica una fila descargada. Una salida de este script
como semilla necesita --origen-semilla.
Errores conocidos del publicado v2026.02 (el codigo de 5cd4854 escribia CSV y el parquet
se hizo aparte desde el CSV):
- Los vacios estan como el texto 'nan' (788.676 filas en todos_adjudicatarios_nif,
  769.033 en fecha_limite_presentacion, 505.176 en codigo_dir3...): en su CSV son celdas
  vacias y ninguna tiene el texto 'nan'. Al sembrar se leen como ''.
- num_adjudicaciones y num_anuncios son decimales y estan vacios sin adjudicaciones o sin
  anuncios (10.619 y 1 filas): al sembrar, 0 y enteros, como en el codigo actual.
- importe_adjudicacion_iva esta vacio en todas las filas (tambien en el CSV): el portal
  no servia importeAdjudicacionConIva.
- Sin adjudicaciones_json, lotes_json, anuncios_json ni campos_extra_json: faltan las
  adjudicaciones 2a y siguientes (19.765 expedientes tienen varias) y el resto de campos.
- Faltan unos 41K menores del SAS por encima del tope de 10.000 (segmentos PARTIAL de su
  scraper.log) y no trae universidades, diputaciones ni ayuntamientos (0 filas), aunque
  el README los cite.
- Deduplicaba por idExpediente: de cada id compartido por las dos numeraciones solo
  tiene uno de los dos expedientes.
"""

import argparse
import gzip
import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests import RequestException

BASE = "https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico"
ES_URL = f"{BASE}/elastic/sirec_pdc_expedientes/_search?pretty"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "ccaa_Andalucia"
DATA_DIR.mkdir(exist_ok=True)
PERFILES_CACHE_PATH = DATA_DIR / "perfiles_cache.json"

sys.path.insert(0, str(ROOT_DIR))
from comun.historico import (  # noqa: E402
    ANADIDA,
    FUERA_AMBITO,
    HISTORICO,
    IGNORAR_POR_DEFECTO,
    ORIGEN_SEMILLA,
    PRESENTE_CLAVE,
    PRESENTE_CONTENIDO,
    acumular,
    guardar_version,
    imprimir_informe_semilla,
    informe_semilla,
    versiones,
)

# pandas es obligatorio (requirements.txt): lo usa la capa de historico
HAS_PANDAS = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "scraper.log", encoding="utf-8", delay=True),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DELAY = 0.3
PAGE_SIZE = 100
MAX_FROM = 9900
MAX_RETRIES = 3
# Maximo de clausulas must_not por consulta (ramas null y descubrimiento de perfiles)
MAX_EXCLUSIONS = 900
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
# Orden de las paginas: (idExpediente, portalGestor) es el registro del indice (unico), asi
# que el orden es total y from/size no repite ni salta documentos mientras el indice no cambia
ORDEN_PAGINAS = [{"idExpediente": "asc"}, {"portalGestor": "asc"}]
ORDEN_PAGINAS_INVERSO = [{"idExpediente": "desc"}, {"portalGestor": "desc"}]
# Veces que se repite la paginacion de una consulta a la que le faltan documentos (primero en
# orden inverso y luego en el mismo) y pausa antes de cada repeticion, en segundos
REINTENTOS_PAGINACION = 2
PAUSA_REINTENTO = 5
COUNT_TIMEOUT = 60
DEFAULT_TIMEOUT = 90
INTEGER_DEFAULTS = {
    "num_adjudicaciones": 0,
    "num_lotes": 0,
    "num_anuncios": 0,
}
AMOUNT_COLS = [
    "importe_licitacion",
    "valor_estimado",
    "importe_adjudicacion",
    "importe_adjudicacion_iva",
]

# Capa cruda y salidas (dentro de DATA_DIR, que cambia con --salida)
CRUDO = "raw"
EN_CURSO = "_en_curso"
FORMATO_CRUDO = "ccaa_andalucia/crudo-1"
PARQUET_SALIDA = "licitaciones_andalucia.parquet"
CSV_STD = "licitaciones_std.csv"
CSV_MENORES = "licitaciones_menores.csv"
CSV_TODO = "licitaciones_all.csv"
# Metadatos del parquet con la fecha de la ultima version incorporada de cada alcance
CLAVE_METADATOS = b"ccaa_andalucia"
# Columnas temporales con las que se pasa a acumular() el ambito de cada descarga y el orden
# de las filas nuevas cuando se acumula por trozos de FILAS_POR_TROZO filas
COLUMNA_AMBITO = "_ambito_descarga"
COLUMNA_POSICION = "_posicion_descarga"
FILAS_POR_TROZO = 200_000
# Columnas de la semilla con que se decide si una fila ya esta en la salida (_semilla_presente)
COLUMNAS_PRESENCIA = ["id_expediente", "numero_expediente", "codigo_perfil", "titulo", "importe_licitacion",
                      "campos_extra_json"]
# Registro con que deduplica la descarga: va en la cabecera de la capa cruda. Una cabecera sin el
# (codigo anterior, que deduplicaba por idExpediente) no da por releidos los ids que comparten las
# dos numeraciones (IDS_COMPARTIDOS, medido el 2026-09-29: el gestor solo tiene ids 4.402-13.890 y
# desde 400.000; la numeracion anterior, hasta 425.471 y 100000000001): pudo perder alli, sin
# anotarlo, un expediente cuyo id ya habia visto en la otra numeracion.
REGISTRO = ["portalGestor", "idExpediente"]
IDS_COMPARTIDOS = ((4402, 13890), (400000, 425471))
CONSULTAS = ("std", "menores")
# Columnas con pocos valores distintos (se comparte cada cadena al construir la tabla)
COLUMNAS_REPETIDAS = {
    "tipo_contrato", "tipo_contrato_codigo", "organo_contratacion", "codigo_perfil", "codigo_dir3", "estado",
    "estado_codigo", "fecha_publicacion", "fecha_limite_presentacion", "anuncio_primera_fecha",
    "anuncio_ultima_fecha", "adjudicatario_nif", "codigo_procedimiento", "codigo_tramitacion", "codigo_normativa",
    "forma_presentacion", "cofinanciado_ue", "subasta_electronica", "sistema_racionalizacion", "cpv",
    "provincias_ejecucion", "medios_publicacion",
}

S = requests.Session()
S.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.juntadeandalucia.es",
        "Referer": f"{BASE}/perfiles-licitaciones/buscador-general",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
)


class ScraperError(RuntimeError):
    """Error operativo del scraper de Andalucia."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


CSV_COLS = [
    "id_expediente",
    "numero_expediente",
    "titulo",
    "tipo_contrato",
    "tipo_contrato_codigo",
    "organo_contratacion",
    "codigo_perfil",
    "codigo_dir3",
    "estado",
    "estado_codigo",
    "importe_licitacion",
    "valor_estimado",
    "importe_adjudicacion",
    "importe_adjudicacion_iva",
    "fecha_publicacion",
    "fecha_limite_presentacion",
    "anuncio_primera_fecha",
    "anuncio_ultima_fecha",
    "adjudicatario_nif",
    "todos_adjudicatarios_nif",
    "num_adjudicaciones",
    "codigo_procedimiento",
    "codigo_tramitacion",
    "codigo_normativa",
    "forma_presentacion",
    "cofinanciado_ue",
    "subasta_electronica",
    "sistema_racionalizacion",
    "cpv",
    "provincias_ejecucion",
    "medios_publicacion",
    "num_lotes",
    "num_anuncios",
    "url_detalle",
    "adjudicaciones_json",
    "lotes_json",
    "anuncios_json",
    "campos_extra_json",
]

# Campos del _source que flatten() lleva a columnas propias (y los subcampos que aplana)
FLAT_SOURCE_FIELDS = {
    "idExpediente": None,
    "numeroExpediente": None,
    "titulo": None,
    "tipoContrato": {"codigo", "descripcion"},
    "perfilContratante": {"codigo", "descripcion", "codigoDir3"},
    "estado": {"codigo", "nombre"},
    "importeLicitacion": None,
    "valorEstimado": None,
    "fechaPublicacion": None,
    "fechaLimitePresentacion": None,
    "codigoProcedimiento": None,
    "codigoTipoTramitacion": None,
    "codigoNormativa": None,
    "formaPresentacion": None,
    "cofinanciadoUE": None,
    "subastaElectronica": None,
    "sistemaRacionalizacion": None,
    "codigosCpv": None,
    "provinciasEjecucion": None,
    "mediosPublicacion": {"codigo"},
}
# Listas que flatten() resume (primera adjudicacion, numero de lotes, fechas de anuncios):
# se guardan tambien completas, tal como las sirve el portal
JSON_LIST_FIELDS = {
    "adjudicaciones": "adjudicaciones_json",
    "lotes": "lotes_json",
    "anuncios": "anuncios_json",
}

PROCS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
# Los valores que no estan en estas listas solo se recuperan por la rama "null" de cada
# dimension, que agrupa todo lo demas y puede superar los 10k (multi-sort parcial). Se
# anaden los codigos que aparecen en licitaciones_andalucia.parquet y faltaban.
TIPOS = [
    "SERV",
    "SUM",
    "OBR",
    "PRIV",
    "PATR",
    "ESP",
    "COL",
    "CSER",
    "GEST",
    "CONS",
    "MIX",
    "ACON",
    "CA",
    "OBRA",
    "ADMESP",
    "ARRED",
    "PAT",
    "GESSERVPUB",
    "CONOBRPUB",
    "COLABPUBPR",
    "CONSERV",
    "CONOBR",
    "CMIN",
]
ESTADOS = [
    "RES", "PUB", "ADJ", "EVA", "ANU", "DES", "PRE", "FOR", "REN", "PEN", "CER", "REV", "ABD", "PAA",
    "ANUL", "AP", "C", "CERR", "E", "PUBANUL", "SUS",
]
TRAMS = ["O", "U", "E", "S", "N"]
PROVS = ["04", "11", "14", "18", "21", "23", "29", "41", "51", "52", "98", "99", "00"]
FPS = ["E", "P", "M", "N", "S", "O", "A"]
# Hasta el ano siguiente al actual (antes acababa en 2026 fijo) y desde 2000: hay
# expedientes numerados con 2015-2017 (y anteriores) que iban todos a la rama null
YEARS = [str(year) for year in range(2000, date.today().year + 2)]

SORT_COMBOS = [
    [{"idExpediente": "asc"}],
    [{"idExpediente": "desc"}],
    [{"importeLicitacion": "asc"}],
    [{"importeLicitacion": "desc"}],
    [{"numeroExpediente": "asc"}],
    [{"numeroExpediente": "desc"}],
    [{"titulo": "asc"}],
    [{"titulo": "desc"}],
    [{"fechaLimitePresentacion": "asc"}],
    [{"fechaLimitePresentacion": "desc"}],
    [{"adjudicaciones.importeAdjudicacion": "asc"}],
    [{"adjudicaciones.importeAdjudicacion": "desc"}],
    # Mas ventanas de 10k para los bloques que siguen incompletos (el portal ya ordena
    # por fechaPublicacion en get_perfiles)
    [{"fechaPublicacion": "asc"}],
    [{"fechaPublicacion": "desc"}],
]

DIMS = [
    ("tipoContrato.codigo", TIPOS),
    ("estado.codigo", ESTADOS),
    ("codigoTipoTramitacion", TRAMS),
    ("perfilContratante.codigo", "PERFILES"),
    ("provinciasEjecucion", PROVS),
    ("formaPresentacion", FPS),
    ("numeroExpediente", YEARS),
]

# Campo del indice -> columna de la tabla, para decidir con los valores de una fila si
# cae dentro de una consulta 'match' (_Coincidencias)
CAMPOS_COLUMNA = {
    "idExpediente": "id_expediente",
    "codigoProcedimiento": "codigo_procedimiento",
    "tipoContrato.codigo": "tipo_contrato_codigo",
    "estado.codigo": "estado_codigo",
    "codigoTipoTramitacion": "codigo_tramitacion",
    "perfilContratante.codigo": "codigo_perfil",
    "provinciasEjecucion": "provincias_ejecucion",
    "formaPresentacion": "forma_presentacion",
    "numeroExpediente": "numero_expediente",
}
# provinciasEjecucion es una lista en el indice y va unida con ';' en la tabla
CAMPOS_MULTIVALOR = {"provinciasEjecucion"}
# numeroExpediente es texto analizado: la dimension de ano casa '2024' con
# 'CONTR 2024 0000347060' (lo muestra el scraper.log del publicado)
CAMPOS_TEXTO = {"numeroExpediente"}
_PALABRA = re.compile(r"[^\W_]+")

_PERFILES = None


def init():
    try:
        response = S.get(f"{BASE}/perfiles-licitaciones/licitaciones-publicadas", timeout=20)
        response.raise_for_status()
    except RequestException as exc:
        raise ScraperError("No se pudo inicializar la sesion con el portal de Andalucia") from exc


def build_query(must=None, must_not=None, *, size=PAGE_SIZE, sort=None, offset=None, track_total_hits=True):
    query = {"query": {"bool": {}}, "size": size, "track_total_hits": track_total_hits}
    if must:
        query["query"]["bool"]["must"] = must
    if must_not:
        query["query"]["bool"]["must_not"] = must_not
    if sort is not None:
        query["sort"] = sort
    if offset is not None:
        query["from"] = offset
    return query


def es(body, timeout=DEFAULT_TIMEOUT):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = S.post(ES_URL, json=body, timeout=timeout)
            if response.ok:
                try:
                    return response.json()
                except ValueError as exc:
                    last_error = exc
                    log.warning("Respuesta JSON invalida en intento %s/%s", attempt, MAX_RETRIES)
            elif response.status_code in RETRYABLE_STATUS_CODES:
                last_error = ScraperError(
                    f"HTTP {response.status_code} consultando Andalucia API"
                )
                log.warning(
                    "Respuesta reintentable HTTP %s en intento %s/%s",
                    response.status_code,
                    attempt,
                    MAX_RETRIES,
                )
            else:
                body_preview = response.text[:200].replace("\n", " ").strip()
                raise ScraperError(
                    f"HTTP no reintentable {response.status_code} consultando Andalucia API: {body_preview}",
                    status_code=response.status_code,
                )
        except RequestException as exc:
            last_error = exc
            log.warning("Fallo de red en intento %s/%s: %s", attempt, MAX_RETRIES, exc)

        if attempt < MAX_RETRIES:
            time.sleep(2)

    raise ScraperError("La API de Andalucia fallo tras varios reintentos") from last_error


def cnt(must=None, must_not=None):
    data = es(build_query(must=must, must_not=must_not, size=0), timeout=COUNT_TIMEOUT)
    total = data.get("hits", {}).get("total", {})
    return total.get("value", 0) if isinstance(total, dict) else total


def mm(field, value):
    return {"match": {field: value}}


def mn(field, value):
    if isinstance(value, str):
        return {"match": {field: {"query": value}}}
    return {"match": {field: value}}


def _perfil_code(hit):
    perfil = hit.get("_source", {}).get("perfilContratante", {})
    if isinstance(perfil, dict) and perfil.get("codigo"):
        return str(perfil["codigo"])
    return None


def complete_perfiles(perfiles):
    """Anade los perfiles que no salen en las ventanas de ordenacion.

    Esas ventanas solo ven ~40k expedientes (en el parquet publicado darian 368 de 505
    perfiles) y una cache antigua no incluye los perfiles nuevos: sus expedientes solo se
    recuperaban por la rama null, que puede superar los 10k. Se piden expedientes cuyo
    perfil no esta en la lista hasta que no aparece ninguno nuevo.
    """
    while len(perfiles) < MAX_EXCLUSIONS:
        must_not = [mn("perfilContratante.codigo", code) for code in sorted(perfiles)]
        new_codes = set()
        for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
            data = es(
                build_query(
                    must_not=must_not,
                    size=PAGE_SIZE,
                    sort=[{"idExpediente": "asc"}],
                    offset=offset,
                )
            )
            hits = data.get("hits", {}).get("hits", [])
            new_codes.update(code for code in map(_perfil_code, hits) if code and code not in perfiles)
            if new_codes or len(hits) < PAGE_SIZE:
                break
            time.sleep(0.1)
        if not new_codes:
            break
        perfiles.update(new_codes)
        log.info("  +%s perfiles fuera de las ventanas: %s codes", len(new_codes), len(perfiles))
    return perfiles


def get_perfiles():
    global _PERFILES
    if _PERFILES:
        return _PERFILES

    perfiles = set()
    if PERFILES_CACHE_PATH.exists():
        try:
            cached = json.loads(PERFILES_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(cached, list) and cached:
                perfiles = {str(value) for value in cached if value}
                log.info("Loaded %s perfil codes from cache", len(perfiles))
        except (OSError, ValueError) as exc:
            log.warning("No se pudo leer la cache de perfiles: %s", exc)

    if not perfiles:
        log.info("Discovering perfil codes...")
        discovery_sorts = [
            ("idExpediente", "asc"),
            ("idExpediente", "desc"),
            ("fechaPublicacion", "asc"),
            ("fechaPublicacion", "desc"),
        ]

        for sort_field, sort_order in discovery_sorts:
            for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
                data = es(
                    build_query(
                        size=PAGE_SIZE,
                        sort=[{sort_field: sort_order}],
                        offset=offset,
                    )
                )
                hits = data.get("hits", {}).get("hits", [])
                if not hits:
                    break
                perfiles.update(code for code in map(_perfil_code, hits) if code)
                time.sleep(0.1)
            log.info("  %s %s: %s codes", sort_field, sort_order, len(perfiles))

    complete_perfiles(perfiles)
    _PERFILES = sorted(perfiles)
    try:
        # La lista anterior no se pierde: si cambia pasa a _historico/
        guardar_version(
            PERFILES_CACHE_PATH,
            json.dumps(_PERFILES, ensure_ascii=False, indent=2).encode("utf-8"),
        )
    except OSError as exc:
        log.warning("No se pudo guardar la cache de perfiles: %s", exc)
    log.info("  Total: %s perfil codes", len(_PERFILES))
    return _PERFILES


def build_unknown_standard_exclusions(base_must_not):
    exclusions = list(base_must_not)
    for proc in PROCS:
        if proc == 9:
            continue
        exclusions.append(mn("codigoProcedimiento", proc))
    return exclusions


def extract(data):
    """Documentos de una respuesta: el _source tal cual (va a la capa cruda) y su
    registro (clave_documento), con el que se deduplica. Las columnas las saca flatten()
    al generar las salidas."""
    return [_registro(hit.get("_source", {})) for hit in data.get("hits", {}).get("hits", [])]


def clave_expediente(portal_gestor, id_expediente):
    """Registro de un expediente del indice: (portalGestor, idExpediente) como texto
    (_texto_campo: True/'True', 9/9.0/'9' son el mismo valor; sin valor, '').
    idExpediente solo no basta: las dos numeraciones del indice comparten ids (ver la
    cabecera del modulo)."""
    return (_texto_campo(portal_gestor), _texto_campo(id_expediente))


def clave_documento(source):
    """Registro (clave_expediente) de un _source del indice."""
    return clave_expediente(source.get("portalGestor"), source.get("idExpediente"))


def _registro(source):
    return {"id_expediente": source.get("idExpediente", ""), "_clave": clave_documento(source), "_source": source}


def _clave_registro(record):
    """Registro de un documento de extract() (o de un registro sin _clave: el de su
    _source, o solo su id si no lo trae)."""
    clave = record.get("_clave")
    if clave is None:
        source = record.get("_source") or {}
        clave = clave_expediente(source.get("portalGestor"), record.get("id_expediente", source.get("idExpediente")))
    return clave


def flatten(source):
    row = {column: INTEGER_DEFAULTS.get(column, "") for column in CSV_COLS}
    row["id_expediente"] = source.get("idExpediente", "")
    row["numero_expediente"] = source.get("numeroExpediente", "")
    row["titulo"] = source.get("titulo", "")

    tipo_contrato = source.get("tipoContrato") or {}
    if isinstance(tipo_contrato, dict):
        row["tipo_contrato"] = tipo_contrato.get("descripcion", "")
        row["tipo_contrato_codigo"] = tipo_contrato.get("codigo", "")
    else:
        row["tipo_contrato"] = str(tipo_contrato or "")

    perfil = source.get("perfilContratante") or {}
    if isinstance(perfil, dict):
        row["organo_contratacion"] = perfil.get("descripcion", "")
        row["codigo_perfil"] = perfil.get("codigo", "")
        row["codigo_dir3"] = perfil.get("codigoDir3", "")

    estado = source.get("estado") or {}
    if isinstance(estado, dict):
        row["estado"] = estado.get("nombre", "")
        row["estado_codigo"] = estado.get("codigo", "")

    row["importe_licitacion"] = source.get("importeLicitacion", "")
    row["valor_estimado"] = source.get("valorEstimado", "")
    row["fecha_publicacion"] = _dt(source.get("fechaPublicacion"))
    row["fecha_limite_presentacion"] = _dt(source.get("fechaLimitePresentacion"))
    row["codigo_procedimiento"] = source.get("codigoProcedimiento", "")
    row["codigo_tramitacion"] = source.get("codigoTipoTramitacion", "")
    row["codigo_normativa"] = source.get("codigoNormativa", "")
    row["forma_presentacion"] = source.get("formaPresentacion", "")
    row["cofinanciado_ue"] = source.get("cofinanciadoUE", "")
    row["subasta_electronica"] = source.get("subastaElectronica", "")
    row["sistema_racionalizacion"] = source.get("sistemaRacionalizacion", "")

    cpvs = source.get("codigosCpv") or []
    if isinstance(cpvs, list):
        row["cpv"] = ";".join(str(code) for code in cpvs)

    provincias = source.get("provinciasEjecucion") or []
    if isinstance(provincias, list):
        row["provincias_ejecucion"] = ";".join(str(provincia) for provincia in provincias)

    adjudicaciones = source.get("adjudicaciones") or []
    if isinstance(adjudicaciones, list) and adjudicaciones:
        first_award = adjudicaciones[0] if isinstance(adjudicaciones[0], dict) else {}
        nif = first_award.get("nifAdjudicatario") or ""
        row["adjudicatario_nif"] = nif.rstrip(";") if isinstance(nif, str) else str(nif)
        row["importe_adjudicacion"] = first_award.get("importeAdjudicacion", "")
        row["importe_adjudicacion_iva"] = first_award.get("importeAdjudicacionConIva", "")
        row["num_adjudicaciones"] = len(adjudicaciones)
        if len(adjudicaciones) > 1:
            row["todos_adjudicatarios_nif"] = ";".join(
                (award.get("nifAdjudicatario") or "").rstrip(";")
                for award in adjudicaciones
                if isinstance(award, dict)
            )

    anuncios = source.get("anuncios") or []
    if isinstance(anuncios, list) and anuncios:
        fechas = [
            anuncio.get("fechaPublicacion")
            for anuncio in anuncios
            if isinstance(anuncio, dict) and anuncio.get("fechaPublicacion")
        ]
        if fechas:
            row["anuncio_primera_fecha"] = _dt(min(fechas))
            row["anuncio_ultima_fecha"] = _dt(max(fechas))
        row["num_anuncios"] = len(anuncios)

    medios = source.get("mediosPublicacion") or []
    if isinstance(medios, list):
        row["medios_publicacion"] = ";".join(
            str(medio.get("codigo") or "") for medio in medios if isinstance(medio, dict)
        )

    row["num_lotes"] = len(source.get("lotes") or [])
    row["url_detalle"] = (
        f"{BASE}/perfiles-licitaciones/detalle-licitacion?idExpediente={row['id_expediente']}"
    )

    # Sin esto se perdia lo que el portal sirve y no cabe en las columnas anteriores: los
    # importes de la 2a y siguientes adjudicaciones (19.765 expedientes del parquet
    # publicado tienen varias), el detalle de lotes y anuncios y cualquier otro campo
    for field, column in JSON_LIST_FIELDS.items():
        if source.get(field):
            row[column] = _json(source[field])
    extra = {}
    for key, value in source.items():
        if key in JSON_LIST_FIELDS:
            continue
        if key not in FLAT_SOURCE_FIELDS:
            extra[key] = value
            continue
        subfields = FLAT_SOURCE_FIELDS[key]
        items = value if isinstance(value, list) else [value]
        if subfields and any(isinstance(item, dict) and set(item) - subfields for item in items):
            extra[key] = value
    if extra:
        row["campos_extra_json"] = _json(extra)
    return row


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _dt(value):
    if not value:
        return ""
    match = re.match(r"(\d{4}-\d{2}-\d{2})", str(value))
    return match.group(1) if match else str(value)[:10]


def paginate(must=None, must_not=None, sort=None, label=""):
    """Las paginas de una consulta (hasta la ventana de MAX_FROM + PAGE_SIZE), sin repetir
    registro (portalGestor, idExpediente). Devuelve (documentos, total que declara el portal)."""
    del label
    if sort is None:
        sort = ORDEN_PAGINAS

    records = []
    seen = set()
    total_count = None

    for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
        data = es(build_query(must=must, must_not=must_not, sort=sort, offset=offset))
        if total_count is None:
            total = data.get("hits", {}).get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total

        batch = extract(data)
        if not batch:
            break

        for record in batch:
            clave = _clave_registro(record)
            if clave not in seen:
                seen.add(clave)
                records.append(record)

        if len(records) >= total_count:
            break
        time.sleep(DELAY)

    return records, total_count or 0


def _paginar_hoja(must, must_not, total, label):
    """paginate() de una consulta que cabe en la ventana. Si no llegan todos sus
    documentos (el mayor de `total` y lo que declara el portal: el indice cambia mientras
    se pagina), se repite hasta REINTENTOS_PAGINACION veces, en orden inverso y luego en el
    mismo, juntando los registros nuevos. Antes una sola pagina perdida dejaba la consulta
    entera fuera del ambito. Devuelve (documentos, total declarado)."""
    registros, vistos, declarado = [], set(), total or 0
    ordenes = (ORDEN_PAGINAS, ORDEN_PAGINAS_INVERSO)
    for intento in range(REINTENTOS_PAGINACION + 1):
        if intento:
            log.warning(
                "  %s: %s de %s documentos; se repite la paginacion (%s/%s)",
                label,
                f"{len(registros):,}",
                f"{declarado:,}",
                intento,
                REINTENTOS_PAGINACION,
            )
            time.sleep(PAUSA_REINTENTO)
        records, total_count = paginate(must=must, must_not=must_not, sort=ordenes[intento % 2], label=label)
        declarado = max(declarado, total_count)
        _anadir_nuevos(records, registros, vistos)
        if len(registros) >= declarado:
            if intento:
                log.info("  %s: completa al repetir la paginacion (%s)", label, f"{len(registros):,}")
            break
    return registros, declarado


def paginate_multisort(must=None, must_not=None, label="", target=None):
    all_records = []
    seen = set()
    if target is None:
        target = cnt(must=must, must_not=must_not)

    for sort_index, sort in enumerate(SORT_COMBOS, start=1):
        sort_name = f"{list(sort[0].keys())[0]}:{list(sort[0].values())[0]}"
        new_this_sort = 0

        for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
            try:
                data = es(build_query(must=must, must_not=must_not, sort=sort, offset=offset))
            except ScraperError as exc:
                # Un campo que el indice no deja ordenar (p. ej. texto sin fielddata) responde
                # 400 en la primera pagina: antes abortaba todo el scrape; se prueba la siguiente
                if offset == 0 and exc.status_code == 400:
                    log.warning("    %s: ordenacion %s no admitida, se omite: %s", label, sort_name, exc)
                    break
                raise
            batch = extract(data)
            if not batch:
                break

            for record in batch:
                clave = _clave_registro(record)
                if clave not in seen:
                    seen.add(clave)
                    all_records.append(record)
                    new_this_sort += 1

            # No cortamos al ver una pagina 100% duplicada: una ventana posterior
            # puede seguir aportando ids unicos dentro del limite de 10k.
            time.sleep(DELAY)

        pct = len(all_records) / target * 100 if target else 0
        log.info(
            "    %s sort %s/%s (%s): +%s -> %s/%s (%.0f%%)",
            label,
            sort_index,
            len(SORT_COMBOS),
            sort_name,
            f"{new_this_sort:,}",
            f"{len(all_records):,}",
            f"{target:,}",
            pct,
        )

        if len(all_records) >= target:
            break

    if len(all_records) < target:
        log.warning(
            "  %s: %s/%s (%.0f%%) PARTIAL",
            label,
            f"{len(all_records):,}",
            f"{target:,}",
            (len(all_records) / target * 100) if target else 0,
        )

    return all_records


def _anotar_incompleto(incompletos, etiqueta, must, must_not, total, descargados, motivo):
    """Anota una consulta que no se ha podido releer entera: lo que pueda caer en ella no
    se da por retirado (ni se siembra)."""
    if incompletos is not None:
        incompletos.append(
            {
                "etiqueta": etiqueta,
                "must": list(must or []),
                "must_not": list(must_not or []),
                "total": total,
                "descargados": descargados,
                "motivo": motivo,
            }
        )


def _anadir_nuevos(records, all_records, seen_ids):
    """Anade a `all_records` los documentos cuyo registro (portalGestor, idExpediente) no
    esta en `seen_ids` (antes bastaba el idExpediente: se perdia el otro expediente de cada
    id compartido por las dos numeraciones). Devuelve cuantos."""
    new_records = 0
    for record in records:
        clave = _clave_registro(record)
        if clave not in seen_ids:
            seen_ids.add(clave)
            all_records.append(record)
            new_records += 1
    return new_records


def scrape_recursive(must, must_not, label, all_records, seen_ids, dim_idx=0, known_total=None, *,
                     incompletos=None, fijas=()):
    """Descarga una consulta partiendola por DIMS hasta que cada trozo cabe en una ventana
    de 10k (y, si ya no quedan dimensiones, por tramos de idExpediente: _scrape_por_tramos).
    Anota en `incompletos` las consultas que no se han podido releer enteras; `fijas` son
    los campos que fija el alcance (--perfil, --anio), que no se parten."""
    total = known_total if known_total is not None else cnt(must=must, must_not=must_not)
    if total == 0:
        return 0

    if total <= MAX_FROM + PAGE_SIZE:
        records, declarado = _paginar_hoja(must, must_not, total, label)
        if len(records) < declarado:
            _anotar_incompleto(incompletos, label, must, must_not, declarado, len(records), "paginacion incompleta")
        return _anadir_nuevos(records, all_records, seen_ids)

    # Partir por una dimension que fija el alcance solo daria su valor y 0 en los demas
    while dim_idx < len(DIMS) and DIMS[dim_idx][0] in fijas:
        dim_idx += 1

    if dim_idx < len(DIMS):
        field, values = DIMS[dim_idx]
        dim_name = field.split(".")[-1]
        if values == "PERFILES":
            values = get_perfiles()

        log.info("  %s (%s) -> %s (%s vals)", label, f"{total:,}", dim_name, len(values))
        # Todos los recuentos antes de descargar, para compararlos con el total en una
        # ventana de segundos: si no lo cubren (un recuento que da 0 sin serlo...) lo que
        # falta no se sabe donde esta y el trozo entero queda como incompleto
        sub_counts = [(value, cnt(must=list(must) + [mm(field, value)], must_not=must_not)) for value in values]
        excluded = list(must_not)
        for value in values:
            excluded.append(mn(field, value) if isinstance(value, str) else {"match": {field: value}})
        null_count = cnt(must=must, must_not=excluded) if len(excluded) < MAX_EXCLUSIONS else None
        if null_count is not None and sum(count for _, count in sub_counts) + null_count < total:
            log.warning(
                "  %s: los recuentos por %s (%s) no cubren el total (%s); no se retira nada de este bloque",
                label,
                dim_name,
                f"{sum(count for _, count in sub_counts) + null_count:,}",
                f"{total:,}",
            )
            _anotar_incompleto(incompletos, label, must, must_not, total, None, "recuentos que no cubren el total")

        total_new = 0
        for value, sub_count in sub_counts:
            if sub_count == 0:
                continue
            total_new += scrape_recursive(
                list(must) + [mm(field, value)],
                must_not,
                f"{label}/{value}",
                all_records,
                seen_ids,
                dim_idx + 1,
                known_total=sub_count,
                incompletos=incompletos,
                fijas=fijas,
            )
            time.sleep(0.05)

        if total_new < total:
            if null_count is None:
                # Sin este aviso los registros sin valor en esta dimension se perdian en silencio
                log.warning(
                    "  %s/null_%s: %s exclusiones, se omite la rama sin valor (%s/%s recuperados)",
                    label,
                    dim_name,
                    len(excluded),
                    f"{total_new:,}",
                    f"{total:,}",
                )
                _anotar_incompleto(incompletos, f"{label}/null_{dim_name}", must, excluded, None, None,
                                   "rama sin valor omitida")
            elif null_count > 0:
                log.info("  %s/null_%s: %s", label, dim_name, f"{null_count:,}")
                total_new += scrape_recursive(
                    must,
                    excluded,
                    f"{label}/null_{dim_name}",
                    all_records,
                    seen_ids,
                    dim_idx + 1,
                    known_total=null_count,
                    incompletos=incompletos,
                    fijas=fijas,
                )

        return total_new

    return _scrape_por_tramos(must, must_not, label, all_records, seen_ids, total, incompletos)


class _RangoNoAplicado(ScraperError):
    """El proxy no aplica las clausulas 'range' (las dos mitades de un tramo cuentan todo)."""


def _rango_id(desde, hasta):
    return {"range": {"idExpediente": {"gte": int(desde), "lte": int(hasta)}}}


def _extremos_id(must, must_not):
    """(menor, mayor) idExpediente de una consulta, con dos paginas de un documento; None
    si no se pueden leer (un documento sin idExpediente va al final en los dos ordenes)."""
    extremos = []
    for orden in ("asc", "desc"):
        data = es(build_query(must=must, must_not=must_not, size=1, sort=[{"idExpediente": orden}]))
        hits = data.get("hits", {}).get("hits", [])
        try:
            extremos.append(int(hits[0]["_source"]["idExpediente"]))
        except (IndexError, KeyError, TypeError, ValueError):
            return None
    return tuple(extremos)


def _scrape_tramo(must, must_not, label, all_records, seen_ids, desde, hasta, total, incompletos):
    """Documentos de la consulta con idExpediente en [desde, hasta] (`total` segun el
    portal): se pagina si caben en la ventana y si no se parte el tramo en dos mitades
    (con su recuento) hasta que caben. Si las mitades no suman el total (documentos sin
    idExpediente, o el indice cambia) el tramo queda incompleto; si las dos cuentan el
    total, el proxy no aplica 'range' (_RangoNoAplicado)."""
    tramo_must = list(must) + [_rango_id(desde, hasta)]
    etiqueta = f"{label}/id_{desde}-{hasta}"
    if total <= MAX_FROM + PAGE_SIZE or desde >= hasta:
        records, declarado = _paginar_hoja(tramo_must, must_not, total, etiqueta)
        if len(records) < declarado:
            motivo = "paginacion incompleta" if declarado <= MAX_FROM + PAGE_SIZE else "tope de 10.000 resultados"
            _anotar_incompleto(incompletos, etiqueta, tramo_must, must_not, declarado, len(records), motivo)
        return _anadir_nuevos(records, all_records, seen_ids)

    medio = (desde + hasta) // 2
    mitades = [(desde, medio), (medio + 1, hasta)]
    cuentas = [cnt(must=list(must) + [_rango_id(a, b)], must_not=must_not) for a, b in mitades]
    if min(cuentas) >= total:
        raise _RangoNoAplicado(f"{etiqueta}: las dos mitades cuentan {cuentas} de {total}")
    if sum(cuentas) < total:
        log.warning(
            "  %s: los recuentos de las mitades (%s) no cubren el total (%s); no se retira nada de este tramo",
            etiqueta,
            f"{sum(cuentas):,}",
            f"{total:,}",
        )
        _anotar_incompleto(incompletos, etiqueta, tramo_must, must_not, total, None, "recuentos que no cubren el total")
    nuevos = 0
    for (a, b), cuenta in zip(mitades, cuentas):
        if cuenta:
            nuevos += _scrape_tramo(must, must_not, label, all_records, seen_ids, a, b, cuenta, incompletos)
    return nuevos


def _scrape_por_tramos(must, must_not, label, all_records, seen_ids, total, incompletos):
    """Una consulta que sigue por encima de la ventana tras las dimensiones: tramos de
    idExpediente ('range', que el proxy admite: medido el 2026-09-29) partidos por la mitad
    hasta que cada uno cabe (como mucho hay dos documentos por id, uno de cada
    numeracion). Antes se usaba solo el multi-sort, que en los menores del SAS dejaba 4.901
    documentos sin descargar; se vuelve a el si el proxy rechaza 'range' (HTTP 400) o no
    lo aplica. Lo que no cubren los tramos (documentos sin idExpediente) queda incompleto."""
    log.info("  %s (%s) -> tramos de idExpediente", label, f"{total:,}")
    try:
        extremos = _extremos_id(must, must_not)
        if extremos is not None:
            desde, hasta = extremos
            cuenta = cnt(must=list(must) + [_rango_id(desde, hasta)], must_not=must_not)
            if cuenta < total:
                log.warning(
                    "  %s: %s de %s documentos con idExpediente entre %s y %s; lo demas queda incompleto",
                    label, f"{cuenta:,}", f"{total:,}", desde, hasta,
                )
                _anotar_incompleto(incompletos, label, must, must_not, total, None, "recuentos que no cubren el total")
            return _scrape_tramo(must, must_not, label, all_records, seen_ids, desde, hasta, cuenta, incompletos)
        log.warning("  %s: no se pudo leer el rango de idExpediente; se usa el multi-sort", label)
    except _RangoNoAplicado as exc:
        log.warning("  %s: el proxy no aplica 'range' (%s); se usa el multi-sort", label, exc)
    except ScraperError as exc:
        if exc.status_code != 400:
            raise
        log.warning("  %s: el proxy rechaza 'range' (%s); se usa el multi-sort", label, exc)

    log.info("  %s (%s) -> multi-sort", label, f"{total:,}")
    records = paginate_multisort(must=must, must_not=must_not, label=label, target=total)
    if len(records) < total:
        _anotar_incompleto(incompletos, label, must, must_not, total, len(records), "tope de 10.000 resultados")
    return _anadir_nuevos(records, all_records, seen_ids)


def clean_records(records):
    cleaned = []
    for record in records:
        row = {column: record.get(column, INTEGER_DEFAULTS.get(column, "")) for column in CSV_COLS}
        for key, value in record.items():
            if key.startswith("_") or key in row:
                continue
            row[key] = value
        cleaned.append(row)
    return cleaned


def records_to_dataframe(records):
    cleaned = clean_records(records)
    dataframe = pd.DataFrame(cleaned)
    ordered = [column for column in CSV_COLS if column in dataframe.columns]
    extra = [column for column in dataframe.columns if column not in CSV_COLS]
    return dataframe[ordered + extra]


def _tipos_salida(dataframe):
    """Tipos con los que se escribe la tabla: importes numericos y el resto de columnas
    mixtas como texto. flatten() deja "" en los campos ausentes (p.ej. importe_adjudicacion
    sin adjudicaciones) y pyarrow no puede escribir columnas que mezclan float y str.
    Los importes numericos van siempre como float64 (antes, int64 si todos eran enteros):
    asi la misma cifra casa entre descargas en acumular()."""
    for column in dataframe.columns:
        values = dataframe[column]
        if column in AMOUNT_COLS:
            if pd.api.types.is_string_dtype(values.dtype):
                try:
                    dataframe[column] = pd.to_numeric(values.where(values.ne(""))).astype("float64")
                    continue
                except (TypeError, ValueError):
                    pass  # importes no numericos: se guardan como texto
            elif pd.api.types.is_numeric_dtype(values.dtype) and not pd.api.types.is_bool_dtype(values.dtype):
                dataframe[column] = values.astype("float64")
                continue
        if values.dtype == object:
            dataframe[column] = values.map(
                lambda value: value if isinstance(value, str) or pd.isna(value) else str(value)
            )
    return dataframe


def _escribir_csv(dataframe, path):
    """CSV derivado del parquet: se sustituye de forma atomica (el parquet guarda las
    versiones anteriores)."""
    tmp = path.with_name(f".{path.name}.nuevo")
    try:
        dataframe.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _escribir_parquet(dataframe, path, metadatos=None):
    """Parquet en un temporal que pasa por guardar_version: la version anterior queda en
    _historico/. `metadatos` va en los metadatos del esquema (CLAVE_METADATOS)."""
    tabla = pa.Table.from_pandas(dataframe, preserve_index=False)
    if metadatos is not None:
        esquema = dict(tabla.schema.metadata or {})
        esquema[CLAVE_METADATOS] = json.dumps(metadatos, sort_keys=True).encode("utf-8")
        tabla = tabla.replace_schema_metadata(esquema)
    tmp = path.with_name(f".{path.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        return guardar_version(path, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def save_csv(records, filename):
    if not records:
        return None
    path = DATA_DIR / filename
    _escribir_csv(records_to_dataframe(records), path)
    log.info("Guardado CSV %s (%s)", path, f"{len(records):,}")
    return path


def save_parquet(records, filename):
    if not records:
        return None
    path = DATA_DIR / filename
    _escribir_parquet(_tipos_salida(records_to_dataframe(records)), path)
    log.info("Guardado Parquet %s (%s)", path, f"{len(records):,}")
    return path


# ============================================================================
# Capa cruda: descargas por alcance, reanudables
# ============================================================================

def _dir_crudo():
    return DATA_DIR / CRUDO


def nombre_alcance(consulta, perfil=None, anio=None):
    """Nombre de la descarga en raw/: std o menores y, en una parcial, sus filtros."""
    partes = [consulta]
    if perfil:
        partes.append("perfil-" + re.sub(r"[^0-9A-Za-z.-]", "_", str(perfil)))
    if anio:
        partes.append(f"anio-{anio}")
    return "__".join(partes)


def consulta_base(consulta, perfil=None, anio=None):
    """(must, must_not) de una descarga: la de siempre de std o de menores (sin BRR) mas
    los filtros de una descarga parcial."""
    filtros = []
    if perfil:
        filtros.append(mm("perfilContratante.codigo", perfil))
    if anio:
        filtros.append(mm("numeroExpediente", str(anio)))
    if consulta == "std":
        return filtros, [mn("estado.codigo", "BRR"), mn("codigoProcedimiento", 9)]
    return [mm("codigoProcedimiento", 9)] + filtros, [mn("estado.codigo", "BRR")]


def _bloques(consulta, must, must_not, total, fijas, incompletos):
    """Bloques de primer nivel de una descarga con su recuento: cada uno se guarda al
    terminar y una ejecucion cortada sigue por el primero que falta. std: uno por
    procedimiento y el de procedimiento desconocido, como hasta ahora; menores: el primer
    corte de scrape_recursive (uno por tipo de contrato y el sin tipo) o uno solo si caben
    en una ventana. Si los recuentos no cubren el total, la descarga entera queda como
    incompleta."""
    raiz = "std" if consulta == "std" else "men"
    if consulta == "std":
        candidatos = [
            (f"p{proc}", list(must) + [mm("codigoProcedimiento", proc)], list(must_not), 0)
            for proc in PROCS
            if proc != 9
        ]
        candidatos.append(("p_unknown", list(must), build_unknown_standard_exclusions(must_not), 0))
    elif total <= MAX_FROM + PAGE_SIZE or not DIMS or DIMS[0][0] in fijas:
        return [(raiz, list(must), list(must_not), 0, total)] if total else []
    else:
        field, values = DIMS[0]
        dim_name = field.split(".")[-1]
        if values == "PERFILES":
            values = get_perfiles()
        log.info("  %s (%s) -> %s (%s vals)", raiz, f"{total:,}", dim_name, len(values))
        candidatos = [(f"{raiz}/{value}", list(must) + [mm(field, value)], list(must_not), 1) for value in values]
        candidatos.append(
            (f"{raiz}/null_{dim_name}", list(must), list(must_not) + [mn(field, value) for value in values], 1)
        )

    bloques = [
        (etiqueta, b_must, b_must_not, dim_idx, cnt(must=b_must, must_not=b_must_not))
        for etiqueta, b_must, b_must_not, dim_idx in candidatos
    ]
    suma = sum(bloque[-1] for bloque in bloques)
    if suma < total:
        log.warning(
            "  %s: los recuentos de los bloques (%s) no cubren el total (%s); no se retira nada de esta descarga",
            raiz,
            f"{suma:,}",
            f"{total:,}",
        )
        _anotar_incompleto(incompletos, raiz, must, must_not, total, None, "recuentos que no cubren el total")
    return [bloque for bloque in bloques if bloque[-1] > 0]


def _leer_jsonl(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _escribir_jsonl(path, objetos):
    tmp = path.with_name(f".{path.name}.nuevo")
    with gzip.open(tmp, "wt", encoding="utf-8") as handle:
        for objeto in objetos:
            handle.write(_json(objeto) + "\n")
    os.replace(tmp, path)


def _escribir_json(path, datos):
    tmp = path.with_name(f".{path.name}.nuevo")
    tmp.write_text(json.dumps(datos, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


class _Trabajo:
    """Descarga en curso de un alcance (raw/_en_curso/<alcance>/): un fichero por bloque
    terminado y estado.json. Si la ejecucion se corta, la siguiente con el mismo alcance
    sigue desde el primer bloque que falta (con los registros (portalGestor, idExpediente)
    ya vistos, para deduplicar igual); al guardar la descarga entera en raw/ se borra."""

    def __init__(self, carpeta, alcance):
        self.carpeta = Path(carpeta)
        self.alcance = alcance
        self.bloques = []
        self.vistos = set()
        estado = None
        if (self.carpeta / "estado.json").exists():
            try:
                estado = json.loads((self.carpeta / "estado.json").read_text(encoding="utf-8"))
            except (OSError, ValueError):
                estado = None
        if estado is not None and estado.get("alcance") == alcance:
            self.bloques = list(estado.get("bloques", []))
            for bloque in self.bloques:
                for documento in _leer_jsonl(self.carpeta / bloque["archivo"]):
                    self.vistos.add(clave_documento(documento))
            if self.bloques:
                log.info(
                    "Reanudando %s: %s bloques ya descargados (%s expedientes)",
                    self.carpeta.name,
                    len(self.bloques),
                    f"{len(self.vistos):,}",
                )
        elif self.carpeta.exists():
            log.warning("Descarga a medias de otro alcance o ilegible en %s: se descarta", self.carpeta)
            shutil.rmtree(self.carpeta)

    def hecho(self, etiqueta):
        return any(bloque["etiqueta"] == etiqueta for bloque in self.bloques)

    def guardar_bloque(self, etiqueta, documentos, incompletos):
        self.carpeta.mkdir(parents=True, exist_ok=True)
        archivo = f"bloque_{len(self.bloques):04d}.jsonl.gz"
        _escribir_jsonl(self.carpeta / archivo, documentos)
        self.bloques.append(
            {"etiqueta": etiqueta, "archivo": archivo, "documentos": len(documentos), "incompletos": incompletos}
        )
        _escribir_json(self.carpeta / "estado.json", {"alcance": self.alcance, "bloques": self.bloques})

    @property
    def documentos(self):
        return sum(bloque["documentos"] for bloque in self.bloques)

    def incompletos(self):
        return [incompleto for bloque in self.bloques for incompleto in bloque["incompletos"]]

    def documentos_en_orden(self):
        for bloque in self.bloques:
            yield from _leer_jsonl(self.carpeta / bloque["archivo"])

    def borrar(self):
        if self.carpeta.exists():
            shutil.rmtree(self.carpeta)
        padre = self.carpeta.parent
        if padre.is_dir() and not any(padre.iterdir()):
            padre.rmdir()


def _sha256_descomprimido(path):
    resumen = hashlib.sha256()
    try:
        with gzip.open(path, "rb") as handle:
            for trozo in iter(lambda: handle.read(1 << 20), b""):
                resumen.update(trozo)
    except (OSError, EOFError):
        return None
    return resumen.hexdigest()


def _escribir_crudo(destino, cabecera, documentos):
    """Guarda una descarga en la capa cruda: JSON Lines comprimido sin fecha en el gzip
    (el mismo contenido da los mismos bytes), con la cabecera en la primera linea y cada
    _source como lo sirve el portal. Si el contenido no cambio no se toca nada (se compara
    sin comprimir: otra version de zlib podria comprimir distinto); si cambio,
    guardar_version deja la anterior en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.nuevo")
    resumen = hashlib.sha256()
    lineas = itertools.chain(
        [json.dumps(cabecera, ensure_ascii=False, sort_keys=True, separators=(",", ":"))],
        (_json(documento) for documento in documentos),
    )
    try:
        with open(tmp, "wb") as bruto, gzip.GzipFile(filename="", mode="wb", fileobj=bruto, mtime=0,
                                                   compresslevel=6) as comprimido:
            for linea in lineas:
                datos = (linea + "\n").encode("utf-8")
                resumen.update(datos)
                comprimido.write(datos)
        if destino.exists() and _sha256_descomprimido(destino) == resumen.hexdigest():
            return "sin_cambios"
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def _cabecera_crudo(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.loads(handle.readline())


def _documentos_crudo(path):
    documentos = _leer_jsonl(path)
    next(documentos, None)  # cabecera
    yield from documentos


def descargar(consulta, perfil=None, anio=None):
    """Descarga un alcance (std o menores, con los filtros de una descarga parcial) a la
    capa cruda y devuelve un resumen. Si el portal falla lanza ScraperError: no se guarda
    nada en raw/ y lo ya descargado queda en raw/_en_curso/ para reanudar."""
    if consulta not in CONSULTAS:
        raise ValueError(f"Consulta desconocida: {consulta}")
    nombre = nombre_alcance(consulta, perfil, anio)
    must, must_not = consulta_base(consulta, perfil, anio)
    fijas = set()
    if perfil:
        fijas.add("perfilContratante.codigo")
    if anio:
        fijas.add("numeroExpediente")
    alcance = {"consulta": consulta, "perfil": perfil, "anio": anio, "must": must, "must_not": must_not}

    init()
    total = cnt(must=must, must_not=must_not)
    print("=" * 70)
    print(f"  {'SCRAPE ESTANDAR' if consulta == 'std' else 'SCRAPE MENORES'}: {total:,}")
    print("=" * 70)

    started_at = time.time()
    trabajo = _Trabajo(_dir_crudo() / EN_CURSO / nombre, alcance)
    incompletos = []
    for etiqueta, b_must, b_must_not, dim_idx, b_total in _bloques(consulta, must, must_not, total, fijas,
                                                                   incompletos):
        if trabajo.hecho(etiqueta):
            continue
        log.info("\n%s\n  %s: %s", "-" * 60, etiqueta, f"{b_total:,}")
        registros, incompletos_bloque = [], []
        scrape_recursive(
            b_must,
            b_must_not,
            etiqueta,
            registros,
            trabajo.vistos,
            dim_idx,
            known_total=b_total,
            incompletos=incompletos_bloque,
            fijas=fijas,
        )
        trabajo.guardar_bloque(etiqueta, [registro["_source"] for registro in registros], incompletos_bloque)

        descargados = trabajo.documentos
        elapsed = time.time() - started_at
        rate = descargados / elapsed if elapsed else 0
        eta = (total - descargados) / rate / 60 if rate > 0 else 0
        pct = descargados / total * 100 if total else 0
        log.info("  %s/%s (%.1f%%) %.0f/s ETA=%.1fm", f"{descargados:,}", f"{total:,}", pct, rate, eta)

    descargados = trabajo.documentos
    log.info("  %s: %s/%s in %.1fm", nombre, f"{descargados:,}", f"{total:,}", (time.time() - started_at) / 60)
    if not descargados:
        # Casi siempre es un fallo (no que el portal lo haya retirado todo): no retira nada
        log.warning("  %s: descarga vacia; no se guarda ni se retira nada", nombre)
        trabajo.borrar()
        return {"alcance": nombre, "estado": "vacia", "documentos": 0, "total": total, "incompletos": []}

    incompletos += trabajo.incompletos()
    cabecera = {
        "formato": FORMATO_CRUDO,
        "registro": REGISTRO,
        "alcance": alcance,
        "total": total,
        "documentos": descargados,
        "incompletos": incompletos,
    }
    destino = _dir_crudo() / f"{nombre}.jsonl.gz"
    estado = _escribir_crudo(destino, cabecera, trabajo.documentos_en_orden())
    trabajo.borrar()
    log.info("  Capa cruda %s: %s (%s expedientes)", destino, estado, f"{descargados:,}")
    if incompletos:
        log.warning(
            "  %s: %s consultas incompletas; lo que pueda caer en ellas no se da por retirado: %s",
            nombre,
            len(incompletos),
            ", ".join(incompleto["etiqueta"] for incompleto in incompletos[:10]),
        )
    return {"alcance": nombre, "estado": estado, "documentos": descargados, "total": total, "incompletos": incompletos}


# ============================================================================
# Salidas: registros acumulados de todas las versiones (comun/historico.py)
# ============================================================================

def _iso(momento):
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fecha_version(path):
    """Fecha de una version de la capa cruda (ISO en UTC al segundo): el sello que
    guardar_version pone en _historico/ o, en la copia actual, su fecha de modificacion.
    Al pasar a _historico/ la copia conserva la misma fecha."""
    path = Path(path)
    if path.parent.name == HISTORICO:
        sellos = re.findall(r"__(\d{8}T\d{6}Z)", path.name)
        if sellos:
            return _iso(datetime.strptime(sellos[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc))
    return _iso(datetime.fromtimestamp(path.stat().st_mtime, timezone.utc))


def _ficheros_crudos():
    carpeta = _dir_crudo()
    return sorted(carpeta.glob("*.jsonl.gz")) if carpeta.is_dir() else []


def _sha256_fichero(path):
    resumen = hashlib.sha256()
    with open(path, "rb") as handle:
        for trozo in iter(lambda: handle.read(1 << 20), b""):
            resumen.update(trozo)
    return resumen.hexdigest()


def _versiones_pendientes(incorporadas):
    """Versiones de raw/ posteriores a la ultima incorporada de su alcance, en orden
    (fecha; a la misma fecha, std antes que menores). Con la misma fecha (dos descargas en
    el mismo segundo) cuenta el sha256: solo es la incorporada si es el mismo fichero."""
    pendientes = []
    for actual in _ficheros_crudos():
        ultima = incorporadas.get(actual.name) or {}
        desde = ultima.get("fecha", "")
        orden = 0 if actual.name.startswith("std") else 1
        for version in versiones(actual):
            fecha = fecha_version(version)
            if fecha > desde or (fecha == desde and _sha256_fichero(version) != ultima.get("sha256")):
                pendientes.append((fecha, orden, actual.name, str(version)))
    return sorted(pendientes)


def _compactar(tabla):
    """La tabla con los tipos de _tipos_salida() tal como queda al escribirla en parquet y
    volver a leerla (texto en Arrow con pandas 3; objetos deduplicados con pandas 2): los
    mismos valores y la mitad de memoria que columnas de objetos de Python (con ~800K
    filas, de 2,4 a ~1 GB)."""
    return pa.Table.from_pandas(_tipos_salida(tabla), preserve_index=False).to_pandas()


def _tabla_de_documentos(documentos):
    """Tabla de una version de la capa cruda con el codigo actual: flatten() de cada
    _source, las columnas en el orden de siempre y los tipos de _tipos_salida()."""
    columnas = {column: [] for column in CSV_COLS}
    # json.loads crea una cadena por documento: las que se repiten (codigos, organos,
    # fechas...) se comparten, o 700K documentos ocupan GB antes de ser tabla
    compartidas = {}
    for documento in documentos:
        fila = flatten(documento)
        for column in CSV_COLS:
            valor = fila[column]
            if column in COLUMNAS_REPETIDAS and isinstance(valor, str):
                valor = compartidas.setdefault(valor, valor)
            columnas[column].append(valor)
    del compartidas
    return _compactar(pd.DataFrame(columnas, columns=CSV_COLS))


def _texto_campo(valor):
    """Una celda como el valor del indice: '' si es nula; enteros (tambien 9.0) sin decimales."""
    if valor is None or valor is pd.NA:
        return ""
    if isinstance(valor, (bool, np.bool_)):
        return str(bool(valor))
    if isinstance(valor, (int, np.integer)):
        return str(int(valor))
    if isinstance(valor, (float, np.floating)):
        if valor != valor:
            return ""
        return str(int(valor)) if float(valor).is_integer() else repr(float(valor))
    return str(valor)


def _clausulas(lista):
    """(campo, valor) de cada clausula 'match'; (None, None) si es de otro tipo."""
    for clausula in lista or []:
        match = clausula.get("match") if isinstance(clausula, dict) else None
        if not isinstance(match, dict) or len(match) != 1:
            yield None, None
            continue
        ((campo, valor),) = match.items()
        if isinstance(valor, dict):
            valor = valor.get("query")
        yield campo, valor


# Campos con un numero exacto por fila: un 'range' sobre ellos (los tramos de idExpediente)
# se decide con el valor de la columna, con certeza; sobre otro campo nunca es seguro
CAMPOS_RANGO = {"idExpediente"}


def _rango(clausula):
    """(campo, desde, hasta) de una clausula 'range' con gte y/o lte, o None."""
    rango = clausula.get("range") if isinstance(clausula, dict) else None
    if not isinstance(rango, dict) or len(rango) != 1:
        return None
    ((campo, limites),) = rango.items()
    if not isinstance(limites, dict) or not limites or set(limites) - {"gte", "lte"}:
        return None
    return campo, limites.get("gte"), limites.get("lte")


class _Coincidencias:
    """Si cada fila de una tabla cae dentro de una consulta del portal (bool must/must_not
    de clausulas 'match' y, en must, 'range' de idExpediente), con los valores de sus
    columnas y en dos grados:
    - seguro: el indice la devolveria con certeza. Codigos: el valor exacto (o un elemento
      de provinciasEjecucion); numeroExpediente: la palabra separada por espacios;
      idExpediente en un tramo: el numero dentro de sus limites;
    - posible: podria devolverla. La misma palabra (letras y cifras) sin distinguir
      mayusculas, sea el campo keyword, numerico o texto analizado; en un tramo, el numero
      dentro de sus limites o una fila sin numero.
    Un campo sin columna (o una clausula de otro tipo, o un 'range' en must_not) nunca es
    seguro y siempre es posible. Una fila de una descarga anterior solo se da por releida
    (y retirada si no esta) si SEGURO cae en el alcance y no es POSIBLE que caiga en una
    consulta incompleta: si hay duda no se retira."""

    def __init__(self, tabla):
        self.tabla = tabla
        self.filas = len(tabla)
        self._campos = {}
        self._numeros = {}

    def _en_rango(self, campo, desde, hasta, grado):
        """Filas cuyo numero en `campo` esta entre `desde` y `hasta` (incluidos)."""
        columna = CAMPOS_COLUMNA.get(campo)
        if campo not in CAMPOS_RANGO or columna is None or columna not in self.tabla.columns:
            return np.full(self.filas, grado == "posible")
        if campo not in self._numeros:
            numeros = pd.to_numeric(pd.Series(self.tabla[columna].to_numpy(dtype=object)), errors="coerce")
            self._numeros[campo] = numeros.to_numpy(dtype="float64")
        numeros = self._numeros[campo]
        sin_numero = np.isnan(numeros)
        valores = np.where(sin_numero, 0.0, numeros)
        dentro = ~sin_numero
        if desde is not None:
            dentro &= valores >= float(desde)
        if hasta is not None:
            dentro &= valores <= float(hasta)
        return dentro | sin_numero if grado == "posible" else dentro

    def _must(self, lista, grado):
        filas = np.ones(self.filas, dtype=bool)
        for clausula in lista or []:
            rango = _rango(clausula)
            if rango is not None:
                filas &= self._en_rango(*rango, grado)
                continue
            for campo, valor in _clausulas([clausula]):
                filas &= self._coinciden(campo, [valor], grado)
        return filas

    def _valores(self, campo):
        """(codigo por fila, valores distintos como texto) de la columna del campo, o None."""
        if campo not in self._campos:
            columna = CAMPOS_COLUMNA.get(campo)
            if columna is None or columna not in self.tabla.columns:
                self._campos[campo] = None
            else:
                textos = pd.Series([_texto_campo(valor) for valor in self.tabla[columna].astype(object)],
                                   dtype=object)
                codigos, unicos = pd.factorize(textos)
                self._campos[campo] = (codigos, pd.Series(unicos, dtype=object))
        return self._campos[campo]

    def _coinciden(self, campo, valores, grado):
        """Filas en las que alguno de `valores` casa con `campo` en el `grado` dado."""
        info = self._valores(campo)
        if info is None:
            return np.full(self.filas, grado == "posible")
        codigos, unicos = info
        textos = [_texto_campo(valor) for valor in valores]
        if grado == "seguro" and campo not in CAMPOS_TEXTO:
            buscados = set(textos)
            if campo in CAMPOS_MULTIVALOR:
                por_valor = unicos.map(lambda texto: any(parte in buscados for parte in texto.split(";")))
            else:
                por_valor = unicos.isin(buscados)
        else:
            if grado == "seguro":
                # Texto analizado: la palabra entre espacios (o al principio o al final)
                palabras = {palabra for texto in textos for palabra in texto.split()}
                patron = r"(?:^|(?<=\s))(?:{})(?=\s|$)"
            else:
                # Cualquier analizador: la misma palabra de letras y cifras
                palabras = {palabra for texto in textos for palabra in _PALABRA.findall(texto)}
                patron = r"(?<![^\W_])(?:{})(?![^\W_])"
            if not palabras:
                return np.zeros(self.filas, dtype=bool)
            regex = re.compile(patron.format("|".join(map(re.escape, sorted(palabras)))), re.IGNORECASE)
            por_valor = unicos.map(lambda texto: regex.search(texto) is not None)
        return np.append(np.asarray(por_valor, dtype=bool), False)[codigos]

    @staticmethod
    def _por_campo(lista):
        grupos = {}
        for campo, valor in _clausulas(lista):
            grupos.setdefault(campo, []).append(valor)
        return grupos

    def seguro(self, consulta):
        filas = self._must(consulta.get("must"), "seguro")
        for campo, valores in self._por_campo(consulta.get("must_not")).items():
            filas &= ~self._coinciden(campo, valores, "posible")
        return filas

    def posible(self, consulta):
        filas = self._must(consulta.get("must"), "posible")
        for campo, valores in self._por_campo(consulta.get("must_not")).items():
            filas &= ~self._coinciden(campo, valores, "seguro")
        return filas

    def ambito(self, cabecera):
        """Filas que una descarga (su cabecera) ha releido con certeza. Una descarga del
        codigo anterior (cabecera sin 'registro') no ha releido con certeza los ids que
        comparten las dos numeraciones (IDS_COMPARTIDOS): deduplicaba por idExpediente."""
        filas = self.seguro(cabecera.get("alcance") or {})
        incompletos = list(cabecera.get("incompletos") or [])
        if cabecera.get("registro") != REGISTRO:
            incompletos += [{"must": [_rango_id(desde, hasta)]} for desde, hasta in IDS_COMPARTIDOS]
        for incompleto in incompletos:
            filas &= ~self.posible(incompleto)
        return filas


def _trozos_por_expediente(ids, trozos):
    """Trozo de cada fila por su id_expediente (como texto: 1 y '1' van al mismo). Los dos
    registros de un id compartido caen en el mismo trozo: da igual, dos filas iguales
    tienen siempre el mismo id."""
    return pd.util.hash_pandas_object(ids.astype(str), index=False).to_numpy() % trozos


def _portal_gestor(extra):
    """portalGestor de un campos_extra_json (texto JSON de flatten()) o None."""
    if not isinstance(extra, str) or '"portalGestor"' not in extra:
        return None
    try:
        valor = json.loads(extra)
    except ValueError:
        return None
    return valor.get("portalGestor") if isinstance(valor, dict) else None


def _gestores_tabla(tabla):
    """portalGestor de cada fila de una tabla (de campos_extra_json; None si no lo trae:
    filas del publicado, que no tiene la columna)."""
    if "campos_extra_json" not in tabla.columns:
        return [None] * len(tabla)
    return [_portal_gestor(extra) for extra in tabla["campos_extra_json"].astype(object)]


def _claves_tabla(tabla):
    """Registro (clave_expediente) de cada fila de una tabla, como texto 'gestor|id'."""
    return pd.Series(
        [
            "|".join(clave_expediente(gestor, expediente))
            for gestor, expediente in zip(_gestores_tabla(tabla), tabla["id_expediente"].astype(object))
        ],
        index=tabla.index,
        dtype=object,
    )


def _acumular_version(anterior, filas, fecha, cabecera):
    """acumular() de una version de la capa cruda sobre la tabla acumulada. Ambito: las
    filas cuyo registro (portalGestor, idExpediente) vuelve (una version antigua de un
    expediente que el portal sirve cambiado) y las que la descarga ha releido con certeza
    (_Coincidencias.ambito). Por el idExpediente solo, una licitacion con el id de un menor
    de la otra numeracion quedaba retirada al incorporar los menores (5.030 el 29-sep).

    Con tablas grandes se llama a acumular() por trozos de id_expediente (dos filas solo
    son iguales si tienen el mismo) y se recompone el orden de una sola llamada: acumular
    compara cada celda como texto y con ~800K filas por lado pasaba de 5 GB."""
    if anterior is None or not len(anterior):
        return acumular(None, filas, fecha)
    releidos = _claves_tabla(anterior).isin(set(_claves_tabla(filas))).to_numpy()
    en_ambito = releidos | _Coincidencias(anterior).ambito(cabecera)
    # Sin copias (las dos tablas son de esta ejecucion): con ~800K filas cada copia son GB
    anterior[COLUMNA_AMBITO] = np.where(en_ambito, "si", "no")
    filas[COLUMNA_AMBITO] = "si"
    ignorar = tuple(IGNORAR_POR_DEFECTO) + (COLUMNA_AMBITO, COLUMNA_POSICION)

    trozos = -(-max(len(anterior), len(filas)) // FILAS_POR_TROZO)
    if trozos > 1:
        trozo_anterior = _trozos_por_expediente(anterior["id_expediente"], trozos)
        trozo_filas = _trozos_por_expediente(filas["id_expediente"], trozos)
        if len(np.unique(trozo_filas)) < trozos:
            trozos = 1  # un trozo sin filas nuevas no retiraria nada: una sola llamada
    if trozos <= 1:
        acumulada = acumular(anterior, filas, fecha, ambito=[COLUMNA_AMBITO], ignorar=ignorar)
        return acumulada.drop(columns=COLUMNA_AMBITO)

    filas[COLUMNA_POSICION] = np.arange(len(filas))
    viejas, nuevas = [], []
    for trozo in range(trozos):
        posiciones = np.flatnonzero(trozo_anterior == trozo)
        parte = acumular(anterior.iloc[posiciones], filas[trozo_filas == trozo], fecha,
                         ambito=[COLUMNA_AMBITO], ignorar=ignorar)
        # acumular devuelve primero las filas de `anterior` (en su orden) y despues las altas
        viejas.append(parte.iloc[: len(posiciones)].set_axis(posiciones))
        nuevas.append(parte.iloc[len(posiciones):])
    acumulada = pd.concat(
        [pd.concat(viejas).sort_index(), pd.concat(nuevas).sort_values(COLUMNA_POSICION, kind="stable")],
        ignore_index=True,
    )
    return acumulada.drop(columns=[COLUMNA_AMBITO, COLUMNA_POSICION])


def _leer_salida_anterior(path):
    """(tabla acumulada, fecha de la ultima version incorporada de cada alcance) de la
    salida anterior. Una salida sin _en_ultima_descarga (la del codigo anterior o el
    publicado) no sirve como registros acumulados: sus filas son de otra semantica y
    casarlas fila a fila las duplicaria; se incorporan por clave con --semilla."""
    path = Path(path)
    if not path.exists():
        return None, {}
    try:
        esquema = pq.read_schema(path)
    except Exception as exc:  # noqa: BLE001 - puntero LFS, fichero cortado...
        raise ScraperError(
            f"No se puede leer la salida anterior {path} ({exc}): no se regenera. La capa cruda sigue en "
            f"{_dir_crudo()}; aparta ese fichero o usa otra --salida y ejecuta 'procesar'"
        ) from exc
    if "_en_ultima_descarga" not in esquema.names:
        log.warning(
            "%s no tiene las columnas de historico (es del codigo anterior o el publicado): no se usa como registros "
            "acumulados y pasa a %s/ al escribir la nueva salida. Para conservar sus filas por clave, ejecuta "
            "'procesar --semilla' con esa copia de %s/",
            path,
            HISTORICO,
            HISTORICO,
        )
        return None, {}
    metadatos = json.loads((esquema.metadata or {}).get(CLAVE_METADATOS, b"{}").decode("utf-8"))
    return pd.read_parquet(path), dict(metadatos.get("incorporadas", {}))


def _preparar_semilla(semilla):
    """Filas de un parquet publicado con la semantica actual (errores conocidos del
    publicado v2026.02, ver la cabecera del modulo): los vacios escritos como el texto
    'nan' pasan a '' y los recuentos vacios a 0, como enteros. Una salida de este script
    (con _en_ultima_descarga) se usa tal cual."""
    if "_en_ultima_descarga" in semilla.columns:
        return semilla
    for column in semilla.columns:
        values = semilla[column]
        if pd.api.types.is_string_dtype(values.dtype):
            semilla[column] = values.mask(values.astype(object).eq("nan"), "")
    for column in INTEGER_DEFAULTS:
        if column in semilla.columns and pd.api.types.is_float_dtype(semilla[column].dtype):
            values = semilla[column].fillna(0)
            if values.eq(values.round()).all():
                semilla[column] = values.astype("int64")
    return semilla


def _textos(tabla, columna):
    """Valores de una columna como texto sin espacios a los lados ('' si es nulo o no esta)."""
    if columna not in tabla.columns:
        return np.full(len(tabla), "", dtype=object)
    return np.array([_texto_campo(valor).strip() for valor in tabla[columna].astype(object)], dtype=object)


def _id_compartido(texto):
    """Si un id_expediente (texto) cae en los ids que comparten las dos numeraciones
    (IDS_COMPARTIDOS); uno que no es un numero, tambien (no se sabe)."""
    try:
        numero = int(float(texto))
    except (TypeError, ValueError):
        return True
    return any(desde <= numero <= hasta for desde, hasta in IDS_COMPARTIDOS)


def _semilla_presente(salida, semilla):
    """Filas de la semilla que ya estan en la salida (descarga, retiradas y semillas ya
    incorporadas), con su motivo (None si no estan). Fuera de los ids que comparten las
    dos numeraciones (IDS_COMPARTIDOS) cada id es de un solo expediente: esta si la salida
    tiene su id (PRESENTE_CLAVE), como siempre, aunque haya cambiado su n.o y su titulo.
    En los compartidos:
    - PRESENTE_CLAVE: el mismo registro (portalGestor, idExpediente), si la fila lo trae
      (una salida de este script); el mismo id_expediente y n.o de expediente; o la misma
      fila (id, n.o, perfil y titulo iguales, tambien vacios: una fila sembrada antes);
    - PRESENTE_CONTENIDO: el mismo id, perfil, titulo e importe de licitacion (el n.o
      cambia: 3 renumerados entre febrero y septiembre de 2026; y el publicado tiene 'nan'
      donde el portal 'N/A'; las 15 filas asi del publicado v2026.02 tienen el mismo
      importe), o un id que la salida tiene en las dos numeraciones (la fila tiene que ser
      una de ellas).
    Una fila de la semilla con portalGestor solo se compara con filas de su numeracion o
    sin ella. El id solo no basta: la semilla trae uno de los dos expedientes de cada id
    compartido y puede ser justo el que falta."""
    def claves(tabla):
        ids, nums = _textos(tabla, "id_expediente"), _textos(tabla, "numero_expediente")
        perfiles, titulos = _textos(tabla, "codigo_perfil"), _textos(tabla, "titulo")
        importes = _textos(tabla, "importe_licitacion")
        gestores = [_texto_campo(gestor) for gestor in _gestores_tabla(tabla)]
        return list(zip(gestores, ids, nums, perfiles, titulos, importes))

    registros, numeraciones, ids = set(), {}, set()
    pares, exactas, trios = set(), set(), set()
    for gestor, i, n, p, t, importe in claves(salida):
        if not i:
            continue
        ids.add(i)
        if gestor:
            registros.add((gestor, i))
            numeraciones.setdefault(i, set()).add(gestor)
        if n:
            pares.add((gestor, i, n))
        exactas.add((gestor, i, n, p, t))
        if t:
            trios.add((gestor, i, p, t, importe))
    dobles = {i for i, gestores in numeraciones.items() if len(gestores) > 1}
    del numeraciones
    # Sin numeracion en la fila de la semilla vale la de cualquier fila de la salida
    pares_id = {clave[1:] for clave in pares}
    exactas_id = {clave[1:] for clave in exactas}
    trios_id = {clave[1:] for clave in trios}

    def esta(gestor, clave, por_gestor, por_id):
        if gestor:
            return (gestor,) + clave in por_gestor or ("",) + clave in por_gestor
        return clave in por_id

    motivo = np.full(len(semilla), None, dtype=object)
    for fila, (gestor, i, n, p, t, importe) in enumerate(claves(semilla)):
        if not i:
            continue
        if not _id_compartido(i):
            if i in ids:
                motivo[fila] = PRESENTE_CLAVE
            continue
        if ((gestor and (gestor, i) in registros) or (n and esta(gestor, (i, n), pares, pares_id))
                or esta(gestor, (i, n, p, t), exactas, exactas_id)):
            motivo[fila] = PRESENTE_CLAVE
        elif (t and esta(gestor, (i, p, t, importe), trios, trios_id)) or (not gestor and i in dobles):
            motivo[fila] = PRESENTE_CONTENIDO
    return motivo


def _sembrar(salida, semillas, origen=None):
    """Incorpora cada semilla: primero se mira que filas ya estan en la salida
    (_semilla_presente) y solo de las que faltan, cuales ha dejado de traer una descarga
    de raw/ que cubria su alcance (el ambito de cualquiera de sus versiones): esas se
    anaden y las demas quedan fuera del ambito. Antes se miraba primero el ambito y las
    filas de fuera no se comparaban (95.258 el 29-sep, de las que 95.255 estaban).
    Devuelve (salida, filas anadidas).

    De la semilla se leen primero solo las columnas con que se decide y luego, del parquet,
    las filas que se anaden. Una fila de la semilla sin id_expediente se anade si esta en el
    ambito (no hay con que compararla)."""
    cabeceras = [_cabecera_crudo(version) for actual in _ficheros_crudos() for version in versiones(actual)]
    origen = origen or ORIGEN_SEMILLA
    anadidas = 0
    for path in semillas:
        disponibles = pq.read_schema(path).names
        columnas = [c for c in dict.fromkeys(COLUMNAS_PRESENCIA + list(CAMPOS_COLUMNA.values())
                                             + ["_origen", "_en_ultima_descarga"]) if c in disponibles]
        semilla = _preparar_semilla(pd.read_parquet(path, columns=columnas))
        motivo = _semilla_presente(salida, semilla)
        faltan = np.flatnonzero(np.array([m is None for m in motivo], dtype=bool))
        evaluador = _Coincidencias(semilla.iloc[faltan].reset_index(drop=True))
        en_ambito = np.zeros(len(faltan), dtype=bool)
        for cabecera in cabeceras:
            en_ambito |= evaluador.ambito(cabecera)
        motivo[faltan[en_ambito]] = ANADIDA
        motivo[faltan[~en_ambito]] = FUERA_AMBITO
        informe = informe_semilla(motivo, origen,
                                  semilla[[c for c in ("id_expediente", "numero_expediente") if c in semilla.columns]])
        informe["ruta"] = str(path)
        del semilla, evaluador
        imprimir_informe_semilla(informe)
        log.info(
            "Semilla %s: %s filas leidas, %s anadidas; ya estan %s con la clave presente (id y n.o de expediente) y "
            "%s con el contenido presente (id, perfil y titulo); %s que faltan, fuera del ambito",
            path,
            f"{informe['leidas']:,}",
            f"{informe['anadidas']:,}",
            f"{informe['descartadas_clave']:,}",
            f"{informe['descartadas_contenido']:,}",
            f"{informe['fuera_ambito']:,}",
        )
        posiciones = np.flatnonzero(np.array([m == ANADIDA for m in motivo], dtype=bool))
        if not len(posiciones):
            continue
        nuevas = _preparar_semilla(pq.read_table(path).take(pa.array(posiciones)).to_pandas())
        propio = nuevas["_origen"] if "_origen" in nuevas.columns else pd.Series(None, index=nuevas.index, dtype=object)
        nuevas["_origen"] = propio.astype(object).where(propio.notna(), origen)
        nuevas["_en_ultima_descarga"] = False
        if "_origen" not in salida.columns:
            salida["_origen"] = pd.Series([None] * len(salida), dtype=object)
        orden = list(salida.columns) + [c for c in nuevas.columns if c not in salida.columns]
        salida = pd.concat([salida, nuevas], ignore_index=True, sort=False)[orden]
        salida["_en_ultima_descarga"] = salida["_en_ultima_descarga"].astype(bool)
        anadidas += len(nuevas)
    return salida, anadidas


def _escribir_salidas(tabla, incorporadas=None, parquet=True):
    """Escribe el parquet (guardar_version) y los tres CSV, partidos por
    codigo_procedimiento como antes (9: menores)."""
    tabla = _tipos_salida(tabla)
    estado = None
    if parquet:
        estado = _escribir_parquet(tabla, DATA_DIR / PARQUET_SALIDA, {"incorporadas": incorporadas or {}})
    menores = pd.to_numeric(tabla["codigo_procedimiento"], errors="coerce").eq(9).to_numpy()
    for nombre, parte in ((CSV_STD, tabla[~menores]), (CSV_MENORES, tabla[menores]), (CSV_TODO, tabla)):
        if len(parte):
            _escribir_csv(parte, DATA_DIR / nombre)
    return estado


def construir_salidas(semillas=(), origen_semilla=None):
    """Regenera licitaciones_andalucia.parquet y los CSV desde la salida anterior y las
    versiones de raw/ que aun no tiene (acumular), y siembra --semilla. Si no hay nada
    nuevo no se escribe nada."""
    anterior, incorporadas = _leer_salida_anterior(DATA_DIR / PARQUET_SALIDA)
    pendientes = _versiones_pendientes(incorporadas)
    for fecha, _, nombre, version in pendientes:
        cabecera = _cabecera_crudo(version)
        filas = _tabla_de_documentos(_documentos_crudo(version))
        incorporadas[nombre] = {"fecha": fecha, "sha256": _sha256_fichero(version)}
        if not len(filas):
            log.warning("  %s: version sin expedientes; no se retira nada", version)
            continue
        antes, expedientes = (0 if anterior is None else len(anterior)), len(filas)
        anterior = _compactar(_acumular_version(anterior, filas, fecha, cabecera))
        del filas
        log.info(
            "  Incorporada %s (%s): %s expedientes, %s filas nuevas; %s filas fuera de la ultima descarga",
            Path(version).name,
            fecha,
            f"{expedientes:,}",
            f"{len(anterior) - antes:,}",
            f"{int((~anterior['_en_ultima_descarga'].astype(bool)).sum()):,}",
        )

    anadidas = 0
    if semillas:
        if anterior is None:
            log.warning("Sin ninguna descarga en %s no se siembra (no se sabe que sigue publicado)", _dir_crudo())
        else:
            anterior, anadidas = _sembrar(anterior, semillas, origen_semilla)

    if anterior is None:
        log.info("Sin descargas en %s: no hay salidas que generar", _dir_crudo())
        return None
    if not pendientes and not anadidas:
        if not (DATA_DIR / CSV_TODO).exists():
            _escribir_salidas(anterior, parquet=False)
        log.info("Salidas sin cambios: %s", DATA_DIR / PARQUET_SALIDA)
        return "sin_cambios"

    retiradas = int((~anterior["_en_ultima_descarga"].astype(bool)).sum())
    estado = _escribir_salidas(anterior, incorporadas)
    log.info(
        "Guardado %s (%s): %s filas, %s fuera de la ultima descarga",
        DATA_DIR / PARQUET_SALIDA,
        estado,
        f"{len(anterior):,}",
        f"{retiradas:,}",
    )
    print(f"  Salida: {len(anterior):,} filas ({retiradas:,} con _en_ultima_descarga=False), parquet {estado}")
    return estado


# ============================================================================
# CLI
# ============================================================================

def scrape_std(perfil=None, anio=None):
    """Descarga las licitaciones regulares a la capa cruda (sin generar las salidas)."""
    return descargar("std", perfil, anio)


def scrape_menores(perfil=None, anio=None):
    """Descarga los contratos menores a la capa cruda (sin generar las salidas)."""
    return descargar("menores", perfil, anio)


def scrape_all(perfil=None, anio=None, semillas=(), origen_semilla=None):
    standard = scrape_std(perfil, anio)
    menores = scrape_menores(perfil, anio)
    construir_salidas(semillas, origen_semilla)
    return standard, menores


def configurar_salida(path):
    """Cambia la carpeta de salida: DATA_DIR, la cache de perfiles y scraper.log."""
    global DATA_DIR, PERFILES_CACHE_PATH
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    anterior = os.path.abspath(DATA_DIR / "scraper.log")
    DATA_DIR = path
    PERFILES_CACHE_PATH = path / "perfiles_cache.json"
    raiz = logging.getLogger()
    for handler in list(raiz.handlers):
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == anterior:
            raiz.removeHandler(handler)
            handler.close()
            nuevo = logging.FileHandler(path / "scraper.log", encoding="utf-8", delay=True)
            nuevo.setFormatter(handler.formatter)
            nuevo.setLevel(handler.level)
            raiz.addHandler(nuevo)


def _validar_semillas(semillas, origen_semilla):
    """Mensaje de error si alguna --semilla no se puede usar (o None)."""
    salida = (DATA_DIR / PARQUET_SALIDA).resolve()
    for semilla in semillas:
        if not semilla.is_file():
            return f"No existe la semilla {semilla}"
        if semilla.resolve() == salida:
            return (
                f"La semilla {semilla} es la salida de esta ejecucion: pasaria a _historico/ y la siguiente "
                "sembraria desde la salida. Copia el publicado fuera o usa otra --salida"
            )
        try:
            columnas = pq.read_schema(semilla).names
        except Exception as exc:  # noqa: BLE001
            return f"No se puede leer la semilla {semilla}: {exc}"
        if "id_expediente" not in columnas:
            return f"La semilla {semilla} no tiene la columna id_expediente"
        if "_en_ultima_descarga" in columnas and not origen_semilla:
            return (
                f"La semilla {semilla} es una salida de este script (tiene _en_ultima_descarga): indica su "
                "procedencia con --origen-semilla; si no, sus filas pasarian por filas del release v2026.02"
            )
    return None


COMANDOS = ("scrape-std", "scrape-men", "scrape", "procesar")


def main(argv=None):
    args = list(argv) if argv is not None else sys.argv[1:]
    if not args:
        print(
            f"""
  python {Path(__file__).name} scrape-std    Licitaciones regulares
  python {Path(__file__).name} scrape-men    Contratos menores
  python {Path(__file__).name} scrape        Dataset completo + Parquet
  python {Path(__file__).name} procesar      Solo regenera las salidas desde raw/ (sin red)

  Opciones: --salida DIR, --perfil CODIGO, --anio AAAA, --semilla PARQUET, --origen-semilla TEXTO
"""
        )
        return 0

    parser = argparse.ArgumentParser(prog=Path(__file__).name, description="Scraper de la Junta de Andalucia")
    parser.add_argument("command", help=", ".join(COMANDOS))
    parser.add_argument("--salida", type=Path, default=None, help=f"carpeta de salida (por defecto {DATA_DIR})")
    parser.add_argument("--perfil", default=None,
                        help="descarga parcial: solo este perfil del contratante (perfilContratante.codigo)")
    parser.add_argument("--anio", default=None,
                        help="descarga parcial: solo este ano del numero de expediente (dimension numeroExpediente)")
    parser.add_argument("--semilla", type=Path, action="append", default=[],
                        help="parquet publicado (p.ej. licitaciones_andalucia.parquet de v2026.02) que se incorpora "
                             "como la instantanea mas antigua: se anaden las filas que no estan en la salida y ha "
                             "dejado de traer una descarga que cubria su alcance. Esta si tiene su id_expediente; en "
                             "los ids que comparten las dos numeraciones, si coincide su registro (portalGestor, id), "
                             "su id y n.o de expediente, su id, perfil, titulo e importe, o la salida tiene el id en "
                             "las dos numeraciones. Repetible")
    parser.add_argument("--origen-semilla", default=None,
                        help=f"_origen de las filas de --semilla (por defecto '{ORIGEN_SEMILLA}')")
    options = parser.parse_args(args)

    command = options.command.lower()
    if command not in COMANDOS:
        print(f"Comando desconocido: {command}")
        return 1
    if options.anio is not None and not re.fullmatch(r"\d{4}", options.anio):
        print(f"--anio debe ser un ano de cuatro cifras: {options.anio}")
        return 2
    if options.salida is not None:
        configurar_salida(options.salida)
    error = _validar_semillas(options.semilla, options.origen_semilla)
    if error:
        print(error)
        return 2

    descargas = []
    try:
        if command in ("scrape-std", "scrape"):
            descargas.append(scrape_std(options.perfil, options.anio))
        if command in ("scrape-men", "scrape"):
            descargas.append(scrape_menores(options.perfil, options.anio))
        construir_salidas(options.semilla, options.origen_semilla)
    except ScraperError as exc:
        log.error("ERROR: %s", exc)
        return 1
    # Una descarga vacia casi siempre es un fallo: no ha retirado nada, pero se avisa
    return 1 if any(descarga["estado"] == "vacia" for descarga in descargas) else 0


if __name__ == "__main__":
    raise SystemExit(main())
