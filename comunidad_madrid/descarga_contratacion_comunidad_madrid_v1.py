"""
=============================================================================
DESCARGA DE CONTRATACIÓN PÚBLICA - COMUNIDAD DE MADRID
=============================================================================
Portal: https://contratos-publicos.comunidad.madrid
Método: Buscador avanzado → Exportar CSV

v4 - Producción:
    Flujo HTTP:
      0. GET /contratos → extraer antibot_key de drupal-settings JSON
         (transformar: invertir en pares de 2 chars desde el final)
      1. GET /contratos?antibot_key=XXX&filtros → registra filtros en sesión
      2. GET /buscador-contratos/csv → formulario CAPTCHA matemático
      3. POST CAPTCHA → formulario completion
      4. POST completion → CSV

    Estrategia por tipo de publicación:

    A) CONTRATOS MENORES (99% del volumen, 4.832.623 en sept. 2026):
       - Fecha hasta NO funciona, fecha desde rompe combinada con entidad
       - Solución: descargar por ENTIDAD ADJUDICADORA (las del desplegable)
       - Sin filtro de fecha; si una entidad llega a UMBRAL se subdivide por
         rango de presupuesto (incluidos ≤0 y ≥50.000, que también existen)
       - Ojo: los menores de entidades que ya no están en el desplegable
         (consejerías de legislaturas anteriores...) no se descargan por
         esta vía: faltaban ~2 M (2,79 M de 4,83 M), casi todos de 2015-2022.
         Los recoge la vía A2.

    A2) CONTRATOS MENORES POR VENTANAS DE FECHA, SIN ENTIDAD (vía por fecha):
       - Filtro «Fecha del contrato o encargo» (ds_fecha_encargo / _1) con
         la entidad en «Cualquiera». Medido el 30-9-2026 contra el portal:
         · cuadra con la faceta: 4.832.621 menores con fecha de 1900 a 2099
           y 2 de 1899 = los 4.832.623 del portal;
         · por año de 2015 a 2026: 124.070, 494.836, 628.128, 505.580,
           473.356, 448.316, 450.747, 405.235, 443.460, 387.866, 349.025 y
           127.001 (la vía A tenía 18.813, 177.229, 255.747, 206.630,
           174.992, 171.760, 164.473, 320.027, 439.418, 384.812, 348.814 y
           127.001, por el año de «Fecha del contrato»);
         · las ventanas son intervalos cerrados en UTC: el día D va de las
           00:00 UTC de D a las 00:00 UTC de D+1, y los menores guardados a
           las 00:00 UTC de la frontera salen en las dos ventanas contiguas
           (14-5-2019: 2.262; 15-5-2019: 2.374; los dos días: 4.107, con 529
           en común). Por eso la suma de las ventanas pasa del total y cada
           menor repetido en dos ventanas se deja una vez (unificar);
         · en el día 15-5-2019, 1.251 de los 2.374 menores (53 %) no estaban
           en la vía A: todos de entidades con el nombre de entonces
           ('Hospital Ramón y Cajal', 'Hospital Universitario Doce de
           Octubre', 'Gerencia de Atención Primaria', 'SUMMA 112',
           'Consejería de Cultura y Turismo'...). Los otros 1.123, idénticos
           en las 18 columnas a los de la vía A;
         · la exportación es la misma (CAPTCHA incluido) y es estable: el
           mismo día pedido dos veces da el mismo CSV byte a byte;
         · el feed Atom feed/licitaciones2 solo trae licitaciones (la
           colección 121 de la agregación de la PLACSP), ningún menor: no
           sirve para los menores.
       - Ventanas: 1-1-1800 a 31-12-2014 (49 menores), un mes desde
         ANIO_VENTANAS hasta el mes actual y una de lo posterior (fechas
         futuras, hoy 0). La página del buscador trae el recuento («Mostrando
         1 - 10 de N»), así que antes de exportar se sabe si la ventana llega
         al tope de la exportación (UMBRAL_TRUNCADO): entonces se parte en dos
         mitades por días, sin exportarla, hasta que quepa.
       - Cada exportación se comprueba contra el recuento del portal: una
         ventana cuyo CSV no trae tantos registros como dice el buscador está
         incompleta. Se reintenta; si sigue sin cuadrar, se parte en dos
         mitades, y una ventana de un día que no cuadra se marca incompleta:
         nunca sustituye a la copia anterior (salvo que la contenga entera,
         así no retira nada), se anota sin «comprobado» y se vuelve a pedir en
         la ejecución siguiente.
       - CSV en csv_originales/por_fecha/ (su propio _historico/ y su
         propio _comprobaciones.json): la vía A ni los ve ni los archiva.
       - Al terminar una pasada completa, un CSV de ventana que ya no es de
         ninguna consulta (una ventana que ahora se parte) pasa a _historico/
         si todas las ventanas que lo cubren han llegado completas o sin
         ningún menor según el portal; con alguna incompleta, se conserva.

    B) OTROS TIPOS (licitaciones, adjudicaciones, etc., ~36K):
       - Fecha hasta SÍ funciona
       - Descargar por MES + TIPO PUBLICACIÓN (como v3)
       - Período: 2017-año actual por meses + un CSV por tipo con todo lo
         publicado antes (el portal tiene anuncios desde 2014)

    Cada ejecución vuelve a pedir los CSV comprobados hace más de
    VIGENCIA_HORAS: los de menores (sin fechas) acumulan los contratos nuevos
    y los de cada mes cambian de estado, adjudicatario, prórrogas... Los
    comprobados hace menos se saltan (también los que dieron 0 filas o no
    cambiaron), así que una ejecución cortada se reanuda donde se quedó.

    Columnas CSV (18):
    Tipo de Publicación; Estado; Entidad Adjudicadora; Nº Expediente;
    Referencia; Título del contrato; Tipo de contrato;
    Procedimiento de adjudicación; Presupuesto de licitación;
    Nº de ofertas; Resultado; NIF del adjudicatario; Adjudicatario;
    Fecha del contrato; Importe de adjudicación;
    Importe de las modificaciones; Importe de las prórrogas;
    Importe de la liquidación

HISTÓRICO: NUNCA SE MACHACA NADA (comun/historico.py)
    Capa cruda (csv_originales/):
      - Cada CSV descargado pasa por guardar_version: si no cambió no se
        toca; si cambió, la copia anterior va a csv_originales/_historico/
        <nombre>__<AAAAMMDDTHHMMSSZ>.csv (fecha de esa copia).
      - Una descarga vacía (0 filas) o fallida no sustituye nada.
      - Una respuesta con UMBRAL_TRUNCADO filas que se va a partir por
        importe no se guarda: son las primeras 50.000 filas, no lo que
        publica el portal para esa consulta. Se guardan sus rangos.
      - El número de entidad del desplegable cambia entre ejecuciones (la
        Consejería de Sanidad era la 28 en febrero de 2026 y la 60 en
        septiembre): el mismo menor llega en CSV de nombres distintos. Al
        terminar una descarga completa de menores, los CSV de menores que ya
        no son de ninguna consulta de la ejecución pasan a _historico/
        (archivar): otro número de entidad, o el CSV entero de una entidad
        o de un rango que ahora se parte. Si el desplegable trae menos de la
        mitad de las entidades conocidas no se archiva nada: es más probable
        un fallo del portal.
      - csv_originales/_comprobaciones.json: fecha y resultado de la última
        comprobación de cada consulta, también de las que dan 0 filas, no
        cambian (guardar_version no toca el fichero) o se parten. Con ella
        se reanuda una ejecución cortada. Si se pierde, se usa la fecha del
        fichero.
    Tabla consolidada (unificar):
      - Se construye desde TODAS las versiones de cada CSV (versiones()), de
        la más antigua a la vigente, con acumular(): nada de lo visto se
        pierde, y lo que el portal retira o cambia queda con
        _en_ultima_descarga=False. _primera_descarga y _ultima_descarga son
        las fechas de la primera y la última versión del CSV que traen el
        registro. La de una versión es la de su sello en _historico/ o la de
        modificación de la vigente; una descarga idéntica a la anterior no
        crea versión.
      - Se compara por bloque: el registro con sus filas de continuación
        (sin Tipo de Publicación: más lotes, adjudicatarios, prórrogas,
        modificaciones). Esas filas se repiten entre contratos distintos
        ('...;0,00;0,00;0,00;0,00'), así que fila a fila la de un contrato
        retirado casaría con la de otro. Un bloque que cambia en cualquier
        fila es un registro nuevo entero (el anterior queda con False), y sus
        filas siguen juntas y en orden.
      - Una versión vacía o ilegible no retira nada (se avisa).
      - Un CSV sin copia vigente (solo con versiones en _historico/, p.ej.
        archivado por la descarga) está sustituido: sus filas quedan con
        _en_ultima_descarga=False, salvo que otro CSV traiga el mismo bloque.
      - Consultas solapadas (bloques en varios CSV): frontera entre rangos de
        importe, entidades que incluyen las de sus dependientes, un CSV
        sustituido y su sucesor. De cada bloque se deja una copia por cada
        una de las que tiene el CSV que más tiene (duplicados de origen),
        contando primero las presentes en la última descarga. De cada copia
        se queda la de un CSV que la trae en su última versión, si lo hay
        (_en_ultima_descarga=True), con la _primera_descarga mínima y la
        _ultima_descarga máxima de todos.
      - Salidas: contratacion_comunidad_madrid_completo.csv (';', utf-8-sig)
        y .parquet (texto; _en_ultima_descarga booleana), las dos con
        guardar_version: la anterior va a _historico/, y si no cambian no se
        tocan. Columnas: las del portal, _archivo_fuente, _primera_descarga,
        _ultima_descarga, _en_ultima_descarga y, con --semilla, _origen.
      - Memoria: la tabla se procesa por partes (una por CSV) y nunca está
        dos veces en memoria. Con pandas 3, la descarga de septiembre de
        2026 (2,85 millones de filas) pide unos 2,8 GB, y 3,4 GB con
        --semilla. El código anterior pedía 3,7 GB sin semilla. Medido el
        30-9-2026 como RSS máximo del proceso (getrusage), pandas 3.0.6, con
        la capa cruda real de la descarga del 29-9-2026: 5,6-5,9 GB con
        --semilla, y 5,7 GB (4 min en vez de 3) con la vía por fecha entera
        simulada a escala (4,84 M filas más en 133 ventanas): cada CSV de
        ventana se filtra al leerlo y solo se queda lo que entra en la tabla.
      - Vía por fecha (A2): sus CSV (csv_originales/por_fecha/) se
        acumulan igual, versión a versión, y entran DESPUÉS de la tabla de la
        vía A sin tocar ninguna de sus filas presentes (el mismo menor por
        las dos vías es uno solo, el de la vía A):
        · no entra un bloque cuya clave estable (Referencia + Entidad
          Adjudicadora, la de la semilla) tiene una fila presente en la vía A
          (en la última descarga de su CSV), sea cual sea su contenido;
        · ni uno sin Referencia idéntico a un bloque presente de la vía A;
        · el resto entra al final, en orden de ventana. Un bloque repetido
          en dos ventanas (la frontera) o idéntico a uno que la vía A ya no
          trae en su última descarga (p.ej. su entidad salió del desplegable)
          queda una vez, como en las consultas solapadas: la copia presente,
          con la _primera_descarga mínima y la _ultima_descarga máxima;
        · si una clave queda presente en dos ventanas con distinto contenido
          (el portal la cambió entre las dos descargas), sigue presente la de
          la descarga más reciente y la otra queda como versión anterior
          (_en_ultima_descarga=False): nunca dos filas presentes por clave.
        Sin CSV de la vía por fecha, la tabla es la de antes, byte a byte.
    Semilla (unificar --semilla <parquet publicado>, repetible):
      - Clave estable: Referencia + Entidad Adjudicadora. Referencia es el
        identificador del anuncio en el portal ('D957_2', '1152625',
        'C11537'). Comprobado con datos reales:
        · Es única en cada instantánea, quitados los solapes entre CSV: en
          los 2.568.350 registros de los CSV originales del release (9-2-2026)
          y en los de septiembre de 2026.
        · Es estable: de los 1.612.229 menores de febrero de las entidades ya
          descargadas en septiembre, 1.612.228 siguen con la misma clave y la
          fila idéntica. El que falta (D1325_1, Hospital Central de la Cruz
          Roja) no reaparece con otra referencia.
        · La Referencia sola no basta: en el publicado, 62 anuncios comparten
          referencia con el de otra entidad.
        · No se añade el Nº Expediente: no hace falta para que sea única, y
          así una corrección del expediente no crea otro registro.
      - Solo se añaden las filas de la semilla cuya clave no está en la
        tabla (contando las semillas anteriores), al final, marcadas con
        _origen='release v2026.02' y _en_ultima_descarga=False. Nunca se toca
        ni se duplica una fila descargada. La tabla se reconstruye desde los
        crudos en cada unificación, así que --semilla hay que darla siempre.
        Con la descarga de septiembre de 2026 añade 110 menores que el portal
        ya no sirve (de 2.563.527 filas del publicado). Las filas sin Referencia
        (continuaciones y 6 anuncios del publicado) tienen la clave
        incompleta y se comparan por contenido (comun.historico); las que no
        tienen ninguna columna de la clave, solo con las filas de la tabla
        que tampoco la tienen.
      - Ámbito: una fila de la semilla solo se añade si su consulta se ha
        vuelto a descargar. Un menor, si su entidad aparece en los menores de
        la tabla; lo demás, si su CSV (_archivo_fuente, por mes y tipo) está
        en csv_originales/ o se comprobó (_comprobaciones.json). Si no, no se
        sabe si sigue publicada: no se añade y se cuenta en el informe.
      - Errores conocidos del publicado (release v2026.02; CSV originales
        descargados el 9-2-2026 y que vienen en el ZIP del release):
        · Las celdas vacías son el texto 'nan'. Al sembrar se dejan vacías
          otra vez: en los CSV originales no hay ningún 'nan' literal.
        · Deduplicó por Nº Expediente + Referencia + Entidad y perdió 22.628
          de las 22.629 filas de continuación (lotes, adjudicatarios,
          prórrogas y modificaciones de los tipos que no son menores).
          También quitó los 4.824 menores repetidos en dos rangos de importe,
          que sí sobraban.
        · Fuera de eso, cada fila es idéntica a una de su CSV original.
        · Presupuesto de licitación llega en varios formatos ('1.161,60',
          '252338.62', '1.448228264E7'): así lo sirve el portal, no es un
          error del publicado.
        Esos CSV originales conservan las filas de continuación. Se pueden
        usar como la versión más antigua de la capa cruda: basta copiarlos a
        csv_originales/, con su fecha, antes de la primera descarga.
=============================================================================
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time
import warnings
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from calendar import monthrange
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (ANADIDA, COLUMNAS_META, FUERA_AMBITO, HISTORICO, ORIGEN_SEMILLA,  # noqa: E402
                             acumular, archivar, guardar_version, imprimir_informe_semilla,
                             informe_semilla, seleccionar_semilla, versiones)
from comun.lectura_csv import registros_csv  # noqa: E402

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
BASE_URL = "https://contratos-publicos.comunidad.madrid"
BUSCAR_URL = f"{BASE_URL}/contratos"
CSV_URL = f"{BASE_URL}/buscador-contratos/csv"

# Carpeta del propio script (comunidad_madrid/), sea cual sea el directorio actual
OUTPUT_DIR = Path(__file__).resolve().parent
CSV_DIR = OUTPUT_DIR / "csv_originales"
OUTPUT_DIR.mkdir(exist_ok=True)
CSV_DIR.mkdir(exist_ok=True)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO, format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "descarga.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
}

# Tipos de publicación NO menores (se descargan por mes)
TIPOS_NO_MENORES = [
    "Convocatoria anunciada a licitación",
    "Contratos adjudicados por procedimientos sin publicidad",
    "Encargos a medios propios",
    "Anuncio de información previa",
    "Consultas preliminares del mercado",
]

UMBRAL_TRUNCADO = 50000
MAX_REINTENTOS = 3
PAUSA_BASE = 5

# Primer año de la descarga mensual de "otros tipos"; lo publicado antes va en
# un único CSV por tipo con solo fecha hasta (el portal tiene anuncios de 2014)
ANIO_INICIO = 2017

# Un CSV descargado o comprobado hace más de VIGENCIA_HORAS se vuelve a
# descargar (menos de 24 h para que una ejecución diaria los refresque todos)
VIGENCIA_HORAS = 20

# Última comprobación de cada consulta (en CSV_DIR; ver HISTÓRICO)
COMPROBACIONES = "_comprobaciones.json"

# Tabla consolidada (en OUTPUT_DIR)
SALIDA_CSV = "contratacion_comunidad_madrid_completo.csv"
SALIDA_PARQUET = "contratacion_comunidad_madrid_completo.parquet"

# Profundidad máxima de la subdivisión de un rango de importe truncado
PROFUNDIDAD_MAXIMA = 5

# Filas por grupo del Parquet consolidado (se escribe por partes)
FILAS_POR_GRUPO = 1_000_000

# Rangos de presupuesto para subdividir entidades truncadas (>50K)
# La mayoría de contratos menores son <100€, necesitamos rangos muy finos abajo.
# Un límite vacío es un rango abierto: hay menores con presupuesto negativo, 0
# o por encima de 50.000 € que ningún rango cerrado recogería. Los límites son
# inclusivos: lo que cae justo en la frontera sale en dos CSV y unificar_csvs()
# lo deja una vez (quitar_repetidos_entre_ficheros).
RANGOS_IMPORTE = [
    ("", "0"),
    ("0", "10"),
    ("10", "20"),
    ("20", "30"),
    ("30", "50"),
    ("50", "75"),
    ("75", "100"),
    ("100", "150"),
    ("150", "200"),
    ("200", "300"),
    ("300", "500"),
    ("500", "1000"),
    ("1000", "3000"),
    ("3000", "5000"),
    ("5000", "10000"),
    ("10000", "15000"),
    ("15000", "50000"),
    ("50000", ""),
]

# --- Vía por fecha (A2 en el docstring): menores sin entidad por ventanas ---
# Subcarpeta de csv_originales/ con sus CSV, su _historico/ y su propio
# _comprobaciones.json: la vía por entidad no la ve ni la archiva (y no empieza
# por 'menores_', así ningún 'menores_*' la confunde con un CSV de esa vía)
CARPETA_FECHA = "por_fecha"
# Filtro «Fecha del contrato o encargo (desde/hasta)» del buscador
PARAM_FECHA_DESDE = "ds_fecha_encargo"
PARAM_FECHA_HASTA = "ds_fecha_encargo_1"
# Ventanas: de FECHA_MINIMA al 31-12 del año anterior a ANIO_VENTANAS (49
# menores en septiembre de 2026, 2 de ellos de 1899), un mes desde
# ANIO_VENTANAS hasta el mes actual y lo posterior hasta FECHA_MAXIMA
FECHA_MINIMA = date(1800, 1, 1)
FECHA_MAXIMA = date(2099, 12, 31)
ANIO_VENTANAS = 2015
VENTANA_POSTERIORES = "menores_fecha_posteriores.csv"
# Ventanas seguidas sin respuesta útil (sin recuento o sin CSV) tras las que se
# deja la vía por fecha para la ejecución siguiente: el portal está caído o ha
# cambiado, y seguir sería martillearlo
FALLOS_SEGUIDOS_MAX = 5
# Recuento de la página del buscador ('Mostrando 1 - 10 de 473356') y la
# página sin resultados ('NO EXISTEN RESULTADOS PARA LA BÚSQUEDA ACTUAL EN
# ESTE PORTAL'), medidos el 30-9-2026
PATRON_RECUENTO = re.compile(r"Mostrando\s+\d[\d.]*\s*-\s*\d[\d.]*\s+de\s+(\d[\d.]*)")
PATRON_SIN_RESULTADOS = re.compile(r"no\s+existen\s+resultados\s+para\s+la\s+b[uú]squeda", re.IGNORECASE)
# Estados de una ventana que cuentan como llegada entera (su CSV cuadra con el
# recuento del portal)
COMPLETAS = ("nuevo", "actualizado", "sin_cambios")


# ---------------------------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------------------------
def resolver_captcha(text):
    """Resuelve CAPTCHA matemático: '3 + 8 =' → 11"""
    match = re.search(r'(\d+)\s*([+\-*/])\s*(\d+)\s*=', text)
    if not match:
        return None
    a, op, b = int(match.group(1)), match.group(2), int(match.group(3))
    ops = {'+': lambda x, y: x+y, '-': lambda x, y: x-y,
           '*': lambda x, y: x*y, '/': lambda x, y: x//y}
    return ops.get(op, lambda x, y: None)(a, b)


def transformar_antibot_key(key):
    """
    Drupal antibot module: el JavaScript invierte la key en pares de 2
    caracteres desde el final.
    """
    result = ''
    for i in range(len(key) - 1, -1, -2):
        if i - 1 >= 0:
            result += key[i-1] + key[i]
        else:
            result += key[i]
    return result


def nombre_csv_entidad(entidad_idx, entidad_nombre):
    """Nombre para CSV de contratos menores por entidad."""
    slug = re.sub(r'[^a-z0-9]+', '_', entidad_nombre.lower().strip(' -'))[:40]
    return f"menores_ent{entidad_idx:03d}_{slug}.csv"


def nombre_csv_entidad_rango(entidad_idx, entidad_nombre, importe_desde, importe_hasta):
    """Nombre para CSV de contratos menores por entidad + rango importe."""
    slug = re.sub(r'[^a-z0-9]+', '_', entidad_nombre.lower().strip(' -'))[:30]
    return f"menores_ent{entidad_idx:03d}_{slug}_imp{importe_desde}-{importe_hasta}.csv"


def nombre_csv_mes(anio, mes, tipo_pub):
    """Nombre para CSV de otros tipos por mes."""
    tp = re.sub(r'[^a-z0-9]+', '_', tipo_pub.lower())[:25]
    return f"{anio}_{mes:02d}_{tp}.csv"


def nombre_csv_hasta(anio, tipo_pub):
    """Nombre para CSV de otros tipos publicados hasta el 31-12 de `anio`."""
    tp = re.sub(r'[^a-z0-9]+', '_', tipo_pub.lower())[:25]
    return f"hasta_{anio}_{tp}.csv"


def iso_utc(epoch):
    """Fecha ISO 8601 en UTC al segundo ('2026-09-27T14:43:00Z', ordenable como texto)."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_de_iso(texto):
    """Epoch de una fecha de iso_utc, o None si no lo es."""
    try:
        return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def leer_comprobaciones(carpeta=None):
    """{nombre del CSV: {'comprobado', 'intento', 'resultado', 'filas'}} de
    csv_originales/_comprobaciones.json (o del de `carpeta`: la vía por fecha
    lleva el suyo, con 'recuento' del portal). Si no existe o no se puede leer
    se empieza de cero: solo sirve para no repetir comprobaciones recientes y
    para el ámbito de la semilla."""
    ruta = (CSV_DIR if carpeta is None else Path(carpeta)) / COMPROBACIONES
    if not ruta.exists():
        return {}
    try:
        datos = json.loads(ruta.read_text(encoding="utf-8"))
        if isinstance(datos, dict):
            return {k: v for k, v in datos.items() if isinstance(v, dict)}
        motivo = "no es un objeto JSON"
    except (OSError, ValueError) as e:
        motivo = str(e)
    log.warning(f"  {COMPROBACIONES} no se puede leer ({motivo}): se ignora")
    return {}


def guardar_comprobaciones(comprobaciones, carpeta=None):
    """Escribe _comprobaciones.json (el de `carpeta`, si se da) de forma atómica."""
    ruta = (CSV_DIR if carpeta is None else Path(carpeta)) / COMPROBACIONES
    tmp = ruta.with_name(f".{ruta.name}.nuevo")
    tmp.write_text(json.dumps(comprobaciones, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    os.replace(tmp, ruta)


def es_reciente(filepath, comprobaciones=None):
    """¿Se descargó o comprobó hace menos de VIGENCIA_HORAS? Cuenta la fecha de
    la copia vigente y la última comprobación anotada: una descarga idéntica a
    la anterior no toca el fichero, y una de 0 filas no lo crea."""
    momentos = []
    if filepath.exists() and filepath.stat().st_size > 100:
        momentos.append(filepath.stat().st_mtime)
    anotada = epoch_de_iso((comprobaciones or {}).get(filepath.name, {}).get("comprobado"))
    if anotada is not None:
        momentos.append(anotada)
    return bool(momentos) and time.time() - max(momentos) < VIGENCIA_HORAS * 3600


def sub_rangos(importe_desde, importe_hasta):
    """Las dos mitades de un rango de importe, o None si no se puede partir
    (rango abierto o de un euro)."""
    if not importe_desde or not importe_hasta:
        return None
    low, high = int(importe_desde), int(importe_hasta)
    mid = (low + high) // 2
    if mid <= low or mid >= high:
        return None
    return [(str(low), str(mid)), (str(mid), str(high))]


def generar_segmentos_mensuales(anio_inicio, anio_fin):
    """Genera (fecha_desde, fecha_hasta, anio, mes) por mes."""
    hoy = datetime.now()
    segmentos = []
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if anio == hoy.year and mes > hoy.month:
                break
            _, ultimo_dia = monthrange(anio, mes)
            desde = f"01-{mes:02d}-{anio}"
            hasta = f"{ultimo_dia:02d}-{mes:02d}-{anio}"
            segmentos.append((desde, hasta, anio, mes))
    return segmentos


# --- Vía por fecha -----------------------------------------------------------
def carpeta_fecha():
    """csv_originales/por_fecha/ (se calcula al llamar: CSV_DIR puede cambiar)."""
    return CSV_DIR / CARPETA_FECHA


def fecha_portal(dia):
    """Fecha como la pide el buscador ('15-05-2019')."""
    return dia.strftime("%d-%m-%Y")


def nombre_csv_ventana(desde, hasta):
    """CSV de la ventana [desde, hasta] de la vía por fecha."""
    return f"menores_fecha_{desde:%Y%m%d}_{hasta:%Y%m%d}.csv"


def ventanas_fecha(hoy=None):
    """[(desde, hasta, nombre del CSV)] de la vía por fecha, en orden y sin
    huecos: lo anterior a ANIO_VENTANAS en una ventana, un mes por ventana
    hasta el mes de `hoy` (entero) y lo posterior (fechas futuras) en otra, de
    nombre fijo aunque su inicio avance cada mes."""
    hoy = hoy or date.today()
    ventanas = [(FECHA_MINIMA, date(ANIO_VENTANAS - 1, 12, 31))]
    anio, mes = ANIO_VENTANAS, 1
    while (anio, mes) <= (hoy.year, hoy.month):
        ventanas.append((date(anio, mes, 1), date(anio, mes, monthrange(anio, mes)[1])))
        anio, mes = (anio + 1, 1) if mes == 12 else (anio, mes + 1)
    salida = [(d, h, nombre_csv_ventana(d, h)) for d, h in ventanas]
    salida.append((date(anio, mes, 1), FECHA_MAXIMA, VENTANA_POSTERIORES))
    return salida


def mitades_ventana(desde, hasta):
    """Las dos mitades (por días) de la ventana [desde, hasta], o None si es
    de un solo día."""
    if hasta <= desde:
        return None
    medio = desde + timedelta(days=(hasta - desde).days // 2)
    return [(desde, medio), (medio + timedelta(days=1), hasta)]


def rango_de_ventana(nombre):
    """(desde, hasta) del nombre de un CSV de ventana, o None (el de las
    posteriores, que avanza, u otro fichero)."""
    m = re.fullmatch(r"menores_fecha_(\d{8})_(\d{8})\.csv", nombre)
    if not m:
        return None
    try:
        return tuple(datetime.strptime(g, "%Y%m%d").date() for g in m.groups())
    except ValueError:
        return None


def recuento_portal(html):
    """Nº de resultados de una página del buscador: el de 'Mostrando 1 - 10
    de N', 0 si dice que no hay resultados o None si no trae ninguno de los
    dos (otra página, o el portal ha cambiado)."""
    m = PATRON_RECUENTO.search(html)
    if m:
        return int(m.group(1).replace(".", ""))
    if PATRON_SIN_RESULTADOS.search(html):
        return 0
    return None


def registros_de_csv(ruta):
    """Registros (filas con Tipo de Publicación) del CSV `ruta`, leído como lo
    leerá unificar (leer_csv): el nº que se compara con el recuento del
    portal. Las filas de continuación no son registros."""
    df, _ = leer_csv(ruta)
    if len(df) == 0:
        return 0
    return int(inicio_de_bloque(df).sum())


def contiene_todo(actual, nuevo):
    """¿Trae el CSV `nuevo` todas las filas del CSV `actual` (contando las
    repetidas)? Entonces guardarlo como versión nueva no retira nada."""
    a, _ = leer_csv(actual)
    b, _ = leer_csv(nuevo)
    if len(a) == 0:
        return True
    if list(a.columns) != list(b.columns):
        return False
    filas = lambda df: Counter(map(tuple, df.astype(object).where(df.notna(), "").astype(str).to_numpy().tolist()))  # noqa: E731
    return not (filas(a) - filas(b))


# ---------------------------------------------------------------------------
# DESCARGADOR
# ---------------------------------------------------------------------------
class DescargadorComunidadMadrid:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.antibot_key = None
        self.entidades = []  # Se carga del dropdown
        self.stats = {
            "ok": 0, "nuevo": 0, "actualizado": 0, "sin_cambios": 0,
            "error": 0, "skip_existe": 0, "skip_vacio": 0, "partidos": 0,
            "archivados": 0, "incompletas": 0, "filas": 0, "bytes": 0, "archivos": [],
        }
        self.t_inicio = None
        self.comprobaciones = leer_comprobaciones()
        # CSV de menores de las consultas de esta ejecución (las que no se
        # parten): al terminar todas, los demás CSV de menores se archivan
        self.vigentes = set()
        # Vía por fecha: su _comprobaciones.json (se lee al empezarla) y las
        # ventanas que no se parten de esta ejecución: {nombre: (estado,
        # desde, hasta)}
        self.comprobaciones_fecha = {}
        self.hojas_fecha = {}
        self.fallos_seguidos = 0

    # -----------------------------------------------------------------------
    # PASO 0: Obtener antibot_key + lista de entidades
    # -----------------------------------------------------------------------
    def _obtener_antibot_key(self, forzar=False):
        if self.antibot_key and not forzar:
            return self.antibot_key

        log.info("  [antibot] Obteniendo key...")
        resp = self.session.get(BUSCAR_URL, timeout=60)
        resp.raise_for_status()

        # Extraer antibot_key
        match = re.search(
            r'<script[^>]*data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
            resp.text, re.DOTALL
        )
        if match:
            try:
                settings = json.loads(match.group(1))
                forms = settings.get('antibot', {}).get('forms', {})
                for form_data in forms.values():
                    if 'key' in form_data:
                        raw_key = form_data['key']
                        self.antibot_key = transformar_antibot_key(raw_key)
                        log.info(f"  [antibot] OK: {self.antibot_key[:20]}...")
                        break
            except (json.JSONDecodeError, KeyError):
                pass

        if not self.antibot_key:
            match2 = re.search(r'"key"\s*:\s*"([A-Za-z0-9_\-]+)"', resp.text)
            if match2:
                raw_key = match2.group(1)
                self.antibot_key = transformar_antibot_key(raw_key)
                log.info(f"  [antibot] OK (fallback): {self.antibot_key[:20]}...")

        if not self.antibot_key:
            log.error("  [antibot] No se encontró key")
            return None

        # Extraer entidades del dropdown (solo una vez)
        if not self.entidades:
            soup = BeautifulSoup(resp.text, 'html.parser')
            select = soup.find('select', {'name': 'entidad_adjudicadora'})
            if select:
                for opt in select.find_all('option'):
                    val = opt.get('value', '')
                    txt = opt.get_text(strip=True)
                    if val and val != 'All':
                        self.entidades.append((val, txt))
                log.info(f"  [entidades] {len(self.entidades)} encontradas")

        return self.antibot_key

    def _reset_sesion(self):
        """Resetea cookies y antibot_key para reintentos."""
        self.session.cookies.clear()
        self.antibot_key = None

    # -----------------------------------------------------------------------
    # PASO 1: Búsqueda con filtros + antibot_key
    # -----------------------------------------------------------------------
    def _buscar(self, fecha_desde="", fecha_hasta="",
                tipo_pub=None, entidad=None, extra_params=None):
        self._obtener_antibot_key()

        params = {
            "t": "",
            "tipo_publicacion": "All",
            "createddate": fecha_desde,
            "createddate_1": fecha_hasta,
            "fin_presentacion": "", "fin_presentacion_1": "",
            "ss_buscador_estado_situacion": "All",
            "numero_expediente": "", "referencia": "",
            "ss_identificador_ted": "",
            "entidad_adjudicadora": entidad or "All",
            "tipo_contrato": "All",
            "codigo_cpv": "",
            "ss_field_contrato_lote_reservado": "All",
            "bs_regulacion_armonizada": "All",
            "ss_sist_de_contratacion": "All",
            "modalidad_compra_publica": "All",
            "ss_financiacion_ue": "All",
            "ss_field_pcon_codigo_referencia": "",
            "procedimiento_adjudicacion": "All",
            "ss_tipo_de_tramitacion": "All",
            "ss_metodo_presentacion": "All",
            "bs_subasta_electronica": "All",
            "presupuesto_base_licitacion_total": "",
            "presupuesto_base_licitacion_total_1": "",
            "ds_field_pcon_fecha_desierto": "",
            "ds_field_pcon_fecha_desierto_1": "",
            "nif_adjudicatario": "", "nombre_adjudicatario": "",
            "importacion_adjudicacion_con_impuestos": "",
            "importacion_adjudicacion_con_impuestos_1": "",
            "ds_fecha_encargo": "", "ds_fecha_encargo_1": "",
            "ds_field_pcon_fecha_publi_anun_form": "",
            "ds_field_pcon_fecha_publi_anun_form_1": "",
        }

        if self.antibot_key:
            params["antibot_key"] = self.antibot_key

        if tipo_pub:
            params["f[0]"] = f"tipo_publicacion:{tipo_pub}"

        if extra_params:
            params.update(extra_params)

        resp = self.session.get(BUSCAR_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.text

    # -----------------------------------------------------------------------
    # PASO 2: GET página CSV → parsear formulario CAPTCHA
    # -----------------------------------------------------------------------
    def _obtener_form_captcha(self):
        resp = self.session.get(CSV_URL, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        form = (soup.find('form', {'id': 'pcon-contratos-menores-export-results-form'})
                or soup.find('form', action='/buscador-contratos/csv'))

        if not form:
            log.error("  No se encontró formulario CSV")
            return None

        data = {}
        for inp in form.find_all('input'):
            name = inp.get('name')
            if name:
                data[name] = inp.get('value', '')

        captcha = resolver_captcha(str(form))
        if captcha is None:
            log.error("  No se pudo resolver CAPTCHA")
            return None

        data['captcha_response'] = str(captcha)

        action = form.get('action', '/buscador-contratos/csv')
        if action.startswith('/'):
            action = BASE_URL + action

        return {'action': action, 'data': data}

    # -----------------------------------------------------------------------
    # PASO 3: POST CAPTCHA → formulario completion
    # -----------------------------------------------------------------------
    def _post_captcha(self, form_info):
        resp = self.session.post(form_info['action'], data=form_info['data'],
                                 timeout=120)
        resp.raise_for_status()

        ct = resp.headers.get('Content-Type', '')
        cd = resp.headers.get('Content-Disposition', '')

        if 'csv' in ct or 'octet-stream' in ct or cd:
            return resp.content

        if 'text/html' in ct:
            soup = BeautifulSoup(resp.text, 'html.parser')

            form2 = (soup.find('form', {'id': 'pcon-contratos-menores-export-results-completion-form'})
                     or soup.find('form', action=re.compile(r'(completion|execute)')))

            if form2:
                return self._post_completion(form2)

            if soup.find('form', {'id': 'pcon-contratos-menores-export-results-form'}):
                log.warning("  CAPTCHA rechazado")
                return None

            log.warning("  Respuesta HTML inesperada")
            (OUTPUT_DIR / "debug_post_captcha.html").write_text(
                resp.text[:10000], encoding='utf-8')

        return None

    # -----------------------------------------------------------------------
    # PASO 4: POST completion → CSV
    # -----------------------------------------------------------------------
    def _post_completion(self, form):
        data = {}
        for inp in form.find_all('input'):
            name = inp.get('name')
            if name:
                data[name] = inp.get('value', '')

        action = form.get('action', '/buscador-contratos/csv/completion')
        if action.startswith('/'):
            action = BASE_URL + action

        resp = self.session.post(action, data=data, timeout=300)
        resp.raise_for_status()

        ct = resp.headers.get('Content-Type', '')
        cd = resp.headers.get('Content-Disposition', '')

        if 'csv' in ct or 'octet-stream' in ct or cd:
            return resp.content

        log.warning(f"  Completion no devolvió CSV (Content-Type: {ct})")
        (OUTPUT_DIR / "debug_completion.html").write_text(
            resp.text[:10000], encoding='utf-8')
        return None

    # -----------------------------------------------------------------------
    # FLUJO COMPLETO: búsqueda → CAPTCHA → CSV
    # -----------------------------------------------------------------------
    def _descargar_csv(self, fecha_desde="", fecha_hasta="",
                       tipo_pub=None, entidad=None, extra_params=None):
        """Ejecuta el flujo completo. Retorna bytes del CSV o None."""
        self._buscar(fecha_desde, fecha_hasta, tipo_pub, entidad, extra_params)
        time.sleep(PAUSA_BASE)

        form_info = self._obtener_form_captcha()
        if not form_info:
            return None
        time.sleep(1)

        return self._post_captcha(form_info)

    # -----------------------------------------------------------------------
    # GUARDAR CSV
    # -----------------------------------------------------------------------
    def _guardar(self, csv_data, filepath):
        """Guarda el CSV con guardar_version: escritura atómica, y si cambió la
        copia anterior pasa a _historico/ (si no cambió no se toca). Devuelve
        'nuevo', 'actualizado' o 'sin_cambios'."""
        estado = guardar_version(filepath, csv_data)
        n_filas = csv_data.count(b'\n') - 1
        size_mb = len(csv_data) / (1024 * 1024)
        log.info(f"  ✓ {filepath.name} ({n_filas:,} filas, {size_mb:.1f} MB, {estado})")
        self.stats["ok"] += 1
        self.stats[estado] += 1
        self.stats["filas"] += max(n_filas, 0)
        self.stats["bytes"] += len(csv_data)
        self.stats["archivos"].append(str(filepath))
        return estado

    def _anotar(self, nombre, resultado, filas=None):
        """Anota en _comprobaciones.json el resultado de una consulta: 'nuevo',
        'actualizado', 'sin_cambios', 'vacio' o 'partido' (comprobada) o
        'error' (solo el intento: se vuelve a pedir)."""
        entrada = dict(self.comprobaciones.get(nombre, {}))
        ahora = iso_utc(time.time())
        entrada.update(intento=ahora, resultado=resultado)
        if resultado != "error":
            entrada["comprobado"] = ahora
        if filas is not None:
            entrada["filas"] = filas
        self.comprobaciones[nombre] = entrada
        guardar_comprobaciones(self.comprobaciones)

    # -----------------------------------------------------------------------
    # DESCARGA CON REINTENTOS
    # -----------------------------------------------------------------------
    def _descargar_con_reintentos(self, filepath, label,
                                   fecha_desde="", fecha_hasta="",
                                   tipo_pub=None, entidad=None,
                                   extra_params=None, partir_si_truncado=False):
        """Descarga un CSV con reintentos. Devuelve (estado, n_filas):
          'reciente'  comprobado hace menos de VIGENCIA_HORAS: no se pide;
          'guardado'  guardar_version (nuevo, actualizado o sin cambios);
          'vacio'     0 filas: no se guarda y se conserva la copia anterior;
          'truncado'  llega a UMBRAL_TRUNCADO y partir_si_truncado: no se
                      guarda (son las primeras filas) y el llamante lo parte;
          'error'     falló: se conserva la copia anterior.
        Un CSV truncado que no se puede partir se guarda tal cual (con aviso).
        """
        if es_reciente(filepath, self.comprobaciones):
            log.info(f"    Ya comprobado hace menos de {VIGENCIA_HORAS} h: {filepath.name}, skip")
            self.stats["skip_existe"] += 1
            return "reciente", 0
        if filepath.exists():
            log.info(f"    {filepath.name}: comprobado hace más de {VIGENCIA_HORAS} h, se vuelve "
                     f"a descargar (si cambió, la copia anterior pasa a {HISTORICO}/)")

        for intento in range(MAX_REINTENTOS):
            try:
                csv_data = self._descargar_csv(
                    fecha_desde, fecha_hasta, tipo_pub, entidad, extra_params
                )

                if csv_data:
                    n_filas = csv_data.count(b'\n') - 1

                    if n_filas <= 0:
                        if filepath.exists():
                            log.warning(f"    0 filas: se conserva la descarga "
                                        f"anterior de {filepath.name}")
                        else:
                            log.info("    0 filas, skip")
                        self.stats["skip_vacio"] += 1
                        self._anotar(filepath.name, "vacio", 0)
                        return "vacio", 0

                    if n_filas >= UMBRAL_TRUNCADO:
                        log.warning(f"    ⚠ {n_filas:,} filas — posible "
                                    f"truncamiento para: {label}")
                        if partir_si_truncado:
                            self.stats["partidos"] += 1
                            self._anotar(filepath.name, "partido", n_filas)
                            return "truncado", n_filas

                    estado = self._guardar(csv_data, filepath)
                    self._anotar(filepath.name, estado, n_filas)
                    return "guardado", n_filas

                log.warning(f"    Intento {intento+1}/{MAX_REINTENTOS} sin CSV")

            except Exception as e:
                log.error(f"    Error intento {intento+1}: {e}")

            time.sleep(5 * (intento + 1))
            self._reset_sesion()

        self.stats["error"] += 1
        self._anotar(filepath.name, "error")
        return "error", 0

    # ===================================================================
    # A) CONTRATOS MENORES — por entidad adjudicadora (sin fechas)
    # ===================================================================
    def descargar_menores(self):
        """Descarga contratos menores por entidad adjudicadora."""
        self._obtener_antibot_key()

        if not self.entidades:
            log.error("No se pudieron obtener entidades")
            return

        total = len(self.entidades)
        log.info(f"\n{'='*65}")
        log.info("CONTRATOS MENORES — Por entidad adjudicadora")
        log.info(f"  Entidades: {total}")
        log.info("  Subdivisión automática por rango de importe si >50K")
        log.info(f"{'='*65}")

        self.vigentes = set()
        for i, (val, nombre) in enumerate(self.entidades):
            log.info(f"\n  [{i+1}/{total}] {nombre[:50]}")
            if self._descargar_entidad(val, nombre):
                time.sleep(PAUSA_BASE)

        self._archivar_sustituidos()

    def _descargar_entidad(self, val, nombre):
        """Menores de una entidad: su CSV entero o, si llega a UMBRAL_TRUNCADO,
        por rangos de importe. Devuelve False si ya se descargaba por rangos."""
        if self._entidad_partida(val, nombre):
            # No saltar la entidad entera: si la ejecución anterior se cortó
            # o falló algún rango, hay que completar los que falten
            # (los rangos ya descargados se saltan uno a uno)
            log.info("    Ya subdividido por importe, completando rangos pendientes")
            self._descargar_menores_por_importe(val, nombre)
            return False

        fp = CSV_DIR / nombre_csv_entidad(int(val), nombre)
        estado, _ = self._descargar_con_reintentos(
            fp, nombre[:50],
            tipo_pub="Contratos Menores",
            entidad=val,
            partir_si_truncado=True,
        )

        # ¿Truncado? → subdividir por rango de importe. La respuesta truncada
        # no se ha guardado; una copia anterior del CSV entero (de cuando la
        # entidad no llegaba al límite) se conserva, y al final de una
        # descarga completa se archiva (_archivar_sustituidos)
        if estado == "truncado":
            log.info("    → Subdividiendo por rango de importe...")
            self._descargar_menores_por_importe(val, nombre)
        else:
            self.vigentes.add(fp.name)
        return True

    def _entidad_partida(self, val, nombre):
        """¿La entidad ya se descarga por rangos de importe? (hay CSV de sus
        rangos o su consulta entera se partió)"""
        slug = re.sub(r'[^a-z0-9]+', '_', nombre.lower().strip(' -'))[:30]
        patron_rango = f"menores_ent{int(val):03d}_{slug}_imp"
        if any(f.name.startswith(patron_rango)
               for f in CSV_DIR.glob(f"menores_ent{int(val):03d}_*_imp*.csv")):
            return True
        entera = nombre_csv_entidad(int(val), nombre)
        return self.comprobaciones.get(entera, {}).get("resultado") == "partido"

    def _rango_partido(self, entidad_val, entidad_nombre, imp_desde, imp_hasta, mitades):
        """¿El rango ya se descarga por sus dos mitades? (hay CSV de alguna o su
        consulta se partió). Así no se vuelve a pedir en cada ejecución una
        respuesta truncada que no se guarda."""
        fp = CSV_DIR / nombre_csv_entidad_rango(int(entidad_val), entidad_nombre, imp_desde, imp_hasta)
        if self.comprobaciones.get(fp.name, {}).get("resultado") == "partido":
            return True
        return any((CSV_DIR / nombre_csv_entidad_rango(int(entidad_val), entidad_nombre, d, h)).exists()
                   for d, h in mitades)

    def _descargar_menores_por_importe(self, entidad_val, entidad_nombre,
                                        rangos=None, depth=0):
        """Descarga contratos menores de una entidad subdivididos por importe.
        Si un rango sigue truncado, lo subdivide recursivamente (hasta
        PROFUNDIDAD_MAXIMA; más abajo se guarda tal cual, con aviso)."""
        if rangos is None:
            rangos = RANGOS_IMPORTE

        indent = "      " + "  " * depth
        for imp_desde, imp_hasta in rangos:
            fp = CSV_DIR / nombre_csv_entidad_rango(
                int(entidad_val), entidad_nombre, imp_desde, imp_hasta
            )
            label = f"{entidad_nombre[:30]} imp {imp_desde}-{imp_hasta}"
            log.info(f"{indent}→ Importe {imp_desde}-{imp_hasta}€")

            mitades = sub_rangos(imp_desde, imp_hasta) if depth < PROFUNDIDAD_MAXIMA else None
            if mitades and self._rango_partido(entidad_val, entidad_nombre, imp_desde, imp_hasta, mitades):
                log.info(f"{indent}  Ya subdividido, completando sus mitades")
                self._descargar_menores_por_importe(entidad_val, entidad_nombre, mitades, depth + 1)
                continue

            estado, n_filas = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=entidad_val,
                extra_params={
                    "presupuesto_base_licitacion_total": imp_desde,
                    "presupuesto_base_licitacion_total_1": imp_hasta,
                },
                partir_si_truncado=bool(mitades),
            )

            # Si sigue truncado → partir el rango por la mitad (la respuesta
            # truncada no se ha guardado)
            if estado == "truncado":
                (low, mid), (_, high) = mitades
                log.info(f"{indent}  → Re-subdividiendo {imp_desde}-{imp_hasta} "
                         f"en {low}-{mid} y {mid}-{high}")
                self._descargar_menores_por_importe(
                    entidad_val, entidad_nombre, mitades, depth + 1
                )
            else:
                self.vigentes.add(fp.name)
                if estado == "guardado" and n_filas >= UMBRAL_TRUNCADO:
                    log.warning(f"{indent}  ⚠ Rango {imp_desde}-{imp_hasta} truncado "
                                f"({n_filas:,} filas): no se puede subdividir más, se guarda tal cual")

            time.sleep(PAUSA_BASE)

    def _archivar_sustituidos(self):
        """Tras recorrer todas las entidades: los CSV de menores que no son de
        ninguna consulta de esta ejecución pasan a _historico/ (archivar).
        Pueden ser de otro número de entidad (el desplegable se renumera al
        añadir o quitar entidades) o el CSV entero de una entidad o de un
        rango que ahora se parte. Sus filas siguen en la tabla consolidada,
        con _en_ultima_descarga=False salvo que otra consulta vigente traiga
        el mismo bloque. Si el desplegable trae menos de la mitad de las
        entidades con CSV no se archiva nada (más probable un fallo)."""
        actuales = sorted(p for p in CSV_DIR.glob("menores_*.csv") if not p.name.startswith("."))
        sobran = [p for p in actuales if p.name not in self.vigentes]
        if not sobran:
            return
        conocidas = {p.name.split("_")[1] for p in actuales}
        if 2 * len(self.entidades) < len(conocidas):
            log.warning(f"  ⚠ El desplegable trae {len(self.entidades)} entidades y hay CSV de "
                        f"{len(conocidas)}: no se archiva ningún CSV de menores (más probable un "
                        f"fallo del portal que tantas entidades retiradas)")
            return
        log.info(f"\n  {len(sobran)} CSV de menores ya no son de ninguna consulta (otro número de "
                 f"entidad en el desplegable, o una consulta que ahora se parte por importe): pasan "
                 f"a {HISTORICO}/ y sus filas quedan con _en_ultima_descarga=False salvo que otra "
                 f"consulta las traiga")
        for p in sobran:
            destino = archivar(p)
            self.stats["archivados"] += 1
            log.info(f"    {p.name} → {HISTORICO}/{destino.name}")

    # ===================================================================
    # A2) CONTRATOS MENORES — por ventanas de fecha, sin entidad
    # ===================================================================
    def descargar_menores_por_fecha(self, hoy=None):
        """Menores de todas las entidades, también las que ya no están en el
        desplegable, por ventanas de «Fecha del contrato o encargo» (A2 en el
        docstring). Cada exportación se comprueba contra el recuento del
        portal. Devuelve False si se deja a medias (fallos seguidos)."""
        carpeta = carpeta_fecha()
        carpeta.mkdir(parents=True, exist_ok=True)
        self.comprobaciones_fecha = leer_comprobaciones(carpeta)
        self.hojas_fecha = {}
        self.fallos_seguidos = 0
        ventanas = ventanas_fecha(hoy)

        log.info(f"\n{'='*65}")
        log.info("CONTRATOS MENORES — Por ventanas de fecha del contrato, sin entidad")
        log.info(f"  Ventanas: {len(ventanas)} (una que llegue a {UMBRAL_TRUNCADO:,} se parte en dos)")
        log.info(f"  Directorio: {carpeta}")
        log.info(f"{'='*65}")

        entera = True
        for i, (desde, hasta, nombre) in enumerate(ventanas, 1):
            log.info(f"\n  [{i}/{len(ventanas)}] {fecha_portal(desde)} a {fecha_portal(hasta)}")
            if not self._ventana(desde, hasta, nombre):
                log.error(f"  ✗ {FALLOS_SEGUIDOS_MAX} ventanas seguidas sin respuesta útil del portal: se deja la "
                          f"vía por fecha para la próxima ejecución (no se toca ni se retira nada)")
                entera = False
                break
        self._resumen_fecha()
        if entera:
            self._archivar_ventanas_sustituidas()
        return entera

    def _ventana(self, desde, hasta, nombre):
        """La ventana [desde, hasta]: su CSV o, si se parte, sus dos mitades.
        Devuelve False si hay que dejar la vía (FALLOS_SEGUIDOS_MAX)."""
        if self.fallos_seguidos >= FALLOS_SEGUIDOS_MAX:
            return False
        mitades = mitades_ventana(desde, hasta)
        if mitades and self._ventana_partida(nombre, mitades):
            # No saltarla entera: si la ejecución anterior se cortó, hay que
            # completar las mitades que falten (las comprobadas se saltan)
            log.info(f"    Ya se descarga en dos mitades: {nombre}")
        else:
            estado = self._descargar_ventana(desde, hasta, nombre, partible=bool(mitades))
            if estado != "reciente":
                time.sleep(PAUSA_BASE)
            if estado != "partir":
                return self.fallos_seguidos < FALLOS_SEGUIDOS_MAX
            log.info(f"    → Partiendo {fecha_portal(desde)} a {fecha_portal(hasta)} en dos mitades")
        for d, h in mitades:
            if not self._ventana(d, h, nombre_csv_ventana(d, h)):
                return False
        return True

    def _ventana_partida(self, nombre, mitades):
        """¿La ventana ya se descarga por sus dos mitades? (se partió o hay CSV
        de alguna). Así no se vuelve a pedir en cada ejecución una ventana que
        no cabe en una exportación."""
        if self.comprobaciones_fecha.get(nombre, {}).get("resultado") == "partido":
            return True
        return any((carpeta_fecha() / nombre_csv_ventana(d, h)).exists() for d, h in mitades)

    def _exportar_ventana(self, desde, hasta, exportar_con_tope):
        """Búsqueda de los menores de la ventana (su página trae el recuento) y,
        si hay algo que exportar, su CSV. Devuelve (recuento, bytes o None):
        None si el recuento es 0 o llega al tope y la ventana se puede partir
        (exportar_con_tope=False). Un fallo del portal es una excepción."""
        html = self._buscar(tipo_pub="Contratos Menores", extra_params={
            PARAM_FECHA_DESDE: fecha_portal(desde), PARAM_FECHA_HASTA: fecha_portal(hasta)})
        n = recuento_portal(html)
        if n is None:
            raise ValueError("la página del buscador no trae el recuento ('Mostrando 1 - 10 de N')")
        if n == 0 or (n >= UMBRAL_TRUNCADO and not exportar_con_tope):
            return n, None
        time.sleep(PAUSA_BASE)
        form = self._obtener_form_captcha()
        if not form:
            raise ValueError("sin formulario de exportación (CAPTCHA)")
        time.sleep(1)
        datos = self._post_captcha(form)
        if not datos:
            raise ValueError("la exportación no devolvió un CSV")
        return n, datos

    def _descargar_ventana(self, desde, hasta, nombre, partible):
        """Exporta una ventana y la guarda (guardar_version) si trae tantos
        registros como dice el portal. Devuelve:
          'reciente'    comprobada hace menos de VIGENCIA_HORAS: no se pide;
          'completa'    cuadra y se guarda (nueva, actualizada o sin cambios);
          'vacia'       0 según el portal: no se guarda nada (se conserva la
                        copia anterior, si la hay);
          'partir'      llega al tope, o no cuadra tras MAX_REINTENTOS: el
                        llamante la parte en dos (nada se guarda);
          'incompleta'  de un día y no cuadra (_guardar_incompleta);
          'error'       el portal no responde bien: se conserva lo anterior."""
        fp = carpeta_fecha() / nombre
        fp.parent.mkdir(parents=True, exist_ok=True)
        etiqueta = f"{fecha_portal(desde)} a {fecha_portal(hasta)}"
        if es_reciente(fp, self.comprobaciones_fecha):
            log.info(f"    Ya comprobada hace menos de {VIGENCIA_HORAS} h: {nombre}, skip")
            self.stats["skip_existe"] += 1
            self.hojas_fecha[nombre] = ("reciente", desde, hasta)
            return "reciente"

        desajuste = None      # (recuento, registros, bytes) de la última exportación que no cuadró
        for intento in range(MAX_REINTENTOS):
            try:
                n, datos = self._exportar_ventana(desde, hasta, exportar_con_tope=not partible)
                if n == 0:
                    if fp.exists():
                        log.warning(f"    0 menores según el portal: se conserva la descarga anterior de {nombre}")
                    else:
                        log.info("    0 menores, skip")
                    self.stats["skip_vacio"] += 1
                    self.fallos_seguidos = 0
                    self._anotar_fecha(nombre, "vacio", filas=0, recuento=0)
                    self.hojas_fecha[nombre] = ("vacia", desde, hasta)
                    return "vacia"
                if datos is None:
                    log.info(f"    {n:,} menores: llega al tope de la exportación ({UMBRAL_TRUNCADO:,}), se "
                             f"parte sin exportarla")
                    self.stats["partidos"] += 1
                    self.fallos_seguidos = 0
                    self._anotar_fecha(nombre, "partido", recuento=n, motivo="tope")
                    return "partir"
                registros = self._registros(fp, datos)
                if registros == n:
                    self._guardar_ventana(fp, datos, registros, n)
                    self.fallos_seguidos = 0
                    self.hojas_fecha[nombre] = ("completa", desde, hasta)
                    return "completa"
                desajuste = (n, registros, datos)
                log.warning(f"    Intento {intento+1}/{MAX_REINTENTOS}: el portal cuenta {n:,} menores en "
                            f"{etiqueta} y el CSV trae {registros:,}")
            except Exception as e:  # noqa: BLE001 - se reintenta y, si sigue, se anota
                log.error(f"    Error intento {intento+1}: {e}")
            time.sleep(5 * (intento + 1))
            self._reset_sesion()

        if desajuste is not None:
            n, registros, datos = desajuste
            self.fallos_seguidos = 0
            if partible:
                log.warning(f"    → {etiqueta} no cuadra con el recuento del portal: se parte en dos")
                self.stats["partidos"] += 1
                self._anotar_fecha(nombre, "partido", filas=registros, recuento=n, motivo="incompleta")
                return "partir"
            return self._guardar_incompleta(fp, datos, registros, n, desde, hasta)

        self.stats["error"] += 1
        self.fallos_seguidos += 1
        self._anotar_fecha(nombre, "error")
        self.hojas_fecha[nombre] = ("error", desde, hasta)
        return "error"

    @staticmethod
    def _registros(fp, datos):
        """Registros del CSV `datos` leído como lo leerá unificar (se escribe en
        un temporal junto a `fp`, que se borra)."""
        tmp = fp.with_name(f".{fp.name}.contar")
        try:
            tmp.write_bytes(datos)
            return registros_de_csv(tmp)
        finally:
            if tmp.exists():
                tmp.unlink()

    def _guardar_ventana(self, fp, datos, registros, recuento):
        """Guarda una ventana que cuadra con el recuento (guardar_version)."""
        estado = guardar_version(fp, datos)
        log.info(f"  ✓ {fp.name} ({registros:,} registros, los del portal; "
                 f"{len(datos) / (1024 * 1024):.1f} MB, {estado})")
        self.stats["ok"] += 1
        self.stats[estado] += 1
        self.stats["filas"] += registros
        self.stats["bytes"] += len(datos)
        self.stats["archivos"].append(str(fp))
        self._anotar_fecha(fp.name, estado, filas=registros, recuento=recuento)

    def _guardar_incompleta(self, fp, datos, registros, recuento, desde, hasta):
        """Ventana de un día que no cuadra con el recuento del portal. Nunca
        retira nada: se guarda solo si no hay copia anterior o si la nueva la
        contiene entera (así solo añade); si no, se conserva la anterior. Se
        anota sin 'comprobado': la ejecución siguiente la vuelve a pedir."""
        tmp = fp.with_name(f".{fp.name}.nuevo")
        try:
            tmp.write_bytes(datos)
            if registros == 0:
                log.warning(f"    ⚠ {fp.name}: incompleta (0 de {recuento:,} registros): una descarga sin "
                            f"registros no se guarda")
            elif fp.exists() and not contiene_todo(fp, tmp):
                log.warning(f"    ⚠ {fp.name}: incompleta ({registros:,} de {recuento:,} registros) y le faltan "
                            f"filas de la copia anterior: no la sustituye (se retirarían sin motivo)")
            else:
                estado = guardar_version(fp, desde=tmp)
                log.warning(f"    ⚠ {fp.name}: incompleta ({registros:,} de {recuento:,} registros): se guarda "
                            f"({estado}) porque no retira nada")
        finally:
            if tmp.exists():
                tmp.unlink()
        self.stats["incompletas"] += 1
        self._anotar_fecha(fp.name, "incompleta", filas=registros, recuento=recuento)
        self.hojas_fecha[fp.name] = ("incompleta", desde, hasta)
        return "incompleta"

    def _anotar_fecha(self, nombre, resultado, filas=None, recuento=None, motivo=None):
        """Como _anotar, en el _comprobaciones.json de la vía por fecha y con el
        recuento del portal. 'error' e 'incompleta' no cuentan como comprobada:
        se vuelven a pedir."""
        entrada = dict(self.comprobaciones_fecha.get(nombre, {}))
        ahora = iso_utc(time.time())
        entrada.update(intento=ahora, resultado=resultado)
        if resultado not in ("error", "incompleta"):
            entrada["comprobado"] = ahora
        for clave, valor in (("filas", filas), ("recuento", recuento), ("motivo", motivo)):
            if valor is not None:
                entrada[clave] = valor
            elif clave == "motivo":
                entrada.pop(clave, None)
        self.comprobaciones_fecha[nombre] = entrada
        guardar_comprobaciones(self.comprobaciones_fecha, carpeta_fecha())

    def _archivar_ventanas_sustituidas(self):
        """Tras una pasada completa: un CSV de ventana que ya no es de ninguna
        consulta de esta ejecución (una ventana que ahora se parte en dos) pasa
        a _historico/ (archivar) si todas las ventanas de la pasada que lo
        cubren han llegado completas (su CSV cuadra con el recuento) o el
        portal dice que no tienen ningún menor. Si alguna está incompleta o
        falló, se conserva: sus filas se retirarían por una ventana incompleta."""
        def completa(nombre, estado):
            anotado = self.comprobaciones_fecha.get(nombre, {}).get("resultado")
            return (estado in ("completa", "vacia")
                    or (estado == "reciente" and (anotado in COMPLETAS or anotado == "vacio")))

        for p in sorted(carpeta_fecha().glob("*.csv")):
            if p.name.startswith(".") or p.name in self.hojas_fecha:
                continue
            rango = rango_de_ventana(p.name)
            if rango is None:
                log.warning(f"  {p.name}: no es de ninguna ventana de esta ejecución ni se sabe qué fechas cubre: "
                            f"se conserva")
                continue
            cubren = [(n, e) for n, (e, d, h) in self.hojas_fecha.items() if d <= rango[1] and h >= rango[0]]
            if cubren and all(completa(n, e) for n, e in cubren):
                destino = archivar(p)
                self.stats["archivados"] += 1
                log.info(f"    {p.name} → {HISTORICO}/{destino.name} (sustituido por {len(cubren)} ventanas "
                         f"completas o sin menores)")
            else:
                log.warning(f"  {p.name}: ya no es de ninguna consulta, pero alguna de las ventanas que lo cubren está "
                            f"incompleta o falló: se conserva")

    def _resumen_fecha(self):
        estados = Counter(e for e, _, _ in self.hojas_fecha.values())
        recuento = sum(self.comprobaciones_fecha.get(n, {}).get("recuento") or 0
                       for n, (e, _, _) in self.hojas_fecha.items() if e != "vacia")
        log.info(f"\n  Vía por fecha: {len(self.hojas_fecha)} ventanas ({', '.join(f'{k} {v}' for k, v in sorted(estados.items()))})")
        log.info(f"  Menores según el portal en esas ventanas: {recuento:,} (los de las fronteras, dos veces)")
        pendientes = sorted(n for n, (e, _, _) in self.hojas_fecha.items() if e in ("incompleta", "error"))
        if pendientes:
            log.warning(f"  ⚠ {len(pendientes)} ventanas incompletas o con error (no retiran nada y se vuelven a pedir "
                        f"en la próxima ejecución): {', '.join(pendientes[:20])}")

    # ===================================================================
    # B) OTROS TIPOS — por mes + tipo publicación (con fechas)
    # ===================================================================
    def descargar_otros(self, anio_inicio=ANIO_INICIO, anio_fin=datetime.now().year):
        """Descarga tipos no menores por mes.

        Si se pide la serie completa (desde ANIO_INICIO o antes), lo publicado
        antes de anio_inicio va en un CSV más por tipo, con solo fecha hasta:
        el portal tiene anuncios desde 2014 y ninguna fecha de inicio fija
        garantiza no dejarse los más antiguos.
        """
        segmentos = generar_segmentos_mensuales(anio_inicio, anio_fin)
        if anio_inicio <= ANIO_INICIO:
            segmentos.insert(0, ("", f"31-12-{anio_inicio - 1}", anio_inicio - 1, None))
        total_meses = len(segmentos)
        total_descargas = total_meses * len(TIPOS_NO_MENORES)

        log.info(f"\n{'='*65}")
        log.info("OTROS TIPOS — Por mes + tipo publicación")
        log.info(f"  Período: {anio_inicio}-{anio_fin} ({total_meses} meses)")
        log.info(f"  Tipos: {len(TIPOS_NO_MENORES)}")
        log.info(f"  Descargas estimadas: ~{total_descargas}")
        log.info(f"{'='*65}")

        for i, (desde, hasta, anio, mes) in enumerate(segmentos, 1):
            periodo = f"{mes:02d}/{anio}" if mes else f"hasta {hasta}"
            log.info(f"\n  [{i}/{total_meses}] {periodo}")

            for tipo_pub in TIPOS_NO_MENORES:
                fp = CSV_DIR / (nombre_csv_mes(anio, mes, tipo_pub) if mes
                                else nombre_csv_hasta(anio, tipo_pub))
                label = f"{periodo} {tipo_pub[:30]}"
                log.info(f"    → {tipo_pub[:45]}")

                self._descargar_con_reintentos(
                    fp, label,
                    fecha_desde=desde, fecha_hasta=hasta,
                    tipo_pub=tipo_pub
                )
                time.sleep(PAUSA_BASE)

    # ===================================================================
    # DESCARGA COMPLETA
    # ===================================================================
    def descargar_todo(self, anio_inicio=ANIO_INICIO, anio_fin=datetime.now().year):
        self.t_inicio = time.time()

        log.info("=" * 65)
        log.info("DESCARGA CONTRATACIÓN PÚBLICA - COMUNIDAD DE MADRID v4")
        log.info(f"  Portal: {BASE_URL}")
        log.info(f"  Directorio: {CSV_DIR}")
        log.info("=" * 65)

        # Fase 1: Contratos menores por entidad
        self.descargar_menores()

        # Fase 1b: Contratos menores por ventanas de fecha, sin entidad (los de
        # las entidades que ya no están en el desplegable). Un fallo inesperado
        # de esta vía no para las demás: lo que ya había no se toca
        try:
            self.descargar_menores_por_fecha()
        except Exception:  # noqa: BLE001 - se registra y se sigue con los otros tipos
            log.exception("  ✗ La vía por fecha falló: se sigue con los otros tipos (no se toca ni se retira nada)")
            self.stats["error"] += 1

        # Fase 2: Otros tipos por mes
        self.descargar_otros(anio_inicio, anio_fin)

        self._resumen()

    def descargar_fechas(self):
        """Solo la vía por fecha (modo 'fechas'), con su resumen."""
        self.t_inicio = time.time()
        self.descargar_menores_por_fecha()
        self._resumen()

    def descargar_prueba(self):
        """Test: un hospital grande (menores, con subdivisión recursiva)."""
        self.t_inicio = time.time()
        self._obtener_antibot_key()

        log.info("=" * 65)
        log.info("PRUEBA v4 — Con subdivisión recursiva por importe")
        log.info("=" * 65)

        # Test menores: Hospital Gregorio Marañón (entidad 38) — truncará y subdividirá
        if self.entidades:
            val, nombre = "38", "Hospital General Universitario Gregorio Marañón"
            for v, n in self.entidades:
                if v == "38":
                    nombre = n
                    break

            log.info(f"\n  [MENORES] {nombre[:50]}")
            # Sin _archivar_sustituidos: solo una entidad, no todo el desplegable
            self._descargar_entidad(val, nombre)

        self._resumen()

    def _resumen(self):
        elapsed = time.time() - self.t_inicio
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        total_mb = self.stats["bytes"] / (1024 * 1024)

        log.info(f"\n{'='*65}")
        log.info("RESUMEN")
        log.info(f"  Descargas OK:    {self.stats['ok']} (nuevos {self.stats['nuevo']}, "
                 f"actualizados {self.stats['actualizado']}, sin cambios {self.stats['sin_cambios']})")
        log.info(f"  Errores:         {self.stats['error']}")
        log.info(f"  Ya comprobados:  {self.stats['skip_existe']}")
        log.info(f"  Vacíos:          {self.stats['skip_vacio']}")
        log.info(f"  Partidos:        {self.stats['partidos']}")
        log.info(f"  Incompletas:     {self.stats['incompletas']}")
        log.info(f"  Archivados:      {self.stats['archivados']}")
        log.info(f"  Filas totales:   {self.stats['filas']:,}")
        log.info(f"  Tamaño total:    {total_mb:.1f} MB")
        log.info(f"  Archivos:        {len(self.stats['archivos'])}")
        log.info(f"  Tiempo:          {mins}m {secs}s")
        log.info("=" * 65)


# ---------------------------------------------------------------------------
# UNIFICACIÓN DE CSVs: todas las versiones, sin perder nada (ver HISTÓRICO)
# ---------------------------------------------------------------------------
TIPO = "Tipo de Publicación"
MENORES = "Contratos menores"
ARCHIVO = "_archivo_fuente"
# Firma del bloque de cada fila mientras se acumula (no sale en la tabla)
BLOQUE = "_bloque"
# Columnas del script: al final de la tabla y en este orden
PROPIAS = (ARCHIVO, *COLUMNAS_META, "_origen")
# <nombre>__<sello>[_N] de una versión en _historico/ (archivar() añade _N si
# dos versiones tienen el mismo sello)
PATRON_VERSION = re.compile(r"(?P<base>.+)__(?P<sello>\d{8}T\d{6}Z)(?:_\d+)?")
# Clave estable de la semilla (ver Semilla en el docstring)
CLAVE_SEMILLA = ["Referencia", "Entidad Adjudicadora"]


def fecha_version(ruta):
    """Fecha (iso_utc) de una versión de un CSV crudo: la de su sello en
    _historico/ o, la vigente, su fecha de modificación, que es la que tendrá
    su sello cuando se archive (la fecha de una versión no cambia)."""
    ruta = Path(ruta)
    if ruta.parent.name == HISTORICO:
        m = PATRON_VERSION.fullmatch(ruta.stem)
        if m:
            return datetime.strptime(m.group("sello"), "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso_utc(ruta.stat().st_mtime)


def ficheros_crudos(raiz=None):
    """{nombre: [(ruta, fecha, vigente)]} de cada CSV de csv_originales/ (o de
    `raiz`: la carpeta de la vía por fecha), de su versión más antigua a la
    vigente, en orden de nombre. Incluye los que ya solo tienen versiones en
    _historico/ (sustituidos: ninguna vigente)."""
    raiz = CSV_DIR if raiz is None else Path(raiz)
    nombres = {p.name for p in raiz.glob("*.csv") if not p.name.startswith(".")}
    carpeta = raiz / HISTORICO
    if carpeta.is_dir():
        for p in carpeta.glob("*.csv"):
            m = PATRON_VERSION.fullmatch(p.stem)
            if m:
                nombres.add(m.group("base") + p.suffix)
    salida = {}
    for nombre in sorted(nombres):
        destino = raiz / nombre
        lista = []
        for ruta in versiones(destino):
            if ruta == destino:
                lista.append((ruta, fecha_version(ruta), True))
                continue
            # el glob 'X__*' de versiones() casaría también con las de 'X__algo'
            m = PATRON_VERSION.fullmatch(ruta.stem)
            if m and m.group("base") == destino.stem:
                lista.append((ruta, fecha_version(ruta), False))
        if lista:
            salida[nombre] = lista
    return salida


def _nombres_unicos(cabecera):
    """Nombres de columna sin repetir, como los deja pandas ('X', 'X.1'...)."""
    vistos, nombres = set(), []
    for nombre in cabecera:
        nuevo, k = nombre, 1
        while nuevo in vistos:
            nuevo = f"{nombre}.{k}"
            k += 1
        vistos.add(nuevo)
        nombres.append(nuevo)
    return nombres


def leer_csv(ruta):
    """CSV del portal como texto sin perder filas ni campos: dtype=str y
    keep_default_na=False (un adjudicatario 'NA' o una referencia 'NULL' son
    texto del portal, no valores vacíos). Si alguna fila trae más campos que
    la cabecera, pandas fallaría o correría columnas: entonces se lee con
    comun.lectura_csv y los campos de más van a columnas _columna_extra_N.
    Devuelve (df, avisos)."""
    ruta = Path(ruta)
    try:
        with warnings.catch_warnings():
            # "Length of header or names does not match": se perderían campos
            warnings.simplefilter("error", pd.errors.ParserWarning)
            # index_col=False: si todas las filas traen un campo de más, pandas
            # tomaría la primera columna como índice y correría las demás
            df = pd.read_csv(ruta, sep=';', encoding='utf-8-sig', dtype=str,
                             keep_default_na=False, index_col=False)
        return df, []
    except pd.errors.EmptyDataError:
        return pd.DataFrame(), []
    except (pd.errors.ParserError, pd.errors.ParserWarning):
        pass
    with open(ruta, encoding='utf-8-sig', newline='') as f:
        filas, literales = registros_csv(f.read(), ';')
    if not filas:
        return pd.DataFrame(), []
    ancho = max(len(fila) for fila in filas)
    nombres = _nombres_unicos(filas[0])
    nombres += [f"_columna_extra_{k}" for k in range(1, ancho - len(nombres) + 1)]
    df = pd.DataFrame([fila + [None] * (ancho - len(fila)) for fila in filas[1:]],
                      columns=nombres, dtype=object)
    extra = [c for c in nombres if c.startswith("_columna_extra_")]
    con_valor = df[extra].fillna("").ne("")
    df = df.drop(columns=[c for c in extra if not con_valor[c].any()])
    avisos = [f"{ruta.name}: {int(con_valor.any(axis=1).sum()):,} filas con más campos que la "
              "cabecera; los campos de más se conservan en columnas _columna_extra_N"]
    if literales:
        avisos.append(f"{ruta.name}: {literales:,} comillas literales al principio de un campo "
                      "(se conservan en el texto)")
    return df, avisos


def inicio_de_bloque(df):
    """Filas que empiezan un bloque: las que traen Tipo de Publicación. Las
    demás continúan el registro anterior (más lotes, adjudicatarios,
    prórrogas, modificaciones)."""
    if TIPO not in df.columns:
        return pd.Series(True, index=df.index)
    return df[TIPO].fillna('').astype(str).str.strip() != ''


def firmas_de_bloque(df, columnas):
    """Firma del bloque de cada fila para acumular(): '' en los bloques de una
    sola fila y, en los de varias, el texto de todas sus filas en orden. Con
    ella una fila de continuación solo casa con la misma fila de un bloque
    idéntico, y un bloque que cambia en cualquier fila no casa entero."""
    bloque = inicio_de_bloque(df).cumsum().to_numpy()
    tam = np.bincount(bloque)[bloque]
    firma = np.full(len(df), "", dtype=object)
    varias = tam > 1
    if varias.any():
        valores = df.loc[varias, columnas].astype(object).to_numpy()
        texto = pd.Series(["\x1f".join("\x00" if pd.isna(v) else str(v) for v in fila)
                           for fila in valores])
        firma[varias] = texto.groupby(bloque[varias]).transform(lambda s: "\x1e".join(s)).to_numpy()
    return firma


def acumular_fichero(nombre, lista):
    """Filas de TODAS las versiones de un CSV crudo (`lista`, de
    ficheros_crudos) en orden cronológico, con acumular() por bloques (ver
    HISTÓRICO). Una versión vacía o ilegible se salta: no retira nada. Si el
    CSV no tiene copia vigente, sus filas quedan con _en_ultima_descarga=False.
    Devuelve None si ninguna versión tiene filas."""
    acumulado = None
    for ruta, fecha, _ in lista:
        try:
            df, avisos = leer_csv(ruta)
        except Exception as e:  # noqa: BLE001 - se avisa y se sigue con las demás versiones
            log.error(f"  {ruta.name}: no se puede leer ({type(e).__name__}: {e}); esta versión no "
                      f"entra en la tabla ni retira nada")
            continue
        for aviso in avisos:
            log.warning(f"  {aviso}")
        if len(df) == 0:
            log.warning(f"  {ruta.name}: sin filas; no se marca nada como retirado")
            continue
        contenido = list(df.columns)
        df[ARCHIVO] = nombre
        df[BLOQUE] = firmas_de_bloque(df, contenido)
        acumulado = acumular(acumulado, df, fecha)
    if acumulado is None:
        return None
    acumulado = acumulado.drop(columns=[BLOQUE])
    if not lista[-1][2]:
        # Sin copia vigente: sustituido por otras consultas (_archivar_sustituidos)
        acumulado["_en_ultima_descarga"] = False
    return acumulado


def _agregar(valores, grupos, funcion):
    """'min' o 'max' por grupo de fechas ISO en texto (None si el grupo no
    tiene ninguna). Se agregan sus códigos ordenados: agrupar el texto
    (object) iría grupo a grupo en Python."""
    codigos, unicos = pd.factorize(pd.Series(valores, dtype=object), sort=True)
    codigos = codigos.astype(np.int64)
    if funcion == "min":
        codigos[codigos < 0] = len(unicos)     # sin fecha: después de todas
    resultado = pd.Series(codigos).groupby(grupos, sort=False).transform(funcion).to_numpy()
    # len(unicos) (min) y -1 (max) son el None del final
    return np.array(list(unicos) + [None], dtype=object)[resultado]


def _posiciones(partes):
    """Primera fila (en la tabla entera) de cada parte, y el total al final."""
    return np.concatenate([[0], np.cumsum([len(p) for _, p in partes])]).astype(np.int64)


def _filas_de_partes(partes, posiciones, filas, columnas):
    """Filas `filas` (posiciones en la tabla entera) de las `columnas`, en
    ese orden, sin juntar la tabla: cada parte es la de un CSV."""
    filas = np.asarray(filas, dtype=np.int64)
    parte = np.searchsorted(posiciones, filas, side="right") - 1
    trozos, orden = [], []
    for i in np.unique(parte):
        cuales = np.flatnonzero(parte == i)
        p = partes[i][1]
        trozos.append(p.iloc[filas[cuales] - posiciones[i]].reindex(columns=columnas))
        orden.append(cuales)
    if not trozos:
        return pd.DataFrame({c: pd.Series(dtype=object) for c in columnas})
    juntas = pd.concat(trozos, ignore_index=True)
    return juntas.iloc[np.argsort(np.concatenate(orden), kind="stable")].reset_index(drop=True)


def quitar_repetidos_entre_ficheros(partes):
    """Quita los bloques de un registro repetidos en otro CSV (consultas
    solapadas: ver HISTÓRICO). partes: [(nombre, filas acumuladas)] de cada
    CSV, en orden de nombre. No se quitan duplicados sin más:
      · dentro de un mismo CSV, bloques idénticos son lo que sirve el portal
        y se conservan;
      · un registro puede ocupar varias filas (las de continuación), que
        suelen ser idénticas entre contratos distintos: se compara el bloque
        entero.
    La n-ésima copia de un bloque en cada CSV (contando primero las presentes
    en su última descarga) es la misma. De ella se queda la primera presente
    por orden de los CSV (o la primera, si ninguna lo está: así queda con
    _en_ultima_descarga=True si alguna lo está), con la _primera_descarga
    mínima y la _ultima_descarga máxima. Se compara columna a columna, sin
    juntar la tabla, y la lista `partes` se modifica en su sitio: cada parte
    filtrada sustituye a la anterior, así la tabla no está dos veces en
    memoria. Devuelve el nº de filas quitadas."""
    for i, (nombre, p) in enumerate(partes):
        if not isinstance(p.index, pd.RangeIndex) or p.index.start != 0:
            partes[i] = (nombre, p.reset_index(drop=True))
    posiciones = _posiciones(partes)
    if posiciones[-1] == 0:
        return 0
    columnas = list(dict.fromkeys(c for _, p in partes for c in p.columns
                                  if c != ARCHIVO and c not in COLUMNAS_META))
    # Identificador de fila: igual solo si todas las columnas son iguales
    fila = np.zeros(posiciones[-1], dtype=np.int64)
    for c in columnas:
        serie = pd.concat([p[c] if c in p.columns else pd.Series([None] * len(p), dtype=object)
                           for _, p in partes], ignore_index=True)
        codigos, valores = pd.factorize(serie, use_na_sentinel=False)
        fila = pd.factorize(fila * (len(valores) + 1) + codigos)[0]
        del serie
    # Bloque de cada fila: empieza en cada registro y en cada CSV
    fichero = np.repeat(np.arange(len(partes)), np.diff(posiciones))
    nuevo = np.concatenate([inicio_de_bloque(p).to_numpy(dtype=bool) for _, p in partes])
    nuevo[0] = True
    nuevo[1:] |= fichero[1:] != fichero[:-1]
    bid = np.cumsum(nuevo) - 1
    primeras = np.flatnonzero(nuevo)
    # Firma del bloque: la de su única fila o la unión de las de todas
    firma = fila.copy()
    varias = np.bincount(bid)[bid] > 1
    if varias.any():
        unidas = pd.Series(fila[varias].astype(str)).groupby(bid[varias]).transform(lambda s: '|'.join(s))
        firma[varias] = pd.factorize(unidas)[0] + fila.max() + 1
    en = np.concatenate([p['_en_ultima_descarga'].to_numpy(dtype=bool) if '_en_ultima_descarga' in p.columns
                         else np.ones(len(p), dtype=bool) for _, p in partes])
    bloques = pd.DataFrame({'f': fichero[primeras], 'firma': firma[primeras], 'en': en[primeras],
                            'orden': np.arange(len(primeras))})
    # n-ésima copia del bloque en su CSV, primero las presentes en su última
    # descarga; de cada (firma, n) sobra todo salvo la primera presente
    bloques = bloques.sort_values(['en', 'orden'], ascending=[False, True], kind='stable')
    bloques['n'] = bloques.groupby(['f', 'firma'], sort=False).cumcount()
    bloques['sobra'] = bloques.duplicated(['firma', 'n'])
    bloques = bloques.sort_values('orden', kind='stable')
    sobra = bloques['sobra'].to_numpy()[bid]
    varias_copias = bloques.duplicated(['firma', 'n'], keep=False).to_numpy()
    if varias_copias.any() and all(c in p.columns for _, p in partes for c in COLUMNAS_META):
        # Fechas de las copias que se juntan (solo las de bloques en varios CSV)
        copias = bloques[varias_copias]
        grupos = [copias['firma'].to_numpy(), copias['n'].to_numpy()]
        cuales = primeras[copias['orden'].to_numpy()]
        filas = np.flatnonzero(varias_copias[bid])
        for columna, funcion in (('_primera_descarga', 'min'), ('_ultima_descarga', 'max')):
            valores = _filas_de_partes(partes, posiciones, cuales, [columna])[columna].to_numpy(dtype=object)
            por_bloque = dict(zip(copias['orden'].to_numpy(), _agregar(valores, grupos, funcion)))
            for i in np.unique(fichero[filas]):
                de_esta = filas[fichero[filas] == i]
                partes[i][1].loc[de_esta - posiciones[i], columna] = [por_bloque[b] for b in bid[de_esta]]
    for i in np.flatnonzero(np.bincount(fichero, weights=sobra, minlength=len(partes)) > 0):
        nombre, p = partes[i]
        partes[i] = (nombre, p.loc[~sobra[posiciones[i]:posiciones[i + 1]]].reset_index(drop=True))
    return int(sobra.sum())


# ---------------------------------------------------------------------------
# VÍA POR FECHA EN LA TABLA: solo entra lo que la vía por entidad no trae
# ---------------------------------------------------------------------------
def _texto(serie):
    """Serie como texto, con la celda nula vacía."""
    return serie.astype(object).where(serie.notna(), "").astype(str)


def claves_de_filas(p):
    """Clave estable de cada fila (CLAVE_SEMILLA unidas por \\x1f), o '' si no
    tiene Referencia (como en la semilla, la celda vacía no es clave)."""
    ref, ent = (_texto(p[c]) if c in p.columns else pd.Series([""] * len(p), index=p.index, dtype=object)
                for c in CLAVE_SEMILLA)
    return (ref + "\x1f" + ent).where(ref != "", "")


def _bloque_de_cada_fila(p):
    """Nº de bloque de cada fila (0 para las de continuación antes de la
    primera cabecera, que no son de ningún registro)."""
    return inicio_de_bloque(p).cumsum().to_numpy()


def _firmas_de_bloques(p, cabeceras):
    """{posición de la cabecera: firma del bloque} de los bloques cuya cabecera
    está en `cabeceras` (máscara): el texto de todas sus filas, con las
    columnas del portal con valor (nombre=valor), así casan dos CSV aunque uno
    traiga una columna de más vacía."""
    bloque = _bloque_de_cada_fila(p)
    elegidos = np.unique(bloque[cabeceras])
    filas = np.flatnonzero(np.isin(bloque, elegidos))
    columnas = sorted(c for c in p.columns if c not in PROPIAS and c != BLOQUE)
    valores = p.iloc[filas][columnas].astype(object).where(p.iloc[filas][columnas].notna(), "").astype(str)
    texto = ["\x1f".join(f"{c}={v}" for c, v in zip(columnas, fila) if v != "") for fila in valores.to_numpy()]
    por_bloque = pd.Series(texto).groupby(bloque[filas]).agg("\x1e".join)
    cabecera = {b: pos for pos, b in zip(np.flatnonzero(cabeceras), bloque[cabeceras])}
    return {cabecera[b]: firma for b, firma in por_bloque.items()}


def _presentes_via_entidad(partes):
    """Claves (pd.Index de texto, sin repetir) de los registros presentes en
    la tabla de la vía por entidad y firmas de sus bloques presentes sin
    Referencia (se comparan por contenido)."""
    claves, firmas = [], set()
    for _, p in partes:
        if len(p) == 0:
            continue
        cab = inicio_de_bloque(p).to_numpy(dtype=bool)
        en = (p["_en_ultima_descarga"].to_numpy(dtype=bool) if "_en_ultima_descarga" in p.columns
              else np.ones(len(p), dtype=bool))
        clave = claves_de_filas(p).to_numpy(dtype=object)
        claves.append(clave[cab & en & (clave != "")])
        sin = cab & en & (clave == "")
        if sin.any():
            firmas.update(_firmas_de_bloques(p, sin).values())
    indice = pd.Index(np.concatenate(claves) if claves else np.array([], dtype=object), dtype=object)
    return indice.unique(), firmas


def _sin_bloques(p, cabeceras):
    """`p` sin los bloques (cabecera y continuaciones) de las `cabeceras`."""
    bloque = _bloque_de_cada_fila(p)
    quitar = np.zeros(bloque.max() + 1 if len(bloque) else 1, dtype=bool)
    quitar[bloque[cabeceras]] = True
    return p.loc[~quitar[bloque]].reset_index(drop=True)


def _una_presente_por_clave(partes):
    """Si una clave queda presente en varios bloques de la vía por fecha (con
    distinto contenido: los idénticos ya se quitaron), sigue presente el de la
    descarga más reciente (a igualdad, el último) y los demás pasan a
    _en_ultima_descarga=False, como una versión anterior. Modifica `partes` y
    devuelve cuántos bloques pasan."""
    trozos = []
    for i, (_, p) in enumerate(partes):
        cab = inicio_de_bloque(p).to_numpy(dtype=bool)
        clave = claves_de_filas(p).to_numpy(dtype=object)
        pos = np.flatnonzero(cab & p["_en_ultima_descarga"].to_numpy(dtype=bool) & (clave != ""))
        trozos.append(pd.DataFrame({"parte": i, "pos": pos, "clave": clave[pos],
                                    "ultima": _texto(p["_ultima_descarga"]).to_numpy(dtype=object)[pos]}))
    if not trozos:
        return 0
    t = pd.concat(trozos, ignore_index=True)
    t = t[t.duplicated("clave", keep=False)]
    if t.empty:
        return 0
    t = t.sort_values(["clave", "ultima", "parte", "pos"], kind="stable")
    pasan = t.drop(t.drop_duplicates("clave", keep="last").index)
    for i, grupo in pasan.groupby("parte"):
        nombre, p = partes[i]
        bloque = _bloque_de_cada_fila(p)
        p = p.copy()
        p.loc[np.isin(bloque, bloque[grupo["pos"].to_numpy()]), "_en_ultima_descarga"] = False
        partes[i] = (nombre, p)
    return len(pasan)


def anadir_via_fecha(partes):
    """Añade a `partes` (la tabla de la vía por entidad, ya sin repetidos) lo
    que traen los CSV de la vía por fecha y ella no (ver «Vía por fecha» en
    HISTÓRICO): no entra un bloque cuya clave tiene una fila presente en la vía
    por entidad, ni uno sin Referencia idéntico a un bloque presente de ella;
    el resto va al final, sin repetidos (quitar_repetidos_entre_ficheros, que
    también deja la copia presente de un bloque que la vía por entidad ya no
    trae) y con una sola fila presente por clave. Devuelve (partes, informe);
    sin CSV de la vía por fecha, (partes, None) sin tocar nada."""
    raiz = carpeta_fecha()
    ficheros = ficheros_crudos(raiz) if raiz.is_dir() else {}
    if not ficheros:
        return partes, None
    claves, firmas = _presentes_via_entidad(partes)
    informe = {"csv": 0, "filas_leidas": 0, "clave_en_via_entidad": 0, "contenido_en_via_entidad": 0}
    nuevas = []
    for nombre, lista in ficheros.items():
        acumulado = acumular_fichero(nombre, lista)
        if acumulado is None:
            continue
        informe["csv"] += 1
        informe["filas_leidas"] += len(acumulado)
        cab = inicio_de_bloque(acumulado).to_numpy(dtype=bool)
        clave = claves_de_filas(acumulado).to_numpy(dtype=object)
        por_clave = np.zeros(len(acumulado), dtype=bool)
        con = np.flatnonzero(cab & (clave != ""))
        if len(con) and len(claves):
            por_clave[con] = claves.get_indexer(pd.Index(clave[con], dtype=object)) >= 0
        por_contenido = np.zeros(len(acumulado), dtype=bool)
        sin = cab & (clave == "")
        if sin.any() and firmas:
            for pos, firma in _firmas_de_bloques(acumulado, sin).items():
                por_contenido[pos] = firma in firmas
        informe["clave_en_via_entidad"] += int(por_clave.sum())
        informe["contenido_en_via_entidad"] += int(por_contenido.sum())
        quedan = _sin_bloques(acumulado, por_clave | por_contenido)
        if len(quedan):
            nuevas.append((nombre, quedan))
    filas_entidad = sum(len(p) for _, p in partes)
    todas = partes + nuevas
    informe["repetidas"] = quitar_repetidos_entre_ficheros(todas) if nuevas else 0
    # Bloques que la vía por entidad ya no trae y la vía por fecha sí (idénticos): la copia presente
    informe["sustituidas_en_via_entidad"] = filas_entidad - sum(len(p) for _, p in todas[:len(partes)])
    via_fecha = todas[len(partes):]
    informe["versiones_anteriores"] = _una_presente_por_clave(via_fecha)
    todas[len(partes):] = via_fecha
    informe["anadidas"] = sum(len(p) for _, p in via_fecha)
    return todas, informe


def ordenar_columnas(columnas):
    """Columnas del portal (y _columna_extra_N) y después las del script, en el
    orden de PROPIAS."""
    propias = [c for c in PROPIAS if c in columnas]
    return [c for c in columnas if c not in propias] + propias


def _escribir(destino, escribir):
    """Escribe una salida en un temporal y la entrega a guardar_version."""
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        escribir(tmp)
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


def escribir_salidas(partes, columnas):
    """Escribe la tabla (las partes, una tras otra, con `columnas`) en CSV
    (';', utf-8-sig, como siempre) y en Parquet (texto; _en_ultima_descarga
    booleana), las dos con guardar_version: la anterior pasa a _historico/ y,
    si no cambian, no se tocan. Se escriben por partes: la tabla no se junta
    en memoria. Devuelve {nombre: estado}."""
    def a_csv(tmp):
        with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
            for k, (_, p) in enumerate(partes):
                p.reindex(columns=columnas).to_csv(f, index=False, sep=';', header=k == 0)

    def a_parquet(tmp):
        esquema = pa.schema([(c, pa.bool_() if c == "_en_ultima_descarga" else pa.string()) for c in columnas])

        def escribir(escritor, lote):
            # combine_chunks: los mismos bytes con pandas 2 y 3 (con pandas 3
            # las columnas llegan troceadas y el diccionario de Parquet se
            # desbordaría en otra fila)
            escritor.write_table(pa.concat_tables(lote).combine_chunks())

        with pq.ParquetWriter(tmp, esquema, compression="snappy") as escritor:
            lote, filas = [], 0
            for _, p in partes:
                p = p.reindex(columns=columnas)
                lote.append(pa.table({c: pa.array(p[c].to_numpy(dtype=bool) if c == "_en_ultima_descarga" else p[c],
                                                   type=esquema.field(c).type, from_pandas=True)
                                      for c in columnas}, schema=esquema))
                filas += len(p)
                if filas >= FILAS_POR_GRUPO:
                    escribir(escritor, lote)
                    lote, filas = [], 0
            if lote:
                escribir(escritor, lote)

    return {SALIDA_CSV: _escribir(OUTPUT_DIR / SALIDA_CSV, a_csv),
            SALIDA_PARQUET: _escribir(OUTPUT_DIR / SALIDA_PARQUET, a_parquet)}


# ---------------------------------------------------------------------------
# SEMILLA: el parquet publicado como la instantánea más antigua
# ---------------------------------------------------------------------------
def _filas_parquet(pf, filas, columnas):
    """Filas `filas` (posiciones) de las `columnas` de un Parquet, en ese
    orden, leyéndolo por lotes (sin cargarlo entero)."""
    unicas, inversa = np.unique(np.asarray(filas, dtype=np.int64), return_inverse=True)
    partes, leidas = [], 0
    for lote in pf.iter_batches(batch_size=250_000, columns=columnas):
        i, j = np.searchsorted(unicas, [leidas, leidas + lote.num_rows])
        if i < j:
            partes.append(pa.Table.from_batches([lote]).take(pa.array(unicas[i:j] - leidas)))
        leidas += lote.num_rows
    if not partes:
        return pd.DataFrame({c: pd.Series(dtype=object) for c in columnas})
    return pa.concat_tables(partes).to_pandas().iloc[inversa.ravel()].reset_index(drop=True)


def _codigos_clave(partes, semilla):
    """Columnas de CLAVE_SEMILLA de la tabla (las partes) y de la semilla como
    códigos enteros comunes (Int64): el mismo texto, el mismo código. La
    celda vacía es nula: una fila sin Referencia tiene la clave incompleta y
    se compara por contenido. Comparar códigos y no texto ahorra memoria (la
    semilla tiene 2,5 millones de filas). Devuelve (claves de la tabla,
    claves de la semilla)."""
    nuevos, antiguos = {}, {}
    for c in CLAVE_SEMILLA:
        serie = pd.concat([p[c] if c in p.columns else pd.Series([None] * len(p), dtype=object)
                           for _, p in partes] + [semilla[c]], ignore_index=True)
        vacia = (serie.isna() | (serie == "")).to_numpy(dtype=bool)
        codigos = pd.arrays.IntegerArray(pd.factorize(serie)[0].astype(np.int64), vacia)
        n = len(serie) - len(semilla)
        nuevos[c], antiguos[c] = codigos[:n], codigos[n:]
        del serie
    return pd.DataFrame(nuevos), pd.DataFrame(antiguos)


def sembrar_publicado(partes, ruta, origen, consultas):
    """Incorpora el parquet publicado `ruta` como la instantánea más antigua
    (ver Semilla en el docstring). Añade, en una parte más al final, con
    _origen y _en_ultima_descarga=False, sus filas del ámbito de la ejecución
    cuya clave (CLAVE_SEMILLA) no está en `partes` (la tabla con las
    semillas anteriores). Las filas de `partes` no se tocan. consultas:
    nombres de los CSV descargados o comprobados. Devuelve (partes, informe
    de comun.historico)."""
    ruta = Path(ruta)
    pf = pq.ParquetFile(ruta)
    nombres = pf.schema_arrow.names
    faltan = [c for c in CLAVE_SEMILLA + [TIPO] if c not in nombres]
    if faltan:
        raise ValueError(f"La semilla {ruta} no tiene las columnas {faltan}")
    # El publicado v2026.02 (sin columnas de control) guarda las celdas vacías
    # como el texto 'nan'
    publicado = "_en_ultima_descarga" not in nombres
    del_portal = [c for c in nombres if c not in PROPIAS]

    def leer(columnas, filas=None, vaciadas=None):
        """Columnas de la semilla (todas las filas o las `filas`), con las
        celdas 'nan' del publicado vacías; cuenta las vaciadas si se pide."""
        tabla = pf.read(columns=columnas).to_pandas() if filas is None else _filas_parquet(pf, filas, columnas)
        if publicado:
            for c in columnas:
                if c in del_portal:
                    nan = tabla[c] == "nan"
                    if vaciadas is not None:
                        vaciadas.append(int(nan.sum()))
                    tabla[c] = tabla[c].mask(nan, "")
        return tabla

    posiciones = _posiciones(partes)
    base = leer(CLAVE_SEMILLA + [TIPO] + ([ARCHIVO] if ARCHIVO in nombres else []))
    claves_n, claves_s = _codigos_clave(partes, base)
    # Ámbito: un menor, si su entidad tiene menores en la tabla; lo demás, si
    # su CSV (consulta por mes y tipo) se ha descargado o comprobado
    entidades = set()
    for _, p in partes:
        if TIPO in p.columns and "Entidad Adjudicadora" in p.columns:
            entidades.update(p.loc[p[TIPO] == MENORES, "Entidad Adjudicadora"])
    es_menor = (base[TIPO] == MENORES).to_numpy()
    por_csv = (base[ARCHIVO].isin(consultas).to_numpy() if ARCHIVO in base.columns
               else np.zeros(len(base), dtype=bool))
    en_ambito = np.where(es_menor, base["Entidad Adjudicadora"].isin(entidades).to_numpy(), por_csv)
    del base
    en_tabla = set(c for _, p in partes for c in p.columns)
    contenido = [c for c in del_portal if c in en_tabla and c not in CLAVE_SEMILLA]

    def de_la_tabla(filas):
        return _filas_de_partes(partes, posiciones, filas, contenido)

    motivo = np.empty(len(claves_s), dtype=object)
    sin_clave = claves_s.isna().all(axis=1).to_numpy()
    # Con alguna columna de la clave: frente a toda la tabla
    con = np.flatnonzero(~sin_clave)
    motivo[con] = seleccionar_semilla(
        claves_n, claves_s.iloc[con].reset_index(drop=True), de_la_tabla,
        lambda filas: leer(contenido, con[filas]), en_ambito[con])
    # Sin ninguna (filas de continuación): solo frente a las filas de la tabla
    # que tampoco la tienen (con toda la tabla sería lentísimo)
    sin = np.flatnonzero(sin_clave)
    if len(sin):
        sin_n = np.flatnonzero(claves_n.isna().all(axis=1).to_numpy())
        motivo[sin] = seleccionar_semilla(
            claves_n.iloc[sin_n].reset_index(drop=True), claves_s.iloc[sin].reset_index(drop=True),
            lambda filas: de_la_tabla(sin_n[filas]),
            lambda filas: leer(contenido, sin[filas]), en_ambito[sin])

    # Los ejemplos del informe, con la clave en texto (solo se leen esas filas)
    informe = informe_semilla(motivo, origen, lambda filas: leer(CLAVE_SEMILLA, filas))
    informe["ruta"] = str(ruta)
    fuera = motivo == FUERA_AMBITO
    informe["fuera_ambito_detalle"] = {
        "menores de entidades sin menores en la tabla": int((fuera & es_menor).sum()),
        "anuncios de CSV (mes y tipo) no descargados": int((fuera & ~es_menor).sum())}

    anadir = np.flatnonzero(motivo == ANADIDA)
    if len(anadir) == 0:
        return partes, informe
    vaciadas = []
    nuevas = leer(nombres, anadir, vaciadas)
    informe["celdas_nan_vaciadas"] = sum(vaciadas)
    for c in nuevas.columns:
        # todo texto (como las columnas de la descarga), salvo la marca booleana
        if c != "_en_ultima_descarga":
            valores = nuevas[c].astype(object)
            nuevas[c] = valores.where(valores.isna(), valores.astype(str))
    propio = nuevas["_origen"] if "_origen" in nuevas.columns else pd.Series([None] * len(nuevas), dtype=object)
    nuevas["_origen"] = propio.astype(object).where(propio.notna(), origen)
    nuevas["_en_ultima_descarga"] = False
    return partes + [(f"semilla {ruta.name}", nuevas)], informe


def unificar_csvs(semillas=(), origen=ORIGEN_SEMILLA):
    """Tabla consolidada desde TODAS las versiones de todos los CSV descargados
    (ver HISTÓRICO), con las semillas (--semilla) si se dan, en su orden.
    Escribe el CSV y el Parquet y devuelve un resumen {'filas', 'retiradas',
    'salidas': {nombre: estado}, 'semillas': [informes], 'via_fecha':
    informe de anadir_via_fecha o None}, o None si no hay nada que unificar
    (entonces no se escribe nada)."""
    log.info("Unificando CSVs...")
    semillas = [Path(s) for s in semillas]
    faltan = [str(s) for s in semillas if not s.is_file()]
    if faltan:
        log.error(f"No existen las semillas {faltan}: no se unifica nada")
        return None
    ficheros = ficheros_crudos()
    if not ficheros:
        log.error("No hay CSVs para unificar")
        return None

    partes = []
    for nombre, lista in ficheros.items():
        acumulado = acumular_fichero(nombre, lista)
        if acumulado is None:
            continue
        retiradas = int((~acumulado["_en_ultima_descarga"].astype(bool)).sum())
        detalle = ""
        if len(lista) > 1 or retiradas:
            detalle = (f" ({len(lista)} versiones; {retiradas:,} ya no están en la última"
                       f"{'' if lista[-1][2] else ': CSV sustituido por otras consultas'})")
        log.info(f"  {nombre}: {len(acumulado):,} filas{detalle}")
        partes.append((nombre, acumulado))
    if not partes:
        log.error("No se cargó ningún CSV")
        return None

    # Solo sobra un registro repetido en dos CSV (frontera entre rangos de
    # importe, entidades que incluyen a sus dependientes, CSV sustituidos).
    # No por Nº Expediente + Referencia + Entidad (colapsaba lotes y todas las
    # filas de continuación) ni por filas idénticas (se perdían duplicados que
    # sirve el portal y continuaciones de otros contratos).
    quitadas = quitar_repetidos_entre_ficheros(partes)
    if quitadas:
        log.info(f"  Eliminadas {quitadas:,} filas de registros repetidos en dos CSV")

    # Vía por fecha: al final, solo lo que la vía por entidad no trae
    partes, via_fecha = anadir_via_fecha(partes)
    if via_fecha:
        log.info(f"  Vía por fecha: {via_fecha['csv']:,} CSV de ventana, {via_fecha['filas_leidas']:,} filas; no "
                 f"entran {via_fecha['clave_en_via_entidad']:,} con la clave presente en la vía por entidad ni "
                 f"{via_fecha['contenido_en_via_entidad']:,} sin Referencia idénticas a una suya; "
                 f"{via_fecha['repetidas']:,} repetidas (fronteras de las ventanas y copias presentes de "
                 f"{via_fecha['sustituidas_en_via_entidad']:,} filas que la vía por entidad ya no trae); "
                 f"{via_fecha['versiones_anteriores']:,} bloques con la clave presente en otra ventana más "
                 f"reciente pasan a versión anterior; entran {via_fecha['anadidas']:,}")

    informes = []
    if semillas:
        consultas = set(ficheros) | {n for n, e in leer_comprobaciones().items() if e.get("comprobado")}
        for ruta in semillas:
            partes, informe = sembrar_publicado(partes, ruta, origen, consultas)
            imprimir_informe_semilla(informe)
            if informe.get("celdas_nan_vaciadas"):
                log.info(f"   {informe['celdas_nan_vaciadas']:,} celdas 'nan' del publicado se dejan "
                         f"vacías, como las sirve el portal")
            informes.append(informe)

    columnas = ordenar_columnas(list(dict.fromkeys(c for _, p in partes for c in p.columns)))
    estados = escribir_salidas(partes, columnas)
    filas = sum(len(p) for _, p in partes)
    retiradas = sum(int((~p["_en_ultima_descarga"].astype(bool)).sum()) for _, p in partes)
    for nombre, estado in estados.items():
        salida = OUTPUT_DIR / nombre
        log.info(f"\n✓ {salida} ({estado}, {salida.stat().st_size / (1024 * 1024):.1f} MB)")
    log.info(f"  {filas:,} filas; {retiradas:,} ya no están en la última descarga")
    log.info(f"  Columnas: {columnas}")
    return {"filas": filas, "retiradas": retiradas, "salidas": estados, "semillas": informes, "via_fecha": via_fecha}


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
USO = """
Descarga de Contratación Pública - Comunidad de Madrid v4
===========================================================

Uso:
  python script.py prueba           → Test: 1 entidad + 1 mes
  python script.py menores          → Solo contratos menores (por entidad)
  python script.py fechas           → Solo contratos menores por ventanas de fecha
                                      del contrato, sin entidad (también los de
                                      entidades que ya no están en el desplegable)
  python script.py otros            → Solo otros tipos (por mes, 2017-año actual
                                      + lo publicado antes de 2017)
  python script.py otros 2020 2025  → Otros tipos, período parcial
  python script.py todo             → Todo: menores (por entidad y por fecha) + otros
  python script.py unificar         → Une todas las versiones de los CSVs en la
                                      tabla consolidada (CSV + Parquet)
  python script.py unificar --semilla contratacion_comunidad_madrid_completo.parquet
                                    → Además, lo que tenía el publicado (release
                                      v2026.02) y ya no está (por Referencia +
                                      Entidad Adjudicadora); repetible

Estrategia:
  Contratos menores: por entidad adjudicadora (126), sin fechas, y por ventanas
    de un mes de la fecha del contrato, sin entidad (solo entra en la tabla lo
    que no trae la vía por entidad)
  Otros tipos: por mes + tipo publicación, con fechas

Directorio de salida: comunidad_madrid/csv_originales/ (versiones anteriores
en csv_originales/_historico/); la vía por fecha, en
csv_originales/por_fecha/
"""

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(usage=USO, add_help=False)
    parser.add_argument("modo", nargs="?", default="")
    parser.add_argument("anios", nargs="*", type=int)
    parser.add_argument("--semilla", action="append", type=Path, default=[])
    args = parser.parse_args()
    if args.semilla and args.modo != "unificar":
        parser.error("--semilla solo se usa con unificar")
    a1 = args.anios[0] if len(args.anios) > 0 else ANIO_INICIO
    a2 = args.anios[1] if len(args.anios) > 1 else datetime.now().year

    if args.modo == "prueba":
        DescargadorComunidadMadrid().descargar_prueba()

    elif args.modo == "menores":
        DescargadorComunidadMadrid().descargar_menores()

    elif args.modo == "fechas":
        DescargadorComunidadMadrid().descargar_fechas()

    elif args.modo == "otros":
        DescargadorComunidadMadrid().descargar_otros(a1, a2)

    elif args.modo == "todo":
        DescargadorComunidadMadrid().descargar_todo(a1, a2)

    elif args.modo == "unificar":
        if unificar_csvs(args.semilla) is None:
            sys.exit(1)

    else:
        print(USO)
