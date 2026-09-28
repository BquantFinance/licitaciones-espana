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

    A) CONTRATOS MENORES (99% del volumen, ~4.5M):
       - Fecha hasta NO funciona, fecha desde rompe combinada con entidad
       - Solución: descargar por ENTIDAD ADJUDICADORA (las del desplegable)
       - Sin filtro de fecha; si una entidad llega a UMBRAL se subdivide por
         rango de presupuesto (incluidos ≤0 y ≥50.000, que también existen)
       - Ojo: los menores de entidades que ya no están en el desplegable
         (consejerías de legislaturas anteriores...) no se descargan

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
        contando primero las presentes en la última descarga. Cada copia se
        queda con _en_ultima_descarga=True si algún CSV la trae en su última
        versión, la _primera_descarga mínima y la _ultima_descarga máxima.
      - Salidas: contratacion_comunidad_madrid_completo.csv (';', utf-8-sig)
        y .parquet (texto; _en_ultima_descarga booleana), las dos con
        guardar_version: la anterior va a _historico/, y si no cambian no se
        tocan. Columnas: las del portal, _archivo_fuente, _primera_descarga,
        _ultima_descarga, _en_ultima_descarga y, con --semilla, _origen.
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
        ni se duplica una fila descargada. Las filas sin Referencia
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
from datetime import datetime, timezone
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (ANADIDA, COLUMNAS_META, HISTORICO, ORIGEN_SEMILLA, acumular,  # noqa: E402
                             archivar, guardar_version, imprimir_informe_semilla, informe_semilla,
                             seleccionar_semilla, versiones)
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


def leer_comprobaciones():
    """{nombre del CSV: {'comprobado', 'intento', 'resultado', 'filas'}} de
    csv_originales/_comprobaciones.json. Si no existe o no se puede leer se
    empieza de cero: solo sirve para no repetir comprobaciones recientes y
    para el ámbito de la semilla."""
    ruta = CSV_DIR / COMPROBACIONES
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


def guardar_comprobaciones(comprobaciones):
    """Escribe _comprobaciones.json de forma atómica."""
    ruta = CSV_DIR / COMPROBACIONES
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
            "archivados": 0, "filas": 0, "bytes": 0, "archivos": [],
        }
        self.t_inicio = None
        self.comprobaciones = leer_comprobaciones()
        # CSV de menores de las consultas de esta ejecución (las que no se
        # parten): al terminar todas, los demás CSV de menores se archivan
        self.vigentes = set()

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
        log.info(f"  Subdivisión automática por rango de importe si >50K")
        log.info(f"{'='*65}")

        self.vigentes = set()
        for i, (val, nombre) in enumerate(self.entidades):
            fp = CSV_DIR / nombre_csv_entidad(int(val), nombre)
            label = f"{nombre[:50]}"
            log.info(f"\n  [{i+1}/{total}] {label}")

            if self._entidad_partida(val, nombre):
                # No saltar la entidad entera: si la ejecución anterior se cortó
                # o falló algún rango, hay que completar los que falten
                # (los rangos ya descargados se saltan uno a uno)
                log.info("    Ya subdividido por importe, completando rangos pendientes")
                self._descargar_menores_por_importe(val, nombre)
                continue

            estado, n_filas = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=val,
                partir_si_truncado=True,
            )

            # ¿Truncado? → subdividir por rango de importe. La respuesta
            # truncada no se ha guardado; una copia anterior del CSV entero
            # (de cuando la entidad no llegaba al límite) se conserva y se
            # archiva al final (_archivar_sustituidos)
            if estado == "truncado":
                log.info("    → Subdividiendo por rango de importe...")
                self._descargar_menores_por_importe(val, nombre)
            else:
                self.vigentes.add(fp.name)

            time.sleep(PAUSA_BASE)

        self._archivar_sustituidos()

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

        # Fase 2: Otros tipos por mes
        self.descargar_otros(anio_inicio, anio_fin)

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

            fp = CSV_DIR / nombre_csv_entidad(int(val), nombre)
            label = f"{nombre[:50]}"
            log.info(f"\n  [MENORES] {label}")

            estado, _ = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=val,
                partir_si_truncado=True,
            )

            if estado == "truncado":
                log.info("    → Subdividiendo recursivamente por importe...")
                self._descargar_menores_por_importe(val, nombre)

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
        log.info(f"  Archivados:      {self.stats['archivados']}")
        log.info(f"  Filas totales:   {self.stats['filas']:,}")
        log.info(f"  Tamaño total:    {total_mb:.1f} MB")
        log.info(f"  Archivos:        {len(self.stats['archivos'])}")
        log.info(f"  Tiempo:          {mins}m {secs}s")
        log.info("=" * 65)


# ---------------------------------------------------------------------------
# UNIFICACIÓN DE CSVs
# ---------------------------------------------------------------------------
def filas_repetidas_entre_ficheros(df):
    """Máscara de las filas cuyo registro ya ha salido en otro CSV.

    Los CSV se solapan: un menor con presupuesto justo en la frontera entre
    dos rangos sale en los dos (y el rango "hasta 0" repite los de 0 €). Pero
    no hay que quitar duplicados sin más:
      · dentro de un mismo CSV, filas idénticas son lo que sirve el portal y
        se conservan;
      · un registro puede ocupar varias filas: las que vienen sin "Tipo de
        Publicación" son la continuación del anterior (más lotes o
        adjudicatarios, prórrogas, modificaciones) y suelen ser idénticas
        entre contratos distintos ("...;0,00;0,00;0,00;0,00").
    Así que se compara el bloque entero (registro + continuaciones) y de cada
    bloque repetido en varios CSV se dejan tantas copias como tenga el CSV que
    más tiene (el primero en orden de nombre).
    """
    cols = [c for c in df.columns if c != '_archivo_fuente']
    fichero = df['_archivo_fuente']
    if 'Tipo de Publicación' in df.columns:
        cabecera = df['Tipo de Publicación'].fillna('').astype(str).str.strip() != ''
    else:
        cabecera = pd.Series(True, index=df.index)
    # Identificador de fila: igual solo si todas las columnas son iguales
    fila = np.zeros(len(df), dtype=np.int64)
    for c in cols:
        codigos, valores = pd.factorize(df[c], use_na_sentinel=False)
        fila = pd.factorize(fila * (len(valores) + 1) + codigos)[0]
    claves = pd.DataFrame({
        'f': fichero,
        'b': cabecera.astype(int).groupby(fichero, sort=False).cumsum(),
        'firma': pd.Series(fila, index=df.index).astype(str),
    })
    grupos = ['f', 'b']
    # Firma del bloque: la de su única fila o la unión de las de todas
    varias = claves.groupby(grupos, sort=False)['firma'].transform('size') > 1
    if varias.any():
        claves.loc[varias, 'firma'] = claves[varias].groupby(
            grupos, sort=False)['firma'].transform(lambda s: '|'.join(s))
    inicio = ~claves.duplicated(grupos)
    bloques = claves[inicio].copy()
    # n-ésima aparición del bloque dentro de su CSV: la 2ª copia de un CSV solo
    # sobra si otro CSV anterior ya tenía también dos
    bloques['n'] = bloques.groupby(['f', 'firma'], sort=False).cumcount()
    claves['sobra'] = False
    claves.loc[bloques.index, 'sobra'] = bloques.duplicated(['firma', 'n'])
    return claves.groupby(grupos, sort=False)['sobra'].transform('first').astype(bool)


def unificar_csvs():
    """Une todos los CSVs descargados en un único archivo."""
    log.info("Unificando CSVs...")
    csvs = sorted(CSV_DIR.glob("*.csv"))
    if not csvs:
        log.error("No hay CSVs para unificar")
        return

    dfs = []
    for csv_path in csvs:
        try:
            # keep_default_na=False: un adjudicatario "NA" o una referencia
            # "NULL" son texto del portal, no valores vacíos
            df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig',
                             dtype=str, keep_default_na=False,
                             on_bad_lines='skip')
            if len(df) > 0:
                df['_archivo_fuente'] = csv_path.name
                dfs.append(df)
                log.info(f"  {csv_path.name}: {len(df):,} filas")
        except Exception as e:
            log.error(f"  Error leyendo {csv_path.name}: {e}")

    if not dfs:
        log.error("No se cargó ningún CSV")
        return

    df_total = pd.concat(dfs, ignore_index=True)

    # Solo sobra un registro repetido en dos CSV (frontera entre rangos de
    # importe). No por Nº Expediente + Referencia + Entidad (colapsaba lotes y
    # todas las filas de continuación) ni por filas idénticas (se perdían
    # duplicados que sirve el portal y continuaciones de otros contratos).
    sobran = filas_repetidas_entre_ficheros(df_total)
    if sobran.any():
        log.info(f"  Eliminadas {int(sobran.sum()):,} filas de registros "
                 f"repetidos en dos CSV")
        df_total = df_total[~sobran]

    salida = OUTPUT_DIR / "contratacion_comunidad_madrid_completo.csv"
    df_total.to_csv(salida, index=False, sep=';', encoding='utf-8-sig')
    size_mb = salida.stat().st_size / (1024 * 1024)
    log.info(f"\n✓ {salida}")
    log.info(f"  {len(df_total):,} filas, {size_mb:.1f} MB")
    log.info(f"  Columnas: {list(df_total.columns)}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else ""

    if modo == "prueba":
        DescargadorComunidadMadrid().descargar_prueba()

    elif modo == "menores":
        DescargadorComunidadMadrid().descargar_menores()

    elif modo == "otros":
        d = DescargadorComunidadMadrid()
        a1 = int(sys.argv[2]) if len(sys.argv) > 2 else ANIO_INICIO
        a2 = int(sys.argv[3]) if len(sys.argv) > 3 else datetime.now().year
        d.descargar_otros(a1, a2)

    elif modo == "todo":
        d = DescargadorComunidadMadrid()
        a1 = int(sys.argv[2]) if len(sys.argv) > 2 else ANIO_INICIO
        a2 = int(sys.argv[3]) if len(sys.argv) > 3 else datetime.now().year
        d.descargar_todo(a1, a2)

    elif modo == "unificar":
        unificar_csvs()

    else:
        print("""
Descarga de Contratación Pública - Comunidad de Madrid v4
===========================================================

Uso:
  python script.py prueba           → Test: 1 entidad + 1 mes
  python script.py menores          → Solo contratos menores (por entidad)
  python script.py otros            → Solo otros tipos (por mes, 2017-año actual
                                      + lo publicado antes de 2017)
  python script.py otros 2020 2025  → Otros tipos, período parcial
  python script.py todo             → Todo: menores + otros
  python script.py unificar         → Une CSVs en archivo único

Estrategia:
  Contratos menores: por entidad adjudicadora (125), sin fechas
  Otros tipos: por mes + tipo publicación, con fechas

Directorio de salida: comunidad_madrid/csv_originales/
        """)