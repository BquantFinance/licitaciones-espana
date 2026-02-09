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
       - Solución: descargar por ENTIDAD ADJUDICADORA (125 entidades)
       - Sin filtro de fecha → cada entidad tiene <50K filas
       - Si alguna entidad supera UMBRAL → no se puede subdividir más

    B) OTROS TIPOS (licitaciones, adjudicaciones, etc., ~36K):
       - Fecha hasta SÍ funciona
       - Descargar por MES + TIPO PUBLICACIÓN (como v3)
       - Período: 2017-2025 (datos empiezan en 2017)

    Columnas CSV (18):
    Tipo de Publicación; Estado; Entidad Adjudicadora; Nº Expediente;
    Referencia; Título del contrato; Tipo de contrato;
    Procedimiento de adjudicación; Presupuesto de licitación;
    Nº de ofertas; Resultado; NIF del adjudicatario; Adjudicatario;
    Fecha del contrato; Importe de adjudicación;
    Importe de las modificaciones; Importe de las prórrogas;
    Importe de la liquidación
=============================================================================
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import time
import pandas as pd
from pathlib import Path
from calendar import monthrange
from datetime import datetime
import logging
import sys

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
BASE_URL = "https://contratos-publicos.comunidad.madrid"
BUSCAR_URL = f"{BASE_URL}/contratos"
CSV_URL = f"{BASE_URL}/buscador-contratos/csv"

OUTPUT_DIR = Path("comunidad_madrid")
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

# Rangos de presupuesto para subdividir entidades truncadas (>50K)
# La mayoría de contratos menores son <100€, necesitamos rangos muy finos abajo
RANGOS_IMPORTE = [
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
            "ok": 0, "error": 0, "skip_existe": 0,
            "skip_vacio": 0, "filas": 0, "bytes": 0,
            "archivos": [],
        }
        self.t_inicio = None

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
        """Guarda CSV, retorna nº de filas."""
        filepath.write_bytes(csv_data)
        n_filas = csv_data.count(b'\n') - 1
        size_mb = len(csv_data) / (1024 * 1024)
        log.info(f"  ✓ {filepath.name} ({n_filas:,} filas, {size_mb:.1f} MB)")
        self.stats["ok"] += 1
        self.stats["filas"] += max(n_filas, 0)
        self.stats["bytes"] += len(csv_data)
        self.stats["archivos"].append(str(filepath))
        return n_filas

    # -----------------------------------------------------------------------
    # DESCARGA CON REINTENTOS
    # -----------------------------------------------------------------------
    def _descargar_con_reintentos(self, filepath, label,
                                   fecha_desde="", fecha_hasta="",
                                   tipo_pub=None, entidad=None,
                                   extra_params=None):
        """Descarga un CSV con reintentos. Retorna (True/False, n_filas)."""
        if filepath.exists() and filepath.stat().st_size > 100:
            log.info(f"    Ya existe: {filepath.name}, skip")
            self.stats["skip_existe"] += 1
            return True, 0

        for intento in range(MAX_REINTENTOS):
            try:
                csv_data = self._descargar_csv(
                    fecha_desde, fecha_hasta, tipo_pub, entidad, extra_params
                )

                if csv_data:
                    n_filas = csv_data.count(b'\n') - 1

                    if n_filas <= 0:
                        log.info(f"    0 filas, skip")
                        self.stats["skip_vacio"] += 1
                        return True, 0

                    if n_filas >= UMBRAL_TRUNCADO:
                        log.warning(f"    ⚠ {n_filas:,} filas — posible "
                                    f"truncamiento para: {label}")

                    self._guardar(csv_data, filepath)
                    return True, n_filas

                log.warning(f"    Intento {intento+1}/{MAX_REINTENTOS} sin CSV")

            except Exception as e:
                log.error(f"    Error intento {intento+1}: {e}")

            time.sleep(5 * (intento + 1))
            self._reset_sesion()

        self.stats["error"] += 1
        return False, 0

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

        for i, (val, nombre) in enumerate(self.entidades):
            fp = CSV_DIR / nombre_csv_entidad(int(val), nombre)
            label = f"{nombre[:50]}"
            log.info(f"\n  [{i+1}/{total}] {label}")

            # Comprobar si ya está subdividido por importe
            slug = re.sub(r'[^a-z0-9]+', '_', nombre.lower().strip(' -'))[:30]
            patron_rango = f"menores_ent{int(val):03d}_{slug}_imp"
            ya_subdividido = any(
                f.name.startswith(patron_rango)
                for f in CSV_DIR.glob(f"menores_ent{int(val):03d}_*_imp*.csv")
            )
            if ya_subdividido:
                log.info(f"    Ya subdividido por importe, skip")
                self.stats["skip_existe"] += 1
                continue

            ok, n_filas = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=val
            )

            # ¿Truncado? → subdividir por rango de importe
            if ok and n_filas >= UMBRAL_TRUNCADO:
                log.info(f"    → Subdividiendo por rango de importe...")
                # Borrar el CSV truncado y revert stats
                if fp.exists():
                    file_size = fp.stat().st_size
                    fp.unlink()
                    self.stats["ok"] -= 1
                    self.stats["filas"] -= n_filas
                    self.stats["bytes"] -= file_size
                    if self.stats["archivos"] and str(fp) == self.stats["archivos"][-1]:
                        self.stats["archivos"].pop()

                self._descargar_menores_por_importe(val, nombre)

            time.sleep(PAUSA_BASE)

    def _descargar_menores_por_importe(self, entidad_val, entidad_nombre,
                                        rangos=None, depth=0):
        """Descarga contratos menores de una entidad subdivididos por importe.
        Si un rango sigue truncado, lo subdivide recursivamente."""
        if rangos is None:
            rangos = RANGOS_IMPORTE
        if depth > 5:
            log.error(f"      ⚠ Profundidad máxima alcanzada, abortando subdivisión")
            return

        indent = "      " + "  " * depth
        for imp_desde, imp_hasta in rangos:
            fp = CSV_DIR / nombre_csv_entidad_rango(
                int(entidad_val), entidad_nombre, imp_desde, imp_hasta
            )
            label = f"{entidad_nombre[:30]} imp {imp_desde}-{imp_hasta}"
            log.info(f"{indent}→ Importe {imp_desde}-{imp_hasta}€")

            ok, n_filas = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=entidad_val,
                extra_params={
                    "presupuesto_base_licitacion_total": imp_desde,
                    "presupuesto_base_licitacion_total_1": imp_hasta,
                }
            )

            # Si sigue truncado → partir el rango por la mitad
            if ok and n_filas >= UMBRAL_TRUNCADO:
                low = int(imp_desde)
                high = int(imp_hasta)
                mid = (low + high) // 2
                if mid <= low or mid >= high:
                    log.warning(f"{indent}  ⚠ Rango {imp_desde}-{imp_hasta} "
                                f"no se puede subdividir más ({n_filas:,} filas)")
                    continue

                log.info(f"{indent}  → Re-subdividiendo {imp_desde}-{imp_hasta} "
                         f"en {low}-{mid} y {mid}-{high}")
                # Borrar CSV truncado
                if fp.exists():
                    file_size = fp.stat().st_size
                    fp.unlink()
                    self.stats["ok"] -= 1
                    self.stats["filas"] -= n_filas
                    self.stats["bytes"] -= file_size
                    if self.stats["archivos"] and str(fp) == self.stats["archivos"][-1]:
                        self.stats["archivos"].pop()

                sub_rangos = [(str(low), str(mid)), (str(mid), str(high))]
                self._descargar_menores_por_importe(
                    entidad_val, entidad_nombre, sub_rangos, depth + 1
                )

            time.sleep(PAUSA_BASE)

    # ===================================================================
    # B) OTROS TIPOS — por mes + tipo publicación (con fechas)
    # ===================================================================
    def descargar_otros(self, anio_inicio=2017, anio_fin=2025):
        """Descarga tipos no menores por mes."""
        segmentos = generar_segmentos_mensuales(anio_inicio, anio_fin)
        total_meses = len(segmentos)
        total_descargas = total_meses * len(TIPOS_NO_MENORES)

        log.info(f"\n{'='*65}")
        log.info("OTROS TIPOS — Por mes + tipo publicación")
        log.info(f"  Período: {anio_inicio}-{anio_fin} ({total_meses} meses)")
        log.info(f"  Tipos: {len(TIPOS_NO_MENORES)}")
        log.info(f"  Descargas estimadas: ~{total_descargas}")
        log.info(f"{'='*65}")

        for i, (desde, hasta, anio, mes) in enumerate(segmentos, 1):
            log.info(f"\n  [{i}/{total_meses}] {mes:02d}/{anio}")

            for tipo_pub in TIPOS_NO_MENORES:
                fp = CSV_DIR / nombre_csv_mes(anio, mes, tipo_pub)
                label = f"{mes:02d}/{anio} {tipo_pub[:30]}"
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
    def descargar_todo(self, anio_inicio=2017, anio_fin=2025):
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

            ok, n_filas = self._descargar_con_reintentos(
                fp, label,
                tipo_pub="Contratos Menores",
                entidad=val
            )

            if ok and n_filas >= UMBRAL_TRUNCADO:
                log.info(f"    → Subdividiendo recursivamente por importe...")
                if fp.exists():
                    file_size = fp.stat().st_size
                    fp.unlink()
                    self.stats["ok"] -= 1
                    self.stats["filas"] -= n_filas
                    self.stats["bytes"] -= file_size
                    if self.stats["archivos"] and str(fp) == self.stats["archivos"][-1]:
                        self.stats["archivos"].pop()
                self._descargar_menores_por_importe(val, nombre)

        self._resumen()

    def _resumen(self):
        elapsed = time.time() - self.t_inicio
        mins = int(elapsed // 60)
        secs = int(elapsed % 60)
        total_mb = self.stats["bytes"] / (1024 * 1024)

        log.info(f"\n{'='*65}")
        log.info("RESUMEN")
        log.info(f"  Descargas OK:    {self.stats['ok']}")
        log.info(f"  Errores:         {self.stats['error']}")
        log.info(f"  Ya existían:     {self.stats['skip_existe']}")
        log.info(f"  Vacíos:          {self.stats['skip_vacio']}")
        log.info(f"  Filas totales:   {self.stats['filas']:,}")
        log.info(f"  Tamaño total:    {total_mb:.1f} MB")
        log.info(f"  Archivos:        {len(self.stats['archivos'])}")
        log.info(f"  Tiempo:          {mins}m {secs}s")
        log.info("=" * 65)


# ---------------------------------------------------------------------------
# UNIFICACIÓN DE CSVs
# ---------------------------------------------------------------------------
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
            df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig',
                             dtype=str, on_bad_lines='skip')
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

    # Eliminar duplicados por Nº Expediente + Referencia + Entidad
    cols_dedup = ['Nº Expediente', 'Referencia', 'Entidad Adjudicadora']
    cols_presentes = [c for c in cols_dedup if c in df_total.columns]
    if cols_presentes:
        antes = len(df_total)
        df_total = df_total.drop_duplicates(subset=cols_presentes)
        dupes = antes - len(df_total)
        if dupes:
            log.info(f"  Eliminados {dupes:,} duplicados")

    salida = OUTPUT_DIR / "contratacion_comunidad_madrid_completo.csv"
    df_total.to_csv(salida, index=False, sep=';', encoding='utf-8-sig')
    size_mb = salida.stat().st_size / (1024 * 1024)
    log.info(f"\n✓ {salida}")
    log.info(f"  {len(df_total):,} filas únicas, {size_mb:.1f} MB")
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
        a1 = int(sys.argv[2]) if len(sys.argv) > 2 else 2017
        a2 = int(sys.argv[3]) if len(sys.argv) > 3 else 2025
        d.descargar_otros(a1, a2)

    elif modo == "todo":
        d = DescargadorComunidadMadrid()
        a1 = int(sys.argv[2]) if len(sys.argv) > 2 else 2017
        a2 = int(sys.argv[3]) if len(sys.argv) > 3 else 2025
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
  python script.py otros            → Solo otros tipos (por mes, 2017-2025)
  python script.py otros 2020 2025  → Otros tipos, período parcial
  python script.py todo             → Todo: menores + otros
  python script.py unificar         → Une CSVs en archivo único

Estrategia:
  Contratos menores: por entidad adjudicadora (125), sin fechas
  Otros tipos: por mes + tipo publicación, con fechas

Directorio de salida: comunidad_madrid/csv_originales/
        """)