#!/usr/bin/env python3
"""
SCRAPER COMPLETO DE LICITACIONES PÚBLICAS
==========================================
Descarga TODOS los conjuntos de datos de la Plataforma de Contratación del Sector Público.

Conjuntos disponibles:
1. Licitaciones (excluyendo menores) - sindicacion_643
2. Licitaciones por agregación (excluyendo menores) - sindicacion_1044
3. Contratos menores - sindicacion_1143
4. Encargos a medios propios - sindicacion_1383
5. Consultas preliminares de mercado - sindicacion_1403

Uso:
    python nacional/licitaciones.py                          # 2012 - año actual, todos los conjuntos
    python nacional/licitaciones.py --anos 2020-2026
    python nacional/licitaciones.py --anos 2024-2026 --conjunto licitaciones
    python nacional/licitaciones.py --anos 2024-2026 --conjunto menores
    python nacional/licitaciones.py --anos 2012-2026 --solo-procesar --data-dir ./datos_placsp --output-dir ./nacional

Descarga (sin perder nada de lo publicado):
    Por año se pide primero el ZIP anual y, si da 404, los mensuales de ese año.
    Un ZIP ya descargado solo se vuelve a pedir si puede estar incompleto: su
    periodo sigue abierto (año en curso; mes en curso o anterior) o la copia
    local es de antes de que se cerrase. Una descarga nunca machaca la anterior:
    si el ZIP cambió, la copia previa pasa a <conjunto>/_historico/
    (comun/historico.py). El procesado lee todas las versiones de cada ZIP y,
    de un año con ZIP anual y mensuales, todos: ninguna entrada publicada
    alguna vez se pierde aunque el ZIP nuevo ya no la traiga (las copias quedan
    marcadas con entrada_repetida). Al terminar se imprime un informe de 404 por
    conjunto/año y, por ZIP, entradas leídas, filas y descartes por motivo.

Salida (todas las entradas de los ATOM, tal como las publica la PLACSP: cada
actualización de una licitación es una entrada con el mismo id; n_versiones y
es_ultima_version permiten contar licitaciones distintas):
    licitaciones_completo_{inicio}_{fin}.parquet/.csv
    licitaciones_completo_{inicio}_{fin}_resultados.parquet/.csv      (una fila por cac:TenderResult / lote)
    licitaciones_completo_{inicio}_{fin}_adjudicatarios.parquet/.csv  (una por cac:WinningParty de cada resultado: UTE = varias)
    licitaciones_completo_{inicio}_{fin}_lotes.parquet/.csv           (una por cac:ProcurementProjectLot)
    licitaciones_completo_{inicio}_{fin}_criterios.parquet/.csv       (una por criterio de adjudicación, del expediente o de un lote)
    licitaciones_completo_{inicio}_{fin}_modificaciones.parquet/.csv  (una por ContractModification)
    licitaciones_completo_{inicio}_{fin}_borrados.parquet/.csv        (una por entrada borrada, at:deleted-entry)
    Las tablas de detalle llevan id, expediente y conjunto de su entrada y, al
    final, su fecha_updated / es_ultima_version / entrada_repetida: se cruzan
    con la principal por id + fecha_updated.

Versiones (tabla principal):
    n_versiones       = versiones distintas del id (pares id / atom:updated distintos)
    es_ultima_version = una fila por id: la de atom:updated más reciente
    entrada_repetida  = la misma entrada (id y atom:updated) ya se había leído:
                        publicada dos veces, en otro ZIP o en otra versión del ZIP

Importes (cac:ProcurementProject/cac:BudgetAmount):
    valor_estimado_contrato = EstimatedOverallContractAmount (valor estimado, incluye prórrogas/modificaciones)
    importe_sin_iva         = TaxExclusiveAmount (presupuesto base de licitación sin impuestos)
    importe_con_iva         = TotalAmount (presupuesto base de licitación con impuestos)

Columnas añadidas a la tabla principal (al final, sin mover las anteriores):
    id_consulta, nombre_consulta, condiciones, tipo_condicion, fecha_planificada,
    fecha_limite_respuestas     consultas preliminares de mercado (tipo_registro='CPM')
    sara                        OverThresholdIndicator (sujeto a regulación armonizada)
    sistema_contratacion_code   TenderingProcess/ContractingSystemCode (acuerdo marco, sistema dinámico...)
    forma_presentacion_code     TenderingProcess/SubmissionMethodCode (electrónica, manual...)
    fecha_inicio_presentacion   TenderSubmissionDeadlinePeriod/StartDate
    fecha_limite_solicitudes, hora_limite_solicitudes   ParticipationRequestReceptionPeriod
    fecha_limite_pliegos, hora_limite_pliegos           DocumentAvailabilityPeriod
    programas_financiacion      todos los TenderingTerms/FundingProgramCode (';')
    pliego_administrativo(_url), pliego_tecnico(_url), otros_documentos(_url)
                                Legal/Technical/AdditionalDocumentReference: nombre (cbc:ID)
                                y URI, varios separados por ' | '
    n_modificaciones            ContractModification de la entrada (detalle en _modificaciones)
    zip_historico               versión antigua del ZIP (en _historico/) de la que se leyó;
                                vacío = la copia actual
    entrada_repetida            ver Versiones

Columnas añadidas a _resultados (al final): orden_resultado, n_adjudicatarios,
adjudicatarios_todos / nifs_adjudicatarios_todos (todos los WinningParty, en
orden, ' | '), descripcion_resultado (cbc:Description), oferta_mas_baja /
oferta_mas_alta (Lower/HigherTenderAmount), num_ofertas_pyme
(SMEsReceivedTenderQuantity), contadores_ofertas (JSON con todos los
contadores *Quantity del TenderResult), ofertas_anormalmente_bajas
(AbnormallyLowTendersIndicator), num_contrato / fecha_formalizacion
(Contract/ID, Contract/IssueDate), fecha_inicio_contrato (StartDate),
entrada_repetida.
"""

import json
import os
import re
import sys
import zipfile
import requests
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import argparse
import time
import tempfile

# ============================================================================
# CONFIGURACIÓN DE CONJUNTOS DE DATOS
# ============================================================================

# Para cada año se prueba primero el ZIP anual y, si da 404, los mensuales
# (hoy la PLACSP publica mensuales para el año en curso de los tres primeros).
CONJUNTOS = {
    'licitaciones': {
        'nombre': 'Licitaciones (sin menores)',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/',
        'patron_archivo': 'licitacionesPerfilesContratanteCompleto3_{periodo}.zip',
        'ano_inicio': 2012,
    },
    'agregacion': {
        'nombre': 'Licitaciones por agregación (sin menores)',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/',
        'patron_archivo': 'PlataformasAgregadasSinMenores_{periodo}.zip',
        'ano_inicio': 2016,
    },
    'menores': {
        'nombre': 'Contratos menores',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/',
        'patron_archivo': 'contratosMenoresPerfilesContratantes_{periodo}.zip',
        'ano_inicio': 2018,
    },
    'encargos': {
        'nombre': 'Encargos a medios propios',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1383/',
        'patron_archivo': 'EMP_SectorPublico_{periodo}.zip',
        'ano_inicio': 2022,  # Incluye datos desde julio 2021
    },
    'consultas': {
        'nombre': 'Consultas preliminares de mercado',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1403/',
        'patron_archivo': 'CPM_SectorPublico_{periodo}.zip',
        'ano_inicio': 2022,
    },
}

# Directorios - CAMBIAR AQUÍ LA RUTA SI ES NECESARIO (o usar --data-dir / --output-dir)
DATA_DIR = Path('D:/licitaciones_data')
OUTPUT_DIR = Path('D:/licitaciones_output')

# Namespaces XML
NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'at': 'http://purl.org/atompub/tombstones/1.0',
    'cbc': 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
    'cac': 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    'cbc-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
    'cac-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
}
TAG_ENTRY = f"{{{NS['atom']}}}entry"
TAG_BORRADO = f"{{{NS['at']}}}deleted-entry"

# Mapeos de códigos CODICE (listas de códigos de la PLACSP).
# Contrastados con la distribución real de los datos publicados (release v2026.02):
#   - Tipo 22/32 solo aparecen desde 2018 (concesiones de la LCSP 9/2017) y el 40
#     contiene literalmente "contrato de colaboración entre el sector público y el
#     sector privado" en el objeto.
#   - Procedimiento 6 solo aparece en el conjunto de contratos menores; 9 (abierto
#     simplificado) y 10-13 arrancan en 2018; el 3 tiene un 82% de licitador único
#     (negociado SIN publicidad) y el 4 desaparece tras la LCSP (CON publicidad);
#     el 100 lo usan Renfe, Correos, Navantia, mutuas... (normas internas).
TIPOS_CONTRATO = {
    '1': 'Suministros', '2': 'Servicios', '3': 'Obras',
    '21': 'Gestión Servicios Públicos', '22': 'Concesión Servicios',
    '31': 'Concesión Obras Públicas', '32': 'Concesión Obras',
    '40': 'Colaboración Público-Privada', '7': 'Administrativo Especial',
    '8': 'Privado', '50': 'Patrimonial', '999': 'Otros',
}

ESTADOS = {
    'PRE': 'Anuncio previo', 'PUB': 'Publicada', 'EV': 'En evaluación',
    'ADJ': 'Adjudicada', 'RES': 'Resuelta', 'ANUL': 'Anulada', 'DES': 'Desierta',
}

PROCEDIMIENTOS = {
    '1': 'Abierto', '2': 'Restringido', '3': 'Negociado sin publicidad',
    '4': 'Negociado con publicidad', '5': 'Diálogo competitivo',
    '6': 'Contrato menor', '7': 'Derivado de acuerdo marco',
    '8': 'Concurso de proyectos', '9': 'Abierto simplificado',
    '10': 'Asociación para la innovación',
    '11': 'Derivado de asociación para la innovación',
    '12': 'Basado en sistema dinámico de adquisición',
    '13': 'Licitación con negociación', '100': 'Normas internas',
    '999': 'Otros',
}

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def crear_directorios():
    """Crea estructura de directorios."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for conjunto in CONJUNTOS.keys():
        (DATA_DIR / conjunto).mkdir(exist_ok=True)
    print(f"📁 Directorios creados")

def get_session():
    """Crea sesión HTTP configurada."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    })
    return session

def _historico():
    """comun/historico.py (versiones de los ZIP descargados). Import diferido:
    leer_placsp y el parseo no lo necesitan y este fichero también se usa
    copiado suelto."""
    raiz = str(Path(__file__).resolve().parents[1])
    if raiz not in sys.path:
        sys.path.insert(0, raiz)
    from comun import historico
    return historico

# ============================================================================
# GENERACIÓN DE URLs
# ============================================================================

PATRON_ZIP = re.compile(r'_(\d{4})(\d{2})?\.zip$')

def archivo_periodo(conjunto_id, ano, mes=None):
    """Nombre y URL del ZIP de un año (mes=None) o de un mes."""
    config = CONJUNTOS[conjunto_id]
    periodo = f"{ano}{mes:02d}" if mes else str(ano)
    nombre = config['patron_archivo'].format(periodo=periodo)
    return {'nombre': nombre, 'url': config['url_base'] + nombre, 'ano': ano, 'mes': mes}

def generar_urls_conjunto(conjunto_id, ano_inicio, ano_fin, hoy=None):
    """Por año, el ZIP anual y los mensuales que se piden si el anual da 404.

    No se fija desde qué año hay mensuales: la PLACSP los publica para el año
    en curso y los sustituye por el anual cuando se cierra.
    """
    hoy = hoy or datetime.now()
    anos = []
    for ano in range(max(ano_inicio, CONJUNTOS[conjunto_id]['ano_inicio']), min(ano_fin, hoy.year) + 1):
        max_mes = hoy.month if ano == hoy.year else 12
        anos.append({
            'ano': ano,
            'anual': archivo_periodo(conjunto_id, ano),
            'mensuales': [archivo_periodo(conjunto_id, ano, mes) for mes in range(1, max_mes + 1)],
        })
    return anos

# ============================================================================
# DESCARGA
# ============================================================================

def es_periodo_reciente(ano, mes, hoy=None):
    """True si el fichero mensual corresponde al mes actual o al anterior.

    La PLACSP regenera el ZIP del mes en curso a diario (y el del mes anterior
    hasta que se cierra), así que una copia local de esos meses puede estar
    incompleta y hay que volver a descargarla.
    """
    if mes is None:
        return False
    hoy = hoy or datetime.now()
    meses = (hoy.year * 12 + hoy.month) - (ano * 12 + mes)
    return meses <= 1

def cierre_periodo(ano, mes=None):
    """Desde cuándo el ZIP de un periodo ya no cambia: 1 de enero del año
    siguiente (anual) o fin del mes siguiente (mensual, ver es_periodo_reciente)."""
    if mes is None:
        return datetime(ano + 1, 1, 1)
    siguiente = ano * 12 + mes + 1  # mes siguiente al siguiente, índice base 0
    return datetime(siguiente // 12, siguiente % 12 + 1, 1)

def hay_que_refrescar(ano, mes, filepath, hoy=None):
    """True si la copia local del ZIP de un periodo puede estar incompleta: el
    periodo sigue abierto (año en curso o posterior; mes en curso o anterior) o
    la copia se descargó (o se comprobó por última vez) antes de que se cerrase."""
    hoy = hoy or datetime.now()
    cierre = cierre_periodo(ano, mes)
    if hoy < cierre:
        return True
    try:
        return datetime.fromtimestamp(Path(filepath).stat().st_mtime) < cierre
    except OSError:
        return False  # no hay copia local: se descargará igualmente

# Estados de descarga con fichero disponible
DISPONIBLE = ('existente', 'nuevo', 'sin_cambios', 'actualizado')

def _descargar(session, url, filepath, max_reintentos=3, forzar=False):
    """Descarga un archivo sin perder nunca la copia anterior.

    Devuelve (ruta o None, estado): 'existente' (no se pidió), 'nuevo',
    'sin_cambios', 'actualizado' (la copia anterior queda en _historico/, ver
    comun/historico.py), '404' o 'error'.
    """
    # Skip si existe, es un ZIP íntegro y no hay que refrescarlo
    if not forzar and filepath.exists() and filepath.stat().st_size > 1000:
        if zipfile.is_zipfile(filepath):
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"   ⏭ Ya existe ({size_mb:.1f} MB)")
            return filepath, 'existente'
        print(f"   ⚠ ZIP local incompleto/corrupto, se vuelve a descargar", end='')

    # Se descarga a un .part: un corte a mitad no deja un ZIP truncado que la
    # siguiente ejecución daría por bueno, y la copia anterior no se machaca.
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = filepath.with_name(filepath.name + '.part')
    for intento in range(max_reintentos):
        try:
            response = session.get(url, timeout=600, stream=True)
            response.raise_for_status()

            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
            estado = _historico().guardar_version(filepath, desde=tmp_path)
            if estado == 'sin_cambios':
                # mtime = última vez que se comprobó que se sirve este contenido
                # (hay_que_refrescar no vuelve a pedirlo si ya es posterior al cierre)
                os.utime(filepath)

            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"   ✓ ({size_mb:.1f} MB, {estado.replace('_', ' ')})")
            return filepath, estado

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"   ⚠ No disponible (404)")
                return None, '404'
            status = e.response.status_code if e.response is not None else '?'
            print(f"   ✗ Error HTTP {status} (intento {intento + 1}/{max_reintentos})")
            time.sleep(2 ** intento)
        except Exception as e:
            print(f"   ✗ Intento {intento + 1}/{max_reintentos}: {e}")
            time.sleep(2 ** intento)

    if tmp_path.exists():
        tmp_path.unlink()
    return None, 'error'

def descargar_archivo(session, url, filepath, max_reintentos=3, forzar=False):
    """Descarga un archivo (ver _descargar); None si no está disponible o falla."""
    return _descargar(session, url, filepath, max_reintentos, forzar)[0]

def descargar_conjunto(session, conjunto_id, ano_inicio, ano_fin, informe=None):
    """Descarga un conjunto: por año, el ZIP anual o, si da 404, los mensuales.

    Con 'informe' (lista) añade por año {'conjunto', 'ano', 'estados'}: el
    estado de cada ZIP pedido (ver imprimir_informe_descarga).
    """
    config = CONJUNTOS[conjunto_id]
    anos = generar_urls_conjunto(conjunto_id, ano_inicio, ano_fin)

    print(f"\n{'='*60}")
    print(f"📦 {config['nombre'].upper()}")
    print(f"   URL base: {config['url_base']}")
    print(f"   Años: {len(anos)}")
    print(f"{'='*60}")

    descargados = []

    def bajar(archivo, prefijo):
        print(f"{prefijo}{archivo['nombre']}", end='')
        filepath = DATA_DIR / conjunto_id / archivo['nombre']
        forzar = hay_que_refrescar(archivo['ano'], archivo['mes'], filepath)
        ruta, estado = _descargar(session, archivo['url'], filepath, forzar=forzar)
        if ruta:
            descargados.append({**archivo, 'filepath': ruta, 'conjunto': conjunto_id})
        time.sleep(0.3)
        return estado

    for i, grupo in enumerate(anos, 1):
        ano = grupo['ano']
        estados = {f'{ano} (anual)': bajar(grupo['anual'], f"[{i}/{len(anos)}] ")}
        if estados[f'{ano} (anual)'] == '404':
            for archivo in grupo['mensuales']:
                estados[f"{ano}{archivo['mes']:02d}"] = bajar(archivo, '      ↳ ')
        if informe is not None:
            informe.append({'conjunto': conjunto_id, 'ano': ano, 'estados': estados})

    print(f"\n✓ {len(descargados)} archivos disponibles")
    return descargados

def imprimir_informe_descarga(informe):
    """Por conjunto y año: ZIP no disponibles (404) o con error y años sin ningún fichero."""
    print(f"\n📋 INFORME DE DESCARGA")
    print("=" * 60)
    sin_datos = []
    for r in informe:
        estados = r['estados']
        disponibles = [p for p, e in estados.items() if e in DISPONIBLE]
        no_disponibles = [p for p, e in estados.items() if e == '404']
        errores = [p for p, e in estados.items() if e == 'error']
        actualizados = [p for p, e in estados.items() if e == 'actualizado']
        etiqueta = f"{r['conjunto']} {r['ano']}"
        if not disponibles:
            sin_datos.append(etiqueta)
            print(f"   ⚠ {etiqueta}: NINGÚN FICHERO DISPONIBLE — 404: {', '.join(no_disponibles) or '-'}"
                  f"; error: {', '.join(errores) or '-'}")
            continue
        if no_disponibles or errores:
            print(f"   ℹ {etiqueta}: {len(disponibles)} ZIP disponibles; 404: {', '.join(no_disponibles) or '-'}"
                  + (f"; error: {', '.join(errores)}" if errores else ''))
        if actualizados:
            print(f"   ℹ {etiqueta}: cambiaron {', '.join(actualizados)} (la versión anterior queda en _historico/)")
    if sin_datos:
        print(f"   ⚠ AÑOS SIN NINGÚN FICHERO: {', '.join(sin_datos)}")
    else:
        print(f"   ✓ Todos los años ({len(informe)}) tienen al menos un ZIP")

# ============================================================================
# PARSING XML
# ============================================================================

def safe_text(element, xpath):
    """Extrae texto de forma segura."""
    if element is None:
        return None
    try:
        found = element.find(xpath, NS)
        if found is not None and found.text:
            return found.text.strip()
    except Exception:
        pass
    return None

def safe_attr(element, xpath, attr):
    """Extrae atributo de forma segura."""
    if element is None:
        return None
    try:
        found = element.find(xpath, NS)
        if found is not None:
            return found.get(attr)
    except Exception:
        pass
    return None

def safe_float(value):
    """Convierte a float; None si no es numérico."""
    if value:
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
    return None

def safe_int(value):
    """Convierte a int; None si no es entero."""
    if value:
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
    return None

def safe_bool(value):
    """xs:boolean ('true'/'false'/'1'/'0') a bool; None si falta."""
    if value:
        return {'true': True, '1': True, 'false': False, '0': False}.get(value.strip().lower())
    return None

def textos(element, xpath):
    """Textos (no vacíos) de todos los elementos que casan con xpath."""
    if element is None:
        return []
    return [e.text.strip() for e in element.findall(xpath, NS) if e.text and e.text.strip()]

def unir(valores, sep=' | '):
    """Une valores conservando su posición (vacío si falta uno); None si no hay ninguno."""
    return sep.join(v or '' for v in valores) if any(valores) else None

def nombre_local(tag):
    """'{namespace}Nombre' → 'Nombre'."""
    return tag.rsplit('}', 1)[-1]

def valores_elemento(elem):
    """Todos los valores de un elemento: {ruta de nombres locales: texto}, con
    los atributos como 'ruta/@atributo'; si una ruta se repite, lista de valores."""
    valores = {}

    def poner(clave, valor):
        if clave in valores:
            previo = valores[clave]
            valores[clave] = (previo if isinstance(previo, list) else [previo]) + [valor]
        else:
            valores[clave] = valor

    def visitar(e, ruta):
        for k, v in e.attrib.items():
            poner(f'{ruta}/@{nombre_local(k)}' if ruta else f'@{nombre_local(k)}', v)
        if e.text and e.text.strip():
            poner(ruta or '.', e.text.strip())
        for hijo in e:
            if isinstance(hijo.tag, str):
                nombre = nombre_local(hijo.tag)
                visitar(hijo, f'{ruta}/{nombre}' if ruta else nombre)

    visitar(elem, '')
    return valores

def parsear_adjudicatarios(result):
    """Un dict por cac:WinningParty de un TenderResult (una UTE tiene varios)."""
    filas = []
    for i, party in enumerate(result.findall('.//cac:WinningParty', NS), 1):
        id_elem = party.find('cac:PartyIdentification/cbc:ID', NS)
        filas.append({
            'orden_adjudicatario': i,
            'adjudicatario': safe_text(party, 'cac:PartyName/cbc:Name'),
            'nif_adjudicatario': id_elem.text.strip() if id_elem is not None and id_elem.text else None,
            'tipo_id_adjudicatario': id_elem.get('schemeName') if id_elem is not None else None,
        })
    return filas

def parsear_resultado(result):
    """Parsea un cac:TenderResult (hay uno por lote adjudicado/desierto).

    '_adjudicatarios' lleva todos sus WinningParty (tabla _adjudicatarios).
    """
    nif_adjudicatario = None
    winner_id = result.find('.//cac:WinningParty/cac:PartyIdentification/cbc:ID', NS)
    if winner_id is not None and winner_id.text:
        nif_adjudicatario = winner_id.text.strip()

    num_ofertas = None
    val = safe_text(result, 'cbc:ReceivedTenderQuantity')
    if val:
        try:
            num_ofertas = int(val)
        except ValueError:
            pass

    pyme = safe_text(result, 'cbc:SMEAwardedIndicator')

    adjudicatarios = parsear_adjudicatarios(result)
    # Todos los contadores de ofertas (ReceivedTenderQuantity, SMEsReceivedTenderQuantity...)
    contadores = {nombre_local(e.tag): e.text.strip() for e in result
                  if isinstance(e.tag, str) and nombre_local(e.tag).endswith('Quantity')
                  and e.text and e.text.strip()}

    return {
        'lote': safe_text(result, './/cac:AwardedTenderedProject/cbc:ProcurementProjectLotID'),
        'resultado_code': safe_text(result, 'cbc:ResultCode'),
        'adjudicatario': safe_text(result, './/cac:WinningParty/cac:PartyName/cbc:Name'),
        'nif_adjudicatario': nif_adjudicatario,
        'importe_adjudicacion': safe_float(safe_text(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount')),
        'importe_adj_con_iva': safe_float(safe_text(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:PayableAmount')),
        'fecha_adjudicacion': safe_text(result, 'cbc:AwardDate'),
        'num_ofertas': num_ofertas,
        'es_pyme': pyme == 'true' if pyme else None,
        # Nuevas (van al final de _resultados). Con {*}: namespace sin
        # verificar con datos reales (cbc o cbc-place-ext)
        'n_adjudicatarios': len(adjudicatarios),
        'adjudicatarios_todos': unir([a['adjudicatario'] for a in adjudicatarios]),
        'nifs_adjudicatarios_todos': unir([a['nif_adjudicatario'] for a in adjudicatarios]),
        'descripcion_resultado': safe_text(result, 'cbc:Description'),
        'oferta_mas_baja': safe_float(safe_text(result, 'cbc:LowerTenderAmount')),
        'oferta_mas_alta': safe_float(safe_text(result, 'cbc:HigherTenderAmount')),
        'num_ofertas_pyme': safe_int(safe_text(result, '{*}SMEsReceivedTenderQuantity')),
        'contadores_ofertas': json.dumps(contadores, ensure_ascii=False) if contadores else None,
        'ofertas_anormalmente_bajas': safe_bool(safe_text(result, '{*}AbnormallyLowTendersIndicator')),
        'num_contrato': unir(textos(result, 'cac:Contract/cbc:ID')),
        'fecha_formalizacion': safe_text(result, 'cac:Contract/cbc:IssueDate'),
        'fecha_inicio_contrato': safe_text(result, 'cbc:StartDate'),
        '_adjudicatarios': adjudicatarios,
    }

def importes_presupuesto(project):
    """valor_estimado_contrato, importe_sin_iva e importe_con_iva de un cac:ProcurementProject."""
    budget = project.find('cac:BudgetAmount', NS) if project is not None else None
    return {
        'valor_estimado_contrato': safe_float(safe_text(budget, 'cbc:EstimatedOverallContractAmount')),
        'importe_sin_iva': safe_float(safe_text(budget, 'cbc:TaxExclusiveAmount')),
        'importe_con_iva': safe_float(safe_text(budget, 'cbc:TotalAmount')),
    }

def parsear_criterios(terms, lote=None):
    """Criterios de adjudicación (TenderingTerms/AwardingTerms/AwardingCriteria)
    del expediente (lote=None) o de un lote."""
    filas = []
    if terms is None:
        return filas
    for i, crit in enumerate(terms.findall('cac:AwardingTerms/cac:AwardingCriteria', NS), 1):
        filas.append({
            'lote': lote,
            'orden_criterio': i,
            'tipo_criterio_code': safe_text(crit, 'cbc:AwardingCriteriaTypeCode'),
            'subtipo_criterio_code': safe_text(crit, 'cbc:AwardingCriteriaSubTypeCode'),
            'descripcion': unir(textos(crit, 'cbc:Description')),
            'peso': safe_float(safe_text(crit, 'cbc:WeightNumeric')),
            'nota': unir(textos(crit, 'cbc:Note')),
        })
    return filas

def parsear_lotes(status):
    """Filas de _lotes (una por cac:ProcurementProjectLot) y los criterios de adjudicación de cada lote."""
    lotes, criterios = [], []
    for i, lot in enumerate(status.findall('cac:ProcurementProjectLot', NS), 1):
        id_lote = safe_text(lot, 'cbc:ID')
        project = lot.find('cac:ProcurementProject', NS)
        cpvs = textos(project, './/cac:RequiredCommodityClassification/cbc:ItemClassificationCode')
        terms = lot.find('cac:TenderingTerms', NS)
        lotes.append({
            'orden_lote': i,
            'lote': id_lote,
            'objeto_lote': safe_text(project, 'cbc:Name'),
            **importes_presupuesto(project),
            'cpv_principal': cpvs[0] if cpvs else None,
            'cpvs': ';'.join(cpvs) or None,
            'ubicacion': safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentity'),
            'nuts': safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentityCode'),
            'programas_financiacion': ';'.join(textos(terms, 'cbc:FundingProgramCode')) or None,
        })
        criterios += parsear_criterios(terms, id_lote)
    return lotes, criterios

def parsear_modificaciones(status):
    """Filas de _modificaciones: una por ContractModification.

    Sin verificar con datos reales: los elementos se buscan por nombre local en
    cualquier namespace y 'detalle' guarda en JSON todos los valores del
    elemento, para no perder nada si la estructura publicada es otra.
    """
    filas = []
    for i, mod in enumerate(status.findall('.//{*}ContractModification', NS), 1):
        filas.append({
            'orden_modificacion': i,
            'id_modificacion': safe_text(mod, '{*}ID'),
            'id_contrato': safe_text(mod, '{*}ContractID'),
            'nota': unir(textos(mod, '{*}Note')),
            'importe_modificacion_sin_iva': safe_float(safe_text(mod, '{*}ContractModificationLegalMonetaryTotal/{*}TaxExclusiveAmount')),
            'importe_final_sin_iva': safe_float(safe_text(mod, '{*}FinalLegalMonetaryTotal/{*}TaxExclusiveAmount')),
            'duracion_modificacion': safe_text(mod, '{*}ContractModificationDurationMeasure'),
            'duracion_modificacion_unidad': safe_attr(mod, '{*}ContractModificationDurationMeasure', 'unitCode'),
            'duracion_final': safe_text(mod, '{*}FinalDurationMeasure'),
            'duracion_final_unidad': safe_attr(mod, '{*}FinalDurationMeasure', 'unitCode'),
            'detalle': json.dumps(valores_elemento(mod), ensure_ascii=False),
        })
    return filas

def documentos(status, xpath):
    """(nombres, URLs) de los documentos de un tipo (cbc:ID y ExternalReference/cbc:URI), ' | '."""
    docs = status.findall(xpath, NS)
    return (unir([safe_text(d, 'cbc:ID') for d in docs]),
            unir([safe_text(d, './/cac:ExternalReference/cbc:URI') for d in docs]))

# Columnas propias de las consultas preliminares de mercado (esquema v2026.02)
COLUMNAS_CPM = ['id_consulta', 'nombre_consulta', 'condiciones', 'tipo_condicion',
                'fecha_planificada', 'fecha_limite_respuestas']

def datos_consulta(status):
    """Campos propios de una consulta preliminar de mercado (CPM).

    Sin verificar con datos reales: su posición dentro de
    PreliminaryMarketConsultationStatus (y si son cbc o cbc-place-ext) no está
    contrastada, así que se buscan por nombre local a cualquier profundidad.
    Los textos que se repiten se unen con ' | '.
    """
    def todos(nombre):
        return unir(textos(status, f'.//{{*}}{nombre}'))

    def primero(nombre):
        return safe_text(status, f'.//{{*}}{nombre}')

    return {
        'id_consulta': primero('PreliminaryMarketConsultationID'),
        'nombre_consulta': todos('ConsultationName'),
        'condiciones': todos('ConditionsText'),
        'tipo_condicion': todos('ConditionTypeCode'),
        'fecha_planificada': primero('PlannedDate'),
        'fecha_limite_respuestas': primero('LimitDate'),
    }

def fecha_publicacion_licitacion(status):
    """Fecha del anuncio de licitación (DOC_CN) o, si no lo hay, del primer anuncio.

    Hay un cac-place-ext:ValidNoticeInfo por anuncio (previo, licitación,
    adjudicación, formalización...). Tomar el primero sin mirar su tipo daba en
    los expedientes adjudicados la fecha del anuncio de adjudicación o
    formalización: en el 57% de las licitaciones con ambas fechas el plazo de
    presentación quedaba antes de la "publicación".
    """
    fechas_licitacion, fechas = [], []
    for notice in status.findall('cac-place-ext:ValidNoticeInfo', NS):
        tipo = safe_text(notice, 'cbc-place-ext:NoticeTypeCode')
        for issue in notice.findall('.//cac-place-ext:AdditionalPublicationDocumentReference/cbc:IssueDate', NS):
            if issue.text and issue.text.strip():
                fecha = issue.text.strip()[:10]  # sin zona horaria ('2024-01-15+01:00')
                fechas.append(fecha)
                if tipo == 'DOC_CN':
                    fechas_licitacion.append(fecha)
    return min(fechas_licitacion or fechas) if fechas else None

def parsear_entry(entry, descartes=None):
    """Parsea una entrada del ATOM: licitación (cac-place-ext:ContractFolderStatus)
    o consulta preliminar de mercado (cac-place-ext:PreliminaryMarketConsultationStatus,
    tipo_registro='CPM').

    Devuelve None si no se puede parsear; con 'descartes' (dict) suma 1 al motivo.
    """
    try:
        status = entry.find('cac-place-ext:ContractFolderStatus', NS)
        if status is not None:
            return _parsear_status(entry, status, es_cpm=False)
        status = entry.find('cac-place-ext:PreliminaryMarketConsultationStatus', NS)
        if status is not None:
            return _parsear_status(entry, status, es_cpm=True)
        otros = [nombre_local(h.tag) for h in entry
                 if isinstance(h.tag, str) and not h.tag.startswith(f"{{{NS['atom']}}}")]
        motivo = 'sin ContractFolderStatus ni PreliminaryMarketConsultationStatus'
        if otros:
            motivo += f' (contiene {otros[0]})'
    except Exception as e:
        motivo = f'error al parsear: {type(e).__name__}: {e}'[:200]
    if descartes is not None:
        descartes[motivo] = descartes.get(motivo, 0) + 1
    return None

def _parsear_status(entry, status, es_cpm):
    """Fila de una entrada a partir de su ContractFolderStatus o PreliminaryMarketConsultationStatus."""
    def buscar(xpath):
        # CPM: estructura sin verificar con datos reales; si el elemento no
        # cuelga directamente del status se busca a cualquier profundidad
        elem = status.find(xpath, NS)
        if elem is None and es_cpm:
            elem = status.find('.//' + xpath, NS)
        return elem

    # ID y URL
    id_lic = safe_text(entry, 'atom:id')
    link = entry.find('atom:link', NS)
    url = link.get('href') if link is not None else None

    # Expediente y estado
    expediente = safe_text(status, 'cbc:ContractFolderID')
    if es_cpm:
        estado_code = safe_text(status, './/{*}PreliminaryMarketConsultationStatusCode')
    else:
        estado_code = safe_text(status, 'cbc-place-ext:ContractFolderStatusCode')

    # Órgano contratante
    # (siempre "is not None": un Element sin hijos evalúa a False)
    located_party = buscar('cac-place-ext:LocatedContractingParty')
    party = located_party.find('cac:Party', NS) if located_party is not None else None

    nombre_organo = safe_text(party, 'cac:PartyName/cbc:Name')
    ciudad_organo = safe_text(party, 'cac:PostalAddress/cbc:CityName')

    # Identificadores del órgano
    nif_organo = None
    dir3_organo = None
    id_plataforma = None

    if party is not None:
        for pid in party.findall('cac:PartyIdentification', NS):
            id_elem = pid.find('cbc:ID', NS)
            if id_elem is not None and id_elem.text:
                scheme = id_elem.get('schemeName', '')
                if scheme == 'NIF':
                    nif_organo = id_elem.text.strip()
                elif scheme == 'DIR3':
                    dir3_organo = id_elem.text.strip()
                elif scheme == 'ID_PLATAFORMA':
                    id_plataforma = id_elem.text.strip()

    # Jerarquía del órgano
    parent_names = []
    parent = located_party.find('cac-place-ext:ParentLocatedParty', NS) if located_party is not None else None
    while parent is not None:
        pname = safe_text(parent, 'cac:PartyName/cbc:Name')
        if pname:
            parent_names.append(pname)
        parent = parent.find('cac-place-ext:ParentLocatedParty', NS)

    dependencia = ' > '.join(reversed(parent_names)) if parent_names else None

    # Proyecto
    project = buscar('cac:ProcurementProject')
    objeto = safe_text(project, 'cbc:Name')
    tipo_code = safe_text(project, 'cbc:TypeCode')
    subtipo_code = safe_text(project, 'cbc:SubTypeCode')

    # Importes
    budget = project.find('cac:BudgetAmount', NS) if project is not None else None
    valor_estimado_contrato = None
    importe_sin_iva = None
    importe_con_iva = None

    if budget is not None:
        val = safe_text(budget, 'cbc:EstimatedOverallContractAmount')
        if val:
            try:
                valor_estimado_contrato = float(val)
            except (ValueError, TypeError):
                pass
        val = safe_text(budget, 'cbc:TotalAmount')
        if val:
            try:
                importe_con_iva = float(val)
            except (ValueError, TypeError):
                pass
        val = safe_text(budget, 'cbc:TaxExclusiveAmount')
        if val:
            try:
                importe_sin_iva = float(val)
            except (ValueError, TypeError):
                pass

    # CPV
    cpvs = []
    if project is not None:
        for cpv_elem in project.findall('.//cac:RequiredCommodityClassification/cbc:ItemClassificationCode', NS):
            if cpv_elem.text:
                cpvs.append(cpv_elem.text.strip())
    cpv_principal = cpvs[0] if cpvs else None
    cpvs_todos = ';'.join(cpvs) if cpvs else None

    # Ubicación
    ubicacion = safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentity')
    nuts = safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentityCode')

    # Duración
    duracion = safe_text(project, './/cac:PlannedPeriod/cbc:DurationMeasure')
    duracion_unidad = safe_attr(project, './/cac:PlannedPeriod/cbc:DurationMeasure', 'unitCode')

    # Proceso
    process = buscar('cac:TenderingProcess')
    procedimiento_code = safe_text(process, 'cbc:ProcedureCode')
    urgencia = safe_text(process, 'cbc:UrgencyCode')

    # Fecha límite
    fecha_limite = safe_text(process, './/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate')
    hora_limite = safe_text(process, './/cac:TenderSubmissionDeadlinePeriod/cbc:EndTime')

    # Términos
    terms = buscar('cac:TenderingTerms')
    financiacion_ue = safe_text(terms, 'cbc:FundingProgramCode')

    # Resultado/Adjudicación: hay un cac:TenderResult por lote. Las columnas
    # principales reflejan el primero (como hasta ahora); el detalle de todos
    # los lotes se exporta en la tabla de resultados (y cada adjudicatario,
    # también los de una UTE, en la de adjudicatarios).
    resultados, adjudicatarios = [], []
    for i, r in enumerate(status.findall('cac:TenderResult', NS), 1):
        res = parsear_resultado(r)
        res['orden_resultado'] = i
        for adj in res.pop('_adjudicatarios'):
            adjudicatarios.append({'lote': res['lote'], 'orden_resultado': i, **adj})
        resultados.append(res)
    primero = resultados[0] if resultados else {}
    n_lotes = len(status.findall('cac:ProcurementProjectLot', NS))
    lotes, criterios_lotes = parsear_lotes(status)
    criterios = parsear_criterios(terms) + criterios_lotes
    modificaciones = parsear_modificaciones(status)

    # Pliegos y demás documentos
    legal = documentos(status, 'cac:LegalDocumentReference')
    tecnico = documentos(status, 'cac:TechnicalDocumentReference')
    adicionales = documentos(status, 'cac:AdditionalDocumentReference')

    # Fechas
    fecha_updated = safe_text(entry, 'atom:updated')
    fecha_publicacion = fecha_publicacion_licitacion(status)

    return {
        'id': id_lic,
        'expediente': expediente,
        'objeto': objeto,
        'organo_contratante': nombre_organo,
        'nif_organo': nif_organo,
        'dir3_organo': dir3_organo,
        'id_plataforma': id_plataforma,
        'ciudad_organo': ciudad_organo,
        'dependencia': dependencia,
        'tipo_contrato_code': tipo_code,
        'tipo_contrato': TIPOS_CONTRATO.get(tipo_code, tipo_code),
        'subtipo_code': subtipo_code,
        'procedimiento_code': procedimiento_code,
        'procedimiento': PROCEDIMIENTOS.get(procedimiento_code, procedimiento_code),
        'estado_code': estado_code,
        'estado': ESTADOS.get(estado_code, estado_code),
        'valor_estimado_contrato': valor_estimado_contrato,
        'importe_sin_iva': importe_sin_iva,
        'importe_con_iva': importe_con_iva,
        'importe_adjudicacion': primero.get('importe_adjudicacion'),
        'importe_adj_con_iva': primero.get('importe_adj_con_iva'),
        'adjudicatario': primero.get('adjudicatario'),
        'nif_adjudicatario': primero.get('nif_adjudicatario'),
        'num_ofertas': primero.get('num_ofertas'),
        'es_pyme': primero.get('es_pyme'),
        'n_lotes': n_lotes,
        'n_resultados': len(resultados),
        'cpv_principal': cpv_principal,
        'cpvs': cpvs_todos,
        'ubicacion': ubicacion,
        'nuts': nuts,
        'duracion': duracion,
        'duracion_unidad': duracion_unidad,
        'financiacion_ue': financiacion_ue,
        'urgencia': urgencia,
        'fecha_limite': fecha_limite,
        'hora_limite': hora_limite,
        'fecha_adjudicacion': primero.get('fecha_adjudicacion'),
        'fecha_publicacion': fecha_publicacion,
        'fecha_updated': fecha_updated,
        'url': url,
        # --- Columnas nuevas: van al final de la tabla (COLUMNAS_NUEVAS) ---
        **(datos_consulta(status) if es_cpm else dict.fromkeys(COLUMNAS_CPM)),
        # Sin verificar con datos reales: posición y namespace de OverThresholdIndicator
        'sara': safe_bool(safe_text(status, './/{*}OverThresholdIndicator')),
        'sistema_contratacion_code': safe_text(process, '{*}ContractingSystemCode'),
        'forma_presentacion_code': safe_text(process, '{*}SubmissionMethodCode'),
        'fecha_inicio_presentacion': safe_text(process, './/cac:TenderSubmissionDeadlinePeriod/cbc:StartDate'),
        'fecha_limite_solicitudes': safe_text(process, './/cac:ParticipationRequestReceptionPeriod/cbc:EndDate'),
        'hora_limite_solicitudes': safe_text(process, './/cac:ParticipationRequestReceptionPeriod/cbc:EndTime'),
        'fecha_limite_pliegos': safe_text(process, './/cac:DocumentAvailabilityPeriod/cbc:EndDate'),
        'hora_limite_pliegos': safe_text(process, './/cac:DocumentAvailabilityPeriod/cbc:EndTime'),
        'programas_financiacion': ';'.join(textos(terms, 'cbc:FundingProgramCode')) or None,
        'pliego_administrativo': legal[0],
        'pliego_administrativo_url': legal[1],
        'pliego_tecnico': tecnico[0],
        'pliego_tecnico_url': tecnico[1],
        'otros_documentos': adicionales[0],
        'otros_documentos_url': adicionales[1],
        'n_modificaciones': len(modificaciones),
        'tipo_registro': 'CPM' if es_cpm else 'LICITACION',
        '_resultados': resultados,
        '_adjudicatarios': adjudicatarios,
        '_lotes': lotes,
        '_criterios': criterios,
        '_modificaciones': modificaciones,
    }

def parsear_borrado(elem):
    """Entrada borrada del ATOM (at:deleted-entry): id, cuándo y motivo (at:comment/@type)."""
    comentario = elem.find('at:comment', NS)
    texto = comentario.text.strip() if comentario is not None and comentario.text else ''
    return {
        'id': elem.get('ref'),
        'fecha_borrado': elem.get('when'),
        'motivo': comentario.get('type') if comentario is not None else None,
        'comentario': texto or None,
    }

def nuevo_informe(**datos):
    """Recuento del procesado de un ZIP o de un ATOM."""
    return {'atom': 0, 'entradas': 0, 'borrados': 0, 'filas': 0,
            'descartadas': {}, 'errores': [], **datos}

def sumar_informe(total, parcial):
    """Acumula los recuentos de 'parcial' en 'total'."""
    for clave in ('atom', 'entradas', 'borrados', 'filas'):
        total[clave] += parcial[clave]
    for motivo, n in parcial['descartadas'].items():
        total['descartadas'][motivo] = total['descartadas'].get(motivo, 0) + n
    total['errores'].extend(parcial['errores'])

def _leer_elementos(elementos, licitaciones, borrados, informe):
    """Parsea las entradas y entradas borradas de un ATOM (vacía cada una al terminar)."""
    for elem in elementos:
        if elem.tag == TAG_ENTRY:
            informe['entradas'] += 1
            lic = parsear_entry(elem, informe['descartadas'])
            if lic:
                licitaciones.append(lic)
        elif elem.tag == TAG_BORRADO:
            informe['borrados'] += 1
            borrados.append(parsear_borrado(elem))
        else:
            continue
        elem.clear()

def procesar_archivo_atom(filepath, borrados=None, informe=None):
    """Procesa un archivo ATOM: una fila por entrada.

    Las entradas borradas (at:deleted-entry) se añaden a 'borrados' y los
    recuentos (entradas, filas, descartes por motivo, errores) a 'informe',
    si se pasan.
    """
    nombre = Path(filepath).name
    licitaciones, borr, inf = [], [], nuevo_informe()
    try:
        context = ET.iterparse(str(filepath), events=('end',))
        _leer_elementos((elem for _, elem in context), licitaciones, borr, inf)
    except Exception as e:
        print(f"\n   ⚠ Error leyendo {nombre}: {e}", end=' ')
        parciales = (licitaciones, borr, inf)
        licitaciones, borr, inf = [], [], nuevo_informe()
        try:
            root = ET.parse(filepath).getroot()
            _leer_elementos(list(root), licitaciones, borr, inf)
            inf['errores'].append(f'{nombre}: {type(e).__name__}: {e} (releído entero)')
        except Exception:
            # Se conserva lo leído antes del error
            licitaciones, borr, inf = parciales
            inf['errores'].append(f'{nombre}: {type(e).__name__}: {e} (lectura interrumpida: '
                                  f'se conservan las {inf["entradas"]:,} entradas anteriores)')
    inf['filas'] = len(licitaciones)
    if borrados is not None:
        borrados.extend(borr)
    if informe is not None:
        sumar_informe(informe, inf)
    return licitaciones

def ano_de_zip(nombre):
    """Año del nombre de un ZIP de la PLACSP ('..._2024.zip', '..._202401.zip') o None."""
    match = PATRON_ZIP.search(nombre)
    return int(match.group(1)) if match else None

def procesar_zip(zip_path, conjunto_id, borrados=None, informes=None, archivo_origen=None):
    """Procesa un archivo ZIP: una fila por entrada de sus ATOM.

    archivo_origen: nombre del ZIP del periodo cuando zip_path es una versión
    antigua guardada en _historico/ (cuyo nombre queda en zip_historico).
    Las entradas borradas se añaden a 'borrados' y el recuento del ZIP
    (nuevo_informe) a 'informes', si se pasan.
    """
    zip_path = Path(zip_path)
    archivo_origen = archivo_origen or zip_path.name
    zip_historico = zip_path.name if zip_path.name != archivo_origen else None
    informe = nuevo_informe(archivo=archivo_origen, zip_historico=zip_historico,
                            conjunto=conjunto_id, ano=ano_de_zip(archivo_origen))
    licitaciones, borr = [], []

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            print(f"   📦 Extrayendo...", end=' ', flush=True)
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(temp_path)

            # Buscar archivos .atom (orden estable: la primera aparición de
            # cada entrada es la que no se marca como entrada_repetida)
            atom_files = sorted(temp_path.rglob('*.atom'))
            informe['atom'] = len(atom_files)
            print(f"✓ {len(atom_files)} ATOM", end=' ', flush=True)

            for atom_file in atom_files:
                lics = procesar_archivo_atom(atom_file, borr, informe)
                for lic in lics:
                    lic['conjunto'] = conjunto_id
                    lic['archivo_origen'] = archivo_origen
                    # Mismo esquema que los parquet publicados (CPM = consulta preliminar)
                    lic['tipo_registro'] = lic.pop('tipo_registro', None) or (
                        'CPM' if conjunto_id == 'consultas' else 'LICITACION')
                    lic['zip_historico'] = zip_historico
                licitaciones.extend(lics)

            descartadas = sum(informe['descartadas'].values())
            print(f"→ {len(licitaciones):,} registros ({informe['entradas']:,} entradas"
                  + (f", {informe['borrados']:,} borradas" if informe['borrados'] else '')
                  + (f", {descartadas:,} descartadas" if descartadas else '') + ")")

    except zipfile.BadZipFile:
        print(f"   ✗ ZIP corrupto")
        informe['errores'].append('ZIP corrupto')
    except Exception as e:
        print(f"   ✗ Error: {e}")
        informe['errores'].append(f'{type(e).__name__}: {e}')

    for b in borr:
        b.update(conjunto=conjunto_id, archivo_origen=archivo_origen, zip_historico=zip_historico)
    if borrados is not None:
        borrados.extend(borr)
    if informes is not None:
        informes.append(informe)
    return licitaciones

def seleccionar_zips(zip_files, ano_inicio, ano_fin):
    """ZIP del rango de años, ordenados por año y con el anual antes que los mensuales.

    Se leen todos: si un año tiene ZIP anual y mensuales (el anual aparece al
    cerrarse el año, con los mensuales ya descargados), lo que solo esté en los
    mensuales no se pierde, y lo que está en ambos queda marcado como
    entrada_repetida en la copia de los mensuales (se leen después).
    """
    elegidos = []
    for z in zip_files:
        z = Path(z)
        match = PATRON_ZIP.search(z.name)
        if match and ano_inicio <= int(match.group(1)) <= ano_fin:
            elegidos.append((int(match.group(1)), match.group(2) is not None, z.name, z))
    return [z for *_, z in sorted(elegidos)]

def procesar_conjunto(conjunto_id, ano_inicio, ano_fin, borrados=None, informes=None, avisos=None):
    """Lee los ZIP descargados de un conjunto (DATA_DIR/<conjunto>) con todas sus versiones.

    De cada ZIP se lee la copia actual y después las antiguas de _historico/
    (de la más reciente a la más antigua): así ninguna entrada publicada alguna
    vez se pierde, y la primera aparición de cada una es la de la copia más
    reciente que la trae (las demás quedan como entrada_repetida). En 'avisos'
    se añaden los años sin ningún ZIP y las versiones que no son un ZIP válido.
    """
    avisos = avisos if avisos is not None else []
    hist = _historico()
    zips = seleccionar_zips(sorted((DATA_DIR / conjunto_id).glob('*.zip')), ano_inicio, ano_fin)

    con_zip = {ano_de_zip(z.name) for z in zips}
    desde = max(ano_inicio, CONJUNTOS[conjunto_id]['ano_inicio'])
    faltan = [a for a in range(desde, min(ano_fin, datetime.now().year) + 1) if a not in con_zip]
    if faltan:
        avisos.append(f"{conjunto_id}: ningún ZIP de {', '.join(map(str, faltan))}")

    copias = []
    for z in zips:
        for copia in reversed(hist.versiones(z)):  # actual primero
            if copia != z and not zipfile.is_zipfile(copia):
                avisos.append(f"{conjunto_id}: {copia.name} (en _historico/) no es un ZIP válido; se ignora")
                continue
            copias.append((copia, z.name))

    licitaciones = []
    if copias:
        print(f"\n📦 {CONJUNTOS[conjunto_id]['nombre']}: {len(zips)} archivos"
              + (f" (+{len(copias) - len(zips)} versiones anteriores)" if len(copias) > len(zips) else ''))
        for i, (copia, origen) in enumerate(copias, 1):
            print(f"   [{i}/{len(copias)}] {copia.name}", end='')
            licitaciones.extend(procesar_zip(copia, conjunto_id, borrados, informes, archivo_origen=origen))
    return licitaciones

def imprimir_informe_procesado(informes, avisos=()):
    """Por conjunto: ZIP leídos, entradas, filas, borradas y descartadas; detalle
    de descartes (por motivo) y errores de cada ZIP, años sin filas y avisos."""
    print(f"\n📋 INFORME DE PROCESADO")
    print("=" * 60)
    por_conjunto = {}
    for inf in informes:
        por_conjunto.setdefault(inf['conjunto'], []).append(inf)
    for conjunto, infs in por_conjunto.items():
        total = nuevo_informe()
        filas_por_ano = {}
        for inf in infs:
            sumar_informe(total, inf)
            filas_por_ano[inf['ano']] = filas_por_ano.get(inf['ano'], 0) + inf['filas']
        print(f"   {conjunto}: {len(infs)} ZIP · {total['entradas']:,} entradas → {total['filas']:,} filas · "
              f"{total['borrados']:,} borradas · {sum(total['descartadas'].values()):,} descartadas · "
              f"{len(total['errores'])} errores")
        for inf in infs:
            nombre = inf['zip_historico'] or inf['archivo']
            for motivo, n in inf['descartadas'].items():
                print(f"      ⚠ {nombre}: {n:,} entradas descartadas — {motivo}")
            for error in inf['errores']:
                print(f"      ✗ {nombre}: {error}")
        for ano, filas in filas_por_ano.items():
            if not filas:
                print(f"      ⚠ {conjunto} {ano}: sus ZIP no dieron ninguna fila")
    for aviso in avisos:
        print(f"   ⚠ {aviso}")

# ============================================================================
# EXPORTACIÓN
# ============================================================================

# Columnas de importes con su significado (para los resúmenes)
IMPORTES_RESUMEN = [
    ('valor_estimado_contrato', 'Valor estimado'),
    ('importe_sin_iva', 'Presupuesto base sin IVA'),
    ('importe_adjudicacion', 'Adjudicado sin IVA (1er lote)'),
]

# Columnas añadidas tras v2026.02: van al final para no mover las existentes
COLUMNAS_NUEVAS = COLUMNAS_CPM + [
    'sara', 'sistema_contratacion_code', 'forma_presentacion_code',
    'fecha_inicio_presentacion', 'fecha_limite_solicitudes', 'hora_limite_solicitudes',
    'fecha_limite_pliegos', 'hora_limite_pliegos', 'programas_financiacion',
    'pliego_administrativo', 'pliego_administrativo_url', 'pliego_tecnico', 'pliego_tecnico_url',
    'otros_documentos', 'otros_documentos_url', 'n_modificaciones', 'zip_historico',
    'entrada_repetida',
]
COLUMNAS_NUEVAS_RESULTADOS = [
    'orden_resultado', 'n_adjudicatarios', 'adjudicatarios_todos', 'nifs_adjudicatarios_todos',
    'descripcion_resultado', 'oferta_mas_baja', 'oferta_mas_alta', 'num_ofertas_pyme',
    'contadores_ofertas', 'ofertas_anormalmente_bajas', 'num_contrato', 'fecha_formalizacion',
    'fecha_inicio_contrato', 'entrada_repetida',
]
FECHAS = ['fecha_limite', 'fecha_adjudicacion', 'fecha_publicacion', 'fecha_planificada',
          'fecha_limite_respuestas', 'fecha_inicio_presentacion', 'fecha_limite_solicitudes',
          'fecha_limite_pliegos']
FECHAS_DETALLE = ['fecha_adjudicacion', 'fecha_formalizacion', 'fecha_inicio_contrato']
# Tablas de detalle: cada entrada lleva su lista en '_<tabla>'
TABLAS_DETALLE = ('resultados', 'adjudicatarios', 'lotes', 'criterios', 'modificaciones')

def parsear_fechas(serie):
    """Fechas xs:date de CODICE ('2024-01-15', a veces con zona: '2024-01-15+01:00')."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie
    return pd.to_datetime(serie.astype('string').str[:10], errors='coerce', format='%Y-%m-%d')

def parsear_fecha_updated(serie):
    """atom:updated en UTC. format='ISO8601' evita que pandas infiera el formato del
    primer valor y convierta en NaT los que no llevan milisegundos (o viceversa)."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie if getattr(serie.dt, 'tz', None) is not None else serie.dt.tz_localize('UTC')
    return pd.to_datetime(serie, errors='coerce', utc=True, format='ISO8601')

def indices_ultima_version(ids, fechas_updated=None):
    """Posiciones (ordenadas) de la versión más reciente de cada id.

    Con fechas gana la de atom:updated más reciente (una fecha nula no gana a
    una fechada) y, entre copias de la misma entrada (mismo id y fecha), la
    primera leída, que es la que no se marca como entrada_repetida. Sin fechas
    gana la última leída. Las filas sin id se conservan todas.
    """
    orden = np.arange(len(ids))
    claves = pd.DataFrame({'id': pd.Series(ids).reset_index(drop=True), '_orden': orden})
    columnas_orden, ascendente = ['_orden'], [True]
    if fechas_updated is not None:
        claves['fecha_updated'] = parsear_fecha_updated(pd.Series(fechas_updated).reset_index(drop=True))
        columnas_orden, ascendente = ['fecha_updated', '_orden'], [True, False]
    con_id = claves['id'].notna().to_numpy()
    ultimas = (claves[con_id]
               .sort_values(columnas_orden, ascending=ascendente, kind='mergesort', na_position='first')
               .drop_duplicates(subset=['id'], keep='last')['_orden']
               .to_numpy())
    return np.sort(np.concatenate([ultimas, orden[~con_id]]))

def marcas_version(ids, fechas_updated=None):
    """(es_ultima_version, n_versiones, entrada_repetida) de cada fila, sin eliminar ninguna.

    La PLACSP publica una entrada nueva del ATOM cada vez que se actualiza una
    licitación (anuncio, adjudicación, formalización...), con el mismo 'id', y a
    veces publica otra vez la misma entrada (mismo id y atom:updated), en el
    mismo ZIP o en otro. Se sirven todas tal cual, con estas marcas:
    - entrada_repetida: True en la 2ª y siguientes apariciones (en orden de
      lectura) de cada par (id, fecha_updated).
    - n_versiones: versiones distintas del id (pares id / fecha_updated distintos).
    - es_ultima_version: exactamente una fila por id, la de atom:updated más
      reciente (nunca una entrada_repetida); filtrarla permite contar
      licitaciones distintas.
    Las filas sin id cuentan como una versión, la última, y nunca repetida. Sin
    fechas cada fila es una versión distinta.
    """
    ids = pd.Series(ids).reset_index(drop=True)
    fechas = None
    if fechas_updated is not None:
        fechas = parsear_fecha_updated(pd.Series(fechas_updated).reset_index(drop=True))
    ultima = np.zeros(len(ids), dtype=bool)
    ultima[indices_ultima_version(ids, fechas)] = True
    repetida = np.zeros(len(ids), dtype=bool)
    if fechas is not None:
        repetida = (pd.DataFrame({'id': ids, 'fecha': fechas}).duplicated().to_numpy()
                    & ids.notna().to_numpy())
    distintas = ids[~repetida].value_counts()
    n_versiones = ids.map(distintas).fillna(1).astype('int64').to_numpy()
    return ultima, n_versiones, repetida

def info_versiones(ids, fechas_updated=None):
    """(es_ultima_version, n_versiones) de cada fila, sin eliminar ninguna (ver marcas_version)."""
    ultima, n_versiones, _ = marcas_version(ids, fechas_updated)
    return ultima, n_versiones

def marcar_versiones(df):
    """Añade n_versiones, es_ultima_version y entrada_repetida (modifica df; no elimina filas)."""
    if 'id' not in df.columns:
        return df
    fecha = df['fecha_updated'] if 'fecha_updated' in df.columns else None
    ultima, n_versiones, repetida = marcas_version(df['id'], fecha)
    df['n_versiones'] = n_versiones
    df['es_ultima_version'] = ultima
    df['entrada_repetida'] = repetida
    return df

# Columnas de códigos que en algunos parquet publicados se guardaron como float
COLUMNAS_CODIGO = ['tipo_contrato_code', 'subtipo_code', 'procedimiento_code',
                   'urgencia', 'id_plataforma']

def codigos_a_texto(serie, ancho=None):
    """Códigos como texto: 1.0 → '1'. Con ancho rellena con ceros (CPV 9134100.0 → '09134100')."""
    if pd.api.types.is_float_dtype(serie):
        valores = serie.dropna()
        if (valores == valores.round()).all():
            serie = serie.astype('Int64')
    texto = serie.astype('string').str.strip().str.replace(r'\.0$', '', regex=True)
    texto = texto.mask(texto.isin(['', 'nan', 'None', '<NA>']))
    if ancho:
        numerico = texto.str.fullmatch(r'\d+').fillna(False).astype(bool)
        texto = texto.mask(numerico, texto.str.zfill(ancho))
    return texto.astype(object).where(texto.notna(), None)

def etiquetar(codigos, mapa):
    """Etiqueta legible a partir del código (si no está en el mapa, el propio código)."""
    return codigos.map(mapa).fillna(codigos)

def _normalizar_columnas(df):
    """Semántica actual de importes, códigos como texto y etiquetas desde los códigos."""
    if 'importe_sin_iva' in df.columns and 'valor_estimado_contrato' not in df.columns:
        # Esquema de los parquet publicados hasta v2026.02: importe_sin_iva
        # contenía EstimatedOverallContractAmount (issue #6)
        df.rename(columns={'importe_sin_iva': 'valor_estimado_contrato'}, inplace=True)
        df.insert(df.columns.get_loc('valor_estimado_contrato') + 1, 'importe_sin_iva', np.nan)

    for col in COLUMNAS_CODIGO:
        if col in df.columns:
            df[col] = codigos_a_texto(df[col])
    if 'cpv_principal' in df.columns:
        df['cpv_principal'] = codigos_a_texto(df['cpv_principal'], ancho=8)

    if 'tipo_contrato_code' in df.columns:
        df['tipo_contrato'] = etiquetar(df['tipo_contrato_code'], TIPOS_CONTRATO)
    if 'procedimiento_code' in df.columns:
        procedimiento = etiquetar(df['procedimiento_code'], PROCEDIMIENTOS)
        if 'procedimiento' in df.columns and 'conjunto' in df.columns:
            # Las consultas preliminares de mercado usan sus propias etiquetas
            es_consulta = df['conjunto'].astype(str) == 'consultas'
            procedimiento = procedimiento.where(~es_consulta, df['procedimiento'].astype(object))
        df['procedimiento'] = procedimiento
    if 'estado_code' in df.columns:
        estado_code = codigos_a_texto(df['estado_code'])
        df['estado_code'] = estado_code
        df['estado'] = etiquetar(estado_code, ESTADOS)
    return df

def normalizar_placsp(df, solo_ultima_version=False):
    """Lleva un DataFrame PLACSP (salida de este script o parquet publicado) a la
    semántica actual de columnas, sin eliminar ningún registro.

    - Esquema de los parquet publicados hasta v2026.02: 'importe_sin_iva'
      contenía EstimatedOverallContractAmount (valor estimado), no
      TaxExclusiveAmount (issue #6). Si falta 'valor_estimado_contrato' se
      renombra y 'importe_sin_iva' queda vacía (el presupuesto sin IVA real solo
      se recupera reprocesando los ATOM).
    - Códigos guardados como float pasan a texto (CPV con cero inicial) y las
      etiquetas de tipo_contrato/procedimiento/estado se recalculan desde ellos.
    - Añade n_versiones (versiones distintas: pares id / fecha_updated),
      es_ultima_version y entrada_repetida (ver marcas_version): cada
      actualización de una licitación es una entrada del ATOM y se conserva,
      igual que las entradas publicadas dos veces; para contar licitaciones
      distintas hay que filtrar es_ultima_version. solo_ultima_version=True
      devuelve solo esas filas (para análisis; no altera los datos servidos).
    """
    df = df.copy(deep=False)  # no tocar el DataFrame del llamador
    marcar_versiones(df)
    if solo_ultima_version and 'es_ultima_version' in df.columns:
        df = df[df['es_ultima_version']].reset_index(drop=True)
    return _normalizar_columnas(df)

def leer_placsp(path, solo_ultima_version=False):
    """Lee un parquet PLACSP y lo devuelve normalizado (ver normalizar_placsp).

    Las marcas de versión se calculan leyendo solo id/fecha_updated; con
    solo_ultima_version=True se filtra en Arrow, row group a row group, antes de
    pasar a pandas (así también cabe en memoria licitaciones_espana.parquet).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    nombres = pf.schema_arrow.names
    if 'id' not in nombres:
        return _normalizar_columnas(pf.read().to_pandas())

    claves = pq.read_table(path, columns=[c for c in ('id', 'fecha_updated') if c in nombres]).to_pandas()
    ultima, n_versiones, repetida = marcas_version(claves['id'], claves.get('fecha_updated'))
    del claves

    if not solo_ultima_version:
        df = pf.read().to_pandas()
        df['n_versiones'] = n_versiones
        df['es_ultima_version'] = ultima
        df['entrada_repetida'] = repetida
        return _normalizar_columnas(df)

    conservar = np.flatnonzero(ultima)
    partes = []
    leidas = 0
    for i in range(pf.num_row_groups):
        tabla = pf.read_row_group(i)
        n = tabla.num_rows
        desde, hasta = np.searchsorted(conservar, [leidas, leidas + n])
        partes.append(tabla.take(conservar[desde:hasta] - leidas))
        leidas += n
    df = pa.concat_tables(partes).to_pandas()
    del partes
    df['n_versiones'] = n_versiones[conservar]
    df['es_ultima_version'] = True
    df['entrada_repetida'] = repetida[conservar]
    return _normalizar_columnas(df)

def mover_al_final(df, columnas):
    """Pone 'columnas' (las que existan, en ese orden) detrás de las demás."""
    finales = [c for c in columnas if c in df.columns]
    return df[[c for c in df.columns if c not in finales] + finales]

def separar_detalle(licitaciones):
    """Saca de cada entrada sus listas de detalle ('_resultados', '_lotes'...) como
    filas de tablas aparte, con id / expediente / conjunto de la entrada."""
    tablas = {nombre: [] for nombre in TABLAS_DETALLE}
    for n, lic in enumerate(licitaciones):
        lic['_n'] = n
        for nombre in TABLAS_DETALLE:
            for fila in lic.pop(f'_{nombre}', None) or []:
                tablas[nombre].append({
                    '_n': n,
                    'id': lic.get('id'),
                    'expediente': lic.get('expediente'),
                    'conjunto': lic.get('conjunto'),
                    **fila,
                })
    return tablas

def tabla_borrados(borrados):
    """Tabla _borrados: fecha_borrado en UTC y entrada_repetida (mismo id y fecha ya leídos)."""
    df = pd.DataFrame(borrados)
    df['fecha_borrado'] = parsear_fecha_updated(df['fecha_borrado'])
    df['entrada_repetida'] = (df.duplicated(['id', 'fecha_borrado']).to_numpy()
                              & df['id'].notna().to_numpy())
    return df

def guardar_tabla(df, nombre):
    """Guarda un DataFrame en CSV y Parquet dentro de OUTPUT_DIR."""
    csv_path = OUTPUT_DIR / f'{nombre}.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    size_mb = csv_path.stat().st_size / 1024 / 1024
    print(f"   ✓ CSV: {csv_path} ({size_mb:.1f} MB)")

    try:
        parquet_path = OUTPUT_DIR / f'{nombre}.parquet'
        df.to_parquet(parquet_path, index=False, compression='snappy')
        size_mb = parquet_path.stat().st_size / 1024 / 1024
        print(f"   ✓ Parquet: {parquet_path} ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"   ⚠ Parquet no disponible: {e}")

def exportar_datos(licitaciones, nombre_base='licitaciones_completo', borrados=None):
    """Exporta todas las entradas del ATOM (sin deduplicar), sus tablas de detalle
    (resultados, adjudicatarios, lotes, criterios, modificaciones) y las
    entradas borradas."""
    print(f"\n💾 EXPORTANDO DATOS")
    print("=" * 60)

    detalle = separar_detalle(licitaciones)
    df = pd.DataFrame(licitaciones)

    # Convertir tipos
    for col in FECHAS:
        if col in df.columns:
            df[col] = parsear_fechas(df[col])

    if 'fecha_updated' in df.columns:
        df['fecha_updated'] = parsear_fecha_updated(df['fecha_updated'])

    # Extraer año
    df['ano'] = df['fecha_publicacion'].dt.year

    # Se sirven todas las entradas tal como las publica la PLACSP: cada
    # actualización de una licitación es una entrada nueva con el mismo id.
    # n_versiones / es_ultima_version permiten contar licitaciones distintas.
    marcar_versiones(df)
    df = mover_al_final(df, COLUMNAS_NUEVAS)
    n_licitaciones = int(df['es_ultima_version'].sum())
    print(f"   ℹ {len(df):,} entradas de {n_licitaciones:,} licitaciones distintas "
          f"(es_ultima_version marca la más reciente de cada una); "
          f"{int(df['entrada_repetida'].sum()):,} repiten una entrada ya leída (entrada_repetida)")

    # Tablas de detalle de cada entrada (con la fecha y las marcas de su entrada)
    marcas = df[['_n', 'fecha_updated', 'es_ultima_version', 'entrada_repetida']]
    tablas = {}
    for nombre, filas in detalle.items():
        if not filas:
            continue
        tabla = pd.DataFrame(filas).merge(marcas, on='_n', how='left').drop(columns='_n')
        for col in FECHAS_DETALLE:
            if col in tabla.columns:
                tabla[col] = parsear_fechas(tabla[col])
        if nombre == 'resultados':
            tabla = mover_al_final(tabla, COLUMNAS_NUEVAS_RESULTADOS)
        tablas[nombre] = tabla
    df = df.drop(columns='_n')

    guardar_tabla(df, nombre_base)
    for nombre, tabla in tablas.items():
        guardar_tabla(tabla, f'{nombre_base}_{nombre}')
    if borrados:
        guardar_tabla(tabla_borrados(borrados), f'{nombre_base}_borrados')

    # Resúmenes sobre licitaciones distintas (última versión de cada una): sumar
    # todas las entradas contaría varias veces la misma licitación
    ultimas = df[df['es_ultima_version']]

    # Resumen por conjunto
    print(f"\n📊 RESUMEN POR CONJUNTO")
    print("=" * 60)
    if 'conjunto' in df.columns:
        for conjunto in df['conjunto'].unique():
            df_c = ultimas[ultimas['conjunto'] == conjunto]
            print(f"\n   {CONJUNTOS.get(conjunto, {}).get('nombre', conjunto)}:")
            print(f"      Entradas: {(df['conjunto'] == conjunto).sum():,} | Licitaciones: {len(df_c):,}")
            print(f"      Años: {df_c['ano'].min():.0f} - {df_c['ano'].max():.0f}")
            for col, etiqueta in IMPORTES_RESUMEN:
                if col in df_c.columns:
                    print(f"      {etiqueta}: {df_c[col].sum()/1e9:.2f}B €")

    # Resumen total
    print(f"\n📊 RESUMEN TOTAL")
    print("=" * 60)
    print(f"   Entradas: {len(df):,} | Licitaciones distintas: {len(ultimas):,}")
    print(f"   Rango fechas: {df['ano'].min():.0f} - {df['ano'].max():.0f}")
    print(f"   Órganos únicos: {df['organo_contratante'].nunique():,}")
    print(f"   Adjudicatarios únicos: {df['adjudicatario'].nunique():,}")

    for col, etiqueta in IMPORTES_RESUMEN:
        if col in ultimas.columns:
            print(f"   {etiqueta}: {ultimas[col].sum()/1e9:.2f}B €")
    df_res = tablas.get('resultados')
    if df_res is not None:
        res_ultimas = df_res[df_res['es_ultima_version'].fillna(False).astype(bool)]
        print(f"   Resultados (lotes) de la última versión: {len(res_ultimas):,} — adjudicado sin IVA: "
              f"{res_ultimas['importe_adjudicacion'].sum()/1e9:.2f}B €")
    for nombre, tabla in tablas.items():
        if nombre != 'resultados':
            print(f"   Tabla _{nombre}: {len(tabla):,} filas")
    if borrados:
        print(f"   Tabla _borrados: {len(borrados):,} entradas borradas")

    return df

# ============================================================================
# MAIN
# ============================================================================

def main():
    global DATA_DIR, OUTPUT_DIR
    ano_actual = datetime.now().year
    parser = argparse.ArgumentParser(description='Scraper completo de licitaciones públicas')
    parser.add_argument('--anos', type=str, default=f'2012-{ano_actual}',
                        help=f'Rango de años (ej: 2020-2026; por defecto 2012-{ano_actual})')
    parser.add_argument('--conjunto', type=str, choices=list(CONJUNTOS.keys()) + ['todos'],
                        default='todos', help='Conjunto de datos a descargar')
    parser.add_argument('--solo-descargar', action='store_true', help='Solo descargar, no procesar')
    parser.add_argument('--solo-procesar', action='store_true', help='Solo procesar archivos existentes')
    parser.add_argument('--data-dir', type=Path, default=None,
                        help=f'Directorio de los ZIP descargados (por defecto {DATA_DIR})')
    parser.add_argument('--output-dir', type=Path, default=None,
                        help=f'Directorio de salida CSV/Parquet (por defecto {OUTPUT_DIR})')

    args = parser.parse_args()

    if args.data_dir is not None:
        DATA_DIR = args.data_dir
    if args.output_dir is not None:
        OUTPUT_DIR = args.output_dir

    # Parsear años
    partes = args.anos.split('-')
    ano_inicio = int(partes[0])
    ano_fin = int(partes[1]) if len(partes) > 1 else ano_inicio

    # Determinar conjuntos a procesar
    if args.conjunto == 'todos':
        conjuntos = list(CONJUNTOS.keys())
    else:
        conjuntos = [args.conjunto]

    print("=" * 60)
    print("🔎 SCRAPER COMPLETO DE LICITACIONES PÚBLICAS")
    print("=" * 60)
    print(f"   Años: {ano_inicio} - {ano_fin}")
    print(f"   Conjuntos: {', '.join(conjuntos)}")

    crear_directorios()
    session = get_session()

    # Descargar
    if not args.solo_procesar:
        informe_descarga = []
        for conjunto_id in conjuntos:
            descargar_conjunto(session, conjunto_id, ano_inicio, ano_fin, informe_descarga)
        imprimir_informe_descarga(informe_descarga)

    if args.solo_descargar:
        print("\n✅ Descarga completada")
        return

    # Procesar
    print(f"\n{'='*60}")
    print(f"⚙️ PROCESANDO ARCHIVOS")
    print(f"{'='*60}")

    todas_licitaciones, borrados, informes, avisos = [], [], [], []
    for conjunto_id in conjuntos:
        todas_licitaciones.extend(procesar_conjunto(conjunto_id, ano_inicio, ano_fin,
                                                    borrados, informes, avisos))
    imprimir_informe_procesado(informes, avisos)

    nombre_base = f'licitaciones_completo_{ano_inicio}_{ano_fin}'
    if todas_licitaciones:
        exportar_datos(todas_licitaciones, nombre_base, borrados)
        print("\n✅ SCRAPING COMPLETADO")
    else:
        print("\n⚠️ No se encontraron registros")
        if borrados:
            guardar_tabla(tabla_borrados(borrados), f'{nombre_base}_borrados')

if __name__ == '__main__':
    main()
