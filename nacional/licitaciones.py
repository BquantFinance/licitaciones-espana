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
    python nacional/licitaciones.py --solo-procesar --procesos 3 --sin-csv \
        --semilla licitaciones_espana.parquet --semilla licitaciones_completo_2012_2026.parquet

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

Procesado por lotes (memoria acotada):
    Los ZIP se leen sin extraerlos a disco y sus entradas se escriben por lotes
    de --lote entradas (50.000 por defecto) como partes parquet temporales, en
    una carpeta propia de la ejecución dentro de la de salida. Al terminar se
    calculan las marcas que dependen de todas las entradas leyendo solo sus
    claves (id, fecha_updated) y cada tabla se escribe parte a parte con un
    esquema unificado: las mismas tablas (filas, orden, columnas, tipos y
    valores, en parquet y CSV) que con un único DataFrame. --procesos N lee N
    copias de ZIP a la vez (misma salida); si el sistema mata un proceso (p.ej.
    por falta de memoria) la ejecución termina con error sin publicar nada.
    --sin-csv no escribe los CSV.
    Orden de lectura (define entrada_repetida y el desempate de
    es_ultima_version): conjuntos como en CONJUNTOS, ZIP como seleccionar_zips
    y de cada ZIP la copia actual y luego las de _historico/ de la más
    reciente a la más antigua.
    Una ejecución interrumpida no deja tablas a medias: se publican solo
    cuando están todas escritas. El parquet anterior de cada tabla no se
    pierde: pasa a <salida>/_historico/ (comun.historico.guardar_version; si
    no cambió no se toca), porque puede tener entradas de ZIP que ya no están
    en disco; el CSV se sustituye (tiene los mismos datos que su parquet; un
    CSV sin parquet también pasa a _historico/). Sin ninguna entrada leída no
    se escribe ni se publica ninguna tabla (tampoco _borrados).

Semilla (--semilla, repetible; el orden es la prioridad):
    Un parquet publicado (p.ej. licitaciones_espana.parquet de v2026.02) se
    incorpora como la instantánea más antigua (docs/CONTINUACION.md, regla 4):
    de él solo se añaden, detrás de la descarga, las filas cuya clave (id,
    fecha_updated) no está en la descarga ni en una semilla anterior; nunca se
    modifica ni se duplica una fila de la descarga. Pasan por
    _normalizar_columnas (esquema antiguo) y conservan las columnas que el
    código actual no produce (al final). Una fila del publicado con
    fecha_updated nula (el código antiguo no supo leer ese atom:updated) se
    añade si ninguna fila de la descarga con su id coincide en las columnas
    de CONTENIDO_SEMILLA que tiene la semilla; si coincide, como sin fecha no
    se puede saber si es la misma versión o una anterior igual en esas
    columnas, va a la tabla _semilla_contenido (no se pierde ni duplica la
    principal). Primero se siembran las filas con la clave completa de todas
    las semillas (en su orden) y después las que no la tienen: la misma
    entrada retirada, sin fecha en licitaciones_espana y con ella en
    licitaciones_completo, sale una sola vez y con su fecha con cualquier
    orden de --semilla. Una semilla no puede ser una tabla de salida de la
    ejecución (el publicado es la única copia histórica: usar otro
    --output-dir) y una salida de este script (con _en_ultima_descarga)
    necesita --origen-semilla.
    Ámbito: solo se añaden filas de los conjuntos y años de ZIP (el de
    archivo_origen) de los que esta ejecución ha leído alguna entrada de la
    copia actual; las de otros conjuntos, de otro --anos o de años sin ZIP en
    disco (descarga a medias) no se sabe si siguen publicadas: no se añaden y
    se cuentan en el informe. Sin archivo_origen (licitaciones_completo) una
    fila solo entra si se han leído todos los años de su conjunto. Sin ninguna
    entrada en la descarga no se siembra nada ni se toca la salida anterior.
    Las marcas de versión (n_versiones, es_ultima_version, entrada_repetida)
    se calculan sobre la unión: una versión de la semilla posterior a las de
    la descarga pasa a ser la última y cambia las marcas de esas filas.
    Errores conocidos del publicado que traen esas filas: importe_sin_iva era
    el valor estimado (queda en valor_estimado_contrato e importe_sin_iva
    vacío), fecha_publicacion / ano eran las del primer anuncio y
    fecha_updated es nula en 35.627 filas (todas las consultas preliminares).
    Verificado con los datos reales: de las filas de consultas y encargos
    (18.376) y de las de los ZIP mensuales de agregación 2025/202601
    (277.570) no se añade ninguna: sus claves (o su contenido, las de fecha
    nula) están en los ZIP anuales de hoy. Con 1 de cada 40 entradas quitada
    de dos ZIP reales (encargos 2024 y consultas 2023) se añaden exactamente
    las 72 con clave completa que estaban en el publicado y las 4 sin fecha
    cuyo id ya no está en la descarga (2026-09-27).

Procedencia (tabla principal, detrás de COLUMNAS_NUEVAS; después solo las que trae una semilla):
    _origen             nulo = leída de los ZIP; si no, la semilla de la que viene
                        ('release v2026.02' o --origen-semilla)
    _en_ultima_descarga True si la entrada (id, fecha_updated) está en la copia
                        actual de algún ZIP leído (también sus copias repetidas
                        de versiones antiguas); False si solo está en versiones
                        antiguas de _historico/ (la PLACSP ya no la sirve) o
                        viene de una semilla. Sin id o sin fecha, si la fila es
                        de una copia actual.

Salida (todas las entradas de los ATOM, tal como las publica la PLACSP: cada
actualización de una licitación es una entrada con el mismo id; n_versiones y
es_ultima_version permiten contar licitaciones distintas):
    licitaciones_completo_{inicio}_{fin}.parquet/.csv
    licitaciones_completo_{inicio}_{fin}_resultados.parquet/.csv      (una fila por cac:TenderResult / lote)
    licitaciones_completo_{inicio}_{fin}_adjudicatarios.parquet/.csv  (una por cac:WinningParty de cada resultado; una UTE
                                                                       suele venir como uno solo, con id de tipo 'UTE')
    licitaciones_completo_{inicio}_{fin}_lotes.parquet/.csv           (una por cac:ProcurementProjectLot)
    licitaciones_completo_{inicio}_{fin}_criterios.parquet/.csv       (una por criterio de adjudicación, del expediente o de un lote)
    licitaciones_completo_{inicio}_{fin}_modificaciones.parquet/.csv  (una por ContractModification)
    licitaciones_completo_{inicio}_{fin}_borrados.parquet/.csv        (una por entrada borrada, at:deleted-entry; al final
                                                                       textos_originales: el @when que no es un instante)
    licitaciones_completo_{inicio}_{fin}_semilla_contenido.parquet/.csv (solo con --semilla: filas sin fecha de la
                                                                       semilla con el contenido de una fila de la descarga)
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
    fecha_limite_respuestas     consultas preliminares de mercado (tipo_registro='CPM'),
                                como en v2026.02 (tipo_condicion: 'A' → 'Tipo A'); como allí,
                                su expediente es el id de la consulta y fecha_limite, LimitDate
    tipo_condicion_code, motivo_tipo_condicion, motivo_seleccion, adjunto_consulta_url
                                ConditionTypeCode tal cual, ConditionTypeReasonText,
                                PartySelectionReasonText y cac:Attachment de la consulta
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
    documentos_generales(_url)  cac-place-ext:GeneralDocument (anuncios, actas, documento de
                                formalización de los encargos...): FileName y URI, ' | '
    n_modificaciones            ContractModification de la entrada (detalle en _modificaciones)
    textos_originales           JSON con los textos publicados que no se pudieron convertir a
                                número o fecha (quedan nulos en su columna), de la entrada y de
                                su detalle: {"fecha_publicacion": "0202-07-03",
                                "resultados[1].importe_adjudicacion": "1.234,56"}. Las fechas
                                (también atom:updated) fuera de 1677-2262 (años mal escritos)
                                son nulas con pandas 2 y 3
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
entrada_repetida. En _adjudicatarios, ids_adjudicatario: todos los
PartyIdentification del adjudicatario ('NIF:A79365821 | ID_PLATAFORMA:...').
"""

import contextlib
import io
import itertools
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import os
import re
import shutil
import socket
import sys
import zipfile
import requests
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path, PurePosixPath
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
    en curso y, cuando se cierra, publica el anual. Los mensuales de ese año
    se siguen sirviendo, pero ya no se piden: sus copias locales se leen
    igualmente (seleccionar_zips), aunque ya no se refrescan.
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
            if filepath.suffix.lower() == '.zip' and not zipfile.is_zipfile(tmp_path):
                raise ValueError('la respuesta no es un ZIP válido; se conserva la copia actual')
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

# Mayor entero que cabe con exactitud en un double. Una columna de enteros con
# algún nulo es double (como en pandas) y Arrow no pasa a double un entero
# mayor sin perder precisión: la exportación se caería al final
ENTERO_MAXIMO = 2 ** 53

def safe_int(value):
    """Convierte a int; None si no es entero o si no cabe con exactitud en un
    double (|v| >= 2**53: siempre un error de origen, p.ej. 10000000000000001
    ofertas); con numero() su texto queda en textos_originales."""
    if value:
        try:
            entero = int(value)
        except (ValueError, TypeError):
            return None
        return entero if abs(entero) < ENTERO_MAXIMO else None
    return None

def numero(elem, xpath, perdidos=None, campo=None, convertir=safe_float):
    """Número (convertir: safe_float o safe_int) del texto de un elemento. Si
    hay texto pero no es un número, ese texto se guarda en perdidos[campo]
    para no perderlo (columna textos_originales)."""
    texto = safe_text(elem, xpath)
    valor = convertir(texto)
    if texto and perdidos is not None and (valor is None or valor != valor):   # None o NaN
        perdidos[campo] = texto
    return valor

def _anotar_perdidos(perdidos, prefijo, fila):
    """Pasa los textos no convertidos de una fila de detalle a los de su entrada."""
    for campo, texto in (fila.pop('_textos_originales', None) or {}).items():
        perdidos[f'{prefijo}.{campo}'] = texto

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
    """Un dict por cac:WinningParty de un TenderResult.

    Verificado con los datos reales: una UTE suele venir como un único
    WinningParty con un id de tipo 'UTE' (4.083 en licitaciones 2012 y 2018,
    8.457 en agregación 2025) y no como varios; los encargos traen dos ids por
    adjudicatario (NIF e ID_PLATAFORMA). nif/tipo_id son los del primero (como
    en las demás tablas) e ids_adjudicatario, todos ('tipo:valor', ' | ').
    """
    filas = []
    for i, party in enumerate(result.findall('.//cac:WinningParty', NS), 1):
        ids = party.findall('cac:PartyIdentification/cbc:ID', NS)
        id_elem = ids[0] if ids else None
        filas.append({
            'orden_adjudicatario': i,
            'adjudicatario': safe_text(party, 'cac:PartyName/cbc:Name'),
            'nif_adjudicatario': id_elem.text.strip() if id_elem is not None and id_elem.text else None,
            'tipo_id_adjudicatario': id_elem.get('schemeName') if id_elem is not None else None,
            'ids_adjudicatario': unir([f"{e.get('schemeName') or ''}:{(e.text or '').strip()}" for e in ids]),
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

    perdidos = {}
    num_ofertas = numero(result, 'cbc:ReceivedTenderQuantity', perdidos, 'num_ofertas', safe_int)
    pyme = safe_text(result, 'cbc:SMEAwardedIndicator')

    adjudicatarios = parsear_adjudicatarios(result)
    # Todos los contadores de ofertas (ReceivedTenderQuantity, SMEsReceivedTenderQuantity...)
    contadores = {nombre_local(e.tag): e.text.strip() for e in result
                  if isinstance(e.tag, str) and nombre_local(e.tag).endswith('Quantity')
                  and e.text and e.text.strip()}

    fila = {
        'lote': safe_text(result, './/cac:AwardedTenderedProject/cbc:ProcurementProjectLotID'),
        'resultado_code': safe_text(result, 'cbc:ResultCode'),
        'adjudicatario': safe_text(result, './/cac:WinningParty/cac:PartyName/cbc:Name'),
        'nif_adjudicatario': nif_adjudicatario,
        'importe_adjudicacion': numero(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount',
                                       perdidos, 'importe_adjudicacion'),
        'importe_adj_con_iva': numero(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:PayableAmount',
                                      perdidos, 'importe_adj_con_iva'),
        'fecha_adjudicacion': safe_text(result, 'cbc:AwardDate'),
        'num_ofertas': num_ofertas,
        'es_pyme': pyme == 'true' if pyme else None,
        # Nuevas (van al final de _resultados). Con {*}: namespace sin
        # verificar con datos reales (cbc o cbc-place-ext)
        'n_adjudicatarios': len(adjudicatarios),
        'adjudicatarios_todos': unir([a['adjudicatario'] for a in adjudicatarios]),
        'nifs_adjudicatarios_todos': unir([a['nif_adjudicatario'] for a in adjudicatarios]),
        'descripcion_resultado': safe_text(result, 'cbc:Description'),
        'oferta_mas_baja': numero(result, 'cbc:LowerTenderAmount', perdidos, 'oferta_mas_baja'),
        'oferta_mas_alta': numero(result, 'cbc:HigherTenderAmount', perdidos, 'oferta_mas_alta'),
        'num_ofertas_pyme': numero(result, '{*}SMEsReceivedTenderQuantity', perdidos, 'num_ofertas_pyme', safe_int),
        'contadores_ofertas': json.dumps(contadores, ensure_ascii=False) if contadores else None,
        'ofertas_anormalmente_bajas': safe_bool(safe_text(result, '{*}AbnormallyLowTendersIndicator')),
        'num_contrato': unir(textos(result, 'cac:Contract/cbc:ID')),
        'fecha_formalizacion': safe_text(result, 'cac:Contract/cbc:IssueDate'),
        'fecha_inicio_contrato': safe_text(result, 'cbc:StartDate'),
        '_adjudicatarios': adjudicatarios,
    }
    if perdidos:
        fila['_textos_originales'] = perdidos
    return fila

def importes_presupuesto(project, perdidos=None):
    """valor_estimado_contrato, importe_sin_iva e importe_con_iva de un cac:ProcurementProject."""
    budget = project.find('cac:BudgetAmount', NS) if project is not None else None
    return {
        'valor_estimado_contrato': numero(budget, 'cbc:EstimatedOverallContractAmount', perdidos,
                                          'valor_estimado_contrato'),
        'importe_sin_iva': numero(budget, 'cbc:TaxExclusiveAmount', perdidos, 'importe_sin_iva'),
        'importe_con_iva': numero(budget, 'cbc:TotalAmount', perdidos, 'importe_con_iva'),
    }

def parsear_criterios(terms, lote=None):
    """Criterios de adjudicación (TenderingTerms/AwardingTerms/AwardingCriteria)
    del expediente (lote=None) o de un lote."""
    filas = []
    if terms is None:
        return filas
    for i, crit in enumerate(terms.findall('cac:AwardingTerms/cac:AwardingCriteria', NS), 1):
        perdidos = {}
        filas.append({
            'lote': lote,
            'orden_criterio': i,
            'tipo_criterio_code': safe_text(crit, 'cbc:AwardingCriteriaTypeCode'),
            'subtipo_criterio_code': safe_text(crit, 'cbc:AwardingCriteriaSubTypeCode'),
            'descripcion': unir(textos(crit, 'cbc:Description')),
            'peso': numero(crit, 'cbc:WeightNumeric', perdidos, 'peso'),
            'nota': unir(textos(crit, 'cbc:Note')),
        })
        if perdidos:
            filas[-1]['_textos_originales'] = perdidos
    return filas

def parsear_lotes(status):
    """Filas de _lotes (una por cac:ProcurementProjectLot) y los criterios de adjudicación de cada lote."""
    lotes, criterios = [], []
    for i, lot in enumerate(status.findall('cac:ProcurementProjectLot', NS), 1):
        id_lote = safe_text(lot, 'cbc:ID')
        project = lot.find('cac:ProcurementProject', NS)
        cpvs = textos(project, './/cac:RequiredCommodityClassification/cbc:ItemClassificationCode')
        terms = lot.find('cac:TenderingTerms', NS)
        perdidos = {}
        lotes.append({
            'orden_lote': i,
            'lote': id_lote,
            'objeto_lote': safe_text(project, 'cbc:Name'),
            **importes_presupuesto(project, perdidos),
            'cpv_principal': cpvs[0] if cpvs else None,
            'cpvs': ';'.join(cpvs) or None,
            'ubicacion': safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentity'),
            'nuts': safe_text(project, './/cac:RealizedLocation/cbc:CountrySubentityCode'),
            'programas_financiacion': ';'.join(textos(terms, 'cbc:FundingProgramCode')) or None,
        })
        if perdidos:
            lotes[-1]['_textos_originales'] = perdidos
        criterios += parsear_criterios(terms, id_lote)
    return lotes, criterios

def parsear_modificaciones(status):
    """Filas de _modificaciones: una por ContractModification.

    Verificado con los datos reales (17.445 en licitaciones 2012 y 2018):
    cuelgan de ContractFolderStatus (cac:) con los campos de abajo en cbc:.
    Aun así se buscan por nombre local y 'detalle' guarda en JSON todos los
    valores del elemento, para no perder nada si la estructura cambia.
    """
    filas = []
    for i, mod in enumerate(status.findall('.//{*}ContractModification', NS), 1):
        perdidos = {}
        filas.append({
            'orden_modificacion': i,
            'id_modificacion': safe_text(mod, '{*}ID'),
            'id_contrato': safe_text(mod, '{*}ContractID'),
            'nota': unir(textos(mod, '{*}Note')),
            'importe_modificacion_sin_iva': numero(mod, '{*}ContractModificationLegalMonetaryTotal/{*}TaxExclusiveAmount',
                                                   perdidos, 'importe_modificacion_sin_iva'),
            'importe_final_sin_iva': numero(mod, '{*}FinalLegalMonetaryTotal/{*}TaxExclusiveAmount',
                                            perdidos, 'importe_final_sin_iva'),
            'duracion_modificacion': safe_text(mod, '{*}ContractModificationDurationMeasure'),
            'duracion_modificacion_unidad': safe_attr(mod, '{*}ContractModificationDurationMeasure', 'unitCode'),
            'duracion_final': safe_text(mod, '{*}FinalDurationMeasure'),
            'duracion_final_unidad': safe_attr(mod, '{*}FinalDurationMeasure', 'unitCode'),
            'detalle': json.dumps(valores_elemento(mod), ensure_ascii=False),
        })
        if perdidos:
            filas[-1]['_textos_originales'] = perdidos
    return filas

def documentos(status, xpath, nombre='cbc:ID'):
    """(nombres, URLs) de los documentos de un tipo (`nombre` y ExternalReference/cbc:URI), ' | '."""
    docs = status.findall(xpath, NS)
    return (unir([safe_text(d, nombre) for d in docs]),
            unir([safe_text(d, './/cac:ExternalReference/cbc:URI') for d in docs]))

# Columnas propias de las consultas preliminares de mercado (las seis primeras,
# como en el release v2026.02; las demás no se extraían)
COLUMNAS_CPM = ['id_consulta', 'nombre_consulta', 'condiciones', 'tipo_condicion',
                'fecha_planificada', 'fecha_limite_respuestas', 'tipo_condicion_code',
                'motivo_tipo_condicion', 'motivo_seleccion', 'adjunto_consulta_url']
# Etiqueta de ConditionTypeCode en tipo_condicion como en el release v2026.02
# ('A' → 'Tipo A'; los demás códigos, p.ej. 'S', tal cual). El código
# publicado va en tipo_condicion_code.
TIPOS_CONDICION = {'A': 'Tipo A'}

def datos_consulta(status):
    """Campos propios de una consulta preliminar de mercado (CPM).

    Verificado con los datos reales (las 4.063 entradas de los ZIP de
    consultas 2022-2026): todos cuelgan directamente de
    PreliminaryMarketConsultationStatus (cbc:), y ninguno se repite en una
    entrada; aun así se buscan por nombre local a cualquier profundidad y los
    textos que se repitan se unen con ' | '.
    """
    def todos(nombre):
        return unir(textos(status, f'.//{{*}}{nombre}'))

    def primero(nombre):
        return safe_text(status, f'.//{{*}}{nombre}')

    codigos = textos(status, './/{*}ConditionTypeCode')
    return {
        'id_consulta': primero('PreliminaryMarketConsultationID'),
        'nombre_consulta': todos('ConsultationName'),
        'condiciones': todos('ConditionsText'),
        'tipo_condicion': unir([TIPOS_CONDICION.get(c, c) for c in codigos]),
        'fecha_planificada': primero('PlannedDate'),
        'fecha_limite_respuestas': primero('LimitDate'),
        'tipo_condicion_code': unir(codigos),
        # cbc-place-ext:ConditionTypeReasonText y cbc:PartySelectionReasonText
        # (59 consultas de 4.063) y el enlace adjunto de la consulta (cac:Attachment, 974)
        'motivo_tipo_condicion': todos('ConditionTypeReasonText'),
        'motivo_seleccion': todos('PartySelectionReasonText'),
        'adjunto_consulta_url': unir(textos(status, 'cac:Attachment/cac:ExternalReference/cbc:URI')),
    }

PATRON_FECHA = re.compile(r'\d{4}-\d{2}-\d{2}')

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
                fecha = issue.text.strip()   # con su zona ('2024-01-15+01:00'), ver abajo
                fechas.append(fecha)
                if tipo == 'DOC_CN':
                    fechas_licitacion.append(fecha)
    candidatas = fechas_licitacion or fechas
    if not candidatas:
        return None
    # La más antigua por su fecha, sin zona ('2024-01-15'); una mal escrita
    # (p.ej. '0202-07-03') no gana a una válida y, si no hay ninguna válida,
    # se devuelve completa para que su texto quede en textos_originales
    validas = [f for f in candidatas if PATRON_FECHA.match(f) and '1678' <= f[:4] <= '2261']
    if validas:
        return min(validas, key=lambda f: f[:10])[:10]
    return min(candidatas, key=lambda f: f[:10])

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

    # Expediente y estado. Una consulta preliminar no tiene ContractFolderID:
    # como en el release v2026.02, su expediente es el id de la consulta
    consulta = datos_consulta(status) if es_cpm else dict.fromkeys(COLUMNAS_CPM)
    expediente = safe_text(status, 'cbc:ContractFolderID')
    if es_cpm:
        expediente = expediente or consulta['id_consulta']
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

    # Importes (un texto que no es un número se guarda en textos_originales)
    perdidos = {}
    budget = project.find('cac:BudgetAmount', NS) if project is not None else None
    valor_estimado_contrato = numero(budget, 'cbc:EstimatedOverallContractAmount', perdidos,
                                     'valor_estimado_contrato')
    importe_con_iva = numero(budget, 'cbc:TotalAmount', perdidos, 'importe_con_iva')
    importe_sin_iva = numero(budget, 'cbc:TaxExclusiveAmount', perdidos, 'importe_sin_iva')

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
    if es_cpm and fecha_limite is None:
        # Como en el release v2026.02: el plazo de una consulta es el de respuesta
        fecha_limite = consulta['fecha_limite_respuestas']
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
        _anotar_perdidos(perdidos, f'resultados[{i}]', res)
        res['orden_resultado'] = i
        for adj in res.pop('_adjudicatarios'):
            adjudicatarios.append({'lote': res['lote'], 'orden_resultado': i, **adj})
        resultados.append(res)
    primero = resultados[0] if resultados else {}
    n_lotes = len(status.findall('cac:ProcurementProjectLot', NS))
    lotes, criterios_lotes = parsear_lotes(status)
    criterios = parsear_criterios(terms) + criterios_lotes
    modificaciones = parsear_modificaciones(status)
    for nombre, filas in [('lotes', lotes), ('criterios', criterios), ('modificaciones', modificaciones)]:
        for i, fila in enumerate(filas, 1):
            _anotar_perdidos(perdidos, f'{nombre}[{i}]', fila)

    # Pliegos y demás documentos
    legal = documentos(status, 'cac:LegalDocumentReference')
    tecnico = documentos(status, 'cac:TechnicalDocumentReference')
    adicionales = documentos(status, 'cac:AdditionalDocumentReference')
    # cac-place-ext:GeneralDocument (anuncios, actas, el documento de
    # formalización de un encargo...): nombre en ExternalReference/cbc:FileName
    generales = documentos(status, 'cac-place-ext:GeneralDocument/cac-place-ext:GeneralDocumentDocumentReference',
                           './/cac:ExternalReference/cbc:FileName')

    # Fechas
    fecha_updated = safe_text(entry, 'atom:updated')
    fecha_publicacion = fecha_publicacion_licitacion(status)

    fila = {
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
        **consulta,
        # Verificado: TenderingProcess/cbc:OverThresholdIndicator ('true'/'false'),
        # en 250.494 de las 250.652 entradas de agregación 2025 (ninguna en
        # licitaciones 2012 y 2018, encargos ni consultas). Se busca a cualquier
        # profundidad por si otro conjunto o año lo pone en otro sitio
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
        'documentos_generales': generales[0],
        'documentos_generales_url': generales[1],
        'n_modificaciones': len(modificaciones),
        'tipo_registro': 'CPM' if es_cpm else 'LICITACION',
        '_resultados': resultados,
        '_adjudicatarios': adjudicatarios,
        '_lotes': lotes,
        '_criterios': criterios,
        '_modificaciones': modificaciones,
    }
    if perdidos:
        # Textos que no son un número (con las fechas, a textos_originales al exportar)
        fila['_textos_originales'] = perdidos
    return fila

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

def _leer_atom(abrir, nombre, borrados=None, informe=None):
    """Entradas de un ATOM (lista, una por entrada); abrir() devuelve el fichero
    en binario (se vuelve a abrir si hay que releerlo entero). Ver procesar_archivo_atom."""
    licitaciones, borr, inf = [], [], nuevo_informe()
    try:
        with abrir() as f:
            context = ET.iterparse(f, events=('end',))
            _leer_elementos((elem for _, elem in context), licitaciones, borr, inf)
    except Exception as e:
        print(f"\n   ⚠ Error leyendo {nombre}: {e}", end=' ')
        parciales = (licitaciones, borr, inf)
        licitaciones, borr, inf = [], [], nuevo_informe()
        try:
            with abrir() as f:
                root = ET.parse(f).getroot()
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

def procesar_archivo_atom(filepath, borrados=None, informe=None):
    """Procesa un archivo ATOM: una fila por entrada.

    Las entradas borradas (at:deleted-entry) se añaden a 'borrados' y los
    recuentos (entradas, filas, descartes por motivo, errores) a 'informe',
    si se pasan.
    """
    return _leer_atom(lambda: open(filepath, 'rb'), Path(filepath).name, borrados, informe)

def ano_de_zip(nombre):
    """Año del nombre de un ZIP de la PLACSP ('..._2024.zip', '..._202401.zip') o None."""
    match = PATRON_ZIP.search(nombre)
    return int(match.group(1)) if match else None

def miembros_atom(zf):
    """Ficheros .atom de un ZIP (ZipInfo) en orden de lectura: el que tenían
    extraídos (sorted(rglob('*.atom')), por componentes de la ruta). El orden
    fija qué aparición de cada entrada es la primera (la que no es
    entrada_repetida). Dos miembros con el mismo nombre se leen los dos, en
    su orden en el ZIP (abrir por nombre abriría dos veces el último)."""
    miembros = [i for i in zf.infolist()
                if not i.is_dir() and PurePosixPath(i.filename).name.endswith('.atom')]
    return sorted(miembros, key=lambda info: PurePosixPath(info.filename).parts)

def iterar_zip(zip_path, conjunto_id, informe=None, archivo_origen=None, lote=None):
    """Lee un ZIP de la PLACSP por lotes, sin extraerlo a disco.

    Genera tuplas (licitaciones, borrados) en orden de lectura: cada lote
    tiene como mucho `lote` entradas (None = todo el ZIP en un lote) y las
    borradas leídas desde el lote anterior. Cada ATOM se lee entero antes de
    repartirlo (así se conserva cómo se recupera un ATOM con errores). Los
    recuentos se acumulan en `informe` (nuevo_informe), que se completa con
    archivo, zip_historico, conjunto y año. archivo_origen: ver procesar_zip.
    """
    if lote is not None and lote < 0:
        raise ValueError(f'lote debe ser positivo: {lote}')
    zip_path = Path(zip_path)
    archivo_origen = archivo_origen or zip_path.name
    zip_historico = zip_path.name if zip_path.name != archivo_origen else None
    informe = informe if informe is not None else nuevo_informe()
    informe.update(archivo=archivo_origen, zip_historico=zip_historico,
                   conjunto=conjunto_id, ano=ano_de_zip(archivo_origen))
    pendientes, borr_pendientes, n_filas = [], [], 0
    try:
        print(f"   📦 Leyendo...", end=' ', flush=True)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            miembros = miembros_atom(zf)
            informe['atom'] = len(miembros)
            print(f"✓ {len(miembros)} ATOM", end=' ', flush=True)
            for info in miembros:
                borr = []
                lics = _leer_atom(lambda: zf.open(info), PurePosixPath(info.filename).name, borr, informe)
                for lic in lics:
                    lic['conjunto'] = conjunto_id
                    lic['archivo_origen'] = archivo_origen
                    # Mismo esquema que los parquet publicados (CPM = consulta preliminar)
                    # (todo lo del conjunto 'consultas' sigue siendo CPM, como hasta ahora)
                    es_cpm = lic.pop('tipo_registro', None) == 'CPM' or conjunto_id == 'consultas'
                    lic['tipo_registro'] = 'CPM' if es_cpm else 'LICITACION'
                    lic['zip_historico'] = zip_historico
                for b in borr:
                    b.update(conjunto=conjunto_id, archivo_origen=archivo_origen, zip_historico=zip_historico)
                n_filas += len(lics)
                pendientes.extend(lics)
                borr_pendientes.extend(borr)
                while lote and len(pendientes) >= lote:
                    yield pendientes[:lote], borr_pendientes
                    pendientes, borr_pendientes = pendientes[lote:], []
        descartadas = sum(informe['descartadas'].values())
        print(f"→ {n_filas:,} registros ({informe['entradas']:,} entradas"
              + (f", {informe['borrados']:,} borradas" if informe['borrados'] else '')
              + (f", {descartadas:,} descartadas" if descartadas else '') + ")")
    except zipfile.BadZipFile:
        print(f"   ✗ ZIP corrupto")
        informe['errores'].append('ZIP corrupto')
    except Exception as e:
        print(f"   ✗ Error: {e}")
        informe['errores'].append(f'{type(e).__name__}: {e}')
    if pendientes or borr_pendientes:
        yield pendientes, borr_pendientes

def procesar_zip(zip_path, conjunto_id, borrados=None, informes=None, archivo_origen=None):
    """Procesa un archivo ZIP: una fila por entrada de sus ATOM (todas en una lista;
    iterar_zip las da por lotes).

    archivo_origen: nombre del ZIP del periodo cuando zip_path es una versión
    antigua guardada en _historico/ (cuyo nombre queda en zip_historico).
    Las entradas borradas se añaden a 'borrados' y el recuento del ZIP
    (nuevo_informe) a 'informes', si se pasan.
    """
    informe = nuevo_informe()
    licitaciones, borr = [], []
    for lics, bs in iterar_zip(zip_path, conjunto_id, informe, archivo_origen):
        licitaciones.extend(lics)
        borr.extend(bs)
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

def copias_conjunto(conjunto_id, ano_inicio, ano_fin, avisos=None):
    """Copias de los ZIP descargados de un conjunto (DATA_DIR/<conjunto>) en orden
    de lectura: [(copia, nombre del ZIP del periodo)].

    Los ZIP van como seleccionar_zips y de cada uno la copia actual y después
    las antiguas de _historico/ (de la más reciente a la más antigua): así
    ninguna entrada publicada alguna vez se pierde, y la primera aparición de
    cada una es la de la copia más reciente que la trae (las demás quedan como
    entrada_repetida). Un ZIP con versiones en _historico/ pero sin copia
    actual (un guardar_version interrumpido, una copia borrada a mano) también
    se lee, con aviso. En 'avisos' se añaden los años sin ningún ZIP actual,
    esos ZIP y las versiones que no son un ZIP válido.
    """
    avisos = avisos if avisos is not None else []
    hist = _historico()
    carpeta = DATA_DIR / conjunto_id
    actuales = sorted(carpeta.glob('*.zip'))
    # Nombre del ZIP del periodo de cada versión antigua: <periodo>__<sello>.zip
    huerfanos = {carpeta / (v.name.split('__', 1)[0] + '.zip')
                 for v in (carpeta / hist.HISTORICO).glob('*__*.zip')} - set(actuales)
    zips = seleccionar_zips(actuales + sorted(huerfanos), ano_inicio, ano_fin)

    con_zip = {ano_de_zip(z.name) for z in zips if z.exists()}
    desde = max(ano_inicio, CONJUNTOS[conjunto_id]['ano_inicio'])
    faltan = [a for a in range(desde, min(ano_fin, datetime.now().year) + 1) if a not in con_zip]
    if faltan:
        avisos.append(f"{conjunto_id}: ningún ZIP de {', '.join(map(str, faltan))}")

    copias = []
    for z in zips:
        if not z.exists():
            avisos.append(f"{conjunto_id}: {z.name} no tiene copia actual; se leen sus versiones de _historico/")
        for copia in reversed(hist.versiones(z)):  # actual primero
            if copia != z and not zipfile.is_zipfile(copia):
                avisos.append(f"{conjunto_id}: {copia.name} (en _historico/) no es un ZIP válido; se ignora")
                continue
            copias.append((copia, z.name))
    return copias

def _cabecera_conjunto(conjunto_id, copias):
    """Línea de progreso al empezar a leer las copias de un conjunto."""
    n_zips = len({origen for _, origen in copias})
    print(f"\n📦 {CONJUNTOS[conjunto_id]['nombre']}: {n_zips} archivos"
          + (f" (+{len(copias) - n_zips} versiones anteriores)" if len(copias) > n_zips else ''))

def procesar_conjunto(conjunto_id, ano_inicio, ano_fin, borrados=None, informes=None, avisos=None):
    """Lee los ZIP descargados de un conjunto con todas sus versiones, en el orden
    de copias_conjunto, y devuelve todas sus entradas en una lista (main() las
    procesa por lotes: procesar_copias)."""
    copias = copias_conjunto(conjunto_id, ano_inicio, ano_fin, avisos)
    licitaciones = []
    if copias:
        _cabecera_conjunto(conjunto_id, copias)
        for i, (copia, origen) in enumerate(copias, 1):
            print(f"   [{i}/{len(copias)}] {copia.name}", end='')
            licitaciones.extend(procesar_zip(copia, conjunto_id, borrados, informes, archivo_origen=origen))
    return licitaciones

def _tarea_copia(tarea):
    """Lee una copia de ZIP y escribe sus lotes como partes (escribir_parte).

    Es el trabajo de cada proceso de procesar_copias: devuelve (partes,
    informe, texto impreso); con capturar=True lo impreso se devuelve en vez
    de escribirse (el proceso principal lo imprime en orden de lectura).
    """
    indice, copia, conjunto_id, archivo_origen, dir_partes, lote, capturar = tarea
    salida = io.StringIO()
    with contextlib.redirect_stdout(salida) if capturar else contextlib.nullcontext():
        informe = nuevo_informe()
        partes = [escribir_parte(dir_partes, f'{indice:06d}-{j:06d}', lics, borr)
                  for j, (lics, borr) in enumerate(iterar_zip(copia, conjunto_id, informe,
                                                              archivo_origen, lote))]
    return partes, informe, salida.getvalue()

def _ejecutor_procesos(procesos):
    """Procesos para leer copias a la vez. ProcessPoolExecutor, y no
    multiprocessing.Pool: si el sistema mata un proceso (p.ej. por falta de
    memoria) da BrokenProcessPool en vez de esperar para siempre su resultado
    (no se publica nada). Con spawn/forkserver cada proceso lee una sola copia
    y se sustituye (libera su memoria); con fork no se puede."""
    extra = {}
    if multiprocessing.get_start_method() != 'fork' and sys.version_info >= (3, 11):
        extra['max_tasks_per_child'] = 1
    return ProcessPoolExecutor(procesos, **extra)

def procesar_copias(copias, exportacion, informes=None, procesos=1):
    """Lee las copias de ZIP [(conjunto, copia, archivo_origen)] y añade sus
    lotes a `exportacion` (ExportacionPlacsp) en orden de lectura. Se puede
    llamar varias veces (p.ej. un conjunto por llamada): cada copia escribe
    partes con un índice propio de la exportación.

    Con procesos > 1 se leen varias copias a la vez (cada una a sus propias
    partes) y se ensamblan en el orden de la lista: la salida es idéntica a
    la de procesos=1. Los recuentos de cada copia se añaden a 'informes' y el
    (conjunto, año) de cada copia actual de la que se ha leído alguna entrada,
    al ámbito de la exportación (lo que se ha vuelto a descargar: ver
    ExportacionPlacsp).
    """
    if exportacion.ambito is None:
        exportacion.ambito = set()   # sin ninguna copia leída, las semillas no aportan nada
    tareas = [(exportacion.siguiente_indice(), str(copia), conjunto, origen, str(exportacion.dir_partes),
               exportacion.lote, procesos > 1)
              for conjunto, copia, origen in copias]
    total = {}
    for conjunto, _, _ in copias:
        total[conjunto] = total.get(conjunto, 0) + 1
    with contextlib.ExitStack() as pila:
        if procesos > 1:
            resultados = pila.enter_context(_ejecutor_procesos(procesos)).map(_tarea_copia, tareas)
        else:
            resultados = map(_tarea_copia, tareas)
        vistos = {}
        for (conjunto, copia, origen), tarea in zip(copias, tareas):
            if conjunto not in vistos:
                _cabecera_conjunto(conjunto, [(c, o) for k, c, o in copias if k == conjunto])
            vistos[conjunto] = vistos.get(conjunto, 0) + 1
            print(f"   [{vistos[conjunto]}/{total[conjunto]}] {Path(copia).name}", end='', flush=True)
            partes, informe, texto = next(resultados)
            print(texto, end='')
            for parte in partes:
                exportacion.anadir_parte(parte)
            if informe['filas'] and informe['zip_historico'] is None:
                exportacion.anadir_ambito(conjunto, informe['ano'])
            if informes is not None:
                informes.append(informe)

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
    'otros_documentos', 'otros_documentos_url', 'documentos_generales', 'documentos_generales_url',
    'n_modificaciones', 'textos_originales', 'zip_historico', 'entrada_repetida',
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
# Todas las tablas que escribe la exportación (_semilla_contenido: ver --semilla)
TABLAS_SALIDA = ('principal',) + TABLAS_DETALLE + ('borrados', 'semilla_contenido')

# Rango de datetime64[ns] (1677-09-21 a 2262-04-11). Fuera de él (años mal
# escritos como '0202-07-03' o '24-12-27') pandas 2 da NaT y pandas 3 lee la
# fecha tal cual (año 202, con otra resolución): para que las dos versiones
# den la misma salida, esas fechas quedan nulas con las dos y el texto
# publicado se conserva en textos_originales (ningún valor se pierde). Así
# además todas las fechas caben en ns, que es como se comparan las claves.
FECHA_MINIMA, FECHA_MAXIMA = pd.Timestamp.min.ceil('D'), pd.Timestamp.max.floor('D')

def parsear_fechas(serie):
    """Fechas xs:date de CODICE ('2024-01-15', a veces con zona: '2024-01-15+01:00').
    Nulas si no son una fecha AAAA-MM-DD o están fuera de FECHA_MINIMA-FECHA_MAXIMA."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie
    fechas = pd.to_datetime(serie.astype('string').str[:10], errors='coerce', format='%Y-%m-%d')
    return fechas.where(fechas.isna() | ((fechas >= FECHA_MINIMA) & (fechas <= FECHA_MAXIMA)))

def parsear_fecha_updated(serie):
    """atom:updated (y at:deleted-entry/@when) en UTC. format='ISO8601' evita que
    pandas infiera el formato del primer valor y convierta en NaT los que no
    llevan milisegundos (o viceversa). Nulas si no son un instante ISO 8601 o
    están fuera de FECHA_MINIMA-FECHA_MAXIMA (en UTC; ver parsear_fechas)."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie if getattr(serie.dt, 'tz', None) is not None else serie.dt.tz_localize('UTC')
    fechas = pd.to_datetime(serie, errors='coerce', utc=True, format='ISO8601')
    return fechas.where(fechas.isna() | ((fechas >= FECHA_MINIMA.tz_localize('UTC'))
                                         & (fechas <= FECHA_MAXIMA.tz_localize('UTC'))))

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
    fechas (o con atom:updated nulo) cada fila es una versión distinta.
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
                    & ids.notna().to_numpy() & fechas.notna().to_numpy())
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
    df = (pa.concat_tables(partes) if partes else pf.schema_arrow.empty_table()).to_pandas()
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
                              & df['id'].notna().to_numpy() & df['fecha_borrado'].notna().to_numpy())
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

# ----------------------------------------------------------------------------
# Exportación por lotes (memoria acotada)
# ----------------------------------------------------------------------------
# Las entradas se escriben por lotes como partes parquet temporales en una
# carpeta propia de cada ejecución (.<nombre>.partes-XXXX, en la de salida):
# por lote, la tabla principal sin las marcas de versión, sus tablas de
# detalle y las entradas borradas. Al cerrar se calculan las marcas que
# dependen de todas las entradas (versiones, entrada_repetida,
# _en_ultima_descarga) leyendo solo sus claves, y cada tabla final se escribe
# parte a parte con un esquema Arrow unificado: el mismo resultado que un único
# DataFrame con todas las entradas (lo que hacía exportar_datos), con la
# memoria acotada por el tamaño del lote más las claves (id, fecha_updated) y
# las marcas de todas las filas.

TAM_LOTE = 50_000
FILAS_GRUPO = 1024 * 1024    # filas por row group: las de pandas/pyarrow por defecto
MARCAS_VERSION = ['n_versiones', 'es_ultima_version', 'entrada_repetida']
MARCAS_DETALLE = ['fecha_updated', 'es_ultima_version', 'entrada_repetida']
# Procedencia de cada fila (van detrás de COLUMNAS_NUEVAS; ver el docstring del módulo)
COLUMNAS_PROCEDENCIA = ['_origen', '_en_ultima_descarga']
# Columnas que el código que generó el release v2026.02 y el actual extraen
# igual. Verificado con los datos reales: en las 188.193 claves (id,
# fecha_updated) no nulas de licitaciones 2012 y 2018, encargos y consultas
# presentes en el publicado y en la descarga de hoy coinciden todas (salvo 69
# fechas de adjudicación mal escritas, p.ej. '0202-07-03', que el publicado
# guardó nulas y parsear_fechas también deja nulas), y las 3.681 consultas del
# publicado (todas con fecha_updated nula) casan con una entrada de la
# descarga en todas ellas. Con ellas se decide si una fila de la semilla con
# fecha_updated nula (el código antiguo no supo leer ese atom:updated) sigue
# publicada: si alguna fila de la descarga con el mismo id coincide en todas,
# no se añade. Y al revés: una fila con fecha que no está en la descarga no
# se añade si coincide en todas con una fila sin fecha ya presente (la misma
# entrada, sembrada antes desde otro publicado). Verificado con los datos
# reales: las 6.975 filas de licitaciones_completo cuya clave no está en
# licitaciones_espana tienen allí una fila con su id, fecha nula y el mismo
# contenido (y hay una sola de ellas por id y contenido).
CONTENIDO_SEMILLA = ['expediente', 'objeto', 'organo_contratante', 'nif_organo', 'estado_code',
                     'valor_estimado_contrato', 'importe_con_iva', 'importe_adjudicacion',
                     'importe_adj_con_iva', 'adjudicatario', 'nif_adjudicatario',
                     'fecha_adjudicacion', 'url', 'tipo_contrato_code', 'procedimiento_code',
                     'cpv_principal', 'id_consulta', 'nombre_consulta', 'condiciones',
                     'tipo_condicion', 'fecha_planificada', 'fecha_limite_respuestas']
_UNIDADES = ['s', 'ms', 'us', 'ns']

def _es_texto(tipo):
    return pa.types.is_string(tipo) or pa.types.is_large_string(tipo)

def _tipo_texto():
    """Tipo Arrow con el que pandas guarda el texto en este entorno
    (large_string con pandas 3, string con pandas 2)."""
    return pa.Table.from_pandas(pd.DataFrame({'x': ['a']}), preserve_index=False).schema.field('x').type

def tipo_unificado(tipos, con_nulos=False):
    """Tipo Arrow de una columna en la tabla final a partir de los de cada parte:
    el mismo que tendría si todas las filas estuvieran en un único DataFrame.

    - Nulo (la columna estaba vacía en una parte) → el tipo real de las demás.
    - Entero con algún nulo (con_nulos: nulos en alguna parte o columna que
      falta en alguna) o junto a decimales → double, como float64 en pandas.
    - Fechas → la resolución más fina (pandas 3 la deduce de cada lote);
      date32 junto a timestamp → timestamp.
    - Texto → large_string si alguna parte lo es; categorías → sus valores.
    - Números o booleanos junto a texto → texto (sin perder ningún valor).
    """
    reales = []
    for tipo in tipos:
        if pa.types.is_dictionary(tipo):
            tipo = tipo.value_type
        if not pa.types.is_null(tipo) and tipo not in reales:
            reales.append(tipo)
    if not reales:
        return pa.null()
    if any(_es_texto(t) for t in reales):
        return pa.large_string() if any(pa.types.is_large_string(t) for t in reales) else pa.string()
    if all(pa.types.is_timestamp(t) or pa.types.is_date(t) for t in reales):
        marcas = [t for t in reales if pa.types.is_timestamp(t)]
        if not marcas:
            return reales[0] if len(reales) == 1 else pa.date32()
        zonas = {t.tz for t in marcas}
        if len(zonas) > 1:
            raise ValueError(f'Fechas con zonas horarias distintas en las partes: {sorted(map(str, zonas))}')
        return pa.timestamp(max((t.unit for t in marcas), key=_UNIDADES.index), tz=zonas.pop())
    if all(pa.types.is_boolean(t) for t in reales):
        return pa.bool_()
    if all(pa.types.is_integer(t) for t in reales) and not con_nulos:
        return max(reales, key=lambda t: t.bit_width)
    if all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in reales):
        return pa.float64()
    return _tipo_texto()

def _convertir(columna, tipo):
    """Columna Arrow (Array o ChunkedArray) al tipo unificado, sin perder valores
    (un ChunkedArray sigue siéndolo: quien lo lee recorre sus .chunks)."""
    if columna.type == tipo:
        return columna
    if pa.types.is_dictionary(columna.type):
        columna = columna.cast(columna.type.value_type)
        if columna.type == tipo:
            return columna
    if pa.types.is_null(columna.type):
        # Columna vacía en esta parte (p.ej. id en un lote de entradas sin <id>)
        nulos = pa.nulls(len(columna), tipo)
        return pa.chunked_array([nulos], type=tipo) if isinstance(columna, pa.ChunkedArray) else nulos
    if _es_texto(tipo) and pa.types.is_boolean(columna.type):
        # Como los escribe pandas ('True'/'False'), no como Arrow ('true'/'false')
        return pc.if_else(columna, 'True', 'False').cast(tipo)
    return columna.cast(tipo)

def _union_ordenada(listas):
    """Columnas en orden de primera aparición (como pd.DataFrame con una lista de dicts)."""
    vistas = {}
    for lista in listas:
        for columna in lista:
            vistas.setdefault(columna, None)
    return list(vistas)

def _al_final(columnas, finales):
    """mover_al_final sobre una lista de nombres de columna."""
    finales = [c for c in finales if c in columnas]
    return [c for c in columnas if c not in finales] + finales

def _esquema_final(campos, bool_con_nulos=()):
    """Esquema Arrow de una tabla final con los metadatos de pandas que tendría
    escrita con to_parquet desde un único DataFrame: se generan desde un
    DataFrame vacío con esos tipos, con las columnas booleanas que tienen
    algún nulo como object (así las guarda pandas)."""
    esquema = pa.schema(campos)
    vacio = esquema.empty_table().to_pandas()
    for columna in bool_con_nulos:
        vacio[columna] = vacio[columna].astype(object)
    return pa.Table.from_pandas(vacio, schema=esquema, preserve_index=False).schema

def _tabla_parte(df, ruta, columnas, tipos=None):
    """Escribe una tabla de una parte y devuelve su descripción: ruta, filas,
    columnas en el orden de las claves de sus dicts y tipo Arrow y nulos de
    cada columna (para unificar el esquema al ensamblar). Con `tipos`, las
    columnas se pasan a esos tipos si se puede sin perder valores. Nunca pisa
    una parte ya escrita (sus filas se perderían en silencio)."""
    if Path(ruta).exists():
        raise FileExistsError(f'La parte {ruta} ya existe: no se sobrescribe')
    tabla = pa.Table.from_pandas(df, preserve_index=False)
    if tipos:
        for i, campo in enumerate(tabla.schema):
            tipo = tipos.get(campo.name)
            if tipo is not None and not pa.types.is_null(tipo) and campo.type != tipo:
                try:
                    tabla = tabla.set_column(i, campo.name, _convertir(tabla.column(i), tipo))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                    pass   # se queda con su tipo; tipo_unificado decide al ensamblar
    pq.write_table(tabla, ruta, compression='snappy')
    return {'ruta': str(ruta), 'filas': tabla.num_rows, 'columnas': list(columnas),
            'tipos': {c: tabla.schema.field(c).type for c in tabla.column_names},
            'nulos': {c: tabla.column(c).null_count for c in tabla.column_names}}

def _convertir_fechas(df, columnas, convertir, textos, clave=None):
    """Convierte las columnas de fecha de df y anota en textos[n] (n = fila de
    la entrada; clave(i) da su nombre en el JSON) los textos no vacíos que
    quedan nulos: fechas mal escritas o fuera de rango."""
    for col in columnas:
        if col not in df.columns:
            continue
        crudo = df[col]
        df[col] = convertir(crudo)
        if pd.api.types.is_datetime64_any_dtype(crudo):
            continue
        texto = crudo.astype('string').str.strip()
        for i in np.flatnonzero((texto.fillna('') != '').to_numpy() & df[col].isna().to_numpy()):
            n, campo = (i, col) if clave is None else clave(i, col)
            textos.setdefault(n, {})[campo] = crudo.iloc[i]

def escribir_parte(dir_partes, prefijo, licitaciones, borrados=None):
    """Escribe un lote como partes parquet temporales y devuelve su descripción.

    Aplica a las entradas del lote lo mismo que se aplicaba a un único
    DataFrame con todas (fechas, fecha_updated en UTC, ano); lo que depende de
    todas las entradas (marcas de versión, entrada_repetida de los borrados,
    _en_ultima_descarga) se calcula al ensamblar. En el detalle, '_n' es la
    posición de su entrada dentro del lote. textos_originales (JSON) guarda,
    por entrada, los textos publicados que no se pudieron convertir a número o
    fecha, suyos y de su detalle ('resultados[2].fecha_formalizacion').
    """
    dir_partes = Path(dir_partes)
    parte = {'prefijo': prefijo, 'filas': len(licitaciones), 'semilla': False, 'tablas': {}}
    if licitaciones:
        textos = {n: dict(t) for n, t in enumerate(lic.pop('_textos_originales', None) for lic in licitaciones) if t}
        detalle = separar_detalle(licitaciones)
        tablas = {}
        for nombre, filas in detalle.items():
            if not filas:
                continue
            tabla = pd.DataFrame(filas)
            orden = tabla.groupby('_n', sort=False).cumcount().to_numpy() + 1
            _convertir_fechas(tabla, FECHAS_DETALLE, parsear_fechas, textos,
                              lambda i, col: (tabla['_n'].iloc[i], f'{nombre}[{orden[i]}].{col}'))
            tablas[nombre] = tabla
        df = pd.DataFrame(licitaciones)
        columnas = list(df.columns)
        _convertir_fechas(df, FECHAS, parsear_fechas, textos)
        _convertir_fechas(df, ['fecha_updated'], parsear_fecha_updated, textos)
        if 'fecha_publicacion' in df.columns:
            df['ano'] = df['fecha_publicacion'].dt.year
        else:
            df['ano'] = np.nan
        df['textos_originales'] = [json.dumps(textos[n], ensure_ascii=False) if n in textos else None
                                   for n in range(len(df))]
        parte['tablas']['principal'] = _tabla_parte(df, dir_partes / f'{prefijo}.principal.parquet', columnas)
        del df
        for nombre, tabla in tablas.items():
            parte['tablas'][nombre] = _tabla_parte(tabla, dir_partes / f'{prefijo}.{nombre}.parquet',
                                                   list(tabla.columns))
    if borrados:
        tabla = pd.DataFrame(borrados)
        columnas = list(tabla.columns)
        # Como en la tabla principal: un @when que no es un instante (o fuera de
        # rango) queda nulo y su texto va a textos_originales (al final)
        textos_b = {}
        _convertir_fechas(tabla, ['fecha_borrado'], parsear_fecha_updated, textos_b)
        tabla['textos_originales'] = [json.dumps(textos_b[n], ensure_ascii=False) if n in textos_b else None
                                      for n in range(len(tabla))]
        parte['tablas']['borrados'] = _tabla_parte(tabla, dir_partes / f'{prefijo}.borrados.parquet', columnas)
    return parte

def _leer_columna(tabla, columna, tipo):
    """Columna de la parte `tabla` (descripción) convertida a `tipo`; nulos si no la tiene."""
    if columna not in tabla['tipos']:
        return pa.chunked_array([pa.nulls(tabla['filas'], tipo)], type=tipo)
    leida = pq.read_table(tabla['ruta'], columns=[columna]).column(0)
    return _convertir(leida, tipo) if leida.type != tipo else leida

def _instantes(columna):
    """Columna Arrow de fechas de actualización como timestamp[ns, UTC] (texto: ISO 8601)."""
    if pa.types.is_dictionary(columna.type):
        columna = columna.cast(columna.type.value_type)
    tipo = pa.timestamp('ns', tz='UTC')
    if pa.types.is_timestamp(columna.type) or pa.types.is_date(columna.type) or pa.types.is_null(columna.type):
        return _convertir(columna, tipo)
    serie = parsear_fecha_updated(columna.to_pandas()).dt.as_unit('ns')
    return pa.chunked_array([pa.array(serie, type=tipo)], type=tipo)

def _codigos_id(columnas):
    """Código entero de cada id en varias columnas Arrow de texto (el mismo id,
    el mismo código en todas; -1 = sin id), sin pasar por objetos de Python."""
    trozos = []
    for columna in columnas:
        for trozo in columna.chunks:
            if pa.types.is_dictionary(trozo.type):
                trozo = trozo.dictionary_decode()
            trozos.append(trozo if pa.types.is_large_string(trozo.type) else trozo.cast(pa.large_string()))
    if not trozos:
        return np.zeros(0, dtype=np.int64)
    codificada = pa.chunked_array(trozos, type=pa.large_string()).dictionary_encode()
    return np.concatenate([pc.fill_null(trozo.indices, -1).to_numpy(zero_copy_only=False)
                           for trozo in codificada.chunks]).astype(np.int64)

def _enteros(codigos):
    """Códigos (-1 = nulo) como enteros de pandas con nulos."""
    codigos = np.asarray(codigos, dtype=np.int64)
    return pd.arrays.IntegerArray(np.where(codigos < 0, 0, codigos), codigos < 0)

def _factorizar(columna):
    """(código por fila, valores) de una columna Arrow: el mismo valor, el mismo
    código (nulo: -1), sin crear un objeto de Python por fila."""
    if pa.types.is_null(columna.type) or len(columna) == 0:
        return np.full(len(columna), -1, dtype=np.int64), []
    if not pa.types.is_dictionary(columna.type):
        columna = columna.dictionary_encode()
    columna = pa.table({'c': columna}).unify_dictionaries().column('c')
    valores = columna.chunk(0).dictionary.to_pylist()
    codigos = [pc.fill_null(trozo.indices.cast(pa.int64()), -1).to_numpy(zero_copy_only=False)
               for trozo in columna.chunks]
    return np.concatenate(codigos).astype(np.int64), valores

def periodos_posibles(conjunto, ano, ano_actual=None):
    """(conjunto, año de ZIP) de los que puede venir una fila de una semilla: el
    suyo o, si no se sabe el conjunto (nulo) o el año (sin archivo_origen, como
    en licitaciones_completo_2012_2026.parquet), todos los posibles de
    CONJUNTOS hasta el año actual. Vacío si no puede venir de ningún ZIP."""
    if conjunto is not None and ano is not None:
        return {(conjunto, ano)}
    ano_actual = ano_actual or datetime.now().year
    posibles = set()
    for c in ([conjunto] if conjunto is not None else CONJUNTOS):
        if c in CONJUNTOS:
            desde = CONJUNTOS[c]['ano_inicio']
            anos = [ano] if ano is not None else range(desde, ano_actual + 1)
            posibles |= {(c, a) for a in anos if desde <= a <= ano_actual}
    return posibles

# Columnas de texto del scraper que licitaciones_espana.parquet (v2026.02)
# guardó como número y _normalizar_columnas no convierte
COLUMNAS_TEXTO_SEMILLA = ['duracion']

def _preparar_semilla(df, tipos, origen):
    """Filas de una semilla (parquet publicado) con la semántica actual:
    _normalizar_columnas (esquema antiguo de importes, códigos como texto y
    etiquetas), sin las marcas que se recalculan sobre la unión, con _origen y
    como texto las columnas que el publicado guardó como número y la descarga
    tiene como texto (duracion: 12.0 → '12', como los códigos)."""
    df = _normalizar_columnas(df)
    df = df.drop(columns=[c for c in MARCAS_VERSION + ['_en_ultima_descarga'] if c in df.columns])
    propio = df['_origen'] if '_origen' in df.columns else pd.Series(None, index=df.index, dtype=object)
    df['_origen'] = propio.astype(object).where(propio.notna(), origen)
    for col in df.columns:
        serie, tipo = df[col], tipos.get(col)
        if isinstance(serie.dtype, pd.api.extensions.ExtensionDtype) and pd.api.types.is_float_dtype(serie.dtype):
            df[col] = serie = serie.astype('float64')   # Float64: NA y NaN, nulos como en la descarga
        if ((col in COLUMNAS_TEXTO_SEMILLA or (tipo is not None and _es_texto(tipo)))
                and pd.api.types.is_numeric_dtype(serie) and not pd.api.types.is_bool_dtype(serie)):
            df[col] = codigos_a_texto(serie)
    return df

def _sumar_informes(anterior, informe):
    """Informe de una semilla con sus dos fases (claves completas e incompletas)."""
    if anterior is None:
        return informe
    total = dict(anterior)
    for campo in ('leidas', 'anadidas', 'descartadas_clave', 'descartadas_contenido', 'fuera_ambito'):
        total[campo] = anterior[campo] + informe[campo]
    total['ejemplos'] = {caso: (anterior['ejemplos'].get(caso, []) + informe['ejemplos'].get(caso, []))[:5]
                         for caso in anterior['ejemplos']}
    detalle = dict(anterior.get('fuera_ambito_detalle') or {})
    for etiqueta, filas in (informe.get('fuera_ambito_detalle') or {}).items():
        detalle[etiqueta] = detalle.get(etiqueta, 0) + filas
    total['fuera_ambito_detalle'] = dict(sorted(detalle.items()))
    return total

_ENTEROS_PANDAS = {pa.int8(): pd.Int8Dtype(), pa.int16(): pd.Int16Dtype(), pa.int32(): pd.Int32Dtype(),
                   pa.int64(): pd.Int64Dtype(), pa.uint8(): pd.UInt8Dtype(), pa.uint16(): pd.UInt16Dtype(),
                   pa.uint32(): pd.UInt32Dtype(), pa.uint64(): pd.UInt64Dtype()}

class _EscritorTabla:
    """Escribe una tabla final parte a parte: parquet con row groups de
    filas_grupo filas (como to_parquet de un único DataFrame) y, si se pide,
    CSV con un solo BOM (utf-8-sig) y una sola cabecera, desde las partes ya
    convertidas al esquema unificado (como escribiría to_csv ese DataFrame)."""

    def __init__(self, esquema, ruta_parquet, ruta_csv=None, filas_grupo=FILAS_GRUPO):
        self.esquema, self.filas_grupo = esquema, filas_grupo
        self.parquet = pq.ParquetWriter(ruta_parquet, esquema, compression='snappy')
        self.csv = open(ruta_csv, 'w', encoding='utf-8-sig', newline='') if ruta_csv else None
        self.pendientes, self.n_pendientes, self.filas = [], 0, 0

    def escribir(self, tabla):
        if self.csv is not None:
            # Enteros con nulos (filas de una semilla) como enteros: '3', no '3.0'
            tabla.to_pandas(types_mapper=_ENTEROS_PANDAS.get).to_csv(self.csv, index=False,
                                                                      header=self.filas == 0)
        self.filas += tabla.num_rows
        self.pendientes.append(tabla)
        self.n_pendientes += tabla.num_rows
        while self.n_pendientes >= self.filas_grupo:
            junta = pa.concat_tables(self.pendientes)
            self.parquet.write_table(junta.slice(0, self.filas_grupo), row_group_size=self.filas_grupo)
            resto = junta.slice(self.filas_grupo)
            self.pendientes, self.n_pendientes = ([resto] if resto.num_rows else []), resto.num_rows

    def cerrar(self):
        try:
            if self.pendientes:
                self.parquet.write_table(pa.concat_tables(self.pendientes), row_group_size=self.filas_grupo)
        finally:
            self.parquet.close()
            if self.csv is not None:
                self.csv.close()

class _Resumen:
    """Recuentos de la tabla principal para los resúmenes finales, parte a parte."""

    def __init__(self):
        self.conjuntos = {}     # conjunto → [entradas, licitaciones, años, sumas]
        self.anos = []
        self.sumas = {}

    def anadir(self, tabla):
        columnas = [c for c in ['conjunto', 'ano', 'es_ultima_version'] + [c for c, _ in IMPORTES_RESUMEN]
                    if c in tabla.column_names]
        df = tabla.select(columnas).to_pandas()
        if 'ano' in df.columns:
            self.anos.append((df['ano'].min(), df['ano'].max()))
        ultimas = df[df['es_ultima_version']]
        for col, _ in IMPORTES_RESUMEN:
            if col in ultimas.columns:
                self.sumas[col] = self.sumas.get(col, 0.0) + ultimas[col].sum()
        if 'conjunto' not in df.columns:
            return
        for conjunto, grupo in df.groupby('conjunto', sort=False, dropna=False):
            c = self.conjuntos.setdefault(None if pd.isna(conjunto) else conjunto, [0, 0, [], {}])
            ult = grupo[grupo['es_ultima_version']]
            c[0] += len(grupo)
            c[1] += len(ult)
            if 'ano' in ult.columns and len(ult):
                c[2].append((ult['ano'].min(), ult['ano'].max()))
            for col, _ in IMPORTES_RESUMEN:
                if col in ult.columns:
                    c[3][col] = c[3].get(col, 0.0) + ult[col].sum()

def _rango(pares):
    """(mínimo, máximo) de varios (mín, máx) sin contar los nulos (NaN si no hay ninguno)."""
    minimos = [a for a, _ in pares if pd.notna(a)]
    maximos = [b for _, b in pares if pd.notna(b)]
    return (min(minimos) if minimos else np.nan), (max(maximos) if maximos else np.nan)

def _borrar_partes(tablas):
    """Borra las partes de una tabla ya escrita (así el disco no necesita las
    partes y todas las tablas finales a la vez)."""
    for tabla in tablas:
        Path(tabla['ruta']).unlink(missing_ok=True)

def _proceso_vivo(pid):
    """Si el proceso `pid` de esta máquina sigue vivo (None si no se puede saber)."""
    if os.name != 'posix':
        return None   # os.kill(pid, 0) en Windows terminaría el proceso
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True

class ExportacionPlacsp:
    """Exportación por lotes de las tablas PLACSP (ver arriba):

        with ExportacionPlacsp(nombre_base, dir_salida) as exportacion:
            exportacion.anadir(licitaciones, borrados)     # un lote, en orden de lectura
            ...
            exportacion.cerrar(semillas)

    Una ejecución interrumpida no deja tablas finales a medias ni mezcla sus
    partes con las de otra: cada ejecución tiene su carpeta de partes, las
    tablas finales se escriben dentro y solo cuando están todas completas se
    mueven a su sitio. El parquet anterior de cada tabla no se pierde: pasa a
    _historico/ (comun.historico.guardar_version; si no cambió no se toca).
    El CSV anterior se sustituye (tiene los mismos datos que su parquet). Las
    partes que dejó una ejecución que murió sin limpiar se borran al empezar
    la siguiente (si su proceso ya no existe; las de otra máquina o de un
    proceso vivo solo se avisan, con su tamaño).

    ambito: {(conjunto, año del ZIP)} de las copias actuales de ZIP de las que
    se ha leído alguna entrada (lo anota procesar_copias); las semillas solo
    aportan filas de ese ámbito (ver _ambito_semilla). None (exportar_datos,
    anadir): sin restricción.
    """

    def __init__(self, nombre_base='licitaciones_completo', dir_salida=None, csv=True,
                 lote=TAM_LOTE, filas_grupo=FILAS_GRUPO):
        if lote is not None and lote < 0:
            raise ValueError(f'lote debe ser positivo: {lote}')
        self.nombre_base = nombre_base
        self.dir_salida = Path(dir_salida if dir_salida is not None else OUTPUT_DIR)
        self.csv, self.lote, self.filas_grupo = csv, lote or TAM_LOTE, filas_grupo
        self.dir_salida.mkdir(parents=True, exist_ok=True)
        self._limpiar_huerfanas()
        self.dir_partes = Path(tempfile.mkdtemp(prefix=f'.{nombre_base}.partes-', dir=self.dir_salida))
        (self.dir_partes / 'pid').write_text(f'{socket.gethostname()} {os.getpid()}')
        self.partes = []
        self.informes_semilla = []
        self.ambito = None
        # Índice de cada parte: único en toda la exportación aunque se llame
        # varias veces a procesar_copias o anadir (una parte nunca pisa otra)
        self._indices = itertools.count()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.limpiar()
        return False

    def limpiar(self):
        """Borra la carpeta de partes (y las tablas finales a medio publicar)."""
        shutil.rmtree(self.dir_partes, ignore_errors=True)

    def _limpiar_huerfanas(self):
        """Partes de otras ejecuciones en la carpeta de salida (de esta tabla o de
        otra, p.ej. con otro --anos): se borran si su proceso ya no existe; si
        no se puede saber (otra máquina, o un contenedor que cambió de nombre),
        se avisa con su tamaño."""
        for carpeta in sorted(self.dir_salida.glob('.*.partes-*')):
            if not carpeta.is_dir():
                continue
            try:
                maquina, pid = (carpeta / 'pid').read_text().split()
                vivo = _proceso_vivo(int(pid)) if maquina == socket.gethostname() else None
            except (OSError, ValueError):
                # Sin pid: a medio crear por una ejecución que murió (o que empieza ahora)
                try:
                    vivo = None if time.time() - carpeta.stat().st_mtime < 60 else False
                except OSError:
                    continue
            if vivo is False:
                shutil.rmtree(carpeta, ignore_errors=True)
                print(f"   🧹 Borradas las partes de una ejecución interrumpida: {carpeta.name}")
            else:
                tamano = sum(f.stat().st_size for f in carpeta.rglob('*') if f.is_file()) / 1024 / 1024
                print(f"   ⚠ {carpeta.name}: partes de otra ejecución (¿en curso?); no se tocan "
                      f"({tamano:,.1f} MB; si no hay ninguna en curso se pueden borrar)")

    # --- Lotes -------------------------------------------------------------

    def siguiente_indice(self):
        """Índice para el nombre de una parte nueva (único en la exportación)."""
        return next(self._indices)

    def anadir(self, licitaciones, borrados=None):
        """Escribe un lote (en orden de lectura) como una parte."""
        self.anadir_parte(escribir_parte(self.dir_partes, f'{self.siguiente_indice():06d}',
                                         licitaciones, borrados))

    def anadir_ambito(self, conjunto, ano):
        """Anota que se ha leído alguna entrada de la copia actual de un ZIP del
        conjunto y año (ámbito de las semillas; ver la clase)."""
        if self.ambito is None:
            self.ambito = set()
        self.ambito.add((conjunto, ano))

    def anadir_parte(self, parte):
        """Añade una parte ya escrita (p.ej. por otro proceso, ver procesar_copias)."""
        self.partes.append(parte)

    def _principales(self, semilla=None):
        return [p['tablas']['principal'] for p in self.partes
                if 'principal' in p['tablas'] and (semilla is None or p['semilla'] == semilla)]

    @staticmethod
    def _tipos(tablas, columnas, extra=None, descarga=None):
        """Tipo unificado de cada columna de varias partes (tipo_unificado) y las
        columnas booleanas con algún nulo. Con `descarga` (sus partes), los
        nulos que solo traen las filas de una semilla no pasan a double una
        columna entera de la descarga: sigue siendo entera, con nulos."""
        def hay_nulos(partes):
            presentes = [t for t in partes if col in t['tipos']]
            return (len(presentes) < len(partes)
                    or any(t['nulos'][col] or pa.types.is_null(t['tipos'][col]) for t in presentes))

        tipos, bool_con_nulos = {}, []
        for col in columnas:
            if extra and col in extra:
                tipos[col] = extra[col]
                continue
            referencia = tablas
            if descarga and any(col in t['tipos'] for t in descarga):
                referencia = descarga
            tipos[col] = tipo_unificado([t['tipos'][col] for t in tablas if col in t['tipos']],
                                        hay_nulos(referencia))
            if pa.types.is_boolean(tipos[col]) and hay_nulos(tablas):
                bool_con_nulos.append(col)
        return tipos, bool_con_nulos

    def _columnas_principal(self):
        """Orden de las columnas de la tabla principal: las de las entradas (en
        orden de primera aparición), ano, las marcas de versión, las nuevas al
        final (COLUMNAS_NUEVAS), la procedencia y las que solo trae una semilla."""
        base = (_union_ordenada(t['columnas'] for t in self._principales(semilla=False))
                or _union_ordenada(t['columnas'] for t in self._principales(semilla=True)))
        orden = base + [c for c in ['ano'] + MARCAS_VERSION + ['textos_originales'] if c not in base]
        orden = [c for c in _al_final(orden, COLUMNAS_NUEVAS) if c != '_n']
        orden += [c for c in COLUMNAS_PROCEDENCIA if c not in orden]
        extras = _union_ordenada(t['columnas'] for t in self._principales(semilla=True))
        return orden + [c for c in extras if c not in orden and c != '_n']

    def _esquema_principal(self):
        tablas = self._principales()
        columnas = self._columnas_principal()
        tipos, bool_con_nulos = self._tipos(tablas, columnas, extra={
            'n_versiones': pa.int64(), 'es_ultima_version': pa.bool_(),
            'entrada_repetida': pa.bool_(), '_en_ultima_descarga': pa.bool_()},
            descarga=self._principales(semilla=False))
        tipos['_origen'] = tipo_unificado([_tipo_texto()] + [t['tipos']['_origen'] for t in tablas
                                                             if '_origen' in t['tipos']])
        return _esquema_final([(c, tipos[c]) for c in columnas], bool_con_nulos)

    # --- Semillas ----------------------------------------------------------

    def _filas(self, tablas, filas, columnas):
        """Columnas (DataFrame) de las filas `filas` (posiciones ordenadas en la
        unión de las partes `tablas`)."""
        desde = np.cumsum([0] + [t['filas'] for t in tablas])
        trozos = []
        for t, inicio, fin in zip(tablas, desde[:-1], desde[1:]):
            i, j = np.searchsorted(filas, [inicio, fin])
            if i == j:
                continue
            leer = [c for c in columnas if c in t['tipos']]
            df = (pq.read_table(t['ruta'], columns=leer).take(filas[i:j] - inicio).to_pandas()
                  if leer else pd.DataFrame(index=range(j - i)))
            trozos.append(df.reindex(columns=columnas).astype(object))   # se comparan como texto
        return pd.concat(trozos, ignore_index=True) if trozos else pd.DataFrame(columns=columnas)

    @staticmethod
    def _filas_semilla(pf, filas, columnas):
        """Columnas de contenido de las filas `filas` de una semilla, con la semántica actual."""
        nombres = pf.schema_arrow.names
        fuente = [c for c in nombres if c in columnas]
        if ('valor_estimado_contrato' in columnas and 'valor_estimado_contrato' not in nombres
                and 'importe_sin_iva' in nombres):
            fuente.append('importe_sin_iva')   # esquema antiguo: se renombra al normalizar
        desde = np.cumsum([0] + [pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups)])
        trozos = []
        for grupo in range(pf.num_row_groups):
            i, j = np.searchsorted(filas, [desde[grupo], desde[grupo + 1]])
            if i < j:
                tabla = pf.read_row_group(grupo, columns=fuente).take(filas[i:j] - desde[grupo])
                trozos.append(tabla.replace_schema_metadata(pf.schema_arrow.metadata).to_pandas())
        df = _normalizar_columnas(pd.concat(trozos, ignore_index=True)) if trozos else pd.DataFrame()
        return df.reindex(columns=columnas)

    def _ambito_semilla(self, pf, contar=None):
        """Filas de la semilla `pf` dentro del ámbito de la ejecución (ver la
        clase): las de un conjunto y año de ZIP (el de archivo_origen) que esta
        ejecución ha vuelto a leer; sin archivo_origen o sin conjunto, solo si
        ha leído todos los periodos de los que pueden venir (periodos_posibles).
        Fuera de él no se sabe si siguen publicadas (años sin ZIP en disco,
        otros conjuntos u otro --anos): no se añaden. Devuelve la máscara (None:
        sin ámbito, todas) y {conjunto año: filas} de las de fuera (de las
        filas `contar`, si se indican)."""
        if self.ambito is None:
            return None, {}
        n = pf.metadata.num_rows
        codigos, valores = [], []
        for col in ('conjunto', 'archivo_origen'):
            if col in pf.schema_arrow.names:
                k, v = _factorizar(pf.read(columns=[col]).column(0))
            else:
                k, v = np.full(n, -1, dtype=np.int64), []
            codigos.append(k)
            valores.append(v)
        combos, inversa, cuentas = np.unique(np.column_stack(codigos), axis=0, return_inverse=True,
                                             return_counts=True)
        inversa = np.asarray(inversa).reshape(-1)
        if contar is not None:
            cuentas = np.bincount(inversa[np.asarray(contar, dtype=bool)], minlength=len(combos))
        dentro, fuera = np.zeros(len(combos), dtype=bool), {}
        for i, (kc, ka) in enumerate(combos):
            conjunto = None if kc < 0 or valores[0][kc] is None else str(valores[0][kc])
            ano = None if ka < 0 or valores[1][ka] is None else ano_de_zip(str(valores[1][ka]))
            posibles = periodos_posibles(conjunto, ano)
            dentro[i] = bool(posibles) and posibles <= self.ambito
            if not dentro[i] and cuentas[i]:
                etiqueta = f"{conjunto or '(sin conjunto)'} {ano if ano is not None else '(sin año de ZIP)'}"
                fuera[etiqueta] = fuera.get(etiqueta, 0) + int(cuentas[i])
        return dentro[inversa], dict(sorted(fuera.items()))

    def _sembrar(self, ruta, origen, contenido=None, fase=None, indice=0):
        """Incorpora el parquet publicado `ruta` como la instantánea más antigua
        (regla de comun.historico.sembrar, por lotes): añade como partes de
        semilla sus filas del ámbito de la ejecución (_ambito_semilla) cuya
        clave (id, fecha_updated) no está en la descarga ni en las semillas
        anteriores (ni su contenido, si a una de las dos le falta la fecha) y
        devuelve el informe. fase: 'completa' (solo sus filas con la clave
        completa), 'incompleta' (solo las demás) o None (todas; ver cerrar).
        Las filas sin fecha que se descartan porque su contenido ya está no se
        pierden: van a la tabla _semilla_contenido (sin fecha no se puede
        saber si son la misma versión o una anterior con el mismo contenido).
        Solo se comparan las columnas de contenido que tiene la semilla (una
        que falta no se toma como nula)."""
        hist = _historico()
        pf = pq.ParquetFile(ruta)
        nombres = pf.schema_arrow.names
        if 'id' not in nombres:
            raise ValueError(f'La semilla {ruta} no tiene columna id')
        contenido = [c for c in (CONTENIDO_SEMILLA if contenido is None else contenido)
                     if c in nombres or (c == 'valor_estimado_contrato' and 'importe_sin_iva' in nombres)]
        presentes = self._principales()
        tipos_descarga, _ = self._tipos(self._principales(semilla=False), self._columnas_principal())

        # Claves: id como código entero común (Arrow) y fecha_updated en ns UTC
        ids_p = [_leer_columna(t, 'id', pa.large_string()) for t in presentes]
        claves_s = pf.read(columns=[c for c in ('id', 'fecha_updated') if c in nombres])
        ids_s = claves_s.column('id')
        codigos = _codigos_id(ids_p + [ids_s])
        n_p = sum(t['filas'] for t in presentes)
        fecha_p = pa.chunked_array([c for t in presentes
                                    for c in _leer_columna(t, 'fecha_updated', pa.timestamp('ns', tz='UTC')).chunks],
                                   type=pa.timestamp('ns', tz='UTC'))
        fecha_s = (_instantes(claves_s.column('fecha_updated')) if 'fecha_updated' in nombres
                   else pa.chunked_array([pa.nulls(len(ids_s), pa.timestamp('ns', tz='UTC'))]))
        claves_nuevos = pd.DataFrame({'id': _enteros(codigos[:n_p]),
                                      'fecha_updated': fecha_p.to_pandas().reset_index(drop=True)})
        claves_semilla = pd.DataFrame({'id': _enteros(codigos[n_p:]),
                                       'fecha_updated': fecha_s.to_pandas().reset_index(drop=True)})
        del codigos, ids_p, fecha_p
        completa = (claves_semilla['id'].notna() & claves_semilla['fecha_updated'].notna()).to_numpy()
        en_fase = (np.ones(len(completa), dtype=bool) if fase is None
                   else completa if fase == 'completa' else ~completa)
        en_ambito, fuera = self._ambito_semilla(pf, en_fase)
        motivo = hist.seleccionar_semilla(
            claves_nuevos, claves_semilla,
            lambda filas: self._filas(presentes, filas, contenido),
            lambda filas: self._filas_semilla(pf, filas, contenido),
            en_fase if en_ambito is None else en_ambito & en_fase)
        del claves_nuevos, claves_semilla, en_ambito
        filas_fase = np.flatnonzero(en_fase)   # las de la otra fase no cuentan en el informe
        informe = hist.informe_semilla(motivo[filas_fase], origen,
                                       lambda filas, t=claves_s: t.take(filas_fase[filas]).to_pandas())
        informe['ruta'] = str(ruta)
        informe['fuera_ambito_detalle'] = fuera
        del claves_s, ids_s, fecha_s

        # Filas añadidas (principal) y descartadas por contenido
        # (semilla_contenido): partes de semilla, en el orden del fichero
        destinos = [('principal', np.flatnonzero((motivo == hist.ANADIDA) & en_fase)),
                    ('semilla_contenido', np.flatnonzero((motivo == hist.PRESENTE_CONTENIDO) & en_fase))]
        leidas = 0
        for lote in pf.iter_batches(batch_size=self.lote):
            for tabla_destino, filas in destinos:
                i, j = np.searchsorted(filas, [leidas, leidas + lote.num_rows])
                if i < j:
                    tabla = pa.Table.from_batches([lote]).take(filas[i:j] - leidas)
                    df = _preparar_semilla(tabla.replace_schema_metadata(pf.schema_arrow.metadata).to_pandas(),
                                           tipos_descarga, origen)
                    prefijo = f'semilla{indice:02d}-{self.siguiente_indice():06d}'
                    self.partes.append({'prefijo': prefijo, 'filas': len(df), 'semilla': True, 'tablas': {
                        tabla_destino: _tabla_parte(df, self.dir_partes / f'{prefijo}.{tabla_destino}.parquet',
                                                    list(df.columns), tipos_descarga)}})
            leidas += lote.num_rows
        return informe

    # --- Cierre ------------------------------------------------------------

    def _nombre(self, tabla):
        return self.nombre_base if tabla == 'principal' else f'{self.nombre_base}_{tabla}'

    def cerrar(self, semillas=(), origen_semilla=None, contenido_semilla=None):
        """Incorpora las semillas, calcula las marcas sobre todas las filas, escribe
        las tablas finales y las publica (ver la clase). Devuelve un resumen:
        {'filas', 'rutas': {tabla: parquet}, 'semillas': [informes]}.

        Sin ninguna entrada leída de la descarga (p.ej. --data-dir equivocado)
        no se incorporan las semillas: no se escribe la tabla principal y la
        salida anterior no se toca (una salida con solo la semilla parecería
        completa y retiraría a _historico/ la anterior y su detalle)."""
        hist = _historico()
        origen = origen_semilla or hist.ORIGEN_SEMILLA
        print(f"\n💾 EXPORTANDO DATOS")
        print("=" * 60)
        if semillas and not sum(t['filas'] for t in self._principales(semilla=False)):
            print(f"   ⚠ Ninguna entrada leída de la descarga: no se incorporan las semillas "
                  f"({len(semillas)}) y la salida anterior no se toca")
            semillas = ()
        # Primero las filas con la clave completa de todas las semillas (en su
        # orden de prioridad) y después las que no la tienen: una entrada
        # retirada que un publicado trae sin fecha y otro con ella sale una
        # sola vez y con su fecha, con cualquier orden de --semilla
        rutas = [Path(ruta) for ruta in semillas or ()]
        informes = [None] * len(rutas)
        for fase in ('completa', 'incompleta'):
            for k, ruta in enumerate(rutas):
                informes[k] = _sumar_informes(informes[k], self._sembrar(ruta, origen, contenido_semilla, fase, k))
        for informe in informes:
            hist.imprimir_informe_semilla(informe)
        self.informes_semilla.extend(informes)

        escritas, resumen = {}, {'filas': 0, 'rutas': {}, 'semillas': self.informes_semilla}
        principales = self._principales()
        if not sum(t['filas'] for t in principales):
            # Ni la tabla principal ni las demás (tampoco _borrados): con
            # tablas de ejecuciones distintas no se podrían cruzar
            print("\n⚠️ No se encontraron registros: no se escribe ninguna tabla y la salida anterior no se toca")
            return resumen
        fechas, ultima, repetida = self._escribir_principal(principales, escritas, resumen)
        self._escribir_detalle(principales, fechas, ultima, repetida, escritas)
        self._escribir_borrados(escritas)
        self._escribir_semilla_contenido(escritas)
        self._publicar(escritas, resumen)
        self._imprimir_resumen(resumen)
        return resumen

    def _escribir_principal(self, principales, escritas, resumen):
        hist = _historico()
        esquema = self._esquema_principal()
        n = [t['filas'] for t in principales]
        desde = np.cumsum([0] + n)
        es_semilla = np.repeat([p['semilla'] for p in self.partes if 'principal' in p['tablas']], n)

        # Claves de todas las filas (descarga y semillas): marcas de versión
        ids = _codigos_id([_leer_columna(t, 'id', pa.large_string()) for t in principales])
        tipo_fecha = esquema.field('fecha_updated').type if 'fecha_updated' in esquema.names else None
        fechas = None
        if tipo_fecha is not None:
            fechas = pa.chunked_array([c for t in principales for c in _leer_columna(t, 'fecha_updated', tipo_fecha).chunks],
                                      type=tipo_fecha)
        serie_fechas = fechas.to_pandas().reset_index(drop=True) if fechas is not None else None
        ultima, n_versiones, repetida = marcas_version(pd.Series(ids, dtype='float64').where(ids >= 0),
                                                       serie_fechas)

        # _en_ultima_descarga: la entrada (id, fecha_updated) está en la copia
        # actual de algún ZIP; sin id o sin fecha, si la fila es de una copia actual
        actual = np.zeros(len(ids), dtype=bool)
        for t, inicio in zip(principales, desde[:-1]):
            if 'zip_historico' in t['tipos']:
                zh = pq.read_table(t['ruta'], columns=['zip_historico']).column(0)
                actual[inicio:inicio + t['filas']] = zh.is_null().to_numpy(zero_copy_only=False)
            else:
                actual[inicio:inicio + t['filas']] = True
        actual &= ~es_semilla
        vacia = pd.Series([], dtype='Int64')
        pares = [(_enteros(ids), vacia)]
        if serie_fechas is not None:
            pares.append((hist.instantes_ns(serie_fechas), vacia))
        clave, _ = hist.combinar_codigos(pares, len(ids))
        en_ultima = actual | ((clave >= 0) & np.isin(clave, clave[actual & (clave >= 0)]))
        en_ultima &= ~es_semilla
        del clave, pares, actual

        print(f"   ℹ {len(ids):,} entradas de {int(ultima.sum()):,} licitaciones distintas "
              f"(es_ultima_version marca la más reciente de cada una); "
              f"{int(repetida.sum()):,} repiten una entrada ya leída (entrada_repetida)")
        if es_semilla.any() or not en_ultima.all():
            print(f"   ℹ {int((~en_ultima).sum()):,} filas no están en la última descarga (_en_ultima_descarga=False): "
                  f"{int(es_semilla.sum()):,} de semillas y {int((~en_ultima & ~es_semilla).sum()):,} "
                  f"solo en versiones antiguas de ZIP (_historico/)")
        del ids

        rutas = self._rutas_temporales('principal')
        escritor = _EscritorTabla(esquema, rutas[0], rutas[1], self.filas_grupo)
        cuentas = _Resumen()
        try:
            for t, inicio in zip(principales, desde[:-1]):
                parte = pq.read_table(t['ruta'])
                fin = inicio + parte.num_rows
                calculadas = {'n_versiones': n_versiones[inicio:fin], 'es_ultima_version': ultima[inicio:fin],
                              'entrada_repetida': repetida[inicio:fin], '_en_ultima_descarga': en_ultima[inicio:fin]}
                columnas = []
                for campo in esquema:
                    if campo.name in calculadas:
                        columnas.append(pa.array(calculadas[campo.name], type=campo.type))
                    elif campo.name in parte.column_names:
                        columnas.append(_convertir(parte.column(campo.name), campo.type))
                    else:
                        columnas.append(pa.nulls(parte.num_rows, campo.type))
                tabla = pa.Table.from_arrays(columnas, schema=esquema)
                escritor.escribir(tabla)
                cuentas.anadir(tabla)
        finally:
            escritor.cerrar()
        escritas['principal'] = rutas
        _borrar_partes(principales)
        resumen.update(filas=escritor.filas, cuentas=cuentas, ultimas=int(ultima.sum()),
                       semilla=int(es_semilla.sum()), fuera=int((~en_ultima).sum()))
        return fechas, ultima, repetida

    def _escribir_detalle(self, principales, fechas, ultima, repetida, escritas):
        desde = dict(zip((id(t) for t in principales), np.cumsum([0] + [t['filas'] for t in principales])))
        for nombre in TABLAS_DETALLE:
            partes = [(p['tablas'][nombre], desde[id(p['tablas']['principal'])])
                      for p in self.partes if nombre in p['tablas']]
            if not partes:
                continue
            tablas = [t for t, _ in partes]
            base = _union_ordenada(t['columnas'] for t in tablas)
            columnas = base + [c for c in MARCAS_DETALLE if c not in base
                               and (c != 'fecha_updated' or fechas is not None)]
            if nombre == 'resultados':
                columnas = _al_final(columnas, COLUMNAS_NUEVAS_RESULTADOS)
            columnas = [c for c in columnas if c != '_n']
            extra = {'es_ultima_version': pa.bool_(), 'entrada_repetida': pa.bool_()}
            if fechas is not None:
                extra['fecha_updated'] = fechas.type
            tipos, bool_con_nulos = self._tipos(tablas, columnas, extra)
            esquema = _esquema_final([(c, tipos[c]) for c in columnas], bool_con_nulos)
            rutas = self._rutas_temporales(nombre)
            escritor = _EscritorTabla(esquema, rutas[0], rutas[1], self.filas_grupo)
            try:
                for t, inicio in partes:
                    parte = pq.read_table(t['ruta'])
                    idx = inicio + parte.column('_n').to_numpy()
                    calculadas = {'es_ultima_version': pa.array(ultima[idx]),
                                  'entrada_repetida': pa.array(repetida[idx])}
                    if fechas is not None:
                        calculadas['fecha_updated'] = fechas.take(pa.array(idx))
                    columnas_t = []
                    for campo in esquema:
                        if campo.name in calculadas:
                            columnas_t.append(_convertir(calculadas[campo.name], campo.type))
                        elif campo.name in parte.column_names:
                            columnas_t.append(_convertir(parte.column(campo.name), campo.type))
                        else:
                            columnas_t.append(pa.nulls(parte.num_rows, campo.type))
                    escritor.escribir(pa.Table.from_arrays(columnas_t, schema=esquema))
            finally:
                escritor.cerrar()
            escritas[nombre] = rutas
            _borrar_partes(tablas)

    def _escribir_borrados(self, escritas):
        tablas = [p['tablas']['borrados'] for p in self.partes if 'borrados' in p['tablas']]
        if not tablas:
            return
        base = _union_ordenada(t['columnas'] for t in tablas)
        columnas = base + [c for c in ['entrada_repetida', 'textos_originales'] if c not in base]
        tipos, bool_con_nulos = self._tipos(tablas, columnas, extra={'entrada_repetida': pa.bool_()})
        esquema = _esquema_final([(c, tipos[c]) for c in columnas], bool_con_nulos)
        claves = pd.DataFrame({
            'id': pa.chunked_array([c for t in tablas for c in _leer_columna(t, 'id', tipos['id']).chunks],
                                   type=tipos['id']).to_pandas().reset_index(drop=True),
            'fecha_borrado': pa.chunked_array(
                [c for t in tablas for c in _leer_columna(t, 'fecha_borrado', tipos['fecha_borrado']).chunks],
                type=tipos['fecha_borrado']).to_pandas().reset_index(drop=True)})
        repetida = (claves.duplicated(['id', 'fecha_borrado']).to_numpy()
                    & claves['id'].notna().to_numpy() & claves['fecha_borrado'].notna().to_numpy())
        del claves
        rutas = self._rutas_temporales('borrados')
        escritor = _EscritorTabla(esquema, rutas[0], rutas[1], self.filas_grupo)
        inicio = 0
        try:
            for t in tablas:
                parte = pq.read_table(t['ruta'])
                columnas_t = []
                for campo in esquema:
                    if campo.name == 'entrada_repetida':
                        columnas_t.append(pa.array(repetida[inicio:inicio + parte.num_rows]))
                    elif campo.name in parte.column_names:
                        columnas_t.append(_convertir(parte.column(campo.name), campo.type))
                    else:
                        columnas_t.append(pa.nulls(parte.num_rows, campo.type))
                escritor.escribir(pa.Table.from_arrays(columnas_t, schema=esquema))
                inicio += parte.num_rows
        finally:
            escritor.cerrar()
        escritas['borrados'] = rutas
        _borrar_partes(tablas)

    def _escribir_semilla_contenido(self, escritas):
        """Tabla _semilla_contenido: filas de las semillas sin fecha_updated que
        no se añaden porque alguna fila de la descarga con su id tiene el mismo
        contenido (CONTENIDO_SEMILLA). Sin fecha no se puede saber si son la
        misma versión o una anterior igual en esas columnas: se guardan aquí,
        con _origen, para no perder ninguna ni duplicar la principal."""
        tablas = [p['tablas']['semilla_contenido'] for p in self.partes if 'semilla_contenido' in p['tablas']]
        if not tablas:
            return
        columnas = _union_ordenada(t['columnas'] for t in tablas)
        tipos, bool_con_nulos = self._tipos(tablas, columnas)
        esquema = _esquema_final([(c, tipos[c]) for c in columnas], bool_con_nulos)
        rutas = self._rutas_temporales('semilla_contenido')
        escritor = _EscritorTabla(esquema, rutas[0], rutas[1], self.filas_grupo)
        try:
            for t in tablas:
                parte = pq.read_table(t['ruta'])
                escritor.escribir(pa.Table.from_arrays(
                    [_convertir(parte.column(campo.name), campo.type) if campo.name in parte.column_names
                     else pa.nulls(parte.num_rows, campo.type) for campo in esquema], schema=esquema))
        finally:
            escritor.cerrar()
        escritas['semilla_contenido'] = rutas
        _borrar_partes(tablas)

    def _rutas_temporales(self, tabla):
        parquet = self.dir_partes / f'final.{tabla}.parquet'
        return parquet, (parquet.with_suffix('.csv') if self.csv else None)

    def _publicar(self, escritas, resumen):
        """Mueve las tablas finales a su sitio cuando ya están todas escritas: el
        parquet con guardar_version (la versión anterior, si cambió, pasa a
        _historico/) y el CSV sustituyendo al anterior. Una tabla de una
        ejecución anterior que esta ya no produce se archiva en _historico/
        (solo si esta ha escrito la tabla principal)."""
        hist = _historico()
        for tabla in TABLAS_SALIDA:
            destino = self.dir_salida / f'{self._nombre(tabla)}.parquet'
            destino_csv = destino.with_suffix('.csv')
            if tabla in escritas:
                parquet, csv = escritas[tabla]
                if destino.exists():
                    antes = pq.ParquetFile(destino).metadata.num_rows
                    ahora = pq.ParquetFile(parquet).metadata.num_rows
                    if ahora < antes:
                        print(f"   ⚠ {destino.name}: {antes - ahora:,} filas menos que la versión anterior "
                              f"({antes:,} → {ahora:,}; la anterior queda en _historico/). ¿Faltan ZIP en disco?")
                estado = hist.guardar_version(destino, desde=parquet)
                if estado == 'nuevo' and destino_csv.exists():
                    # Un CSV sin su parquet (p.ej. de una versión antigua del
                    # script): se guarda en _historico/ antes de sustituirlo
                    archivado = hist.archivar(destino_csv)
                    print(f"   ℹ {destino_csv.name} anterior (sin parquet) → {archivado.parent.name}/{archivado.name}")
                print(f"   ✓ Parquet: {destino} ({destino.stat().st_size / 1024 / 1024:.1f} MB, "
                      f"{estado.replace('_', ' ')}"
                      + ('; la versión anterior queda en _historico/' if estado == 'actualizado' else '') + ")")
                if csv is not None:
                    os.replace(csv, destino_csv)
                    print(f"   ✓ CSV: {destino_csv} ({destino_csv.stat().st_size / 1024 / 1024:.1f} MB)")
                elif destino_csv.exists() and estado != 'sin_cambios':
                    destino_csv.unlink()   # ya no corresponde al parquet (sus datos, en _historico/)
                    print(f"   ℹ Borrado {destino_csv.name}: era de la versión anterior (--sin-csv)")
                resumen['rutas'][tabla] = destino
            elif destino.exists() and 'principal' in escritas:
                # (sin tabla principal no se toca nada: una ejecución sin datos,
                # p.ej. con --data-dir equivocado, no retira la salida anterior)
                archivado = hist.archivar(destino)
                if destino_csv.exists():
                    destino_csv.unlink()
                print(f"   ℹ {destino.name}: esta ejecución no tiene filas para esta tabla; "
                      f"la de la ejecución anterior pasa a {archivado.parent.name}/{archivado.name}")

    def _imprimir_resumen(self, resumen):
        cuentas = resumen['cuentas']
        principal = resumen['rutas']['principal']
        print(f"\n📊 RESUMEN POR CONJUNTO")
        print("=" * 60)
        for conjunto, (entradas, licitaciones, anos, sumas) in cuentas.conjuntos.items():
            minimo, maximo = _rango(anos)
            print(f"\n   {CONJUNTOS.get(conjunto, {}).get('nombre', conjunto)}:")
            print(f"      Entradas: {entradas:,} | Licitaciones: {licitaciones:,}")
            print(f"      Años: {minimo:.0f} - {maximo:.0f}")
            for col, etiqueta in IMPORTES_RESUMEN:
                if col in sumas:
                    print(f"      {etiqueta}: {sumas[col]/1e9:.2f}B €")

        print(f"\n📊 RESUMEN TOTAL")
        print("=" * 60)
        minimo, maximo = _rango(cuentas.anos)
        nombres = pq.ParquetFile(principal).schema_arrow.names
        print(f"   Entradas: {resumen['filas']:,} | Licitaciones distintas: {resumen['ultimas']:,}")
        print(f"   Rango fechas: {minimo:.0f} - {maximo:.0f}")
        for col, etiqueta in [('organo_contratante', 'Órganos únicos'), ('adjudicatario', 'Adjudicatarios únicos')]:
            if col in nombres:
                valores = pq.read_table(principal, columns=[col]).column(0)
                distintos = 0 if pa.types.is_null(valores.type) else pc.count_distinct(valores, mode='only_valid').as_py()
                print(f"   {etiqueta}: {distintos:,}")
        for col, etiqueta in IMPORTES_RESUMEN:
            if col in cuentas.sumas:
                print(f"   {etiqueta}: {cuentas.sumas[col]/1e9:.2f}B €")
        if resumen['semilla'] or resumen['fuera']:
            print(f"   Filas de semillas: {resumen['semilla']:,} | fuera de la última descarga: {resumen['fuera']:,}")
        for tabla, ruta in resumen['rutas'].items():
            if tabla == 'resultados':
                res = pq.read_table(ruta, columns=['es_ultima_version', 'importe_adjudicacion']).to_pandas()
                res = res[res['es_ultima_version'].fillna(False).astype(bool)]
                print(f"   Resultados (lotes) de la última versión: {len(res):,} — adjudicado sin IVA: "
                      f"{res['importe_adjudicacion'].sum()/1e9:.2f}B €")
            elif tabla != 'principal':
                print(f"   Tabla _{tabla}: {pq.ParquetFile(ruta).metadata.num_rows:,} filas")

def exportar_datos(licitaciones, nombre_base='licitaciones_completo', borrados=None, lote=None,
                   csv=True, semillas=(), origen_semilla=None, ambito=None):
    """Exporta todas las entradas del ATOM (sin deduplicar), sus tablas de detalle
    (resultados, adjudicatarios, lotes, criterios, modificaciones) y las
    entradas borradas, y devuelve la tabla principal.

    Envuelve la exportación por lotes (ExportacionPlacsp) con lotes de `lote`
    entradas (None: todas en uno): las tablas son las mismas con cualquier
    tamaño de lote. semillas: parquet publicados que se incorporan como la
    instantánea más antigua (ver --semilla); ambito: {(conjunto, año del ZIP)}
    que se ha vuelto a leer (None: sin restricción; ver ExportacionPlacsp).
    """
    with ExportacionPlacsp(nombre_base, OUTPUT_DIR, csv=csv, lote=lote) as exportacion:
        if ambito is not None:
            exportacion.ambito = set(ambito)
        paso = lote or max(len(licitaciones), len(borrados or ()), 1)
        for i in range(0, len(licitaciones), paso):
            exportacion.anadir(licitaciones[i:i + paso])
        for i in range(0, len(borrados or ()), paso):
            exportacion.anadir([], borrados[i:i + paso])
        resumen = exportacion.cerrar(semillas, origen_semilla)
    ruta = resumen['rutas'].get('principal')
    return pd.read_parquet(ruta) if ruta is not None else None

# ============================================================================
# MAIN
# ============================================================================

def _entero_positivo(texto):
    """Tipo de argparse: entero mayor que 0 (un --lote negativo no acabaría nunca)."""
    valor = int(texto)
    if valor < 1:
        raise argparse.ArgumentTypeError(f'debe ser un entero mayor que 0: {texto}')
    return valor

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
    parser.add_argument('--lote', type=_entero_positivo, default=TAM_LOTE,
                        help=f'Entradas por parte temporal: acota la memoria (por defecto {TAM_LOTE:,})')
    parser.add_argument('--procesos', type=_entero_positivo, default=1,
                        help='Copias de ZIP que se leen a la vez (la salida es la misma; por defecto 1)')
    parser.add_argument('--sin-csv', action='store_true',
                        help='No escribir los CSV (con todos los conjuntos pasan de 10 GB)')
    parser.add_argument('--semilla', type=Path, action='append', default=[],
                        help='Parquet publicado que se incorpora como la instantánea más antigua (p.ej. '
                             'licitaciones_espana.parquet de v2026.02): se añaden sus filas cuya clave '
                             '(id, fecha_updated) no está en la descarga, solo de los conjuntos y años '
                             'con algún ZIP leído en esta ejecución. Repetible; el orden es la prioridad')
    parser.add_argument('--origen-semilla', type=str, default=None,
                        help="_origen de las filas añadidas desde --semilla (por defecto 'release v2026.02')")

    args = parser.parse_args()

    if args.data_dir is not None:
        DATA_DIR = args.data_dir
    if args.output_dir is not None:
        OUTPUT_DIR = args.output_dir
    # Parsear años
    partes = args.anos.split('-')
    ano_inicio = int(partes[0])
    ano_fin = int(partes[1]) if len(partes) > 1 else ano_inicio
    nombre_base = f'licitaciones_completo_{ano_inicio}_{ano_fin}'

    salidas = {(OUTPUT_DIR / f"{nombre_base if tabla == 'principal' else f'{nombre_base}_{tabla}'}.parquet").resolve()
               for tabla in TABLAS_SALIDA}
    for semilla in args.semilla:
        if not semilla.is_file():
            parser.error(f'No existe la semilla {semilla}')
        if semilla.resolve() in salidas:
            parser.error(f'La semilla {semilla} es una tabla de salida de esta ejecución: su versión anterior '
                         'pasaría a _historico/ y la siguiente ejecución sembraría desde la salida. Usa otro '
                         '--output-dir (el publicado es la única copia histórica)')
        if '_en_ultima_descarga' in pq.ParquetFile(semilla).schema_arrow.names and not args.origen_semilla:
            parser.error(f'La semilla {semilla} es una salida de este script (tiene _en_ultima_descarga): '
                         "indica su procedencia con --origen-semilla (p.ej. 'release v2026.09'); si no, sus "
                         "filas de la descarga pasarían por filas de 'release v2026.02'")

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

    # Procesar: por lotes, en el orden de lectura (conjuntos como CONJUNTOS,
    # ZIP como seleccionar_zips y de cada uno la copia actual y las de _historico/)
    print(f"\n{'='*60}")
    print(f"⚙️ PROCESANDO ARCHIVOS")
    print(f"{'='*60}")

    informes, avisos, copias = [], [], []
    for conjunto_id in conjuntos:
        copias += [(conjunto_id, copia, origen)
                   for copia, origen in copias_conjunto(conjunto_id, ano_inicio, ano_fin, avisos)]
    with ExportacionPlacsp(nombre_base, OUTPUT_DIR, csv=not args.sin_csv, lote=args.lote) as exportacion:
        procesar_copias(copias, exportacion, informes, procesos=args.procesos)
        imprimir_informe_procesado(informes, avisos)
        resumen = exportacion.cerrar(args.semilla, args.origen_semilla)
    if not resumen['filas']:
        # Sin ninguna entrada casi siempre es un fallo (--data-dir equivocado,
        # descarga caída): la salida anterior no se ha tocado; termina con error
        sys.exit(1)
    print("\n✅ SCRAPING COMPLETADO")

if __name__ == '__main__':
    main()
