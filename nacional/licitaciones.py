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
    python nacional/licitaciones.py --anos 2020-2026
    python nacional/licitaciones.py --anos 2024-2026 --conjunto licitaciones
    python nacional/licitaciones.py --anos 2024-2026 --conjunto menores
    python nacional/licitaciones.py --anos 2012-2026 --solo-procesar --data-dir ./datos_placsp --output-dir ./nacional

Salida (una fila por licitación, versión más reciente según atom:updated):
    licitaciones_completo_{inicio}_{fin}.parquet/.csv
    licitaciones_completo_{inicio}_{fin}_resultados.parquet/.csv  (una fila por cac:TenderResult / lote)

Importes (cac:ProcurementProject/cac:BudgetAmount):
    valor_estimado_contrato = EstimatedOverallContractAmount (valor estimado, incluye prórrogas/modificaciones)
    importe_sin_iva         = TaxExclusiveAmount (presupuesto base de licitación sin impuestos)
    importe_con_iva         = TotalAmount (presupuesto base de licitación con impuestos)
"""

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

CONJUNTOS = {
    'licitaciones': {
        'nombre': 'Licitaciones (sin menores)',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/',
        'patron_archivo': 'licitacionesPerfilesContratanteCompleto3_{periodo}.zip',
        'ano_inicio': 2012,
        'mensual_desde': 2025,  # 2025+ tiene archivos mensuales
    },
    'agregacion': {
        'nombre': 'Licitaciones por agregación (sin menores)',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/',
        'patron_archivo': 'PlataformasAgregadasSinMenores_{periodo}.zip',
        'ano_inicio': 2016,
        'mensual_desde': 2025,  # 2025+ tiene archivos mensuales
    },
    'menores': {
        'nombre': 'Contratos menores',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/',
        'patron_archivo': 'contratosMenoresPerfilesContratantes_{periodo}.zip',
        'ano_inicio': 2018,
        'mensual_desde': 2025,  # 2025+ tiene archivos mensuales
    },
    'encargos': {
        'nombre': 'Encargos a medios propios',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1383/',
        'patron_archivo': 'EMP_SectorPublico_{periodo}.zip',
        'ano_inicio': 2022,  # Incluye datos desde julio 2021
        'mensual_desde': None,  # Solo archivos anuales
    },
    'consultas': {
        'nombre': 'Consultas preliminares de mercado',
        'url_base': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1403/',
        'patron_archivo': 'CPM_SectorPublico_{periodo}.zip',
        'ano_inicio': 2022,
        'mensual_desde': None,  # Solo archivos anuales
    },
}

# Directorios - CAMBIAR AQUÍ LA RUTA SI ES NECESARIO (o usar --data-dir / --output-dir)
DATA_DIR = Path('D:/licitaciones_data')
OUTPUT_DIR = Path('D:/licitaciones_output')

# Namespaces XML
NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'cbc': 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
    'cac': 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    'cbc-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
    'cac-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
}

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

# ============================================================================
# GENERACIÓN DE URLs
# ============================================================================

def generar_urls_conjunto(conjunto_id, ano_inicio, ano_fin):
    """Genera URLs para un conjunto de datos específico."""
    config = CONJUNTOS[conjunto_id]
    archivos = []
    
    ano_actual = datetime.now().year
    mes_actual = datetime.now().month
    
    # Ajustar año inicio según disponibilidad del conjunto
    ano_inicio = max(ano_inicio, config['ano_inicio'])
    
    for ano in range(ano_inicio, ano_fin + 1):
        # Determinar si usar archivos mensuales o anuales
        usar_mensual = (
            config['mensual_desde'] is not None and 
            ano >= config['mensual_desde']
        )
        
        if usar_mensual:
            # Archivos mensuales
            max_mes = mes_actual if ano == ano_actual else 12
            for mes in range(1, max_mes + 1):
                periodo = f"{ano}{mes:02d}"
                nombre = config['patron_archivo'].format(periodo=periodo)
                url = config['url_base'] + nombre
                archivos.append({
                    'nombre': nombre,
                    'url': url,
                    'ano': ano,
                    'mes': mes,
                })
        else:
            # Archivo anual
            periodo = str(ano)
            nombre = config['patron_archivo'].format(periodo=periodo)
            url = config['url_base'] + nombre
            archivos.append({
                'nombre': nombre,
                'url': url,
                'ano': ano,
                'mes': None,
            })
    
    return archivos

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


def descargar_archivo(session, url, filepath, max_reintentos=3, forzar=False):
    """Descarga un archivo."""
    # Skip si existe, es un ZIP íntegro y no hay que refrescarlo
    if not forzar and filepath.exists() and filepath.stat().st_size > 1000:
        if zipfile.is_zipfile(filepath):
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"   ⏭ Ya existe ({size_mb:.1f} MB)")
            return filepath
        print(f"   ⚠ ZIP local incompleto/corrupto, se vuelve a descargar", end='')
    
    # Se descarga a un .part y se renombra al terminar: un corte a mitad no deja
    # un ZIP truncado que la siguiente ejecución daría por bueno.
    tmp_path = filepath.with_name(filepath.name + '.part')
    for intento in range(max_reintentos):
        try:
            response = session.get(url, timeout=600, stream=True)
            response.raise_for_status()

            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
            tmp_path.replace(filepath)

            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"   ✓ ({size_mb:.1f} MB)")
            return filepath

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"   ⚠ No disponible (404)")
                return None
            status = e.response.status_code if e.response is not None else '?'
            print(f"   ✗ Error HTTP {status} (intento {intento + 1}/{max_reintentos})")
            time.sleep(2 ** intento)
        except Exception as e:
            print(f"   ✗ Intento {intento + 1}/{max_reintentos}: {e}")
            time.sleep(2 ** intento)

    if tmp_path.exists():
        tmp_path.unlink()
    return None

def descargar_conjunto(session, conjunto_id, ano_inicio, ano_fin):
    """Descarga todos los archivos de un conjunto."""
    config = CONJUNTOS[conjunto_id]
    archivos = generar_urls_conjunto(conjunto_id, ano_inicio, ano_fin)
    
    print(f"\n{'='*60}")
    print(f"📦 {config['nombre'].upper()}")
    print(f"   URL base: {config['url_base']}")
    print(f"   Archivos: {len(archivos)}")
    print(f"{'='*60}")
    
    descargados = []
    for i, archivo in enumerate(archivos, 1):
        print(f"[{i}/{len(archivos)}] {archivo['nombre']}", end='')
        
        filepath = DATA_DIR / conjunto_id / archivo['nombre']
        forzar = es_periodo_reciente(archivo['ano'], archivo['mes'])
        resultado = descargar_archivo(session, archivo['url'], filepath, forzar=forzar)
        
        if resultado:
            descargados.append({
                **archivo,
                'filepath': resultado,
                'conjunto': conjunto_id,
            })
        
        time.sleep(0.3)
    
    print(f"\n✓ {len(descargados)}/{len(archivos)} archivos descargados")
    return descargados

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

def parsear_resultado(result):
    """Parsea un cac:TenderResult (hay uno por lote adjudicado/desierto)."""
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

def parsear_entry(entry):
    """Parsea una entrada de licitación."""
    try:
        # ID y URL
        id_lic = safe_text(entry, 'atom:id')
        link = entry.find('atom:link', NS)
        url = link.get('href') if link is not None else None
        
        # Container principal
        status = entry.find('cac-place-ext:ContractFolderStatus', NS)
        if status is None:
            return None
        
        # Expediente y estado
        expediente = safe_text(status, 'cbc:ContractFolderID')
        estado_code = safe_text(status, 'cbc-place-ext:ContractFolderStatusCode')
        
        # Órgano contratante
        # (siempre "is not None": un Element sin hijos evalúa a False)
        located_party = status.find('cac-place-ext:LocatedContractingParty', NS)
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
        project = status.find('cac:ProcurementProject', NS)
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
        process = status.find('cac:TenderingProcess', NS)
        procedimiento_code = safe_text(process, 'cbc:ProcedureCode')
        urgencia = safe_text(process, 'cbc:UrgencyCode')
        
        # Fecha límite
        fecha_limite = safe_text(process, './/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate')
        hora_limite = safe_text(process, './/cac:TenderSubmissionDeadlinePeriod/cbc:EndTime')
        
        # Términos
        terms = status.find('cac:TenderingTerms', NS)
        financiacion_ue = safe_text(terms, 'cbc:FundingProgramCode')
        
        # Resultado/Adjudicación: hay un cac:TenderResult por lote. Las columnas
        # principales reflejan el primero (como hasta ahora); el detalle de todos
        # los lotes se exporta en la tabla de resultados.
        resultados = [parsear_resultado(r) for r in status.findall('cac:TenderResult', NS)]
        primero = resultados[0] if resultados else {}
        n_lotes = len(status.findall('cac:ProcurementProjectLot', NS))

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
            '_resultados': resultados,
        }

    except Exception:
        return None

def procesar_archivo_atom(filepath):
    """Procesa un archivo ATOM."""
    licitaciones = []
    
    try:
        context = ET.iterparse(str(filepath), events=('end',))
        
        for event, elem in context:
            if elem.tag == '{http://www.w3.org/2005/Atom}entry':
                lic = parsear_entry(elem)
                if lic:
                    licitaciones.append(lic)
                elem.clear()
    
    except Exception as e:
        print(f"\n   ⚠ Error leyendo {Path(filepath).name}: {e}", end=' ')
        parciales, licitaciones = licitaciones, []
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            for entry in root.findall('atom:entry', NS):
                lic = parsear_entry(entry)
                if lic:
                    licitaciones.append(lic)
        except Exception:
            # Se conserva lo leído antes del error
            licitaciones = parciales

    return licitaciones

def procesar_zip(zip_path, conjunto_id):
    """Procesa un archivo ZIP."""
    licitaciones = []
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            print(f"   📦 Extrayendo...", end=' ', flush=True)
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(temp_path)
            
            # Buscar archivos .atom (orden estable; la versión que se conserva
            # de cada licitación se decide luego por atom:updated)
            atom_files = sorted(temp_path.rglob('*.atom'))
            print(f"✓ {len(atom_files)} ATOM", end=' ', flush=True)

            for atom_file in atom_files:
                lics = procesar_archivo_atom(atom_file)
                for lic in lics:
                    lic['conjunto'] = conjunto_id
                    lic['archivo_origen'] = Path(zip_path).name
                    # Mismo esquema que los parquet publicados (CPM = consulta preliminar)
                    lic['tipo_registro'] = 'CPM' if conjunto_id == 'consultas' else 'LICITACION'
                licitaciones.extend(lics)
            
            print(f"→ {len(licitaciones):,} registros")
    
    except zipfile.BadZipFile:
        print(f"   ✗ ZIP corrupto")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    return licitaciones

# ============================================================================
# EXPORTACIÓN
# ============================================================================

# Columnas de importes con su significado (para los resúmenes)
IMPORTES_RESUMEN = [
    ('valor_estimado_contrato', 'Valor estimado'),
    ('importe_sin_iva', 'Presupuesto base sin IVA'),
    ('importe_adjudicacion', 'Adjudicado sin IVA (1er lote)'),
]

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

    Empates y fechas nulas se resuelven por orden de lectura (gana la última);
    las filas sin id se conservan todas.
    """
    orden = np.arange(len(ids))
    claves = pd.DataFrame({'id': pd.Series(ids).reset_index(drop=True), '_orden': orden})
    columnas_orden = ['_orden']
    if fechas_updated is not None:
        claves['fecha_updated'] = parsear_fecha_updated(pd.Series(fechas_updated).reset_index(drop=True))
        columnas_orden = ['fecha_updated', '_orden']
    con_id = claves['id'].notna().to_numpy()
    ultimas = (claves[con_id]
               .sort_values(columnas_orden, kind='mergesort', na_position='first')
               .drop_duplicates(subset=['id'], keep='last')['_orden']
               .to_numpy())
    return np.sort(np.concatenate([ultimas, orden[~con_id]]))

def deduplicar_versiones(df):
    """Una fila por licitación: conserva la versión con atom:updated más reciente.

    Los ATOM de la PLACSP incluyen una entrada por cada actualización de la
    licitación (anuncio, adjudicación, formalización...), así que la misma
    licitación aparece varias veces y en varios ZIP. El orden de lectura no es
    cronológico, por lo que keep='last' sin ordenar puede quedarse con una versión
    antigua. Las filas sin 'id' se conservan tal cual.
    """
    if df.empty or 'id' not in df.columns:
        return df
    fecha = parsear_fecha_updated(df['fecha_updated']) if 'fecha_updated' in df.columns else None
    conservar = indices_ultima_version(df['id'], fecha)
    resultado = df.iloc[conservar].reset_index(drop=True)
    if fecha is not None:
        resultado['fecha_updated'] = fecha.iloc[conservar].reset_index(drop=True)
    return resultado

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

def normalizar_placsp(df, deduplicar=True):
    """Lleva un DataFrame PLACSP (salida de este script o parquet publicado) a la
    semántica actual de columnas.

    Los parquet publicados hasta el release v2026.02 tienen tres problemas que
    distorsionan cualquier suma o recuento:
      1. 'importe_sin_iva' contenía EstimatedOverallContractAmount (valor
         estimado), no TaxExclusiveAmount (issue #6). Si no existe la columna
         'valor_estimado_contrato' se renombra y 'importe_sin_iva' queda vacía:
         el presupuesto sin IVA real solo se recupera reprocesando los ATOM.
      2. Varias versiones de la misma licitación (licitaciones_espana.parquet:
         8,7M filas para 4,7M licitaciones). Con deduplicar=True se conserva la
         versión más reciente (atom:updated) de cada 'id'.
      3. Etiquetas de tipo_contrato/procedimiento erróneas y códigos guardados
         como float (CPV sin el cero inicial). Se recalculan desde los códigos.
    """
    esquema_antiguo = 'importe_sin_iva' in df.columns and 'valor_estimado_contrato' not in df.columns

    if deduplicar:
        df = deduplicar_versiones(df)
    else:
        df = df.copy(deep=False)  # no tocar el DataFrame del llamador

    if esquema_antiguo:
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

def leer_placsp(path, deduplicar=True):
    """Lee un parquet PLACSP y lo devuelve normalizado (ver normalizar_placsp).

    La deduplicación se hace en Arrow, row group a row group, antes de pasar a
    pandas: así también cabe en memoria licitaciones_espana.parquet (~9M filas).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    nombres = pf.schema_arrow.names
    if not deduplicar or 'id' not in nombres:
        return normalizar_placsp(pf.read().to_pandas(), deduplicar=deduplicar)

    claves = pq.read_table(path, columns=[c for c in ('id', 'fecha_updated') if c in nombres]).to_pandas()
    conservar = indices_ultima_version(claves['id'], claves.get('fecha_updated'))
    del claves
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
    return normalizar_placsp(df, deduplicar=False)

def separar_resultados(licitaciones):
    """Saca de cada licitación su lista de resultados por lote ('_resultados')."""
    resultados = []
    for n, lic in enumerate(licitaciones):
        lic['_n'] = n
        for res in lic.pop('_resultados', None) or []:
            resultados.append({
                '_n': n,
                'id': lic.get('id'),
                'expediente': lic.get('expediente'),
                'conjunto': lic.get('conjunto'),
                **res,
            })
    return resultados

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

def exportar_datos(licitaciones, nombre_base='licitaciones_completo'):
    """Exporta licitaciones (una fila por licitación) y sus resultados por lote."""
    print(f"\n💾 EXPORTANDO DATOS")
    print("=" * 60)
    
    resultados = separar_resultados(licitaciones)
    df = pd.DataFrame(licitaciones)
    
    # Convertir tipos
    for col in ['fecha_limite', 'fecha_adjudicacion', 'fecha_publicacion']:
        if col in df.columns:
            df[col] = parsear_fechas(df[col])
    
    if 'fecha_updated' in df.columns:
        df['fecha_updated'] = parsear_fecha_updated(df['fecha_updated'])
    
    # Extraer año
    df['ano'] = df['fecha_publicacion'].dt.year
    
    # Una fila por licitación (versión más reciente)
    n_antes = len(df)
    df = deduplicar_versiones(df)
    n_despues = len(df)
    if n_antes != n_despues:
        print(f"   ⚠ Descartadas {n_antes - n_despues:,} versiones anteriores/duplicadas "
              f"→ {n_despues:,} licitaciones únicas")
    
    # Resultados por lote de la versión conservada de cada licitación
    df_res = pd.DataFrame(resultados)
    if not df_res.empty:
        df_res = df_res[df_res['_n'].isin(df['_n'])].drop(columns='_n').reset_index(drop=True)
        df_res['fecha_adjudicacion'] = parsear_fechas(df_res['fecha_adjudicacion'])
    df = df.drop(columns='_n')
    
    guardar_tabla(df, nombre_base)
    if not df_res.empty:
        guardar_tabla(df_res, f'{nombre_base}_resultados')
    
    # Resumen por conjunto
    print(f"\n📊 RESUMEN POR CONJUNTO")
    print("=" * 60)
    if 'conjunto' in df.columns:
        for conjunto in df['conjunto'].unique():
            df_c = df[df['conjunto'] == conjunto]
            print(f"\n   {CONJUNTOS.get(conjunto, {}).get('nombre', conjunto)}:")
            print(f"      Licitaciones: {len(df_c):,}")
            print(f"      Años: {df_c['ano'].min():.0f} - {df_c['ano'].max():.0f}")
            for col, etiqueta in IMPORTES_RESUMEN:
                if col in df_c.columns:
                    print(f"      {etiqueta}: {df_c[col].sum()/1e9:.2f}B €")
    
    # Resumen total
    print(f"\n📊 RESUMEN TOTAL")
    print("=" * 60)
    print(f"   Total licitaciones: {len(df):,}")
    print(f"   Rango fechas: {df['ano'].min():.0f} - {df['ano'].max():.0f}")
    print(f"   Órganos únicos: {df['organo_contratante'].nunique():,}")
    print(f"   Adjudicatarios únicos: {df['adjudicatario'].nunique():,}")
    
    for col, etiqueta in IMPORTES_RESUMEN:
        if col in df.columns:
            print(f"   {etiqueta}: {df[col].sum()/1e9:.2f}B €")
    if not df_res.empty:
        print(f"   Resultados (lotes): {len(df_res):,} — adjudicado sin IVA: "
              f"{df_res['importe_adjudicacion'].sum()/1e9:.2f}B €")
    
    return df

# ============================================================================
# MAIN
# ============================================================================

def main():
    global DATA_DIR, OUTPUT_DIR
    parser = argparse.ArgumentParser(description='Scraper completo de licitaciones públicas')
    parser.add_argument('--anos', type=str, required=True, help='Rango de años (ej: 2020-2026)')
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
    
    archivos_descargados = []
    
    # Descargar
    if not args.solo_procesar:
        for conjunto_id in conjuntos:
            descargados = descargar_conjunto(session, conjunto_id, ano_inicio, ano_fin)
            archivos_descargados.extend(descargados)
    
    if args.solo_descargar:
        print("\n✅ Descarga completada")
        return
    
    # Procesar
    print(f"\n{'='*60}")
    print(f"⚙️ PROCESANDO ARCHIVOS")
    print(f"{'='*60}")
    
    todas_licitaciones = []
    
    for conjunto_id in conjuntos:
        conjunto_dir = DATA_DIR / conjunto_id
        zip_files = sorted(conjunto_dir.glob('*.zip'))
        
        # Filtrar por años
        zip_filtrados = []
        for z in zip_files:
            match = re.search(r'_(\d{4})(\d{2})?\.zip$', z.name)
            if match:
                ano = int(match.group(1))
                if ano_inicio <= ano <= ano_fin:
                    zip_filtrados.append(z)
        
        if zip_filtrados:
            print(f"\n📦 {CONJUNTOS[conjunto_id]['nombre']}: {len(zip_filtrados)} archivos")
            
            for i, zip_path in enumerate(zip_filtrados, 1):
                print(f"   [{i}/{len(zip_filtrados)}] {zip_path.name}", end='')
                lics = procesar_zip(zip_path, conjunto_id)
                todas_licitaciones.extend(lics)
    
    if todas_licitaciones:
        df = exportar_datos(todas_licitaciones, f'licitaciones_completo_{ano_inicio}_{ano_fin}')
        print("\n✅ SCRAPING COMPLETADO")
    else:
        print("\n⚠️ No se encontraron registros")

if __name__ == '__main__':
    main()