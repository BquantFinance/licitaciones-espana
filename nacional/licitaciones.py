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
    python scraper_completo.py --anos 2020-2026 --todos
    python scraper_completo.py --anos 2024-2026 --conjunto licitaciones
    python scraper_completo.py --anos 2024-2026 --conjunto menores
"""

import os
import re
import sys
import zipfile
import requests
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

# Directorios - CAMBIAR AQUÍ LA RUTA SI ES NECESARIO
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

# Mapeos
TIPOS_CONTRATO = {
    '1': 'Suministros', '2': 'Servicios', '3': 'Obras',
    '21': 'Gestión Servicios Públicos', '31': 'Concesión Obras',
    '40': 'Concesión Servicios', '7': 'Administrativo Especial',
    '8': 'Privado', '50': 'Patrimonial', '22': '22',
}

ESTADOS = {
    'PUB': 'Publicada', 'EV': 'En evaluación', 'ADJ': 'Adjudicada',
    'RES': 'Resuelta', 'ANUL': 'Anulada', 'DES': 'Desierta',
}

PROCEDIMIENTOS = {
    '1': 'Abierto', '2': 'Restringido', '3': 'Negociado con publicidad',
    '4': 'Negociado sin publicidad', '5': 'Diálogo competitivo',
    '6': 'Asociación innovación', '100': 'Basado en acuerdo marco',
    '999': 'Otros',
}

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def crear_directorios():
    """Crea estructura de directorios."""
    DATA_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
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

def descargar_archivo(session, url, filepath, max_reintentos=3):
    """Descarga un archivo."""
    # Skip si existe y tiene tamaño razonable
    if filepath.exists() and filepath.stat().st_size > 1000:
        size_mb = filepath.stat().st_size / 1024 / 1024
        print(f"   ⏭ Ya existe ({size_mb:.1f} MB)")
        return filepath
    
    for intento in range(max_reintentos):
        try:
            response = session.get(url, timeout=600, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
            
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"   ✓ ({size_mb:.1f} MB)")
            return filepath
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"   ⚠ No disponible (404)")
                return None
            print(f"   ✗ Error HTTP {e.response.status_code}")
        except Exception as e:
            print(f"   ✗ Intento {intento + 1}/{max_reintentos}: {e}")
            time.sleep(2 ** intento)
    
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
        resultado = descargar_archivo(session, archivo['url'], filepath)
        
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
    except:
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
    except:
        pass
    return None

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
        located_party = status.find('cac-place-ext:LocatedContractingParty', NS)
        party = located_party.find('cac:Party', NS) if located_party else None
        
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
        parent = located_party.find('cac-place-ext:ParentLocatedParty', NS) if located_party else None
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
        budget = project.find('cac:BudgetAmount', NS) if project else None
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
        if project:
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
        
        # Resultado/Adjudicación
        result = status.find('cac:TenderResult', NS)
        adjudicatario = None
        nif_adjudicatario = None
        importe_adjudicacion = None
        importe_adj_con_iva = None
        fecha_adjudicacion = None
        num_ofertas = None
        es_pyme = None
        
        if result is not None:
            adjudicatario = safe_text(result, './/cac:WinningParty/cac:PartyName/cbc:Name')
            
            winner_id = result.find('.//cac:WinningParty/cac:PartyIdentification/cbc:ID', NS)
            if winner_id is not None and winner_id.text:
                nif_adjudicatario = winner_id.text.strip()
            
            val = safe_text(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount')
            if val:
                try:
                    importe_adjudicacion = float(val)
                except:
                    pass
            
            val = safe_text(result, './/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:PayableAmount')
            if val:
                try:
                    importe_adj_con_iva = float(val)
                except:
                    pass
            
            fecha_adjudicacion = safe_text(result, 'cbc:AwardDate')
            
            val = safe_text(result, 'cbc:ReceivedTenderQuantity')
            if val:
                try:
                    num_ofertas = int(val)
                except:
                    pass
            
            pyme = safe_text(result, 'cbc:SMEAwardedIndicator')
            es_pyme = pyme == 'true' if pyme else None
        
        # Fechas
        fecha_updated = safe_text(entry, 'atom:updated')
        valid_notice = status.find('cac-place-ext:ValidNoticeInfo', NS)
        fecha_publicacion = safe_text(valid_notice, './/cac-place-ext:AdditionalPublicationDocumentReference/cbc:IssueDate')
        
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
            'importe_adjudicacion': importe_adjudicacion,
            'importe_adj_con_iva': importe_adj_con_iva,
            'adjudicatario': adjudicatario,
            'nif_adjudicatario': nif_adjudicatario,
            'num_ofertas': num_ofertas,
            'es_pyme': es_pyme,
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
            'fecha_adjudicacion': fecha_adjudicacion,
            'fecha_publicacion': fecha_publicacion,
            'fecha_updated': fecha_updated,
            'url': url,
        }
    
    except Exception as e:
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
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            for entry in root.findall('atom:entry', NS):
                lic = parsear_entry(entry)
                if lic:
                    licitaciones.append(lic)
        except Exception as e2:
            pass
    
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
            
            # Buscar archivos .atom
            atom_files = list(temp_path.rglob('*.atom'))
            print(f"✓ {len(atom_files)} ATOM", end=' ', flush=True)
            
            for atom_file in atom_files:
                lics = procesar_archivo_atom(atom_file)
                for lic in lics:
                    lic['conjunto'] = conjunto_id
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

def exportar_datos(licitaciones, nombre_base='licitaciones_completo'):
    """Exporta licitaciones a CSV y Parquet."""
    print(f"\n💾 EXPORTANDO DATOS")
    print("=" * 60)
    
    df = pd.DataFrame(licitaciones)
    
    # Convertir tipos
    for col in ['fecha_limite', 'fecha_adjudicacion', 'fecha_publicacion']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    if 'fecha_updated' in df.columns:
        df['fecha_updated'] = pd.to_datetime(df['fecha_updated'], errors='coerce', utc=True)
    
    # Extraer año
    df['ano'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce').dt.year
    
    # Eliminar duplicados
    n_antes = len(df)
    df = df.drop_duplicates(subset=['id'], keep='last')
    n_despues = len(df)
    if n_antes != n_despues:
        print(f"   ⚠ Eliminados {n_antes - n_despues:,} duplicados")
    
    # CSV
    csv_path = OUTPUT_DIR / f'{nombre_base}.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    size_mb = csv_path.stat().st_size / 1024 / 1024
    print(f"   ✓ CSV: {csv_path} ({size_mb:.1f} MB)")
    
    # Parquet
    try:
        parquet_path = OUTPUT_DIR / f'{nombre_base}.parquet'
        df.to_parquet(parquet_path, index=False, compression='snappy')
        size_mb = parquet_path.stat().st_size / 1024 / 1024
        print(f"   ✓ Parquet: {parquet_path} ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"   ⚠ Parquet no disponible: {e}")
    
    # Resumen por conjunto
    print(f"\n📊 RESUMEN POR CONJUNTO")
    print("=" * 60)
    if 'conjunto' in df.columns:
        for conjunto in df['conjunto'].unique():
            df_c = df[df['conjunto'] == conjunto]
            print(f"\n   {CONJUNTOS.get(conjunto, {}).get('nombre', conjunto)}:")
            print(f"      Registros: {len(df_c):,}")
            print(f"      Años: {df_c['ano'].min():.0f} - {df_c['ano'].max():.0f}")
            if 'importe_sin_iva' in df_c.columns:
                print(f"      Importe: {df_c['importe_sin_iva'].sum()/1e9:.2f}B €")
    
    # Resumen total
    print(f"\n📊 RESUMEN TOTAL")
    print("=" * 60)
    print(f"   Total registros: {len(df):,}")
    print(f"   Rango fechas: {df['ano'].min():.0f} - {df['ano'].max():.0f}")
    print(f"   Órganos únicos: {df['organo_contratante'].nunique():,}")
    print(f"   Adjudicatarios únicos: {df['adjudicatario'].nunique():,}")
    
    if 'importe_sin_iva' in df.columns:
        total = df['importe_sin_iva'].sum()
        print(f"   Importe total: {total/1e9:.2f}B €")
    
    return df

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Scraper completo de licitaciones públicas')
    parser.add_argument('--anos', type=str, required=True, help='Rango de años (ej: 2020-2026)')
    parser.add_argument('--conjunto', type=str, choices=list(CONJUNTOS.keys()) + ['todos'], 
                        default='todos', help='Conjunto de datos a descargar')
    parser.add_argument('--solo-descargar', action='store_true', help='Solo descargar, no procesar')
    parser.add_argument('--solo-procesar', action='store_true', help='Solo procesar archivos existentes')
    
    args = parser.parse_args()
    
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