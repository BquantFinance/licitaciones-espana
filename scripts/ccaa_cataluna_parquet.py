#!/usr/bin/env python3
"""
================================================================================
CATALUNYA - CSV A PARQUET v1.0
================================================================================
Convierte los CSVs relevantes a Parquet, descartando redundantes.

Sesgo del superviviente: ccaa_cataluna.py guarda en <carpeta>/_historico/ cada
versión anterior de un CSV que el portal ha cambiado (el RPC y los menores de la
Generalitat son ventanas móviles de 5 años). El parquet de cada CSV se
construye con TODAS sus versiones, de la más antigua a la vigente, con
comun.historico.acumular: lo que la administración retira o modifica sigue con
_en_ultima_descarga=False, y cada fila lleva _primera_descarga/_ultima_descarga
(sello de la versión en _historico/, o el mtime del CSV vigente). Con una sola
versión la salida es la de siempre más esas 3 columnas.
================================================================================
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import glob
import re
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import COLUMNAS_META, acumular, versiones  # noqa: E402

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

INPUT_DIR = "catalunya_datos_completos"
OUTPUT_DIR = "catalunya_parquet"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__).info

# =============================================================================
# MAPEO: TODOS los archivos se convierten (sin descartar nada)
# =============================================================================

# Estructura: (csv_relativo, parquet_destino, descripcion)

ARCHIVOS = {
    # =========================================================================
    # CONTRATACIÓN - TODOS
    # =========================================================================
    '01_transparencia_catalunya/01_contratacion/registro_publico_contratos.csv': 
        ('contratacion/contratos_registro.parquet', '⭐ MASTER - Todos los contratos formalizados'),
    
    '01_transparencia_catalunya/01_contratacion/publicaciones_pscp.csv': 
        ('contratacion/publicaciones_pscp.parquet', 'Publicaciones PSCP (ciclo completo)'),
    
    '01_transparencia_catalunya/01_contratacion/licitaciones_adjudicaciones_curso.csv': 
        ('contratacion/licitaciones_adjudicaciones.parquet', 'Licitaciones y adjudicaciones en curso'),
    
    '01_transparencia_catalunya/01_contratacion/contratacion_programada.csv': 
        ('contratacion/contratacion_programada.parquet', 'Contratación planificada'),
    
    '01_transparencia_catalunya/01_contratacion/contratacion_emergencia_covid.csv': 
        ('contratacion/contratos_covid.parquet', 'Contratos emergencia COVID'),
    
    '01_transparencia_catalunya/01_contratacion/resoluciones_tribunal.csv': 
        ('contratacion/resoluciones_tribunal.parquet', 'Resoluciones Tribunal Contratos'),
    
    '01_transparencia_catalunya/01_contratacion/adjudicaciones_generalitat.csv': 
        ('contratacion/adjudicaciones_generalitat.parquet', 'Adjudicaciones Generalitat'),
    
    '01_transparencia_catalunya/01_contratacion/fase_ejecucion.csv': 
        ('contratacion/fase_ejecucion.parquet', 'Contratos en fase ejecución'),
    
    # contratos_menores_generalitat: Socrata qjue-2pk9 (antes ydq4-xy5b, 404); jxvs-kzbu da 404
    '01_transparencia_catalunya/01_contratacion/contratos_menores_generalitat.csv':
        ('contratacion/contratos_menores_generalitat.parquet', 'Contratos menores Generalitat'),

    '01_transparencia_catalunya/01_contratacion/adjudicaciones_contractuales_quincenal.csv':
        ('contratacion/adjudicaciones_quincenales.parquet', 'Adjudicaciones contractuales quincenales'),

    # =========================================================================
    # SUBVENCIONES - TODOS
    # =========================================================================
    '01_transparencia_catalunya/02_subvenciones/raisc_concesiones.csv': 
        ('subvenciones/raisc_concesiones.parquet', '⭐ MASTER - Todas las subvenciones concedidas'),
    
    '01_transparencia_catalunya/02_subvenciones/raisc_convocatorias.csv': 
        ('subvenciones/raisc_convocatorias.parquet', 'Convocatorias RAISC'),
    
    '01_transparencia_catalunya/02_subvenciones/convocatorias_subvenciones.csv': 
        ('subvenciones/convocatorias_subvenciones.parquet', 'Otras convocatorias subvenciones'),
    
    # =========================================================================
    # CONVENIOS
    # =========================================================================
    '01_transparencia_catalunya/03_convenios/registro_convenios.csv': 
        ('convenios/convenios.parquet', 'Convenios de colaboración'),
    
    # =========================================================================
    # PRESUPUESTOS - TODOS
    # =========================================================================
    '01_transparencia_catalunya/04_presupuestos/presupuestos_aprobados.csv': 
        ('presupuestos/presupuestos_aprobados.parquet', 'Presupuestos aprobados'),
    
    '01_transparencia_catalunya/04_presupuestos/ejecucion_mensual_despeses.csv': 
        ('presupuestos/ejecucion_gastos.parquet', 'Ejecución mensual gastos'),
    
    '01_transparencia_catalunya/04_presupuestos/ejecucion_mensual_ingressos.csv': 
        ('presupuestos/ejecucion_ingresos.parquet', 'Ejecución mensual ingresos'),
    
    '01_transparencia_catalunya/04_presupuestos/despeses_2019.csv': 
        ('presupuestos/despeses_2019.parquet', 'Gastos 2019 detallado'),

    # Se descargaban (w2cu-rmuv, wwmk-zys7) pero no se convertían
    '01_transparencia_catalunya/04_presupuestos/evolucion_presupuestos.csv':
        ('presupuestos/evolucion_presupuestos.parquet', 'Evolución presupuestos Generalitat'),

    '01_transparencia_catalunya/04_presupuestos/ejecucion_consolidado_sector_publico.csv':
        ('presupuestos/ejecucion_consolidado_sector_publico.parquet', 'Ejecución consolidado sector público'),
    
    # =========================================================================
    # SECTOR PÚBLICO / ENTIDADES - TODOS
    # =========================================================================
    '01_transparencia_catalunya/05_sector_publico/ens_locals_catalunya.csv': 
        ('entidades/ens_locals.parquet', '⭐ MASTER - Todos los entes locales'),
    
    '01_transparencia_catalunya/05_sector_publico/registro_sector_publico.csv': 
        ('entidades/sector_publico_generalitat.parquet', 'Entidades sector público Generalitat'),
    
    '01_transparencia_catalunya/05_sector_publico/codigos_departamentos_ens.csv': 
        ('entidades/codigos_departamentos.parquet', 'Códigos de departamentos'),
    
    '01_transparencia_catalunya/05_sector_publico/composicio_plens.csv': 
        ('entidades/composicio_plens.parquet', 'Composición plenos (políticos)'),
    
    '01_transparencia_catalunya/05_sector_publico/ajuntaments.csv': 
        ('entidades/ajuntaments.parquet', 'Ayuntamientos (detalle)'),
    
    '01_transparencia_catalunya/05_sector_publico/ajuntaments_catalunya.csv': 
        ('entidades/ajuntaments_lista.parquet', 'Lista ayuntamientos'),
    
    # =========================================================================
    # RECURSOS HUMANOS - TODOS
    # =========================================================================
    '01_transparencia_catalunya/06_recursos_humanos/altos_cargos_retribuciones.csv': 
        ('rrhh/altos_cargos.parquet', 'Altos cargos y retribuciones'),
    
    '01_transparencia_catalunya/06_recursos_humanos/retribuciones_funcionarios.csv': 
        ('rrhh/retribuciones_funcionarios.parquet', 'Tablas retributivas funcionarios'),
    
    '01_transparencia_catalunya/06_recursos_humanos/retribuciones_laboral.csv': 
        ('rrhh/retribuciones_laboral.parquet', 'Tablas retributivas laborales'),
    
    '01_transparencia_catalunya/06_recursos_humanos/convocatorias_personal.csv': 
        ('rrhh/convocatorias_personal.parquet', 'Convocatorias empleo público'),
    
    '01_transparencia_catalunya/06_recursos_humanos/taules_retributives_alts_carrecs.csv': 
        ('rrhh/taules_retributives.parquet', 'Tablas retributivas altos cargos'),
    
    '01_transparencia_catalunya/06_recursos_humanos/enunciats_examens.csv': 
        ('rrhh/enunciats_examens.parquet', 'Enunciados exámenes oposiciones'),
    
    # =========================================================================
    # TERRITORIO - TODOS
    # =========================================================================
    '01_transparencia_catalunya/07_territorio/municipis_catalunya_geo.csv': 
        ('territorio/municipis_catalunya.parquet', 'Municipios Catalunya con geo'),
    
    '01_transparencia_catalunya/07_territorio/municipis_espanya.csv': 
        ('territorio/municipis_espanya.parquet', 'Municipios España'),
}


# =============================================================================
# FUNCIONES
# =============================================================================

def load_csv(path):
    """Carga CSV con detección de encoding y separador"""
    df, enc, sep = _leer_csv(path)
    return restaurar_ceros_iniciales(df, path, enc, sep)


def leer_texto(path):
    """Todas las celdas como texto, con la misma detección que load_csv (para
    comparar versiones tal como las sirvió el portal: un 1 y un 1.0 no casarían)."""
    return _leer_csv(path, dtype=str)[0]


def _leer_csv(path, **kwargs):
    """(DataFrame, encoding, separador) del primer par que da más de una columna."""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    separators = [',', ';', '\t']
    
    # Probar primero el separador más frecuente en la cabecera: un CSV con ';' y una coma
    # en algún nombre de columna se aceptaría con ',' y se leería desalineado
    try:
        with open(path, 'rb') as f:
            cabecera = f.readline(1024 * 1024).decode('latin-1')
        separators.sort(key=lambda s: -cabecera.count(s))
    except OSError:
        pass
    
    for enc in encodings:
        for sep in separators:
            try:
                # Las líneas mal formadas se descartan, pero se cuentan y se avisa
                # (antes se perdían en silencio)
                with warnings.catch_warnings(record=True) as avisos:
                    warnings.simplefilter("always", pd.errors.ParserWarning)
                    df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False, on_bad_lines='warn', **kwargs)
            except Exception:
                continue
            if len(df.columns) > 1:
                descartadas = sum(
                    str(a.message).count("Skipping line")
                    for a in avisos if issubclass(a.category, pd.errors.ParserWarning)
                )
                if descartadas:
                    log(f"   ⚠️ {Path(path).name}: {descartadas:,} líneas mal formadas descartadas")
                return df, enc, sep
    
    raise ValueError(f"No se pudo cargar: {path}")


SELLO = re.compile(r"(\d{8})T(\d{2})(\d{2})(\d{2})Z(_\d+)?")


def versiones_csv(csv_path):
    """[(ruta, fecha)] de las versiones del CSV, de la más antigua a la vigente.
    fecha: sello de la versión en _historico/ o mtime del CSV vigente (UTC)."""
    csv_path = Path(csv_path)
    salida = []
    for v in versiones(csv_path):
        if v == csv_path:
            momento = datetime.fromtimestamp(v.stat().st_mtime, timezone.utc)
            salida.append((v, momento.strftime("%Y-%m-%dT%H:%M:%SZ")))
            continue
        # glob 'X__*' también casaría con las versiones de otro 'X__algo.csv'
        m = SELLO.fullmatch(v.stem[len(csv_path.stem) + 2:])
        if m:
            d, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4)
            salida.append((v, f"{d[:4]}-{d[4:6]}-{d[6:]}T{hh}:{mm}:{ss}Z"))
    return salida


def construir_registros(csv_path, tmp_csv):
    """Registros de todas las versiones del CSV acumulados (comun.historico).
    Devuelve (DataFrame con COLUMNAS_META, nº de versiones)."""
    vers = versiones_csv(csv_path)
    if len(vers) == 1:
        # Una sola versión: exactamente la lectura de siempre + columnas meta
        return acumular(None, load_csv(csv_path), vers[0][1], permitir_vacio=True), 1

    acumulado = None
    for ruta, fecha in vers:
        texto = leer_texto(ruta)
        if len(texto) == 0 and acumulado is not None:
            log(f"   ⚠️ Versión vacía ignorada (no se marca nada como retirado): {ruta.name}")
            continue
        acumulado = acumular(acumulado, texto, fecha, permitir_vacio=acumulado is None)

    # Tipos: se vuelve a leer el texto acumulado con la misma lectura que un CSV
    # suelto (misma inferencia de tipos y de ceros a la izquierda)
    datos = acumulado.drop(columns=list(COLUMNAS_META))
    try:
        datos.to_csv(tmp_csv, index=False, encoding='utf-8')
        df = load_csv(tmp_csv)
    finally:
        if Path(tmp_csv).exists():
            Path(tmp_csv).unlink()
    if list(df.columns) != list(datos.columns) or len(df) != len(datos):
        log("   ⚠️ No se pudieron inferir los tipos: se guarda como texto")
        df = datos
    for c in COLUMNAS_META:
        df[c] = acumulado[c].to_numpy()
    return df, len(vers)


# Texto numérico con ceros a la izquierda: '08002', '0801930008', '-01' (no '0', '0.5')
PATRON_CERO_INICIAL = r'^\s*[+-]?0\d'
FILAS_POR_TROZO = 500_000


def _tiene_cero_inicial(serie):
    valores = serie.dropna()
    return len(valores) > 0 and bool(valores.astype(str).str.match(PATRON_CERO_INICIAL).any())


def restaurar_ceros_iniciales(df, path, encoding, sep):
    """Columnas que pandas leyó como número pero cuyo texto original lleva ceros a
    la izquierda (códigos postales '08002', INE10 '0801930008', municipio '080193'...):
    el 0 se perdía (en los parquet publicados CODIPOSTAL empieza en 8002 y CODI_INE10
    en 801930008). Esas columnas se guardan como texto, tal como las publica la fuente.
    """
    # Si pandas usó la 1ª columna como índice (más campos que cabeceras) las
    # posiciones no casan con las del archivo: no se toca nada
    if not isinstance(df.index, pd.RangeIndex):
        return df
    tipos = df.dtypes
    numericas = [i for i in range(len(tipos))
                 if pd.api.types.is_numeric_dtype(tipos.iloc[i]) and not pd.api.types.is_bool_dtype(tipos.iloc[i])]
    if not numericas:
        return df

    def trozos():
        # Misma lectura (mismas líneas descartadas) pero todo como texto y por trozos
        return pd.read_csv(path, encoding=encoding, sep=sep, dtype=str, on_bad_lines='skip',
                           chunksize=FILAS_POR_TROZO)

    try:
        con_ceros = set()
        for trozo in trozos():
            if trozo.shape[1] != df.shape[1]:
                return df
            for i in numericas:
                if i not in con_ceros and _tiene_cero_inicial(trozo.iloc[:, i]):
                    con_ceros.add(i)
        if not con_ceros:
            return df
        posiciones = sorted(con_ceros)
        texto = pd.concat([t.iloc[:, posiciones] for t in trozos()], ignore_index=True)
    except Exception as e:
        log(f"   ⚠️ {Path(path).name}: no se pudo comprobar ceros a la izquierda ({e})")
        return df
    if len(texto) != len(df):
        log(f"   ⚠️ {Path(path).name}: no se pudo comprobar ceros a la izquierda (filas distintas)")
        return df
    for j, i in enumerate(posiciones):
        df.isetitem(i, texto.iloc[:, j].array)
    log(f"   🔢 Guardadas como texto (ceros a la izquierda): {', '.join(str(df.columns[i]) for i in posiciones)}")
    return df


def texto_sin_nulos(serie):
    """Columnas de texto (object en pandas 2, str en pandas 3) -> str con '' en vez de nulos"""
    return serie.fillna('').astype(str)


def es_texto(serie):
    """True para columnas object (pandas 2) o str (pandas 3)"""
    return serie.dtype == 'object' or pd.api.types.is_string_dtype(serie.dtype)


def anio_de_nombre(nombre):
    """Año (19xx/20xx) contenido en el nombre de archivo, o None"""
    m = re.search(r'(19|20)\d{2}', nombre)
    return int(m.group(0)) if m else None


def convert_to_parquet(input_path, output_path, descripcion):
    """Convierte un CSV a Parquet"""
    log(f"\n📄 {descripcion}")
    log(f"   Input: {input_path.name}")
    
    # Cargar (todas las versiones del CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df, n_versiones = construir_registros(input_path, output_path.with_name(output_path.name + '.csv.tmp'))
    log(f"   📝 {len(df):,} registros, {len(df.columns)} columnas")
    if n_versiones > 1:
        retirados = int((~df['_en_ultima_descarga'].astype(bool)).sum())
        log(f"   📜 {n_versiones} versiones del CSV; {retirados:,} registros ya no servidos (conservados)")
    
    # Optimizar tipos de datos
    for col in df.columns:
        # Convertir object a string para evitar errores de tipos mixtos
        if es_texto(df[col]):
            df[col] = texto_sin_nulos(df[col])
    
    # Crear directorio de salida
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar
    df.to_parquet(output_path, index=False, compression='snappy')
    
    size_csv = input_path.stat().st_size / 1024 / 1024
    size_parquet = output_path.stat().st_size / 1024 / 1024
    ratio = (1 - size_parquet / size_csv) * 100 if size_csv > 0 else 0
    
    log(f"   💾 {output_path.name}: {size_parquet:.1f}MB (↓{ratio:.0f}% de {size_csv:.1f}MB)")
    
    return len(df), size_parquet


# Formatos que descarga ccaa_cataluna.py de Open Data BCN
EXTENSIONES_BCN = ('.csv', '.xlsx', '.xls', '.json')


def archivos_bcn(dir_path):
    """CSV de la carpeta y, además, los XLSX/XLS/JSON que no tienen un CSV con el
    mismo nombre. Antes solo se convertían los CSV: un recurso publicado solo en
    Excel o JSON se descargaba pero no llegaba al parquet. Si hay CSV del mismo
    recurso se usa el CSV (no se duplican filas)."""
    archivos = sorted(p for p in dir_path.iterdir() if p.is_file() and p.suffix.lower() in EXTENSIONES_BCN)
    con_csv = {p.stem.lower() for p in archivos if p.suffix.lower() == '.csv'}
    return [p for p in archivos if p.suffix.lower() == '.csv' or p.stem.lower() not in con_csv]


def cargar_tabla(path):
    """Lee un recurso de Open Data BCN (CSV, Excel o JSON) como DataFrame"""
    ext = path.suffix.lower()
    if ext == '.csv':
        return load_csv(path)
    if ext in ('.xlsx', '.xls'):
        hojas = {nombre: h for nombre, h in pd.read_excel(path, sheet_name=None).items() if not h.empty}
        if not hojas:
            raise ValueError("Excel sin datos")
        if len(hojas) == 1:
            return next(iter(hojas.values()))
        # Todas las hojas (p. ej. una por trimestre), indicando de cuál sale cada fila
        return pd.concat([h.assign(_hoja=nombre) for nombre, h in hojas.items()], ignore_index=True)
    if ext == '.json':
        with open(path, encoding='utf-8-sig') as f:
            datos = json.load(f)
        if isinstance(datos, dict):  # formato datastore de CKAN: {"result": {"records": [...]}}
            datos = datos.get('result', datos)
            if isinstance(datos, dict):
                datos = datos.get('records', datos)
        if not isinstance(datos, list) or not all(isinstance(r, dict) for r in datos):
            raise ValueError("JSON sin lista de registros")
        return pd.json_normalize(datos)
    raise ValueError(f"Formato no soportado: {path.name}")


def consolidar_bcn(input_dir, output_dir, carpeta, destino, titulo, origen='_año'):
    """Consolida todos los recursos de un dataset de Open Data BCN en un parquet.

    origen='_año': año sacado del nombre del archivo; '_archivo_origen': nombre del archivo.
    """
    log("\n" + "="*60)
    log(f"📦 CONSOLIDANDO: {titulo}")
    
    dir_path = input_dir / '02_barcelona' / carpeta
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for archivo in archivos_bcn(dir_path):
        try:
            df = cargar_tabla(archivo)
            if origen == '_archivo_origen':
                df['_archivo_origen'] = archivo.name
            else:
                df['_año'] = anio_de_nombre(archivo.stem)
            dfs.append(df)
            log(f"   ✅ {archivo.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {archivo.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / destino
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


def consolidate_barcelona_menores(input_dir, output_dir):
    """Consolida contratos menores Barcelona (múltiples años) en un solo Parquet"""
    return consolidar_bcn(input_dir, output_dir, 'contratos_menores', 'contratos_menores_bcn.parquet',
                          'Contratos menores Barcelona')


def consolidate_barcelona_contratistas(input_dir, output_dir):
    """Consolida contratistas Barcelona (múltiples años)"""
    return consolidar_bcn(input_dir, output_dir, 'contratistas', 'contratistas_bcn.parquet',
                          'Contratistas Barcelona')


def consolidate_barcelona_perfil(input_dir, output_dir):
    """Consolida perfil contratante Barcelona"""
    return consolidar_bcn(input_dir, output_dir, 'perfil_contratante', 'perfil_contratante_bcn.parquet',
                          'Perfil contratante Barcelona', origen='_archivo_origen')


def consolidate_barcelona_modificaciones(input_dir, output_dir):
    """Consolida modificaciones de contratos Barcelona"""
    return consolidar_bcn(input_dir, output_dir, 'modificaciones_contratos', 'modificaciones_bcn.parquet',
                          'Modificaciones contratos Barcelona')


def consolidate_barcelona_resumen(input_dir, output_dir):
    """Consolida resumen trimestral Barcelona"""
    return consolidar_bcn(input_dir, output_dir, 'resumen_trimestral', 'resumen_trimestral_bcn.parquet',
                          'Resumen trimestral Barcelona')
    
    
def consolidate_barcelona_autorizacion(input_dir, output_dir):
    """Contratos menores derivados de una autorización genérica de gasto (se descargaban
    en 02_barcelona/contratos_menores_autorizacion pero no se convertían)"""
    return consolidar_bcn(input_dir, output_dir, 'contratos_menores_autorizacion',
                          'contratos_menores_autorizacion_bcn.parquet',
                          'Contratos menores (autorización genérica) Barcelona')


# =============================================================================
# MAIN
# =============================================================================

def main():
    start = datetime.now()
    
    print("\n" + "="*70)
    print("📦 CATALUNYA CSV → PARQUET")
    print("="*70)
    
    input_dir = Path(INPUT_DIR)
    output_dir = Path(OUTPUT_DIR)
    
    if not input_dir.exists():
        log(f"❌ No encontrado: {input_dir}")
        return
    
    output_dir.mkdir(exist_ok=True)
    
    stats = {
        'convertidos': 0,
        'registros_total': 0,
        'tamaño_total_mb': 0,
        'errores': 0,
    }
    
    # =========================================================================
    # ARCHIVOS INDIVIDUALES
    # =========================================================================
    log("\n" + "="*70)
    log("📄 CONVIRTIENDO ARCHIVOS INDIVIDUALES")
    log("="*70)
    
    for csv_rel, (parquet_rel, descripcion) in ARCHIVOS.items():
        csv_path = input_dir / csv_rel
        
        if not csv_path.exists():
            log(f"\n⚠️ No encontrado: {csv_rel}")
            continue
        
        parquet_path = output_dir / parquet_rel
        
        try:
            n_records, size_mb = convert_to_parquet(csv_path, parquet_path, descripcion)
            stats['convertidos'] += 1
            stats['registros_total'] += n_records
            stats['tamaño_total_mb'] += size_mb
        except Exception as e:
            log(f"\n❌ Error en {csv_path.name}: {e}")
            stats['errores'] += 1
    
    # =========================================================================
    # CONSOLIDACIONES BARCELONA
    # =========================================================================
    log("\n" + "="*70)
    log("📦 CONSOLIDANDO BARCELONA (múltiples archivos → 1 parquet)")
    log("="*70)
    
    n, s = consolidate_barcelona_menores(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_contratistas(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_perfil(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_modificaciones(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_resumen(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_autorizacion(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    # =========================================================================
    # RESUMEN
    # =========================================================================
    elapsed = (datetime.now() - start).total_seconds()
    
    print("\n" + "="*70)
    log("📊 RESUMEN")
    print("="*70)
    log(f"✅ Archivos convertidos: {stats['convertidos']}")
    log(f"❌ Errores: {stats['errores']}")
    log(f"📝 Registros totales: {stats['registros_total']:,}")
    log(f"💾 Tamaño total Parquet: {stats['tamaño_total_mb']:.1f} MB")
    log(f"⏱️ Tiempo: {elapsed:.1f} segundos")
    
    # Generar índice
    print("\n" + "="*70)
    log("📋 ESTRUCTURA FINAL")
    print("="*70)
    
    for parquet_file in sorted(output_dir.rglob('*.parquet')):
        rel_path = parquet_file.relative_to(output_dir)
        size = parquet_file.stat().st_size / 1024 / 1024
        log(f"   {rel_path} ({size:.1f}MB)")
    
    # README
    with open(output_dir / 'README.md', 'w', encoding='utf-8') as f:
        f.write(f"""# Catalunya - Datos en Parquet

Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## Estadísticas
- Archivos: {stats['convertidos']}
- Registros: {stats['registros_total']:,}
- Tamaño: {stats['tamaño_total_mb']:.1f} MB

## Estructura

```
{OUTPUT_DIR}/
├── contratacion/
│   ├── contratos_registro.parquet          ⭐ MASTER
│   ├── publicaciones_pscp.parquet          (ciclo completo licitación)
│   ├── licitaciones_adjudicaciones.parquet
│   ├── adjudicaciones_generalitat.parquet
│   ├── fase_ejecucion.parquet
│   ├── contratacion_programada.parquet
│   ├── contratos_covid.parquet
│   ├── resoluciones_tribunal.parquet
│   ├── contratos_menores_generalitat.parquet
│   ├── adjudicaciones_quincenales.parquet
│   ├── contratos_menores_bcn.parquet       (consolidado, todos los años publicados)
│   ├── contratos_menores_autorizacion_bcn.parquet
│   ├── contratistas_bcn.parquet            (consolidado, todos los años publicados)
│   ├── perfil_contratante_bcn.parquet
│   ├── modificaciones_bcn.parquet
│   └── resumen_trimestral_bcn.parquet
├── subvenciones/
│   ├── raisc_concesiones.parquet           ⭐ MASTER (9.6M registros)
│   ├── raisc_convocatorias.parquet
│   └── convocatorias_subvenciones.parquet
├── convenios/
│   └── convenios.parquet
├── presupuestos/
│   ├── ejecucion_gastos.parquet            (1.5M registros)
│   ├── ejecucion_ingresos.parquet
│   ├── presupuestos_aprobados.parquet
│   ├── evolucion_presupuestos.parquet
│   ├── ejecucion_consolidado_sector_publico.parquet
│   └── despeses_2019.parquet
├── entidades/
│   ├── ens_locals.parquet                  ⭐ MASTER
│   ├── sector_publico_generalitat.parquet
│   ├── codigos_departamentos.parquet
│   ├── composicio_plens.parquet
│   ├── ajuntaments.parquet
│   └── ajuntaments_lista.parquet
├── rrhh/
│   ├── altos_cargos.parquet
│   ├── convocatorias_personal.parquet
│   ├── retribuciones_funcionarios.parquet
│   ├── retribuciones_laboral.parquet
│   ├── taules_retributives.parquet
│   └── enunciats_examens.parquet
└── territorio/
    ├── municipis_catalunya.parquet
    └── municipis_espanya.parquet
```

## Uso

```python
import pandas as pd

# Cargar contratos (10x más rápido que CSV)
df = pd.read_parquet('contratacion/contratos_registro.parquet')

# Filtrar por año
df['año'] = pd.to_datetime(df['Data formalització']).dt.year
df_2024 = df[df['año'] == 2024]
```

## Notas

- **TODOS** los archivos CSV originales se han convertido (sin descartar nada)
- Los archivos de Barcelona (múltiples años) se han consolidado en uno solo
- Parquet es ~60-80% más pequeño y 10x más rápido de cargar
""")
    
    log(f"\n📄 README: {output_dir}/README.md")


if __name__ == "__main__":
    main()