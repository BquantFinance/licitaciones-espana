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
versión la salida es la de siempre más esas 3 columnas. Las consolidaciones de
Open Data Barcelona (varios recursos → 1 parquet) hacen lo mismo recurso a
recurso (registros_bcn).

Los CSV sueltos (ARCHIVOS) se convierten sin tenerlos enteros en memoria (ver
CONVERSIÓN POR TROZOS): las versiones se leen por trozos y se guardan en disco,
la acumulación se decide con las huellas de las filas, los tipos se infieren
columna a columna y el Parquet se escribe por grupos de filas. La salida es la
misma que cuando se hacía todo en memoria.
================================================================================
"""

import argparse
import codecs
import encodings.cp1252
import os
import shutil
import sys
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import glob
import re
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (  # noqa: E402
    ANADIDA, COLUMNAS_META, COLUMNAS_SEMILLA, IGNORAR_POR_DEFECTO, ORIGEN_SEMILLA, _armonizar, acumular,
    imprimir_informe_semilla, informe_semilla, seleccionar_semilla, sembrar, versiones,
)

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


# Categorías (--categorias): la carpeta del CSV sin su número
# ('01_transparencia_catalunya/01_contratacion/...' → 'contratacion'). Las consolidaciones
# de Open Data Barcelona son de contratación.
CATEGORIAS = None   # None: todas


def categoria_csv(csv_rel):
    return re.sub(r'^\d+_', '', csv_rel.split('/')[1])


CATEGORIAS_DISPONIBLES = sorted({categoria_csv(k) for k in ARCHIVOS})

# Semilla (--semilla <carpeta de Catalunya del release v2026.02>): por Parquet, la clave estable con
# la que se añaden las filas del publicado que no están en la descarga, con _origen='release
# v2026.02' y _en_ultima_descarga=False (comun.historico.sembrar). Medido el 28-sep-2026 contra la
# primera descarga del VPS:
#   - RPC: 751.187 filas, sobre todo menores y liquidaciones de 2021 que la ventana móvil de 5 años ya
#     no sirve. Con Exercici en la clave solo se repiten 4.754 del publicado.
#   - PSCP: 85.397 filas de procedimientos que ya no se publican, por el uuid del procedimiento en la
#     URL (uuid_publicacio). Con la URL entera eran 187.577: cambia con cada fase y entre /ca/ y
#     /es/, y se añadían 102.180 fases antiguas de procedimientos que siguen publicados. Las claves
#     con numero_lot no sirven: el publicado lo guarda con otro formato ('' frente a '0').
#   - Fase de ejecución: 9.047 (por la URL del JSON). Contratación programada: 5.095 (trimestres
#     pasados que el portal retira).
#   - Adjudicaciones de la Generalitat, contratos COVID y resoluciones del Tribunal coinciden con el
#     publicado (0 filas que falten); los menores de la Generalitat (qjue-2pk9) no estaban en él.
SEMILLAS = {
    'contratacion/contratos_registro.parquet': [
        'Identificador organisme contractant', 'Codi de l’expedient', 'Número de lot', 'Situació contractual',
        'Número de modificació', 'Número de pròrroga', 'Exercici'],
    'contratacion/publicaciones_pscp.parquet': (['enllac_publicacio'], 'uuid_publicacio'),
    'contratacion/fase_ejecucion.parquet': ['URL JSON'],
    'contratacion/contratacion_programada.parquet': [
        'Any', 'Trimestre', 'Departament/Ens', 'Descripció del contracte', 'Agrupació', 'Tipus de contracte'],
}
# Consolidaciones de Open Data Barcelona con semilla (parquet de destino → clave, como en SEMILLAS).
# Medido el 29-sep-2026 contra la primera descarga del VPS: menores, contratistas, modificaciones y
# resumen trimestral del release coinciden fila a fila con la descarga (0 filas que falten). El perfil
# de contratante no: su CSV de la PSCP es una ventana que el portal va cerrando y el release trae
# 6.120 publicaciones (7.411 filas, casi todas de 2018-2021) que ya no sirve. Se casan por el uuid del
# procedimiento; las filas sin uuid (el fichero estático anterior a junio de 2016, igual celda a celda
# en la descarga) no se siembran.
SEMILLAS_BCN = {
    'perfil_contratante_bcn.parquet': (['ENLLAC_PUBLICACIO'], 'uuid_publicacio_bcn'),
}
# Carpeta de Catalunya del release (--semilla) para las consolidaciones de Barcelona; la fija main()
SEMILLA = None
# Casos a revisar de la ejecución (acumular_versiones: una versión cuya cabecera cambia); con alguno,
# main() acaba con código 1
REVISAR = []

# Columnas nuestras: sus nulos no pasan a '' (en las filas sembradas no hay fechas de descarga, y
# _origen es nulo en las descargadas)
COLUMNAS_CONTROL = set(COLUMNAS_META) | {'_origen'}


# =============================================================================
# FUNCIONES
# =============================================================================

# Lectura de los CSV: UTF-8 y, solo en las secuencias que no son UTF-8 válido, CP1252 byte a byte
# (_recurso_cp1252, manejador de errores de la decodificación). Los CSV de Open Data Barcelona que no son
# UTF-8 (contratistas 2012-2013, menores 2015 y modificaciones 2013-2014) están en CP1252: leídos como
# latin-1, el '€' (0x80) llegaba como chr(128) y la '’' (0x92) como chr(146) en 37.721 celdas (medido el
# 29-sep-2026: 36.728 '€' y 1.758 comillas, rayas, viñetas y puntos suspensivos). Y un solo byte
# inválido ya no cambia la lectura del resto del fichero: antes (UTF-8 y, si fallaba, el fichero entero
# en CP1252) un UTF-8 con un carácter mal codificado se leía entero en CP1252, sus cabeceras cambiaban
# ('Òrgan contractant' → 'Ã’rgan contractant') y la acumulación de versiones lo duplicaba (revisión de
# la PR #40: 26.115 de las 39.192 filas de 2018 como retiradas y otra vez con mojibake). Medido en los 50
# CSV del crudo del VPS (29-sep-2026): 45 son UTF-8 válido y en los 5 en CP1252 esta lectura da los
# mismos caracteres que la del fichero entero en CP1252. Los 5 bytes que CP1252 deja sin asignar (0x81,
# 0x8D, 0x8F, 0x90 y 0x9D) dan el carácter de control C1 del mismo valor, como en latin-1 y en el
# 'windows-1252' de los navegadores (WHATWG): no se pierde ninguno.
_TABLA_CP1252 = ''.join(chr(i) if c == '\ufffe' else c for i, c in enumerate(encodings.cp1252.decoding_table))
ERRORES_UTF8 = 'ccaa_cataluna_recurso_cp1252'


def _recurso_cp1252(error):
    """Manejador de errores de la lectura UTF-8 (ERRORES_UTF8): cada byte de la secuencia inválida,
    como su carácter CP1252."""
    if not isinstance(error, UnicodeDecodeError):
        raise error
    return ''.join(_TABLA_CP1252[b] for b in error.object[error.start:error.end]), error.end


codecs.register_error(ERRORES_UTF8, _recurso_cp1252)


def load_csv(path):
    """Carga CSV con detección de encoding y separador"""
    df, enc, sep = _leer_csv(path)
    return restaurar_ceros_iniciales(df, path, enc, sep)


def leer_texto(path):
    """Todas las celdas como texto, con la misma detección que load_csv (para
    comparar versiones tal como las sirvió el portal: un 1 y un 1.0 no casarían)."""
    return _leer_csv(path, dtype=str)[0]


def _separadores(path):
    """Separadores que se prueban, el más frecuente en la cabecera primero: un CSV con ';' y una coma
    en algún nombre de columna se aceptaría con ',' y se leería desalineado."""
    separators = [',', ';', '\t']
    try:
        with open(path, 'rb') as f:
            cabecera = f.readline(1024 * 1024).decode('latin-1')
        separators.sort(key=lambda s: -cabecera.count(s))
    except OSError:
        pass
    return separators


def _leer_csv(path, **kwargs):
    """(DataFrame, encoding, separador) del primer separador que da más de una columna. UTF-8 y, solo
    en las secuencias que no lo son, CP1252 (ERRORES_UTF8)."""
    for sep in _separadores(path):
        try:
            # Las líneas mal formadas se descartan, pero se cuentan y se avisa
            # (antes se perdían en silencio)
            with warnings.catch_warnings(record=True) as avisos:
                warnings.simplefilter("always", pd.errors.ParserWarning)
                df = pd.read_csv(path, encoding='utf-8', encoding_errors=ERRORES_UTF8, sep=sep, low_memory=False,
                                 on_bad_lines='warn', **kwargs)
        except Exception:
            continue
        if len(df.columns) > 1:
            descartadas = sum(
                str(a.message).count("Skipping line")
                for a in avisos if issubclass(a.category, pd.errors.ParserWarning)
            )
            if descartadas:
                log(f"   ⚠️ {Path(path).name}: {descartadas:,} líneas mal formadas descartadas")
            return df, 'utf-8', sep
    
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


def acumular_versiones(vers, leer, saltar_ilegibles=False):
    """Registros de las versiones [(ruta, fecha)], de la más antigua a la vigente, leídas con `leer`
    y acumuladas con comun.historico.acumular. Una versión vacía (salvo la primera) no retira nada:
    se ignora. Con saltar_ilegibles, una versión que no se puede leer tampoco (se avisa); sin él, el
    error sube. Una versión cuya cabecera pierde alguna columna de la anterior (renombrada, quitada o
    leída de otra forma) no se acumula a ciegas: acumular solo compararía las columnas comunes y
    daría el fichero entero por retirado y vuelto a publicar (o casaría filas distintas). Se avisa,
    se anota en REVISAR (main() acaba con código 1) y no se retira ni se duplica nada. Las columnas
    nuevas sí se aceptan: se siguen comparando todas las de antes. None si no se ha podido leer
    ninguna."""
    acumulado = cabecera = None
    for ruta, fecha in vers:
        try:
            texto = leer(ruta)
        except Exception as e:
            if not saltar_ilegibles:
                raise
            log(f"   ⚠️ Versión ilegible ignorada (no se marca nada como retirado): {ruta.name} ({e})")
            continue
        if len(texto) == 0 and acumulado is not None:
            log(f"   ⚠️ Versión vacía ignorada (no se marca nada como retirado): {ruta.name}")
            continue
        faltan = [c for c in cabecera if c not in texto.columns] if cabecera is not None else []
        if faltan:
            nuevas = [c for c in texto.columns if c not in cabecera]
            aviso = (f"{ruta.parent.name}/{ruta.name}: la cabecera cambia respecto a la versión anterior "
                     f"(faltan {faltan}; nuevas {nuevas}); no se acumula: no se retira ni se duplica nada")
            log(f"   ⚠️ REVISAR {aviso}")
            REVISAR.append(aviso)
            continue
        acumulado = acumular(acumulado, texto, fecha, permitir_vacio=acumulado is None)
        cabecera = list(texto.columns)
    return acumulado


def tipos_como_csv(acumulado, tmp_csv):
    """Registros acumulados como texto con los tipos de una lectura suelta del CSV."""
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
    return df


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
    texto = _texto_con_ceros(path, encoding, sep, df.shape[1], numericas, len(df))
    for i, valores in texto.items():
        df.isetitem(i, valores)
    if texto:
        log(f"   🔢 Guardadas como texto (ceros a la izquierda): {', '.join(str(df.columns[i]) for i in texto)}")
    return df


def _texto_con_ceros(path, encoding, sep, n_columnas, numericas, n_filas):
    """Lo que restaurar_ceros_iniciales lee del CSV: {posición: texto (array)} de las columnas `numericas`
    (posiciones) cuyo texto lleva ceros a la izquierda, en orden; {} si no hay o no se puede comprobar
    (se avisa). n_columnas y n_filas son los de la lectura con tipos."""
    if not numericas:
        return {}

    def trozos():
        # Misma lectura (mismas líneas descartadas) pero todo como texto y por trozos
        return pd.read_csv(path, encoding=encoding, encoding_errors=ERRORES_UTF8, sep=sep, dtype=str,
                           on_bad_lines='skip', chunksize=FILAS_POR_TROZO)

    try:
        con_ceros = set()
        for trozo in trozos():
            if trozo.shape[1] != n_columnas:
                return {}
            for i in numericas:
                if i not in con_ceros and _tiene_cero_inicial(trozo.iloc[:, i]):
                    con_ceros.add(i)
        if not con_ceros:
            return {}
        posiciones = sorted(con_ceros)
        texto = pd.concat([t.iloc[:, posiciones] for t in trozos()], ignore_index=True)
    except Exception as e:
        log(f"   ⚠️ {Path(path).name}: no se pudo comprobar ceros a la izquierda ({e})")
        return {}
    if len(texto) != n_filas:
        log(f"   ⚠️ {Path(path).name}: no se pudo comprobar ceros a la izquierda (filas distintas)")
        return {}
    return {i: texto.iloc[:, j].array for j, i in enumerate(posiciones)}


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


def clave_texto(df, columnas):
    """Clave comparable entre la descarga y el publicado, que guardan los mismos datos con tipos
    distintos: texto sin espacios a los lados y enteros sin '.0'. El vacío es un valor ('Número de
    modificació' vacío en el RPC es «sin modificación», no una clave incompleta). Si todas las columnas
    están vacías, nula: no se sabe qué fila es y sembrar la compara por contenido."""
    partes = []
    for c in columnas:
        s = df[c].astype(object)
        s = s.where(s.notna(), '').astype(str).str.strip().str.replace(r'^(-?\d+)\.0+$', r'\1', regex=True)
        partes.append(s.reset_index(drop=True))
    clave = partes[0].str.cat(partes[1:], sep='\x1f') if len(partes) > 1 else partes[0]
    vacia = pd.concat([p.eq('') for p in partes], axis=1).all(axis=1).to_numpy()
    valores = clave.to_numpy(dtype=object)
    valores[vacia] = None   # None en pandas 2 y 3 (con where, pandas 3 pone NaN)
    return pd.Series(valores, index=df.index, dtype=object)


def uuid_publicacio(df, columna='enllac_publicacio'):
    """Clave de la PSCP: el uuid del procedimiento en la URL de la publicación
    (.../detall-publicacio/<uuid>/<id>). La URL entera cambia con cada fase (el <id>) y entre /ca/ y
    /es/; el uuid no. Sin uuid, nula (sembrar compara por contenido)."""
    url = df[columna].astype(object).where(df[columna].notna(), '').astype(str)
    uuid = url.str.extract(r'detall-publicacio/([0-9a-fA-F-]{36})', expand=False).str.lower()
    valores = uuid.to_numpy(dtype=object)
    valores[uuid.isna().to_numpy()] = None
    return pd.Series(valores, index=df.index, dtype=object)


def uuid_publicacio_bcn(df):
    """Clave del perfil de contratante de Barcelona: la misma que la PSCP (es su extracto), en la
    columna ENLLAC_PUBLICACIO del CSV del Ayuntamiento."""
    return uuid_publicacio(df, 'ENLLAC_PUBLICACIO')


def sembrar_release(df, ruta, columnas, solo_con_clave=False):
    """Añade a `df` las filas del Parquet publicado `ruta` cuya clave (ver SEMILLAS: columnas, que se
    comparan con clave_texto, o (columnas, nombre de la función que da la clave)) no está en la
    descarga. Sin el fichero, `df` tal cual (con un aviso). Con solo_con_clave, las filas del
    publicado sin clave no se siembran (sembrar las compararía por contenido)."""
    columnas, funcion = (columnas, None) if isinstance(columnas, list) else columnas
    clave = globals()[funcion] if funcion else (lambda d: clave_texto(d, columnas))
    ruta = Path(ruta)
    if not ruta.exists():
        log(f"   ⚠️ Sin semilla: no existe {ruta}")
        return df
    publicado = pd.read_parquet(ruta)
    faltan = [c for c in columnas if c not in df.columns or c not in publicado.columns]
    if faltan:
        log(f"   ⚠️ Semilla {ruta.name} sin sembrar: faltan columnas de la clave ({', '.join(faltan)})")
        return df
    publicado = publicado.assign(_clave_semilla=clave(publicado))
    if solo_con_clave:
        sin_clave = publicado['_clave_semilla'].isna()
        if sin_clave.any():
            log(f"   ℹ️ Semilla {ruta.name}: {int(sin_clave.sum()):,} filas sin clave no se siembran")
            publicado = publicado[~sin_clave.to_numpy()]
    out, informe = sembrar(df.assign(_clave_semilla=clave(df)), publicado, '_clave_semilla')
    informe['ruta'] = str(ruta)
    imprimir_informe_semilla(informe)
    return out.drop(columns='_clave_semilla')


# =============================================================================
# CONVERSIÓN POR TROZOS (convert_to_parquet)
# =============================================================================
# Antes, convert_to_parquet tenía en memoria las versiones del CSV, la tabla acumulada, la semilla y sus
# copias: con las tres versiones de publicaciones_pscp.csv (2,5 GB cada una) el pico era de 15,2 GiB
# (medido el 7-oct-2026; leer una sola versión entera ya pasa de 8 GiB) y el semanal moría por memoria.
# Ahora solo hay en memoria trozos de FILAS_POR_LOTE filas, una columna entera, las huellas de las filas
# (8 bytes por fila) y las filas que se añaden de la semilla. Lo demás va a una carpeta temporal junto al
# Parquet (CARPETA_TROZOS), que se borra al acabar:
#   1. Cada versión se lee como texto por trozos y se guarda en Arrow (leer_version).
#   2. La acumulación de comun.historico.acumular se decide con las huellas de las filas (las de
#      _claves): la tabla acumulada son referencias a filas de las versiones (Registros).
#   3. Los tipos se infieren columna a columna con la misma lectura de pandas sobre la columna entera
#      (_tipar): pandas infiere cada columna solo con sus valores. Los ceros a la izquierda, con la
#      lectura de siempre (_texto_con_ceros).
#   4. La semilla se lee columna a columna (_preparar_semilla), cada columna de la salida se arma entera
#      con las operaciones de antes (sembrar, texto_sin_nulos) y el Parquet se escribe por grupos de filas
#      (_escribir_parquet) en un fichero aparte que solo sustituye al anterior al acabar.
# La salida es la de antes: mismas filas en el mismo orden, mismas columnas, tipos y metadatos de pandas
# (tests/test_ccaa_cataluna_trozos.py la compara con la del código anterior).
FILAS_POR_LOTE = 100_000
CARPETA_TROZOS = '.{}.trozos'


def _huellas_lote(lote, columnas):
    """comun.historico._claves de un lote Arrow de texto (large_string) sobre `columnas`: la huella con la
    que acumular casa las filas de dos versiones (el texto de cada celda o '\\x00' si es nula), sin
    pasar celda a celda por Python."""
    if not columnas:
        return np.zeros(lote.num_rows, dtype=np.uint64)
    return pd.util.hash_pandas_object(pd.DataFrame(
        {j: pd.Series(pc.fill_null(lote.column(c), '\x00').to_numpy(zero_copy_only=False), dtype=object)
         for j, c in enumerate(columnas)}), index=False).to_numpy()


def _lector_texto(path, sep, filas):
    """La lectura de leer_texto con el separador `sep`, por trozos de `filas` filas."""
    return pd.read_csv(path, encoding='utf-8', encoding_errors=ERRORES_UTF8, sep=sep, low_memory=False,
                       on_bad_lines='warn', dtype=str, chunksize=filas)


def _esquema_texto(columnas):
    return pa.schema([pa.field(str(c), pa.large_string()) for c in columnas])


def _guardar_texto(trozos, destino):
    """Guarda los DataFrames de texto `trozos` en `destino` (Arrow, columnas large_string). Devuelve
    (columnas, filas de cada trozo, índice implícito: pandas usó la 1ª columna como índice porque hay
    más campos que cabeceras)."""
    columnas = escritor = None
    implicito = False
    lotes = []
    try:
        for trozo in trozos:
            if columnas is None:
                columnas = list(trozo.columns)
                implicito = not isinstance(trozo.index, pd.RangeIndex)
                esquema = _esquema_texto(columnas)
                escritor = pa.ipc.new_file(str(destino), esquema)
            lotes.append(len(trozo))
            escritor.write_table(pa.Table.from_pandas(trozo.reset_index(drop=True), schema=esquema,
                                                      preserve_index=False))
    finally:
        if escritor is not None:
            escritor.close()
        if hasattr(trozos, 'close'):
            trozos.close()
    if columnas is None:
        raise ValueError("sin cabecera")
    return columnas, lotes, implicito


def _misma_lectura(trozos, ruta, columnas, lotes, implicito):
    """Si los DataFrames de texto `trozos` (otra lectura del mismo CSV) son, fila a fila, los guardados
    en `ruta` por _guardar_texto (columnas, lotes, implicito)."""
    esquema = _esquema_texto(columnas)
    inicios = np.concatenate([[0], np.cumsum(lotes)]).astype(np.int64)
    mapa = pa.memory_map(str(ruta))
    try:
        lector = pa.ipc.open_file(mapa)
        desde = 0
        for k, trozo in enumerate(trozos):
            if list(trozo.columns) != columnas or (k == 0 and implicito == isinstance(trozo.index, pd.RangeIndex)):
                return False
            tabla = pa.Table.from_pandas(trozo.reset_index(drop=True), schema=esquema, preserve_index=False)
            hasta = desde + tabla.num_rows
            if hasta > inicios[-1]:
                return False
            k0 = int(np.searchsorted(inicios, desde, side='right')) - 1
            k1 = int(np.searchsorted(inicios, hasta, side='left'))
            guardada = pa.Table.from_batches([lector.get_batch(j) for j in range(k0, max(k1, k0))], schema=esquema)
            if not guardada.slice(desde - inicios[k0], tabla.num_rows).equals(tabla):
                return False
            desde = hasta
        return desde == inicios[-1]
    finally:
        if hasattr(trozos, 'close'):
            trozos.close()
        mapa.close()


def _trozo_comprobacion(filas_por_lote, filas):
    """Filas por trozo de la segunda lectura: una menos, si así ninguna frontera de las dos lecturas
    coincide (coinciden cada mcm = filas_por_lote · (filas_por_lote - 1) filas); si no, todas de una vez."""
    if filas_por_lote > 1 and filas_por_lote * (filas_por_lote - 1) > filas + 1:
        return filas_por_lote - 1
    return filas + 1


def leer_version(path, destino):
    """leer_texto(path) sin tenerlo entero en memoria: la misma lectura (separador, UTF-8 y CP1252, líneas
    mal formadas, índice implícito) por trozos de FILAS_POR_LOTE filas, guardada en `destino` (Arrow, todo
    texto). Devuelve (columnas, filas de cada trozo, índice implícito, separador). ValueError, como
    _leer_csv, si ningún separador da más de una columna.

    pandas no lee igual por trozos que de una vez un CSV con líneas de más campos: la primera línea de
    cada trozo no se comprueba, y una con campos de más se queda (recortada) en vez de descartarse. Por
    eso se vuelve a leer con trozos de otro tamaño (_trozo_comprobacion) y se compara con lo guardado:
    las fronteras caen en filas distintas, y la primera fila en la que una lectura se desviase de la de
    una vez la leería bien la otra. Si no coinciden, o si la lectura por trozos falla, se lee el CSV
    entero, como antes."""
    for sep in _separadores(path):
        resultado = None
        try:
            with warnings.catch_warnings(record=True) as avisos:
                warnings.simplefilter("always", pd.errors.ParserWarning)
                resultado = _guardar_texto(_lector_texto(path, sep, FILAS_POR_LOTE), destino)
            if len(resultado[0]) > 1:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", pd.errors.ParserWarning)
                    trozo = _trozo_comprobacion(FILAS_POR_LOTE, sum(resultado[1]))
                    if not _misma_lectura(_lector_texto(path, sep, trozo), destino, *resultado):
                        resultado = None
        except Exception:
            resultado = None
        if resultado is None:
            try:
                with warnings.catch_warnings(record=True) as avisos:
                    warnings.simplefilter("always", pd.errors.ParserWarning)
                    entero = pd.read_csv(path, encoding='utf-8', encoding_errors=ERRORES_UTF8, sep=sep,
                                         low_memory=False, on_bad_lines='warn', dtype=str)
            except Exception:
                continue
            if len(entero.columns) > 1:
                log(f"   ℹ️ {Path(path).name}: leído de una vez (por trozos no se lee igual)")
            resultado = _guardar_texto(
                (entero.iloc[a:a + FILAS_POR_LOTE] for a in range(0, max(len(entero), 1), FILAS_POR_LOTE)),
                destino)
            del entero
        if len(resultado[0]) > 1:
            descartadas = sum(
                str(a.message).count("Skipping line")
                for a in avisos if issubclass(a.category, pd.errors.ParserWarning)
            )
            if descartadas:
                log(f"   ⚠️ {Path(path).name}: {descartadas:,} líneas mal formadas descartadas")
            return resultado + (sep,)
    raise ValueError(f"No se pudo cargar: {path}")


class Registros:
    """Registros de las versiones de un CSV acumulados como hacía acumular_versiones(versiones,
    leer_texto) (o, con una sola versión, la lectura de siempre), sin tenerlos en memoria. Cada versión
    se guarda como texto en `carpeta` (leer_version); la tabla acumulada son segmentos (versión,
    posiciones) en el orden de sus filas, las columnas que una versión no tiene van nulas o con el valor
    que acumular les dio (rellenos), y las columnas de control son índices de versión y un booleano."""

    def __init__(self, carpeta, n_versiones):
        self.carpeta = Path(carpeta)
        self.n_versiones = n_versiones
        self.versiones = []        # dicts: ruta, origen, columnas, filas, fecha, lotes, implicito, sep
        self.segmentos = []        # [(versión, posiciones crecientes)]
        self.columnas = []         # columnas de datos, en el orden de acumular
        self.columnas_base = []    # las de la versión con la que empieza la acumulación (las demás: object)
        self.rellenos = {}         # columna nueva -> (filas, valores) de las filas anteriores que casaron
        self.primera = np.zeros(0, dtype=np.int64)
        self.ultima = np.zeros(0, dtype=np.int64)
        self.en_ultima = np.zeros(0, dtype=bool)
        self.iniciado = False      # (acumulado is not None)
        self._lectores = {}
        self._huellas = {}         # (versión, columnas) -> huellas de todas sus filas

    @property
    def filas(self):
        return len(self.en_ultima)

    def leer(self, ruta, fecha):
        i = len(self.versiones)
        destino = self.carpeta / f'version_{i}.arrow'
        columnas, lotes, implicito, sep = leer_version(ruta, destino)
        self.versiones.append(dict(ruta=destino, origen=Path(ruta), columnas=columnas, filas=int(sum(lotes)),
                                   fecha=fecha, lotes=lotes, implicito=implicito, sep=sep))
        return i

    def descartar(self, i):
        """Una versión que no se acumula: fuera del disco."""
        Path(self.versiones[i]['ruta']).unlink(missing_ok=True)

    def _lector(self, i):
        if i not in self._lectores:
            mapa = pa.memory_map(str(self.versiones[i]['ruta']))
            inicios = np.concatenate([[0], np.cumsum(self.versiones[i]['lotes'])]).astype(np.int64)
            self._lectores[i] = (mapa, pa.ipc.open_file(mapa), inicios)
        return self._lectores[i][1:]

    def cerrar(self):
        lectores, self._lectores = self._lectores, {}
        for mapa, _, _ in lectores.values():
            mapa.close()

    def _filas(self, i, posiciones, columnas):
        """Columnas `columnas` (las tiene la versión i) de sus filas `posiciones` (crecientes): tabla Arrow."""
        lector, inicios = self._lector(i)
        lotes = np.searchsorted(inicios, posiciones, side='right') - 1
        partes = []
        for k in np.unique(lotes):
            lote = pa.Table.from_batches([lector.get_batch(int(k))]).select(columnas)
            locales = posiciones[lotes == k] - inicios[k]
            if len(locales) == lote.num_rows:   # el lote entero (posiciones crecientes y sin repetir)
                partes.append(lote)
            else:
                partes.append(lote.take(pa.array(locales)))
        if not partes:
            return _esquema_texto(columnas).empty_table()
        return pa.concat_tables(partes)

    def huellas_version(self, i, columnas):
        """Huellas de todas las filas de la versión i sobre `columnas` (_huellas_lote), guardadas."""
        clave = (i, tuple(columnas))
        if clave not in self._huellas:
            lector, _ = self._lector(i)
            partes = [_huellas_lote(lector.get_batch(k), list(columnas)) for k in range(lector.num_record_batches)]
            self._huellas[clave] = np.concatenate(partes) if partes else np.zeros(0, dtype=np.uint64)
        return self._huellas[clave]

    def huellas(self, columnas):
        """_claves(acumulado, columnas): huellas de los registros acumulados, en su orden."""
        partes = []
        for k, (i, posiciones) in enumerate(self.segmentos):
            if all(c in self.versiones[i]['columnas'] for c in columnas):
                partes.append(self.huellas_version(i, columnas)[posiciones])
            else:
                partes.extend(_huellas_lote(t, columnas) for t in self._trozos_segmento(k, columnas))
        return np.concatenate(partes) if partes else np.zeros(0, dtype=np.uint64)

    def _relleno(self, c, desde, n):
        valores = np.full(n, None, dtype=object)
        if c in self.rellenos:
            filas, vals = self.rellenos[c]
            a, b = np.searchsorted(filas, [desde, desde + n])
            valores[filas[a:b] - desde] = vals[a:b]
        return pa.array(valores, type=pa.large_string())

    def _trozos_segmento(self, k, columnas):
        i, posiciones = self.segmentos[k]
        inicio = int(sum(len(p) for _, p in self.segmentos[:k]))
        propias = [c for c in columnas if c in self.versiones[i]['columnas']]
        for a in range(0, len(posiciones), FILAS_POR_LOTE):
            p = posiciones[a:a + FILAS_POR_LOTE]
            tabla = self._filas(i, p, propias)
            yield pa.table([tabla.column(c) if c in propias else self._relleno(c, inicio + a, len(p))
                            for c in columnas], schema=_esquema_texto(columnas))

    def trozos(self, columnas=None):
        """Los registros acumulados como texto (tablas Arrow), por trozos de FILAS_POR_LOTE filas como
        mucho y en su orden: las columnas `columnas` (por defecto todas), nulas en las versiones que no las
        tienen (salvo las filas que acumular rellenó)."""
        columnas = self.columnas if columnas is None else columnas
        for k in range(len(self.segmentos)):
            yield from self._trozos_segmento(k, columnas)

    def acumular(self, i, permitir_vacio=False):
        """comun.historico.acumular(acumulado, versión i, su fecha) sobre los registros."""
        v = self.versiones[i]
        n = v['filas']
        if n == 0 and not permitir_vacio:
            raise ValueError("Descarga vacía: no se marca nada como retirado")
        self.iniciado = True
        if self.filas == 0:
            self.segmentos = [(i, np.arange(n, dtype=np.int64))]
            self.columnas, self.columnas_base, self.rellenos = list(v['columnas']), list(v['columnas']), {}
            self.primera = np.full(n, i, dtype=np.int64)
            self.ultima = np.full(n, i, dtype=np.int64)
            self.en_ultima = np.ones(n, dtype=bool)
            return
        excluir = set(COLUMNAS_META) | set(IGNORAR_POR_DEFECTO)
        comunes = [c for c in v['columnas'] if c in self.columnas and c not in excluir]
        if comunes:
            # Como en acumular: multiconjunto de huellas (la k-ésima repetición casa con la k-ésima)
            k_ant = pd.Series(self.huellas(comunes))
            k_nue = pd.Series(self.huellas_version(i, comunes))
            pos_ant = pd.Series(np.arange(len(k_ant)), index=pd.MultiIndex.from_arrays(
                [k_ant.to_numpy(), k_ant.groupby(k_ant).cumcount().to_numpy()]))
            pos = pos_ant.reindex(pd.MultiIndex.from_arrays(
                [k_nue.to_numpy(), k_nue.groupby(k_nue).cumcount().to_numpy()]))
            casada = pos.notna().to_numpy()
            i_ant = pos.to_numpy()[casada].astype("int64")
        else:
            casada = np.zeros(n, dtype=bool)
            i_ant = np.zeros(0, dtype="int64")
        self.en_ultima[:] = False
        self.ultima[i_ant] = i
        self.en_ultima[i_ant] = True
        nuevas = [c for c in v['columnas'] if c not in self.columnas]
        if nuevas:
            # acumular da a las filas anteriores que casan el valor de la versión nueva
            orden = np.argsort(i_ant, kind='stable')
            valores = self._filas(i, np.flatnonzero(casada), nuevas)
            for c in nuevas:
                self.rellenos[c] = (i_ant[orden], valores.column(c).to_numpy(zero_copy_only=False)[orden])
            self.columnas.extend(nuevas)
        altas = np.flatnonzero(~casada)
        self.segmentos.append((i, altas))
        self.primera = np.concatenate([self.primera, np.full(len(altas), i, dtype=np.int64)])
        self.ultima = np.concatenate([self.ultima, np.full(len(altas), i, dtype=np.int64)])
        self.en_ultima = np.concatenate([self.en_ultima, np.ones(len(altas), dtype=bool)])

    def meta(self):
        """Las columnas de control como las añadía acumular (una versión) o tipos_como_csv (varias)."""
        m = pd.DataFrame(index=pd.RangeIndex(self.filas))
        if self.n_versiones == 1:
            fecha = self.versiones[self.segmentos[0][0]]['fecha']
            m["_primera_descarga"] = fecha
            m["_ultima_descarga"] = fecha
            m["_en_ultima_descarga"] = True
        else:
            fechas = np.array([v['fecha'] for v in self.versiones], dtype=object)
            m["_primera_descarga"] = fechas[self.primera]
            m["_ultima_descarga"] = fechas[self.ultima]
            m["_en_ultima_descarga"] = self.en_ultima.copy()
        return m


def construir_registros(csv_path, carpeta):
    """Registros de todas las versiones del CSV acumulados (comun.historico), en disco (Registros): lo
    que daba acumular_versiones(versiones, leer_texto) o, con una sola versión, la lectura de siempre."""
    vers = versiones_csv(csv_path)
    reg = Registros(carpeta, len(vers))
    if len(vers) == 1:
        reg.acumular(reg.leer(*vers[0]), permitir_vacio=True)
        return reg
    cabecera = None
    for ruta, fecha in vers:
        i = reg.leer(ruta, fecha)
        columnas = reg.versiones[i]['columnas']
        if reg.versiones[i]['filas'] == 0 and reg.iniciado:
            log(f"   ⚠️ Versión vacía ignorada (no se marca nada como retirado): {ruta.name}")
            reg.descartar(i)
            continue
        faltan = [c for c in cabecera if c not in columnas] if cabecera is not None else []
        if faltan:
            nuevas = [c for c in columnas if c not in cabecera]
            aviso = (f"{ruta.parent.name}/{ruta.name}: la cabecera cambia respecto a la versión anterior "
                     f"(faltan {faltan}; nuevas {nuevas}); no se acumula: no se retira ni se duplica nada")
            log(f"   ⚠️ REVISAR {aviso}")
            REVISAR.append(aviso)
            reg.descartar(i)
            continue
        reg.acumular(i, permitir_vacio=not reg.iniciado)
        cabecera = list(columnas)
    return reg


def _tipar(reg, tmp_csv):
    """Las columnas de datos con los tipos de la lectura de siempre: con una sola versión, la del CSV
    (load_csv); con varias, la del texto acumulado escrito en tmp_csv (tipos_como_csv), o texto si esa
    lectura no devuelve las mismas columnas y filas. Cada columna se infiere entera, como en la lectura de
    una vez, con un CSV estrecho por columna (la columna y otra fija, para que una celda vacía no sea una
    línea en blanco) y la misma lectura. Devuelve {columna: (pickle de la serie, dtype, si tiene algún
    valor)}; las versiones guardadas ya no hacen falta y se borran."""
    columnas = list(reg.columnas)
    una = reg.n_versiones == 1
    estrechos = [reg.carpeta / f'columna_{j}.csv' for j in range(len(columnas))]
    # Los CSV estrechos los escribe Arrow (mucho más rápido que to_csv): entrecomilla todos los textos,
    # y pandas lee igual un valor con comillas que sin ellas (mismos valores, mismos tipos)
    esquema_estrecho = pa.schema([pa.field('_', pa.int8()), pa.field('v', pa.large_string())])
    escritores, tmp, texto = [], None, False
    try:
        try:
            for p in estrechos:
                escritores.append(pacsv.CSVWriter(str(p), esquema_estrecho,
                                                  write_options=pacsv.WriteOptions(quoting_style='needed')))
            if not una:
                tmp = open(tmp_csv, 'w', encoding='utf-8', newline='')
            primero = True
            for trozo in reg.trozos():
                if tmp is not None:
                    trozo.to_pandas().to_csv(tmp, header=primero, index=False)
                cero = pa.array(np.zeros(trozo.num_rows, dtype=np.int8))
                for j, c in enumerate(columnas):
                    escritores[j].write_table(pa.table([cero, trozo.column(c)], schema=esquema_estrecho))
                primero = False
            if primero and tmp is not None:   # sin filas: solo la cabecera
                pd.DataFrame(columns=columnas).to_csv(tmp, index=False)
        finally:
            for e in escritores:
                e.close()
            if tmp is not None:
                tmp.close()
        if una:
            v = reg.versiones[reg.segmentos[0][0]]
            fuente, sep, restaurar = v['origen'], v['sep'], not v['implicito']
        else:
            guardado = reg.carpeta / 'tmp.arrow'
            columnas_tmp, lotes_tmp, _, sep = leer_version(tmp_csv, guardado)
            guardado.unlink()
            fuente, restaurar = tmp_csv, True
            if list(columnas_tmp) != columnas or sum(lotes_tmp) != reg.filas:
                log("   ⚠️ No se pudieron inferir los tipos: se guarda como texto")
                texto = True
        salida, numericas = {}, []
        for j, c in enumerate(columnas):
            if texto:
                # tipos_como_csv deja el texto acumulado tal cual: str (object en las columnas que acumular
                # añadió después)
                partes = [t.column(c).to_pandas() for t in reg.trozos([c])]
                serie = pd.concat(partes, ignore_index=True) if partes else pd.Series([], dtype=object)
                if c not in reg.columnas_base:
                    serie = serie.astype(object)
            else:
                serie = pd.read_csv(estrechos[j], encoding='utf-8', encoding_errors=ERRORES_UTF8, sep=',',
                                    low_memory=False, on_bad_lines='warn').iloc[:, 1]
                if pd.api.types.is_numeric_dtype(serie.dtype) and not pd.api.types.is_bool_dtype(serie.dtype):
                    numericas.append(j)
            estrechos[j].unlink()
            ruta = reg.carpeta / f'tipada_{j}.pkl'
            serie = serie.rename(c)
            serie.to_pickle(ruta)
            salida[c] = (ruta, serie.dtype, bool(serie.notna().any()))
            del serie
        if not texto and restaurar:
            con_ceros = _texto_con_ceros(fuente, 'utf-8', sep, len(columnas), numericas, reg.filas)
            for j, valores in con_ceros.items():
                c = columnas[j]
                serie = pd.Series(valores, name=c)
                serie.to_pickle(salida[c][0])
                salida[c] = (salida[c][0], serie.dtype, bool(serie.notna().any()))
            if con_ceros:
                log(f"   🔢 Guardadas como texto (ceros a la izquierda): {', '.join(columnas[j] for j in con_ceros)}")
        return salida
    finally:
        Path(tmp_csv).unlink(missing_ok=True)
        for p in estrechos:
            p.unlink(missing_ok=True)
        reg.cerrar()
        for v in reg.versiones:
            Path(v['ruta']).unlink(missing_ok=True)


def _clave_por_trozos(clave, df):
    """clave(df) por trozos de FILAS_POR_LOTE filas: las claves (clave_texto, uuid_publicacio) son fila a
    fila y, de una vez, copian la tabla de la clave varias veces."""
    if len(df) <= FILAS_POR_LOTE:
        return clave(df)
    return pd.concat([clave(df.iloc[a:a + FILAS_POR_LOTE]) for a in range(0, len(df), FILAS_POR_LOTE)])


def _preparar_semilla(semilla, dtypes, columna, hay_datos):
    """sembrar_release(df, *semilla) sin cargar df ni el Parquet publicado: la clave de la descarga sale de
    sus columnas de la clave, la del publicado de las suyas, y cada columna del publicado se lee y se
    armoniza (comun.historico._armonizar) por separado. Devuelve (filas del publicado que se añaden, ya
    con _origen y _en_ultima_descarga, y las columnas de la salida en orden) o None si no se siembra (no
    existe o le faltan columnas de la clave), con los avisos de siempre.

    dtypes: {columna de df: dtype}; columna(c): la columna c de df entera; hay_datos(c):
    si la columna c tiene algún valor en las filas descargadas (sembrar)."""
    ruta, columnas_clave = semilla
    columnas_clave, funcion = (columnas_clave, None) if isinstance(columnas_clave, list) else columnas_clave
    clave = globals()[funcion] if funcion else (lambda d: clave_texto(d, columnas_clave))
    ruta = Path(ruta)
    if not ruta.exists():
        log(f"   ⚠️ Sin semilla: no existe {ruta}")
        return None
    publicadas = list(pq.read_schema(ruta).empty_table().to_pandas().columns)
    faltan = [c for c in columnas_clave if c not in dtypes or c not in publicadas]
    if faltan:
        log(f"   ⚠️ Semilla {ruta.name} sin sembrar: faltan columnas de la clave ({', '.join(faltan)})")
        return None

    nombre = '_clave_semilla'
    clave_descarga = _clave_por_trozos(clave, pd.DataFrame({c: columna(c) for c in columnas_clave}))
    clave_publicado = _clave_por_trozos(clave, pd.read_parquet(ruta, columns=columnas_clave))
    nuevos_clave = clave_descarga.reset_index(drop=True).to_frame(nombre)
    semilla_clave = clave_publicado.reset_index(drop=True).to_frame(nombre)
    # sembrar arma la semilla con los tipos de la descarga (_armonizar solo mira sus tipos)
    tipos_nuevos = dict(dtypes, **{nombre: clave_descarga.dtype})
    modelo = pd.DataFrame({c: pd.Series([], dtype=d) for c, d in tipos_nuevos.items()})
    armonizadas = {}

    def publicada(c):
        if c == nombre:
            return semilla_clave[nombre]
        if c not in armonizadas:
            serie = pd.read_parquet(ruta, columns=[c]).reset_index(drop=True)
            armonizadas.clear()   # una columna cada vez
            armonizadas[c] = _armonizar(serie, modelo)[c]
        return armonizadas[c]

    excluir = set(COLUMNAS_META) | set(COLUMNAS_SEMILLA) | {nombre}
    con_clave = publicadas + [nombre]
    contenido = [c for c in con_clave if c in tipos_nuevos and c not in excluir and hay_datos(c)]

    motivo = seleccionar_semilla(
        nuevos_clave, semilla_clave,
        lambda filas: pd.DataFrame({c: columna(c).loc[filas] for c in contenido}, index=pd.Index(filas)),
        lambda filas: pd.DataFrame({c: publicada(c).loc[filas] for c in contenido}, index=pd.Index(filas)),
        None)
    elegidas = motivo == ANADIDA
    anadidas = pd.DataFrame({c: publicada(c).loc[elegidas] for c in con_clave})
    armonizadas.clear()
    propio = anadidas["_origen"] if "_origen" in anadidas.columns else pd.Series(None, index=anadidas.index)
    anadidas["_origen"] = propio.astype(object).where(propio.notna(), ORIGEN_SEMILLA)
    anadidas["_en_ultima_descarga"] = False

    salida = list(tipos_nuevos)
    for c in ("_origen", "_en_ultima_descarga"):
        if c not in salida:
            salida.append(c)
    columnas = [c for c in salida + [c for c in anadidas.columns if c not in salida] if c != nombre]

    informe = informe_semilla(motivo, ORIGEN_SEMILLA, semilla_clave)
    informe['ruta'] = str(ruta)
    imprimir_informe_semilla(informe)
    return anadidas.drop(columns=nombre), columnas


def _columna_salida(c, descarga, anadidas, otra):
    """La columna c de la salida, entera, con las operaciones de antes: sembrar (pd.concat de la descarga
    y las filas añadidas, _en_ultima_descarga como bool) y texto_sin_nulos. descarga: la columna de df
    (None si solo la tiene la semilla); anadidas: las filas añadidas de la semilla (None sin semilla);
    otra: otra columna de df (_en_ultima_descarga).

    Al lado que no tiene la columna se le deja otra, como en el concat de las tablas enteras: pandas
    trata distinto una tabla sin columnas (sin filas, ni la mira) que una a la que le falta esa (cuenta
    como nulos, y un bool o un entero cambian de tipo aunque no tenga filas)."""
    if anadidas is not None:
        izq = descarga.to_frame(c) if descarga is not None else otra.to_frame("_en_ultima_descarga")
        der = anadidas[[c]] if c in anadidas.columns else anadidas[["_en_ultima_descarga"]]
        serie = pd.concat([izq, der], ignore_index=True, sort=False)[c]
        if c == "_en_ultima_descarga":
            serie = serie.astype(bool)
    else:
        serie = descarga
    if es_texto(serie) and c not in COLUMNAS_CONTROL:
        serie = texto_sin_nulos(serie)
    return serie


def _escribir_parquet(columnas, carpeta, destino):
    """Escribe las columnas [(ruta Arrow de una columna, metadatos de pandas de esa columna)] como un
    Parquet por grupos de FILAS_POR_LOTE filas, con el esquema y los metadatos de pandas que daba
    df.to_parquet(index=False, compression='snappy'), en un fichero aparte que sustituye a `destino` al
    acabar (si la ejecución muere a medias, el Parquet anterior sigue entero). Devuelve las filas."""
    mapas, tablas, campos, metadatos = [], [], [], None
    try:
        for ruta, meta in columnas:
            mapas.append(pa.memory_map(str(ruta)))
            tablas.append(pa.ipc.open_file(mapas[-1]).read_all())
            campos.append(tablas[-1].schema.field(0))
            meta = json.loads(meta)
            if metadatos is None:
                metadatos = meta
            else:
                metadatos['columns'].extend(meta['columns'])
        esquema = pa.schema(campos, metadata={b'pandas': json.dumps(metadatos).encode('utf8')})
        filas = tablas[0].num_rows if tablas else 0
        parcial = carpeta / f'{destino.name}.parcial'
        with pq.ParquetWriter(str(parcial), esquema, compression='snappy') as escritor:
            if filas == 0:
                escritor.write_table(esquema.empty_table())
            for a in range(0, filas, FILAS_POR_LOTE):
                escritor.write_table(pa.Table.from_arrays(
                    [t.column(0).slice(a, FILAS_POR_LOTE) for t in tablas], schema=esquema))
        os.replace(parcial, destino)
        return filas
    finally:
        del tablas
        for mapa in mapas:
            mapa.close()


def convert_to_parquet(input_path, output_path, descripcion, semilla=None):
    """Convierte un CSV a Parquet, con todas sus versiones y, si se da `semilla` (ruta del Parquet
    publicado y columnas de la clave), las filas que el publicado tiene y la descarga ya no. Por trozos
    (ver CONVERSIÓN POR TROZOS), con la salida que daba hacerlo todo en memoria."""
    log(f"\n📄 {descripcion}")
    log(f"   Input: {input_path.name}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    carpeta = output_path.with_name(CARPETA_TROZOS.format(output_path.name))
    shutil.rmtree(carpeta, ignore_errors=True)   # de una ejecución que murió a medias
    carpeta.mkdir()
    reg = None
    try:
        # Cargar (todas las versiones del CSV) y tipos
        reg = construir_registros(input_path, carpeta)
        tipadas = _tipar(reg, output_path.with_name(output_path.name + '.csv.tmp'))
        meta = reg.meta()
        n = reg.filas
        log(f"   📝 {n:,} registros, {len(tipadas) + len(meta.columns)} columnas")
        if reg.n_versiones > 1:
            retirados = int((~meta['_en_ultima_descarga'].astype(bool)).sum())
            log(f"   📜 {reg.n_versiones} versiones del CSV; {retirados:,} registros ya no servidos (conservados)")

        dtypes = {c: d for c, (_, d, _) in tipadas.items()}
        dtypes.update(meta.dtypes.to_dict())

        def columna(c):
            return meta[c] if c in meta.columns else pd.read_pickle(tipadas[c][0])

        def hay_datos(c):
            if "_origen" not in dtypes:
                return tipadas[c][2] if c in tipadas else bool(meta[c].notna().any())
            return bool(columna(c)[columna("_origen").isna().to_numpy()].notna().any())

        anadidas, columnas = None, list(dtypes)
        if semilla is not None:
            sembrada = _preparar_semilla(semilla, dtypes, columna, hay_datos)
            if sembrada is not None:
                anadidas, columnas = sembrada

        # Cada columna de la salida, entera y ya convertida a Arrow, a su fichero
        salida = []
        for j, c in enumerate(columnas):
            descarga = columna(c) if c in dtypes else (
                pd.Series([None] * n, dtype=object) if c == "_origen" else None)
            serie = _columna_salida(c, descarga, anadidas, meta["_en_ultima_descarga"])
            del descarga
            tabla = pa.Table.from_pandas(serie.to_frame(c), preserve_index=False)
            del serie
            ruta = carpeta / f'salida_{j}.arrow'
            with pa.ipc.new_file(str(ruta), tabla.schema) as escritor:
                escritor.write_table(tabla)
            salida.append((ruta, tabla.schema.metadata[b'pandas']))
            del tabla
            if c in tipadas:
                Path(tipadas[c][0]).unlink()
        total = _escribir_parquet(salida, carpeta, output_path)
    finally:
        if reg is not None:
            reg.cerrar()
        shutil.rmtree(carpeta, ignore_errors=True)

    size_csv = input_path.stat().st_size / 1024 / 1024
    size_parquet = output_path.stat().st_size / 1024 / 1024
    ratio = (1 - size_parquet / size_csv) * 100 if size_csv > 0 else 0

    log(f"   💾 {output_path.name}: {size_parquet:.1f}MB (↓{ratio:.0f}% de {size_csv:.1f}MB)")

    return total, size_parquet


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


def registros_bcn(archivo, tmp_csv):
    """Registros de todas las versiones de un recurso de Open Data BCN (ccaa_cataluna.py guarda la
    anterior en _historico/ con guardar_version), de la más antigua a la vigente, acumulados con
    comun.historico.acumular: lo que el portal retira o cambia sigue con _en_ultima_descarga=False.
    El ámbito es el recurso: cada versión es el fichero entero. Con una sola versión, la lectura de
    siempre más las columnas meta. Con varias, los CSV se comparan como texto (como construir_registros)
    y los Excel/JSON con su lectura. Una versión vacía o ilegible no retira nada: se avisa y se salta
    (antes, un recurso que no se podía leer desaparecía entero del parquet). Devuelve (DataFrame con
    COLUMNAS_META, nº de versiones); ValueError si no se puede leer ninguna versión."""
    vers = versiones_csv(archivo)
    if len(vers) == 1:
        return acumular(None, cargar_tabla(archivo), vers[0][1], permitir_vacio=True), 1
    es_csv = archivo.suffix.lower() == '.csv'
    acumulado = acumular_versiones(vers, leer_texto if es_csv else cargar_tabla, saltar_ilegibles=True)
    if acumulado is None:
        raise ValueError(f"ninguna de sus {len(vers)} versiones se puede leer")
    return (tipos_como_csv(acumulado, tmp_csv) if es_csv else acumulado), len(vers)


def consolidar_bcn(input_dir, output_dir, carpeta, destino, titulo, origen='_año'):
    """Consolida todos los recursos de un dataset de Open Data BCN en un parquet.

    origen='_año': año sacado del nombre del archivo; '_archivo_origen': nombre del archivo.
    Cada recurso con todas sus versiones (registros_bcn); columnas COLUMNAS_META al final y, si hay
    semilla (SEMILLA y SEMILLAS_BCN), las filas del release que la descarga ya no trae (_origen).
    """
    log("\n" + "="*60)
    log(f"📦 CONSOLIDANDO: {titulo}")

    dir_path = input_dir / '02_barcelona' / carpeta
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0

    output_path = output_dir / 'contratacion' / destino
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dfs = []
    for archivo in archivos_bcn(dir_path):
        try:
            df, n_versiones = registros_bcn(archivo, output_path.with_name(output_path.name + '.csv.tmp'))
            if origen == '_archivo_origen':
                df['_archivo_origen'] = archivo.name
            else:
                df['_año'] = anio_de_nombre(archivo.stem)
            dfs.append(df)
            detalle = ""
            if n_versiones > 1:
                retirados = int((~df['_en_ultima_descarga'].astype(bool)).sum())
                detalle = f" ({n_versiones} versiones; {retirados:,} ya no servidos, conservados)"
            log(f"   ✅ {archivo.name}: {len(df):,} registros{detalle}")
        except Exception as e:
            log(f"   ❌ {archivo.name}: {e}")

    if not dfs:
        return 0, 0

    df_all = pd.concat(dfs, ignore_index=True)
    meta = [c for c in COLUMNAS_META if c in df_all.columns]
    df_all = df_all[[c for c in df_all.columns if c not in meta] + meta]
    if SEMILLA is not None and destino in SEMILLAS_BCN:
        df_all = sembrar_release(df_all, Path(SEMILLA) / 'contratacion' / destino, SEMILLAS_BCN[destino],
                                 solo_con_clave=True)

    # Convertir columnas object a string para evitar errores de tipos mixtos (las nuestras no: sus
    # nulos son nulos)
    for col in df_all.columns:
        if es_texto(df_all[col]) and col not in COLUMNAS_CONTROL:
            df_all[col] = texto_sin_nulos(df_all[col])

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

def _lista_categorias(texto, disponibles):
    """'contratacion,convenios' → {'contratacion', 'convenios'}; error si alguna no existe."""
    pedidas = {c.strip() for c in texto.split(',') if c.strip()}
    malas = sorted(pedidas - set(disponibles))
    if malas or not pedidas:
        raise argparse.ArgumentTypeError(
            f"categorías desconocidas: {', '.join(malas) or '(ninguna)'}; hay: {', '.join(sorted(disponibles))}")
    return pedidas


def argumentos(argv):
    parser = argparse.ArgumentParser(description="Convierte a Parquet los CSV descargados de Catalunya")
    parser.add_argument("--entrada", default=None,
                        help=f"carpeta de los CSV (por defecto {INPUT_DIR}, relativa al directorio actual)")
    parser.add_argument("--salida", default=None,
                        help=f"carpeta de los Parquet (por defecto {OUTPUT_DIR}, relativa al directorio actual)")
    parser.add_argument("--categorias", default=None,
                        type=lambda s: _lista_categorias(s, CATEGORIAS_DISPONIBLES),
                        help=f"solo estas categorías, separadas por comas ({', '.join(CATEGORIAS_DISPONIBLES)}); "
                             "Barcelona va con 'contratacion'. Por defecto, todas")
    parser.add_argument("--semilla", default=None,
                        help="carpeta de Catalunya del release v2026.02 (p.ej. .../extraido/catalunya): añade las filas "
                             "del publicado que ya no están en la descarga (ver SEMILLAS y SEMILLAS_BCN)")
    return parser.parse_args(list(argv))


def main(argv=()):
    global INPUT_DIR, OUTPUT_DIR, CATEGORIAS, SEMILLA
    args = argumentos(argv)
    CATEGORIAS = args.categorias
    if args.semilla is not None and not Path(args.semilla).is_dir():
        log(f"❌ No existe la carpeta de la semilla: {args.semilla}")
        return 1
    SEMILLA = args.semilla   # también la usan las consolidaciones de Barcelona (SEMILLAS_BCN)
    REVISAR.clear()
    if args.entrada is not None:
        INPUT_DIR = str(args.entrada)
    if args.salida is not None:
        OUTPUT_DIR = str(args.salida)
    start = datetime.now()
    
    print("\n" + "="*70)
    print("📦 CATALUNYA CSV → PARQUET")
    print("="*70)
    
    input_dir = Path(INPUT_DIR)
    output_dir = Path(OUTPUT_DIR)
    
    if not input_dir.exists():
        log(f"❌ No encontrado: {input_dir}")
        return 1
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
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
        if CATEGORIAS is not None and categoria_csv(csv_rel) not in CATEGORIAS:
            continue
        csv_path = input_dir / csv_rel
        
        if not csv_path.exists():
            log(f"\n⚠️ No encontrado: {csv_rel}")
            continue
        
        parquet_path = output_dir / parquet_rel
        
        try:
            semilla = None
            if args.semilla is not None and parquet_rel in SEMILLAS:
                semilla = (Path(args.semilla) / parquet_rel, SEMILLAS[parquet_rel])
            n_records, size_mb = convert_to_parquet(csv_path, parquet_path, descripcion, semilla=semilla)
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
    bcn = CATEGORIAS is None or 'contratacion' in CATEGORIAS
    if not bcn:
        log("   (fuera de las categorías pedidas)")
    
    n, s = consolidate_barcelona_menores(input_dir, output_dir) if bcn else (0, 0)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_contratistas(input_dir, output_dir) if bcn else (0, 0)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_perfil(input_dir, output_dir) if bcn else (0, 0)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_modificaciones(input_dir, output_dir) if bcn else (0, 0)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_resumen(input_dir, output_dir) if bcn else (0, 0)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_autorizacion(input_dir, output_dir) if bcn else (0, 0)
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
    if REVISAR:
        log(f"⚠️ Casos a revisar: {len(REVISAR)} (versiones que no se han acumulado)")
        for aviso in REVISAR:
            log(f"   - {aviso}")
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
    return 1 if stats['errores'] or REVISAR else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))