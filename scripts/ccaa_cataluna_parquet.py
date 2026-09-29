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
================================================================================
"""

import argparse
import codecs
import encodings.cp1252
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
from comun.historico import (  # noqa: E402
    COLUMNAS_META, acumular, imprimir_informe_semilla, sembrar, versiones,
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

# Columnas nuestras: sus nulos no pasan a '' (en las filas sembradas no hay fechas de descarga, y
# _origen es nulo en las descargadas)
COLUMNAS_CONTROL = set(COLUMNAS_META) | {'_origen'}


# =============================================================================
# FUNCIONES
# =============================================================================

# CP1252 sin bytes que fallen: los 5 que CP1252 deja sin asignar (0x81, 0x8D, 0x8F, 0x90 y 0x9D) dan
# el carácter de control C1 del mismo valor, como en latin-1 y en el 'windows-1252' de los navegadores
# (WHATWG). Cada byte es un carácter y se puede volver al byte original: no se pierde nada. Los CSV
# de Open Data Barcelona que no son UTF-8 (contratistas 2012-2013, menores 2015 y modificaciones
# 2013-2014) están en CP1252: leídos como latin-1, el '€' (0x80) llegaba como chr(128) y la '’'
# (0x92) como chr(146) en 37.721 celdas (medido el 29-sep-2026: 36.728 '€' y 1.758 comillas, rayas,
# viñetas y puntos suspensivos; ninguno de esos ficheros trae los 5 bytes sin asignar).
CP1252 = 'cp1252_c1'
_TABLA_CP1252 = ''.join(chr(i) if c == '\ufffe' else c for i, c in enumerate(encodings.cp1252.decoding_table))
_TABLA_CP1252_CODIF = codecs.charmap_build(_TABLA_CP1252)


class _CodecCP1252(codecs.Codec):
    def encode(self, texto, errors='strict'):
        return codecs.charmap_encode(texto, errors, _TABLA_CP1252_CODIF)

    def decode(self, datos, errors='strict'):
        return codecs.charmap_decode(datos, errors, _TABLA_CP1252)


class _CodificadorCP1252(codecs.IncrementalEncoder):
    def encode(self, texto, final=False):
        return codecs.charmap_encode(texto, self.errors, _TABLA_CP1252_CODIF)[0]


class _DecodificadorCP1252(codecs.IncrementalDecoder):
    def decode(self, datos, final=False):
        return codecs.charmap_decode(datos, self.errors, _TABLA_CP1252)[0]


class _EscritorCP1252(_CodecCP1252, codecs.StreamWriter):
    pass


class _LectorCP1252(_CodecCP1252, codecs.StreamReader):
    pass


def _buscar_codec(nombre):
    if nombre.replace('-', '_').lower() != CP1252:
        return None
    return codecs.CodecInfo(name=CP1252, encode=_CodecCP1252().encode, decode=_CodecCP1252().decode,
                            incrementalencoder=_CodificadorCP1252, incrementaldecoder=_DecodificadorCP1252,
                            streamreader=_LectorCP1252, streamwriter=_EscritorCP1252)


codecs.register(_buscar_codec)

# Codificaciones que se prueban, por orden. Antes: utf-8, latin-1 y cp1252; latin-1 no falla nunca,
# así que cp1252 no se llegaba a probar
CODIFICACIONES = ['utf-8', CP1252]


def load_csv(path):
    """Carga CSV con detección de encoding y separador"""
    df, enc, sep = _leer_csv(path)
    return restaurar_ceros_iniciales(df, path, enc, sep)


def leer_texto(path):
    """Todas las celdas como texto, con la misma detección que load_csv (para
    comparar versiones tal como las sirvió el portal: un 1 y un 1.0 no casarían)."""
    return _leer_csv(path, dtype=str)[0]


def _leer_csv(path, **kwargs):
    """(DataFrame, encoding, separador) del primer par que da más de una columna. UTF-8 y, si el
    fichero no lo es, CP1252 (ver CP1252)."""
    encodings = list(CODIFICACIONES)
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
    return tipos_como_csv(acumular_versiones(vers, leer_texto), tmp_csv), len(vers)


def acumular_versiones(vers, leer, saltar_ilegibles=False):
    """Registros de las versiones [(ruta, fecha)], de la más antigua a la vigente, leídas con `leer`
    y acumuladas con comun.historico.acumular. Una versión vacía (salvo la primera) no retira nada:
    se ignora. Con saltar_ilegibles, una versión que no se puede leer tampoco (se avisa); sin él, el
    error sube. None si no se ha podido leer ninguna."""
    acumulado = None
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
        acumulado = acumular(acumulado, texto, fecha, permitir_vacio=acumulado is None)
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


def convert_to_parquet(input_path, output_path, descripcion, semilla=None):
    """Convierte un CSV a Parquet, con todas sus versiones y, si se da `semilla` (ruta del Parquet
    publicado y columnas de la clave), las filas que el publicado tiene y la descarga ya no"""
    log(f"\n📄 {descripcion}")
    log(f"   Input: {input_path.name}")
    
    # Cargar (todas las versiones del CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df, n_versiones = construir_registros(input_path, output_path.with_name(output_path.name + '.csv.tmp'))
    log(f"   📝 {len(df):,} registros, {len(df.columns)} columnas")
    if n_versiones > 1:
        retirados = int((~df['_en_ultima_descarga'].astype(bool)).sum())
        log(f"   📜 {n_versiones} versiones del CSV; {retirados:,} registros ya no servidos (conservados)")
    if semilla is not None:
        df = sembrar_release(df, *semilla)
    
    # Optimizar tipos de datos
    for col in df.columns:
        # Convertir object a string para evitar errores de tipos mixtos
        if es_texto(df[col]) and col not in COLUMNAS_CONTROL:
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
    return 1 if stats['errores'] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))