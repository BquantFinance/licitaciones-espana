"""
=============================================================================
DESCARGA Y UNIFICACIÓN DE ACTIVIDAD CONTRACTUAL COMPLETA
AYUNTAMIENTO DE MADRID
=============================================================================
Fuente: portal de datos abiertos del Ayuntamiento (https://datos.madrid.es),
un CKAN con dos conjuntos de datos:
  - 300253-0-contratos-actividad-menores: contratos menores, un fichero por
    año desde 2015 (2021 partido en "hasta febrero" y "desde marzo").
  - 216876-0-contratos-actividad: registro de contratos, un fichero por
    categoría y año desde 2015 (en 2021 algunos partidos en "formalizados en
    2020" / "formalizados en 2021").
Cada fichero se publica en CSV y en XLSX (XLS hasta 2017), más un PDF con la
estructura de cada época. El release v2026.02 no tiene copia del Ayuntamiento
(su ZIP es una copia del de la Comunidad): por eso no hay --semilla.

CATEGORÍAS (según la descripción de cada recurso en CKAN):
  1. contratos_menores        → Contratos menores
  2. contratos_formalizados   → Contratos inscritos en Registro
  3. acuerdo_marco            → Basados en acuerdo marco / sist. dinámico
  4. modificados              → Contratos modificados
  5. prorrogados              → Contratos prorrogados
  6. penalidades              → Penalidades en contratos
  7. cesiones                 → Cesiones de contratos
  8. resoluciones             → Resoluciones de contratos
  9. homologacion             → Derivados de procedimientos de homologación
  (documentacion: los PDF "Contenido y estructura del fichero"; solo se descargan)

DESCUBRIMIENTO Y DESCARGA (capa cruda)
- package_show de cada conjunto lista todos sus recursos; categoría, año y
  parte salen de la descripción ('Contratos menores 2021 (hasta febrero)',
  'Contratos modificados inscritos en el Registro de Contratos. 2021.
  Formalizados en 2020'). Si la API no responde no se descarga ni se retira
  nada, y sin ninguna copia descargada no se escribe ninguna tabla.
- Se descargan TODOS los recursos (CSV, XLSX, XLS y PDF) en
  <salida>/originales/<menores|actividad>/<categoría>_<año>[_<parte>]__<id>.<ext>
  (el id del recurso CKAN hace el nombre único). Si el portal reutiliza un id
  para otro fichero (otra categoría, año o parte: en datos.madrid.es el id
  sale de la posición del recurso), es un fichero nuevo con su propio nombre
  y el anterior queda como retirado: sus versiones nunca se mezclan.
- Todo lo listado se anota en el manifiesto antes de descargar nada, y el
  manifiesto se guarda tras cada descarga: una ejecución cortada no deja
  ficheros descargados sin anotar ni ficheros listados sin constancia.
- Nunca se machaca: guardar_version deja la versión anterior en _historico/
  si el contenido cambió. No sustituye nada una descarga fallida, vacía,
  cortada, que no es del formato esperado (p.ej. la página HTML de error del
  portal), que no es una tabla (una sola columna: p.ej. un texto de error) o,
  si ya había copia, un CSV/XLSX sin ningún registro con datos, sin cabecera
  o con una cabecera sin ninguna columna en común con la anterior ni columnas
  conocidas.
- Re-ejecuciones: un recurso se vuelve a pedir si no se tiene, si alguno de
  sus metadatos CKAN (url, hash, size, last_modified, metadata_modified,
  modified) cambió desde la última descarga, si su copia no tiene registros
  con datos o no tiene el tamaño ('size') que indica CKAN, si es del año en
  curso o del anterior (o no tiene año), o con --comprobar-todo. El resto no
  se pide. El hash de CKAN no basta para saltarse una descarga: en septiembre
  de 2026 no coincidía con el contenido servido en 10 de los 169 recursos (el
  tamaño sí, en los 169).
- originales/_manifiesto.json, por fichero: id, conjunto, descripción,
  categoría, año, parte, formato, url, metadatos CKAN (created, issued,
  last_modified, metadata_modified, size, hash...), sha256, md5 y bytes de la
  copia, registros con datos, fecha de descarga (la de modificación del
  fichero, que es también la de su sello en _historico/) y de la última
  comprobación, estado ('publicado' o 'retirado': el portal ya no lo lista) y
  versiones descargadas (con su fecha, sha256, descripción y clasificación).
  Si falta, se recupera el último de _historico/ y, si no hay, se deduce cada
  fichero de su nombre.

LECTURA SIN PÉRDIDAS
- Texto tal cual: utf-8 (con o sin BOM; un byte suelto de otra codificación
  se lee como cp1252 sin cambiar el resto), CP850 (ficheros antiguos de
  MS-DOS) o cp1252; los bytes sin carácter en cp1252 se conservan (como
  latin-1) sin pasar el fichero entero a latin-1.
- Lector principal: pandas con dtype=str y keep_default_na=False, sin saltar
  líneas. El módulo csv de Python lee cada fichero como referencia
  independiente y los dos deben coincidir registro a registro (si no, se usa
  el del módulo csv y se anota en el informe). Ningún registro se descarta:
  los campos de más van a columnas 'Unnamed: N' y los títulos y filas vacías
  sobre la cabecera a informes/lineas_fuera_de_tabla.csv. Unas comillas mal
  formadas (p.ej. sin cerrar: juntan en un campo el resto del fichero) son un
  fallo.
- La cabecera se busca entre las primeras filas (hay títulos como 'CONTRATOS
  BASADOS (A.M.) 2023') con la detección de estructuras de siempre (A-F,
  AC_OLD, AC_OLD_MOD, AC_NEW, AC_2025, homologación, SIN_CABECERA).
- XLSX con openpyxl y XLS con xlrd (pip install xlrd), celdas como texto.

SALIDAS (en <salida>, por defecto ./datos_madrid_contratacion_completa)
- actividad_contractual_madrid_original.parquet (tabla fiel): todas las filas
  de los ficheros consolidados, con todas sus columnas, nombres de columna y
  texto originales (celda vacía = ''; nulo = la columna no existe en ese
  fichero), más _dataset, _recurso, _descripcion, _categoria, _anio_fichero,
  _parte, _formato, _estructura, _archivo_origen, _fila_origen (nº de registro
  en el fichero, contando la cabecera), _encabezado (títulos sobre la
  cabecera), _fecha_descarga, _duplicado (idéntica a otra fila anterior del
  mismo recurso), _repetido_en_csv y las 3 columnas de comun.historico.
  _archivo_origen, _fila_origen, _estructura, _encabezado y _fecha_descarga
  son los de la última versión del fichero en que aparece el registro.
- actividad_contractual_madrid_completo.parquet (tabla unificada): se construye
  desde la fiel fila a fila (mismas filas, enlazadas por _archivo_origen y
  _fila_origen; cada fila se mapea con la cabecera de su versión):
  COLUMNAS_UNIFICADAS + anio, como siempre (fuente_fichero es
  <categoría>_<año>[_<parte>], p.ej. 'menores_2019'); importes a número (los
  de un XLSX/XLS, el número de la celda) y fechas a fecha, con el texto
  publicado en <columna>_texto; tipo_contrato y pyme tal cual y su versión
  normalizada en tipo_contrato_normalizado y pyme_normalizado; las filas que
  antes se eliminaban por vacías siguen, con _fila_vacia=True.
- informes/lectura_ficheros.csv (por versión de fichero: registros según el
  módulo csv, filas leídas, cabecera, estructura, celdas con valor...),
  informes/lineas_fuera_de_tabla.csv e informes/comparacion_csv_xlsx.csv.

CSV FRENTE A XLSX/XLS
Se consolida el CSV. El XLSX/XLS de su mismo grupo (categoría, año y parte) se
compara fila a fila (valores normalizados: fechas, números, mayúsculas sin
tildes; 'casi iguales' si solo difiere una celda) y solo se consolida también
si menos de la mitad de sus filas están en algún CSV publicado del conjunto
(p.ej. 'Contratos formalizados 2015': su CSV publica en realidad contratos
basados en acuerdo marco y los formalizados solo están en el XLS). En un
grupo sin CSV publicado se consolida el primero (los publicados antes que los
retirados) y cada uno de los demás cuyas filas no estén (en más de la mitad)
en los ya consolidados. Desde entonces se sigue consolidando aunque el portal
corrija el CSV (consolidado_desde en el manifiesto), para que sus filas no
desaparezcan, y si su última versión no se puede leer es un fallo. No se
decide (fallo) mientras un CSV del grupo que el portal lista no se haya
descargado, ni sobre un XLSX/XLS que no se puede leer. Sus filas que están,
iguales o casi iguales, en un CSV publicado llevan _repetido_en_csv=True.

HISTÓRICO (sesgo del superviviente, comun/historico.py)
Las dos tablas se construyen con TODAS las versiones guardadas de cada fichero
(acumular en orden cronológico, ámbito = recurso): un registro que el
Ayuntamiento retira o modifica sigue en ellas con _en_ultima_descarga=False, y
un fichero que el portal deja de listar conserva sus filas con
_en_ultima_descarga=False. Se comparan todas las columnas que ha tenido el
fichero (la que falta en una versión cuenta como nula): si cambia la cabecera,
cada registro de la versión anterior queda como versión antigua y ninguna fila
mezcla dos registros. Una versión sin registros con datos no retira nada. Las
salidas se escriben con guardar_version (la anterior va a _historico/); si la
tabla fiel nueva tuviera menos filas de algún fichero que la anterior (no
debería: acumular no quita filas), es un fallo.

Uso:
    python ccaa_madrid_ayuntamiento.py                    # descarga + tablas
    python ccaa_madrid_ayuntamiento.py --solo-descargar   # solo la capa cruda
    python ccaa_madrid_ayuntamiento.py --solo-procesar    # tablas, sin red
    python ccaa_madrid_ayuntamiento.py --comprobar-todo   # vuelve a pedirlo todo
    python ccaa_madrid_ayuntamiento.py --output-dir /ruta/salida
=============================================================================
"""

import argparse
import codecs
import csv
import functools
import hashlib
import io
import json
import math
import os
import re
import sys
import time
import unicodedata
import warnings
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from datetime import time as hora_del_dia
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import COLUMNAS_META, HISTORICO, acumular, guardar_version, versiones  # noqa: E402


# ===========================================================================
# CONFIGURACIÓN
# ===========================================================================
OUTPUT_DIR = Path("datos_madrid_contratacion_completa")
CARPETA_ORIGINALES = "originales"
CARPETA_INFORMES = "informes"
NOMBRE_MANIFIESTO = "_manifiesto.json"
SALIDA_FIEL = "actividad_contractual_madrid_original.parquet"
SALIDA_UNIFICADA = "actividad_contractual_madrid_completo.parquet"

CATEGORIAS_ACTIVAS = {
    "contratos_menores": True,
    "contratos_formalizados": True,
    "acuerdo_marco": True,
    "modificados": True,
    "prorrogados": True,
    "penalidades": True,
    "cesiones": True,
    "resoluciones": True,
    "homologacion": True,
}


# ===========================================================================
# PORTAL (CKAN)
# ===========================================================================
# datos.madrid.es es un CKAN: package_show lista todos los recursos de cada
# conjunto (las URL de recurso no se pueden deducir del año). Las páginas y
# URL 'egob/catalogo/...' del portal anterior ya no sirven los ficheros.
CKAN_API = "https://datos.madrid.es/api/3/action"
DATASET_MENORES = "300253-0-contratos-actividad-menores"
DATASET_ACTIVIDAD = "216876-0-contratos-actividad"
# Carpeta de cada conjunto dentro de originales/
DATASETS = {DATASET_MENORES: "menores", DATASET_ACTIVIDAD: "actividad"}

REINTENTOS = 4
ESPERA_BASE = 2            # segundos; espera exponencial: 2, 4, 8
TIMEOUT = (30, 300)
ESTADOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
CABECERAS = {"User-Agent": "licitaciones-espana/1.0 (+https://github.com/BquantFinance/licitaciones-espana)"}

# Metadatos de cada recurso que se guardan en el manifiesto. Si alguno de
# CAMPOS_CAMBIO cambia desde la última descarga, el fichero se vuelve a pedir.
CAMPOS_CKAN = ("created", "issued", "last_modified", "metadata_modified", "modified", "size",
               "hash", "hash_algorithm", "mimetype", "format", "position", "state", "hierarchy")
CAMPOS_CAMBIO = ("url", "hash", "size", "last_modified", "metadata_modified", "modified")

# Formatos que se leen como tabla (el resto, p.ej. los PDF, solo se descarga)
FORMATOS_TABLA = ("csv", "xlsx", "xls")
FORMATOS = {
    "csv": "csv", "text/csv": "csv", "xlsx": "xlsx", "xls": "xls", "pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/vnd.ms-excel": "xls", "application/pdf": "pdf",
    "json": "json", "application/json": "json", "xml": "xml", "ods": "ods", "zip": "zip", "txt": "txt",
}
# Firma de los formatos binarios: una respuesta sin ella no es el fichero
FIRMAS = {"xlsx": b"PK\x03\x04", "ods": b"PK\x03\x04", "zip": b"PK\x03\x04",
          "xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "pdf": b"%PDF"}
# Prefijo legible de los ficheros locales de cada categoría
PREFIJOS = {
    "contratos_menores": "menores", "contratos_formalizados": "formalizados",
    "acuerdo_marco": "acuerdo_marco", "modificados": "modificados", "prorrogados": "prorrogados",
    "penalidades": "penalidades", "cesiones": "cesiones", "resoluciones": "resoluciones",
    "homologacion": "homologacion", "documentacion": "estructura",
}
# Un XLSX/XLS se consolida (además del CSV de su grupo) si menos de esta
# fracción de sus filas está en algún CSV del mismo conjunto
UMBRAL_XLSX_EN_CSV = 0.5
# La cabecera se busca entre las primeras filas con contenido (antes: skiprows 1..6)
MAX_FILA_CABECERA = 10

csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))


# ===========================================================================
# ESQUEMA UNIFICADO FINAL
# ===========================================================================
COLUMNAS_UNIFICADAS = [
    # --- Metadatos ---
    "fuente_fichero",
    "categoria",
    "estructura",
    # --- Identificación ---
    "n_registro_contrato",
    "n_expediente",
    # --- Organización ---
    "centro_seccion",
    "organo_contratacion",
    "organismo_contratante",
    "organismo_promotor",
    # --- Objeto ---
    "objeto_contrato",
    "tipo_contrato",
    "subtipo_contrato",
    "procedimiento_adjudicacion",
    "criterios_adjudicacion",
    "codigo_cpv",
    # --- Licitación ---
    "n_invitaciones_cursadas",
    "invitados_presentar_oferta",
    "importe_licitacion_sin_iva",
    "importe_licitacion_iva_inc",
    "n_licitadores_participantes",
    "n_lotes",
    "n_lote",
    # --- Adjudicación ---
    "nif_adjudicatario",
    "razon_social_adjudicatario",
    "pyme",
    "importe_adjudicacion_sin_iva",
    "importe_adjudicacion_iva_inc",
    "fecha_adjudicacion",
    "porcentaje_baja_adjudicacion",
    # --- Formalización ---
    "fecha_formalizacion",
    "fecha_inicio",
    "fecha_fin",
    "plazo",
    "fecha_inscripcion",
    "fecha_inscripcion_contrato",
    "valor_estimado",
    "presupuesto_total_iva_inc",
    "aplicacion_presupuestaria",
    "acuerdo_marco_flag",
    "ingreso_gasto",
    # --- Contrato basado / derivado ---
    "n_contrato_derivado",
    "n_expediente_derivado",
    "objeto_derivado",
    "presupuesto_total_derivado",
    "plazo_derivado",
    "fecha_aprobacion_derivado",
    "fecha_formalizacion_derivado",
    # --- Incidencias (modificaciones/prórrogas/penalidades) ---
    "centro_seccion_incidencia",
    "n_registro_incidencia",
    "tipo_incidencia",
    "importe_modificacion",
    "fecha_formalizacion_incidencia",
    "importe_prorroga",
    "plazo_prorroga",
    "importe_penalidad",
    "fecha_acuerdo_penalidad",
    "causa_penalidad",
    "motivo",
    # --- Cesiones ---
    "adjudicatario_cedente",
    "cesionario",
    "importe_cedido",
    "fecha_autorizacion_cesion",
    "fecha_peticion_cesion",
    # --- Resoluciones ---
    "causa_resolucion",
    "causas_generales",
    "otras_causas",
    "causas_especificas",
    "fecha_acuerdo_resolucion",
    # --- Homologación ---
    "n_expediente_sh",
    "objeto_sh",
    "duracion_procedimiento",
    "fecha_fin_actualizada",
    "titulo_expediente",
]


# ===========================================================================
# MAPEOS CONTRATOS MENORES (heredados v3, funciona perfecto)
# ===========================================================================
MAPA_MENORES_A = {
    "Centro": "centro_seccion",
    "Descripción": "organo_contratacion",
    "Título del expediente": "objeto_contrato",
    "Nº exped. Adm.": "n_expediente",
    "Importe": "importe_adjudicacion_iva_inc",
    "Fe.contab.": "fecha_adjudicacion",
    "NIF": "nif_adjudicatario",
    "Tercero": "razon_social_adjudicatario",
    "T. expediente": "tipo_contrato",
}

MAPA_MENORES_B = {
    "Ce.gestor": "centro_seccion",
    "Descripción": "organo_contratacion",
    "Título del expediente": "objeto_contrato",
    "Nº expediente": "n_expediente",
    "NIF": "nif_adjudicatario",
    "Tercero": "razon_social_adjudicatario",
    "Contratista": "razon_social_adjudicatario",
    "Importe": "importe_adjudicacion_iva_inc",
    "Fech/ apro": "fecha_adjudicacion",
    "Fech. apro": "fecha_adjudicacion",
    "Tipo de expediente": "tipo_contrato",
    "Fe/contab/": "fecha_inscripcion",
    "Fe.contab.": "fecha_inscripcion",
}

MAPA_MENORES_C = {
    "Nº RECON": "n_registro_contrato", "NºRECON": "n_registro_contrato",
    "NÚMERO EXPEDIENTE": "n_expediente", "NUMERO EXPEDIENTE": "n_expediente",
    "SECCIÓN": "centro_seccion", "SECCION": "centro_seccion",
    "ÓRG.CONTRATACIÓN": "organo_contratacion",
    "ORG.CONTRATACION": "organo_contratacion",
    "ORG.CONTRATACIÓN": "organo_contratacion",
    "OBJETO DEL CONTRATO": "objeto_contrato",
    "TIPO DE CONTRATO": "tipo_contrato",
    "N.I.F.": "nif_adjudicatario", "N.I.F": "nif_adjudicatario",
    "NIF": "nif_adjudicatario",
    "CONTRATISTA": "razon_social_adjudicatario",
    "IMPORTE": "importe_adjudicacion_iva_inc",
    "FECHA APROBACION": "fecha_adjudicacion",
    "PLAZO": "plazo",
    "FCH.COMUNIC.REG": "fecha_inscripcion",
}

MAPA_MENORES_D = {
    "CONTRATO": "n_registro_contrato", "EXPEDIENTE": "n_expediente",
    "SECCIÓN": "centro_seccion", "ORG_CONTRATACIÓN": "organo_contratacion",
    "OBJETO": "objeto_contrato", "TIPO_CONTRATO": "tipo_contrato",
    "CIF": "nif_adjudicatario", "RAZÓN_SOCIAL": "razon_social_adjudicatario",
    "IMPORTE": "importe_adjudicacion_iva_inc",
    "F_APROBACIÓN": "fecha_adjudicacion", "PLAZO": "plazo",
    "F_INSCRIPCION": "fecha_inscripcion",
}

MAPA_MENORES_E_KEYWORDS = {
    ("REGISTRO", "CONTRATO"): "n_registro_contrato",
    ("EXPEDIENTE",): "n_expediente",
    ("CENTRO",): "centro_seccion",
    ("ORGANO", "CONTRATACION"): "organo_contratacion",
    ("OBJETO",): "objeto_contrato",
    ("TIPO", "CONTRATO"): "tipo_contrato",
    ("INVITACIONES",): "n_invitaciones_cursadas",
    ("INVITADOS",): "invitados_presentar_oferta",
    ("IMPORTE", "LICITACION"): "importe_licitacion_iva_inc",
    ("LICITADORES",): "n_licitadores_participantes",
    ("NIF",): "nif_adjudicatario",
    ("RAZON", "SOCIAL"): "razon_social_adjudicatario",
    ("PYME",): "pyme",
    ("IMPORTE", "ADJUDICACION"): "importe_adjudicacion_iva_inc",
    ("FECHA", "ADJUDICACION"): "fecha_adjudicacion",
    ("PLAZO",): "plazo",
    ("FECHA", "INSCRIPCION"): "fecha_inscripcion",
}

MAPA_MENORES_F_EXTRA = {
    ("ORGANISMO", "CONTRATANTE"): "organismo_contratante",
    ("ORGANISMO", "PROMOTOR"): "organismo_promotor",
}


# ===========================================================================
# MAPEOS FORMALIZADOS/ACUERDO_MARCO ESTRUCTURA ANTIGUA (2015-2020)
# Columnas reales: Mes, Año, Descripción Centro, Organismo,
#   Número Contrato, [Número Expediente], Descripción Contrato,
#   Tipo Contrato, [Procedimiento Adjudicación], [Artículo], [Apartado],
#   Criterios Adjudicación, [Presupuesto Total(IVA Incluido)],
#   [Importe Adjudicación (IVA Incluido)], [Plazo], Fecha Adjudicación,
#   Nombre/Razón Social, NIF/CIF Adjudicatario, [Fecha Formalización],
#   [Acuerdo Marco], [Ingreso/Coste Cero], [Observaciones]
# Para acuerdo_marco: + Número Derivado, Objeto Derivado,
#   Plazo Derivado, Fecha Aprobación Derivado, Fecha Formalización Derivado
# ===========================================================================
MAPA_FORMALIZADOS_OLD = {
    # Centro / Organismo
    "Descripción Centro": "centro_seccion",
    "Descripci¢n Centro": "centro_seccion",  # encoding roto
    "Organismo": "organo_contratacion",
    # Contrato
    "Número Contrato": "n_registro_contrato",
    "N£mero Contrato": "n_registro_contrato",
    "Número Expediente": "n_expediente",
    "N£mero Expediente": "n_expediente",
    # Objeto
    "Descripción Contrato": "objeto_contrato",
    "Descripci¢n Contrato": "objeto_contrato",
    "Tipo Contrato": "tipo_contrato",
    "Procedimiento Adjudicación": "procedimiento_adjudicacion",
    "Procedimiento Adjudicaci¢n": "procedimiento_adjudicacion",
    "Criterios Adjudicación": "criterios_adjudicacion",
    "Criterios Adjudicaci¢n": "criterios_adjudicacion",
    # Importes (múltiples variantes de espaciado)
    "Presupuesto Total             (IVA Incluido)": "presupuesto_total_iva_inc",
    "Presupuesto Total          (IVA Incluido)": "presupuesto_total_iva_inc",
    "Presupuesto Total  IVA Incluido)": "presupuesto_total_iva_inc",
    "Presupuesto Total(IVA Incluido)": "presupuesto_total_iva_inc",
    "Importe Adjudicación   (IVA Incluido)": "importe_adjudicacion_iva_inc",
    "Importe Adjudicación (IVA Incluido)": "importe_adjudicacion_iva_inc",
    "Importe Adjudicaci¢n   (IVA Incluido)": "importe_adjudicacion_iva_inc",
    # Adjudicatario
    "Nombre/Razón Social": "razon_social_adjudicatario",
    "Nombre/Raz¢n Social": "razon_social_adjudicatario",
    "NIF/CIF Adjudicatario": "nif_adjudicatario",
    # Fechas
    "Fecha Adjudicación": "fecha_adjudicacion",
    "Fecha Adjudicaci¢n": "fecha_adjudicacion",
    "Fecha Formalización": "fecha_formalizacion",
    "Fecha Formalizaci¢n": "fecha_formalizacion",
    # Extras
    "Acuerdo Marco": "acuerdo_marco_flag",
    "Ingreso/Coste Cero": "ingreso_gasto",
    "Plazo": "plazo",
    # Derivados (acuerdo marco)
    "Número Derivado": "n_contrato_derivado",
    "N£mero Derivado": "n_contrato_derivado",
    "Objeto Derivado": "objeto_derivado",
    "Plazo Derivado": "plazo_derivado",
    "Fecha Aprobación Derivado": "fecha_aprobacion_derivado",
    "Fecha Aprobaci¢n Derivado": "fecha_aprobacion_derivado",
    "Fecha Formalización Derivado": "fecha_formalizacion_derivado",
    "Fecha Formalizaci¢n Derivado": "fecha_formalizacion_derivado",
}


# ===========================================================================
# MAPEOS MODIFICADOS ESTRUCTURA ANTIGUA (2016-2020)
# 2016-2018: FECHA INSCRIPCION; NUM.CONTRATO; NUM.EXPEDIENTE; GESTOR;
#            OBJETO; C.I.F; ADJUDICATARIO; IMPORTE ADJUDICACION;
#            FECHA FORMALIZACION INCIDENCIA; IMPORTE MODIFICACION;
#            INGRESO/GASTO; [TIPO INCID.]
# 2019-2020: INCIDENCIA; FECHA INSCRIPCION CTO.; NUM.CONTRATO;
#            NUM.EXPEDIENTE; GESTOR; -OBJETO CTO.-; C.I.F; ADJUDICATARIO;
#            IMPORTE ADJUDICACION; FCH. FORMALIZ/APROBAC.INCID.;
#            IMPORTE MODIFICACION; INGRESO/GASTO; MES INSCRIPCION
# 2021 (formalizados en 2020): INCIDENCIA; F_INSC_CONTRATO; CONTRATO;
#            EXPEDIENTE; F_INSCRIPCION; GESTOR; OBJETO_CONTRATO; CIF;
#            ADJUCICATARIO; F_FORMALIZACION; IMPORTE_ADJUDICACIÓN;
#            F_FORM / F_APROB; IMPORTE_MODIFICACION; F_FORM_DERIVADO;
#            OBJETO_DERIVADO; GASTO / INGRESO; INSCRIPCIÓN (mes)
# ===========================================================================
MAPA_MODIFICADOS_OLD = {
    # --- 2016-2018 format ---
    "FECHA INSCRIPCION": "fecha_inscripcion",
    "FECHA INSCRIPCION CTO.": "fecha_inscripcion",
    "NUM.CONTRATO": "n_registro_contrato",
    "NUM.EXPEDIENTE": "n_expediente",
    "GESTOR": "centro_seccion",
    "OBJETO": "objeto_contrato",
    "-OBJETO CTO.-": "objeto_contrato",
    "C.I.F": "nif_adjudicatario",
    "ADJUDICATARIO": "razon_social_adjudicatario",
    "IMPORTE ADJUDICACION": "importe_adjudicacion_iva_inc",
    "FECHA FORMALIZACION INCIDENCIA": "fecha_formalizacion_incidencia",
    "FECHA FORMALIZACION": "fecha_formalizacion",
    "FCH. FORMALIZ/APROBAC.INCID.": "fecha_formalizacion_incidencia",
    "IMPORTE MODIFICACION": "importe_modificacion",
    "INGRESO/GASTO": "ingreso_gasto",
    "TIPO INCID.": "tipo_incidencia",
    "INCIDENCIA": "tipo_incidencia",
    "MES INSCRIPCION": "fecha_inscripcion",  # fallback
    # --- 2015 format (exact headers with accents/typos) ---
    "FECHA INSCRIPCIÓN": "fecha_inscripcion",
    "Nº CONTRATO": "n_registro_contrato",
    "Nº EXPEDIENTE": "n_expediente",
    # "GESTOR" already mapped above
    # "OBJETO" already mapped above
    "CIF": "nif_adjudicatario",
    # "ADJUDICATARIO" already mapped above
    "FECHA FORMALIZACIÓN": "fecha_formalizacion",
    "IMPORTE ADJUDICACIÓN": "importe_adjudicacion_iva_inc",
    "FECJA FORMALIZACIÓN INCIDENCIA": "fecha_formalizacion_incidencia",  # typo in source
    "IMPORTE DE LA MODIFICACIÓN": "importe_modificacion",
    "INGRESO / GASTO": "ingreso_gasto",
    # --- 2021 "formalizados en 2020" (nombres con guion bajo; antes se leía
    # como fichero sin cabecera y las columnas salían corridas) ---
    "F_INSC_CONTRATO": "fecha_inscripcion_contrato",
    "CONTRATO": "n_registro_contrato",
    "EXPEDIENTE": "n_expediente",
    "F_INSCRIPCION": "fecha_inscripcion",
    "OBJETO_CONTRATO": "objeto_contrato",
    "ADJUCICATARIO": "razon_social_adjudicatario",  # errata del origen
    "F_FORMALIZACION": "fecha_formalizacion",
    "IMPORTE_ADJUDICACIÓN": "importe_adjudicacion_iva_inc",
    "F_FORM / F_APROB": "fecha_formalizacion_incidencia",
    "IMPORTE_MODIFICACION": "importe_modificacion",
    "F_FORM_DERIVADO": "fecha_formalizacion_derivado",
    "OBJETO_DERIVADO": "objeto_derivado",
    "GASTO / INGRESO": "ingreso_gasto",
}


# ===========================================================================
# MAPEOS HOMOLOGACIÓN (2022-2025)
# Columnas: FECHA DE INSCRIPCION, CENTRO - SECCION, N. EXPEDIENTE S.H.,
#   OBJETO DEL S.H., [DURACION PROCEDIMIENTO (MESES)],
#   [FECHA DE FIN ACTUALIZADA], N. DE REGISTRO, N. DE EXPEDIENTE,
#   TITULO DEL EXPEDIENTE, TIPO DE CONTRATO, CRITERIOS DE ADJUDICACION,
#   ADJUDICATARIO, IMPORTE ADJUDICACION IVA INC.,
#   PLAZO DE EJECUCION, FECHA DE ADJUDICACION, FECHA DE FORMALIZACION
#   [+ ORGANISMO_CONTRATANTE, ORGANISMO_PROMOTOR en 2025]
# ===========================================================================
MAPA_HOMOLOGACION = {
    "FECHA DE INSCRIPCION": "fecha_inscripcion",
    "CENTRO - SECCION": "centro_seccion",
    "N. EXPEDIENTE S.H.": "n_expediente_sh",
    "OBJETO DEL S.H.": "objeto_sh",
    "DURACION PROCEDIMIENTO (MESES)": "duracion_procedimiento",
    "FECHA DE FIN ACTUALIZADA": "fecha_fin_actualizada",
    "N. DE REGISTRO": "n_registro_contrato",
    "N. DE EXPEDIENTE": "n_expediente",
    "TITULO DEL EXPEDIENTE": "objeto_contrato",
    "TIPO DE CONTRATO": "tipo_contrato",
    "CRITERIOS DE ADJUDICACION": "criterios_adjudicacion",
    "ADJUDICATARIO": "razon_social_adjudicatario",
    "IMPORTE ADJUDICACION IVA INC.": "importe_adjudicacion_iva_inc",
    "PLAZO DE EJECUCION": "plazo",
    "FECHA DE ADJUDICACION": "fecha_adjudicacion",
    "FECHA DE FORMALIZACION": "fecha_formalizacion",
    "ORGANISMO_CONTRATANTE": "organismo_contratante",
    "ORGANISMO_PROMOTOR": "organismo_promotor",
}


# ===========================================================================
# MAPEOS ACTIVIDAD CONTRACTUAL MODERNA (AC_NEW / AC_2025)
# Usado por formalizados, acuerdo_marco, modificados, prorrogados,
# penalidades, cesiones, resoluciones desde 2021+
# ===========================================================================
MAPA_AC_MODERN_KEYWORDS = {
    ("REGISTRO", "CONTRATO"): "n_registro_contrato",
    ("EXPEDIENTE",): "n_expediente",
    ("CENTRO", "SECCION"): "centro_seccion",
    ("ORGANO", "CONTRATACION"): "organo_contratacion",
    ("ORGANISMO", "CONTRATANTE"): "organismo_contratante",
    ("ORGANISMO", "PROMOTOR"): "organismo_promotor",
    ("OBJETO",): "objeto_contrato",
    ("TIPO", "CONTRATO"): "tipo_contrato",
    ("SUBTIPO",): "subtipo_contrato",
    # "SUBTIPO DE CONTRATO" también casa con ("TIPO","CONTRATO") y puntúa más que
    # ("SUBTIPO",): sin esta clave se quedaba sin mapear
    ("SUBTIPO", "CONTRATO"): "subtipo_contrato",
    ("CPV",): "codigo_cpv",
    ("INVITACIONES",): "n_invitaciones_cursadas",
    ("INVITADOS",): "invitados_presentar_oferta",
    ("LICITADORES",): "n_licitadores_participantes",
    ("LOTES",): "n_lotes",
    ("LOTE",): "n_lote",
    ("NIF",): "nif_adjudicatario",
    ("RAZON", "SOCIAL"): "razon_social_adjudicatario",
    ("PYME",): "pyme",
    ("FECHA", "FORMALIZACION"): "fecha_formalizacion",
    ("FECHA", "INICIO"): "fecha_inicio",
    ("FECHA", "FIN"): "fecha_fin",
    ("PLAZO",): "plazo",
    ("FECHA", "INSCRIPCION"): "fecha_inscripcion",
    ("FECHA", "ADJUDICACION"): "fecha_adjudicacion",
}

# Importes: ordered list, most specific first
MAPA_AC_IMPORTES = [
    (("IMPORTE", "LICITACION", "SIN"), "importe_licitacion_sin_iva"),
    (("IMPORTE", "LICITACION"), "importe_licitacion_iva_inc"),
    (("IMPORTE", "ADJUDICACION", "SIN"), "importe_adjudicacion_sin_iva"),
    (("IMPORTE", "ADJUDICACION"), "importe_adjudicacion_iva_inc"),
    (("IMPORTE", "MODIFICACION"), "importe_modificacion"),
    (("IMPORTE", "PENALIDAD"), "importe_penalidad"),
    (("IMPORTE", "PRORROGA"), "importe_prorroga"),
    (("IMPORTE", "CEDIDO"), "importe_cedido"),
]

# --- Extra keywords per category ---

MAPA_EXTRA_FORMALIZADOS = {
    ("VALOR", "ESTIMADO"): "valor_estimado",
    ("PORCENTAJE", "BAJA"): "porcentaje_baja_adjudicacion",
    ("CRITERIOS", "ADJUDICACION"): "criterios_adjudicacion",
    ("ACUERDO", "MARCO"): "acuerdo_marco_flag",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("PRESUPUESTO", "TOTAL"): "presupuesto_total_iva_inc",
    ("APLICACION", "PRESUPUESTARIA"): "aplicacion_presupuestaria",
    ("INGRESO",): "ingreso_gasto",
    ("PROCEDIMIENTO",): "procedimiento_adjudicacion",
}

MAPA_EXTRA_ACUERDO_MARCO = {
    # These will be matched by keywords
    ("PRESUPUESTO", "TOTAL"): "presupuesto_total_iva_inc",
    ("CRITERIOS", "ADJUDICACION"): "criterios_adjudicacion",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("INGRESO",): "ingreso_gasto",
}

# Direct name matching for C.B./CB/CESDA derivado columns (keywords fail on these)
MAPA_DIRECTO_ACUERDO_MARCO_DERIVADOS = {
    "N. DE CONTRATO DEL C.B.": "n_contrato_derivado",
    "N. DE CONTRATO DEL CB/CESDA": "n_contrato_derivado",
    "N. DE EXPEDIENTE C.B.": "n_expediente_derivado",
    "N. DE EXPEDIENTE CB/CESDA": "n_expediente_derivado",
    "OBJETO C.B.": "objeto_derivado",
    "OBJETO CB/CESDA": "objeto_derivado",
    "PRESUPUESTO TOTAL IVA INC.": "presupuesto_total_derivado",
    "PRESUPUESTO TOTAL IVA INC. C.B.": "presupuesto_total_derivado",
    "PRESUPUESTO TOTAL IVA INC. CB/CESDA": "presupuesto_total_derivado",
    "PLAZO C.B.": "plazo_derivado",
    "PLAZO CB/CESDA": "plazo_derivado",
    "FECHA DE APROBACION C.B.": "fecha_aprobacion_derivado",
    "FECHA DE APROBACION CB/CESDA": "fecha_aprobacion_derivado",
    "FECHA DE FORMALIZACION C.B.": "fecha_formalizacion_derivado",
    "FECHA DE FORMALIZACION CB/CESDA": "fecha_formalizacion_derivado",
}

MAPA_EXTRA_MODIFICADOS = {
    ("TIPO", "INCIDENCIA"): "tipo_incidencia",
    ("INSCRIPCION", "CONTRATO"): "fecha_inscripcion_contrato",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("FORMALIZACION", "INC"): "fecha_formalizacion_incidencia",
    ("REGISTRO", "INCIDENCIA"): "n_registro_incidencia",
    ("INGRESO",): "ingreso_gasto",
}

MAPA_EXTRA_PRORROGADOS = {
    ("TIPO", "INCIDENCIA"): "tipo_incidencia",
    ("INSCRIPCION", "CONTRATO"): "fecha_inscripcion_contrato",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("FORMALIZACION", "INC"): "fecha_formalizacion_incidencia",
    ("IMPORTE", "PRORROGA"): "importe_prorroga",
    ("REGISTRO", "INCIDENCIA"): "n_registro_incidencia",
    ("INGRESO",): "ingreso_gasto",
}

MAPA_EXTRA_PENALIDADES = {
    ("TIPO", "INCIDENCIA"): "tipo_incidencia",
    ("INSCRIPCION", "CONTRATO"): "fecha_inscripcion_contrato",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("REGISTRO", "INCIDENCIA"): "n_registro_incidencia",
    ("ACUERDO", "PENALIDAD"): "fecha_acuerdo_penalidad",
    ("CAUSA",): "causa_penalidad",
}

MAPA_EXTRA_CESIONES = {
    ("TIPO", "INCIDENCIA"): "tipo_incidencia",
    ("INSCRIPCION", "CONTRATO"): "fecha_inscripcion_contrato",
    ("ADJUDICATARIO", "CEDENTE"): "adjudicatario_cedente",
    ("AUTORIZACION", "CESION"): "fecha_autorizacion_cesion",
    ("PETICION", "CESION"): "fecha_peticion_cesion",
    ("IMPORTE", "PRORROGA"): "importe_prorroga",
    ("IMPORTE", "CEDIDO"): "importe_cedido",
    ("CESIONARIO",): "cesionario",
    ("REGISTRO", "INCIDENCIA"): "n_registro_incidencia",
    ("INGRESO",): "ingreso_gasto",
}

MAPA_EXTRA_RESOLUCIONES = {
    ("TIPO", "INCIDENCIA"): "tipo_incidencia",
    ("INSCRIPCION", "CONTRATO"): "fecha_inscripcion_contrato",
    ("ADJUDICATARIO",): "razon_social_adjudicatario",
    ("OTRAS", "CAUSAS"): "otras_causas",
    ("CAUSAS", "ESPECIFICAS"): "causas_especificas",
    ("CAUSAS", "GENERALES"): "causas_generales",
    ("ACUERDO", "RESOLUCION"): "fecha_acuerdo_resolucion",
    ("REGISTRO", "INCIDENCIA"): "n_registro_incidencia",
    ("INGRESO",): "ingreso_gasto",
}


# ===========================================================================
# UTILIDADES
# ===========================================================================
def anio_en_curso():
    return date.today().year


def ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_de_epoch(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash_fichero(ruta, algoritmo):
    h = hashlib.new(algoritmo)
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def _clasificar_categoria(nombre):
    """Categoría por el nombre corto de un fichero ('menores_2019',
    'formalizados_2021_nuevos'...): la usa procesar_fichero cuando no se le
    indica la categoría."""
    n = nombre.lower()
    if "menor" in n: return "contratos_menores"
    if "homologacion" in n or "homologación" in n: return "homologacion"
    if "acuerdo" in n or "marco" in n: return "acuerdo_marco"
    if "modific" in n: return "modificados"
    if "prorro" in n: return "prorrogados"
    if "penalid" in n: return "penalidades"
    if "cesion" in n or "cesión" in n: return "cesiones"
    if "resolucion" in n or "resolución" in n: return "resoluciones"
    return "contratos_formalizados"


def strip_normalize(col_name):
    s = col_name.upper().strip()
    for old, new in {'Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U','Ñ':'N',
                     'º':'','ª':'','¢':'O','£':'U','¡':'I','¥':'N'}.items():
        s = s.replace(old, new)
    s = re.sub(r'[.\-,;:()_/]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _sin_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFKD', texto)
                   if not unicodedata.combining(c))


def _texto_plano(texto):
    """Minúsculas, sin tildes y con los espacios colapsados."""
    return " ".join(_sin_acentos(str(texto or "")).lower().split())


def _slug(texto, largo=40):
    return re.sub(r"[^a-z0-9]+", "_", _texto_plano(texto)).strip("_")[:largo].strip("_")


class Resumen:
    """Lo descargado, lo que no hacía falta pedir, las tablas escritas, los
    avisos y los fallos (con fallos el script termina con código 1)."""

    def __init__(self):
        self.descargados = []    # (fichero, estado, bytes, motivo)
        self.sin_pedir = 0
        self.tablas = []         # (ruta, filas, columnas, estado)
        self.avisos = []
        self.fallos = []

    def descargado(self, fichero, estado, tam, motivo):
        self.descargados.append((fichero, estado, tam, motivo))
        print(f"    {estado}: {fichero} ({tam / 1024:.0f} KB; {motivo})")

    def aviso(self, texto):
        self.avisos.append(texto)
        print(f"  AVISO: {texto}")

    def fallo(self, texto):
        self.fallos.append(texto)
        print(f"  ERROR: {texto}")

    def imprimir(self, titulo):
        print("\n" + "=" * 70)
        print(f"RESUMEN - {titulo}")
        print("=" * 70)
        cambiados = [d for d in self.descargados if d[1] != "sin_cambios"]
        total = sum(t for _, _, t, _ in self.descargados)
        print(f"Ficheros pedidos al portal: {len(self.descargados)} ({total / 1024 / 1024:.1f} MB); "
              f"nuevos o cambiados: {len(cambiados)}")
        for fichero, estado, tam, motivo in cambiados:
            print(f"  {estado:12} {fichero} ({tam / 1024:.0f} KB; {motivo})")
        print(f"Ficheros que no hacía falta volver a pedir: {self.sin_pedir}")
        for ruta, filas, columnas, estado in self.tablas:
            print(f"Tabla {ruta.name}: {filas:,} filas x {columnas} columnas ({estado})")
        if self.avisos:
            print(f"\nAVISOS ({len(self.avisos)}):")
            for texto in self.avisos:
                print(f"  - {texto}")
        if self.fallos:
            print(f"\nFALLOS ({len(self.fallos)}) - vuelve a ejecutar el script para reintentarlos:")
            for texto in self.fallos:
                print(f"  - {texto}")
        else:
            print("\nSin fallos.")


def _leer_manifiesto(ruta):
    """Entradas de un manifiesto, o None si no se puede leer (JSON corrupto o
    cortado, p.ej. por un disco lleno)."""
    try:
        entradas = json.loads(Path(ruta).read_text(encoding="utf-8"))
    except (OSError, ValueError):          # UnicodeDecodeError y JSONDecodeError son ValueError
        return None
    return entradas if isinstance(entradas, dict) else None


class Manifiesto:
    """originales/_manifiesto.json: {ruta del fichero relativa a la salida:
    entrada}. La primera vez que se guarda en cada ejecución se usa
    guardar_version (el manifiesto de la ejecución anterior queda en
    _historico/); después se sustituye de forma atómica el de esta misma
    ejecución, que se guarda tras cada descarga para que un corte (SIGHUP,
    SIGKILL...) no deje ficheros descargados sin anotar. Si no existe o no se
    puede leer (corrupto = True; al guardar pasa a _historico/, no se borra),
    se recupera la última versión legible de _historico/ (recuperado = su
    ruta)."""

    def __init__(self, ruta):
        self.ruta = Path(ruta)
        self.entradas = {}
        self.recuperado = None
        self.corrupto = False
        self._versionado = False
        if self.ruta.exists():
            entradas = _leer_manifiesto(self.ruta)
            if entradas is not None:
                self.entradas = entradas
                return
            self.corrupto = True
        anteriores = sorted((self.ruta.parent / HISTORICO).glob(f"{self.ruta.stem}__*{self.ruta.suffix}"))
        for anterior in reversed(anteriores):      # la más reciente que se pueda leer
            entradas = _leer_manifiesto(anterior)
            if entradas is not None:
                self.entradas = entradas
                self.recuperado = anterior
                break

    def guardar(self):
        contenido = json.dumps(self.entradas, ensure_ascii=False, indent=1, sort_keys=True).encode("utf-8")
        if not self._versionado:
            estado = guardar_version(self.ruta, contenido)
            # con 'sin_cambios' el fichero sigue siendo el de la ejecución anterior:
            # el siguiente guardado tiene que volver a pasar por guardar_version
            self._versionado = estado != "sin_cambios"
            return estado
        tmp = self.ruta.with_name(f".{self.ruta.name}.nuevo")
        tmp.write_bytes(contenido)
        os.replace(tmp, self.ruta)
        return "actualizado"

    def claves_de(self, dataset, rid, formato):
        return [clave for clave, entrada in self.entradas.items()
                if (entrada.get("dataset"), entrada.get("id"), entrada.get("formato")) == (dataset, rid, formato)]


# ===========================================================================
# DESCUBRIMIENTO (API CKAN)
# ===========================================================================
class ErrorDescarga(Exception):
    def __init__(self, mensaje, estado=None):
        super().__init__(mensaje)
        self.estado = estado


def _get(url, params=None):
    """GET con reintentos (red, 429, 5xx) y espera exponencial. Un 4xx es
    definitivo. Devuelve la respuesta o lanza ErrorDescarga."""
    error = None
    for intento in range(1, REINTENTOS + 1):
        try:
            r = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT)
        except requests.RequestException as e:
            error = f"{type(e).__name__}: {e}"
        else:
            if r.status_code < 400:
                return r
            if r.status_code not in ESTADOS_REINTENTABLES:
                raise ErrorDescarga(f"HTTP {r.status_code}", r.status_code)
            error = f"HTTP {r.status_code}"
        if intento < REINTENTOS:
            time.sleep(ESPERA_BASE * 2 ** (intento - 1))
    raise ErrorDescarga(f"{error} (tras {REINTENTOS} intentos)")


def package_show(dataset):
    """Paquete CKAN del conjunto. Lanza ErrorDescarga si la API no responde o
    no devuelve un paquete con recursos (el portal contesta a veces con su
    página HTML de mantenimiento)."""
    r = _get(f"{CKAN_API}/package_show", params={"id": dataset})
    try:
        datos = r.json()
    except ValueError:
        raise ErrorDescarga("la API no devolvió JSON (¿página de error del portal?)") from None
    resultado = datos.get("result") if isinstance(datos, dict) and datos.get("success") else None
    recursos = resultado.get("resources") if isinstance(resultado, dict) else None
    if not isinstance(recursos, list) or not recursos:
        raise ErrorDescarga("package_show no devolvió ningún recurso")
    return resultado


PATRON_ANIO = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def categoria_de(descripcion, dataset):
    """Categoría de un recurso por su descripción en CKAN."""
    d = _texto_plano(descripcion)
    if "contenido y estructura" in d:
        return "documentacion"
    if dataset == DATASET_MENORES or d.startswith("contratos menores"):
        return "contratos_menores"
    if "homologaci" in d:
        return "homologacion"
    if "acuerdo marco" in d or "sistema dinamico" in d:
        return "acuerdo_marco"
    if "modificad" in d:
        return "modificados"
    if "prorroga" in d or "prorrogad" in d:
        return "prorrogados"
    if "penalidad" in d:
        return "penalidades"
    if "cesion" in d:
        return "cesiones"
    if "resolucion" in d:
        return "resoluciones"
    return "contratos_formalizados"


def anio_y_parte(descripcion, categoria):
    """(año, parte) de la descripción: el primer año que aparece y lo que la
    sigue ('Contratos menores 2021 (hasta febrero)' -> (2021, 'hasta_febrero');
    '... 2021. Formalizados en 2020' -> (2021, 'formalizados_en_2020')). Los
    PDF de estructura no tienen año: su parte es el paréntesis ('desde_2025')."""
    d = _texto_plano(descripcion)
    if categoria == "documentacion":
        m = re.search(r"\(([^)]*)\)", d)
        return None, (_slug(m.group(1)) or None) if m else None
    m = PATRON_ANIO.search(d)
    if not m:
        return None, None
    return int(m.group(1)), _slug(d[m.end():]) or None


def anio_de_jerarquia(recurso):
    """Año del recurso en la jerarquía del portal (primer nivel, p.ej. '2025')."""
    for nivel in recurso.get("hierarchy") or []:
        nombre = str((nivel or {}).get("hierarchy_name") or "").strip() if isinstance(nivel, dict) else ""
        if re.fullmatch(r"20\d{2}", nombre):
            return int(nombre)
    return None


def formato_de(recurso):
    """csv | xlsx | xls | pdf | ... por el formato, el tipo MIME o la extensión."""
    for valor in (recurso.get("format"), recurso.get("mimetype")):
        formato = FORMATOS.get(str(valor or "").strip().lower())
        if formato:
            return formato
    nombre = unquote(urlparse(str(recurso.get("url") or "")).path).rsplit("/", 1)[-1].lower()
    extension = nombre.rsplit(".", 1)[-1] if "." in nombre else ""
    return FORMATOS.get(extension) or re.sub(r"[^a-z0-9]", "", extension)[:8] or "bin"


def clasificar_recurso(recurso, dataset):
    """Datos de un recurso CKAN: id, descripción, categoría, año, parte,
    formato, url y metadatos (más un aviso si el año de la descripción no es
    el de la jerarquía del portal)."""
    rid = str(recurso.get("id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,120}", rid):
        rid = "sin_id_" + hashlib.sha1(str(recurso.get("url") or "").encode()).hexdigest()[:12]
    descripcion = str(recurso.get("description") or recurso.get("name") or "")
    categoria = categoria_de(descripcion, dataset)
    anio, parte = anio_y_parte(descripcion, categoria)
    jerarquia = anio_de_jerarquia(recurso)
    aviso = None
    if anio is None and categoria != "documentacion":
        anio = jerarquia
    elif anio is not None and jerarquia is not None and anio != jerarquia:
        aviso = f"año {anio} en la descripción y {jerarquia} en la jerarquía del portal"
    return {"id": rid, "dataset": dataset, "descripcion": descripcion, "nombre_ckan": recurso.get("name"),
            "categoria": categoria, "anio": anio, "parte": parte, "formato": formato_de(recurso),
            "url": str(recurso.get("url") or "").strip(),
            "ckan": {c: recurso.get(c) for c in CAMPOS_CKAN if c in recurso}, "aviso": aviso}


def nombre_local(info):
    """<categoría>_<año>[_<parte>]__<id>.<formato>: legible y único por el id."""
    partes = [PREFIJOS.get(info["categoria"], info["categoria"])]
    partes += [str(info["anio"])] if info["anio"] else []
    partes += [info["parte"]] if info["parte"] else []
    return f"{'_'.join(partes)}__{info['id']}.{info['formato']}"


def clasificacion(entrada):
    return entrada.get("categoria"), entrada.get("anio"), entrada.get("parte")


def clave_local(manifiesto, info):
    """(ruta del fichero de un recurso relativa a la salida, clave anterior).

    Se reutiliza la entrada del manifiesto con el mismo id, formato y
    clasificación (categoría, año y parte), o con la clasificación con que el
    portal lo describe ahora (clasificacion_portal: un fichero que se volvió a
    describir sirviendo los mismos bytes, ver plegar_reclasificado). Si el id
    ya existía con otra clasificación, el portal lo ha reutilizado para otro
    fichero (en datos.madrid.es el id sale de la posición del recurso) o solo
    ha cambiado su descripción: la clave es la de un fichero nuevo, con su
    propio nombre, y el segundo valor es la clave anterior. La primera
    descarga decide: los mismos bytes que la copia vigente de la anterior son
    el mismo fichero (se pliega en la clave anterior); si no, la anterior
    queda como retirada. Así las versiones de dos ficheros distintos nunca se
    mezclan ni se reetiquetan. También se devuelve la anterior si la clave
    que casa aún no tiene copia (una ejecución cortada antes de plegarla)."""
    mismas = manifiesto.claves_de(info["dataset"], info["id"], info["formato"])
    actual = clasificacion(info)

    def anterior(excepto=None):
        otras = [c for c in mismas if c != excepto and clasificacion(manifiesto.entradas[c]) != actual]
        publicadas = [c for c in otras if _publicado(manifiesto.entradas[c])]
        return (publicadas or otras or [None])[0]

    for clave in mismas:
        if clasificacion(manifiesto.entradas[clave]) == actual:
            return clave, (None if manifiesto.entradas[clave].get("sha256") else anterior(clave))
    for clave in mismas:
        if tuple(manifiesto.entradas[clave].get("clasificacion_portal") or ()) == actual:
            return clave, None
    ruta = PurePosixPath(CARPETA_ORIGINALES) / DATASETS[info["dataset"]] / nombre_local(info)
    return ruta.as_posix(), anterior()


# ===========================================================================
# DESCARGA (capa cruda: nunca se machaca la versión anterior)
# ===========================================================================
def comprobar_contenido(datos, formato):
    """Rechaza lo que no es el fichero: respuesta vacía, página HTML (el portal
    devuelve su página de error o de mantenimiento) o sin la firma del formato."""
    limpio = datos.lstrip(b"\xef\xbb\xbf \t\r\n")
    if not limpio:
        raise ErrorDescarga("respuesta vacía")
    cabeza = limpio[:2048].lower()
    if cabeza.startswith((b"<!doctype html", b"<html")) or b"<body" in cabeza or b"<head" in cabeza:
        raise ErrorDescarga("el portal devolvió una página HTML en lugar del fichero")
    firma = FIRMAS.get(formato)
    if firma and not datos.startswith(firma):
        raise ErrorDescarga(f"la respuesta no es un fichero {formato.upper()}")


def _columnas_con_nombre(tabla):
    return {c for c in tabla.columnas_mapeo if not c.startswith("Unnamed: ")}


def columnas_reconocidas(tabla, categoria):
    """Columnas del esquema unificado que salen de la cabecera de una tabla."""
    vacia = pd.DataFrame({c: pd.Series(dtype=object) for c in tabla.columnas_mapeo})
    return len(_mapear_fichero(vacia, "", categoria, tabla.estructura))


PATRON_CIFRA = re.compile(r"\d")


def _sin_cifras(tabla):
    """Ninguna celda de datos tiene una cifra (un fichero de contratos siempre
    trae fechas, importes o números de expediente)."""
    return not any(PATRON_CIFRA.search(c) for fila in tabla.df.itertuples(index=False, name=None) for c in fila
                   if isinstance(c, str))


def motivo_rechazo(tabla, previa, hay_copia, categoria):
    """Por qué una descarga tabular no es el fichero (o None).

    - Una sola columna: no es una tabla, sino un texto (p.ej. un mensaje de
      error del portal servido con 200 y sin HTML).
    - Registros sin ninguna cifra y una cabecera sin ninguna columna conocida:
      un texto con comas ('Servicio no disponible, disculpe las molestias')
      leído como tabla de dos columnas.
    - Habiendo copia: sin ningún registro con datos; sin cabecera cuando la
      versión anterior la tenía; o con una cabecera sin ninguna columna en
      común con la de la versión anterior y en la que no se reconocen al
      menos dos columnas del esquema unificado (un fichero renombrado entero,
      p.ej. con los nombres de otra época, sí se acepta). Aceptarla retiraría
      todos los registros del fichero.
    La primera descarga de un fichero sin registros con datos (p.ej. un año
    recién creado) se acepta, pero se vuelve a pedir en cada ejecución
    (motivo_descarga) hasta que los traiga.
    """
    if tabla.info.get("ancho", 0) < 2:
        return "la respuesta no parece una tabla (una sola columna)"
    if filas_con_datos(tabla) and columnas_reconocidas(tabla, categoria) == 0 and _sin_cifras(tabla):
        return ("la respuesta no parece una tabla de contratos (ninguna columna conocida y ninguna cifra en sus "
                "registros: p.ej. un texto de error)")
    if not hay_copia:
        return None
    if filas_con_datos(tabla) == 0:
        return "el fichero descargado no trae ningún registro con datos"
    if previa is None or previa.cabecera is None:
        return None
    if tabla.cabecera is None:
        return "el fichero descargado no tiene cabecera y la versión anterior sí"
    if _columnas_con_nombre(previa) and not (_columnas_con_nombre(tabla) & _columnas_con_nombre(previa)) \
            and columnas_reconocidas(tabla, categoria) < 2:
        return ("la cabecera del fichero descargado no tiene ninguna columna en común con la de la versión "
                "anterior ni columnas conocidas")
    return None


def descargar(url, destino, formato, categoria, tam_ckan=None, identico_a=None):
    """Descarga `url` en `destino` sin machacar la copia anterior
    (guardar_version: si cambió, la anterior pasa a _historico/).

    Devuelve (estado, bytes, md5, filas con datos o None si no es una tabla)
    con estado 'nuevo', 'actualizado' o 'sin_cambios'; o 'identico' (sin
    escribir nada) si la descarga tiene los mismos bytes que el fichero
    `identico_a`. Lanza ErrorDescarga, sin tocar nada, si la descarga falla,
    está cortada (menos bytes que su Content-Length o que el 'size' de CKAN,
    `tam_ckan`: un corte en un límite de registro, o con un Content-Length
    coherente, pasaba por un fichero válido), no es del formato esperado, no
    se puede leer como tabla o no es el fichero (motivo_rechazo).
    """
    destino = Path(destino)
    r = _get(url)
    datos = r.content
    cabeceras = getattr(r, "headers", None) or {}
    esperado = str(cabeceras.get("Content-Length") or "")
    if esperado.isdigit() and int(esperado) != len(datos) and not cabeceras.get("Content-Encoding"):
        raise ErrorDescarga(f"descarga incompleta ({len(datos)} de {esperado} bytes)")
    comprobar_contenido(datos, formato)
    tam = str(tam_ckan if tam_ckan is not None else "").strip()
    if tam.isdigit() and int(tam) != len(datos):
        raise ErrorDescarga(f"la descarga tiene {len(datos)} bytes y CKAN indica {tam} (¿cortada o no es el fichero?)")
    if identico_a is not None and Path(identico_a).is_file() and \
            hashlib.sha256(datos).hexdigest() == _hash_fichero(identico_a, "sha256"):
        return "identico", len(datos), hashlib.md5(datos).hexdigest(), None
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_name(f".{destino.name}.part")
    filas = None
    try:
        parcial.write_bytes(datos)
        if formato in FORMATOS_TABLA:
            try:
                tabla = leer_tabla(parcial, categoria)
            except Exception as e:  # noqa: BLE001 - cualquier fallo de lectura invalida la descarga
                raise ErrorDescarga(f"no se puede leer como {formato.upper()} ({type(e).__name__}: {e})") from None
            anteriores = versiones(destino)
            # lo mismo que ya se tiene no sustituye nada (sin_cambios): no se
            # rechaza, p.ej. un fichero del año en curso que aún no tiene registros
            igual = destino.exists() and hashlib.sha256(datos).hexdigest() == _hash_fichero(destino, "sha256")
            previa = None
            if anteriores and not igual:
                try:
                    previa = leer_tabla(anteriores[-1], categoria)
                except Exception:  # noqa: BLE001 - sin versión anterior legible no se compara la cabecera
                    previa = None
            rechazo = None if igual else motivo_rechazo(tabla, previa, bool(anteriores), categoria)
            if rechazo:
                raise ErrorDescarga(rechazo)
            filas = filas_con_datos(tabla)
        estado = guardar_version(destino, desde=parcial)
    finally:
        if parcial.exists():
            parcial.unlink()
    return estado, len(datos), hashlib.md5(datos).hexdigest(), filas


def motivo_descarga(destino, entrada, info, anio_actual, comprobar_todo):
    """Por qué hay que (volver a) pedir un recurso, o None si ya se tiene y no
    puede haber cambiado. Volver a pedirlo nunca machaca: guardar_version.

    Además de los cambios en CKAN y los años que siguen cambiando, se vuelve a
    pedir una copia que no puede ser la que describe CKAN: sin registros con
    datos (p.ej. una primera descarga que solo trajo la cabecera) o con otro
    tamaño que el que indica su 'size' (en septiembre de 2026 coincidía en
    los 169 recursos)."""
    if not destino.exists():
        return "nuevo"
    if comprobar_todo:
        return "comprobación completa"
    anterior = {"url": entrada.get("url_descarga", entrada.get("url")), **(entrada.get("ckan") or {})}
    actual = {"url": info["url"], **info["ckan"]}
    cambios = [c for c in CAMPOS_CAMBIO if str(actual.get(c)) != str(anterior.get(c))]
    if cambios:
        return f"cambiado en el portal: {', '.join(cambios)}"
    if info["formato"] in FORMATOS_TABLA and entrada.get("filas_con_datos") == 0:
        return "la copia no tiene registros con datos"
    tam = str(info["ckan"].get("size") or "").strip()
    if tam.isdigit() and int(tam) != destino.stat().st_size:
        return f"la copia ({destino.stat().st_size} bytes) no tiene el tamaño que indica CKAN ({tam})"
    if info["categoria"] != "documentacion" and (info["anio"] is None or info["anio"] >= anio_actual - 1):
        return "año en curso o anterior"
    return None


def anotar_copia(entrada, destino, estado=None):
    """Anota en la entrada del manifiesto la copia vigente de `destino`:
    sha256, bytes y fecha de descarga, que es su fecha de modificación (la
    misma que tendrá su sello cuando pase a _historico/). Añade la versión a
    'versiones' si es nueva o actualizada, o si no estaba anotada (p.ej. un
    corte entre la descarga y el guardado del manifiesto: se anota con
    estado 'no_registrada'). Devuelve (entrada, si había una versión sin
    anotar)."""
    entrada = dict(entrada)
    sha = _hash_fichero(destino, "sha256")
    tam = destino.stat().st_size
    fecha = iso_de_epoch(destino.stat().st_mtime)
    lista = list(entrada.get("versiones") or [])
    ultima = entrada.get("sha256") or (lista[-1].get("sha256") if lista else None)
    sin_anotar = estado not in ("nuevo", "actualizado") and sha != ultima
    if estado in ("nuevo", "actualizado") or sin_anotar:
        lista.append({"fecha": fecha, "estado": estado if not sin_anotar else "no_registrada",
                      "sha256": sha, "bytes": tam, **{k: entrada.get(k) for k in (
                          "descripcion", "categoria", "anio", "parte", "url")}})
    entrada.update(sha256=sha, bytes=tam, fecha_descarga=fecha, versiones=lista)
    return entrada, sin_anotar


def registrar_descarga(entrada, destino, estado, md5, info, filas=None):
    """Entrada del manifiesto tras una descarga correcta. Devuelve (entrada,
    si la copia vigente anterior no estaba anotada)."""
    entrada = dict(entrada)
    entrada["ckan"] = info["ckan"]
    entrada["url_descarga"] = info["url"]
    entrada["md5"] = md5
    hash_ckan = str(info["ckan"].get("hash") or "").strip().lower()
    entrada["md5_coincide_ckan"] = (md5 == hash_ckan) if hash_ckan else None
    if filas is not None:
        entrada["filas_con_datos"] = filas
    entrada, sin_anotar = anotar_copia(entrada, destino, estado)
    entrada["fecha_comprobacion"] = ahora_iso()
    entrada.pop("ultimo_error", None)
    return entrada, sin_anotar


def descargar_dataset(dataset, salida, manifiesto, resumen, anio_actual, comprobar_todo=False):
    """Descubre los recursos del conjunto y descarga lo que falta o puede haber
    cambiado. Devuelve las claves listadas por el portal, o None si la API no
    responde (entonces no se descarga ni se retira nada)."""
    salida = Path(salida)
    print(f"\n[CKAN] {dataset}")
    try:
        paquete = package_show(dataset)
    except ErrorDescarga as e:
        resumen.fallo(f"{dataset}: la API CKAN no responde ({e}); no se descarga ni se retira nada")
        return None
    recursos = paquete["resources"]
    print(f"  {len(recursos)} recursos")
    if paquete.get("num_resources") not in (None, len(recursos)):
        resumen.aviso(f"{dataset}: num_resources={paquete.get('num_resources')} pero la API lista {len(recursos)}")
    # 1. Todo lo listado queda anotado en el manifiesto antes de descargar nada:
    # si la ejecución se corta, la siguiente sabe qué ficheros faltan (p.ej. el
    # CSV de un grupo cuyo XLSX sí bajó: sin él no se decide si se consolida)
    listados, ids_listados, pendientes = set(), set(), []
    for recurso in recursos:
        info = clasificar_recurso(recurso, dataset)
        ids_listados.add((info["id"], info["formato"]))
        if info["aviso"]:
            resumen.aviso(f"{info['id']}: {info['aviso']}")
        clave, antes = clave_local(manifiesto, info)
        if clave in listados:
            resumen.fallo(f"{dataset}: el recurso {info['id']} ({info['formato']}) aparece dos veces en la API")
            continue
        listados.add(clave)           # listado (aunque no se pueda descargar): no se retira
        if antes:
            anterior = manifiesto.entradas[antes]
            resumen.aviso(f"{info['id']} ({info['formato']}): el portal lo describe ahora como "
                          f"'{info['descripcion']}' ({'/'.join(str(v) for v in clasificacion(info))}) y antes como "
                          f"'{anterior.get('descripcion')}' ({'/'.join(str(v) for v in clasificacion(anterior))}): "
                          f"se trata como un fichero nuevo ({clave}) y {antes} queda como retirado")
        if not info["url"]:
            resumen.fallo(f"{dataset}: el recurso {info['id']} no tiene URL")
            continue
        entrada = manifiesto.entradas.get(clave, {})
        base = {k: info[k] for k in ("id", "dataset", "descripcion", "nombre_ckan", "categoria", "anio",
                                     "parte", "formato", "url")}
        base.update(archivo=clave, estado="publicado", fecha_listado=ahora_iso())
        manifiesto.entradas[clave] = {**entrada, **base}
        pendientes.append((clave, info, entrada))
    manifiesto.guardar()

    # 2. Descargas. El manifiesto se guarda tras cada una
    for clave, info, entrada in pendientes:
        destino = salida / clave
        motivo = motivo_descarga(destino, entrada, info, anio_actual, comprobar_todo)
        if motivo is None:
            resumen.sin_pedir += 1
            continue
        if destino.exists():
            # una copia que no se llegó a anotar (ejecución cortada) se anota con
            # su fecha antes de que una versión nueva la mande a _historico/
            anotada, sin_anotar = anotar_copia(manifiesto.entradas[clave], destino)
            if sin_anotar:
                manifiesto.entradas[clave] = anotada
                resumen.aviso(f"{clave}: su copia no estaba anotada en el manifiesto (¿ejecución cortada?); "
                              f"se anota con la fecha del fichero ({anotada['fecha_descarga']})")
        try:
            estado, tam, md5, filas = descargar(info["url"], destino, info["formato"], info["categoria"])
        except ErrorDescarga as e:
            # 'ckan' y 'url_descarga' conservan los de la última descarga buena:
            # así la próxima ejecución vuelve a intentarlo
            manifiesto.entradas[clave] = {**manifiesto.entradas[clave], "ultimo_error": f"{ahora_iso()}: {e}"}
            resumen.fallo(f"{clave}: {e}" + ("; se conserva la copia anterior" if destino.exists() else ""))
            manifiesto.guardar()
            continue
        manifiesto.entradas[clave], _ = registrar_descarga(manifiesto.entradas[clave], destino, estado, md5,
                                                           info, filas)
        manifiesto.guardar()
        resumen.descargado(clave, estado, tam, motivo)

    # Ficheros que el portal ya no lista: sus filas se conservan en las tablas
    # con _en_ultima_descarga=False. Un listado que pierde de golpe más de la
    # mitad de lo conocido no retira nada (más probable un fallo del portal).
    # Los que siguen listados con otra clasificación (id reutilizado:
    # clave_local) sí se retiran: su id está en el listado.
    conocidos = [c for c, e in manifiesto.entradas.items() if e.get("dataset") == dataset]
    publicados = [c for c in conocidos if manifiesto.entradas[c].get("estado") != "retirado"]
    faltan = [c for c in publicados if c not in listados]
    reclasificados = [c for c in faltan if (manifiesto.entradas[c].get("id"),
                                            manifiesto.entradas[c].get("formato")) in ids_listados]
    ausentes = [c for c in faltan if c not in reclasificados]
    if ausentes and len(ausentes) * 2 > len(publicados):
        resumen.fallo(f"{dataset}: la API no lista {len(ausentes)} de los {len(publicados)} ficheros conocidos; "
                      "no se marca ninguno como retirado")
        ausentes = []
    for clave in ausentes:
        resumen.aviso(f"{clave}: el portal ya no lo lista; sus filas se conservan con "
                      "_en_ultima_descarga=False")
    for clave in reclasificados + ausentes:
        manifiesto.entradas[clave]["estado"] = "retirado"
        manifiesto.entradas[clave]["fecha_retirada"] = ahora_iso()
    return listados


# ===========================================================================
# LECTURA SIN PÉRDIDAS
# ===========================================================================
BOM = b"\xef\xbb\xbf"
LETRAS_ES = set('áéíóúñüÁÉÍÓÚÑÜ')
BYTES_SIN_CP1252 = (0x81, 0x8D, 0x8F, 0x90, 0x9D)
# Bytes que no forman utf-8 válido, tal como los deja errors='surrogateescape'
PATRON_BYTE_SUELTO = re.compile("[\udc80-\udcff]")
PATRON_NO_ASCII = re.compile("[^\x00-\x7f]")

# Columnas que añade el script a la tabla fiel (van al final, en este orden).
# IGNORAR_FIEL: cambian de una versión a otra del mismo fichero sin que cambie
# el registro (posición, nombre de la versión...): no cuentan en acumular().
META_FIEL = ["_dataset", "_recurso", "_descripcion", "_categoria", "_anio_fichero", "_parte", "_formato",
             "_estructura", "_archivo_origen", "_fila_origen", "_encabezado", "_fecha_descarga"]
IGNORAR_FIEL = ("_fecha_descarga", "_archivo_origen", "_fila_origen", "_estructura", "_encabezado",
                "_descripcion")
# Una fila que casa con una de la versión siguiente pasa a apuntar a ella: el
# enlace (_archivo_origen, _fila_origen) y la cabecera con que se mapea son
# los de la última versión en que aparece el registro
REATRIBUIR_FIEL = ("_archivo_origen", "_fila_origen", "_estructura", "_encabezado", "_fecha_descarga")
PAREJA = "_pareja_en_la_version"      # columna temporal de acumular_version
MARCAS_FIEL = ["_duplicado", "_repetido_en_csv"]
COLUMNAS_RESERVADAS = set(META_FIEL + MARCAS_FIEL + list(COLUMNAS_META) + [PAREJA])


def _latin1(error):
    """Manejador de errores de decodificación: un byte sin carácter en cp1252
    se conserva como el carácter latin-1 del mismo código."""
    return error.object[error.start:error.end].decode("latin-1"), error.end


codecs.register_error("ayto_latin1", _latin1)


def decodificar(datos):
    """(texto, codificación) de un CSV.

    utf-8 (con o sin BOM) si el fichero lo es, también si solo tiene algún
    byte suelto de otra codificación (esos bytes se leen como cp1252). Si no,
    CP850 cuando da más letras castellanas que cp1252: algunos CSV antiguos del
    portal están en la página de códigos de MS-DOS y leídos como cp1252 salían
    "Descripci¢n", "N£mero", "A¤o" (las variantes que tuvieron que añadirse a
    los mapeos) y el texto de todos los registros igual de roto. Si no, cp1252,
    conservando como latin-1 los bytes que no existen en cp1252 (0x81,
    0x8D...): antes uno solo pasaba el fichero entero a latin-1 y '€', comillas
    y guiones de todos sus registros quedaban como caracteres de control.
    """
    try:
        return datos.decode("utf-8-sig"), ("utf-8-sig" if datos.startswith(BOM) else "utf-8")
    except UnicodeDecodeError:
        pass
    # utf-8 con algún byte suelto de otra codificación (más caracteres utf-8
    # válidos fuera de ASCII que bytes sueltos): solo esos bytes se leen como
    # cp1252; antes uno solo pasaba el fichero entero a CP850 y cambiaban todas
    # las tildes. En un fichero cp1252 o CP850 casi ninguna letra con tilde
    # forma una secuencia utf-8 válida.
    texto = datos.decode("utf-8", errors="surrogateescape")
    sueltos = len(PATRON_BYTE_SUELTO.findall(texto))
    validos = len(PATRON_NO_ASCII.findall(texto)) - sueltos
    if validos > sueltos:
        texto = PATRON_BYTE_SUELTO.sub(
            lambda m: bytes([ord(m.group()) - 0xDC00]).decode("cp1252", errors="ayto_latin1"), texto)
        con_bom = datos.startswith(BOM)
        return texto[1:] if con_bom else texto, \
            f"{'utf-8-sig' if con_bom else 'utf-8'} ({sueltos} bytes sueltos como cp1252)"

    def letras(codificacion):
        return sum(c in LETRAS_ES for c in datos.decode(codificacion, errors="replace"))

    if letras("cp850") > letras("cp1252"):
        return datos.decode("cp850"), "cp850"
    indefinidos = sum(datos.count(bytes([b])) for b in BYTES_SIN_CP1252)
    codificacion = f"cp1252 ({indefinidos} bytes sin carácter como latin-1)" if indefinidos else "cp1252"
    return datos.decode("cp1252", errors="ayto_latin1"), codificacion


def detectar_separador(texto):
    """';' (el del portal) salvo que ',', tabulador o '|' aparezcan más en las
    primeras líneas con contenido."""
    lineas = [linea for linea in texto[:200_000].splitlines() if linea.strip()][:5]
    cuentas = {s: sum(linea.count(s) for linea in lineas) for s in (";", ",", "\t", "|")}
    mejor = max(cuentas, key=lambda s: (cuentas[s], s == ";"))
    return mejor if cuentas[mejor] else ";"


def registros_csv(datos):
    """Registros de un CSV, sin descartar ninguno. Devuelve (registros, info).

    Lector principal: pandas con todo como texto (dtype=str,
    keep_default_na=False: 'NA', 'N/A' o 'NULL' no pasan a nulo), sin saltar
    líneas vacías ni mal formadas. Referencia independiente: el módulo csv de
    Python. Los dos deben dar los mismos registros campo a campo; si no (o si
    pandas no puede leer el fichero, p.ej. una comilla sin cerrar), se usan los
    del módulo csv, que conservan todo el texto, y se anota en `info`.
    Las comillas mal formadas (una sin cerrar al final del fichero, que junta
    en un campo todos los registros que siguen, o texto pegado tras unas
    comillas de cierre) se detectan con el módulo csv en modo estricto y se
    anotan en info['error_comillas']: los dos lectores las leen igual, pero el
    resultado no son los registros publicados.
    """
    texto, codificacion = decodificar(datos)
    separador = detectar_separador(texto)
    referencia = list(csv.reader(io.StringIO(texto, newline=""), delimiter=separador))
    info = {"codificacion": codificacion, "separador": separador,
            "lineas_fisicas": texto.count("\n") + (1 if texto and not texto.endswith("\n") else 0),
            "registros": len(referencia)}
    estricto = csv.reader(io.StringIO(texto, newline=""), delimiter=separador, strict=True)
    try:
        for _ in estricto:
            pass
    except csv.Error as e:
        info["error_comillas"] = f"línea {estricto.line_num}: {e}"
    registros = _registros_pandas(texto, separador, referencia, info)
    if info.get("error_comillas"):
        info["aviso_lectura"] = "; ".join(filter(None, [
            info.get("aviso_lectura"), f"comillas mal formadas ({info['error_comillas']})"]))
    return registros, info


def _registros_pandas(texto, separador, referencia, info):
    """Registros de pandas si coinciden con los del módulo csv (`referencia`);
    si no, los del módulo csv. Anota en `info` la comparación."""
    if not referencia:
        info.update(filas_pandas=0, lectores_coinciden=True)
        return referencia
    ancho = max(len(r) for r in referencia)
    try:
        principal = pd.read_csv(io.StringIO(texto), sep=separador, header=None, names=range(ancho),
                                dtype=str, keep_default_na=False, na_filter=False,
                                skip_blank_lines=False, quotechar='"', index_col=False,
                                engine="c").to_numpy(dtype=object).tolist()
    except (pd.errors.ParserError, pd.errors.EmptyDataError, ValueError) as e:
        info.update(filas_pandas=None, lectores_coinciden=False,
                    aviso_lectura=f"pandas no puede leerlo ({type(e).__name__}: {str(e)[:200]}); "
                                  "se usa el módulo csv")
        return referencia
    distintos = [i + 1 for i, (a, b) in enumerate(zip(referencia, principal))
                 if a + [""] * (ancho - len(a)) != b]
    info["filas_pandas"] = len(principal)
    info["lectores_coinciden"] = len(principal) == len(referencia) and not distintos
    if not info["lectores_coinciden"]:
        info["aviso_lectura"] = (f"pandas da {len(principal)} registros y el módulo csv {len(referencia)} "
                                 f"(distintos: {distintos[:10]}); se usa el módulo csv")
        return referencia
    return principal


PATRON_ESCAPE_OOXML = re.compile(r"_x([0-9A-Fa-f]{4})_")


def _texto_celda(valor):
    """Celda de Excel -> texto ('' si está vacía). Enteros sin '.0', fechas en
    ISO y los escapes de OOXML deshechos ('_x000D_' es el retorno de carro que
    el CSV trae tal cual)."""
    if valor is None:
        return ""
    if isinstance(valor, str):
        return PATRON_ESCAPE_OOXML.sub(lambda m: chr(int(m.group(1), 16)), valor)
    if isinstance(valor, np.datetime64):
        valor = pd.Timestamp(valor)
    elif isinstance(valor, np.generic):
        valor = valor.item()
    if isinstance(valor, bool):
        return "TRUE" if valor else "FALSE"
    if isinstance(valor, float):
        if math.isnan(valor):
            return ""
        return str(int(valor)) if valor.is_integer() and abs(valor) < 1e16 else repr(valor)
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, (pd.Timestamp, datetime)):
        if pd.isna(valor):
            return ""
        if (valor.hour, valor.minute, valor.second, valor.microsecond) == (0, 0, 0, 0):
            return valor.date().isoformat()
        return valor.isoformat(sep=" ")
    if isinstance(valor, (date, hora_del_dia)):
        return valor.isoformat()
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        pass
    return str(valor)


def registros_excel(ruta, tipo):
    """Filas (texto) de la hoja con datos de un XLSX (openpyxl) o XLS (xlrd).
    Si hay varias hojas con datos no se adivina cuál es la tabla: error."""
    if tipo == "xlsx":
        import openpyxl
        # Se abre el fichero (no la ruta): openpyxl rechaza por la extensión
        # los .part de una descarga que aún se está comprobando
        with open(ruta, "rb") as f:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")   # "Workbook contains no default style"...
                libro = openpyxl.load_workbook(f, read_only=True, data_only=True)
            try:
                hojas = []
                for hoja in libro.worksheets:
                    hoja.reset_dimensions()       # no fiarse de la dimensión declarada
                    hojas.append((str(hoja.title), [[_texto_celda(v) for v in fila]
                                                    for fila in hoja.iter_rows(values_only=True)]))
            finally:
                libro.close()
    else:
        libros = pd.read_excel(ruta, sheet_name=None, header=None, dtype=object, engine="xlrd",
                               keep_default_na=False, na_values=[])
        hojas = [(str(nombre), [[_texto_celda(v) for v in fila] for fila in df.itertuples(index=False, name=None)])
                 for nombre, df in libros.items()]
    con_datos = [(nombre, filas) for nombre, filas in hojas if any(c.strip() for f in filas for c in f)]
    if len(con_datos) > 1:
        raise ValueError(f"varias hojas con datos ({', '.join(n for n, _ in con_datos)})")
    nombre, filas = con_datos[0] if con_datos else (hojas[0][0] if hojas else "", [])
    return filas, {"hoja": nombre, "registros": len(filas)}


def tipo_contenido(ruta):
    """Formato real por el contenido: xls, xlsx, pdf, html o csv."""
    with open(ruta, "rb") as f:
        cabeza = f.read(4096)
    if cabeza.startswith(FIRMAS["xls"]):
        return "xls"
    if cabeza.startswith(FIRMAS["xlsx"]):
        return "xlsx"
    if cabeza.startswith(FIRMAS["pdf"]):
        return "pdf"
    minus = cabeza.lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    if minus.startswith((b"<!doctype html", b"<html")) or b"<body" in minus[:2048]:
        return "html"
    return "csv"


class Tabla:
    """Un fichero leído sin perder nada.

    df: filas de datos con los nombres de columna originales, todo texto.
    columnas_mapeo: los mismos nombres como los daba pandas (sin espacios en
      los extremos, vacíos 'Unnamed: N', repetidos 'x.1'): los usan los mapeos.
    filas_origen: nº de registro (desde 1, la cabecera cuenta) de cada fila.
    previas: [(registro, 'titulo'|'vacia', texto)] de lo que hay sobre los datos.
    """

    def __init__(self, df, columnas, columnas_mapeo, filas_origen, estructura, cabecera, previas,
                 encabezado, info):
        self.df = df
        self.columnas = columnas
        self.columnas_mapeo = columnas_mapeo
        self.filas_origen = filas_origen
        self.estructura = estructura
        self.cabecera = cabecera
        self.previas = previas
        self.encabezado = encabezado
        self.info = info


def _con_contenido(valor):
    return bool(valor and valor.strip())


def filas_con_datos(tabla):
    """Filas de datos con alguna celda con contenido (una versión sin ninguna
    es una descarga vacía: no retira nada)."""
    return len(tabla.df) - tabla.info.get("filas_sin_contenido", 0)


def _nombres_columnas(celdas, sin_espacios):
    """Nombres de columna de una fila de cabecera. Tal cual (tabla fiel) o sin
    espacios en los extremos (mapeos, como con pandas); vacíos -> 'Unnamed: N',
    repetidos -> 'x.1', 'x.2'. Un nombre que choca con una columna del script
    (p.ej. '_recurso') se renombra a '<nombre> (original)'."""
    nombres, vistos = [], set()
    for j, celda in enumerate(celdas):
        base = (celda.strip() if sin_espacios else celda) if _con_contenido(celda) else f"Unnamed: {j}"
        if base in COLUMNAS_RESERVADAS:
            base = f"{base} (original)"
        nombre, n = base, 0
        while nombre in vistos:
            n += 1
            nombre = f"{base}.{n}"
        vistos.add(nombre)
        nombres.append(nombre)
    return nombres


def _parece_cabecera(celdas):
    """Más de un tercio de las celdas son texto (no números): la antigua prueba
    de 'fila 0 como cabecera' de la lectura sin cabecera."""
    texto = [c.strip() for c in celdas]
    return sum(1 for c in texto if len(c) > 2 and not c.replace('.', '').replace(',', '').isdigit()) \
        > len(texto) // 3


def detectar_cabecera(filas, es_menor, nombre=""):
    """(índice de la fila de cabecera o None, estructura).

    Como la lectura anterior con pandas: la primera fila con contenido, salvo
    que parezca de datos (SKIP_ROW); entonces se prueban las siguientes (títulos
    y filas vacías encima de la cabecera) y, si ninguna vale, la primera fila con
    huecos en la cabecera. Si no hay cabecera: SIN_CABECERA.
    """
    detectar = detectar_estructura_menores if es_menor else detectar_estructura_actividad
    con_datos = [i for i, fila in enumerate(filas) if any(_con_contenido(c) for c in fila)]
    if not con_datos:
        return None, "VACIO"
    ancho = len(filas[0])
    candidatas = [i for i in con_datos if i <= max(MAX_FILA_CABECERA, con_datos[0])]
    primera = candidatas[0]
    estructura = detectar(nombre, _nombres_columnas(filas[primera], True))
    if estructura != "SKIP_ROW":
        return primera, estructura
    for i in candidatas[1:]:
        columnas = _nombres_columnas(filas[i], True)
        if ancho < 5 or i == len(filas) - 1 or \
                sum(1 for c in columnas if c.startswith("Unnamed: ")) > ancho // 2:
            continue
        estructura = detectar(nombre, columnas)
        if estructura not in ("SKIP_ROW", "DESCONOCIDA"):
            return i, estructura
    if _parece_cabecera(filas[primera]):
        con_nombre = [c for c in _nombres_columnas(filas[primera], True) if not c.startswith("Unnamed: ")]
        estructura = detectar(nombre, con_nombre)
        if estructura not in ("SKIP_ROW", "DESCONOCIDA"):
            return primera, estructura
    return None, "SIN_CABECERA"


def tabla_desde_registros(registros, categoria, nombre="", info=None):
    """Tabla a partir de los registros de un fichero (listas de texto).

    Se quitan solo las columnas finales vacías en todos los registros (un
    fichero exportado de Excel trae 16.384 campos por registro); los registros
    cortos se completan con ''. Todas las filas tras la cabecera son datos,
    también las vacías o repetidas.
    """
    info = dict(info or {})
    total = len(registros)
    ancho = 0
    for registro in registros:
        k = len(registro)
        while k > ancho and registro[k - 1] == "":
            k -= 1
        ancho = max(ancho, k)
    filas = [list(r[:ancho]) + [""] * (ancho - len(r)) for r in registros]
    cabecera, estructura = detectar_cabecera(filas, categoria == "contratos_menores", nombre)
    if cabecera is None:
        inicio = next((i for i, fila in enumerate(filas) if any(_con_contenido(c) for c in fila)), total)
        columnas = [f"Unnamed: {j}" for j in range(ancho)]
        columnas_mapeo = list(columnas)
    else:
        inicio = cabecera + 1
        columnas = _nombres_columnas(filas[cabecera], False)
        columnas_mapeo = _nombres_columnas(filas[cabecera], True)
    datos = filas[inicio:]
    df = pd.DataFrame(datos, columns=columnas, dtype=object) if datos else \
        pd.DataFrame({c: pd.Series(dtype=object) for c in columnas})
    separador = info.get("separador") or ";"
    antes = range(cabecera if cabecera is not None else inicio)
    previas = [(i + 1, "titulo" if any(_con_contenido(c) for c in filas[i]) else "vacia",
                separador.join(filas[i]).rstrip(separador)) for i in antes]
    encabezado = " | ".join(c.strip() for i in antes for c in filas[i] if _con_contenido(c)) or None
    info.update(ancho=ancho, fila_cabecera=cabecera + 1 if cabecera is not None else None,
                estructura=estructura, filas_previas=len(previas), filas_datos=len(datos),
                filas_sin_contenido=sum(1 for f in datos if not any(_con_contenido(c) for c in f)),
                celdas_con_valor=sum(1 for f in datos for c in f if c != ""))
    return Tabla(df, columnas, columnas_mapeo, list(range(inicio + 1, total + 1)), estructura, cabecera,
                 previas, encabezado, info)


def leer_tabla(ruta, categoria, nombre=""):
    """Tabla de un fichero descargado (CSV, XLSX o XLS, según su contenido)."""
    ruta = Path(ruta)
    tipo = tipo_contenido(ruta)
    if tipo == "csv":
        registros, info = registros_csv(ruta.read_bytes())
    elif tipo in ("xlsx", "xls"):
        registros, info = registros_excel(ruta, tipo)
    else:
        raise ValueError(f"no es una tabla ({tipo})")
    info["tipo"] = tipo
    return tabla_desde_registros(registros, categoria, nombre or ruta.stem, info)


def mapear_tabla(df, columnas_mapeo, nombre, categoria, estructura, primera=None):
    """Filas de un fichero en el esquema unificado (texto tal cual; celda vacía
    = nulo). `primera`: la primera fila de datos del fichero, si `df` no la
    empieza (los ficheros sin cabecera eligen su disposición por ella).
    Devuelve (DataFrame con COLUMNAS_UNIFICADAS, columnas sin mapear)."""
    datos = pd.DataFrame(df.to_numpy(dtype=object), index=df.index, columns=list(columnas_mapeo))
    mapped = _mapear_fichero(datos, nombre, categoria, estructura, primera)
    df_out = pd.DataFrame(index=datos.index)
    for col_unif in COLUMNAS_UNIFICADAS:
        serie = mapped.get(col_unif)
        df_out[col_unif] = serie.mask(serie == "") if serie is not None else None
    df_out['fuente_fichero'] = nombre
    df_out['categoria'] = categoria
    df_out['estructura'] = estructura
    usadas = {s.name for s in mapped.values() if hasattr(s, 'name')}
    sin_mapear = [c for c in datos.columns if c not in usadas and not c.startswith("Unnamed: ")]
    return df_out[COLUMNAS_UNIFICADAS], sin_mapear


def procesar_fichero(nombre, filepath, categoria=None):
    """Un fichero en el esquema unificado, sin convertir ni eliminar filas.
    Sin categoría se deduce del nombre ('menores_2019', 'cesiones_2022'...)."""
    categoria = categoria or _clasificar_categoria(nombre)
    tabla = leer_tabla(filepath, categoria, nombre)
    print(f"    {nombre}: {tabla.estructura}, {len(tabla.df):,} filas, {len(tabla.columnas)} cols", end="")
    df_out, sin_mapear = mapear_tabla(tabla.df, tabla.columnas_mapeo, nombre, categoria, tabla.estructura)
    print(f" → {df_out['objeto_contrato'].notna().sum()} objeto, "
          f"{df_out['importe_adjudicacion_iva_inc'].notna().sum()} importe")
    if sin_mapear:
        print(f"      Sin mapear (están en la tabla fiel): {sin_mapear}")
    return df_out


# ===========================================================================
# DETECCIÓN DE ESTRUCTURA
# ===========================================================================
def _es_skip_row(columnas):
    """Detect if the 'columns' are actually data (file has no header or title row)."""
    cols_upper = [str(c).upper().strip() for c in columnas]
    # Many Unnamed columns → pandas couldn't parse headers
    if sum(1 for c in cols_upper if 'UNNAMED' in c) > len(cols_upper) // 2:
        return True
    first = cols_upper[0] if cols_upper else ''
    meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
             'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE']
    # First column is a month name → data row
    if first in meses:
        return True
    # First column is a bare year (4 digits, nothing else) → data row
    if first.isdigit() and len(first) == 4 and int(first) > 1990:
        return True
    # First column looks like a date dd/mm/yyyy → data row
    if re.match(r'^\d{2}/\d{2}/\d{4}$', first):
        return True
    # Most columns are numeric → likely data
    n_numeric = sum(1 for c in cols_upper if c.replace('.','').replace(',','').replace('-','').isdigit())
    if n_numeric > len(cols_upper) * 0.7:
        return True
    return False


def detectar_estructura_menores(nombre, columnas):
    cols_upper = [c.upper().strip() for c in columnas]
    cols_str = ' '.join(cols_upper)
    if _es_skip_row(columnas): return 'SKIP_ROW'
    if 'ORG_CONTRATACIÓN' in cols_str or 'ORG_CONTRATACION' in cols_str: return 'D'
    if 'ORGANISMO_CONTRATANTE' in cols_str or 'ORGANISMO CONTRATANTE' in cols_str: return 'F'
    if any('REGISTRO' in c and 'CONTRATO' in c for c in cols_upper): return 'E'
    if any('RECON' in c for c in cols_upper) or \
       any('NUMERO EXPEDIENTE' in c or 'NÚMERO EXPEDIENTE' in c for c in cols_upper): return 'C'
    if any('CE.GESTOR' in c or 'CEGESTOR' in c.replace('.', '') for c in cols_upper): return 'B'
    if any('CENTRO' == c for c in cols_upper): return 'A'
    return 'DESCONOCIDA'


def detectar_estructura_actividad(nombre, columnas):
    cols_upper = [c.upper().strip() for c in columnas]
    cols_str = ' '.join(cols_upper)
    if _es_skip_row(columnas): return 'SKIP_ROW'
    if 'ORGANISMO_CONTRATANTE' in cols_str or 'ORGANISMO CONTRATANTE' in cols_str: return 'AC_2025'
    if any('REGISTRO' in c and 'CONTRATO' in c for c in cols_upper): return 'AC_NEW'
    # Fallback: "N. DE REGISTRO" without "CONTRATO" (truncated headers)
    if any('N. DE REGISTRO' in c for c in cols_upper): return 'AC_NEW'
    if any('FECHA DE INSCRIPCION' in c for c in cols_upper): return 'AC_HOMOLOGACION'
    if any('NUM.CONTRATO' in c for c in cols_upper): return 'AC_OLD_MOD'
    if any('INCIDENCIA' == c for c in cols_upper): return 'AC_OLD_MOD'
    # 2015 modificados variant: has "Nº CONTRATO"/"CONTRATO" and "GESTOR"
    if any('CONTRATO' in c for c in cols_upper) and any('GESTOR' in c for c in cols_upper):
        return 'AC_OLD_MOD'
    if any('DESCRIPCI' in c for c in cols_upper): return 'AC_OLD'
    return 'AC_OLD'


# ===========================================================================
# MAPEO FUNCIONES
# ===========================================================================
def mapear_directo(df, mapa):
    resultado = {}
    for col_orig, col_unif in mapa.items():
        for c in df.columns:
            cs = c.strip()
            # Exact match or case-insensitive
            if cs == col_orig or cs.upper() == col_orig.upper():
                if col_unif not in resultado:
                    resultado[col_unif] = df[c]
                break
            # Fuzzy: normalize spaces
            cs_c = re.sub(r'\s+', ' ', cs)
            co_c = re.sub(r'\s+', ' ', col_orig)
            if cs_c == co_c or cs_c.upper() == co_c.upper():
                if col_unif not in resultado:
                    resultado[col_unif] = df[c]
                break
    return resultado


def mapear_keywords(df, keywords_map):
    resultado = {}
    for col in df.columns:
        norm = strip_normalize(col)
        best_match = None
        best_score = 0
        for keywords, col_unif in keywords_map.items():
            if all(kw in norm for kw in keywords):
                score = len(keywords) * 10 + sum(len(kw) for kw in keywords)
                if score > best_score:
                    best_score = score
                    best_match = col_unif
        if best_match and best_match not in resultado:
            resultado[best_match] = df[col]
    return resultado


def mapear_importes(df, already_mapped):
    """Map importe columns using ordered specificity."""
    resultado = {}
    for col in df.columns:
        norm = strip_normalize(col)
        if 'IMPORTE' not in norm:
            continue
        for keywords, col_unif in MAPA_AC_IMPORTES:
            if col_unif in resultado or col_unif in already_mapped:
                continue
            if all(kw in norm for kw in keywords):
                resultado[col_unif] = df[col]
                break
    return resultado


# ===========================================================================
# MAPEO POR CATEGORÍA Y ESTRUCTURA
# ===========================================================================
def _mapear_fichero(df, nombre, categoria, estructura, primera=None):
    """Dispatch to correct mapping based on category and structure."""

    # === CONTRATOS MENORES ===
    if categoria == "contratos_menores":
        if estructura == 'A': return mapear_directo(df, MAPA_MENORES_A)
        if estructura == 'B': return mapear_directo(df, MAPA_MENORES_B)
        if estructura == 'C': return mapear_directo(df, MAPA_MENORES_C)
        if estructura == 'D': return mapear_directo(df, MAPA_MENORES_D)
        if estructura == 'E': return mapear_keywords(df, MAPA_MENORES_E_KEYWORDS)
        if estructura == 'F':
            return mapear_keywords(df, {**MAPA_MENORES_E_KEYWORDS, **MAPA_MENORES_F_EXTRA})
        return {}

    # === HOMOLOGACIÓN ===
    if categoria == "homologacion":
        if estructura == 'AC_2025':
            return mapear_directo(df, MAPA_HOMOLOGACION)
        return mapear_directo(df, MAPA_HOMOLOGACION)

    # === ESTRUCTURA ANTIGUA: formalizados/acuerdo_marco ===
    if estructura == 'AC_OLD':
        return mapear_directo(df, MAPA_FORMALIZADOS_OLD)

    # === ESTRUCTURA ANTIGUA: modificados ===
    if estructura == 'AC_OLD_MOD':
        return mapear_directo(df, MAPA_MODIFICADOS_OLD)

    # === SIN CABECERA ===
    if estructura == 'SIN_CABECERA':
        return _mapear_sin_cabecera(df, nombre, categoria, primera)

    # === MODERNAS (AC_NEW, AC_2025) ===
    if estructura in ('AC_NEW', 'AC_2025'):
        return _mapear_moderno(df, nombre, categoria, estructura)

    return {}


def _mapear_sin_cabecera(df, nombre, categoria, primera=None):
    """Map files that had no header (read with header=None). La disposición
    se elige por la primera fila de datos del fichero (`primera`, o la de `df`)."""
    ncols = len(df.columns)
    if primera is None and len(df) > 0:
        primera = list(df.iloc[0])

    # Print first row for diagnostics
    if len(df) > 0:
        row0 = [str(df.iloc[0, i])[:30] for i in range(min(ncols, 20))]
        print(f"\n      [DEBUG SIN_CABECERA] {ncols} cols, row0: {row0}")

    if categoria == 'modificados':
        # modificados_2015: 19 cols
        # Layout based on 2016+ AC_OLD_MOD pattern + extra cols:
        # 0=INCIDENCIA/tipo, 1=fecha_inscripcion, 2=num_contrato,
        # 3=num_expediente, 4=gestor, 5=objeto, 6=CIF, 7=adjudicatario,
        # 8=importe_adjudicacion, 9=fecha_form_orig, 10=importe_modif,
        # 11=ingreso_gasto, ...
        # But we need to auto-detect: check if col 0 looks like a date or type
        mapped = {}
        # Heuristic: if col 0 values look like dates (dd/mm/yyyy), it's a date-first layout
        first_val = str(primera[0]).strip() if primera else ''
        if re.match(r'\d{2}/\d{2}/\d{4}', first_val):
            # Date-first layout (like 2016-2018 AC_OLD_MOD):
            # 0=fecha_insc, 1=num_cto, 2=num_exp, 3=gestor, 4=objeto,
            # 5=CIF, 6=adjudicatario, 7=imp_adj, 8=fch_form_inc, 9=imp_modif, 10=ing/gasto
            if ncols >= 1: mapped['fecha_inscripcion'] = df.iloc[:, 0]
            if ncols >= 2: mapped['n_registro_contrato'] = df.iloc[:, 1]
            if ncols >= 3: mapped['n_expediente'] = df.iloc[:, 2]
            if ncols >= 4: mapped['centro_seccion'] = df.iloc[:, 3]
            if ncols >= 5: mapped['objeto_contrato'] = df.iloc[:, 4]
            if ncols >= 6: mapped['nif_adjudicatario'] = df.iloc[:, 5]
            if ncols >= 7: mapped['razon_social_adjudicatario'] = df.iloc[:, 6]
            if ncols >= 8: mapped['importe_adjudicacion_iva_inc'] = df.iloc[:, 7]
            if ncols >= 9: mapped['fecha_formalizacion_incidencia'] = df.iloc[:, 8]
            if ncols >= 10: mapped['importe_modificacion'] = df.iloc[:, 9]
            if ncols >= 11: mapped['ingreso_gasto'] = df.iloc[:, 10]
        else:
            # Type-first layout (like 2019-2020):
            # 0=incidencia, 1=fecha_insc, 2=num_cto, 3=num_exp, 4=gestor,
            # 5=objeto, 6=CIF, 7=adjudicatario, 8=imp_adj, 9=fch_form, 10=imp_modif
            if ncols >= 1: mapped['tipo_incidencia'] = df.iloc[:, 0]
            if ncols >= 2: mapped['fecha_inscripcion'] = df.iloc[:, 1]
            if ncols >= 3: mapped['n_registro_contrato'] = df.iloc[:, 2]
            if ncols >= 4: mapped['n_expediente'] = df.iloc[:, 3]
            if ncols >= 5: mapped['centro_seccion'] = df.iloc[:, 4]
            if ncols >= 6: mapped['objeto_contrato'] = df.iloc[:, 5]
            if ncols >= 7: mapped['nif_adjudicatario'] = df.iloc[:, 6]
            if ncols >= 8: mapped['razon_social_adjudicatario'] = df.iloc[:, 7]
            if ncols >= 9: mapped['importe_adjudicacion_iva_inc'] = df.iloc[:, 8]
            if ncols >= 10: mapped['fecha_formalizacion_incidencia'] = df.iloc[:, 9]
            if ncols >= 11: mapped['importe_modificacion'] = df.iloc[:, 10]
            if ncols >= 12: mapped['ingreso_gasto'] = df.iloc[:, 11]
        return mapped

    elif categoria == 'prorrogados':
        # prorrogados_2021: 18 cols
        # First check if row 0 col 0 looks like a type name
        mapped = {}
        first_val = str(primera[0]).strip().upper() if primera else ''
        if 'PRORROGA' in first_val or 'MODIFICACION' in first_val or len(first_val) < 30:
            # Type-first layout: TIPO_INC, FCH_INSC_CTO, N_REG_CTO, N_EXP,
            # CENTRO, ORGANO, OBJETO, TIPO_CTO, NIF, RAZON_SOCIAL,
            # CENTRO_INC, FCH_FORM_INC, IMP_PRORROGA, N_REG_INC,
            # ING/GASTO, IMP_ADJ, PLAZO, ...
            col_map = {
                0: "tipo_incidencia", 1: "fecha_inscripcion_contrato",
                2: "n_registro_contrato", 3: "n_expediente",
                4: "centro_seccion", 5: "organo_contratacion",
                6: "objeto_contrato", 7: "tipo_contrato",
                8: "nif_adjudicatario", 9: "razon_social_adjudicatario",
                10: "centro_seccion_incidencia",
                11: "fecha_formalizacion_incidencia",
                12: "importe_prorroga", 13: "n_registro_incidencia",
                14: "ingreso_gasto", 15: "importe_adjudicacion_iva_inc",
                16: "plazo",
            }
        else:
            col_map = {
                0: "fecha_inscripcion_contrato",
                1: "n_registro_contrato", 2: "n_expediente",
                3: "centro_seccion", 4: "organo_contratacion",
                5: "objeto_contrato", 6: "tipo_contrato",
                7: "nif_adjudicatario", 8: "razon_social_adjudicatario",
                9: "centro_seccion_incidencia",
                10: "fecha_formalizacion_incidencia",
                11: "importe_prorroga", 12: "n_registro_incidencia",
                13: "ingreso_gasto", 14: "importe_adjudicacion_iva_inc",
                15: "plazo",
            }
        for idx, col_unif in col_map.items():
            if idx < ncols:
                mapped[col_unif] = df.iloc[:, idx]
        return mapped

    return {}


def _mapear_moderno(df, nombre, categoria, estructura):
    """Map files with modern structure (AC_NEW, AC_2025)."""
    # Base keywords
    mapped = mapear_keywords(df, MAPA_AC_MODERN_KEYWORDS)

    # Importes (ordered specificity)
    mapped_imp = mapear_importes(df, mapped)
    mapped.update(mapped_imp)

    # Category-specific extras (keyword-based)
    extra_maps = {
        'contratos_formalizados': MAPA_EXTRA_FORMALIZADOS,
        'acuerdo_marco': MAPA_EXTRA_ACUERDO_MARCO,
        'modificados': MAPA_EXTRA_MODIFICADOS,
        'prorrogados': MAPA_EXTRA_PRORROGADOS,
        'penalidades': MAPA_EXTRA_PENALIDADES,
        'cesiones': MAPA_EXTRA_CESIONES,
        'resoluciones': MAPA_EXTRA_RESOLUCIONES,
    }
    if categoria in extra_maps:
        extra = mapear_keywords(df, extra_maps[categoria])
        for k, v in extra.items():
            if k not in mapped:
                mapped[k] = v

    # Acuerdo marco: direct name matching for C.B./CB/CESDA derivado columns
    if categoria == 'acuerdo_marco':
        directo = mapear_directo(df, MAPA_DIRECTO_ACUERDO_MARCO_DERIVADOS)
        for k, v in directo.items():
            if k not in mapped:
                mapped[k] = v

    # Direct column matches that keywords can't handle well
    for col in df.columns:
        cs = col.strip()
        # CENTRO - SECCION INC. → centro_seccion_incidencia
        if 'CENTRO' in cs.upper() and 'INC' in cs.upper() and 'centro_seccion_incidencia' not in mapped:
            # Only match if it's the incidence variant (has INC)
            if 'INC' in cs.upper().replace('INSCRIPCION', '').replace('INCL', ''):
                mapped['centro_seccion_incidencia'] = df[col]
        # PROCEDIMIENTO DE ADJUDICACION.1 → procedimiento_adjudicacion
        if 'PROCEDIMIENTO' in cs.upper() and 'procedimiento_adjudicacion' not in mapped:
            mapped['procedimiento_adjudicacion'] = df[col]

    return mapped


# ===========================================================================
# IMPORTES
# ===========================================================================
def importe_excel(valor):
    """Importe de una celda de un XLSX/XLS (_texto_celda): el número con
    punto decimal tal cual; una celda de texto, como en los CSV."""
    if isinstance(valor, str):
        try:
            return float(valor.strip())
        except ValueError:
            pass
    return normalizar_importe(valor)


def normalizar_importe(valor):
    if pd.isna(valor) or str(valor).strip() == '':
        return None
    s = str(valor).strip()
    s = re.sub(r'[€\x80?]', '', s).strip()
    if not s: return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    elif re.fullmatch(r'-?[1-9]\d{0,2}(\.\d{3})+', s):
        # Solo puntos de miles, sin decimales: "15.000" → 15000, "1.234.567" → 1234567
        s = s.replace('.', '')
    try:
        return float(s)
    except ValueError:
        return None


# ===========================================================================
# TIPOS PARA ANÁLISIS (sin perder el texto publicado)
# ===========================================================================
COLUMNAS_IMPORTE = [c for c in COLUMNAS_UNIFICADAS if 'importe' in c or 'presupuesto' in c or 'valor' in c]
COLUMNAS_FECHA = [c for c in COLUMNAS_UNIFICADAS if 'fecha' in c]
# Criterio de "fila vacía" de antes (esas filas se eliminaban; ahora se marcan)
COLUMNAS_FILA_CON_DATOS = ['objeto_contrato', 'importe_adjudicacion_iva_inc', 'importe_penalidad',
                           'importe_modificacion', 'importe_prorroga', 'causa_resolucion',
                           'causas_generales', 'n_registro_contrato', 'objeto_derivado',
                           'titulo_expediente', 'objeto_sh', 'importe_cedido']
COLUMNAS_ANIO = ['fecha_adjudicacion', 'fecha_formalizacion', 'fecha_inscripcion',
                 'fecha_aprobacion_derivado', 'fecha_inscripcion_contrato',
                 'fecha_formalizacion_incidencia', 'fecha_acuerdo_penalidad',
                 'fecha_acuerdo_resolucion', 'fecha_autorizacion_cesion',
                 'fecha_peticion_cesion', 'fecha_inicio']
UNIFICAR_TIPO_CONTRATO = {
    'Suministros': 'Suministro', 'Servicio': 'Servicios',
    'Contrato De Servicios': 'Servicios', 'Contrato De Obras': 'Obras',
    'Contrato Privado': 'Privado',
    'Contrato Administrativo Especial': 'Administrativo Especial',
}
# Columnas de la tabla fiel que se copian a la unificada (enlace y control)
META_UNIFICADA = ["_recurso", "_formato", "_anio_fichero", "_archivo_origen", "_fila_origen",
                  "_duplicado", "_repetido_en_csv", "_fecha_descarga", *COLUMNAS_META]


def convertir_fechas(serie):
    fechas = pd.to_datetime(serie, format='mixed', dayfirst=True, errors='coerce')
    # Fechas con el año delante (aaaa-mm-dd): pandas 3 les aplica dayfirst e
    # intercambia día y mes ("2025-03-05" → 3 de mayo); se parsean sin dayfirst
    es_iso = serie.astype(str).str.match(r'\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}')
    if es_iso.any():
        fechas[es_iso] = pd.to_datetime(serie[es_iso], format='mixed', errors='coerce')
    return fechas


def normalizar_tipo_contrato(serie):
    tipo = serie.map(lambda v: v.strip().title() if isinstance(v, str) and v.strip() else None)
    tipo = tipo.replace(UNIFICAR_TIPO_CONTRATO)
    tipo[tipo.map(lambda v: isinstance(v, str) and 'suministro' in v.lower())] = 'Suministro'
    return tipo


def convertir_unificada(df):
    """Tipos para análisis sin perder nada de lo publicado.

    - Importes a número y fechas a fecha; el texto publicado de cada columna
      convertida queda en <columna>_texto (un valor que no se entiende como
      número o fecha es nulo en la columna convertida, nunca en la de texto).
    - Los importes de las filas de un XLSX/XLS son el número de la celda con
      punto decimal ('1.125' es 1,125, no 1.125 miles): solo si la celda es
      texto se lee con el formato español de los CSV.
    - tipo_contrato y pyme tal cual; la unificación de antes ('Suministros' ->
      'Suministro'...; pyme sin espacios y en mayúsculas) va en
      tipo_contrato_normalizado y pyme_normalizado.
    - No se elimina ninguna fila: las que antes se quitaban por no tener
      objeto, importe ni nº de registro quedan con _fila_vacia=True.
    - anio: año de la adjudicación, o de otra fecha, o el del fichero.
    """
    df = df.copy()
    textos = {}
    excel = df["_formato"].isin(["xlsx", "xls"]).tolist() if "_formato" in df.columns else [False] * len(df)
    convertir = [importe_excel if e else normalizar_importe for e in excel]
    for col in COLUMNAS_IMPORTE:
        textos[f"{col}_texto"] = df[col].astype(object)
        valores = [f(v) for f, v in zip(convertir, df[col].astype(object))]
        df[col] = pd.to_numeric(pd.Series(valores, index=df.index, dtype=object), errors='coerce').astype(float)
    for col in COLUMNAS_FECHA:
        textos[f"{col}_texto"] = df[col].astype(object)
        df[col] = convertir_fechas(df[col].astype(object))
    con_datos = pd.Series(False, index=df.index)
    for col in COLUMNAS_FILA_CON_DATOS:
        texto = textos.get(f"{col}_texto", df[col])
        con_datos |= texto.map(lambda v: isinstance(v, str) and v.strip() != "").astype(bool)
    df['tipo_contrato_normalizado'] = normalizar_tipo_contrato(df['tipo_contrato'])
    df['pyme_normalizado'] = df['pyme'].map(
        lambda v: v.strip().upper() if isinstance(v, str) and v.strip() else None).astype(object)
    anio = df['fecha_adjudicacion'].dt.year.astype(float)
    for col in COLUMNAS_ANIO[1:]:
        anio = anio.fillna(df[col].dt.year.astype(float))
    anio = anio.fillna(pd.to_numeric(df['_anio_fichero'], errors='coerce').astype(float))
    df['anio'] = anio
    df['_fila_vacia'] = ~con_datos
    for col, serie in textos.items():
        df[col] = serie
    orden = (COLUMNAS_UNIFICADAS + ['anio', 'tipo_contrato_normalizado', 'pyme_normalizado'] + list(textos)
             + ['_fila_vacia'] + META_UNIFICADA)
    return df[[c for c in orden if c in df.columns]]


# ===========================================================================
# TABLA FIEL E HISTÓRICO (todas las versiones de cada fichero)
# ===========================================================================
# Sello de una versión en _historico/ (archivar añade _1, _1_2... si coincide)
PATRON_SELLO = re.compile(r"(\d{8})T(\d{2})(\d{2})(\d{2})Z(?:_\d+)*")


def versiones_con_fecha(destino, entrada=None):
    """[(ruta, fecha de descarga, vigente)] de la versión más antigua a la
    actual: las de _historico/ con la fecha de su sello y la vigente con su
    fecha de modificación, que es la que tendrá su sello cuando pase a
    _historico/ (y la fecha_descarga del manifiesto): así la fecha de una
    versión no cambia al archivarla."""
    salida = []
    for ruta in versiones(destino):
        if ruta == destino:
            salida.append((ruta, iso_de_epoch(ruta.stat().st_mtime), True))
            continue
        # el glob 'X__*' de versiones() casaría también con las de otro 'X__algo'
        m = PATRON_SELLO.fullmatch(ruta.stem[len(destino.stem) + 2:])
        if m:
            d = m.group(1)
            salida.append((ruta, f"{d[:4]}-{d[4:6]}-{d[6:]}T{m.group(2)}:{m.group(3)}:{m.group(4)}Z", False))
    return salida


def fila_informe(clave, entrada, archivo, version, fecha, consolidado, motivo, tabla=None, error=None):
    """Fila de informes/lectura_ficheros.csv."""
    fila = {"archivo": archivo, "recurso": entrada.get("id"), "dataset": entrada.get("dataset"),
            "categoria": entrada.get("categoria"), "anio": entrada.get("anio"), "parte": entrada.get("parte"),
            "formato": entrada.get("formato"), "version": version, "fecha_descarga": fecha,
            "consolidado": consolidado, "motivo": motivo}
    if tabla is not None:
        info = tabla.info
        fila.update({k: info.get(k) for k in (
            "tipo", "codificacion", "separador", "hoja", "lineas_fisicas", "registros", "filas_pandas",
            "lectores_coinciden", "fila_cabecera", "estructura", "filas_previas", "filas_datos",
            "filas_sin_contenido", "celdas_con_valor", "ancho", "aviso_lectura")})
        fila["celdas_con_valor_tabla"] = int((tabla.df != "").sum().sum()) if len(tabla.df.columns) else 0
        fila["cuadra"] = (info.get("registros") == info.get("filas_previas", 0) + info.get("filas_datos", 0)
                          + (1 if info.get("fila_cabecera") else 0)
                          and fila["celdas_con_valor_tabla"] == info.get("celdas_con_valor"))
    if error:
        fila["error"] = error
    return fila


def acumular_version(acumulado, df, fecha):
    """acumular() de una versión de un fichero (`df`, con las columnas de la
    tabla fiel) sobre las anteriores (`acumulado`), ámbito = recurso.

    - Se comparan todas las columnas que ha tenido el fichero: las que no
      tiene una de las dos partes cuentan como nulas en ella (celda vacía = ''
      y nulo = la columna no existe en esa versión). Con otra cabecera
      (columna añadida, quitada o renombrada) ningún registro casa con uno de
      la versión anterior: los anteriores quedan con _en_ultima_descarga=False
      y los nuevos con True, y ninguna fila mezcla valores de dos registros.
    - Una fila que casa pasa a apuntar a la versión nueva (REATRIBUIR_FIEL):
      _archivo_origen y _fila_origen son siempre los de la última versión en
      que aparece el registro, y la tabla unificada la mapea con esa cabecera.
    """
    df = df.copy()
    df[PAREJA] = np.arange(len(df))
    n_antes = 0
    if acumulado is not None:
        n_antes = len(acumulado)
        acumulado = acumulado.copy()
        for c in acumulado.columns:
            if c not in df.columns and c not in COLUMNAS_META:
                df[c] = pd.Series([None] * len(df), index=df.index, dtype=object)
        for c in df.columns:
            if c not in acumulado.columns and c != PAREJA:
                acumulado[c] = pd.Series([None] * n_antes, index=acumulado.index, dtype=object)
    # PAREJA no está en `acumulado`: no cuenta al comparar y acumular copia en
    # cada fila anterior que casa la posición de su pareja en `df`
    out = acumular(acumulado, df, fecha, ambito=["_recurso"], ignorar=IGNORAR_FIEL)
    pareja = out[PAREJA].iloc[:n_antes]
    casadas = np.flatnonzero(pareja.notna().to_numpy())
    if len(casadas) != len(df) - (len(out) - n_antes):      # filas de `df` que no son altas
        raise RuntimeError("acumular() no ha copiado la pareja de cada fila que casa")
    if len(casadas):
        posiciones = pareja.iloc[casadas].to_numpy().astype("int64")
        for columna in REATRIBUIR_FIEL:
            out.loc[casadas, columna] = df[columna].to_numpy()[posiciones]
    return out.drop(columns=[PAREJA])


def acumular_recurso(salida, clave, entrada, ultima, informe, fuera, resumen, motivo):
    """Filas de TODAS las versiones de un fichero acumuladas en orden
    cronológico (acumular_version). Devuelve (tabla fiel del recurso o None,
    {versión: columnas, estructura, primera fila y columnas sin mapear})."""
    destino = Path(salida) / clave
    nombre = Path(clave).stem
    lista = versiones_con_fecha(destino, entrada)
    acumulado, infos, con_datos_antes, columnas_antes = None, {}, 0, None
    for n, (ruta, fecha, vigente) in enumerate(lista):
        archivo = ruta.relative_to(salida).as_posix()
        version = "vigente" if vigente else "historico"
        try:
            tabla = ultima if n == len(lista) - 1 and isinstance(ultima, Tabla) else \
                leer_tabla(ruta, entrada["categoria"], nombre)
        except Exception as e:  # noqa: BLE001 - se informa y se sigue con las demás versiones
            error = f"{type(e).__name__}: {e}"
            informe.append(fila_informe(clave, entrada, archivo, version, fecha, True, motivo, error=error))
            resumen.fallo(f"{archivo}: no se puede leer ({error}); sus filas no entran en las tablas")
            continue
        informe.append(fila_informe(clave, entrada, archivo, version, fecha, True, motivo, tabla))
        if tabla.info.get("error_comillas"):
            resumen.fallo(f"{archivo}: {tabla.info['aviso_lectura']}; sus registros no se pueden separar "
                          "bien: revisa el fichero")
        elif tabla.info.get("aviso_lectura"):
            resumen.aviso(f"{archivo}: {tabla.info['aviso_lectura']}")
        fuera.extend({"archivo": archivo, "registro": r, "tipo": t, "texto": x} for r, t, x in tabla.previas)
        con_datos = filas_con_datos(tabla)
        if con_datos == 0:
            # versión vacía (solo cabecera o filas sin datos): no retira nada; sus
            # filas vacías quedan en el informe de líneas fuera de la tabla
            resumen.aviso(f"{archivo}: sin registros con datos; no se marca nada como retirado")
            separador = tabla.info.get("separador") or ";"
            fuera.extend({"archivo": archivo, "registro": r, "tipo": "vacia",
                          "texto": separador.join(f).rstrip(separador)}
                         for r, f in zip(tabla.filas_origen, tabla.df.itertuples(index=False, name=None)))
            continue
        if con_datos * 2 < con_datos_antes:
            resumen.aviso(f"{archivo}: {con_datos:,} registros con datos frente a {con_datos_antes:,} de la "
                          "versión anterior (los que faltan se conservan con _en_ultima_descarga=False)")
        if columnas_antes is not None and set(tabla.columnas) != set(columnas_antes):
            nuevas = [c for c in tabla.columnas if c not in columnas_antes]
            quitadas = [c for c in columnas_antes if c not in tabla.columnas]
            resumen.aviso(f"{archivo}: la cabecera cambió respecto a la versión anterior (columnas nuevas: "
                          f"{nuevas[:5]}; ya no están: {quitadas[:5]}): sus registros son versiones nuevas y los "
                          "de la anterior quedan con _en_ultima_descarga=False")
        con_datos_antes, columnas_antes = con_datos, tabla.columnas
        df = tabla.df.copy()
        valores = {"_dataset": entrada.get("dataset"), "_recurso": entrada.get("id"),
                   "_descripcion": entrada.get("descripcion"), "_categoria": entrada.get("categoria"),
                   "_anio_fichero": entrada.get("anio"), "_parte": entrada.get("parte"),
                   "_formato": entrada.get("formato"), "_estructura": tabla.estructura,
                   "_archivo_origen": archivo}
        for columna, valor in valores.items():
            df[columna] = pd.Series([valor] * len(df), index=df.index, dtype=object)
        df["_fila_origen"] = tabla.filas_origen
        df["_encabezado"] = pd.Series([tabla.encabezado] * len(df), index=df.index, dtype=object)
        df["_fecha_descarga"] = pd.Series([fecha] * len(df), index=df.index, dtype=object)
        primera = tabla.df.iloc[0].tolist()
        infos[archivo] = {"columnas": tabla.columnas, "columnas_mapeo": tabla.columnas_mapeo,
                          "estructura": tabla.estructura, "primera": primera,
                          "sin_mapear": mapear_tabla(tabla.df.iloc[:0], tabla.columnas_mapeo, nombre,
                                                     entrada["categoria"], tabla.estructura, primera)[1]}
        acumulado = acumular_version(acumulado, df, fecha)
    if acumulado is None:
        return None, infos
    if entrada.get("estado") == "retirado":
        acumulado["_en_ultima_descarga"] = False
    originales = list(dict.fromkeys(c for info in infos.values() for c in info["columnas"]))
    acumulado["_duplicado"] = acumulado.duplicated(subset=originales, keep="first")
    return acumulado, infos


def unificar_recurso(acumulado, infos, nombre, categoria):
    """Tabla unificada de un recurso desde su tabla fiel, fila a fila: cada fila
    se mapea con la cabecera y la estructura de su versión (_archivo_origen:
    la última en que aparece el registro)."""
    trozos = []
    for archivo, posiciones in acumulado.groupby("_archivo_origen", sort=False).indices.items():
        info = infos[archivo]
        filas = acumulado.iloc[posiciones]
        uni, _ = mapear_tabla(filas[info["columnas"]], info["columnas_mapeo"], nombre, categoria,
                              info["estructura"], info.get("primera"))
        trozos.append(uni)
    uni = pd.concat(trozos).loc[acumulado.index]
    for columna in META_UNIFICADA:
        uni[columna] = acumulado[columna].to_numpy()
    return uni


# ===========================================================================
# CSV FRENTE A XLSX/XLS
# ===========================================================================
@functools.lru_cache(maxsize=1 << 18)
def valor_comparable(valor, origen):
    """Valor normalizado para comparar el CSV con su XLSX/XLS: fechas en ISO,
    números con 2 decimales (el CSV en formato español, el Excel con punto),
    texto en mayúsculas sin tildes y con los espacios colapsados."""
    s = " ".join(str(valor).split())
    if not s:
        return ""
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})(?: \d{1,2}:\d{2}(?::\d{2})?)?", s)
    if m:
        anio = int(m.group(3)) + (2000 if len(m.group(3)) == 2 else 0)
        return f"{anio:04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})(?:[ T]\d{2}:\d{2}(?::\d{2})?)?", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    numero = s.replace("€", "").replace("\x80", "").strip()
    if origen == "excel":
        try:
            return f"{float(numero):.2f}"
        except ValueError:
            pass
    importe = normalizar_importe(numero) if re.search(r"\d", numero) else None
    if importe is not None and math.isfinite(importe):
        return f"{importe:.2f}"
    return _sin_acentos(s).upper()


def _nombre_comparable(nombre):
    return re.sub(r"[^A-Z0-9]", "", _sin_acentos(str(nombre)).upper())


def claves_filas(df, columnas_mapeo, origen):
    """Clave de cada fila para comparar ficheros: pares (columna, valor)
    normalizados de sus celdas con valor, sin depender del orden de las
    columnas. None para las filas sin contenido."""
    nombres = [_nombre_comparable(c) for c in columnas_mapeo]
    claves = []
    for fila in df.itertuples(index=False, name=None):
        pares = tuple(sorted((n, v) for n, v in ((n, valor_comparable(x, origen) if isinstance(x, str) else "")
                                                   for n, x in zip(nombres, fila)) if v))
        claves.append(pares or None)
    return claves


def comparar_filas(filas_csv, filas_x):
    """Compara dos ficheros por sus claves de fila, emparejando cada fila del
    Excel con como mucho una del CSV (y al revés). Devuelve (iguales, casi
    iguales, filas del Excel sin pareja, nº de filas del CSV sin pareja,
    {columna: casi iguales con esa diferencia}, {columna: (valor CSV, valor
    Excel)}, [por cada fila de filas_x: si tiene pareja igual o casi igual]).
    'Casi igual': una sola celda distinta (otro valor, otro nombre de columna,
    o con valor en un fichero y vacía en el otro), p.ej. el N. DE EXPEDIENTE
    que el CSV publica en notación científica ('1,91E+11')."""
    csv_ = Counter(f for f in filas_csv if f)
    exc = Counter(f for f in filas_x if f)
    libres = csv_ & exc
    iguales = sum(libres.values())
    resto_csv = list((csv_ - exc).elements())
    emparejadas, resto_x = [False] * len(filas_x), []
    for k, fila in enumerate(filas_x):
        if fila and libres[fila] > 0:
            libres[fila] -= 1
            emparejadas[k] = True
        elif fila:
            resto_x.append((k, fila))
    sin_una, exactas = defaultdict(list), defaultdict(list)
    for i, fila in enumerate(resto_csv):
        exactas[fila].append(i)
        for j, par in enumerate(fila):
            sin_una[fila[:j] + fila[j + 1:]].append((i, par))
    usadas = set()

    def libre(candidatas):
        return next((c for c in candidatas if c[0] not in usadas), None)

    casi, diferencias, ejemplos, sin_pareja = 0, Counter(), {}, []
    for k, fila in resto_x:
        hallada = None                            # (fila del CSV, par del CSV, par del Excel)
        candidata = libre(sin_una.get(fila, ()))
        if candidata:                             # el CSV tiene una celda con valor de más
            hallada = (candidata[0], candidata[1], None)
        else:
            for j, par in enumerate(fila):
                resto = fila[:j] + fila[j + 1:]
                candidata = libre(sin_una.get(resto, ()))
                if candidata:                     # una celda distinta
                    hallada = (candidata[0], candidata[1], par)
                    break
                i = next((i for i in exactas.get(resto, ()) if i not in usadas), None)
                if i is not None:                 # el Excel tiene una celda con valor de más
                    hallada = (i, None, par)
                    break
        if hallada is None:
            sin_pareja.append(fila)
            continue
        i, par_csv, par_x = hallada
        casi += 1
        usadas.add(i)
        emparejadas[k] = True
        if par_csv and par_x and par_csv[0] != par_x[0]:
            columna = f"{par_csv[0]} / {par_x[0]}"
        else:
            columna = (par_x or par_csv)[0]
        diferencias[columna] += 1
        ejemplos.setdefault(columna, (par_csv[1] if par_csv else "", par_x[1] if par_x else ""))
    return iguales, casi, sin_pareja, len(resto_csv) - len(usadas), diferencias, ejemplos, emparejadas


def _grupo(entrada):
    """Grupo de un fichero: conjunto, categoría, año y parte."""
    return (entrada["dataset"], entrada["categoria"], entrada.get("anio") or 0, entrada.get("parte") or "")


def _publicado(entrada):
    return entrada.get("estado") != "retirado"


def decidir_consolidacion(tabulares, ultimas, resumen, pendientes=None):
    """Qué ficheros entran en las tablas y comparación de cada XLSX/XLS con el
    CSV de su grupo (categoría, año y parte).

    Entran todos los CSV. Un XLSX/XLS entra si menos de UMBRAL_XLSX_EN_CSV de
    sus filas están (iguales o casi iguales: comparar_filas) en los CSV
    publicados de su grupo o (iguales) en otro CSV publicado del conjunto. En
    un grupo sin CSV publicado entra el primero (los publicados antes que los
    retirados) y cada uno de los demás si menos de ese umbral de sus filas
    está en los ya consolidados del grupo o en algún CSV. Desde que entra, lo
    hace siempre (consolidado_desde en su entrada del manifiesto), para que sus
    filas no desaparezcan de las tablas si el portal corrige después el CSV,
    aunque su última versión no se pueda leer (entonces es un fallo).

    No se decide sobre un XLSX/XLS (fallo) mientras un CSV de su grupo que el
    portal lista (`pendientes`: sin ninguna copia) no se haya descargado: si
    no, un fallo pasajero del CSV consolidaría para siempre su gemelo. Un
    XLSX/XLS que no se puede leer es un fallo: no se sabe si trae registros
    que no están en ningún CSV.

    Devuelve ({clave: motivo}, filas del informe, {conjunto: claves de fila de
    sus CSV publicados}, {clave de XLSX/XLS consolidado: claves de fila de los
    CSV publicados de su grupo}).
    """
    consolidar = {clave: "CSV" for clave, e in tabulares.items() if e["formato"] == "csv"}
    claves_csv, filas_por_csv = {}, {}
    for clave in consolidar:
        tabla = ultimas.get(clave)
        if isinstance(tabla, Tabla):
            filas = claves_filas(tabla.df, tabla.columnas_mapeo, "csv")
            filas_por_csv[clave] = filas
            if _publicado(tabulares[clave]):
                indice = claves_csv.setdefault(tabulares[clave]["dataset"], {})
                for fila in filas:
                    if fila:
                        indice.setdefault(fila, set()).add(clave)
    grupos, pendientes_grupo = {}, {}
    for clave, e in tabulares.items():
        grupos.setdefault(_grupo(e), []).append(clave)
    for clave, e in (pendientes or {}).items():
        if e.get("formato") == "csv":
            pendientes_grupo.setdefault(_grupo(e), []).append(clave)
    informe, referencias = [], {}
    for grupo, claves in sorted(grupos.items()):
        dataset, categoria, anio, parte = grupo
        csvs = [c for c in claves if tabulares[c]["formato"] == "csv"]
        publicados = [c for c in csvs if _publicado(tabulares[c])]
        pendientes_csv = sorted(pendientes_grupo.get(grupo, []))
        otros = sorted((c for c in claves if c not in csvs),
                       key=lambda c: (not _publicado(tabulares[c]), FORMATOS_TABLA.index(tabulares[c]["formato"]), c))
        filas_ref = [f for c in publicados for f in filas_por_csv.get(c, [])]
        base = {"dataset": dataset, "categoria": categoria, "anio": anio or None, "parte": parte or None,
                "csv": " | ".join(publicados or csvs) or None, "filas_csv": sum(1 for f in filas_ref if f)}
        if not otros:
            informe.append({**base, "xlsx": None, "consolidado_xlsx": False, "motivo": "sin XLSX/XLS"})
            continue
        consolidados = []                          # claves de fila de los XLSX/XLS que ya entran
        for clave in otros:
            fila = {**base, "xlsx": clave}
            desde = tabulares[clave].get("consolidado_desde")
            tabla = ultimas.get(clave)
            if not isinstance(tabla, Tabla):
                fila.update(consolidado_xlsx=bool(desde), error=str(tabla),
                            motivo=f"consolidado desde {desde}" if desde else "no se puede leer")
                if desde:
                    # acumular_recurso da el fallo de cada versión que no se puede leer
                    consolidar[clave] = f"consolidado desde {desde}"
                else:
                    resumen.fallo(f"{clave}: no se puede leer ({tabla}); no se sabe si trae registros que no "
                                  "están en ningún CSV y sus filas no entran en las tablas")
                informe.append(fila)
                continue
            filas_x = claves_filas(tabla.df, tabla.columnas_mapeo, "excel")
            referencia = filas_ref if publicados else [f for filas in consolidados for f in filas]
            iguales, casi, sin_pareja, solo_ref, diferencias, ejemplos, _ = comparar_filas(referencia, filas_x)
            indice = claves_csv.get(dataset, {})
            en_otro = Counter(c for f in sin_pareja for c in indice.get(f, ()) if c not in publicados)
            n_otro = sum(1 for f in sin_pareja if f in indice)
            n_x = sum(1 for f in filas_x if f)
            fraccion = (iguales + casi + n_otro) / n_x if n_x else 1.0
            if pendientes_csv:
                motivo = None
            elif not publicados and not consolidados:
                motivo = "su grupo no tiene CSV publicado"
            elif fraccion < UMBRAL_XLSX_EN_CSV:
                motivo = (f"solo el {fraccion:.0%} de sus filas está en algún CSV del conjunto" if publicados else
                          f"solo el {fraccion:.0%} de sus filas está en otro XLSX/XLS consolidado de su grupo "
                          "o en algún CSV")
            else:
                motivo = None
            if motivo is None and desde:
                # una vez en las tablas, sus filas no desaparecen aunque cambie el CSV
                motivo = f"consolidado desde {desde}"
            elif motivo is None and pendientes_csv:
                resumen.fallo(f"{clave}: el CSV de su grupo ({', '.join(pendientes_csv)}) no se ha descargado; "
                              "no se decide si se consolida y sus filas no entran en las tablas hasta entonces")
            if motivo:
                consolidar[clave] = motivo
                tabulares[clave].setdefault("consolidado_desde", ahora_iso())
                resumen.aviso(f"{clave}: se consolida también ({motivo})")
                consolidados.append(filas_x)
                referencias[clave] = filas_ref
            cabecera_csv = ultimas.get(publicados[0]) if publicados else None
            # nombres legibles de las columnas del informe (la clave los normaliza)
            legibles = {_nombre_comparable(c): c for c in tabla.columnas_mapeo}
            if isinstance(cabecera_csv, Tabla):
                legibles.update({_nombre_comparable(c): c for c in cabecera_csv.columnas_mapeo})

            def legible(columna):
                return " / ".join(legibles.get(parte, parte) for parte in columna.split(" / "))
            fila.update(
                filas_xlsx=n_x, filas_iguales=iguales, filas_casi_iguales=casi,
                filas_solo_csv=solo_ref if publicados else None, filas_solo_xlsx=len(sin_pareja),
                solo_xlsx_en_otro_csv=n_otro,
                otros_csv=" | ".join(f"{c}: {n}" for c, n in en_otro.most_common()) or None,
                fraccion_xlsx_en_csv=round(fraccion, 4), consolidado_xlsx=bool(motivo),
                motivo=motivo or ("el CSV de su grupo no se ha descargado" if pendientes_csv else
                                  "el CSV trae lo mismo" if publicados else "otro formato del mismo grupo"),
                cabeceras_iguales=(isinstance(cabecera_csv, Tabla) and
                                   [_nombre_comparable(c) for c in cabecera_csv.columnas_mapeo
                                    if not c.startswith("Unnamed: ")] ==
                                   [_nombre_comparable(c) for c in tabla.columnas_mapeo
                                    if not c.startswith("Unnamed: ")]),
                columnas_con_diferencias=" | ".join(
                    f"{legible(c)}: {n}" for c, n in diferencias.most_common()) or None,
                ejemplos=" | ".join(f"{legible(c)}: CSV {a!r} / XLSX {b!r}"
                                    for c, (a, b) in list(ejemplos.items())[:5]) or None)
            informe.append(fila)
    return consolidar, informe, claves_csv, referencias


# ===========================================================================
# ESCRITURA
# ===========================================================================
COLUMNAS_BOOL = {"_duplicado", "_repetido_en_csv", "_en_ultima_descarga", "_fila_vacia"}
COLUMNAS_ENTERAS = {"_fila_origen", "_anio_fichero"}


def escribir_parquet(df, ruta, resumen):
    """Escribe con esquema explícito (texto salvo booleanos, enteros, importes
    y fechas) sin machacar la versión anterior (guardar_version)."""
    campos = []
    for columna in df.columns:
        if columna in COLUMNAS_BOOL:
            tipo = pa.bool_()
        elif columna in COLUMNAS_ENTERAS:
            tipo = pa.int64()
        elif pd.api.types.is_datetime64_any_dtype(df[columna]):
            tipo = pa.timestamp("us")
        elif pd.api.types.is_float_dtype(df[columna]):
            tipo = pa.float64()
        else:
            tipo = pa.string()
        campos.append(pa.field(str(columna), tipo))
    tabla = pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_name(f".{ruta.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        estado = guardar_version(ruta, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()
    resumen.tablas.append((ruta, len(df), len(df.columns), estado))
    print(f"  {ruta.name}: {len(df):,} filas x {len(df.columns)} columnas ({estado})")
    return estado


def escribir_informe(filas, columnas, ruta):
    """CSV de un informe (';', utf-8) con guardar_version. El contenido solo
    depende de los ficheros descargados: sin cambios no crea versión nueva."""
    # dtype=object: los contadores se escriben como enteros aunque falten en alguna fila
    df = pd.DataFrame(filas, columns=columnas, dtype=object) if filas else pd.DataFrame(columns=columnas)
    return guardar_version(Path(ruta), df.to_csv(sep=";", index=False).encode("utf-8"))


COLUMNAS_INFORME_LECTURA = [
    "archivo", "recurso", "dataset", "categoria", "anio", "parte", "formato", "version", "fecha_descarga",
    "consolidado", "motivo", "tipo", "codificacion", "separador", "hoja", "lineas_fisicas", "registros",
    "filas_pandas", "lectores_coinciden", "fila_cabecera", "estructura", "filas_previas", "filas_datos",
    "filas_sin_contenido", "celdas_con_valor", "celdas_con_valor_tabla", "ancho", "cuadra",
    "columnas_sin_mapear", "aviso_lectura", "error"]
COLUMNAS_INFORME_COMPARACION = [
    "dataset", "categoria", "anio", "parte", "csv", "xlsx", "filas_csv", "filas_xlsx", "filas_iguales",
    "filas_casi_iguales", "filas_solo_csv", "filas_solo_xlsx", "solo_xlsx_en_otro_csv", "otros_csv",
    "fraccion_xlsx_en_csv", "consolidado_xlsx", "motivo", "cabeceras_iguales", "columnas_con_diferencias",
    "ejemplos", "error"]


# ===========================================================================
# PROCESAMIENTO PRINCIPAL
# ===========================================================================
def registrar_huerfanos(salida, manifiesto, resumen):
    """Ficheros de originales/ que no están en el manifiesto (p.ej. si se
    perdió): se incorporan deduciendo categoría, año, parte e id del nombre."""
    prefijos = sorted(((p, c) for c, p in PREFIJOS.items()), key=lambda x: -len(x[0]))
    for dataset, carpeta in DATASETS.items():
        directorio = Path(salida) / CARPETA_ORIGINALES / carpeta
        if not directorio.is_dir():
            continue
        for ruta in sorted(directorio.iterdir()):
            clave = ruta.relative_to(salida).as_posix()
            if not ruta.is_file() or ruta.name.startswith(".") or clave in manifiesto.entradas \
                    or "__" not in ruta.stem:
                continue
            izquierda, rid = ruta.stem.split("__", 1)
            prefijo, categoria = next(((p, c) for p, c in prefijos
                                       if izquierda == p or izquierda.startswith(p + "_")), (None, None))
            if categoria is None:
                continue
            resto = izquierda[len(prefijo) + 1:]
            m = re.match(r"(20\d{2})(?:_(.*))?$", resto)
            anio, parte = (int(m.group(1)), m.group(2) or None) if m else (None, resto or None)
            manifiesto.entradas[clave] = {
                "id": rid, "dataset": dataset, "descripcion": None, "categoria": categoria, "anio": anio,
                "parte": parte, "formato": ruta.suffix.lstrip(".").lower(), "archivo": clave,
                "estado": "desconocido"}
            resumen.aviso(f"{clave}: no estaba en el manifiesto; se deduce su categoría del nombre")


def _orden_entrada(par):
    clave, e = par
    return (list(DATASETS).index(e["dataset"]) if e.get("dataset") in DATASETS else 9, e.get("categoria") or "",
            e.get("anio") or 0, e.get("parte") or "", FORMATOS_TABLA.index(e["formato"]), clave)


def nombre_corto(clave):
    """<categoría>_<año>[_<parte>] de un fichero (su nombre sin el id): el
    fuente_fichero de la tabla unificada ('menores_2019')."""
    return Path(clave).stem.split("__")[0]


def repetidas_en_csv(acumulado, infos, entrada, claves_csv, filas_ref):
    """_repetido_en_csv de las filas de un fichero: False en los CSV; en un
    XLSX/XLS, True si la fila está igual en algún CSV publicado del conjunto
    (claves_csv) o igual o casi igual (comparar_filas, pareja a pareja) en los
    CSV publicados de su grupo (filas_ref). Los CSV retirados no cuentan: si
    el portal retira el CSV y mantiene el XLSX, sus filas son las vigentes."""
    repetido = np.zeros(len(acumulado), dtype=bool)
    if entrada["formato"] == "csv":
        return repetido
    indice = claves_csv.get(entrada["dataset"], {})
    for archivo, posiciones in acumulado.groupby("_archivo_origen", sort=False).indices.items():
        info = infos[archivo]
        filas = claves_filas(acumulado.iloc[posiciones][info["columnas"]], info["columnas_mapeo"], "excel")
        emparejadas = comparar_filas(filas_ref or [], filas)[6]
        repetido[posiciones] = [bool(f) and (f in indice or pareja) for f, pareja in zip(filas, emparejadas)]
    return repetido


PATRON_SELLO_FINAL = re.compile(r"__\d{8}T\d{6}Z(?:_\d+)*$")


def recurso_de_archivo(archivo):
    """Fichero (clave del manifiesto) de un _archivo_origen: sin '_historico/'
    ni el sello de la versión."""
    ruta = PurePosixPath(str(archivo))
    if ruta.parent.name != HISTORICO:
        return ruta.as_posix()
    m = PATRON_SELLO_FINAL.search(ruta.stem)
    stem = ruta.stem[:m.start()] if m else ruta.stem
    return (ruta.parent.parent / f"{stem}{ruta.suffix}").as_posix()


def filas_por_recurso(archivos):
    cuentas = Counter()
    for archivo, n in pd.Series(list(archivos), dtype=object).value_counts().items():
        cuentas[recurso_de_archivo(archivo)] += int(n)
    return cuentas


def comprobar_filas_por_recurso(anterior, fiel, resumen):
    """Las filas de cada fichero solo pueden crecer (acumular no quita filas):
    si la tabla fiel anterior tenía más de alguno, algo falta (un original
    borrado o que ya no se puede leer), aunque otro fichero crezca más. Es un
    fallo y la tabla anterior queda en _historico/."""
    try:
        previas = pq.read_table(anterior, columns=["_archivo_origen"]).column(0).to_pylist()
    except Exception as e:  # noqa: BLE001 - p.ej. una tabla anterior sin _archivo_origen
        resumen.fallo(f"No se puede comprobar que no se pierden filas: la tabla fiel anterior no se puede leer "
                      f"({type(e).__name__}: {e})")
        return
    antes, ahora = filas_por_recurso(previas), filas_por_recurso(fiel["_archivo_origen"])
    for recurso, n in sorted(antes.items()):
        if ahora.get(recurso, 0) < n:
            resumen.fallo(f"{recurso}: {ahora.get(recurso, 0):,} filas en la tabla fiel y {n:,} en la anterior: "
                          "revisa los originales (la anterior queda en _historico/)")


def procesar(salida, manifiesto, resumen):
    """Tablas fiel y unificada desde todas las versiones de los originales.
    Devuelve (fiel, unificada) o None si no hay nada que publicar."""
    salida = Path(salida)
    print("\n[TABLAS]")
    registrar_huerfanos(salida, manifiesto, resumen)
    tabulares, pendientes = {}, {}
    for clave, e in sorted(manifiesto.entradas.items()):
        if e.get("formato") not in FORMATOS_TABLA or e.get("categoria") in (None, "documentacion"):
            continue
        if not CATEGORIAS_ACTIVAS.get(e["categoria"], True):
            continue
        destino = salida / clave
        if not versiones(destino):
            resumen.fallo(f"{clave}: no hay ninguna copia descargada")
            if _publicado(e):
                pendientes[clave] = e
            continue
        if not destino.exists():
            resumen.fallo(f"{clave}: falta la copia vigente; se usan sus {len(versiones(destino))} versiones de "
                          "_historico/ y la última pasa por vigente")
        else:
            anotada, sin_anotar = anotar_copia(e, destino)
            if sin_anotar:
                e.update(anotada)
                resumen.aviso(f"{clave}: su copia no estaba anotada en el manifiesto (¿ejecución cortada?); "
                              f"se anota con la fecha del fichero ({anotada['fecha_descarga']})")
        tabulares[clave] = e
    if not tabulares:
        resumen.fallo("No hay ficheros descargados: no se escribe ninguna tabla")
        return None

    # 1. Última versión de cada fichero: comparación CSV/XLSX y qué se consolida
    ultimas = {}
    for clave, e in tabulares.items():
        try:
            ultimas[clave] = leer_tabla(versiones(salida / clave)[-1], e["categoria"], Path(clave).stem)
        except Exception as ex:  # noqa: BLE001 - se informa en la comparación / lectura
            ultimas[clave] = f"{type(ex).__name__}: {ex}"
            continue
        if (salida / clave).exists():
            # motivo_descarga vuelve a pedir una copia sin registros con datos
            e["filas_con_datos"] = filas_con_datos(ultimas[clave])
    consolidar, comparacion, claves_csv, referencias = decidir_consolidacion(tabulares, ultimas, resumen,
                                                                             pendientes)

    # 2. Tabla fiel (todas las versiones) y unificada (desde la fiel)
    fieles, unificadas, informe, fuera, sin_mapear = [], [], [], [], {}
    for clave, e in sorted(tabulares.items(), key=_orden_entrada):
        if clave not in consolidar:
            tabla = ultimas[clave]
            ruta, fecha, vigente = versiones_con_fecha(salida / clave)[-1]
            informe.append(fila_informe(clave, e, ruta.relative_to(salida).as_posix(),
                                        "vigente" if vigente else "historico", fecha, False,
                                        "comparado con el CSV de su grupo",
                                        tabla if isinstance(tabla, Tabla) else None,
                                        None if isinstance(tabla, Tabla) else tabla))
            continue
        acumulado, infos = acumular_recurso(salida, clave, e, ultimas.get(clave), informe, fuera, resumen,
                                            consolidar[clave])
        sin_mapear.update({archivo: info["sin_mapear"] for archivo, info in infos.items()})
        if acumulado is None:
            continue
        acumulado["_repetido_en_csv"] = repetidas_en_csv(acumulado, infos, e, claves_csv, referencias.get(clave))
        unificadas.append(unificar_recurso(acumulado, infos, nombre_corto(clave), e["categoria"]))
        fieles.append(acumulado)
    for fila in informe:
        if fila["archivo"] in sin_mapear:
            fila["columnas_sin_mapear"] = " | ".join(sin_mapear[fila["archivo"]]) or None
    if not fieles:
        resumen.fallo("Ningún fichero tiene registros: no se escribe ninguna tabla")
        return None

    # Tabla fiel: columnas originales en orden de aparición y después las del script
    fiel = pd.concat(fieles, ignore_index=True, sort=False)
    finales = META_FIEL + MARCAS_FIEL + list(COLUMNAS_META)
    fiel = fiel[[c for c in fiel.columns if c not in finales] + [c for c in finales if c in fiel.columns]]
    unificada = convertir_unificada(pd.concat(unificadas, ignore_index=True, sort=False))
    # Ninguna fila se pierde entre las dos tablas: mismas filas, mismo orden
    if len(unificada) != len(fiel) or not (
            unificada["_archivo_origen"].to_numpy() == fiel["_archivo_origen"].to_numpy()).all() or not (
            unificada["_fila_origen"].to_numpy() == fiel["_fila_origen"].to_numpy()).all():
        raise RuntimeError("La tabla unificada no tiene las mismas filas que la fiel")

    print("\n[ESCRITURA]")
    anterior = salida / SALIDA_FIEL
    if anterior.exists():
        comprobar_filas_por_recurso(anterior, fiel, resumen)
    escribir_parquet(fiel, salida / SALIDA_FIEL, resumen)
    escribir_parquet(unificada, salida / SALIDA_UNIFICADA, resumen)
    informes = salida / CARPETA_INFORMES
    escribir_informe(informe, COLUMNAS_INFORME_LECTURA, informes / "lectura_ficheros.csv")
    escribir_informe(fuera, ["archivo", "registro", "tipo", "texto"], informes / "lineas_fuera_de_tabla.csv")
    escribir_informe(comparacion, COLUMNAS_INFORME_COMPARACION, informes / "comparacion_csv_xlsx.csv")
    for fila in informe:
        if fila.get("consolidado") and fila.get("cuadra") is False:
            resumen.fallo(f"{fila['archivo']}: los registros o las celdas no cuadran con la tabla")
    imprimir_estadisticas(unificada)
    return fiel, unificada


# ===========================================================================
# ESTADÍSTICAS
# ===========================================================================
def imprimir_estadisticas(df):
    print("\n  ESTADÍSTICAS (tabla unificada)")
    print(f"  {'─' * 60}")
    vigentes = df['_en_ultima_descarga'].astype(bool)
    print(f"  Filas: {len(df):,} ({int(vigentes.sum()):,} en la última descarga; "
          f"{int(df['_fila_vacia'].sum()):,} vacías; {int(df['_duplicado'].sum()):,} repetidas en su fichero)")
    fechas = df['fecha_adjudicacion'].dropna()
    if len(fechas):
        print(f"  Fechas de adjudicación: {fechas.min():%Y-%m-%d} → {fechas.max():%Y-%m-%d}")
    # Importe más representativo de cada categoría (en las incidencias,
    # importe_adjudicacion_iva_inc es el del contrato y se repite en cada una)
    importe_de = {'prorrogados': 'importe_prorroga', 'modificados': 'importe_modificacion',
                  'penalidades': 'importe_penalidad', 'cesiones': 'importe_cedido'}
    print(f"\n  {'categoría':<26} {'filas':>8} {'vigentes':>9}   importe (filas vigentes)")
    for cat, grp in df.groupby('categoria'):
        columna = importe_de.get(cat, 'importe_adjudicacion_iva_inc')
        importe = grp.loc[grp['_en_ultima_descarga'].astype(bool), columna].sum()
        print(f"  {cat:<26} {len(grp):>8,} {int(grp['_en_ultima_descarga'].sum()):>9,}   "
              f"{importe:,.0f} € ({columna})")
    print("\n  Por año:")
    for anio, n in df.groupby('anio').size().sort_index().items():
        marca = " (año < 2010: contratos antiguos inscritos después)" if anio < 2010 else ""
        print(f"    {int(anio)}: {n:,}{marca}")
    print("\n  Por estructura:")
    for estructura, n in df.groupby('estructura').size().sort_index().items():
        print(f"    {estructura}: {n:,}")


# ===========================================================================
# MAIN
# ===========================================================================
def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Actividad contractual del Ayuntamiento de Madrid (datos.madrid.es, CKAN)")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR,
                        help=f"carpeta de salida (por defecto ./{OUTPUT_DIR})")
    modo = parser.add_mutually_exclusive_group()
    modo.add_argument("--solo-descargar", action="store_true",
                      help="solo la capa cruda (originales/ y manifiesto), sin tablas")
    modo.add_argument("--solo-procesar", action="store_true",
                      help="solo las tablas, desde lo ya descargado (sin red)")
    parser.add_argument("--comprobar-todo", action="store_true",
                        help="volver a pedir también los años cerrados cuyos metadatos no han cambiado")
    args = parser.parse_args(argv)

    salida = Path(args.output_dir)
    manifiesto = Manifiesto(salida / CARPETA_ORIGINALES / NOMBRE_MANIFIESTO)
    resumen = Resumen()
    inicio = datetime.now()
    print("=" * 70)
    print("ACTIVIDAD CONTRACTUAL - AYUNTAMIENTO DE MADRID")
    print("=" * 70)
    print(f"Salida: {salida}")
    if manifiesto.corrupto:
        resumen.aviso(f"{manifiesto.ruta} no se puede leer (JSON corrupto o cortado); se conserva en _historico/ "
                      "al guardar el nuevo")
    if manifiesto.recuperado:
        resumen.aviso(f"No está {manifiesto.ruta} o no se puede leer: se recupera el último guardado en _historico/ "
                      f"({manifiesto.recuperado.name})")

    if not args.solo_procesar:
        try:
            for dataset in DATASETS:
                descargar_dataset(dataset, salida, manifiesto, resumen, anio_en_curso(), args.comprobar_todo)
        finally:
            if manifiesto.entradas:
                manifiesto.guardar()
    if not args.solo_descargar:
        antes = json.dumps(manifiesto.entradas, sort_keys=True)
        procesar(salida, manifiesto, resumen)
        if manifiesto.entradas and json.dumps(manifiesto.entradas, sort_keys=True) != antes:
            manifiesto.guardar()      # consolidado_desde de los XLSX/XLS, ficheros sin entrada

    resumen.imprimir(f"AYUNTAMIENTO DE MADRID ({datetime.now() - inicio})")
    return 1 if resumen.fallos else 0


if __name__ == "__main__":
    sys.exit(main())
