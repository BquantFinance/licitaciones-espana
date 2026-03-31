"""
=============================================================================
DESCARGA Y UNIFICACIÓN DE ACTIVIDAD CONTRACTUAL COMPLETA
AYUNTAMIENTO DE MADRID
=============================================================================
Fuente: https://datos.madrid.es
Datos desde 2015 hasta 2026

CATEGORÍAS DE CONTRATOS:
  1. contratos_menores        → Contratos menores
  2. contratos_formalizados   → Contratos inscritos en Registro
  3. acuerdo_marco            → Basados en acuerdo marco / sist. dinámico
  4. modificados              → Contratos modificados
  5. prorrogados              → Contratos prorrogados
  6. penalidades              → Penalidades en contratos
  7. cesiones                 → Cesiones de contratos
  8. resoluciones             → Resoluciones de contratos
  9. homologacion             → Derivados de procedimientos de homologación

v12:
    - rutas y salidas ancladas al repo
    - discovery API-first usando package_show de datos.madrid.es
    - fallback HTML / manifiesto estable como respaldo
    - manifiesto JSON reproducible de los recursos descubiertos
    - descarga paralela de CSV
    - preparado para ejecución reproducible y testeable
=============================================================================
"""

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import pandas as pd
import requests
import re
from pathlib import Path
from bs4 import BeautifulSoup
import warnings

warnings.filterwarnings('ignore')


# ===========================================================================
# CONFIGURACIÓN
# ===========================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "datos_madrid_contratacion_completa"
DEFAULT_LOG_PATH = SCRIPT_DIR / "descarga_madrid_ayuntamiento.log"
DEFAULT_DOWNLOAD_WORKERS = 8
PACKAGE_ID_MENORES = "300253-0-contratos-actividad-menores"
PACKAGE_ID_ACTIVIDAD = "216876-0-contratos-actividad"
PACKAGE_SHOW_URL = "https://datos.madrid.es/api/3/action/package_show?id={package_id}"


def build_logger(log_path: Path | None = None) -> logging.Logger:
    logger = logging.getLogger("madrid.ayuntamiento")
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def configure_runtime(
    output_dir: Path | None = None,
    log_path: Path | None = None,
    download_workers: int | None = None,
) -> None:
    global OUTPUT_DIR, CSV_DIR, LOG_PATH, DOWNLOAD_WORKERS, log

    OUTPUT_DIR = (output_dir or DEFAULT_OUTPUT_DIR).resolve()
    CSV_DIR = OUTPUT_DIR / "csv_originales"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH = (log_path or DEFAULT_LOG_PATH).resolve()
    DOWNLOAD_WORKERS = max(1, int(download_workers or DEFAULT_DOWNLOAD_WORKERS))
    log = build_logger(LOG_PATH)


OUTPUT_DIR = DEFAULT_OUTPUT_DIR
CSV_DIR = OUTPUT_DIR / "csv_originales"
LOG_PATH = DEFAULT_LOG_PATH
DOWNLOAD_WORKERS = DEFAULT_DOWNLOAD_WORKERS
log = logging.getLogger("madrid.ayuntamiento")
log.addHandler(logging.NullHandler())

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
# PÁGINAS FUENTE
# ===========================================================================
PAGINA_CONTRATOS_MENORES = (
    "https://datos.madrid.es/portal/site/egob/"
    "menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/"
    "?vgnextoid=c331ef300ebe5610VgnVCM1000001d4a900aRCRD"
    "&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD"
    "&vgnextfmt=default"
)

PAGINA_ACTIVIDAD_CONTRACTUAL = (
    "https://datos.madrid.es/portal/site/egob/"
    "menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/"
    "?vgnextoid=139afaf464830510VgnVCM1000000b205a0aRCRD"
    "&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD"
    "&vgnextfmt=default"
)


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
# URLs DE RESPALDO
# ===========================================================================
def _urls_respaldo_menores():
    return {
        "menores_2015": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-17-contratos-actividad-menores-csv/download/300253-17-contratos-actividad-menores-csv.csv",
        "menores_2016": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-15-contratos-actividad-menores-csv/download/300253-15-contratos-actividad-menores-csv.csv",
        "menores_2017": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-18-contratos-actividad-menores-csv/download/300253-18-contratos-actividad-menores-csv",
        "menores_2018": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-14-contratos-actividad-menores-csv/download/300253-14-contratos-actividad-menores-csv.csv",
        "menores_2019": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-12-contratos-actividad-menores-csv/download/300253-12-contratos-actividad-menores-csv.csv",
        "menores_2020": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-10-contratos-actividad-menores-csv/download/300253-10-contratos-actividad-menores-csv.csv",
        "menores_2021_desde_marzo": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-8-contratos-actividad-menores-csv/download/300253-8-contratos-actividad-menores-csv.csv",
        "menores_2021_hasta_febrero": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-9-contratos-actividad-menores-csv/download/300253-9-contratos-actividad-menores-csv.csv",
        "menores_2022": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-23-contratos-actividad-menores-csv/download/300253-23-contratos-actividad-menores-csv.csv",
        "menores_2023": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-4-contratos-actividad-menores-csv/download/300253-4-contratos-actividad-menores-csv",
        "menores_2024": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-2-contratos-actividad-menores-csv/download/300253-2-contratos-actividad-menores-csv.csv",
        "menores_2025": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-27-contratos-actividad-menores-csv/download/300253-27-contratos-actividad-menores-csv.csv",
        "menores_2026": "https://datos.madrid.es/dataset/300253-0-contratos-actividad-menores/resource/300253-26-contratos-actividad-menores-csv/download/contratos_menores_2026.csv",
    }


def _urls_respaldo_actividad():
    u = {}
    # fmt: off
    u["acuerdo_marco_2015"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-81-contratos-actividad-csv/download/216876-81-contratos-actividad-csv.csv"
    u["acuerdo_marco_2016"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-25-contratos-actividad-csv/download/216876-25-contratos-actividad-csv.csv"
    u["acuerdo_marco_2017"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-94-contratos-actividad-csv/download/216876-94-contratos-actividad-csv.csv"
    u["acuerdo_marco_2018"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-129-contratos-actividad-csv/download/216876-129-contratos-actividad-csv.csv"
    u["acuerdo_marco_2019"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-24-contratos-actividad-csv/download/216876-24-contratos-actividad-csv.csv"
    u["acuerdo_marco_2020"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-23-contratos-actividad-csv/download/216876-23-contratos-actividad-csv.csv"
    u["acuerdo_marco_2021_anteriores"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-22-contratos-actividad-csv/download/216876-22-contratos-actividad-csv"
    u["acuerdo_marco_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-5-contratos-actividad-csv/download/216876-5-contratos-actividad-csv.csv"
    u["acuerdo_marco_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-20-contratos-actividad-csv/download/216876-20-contratos-actividad-csv"
    u["acuerdo_marco_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-19-contratos-actividad-csv/download/216876-19-contratos-actividad-csv.csv"
    u["acuerdo_marco_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-117-contratos-actividad-csv/download/216876-117-contratos-actividad-csv"
    u["acuerdo_marco_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-18-contratos-actividad-csv/download/216876-18-contratos-actividad-csv"
    u["acuerdo_marco_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-102-contratos-actividad-csv/download/contratos_basados_cesda_2026.csv"
    u["cesiones_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-59-contratos-actividad-csv/download/216876-59-contratos-actividad-csv.csv"
    u["cesiones_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-108-contratos-actividad-csv/download/216876-108-contratos-actividad-csv.csv"
    u["cesiones_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-58-contratos-actividad-csv/download/216876-58-contratos-actividad-csv.csv"
    u["cesiones_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-92-contratos-actividad-csv/download/216876-92-contratos-actividad-csv.csv"
    u["cesiones_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-57-contratos-actividad-csv/download/216876-57-contratos-actividad-csv.csv"
    u["formalizados_2015"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-10-contratos-actividad-csv/download/216876-10-contratos-actividad-csv.csv"
    u["formalizados_2016"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-86-contratos-actividad-csv/download/216876-86-contratos-actividad-csv.csv"
    u["formalizados_2017"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-8-contratos-actividad-csv/download/216876-8-contratos-actividad-csv.csv"
    u["formalizados_2018"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-7-contratos-actividad-csv/download/216876-7-contratos-actividad-csv.csv"
    u["formalizados_2019"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-95-contratos-actividad-csv/download/216876-95-contratos-actividad-csv.csv"
    u["formalizados_2020"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-124-contratos-actividad-csv/download/216876-124-contratos-actividad-csv.csv"
    u["formalizados_2021_anteriores"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-21-contratos-actividad-csv/download/216876-21-contratos-actividad-csv.csv"
    u["formalizados_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-4-contratos-actividad-csv/download/216876-4-contratos-actividad-csv"
    u["formalizados_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-3-contratos-actividad-csv/download/216876-3-contratos-actividad-csv"
    u["formalizados_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-1-contratos-actividad-csv/download/216876-1-contratos-actividad-csv"
    u["formalizados_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-0-contratos-actividad-csv/download/216876-0-contratos-actividad-csv"
    u["formalizados_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-125-contratos-actividad-csv/download/216876-125-contratos-actividad-csv"
    u["formalizados_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-2-contratos-actividad-csv/download/contratos_inscritos_2026.csv"
    u["homologacion_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-69-contratos-actividad-csv/download/216876-69-contratos-actividad-csv.csv"
    u["homologacion_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-123-contratos-actividad-csv/download/216876-123-contratos-actividad-csv.csv"
    u["homologacion_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-93-contratos-actividad-csv/download/216876-93-contratos-actividad-csv.csv"
    u["homologacion_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-68-contratos-actividad-csv/download/216876-68-contratos-actividad-csv.csv"
    u["homologacion_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-91-contratos-actividad-csv/download/derivados_homologacion_inscritos_2026.csv"
    u["modificados_2015"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-137-contratos-actividad-csv/download/216876-137-contratos-actividad-csv"
    u["modificados_2016"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-38-contratos-actividad-csv/download/216876-38-contratos-actividad-csv.csv"
    u["modificados_2017"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-128-contratos-actividad-csv/download/216876-128-contratos-actividad-csv.csv"
    u["modificados_2018"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-37-contratos-actividad-csv/download/216876-37-contratos-actividad-csv.csv"
    u["modificados_2019"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-42-contratos-actividad-csv/download/216876-42-contratos-actividad-csv"
    u["modificados_2020"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-106-contratos-actividad-csv/download/216876-106-contratos-actividad-csv.csv"
    u["modificados_2021_anteriores"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-111-contratos-actividad-csv/download/216876-111-contratos-actividad-csv.csv"
    u["modificados_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-6-contratos-actividad-csv/download/216876-6-contratos-actividad-csv.csv"
    u["modificados_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-35-contratos-actividad-csv/download/216876-35-contratos-actividad-csv.csv"
    u["modificados_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-34-contratos-actividad-csv/download/216876-34-contratos-actividad-csv.csv"
    u["modificados_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-100-contratos-actividad-csv/download/216876-100-contratos-actividad-csv.csv"
    u["modificados_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-33-contratos-actividad-csv/download/216876-33-contratos-actividad-csv.csv"
    u["modificados_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-119-contratos-actividad-csv/download/modificaciones_inscritas_2026.csv"
    u["penalidades_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-133-contratos-actividad-csv/download/216876-133-contratos-actividad-csv.csv"
    u["penalidades_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-52-contratos-actividad-csv/download/216876-52-contratos-actividad-csv.csv"
    u["penalidades_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-99-contratos-actividad-csv/download/216876-99-contratos-actividad-csv.csv"
    u["penalidades_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-51-contratos-actividad-csv/download/216876-51-contratos-actividad-csv.csv"
    u["penalidades_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-104-contratos-actividad-csv/download/216876-104-contratos-actividad-csv.csv"
    u["penalidades_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-50-contratos-actividad-csv/download/penalidades_inscritas_2026.csv"
    u["prorrogados_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-132-contratos-actividad-csv/download/216876-132-contratos-actividad-csv.csv"
    u["prorrogados_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-122-contratos-actividad-csv/download/216876-122-contratos-actividad-csv.csv"
    u["prorrogados_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-46-contratos-actividad-csv/download/216876-46-contratos-actividad-csv.csv"
    u["prorrogados_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-45-contratos-actividad-csv/download/216876-45-contratos-actividad-csv"
    u["prorrogados_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-121-contratos-actividad-csv/download/216876-121-contratos-actividad-csv.csv"
    u["prorrogados_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-112-contratos-actividad-csv/download/prorrogas_inscritas_2026.csv"
    u["resoluciones_2021_nuevos"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-97-contratos-actividad-csv/download/216876-97-contratos-actividad-csv.csv"
    u["resoluciones_2022"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-64-contratos-actividad-csv/download/216876-64-contratos-actividad-csv.csv"
    u["resoluciones_2023"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-135-contratos-actividad-csv/download/216876-135-contratos-actividad-csv"
    u["resoluciones_2024"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-89-contratos-actividad-csv/download/216876-89-contratos-actividad-csv.csv"
    u["resoluciones_2025"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-63-contratos-actividad-csv/download/216876-63-contratos-actividad-csv.csv"
    u["resoluciones_2026"]="https://datos.madrid.es/dataset/216876-0-contratos-actividad/resource/216876-56-contratos-actividad-csv/download/resoluciones_inscritas_2026.csv"
    # fmt: on
    return u


# ===========================================================================
# UTILIDADES
# ===========================================================================
def fetch_package_resources(package_id):
    url = PACKAGE_SHOW_URL.format(package_id=package_id)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise ValueError(f"package_show falló para {package_id}")
    return payload["result"].get("resources", [])


def resource_is_csv(resource):
    fmt = (resource.get("format") or "").strip().lower()
    url = resource.get("url") or ""
    return fmt == "csv" or url.endswith(".csv")


def descubrir_recursos_dataset(package_id, extractor):
    recursos = fetch_package_resources(package_id)
    csv_urls = {}
    for resource in recursos:
        if not resource_is_csv(resource):
            continue
        url = resource.get("url") or ""
        context = " ".join(
            part for part in (
                resource.get("description"),
                resource.get("name"),
                resource.get("id"),
            ) if part
        )
        nombre = extractor(context, url)
        if nombre and nombre not in csv_urls:
            csv_urls[nombre] = url
    return dict(sorted(csv_urls.items()))


def _clasificar_categoria(nombre):
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


def _filtrar_activas(urls):
    return {n: u for n, u in urls.items() if CATEGORIAS_ACTIVAS.get(_clasificar_categoria(n), False)}


def exportar_manifiesto(urls):
    # Persist the discovered catalog so later runs can compare coverage even if the portal HTML changes.
    categorias = {}
    for nombre, url in sorted(urls.items()):
        categoria = _clasificar_categoria(nombre)
        categorias[categoria] = categorias.get(categoria, 0) + 1

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "package_ids": {
            "menores": PACKAGE_ID_MENORES,
            "actividad": PACKAGE_ID_ACTIVIDAD,
        },
        "total_csvs": len(urls),
        "download_workers": DOWNLOAD_WORKERS,
        "categorias": categorias,
        "resources": [
            {
                "nombre": nombre,
                "categoria": _clasificar_categoria(nombre),
                "url": url,
            }
            for nombre, url in sorted(urls.items())
        ],
    }
    manifest_path = OUTPUT_DIR / "actividad_contractual_madrid_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest_path


def strip_normalize(col_name):
    s = col_name.upper().strip()
    for old, new in {'Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U','Ñ':'N',
                     'º':'','ª':'','¢':'O','£':'U','¡':'I','¥':'N'}.items():
        s = s.replace(old, new)
    s = re.sub(r'[.\-,;:()_/]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


# ===========================================================================
# DESCUBRIMIENTO URLs
# ===========================================================================
def descubrir_csv_urls_menores():
    print("  🔍 Descubriendo contratos menores...")
    try:
        csv_urls = descubrir_recursos_dataset(PACKAGE_ID_MENORES, _extraer_nombre_menores)
        if csv_urls:
            print(f"  ✓ API dataset: {len(csv_urls)} ficheros")
            return csv_urls
    except Exception as e:
        print(f"  ⚠ API dataset: {e}")

    print("  ↪ Fallback HTML...")
    try:
        resp = requests.get(PAGINA_CONTRATOS_MENORES, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        csv_urls = {}
        for link in soup.find_all('a', href=True):
            href = link['href']
            if not href.endswith('.csv'): continue
            url = href if href.startswith('http') else f"https://datos.madrid.es{href}"
            parent = link.find_parent(['div', 'li', 'td', 'p'])
            context = (parent.get_text() if parent else link.get_text()).strip()
            nombre = _extraer_nombre_menores(context, url)
            if nombre and nombre not in csv_urls:
                csv_urls[nombre] = url
        if csv_urls:
            print(f"  ✓ HTML: {len(csv_urls)} ficheros")
            return dict(sorted(csv_urls.items()))
    except Exception as e:
        print(f"  ⚠ HTML: {e}")
    print("  ⚠ Usando URLs de respaldo")
    return _urls_respaldo_menores()


def descubrir_csv_urls_actividad():
    print("  🔍 Descubriendo actividad contractual...")
    try:
        csv_urls = descubrir_recursos_dataset(PACKAGE_ID_ACTIVIDAD, _extraer_nombre_actividad)
        if csv_urls:
            print(f"  ✓ API dataset: {len(csv_urls)} ficheros")
            return csv_urls
    except Exception as e:
        print(f"  ⚠ API dataset: {e}")

    print("  ↪ Fallback HTML...")
    try:
        resp = requests.get(PAGINA_ACTIVIDAD_CONTRACTUAL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        csv_urls = {}
        for link in soup.find_all('a', href=True):
            href = link['href']
            if not href.endswith('.csv'): continue
            url = href if href.startswith('http') else f"https://datos.madrid.es{href}"
            parent = link.find_parent(['li', 'div'])
            grandparent = parent.find_parent(['li', 'div']) if parent else None
            context = (grandparent.get_text() if grandparent else
                      (parent.get_text() if parent else link.get_text())).strip()
            nombre = _extraer_nombre_actividad(context, url)
            if nombre and nombre not in csv_urls:
                csv_urls[nombre] = url
        if csv_urls:
            print(f"  ✓ HTML: {len(csv_urls)} ficheros")
            return dict(sorted(csv_urls.items()))
    except Exception as e:
        print(f"  ⚠ HTML: {e}")
    print("  ⚠ Usando URLs de respaldo")
    return _urls_respaldo_actividad()


def _extraer_nombre_menores(context, url):
    ctx_lower = context.lower()
    years = re.findall(r'20\d{2}', context) or re.findall(r'20\d{2}', url)
    if not years: return None
    year = years[0]
    if year == '2021':
        if 'hasta' in ctx_lower or 'febrero' in ctx_lower: return 'menores_2021_hasta_febrero'
        elif 'desde' in ctx_lower or 'marzo' in ctx_lower: return 'menores_2021_desde_marzo'
        return None
    return f'menores_{year}'


def _extraer_nombre_actividad(context, url):
    ctx_lower = context.lower()
    years = re.findall(r'20\d{2}', context) or re.findall(r'20\d{2}', url)
    if not years: return None
    year = years[0]
    if 'menor' in ctx_lower: return None
    if 'homologaci' in ctx_lower: tipo = 'homologacion'
    elif 'acuerdo marco' in ctx_lower or 'sistema din' in ctx_lower: tipo = 'acuerdo_marco'
    elif 'modificad' in ctx_lower: tipo = 'modificados'
    elif 'prorroga' in ctx_lower: tipo = 'prorrogados'
    elif 'penalidad' in ctx_lower: tipo = 'penalidades'
    elif 'cesion' in ctx_lower or 'cesión' in ctx_lower: tipo = 'cesiones'
    elif 'resolucion' in ctx_lower or 'resolución' in ctx_lower: tipo = 'resoluciones'
    else: tipo = 'formalizados'
    if year == '2021':
        if '2020' in ctx_lower and ('formalizado' in ctx_lower or 'contrato' in ctx_lower):
            return f'{tipo}_2021_anteriores'
        elif '2021' in ctx_lower:
            return f'{tipo}_2021_nuevos'
    return f'{tipo}_{year}'


# ===========================================================================
# LECTURA CSV
# ===========================================================================
def descargar_csv(nombre, url, force=False):
    filepath = CSV_DIR / f"{nombre}.csv"
    if filepath.exists() and not force:
        return filepath, f"    ✓ {nombre}.csv ya existe"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        filepath.write_bytes(resp.content)
        return filepath, f"    ⬇ {nombre}... OK ({len(resp.content)/1024:.0f} KB)"
    except Exception as e:
        return None, f"    ⬇ {nombre}... ERROR: {e}"


def descargar_csvs(urls, force=False, workers=1):
    ficheros = {}

    if workers <= 1:
        for nombre, url in sorted(urls.items()):
            path, message = descargar_csv(nombre, url, force=force)
            print(message)
            if path is not None:
                ficheros[nombre] = path
        return ficheros

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(descargar_csv, nombre, url, force): nombre
            for nombre, url in sorted(urls.items())
        }
        for future in as_completed(futures):
            nombre = futures[future]
            try:
                path, message = future.result()
            except Exception as e:
                path = None
                message = f"    ⬇ {nombre}... ERROR: {e}"
            print(message)
            if path is not None:
                ficheros[nombre] = path

    return dict(sorted(ficheros.items()))


def leer_csv(filepath, skiprows=0, header='infer'):
    for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                lines = f.readlines()
            idx = skiprows if len(lines) > skiprows else 0
            primera = lines[idx]
            sep = ';' if primera.count(';') > primera.count(',') else ','
            df = pd.read_csv(filepath, sep=sep, encoding=encoding, dtype=str,
                             on_bad_lines='skip', quotechar='"',
                             skiprows=skiprows, header=header)
            if len(df) > 0 and len(df.columns) > 2:
                if header == 'infer':
                    df.columns = [c.strip() for c in df.columns]
                return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return pd.read_csv(filepath, sep=';', encoding='latin-1', dtype=str,
                       on_bad_lines='skip', skiprows=skiprows, header=header)


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
# PROCESAMIENTO PRINCIPAL
# ===========================================================================
def procesar_fichero(nombre, filepath):
    categoria = _clasificar_categoria(nombre)
    es_menor = (categoria == "contratos_menores")

    df = leer_csv(filepath)

    if es_menor:
        estructura = detectar_estructura_menores(nombre, list(df.columns))
    else:
        estructura = detectar_estructura_actividad(nombre, list(df.columns))

    # Handle files without header
    if estructura == 'SKIP_ROW':
        # Try skipping title rows (some files have 1-6 title/blank rows before header)
        for skip in range(1, 7):
            df2 = leer_csv(filepath, skiprows=skip)
            if len(df2) < 2 or len(df2.columns) < 5: continue
            # Check that most columns have real names (not Unnamed)
            n_unnamed = sum(1 for c in df2.columns if 'Unnamed' in str(c))
            if n_unnamed > len(df2.columns) // 2: continue
            if es_menor:
                est2 = detectar_estructura_menores(nombre, list(df2.columns))
            else:
                est2 = detectar_estructura_actividad(nombre, list(df2.columns))
            if est2 not in ('SKIP_ROW', 'DESCONOCIDA'):
                df, estructura = df2, est2
                break

        # Still SKIP_ROW → read without header, then try to use row 0 as header
        if estructura == 'SKIP_ROW':
            df_raw = leer_csv(filepath, header=None)
            # Check if row 0 contains valid header strings
            row0 = [str(df_raw.iloc[0, i]).strip() for i in range(len(df_raw.columns))]
            row0_looks_like_header = sum(1 for v in row0 if len(v) > 2 and not v.replace('.','').replace(',','').isdigit() and v != 'nan') > len(row0) // 3
            if row0_looks_like_header:
                # Row 0 is header text → assign as column names and drop
                df_raw.columns = row0
                df_raw = df_raw.iloc[1:].reset_index(drop=True)
                # Re-detect structure
                if es_menor:
                    est2 = detectar_estructura_menores(nombre, list(df_raw.columns))
                else:
                    est2 = detectar_estructura_actividad(nombre, list(df_raw.columns))
                if est2 not in ('SKIP_ROW', 'DESCONOCIDA'):
                    df, estructura = df_raw, est2
                else:
                    df = df_raw
                    estructura = 'SIN_CABECERA'
            else:
                # Row 0 is not a header (e.g. all NaN) → skip it and check row 1
                row0_all_nan = all(v == 'nan' or v == '' for v in row0)
                if row0_all_nan and len(df_raw) > 1:
                    row1 = [str(df_raw.iloc[1, i]).strip() for i in range(len(df_raw.columns))]
                    row1_looks_like_header = sum(1 for v in row1 if len(v) > 2 and not v.replace('.','').replace(',','').isdigit() and v != 'nan') > len(row1) // 3
                    if row1_looks_like_header:
                        df_raw.columns = row1
                        df_raw = df_raw.iloc[2:].reset_index(drop=True)
                        if es_menor:
                            est2 = detectar_estructura_menores(nombre, list(df_raw.columns))
                        else:
                            est2 = detectar_estructura_actividad(nombre, list(df_raw.columns))
                        if est2 not in ('SKIP_ROW', 'DESCONOCIDA'):
                            df, estructura = df_raw, est2
                        else:
                            df = df_raw
                            estructura = 'SIN_CABECERA'
                    else:
                        # Skip the NaN row and keep as SIN_CABECERA
                        df = df_raw.iloc[1:].reset_index(drop=True)
                        estructura = 'SIN_CABECERA'
                else:
                    df = df_raw
                    estructura = 'SIN_CABECERA'

    print(f"    {nombre}: {estructura}, {len(df):,} filas, {len(df.columns)} cols", end="")

    # --- MAP ---
    mapped = _mapear_fichero(df, nombre, categoria, estructura)

    # --- Build unified DataFrame ---
    df_out = pd.DataFrame(index=df.index)
    for col_unif in COLUMNAS_UNIFICADAS:
        df_out[col_unif] = mapped.get(col_unif)

    df_out['fuente_fichero'] = nombre
    df_out['categoria'] = categoria
    df_out['estructura'] = estructura

    n_obj = df_out['objeto_contrato'].notna().sum()
    n_imp = df_out['importe_adjudicacion_iva_inc'].notna().sum()
    print(f" → {n_obj} objeto, {n_imp} importe")

    # Report unmapped (skip boring columns)
    mapped_cols = {s.name for s in mapped.values() if hasattr(s, 'name')}
    skip_prefixes = ('Unnamed', '_', 'Mes', 'Año', 'A\xf1o')
    skip_contains = ('documento 20', 'artículo', 'art¡culo', 'apartado',
                     'artículo', 'observaciones', 'mes inscripcion',
                     'f.p. anuncio', 'f.p. en perfil', 'articulo',
                     'procedimiento de adjudicacion.1')
    sin = []
    for c in df.columns:
        cs = str(c).strip()
        if c in mapped_cols: continue
        if any(cs.startswith(p) for p in skip_prefixes): continue
        if any(x in cs.lower() for x in skip_contains): continue
        # Skip single-char columns (R, etc) and encoding-broken year cols (A¤o)
        if len(cs) <= 1: continue
        if cs in ('A¤o', 'Año', 'A\xf1o'): continue
        # Skip pure integer column names from SIN_CABECERA
        if cs.isdigit(): continue
        if cs: sin.append(cs)
    if sin:
        print(f"      ⚠ Sin mapear: {sin}")

    return df_out[COLUMNAS_UNIFICADAS]


def _mapear_fichero(df, nombre, categoria, estructura):
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
        return _mapear_sin_cabecera(df, nombre, categoria)

    # === MODERNAS (AC_NEW, AC_2025) ===
    if estructura in ('AC_NEW', 'AC_2025'):
        return _mapear_moderno(df, nombre, categoria, estructura)

    return {}


def _mapear_sin_cabecera(df, nombre, categoria):
    """Map files that had no header (read with header=None)."""
    ncols = len(df.columns)

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
        first_val = str(df.iloc[0, 0]).strip() if len(df) > 0 else ''
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
        first_val = str(df.iloc[0, 0]).strip().upper() if len(df) > 0 else ''
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
# LIMPIEZA
# ===========================================================================
def normalizar_importe(valor):
    if pd.isna(valor) or str(valor).strip() == '':
        return None
    s = str(valor).strip()
    s = re.sub(r'[€\x80?]', '', s).strip()
    s = re.sub(r'\.1$', '', s).strip()  # pandas duplicate col suffix
    if not s: return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def limpiar_dataframe(df):
    # Importes
    cols_importe = [c for c in df.columns if 'importe' in c or 'presupuesto' in c or 'valor' in c]
    for col in cols_importe:
        df[col] = df[col].apply(normalizar_importe)

    # Fechas
    cols_fecha = [c for c in df.columns if 'fecha' in c]
    for col in cols_fecha:
        df[col] = pd.to_datetime(df[col], format='mixed', dayfirst=True, errors='coerce')

    # Tipo de contrato normalize
    if 'tipo_contrato' in df.columns and df['tipo_contrato'].notna().any():
        df['tipo_contrato'] = df['tipo_contrato'].str.strip().str.title()
        unif = {
            'Suministros': 'Suministro', 'Servicio': 'Servicios',
            'Contrato De Servicios': 'Servicios', 'Contrato De Obras': 'Obras',
            'Contrato Privado': 'Privado',
            'Contrato Administrativo Especial': 'Administrativo Especial',
        }
        df['tipo_contrato'] = df['tipo_contrato'].replace(unif)
        df.loc[df['tipo_contrato'].str.contains('Suministro', na=False, case=False), 'tipo_contrato'] = 'Suministro'

    # PYME
    if 'pyme' in df.columns:
        df['pyme'] = df['pyme'].str.strip().str.upper()

    # Remove empty rows
    mask = pd.Series(False, index=df.index)
    for col in ['objeto_contrato', 'importe_adjudicacion_iva_inc', 'importe_penalidad',
                'importe_modificacion', 'importe_prorroga', 'causa_resolucion',
                'causas_generales', 'n_registro_contrato', 'objeto_derivado',
                'titulo_expediente', 'objeto_sh', 'importe_cedido']:
        if col in df.columns:
            mask = mask | df[col].notna()
    df = df[mask].copy()

    # Year extraction
    df['anio'] = df['fecha_adjudicacion'].dt.year
    # Fallback 1: other date columns
    for col_alt in ['fecha_formalizacion', 'fecha_inscripcion',
                    'fecha_aprobacion_derivado', 'fecha_inscripcion_contrato',
                    'fecha_formalizacion_incidencia', 'fecha_acuerdo_penalidad',
                    'fecha_acuerdo_resolucion', 'fecha_autorizacion_cesion',
                    'fecha_peticion_cesion', 'fecha_inicio']:
        if col_alt in df.columns:
            m = df['anio'].isna()
            if m.any():
                df.loc[m, 'anio'] = df.loc[m, col_alt].dt.year
    # Fallback 2: extract year from filename (e.g. "formalizados_2019" → 2019)
    m = df['anio'].isna()
    if m.any():
        df.loc[m, 'anio'] = df.loc[m, 'fuente_fichero'].str.extract(r'(20\d{2})')[0].astype(float)

    return df


# ===========================================================================
# ESTADÍSTICAS
# ===========================================================================
def imprimir_estadisticas(df):
    print(f"\n  📊 ESTADÍSTICAS GENERALES")
    print(f"  {'─'*50}")
    print(f"  • Filas totales: {len(df):,}")

    fechas = df['fecha_adjudicacion'].dropna()
    if len(fechas):
        print(f"  • Fechas adjudicación: {fechas.min()} → {fechas.max()}")

    imp = df['importe_adjudicacion_iva_inc'].dropna()
    if len(imp):
        print(f"  • Importe adjudicación total: {imp.sum():,.2f} €")
        print(f"  • Importe medio: {imp.mean():,.2f} €  |  Mediano: {imp.median():,.2f} €")

    print(f"\n  📁 POR CATEGORÍA")
    print(f"  {'─'*60}")
    for cat in sorted(df['categoria'].unique()):
        grp = df[df['categoria'] == cat]
        n = len(grp)
        # Use the most relevant importe for each category
        if cat == 'prorrogados':
            imp_col = 'importe_prorroga'
        elif cat == 'modificados':
            imp_col = 'importe_modificacion'
        elif cat == 'penalidades':
            imp_col = 'importe_penalidad'
        elif cat == 'cesiones':
            imp_col = 'importe_cedido'
        else:
            imp_col = 'importe_adjudicacion_iva_inc'
        imp_t = grp[imp_col].sum() if imp_col in grp.columns else 0
        imp_s = f"{imp_t:,.0f} €" if pd.notna(imp_t) and imp_t > 0 else "—"
        print(f"    {cat:<30s} {n:>7,} filas | {imp_s}")

    print(f"\n  📅 POR AÑO")
    print(f"  {'─'*50}")
    suspicious = 0
    for a, n in df.groupby('anio').size().sort_index().items():
        if pd.notna(a):
            flag = " ⚠" if int(a) < 2010 else ""
            print(f"    {int(a)}: {n:,}{flag}")
            if int(a) < 2010: suspicious += n
    if suspicious:
        print(f"\n  ⚠ {suspicious:,} registros con año < 2010 (contratos antiguos inscritos después)")
    n_sf = df['fecha_adjudicacion'].isna().sum()
    n_sa = df['anio'].isna().sum()
    if n_sf: print(f"  ⚠ {n_sf:,} registros sin fecha de adjudicación")
    if n_sa: print(f"  ⚠ {n_sa:,} registros sin año (tras todos los fallbacks)")
    elif n_sf: print(f"  ✓ Todos los registros tienen año asignado (vía fallbacks)")

    print(f"\n  📋 POR TIPO DE CONTRATO (top 10)")
    print(f"  {'─'*50}")
    for t, n in df.groupby('tipo_contrato').size().sort_values(ascending=False).head(10).items():
        print(f"    {t}: {n:,}")

    print(f"\n  🏗️  POR ESTRUCTURA")
    print(f"  {'─'*50}")
    for e, n in df.groupby('estructura').size().sort_index().items():
        print(f"    {e}: {n:,}")


# ===========================================================================
# EXPORTACIÓN
# ===========================================================================
def exportar(df, sufijo=""):
    base = f"actividad_contractual_madrid{sufijo}"
    csv_path = OUTPUT_DIR / f"{base}.csv"
    df.to_csv(csv_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"  ✓ CSV: {csv_path} ({csv_path.stat().st_size/1024/1024:.1f} MB)")
    try:
        pq = OUTPUT_DIR / f"{base}.parquet"
        df.to_parquet(pq, index=False, engine='pyarrow')
        print(f"  ✓ Parquet: {pq} ({pq.stat().st_size/1024/1024:.1f} MB)")
    except ImportError:
        print("  ⚠ pip install pyarrow para Parquet")
    if len(df) < 1_048_576:
        try:
            xl = OUTPUT_DIR / f"{base}.xlsx"
            df.to_excel(xl, index=False, engine='openpyxl')
            print(f"  ✓ Excel: {xl} ({xl.stat().st_size/1024/1024:.1f} MB)")
        except Exception as e:
            print(f"  ⚠ Excel: {e}")


# ===========================================================================
# MAIN
# ===========================================================================
def build_parser():
    parser = argparse.ArgumentParser(
        description="Descarga y unificación de actividad contractual del Ayuntamiento de Madrid",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directorio de salida para CSVs descargados y dataset final.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Ruta del fichero de log.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Vuelve a descargar los CSV aunque ya existan en disco.",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=DEFAULT_DOWNLOAD_WORKERS,
        help="Número de workers para descargar CSVs en paralelo.",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    configure_runtime(
        output_dir=args.output_dir,
        log_path=args.log_path,
        download_workers=args.download_workers,
    )

    print("=" * 70)
    print("ACTIVIDAD CONTRACTUAL COMPLETA - AYUNTAMIENTO DE MADRID (v12)")
    print("=" * 70)
    cats = [k for k, v in CATEGORIAS_ACTIVAS.items() if v]
    print(f"\nCategorías activas: {cats}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Log: {LOG_PATH}")
    print(f"Download workers: {DOWNLOAD_WORKERS}")

    # PASO 1
    print("\n🔍 PASO 1: Descubriendo ficheros CSV...")
    all_urls = {}
    if CATEGORIAS_ACTIVAS.get("contratos_menores"):
        all_urls.update(descubrir_csv_urls_menores())
    no_menores = {k: v for k, v in CATEGORIAS_ACTIVAS.items() if k != "contratos_menores" and v}
    if no_menores:
        act = descubrir_csv_urls_actividad()
        all_urls.update(_filtrar_activas(act))
    print(f"\n  Total ficheros: {len(all_urls)}")
    for n in sorted(all_urls):
        print(f"    [{_clasificar_categoria(n)}] {n}")
    manifest_path = exportar_manifiesto(all_urls)
    print(f"  ✓ Manifiesto: {manifest_path}")

    # PASO 2
    print("\n📥 PASO 2: Descargando...")
    ficheros = descargar_csvs(
        all_urls,
        force=args.force_download,
        workers=args.download_workers,
    )
    print(f"\n  Descargados: {len(ficheros)}")

    # PASO 3
    print("\n📊 PASO 3: Leyendo y mapeando...")
    dfs = []
    for nombre in sorted(ficheros):
        dfs.append(procesar_fichero(nombre, ficheros[nombre]))
    if not dfs:
        print("  ⚠ Sin ficheros"); return None

    # PASO 4
    print("\n🔗 PASO 4: Unificando...")
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"  Filas (bruto): {len(df_all):,}")

    # PASO 5
    print("\n🧹 PASO 5: Limpiando...")
    df_all = limpiar_dataframe(df_all)
    print(f"  Filas (limpio): {len(df_all):,}")

    # PASO 6
    print("\n📈 PASO 6: Estadísticas")
    imprimir_estadisticas(df_all)

    # PASO 7
    print("\n💾 PASO 7: Exportando...")
    exportar(df_all, "_completo")
    print("\n  Por categoría:")
    for cat in sorted(df_all['categoria'].unique()):
        dc = df_all[df_all['categoria'] == cat]
        p = OUTPUT_DIR / f"{cat}_madrid.csv"
        dc.to_csv(p, index=False, sep=';', encoding='utf-8-sig')
        print(f"    ✓ {cat}: {len(dc):,} filas ({p.stat().st_size/1024:.0f} KB)")

    print("\n" + "=" * 70)
    print(f"✅ {len(df_all):,} registros | {df_all['categoria'].nunique()} categorías")
    fmin, fmax = df_all['fecha_adjudicacion'].min(), df_all['fecha_adjudicacion'].max()
    print(f"   {fmin.strftime('%Y-%m-%d') if pd.notna(fmin) else '?'} → "
          f"{fmax.strftime('%Y-%m-%d') if pd.notna(fmax) else '?'}")
    print("=" * 70)
    return df_all


if __name__ == "__main__":
    df = main()
