"""Offline tests for the two Madrid scrapers in ``comunidad_madrid/``.

Neither portal is reachable from the test environment, so every HTTP
interaction is simulated:

* ``ccaa_madrid_ayuntamiento.py`` (datos.madrid.es): the CKAN API
  (package_show) and every resource download are served by a mocked
  ``requests.get`` (``_WebAyto``); the CSV fixtures follow the header layouts
  documented in the script for each structure (A-F, AC_OLD, AC_OLD_MOD, AC_NEW,
  AC_2025, homologación, files with title rows / without header) and the XLSX
  ones are generated with openpyxl.
* ``descarga_contratacion_comunidad_madrid_v1.py``
  (contratos-publicos.comunidad.madrid): ``FakePortalCAM`` implements the flow
  described in the script docstring (antibot key in drupal-settings, search
  that stores the filters in the session, maths CAPTCHA, completion form, CSV
  export with a row cap) and filters its own records by the search params.
"""

import csv
import hashlib
import importlib.util
import io
import json
import os
import runpy
import shutil
import sys
import time
import warnings
from contextlib import contextmanager, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
AYTO_PATH = REPO_ROOT / "comunidad_madrid" / "ccaa_madrid_ayuntamiento.py"
CAM_PATH = REPO_ROOT / "comunidad_madrid" / "descarga_contratacion_comunidad_madrid_v1.py"


@contextmanager
def _sin_efectos_de_importacion():
    """Both scripts create folders/log files and touch global warning filters
    at import time: keep the repo and the test session clean."""
    filtros = warnings.filters[:]
    try:
        with patch.object(Path, "mkdir"), patch("logging.basicConfig"), \
                patch("logging.FileHandler"):
            yield
    finally:
        warnings.filters[:] = filtros


def _importar(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    modulo = importlib.util.module_from_spec(spec)
    with _sin_efectos_de_importacion():
        spec.loader.exec_module(modulo)
    return modulo


ayto = _importar("ccaa_madrid_ayuntamiento", AYTO_PATH)
cam = _importar("descarga_contratacion_comunidad_madrid_v1", CAM_PATH)


@contextmanager
def _cwd(path):
    anterior = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(anterior)


def _csv_bytes(filas, encoding="utf-8", sep=";"):
    buf = io.StringIO()
    csv.writer(buf, delimiter=sep, lineterminator="\r\n").writerows(filas)
    return buf.getvalue().encode(encoding)


# =============================================================================
# AYUNTAMIENTO DE MADRID — fixtures (one or two records per documented layout)
# =============================================================================
H_MENORES_E = [
    "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
    "ORGANO DE CONTRATACION", "OBJETO DEL CONTRATO", "TIPO DE CONTRATO",
    "N. DE INVITACIONES CURSADAS", "INVITADOS A PRESENTAR OFERTA",
    "IMPORTE LICITACION IVA INC.", "N. LICITADORES PARTICIPANTES",
    "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO", "PYME",
    "IMPORTE ADJUDICACION IVA INC.", "FECHA DE ADJUDICACION", "PLAZO",
    "FECHA DE INSCRIPCION",
]
H_FORMALIZADOS_NEW = [
    "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
    "ORGANO DE CONTRATACION", "OBJETO DEL CONTRATO", "TIPO DE CONTRATO",
    "SUBTIPO DE CONTRATO", "PROCEDIMIENTO DE ADJUDICACION",
    "CRITERIOS DE ADJUDICACION", "CODIGO CPV", "IMPORTE LICITACION SIN IVA",
    "IMPORTE LICITACION IVA INC.", "VALOR ESTIMADO",
    "N. LICITADORES PARTICIPANTES", "N. DE LOTES", "N. DE LOTE",
    "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO", "PYME",
    "IMPORTE ADJUDICACION SIN IVA", "IMPORTE ADJUDICACION IVA INC.",
    "PORCENTAJE BAJA ADJUDICACION", "FECHA DE ADJUDICACION",
    "FECHA DE FORMALIZACION", "FECHA DE INICIO", "FECHA DE FIN", "PLAZO",
    "FECHA DE INSCRIPCION", "APLICACION PRESUPUESTARIA", "ACUERDO MARCO",
]
H_RESOLUCIONES = [
    "TIPO DE INCIDENCIA", "FECHA DE INSCRIPCION CONTRATO",
    "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
    "OBJETO DEL CONTRATO", "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO",
    "IMPORTE ADJUDICACION IVA INC.", "CAUSAS GENERALES", "CAUSAS ESPECIFICAS",
    "OTRAS CAUSAS", "FECHA ACUERDO RESOLUCION", "N. DE REGISTRO INCIDENCIA",
]
H_CESIONES = [
    "TIPO DE INCIDENCIA", "FECHA DE INSCRIPCION CONTRATO",
    "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
    "OBJETO DEL CONTRATO", "ADJUDICATARIO CEDENTE", "CESIONARIO",
    "FECHA AUTORIZACION CESION", "FECHA PETICION CESION", "IMPORTE CEDIDO",
    "N. DE REGISTRO INCIDENCIA",
]
# 2021 "formalizados en 2020": underscore names and 16,384 fields per record
# (an Excel export); it used to be read as a headerless file and every column
# after the third was shifted
H_MODIFICADOS_2021 = [
    "INCIDENCIA", "F_INSC_CONTRATO", "CONTRATO", "EXPEDIENTE", "F_INSCRIPCION",
    "GESTOR", "OBJETO_CONTRATO", "CIF", "ADJUCICATARIO", "F_FORMALIZACION",
    "IMPORTE_ADJUDICACIÓN", "F_FORM / F_APROB", "IMPORTE_MODIFICACION",
    "F_FORM_DERIVADO", "OBJETO_DERIVADO", "GASTO / INGRESO", "INSCRIPCIÓN",
]

FICHEROS_AYTO = {
    # --- contratos menores -------------------------------------------------
    "menores_2015": _csv_bytes([
        ["Centro", "Descripción", "Título del expediente", "Nº exped. Adm.",
         "Importe", "Fe.contab.", "NIF", "Tercero", "T. expediente"],
        ["001", "Distrito Centro", "Suministro de papel", "EXP-A1", "1.815,00",
         "15/03/2015", "B11111111", "PAPELES SL", "Suministro"],
        ["002", "Distrito Retiro", "Servicio de limpieza", "EXP-A2", "14.520,00 €",
         "02/11/2015", "B22222222", "LIMPIA SA", "Servicio"],
    ], encoding="cp1252"),
    "menores_2016": _csv_bytes([
        ["Ce.gestor", "Descripción", "Título del expediente", "Nº expediente", "NIF",
         "Tercero", "Importe", "Fech. apro", "Tipo de expediente", "Fe.contab."],
        ["001", "Distrito Centro", "Obras menores", "EXP-B1", "B33333333",
         "OBRAS SL", "3.000,50", "05/02/2016", "Obras", "06/02/2016"],
    ], encoding="cp1252"),
    "menores_2018": _csv_bytes([
        ["Nº RECON", "NÚMERO EXPEDIENTE", "SECCIÓN", "ÓRG.CONTRATACIÓN",
         "OBJETO DEL CONTRATO", "TIPO DE CONTRATO", "N.I.F.", "CONTRATISTA",
         "IMPORTE", "FECHA APROBACION", "PLAZO", "FCH.COMUNIC.REG"],
        ["R1", "EXP-C1", "Sección 1", "Área de Cultura", "Concierto", "Servicios",
         "B44444444", "MUSICA SL", "17.908,86", "07/08/2018", "1 mes", "10/08/2018"],
    ], encoding="cp1252"),
    "menores_2020": _csv_bytes([
        ["CONTRATO", "EXPEDIENTE", "SECCIÓN", "ORG_CONTRATACIÓN", "OBJETO",
         "TIPO_CONTRATO", "CIF", "RAZÓN_SOCIAL", "IMPORTE", "F_APROBACIÓN",
         "PLAZO", "F_INSCRIPCION"],
        ["C1", "EXP-D1", "Sección 2", "Área de Obras", "Reparación", "Obras",
         "B55555555", "REPARA SL", "40.000", "09/01/2020", "2 meses", "12/01/2020"],
    ], encoding="cp1252"),
    "menores_2023": _csv_bytes([
        H_MENORES_E,
        ["2023/1", "EXP-E1", "Centro X", "Junta de Gobierno", "Suministro de sillas",
         "Suministro", "3", "A;B;C", "12.100,00", "3", "B66666666", "SILLAS SL", "Sí",
         "10.890,00", "03/04/2023", "15 días", "10/04/2023"],
    ], encoding="utf-8-sig"),
    "menores_2025": _csv_bytes([
        ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
         "ORGANO DE CONTRATACION", "ORGANISMO_CONTRATANTE", "ORGANISMO_PROMOTOR",
         "OBJETO DEL CONTRATO", "TIPO DE CONTRATO", "IMPORTE LICITACION IVA INC.",
         "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO", "PYME",
         "IMPORTE ADJUDICACION IVA INC.", "FECHA DE ADJUDICACION", "PLAZO",
         "FECHA DE INSCRIPCION"],
        ["2025/1", "EXP-F1", "Centro Y", "Delegada", "Ayuntamiento", "Área Z",
         "Servicio catering", "Servicios", "1.000,00", "B77777777", "CATER SL", "No",
         "900,00", "20/01/2025", "1 día", "25/01/2025"],
    ]),
    # --- actividad contractual, estructura antigua (DOS/cp850 → "Descripci¢n") --
    "formalizados_2016": _csv_bytes([
        ["Mes", "Año", "Descripción Centro", "Organismo", "Número Contrato",
         "Número Expediente", "Descripción Contrato", "Tipo Contrato",
         "Procedimiento Adjudicación", "Artículo", "Apartado", "Criterios Adjudicación",
         "Presupuesto Total(IVA Incluido)", "Importe Adjudicación (IVA Incluido)",
         "Plazo", "Fecha Adjudicación", "Nombre/Razón Social", "NIF/CIF Adjudicatario",
         "Fecha Formalización", "Acuerdo Marco", "Ingreso/Coste Cero", "Observaciones"],
        ["Marzo", "2016", "Economia", "Ayuntamiento", "300/2016/1", "EXP-O1",
         "Mantenimiento de ascensores", "Servicios", "Abierto", "157", "", "Varios",
         "1.210.000,00", "1.089.000,00", "24 meses", "01/03/2016", "ASCENSORES SA",
         "A88888888", "15/03/2016", "No", "Gasto", ""],
    ], encoding="cp850"),
    "modificados_2017": _csv_bytes([
        ["FECHA INSCRIPCION", "NUM.CONTRATO", "NUM.EXPEDIENTE", "GESTOR", "OBJETO",
         "C.I.F", "ADJUDICATARIO", "IMPORTE ADJUDICACION",
         "FECHA FORMALIZACION INCIDENCIA", "IMPORTE MODIFICACION", "INGRESO/GASTO",
         "TIPO INCID."],
        ["05/06/2017", "300/2015/9", "EXP-M1", "Área de Medio Ambiente",
         "Recogida de residuos", "A99999999", "RESIDUOS SA", "2.000.000,00",
         "01/06/2017", "150.000,00", "Gasto", "Modificación"],
    ], encoding="cp1252"),
    "modificados_2021_anteriores": _csv_bytes([
        H_MODIFICADOS_2021 + [""] * 40,
        ["Modificación", "24/05/2019", "201901241", "300/2018/00782", "21/01/2021",
         "ÁREA DE GOBIERNO DE HACIENDA", "SERVICIO DE MANTENIMIENTO", "A41199472",
         "COMPAÑÍA DE SEGURIDAD SA", "26/04/2019", "101.143,46", "23/12/2020", "1.019,46",
         "", "", "G", "ENERO     "] + [""] * 40,
    ], encoding="cp1252"),
    # --- actividad contractual moderna (2021+) ---------------------------------
    "formalizados_2023": _csv_bytes([
        H_FORMALIZADOS_NEW,
        ["2023/100", "EXP-N1", "Centro Z", "Junta de Gobierno", "Limpieza de colegios",
         "Servicios", "Limpieza", "Abierto", "Varios", "90910000", "100.000,00",
         "121.000,00", "200.000,00", "4", "2", "1", "B12345678", "LIMPIEZAS SL", "Sí",
         "90.000,00", "108.900,00", "10", "02/05/2023", "16/05/2023", "01/06/2023",
         "31/05/2025", "24 meses", "20/05/2023", "001/123/456", "No"],
    ]),
    "acuerdo_marco_2023": _csv_bytes([
        ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
         "ORGANO DE CONTRATACION", "OBJETO DEL CONTRATO", "TIPO DE CONTRATO",
         "N. DE CONTRATO DEL C.B.", "N. DE EXPEDIENTE C.B.", "OBJETO C.B.",
         "PRESUPUESTO TOTAL IVA INC. C.B.", "PLAZO C.B.", "FECHA DE APROBACION C.B.",
         "FECHA DE FORMALIZACION C.B.", "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO",
         "IMPORTE ADJUDICACION IVA INC.", "FECHA DE INSCRIPCION"],
        ["2020/5", "EXP-AM1", "Centro AM", "Junta", "AM mobiliario", "Suministro",
         "CB-1", "EXP-CB1", "Sillas oficina", "20.000,00", "1 mes", "10/02/2023",
         "15/02/2023", "B10101010", "MUEBLES SL", "20.000,00", "20/02/2023"],
    ]),
    "prorrogados_2023": _csv_bytes([
        ["TIPO DE INCIDENCIA", "FECHA DE INSCRIPCION CONTRATO",
         "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
         "ORGANO DE CONTRATACION", "OBJETO DEL CONTRATO", "TIPO DE CONTRATO",
         "NIF ADJUDICATARIO", "RAZON SOCIAL ADJUDICATARIO", "CENTRO - SECCION INC.",
         "FECHA DE FORMALIZACION INC.", "IMPORTE PRORROGA IVA INC.",
         "N. DE REGISTRO INCIDENCIA", "INGRESO/GASTO", "IMPORTE ADJUDICACION IVA INC.",
         "PLAZO"],
        ["Prórroga", "01/01/2021", "2021/7", "EXP-P1", "Centro P", "Junta", "Vigilancia",
         "Servicios", "B20202020", "SEGUR SA", "Centro P2", "10/03/2023", "300.000,00",
         "INC-1", "Gasto", "1.000.000,00", "12 meses"],
    ]),
    "penalidades_2023": _csv_bytes([
        ["TIPO DE INCIDENCIA", "FECHA DE INSCRIPCION CONTRATO",
         "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
         "OBJETO DEL CONTRATO", "TIPO DE CONTRATO", "NIF ADJUDICATARIO",
         "RAZON SOCIAL ADJUDICATARIO", "FECHA ACUERDO PENALIDAD", "IMPORTE PENALIDAD",
         "CAUSA", "N. DE REGISTRO INCIDENCIA"],
        ["Penalidad", "01/02/2020", "2020/9", "EXP-PE1", "Centro Q", "Limpieza viaria",
         "Servicios", "B30303030", "LIMPIA SA", "05/07/2023", "12.345,67", "Retraso",
         "INC-2"],
    ]),
    "cesiones_2023": _csv_bytes([
        H_CESIONES,
        ["Cesión", "01/03/2019", "2019/3", "EXP-CE1", "Centro R", "Mantenimiento",
         "EMPRESA A SL", "EMPRESA B SL", "10/10/2023", "01/09/2023", "50.000,00", "INC-3"],
    ]),
    "resoluciones_2023": _csv_bytes([
        H_RESOLUCIONES,
        ["Resolución", "01/04/2018", "2018/4", "EXP-R1", "Centro S", "Obras parque",
         "B40404040", "OBRAS SA", "800.000,00", "Incumplimiento", "Art. 211", "",
         "11/11/2023", "INC-4"],
    ]),
    "homologacion_2023": _csv_bytes([
        ["FECHA DE INSCRIPCION", "CENTRO - SECCION", "N. EXPEDIENTE S.H.",
         "OBJETO DEL S.H.", "DURACION PROCEDIMIENTO (MESES)", "FECHA DE FIN ACTUALIZADA",
         "N. DE REGISTRO", "N. DE EXPEDIENTE", "TITULO DEL EXPEDIENTE",
         "TIPO DE CONTRATO", "CRITERIOS DE ADJUDICACION", "ADJUDICATARIO",
         "IMPORTE ADJUDICACION IVA INC.", "PLAZO DE EJECUCION", "FECHA DE ADJUDICACION",
         "FECHA DE FORMALIZACION"],
        ["12/12/2023", "Centro H", "SH-1", "Homologación papel", "48", "31/12/2026",
         "2023/H1", "EXP-H1", "Papel reciclado", "Suministro", "Precio", "PAPELERA SA",
         "1.234,00", "1 mes", "01/12/2023", "05/12/2023"],
    ]),
    "formalizados_2025": _csv_bytes([
        ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "ORGANISMO_CONTRATANTE",
         "ORGANISMO_PROMOTOR", "OBJETO DEL CONTRATO", "TIPO DE CONTRATO",
         "IMPORTE LICITACION SIN IVA", "IMPORTE LICITACION IVA INC.", "NIF ADJUDICATARIO",
         "RAZON SOCIAL ADJUDICATARIO", "IMPORTE ADJUDICACION SIN IVA",
         "IMPORTE ADJUDICACION IVA INC.", "FECHA DE ADJUDICACION", "FECHA DE FORMALIZACION"],
        ["2025/9", "EXP-N9", "Ayuntamiento", "Área T", "Asfaltado", "Obras",
         "1.000.000,00", "1.210.000,00", "A50505050", "ASFALTOS SA", "900.000,00",
         "1.089.000,00", "2025-03-05", "2025-03-20"],
    ]),
    # --- files with title rows before the header (SKIP_ROW path) -----------------
    "resoluciones_2022": _csv_bytes([
        ["RESOLUCIONES DE CONTRATOS 2022"] + [""] * 13,
        [""] * 14,
        H_RESOLUCIONES,
        ["Resolución", "01/04/2018", "2018/5", "EXP-R2", "Centro S", "Obras plaza",
         "B40404041", "OBRAS2 SA", "700.000,00", "Mutuo acuerdo", "", "", "11/10/2022",
         "INC-5"],
        ["Resolución", "01/04/2019", "2019/6", "EXP-R3", "Centro S", "Obras calle",
         "B40404042", "OBRAS3 SA", "600.000,00", "Incumplimiento", "", "", "12/10/2022",
         "INC-6"],
    ]),
    "cesiones_2022": _csv_bytes([
        ["CESIONES DE CONTRATOS 2022"] + [""] * 11,
        H_CESIONES,
        ["Cesión", "01/03/2019", "2019/8", "EXP-CE2", "Centro R", "Jardinería",
         "EMPRESA C SL", "EMPRESA D SL", "10/10/2022", "01/09/2022", "25.000,00", "INC-7"],
    ]),
}

# (estructura esperada, {columna unificada: valor esperado tras procesar_fichero})
ESPERADO_AYTO = {
    "menores_2015": ("A", {"n_expediente": "EXP-A1", "objeto_contrato": "Suministro de papel",
                           "importe_adjudicacion_iva_inc": "1.815,00",
                           "fecha_adjudicacion": "15/03/2015", "nif_adjudicatario": "B11111111"}),
    "menores_2016": ("B", {"n_expediente": "EXP-B1", "fecha_adjudicacion": "05/02/2016",
                           "importe_adjudicacion_iva_inc": "3.000,50",
                           "razon_social_adjudicatario": "OBRAS SL"}),
    "menores_2018": ("C", {"n_registro_contrato": "R1", "n_expediente": "EXP-C1",
                           "organo_contratacion": "Área de Cultura",
                           "nif_adjudicatario": "B44444444",
                           "fecha_inscripcion": "10/08/2018"}),
    "menores_2020": ("D", {"n_expediente": "EXP-D1", "razon_social_adjudicatario": "REPARA SL",
                           "importe_adjudicacion_iva_inc": "40.000"}),
    "menores_2023": ("E", {"n_registro_contrato": "2023/1", "n_expediente": "EXP-E1",
                           "invitados_presentar_oferta": "A;B;C",
                           "importe_licitacion_iva_inc": "12.100,00",
                           "importe_adjudicacion_iva_inc": "10.890,00", "pyme": "Sí"}),
    "menores_2025": ("F", {"organismo_contratante": "Ayuntamiento",
                           "organismo_promotor": "Área Z",
                           "importe_adjudicacion_iva_inc": "900,00"}),
    "formalizados_2016": ("AC_OLD", {"n_registro_contrato": "300/2016/1",
                                     "objeto_contrato": "Mantenimiento de ascensores",
                                     "presupuesto_total_iva_inc": "1.210.000,00",
                                     "importe_adjudicacion_iva_inc": "1.089.000,00",
                                     "nif_adjudicatario": "A88888888",
                                     "fecha_formalizacion": "15/03/2016"}),
    "modificados_2017": ("AC_OLD_MOD", {"n_registro_contrato": "300/2015/9",
                                        "importe_modificacion": "150.000,00",
                                        "tipo_incidencia": "Modificación",
                                        "fecha_formalizacion_incidencia": "01/06/2017"}),
    "modificados_2021_anteriores": ("AC_OLD_MOD", {
        "tipo_incidencia": "Modificación", "fecha_inscripcion_contrato": "24/05/2019",
        "n_registro_contrato": "201901241", "n_expediente": "300/2018/00782",
        "fecha_inscripcion": "21/01/2021", "centro_seccion": "ÁREA DE GOBIERNO DE HACIENDA",
        "objeto_contrato": "SERVICIO DE MANTENIMIENTO", "nif_adjudicatario": "A41199472",
        "razon_social_adjudicatario": "COMPAÑÍA DE SEGURIDAD SA",
        "importe_adjudicacion_iva_inc": "101.143,46",
        "fecha_formalizacion_incidencia": "23/12/2020", "importe_modificacion": "1.019,46",
        "ingreso_gasto": "G"}),
    "formalizados_2023": ("AC_NEW", {"subtipo_contrato": "Limpieza", "tipo_contrato": "Servicios",
                                     "importe_licitacion_sin_iva": "100.000,00",
                                     "importe_licitacion_iva_inc": "121.000,00",
                                     "importe_adjudicacion_sin_iva": "90.000,00",
                                     "importe_adjudicacion_iva_inc": "108.900,00",
                                     "valor_estimado": "200.000,00", "codigo_cpv": "90910000",
                                     "n_lotes": "2", "n_lote": "1",
                                     "porcentaje_baja_adjudicacion": "10"}),
    "acuerdo_marco_2023": ("AC_NEW", {"n_contrato_derivado": "CB-1",
                                      "n_expediente": "EXP-AM1",
                                      "n_expediente_derivado": "EXP-CB1",
                                      "objeto_contrato": "AM mobiliario",
                                      "objeto_derivado": "Sillas oficina",
                                      "presupuesto_total_derivado": "20.000,00",
                                      "fecha_aprobacion_derivado": "10/02/2023"}),
    "prorrogados_2023": ("AC_NEW", {"importe_prorroga": "300.000,00",
                                    "importe_adjudicacion_iva_inc": "1.000.000,00",
                                    "centro_seccion": "Centro P",
                                    "centro_seccion_incidencia": "Centro P2",
                                    "n_registro_incidencia": "INC-1"}),
    "penalidades_2023": ("AC_NEW", {"importe_penalidad": "12.345,67",
                                    "fecha_acuerdo_penalidad": "05/07/2023",
                                    "causa_penalidad": "Retraso"}),
    "cesiones_2023": ("AC_NEW", {"adjudicatario_cedente": "EMPRESA A SL",
                                 "cesionario": "EMPRESA B SL", "importe_cedido": "50.000,00"}),
    "resoluciones_2023": ("AC_NEW", {"causas_generales": "Incumplimiento",
                                     "causas_especificas": "Art. 211",
                                     "fecha_acuerdo_resolucion": "11/11/2023"}),
    "homologacion_2023": (None, {"n_expediente_sh": "SH-1", "objeto_sh": "Homologación papel",
                                 "n_registro_contrato": "2023/H1",
                                 "objeto_contrato": "Papel reciclado",
                                 "importe_adjudicacion_iva_inc": "1.234,00"}),
    "formalizados_2025": ("AC_2025", {"organismo_contratante": "Ayuntamiento",
                                      "importe_licitacion_sin_iva": "1.000.000,00",
                                      "importe_adjudicacion_sin_iva": "900.000,00",
                                      "importe_adjudicacion_iva_inc": "1.089.000,00"}),
    "resoluciones_2022": ("AC_NEW", {"n_expediente": "EXP-R2",
                                     "causas_generales": "Mutuo acuerdo"}),
    "cesiones_2022": ("AC_NEW", {"n_expediente": "EXP-CE2", "importe_cedido": "25.000,00"}),
}
N_REGISTROS_AYTO = {"resoluciones_2022": 2, "menores_2015": 2}  # default 1
# Record number (header included) of the first data row
PRIMERA_FILA_AYTO = {"resoluciones_2022": 4, "cesiones_2022": 3}  # default 2


def _procesar(tmp_path, nombre, contenido):
    ruta = tmp_path / f"{nombre}.csv"
    ruta.write_bytes(contenido)
    with redirect_stdout(io.StringIO()):
        return ayto.procesar_fichero(nombre, ruta)


@pytest.mark.parametrize("nombre", sorted(FICHEROS_AYTO))
def test_ayto_procesar_fichero_maps_each_documented_layout(tmp_path, nombre):
    df = _procesar(tmp_path, nombre, FICHEROS_AYTO[nombre])
    estructura, valores = ESPERADO_AYTO[nombre]

    assert list(df.columns) == ayto.COLUMNAS_UNIFICADAS
    assert len(df) == N_REGISTROS_AYTO.get(nombre, 1)
    assert (df["fuente_fichero"] == nombre).all()
    assert (df["categoria"] == ayto._clasificar_categoria(nombre)).all()
    if estructura:
        assert df["estructura"].iloc[0] == estructura
    for col, esperado in valores.items():
        assert df[col].iloc[0] == esperado, col


def test_ayto_small_file_with_title_row_keeps_its_only_record(tmp_path):
    # Title row + header + 1 record: used to raise EmptyDataError (aborting the
    # whole run) once the SKIP_ROW loop tried skiprows >= number of lines.
    df = _procesar(tmp_path, "cesiones_2022", FICHEROS_AYTO["cesiones_2022"])
    assert df[["n_expediente", "cesionario"]].values.tolist() == [["EXP-CE2", "EMPRESA D SL"]]


def _modificados_sin_cabecera(n):
    return _csv_bytes([
        [f"0{i}/06/2015", f"300/2014/{i}", f"EXP-{i}", "Área X", f"Objeto {i}",
         f"B0000000{i}", f"EMPRESA {i}", "1.000,00", f"0{i}/05/2015", "100,00", "Gasto"]
        for i in range(1, n + 1)
    ], encoding="cp1252")


@pytest.mark.parametrize("n", [3, 9])
def test_ayto_headerless_file_keeps_first_record(tmp_path, n):
    # n=3 crashed (EmptyDataError); n=9 silently dropped EXP-1 (row 0 was
    # taken as a header and then discarded as SIN_CABECERA).
    df = _procesar(tmp_path, "modificados_2015", _modificados_sin_cabecera(n))
    assert df["estructura"].iloc[0] == "SIN_CABECERA"
    assert df["n_expediente"].tolist() == [f"EXP-{i}" for i in range(1, n + 1)]
    assert df["importe_modificacion"].tolist() == ["100,00"] * n
    assert df["fecha_inscripcion"].iloc[0] == "01/06/2015"


def test_ayto_row0_empty_row1_header_is_recovered(tmp_path):
    # prorrogados_2021: empty first row, header on row 1 with many empty columns.
    cab = ["TIPO DE INCIDENCIA", "FECHA DE INSCRIPCION CONTRATO",
           "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CENTRO - SECCION",
           "OBJETO DEL CONTRATO", "IMPORTE PRORROGA IVA INC."]
    relleno = [""] * 10
    filas = [[""] * 17, cab + relleno] + [
        ["Prórroga", "01/01/2020", f"2020/{i}", f"EXP-P{i}", "Centro", "Objeto", "1.000,00"]
        + relleno for i in range(8)]
    df = _procesar(tmp_path, "prorrogados_2021", _csv_bytes(filas, encoding="cp1252"))
    assert df["estructura"].iloc[0] == "AC_NEW"
    assert df["n_expediente"].tolist() == [f"EXP-P{i}" for i in range(8)]
    assert df["importe_prorroga"].tolist() == ["1.000,00"] * 8


def test_ayto_extra_trailing_separator_does_not_shift_columns(tmp_path):
    contenido = (
        "N. DE REGISTRO DE CONTRATO;N. DE EXPEDIENTE;OBJETO DEL CONTRATO;"
        "NIF ADJUDICATARIO;IMPORTE ADJUDICACION IVA INC.\r\n"
        "R0;E0;Obra 0;B0;10,00;\r\n"
        "R1;E1;Obra 1;B1;20,00\r\n"
    ).encode("utf-8")
    df = _procesar(tmp_path, "resoluciones_2024", contenido)
    assert df[["n_registro_contrato", "n_expediente", "objeto_contrato",
               "importe_adjudicacion_iva_inc"]].values.tolist() == [
        ["R0", "E0", "Obra 0", "10,00"], ["R1", "E1", "Obra 1", "20,00"]]


def test_ayto_decodificar_keeps_cp1252_when_a_byte_is_undefined():
    # One byte without a cp1252 character (0x9D, 0x81...) used to switch the
    # whole file to latin-1: '€', quotes and dashes became control characters.
    datos = ("OBJETO;IMPORTE;NIF\r\n“Suministro” – oficina;1.815,00 €;B1\r\n".encode("cp1252")
             + b"A\x9dB;1,00;B2\r\n")
    texto, codificacion = ayto.decodificar(datos)
    assert "“Suministro” – oficina;1.815,00 €;B1" in texto
    assert "A\x9dB;1,00;B2" in texto                       # the odd byte is kept too
    assert codificacion.startswith("cp1252") and "1 bytes" in codificacion
    assert ayto.decodificar("OBJETO\r\nÁrea\r\n".encode("utf-8-sig")) == ("OBJETO\r\nÁrea\r\n", "utf-8-sig")
    assert ayto.decodificar("OBJETO\r\nÁrea\r\n".encode("utf-8")) == ("OBJETO\r\nÁrea\r\n", "utf-8")


def test_ayto_leer_tabla_decodes_ms_dos_cp850_files(tmp_path):
    # Old files of the portal are CP850: read as cp1252 they gave the
    # "Descripci¢n Centro" / "N£mero Contrato" / "A¤o" headers the mappings
    # had to include, and every record's text was garbled the same way.
    texto = ("Mes;Año;Descripción Centro;Número Contrato;"
             "Nombre/Razón Social;Importe Adjudicación   (IVA Incluido)\r\n"
             "Enero;2016;Área de Gobierno de Cultura;300/2016/001;"
             "Construcciones Peña y Muñoz, S.L.;1.815,00\r\n")
    ruta = tmp_path / "formalizados_2016.csv"
    ruta.write_bytes(texto.encode("cp850"))
    tabla = ayto.leer_tabla(ruta, "contratos_formalizados")
    assert tabla.info["codificacion"] == "cp850"
    assert tabla.columnas[:4] == ["Mes", "Año", "Descripción Centro", "Número Contrato"]
    assert tabla.df.iloc[0, 4] == "Construcciones Peña y Muñoz, S.L."
    with redirect_stdout(io.StringIO()):
        out = ayto.procesar_fichero("formalizados_2016", ruta)
    assert out.loc[0, "centro_seccion"] == "Área de Gobierno de Cultura"
    assert out.loc[0, "razon_social_adjudicatario"] == "Construcciones Peña y Muñoz, S.L."
    assert out.loc[0, "n_registro_contrato"] == "300/2016/001"


def test_ayto_empty_file_gives_an_empty_table(tmp_path):
    vacio = tmp_path / "vacio.csv"
    vacio.write_bytes(b"")
    tabla = ayto.leer_tabla(vacio, "cesiones")
    assert tabla.df.empty and tabla.estructura == "VACIO" and tabla.info["registros"] == 0
    with redirect_stdout(io.StringIO()):
        assert ayto.procesar_fichero("cesiones_2024", vacio).empty


def test_ayto_every_csv_record_ends_in_a_row_or_in_the_report(tmp_path):
    contenido = (
        "INFORME DE RESOLUCIONES;;;;\r\n"                     # title
        ";;;;\r\n"                                            # empty row above the header
        "N. DE REGISTRO DE CONTRATO;N. DE EXPEDIENTE;OBJETO DEL CONTRATO;"
        "NIF ADJUDICATARIO;IMPORTE ADJUDICACION IVA INC.\r\n"
        'R1;E1;"Obra con\nsalto de línea; y separador";B1;NA\r\n'   # multi-line field
        "R2;E2;Obra 2;B2;N/A;campo de más\r\n"                # one field too many
        "R3;E3\r\n"                                           # short record
        ";;;;\r\n"                                            # empty record among the data
        "R2;E2;Obra 2;B2;N/A;campo de más\r\n"                # repeated record
    ).encode("cp1252")
    ruta = tmp_path / "resoluciones_2024.csv"
    ruta.write_bytes(contenido)
    tabla = ayto.leer_tabla(ruta, "resoluciones")

    referencia = list(csv.reader(io.StringIO(contenido.decode("cp1252"), newline=""), delimiter=";"))
    assert len(referencia) == 8
    assert tabla.info["registros"] == tabla.info["filas_pandas"] == 8
    assert tabla.info["lectores_coinciden"] is True
    assert tabla.cabecera == 2 and tabla.estructura == "AC_NEW"
    assert [(r, t) for r, t, _ in tabla.previas] == [(1, "titulo"), (2, "vacia")]
    assert tabla.encabezado == "INFORME DE RESOLUCIONES"
    assert tabla.filas_origen == [4, 5, 6, 7, 8]
    assert tabla.columnas[-1] == "Unnamed: 5"                # the extra field is not lost
    assert tabla.df.iloc[0].tolist() == ["R1", "E1", "Obra con\nsalto de línea; y separador", "B1", "NA", ""]
    assert tabla.df.iloc[1].tolist() == ["R2", "E2", "Obra 2", "B2", "N/A", "campo de más"]
    assert tabla.df.iloc[2].tolist() == ["R3", "E3", "", "", "", ""]
    assert tabla.df.iloc[3].tolist() == [""] * 6
    assert tabla.df.iloc[4].tolist() == tabla.df.iloc[1].tolist()
    celdas = sum(1 for r in referencia[3:] for c in r if c != "")
    assert tabla.info["celdas_con_valor"] == celdas == int((tabla.df != "").sum().sum())


def test_ayto_csv_pandas_cannot_parse_is_read_with_the_csv_module(tmp_path):
    ruta = tmp_path / "cesiones_2024.csv"
    ruta.write_bytes(b'N. DE REGISTRO DE CONTRATO;N. DE EXPEDIENTE;CESIONARIO;IMPORTE CEDIDO;OBJETO\r\n'
                     b'R1;"sin cerrar;3\r\nR2;E2;C2;4\r\n')
    tabla = ayto.leer_tabla(ruta, "cesiones")
    assert tabla.info["lectores_coinciden"] is False and "pandas" in tabla.info["aviso_lectura"]
    # nothing is lost: the rest of the file stays inside the unclosed field
    assert tabla.df.iloc[0, :2].tolist() == ["R1", "sin cerrar;3\r\nR2;E2;C2;4\r\n"]


def test_ayto_excel_cells_as_text():
    assert ayto._texto_celda("Línea 1_x000D_\nLínea 2") == "Línea 1\r\nLínea 2"
    assert ayto._texto_celda("_x005F_x000D_") == "_x000D_"   # an escaped literal stays literal
    assert ayto._texto_celda(191202200633.0) == "191202200633"
    assert ayto._texto_celda(1234.5) == "1234.5"
    assert ayto._texto_celda(datetime(2023, 1, 9)) == "2023-01-09"
    assert ayto._texto_celda(None) == "" and ayto._texto_celda(float("nan")) == ""


@pytest.mark.parametrize("valor, esperado", [
    ("1.234,56", 1234.56),
    ("1234,56", 1234.56),
    ("1234.56", 1234.56),
    ("0,5", 0.5),
    (" 14.520,00 € ", 14520.0),
    ("14.520,00 \x80", 14520.0),
    ("-1.234,56", -1234.56),
    ("1.234.567,89", 1234567.89),
    ("15.000", 15000.0),        # thousands dot, no decimals (was 15.0)
    ("1.234.567", 1234567.0),   # was None
    ("12.100", 12100.0),        # was 12.1
    ("150.1", 150.1),           # was 150.0 (".1" stripped as a "pandas suffix")
    ("0.500", 0.5),
    ("100", 100.0),
    ("", None),
    (None, None),
    ("N/D", None),
])
def test_ayto_normalizar_importe_spanish_formats(valor, esperado):
    assert ayto.normalizar_importe(valor) == esperado


def _frame_unificado(**columnas):
    n = len(next(iter(columnas.values())))
    datos = {c: [None] * n for c in ayto.COLUMNAS_UNIFICADAS}
    datos.update(columnas)
    df = pd.DataFrame(datos, dtype=object)
    for columna in ayto.META_UNIFICADA:
        df[columna] = None
    df["_anio_fichero"] = columnas.get("_anio_fichero", [None] * n)
    return df


def test_ayto_convertir_parses_day_first_and_iso_dates_without_swapping():
    df = _frame_unificado(
        fuente_fichero=["formalizados_2025"] * 5,
        objeto_contrato=["a", "b", "c", "d", "e"],
        fecha_adjudicacion=["05/03/2025", "2025-03-05", "2025-03-05 00:00:00",
                            "15/03/25", None],
        fecha_formalizacion=["2025/03/06", "06/03/2025", None, None, "2025-12-01"],
    )
    out = ayto.convertir_unificada(df)
    assert out["fecha_adjudicacion"].tolist()[:4] == [pd.Timestamp("2025-03-05")] * 3 + [
        pd.Timestamp("2025-03-15")]
    assert out["fecha_formalizacion"].tolist()[:2] == [pd.Timestamp("2025-03-06")] * 2
    # the published text of every converted column is kept
    assert out["fecha_adjudicacion_texto"].tolist()[:4] == [
        "05/03/2025", "2025-03-05", "2025-03-05 00:00:00", "15/03/25"]
    # year fallbacks: fecha_adjudicacion → fecha_formalizacion
    assert out["anio"].tolist() == [2025.0] * 5


def test_ayto_convertir_keeps_rows_and_the_original_text():
    df = _frame_unificado(
        fuente_fichero=["menores_2019"] * 4,
        _anio_fichero=[2019] * 4,
        objeto_contrato=["Obra", None, None, None],
        importe_adjudicacion_iva_inc=["1.234,56", "15.000", None, "N/D"],
        fecha_adjudicacion=[None, None, None, "sin fecha"],
        tipo_contrato=[" suministros ", "Contrato de Servicios", None, "Obras"],
        pyme=[" si ", None, None, None],
    )
    out = ayto.convertir_unificada(df)
    # no row is dropped: the one without objeto/importe/registro is only marked
    assert len(out) == 4
    assert out["_fila_vacia"].tolist() == [False, False, True, False]
    assert out["importe_adjudicacion_iva_inc"].tolist()[:2] == [1234.56, 15000.0]
    assert pd.isna(out["importe_adjudicacion_iva_inc"].iloc[3])
    # ...but no non-empty text becomes null: it stays in <col>_texto
    assert out["importe_adjudicacion_iva_inc_texto"].tolist()[3] == "N/D"
    assert out["fecha_adjudicacion_texto"].tolist()[3] == "sin fecha"
    # tipo_contrato as published; the normalised version goes to its own column
    assert out["tipo_contrato"].tolist()[:2] == [" suministros ", "Contrato de Servicios"]
    assert out["tipo_contrato_normalizado"].tolist()[:2] == ["Suministro", "Servicios"]
    assert out["pyme"].iloc[0] == " si "
    assert out["anio"].tolist() == [2019.0] * 4            # from the file year


# -----------------------------------------------------------------------------
# datos.madrid.es (CKAN) simulated: package_show + downloads
# -----------------------------------------------------------------------------
class _Resp:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}

    @property
    def text(self):
        return self.content.decode("utf-8")

    def json(self):
        return json.loads(self.text)


PAGINA_ERROR = b"<!DOCTYPE html><html><head><title>Ayuntamiento de Madrid</title></head><body>Error</body></html>"


class _WebAyto:
    """package_show of both datasets and one URL per resource. Anything not
    registered answers 404 with the portal's HTML error page."""

    def __init__(self):
        self.paquetes = {ayto.DATASET_MENORES: [], ayto.DATASET_ACTIVIDAD: []}
        self.ficheros = {}
        self.caidos = set()
        self.llamadas = []

    @staticmethod
    def url(dataset, rid, formato):
        return f"https://datos.madrid.es/dataset/{dataset}/resource/{rid}/download/{rid}.{formato.lower()}"

    def poner(self, dataset, rid, descripcion, formato, contenido, **ckan):
        url = self.url(dataset, rid, formato)
        self.quitar(dataset, rid)
        self.paquetes[dataset].append({
            "id": rid, "name": rid, "description": descripcion, "format": formato, "url": url,
            "size": len(contenido), "hash": hashlib.md5(contenido).hexdigest(), "last_modified": None,
            "metadata_modified": "2026-05-06T09:06:08.745037", "created": "2026-01-13T13:35:57",
            **ckan})
        self.ficheros[url] = contenido

    def quitar(self, dataset, rid):
        self.paquetes[dataset] = [r for r in self.paquetes[dataset] if r["id"] != rid]

    def get(self, url, params=None, headers=None, timeout=None, **kwargs):
        self.llamadas.append(url)
        if url == f"{ayto.CKAN_API}/package_show":
            dataset = (params or {}).get("id")
            if dataset in self.caidos or dataset not in self.paquetes:
                return _Resp(404, PAGINA_ERROR)
            cuerpo = {"success": True, "result": {"name": dataset, "resources": self.paquetes[dataset],
                                                  "num_resources": len(self.paquetes[dataset])}}
            return _Resp(content=json.dumps(cuerpo).encode("utf-8"))
        valor = self.ficheros.get(url)
        if valor is None:
            return _Resp(404, PAGINA_ERROR)
        if isinstance(valor, BaseException):
            raise valor
        return valor if isinstance(valor, _Resp) else _Resp(content=valor)

    def pedidos(self, rid):
        return sum(1 for u in self.llamadas if f"/resource/{rid}/" in u)


@pytest.fixture
def web(monkeypatch):
    fake = _WebAyto()
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(ayto.time, "sleep", lambda s: None)
    monkeypatch.setattr(ayto, "anio_en_curso", lambda: 2026)
    return fake


# Real CKAN descriptions of datos.madrid.es (September 2026) for each fixture
DESCRIPCIONES_AYTO = {
    "menores_2015": "Contratos menores 2015",
    "menores_2016": "Contratos menores 2016",
    "menores_2018": "Contratos menores 2018",
    "menores_2020": "Contratos menores 2021 (hasta febrero)",
    "menores_2023": "Contratos menores 2023",
    "menores_2025": "Contratos menores 2025",
    "formalizados_2016": "Contratos formalizados inscritos en el Registro de Contratos. 2016",
    "modificados_2017": "Contratos modificados inscritos en el Registro de Contratos. 2017",
    "modificados_2021_anteriores": "Contratos modificados inscritos en el Registro de Contratos. "
                                   "2021. Formalizados en 2020",
    "formalizados_2023": "Contratos inscritos en el Registro de Contratos. 2023",
    "acuerdo_marco_2023": "Contratos basados en un acuerdo marco inscritos en el Registro de "
                          "Contratos. 2023",
    "prorrogados_2023": "Contratos prorrogados inscritos en el Registro de Contratos. 2023",
    "penalidades_2023": "Penalidades en contratos inscritas en el Registro de Contratos. 2023",
    "cesiones_2023": "Cesiones de contratos inscritos en el Registro de Contratos. 2023",
    "resoluciones_2023": "Resoluciones contratos. 2023",
    "homologacion_2023": "Contratos derivados de procedimientos de homologación. 2023",
    "formalizados_2025": "Contratos inscritos en el Registro de Contratos. 2025",
    "resoluciones_2022": "Resoluciones contratos. 2022",
    "cesiones_2022": "Cesiones de contratos inscritos en el Registro de Contratos. 2022",
}


def _rid(nombre, formato="csv"):
    dataset = "300253" if nombre.startswith("menores") else "216876"
    sufijo = "contratos-actividad-menores" if nombre.startswith("menores") else "contratos-actividad"
    return f"{dataset}-{sorted(FICHEROS_AYTO).index(nombre)}-{sufijo}-{formato}"


def _dataset(nombre):
    return ayto.DATASET_MENORES if nombre.startswith("menores") else ayto.DATASET_ACTIVIDAD


def _xlsx(filas):
    import openpyxl
    libro = openpyxl.Workbook()
    for fila in filas:
        libro.active.append(fila)
    buf = io.BytesIO()
    libro.save(buf)
    return buf.getvalue()


def _portal_con_fixtures(web):
    for nombre, contenido in FICHEROS_AYTO.items():
        web.poner(_dataset(nombre), _rid(nombre), DESCRIPCIONES_AYTO[nombre], "CSV", contenido)
    # XLSX twin of one CSV (same records) and a structure PDF: downloaded, not consolidated
    web.poner(ayto.DATASET_MENORES, _rid("menores_2025", "xlsx"), "Contratos menores 2025", "XLSX",
              _xlsx(list(csv.reader(io.StringIO(FICHEROS_AYTO["menores_2025"].decode("utf-8")),
                                    delimiter=";"))))
    web.poner(ayto.DATASET_MENORES, "300253-20-contratos-actividad-menores",
              "Contratos menores (desde 2025). Contenido y estructura del fichero", "PDF",
              b"%PDF-1.4 estructura")


def _ejecutar(salida, *argv):
    with redirect_stdout(io.StringIO()) as log:
        codigo = ayto.main(["--output-dir", str(salida), *argv])
    return codigo, log.getvalue()


def _leer(salida):
    fiel = pd.read_parquet(salida / ayto.SALIDA_FIEL)
    uni = pd.read_parquet(salida / ayto.SALIDA_UNIFICADA)
    return fiel, uni


def _nulos(serie):
    return [None if pd.isna(v) else v for v in serie]


def test_ayto_classifies_real_ckan_descriptions():
    casos = [
        # (dataset, id, description, format) → (categoria, anio, parte, formato)
        (ayto.DATASET_MENORES, "300253-9-contratos-actividad-menores-csv",
         "Contratos menores 2021 (hasta febrero)", "CSV", ("contratos_menores", 2021, "hasta_febrero", "csv")),
        (ayto.DATASET_MENORES, "300253-7-contratos-actividad-menores-xlsx",
         "Contratos menores 2021 (desde marzo)", "XLSX", ("contratos_menores", 2021, "desde_marzo", "xlsx")),
        (ayto.DATASET_MENORES, "300253-19-contratos-actividad-menores-xls",
         "Contratos menores 2015", "XLS", ("contratos_menores", 2015, None, "xls")),
        (ayto.DATASET_MENORES, "300253-20-contratos-actividad-menores",
         "Contratos menores (desde 2025). Contenido y estructura del fichero", "PDF",
         ("documentacion", None, "desde_2025", "pdf")),
        (ayto.DATASET_ACTIVIDAD, "216876-0-contratos-actividad-csv",
         "Contratos inscritos en el Registro de Contratos. 2024", "CSV",
         ("contratos_formalizados", 2024, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-10-contratos-actividad-csv",
         "Contratos formalizados inscritos en el Registro de Contratos. 2015", "CSV",
         ("contratos_formalizados", 2015, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-21-contratos-actividad-csv",
         "Contratos inscritos en el Registro de Contratos. 2021. Formalizados en 2020", "CSV",
         ("contratos_formalizados", 2021, "formalizados_en_2020", "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-22-contratos-actividad-csv",
         "Contratos basados en un acuerdo marco inscritos en el Registro de Contratos. 2021. "
         "Contratos 2020", "CSV", ("acuerdo_marco", 2021, "contratos_2020", "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-18-contratos-actividad-csv",
         "Contratos basados en un acuerdo marco o específicos derivados de un sistema dinámico de "
         "adquisición inscritos en el Registro de Contratos. 2025", "CSV",
         ("acuerdo_marco", 2025, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-111-contratos-actividad-csv",
         "Contratos modificados inscritos en el Registro de Contratos. 2021. Formalizados en 2020",
         "CSV", ("modificados", 2021, "formalizados_en_2020", "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-112-contratos-actividad-csv",
         " Contratos prorrogados inscritos en el Registro de Contratos. 2026", "CSV",
         ("prorrogados", 2026, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-50-contratos-actividad-csv",
         "Penalidades en contratos inscritas en el Registro de Contratos. 2026", "CSV",
         ("penalidades", 2026, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-139-contratos-actividad",
         "Cesiones de contratos inscritos en el Registro de Contratos. 2026", "CSV",
         ("cesiones", 2026, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026",
         "CSV", ("resoluciones", 2026, None, "csv")),
        (ayto.DATASET_ACTIVIDAD, "216876-71-contratos-actividad-xlsx",
         "Contratos derivados de procedimientos de homologación  2022", "XLSX",
         ("homologacion", 2022, None, "xlsx")),
        (ayto.DATASET_ACTIVIDAD, "216876-75-contratos-actividad",
         "Actividad contractual (hasta febrero 2021). Contenido y estructura del fichero", "PDF",
         ("documentacion", None, "hasta_febrero_2021", "pdf")),
    ]
    for dataset, rid, descripcion, formato, esperado in casos:
        info = ayto.clasificar_recurso({"id": rid, "description": descripcion, "format": formato,
                                        "url": f"https://x/{rid}"}, dataset)
        assert (info["categoria"], info["anio"], info["parte"], info["formato"]) == esperado, descripcion
        assert info["id"] == rid
    # unique, readable and stable local names
    info = ayto.clasificar_recurso({"id": "216876-21-contratos-actividad-csv", "format": "CSV",
                                    "description": casos[6][2], "url": "u"}, ayto.DATASET_ACTIVIDAD)
    assert ayto.nombre_local(info) == \
        "formalizados_2021_formalizados_en_2020__216876-21-contratos-actividad-csv.csv"
    # a year that disagrees with the portal's hierarchy is reported
    info = ayto.clasificar_recurso({"id": "x", "format": "CSV", "description": "Contratos menores 2024",
                                    "hierarchy": [{"hierarchy_name": "2025", "order": 1}], "url": "u"},
                                   ayto.DATASET_MENORES)
    assert info["anio"] == 2024 and "2025" in info["aviso"]


def test_ayto_cli_downloads_everything_and_builds_both_tables(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log

    # raw layer: every resource (CSV, XLSX and PDF) with its CKAN id in the name + manifest
    manifiesto = json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))
    recursos = [r for rs in web.paquetes.values() for r in rs]
    assert len(manifiesto) == len(recursos) == len(FICHEROS_AYTO) + 2
    por_id = {e["id"]: (clave, e) for clave, e in manifiesto.items()}
    for recurso in recursos:
        clave, entrada = por_id[recurso["id"]]
        assert clave.endswith(f"__{recurso['id']}.{recurso['format'].lower()}")
        assert (salida / clave).read_bytes() == web.ficheros[recurso["url"]]
        assert entrada["url"] == recurso["url"] and entrada["descripcion"] == recurso["description"]
        assert entrada["ckan"]["size"] == recurso["size"] and entrada["ckan"]["hash"] == recurso["hash"]
        assert entrada["sha256"] == hashlib.sha256(web.ficheros[recurso["url"]]).hexdigest()
        assert entrada["md5_coincide_ckan"] is True and entrada["fecha_descarga"]
        assert entrada["estado"] == "publicado"
    clave, entrada = por_id[_rid("modificados_2021_anteriores")]
    assert (entrada["categoria"], entrada["anio"], entrada["parte"]) == (
        "modificados", 2021, "formalizados_en_2020")
    assert por_id["300253-20-contratos-actividad-menores"][1]["categoria"] == "documentacion"

    fiel, uni = _leer(salida)
    # faithful table: every record of every CSV, original column names and text
    esperado = {_rid(n): N_REGISTROS_AYTO.get(n, 1) for n in FICHEROS_AYTO}
    assert fiel["_recurso"].value_counts().to_dict() == esperado
    for nombre in FICHEROS_AYTO:
        filas = fiel[fiel["_recurso"] == _rid(nombre)]
        primera = PRIMERA_FILA_AYTO.get(nombre, 2)
        assert filas["_fila_origen"].tolist() == list(range(primera, primera + len(filas)))
        registros = list(csv.reader(io.StringIO(ayto.decodificar(FICHEROS_AYTO[nombre])[0], newline=""),
                                    delimiter=";"))
        cabecera = [c for c in registros[primera - 2] if c]
        for i, registro in enumerate(registros[primera - 1:]):
            assert [filas[c].iloc[i] for c in cabecera] == registro[:len(cabecera)], nombre
    assert "Título del expediente" in fiel.columns and "IMPORTE_MODIFICACION" in fiel.columns
    assert (fiel["_en_ultima_descarga"]).all() and not fiel["_duplicado"].any()
    assert set(fiel["_formato"]) == {"csv"}                        # the XLSX twin is only compared
    assert fiel.loc[fiel["_recurso"] == _rid("resoluciones_2022"), "_encabezado"].iloc[0] == \
        "RESOLUCIONES DE CONTRATOS 2022"
    # the title and the empty row above the header are in the report
    fuera = pd.read_csv(salida / "informes" / "lineas_fuera_de_tabla.csv", sep=";", dtype=str, keep_default_na=False)
    fuera = fuera[fuera["archivo"].str.contains(_rid("resoluciones_2022"))]
    assert fuera[["registro", "tipo", "texto"]].values.tolist() == [
        ["1", "titulo", "RESOLUCIONES DE CONTRATOS 2022"], ["2", "vacia", ""]]

    # unified table: the same rows (linked by _archivo_origen + _fila_origen)
    assert len(uni) == len(fiel)
    assert uni[["_archivo_origen", "_fila_origen"]].values.tolist() == \
        fiel[["_archivo_origen", "_fila_origen"]].values.tolist()
    assert list(uni.columns[:len(ayto.COLUMNAS_UNIFICADAS) + 1]) == ayto.COLUMNAS_UNIFICADAS + ["anio"]
    esperado_por_cat = {}
    for nombre in FICHEROS_AYTO:
        cat = ayto.categoria_de(DESCRIPCIONES_AYTO[nombre], _dataset(nombre))
        esperado_por_cat[cat] = esperado_por_cat.get(cat, 0) + N_REGISTROS_AYTO.get(nombre, 1)
    assert uni["categoria"].value_counts().to_dict() == esperado_por_cat
    fila = uni.set_index("n_expediente")
    # licitación vs adjudicación, con/sin IVA
    assert fila.loc["EXP-N1", ["importe_licitacion_sin_iva", "importe_licitacion_iva_inc",
                               "importe_adjudicacion_sin_iva",
                               "importe_adjudicacion_iva_inc"]].tolist() == [
        100000.0, 121000.0, 90000.0, 108900.0]
    assert fila.loc["EXP-N1", "importe_adjudicacion_iva_inc_texto"] == "108.900,00"
    assert fila.loc["EXP-N1", "nif_adjudicatario"] == "B12345678"
    assert fila.loc["EXP-N1", "fecha_adjudicacion"] == pd.Timestamp("2023-05-02")
    assert fila.loc["EXP-N9", "fecha_adjudicacion"] == pd.Timestamp("2025-03-05")
    assert fila.loc["EXP-O1", "importe_adjudicacion_iva_inc"] == 1089000.0
    assert fila.loc["EXP-D1", "importe_adjudicacion_iva_inc"] == 40000.0
    assert fila.loc["EXP-A2", "importe_adjudicacion_iva_inc"] == 14520.0
    assert fila.loc["EXP-P1", "importe_prorroga"] == 300000.0
    assert fila.loc["EXP-CE2", "importe_cedido"] == 25000.0
    assert fila.loc["300/2018/00782", "importe_modificacion"] == 1019.46
    assert uni["anio"].notna().all() and not uni["_fila_vacia"].any()
    # fuente_fichero: <category>_<year>[_<part>], without the CKAN id (as before)
    assert fila.loc["EXP-A1", "fuente_fichero"] == "menores_2015"
    assert fila.loc["300/2018/00782", "fuente_fichero"] == "modificados_2021_formalizados_en_2020"
    assert set(map(tuple, uni.loc[uni["pyme"].notna(), ["pyme", "pyme_normalizado"]].values.tolist())) == {
        ("Sí", "SÍ"), ("No", "NO")}

    # reports
    lectura = pd.read_csv(salida / "informes" / "lectura_ficheros.csv", sep=";", dtype=str)
    consolidados = lectura[lectura["consolidado"] == "True"]
    assert len(consolidados) == len(FICHEROS_AYTO) and (consolidados["cuadra"] == "True").all()
    comparacion = pd.read_csv(salida / "informes" / "comparacion_csv_xlsx.csv", sep=";", dtype=str)
    gemelo = comparacion[comparacion["xlsx"].notna()].iloc[0]
    assert (gemelo["filas_csv"], gemelo["filas_xlsx"], gemelo["filas_iguales"]) == ("1", "1", "1")
    assert gemelo["consolidado_xlsx"] == "False"

    # second run: nothing changed on the portal → closed years are not requested,
    # nothing is overwritten and the tables are not rewritten
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert web.pedidos(_rid("menores_2015")) == 0 and web.pedidos(_rid("formalizados_2016")) == 0
    assert web.pedidos(_rid("menores_2025")) == 1                 # current / previous year: checked
    assert not list(salida.glob("originales/*/_historico/*"))
    assert "(sin_cambios)" in log and not (salida / "_historico").exists()


def test_ayto_retired_and_modified_records_are_kept(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0

    # The portal edits resoluciones 2022 (EXP-R3 withdrawn, EXP-R2 changed,
    # EXP-R9 added) and stops listing cesiones 2023
    registros = list(csv.reader(io.StringIO(FICHEROS_AYTO["resoluciones_2022"].decode("utf-8")),
                                delimiter=";"))
    cambiada = registros[3][:9] + ["Otra causa"] + registros[3][10:]
    nueva = [c.replace("R2", "R9").replace("INC-5", "INC-9") for c in registros[3]]
    web.poner(ayto.DATASET_ACTIVIDAD, _rid("resoluciones_2022"), DESCRIPCIONES_AYTO["resoluciones_2022"],
              "CSV", _csv_bytes(registros[:3] + [cambiada, nueva]),
              last_modified="2026-09-20T10:00:00")
    web.quitar(ayto.DATASET_ACTIVIDAD, _rid("cesiones_2023"))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log

    historico = list(salida.glob("originales/actividad/_historico/resoluciones_2022__*.csv"))
    assert len(historico) == 1 and historico[0].read_bytes() == FICHEROS_AYTO["resoluciones_2022"]
    manifiesto = json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))
    ces = next(e for e in manifiesto.values() if e["id"] == _rid("cesiones_2023"))
    assert ces["estado"] == "retirado"

    fiel, uni = _leer(salida)
    res = fiel[fiel["_recurso"] == _rid("resoluciones_2022")]
    estado = {(r["N. DE EXPEDIENTE"], r["CAUSAS GENERALES"]): r["_en_ultima_descarga"] for _, r in res.iterrows()}
    assert estado == {("EXP-R2", "Mutuo acuerdo"): False,      # old version of the changed record
                      ("EXP-R3", "Incumplimiento"): False,     # withdrawn by the portal
                      ("EXP-R2", "Otra causa"): True,
                      ("EXP-R9", "Mutuo acuerdo"): True}
    assert res["_archivo_origen"].str.contains("_historico").sum() == 2
    assert (fiel.loc[fiel["_recurso"] == _rid("cesiones_2023"), "_en_ultima_descarga"] == False).all()  # noqa: E712
    # the unified table has exactly the same rows and flags
    assert uni[["_archivo_origen", "_fila_origen", "_en_ultima_descarga"]].values.tolist() == \
        fiel[["_archivo_origen", "_fila_origen", "_en_ultima_descarga"]].values.tolist()
    assert sorted(_nulos(uni.loc[uni["_recurso"] == _rid("resoluciones_2022"), "causas_generales"])) == \
        sorted(["Mutuo acuerdo", "Incumplimiento", "Otra causa", "Mutuo acuerdo"])
    # previous outputs are kept
    assert len(list((salida / "_historico").glob("actividad_contractual_madrid_original__*.parquet"))) == 1
    assert len(list((salida / "_historico").glob("actividad_contractual_madrid_completo__*.parquet"))) == 1

    # a resource that comes back is published again
    web.poner(ayto.DATASET_ACTIVIDAD, _rid("cesiones_2023"), DESCRIPCIONES_AYTO["cesiones_2023"], "CSV",
              FICHEROS_AYTO["cesiones_2023"])
    assert _ejecutar(salida)[0] == 0
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == _rid("cesiones_2023"), "_en_ultima_descarga"].all()


RESOLUCIONES_2022_CORTADO = FICHEROS_AYTO["resoluciones_2022"][:FICHEROS_AYTO["resoluciones_2022"].rindex(b"EXP-R3") + 4]


# (response, part of the error it must give, size given by CKAN or None = the response's own size)
DESCARGAS_RECHAZADAS = {
    "503": (_Resp(503, b"Service Unavailable"), "HTTP 503", None),
    "html": (_Resp(200, PAGINA_ERROR), "página HTML", None),
    "vacio": (_Resp(200, b""), "respuesta vacía", None),
    "solo_cabecera": (_Resp(200, ";".join(H_RESOLUCIONES).encode("utf-8") + b"\r\n"), "ningún registro con datos",
                      None),
    "cortado": (_Resp(200, b"R1;E1\r\nR2;E2\r\n", {"Content-Length": "999"}), "descarga incompleta", None),
    "red": (requests.ConnectionError("connection reset"), "ConnectionError", None),
    # 200 without HTML that is not the file: an error text (one column), an error
    # text with commas (two columns, no known column, no figure), a table sharing
    # no column with the previous version, a file without a header
    "texto_de_error": (_Resp(200, "Servicio no disponible temporalmente.\r\nInténtelo de nuevo más tarde.\r\n"
                                  .encode("utf-8")), "una sola columna", None),
    "texto_con_comas": (_Resp(200, "Servicio no disponible temporalmente, disculpe las molestias\r\n"
                                   "Inténtelo de nuevo más tarde, gracias\r\n".encode("utf-8")),
                        "no parece una tabla de contratos", None),
    "otra_cabecera": (_Resp(200, b"CAMPO A;CAMPO B;CAMPO C\r\nx1;y2;z3\r\n"), "ninguna columna en común", None),
    "sin_cabecera": (_Resp(200, b"01/04/2018;2018/5;EXP-R2;700.000,00\r\n02/04/2018;2018/6;EXP-R3;600.000,00\r\n"),
                     "no tiene cabecera", None),
    # cut in the middle of a record with a matching Content-Length (e.g. an export
    # cut on the server): only the size CKAN gives tells it apart; it used to leave a
    # made-up version of the last record ('EXP-R3' without the rest) for ever
    "tamano_distinto_de_ckan": (_Resp(200, RESOLUCIONES_2022_CORTADO,
                                      {"Content-Length": str(len(RESOLUCIONES_2022_CORTADO))}),
                                "CKAN indica", len(FICHEROS_AYTO["resoluciones_2022"])),
}


@pytest.mark.parametrize("caso", list(DESCARGAS_RECHAZADAS))
def test_ayto_failed_empty_or_wrong_download_replaces_nothing(web, tmp_path, caso):
    respuesta, error, tam_ckan = DESCARGAS_RECHAZADAS[caso]
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    fiel_antes = pd.read_parquet(salida / ayto.SALIDA_FIEL)
    rid = _rid("resoluciones_2022")
    # CKAN gives the size of what is served (each case reaches its own check) unless the case says otherwise
    contenido = respuesta.content if isinstance(respuesta, _Resp) else b"x"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV", contenido,
              last_modified="2026-09-20T10:00:00", **({"size": tam_ckan} if tam_ckan else {}))
    web.ficheros[web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")] = respuesta
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rid in e and error in e for e in _errores(log)), log

    ruta = next((salida / "originales" / "actividad").glob(f"*__{rid}.csv"))
    assert ruta.read_bytes() == FICHEROS_AYTO["resoluciones_2022"]
    assert not list(salida.glob("originales/*/_historico/*")) and not list(salida.glob("originales/*/.*"))
    manifiesto = json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))
    entrada = next(e for e in manifiesto.values() if e["id"] == rid)
    assert entrada["ckan"]["last_modified"] is None and entrada["ultimo_error"]   # retried next run
    fiel, _ = _leer(salida)
    pd.testing.assert_frame_equal(fiel, fiel_antes)                 # nothing marked as withdrawn


def test_ayto_a_first_download_of_an_error_text_is_rejected(web, tmp_path):
    # with no copy yet and no size in CKAN, an error text with commas (two columns)
    # used to become the first version: its sentences stayed as columns of the
    # faithful table and its second line as a withdrawn record
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", _csv_bytes([H_CAMBIO, R_CAMBIO_1]),
              size=None)
    url = web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")
    web.ficheros[url] = "Servicio no disponible temporalmente, disculpe las molestias\r\n" \
                        "Inténtelo de nuevo más tarde, gracias\r\n".encode("utf-8")
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rid in e and "no parece una tabla de contratos" in e for e in _errores(log))
    assert not list((salida / "originales" / "actividad").glob(f"*{rid}*"))
    web.ficheros[url] = _csv_bytes([H_CAMBIO, R_CAMBIO_1])
    assert _ejecutar(salida)[0] == 0
    fiel, _ = _leer(salida)
    assert not [c for c in fiel.columns if "Servicio" in c or "molestias" in c]
    assert fiel.loc[fiel["_recurso"] == rid, "N. DE EXPEDIENTE"].tolist() == ["EXP-1"]


def test_ayto_api_down_publishes_nothing(web, tmp_path):
    web.caidos = {ayto.DATASET_MENORES, ayto.DATASET_ACTIVIDAD}
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "la API CKAN no responde" in log
    assert not salida.exists() or not any(salida.rglob("*.parquet"))
    assert not (salida / "originales" / "_manifiesto.json").exists()

    # with a previous download, an API failure keeps everything as it was
    web.caidos = set()
    _portal_con_fixtures(web)
    assert _ejecutar(salida)[0] == 0
    fiel_antes, _ = _leer(salida)
    web.caidos = {ayto.DATASET_ACTIVIDAD}
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 1
    assert not any("216876" in u and "/resource/" in u for u in web.llamadas)
    fiel, _ = _leer(salida)
    pd.testing.assert_frame_equal(fiel, fiel_antes)
    assert not (salida / "_historico").exists()


def test_ayto_incremental_runs_request_only_what_may_have_changed(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    cerrado, en_curso = _rid("menores_2016"), _rid("menores_2025")

    web.llamadas.clear()
    assert _ejecutar(salida)[0] == 0
    assert web.pedidos(cerrado) == 0 and web.pedidos(en_curso) == 1

    # a closed year whose CKAN metadata changed is requested again (and, if the
    # content changed, the previous copy goes to _historico/)
    nuevo = FICHEROS_AYTO["menores_2016"] + "002;Distrito Sur;Obra 2;EXP-B2;B9;X SL;1,00;01/03/2016;Obras;02/03/2016\r\n".encode("cp1252")
    web.poner(ayto.DATASET_MENORES, cerrado, DESCRIPCIONES_AYTO["menores_2016"], "CSV", nuevo)
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0 and web.pedidos(cerrado) == 1 and "cambiado en el portal: hash, size" in log
    assert [p.read_bytes() for p in salida.glob(f"originales/menores/_historico/*{cerrado}*")] == [
        FICHEROS_AYTO["menores_2016"]]

    # --comprobar-todo requests everything; identical content is not duplicated
    web.llamadas.clear()
    assert _ejecutar(salida, "--comprobar-todo")[0] == 0
    assert all(web.pedidos(r["id"]) == 1 for rs in web.paquetes.values() for r in rs)
    assert len(list(salida.glob("originales/*/_historico/*"))) == 1


def test_ayto_solo_descargar_and_solo_procesar(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida, "--solo-descargar")[0] == 0
    assert (salida / "originales" / "_manifiesto.json").exists()
    assert not list(salida.glob("*.parquet"))

    web.llamadas.clear()
    assert _ejecutar(salida, "--solo-procesar")[0] == 0
    assert web.llamadas == []                                   # no network at all
    fiel, uni = _leer(salida)
    assert len(fiel) == len(uni) == sum(N_REGISTROS_AYTO.get(n, 1) for n in FICHEROS_AYTO)

    # the tables can be rebuilt even without the manifest (names carry id, category and year)
    (salida / "originales" / "_manifiesto.json").unlink()
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 0 and "no estaba en el manifiesto" in log
    assert len(pd.read_parquet(salida / ayto.SALIDA_FIEL)) == len(fiel)


def _un_menor(web):
    """Both datasets must list something (an empty listing is an API failure)."""
    web.poner(ayto.DATASET_MENORES, _rid("menores_2025"), DESCRIPCIONES_AYTO["menores_2025"], "CSV",
              FICHEROS_AYTO["menores_2025"])


def test_ayto_xlsx_is_consolidated_when_no_csv_has_its_records(web, tmp_path):
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO",
           "IMPORTE ADJUDICACION IVA INC.", "FECHA DE INSCRIPCION"]
    fila_csv = ["2019/1", "1,91E+11", "Obra A", "1.500,00", "05/03/2019"]
    fila_x = ["2019/1", 191202200633, "Obra A", 1500, datetime(2019, 3, 5)]
    # (a) CSV and XLSX with the same records (one cell differs in format): only the CSV
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-1-contratos-actividad-csv",
              "Contratos inscritos en el Registro de Contratos. 2019", "CSV", _csv_bytes([cab, fila_csv]))
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-2-contratos-actividad-xlsx",
              "Contratos inscritos en el Registro de Contratos. 2019", "XLSX", _xlsx([cab, fila_x]))
    # (b) a year only published as XLSX: consolidated
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-3-contratos-actividad-xlsx",
              "Resoluciones contratos. 2020", "XLSX",
              _xlsx([["RESOLUCIONES 2020"], [], cab, ["2020/7", "EXP-7", "Obra B", 99.5, datetime(2020, 1, 2)]]))
    # (c) the CSV of a group publishes other records (as 'formalizados 2015', whose
    # CSV holds the acuerdo marco contracts): its XLSX is consolidated too
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-4-contratos-actividad-csv",
              "Contratos formalizados inscritos en el Registro de Contratos. 2015", "CSV",
              _csv_bytes([cab, ["2015/9", "EXP-9", "Otra cosa", "1,00", "01/01/2015"]]))
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-5-contratos-actividad-xlsx",
              "Contratos formalizados inscritos en el Registro de Contratos. 2015", "XLSX",
              _xlsx([cab, ["2015/1", "EXP-1", "Obra C", 10, datetime(2015, 2, 3)],
                     ["2015/2", "EXP-2", "Obra D", 20, datetime(2015, 2, 4)],
                     ["2015/9", "EXP-9", "Otra cosa", 1, datetime(2015, 1, 1)]]))
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    fiel, uni = _leer(salida)
    assert fiel["_recurso"].value_counts().to_dict() == {
        _rid("menores_2025"): 1,
        "216876-1-contratos-actividad-csv": 1, "216876-3-contratos-actividad-xlsx": 1,
        "216876-4-contratos-actividad-csv": 1, "216876-5-contratos-actividad-xlsx": 3}
    x = fiel[fiel["_recurso"] == "216876-3-contratos-actividad-xlsx"].iloc[0]
    assert (x["_formato"], x["_fila_origen"], x["_encabezado"]) == ("xlsx", 4, "RESOLUCIONES 2020")
    assert x["IMPORTE ADJUDICACION IVA INC."] == "99.5" and x["FECHA DE INSCRIPCION"] == "2020-01-02"
    formalizados = uni[uni["_recurso"] == "216876-5-contratos-actividad-xlsx"].set_index("n_expediente")
    assert formalizados.loc["EXP-1", "importe_adjudicacion_iva_inc"] == 10.0
    assert formalizados.loc["EXP-1", "fecha_inscripcion"] == pd.Timestamp("2015-02-03")
    # the XLSX record that is also in a CSV is flagged, not dropped
    assert formalizados["_repetido_en_csv"].to_dict() == {"EXP-1": False, "EXP-2": False, "EXP-9": True}
    assert uni["_repetido_en_csv"].sum() == 1

    comparacion = pd.read_csv(salida / "informes" / "comparacion_csv_xlsx.csv", sep=";", dtype=str)
    a = comparacion[comparacion["xlsx"].str.contains("216876-2-", na=False)].iloc[0]
    assert (a["filas_iguales"], a["filas_casi_iguales"], a["consolidado_xlsx"]) == ("0", "1", "False")
    assert "N. DE EXPEDIENTE: 1" == a["columnas_con_diferencias"]
    assert "191202200633" in a["ejemplos"]
    c = comparacion[comparacion["xlsx"].str.contains("216876-5-", na=False)].iloc[0]
    assert (c["filas_iguales"], c["filas_solo_xlsx"], c["consolidado_xlsx"]) == ("1", "2", "True")


def test_ayto_duplicate_and_empty_rows_are_marked_not_dropped(web, tmp_path):
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE ADJUDICACION IVA INC."]
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-1-contratos-actividad-csv", "Resoluciones contratos. 2024",
              "CSV", _csv_bytes([cab, ["R1", "E1", "Obra", "1,00"], ["R1", "E1", "Obra", "1,00"],
                                 ["", "", "", ""], ["", "E2", "", ""]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    fiel, uni = _leer(salida)
    fiel = fiel[fiel["_recurso"] == "216876-1-contratos-actividad-csv"]
    uni = uni[uni["_recurso"] == "216876-1-contratos-actividad-csv"]
    assert len(fiel) == len(uni) == 4
    assert fiel["_duplicado"].tolist() == [False, True, False, False]
    assert uni["_fila_vacia"].tolist() == [False, False, True, True]
    assert _nulos(uni["n_expediente"]) == ["E1", "E1", None, "E2"]
    assert fiel["N. DE EXPEDIENTE"].tolist() == ["E1", "E1", "", "E2"]    # empty cell = ''


def test_ayto_a_version_without_data_rows_retires_nothing(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    rid = _rid("resoluciones_2022")
    solo_vacias = _csv_bytes([H_RESOLUCIONES] + [[""] * len(H_RESOLUCIONES)] * 3)
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV", solo_vacias,
              last_modified="2026-09-20T10:00:00")
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "no trae ningún registro con datos" in log     # the download is rejected
    ruta = next((salida / "originales" / "actividad").glob(f"*__{rid}.csv"))
    assert ruta.read_bytes() == FICHEROS_AYTO["resoluciones_2022"]

    # and if such a version is already in _historico/ (e.g. an older run), it is skipped
    historico = ruta.parent / "_historico" / f"{ruta.stem}__20260101T000000Z.csv"
    historico.parent.mkdir()
    historico.write_bytes(FICHEROS_AYTO["resoluciones_2022"])
    ruta.write_bytes(solo_vacias)
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 0 and "sin registros con datos" in log
    fiel, uni = _leer(salida)
    res = fiel[fiel["_recurso"] == rid]
    assert len(res) == 2 and res["_en_ultima_descarga"].all()
    fuera = pd.read_csv(salida / "informes" / "lineas_fuera_de_tabla.csv", sep=";", dtype=str)
    assert (fuera["archivo"].str.endswith(ruta.name) & (fuera["tipo"] == "vacia")).sum() == 3


def test_ayto_a_consolidated_xlsx_stays_when_the_csv_appears(web, tmp_path):
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE CEDIDO"]
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-1-contratos-actividad-xlsx",
              "Cesiones de contratos inscritos en el Registro de Contratos. 2020", "XLSX",
              _xlsx([cab, ["2020/1", "EXP-1", "Obra", 5]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    fiel, _ = _leer(salida)
    assert fiel["_recurso"].tolist().count("216876-1-contratos-actividad-xlsx") == 1

    # the portal adds the CSV with the same record: the XLSX rows do not vanish,
    # they are flagged as also present in a CSV
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-2-contratos-actividad-csv",
              "Cesiones de contratos inscritos en el Registro de Contratos. 2020", "CSV",
              _csv_bytes([cab, ["2020/1", "EXP-1", "Obra", "5,00"]]))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    fiel, uni = _leer(salida)
    x = fiel[fiel["_recurso"] == "216876-1-contratos-actividad-xlsx"]
    assert len(x) == 1 and x["_en_ultima_descarga"].all() and x["_repetido_en_csv"].all()
    assert (fiel["_recurso"] == "216876-2-contratos-actividad-csv").sum() == 1
    manifiesto = json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert next(e for e in manifiesto.values() if e["id"] == "216876-1-contratos-actividad-xlsx")[
        "consolidado_desde"]


def test_ayto_a_url_change_is_retried_until_it_downloads(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    rid = _rid("menores_2016")                                      # closed year
    recurso = next(r for r in web.paquetes[ayto.DATASET_MENORES] if r["id"] == rid)
    recurso["url"] = recurso["url"].replace("/download/", "/download/nuevo-")
    codigo, log = _ejecutar(salida)                                 # the new URL answers 404
    assert codigo == 1 and web.pedidos(rid) >= 1
    web.ficheros[recurso["url"]] = FICHEROS_AYTO["menores_2016"]
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0 and web.pedidos(rid) == 1 and "cambiado en el portal: url" in log
    web.llamadas.clear()
    assert _ejecutar(salida)[0] == 0 and web.pedidos(rid) == 0     # done: not requested again


def test_ayto_a_resource_listed_without_url_is_not_retired(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    rid = _rid("cesiones_2023")
    next(r for r in web.paquetes[ayto.DATASET_ACTIVIDAD] if r["id"] == rid)["url"] = ""
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "no tiene URL" in log
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == rid, "_en_ultima_descarga"].all()


def test_ayto_tables_never_shrink_silently(web, tmp_path):
    # a table with fewer rows of some file is not written: it used to replace the
    # previous one and become the reference, so the next run said nothing
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    fiel_antes, uni_antes = _leer(salida)
    next((salida / "originales" / "actividad").glob(f"*__{_rid('resoluciones_2022')}.csv")).unlink()
    for _ in range(2):
        codigo, log = _ejecutar(salida, "--solo-procesar")
        assert codigo == 1 and "revisa los originales" in log and "No se sustituyen las tablas" in log
        fiel, uni = _leer(salida)
        pd.testing.assert_frame_equal(fiel, fiel_antes)            # the previous tables stay in place
        pd.testing.assert_frame_equal(uni, uni_antes)
    assert not (salida / "_historico").exists()


def test_ayto_comprobar_contenido_rejects_what_is_not_the_file():
    for datos, formato in [(PAGINA_ERROR, "csv"), (PAGINA_ERROR, "json"), (b"  \r\n", "csv"),
                           (b"", "pdf"), (b"R1;E1\r\n", "xlsx"), (b"PK\x03\x04...", "xls"),
                           (b"<html><body>x</body></html>", "pdf")]:
        with pytest.raises(ayto.ErrorDescarga):
            ayto.comprobar_contenido(datos, formato)
    for datos, formato in [(b"A;B\r\n1;2\r\n", "csv"), (b"%PDF-1.4", "pdf"), (b"PK\x03\x04...", "xlsx"),
                           (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1...", "xls"), (b'{"a": 1}', "json")]:
        ayto.comprobar_contenido(datos, formato)


def test_ayto_a_listing_that_loses_most_resources_retires_nothing(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.paquetes[ayto.DATASET_ACTIVIDAD] = web.paquetes[ayto.DATASET_ACTIVIDAD][:1]
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "no se marca ninguno como retirado" in log
    manifiesto = json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert all(e["estado"] == "publicado" for e in manifiesto.values())
    fiel, _ = _leer(salida)
    assert fiel["_en_ultima_descarga"].all()


# -----------------------------------------------------------------------------
# Adversarial review (round 1): every re-run keeps what the portal published
# -----------------------------------------------------------------------------
def _manifiesto(salida):
    return json.loads((salida / "originales" / "_manifiesto.json").read_text(encoding="utf-8"))


def _entrada(salida, rid):
    return next(e for e in _manifiesto(salida).values() if e["id"] == rid)


def _errores(log):
    return [linea for linea in log.splitlines() if "ERROR:" in linea]


H_CAMBIO = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "NIF ADJUDICATARIO",
            "IMPORTE ADJUDICACION IVA INC."]
R_CAMBIO_1 = ["2026/1", "EXP-1", "Obras parque", "B11111111", "800.000,00"]
R_CAMBIO_2 = ["2026/2", "EXP-2", "Obras plaza", "B22222222", "700.000,00"]
R_CAMBIO_3 = ["2026/3", "EXP-3", "Obras calle", "B33333333", "600.000,00"]


def _sin_columna(filas, j):
    return [f[:j] + f[j + 1:] for f in filas]


# (description, version 1, version 2, rows in force in the unified table as
# (n_expediente, nif_adjudicatario, importe_adjudicacion_iva_inc, codigo_cpv))
CAMBIOS_DE_CABECERA = {
    # the portal adds the NIF column to the same records (and one more record)
    "column_added": ("Resoluciones contratos. 2026",
                     _sin_columna([H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2], 3), [H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2, R_CAMBIO_3],
                     [("EXP-1", "B11111111", 800000.0, None), ("EXP-2", "B22222222", 700000.0, None),
                      ("EXP-3", "B33333333", 600000.0, None)]),
    # ...or a CPV code
    "cpv_added": ("Contratos inscritos en el Registro de Contratos. 2025",
                  [H_CAMBIO, R_CAMBIO_1],
                  [H_CAMBIO + ["CODIGO CPV"], R_CAMBIO_1 + ["45000000"], R_CAMBIO_2 + ["45000001"]],
                  [("EXP-1", "B11111111", 800000.0, "45000000"), ("EXP-2", "B22222222", 700000.0, "45000001")]),
    # the portal stops publishing the NIF: it is no longer served as in force
    "column_removed": ("Resoluciones contratos. 2026",
                       [H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2], _sin_columna([H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2], 3),
                       [("EXP-1", None, 800000.0, None), ("EXP-2", None, 700000.0, None)]),
    # renamed columns, the records in another order and an amount changed under a
    # renamed column: two records used to end up in one row
    "columns_renamed": ("Resoluciones contratos. 2026",
                        [H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2],
                        [["Nº REGISTRO CONTRATO", "Nº EXPEDIENTE", "OBJETO DEL CONTRATO", "NIF",
                          "IMPORTE ADJUDICACION IVA INCLUIDO"], R_CAMBIO_2, R_CAMBIO_1[:4] + ["900.000,00"]],
                        [("EXP-1", "B11111111", 900000.0, None), ("EXP-2", "B22222222", 700000.0, None)]),
    # every column renamed with the names of another period (the 2021 files): still the file
    "all_columns_renamed": ("Resoluciones contratos. 2026",
                            [H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2],
                            [["INCIDENCIA", "CONTRATO", "EXPEDIENTE", "OBJETO_CONTRATO", "CIF",
                              "IMPORTE_ADJUDICACIÓN"], ["Resolución"] + R_CAMBIO_1, ["Resolución"] + R_CAMBIO_2],
                            [("EXP-1", "B11111111", 800000.0, None), ("EXP-2", "B22222222", 700000.0, None)]),
}


@pytest.mark.parametrize("caso", sorted(CAMBIOS_DE_CABECERA))
def test_ayto_a_header_change_never_mixes_records(web, tmp_path, caso):
    descripcion, v1, v2, en_vigor = CAMBIOS_DE_CABECERA[caso]
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, descripcion, "CSV", _csv_bytes(v1))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rid, descripcion, "CSV", _csv_bytes(v2))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert "la cabecera cambió" in log

    fiel, uni = _leer(salida)
    f = fiel[fiel["_recurso"] == rid]
    # no row mixes two versions: it has values exactly under the header of its own version
    originales = [c for c in fiel.columns if c not in ayto.COLUMNAS_RESERVADAS]
    for archivo, grupo in f.groupby("_archivo_origen"):
        cabecera = ayto.leer_tabla(salida / archivo, grupo["_categoria"].iloc[0]).columnas
        for columna in originales:
            assert (grupo[columna].notna() if columna in cabecera else grupo[columna].isna()).all(), \
                (archivo, columna)
    # the rows in force are exactly the records of the last download; the previous
    # ones stay, no longer served
    vigentes, antiguas = f[f["_en_ultima_descarga"]], f[~f["_en_ultima_descarga"]]
    assert sorted(map(tuple, vigentes[v2[0]].values.tolist())) == sorted(map(tuple, v2[1:]))
    assert sorted(map(tuple, antiguas[v1[0]].values.tolist())) == sorted(map(tuple, v1[1:]))
    assert not vigentes["_archivo_origen"].str.contains("_historico").any()
    assert antiguas["_archivo_origen"].str.contains("_historico").all()
    # the unified table serves what the portal publishes now, mapped with its header
    u = uni[(uni["_recurso"] == rid) & uni["_en_ultima_descarga"]]
    columnas = ["n_expediente", "nif_adjudicatario", "importe_adjudicacion_iva_inc", "codigo_cpv"]
    assert sorted(tuple(_nulos(fila)) for fila in u[columnas].values.tolist()) == en_vigor
    assert uni[["_archivo_origen", "_fila_origen"]].values.tolist() == \
        fiel[["_archivo_origen", "_fila_origen"]].values.tolist()


def test_ayto_a_record_inserted_above_the_others_does_not_duplicate_them(web, tmp_path):
    # every position changes but the records do not: nothing is duplicated, and
    # each row points to the version and position where it is served now
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    rid = _rid("resoluciones_2022")
    enero = datetime(2026, 1, 10, 10, tzinfo=timezone.utc).timestamp()
    os.utime(next((salida / "originales" / "actividad").glob(f"*__{rid}.csv")), (enero, enero))
    registros = FICHEROS_AYTO["resoluciones_2022"].decode("utf-8").split("\r\n")
    nuevo = registros[3].replace("R2", "R8").replace("INC-5", "INC-8")
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV",
              "\r\n".join(registros[:3] + [nuevo] + registros[3:]).encode("utf-8"),
              last_modified="2026-09-20T10:00:00")
    assert _ejecutar(salida)[0] == 0
    fiel, uni = _leer(salida)
    r = fiel[fiel["_recurso"] == rid]
    assert sorted(r["N. DE EXPEDIENTE"]) == ["EXP-R2", "EXP-R3", "EXP-R8"]
    assert r["_en_ultima_descarga"].all() and not r["_duplicado"].any()
    assert not r["_archivo_origen"].str.contains("_historico").any()
    assert dict(zip(r["N. DE EXPEDIENTE"], r["_fila_origen"])) == {"EXP-R8": 4, "EXP-R2": 5, "EXP-R3": 6}
    primera = dict(zip(r["N. DE EXPEDIENTE"], r["_primera_descarga"]))
    assert primera["EXP-R2"] == primera["EXP-R3"] == ayto.iso_de_epoch(enero) != primera["EXP-R8"]


def test_ayto_an_unreadable_consolidated_excel_is_a_failure(web, tmp_path, monkeypatch):
    # 'formalizados 2015' only exists in its XLS: if it can no longer be read (e.g.
    # without xlrd) its rows used to vanish without any failure, the more so if
    # another file grew more than the XLS had
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE CEDIDO"]
    rx, rc = "216876-1-contratos-actividad-xlsx", "216876-2-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rx, "Cesiones de contratos inscritos en el Registro de Contratos. 2020",
              "XLSX", _xlsx([cab, ["2020/1", "EXP-1", "Obra", 5], ["2020/2", "EXP-2", "Obra 2", 6]]))
    penalidades = "Penalidades en contratos inscritas en el Registro de Contratos. 2026"

    def _penalidades(n):
        return _csv_bytes([cab] + [[f"2026/{i}", f"EXP-P{i}", "Obra", "1,00"] for i in range(n)])
    web.poner(ayto.DATASET_ACTIVIDAD, rc, penalidades, "CSV", _penalidades(2))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    assert _entrada(salida, rx)["consolidado_desde"]
    copia = tmp_path / "copia"
    shutil.copytree(salida / "originales", copia / "originales")

    monkeypatch.setitem(sys.modules, "openpyxl", None)          # the XLSX can no longer be read
    web.poner(ayto.DATASET_ACTIVIDAD, rc, penalidades, "CSV", _penalidades(10))
    codigo, log = _ejecutar(salida)
    assert codigo == 1
    assert any(rx in e and "no se puede leer" in e for e in _errores(log))
    assert any(rx in e and "0 filas en la tabla fiel y 2 en la anterior" in e for e in _errores(log))
    # the same with --solo-procesar on a copy of originales/ (no previous table there)
    codigo, log = _ejecutar(copia, "--solo-procesar")
    assert codigo == 1 and any(rx in e and "no se puede leer" in e for e in _errores(log))


def test_ayto_an_unreadable_excel_twin_is_a_failure(web, tmp_path, monkeypatch):
    # a XLSX/XLS that cannot be read cannot be compared with its CSV: it may hold
    # records that no CSV has (as 'formalizados 2015')
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    monkeypatch.setitem(sys.modules, "openpyxl", None)
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 1
    assert any(_rid("menores_2025", "xlsx") in e and "no se puede leer" in e for e in _errores(log))


class _Corte(BaseException):
    """The process dies (SIGHUP, SIGKILL...) in the middle of a download."""


def _gemelos_menores_2023(web):
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE ADJUDICACION IVA INC."]
    filas = [cab, ["2023/1", "EXP-1", "Obra A", "1.500,00"], ["2023/2", "EXP-2", "Obra B", "10,00"]]
    rx, rc = "300253-6-contratos-actividad-menores-xlsx", "300253-4-contratos-actividad-menores-csv"
    web.poner(ayto.DATASET_MENORES, rx, "Contratos menores 2023", "XLSX", _xlsx(filas))
    web.poner(ayto.DATASET_MENORES, rc, "Contratos menores 2023", "CSV", _csv_bytes(filas))
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_RESOLUCIONES, ["Resolución", "", "2026/1", "EXP-9"] + [""] * 10]))
    return rx, rc, web.url(ayto.DATASET_MENORES, rc, "CSV"), filas


def test_ayto_a_csv_not_downloaded_yet_does_not_consolidate_its_twin(web, tmp_path):
    # a transient failure of the CSV used to consolidate its XLSX twin forever
    # ('its group has no CSV'): every contract then appeared twice
    rx, rc, url, filas = _gemelos_menores_2023(web)
    web.ficheros[url] = _Resp(503, b"Service Unavailable")
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 1
    assert any(rx in e and "no se decide si se consolida" in e for e in _errores(log))
    fiel, _ = _leer(salida)
    assert rx not in set(fiel["_recurso"]) and not _entrada(salida, rx).get("consolidado_desde")

    web.ficheros[url] = _csv_bytes(filas)                        # the CSV is back
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    fiel, _ = _leer(salida)
    assert (fiel["_recurso"] == rc).sum() == 2 and rx not in set(fiel["_recurso"])
    assert not _entrada(salida, rx).get("consolidado_desde")


def test_ayto_an_interrupted_download_does_not_consolidate_the_twin(web, tmp_path):
    # --solo-descargar dies after the XLSX and before the CSV: the listing was
    # already in the manifest, so --solo-procesar knows the CSV is missing
    rx, rc, url, filas = _gemelos_menores_2023(web)
    web.ficheros[url] = _Corte()
    salida = tmp_path / "salida"
    with pytest.raises(_Corte):
        _ejecutar(salida, "--solo-descargar")
    assert rc in {e["id"] for e in _manifiesto(salida).values()}
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 1 and any(rx in e and "no se decide" in e for e in _errores(log))
    assert not _entrada(salida, rx).get("consolidado_desde")


def test_ayto_excel_rows_in_a_csv_are_flagged_even_with_one_cell_different(web, tmp_path):
    # A XLSX consolidated while its group had no CSV stays consolidated when the
    # CSV appears. Its rows that are in the CSV are flagged, also those with one
    # different cell (the CSV writes the expediente in scientific notation), each
    # CSV row pairing with a single XLSX row
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE ADJUDICACION IVA INC."]
    rx, rc = "216876-1-contratos-actividad-xlsx", "216876-2-contratos-actividad-csv"
    descripcion = "Contratos inscritos en el Registro de Contratos. 2019"
    web.poner(ayto.DATASET_ACTIVIDAD, rx, descripcion, "XLSX", _xlsx([
        cab, ["2019/1", 191202200633, "Obra A", 1500], ["2019/2", "EXP-2", "Obra B", 10],
        ["2019/2", "EXP-2", "Obra B", 12]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rc, descripcion, "CSV", _csv_bytes([
        cab, ["2019/1", "1,91E+11", "Obra A", "1.500,00"], ["2019/2", "EXP-2", "Obra B", "10,00"]]))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    _, uni = _leer(salida)
    x = uni[uni["_recurso"] == rx]
    assert list(zip(_nulos(x["n_expediente"]), x["importe_adjudicacion_iva_inc"], x["_repetido_en_csv"])) == [
        ("191202200633", 1500.0, True), ("EXP-2", 10.0, True), ("EXP-2", 12.0, False)]


def test_ayto_rows_of_a_withdrawn_csv_do_not_flag_the_published_xlsx(web, tmp_path):
    _un_menor(web)
    descripcion = "Resoluciones contratos. 2022"
    filas = [H_RESOLUCIONES[:5], ["Resolución", "01/04/2018", "2018/5", "EXP-1", "Centro S"]]
    rc, rx = "216876-64-contratos-actividad-csv", "216876-67-contratos-actividad-xlsx"
    web.poner(ayto.DATASET_ACTIVIDAD, rc, descripcion, "CSV", _csv_bytes(filas))
    web.poner(ayto.DATASET_ACTIVIDAD, rx, descripcion, "XLSX", _xlsx(filas))
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1]))                # another file: the listing is never empty
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.quitar(ayto.DATASET_ACTIVIDAD, rc)                      # the CSV is withdrawn, the XLSX stays
    # while its twin is listed, a CSV is withdrawn when two listings in a row miss it
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rx in e and "no se decide" in e for e in _errores(log))
    assert _entrada(salida, rc)["estado"] == "publicado" and not _entrada(salida, rx).get("consolidado_desde")
    assert _ejecutar(salida)[0] == 0
    assert _entrada(salida, rc)["estado"] == "retirado"
    _, uni = _leer(salida)
    u = uni[uni["_recurso"].isin([rc, rx])]
    en_vigor = u[u["_en_ultima_descarga"] & ~u["_repetido_en_csv"]]
    assert en_vigor[["_recurso", "n_expediente"]].values.tolist() == [[rx, "EXP-1"]]
    # if the XLSX is withdrawn too, its rows are repeated in the (withdrawn) CSV
    web.quitar(ayto.DATASET_ACTIVIDAD, rx)
    assert _ejecutar(salida)[0] == 0
    _, uni = _leer(salida)
    u = uni[uni["_recurso"].isin([rc, rx])]
    assert not u["_en_ultima_descarga"].any()
    assert u[["_recurso", "_repetido_en_csv"]].values.tolist() == [[rc, False], [rx, True]]


CESIONES_2020 = "Cesiones de contratos inscritos en el Registro de Contratos. 2020"
CAB_CESIONES = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE CEDIDO",
                "FECHA AUTORIZACION CESION"]


def test_ayto_every_excel_of_a_group_without_csv_with_own_records_enters(web, tmp_path):
    _un_menor(web)
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-140-contratos-actividad-xlsx", CESIONES_2020, "XLSX",
              _xlsx([CAB_CESIONES, ["2020/1", "EXP-1", "Obra A", 5, datetime(2020, 1, 2)]]))
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-141-contratos-actividad-xlsx", CESIONES_2020, "XLSX",
              _xlsx([CAB_CESIONES, ["2020/2", "EXP-2", "Obra B", 7, datetime(2020, 2, 3)],
                     ["2020/3", "EXP-3", "Obra C", 9, datetime(2020, 3, 4)]]))
    # and a copy of the first one with the same records (e.g. the XLS of the same year): it does not
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-142-contratos-actividad-xlsx", CESIONES_2020, "XLSX",
              _xlsx([CAB_CESIONES, ["2020/1", "EXP-1", "Obra A", 5, datetime(2020, 1, 2)]]))
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    _, uni = _leer(salida)
    assert sorted(uni.loc[uni["_recurso"].str.startswith("216876"), "n_expediente"]) == ["EXP-1", "EXP-2", "EXP-3"]


def test_ayto_a_renumbered_excel_is_consolidated_before_the_withdrawn_one(web, tmp_path):
    _un_menor(web)
    viejo, nuevo = "216876-140-contratos-actividad-xlsx", "216876-3-contratos-actividad-xlsx"
    web.poner(ayto.DATASET_ACTIVIDAD, viejo, CESIONES_2020, "XLSX",
              _xlsx([CAB_CESIONES, ["2020/1", "EXP-1", "Obra A", 5, datetime(2020, 1, 2)]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.quitar(ayto.DATASET_ACTIVIDAD, viejo)                   # same file, new id, one more record
    web.poner(ayto.DATASET_ACTIVIDAD, nuevo, CESIONES_2020, "XLSX",
              _xlsx([CAB_CESIONES, ["2020/1", "EXP-1", "Obra A", 5, datetime(2020, 1, 2)],
                     ["2020/4", "EXP-4", "Obra D", 11, datetime(2020, 4, 5)]]))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == nuevo, ["N. DE EXPEDIENTE", "_en_ultima_descarga"]].values.tolist() == [
        ["EXP-1", True], ["EXP-4", True]]
    # the withdrawn copy keeps its rows, no longer in force
    assert fiel.loc[fiel["_recurso"] == viejo, ["N. DE EXPEDIENTE", "_en_ultima_descarga"]].values.tolist() == [
        ["EXP-1", False]]


def test_ayto_a_reused_id_is_a_new_file(web, tmp_path):
    # In datos.madrid.es the id comes from the resource position: if the portal
    # gives the id of 'Resoluciones 2024' to 'Penalidades 2026', the resolution
    # (its only copy) used to be relabelled and mapped as a 2026 penalty
    _un_menor(web)
    rid = "216876-89-contratos-actividad-csv"
    h_res = ["TIPO DE INCIDENCIA", "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO",
             "CAUSAS GENERALES", "CAUSAS ESPECIFICAS", "OTRAS CAUSAS", "FECHA ACUERDO RESOLUCION"]
    h_pen = ["TIPO DE INCIDENCIA", "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO",
             "FECHA ACUERDO PENALIDAD", "IMPORTE PENALIDAD", "CAUSA"]
    resoluciones = _csv_bytes([h_res, ["Resolución", "2024/1", "EXP-R1", "Obras", "Mutuo acuerdo", "Art. 211",
                                       "Otra", "11/11/2024"]])
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2024", "CSV", resoluciones)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Penalidades en contratos inscritas en el Registro de Contratos. 2026",
              "CSV", _csv_bytes([h_pen, ["Penalidad", "2026/9", "EXP-P9", "Limpieza", "05/07/2026", "1.000,00",
                                         "Retraso"]]))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert "se trata como un fichero nuevo" in log

    carpeta = salida / "originales" / "actividad"
    assert (carpeta / f"resoluciones_2024__{rid}.csv").read_bytes() == resoluciones
    assert (carpeta / f"penalidades_2026__{rid}.csv").exists()
    estados = {clave.rsplit("/", 1)[-1]: e["estado"] for clave, e in _manifiesto(salida).items() if e["id"] == rid}
    assert estados == {f"resoluciones_2024__{rid}.csv": "retirado", f"penalidades_2026__{rid}.csv": "publicado"}
    fiel, uni = _leer(salida)
    f = fiel[fiel["_recurso"] == rid].set_index("N. DE EXPEDIENTE")
    assert f.loc["EXP-R1", ["_categoria", "_anio_fichero", "_en_ultima_descarga"]].tolist() == [
        "resoluciones", 2024, False]
    assert f.loc["EXP-P9", ["_categoria", "_anio_fichero", "_en_ultima_descarga"]].tolist() == [
        "penalidades", 2026, True]
    u = uni[uni["_recurso"] == rid].set_index("n_expediente")
    assert u.loc["EXP-R1", ["categoria", "causas_generales", "fecha_acuerdo_resolucion_texto"]].tolist() == [
        "resoluciones", "Mutuo acuerdo", "11/11/2024"]
    assert pd.isna(u.loc["EXP-R1", "causa_penalidad"])
    assert u.loc["EXP-P9", ["categoria", "causa_penalidad"]].tolist() == ["penalidades", "Retraso"]


@pytest.mark.parametrize("ckan_da_el_tamano", [True, False], ids=["con_size", "sin_size"])
def test_ayto_a_first_download_without_records_is_requested_again(web, tmp_path, ckan_da_el_tamano):
    # CKAN describes the real file, but the first answer only had the header: it
    # used to be accepted and, in a closed year, never requested again. With the
    # size in CKAN it is rejected; without it, it is accepted and requested again
    _un_menor(web)
    rid = "216876-64-contratos-actividad-csv"
    real = FICHEROS_AYTO["resoluciones_2022"]
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV", real,
              **({} if ckan_da_el_tamano else {"size": None}))
    url = web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")
    web.ficheros[url] = _csv_bytes([H_RESOLUCIONES])
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    if ckan_da_el_tamano:
        assert codigo == 1 and any(rid in e and "CKAN indica" in e for e in _errores(log))
        assert not list((salida / "originales" / "actividad").glob(f"*{rid}*"))
    else:
        assert codigo == 0 and "sin registros con datos" in log
        assert _entrada(salida, rid)["filas_con_datos"] == 0

    web.ficheros[url] = real
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert web.pedidos(rid) == 1
    assert ("nuevo" if ckan_da_el_tamano else "la copia no tiene registros con datos") in log
    fiel, _ = _leer(salida)
    assert sorted(fiel.loc[fiel["_recurso"] == rid, "N. DE EXPEDIENTE"]) == ["EXP-R2", "EXP-R3"]
    web.llamadas.clear()
    assert _ejecutar(salida)[0] == 0 and web.pedidos(rid) == 0      # complete: a closed year is not requested


def test_ayto_a_file_still_without_records_is_not_a_failure(web, tmp_path):
    # a file of the current year that only has its header (e.g. just created):
    # requested on every run, and the same content again is not rejected
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", _csv_bytes([H_RESOLUCIONES]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0 and web.pedidos(rid) == 1, log
    assert not list(salida.glob("originales/*/_historico/*"))


def test_ayto_a_copy_that_is_not_the_size_ckan_describes_is_requested_again(web, tmp_path):
    # e.g. an export cut at a line boundary (a valid CSV with fewer records): the
    # download is rejected (it used to be accepted and requested again later)
    _un_menor(web)
    rid = "216876-64-contratos-actividad-csv"
    real = FICHEROS_AYTO["resoluciones_2022"]
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV", real)
    url = web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")
    web.ficheros[url] = b"\r\n".join(real.split(b"\r\n")[:4]) + b"\r\n"
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rid in e and "CKAN indica" in e for e in _errores(log))
    web.ficheros[url] = real
    assert _ejecutar(salida)[0] == 0
    fiel, _ = _leer(salida)
    r = fiel[fiel["_recurso"] == rid]
    assert sorted(r["N. DE EXPEDIENTE"]) == ["EXP-R2", "EXP-R3"] and r["_en_ultima_descarga"].all()

    # a copy kept from before this check (not the size CKAN gives) is requested again
    ruta = next((salida / "originales" / "actividad").glob(f"*__{rid}.csv"))
    ruta.write_bytes(b"\r\n".join(real.split(b"\r\n")[:4]) + b"\r\n")
    web.llamadas.clear()
    codigo, log = _ejecutar(salida)
    assert codigo == 0 and web.pedidos(rid) == 1 and "no tiene el tamaño que indica CKAN" in log
    assert ruta.read_bytes() == real


def test_ayto_an_interrupted_run_keeps_every_version_with_its_date(web, tmp_path, monkeypatch):
    # The process dies after saving version 2 and before writing the manifest:
    # the next run found it 'sin_cambios' and never recorded it, and its new
    # records took the date of version 1
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    h = ["TIPO DE INCIDENCIA", "N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "CAUSAS GENERALES"]
    v1 = _csv_bytes([h, ["Resolución", "2026/1", "EXP-1", "Mutuo acuerdo"]])
    v2 = _csv_bytes([h, ["Resolución", "2026/1", "EXP-1", "Mutuo acuerdo"], ["Resolución", "2026/2", "EXP-2", "Otra"]])
    enero = datetime(2026, 1, 10, 10, tzinfo=timezone.utc).timestamp()
    febrero = datetime(2026, 2, 10, 10, tzinfo=timezone.utc).timestamp()
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", v1)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    ruta = next((salida / "originales" / "actividad").glob(f"*__{rid}.csv"))
    os.utime(ruta, (enero, enero))
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", v2)
    with monkeypatch.context() as m:                            # the run that dies
        m.setattr(ayto.Manifiesto, "guardar", lambda self: None)
        m.setattr(ayto, "procesar", lambda *a, **k: None)
        _ejecutar(salida)
    os.utime(ruta, (febrero, febrero))
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert "no estaba anotada" in log
    entrada = _entrada(salida, rid)
    assert [v["estado"] for v in entrada["versiones"]] == ["nuevo", "no_registrada"]
    assert entrada["versiones"][1]["fecha"] == entrada["fecha_descarga"] == ayto.iso_de_epoch(febrero)
    assert entrada["sha256"] == hashlib.sha256(v2).hexdigest()
    fiel, _ = _leer(salida)
    fechas = fiel[fiel["_recurso"] == rid].set_index("N. DE EXPEDIENTE")["_primera_descarga"]
    assert fechas.to_dict() == {"EXP-1": ayto.iso_de_epoch(enero), "EXP-2": ayto.iso_de_epoch(febrero)}


def test_ayto_the_date_of_a_version_does_not_change_when_it_is_archived(web, tmp_path):
    # the version in force is dated by its modification time, which is the stamp
    # it gets in _historico/ (the manifest date used to be taken a bit later)
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", _csv_bytes([H_CAMBIO, R_CAMBIO_1]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    ruta = next((salida / "originales" / "actividad").glob(f"*__{rid}.csv"))
    momento = datetime(2026, 1, 10, 10, 0, 1, tzinfo=timezone.utc).timestamp()
    os.utime(ruta, (momento, momento))
    assert _ejecutar(salida, "--solo-procesar")[0] == 0
    antes = _leer(salida)[0].set_index("N. DE EXPEDIENTE").loc["EXP-1", "_primera_descarga"]
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2]))
    assert _ejecutar(salida)[0] == 0
    despues = _leer(salida)[0].set_index("N. DE EXPEDIENTE").loc["EXP-1", "_primera_descarga"]
    assert antes == despues == ayto.iso_de_epoch(momento) == "2026-01-10T10:00:01Z"


def test_ayto_excel_amounts_with_three_decimals_are_not_thousands(web, tmp_path):
    _un_menor(web)
    cab = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE ADJUDICACION IVA INC."]
    rx = "216876-3-contratos-actividad-xlsx"
    web.poner(ayto.DATASET_ACTIVIDAD, rx, "Resoluciones contratos. 2020", "XLSX", _xlsx([
        cab, ["2020/7", "EXP-7", "Obra B", 1.125], ["2020/8", "EXP-8", "Obra C", 123.456],
        ["2020/9", "EXP-9", "Obra D", "1.234,56"],                   # text cells: Spanish format, as in a CSV
        ["2020/10", "EXP-10", "Obra E", "15.000"], ["2020/11", "EXP-11", "Obra F", "1.125"]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    _, uni = _leer(salida)
    x = uni[uni["_recurso"] == rx].set_index("n_expediente")
    assert x["importe_adjudicacion_iva_inc"].to_dict() == {"EXP-7": 1.125, "EXP-8": 123.456, "EXP-9": 1234.56,
                                                           "EXP-10": 15000.0, "EXP-11": 1125.0}
    assert x["importe_adjudicacion_iva_inc_texto"].to_dict() == {"EXP-7": "1.125", "EXP-8": "123.456",
                                                                 "EXP-9": "1.234,56", "EXP-10": "15.000",
                                                                 "EXP-11": "1.125"}


def test_ayto_the_previous_manifest_and_reports_are_kept(web, tmp_path):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    manifiesto = (salida / "originales" / "_manifiesto.json").read_bytes()
    lectura = (salida / "informes" / "lectura_ficheros.csv").read_bytes()
    registros = FICHEROS_AYTO["resoluciones_2022"].decode("utf-8").split("\r\n")
    web.poner(ayto.DATASET_ACTIVIDAD, _rid("resoluciones_2022"), DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV",
              "\r\n".join(registros[:4]).encode("utf-8") + b"\r\n", last_modified="2026-09-20T10:00:00")
    assert _ejecutar(salida)[0] == 0
    # one previous manifest per run (not one per download) and the previous reports
    assert [p.read_bytes() for p in (salida / "originales" / "_historico").glob("_manifiesto__*.json")] == [
        manifiesto]
    assert lectura in [p.read_bytes() for p in (salida / "informes" / "_historico").glob("lectura_ficheros__*.csv")]


def test_ayto_a_lost_manifest_is_recovered_from_its_history(web, tmp_path):
    _un_menor(web)
    rid = "216876-64-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, DESCRIPCIONES_AYTO["resoluciones_2022"], "CSV",
              FICHEROS_AYTO["resoluciones_2022"])
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-89-contratos-actividad-csv", "Resoluciones contratos. 2024", "CSV",
              FICHEROS_AYTO["resoluciones_2023"])
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_MENORES, _rid("menores_2025"), DESCRIPCIONES_AYTO["menores_2025"], "CSV",
              FICHEROS_AYTO["menores_2025"] + "2025/2;EXP-F2;;;;;;;;;;;1,00;;;\r\n".encode("utf-8"))
    assert _ejecutar(salida)[0] == 0
    fiel_antes = _leer(salida)[0]
    (salida / "originales" / "_manifiesto.json").unlink()
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 0 and "se recupera el último guardado en _historico/" in log
    assert "no estaba en el manifiesto" not in log
    fiel = _leer(salida)[0]
    assert fiel["_descripcion"].notna().all()                   # not deduced from the file names
    pd.testing.assert_frame_equal(fiel, fiel_antes)


def test_ayto_a_missing_copy_in_force_is_a_failure_that_names_it(web, tmp_path):
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV", _csv_bytes([H_CAMBIO, R_CAMBIO_1]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1, R_CAMBIO_2]))
    assert _ejecutar(salida)[0] == 0
    next((salida / "originales" / "actividad").glob(f"*__{rid}.csv")).unlink()
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 1
    assert any(rid in e and "falta la copia vigente" in e for e in _errores(log))
    assert any(rid in e and "revisa los originales" in e for e in _errores(log))


def test_ayto_unbalanced_quotes_are_a_failure(web, tmp_path):
    # an unclosed quote turns the rest of the file into one field: 3 records, 1 row
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              b'N. DE REGISTRO DE CONTRATO;N. DE EXPEDIENTE;OBJETO DEL CONTRATO\r\n'
              b'2026/1;EXP-1;"Obra A\r\n2026/2;EXP-2;Obra B\r\n2026/3;EXP-3;Obra C\r\n')
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rid in e and "comillas mal formadas" in e for e in _errores(log))
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == rid, "OBJETO DEL CONTRATO"].tolist() == [
        "Obra A\r\n2026/2;EXP-2;Obra B\r\n2026/3;EXP-3;Obra C\r\n"]          # nothing is lost


def test_ayto_a_nul_byte_does_not_cut_the_value(tmp_path):
    # pandas cuts the field at the NUL; the csv module keeps the whole value
    ruta = tmp_path / "resoluciones_2024.csv"
    ruta.write_bytes(b"N. DE REGISTRO DE CONTRATO;N. DE EXPEDIENTE;OBJETO DEL CONTRATO;IMPORTE CEDIDO;CESIONARIO\r\n"
                     b"R1;E1;Obra\x00B;1,00;C1\r\n")
    tabla = ayto.leer_tabla(ruta, "resoluciones")
    assert tabla.df.iloc[0].tolist() == ["R1", "E1", "Obra\x00B", "1,00", "C1"]
    assert tabla.info["lectores_coinciden"] is False and tabla.info["aviso_lectura"]


def test_ayto_decodificar_keeps_utf8_with_a_stray_byte():
    # one cp1252 byte in a utf-8 file used to turn the whole file into CP850
    datos = ("N. DE EXPEDIENTE;OBJETO DEL CONTRATO\r\nEXP-1;Rehabilitación de la calle Álvarez\r\n"
             "EXP-2;Nº 5 ").encode("utf-8") + b"\xaa" + " planta\r\n".encode("utf-8")
    texto, codificacion = ayto.decodificar(datos)
    assert texto == ("N. DE EXPEDIENTE;OBJETO DEL CONTRATO\r\nEXP-1;Rehabilitación de la calle Álvarez\r\n"
                     "EXP-2;Nº 5 ª planta\r\n")
    assert codificacion == "utf-8 (1 bytes sueltos como cp1252)"
    # a cp1252 or CP850 file is still read as such
    assert ayto.decodificar("OBJETO\r\nRehabilitación Álvarez\r\n".encode("cp1252"))[1] == "cp1252"
    assert ayto.decodificar("OBJETO\r\nRehabilitación Álvarez\r\n".encode("cp850"))[1] == "cp850"


def test_ayto_every_archived_version_is_read(tmp_path):
    # archivar() names a third version with the same stamp X__S_1_2
    destino = tmp_path / "resoluciones_2026__216876-56-contratos-actividad-csv.csv"
    historico = tmp_path / "_historico"
    historico.mkdir()
    for sufijo in ("", "_1", "_1_2"):
        (historico / f"{destino.stem}__20260101T000000Z{sufijo}.csv").write_bytes(b"x")
    destino.write_bytes(b"x")
    versiones = ayto.versiones_con_fecha(destino)
    assert [(r.name, f, v) for r, f, v in versiones][:3] == [
        (f"{destino.stem}__20260101T000000Z{s}.csv", "2026-01-01T00:00:00Z", False) for s in ("", "_1", "_1_2")]
    assert len(versiones) == 4 and versiones[-1][2]


# -----------------------------------------------------------------------------
# Adversarial review (round 2): re-runs never duplicate, lose or invent records
# -----------------------------------------------------------------------------
def _filas_cesiones(n, anio, desde=1):
    return [[f"{anio}/{i}", f"EXP-{i}", f"Obra {i}", f"{i},00", f"02/01/{anio}"] for i in range(desde, desde + n)]


def _gemelos(web, anio, filas_csv, filas_xlsx=None, rc="216876-150-contratos-actividad-csv",
             rx="216876-151-contratos-actividad-xlsx", **ckan):
    """CSV and XLSX of the same group (same records unless filas_xlsx says otherwise)."""
    descripcion = f"Cesiones de contratos inscritos en el Registro de Contratos. {anio}"
    web.poner(ayto.DATASET_ACTIVIDAD, rc, descripcion, "CSV", _csv_bytes([CAB_CESIONES] + filas_csv), **ckan)
    web.poner(ayto.DATASET_ACTIVIDAD, rx, descripcion, "XLSX",
              _xlsx([CAB_CESIONES] + (filas_csv if filas_xlsx is None else filas_xlsx)), **ckan)
    return rc, rx


def test_ayto_a_withdrawn_year_does_not_duplicate_its_records_with_its_excel(web, tmp_path):
    # The portal withdraws a whole year (CSV and XLSX, e.g. to publish it again
    # under other ids): the withdrawn XLSX used to be consolidated ('its group has
    # no published CSV') and every withdrawn record appeared twice, unflagged
    _portal_con_fixtures(web)
    rc, rx = _gemelos(web, 2019, _filas_cesiones(3, 2019))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.quitar(ayto.DATASET_ACTIVIDAD, rc)
    web.quitar(ayto.DATASET_ACTIVIDAD, rx)
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert _entrada(salida, rc)["estado"] == _entrada(salida, rx)["estado"] == "retirado"
    assert not _entrada(salida, rx).get("consolidado_desde")
    fiel, uni = _leer(salida)
    assert (fiel["_recurso"] == rc).sum() == 3 and rx not in set(fiel["_recurso"])
    assert not fiel.loc[fiel["_recurso"] == rc, "_en_ultima_descarga"].any()
    comparacion = pd.read_csv(salida / "informes" / "comparacion_csv_xlsx.csv", sep=";", dtype=str)
    fila = comparacion[comparacion["xlsx"].str.contains(rx, na=False)].iloc[0]
    assert (fila["consolidado_xlsx"], fila["motivo"]) == ("False", "retirado y sus filas ya están en las tablas")


def test_ayto_a_csv_missing_from_one_listing_is_not_withdrawn(web, tmp_path):
    # A CSV missing from one listing while its XLSX twin is listed used to be
    # withdrawn at once and its twin consolidated for ever: when the CSV came
    # back, every record of the year was in force twice
    _portal_con_fixtures(web)
    rc, rx = _gemelos(web, 2019, _filas_cesiones(3, 2019))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    csv_recurso = next(r for r in web.paquetes[ayto.DATASET_ACTIVIDAD] if r["id"] == rc)
    web.quitar(ayto.DATASET_ACTIVIDAD, rc)
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and any(rx in e and "no se decide" in e for e in _errores(log))
    entrada = _entrada(salida, rc)
    assert entrada["estado"] == "publicado" and entrada["listados_sin_el"] == 1
    assert not _entrada(salida, rx).get("consolidado_desde")
    web.paquetes[ayto.DATASET_ACTIVIDAD].append(csv_recurso)          # it is back
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert "listados_sin_el" not in _entrada(salida, rc) and not _entrada(salida, rx).get("consolidado_desde")
    fiel, _ = _leer(salida)
    assert rx not in set(fiel["_recurso"]) and fiel.loc[fiel["_recurso"] == rc, "_en_ultima_descarga"].all()


def test_ayto_a_listing_without_its_csvs_withdraws_none_of_them(web, tmp_path):
    # 3 of the 7 resources of the dataset (less than half), but all its CSVs:
    # withdrawing them (two listings in a row) consolidated every twin for ever
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1]))
    fila = ["N", "EXP", "Centro", "Órgano", "Obra", "Obras", "1", "", "1,00", "1", "B1", "X SL", "No", "1,00",
            "02/01/{}", "1", "03/01/{}"]
    csvs = []
    for i, anio in enumerate((2017, 2018, 2019)):
        filas = [H_MENORES_E] + [[c.format(anio) if "{}" in c else f"{c}-{anio}-{j}" if c in ("N", "EXP") else c
                                  for c in fila] for j in range(2)]
        web.poner(ayto.DATASET_MENORES, f"300253-{40 + i}-contratos-actividad-menores-csv", f"Contratos menores {anio}",
                  "CSV", _csv_bytes(filas))
        web.poner(ayto.DATASET_MENORES, f"300253-{50 + i}-contratos-actividad-menores-xlsx",
                  f"Contratos menores {anio}", "XLSX", _xlsx(filas))
        csvs.append(f"300253-{40 + i}-contratos-actividad-menores-csv")
    web.poner(ayto.DATASET_MENORES, "300253-60-contratos-actividad-menores",
              "Contratos menores (desde 2025). Contenido y estructura del fichero", "PDF", b"%PDF-1.4 estructura")
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    retirados = [r for r in web.paquetes[ayto.DATASET_MENORES] if r["id"] in csvs]
    for rid in csvs:
        web.quitar(ayto.DATASET_MENORES, rid)
    for _ in range(2):
        codigo, log = _ejecutar(salida)
        assert codigo == 1 and "no se marca ninguno como retirado" in log
    assert all(_entrada(salida, rid)["estado"] == "publicado" for rid in csvs)
    assert not any(e.get("consolidado_desde") for e in _manifiesto(salida).values())
    web.paquetes[ayto.DATASET_MENORES].extend(retirados)
    assert _ejecutar(salida)[0] == 0
    fiel, _ = _leer(salida)
    assert set(fiel["_formato"]) == {"csv"} and fiel["_en_ultima_descarga"].all()


ESTADOS_PASAJEROS = ["descarga_fallida", "primera_copia_sin_registros", "copia_de_otro_tamano"]


@pytest.mark.parametrize("caso", ESTADOS_PASAJEROS)
def test_ayto_a_csv_that_is_not_up_to_date_does_not_consolidate_its_twin(web, tmp_path, caso):
    # The XLSX is compared with the copy of its CSV: if that copy is not what the
    # portal serves (its last download failed, it has no records yet, it is not
    # the size CKAN gives), the XLSX used to be consolidated for ever and, once
    # the CSV was up to date, every record of the group was in force twice
    _un_menor(web)
    salida = tmp_path / "salida"
    if caso == "descarga_fallida":            # E1 with 2 records; E2: the XLSX has 5 and the CSV answers 503
        rc, rx = _gemelos(web, 2026, _filas_cesiones(2, 2026))
        assert _ejecutar(salida)[0] == 0
        rc, rx = _gemelos(web, 2026, _filas_cesiones(5, 2026), last_modified="2026-09-16T08:56:16")
        url = web.url(ayto.DATASET_ACTIVIDAD, rc, "CSV")
        completo, web.ficheros[url] = web.ficheros[url], _Resp(503, b"Service Unavailable")
        argv = ()
    elif caso == "primera_copia_sin_registros":   # a new year: the CSV only has its header, the XLSX 3 records
        rc, rx = _gemelos(web, 2026, [], _filas_cesiones(3, 2026))
        url = web.url(ayto.DATASET_ACTIVIDAD, rc, "CSV")
        completo = _csv_bytes([CAB_CESIONES] + _filas_cesiones(3, 2026))
        argv = ()
    else:                                     # a copy kept from before (not the size CKAN gives)
        rc, rx = _gemelos(web, 2019, _filas_cesiones(3, 2019))
        assert _ejecutar(salida)[0] == 0
        ruta = next((salida / "originales" / "actividad").glob(f"*__{rc}.csv"))
        ruta.write_bytes(_csv_bytes([CAB_CESIONES] + _filas_cesiones(1, 2019, desde=9)))
        url, completo, argv = None, None, ("--solo-procesar",)
    codigo, log = _ejecutar(salida, *argv)
    assert codigo == 1 and any(rx in e and "no se decide" in e for e in _errores(log)), log
    assert not _entrada(salida, rx).get("consolidado_desde")
    fiel, _ = _leer(salida)
    assert rx not in set(fiel["_recurso"])

    if url:                                   # the CSV is up to date again
        web.poner(ayto.DATASET_ACTIVIDAD, rc, next(r for r in web.paquetes[ayto.DATASET_ACTIVIDAD]
                                                   if r["id"] == rc)["description"], "CSV", completo,
                  last_modified="2026-09-20T00:00:00")
    else:
        ruta.write_bytes(web.ficheros[web.url(ayto.DATASET_ACTIVIDAD, rc, "CSV")])
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert not _entrada(salida, rx).get("consolidado_desde")
    fiel, _ = _leer(salida)
    assert rx not in set(fiel["_recurso"])                   # each record once, from its CSV
    vigentes = fiel.loc[(fiel["_recurso"] == rc) & fiel["_en_ultima_descarga"], "N. DE EXPEDIENTE"]
    assert vigentes.tolist() == [f"EXP-{i}" for i in range(1, 6 if caso == "descarga_fallida" else 4)]


def test_ayto_a_description_change_with_the_same_bytes_keeps_the_file(web, tmp_path):
    # The portal adds text after the year to every description ('Contratos menores
    # 2023 - Ayuntamiento de Madrid'): the 'parte' changes, and each file used to be
    # downloaded again under another name while the previous one was withdrawn,
    # so every record appeared as withdrawn (and its XLSX twin consolidated)
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    fiel_antes, _ = _leer(salida)
    archivos = sorted(p.name for p in (salida / "originales" / "menores").iterdir())
    for recurso in web.paquetes[ayto.DATASET_MENORES]:
        if "Contenido y estructura" not in recurso["description"]:
            recurso["description"] += " - Ayuntamiento de Madrid"
            recurso["metadata_modified"] = "2026-09-26T00:00:00"
    codigo, log = _ejecutar(salida)
    assert codigo == 0, log
    assert "es el mismo fichero; se conserva su clave" in log and "queda como retirado" not in log
    assert sorted(p.name for p in (salida / "originales" / "menores").iterdir()) == archivos
    menores = [e for e in _manifiesto(salida).values() if e["dataset"] == ayto.DATASET_MENORES
               and e["categoria"] != "documentacion"]
    assert all(e["estado"] == "publicado" and e["descripcion"].endswith("- Ayuntamiento de Madrid")
               and e["clasificacion_portal"][2].endswith("ayuntamiento_de_madrid") for e in menores)
    fiel, _ = _leer(salida)
    columnas = [c for c in fiel.columns if c != "_descripcion"]
    pd.testing.assert_frame_equal(fiel[columnas], fiel_antes[columnas])
    # the next listing finds the same files: closed years are not requested again
    web.llamadas.clear()
    assert _ejecutar(salida)[0] == 0 and web.pedidos(_rid("menores_2016")) == 0


def test_ayto_a_described_again_file_is_not_withdrawn_before_it_can_be_compared(web, tmp_path):
    # the first download with the new description fails: the previous key is not
    # withdrawn (it may be the same file); the next run finds the same bytes and
    # keeps a single key
    _un_menor(web)
    rid = "216876-60-contratos-actividad-csv"
    contenido = _csv_bytes([H_CAMBIO, ["2019/1", "EXP-1", "Obra", "B1", "1,00"]])
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1]))
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2019", "CSV", contenido)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2019 (datos definitivos)", "CSV", contenido)
    url = web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")
    web.ficheros[url] = _Resp(503, b"Service Unavailable")
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "no se retira hasta saber si es el mismo fichero" in log
    assert all(e["estado"] == "publicado" for e in _manifiesto(salida).values() if e["id"] == rid)
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == rid, "_en_ultima_descarga"].tolist() == [True]
    web.ficheros[url] = contenido
    assert _ejecutar(salida)[0] == 0
    entradas = [e for e in _manifiesto(salida).values() if e["id"] == rid]
    assert [(e["archivo"].rsplit("/", 1)[-1], e["estado"], e["descripcion"]) for e in entradas] == [
        (f"resoluciones_2019__{rid}.csv", "publicado", "Resoluciones contratos. 2019 (datos definitivos)")]
    assert [p.name for p in (salida / "originales" / "actividad").glob(f"*{rid}*")] == [f"resoluciones_2019__{rid}.csv"]


def test_ayto_ids_reused_for_most_files_withdraw_nothing(web, tmp_path):
    # 3 of the 4 files of a dataset described otherwise with other contents (ids
    # reused?): more than half at once is more likely a portal failure
    _un_menor(web)
    ids = [f"216876-{i}-contratos-actividad-csv" for i in (60, 61, 62, 63)]
    for i, rid in enumerate(ids):
        web.poner(ayto.DATASET_ACTIVIDAD, rid, f"Resoluciones contratos. {2016 + i}", "CSV",
                  _csv_bytes([H_CAMBIO, [f"{2016 + i}/1", f"EXP-{i}", "Obra", "B1", "1,00"]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    for i, rid in enumerate(ids[:3]):
        web.poner(ayto.DATASET_ACTIVIDAD, rid, f"Penalidades en contratos inscritas en el Registro de Contratos. "
                  f"{2016 + i}", "CSV", _csv_bytes([H_CAMBIO, [f"{2016 + i}/9", f"EXP-P{i}", "Obra", "B2", "2,00"]]))
    codigo, log = _ejecutar(salida)
    assert codigo == 1 and "no se marca ninguno como retirado" in log
    estados = {(e["id"], e["categoria"]): e["estado"] for e in _manifiesto(salida).values() if e["id"] in ids}
    assert all(v == "publicado" for v in estados.values()) and len(estados) == 7


def test_ayto_a_withdrawn_resource_never_downloaded_is_a_warning(web, tmp_path):
    _un_menor(web)
    rid = "216876-57-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, "216876-56-contratos-actividad-csv", "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1]))
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Cesiones de contratos. 2026", "CSV", b"x")
    web.ficheros[web.url(ayto.DATASET_ACTIVIDAD, rid, "CSV")] = _Resp(404, PAGINA_ERROR)
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 1
    web.quitar(ayto.DATASET_ACTIVIDAD, rid)
    for _ in range(2):                          # it used to be a failure in every run
        codigo, log = _ejecutar(salida)
        assert codigo == 0 and "se llegara a descargar ninguna copia" in log, log


def test_ayto_a_corrupt_manifest_is_recovered_from_its_history(web, tmp_path, monkeypatch):
    _portal_con_fixtures(web)
    salida = tmp_path / "salida"
    # ahora_iso() has second resolution: two runs within the same second would
    # write the same manifest and leave no previous one in _historico/
    ahora = ["2026-09-01T10:00:00Z"]
    monkeypatch.setattr(ayto, "ahora_iso", lambda: ahora[0])
    assert _ejecutar(salida)[0] == 0
    ahora[0] = "2026-09-01T10:00:01Z"
    assert _ejecutar(salida)[0] == 0                             # a previous manifest in _historico/
    assert list((salida / "originales" / "_historico").glob("_manifiesto__*.json"))
    ahora[0] = "2026-09-01T10:00:02Z"
    fiel_antes, uni_antes = _leer(salida)
    ruta = salida / "originales" / "_manifiesto.json"
    corrupto = ruta.read_bytes()[:500]
    ruta.write_bytes(corrupto)                  # e.g. cut by a full disk
    codigo, log = _ejecutar(salida, "--solo-procesar")
    assert codigo == 0 and "no se puede leer" in log and "se recupera" in log, log
    fiel, uni = _leer(salida)
    pd.testing.assert_frame_equal(fiel, fiel_antes)
    pd.testing.assert_frame_equal(uni, uni_antes)
    assert _ejecutar(salida)[0] == 0
    assert json.loads(ruta.read_text(encoding="utf-8"))           # a valid manifest again
    assert corrupto in [p.read_bytes() for p in (ruta.parent / "_historico").glob("_manifiesto__*.json")]


def test_ayto_each_row_is_mapped_with_the_header_of_its_version(web, tmp_path):
    # the rows of a previous version (withdrawn by a header change) keep the values
    # of their own header in the unified table, not those of the last version
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    h1 = ["N. DE REGISTRO DE CONTRATO", "N. DE EXPEDIENTE", "OBJETO DEL CONTRATO", "IMPORTE ADJUDICACION IVA INC."]
    h2 = h1[:3] + ["IMPORTE DE ADJUDICACION (IVA INCLUIDO)"]
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([h1, ["2026/1", "EXP-1", "Obra 1", "100,00"], ["2026/2", "EXP-2", "Obra 2", "200,00"]]))
    salida = tmp_path / "salida"
    assert _ejecutar(salida)[0] == 0
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([h2, ["2026/1", "EXP-1", "Obra 1", "150,00"], ["2026/2", "EXP-2", "Obra 2", "200,00"]]))
    assert _ejecutar(salida)[0] == 0
    _, uni = _leer(salida)
    u = uni[uni["_recurso"] == rid]
    valores = sorted(zip(u["_en_ultima_descarga"], u["n_expediente"], u["importe_adjudicacion_iva_inc"]))
    assert valores == [(False, "EXP-1", 100.0), (False, "EXP-2", 200.0), (True, "EXP-1", 150.0),
                       (True, "EXP-2", 200.0)]


def test_ayto_values_under_unnamed_columns_are_reported(web, tmp_path):
    # records with more fields than the header has names: the extra values are in
    # the faithful table (as 'Unnamed: N') but no mapping takes them to the unified one
    _un_menor(web)
    rid = "216876-56-contratos-actividad-csv"
    web.poner(ayto.DATASET_ACTIVIDAD, rid, "Resoluciones contratos. 2026", "CSV",
              _csv_bytes([H_CAMBIO, R_CAMBIO_1 + ["sobra 1"], R_CAMBIO_2 + ["sobra 2"]]))
    salida = tmp_path / "salida"
    codigo, log = _ejecutar(salida)
    assert codigo == 0 and "2 valores en columnas sin nombre en la cabecera (Unnamed: 5)" in log
    fiel, _ = _leer(salida)
    assert fiel.loc[fiel["_recurso"] == rid, "Unnamed: 5"].tolist() == ["sobra 1", "sobra 2"]
    lectura = pd.read_csv(salida / "informes" / "lectura_ficheros.csv", sep=";", dtype=str)
    fila = lectura[lectura["archivo"].str.contains(rid)].iloc[0]
    assert fila["columnas_sin_nombre_con_valores"] == "Unnamed: 5: 2"


# =============================================================================
# COMUNIDAD DE MADRID — fake portal
# =============================================================================
COLUMNAS_CAM = [
    "Tipo de Publicación", "Estado", "Entidad Adjudicadora", "Nº Expediente",
    "Referencia", "Título del contrato", "Tipo de contrato",
    "Procedimiento de adjudicación", "Presupuesto de licitación", "Nº de ofertas",
    "Resultado", "NIF del adjudicatario", "Adjudicatario", "Fecha del contrato",
    "Importe de adjudicación", "Importe de las modificaciones",
    "Importe de las prórrogas", "Importe de la liquidación",
]
# option value → (texto del desplegable, valor en la columna "Entidad Adjudicadora")
ENTIDADES_CAM = {
    "38": ("Hospital General Universitario Gregorio Marañón",
           "Hospital General Universitario Gregorio Marañón"),
    "5": ("Consejería de Sanidad", "Consejería de Sanidad"),
    "120": ("- Canal de Isabel II", "Canal de Isabel II"),
}
CLAVE_ANTIBOT = "k3y-ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklm"   # 43 chars, like Drupal's


def _clave_transformada(clave):
    # README: "invirtiendo pares de 2 caracteres desde el final"
    return "".join(clave[max(i - 2, 0):i] for i in range(len(clave), 0, -2))


def _registro(tipo, entidad, ref, presupuesto, publicado="", expediente=None,
              adjudicatario="EMPRESA SL", titulo=None):
    importe = f"{presupuesto:.2f}".replace(".", ",")
    return {
        "Tipo de Publicación": tipo, "Estado": "Adjudicado",
        "Entidad Adjudicadora": ENTIDADES_CAM[entidad][1],
        "Nº Expediente": f"EXP-{ref}" if expediente is None else expediente,
        "Referencia": ref, "Título del contrato": titulo or f"Contrato {ref}",
        "Tipo de contrato": "Suministros", "Procedimiento de adjudicación": "Menor",
        "Presupuesto de licitación": importe, "Nº de ofertas": "1",
        "Resultado": "Adjudicado", "NIF del adjudicatario": "B00000000",
        "Adjudicatario": adjudicatario, "Fecha del contrato": "01/02/2024",
        "Importe de adjudicación": importe, "Importe de las modificaciones": "",
        "Importe de las prórrogas": "", "Importe de la liquidación": "",
        "_presupuesto": presupuesto, "_publicado": publicado,
    }


def _registros_cam():
    menor = "Contratos menores"
    regs = []
    # Entity 38 exceeds the (scaled-down) 6-row cap: needs the amount split,
    # including a second-level split of 0-10 and 5-10, and boundary values
    # (5, 7, 10, 20, 50000) that fall in two adjacent ranges.
    # Menores above 50,000 € and negative ones exist too (0.04% of the menores
    # of the entities that were not split in the published data): only the
    # open ranges "≤0" and "≥50000" reach them.
    for i, p in enumerate([1, 2, 3, 4.5, 5, 6, 7, 8, 9, 10, 10, 15, 20, 25, 49.99,
                           120, 999.5, 14999.99, 50000, 59894.62, -186.3]):
        regs.append(_registro(menor, "38", f"38-{i:02d}", p))
    regs += [
        _registro(menor, "5", "5-01", 300),
        _registro(menor, "5", "5-02", 301, titulo="Material; oficina \"urgente\""),
        _registro(menor, "5", "5-03", 302, titulo="Título con\nsalto de línea"),
        # two different contracts without expediente nor referencia
        _registro(menor, "120", "", 40, expediente="", titulo="Reparación bomba"),
        _registro(menor, "120", "", 41, expediente="", titulo="Revisión contadores"),
    ]
    conv, sin_pub = cam.TIPOS_NO_MENORES[0], cam.TIPOS_NO_MENORES[1]
    regs += [
        # two lots of the same expediente: same Nº Expediente/Referencia/Entidad
        _registro(conv, "5", "L-1", 100000, "2024-03-15", expediente="EXP-LOTES",
                  adjudicatario="LOTE UNO SL"),
        _registro(conv, "5", "L-1", 50000, "2024-03-15", expediente="EXP-LOTES",
                  adjudicatario="LOTE DOS SL"),
        _registro(conv, "38", "C-2", 70000, "2024-01-01"),
        _registro(conv, "38", "C-3", 80000, "2023-12-31"),   # outside 2024
        _registro(sin_pub, "120", "S-1", 20000, "2024-11-30"),
        _registro(cam.TIPOS_NO_MENORES[4], "5", "Q-1", 0, "2024-02-29"),
        # published before 2017 (the portal has notices from 2014 on)
        _registro(conv, "5", "V-1", 90000, "2014-01-20"),
        _registro(sin_pub, "38", "V-2", 30000, "2016-12-31"),
    ]
    return regs


def _fecha(txt):
    return datetime.strptime(txt, "%d-%m-%Y").date()


class FakeResponse:
    def __init__(self, status=200, text="", content=None, headers=None):
        self.status_code = status
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
        self.headers = headers or {"Content-Type": "text/html; charset=UTF-8"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Server Error")


class _Cookies:
    def clear(self):
        pass


class FakeSession:
    def __init__(self, portal):
        self.portal = portal
        self.headers = {}
        self.cookies = _Cookies()

    def get(self, url, params=None, timeout=None):
        return self.portal.get(url, params)

    def post(self, url, data=None, timeout=None):
        return self.portal.post(url, data or {})


class FakePortalCAM:
    """Minimal model of contratos-publicos.comunidad.madrid (see script docstring)."""

    COMPLETION = "/buscador-contratos/csv/completion"

    def __init__(self, registros, tope, entidades=None):
        self.registros = registros
        self.tope = tope
        # option value → (dropdown text, "Entidad Adjudicadora"); the portal
        # renumbers its options when entities are added or removed
        self.entidades = dict(ENTIDADES_CAM if entidades is None else entidades)
        self.busqueda = None
        self.busquedas = []
        self.fallar = lambda params: False
        self.vaciar = lambda params: False     # answer with the header only
        self.cortar_en = None                  # the process dies before serving the Nth CSV
        self.servidos = 0
        self.servidas = []                     # searches whose CSV was served
        self.n_captcha = 0
        self.respuesta = None

    def session(self):
        return FakeSession(self)

    # --- GET ---------------------------------------------------------------
    def get(self, url, params):
        if url == cam.BUSCAR_URL and not params:
            return FakeResponse(text=self._portada())
        if url == cam.BUSCAR_URL:
            if params.get("antibot_key") != _clave_transformada(CLAVE_ANTIBOT):
                return FakeResponse(status=403, text="antibot")
            self.busqueda = dict(params)
            self.busquedas.append(dict(params))
            return FakeResponse(text="<html><body>Resultados</body></html>")
        if url == cam.CSV_URL:
            self.n_captcha += 1
            a, b = 7 + self.n_captcha, 3
            op = "+" if self.n_captcha % 2 else "-"
            self.respuesta = a + b if op == "+" else a - b
            return FakeResponse(text=(
                '<html><body><form id="pcon-contratos-menores-export-results-form" '
                'action="/buscador-contratos/csv" method="post">'
                '<div class="description">Solve this simple math problem and enter the '
                'result. E.g. for 1+3, enter 4.</div>'
                f'<span class="field-prefix">{a} {op} {b} =</span>'
                '<input type="text" name="captcha_response" value="">'
                f'<input type="hidden" name="captcha_sid" value="{self.n_captcha}">'
                '<input type="hidden" name="form_build_id" value="form-abc">'
                '<input type="hidden" name="form_id" '
                'value="pcon_contratos_menores_export_results_form">'
                '<input type="submit" name="op" value="Exportar CSV">'
                '</form></body></html>'))
        return FakeResponse(status=404)

    def _portada(self):
        opciones = '<option value="All">- Cualquiera -</option>' + "".join(
            f'<option value="{v}">{texto}</option>' for v, (texto, _) in self.entidades.items())
        ajustes = ('{"path": {"baseUrl": "/"}, "antibot": {"forms": {"views-exposed-form-'
                   'buscador-contratos-page-1": {"id": "views-exposed-form", "key": "'
                   + CLAVE_ANTIBOT + '"}}}}')
        return ('<html><head><script type="application/json" '
                f'data-drupal-selector="drupal-settings-json">{ajustes}</script></head>'
                '<body><form><select name="entidad_adjudicadora">'
                f'{opciones}</select></form></body></html>')

    # --- POST --------------------------------------------------------------
    def post(self, url, data):
        if url == cam.CSV_URL:
            ok = (data.get("captcha_response") == str(self.respuesta)
                  and data.get("captcha_sid") == str(self.n_captcha)
                  and data.get("form_build_id") == "form-abc")
            if not ok:
                return self.get(cam.CSV_URL, None)   # CAPTCHA rechazado: mismo form
            return FakeResponse(text=(
                '<html><body><form id="pcon-contratos-menores-export-results-completion-'
                f'form" action="{self.COMPLETION}" method="post">'
                '<input type="hidden" name="form_build_id" value="form-def">'
                '<input type="submit" name="op" value="Descargar"></form></body></html>'))
        if url == cam.BASE_URL + self.COMPLETION:
            if data.get("form_build_id") != "form-def" or self.busqueda is None:
                return FakeResponse(status=400)
            if self.fallar(self.busqueda):
                return FakeResponse(status=500)
            if self.cortar_en is not None and self.servidos >= self.cortar_en:
                raise _Corte()
            self.servidos += 1
            self.servidas.append(dict(self.busqueda))
            filas = [] if self.vaciar(self.busqueda) else self._filtrar(self.busqueda)
            return FakeResponse(content=self._csv(filas), headers={
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": 'attachment; filename="contratos.csv"'})
        return FakeResponse(status=404)

    def _filtrar(self, p):
        out = []
        for r in self.registros:
            faceta = p.get("f[0]", "")
            if faceta and faceta.split(":", 1)[1].lower() != r["Tipo de Publicación"].lower():
                continue
            ent = p.get("entidad_adjudicadora", "All")
            if ent != "All" and self.entidades[ent][1] != r["Entidad Adjudicadora"]:
                continue
            desde, hasta = (p.get("presupuesto_base_licitacion_total"),
                            p.get("presupuesto_base_licitacion_total_1"))
            if desde and r["_presupuesto"] < float(desde):
                continue
            if hasta and r["_presupuesto"] > float(hasta):
                continue
            if p.get("createddate") or p.get("createddate_1"):
                pub = date.fromisoformat(r["_publicado"]) if r["_publicado"] else None
                if pub is None:
                    continue
                if p.get("createddate") and pub < _fecha(p["createddate"]):
                    continue
                if p.get("createddate_1") and pub > _fecha(p["createddate_1"]):
                    continue
            out.append(r)
        return out[:self.tope]

    @staticmethod
    def _csv(filas):
        """Export of `filas`; a record's "_continuaciones" (more lots,
        awardees, extensions) go right after it, as the portal does."""
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";", lineterminator="\n")
        w.writerow(COLUMNAS_CAM)
        for r in filas:
            w.writerow([r[c] for c in COLUMNAS_CAM])
            w.writerows([[c[k] for k in COLUMNAS_CAM] for c in r.get("_continuaciones", ())])
        return ("﻿" + buf.getvalue()).encode("utf-8")


def _esperado(registros):
    return sorted(tuple(r[c] for c in COLUMNAS_CAM) for r in registros)


META_CAM = ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]


def _leer_unificado(salida, solo_presentes=True):
    """Rows of the consolidated CSV. With a single download every row is in
    the last download and has both download dates."""
    df = pd.read_csv(salida, sep=";", encoding="utf-8-sig", dtype=str,
                     keep_default_na=False)
    assert list(df.columns) == COLUMNAS_CAM + ["_archivo_fuente"] + META_CAM
    if solo_presentes:
        assert set(df["_en_ultima_descarga"]) <= {"True"}
        assert (df["_primera_descarga"] != "").all() and (df["_ultima_descarga"] != "").all()
    return sorted(tuple(f) for f in df[COLUMNAS_CAM].itertuples(index=False))


def _csvs(carpeta):
    """Names of the CSV files in a folder (not the manifest nor _historico/)."""
    return sorted(p.name for p in Path(carpeta).glob("*.csv"))


def _menores(registros):
    return [r for r in registros if r["Tipo de Publicación"] == "Contratos menores"]


@pytest.fixture
def cam_dirs(tmp_path):
    csv_dir = tmp_path / "csv_originales"
    csv_dir.mkdir()
    with patch.object(cam, "OUTPUT_DIR", tmp_path), patch.object(cam, "CSV_DIR", csv_dir), \
            patch.object(cam.time, "sleep"):
        yield tmp_path


@pytest.fixture
def portal():
    p = FakePortalCAM(_registros_cam(), tope=6)
    with patch.object(cam.requests, "Session", p.session), \
            patch.object(cam, "UMBRAL_TRUNCADO", 6):
        yield p


def test_cam_antibot_key_and_captcha_helpers():
    assert cam.transformar_antibot_key("abcdef") == "efcdab"
    assert cam.transformar_antibot_key("abcde") == "debca"
    assert cam.transformar_antibot_key(CLAVE_ANTIBOT) == _clave_transformada(CLAVE_ANTIBOT)
    assert cam.resolver_captcha("E.g. for 1+3, enter 4. <span>3 + 8 =</span>") == 11
    assert cam.resolver_captcha("12 - 5 =") == 7
    assert cam.resolver_captcha("sin captcha") is None


def test_cam_full_http_flow_returns_filtered_csv(cam_dirs, portal):
    d = cam.DescargadorComunidadMadrid()
    datos = d._descargar_csv(tipo_pub="Contratos Menores", entidad="5")

    assert d.entidades == [(v, t) for v, (t, _) in ENTIDADES_CAM.items()]
    params = portal.busquedas[-1]
    assert params["antibot_key"] == _clave_transformada(CLAVE_ANTIBOT)
    assert params["f[0]"] == "tipo_publicacion:Contratos Menores"
    assert params["entidad_adjudicadora"] == "5"
    assert params["createddate"] == params["createddate_1"] == ""
    df = pd.read_csv(io.BytesIO(datos), sep=";", encoding="utf-8-sig", dtype=str)
    assert df["Referencia"].tolist() == ["5-01", "5-02", "5-03"]
    assert df["Título del contrato"].iloc[2] == "Título con\nsalto de línea"


def test_cam_menores_split_by_amount_covers_every_record_once(cam_dirs, portal):
    d = cam.DescargadorComunidadMadrid()
    d.descargar_menores()
    cam.unificar_csvs()

    ficheros = _csvs(cam_dirs / "csv_originales")
    # the truncated whole-entity file was replaced by amount ranges
    assert cam.nombre_csv_entidad(38, ENTIDADES_CAM["38"][0]) not in ficheros
    rango = cam.nombre_csv_entidad_rango
    nombre38 = ENTIDADES_CAM["38"][0]
    assert rango(38, nombre38, "0", "5") in ficheros          # 0-10 re-split
    assert rango(38, nombre38, "7", "10") in ficheros         # 5-10 re-split again
    assert rango(38, nombre38, "0", "10") not in ficheros
    assert cam.nombre_csv_entidad(5, "Consejería de Sanidad") in ficheros
    assert d.stats["error"] == 0

    salida = cam_dirs / "contratacion_comunidad_madrid_completo.csv"
    assert _leer_unificado(salida) == _esperado(_menores(portal.registros))

    rangos = {(b["presupuesto_base_licitacion_total"], b["presupuesto_base_licitacion_total_1"])
              for b in portal.busquedas if b["entidad_adjudicadora"] == "38"}
    assert set(cam.RANGOS_IMPORTE) <= rangos
    assert {b["f[0]"] for b in portal.busquedas} == {"tipo_publicacion:Contratos Menores"}


def test_cam_menores_resumes_an_interrupted_amount_split(cam_dirs, portal):
    # A previous run was interrupted after saving only the first amount range.
    nombre38 = ENTIDADES_CAM["38"][0]
    previo = [r for r in portal.registros
              if r["Entidad Adjudicadora"] == nombre38 and r["_presupuesto"] <= 5]
    (cam_dirs / "csv_originales" / cam.nombre_csv_entidad_rango(38, nombre38, "0", "5")) \
        .write_bytes(FakePortalCAM._csv(previo))

    cam.DescargadorComunidadMadrid().descargar_menores()
    cam.unificar_csvs()

    salida = cam_dirs / "contratacion_comunidad_madrid_completo.csv"
    assert _leer_unificado(salida) == _esperado(_menores(portal.registros))


def test_cam_menores_retries_a_failed_range_on_the_next_run(cam_dirs, portal):
    portal.fallar = lambda p: p.get("presupuesto_base_licitacion_total") == "20"
    primera = cam.DescargadorComunidadMadrid()
    primera.descargar_menores()
    assert primera.stats["error"] == 1

    portal.fallar = lambda p: False
    cam.DescargadorComunidadMadrid().descargar_menores()
    cam.unificar_csvs()

    salida = cam_dirs / "contratacion_comunidad_madrid_completo.csv"
    assert _leer_unificado(salida) == _esperado(_menores(portal.registros))


def test_cam_otros_by_month_and_publication_type(cam_dirs, portal):
    d = cam.DescargadorComunidadMadrid()
    d.descargar_otros(2024, 2024)
    cam.unificar_csvs()

    assert len(portal.busquedas) == 12 * len(cam.TIPOS_NO_MENORES)
    marzo = [b for b in portal.busquedas if b["createddate"] == "01-03-2024"]
    assert {b["createddate_1"] for b in marzo} == {"31-03-2024"}
    assert {b["f[0]"] for b in marzo} == {f"tipo_publicacion:{t}" for t in cam.TIPOS_NO_MENORES}
    assert {b["createddate_1"] for b in portal.busquedas if b["createddate"] == "01-02-2024"} \
        == {"29-02-2024"}

    ficheros = _csvs(cam_dirs / "csv_originales")
    assert ficheros == sorted([
        cam.nombre_csv_mes(2024, 1, cam.TIPOS_NO_MENORES[0]),
        cam.nombre_csv_mes(2024, 2, cam.TIPOS_NO_MENORES[4]),
        cam.nombre_csv_mes(2024, 3, cam.TIPOS_NO_MENORES[0]),
        cam.nombre_csv_mes(2024, 11, cam.TIPOS_NO_MENORES[1]),
    ])
    esperado = [r for r in portal.registros
                if r["Tipo de Publicación"] != "Contratos menores"
                and r["_publicado"].startswith("2024")]
    salida = cam_dirs / "contratacion_comunidad_madrid_completo.csv"
    # both lots of EXP-LOTES are kept (same expediente/referencia/entidad)
    assert _leer_unificado(salida) == _esperado(esperado)


def test_cam_unificar_removes_exact_duplicates_only(cam_dirs):
    base = _registro("Contratos menores", "120", "", 40, expediente="", titulo="Reparación")
    otro = dict(base, **{"Título del contrato": "Revisión", "Importe de adjudicación": "41,00"})
    lote1 = _registro(cam.TIPOS_NO_MENORES[0], "5", "L-1", 1000, expediente="EXP-L",
                      adjudicatario="UNO SL")
    lote2 = dict(lote1, **{"Adjudicatario": "DOS SL", "NIF del adjudicatario": "B2"})
    csv_dir = cam_dirs / "csv_originales"
    # the same record downloaded in two adjacent amount ranges
    (csv_dir / "menores_ent120_x_imp30-50.csv").write_bytes(FakePortalCAM._csv([base, otro]))
    (csv_dir / "menores_ent120_x_imp50-75.csv").write_bytes(FakePortalCAM._csv([base]))
    (csv_dir / "2024_03_convocatoria_anunciada_a_l.csv").write_bytes(
        FakePortalCAM._csv([lote1, lote2]))

    cam.unificar_csvs()
    salida = cam_dirs / "contratacion_comunidad_madrid_completo.csv"
    assert _leer_unificado(salida) == _esperado([base, otro, lote1, lote2])


def _continuacion(adjudicatario="", nif="", importe="0,00", prorroga="0,00"):
    """Row the portal adds after a record for another lot/awardee/extension:
    only the last columns are filled (seen in real exports of the portal)."""
    fila = dict.fromkeys(COLUMNAS_CAM, "")
    fila.update({"NIF del adjudicatario": nif, "Adjudicatario": adjudicatario,
                 "Importe de adjudicación": importe, "Importe de las modificaciones": "0,00",
                 "Importe de las prórrogas": prorroga, "Importe de la liquidación": "0,00"})
    return fila


def test_cam_unificar_keeps_continuation_rows_and_source_duplicates(cam_dirs):
    conv = cam.TIPOS_NO_MENORES[0]
    a = _registro(conv, "5", "A-1", 1000, adjudicatario="NA")    # literal "NA"
    b = _registro(conv, "5", "B-1", 2000)
    c = _registro(conv, "38", "C-1", 3000)
    prorroga = _continuacion(prorroga="1.000,00")
    lote = _continuacion("LOTE DOS SL", "B2", "500,00")
    mes = [a, prorroga, prorroga, lote, b, prorroga, c, c]   # c: served twice
    m1 = _registro("Contratos menores", "120", "M-1", 50)
    m2 = _registro("Contratos menores", "120", "M-2", 60)
    csv_dir = cam_dirs / "csv_originales"
    (csv_dir / "2024_03_convocatoria_anunciada_a_l.csv").write_bytes(FakePortalCAM._csv(mes))
    # 50 € sits on the border of two amount ranges: downloaded twice
    (csv_dir / "menores_ent120_x_imp30-50.csv").write_bytes(FakePortalCAM._csv([m1]))
    (csv_dir / "menores_ent120_x_imp50-75.csv").write_bytes(FakePortalCAM._csv([m1, m2]))

    cam.unificar_csvs()
    df = pd.read_csv(cam_dirs / "contratacion_comunidad_madrid_completo.csv", sep=";",
                     encoding="utf-8-sig", dtype=str, keep_default_na=False)
    filas = [tuple(r[c] for c in COLUMNAS_CAM) for r in [*mes, m1, m2]]
    # same rows, same order: each continuation row still follows its record
    assert [tuple(f) for f in df[COLUMNAS_CAM].itertuples(index=False)] == filas
    assert df.loc[0, "Adjudicatario"] == "NA"


def test_cam_unificar_drops_a_whole_record_repeated_in_another_csv(cam_dirs):
    conv = cam.TIPOS_NO_MENORES[0]
    a = _registro(conv, "5", "A-1", 1000)
    b = _registro(conv, "5", "B-1", 2000)
    prorroga = _continuacion(prorroga="1.000,00")
    csv_dir = cam_dirs / "csv_originales"
    (csv_dir / "2024_03_convocatoria_anunciada_a_l.csv").write_bytes(
        FakePortalCAM._csv([a, prorroga, b]))
    # the same record + its continuation again in another CSV (overlapping
    # searches), and B with a different continuation: B is not a repeat
    (csv_dir / "hasta_2016_convocatoria_anunciada_a_l.csv").write_bytes(
        FakePortalCAM._csv([a, prorroga, b, _continuacion("OTRA SL", "B9", "5,00")]))

    cam.unificar_csvs()
    df = pd.read_csv(cam_dirs / "contratacion_comunidad_madrid_completo.csv", sep=";",
                     encoding="utf-8-sig", dtype=str, keep_default_na=False)
    assert df["_archivo_fuente"].tolist() == ["2024_03_convocatoria_anunciada_a_l.csv"] * 3 + [
        "hasta_2016_convocatoria_anunciada_a_l.csv"] * 2
    assert df["Referencia"].tolist() == ["A-1", "", "B-1", "B-1", ""]


def test_cam_otros_includes_notices_published_before_2017(cam_dirs, portal):
    cam.DescargadorComunidadMadrid().descargar_otros(2017, 2017)
    cam.unificar_csvs()

    csv_dir = cam_dirs / "csv_originales"
    conv, sin_pub = cam.TIPOS_NO_MENORES[0], cam.TIPOS_NO_MENORES[1]
    assert _csvs(csv_dir) == sorted([
        cam.nombre_csv_hasta(2016, conv), cam.nombre_csv_hasta(2016, sin_pub)])
    previas = [b for b in portal.busquedas if b["createddate"] == ""]
    assert {b["createddate_1"] for b in previas} == {"31-12-2016"}
    esperado = [r for r in portal.registros if r["_publicado"][:4] in ("2014", "2016")]
    assert len(esperado) == 2
    assert _leer_unificado(cam_dirs / "contratacion_comunidad_madrid_completo.csv") == \
        _esperado(esperado)


def _envejecer(ruta, horas):
    t = datetime.now().timestamp() - horas * 3600
    os.utime(ruta, (t, t))


def test_cam_stale_csvs_are_downloaded_again_recent_ones_are_kept(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    # entity 5 downloaded two days ago, when it had a single menor
    viejo = csv_dir / cam.nombre_csv_entidad(5, ENTIDADES_CAM["5"][0])
    viejo.write_bytes(FakePortalCAM._csv([portal.registros[21]]))
    _envejecer(viejo, 48)
    # entity 120 downloaded an hour ago (interrupted run): not requested again
    reciente = csv_dir / cam.nombre_csv_entidad(120, ENTIDADES_CAM["120"][0])
    contenido = FakePortalCAM._csv([r for r in portal.registros
                                    if r["Entidad Adjudicadora"] == "Canal de Isabel II"
                                    and r["Tipo de Publicación"] == "Contratos menores"])
    reciente.write_bytes(contenido)
    _envejecer(reciente, 1)

    cam.DescargadorComunidadMadrid().descargar_menores()

    df = pd.read_csv(viejo, sep=";", encoding="utf-8-sig", dtype=str)
    assert df["Referencia"].tolist() == ["5-01", "5-02", "5-03"]
    assert reciente.read_bytes() == contenido
    assert "120" not in {b["entidad_adjudicadora"] for b in portal.busquedas}


def test_cam_failed_refresh_keeps_the_previous_csv(cam_dirs, portal):
    viejo = cam_dirs / "csv_originales" / cam.nombre_csv_entidad(5, ENTIDADES_CAM["5"][0])
    anterior = FakePortalCAM._csv([portal.registros[21]])
    viejo.write_bytes(anterior)
    _envejecer(viejo, 48)
    portal.fallar = lambda p: p.get("entidad_adjudicadora") == "5"

    d = cam.DescargadorComunidadMadrid()
    d.descargar_menores()

    assert d.stats["error"] == 1
    assert viejo.read_bytes() == anterior
    assert not list((cam_dirs / "csv_originales").glob("*.part"))


def test_cam_output_dir_is_the_script_folder_not_the_cwd():
    assert cam.OUTPUT_DIR == CAM_PATH.parent
    assert cam.CSV_DIR == CAM_PATH.parent / "csv_originales"


def test_cam_default_end_year_is_the_current_year():
    import inspect
    for metodo in (cam.DescargadorComunidadMadrid.descargar_otros,
                   cam.DescargadorComunidadMadrid.descargar_todo):
        assert inspect.signature(metodo).parameters["anio_fin"].default == datetime.now().year


@contextmanager
def _cli(tmp_path, portal, argv):
    """Run the CAM script as ``python <copia> <argv>`` from an unrelated cwd."""
    carpeta = tmp_path / "script"
    carpeta.mkdir(exist_ok=True)
    copia = carpeta / CAM_PATH.name
    shutil.copy(CAM_PATH, copia)
    otro_cwd = tmp_path / "otro_cwd"
    otro_cwd.mkdir(exist_ok=True)
    with _cwd(otro_cwd), patch.object(sys, "argv", [str(copia)] + argv), \
            patch("requests.Session", portal.session), patch("time.sleep"), \
            patch("logging.basicConfig"), patch("logging.FileHandler"):
        runpy.run_path(str(copia), run_name="__main__")
        yield carpeta
    assert list(otro_cwd.iterdir()) == []   # nothing written to the cwd


def test_cam_cli_todo_then_unificar(tmp_path):
    portal = FakePortalCAM(_registros_cam(), tope=50000)
    with _cli(tmp_path, portal, ["todo", "2024", "2024"]) as carpeta:
        pass
    with _cli(tmp_path, portal, ["unificar"]):
        pass
    salida = carpeta / "contratacion_comunidad_madrid_completo.csv"
    esperado = _menores(portal.registros) + [
        r for r in portal.registros
        if r["Tipo de Publicación"] != "Contratos menores" and r["_publicado"].startswith("2024")]
    assert _leer_unificado(salida) == _esperado(esperado)
    # one CSV per entity (no split needed below the real 50K cap)
    assert sorted(p.name for p in (carpeta / "csv_originales").glob("menores_*")) == sorted(
        cam.nombre_csv_entidad(int(v), t) for v, (t, _) in ENTIDADES_CAM.items())


def test_cam_cli_otros_defaults_reach_the_current_month(tmp_path):
    portal = FakePortalCAM([], tope=50000)
    with _cli(tmp_path, portal, ["otros"]):
        pass
    hoy = datetime.now()
    inicios = {b["createddate"] for b in portal.busquedas}
    assert "01-01-2017" in inicios
    assert f"01-{hoy.month:02d}-{hoy.year}" in inicios
    # every month since 2017 + one "published up to 31-12-2016" search per type
    assert len(portal.busquedas) == ((hoy.year - 2017) * 12 + hoy.month + 1) * len(
        cam.TIPOS_NO_MENORES)
    anteriores = [b for b in portal.busquedas if b["createddate"] == ""]
    assert {b["createddate_1"] for b in anteriores} == {"31-12-2016"}
    assert len(anteriores) == len(cam.TIPOS_NO_MENORES)


def test_cam_cli_prueba_downloads_hospital_38(tmp_path):
    portal = FakePortalCAM(_registros_cam(), tope=50000)
    with _cli(tmp_path, portal, ["prueba"]) as carpeta:
        pass
    fichero = carpeta / "csv_originales" / cam.nombre_csv_entidad(38, ENTIDADES_CAM["38"][0])
    df = pd.read_csv(fichero, sep=";", encoding="utf-8-sig", dtype=str)
    assert len(df) == len([r for r in portal.registros
                           if r["Entidad Adjudicadora"] == ENTIDADES_CAM["38"][1]
                           and r["Tipo de Publicación"] == "Contratos menores"])


# =============================================================================
# COMUNIDAD DE MADRID — survivorship bias: raw versions, accumulation, seed
# =============================================================================
DIA1 = datetime(2026, 2, 9, 17, 0, tzinfo=timezone.utc).timestamp()
DIA2 = DIA1 + 86400
SELLO1 = "__20260209T170000Z.csv"


def _iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sellar(csv_dir, dia):
    """The CSV copies written by the run that just ended were downloaded on
    `dia` (a version's date is its mtime): simulates runs on different days."""
    for p in Path(csv_dir).glob("*.csv"):
        if p.stat().st_mtime > time.time() - 600:
            os.utime(p, (dia, dia))


def _dia(cam_dirs, dia, menores=True, otros=None):
    """A run on day `dia` that asks for everything again (VIGENCIA_HORAS=0)."""
    with patch.object(cam, "VIGENCIA_HORAS", 0):
        d = cam.DescargadorComunidadMadrid()
        if menores:
            d.descargar_menores()
        if otros:
            d.descargar_otros(*otros)
    _sellar(cam_dirs / "csv_originales", dia)
    return d


def _tabla(carpeta):
    """Consolidated CSV as text; the parquet must hold exactly the same."""
    df = pd.read_csv(Path(carpeta) / "contratacion_comunidad_madrid_completo.csv", sep=";",
                     encoding="utf-8-sig", dtype=str, keep_default_na=False)
    par = pd.read_parquet(Path(carpeta) / "contratacion_comunidad_madrid_completo.parquet")
    assert list(par.columns) == list(df.columns)
    assert par["_en_ultima_descarga"].dtype == bool
    assert par.astype(object).where(par.notna(), "").astype(str).values.tolist() == df.values.tolist()
    return df


def _filas(df):
    return [tuple(f) for f in df[COLUMNAS_CAM].itertuples(index=False)]


def _fila(r):
    return tuple(r[c] for c in COLUMNAS_CAM)


def _estado_carpeta(carpeta):
    """Every file (but the checks manifest) with its bytes and mtime."""
    return {str(p.relative_to(carpeta)): (p.read_bytes(), p.stat().st_mtime)
            for p in sorted(Path(carpeta).rglob("*")) if p.is_file() and p.name != cam.COMPROBACIONES}


def test_cam_a_withdrawn_menor_stays_with_en_ultima_descarga_false(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    todos = _menores(portal.registros)
    retirado = next(r for r in portal.registros if r["Referencia"] == "5-02")
    portal.registros.remove(retirado)
    _dia(cam_dirs, DIA2)
    cam.unificar_csvs()

    df = _tabla(cam_dirs)
    assert sorted(_filas(df)) == _esperado(todos)          # nothing lost, nothing duplicated
    fila = df[df["Referencia"] == "5-02"].iloc[0]
    assert fila[META_CAM].tolist() == [_iso(DIA1), _iso(DIA1), "False"]
    sigue = df[df["Referencia"] == "5-01"].iloc[0]
    assert sigue[META_CAM].tolist() == [_iso(DIA1), _iso(DIA2), "True"]
    assert set(df.loc[df["Referencia"] != "5-02", "_en_ultima_descarga"]) == {"True"}
    # raw layer: the previous copy of the entity CSV, with its date, in _historico/
    sanidad = cam.nombre_csv_entidad(5, ENTIDADES_CAM["5"][0])
    assert [p.name for p in (csv_dir / "_historico").iterdir()] == [sanidad.replace(".csv", SELLO1)]
    assert "5-02" in (csv_dir / "_historico" / sanidad.replace(".csv", SELLO1)).read_text(encoding="utf-8-sig")
    assert "5-02" not in (csv_dir / sanidad).read_text(encoding="utf-8-sig")


def test_cam_a_changed_menor_keeps_both_versions(cam_dirs, portal):
    _dia(cam_dirs, DIA1)
    registro = next(r for r in portal.registros if r["Referencia"] == "5-03")
    anterior = dict(registro)
    registro.update({"Estado": "Anulado", "Importe de adjudicación": "999,99"})
    _dia(cam_dirs, DIA2)
    cam.unificar_csvs()

    filas = _tabla(cam_dirs).query("Referencia == '5-03'")
    assert _filas(filas) == [_fila(anterior), _fila(registro)]
    assert filas[META_CAM].values.tolist() == [[_iso(DIA1), _iso(DIA1), "False"],
                                               [_iso(DIA2), _iso(DIA2), "True"]]


def _convocatoria(ref, adjudicatario, continuaciones=()):
    registro = _registro(cam.TIPOS_NO_MENORES[0], "5", ref, 1000, "2024-03-15", adjudicatario=adjudicatario)
    registro["_continuaciones"] = list(continuaciones)
    return registro


def test_cam_withdrawn_and_changed_records_keep_their_continuation_rows(cam_dirs):
    prorroga = _continuacion(prorroga="1.000,00")       # identical row in both contracts
    lote = _continuacion("LOTE DOS SL", "B2", "500,00")
    a = _convocatoria("A-1", "UNO SL", [prorroga])
    b = _convocatoria("B-1", "DOS SL", [prorroga])
    c = _convocatoria("C-1", "TRES SL")
    portal = FakePortalCAM([a, b, c], tope=50000)
    with patch.object(cam.requests, "Session", portal.session):
        _dia(cam_dirs, DIA1, menores=False, otros=(2024, 2024))
        # A is withdrawn and B gets another lot
        portal.registros = [dict(b, _continuaciones=[prorroga, lote]), c]
        _dia(cam_dirs, DIA2, menores=False, otros=(2024, 2024))
    cam.unificar_csvs()

    df = _tabla(cam_dirs)
    # the old blocks stay whole (record + its continuation) and the new one
    # follows, with its rows together and in order
    assert _filas(df) == [_fila(a), _fila(prorroga), _fila(b), _fila(prorroga), _fila(c),
                          _fila(b), _fila(prorroga), _fila(lote)]
    assert df["_en_ultima_descarga"].tolist() == ["False"] * 4 + ["True"] * 4
    assert df["_primera_descarga"].tolist() == [_iso(DIA1)] * 5 + [_iso(DIA2)] * 3
    assert df["_ultima_descarga"].tolist() == [_iso(DIA1)] * 4 + [_iso(DIA2)] * 4


def test_cam_an_empty_or_failed_download_withdraws_nothing(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    cam.unificar_csvs()
    antes, copias = _tabla(cam_dirs), {p.name: p.read_bytes() for p in csv_dir.glob("*.csv")}
    portal.vaciar = lambda p: p.get("entidad_adjudicadora") == "5"      # header only
    portal.fallar = lambda p: p.get("entidad_adjudicadora") == "120"    # HTTP 500
    d = _dia(cam_dirs, DIA2)
    cam.unificar_csvs()

    assert d.stats["error"] == 1 and d.stats["skip_vacio"] >= 1
    assert {p.name: p.read_bytes() for p in csv_dir.glob("*.csv")} == copias
    assert not (csv_dir / "_historico").exists() and not (cam_dirs / "_historico").exists()
    assert _tabla(cam_dirs).equals(antes)


def test_cam_a_version_without_rows_withdraws_nothing(cam_dirs):
    csv_dir = cam_dirs / "csv_originales"
    m1 = _registro("Contratos menores", "120", "M-1", 50)
    (csv_dir / "_historico").mkdir()
    (csv_dir / "_historico" / f"menores_ent120_canal{SELLO1}").write_bytes(FakePortalCAM._csv([m1]))
    (csv_dir / "menores_ent120_canal.csv").write_bytes(FakePortalCAM._csv([]))
    cam.unificar_csvs()

    df = _tabla(cam_dirs)
    assert _filas(df) == [_fila(m1)]
    assert df[META_CAM].values.tolist() == [[_iso(DIA1), _iso(DIA1), "True"]]


def test_cam_a_rerun_without_changes_writes_nothing_new(cam_dirs, portal):
    _dia(cam_dirs, DIA1, otros=(2024, 2024))
    cam.unificar_csvs()
    antes = _estado_carpeta(cam_dirs)
    d = _dia(cam_dirs, DIA2, otros=(2024, 2024))
    cam.unificar_csvs()

    assert d.stats["nuevo"] == d.stats["actualizado"] == 0 and d.stats["sin_cambios"] > 0
    assert _estado_carpeta(cam_dirs) == antes             # no new version, raw or output


def test_cam_an_interrupted_run_resumes_without_asking_again(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    (csv_dir / cam.COMPROBACIONES).unlink()     # copies from the day before, known only by their date
    portal.servidos, portal.servidas, portal.cortar_en = 0, [], 8
    with pytest.raises(_Corte):
        cam.DescargadorComunidadMadrid().descargar_menores()
    servidas = portal.servidas[:]
    assert len(servidas) == 8 and not list((csv_dir / "_historico").glob("*"))   # nothing changed

    portal.cortar_en, portal.servidas = None, []
    cam.DescargadorComunidadMadrid().descargar_menores()
    consulta = lambda b: (b["entidad_adjudicadora"], b["presupuesto_base_licitacion_total"],  # noqa: E731
                          b["presupuesto_base_licitacion_total_1"])
    # what was checked before the cut (unchanged or empty: nothing to see on
    # disk) is not asked again; the rest is
    assert not {consulta(b) for b in servidas} & {consulta(b) for b in portal.servidas}
    assert portal.servidas
    cam.unificar_csvs()
    assert _leer_unificado(cam_dirs / "contratacion_comunidad_madrid_completo.csv") == \
        _esperado(_menores(portal.registros))


def test_cam_a_renumbered_entity_moves_its_old_csv_to_the_history(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    retirado = next(r for r in portal.registros if r["Referencia"] == "5-02")
    portal.registros.remove(retirado)
    # the dropdown renumbers its options (Sanidad was the 28th in February
    # 2026 and the 60th in September)
    portal.entidades = {"38": ENTIDADES_CAM["38"], "60": ENTIDADES_CAM["5"], "120": ENTIDADES_CAM["120"]}
    _dia(cam_dirs, DIA2)

    viejo = cam.nombre_csv_entidad(5, ENTIDADES_CAM["5"][0])
    nuevo = cam.nombre_csv_entidad(60, ENTIDADES_CAM["5"][0])
    assert not (csv_dir / viejo).exists() and (csv_dir / nuevo).exists()
    assert [p.name for p in (csv_dir / "_historico").iterdir()] == [viejo.replace(".csv", SELLO1)]
    cam.unificar_csvs()
    df = _tabla(cam_dirs)
    assert sorted(_filas(df)) == _esperado(_menores(portal.registros + [retirado]))   # each once
    sanidad = df[df["Entidad Adjudicadora"] == "Consejería de Sanidad"].set_index("Referencia")
    columnas = ["_archivo_fuente"] + META_CAM
    assert sanidad.loc["5-02", columnas].tolist() == [viejo, _iso(DIA1), _iso(DIA1), "False"]
    assert sanidad.loc["5-01", columnas].tolist() == [nuevo, _iso(DIA1), _iso(DIA2), "True"]


def test_cam_a_dropdown_with_few_entities_archives_nothing(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    antes = _csvs(csv_dir)
    portal.entidades = {"5": ENTIDADES_CAM["5"]}          # 1 of the 3 entities with CSV
    _dia(cam_dirs, DIA2)
    assert _csvs(csv_dir) == antes
    assert not (csv_dir / "_historico").exists()


def test_cam_an_entity_that_reaches_the_cap_is_split_without_losing_its_csv(cam_dirs, portal):
    csv_dir = cam_dirs / "csv_originales"
    _dia(cam_dirs, DIA1)
    entera = csv_dir / cam.nombre_csv_entidad(5, ENTIDADES_CAM["5"][0])
    antes = entera.read_bytes()
    # 7 menores ≥ the (scaled-down) cap of 6: the whole-entity answer is truncated
    portal.registros += [_registro("Contratos menores", "5", f"5-{i}", p)
                         for i, p in [(4, 1200), (5, 2500), (6, 7000), (7, 12000)]]
    _dia(cam_dirs, DIA2)

    assert not entera.exists()      # the truncated answer never replaced it...
    assert [p.read_bytes() for p in (csv_dir / "_historico").glob(entera.stem + "__*.csv")] == [antes]
    assert any(p.name.startswith(entera.stem[:14]) and "_imp" in p.name for p in csv_dir.glob("*.csv"))
    cam.unificar_csvs()
    df = _tabla(cam_dirs)
    assert sorted(_filas(df)) == _esperado(_menores(portal.registros))
    assert set(df["_en_ultima_descarga"]) == {"True"}
    sanidad = df[df["Entidad Adjudicadora"] == "Consejería de Sanidad"].set_index("Referencia")
    assert sanidad.loc["5-01", "_primera_descarga"] == _iso(DIA1)   # ...and its dates are kept
    assert sanidad.loc["5-4", "_primera_descarga"] == _iso(DIA2)


# --- --semilla ----------------------------------------------------------------
def _publicado(filas):
    """Published table (release v2026.02 layout): every column is text and
    the empty cells are the string 'nan'."""
    return pd.DataFrame([{**{c: fila.get(c, "") or "nan" for c in COLUMNAS_CAM},
                          "_archivo_fuente": fila["_archivo_fuente"]} for fila in filas])


def _escenario_semilla(cam_dirs, portal):
    """A download (menores + 2024) and a published table with one row of each
    case: {caso: row of the seed}."""
    conv = cam.TIPOS_NO_MENORES[0]
    lotes = next(r for r in portal.registros if r["Referencia"] == "L-1")
    prorroga = _continuacion(prorroga="1.000,00")
    lotes["_continuaciones"] = [prorroga]
    _dia(cam_dirs, DIA1, otros=(2024, 2024))
    marzo, julio = cam.nombre_csv_mes(2024, 3, conv), cam.nombre_csv_mes(2024, 7, conv)
    reparacion = next(r for r in portal.registros if r["Título del contrato"] == "Reparación bomba")
    casos = {
        # key in the download (with other content): not added
        "clave_presente": dict(next(r for r in portal.registros if r["Referencia"] == "5-01"),
                               Estado="Anulado", _archivo_fuente="menores_ent028_consejer_a_de_sanidad.csv"),
        "menor_retirado": dict(_registro("Contratos menores", "5", "5-99", 12.5),
                               _archivo_fuente="menores_ent028_consejer_a_de_sanidad.csv"),
        "entidad_sin_menores": dict(_registro("Contratos menores", "5", "H-1", 30),
                                    **{"Entidad Adjudicadora": "Consejería Histórica",
                                       "_archivo_fuente": "menores_ent099_consejer_a_hist_rica.csv"}),
        "anuncio_retirado": dict(_registro(conv, "5", "L-9", 5000), _archivo_fuente=marzo),
        "anuncio_mes_vacio": dict(_registro(conv, "38", "J-1", 7000), _archivo_fuente=julio),
        "anuncio_no_descargado": dict(_registro(conv, "5", "Z-1", 800),
                                      _archivo_fuente=cam.nombre_csv_mes(2019, 5, conv)),
        # no Referencia: compared by content
        "sin_referencia_presente": dict(reparacion, _archivo_fuente="menores_ent120_canal.csv"),
        "sin_referencia_retirado": dict(reparacion, **{"Título del contrato": "Otra reparación",
                                                       "_archivo_fuente": "menores_ent120_canal.csv"}),
        # continuation rows (no key at all): compared with the continuation rows
        "continuacion_presente": dict(prorroga, _archivo_fuente=marzo),
        "continuacion_retirada": dict(_continuacion("OTRA SL", "B9", "7,00"), _archivo_fuente=marzo),
    }
    ruta = cam_dirs / "publicado.parquet"
    _publicado(list(casos.values())).to_parquet(ruta, index=False)
    return casos, ruta


ANADIDOS = ["menor_retirado", "anuncio_retirado", "anuncio_mes_vacio", "sin_referencia_retirado",
            "continuacion_retirada"]


def test_cam_the_seed_adds_only_the_missing_keys_of_what_was_downloaded(cam_dirs, portal):
    casos, ruta = _escenario_semilla(cam_dirs, portal)
    cam.unificar_csvs()
    sin = _tabla(cam_dirs)
    cam.unificar_csvs([ruta])
    con = _tabla(cam_dirs)

    assert list(con.columns) == list(sin.columns) + ["_origen"]
    # the downloaded rows are untouched: same values, marks and order
    assert con.iloc[:len(sin)][list(sin.columns)].equals(sin)
    assert set(con["_origen"].iloc[:len(sin)]) == {""}
    anadidas = con.iloc[len(sin):]
    assert _filas(anadidas) == [_fila(casos[k]) for k in ANADIDOS]       # 'nan' is empty again
    assert anadidas["_archivo_fuente"].tolist() == [casos[k]["_archivo_fuente"] for k in ANADIDOS]
    assert set(anadidas["_origen"]) == {"release v2026.02"}
    assert set(anadidas["_en_ultima_descarga"]) == {"False"}
    assert set(anadidas["_primera_descarga"]) == set(anadidas["_ultima_descarga"]) == {""}

    # the same seed twice adds nothing more (and the output does not change)
    salida = cam_dirs / "contratacion_comunidad_madrid_completo.parquet"
    antes = salida.read_bytes()
    cam.unificar_csvs([ruta, ruta])
    assert salida.read_bytes() == antes


def test_cam_the_seed_report_counts_each_case(cam_dirs, portal):
    casos, ruta = _escenario_semilla(cam_dirs, portal)
    sin = cam.unificar_csvs()
    con = cam.unificar_csvs([ruta])
    informe, = con["semillas"]
    assert (informe["leidas"], informe["anadidas"], informe["descartadas_clave"],
            informe["descartadas_contenido"], informe["fuera_ambito"]) == (10, 5, 1, 2, 2)
    assert informe["fuera_ambito_detalle"] == {
        "menores de entidades sin menores en la tabla": 1,
        "anuncios de CSV (mes y tipo) no descargados": 1}
    assert informe["celdas_nan_vaciadas"] > 0
    assert (con["filas"], con["retiradas"]) == (sin["filas"] + 5, sin["retiradas"] + 5)
    assert sin["semillas"] == [] and set(con["salidas"].values()) == {"actualizado"}


def test_cam_cli_semilla(tmp_path):
    portal = FakePortalCAM(_registros_cam(), tope=50000)
    with _cli(tmp_path, portal, ["todo", "2024", "2024"]) as carpeta:
        pass
    retirado = dict(_registro("Contratos menores", "5", "5-99", 12.5), _archivo_fuente="x.csv")
    ruta = tmp_path / "publicado.parquet"
    _publicado([retirado]).to_parquet(ruta, index=False)
    with _cli(tmp_path, portal, ["unificar", "--semilla", str(ruta)]):
        pass
    df = _tabla(carpeta)
    assert _filas(df.iloc[-1:]) == [_fila(retirado)]
    assert df.iloc[-1][["_origen", "_en_ultima_descarga"]].tolist() == ["release v2026.02", "False"]


def test_cam_cli_semilla_only_with_unificar_and_it_must_exist(tmp_path):
    portal = FakePortalCAM(_registros_cam(), tope=50000)
    with pytest.raises(SystemExit) as salida:
        with _cli(tmp_path, portal, ["menores", "--semilla", "publicado.parquet"]):
            pass
    assert salida.value.code == 2 and portal.busquedas == []
    with _cli(tmp_path, portal, ["todo", "2024", "2024"]) as carpeta:
        pass
    with pytest.raises(SystemExit) as salida:
        with _cli(tmp_path, portal, ["unificar", "--semilla", str(tmp_path / "no_existe.parquet")]):
            pass
    assert salida.value.code == 1
    assert not (carpeta / "contratacion_comunidad_madrid_completo.csv").exists()


@pytest.mark.parametrize("todas", [True, False], ids=["every_row", "one_row"])
def test_cam_rows_with_more_fields_than_the_header_lose_nothing(cam_dirs, todas):
    # a separator too many at the end of every row (or of one) must neither
    # shift the columns (pandas would take the first one as the index) nor
    # drop the field
    m1 = _registro("Contratos menores", "120", "M-1", 50)
    m2 = _registro("Contratos menores", "120", "M-2", 60)
    lineas = FakePortalCAM._csv([m1, m2]).decode("utf-8").split("\n")
    lineas[2] += ";EXTRA"
    if todas:
        lineas[1] += ";EXTRA"
    (cam_dirs / "csv_originales" / "menores_ent120_canal.csv").write_bytes("\n".join(lineas).encode("utf-8"))
    cam.unificar_csvs()

    df = pd.read_csv(cam_dirs / "contratacion_comunidad_madrid_completo.csv", sep=";",
                     encoding="utf-8-sig", dtype=str, keep_default_na=False)
    assert _filas(df) == [_fila(m1), _fila(m2)]
    assert df["_columna_extra_1"].tolist() == (["EXTRA", "EXTRA"] if todas else ["", "EXTRA"])
    assert list(df.columns) == COLUMNAS_CAM + ["_columna_extra_1", "_archivo_fuente"] + META_CAM


def test_cam_a_block_in_several_csvs_is_kept_as_the_present_copy_with_all_its_dates(cam_dirs):
    csv_dir = cam_dirs / "csv_originales"
    x = _registro("Contratos menores", "120", "X-1", 50)
    y = _registro("Contratos menores", "120", "Y-1", 60)
    (csv_dir / "_historico").mkdir()
    # 'a' (first by name): X on day 1, withdrawn on day 2; 'b': X from day 2 on
    (csv_dir / "_historico" / f"menores_ent120_a{SELLO1}").write_bytes(FakePortalCAM._csv([x, y]))
    (csv_dir / "menores_ent120_a.csv").write_bytes(FakePortalCAM._csv([y]))
    (csv_dir / "menores_ent120_b.csv").write_bytes(FakePortalCAM._csv([x]))
    for nombre in ("menores_ent120_a.csv", "menores_ent120_b.csv"):
        os.utime(csv_dir / nombre, (DIA2, DIA2))
    cam.unificar_csvs()

    df = _tabla(cam_dirs).set_index("Referencia")
    assert sorted(df.index) == ["X-1", "Y-1"]
    assert df.loc["X-1", ["_archivo_fuente"] + META_CAM].tolist() == \
        ["menores_ent120_b.csv", _iso(DIA1), _iso(DIA2), "True"]


def test_cam_the_parquet_bytes_do_not_depend_on_how_pandas_chunks_a_column(cam_dirs):
    # pandas 3 gives the columns in Arrow chunks and pandas 2 in one piece: the
    # output must be the same file (or switching pandas would add a version
    # to _historico/). Enough distinct values to overflow Parquet's dictionary.
    import pyarrow as pa
    valores = [hashlib.md5(str(i).encode()).hexdigest()[:14] for i in range(120_000)]
    columnas = ["Nº Expediente", "_en_ultima_descarga"]
    entera = pd.DataFrame({"Nº Expediente": pd.Series(valores, dtype=object), "_en_ultima_descarga": True})
    troceada = pd.DataFrame({"Nº Expediente": pd.Series(pd.arrays.ArrowStringArray(
        pa.chunked_array([valores[:1000], valores[1000:50_000], valores[50_000:]]))), "_en_ultima_descarga": True})
    assert cam.escribir_salidas([("x.csv", entera)], columnas)["contratacion_comunidad_madrid_completo.parquet"] == "nuevo"
    assert cam.escribir_salidas([("x.csv", troceada)], columnas) == {
        "contratacion_comunidad_madrid_completo.csv": "sin_cambios",
        "contratacion_comunidad_madrid_completo.parquet": "sin_cambios"}
