"""Offline tests for the two Madrid scrapers in ``comunidad_madrid/``.

Neither portal is reachable from the test environment, so every HTTP
interaction is simulated:

* ``ccaa_madrid_ayuntamiento.py`` (datos.madrid.es): discovery pages and CSV
  downloads are served by a mocked ``requests.get``; the CSV fixtures follow the
  header layouts documented in the script for each structure (A-F, AC_OLD,
  AC_OLD_MOD, AC_NEW, AC_2025, homologación, files with title rows / without
  header).
* ``descarga_contratacion_comunidad_madrid_v1.py``
  (contratos-publicos.comunidad.madrid): ``FakePortalCAM`` implements the flow
  described in the script docstring (antibot key in drupal-settings, search
  that stores the filters in the session, maths CAPTCHA, completion form, CSV
  export with a row cap) and filters its own records by the search params.
"""

import csv
import importlib.util
import io
import os
import runpy
import shutil
import sys
import warnings
from contextlib import contextmanager, redirect_stdout
from datetime import date, datetime
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


def test_ayto_leer_csv_decodes_cp1252_and_falls_back_to_latin1(tmp_path):
    cp1252 = tmp_path / "cp1252.csv"
    cp1252.write_bytes("OBJETO;IMPORTE;NIF\r\n“Suministro” – oficina;1.815,00 €;B1\r\n"
                       .encode("cp1252"))
    df = ayto.leer_csv(cp1252)
    assert df.iloc[0].tolist() == ["“Suministro” – oficina", "1.815,00 €", "B1"]

    # 0x81 is undefined in cp1252 → latin-1 fallback, no crash
    latin1 = tmp_path / "latin1.csv"
    latin1.write_bytes(b"OBJETO;IMPORTE;NIF\r\nA\x81B;1,00;B2\r\n")
    assert ayto.leer_csv(latin1).iloc[0].tolist() == ["A\x81B", "1,00", "B2"]

    bom = tmp_path / "bom.csv"
    bom.write_bytes("OBJETO;IMPORTE;NIF\r\nÁrea;2,00;B3\r\n".encode("utf-8-sig"))
    assert list(ayto.leer_csv(bom).columns) == ["OBJETO", "IMPORTE", "NIF"]


def test_ayto_leer_csv_empty_file_returns_empty_frame(tmp_path):
    vacio = tmp_path / "vacio.csv"
    vacio.write_bytes(b"")
    assert ayto.leer_csv(vacio).empty
    with redirect_stdout(io.StringIO()):
        assert ayto.procesar_fichero("cesiones_2024", vacio).empty


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
    return pd.DataFrame(datos, dtype=object)


def test_ayto_limpiar_parses_day_first_and_iso_dates_without_swapping():
    df = _frame_unificado(
        fuente_fichero=["formalizados_2025"] * 5,
        objeto_contrato=["a", "b", "c", "d", "e"],
        fecha_adjudicacion=["05/03/2025", "2025-03-05", "2025-03-05 00:00:00",
                            "15/03/25", None],
        fecha_formalizacion=["2025/03/06", "06/03/2025", None, None, "2025-12-01"],
    )
    out = ayto.limpiar_dataframe(df)
    assert out["fecha_adjudicacion"].tolist()[:4] == [pd.Timestamp("2025-03-05")] * 3 + [
        pd.Timestamp("2025-03-15")]
    assert out["fecha_formalizacion"].tolist()[:2] == [pd.Timestamp("2025-03-06")] * 2
    # year fallbacks: fecha_adjudicacion → fecha_formalizacion
    assert out["anio"].tolist() == [2025.0] * 5


def test_ayto_limpiar_amounts_types_and_empty_rows():
    df = _frame_unificado(
        fuente_fichero=["menores_2019", "menores_2019", "menores_2019"],
        objeto_contrato=["Obra", None, None],
        importe_adjudicacion_iva_inc=["1.234,56", "15.000", None],
        tipo_contrato=[" suministros ", "Contrato de Servicios", None],
        pyme=[" si ", None, None],
    )
    out = ayto.limpiar_dataframe(df)
    # the third row has no objeto/importe/registro → dropped as empty
    assert len(out) == 2
    assert out["importe_adjudicacion_iva_inc"].tolist() == [1234.56, 15000.0]
    assert out["tipo_contrato"].tolist() == ["Suministro", "Servicios"]
    assert out["pyme"].iloc[0] == "SI"
    assert out["anio"].tolist() == [2019.0, 2019.0]   # from the file name


class _Resp:
    def __init__(self, status=200, text="", content=None):
        self.status_code = status
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error")


def _get_datos_madrid(paginas, ficheros, llamadas):
    urls = {**ayto._urls_respaldo_menores(), **ayto._urls_respaldo_actividad()}
    por_url = {urls[n]: b for n, b in ficheros.items()}

    def fake_get(url, timeout=None, **kwargs):
        llamadas.append(url)
        if url in paginas:
            return _Resp(text=paginas[url])
        if url in por_url:
            return _Resp(content=por_url[url])
        return _Resp(status=404)
    return fake_get


def test_ayto_documented_cli_end_to_end(tmp_path):
    """``python ccaa_madrid_ayuntamiento.py``: discovery (falls back to the
    built-in URL list), download, map, clean and export."""
    copia = tmp_path / "ccaa_madrid_ayuntamiento.py"
    shutil.copy(AYTO_PATH, copia)
    trabajo = tmp_path / "cwd"
    trabajo.mkdir()
    paginas = {ayto.PAGINA_CONTRATOS_MENORES: "<html><body>Sin enlaces</body></html>",
               ayto.PAGINA_ACTIVIDAD_CONTRACTUAL: "<html><body>Sin enlaces</body></html>"}
    llamadas = []
    fake_get = _get_datos_madrid(paginas, FICHEROS_AYTO, llamadas)

    def ejecutar():
        filtros = warnings.filters[:]
        try:
            with _cwd(trabajo), patch("requests.get", side_effect=fake_get), \
                    redirect_stdout(io.StringIO()) as salida:
                ns = runpy.run_path(str(copia), run_name="__main__")
        finally:
            warnings.filters[:] = filtros
        return ns["df"], salida.getvalue()

    df, log = ejecutar()
    out = trabajo / "datos_madrid_contratacion_completa"
    assert "Usando URLs de respaldo" in log
    # every fallback URL was requested; the ones without fixture got a 404 and
    # were skipped without aborting the run
    assert len(llamadas) == 2 + len(ayto._urls_respaldo_menores()) + len(
        ayto._urls_respaldo_actividad())
    assert sorted(p.name for p in (out / "csv_originales").iterdir()) == sorted(
        f"{n}.csv" for n in FICHEROS_AYTO)

    esperado_por_cat = {}
    for nombre in FICHEROS_AYTO:
        cat = ayto._clasificar_categoria(nombre)
        esperado_por_cat[cat] = esperado_por_cat.get(cat, 0) + N_REGISTROS_AYTO.get(nombre, 1)
    assert df["categoria"].value_counts().to_dict() == esperado_por_cat
    assert list(df.columns) == ayto.COLUMNAS_UNIFICADAS + ["anio"]

    # documented outputs
    for sufijo in ("csv", "parquet", "xlsx"):
        assert (out / f"actividad_contractual_madrid_completo.{sufijo}").exists()
    for cat, n in esperado_por_cat.items():
        por_cat = pd.read_csv(out / f"{cat}_madrid.csv", sep=";", encoding="utf-8-sig", dtype=str)
        assert len(por_cat) == n
    pq = pd.read_parquet(out / "actividad_contractual_madrid_completo.parquet")
    assert len(pq) == len(df)

    fila = pq.set_index("n_expediente")
    # licitación vs adjudicación, con/sin IVA
    assert fila.loc["EXP-N1", ["importe_licitacion_sin_iva", "importe_licitacion_iva_inc",
                               "importe_adjudicacion_sin_iva",
                               "importe_adjudicacion_iva_inc"]].tolist() == [
        100000.0, 121000.0, 90000.0, 108900.0]
    assert fila.loc["EXP-N1", "nif_adjudicatario"] == "B12345678"
    assert fila.loc["EXP-N1", "fecha_adjudicacion"] == pd.Timestamp("2023-05-02")
    assert fila.loc["EXP-N9", "fecha_adjudicacion"] == pd.Timestamp("2025-03-05")
    assert fila.loc["EXP-O1", "importe_adjudicacion_iva_inc"] == 1089000.0
    assert fila.loc["EXP-D1", "importe_adjudicacion_iva_inc"] == 40000.0
    assert fila.loc["EXP-A2", "importe_adjudicacion_iva_inc"] == 14520.0
    assert fila.loc["EXP-P1", "importe_prorroga"] == 300000.0
    assert fila.loc["EXP-CE2", "importe_cedido"] == 25000.0
    assert pq["anio"].notna().all()

    # second run reuses the downloaded CSVs: same result, nothing duplicated
    llamadas.clear()
    df2, log2 = ejecutar()
    assert "ya existe" in log2
    assert len(df2) == len(df)


def test_ayto_discovery_names_files_by_category_and_year():
    def li(texto, href):
        return (f'<li><div class="info"><p>{texto}</p></div>'
                f'<div class="enlaces"><a href="{href}">CSV</a></div></li>')

    menores = "<ul>" + "".join([
        f'<li><p>Contratos menores {a}</p><a href="/egob/catalogo/m{a}.csv">CSV</a></li>'
        for a in (2024, 2025)
    ]) + ('<li><p>Contratos menores 2021 (hasta febrero)</p>'
          '<a href="/egob/catalogo/m2021a.csv">CSV</a></li>'
          '<li><p>Contratos menores 2021 (desde marzo)</p>'
          '<a href="/egob/catalogo/m2021b.csv">CSV</a></li></ul>')
    actividad = "<ul>" + "".join([
        li("2024. Contratos inscritos en el Registro de Contratos", "/c/f24.csv"),
        li("2024. Contratos basados en acuerdo marco o sistema dinámico", "/c/am24.csv"),
        li("2024. Contratos modificados", "/c/mo24.csv"),
        li("2024. Contratos prorrogados", "/c/pr24.csv"),
        li("2024. Penalidades en contratos", "/c/pe24.csv"),
        li("2024. Cesiones de contratos", "/c/ce24.csv"),
        li("2024. Resoluciones de contratos", "/c/re24.csv"),
        li("2024. Contratos derivados de procedimientos de homologación", "/c/ho24.csv"),
    ]) + "</ul>"
    paginas = {ayto.PAGINA_CONTRATOS_MENORES: menores,
               ayto.PAGINA_ACTIVIDAD_CONTRACTUAL: actividad}
    with patch.object(ayto.requests, "get", side_effect=_get_datos_madrid(paginas, {}, [])), \
            redirect_stdout(io.StringIO()):
        urls_m = ayto.descubrir_csv_urls_menores()
        urls_a = ayto.descubrir_csv_urls_actividad()

    assert urls_m == {
        "menores_2021_desde_marzo": "https://datos.madrid.es/egob/catalogo/m2021b.csv",
        "menores_2021_hasta_febrero": "https://datos.madrid.es/egob/catalogo/m2021a.csv",
        "menores_2024": "https://datos.madrid.es/egob/catalogo/m2024.csv",
        "menores_2025": "https://datos.madrid.es/egob/catalogo/m2025.csv",
    }
    assert {n: ayto._clasificar_categoria(n) for n in urls_a} == {
        "formalizados_2024": "contratos_formalizados",
        "acuerdo_marco_2024": "acuerdo_marco",
        "modificados_2024": "modificados",
        "prorrogados_2024": "prorrogados",
        "penalidades_2024": "penalidades",
        "cesiones_2024": "cesiones",
        "resoluciones_2024": "resoluciones",
        "homologacion_2024": "homologacion",
    }


def test_ayto_discovery_falls_back_to_builtin_urls_on_http_error():
    with patch.object(ayto.requests, "get", side_effect=_get_datos_madrid({}, {}, [])), \
            redirect_stdout(io.StringIO()):
        assert ayto.descubrir_csv_urls_menores() == ayto._urls_respaldo_menores()
        assert ayto.descubrir_csv_urls_actividad() == ayto._urls_respaldo_actividad()


def test_ayto_failed_download_is_reported_not_saved(tmp_path):
    with patch.object(ayto, "CSV_DIR", tmp_path), \
            patch.object(ayto.requests, "get", return_value=_Resp(status=500)), \
            redirect_stdout(io.StringIO()) as salida:
        assert ayto.descargar_csv("menores_2024", "https://datos.madrid.es/x.csv") is None
    assert "ERROR" in salida.getvalue()
    assert not (tmp_path / "menores_2024.csv").exists()


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
    for i, p in enumerate([1, 2, 3, 4.5, 5, 6, 7, 8, 9, 10, 10, 15, 20, 25, 49.99,
                           120, 999.5, 14999.99, 50000]):
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

    def __init__(self, registros, tope):
        self.registros = registros
        self.tope = tope
        self.busqueda = None
        self.busquedas = []
        self.fallar = lambda params: False
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
            f'<option value="{v}">{texto}</option>' for v, (texto, _) in ENTIDADES_CAM.items())
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
            return FakeResponse(content=self._csv(self._filtrar(self.busqueda)), headers={
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
            if ent != "All" and ENTIDADES_CAM[ent][1] != r["Entidad Adjudicadora"]:
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
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";", lineterminator="\n")
        w.writerow(COLUMNAS_CAM)
        w.writerows([[r[c] for c in COLUMNAS_CAM] for r in filas])
        return ("﻿" + buf.getvalue()).encode("utf-8")


def _esperado(registros):
    return sorted(tuple(r[c] for c in COLUMNAS_CAM) for r in registros)


def _leer_unificado(salida):
    df = pd.read_csv(salida, sep=";", encoding="utf-8-sig", dtype=str,
                     keep_default_na=False)
    assert list(df.columns) == COLUMNAS_CAM + ["_archivo_fuente"]
    return sorted(tuple(f) for f in df[COLUMNAS_CAM].itertuples(index=False))


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

    ficheros = sorted(p.name for p in (cam_dirs / "csv_originales").iterdir())
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

    ficheros = sorted(p.name for p in (cam_dirs / "csv_originales").iterdir())
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
    assert len(portal.busquedas) == ((hoy.year - 2017) * 12 + hoy.month) * len(
        cam.TIPOS_NO_MENORES)


def test_cam_cli_prueba_downloads_hospital_38(tmp_path):
    portal = FakePortalCAM(_registros_cam(), tope=50000)
    with _cli(tmp_path, portal, ["prueba"]) as carpeta:
        pass
    fichero = carpeta / "csv_originales" / cam.nombre_csv_entidad(38, ENTIDADES_CAM["38"][0])
    df = pd.read_csv(fichero, sep=";", encoding="utf-8-sig", dtype=str)
    assert len(df) == len([r for r in portal.registros
                           if r["Entidad Adjudicadora"] == ENTIDADES_CAM["38"][1]
                           and r["Tipo de Publicación"] == "Contratos menores"])
