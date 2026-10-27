"""Tests de melilla/melilla_menores.py (sin red).

La PLACSP se simula con una sesión falsa que sirve las dos páginas (el perfil con su formulario JSF y la pestaña
Documentos con el epígrafe) y los documentos, con la forma de las reales (2026-10-27) y datos inventados. Las relaciones
de prueba se escriben como XLSX con openpyxl y como PDF con una tabla con cuadrícula (un PDF mínimo escrito aquí, que
pdfplumber lee como los de la Ciudad); la lectura del XLS (xlrd) se prueba con un libro falso con los tipos de celda de
los reales. La lectura de los 33 XLS y 13 PDF reales se verificó con la descarga completa (ver docs/fuentes/melilla.md).
"""

import importlib.util
import json
from datetime import date
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import pytest

RAIZ = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("melilla_menores", RAIZ / "melilla" / "melilla_menores.py")
M = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(M)

# ----------------------------------------------------------------------------------------------------------------------
# Páginas y documentos de prueba
# ----------------------------------------------------------------------------------------------------------------------

ACCION = "/wps/portal/plataforma/perfil_contratante/lista_perfiles/!ut/p/z1/abc/dz/d5/p0/IZ7_X=CZ6_X=LA0=Eview.jsp==/"
URL_FINAL = "https://contrataciondelestado.es/wps/portal/plataforma/perfil_contratante/lista_perfiles/!ut/p/z1/abc/"
SERVLET = "https://contrataciondelestado.es/FileSystem/servlet/GetDocumentByIdServlet"


def pagina_perfil(con_boton=True):
    boton = ('<input id="viewns_Z7_X_:perfilComp:linkPrepDocs" name="viewns_Z7_X_:perfilComp:linkPrepDocs" '
             'type="submit" value="Documentos" title="Documentos" />') if con_boton else ""
    return f"""<html><body>
<form name="cal"><input type="hidden" name="otro" value="no"/></form>
<form id="viewns_Z7_X_:perfilComp" name="viewns_Z7_X_:perfilComp" method="post" action="{ACCION}"
 class="formMarginLeft10px" enctype="application/x-www-form-urlencoded">
<input type="hidden" name="javax.faces.encodedURL" value="{ACCION}" />
{boton}
<input id="viewns_Z7_X_:perfilComp:linkPrepLic" name="viewns_Z7_X_:perfilComp:linkPrepLic" type="submit"
 value="Licitaciones" />
<input type="hidden" name="viewns_Z7_X_:perfilComp_SUBMIT" value="1" />
<input type="hidden" name="javax.faces.ViewState" value="vista+de/prueba==" />
</form></body></html>"""


def url_documento(doc_id):
    return f"{SERVLET}?cifrado=QUC1GjXXSiLkydRHJBmbpw%3D%3D&amp;DocumentIdParam={doc_id}"


VACIO = '<span class="oculto">vacio</span>'


def pagina_documentos(docs, epigrafe=True):
    """docs: [(doc_id, título, fecha 'dd Mon aaaa', descripción)] en el epígrafe de los menores."""
    filas = "".join(
        f'<div class="flex-inline-xxs" id="epigrafe_12_{i}" style="gap:0">'
        f'<div class="col-4 text-align-center primera-columna p-y-xxs no-desborda-palabra">{t}</div>'
        f'<div class="col-1 text-align-center borde-columna p-y-xxs"><a id="enlaceDescarga12_{i}" class="outputLinkEx" '
        f'href="{url_documento(d)}" target="_blank"><img alt="Descargar documento" /></a></div>'
        f'<div class="col-2 text-align-center borde-columna p-y-xxs no-desborda-palabra">{f}</div>'
        f'<div class="col-4 text-align-center borde-columna p-y-xxs no-desborda-palabra">'
        f'{desc or VACIO}</div>'
        f'<div class="col-1 text-align-center borde-columna p-y-xxs"><a href="#" title="Sello de Tiempo">s</a></div></div>'
        for i, (d, t, f, desc) in enumerate(docs))
    otro = ('<a id="tituloEpigrafe5" class="m-0 bold cl-blue" href="#">Otros documentos</a><div id="epigrafe_5">'
            f'<div class="flex-inline-xxs" id="epigrafe_5_0"><div class="col-4">Pliego</div><div class="col-1">'
            f'<a href="{url_documento("OTRO")}">x</a></div><div class="col-2">01 Jan 2020</div><div class="col-4"></div>'
            '</div></div>')
    menores = (f'<a id="tituloEpigrafe12" class="m-0 bold cl-blue" href="#" onclick="cambiar_Colapsable(12);">'
               f'Publicidad de la contrataci&#243;n menor</a></div><div id="epigrafe_12">{filas}</div>') if epigrafe else ""
    return f"<html><body><div>{otro}<div>{menores}</div></body></html>"


CAB_B = ["Expediente", "Objeto", "Tipo ", "Duración", "Presupuesto", "Importe de Adjudicación",
         "Publicidad\nNº Empresas\nInvitadas", "Número de\nLicitadores\nparticipantes", "Publicado en Tablón Electrónico",
         "Adjudicatario", "Fecha de Adjudicación", "Fecha Informe de Necesidad", "Codigo Unidad",
         "Nombre de la unidad responsable"]
CAB_A = ["EXPEDIENTE", "FECHA DE ADJUDICACIÓN", "OBJETO", "IMPORTE DE ADJUDICACIÓN", "PLAZO DE EJECUCIÓN", "IMPUESTOS",
         "TIPO", "FORMA DE TRAMITACIÓN", "LEGISLACIÓN APLICABLE", "CIF ADJUDICATARIO", "NOMBRE ADJUDICATARIO",
         "¿ADJUDICATARIO EXTRANJERO? (SI/NO)", "CODIGO UNIDAD", "DESCRIPCION UNIDAD"]
PERSONA = "PEREZ*INVENTADO,ANA"
DNI = "00000000T"
FILAS_B = [
    ["101/2024/CME", "SUMINISTRO DE PRUEBA", "SUMINISTROS", "1 MES", 1500.0, 1452.5, "3", "2", "NO",
     "EMPRESA INVENTADA, S.L. CIF: B00000001", "2024-01-15 10:20:30.0", "2024-01-10 09:00:00.0", "07.7",
     "Dirección General de Prueba"],
    ["102/2024/CME", "ACTUACION MUSICAL", "SERVICIOS", "1 DÍA", 3000.0, 3000.0, "1", "1", "NO",
     f"{PERSONA} CIF: {DNI}", "2024-02-01 12:00:00.0", "2024-01-20 08:00:00.0", "07.7", "Dirección General de Prueba"],
    ["103/2024/CME", "OBRA PEQUEÑA", "OBRAS", "2 MESES", 41000.0, 40950.75, "3", "3", "SI",
     "OTRA EMPRESA SA CIF: A00000002", "2024-03-05 13:00:00.0", "2024-02-01 10:00:00.0", "08.3",
     "Dirección General Arquitectura"],
    ["104/2024/CME", "SIN ADJUDICATARIO", "SERVICIOS", "0", 100.0, 0.0, "0", "0", "NO", "null CIF: nullnull",
     "2024-03-30 09:00:00.0", None, "17.", "PATRONATO DE TURISMO"],
]
DOC_ID = {"t1": "AAAA%2Bt1%3D", "anual": "BBBB%2Banual%3D", "oficio": "CCCC%2Boficio%3D", "t2pdf": "DDDD%2Bt2%3D",
          "t2xls": "EEEE%2Bt2x%3D", "vacia": "FFFF%2Bvacia%3D"}


def xlsx(ruta, cabecera, filas, titulo="Contratación Menor de la Ciudad Autónoma de Melilla", periodo=None):
    """Una relación como XLSX (hoja 'Report': título, periodo opcional, cabecera y filas)."""
    import openpyxl

    libro = openpyxl.Workbook()
    hoja = libro.active
    hoja.title = "Report"
    hoja.append([titulo])
    if periodo:
        hoja.append([periodo])
    hoja.append(cabecera)
    for f in filas:
        hoja.append(f)
    libro.save(ruta)
    return Path(ruta).read_bytes()


def _esc(t):
    return str(t).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def pdf_tabla(filas, titulo=None, ancho=170, alto=14):
    """Un PDF de una página con las filas en una tabla con cuadrícula (rectángulos por celda), Helvetica 6 pt."""
    n_col = max(len(f) for f in filas)
    W, H = 40 + ancho * n_col, 80 + alto * len(filas)
    ops, y = [], H - 30
    if titulo:
        ops.append(f"BT /F1 9 Tf 20 {y} Td ({_esc(titulo)}) Tj ET")
        y -= 2 * alto
    for fila in filas:
        x = 20
        for t in list(fila) + [""] * (n_col - len(fila)):
            ops.append(f"{x} {y - alto} {ancho} {alto} re S")
            if t not in (None, ""):
                ops.append(f"BT /F1 6 Tf {x + 2} {y - alto + 4} Td ({_esc(t)}) Tj ET")
            x += ancho
        y -= alto
    contenido = "\n".join(ops).encode("cp1252")
    objetos = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
               (f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {W} {H}] /Resources << /Font << /F1 4 0 R >> >> "
                "/Contents 5 0 R >>").encode(),
               b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>",
               b"<< /Length %d >>\nstream\n" % len(contenido) + contenido + b"\nendstream"]
    salida, offsets = b"%PDF-1.4\n", []
    for i, o in enumerate(objetos, 1):
        offsets.append(len(salida))
        salida += f"{i} 0 obj\n".encode() + o + b"\nendobj\n"
    xref = len(salida)
    salida += f"xref\n0 {len(objetos) + 1}\n0000000000 65535 f \n".encode()
    salida += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    salida += f"trailer\n<< /Size {len(objetos) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return salida


def filas_pdf_de(filas):
    """Las filas de prueba como texto del PDF (importes con coma decimal)."""
    def t(v):
        if isinstance(v, float):
            return (f"{v:.2f}".rstrip("0").rstrip(".")).replace(".", ",")
        return "" if v is None else str(v).replace("\n", " ")
    return [[t(v) for v in f] for f in filas]


class Respuesta:
    def __init__(self, contenido, url, codigo=200, cabeceras=None, saltos=0):
        self.content = contenido if isinstance(contenido, bytes) else contenido.encode("utf-8")
        self.status_code = codigo
        self.url = url
        self.headers = cabeceras or {}
        self.history = [None] * saltos

    @property
    def text(self):
        return self.content.decode("utf-8")


class Portal:
    """La PLACSP falsa: perfil (GET, con 2 redirecciones), Documentos (POST) y los documentos por DocumentIdParam."""

    def __init__(self, docs, codigos=None):
        self.headers = {}
        self.docs = dict(docs)       # doc_id -> (título, fecha, nombre en origen, bytes)
        self.lista = list(docs)       # qué doc_id lista el epígrafe
        self.codigos = codigos or {}  # 'perfil' | 'documentos' | doc_id -> código HTTP
        self.pedidas = []
        self.html_lista = None

    def _codigo(self, clave):
        return self.codigos.get(clave, 200)

    def get(self, url, timeout=None):
        self.pedidas.append(("GET", url))
        if url == M.URL_PERFIL:
            return Respuesta(pagina_perfil(), URL_FINAL, self._codigo("perfil"), saltos=2)
        doc_id = next(k for k in self.docs if unquote(k) == M.id_documento(url))
        _titulo, _fecha, nombre, contenido = self.docs[doc_id]
        return Respuesta(contenido, url, self._codigo(doc_id),
                         {"Content-Disposition": f'attachment; filename="{nombre}"'})

    def post(self, url, data=None, timeout=None):
        self.pedidas.append(("POST", url))
        assert url == "https://contrataciondelestado.es" + ACCION
        campos = dict(data)
        assert campos["viewns_Z7_X_:perfilComp:linkPrepDocs"] == "Documentos"
        assert campos["javax.faces.ViewState"] == "vista+de/prueba=="
        assert "viewns_Z7_X_:perfilComp:linkPrepLic" not in campos
        html = self.html_lista or pagina_documentos([(d, *self.docs[d][:2], None) for d in self.lista])
        return Respuesta(html, url, self._codigo("documentos"))


def cliente(portal):
    return M.Cliente(sesion=portal, pausa=0, dormir=lambda s: None)


def correr(tmp_path, portal, *args):
    c = cliente(portal) if portal is not None else None
    rc = M.main(["--salida", str(tmp_path / "salida"), *args], cliente=c)
    return rc, c


def parquet(tmp_path):
    return pd.read_parquet(tmp_path / "salida" / M.PARQUET)


def log(tmp_path):
    return (tmp_path / "salida" / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


@pytest.fixture
def docs(tmp_path):
    d = tmp_path / "docs"
    d.mkdir()
    t1 = xlsx(d / "t1.xlsx", CAB_B, FILAS_B)
    anual = xlsx(d / "anual.xlsx", CAB_B, [FILAS_B[0][:5] + [1500.0] + FILAS_B[0][6:]],
                 periodo="Del 01/01/2024 al 31/12/2024")
    oficio = pdf_tabla([["Oficio de remisión"]], titulo="Oficio")
    return {
        DOC_ID["t1"]: ("Contratación menor 1 trimestre 2024", "25 Apr 2024",
                       "DOC20240425111656Contratacion menor 1 trimestre 2024.xlsx", t1),
        DOC_ID["anual"]: ("CONTRATACIÓN MENOR AÑO 2024", "07 Mar 2025", "DOC20250307095326Contratacion menor 2024.xlsx",
                          anual),
        DOC_ID["oficio"]: ("Publicación de la información de Contratos Menores", "18 Jun 2019",
                           "DOC20190618100650OFICIO CONTRATOS MENORES.pdf", oficio),
    }


# ----------------------------------------------------------------------------------------------------------------------
# Páginas
# ----------------------------------------------------------------------------------------------------------------------

def test_formulario_documentos():
    accion, campos = M.formulario_documentos(pagina_perfil(), URL_FINAL)
    assert accion == "https://contrataciondelestado.es" + ACCION
    assert campos == [("javax.faces.encodedURL", ACCION), ("viewns_Z7_X_:perfilComp_SUBMIT", "1"),
                      ("javax.faces.ViewState", "vista+de/prueba=="),
                      ("viewns_Z7_X_:perfilComp:linkPrepDocs", "Documentos")]
    with pytest.raises(ValueError, match="botón «Documentos»"):
        M.formulario_documentos(pagina_perfil(con_boton=False), URL_FINAL)
    # Con los rótulos en otro idioma, el submit se encuentra por su name
    accion, campos = M.formulario_documentos(pagina_perfil().replace('value="Documentos"', 'value="Documents"'), URL_FINAL)
    assert campos[-1] == ("viewns_Z7_X_:perfilComp:linkPrepDocs", "Documents")


def test_leer_lista():
    html = pagina_documentos([("ID1%2B%3D", "Contratación menor 1 trimestre 2024", "25 Apr 2024", None),
                              ("ID2", "Contratación Menor CAM 2019 completo", "18 Mar 2021", "Relación anual")])
    lista = M.leer_lista(html)
    assert [d["posicion"] for d in lista] == [0, 1]
    assert lista[0]["titulo"] == "Contratación menor 1 trimestre 2024"
    assert lista[0]["fecha_publicacion"] == "2024-04-25" and lista[0]["descripcion"] is None
    assert lista[0]["doc_id"] == "ID1+=" and "&DocumentIdParam=ID1%2B%3D" in lista[0]["url"]
    assert lista[1]["descripcion"] == "Relación anual"
    assert "OTRO" not in {d["doc_id"] for d in lista}       # el epígrafe de otros documentos no cuenta
    with pytest.raises(ValueError, match="Publicidad de la contratación menor"):
        M.leer_lista(pagina_documentos([], epigrafe=False))
    assert M.leer_lista(pagina_documentos([])) == []


@pytest.mark.parametrize("texto, esperado", [("18 Jun 2019", "2019-06-18"), ("03 Mar 2020", "2020-03-03"),
                                              ("7 Ago 2024", "2024-08-07"), ("31 Feb 2020", None), ("ayer", None)])
def test_fecha_lista(texto, esperado):
    assert M.fecha_lista(texto) == esperado


def test_nombre_seguro_y_formato():
    assert M.nombre_seguro("../../DOC2019Contratación Menor 1º.xls") == "DOC2019Contratacion_Menor_1o.xls"
    assert M.formato_fichero(b"%PDF-1.4\n...") == "pdf"
    assert M.formato_fichero(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1rest") == "xls"
    assert M.formato_fichero(b"PK\x03\x04rest") == "xlsx"
    assert M.formato_fichero(b"<!DOCTYPE html><html>") == "html"


# ----------------------------------------------------------------------------------------------------------------------
# Qué periodo es cada documento: los 47 títulos y nombres reales del epígrafe (2026-10-27)
# ----------------------------------------------------------------------------------------------------------------------

REALES = [
    ("Publicación de la información de Contratos Menores", "DOC20190618100650OFICIO CONTRATOS MENORES PUBLICACION EN "
     "PLATAFORMA.pdf", "2019-06-18", None),
    ("Contratos Menores CAM 1º Trimestre", "DOC20190618100729CONTRATACION MENOR 1 TRIMESTRE 2019.pdf", "2019-06-18",
     ("trimestral", 2019, 1)),
    ("Contratos menores CAM 2º trimestre Excel", "DOC20190731141654Contratacion Menor Ciudad Autonoma de Melilla 2 "
     "trimestre 2019 Excel.xls", "2019-07-31", ("trimestral", 2019, 2)),
    ("Contratación menor 3º trimestre 2019 excel", "DOC20191025095623Contratos menores de la CAM 3 trimestre 2019.xls",
     "2019-10-25", ("trimestral", 2019, 3)),
    ("Contratación menor CAM 4 trimestre excel", "DOC20200303090015Contratacion menor CAM 4 trimestre.xls",
     "2020-03-03", ("trimestral", 2019, 4)),
    ("Contratación Menor CAM 4º trimestre", "DOC20200303092114Contratacion Menor CAM 4 trimestre.pdf", "2020-03-03",
     ("trimestral", 2019, 4)),
    ("Contratación Menor 3 trimestre 2020", "DOC20201217095113CONTRATOS MENORES CAM 3 TRIMESTRE 2020.xls",
     "2020-12-17", ("trimestral", 2020, 3)),
    ("Contratación Menor CAM 2019 completo", "DOC20210318100753Listado contratos menores CAM 2019.pdf", "2021-03-18",
     ("anual", 2019, None)),
    ("Contratación Menor 2020 completo", "DOC20210318101038Listado contratos menores CAM 2020.pdf", "2021-03-18",
     ("anual", 2020, None)),
    ("Contratos menores 3 trimestre excel", "DOC20211112094423contratos menores 3 trimestre 2021.xls", "2021-11-12",
     ("trimestral", 2021, 3)),
    ("CONTRATACIÓN MENOR AÑO 2021", "DOC20220307095326Contratacion menor 2021.xls", "2022-03-07", ("anual", 2021, None)),
    ("Contratación menor 3 trimestre 2024", "DOC20241111134514Contratacion menor 3 trimestre CAM.xls", "2024-11-11",
     ("trimestral", 2024, 3)),
    ("Contratación menor 2 trimestre 2025", "DOC20251006141557CME_2025_2T.xls", "2025-10-06", ("trimestral", 2025, 2)),
    ("Contratación menor 4 trimestre 2025", "DOC20260202084711Contratacion menor 1-10-2025 a 31-12-2025.xls",
     "2026-02-02", ("trimestral", 2025, 4)),
    ("Contratación menor 2 trimestre 2026", "DOC20260803100856Contratacion menor 2 trimestre 2026.xls", "2026-08-03",
     ("trimestral", 2026, 2)),
]


@pytest.mark.parametrize("titulo, nombre, publicado, esperado", REALES)
def test_periodo_por_titulo_reales(titulo, nombre, publicado, esperado):
    assert M.periodo_por_titulo(titulo, nombre, publicado) == esperado


@pytest.mark.parametrize("texto, esperado", [
    ("Contratación Menor de la CAM de 20190101 a 20190331", ("trimestral", 2019, 1)),
    ("Contratación Menor de la Ciudad Autónoma | Del 01/10/2025 al 31/12/2025", ("trimestral", 2025, 4)),
    ("Del 01/01/2024 al 31/12/2024", ("anual", 2024, None)),
    ("Del 01/04/2026 al 30/06/2026", ("trimestral", 2026, 2)),
    ("Del 01/04/2026 al 31/05/2026", None),          # no es un trimestre ni un año
    ("Contratación Menor de la Ciudad Autónoma de Melilla", None),
])
def test_periodo_en_texto(texto, esperado):
    assert M.periodo_en_texto(texto) == esperado


def test_anio_por_publicacion():
    assert M.anio_por_publicacion(4, "2020-03-03") == 2019
    assert M.anio_por_publicacion(2, "2019-07-31") == 2019
    assert M.anio_por_publicacion(1, "2024-04-25") == 2024
    assert M.anio_por_publicacion(3, "2021-11-12") == 2021


# ----------------------------------------------------------------------------------------------------------------------
# Celdas, personas físicas y cabecera
# ----------------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("valor, esperado", [(14196.44, "14196.44"), ("14196,44", "14196.44"), ("5000", "5000"),
                                              (5000.0, "5000"), ("5.000,50", "5000.5"), ("5.000", "5000"),
                                              ("0", "0"), (0.1 + 0.2, "0.3")])
def test_numeros(valor, esperado):
    assert M.numero_texto(M.numero(valor)) == esperado


def test_numero_que_no_lo_es():
    assert M.numero("REALIZADO") is None and M.numero(None) is None and M.numero("") is None


@pytest.mark.parametrize("valor, modo, esperado", [
    ("2019-11-08 15:18:12.0", None, "2019-11-08"),
    ("2019-11-08\n15:18:12.0", None, "2019-11-08"),
    ("07/03/2019", None, "2019-03-07"),
    ("26/08/0119", None, "0119-08-26"),               # imposible en origen: como viene
    ("RO 2021-04-26 14:20:26.0", None, "2021-04-26"),  # el desborde del PDF en la celda de la fecha
    (43531.0, 0, "2019-03-07"),                      # fecha de Excel
    ("", None, None), (None, None, None), ("sin fecha", None, "sin fecha"),
])
def test_fecha_texto(valor, modo, esperado):
    assert M.fecha_texto(valor, modo) == esperado


@pytest.mark.parametrize("nombre, nif, pf", [
    ("EMPRESA INVENTADA, S.L.", "B00000001", False),
    ("OTRA, SA", "A00000002", False),
    ("COMUNIDAD INVENTADA C.B.", "E00000003", False),
    (PERSONA, DNI, True),
    ("ANA INVENTADA", "000000000T", True),            # DNI con un 0 delante
    ("EXTRANJERA", "X0000000T", True),                # NIE
    ("ALGUIEN", "AB1234567", True),                   # pasaporte (el 'null' ya quitado)
    ("EMPRESA RARA S.L.", "12345678", True),          # un identificador que no es de persona jurídica: se oculta
    (PERSONA, None, True),
    ("NOMBRE CON DNI 00000000T", None, True),
    ("INVENTADO PEREZ,ANA", None, True),              # APELLIDOS,NOMBRE sin NIF
    ("SERVICIOS INVENTADOS, S.L.", None, False),      # coma y forma jurídica, sin NIF
    ("ASOCIACION INVENTADA", None, False),
    (None, None, False),
])
def test_persona_fisica(nombre, nif, pf):
    assert M.es_persona_fisica(nombre, nif) is pf


@pytest.mark.parametrize("valor, esperado", [
    ("EMPRESA INVENTADA, S.L. CIF: B00000001", ("EMPRESA INVENTADA, S.L.", "B00000001")),
    ("null CIF: nullnull", (None, None)),
    ("ALGUIEN CIF: AB1234567null", ("ALGUIEN", "AB1234567")),
    ("SIN CIF", ("SIN CIF", None)),
    (None, (None, None)),
])
def test_adjudicatario_b(valor, esperado):
    assert M.adjudicatario_b(valor) == esperado


def test_columnas_de():
    assert M.columnas_de(CAB_B, "x")[6:8] == ["n_empresas_invitadas", "n_licitadores"]
    assert M.columnas_de(CAB_A, "x")[9:12] == ["_nif_publicado", "_nombre_publicado", "adjudicatario_extranjero"]
    with pytest.raises(ValueError, match="cabecera no reconocida"):
        M.columnas_de(CAB_B + ["Columna Nueva"], "x")
    with pytest.raises(ValueError, match="faltan columnas"):
        M.columnas_de(["Expediente", "Objeto"], "x")


def test_registro_formatos():
    b = M.registro(M.columnas_de(CAB_B, "x"), FILAS_B[1])
    assert b["formato"] == "B" and b["adjudicatario_persona_fisica"] == "True"
    assert b["adjudicatario"] is None and b["adjudicatario_nif"] is None
    assert (b["importe_adjudicacion"], b["presupuesto"], b["n_licitadores"]) == ("3000", "3000", "1")
    assert b["fecha_adjudicacion"] == "2024-02-01" and b["fecha_informe_necesidad"] == "2024-01-20"
    fila_a = ["3953/2018/CME", 43531.0, "Campaña de prueba", 15599.98, 0.0, 599.99, "Servicios", "Ordinaria", None,
              "B00000001", "EMPRESA INVENTADA SL", "NO", "01.5", "Gabinete de Prensa"]
    a = M.registro(M.columnas_de(CAB_A, "x"), fila_a, 0)
    assert a["formato"] == "A" and a["fecha_adjudicacion"] == "2019-03-07"
    assert (a["importe_adjudicacion"], a["impuestos"], a["duracion"]) == ("15599.98", "599.99", "0")
    assert (a["adjudicatario"], a["adjudicatario_nif"], a["adjudicatario_persona_fisica"]) == (
        "EMPRESA INVENTADA SL", "B00000001", "False")
    sin = M.registro(M.columnas_de(CAB_A, "x"), fila_a[:9] + ["0", None] + fila_a[11:], 0)
    assert sin["adjudicatario_nif"] is None and sin["adjudicatario_persona_fisica"] == "False"


class _Celda:
    def __init__(self, ctype, value):
        self.ctype, self.value = ctype, value


class _Hoja:
    def __init__(self, filas):
        self.filas = filas
        self.nrows = len(filas)

    def cell_value(self, i, j):
        return self.filas[i][j].value

    def row(self, i):
        return self.filas[i]

    def row_values(self, i):
        return [c.value for c in self.filas[i]]


class _Libro:
    datemode = 0

    def __init__(self, filas):
        self.hoja = _Hoja(filas)

    def sheet_by_index(self, i):
        return self.hoja

    def release_resources(self):
        pass


def test_filas_xls_con_los_tipos_de_celda_reales(monkeypatch, tmp_path):
    """La lectura del XLS (xlrd) con las celdas como las traen los reales: título, periodo, cabecera, fechas como texto
    con hora o como fecha de Excel, números, nº de licitadores como texto y celdas vacías."""
    xlrd = pytest.importorskip("xlrd")
    T, N, F, V = xlrd.XL_CELL_TEXT, xlrd.XL_CELL_NUMBER, xlrd.XL_CELL_DATE, xlrd.XL_CELL_EMPTY
    texto = [_Celda(T, "Contratación Menor de la Ciudad Autónoma")] + [_Celda(V, "")] * 13
    periodo = [_Celda(T, "Del 01/01/2026 al 31/03/2026")] + [_Celda(V, "")] * 13
    cab = [_Celda(T, c) for c in CAB_B]
    f1 = [_Celda(T, "1/2026/CME"), _Celda(T, "OBJETO"), _Celda(T, "SERVICIOS"), _Celda(T, "1 MES"), _Celda(N, 100.0),
          _Celda(N, 96.5), _Celda(N, 3.0), _Celda(T, "2"), _Celda(T, "NO"), _Celda(T, "EMPRESA INVENTADA SL CIF: B00000001"),
          _Celda(F, 46031.5), _Celda(V, ""), _Celda(T, "07.7"), _Celda(T, "Dirección General de Prueba")]
    vacia = [_Celda(V, "")] * 14
    libro = _Libro([texto, periodo, cab, f1, vacia])
    monkeypatch.setattr(xlrd, "open_workbook", lambda *a, **k: libro)
    cabecera, filas, modo = M.filas_xls(tmp_path / "x.xls")
    assert cabecera == CAB_B and modo == 0 and [n for n, _ in filas] == [4, 5]
    registros, avisos = M.leer_relacion(tmp_path / "x.xls", "xls")
    assert len(registros) == 1 and avisos == []          # la fila vacía no es una fila
    r = registros[0]
    assert (r["expediente"], r["importe_adjudicacion"], r["n_empresas_invitadas"], r["n_licitadores"]) == (
        "1/2026/CME", "96.5", "3", "2")
    assert r["fecha_adjudicacion"] == "2026-01-09" and r["fecha_informe_necesidad"] is None and r["_fila"] == "4"
    assert M.texto_de_cabeza(tmp_path / "x.xls", "xls") == "Contratación Menor de la Ciudad Autónoma | Del 01/01/2026 al 31/03/2026"


# ----------------------------------------------------------------------------------------------------------------------
# PDF y cotejo
# ----------------------------------------------------------------------------------------------------------------------

def test_pdf_se_lee_como_el_xlsx(tmp_path):
    pdf = tmp_path / "r.pdf"
    pdf.write_bytes(pdf_tabla(filas_pdf_de([CAB_B] + FILAS_B), titulo="Contratación Menor de la Ciudad Autónoma"))
    x = tmp_path / "r.xlsx"
    xlsx(x, CAB_B, FILAS_B)
    rp, _ = M.leer_relacion(pdf, "pdf")
    rx, _ = M.leer_relacion(x, "xlsx")
    assert len(rp) == len(rx) == 4
    for a, b in zip(rx, rp):
        assert {k: v for k, v in a.items() if k != "_fila"} == {k: v for k, v in b.items() if k != "_fila"}
    c = M.cotejar(rx, rp)
    assert c["ok"] and c["filas_comunes"] == 4 and c["fecha_ilegible_pdf"] == 0
    assert not any(c["distintos"].values())


def test_cotejo_detecta_diferencias():
    base = [M.registro(M.columnas_de(CAB_B, "x"), f) for f in FILAS_B]
    otro = [dict(r) for r in base]
    otro[0]["importe_adjudicacion"] = "1452.6"
    otro[2]["fecha_adjudicacion"] = "AM2E0D2 4-03-05 13:00:00.0"    # ilegible en el PDF: no cuenta como distinta
    c = M.cotejar(base, otro + [dict(base[3])])
    assert not c["ok"] and c["distintos"]["importe_adjudicacion"] == 1 and c["fecha_ilegible_pdf"] == 1
    assert c["solo_pdf"] == 1 and c["solo_xls"] == 0 and c["distintos"]["fecha_adjudicacion"] == 0
    assert M.cotejar(base, [dict(r) for r in base])["ok"]


# ----------------------------------------------------------------------------------------------------------------------
# Punta a punta con la PLACSP falsa
# ----------------------------------------------------------------------------------------------------------------------

def test_primera_descarga(tmp_path, docs):
    portal = Portal(docs)
    rc, c = correr(tmp_path, portal)
    assert rc == 0
    # perfil (+2 redirecciones), Documentos y los 3 documentos
    assert c.peticiones == 7 and [m for m, _ in portal.pedidas] == ["GET", "POST", "GET", "GET", "GET"]
    df = parquet(tmp_path)
    assert len(df) == 5
    assert set(df["_relacion"]) == {"trimestral", "anual"}
    t1 = df[df["_relacion"] == "trimestral"].sort_values("_fila")
    assert list(t1["expediente"]) == ["101/2024/CME", "102/2024/CME", "103/2024/CME", "104/2024/CME"]
    assert list(t1["_anio"]) == ["2024"] * 4 and list(t1["_trimestre"]) == ["1"] * 4
    assert list(t1["importe_adjudicacion"]) == ["1452.5", "3000", "40950.75", "0"]
    assert t1["_publicado"].iloc[0] == "2024-04-25T11:16:56" and t1["_formato_origen"].iloc[0] == "xlsx"
    anual = df[df["_relacion"] == "anual"]
    assert list(anual["_anio"]) == ["2024"] and anual["_trimestre"].isna().all()
    assert df["_en_ultima_descarga"].all()
    # Persona física: ni su nombre ni su NIF en ninguna columna
    texto = " ".join(df.astype(str).fillna("").to_numpy().ravel())
    assert PERSONA.split("*")[0] not in texto and DNI not in texto
    pf = df[df["expediente"] == "102/2024/CME"].iloc[0]
    assert pf["adjudicatario_persona_fisica"] == "True" and pd.isna(pf["adjudicatario"])
    vacio = df[df["expediente"] == "104/2024/CME"].iloc[0]
    assert pd.isna(vacio["adjudicatario"]) and vacio["adjudicatario_persona_fisica"] == "False"
    man = json.loads((tmp_path / "salida" / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    clases = {e["titulo"]: (e["clase"], e["anio"], e["trimestre"], e["formato"]) for e in man.values()}
    assert clases == {"Contratación menor 1 trimestre 2024": ("trimestral", 2024, 1, "xlsx"),
                      "CONTRATACIÓN MENOR AÑO 2024": ("anual", 2024, None, "xlsx"),
                      "Publicación de la información de Contratos Menores": ("otro", None, None, "pdf")}
    texto_log = log(tmp_path)
    assert "Peticiones HTTP: 7" in texto_log and "trimestrales: 1 (2024-T1 a 2024-T1)" in texto_log
    assert "ERRORES" not in texto_log


def test_segunda_ejecucion_sin_documentos_nuevos(tmp_path, docs):
    portal = Portal(docs)
    assert correr(tmp_path, portal)[0] == 0
    antes = parquet(tmp_path)
    ficheros = sorted(p.name for p in (tmp_path / "salida" / "raw" / "documentos").iterdir())
    portal.pedidas.clear()
    rc, c = correr(tmp_path, portal)
    assert rc == 0 and c.peticiones == 4 and [m for m, _ in portal.pedidas] == ["GET", "POST"]
    despues = parquet(tmp_path)
    assert sorted(p.name for p in (tmp_path / "salida" / "raw" / "documentos").iterdir()) == ficheros
    columnas = [c for c in antes.columns if c not in ("_ultima_descarga",)]
    pd.testing.assert_frame_equal(antes[columnas].reset_index(drop=True), despues[columnas].reset_index(drop=True))
    assert "Documentos ya descargados (no se piden): 3" in log(tmp_path)


def test_documento_retirado_y_vuelto_a_publicar(tmp_path, docs):
    portal = Portal(docs)
    assert correr(tmp_path, portal)[0] == 0
    portal.lista = [DOC_ID["t1"], DOC_ID["oficio"]]
    assert correr(tmp_path, portal)[0] == 0
    df = parquet(tmp_path)
    assert not df.loc[df["_relacion"] == "anual", "_en_ultima_descarga"].any()
    assert df.loc[df["_relacion"] == "trimestral", "_en_ultima_descarga"].all()
    assert "RETIRADOS" in log(tmp_path)
    portal.lista = list(docs)
    portal.pedidas.clear()
    assert correr(tmp_path, portal)[0] == 0
    assert parquet(tmp_path)["_en_ultima_descarga"].all()
    assert len(portal.pedidas) == 2       # vuelve a la lista: no se pide otra vez


def test_403_en_el_perfil_para_todo(tmp_path, docs):
    portal = Portal(docs)
    assert correr(tmp_path, portal)[0] == 0
    antes = parquet(tmp_path)
    portal.codigos = {"perfil": 403}
    portal.pedidas.clear()
    rc, _ = correr(tmp_path, portal)
    assert rc == 1 and len(portal.pedidas) == 1
    assert "HTTP 403" in log(tmp_path)
    pd.testing.assert_frame_equal(antes, parquet(tmp_path))     # nada retirado ni cambiado


def test_429_en_un_documento_para_las_demas_descargas(tmp_path, docs):
    portal = Portal(docs, codigos={DOC_ID["anual"]: 429})
    rc, _ = correr(tmp_path, portal)
    assert rc == 1 and "HTTP 429" in log(tmp_path)
    pedidos = [M.id_documento(u) for m, u in portal.pedidas if m == "GET" and u != M.URL_PERFIL]
    assert pedidos == [unquote(DOC_ID["t1"]), unquote(DOC_ID["anual"])]   # el oficio, después, ya no se pide
    man = json.loads((tmp_path / "salida" / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert [e["titulo"] for e in man.values()] == ["Contratación menor 1 trimestre 2024"]
    assert all(e["publicado"] for e in man.values())


def test_lista_vacia_no_retira_nada(tmp_path, docs):
    portal = Portal(docs)
    assert correr(tmp_path, portal)[0] == 0
    portal.lista = []
    rc, _ = correr(tmp_path, portal)
    assert rc == 1 and "no trae ningún documento" in log(tmp_path)
    assert parquet(tmp_path)["_en_ultima_descarga"].all()


def test_pagina_sin_epigrafe_es_un_error(tmp_path, docs):
    portal = Portal(docs)
    portal.html_lista = pagina_documentos([], epigrafe=False)
    rc, _ = correr(tmp_path, portal)
    assert rc == 1 and "no trae el epígrafe" in log(tmp_path)
    assert not (tmp_path / "salida" / M.PARQUET).exists()
    # la página se guarda para verla
    assert (tmp_path / "salida" / "raw" / "_pagina_sin_lista.html").read_text(encoding="utf-8") == portal.html_lista
    assert "guardada en raw/_pagina_sin_lista.html" in log(tmp_path)


def test_respuesta_html_en_vez_del_documento(tmp_path, docs):
    docs[DOC_ID["anual"]] = docs[DOC_ID["anual"]][:3] + (b"<html><body>Error</body></html>",)
    rc, _ = correr(tmp_path, Portal(docs))
    assert rc == 1 and "la respuesta no es un documento (html" in log(tmp_path)
    assert len(list((tmp_path / "salida" / "raw" / "documentos").iterdir())) == 2
    assert set(parquet(tmp_path)["_relacion"]) == {"trimestral"}


def test_periodo_solo_en_pdf_y_periodo_con_los_dos(tmp_path, docs):
    filas_t2 = [[f[0].replace("2024", "2025")] + f[1:] for f in FILAS_B]
    pdf = pdf_tabla(filas_pdf_de([CAB_B] + filas_t2), titulo="Contratación Menor de la Ciudad Autónoma")
    docs[DOC_ID["t2pdf"]] = ("Contratación menor 2 trimestre 2025", "20 Jul 2025",
                             "DOC20250720100000Contratacion menor 2 trimestre 2025.pdf", pdf)
    rc, _ = correr(tmp_path, Portal(docs))
    assert rc == 0
    df = parquet(tmp_path)
    t2 = df[(df["_anio"] == "2025") & (df["_trimestre"] == "2")]
    assert len(t2) == 4 and set(t2["_formato_origen"]) == {"pdf"}
    assert list(t2.sort_values("_fila")["importe_adjudicacion"]) == ["1452.5", "3000", "40950.75", "0"]
    # Llega el XLS del mismo trimestre: el Parquet sale del XLS y se cotejan
    tmp = tmp_path / "t2.xlsx"
    docs[DOC_ID["t2xls"]] = ("Contratación menor 2 trimestre 2025 excel", "20 Jul 2025",
                             "DOC20250720100100Contratacion menor 2 trimestre 2025.xlsx", xlsx(tmp, CAB_B, filas_t2))
    assert correr(tmp_path, Portal(docs))[0] == 0
    cotejo = json.loads((tmp_path / "salida" / "raw" / "_cotejo.json").read_text(encoding="utf-8"))
    assert [(c["periodo"], c["ok"], c["filas_comunes"]) for c in cotejo.values()] == [("2025-T2", True, 4)]
    df = parquet(tmp_path)
    t2 = df[(df["_anio"] == "2025") & (df["_trimestre"] == "2")]
    assert set(t2["_formato_origen"]) == {"pdf", "xlsx"}       # las filas del PDF se conservan (ya se publicaron)
    assert "2025-T2: igual (4 filas en el XLS, 4 en el PDF" in log(tmp_path)


def test_cero_filas(tmp_path):
    """Una relación con la cabecera y ninguna fila: no hay Parquet que escribir, se avisa y no falla; sin ninguna
    relación (solo el oficio) es un error."""
    vacia = xlsx(tmp_path / "v.xlsx", CAB_B, [])
    oficio = pdf_tabla([["Oficio"]])
    docs = {DOC_ID["vacia"]: ("Contratación menor 1 trimestre 2024", "25 Apr 2024", "DOC20240425111656v.xlsx", vacia),
            DOC_ID["oficio"]: ("Publicación de la información de Contratos Menores", "18 Jun 2019", "DOC2019o.pdf",
                               oficio)}
    rc, _ = correr(tmp_path, Portal(docs))
    assert rc == 0 and not (tmp_path / "salida" / M.PARQUET).exists()
    assert "no tiene filas" in log(tmp_path)
    otro = tmp_path / "otro"
    otro.mkdir()
    rc = M.main(["--salida", str(otro / "salida")], cliente=cliente(Portal({DOC_ID["oficio"]: docs[DOC_ID["oficio"]]})))
    assert rc == 1 and "no hay ninguna relación" in (otro / "salida" / "raw" / "descarga_log.txt").read_text("utf-8")


def test_solo_parquet_no_pide_nada(tmp_path, docs):
    assert correr(tmp_path, Portal(docs))[0] == 0
    antes = (tmp_path / "salida" / M.PARQUET).read_bytes()
    rc, c = correr(tmp_path, None, "--solo-parquet")
    assert rc == 0 and c is None
    assert (tmp_path / "salida" / M.PARQUET).read_bytes() == antes


def test_max_descargas(tmp_path, docs):
    portal = Portal(docs)
    rc, _ = correr(tmp_path, portal, "--max-descargas", "1")
    assert rc == 0 and "pasa a la siguiente ejecución" in log(tmp_path)
    assert len(list((tmp_path / "salida" / "raw" / "documentos").iterdir())) == 1
    portal.pedidas.clear()
    assert correr(tmp_path, portal)[0] == 0
    assert len(portal.pedidas) == 4       # lista + los dos que faltaban


def test_hueco_entre_trimestres(tmp_path, docs):
    t4 = xlsx(tmp_path / "t4.xlsx", CAB_B, FILAS_B[:1])
    docs[DOC_ID["t2xls"]] = ("Contratación menor 4 trimestre 2024", "30 Jan 2025",
                             "DOC20250130135611contratos menores 4 trimestre 2024.xlsx", t4)
    assert correr(tmp_path, Portal(docs))[0] == 0
    assert "trimestres sin relación entre el primero y el último: 2024-T2, 2024-T3" in log(tmp_path)


def test_contenido_y_titulo_distintos(tmp_path, docs):
    """Manda el periodo del contenido, y se avisa."""
    mal = xlsx(tmp_path / "m.xlsx", CAB_B, FILAS_B[:1], periodo="Del 01/07/2024 al 30/09/2024")
    docs[DOC_ID["t1"]] = docs[DOC_ID["t1"]][:3] + (mal,)
    assert correr(tmp_path, Portal(docs))[0] == 0
    df = parquet(tmp_path)
    assert set(df.loc[df["_relacion"] == "trimestral", "_trimestre"]) == {"3"}
    assert "manda el contenido" in log(tmp_path)


# ----------------------------------------------------------------------------------------------------------------------
# Red: pausa, reintentos y bloqueo
# ----------------------------------------------------------------------------------------------------------------------

class _Reloj:
    def __init__(self):
        self.t = 0.0
        self.dormido = []

    def __call__(self):
        return self.t

    def dormir(self, s):
        self.dormido.append(round(s, 3))
        self.t += s


class _Sesion:
    def __init__(self, respuestas):
        self.headers = {}
        self.respuestas = list(respuestas)
        self.llamadas = 0

    def get(self, url, timeout=None):
        self.llamadas += 1
        r = self.respuestas.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def test_pausa_entre_peticiones_y_reintentos():
    import requests

    reloj = _Reloj()
    s = _Sesion([Respuesta(b"a", "u"), Respuesta(b"", "u", 503), requests.exceptions.ConnectionError("caída"),
                 Respuesta(b"b", "u")])
    c = M.Cliente(sesion=s, dormir=reloj.dormir, reloj=reloj)
    assert c.pedir("GET", "u").content == b"a"
    assert c.pedir("GET", "u").content == b"b"
    assert reloj.dormido == [M.PAUSA, M.ESPERAS[0], M.ESPERAS[1]] and c.peticiones == 4
    assert "User-Agent" in s.headers and s.headers["Accept-Language"].startswith("es")


def test_bloqueo_y_4xx_sin_reintento():
    for codigo, error in ((403, M.Bloqueo), (429, M.Bloqueo), (404, M.ErrorPeticion)):
        s = _Sesion([Respuesta(b"", "u", codigo)] * 3)
        c = M.Cliente(sesion=s, pausa=0, dormir=lambda x: None)
        with pytest.raises(error, match=f"HTTP {codigo}"):
            c.pedir("GET", "u")
        assert s.llamadas == 1


def test_5xx_agota_los_intentos():
    s = _Sesion([Respuesta(b"", "u", 502)] * M.INTENTOS)
    c = M.Cliente(sesion=s, pausa=0, dormir=lambda x: None)
    with pytest.raises(M.ErrorPeticion, match=r"HTTP 502 \(tras 3 intentos\)"):
        c.pedir("GET", "u")
    assert s.llamadas == M.INTENTOS


def test_sello_de_subida():
    assert M.sello_subida("DOC20190618100729CONTRATACION.pdf") == "2019-06-18T10:07:29"
    assert M.sello_subida("otro.xls") is None
    assert M.id_corto("x") == M.id_corto("x") and len(M.id_corto("x")) == 12
    assert date.fromisoformat(M.sello_subida("DOC20260803100856x.xls")[:10]) == date(2026, 8, 3)
