"""Tests offline de scripts/ccaa_aragon.py.

La API CKAN de opendata.aragon.es, el registro OCDS de Open Contracting y la
API de Zaragoza se simulan con un ``requests.get`` falso; los ficheros (CSV,
SpreadsheetML, JSONL.gz) se generan en el propio test.
"""

import gzip
import importlib.util
import json
import os
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_aragon.py"


def _cargar(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


A = _cargar("ccaa_aragon", SCRIPT)

BASE_MALA = "https://opendata.aragon.es/api/3/action/package_show"
BASE_BUENA = "https://opendata.aragon.es/ckan/api/3/action/package_show"
F = "https://opendata.aragon.es/ficheros"


def L(serie):
    """Valores de una columna con los nulos como None (pandas 3 los lee como NaN)."""
    return [None if pd.isna(v) else v for v in serie]


# ---------------------------------------------------------------------------
# Web simulada
# ---------------------------------------------------------------------------

class Respuesta:
    def __init__(self, estado=200, cuerpo=b"", url=None):
        self.status_code = estado
        # cuerpo: bytes, o lista de trozos (bytes) y excepciones a lanzar en orden
        self._trozos = cuerpo if isinstance(cuerpo, list) else [cuerpo]
        self.url = url

    @property
    def content(self):
        return b"".join(t for t in self._trozos if isinstance(t, bytes))

    @property
    def text(self):
        return self.content.decode("utf-8")

    def json(self):
        return json.loads(self.text)

    def iter_content(self, chunk_size=None):
        for trozo in self._trozos:
            if isinstance(trozo, BaseException):
                raise trozo
            yield trozo

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Web:
    """Rutas: (url, params) -> lista de respuestas (se consumen en orden; la
    última se repite). Lo que no está registrado responde 404 HTML."""

    def __init__(self):
        self.rutas = {}
        self.llamadas = []

    @staticmethod
    def _clave(url, params):
        return url, tuple(sorted((k, str(v)) for k, v in (params or {}).items()))

    def poner(self, url, *respuestas, params=None):
        self.rutas[self._clave(url, params)] = [
            r if isinstance(r, (Respuesta, BaseException)) else Respuesta(200, r, url) for r in respuestas]

    def paquete(self, dataset_id, recursos):
        cuerpo = json.dumps({"success": True, "result": {"name": dataset_id, "resources": recursos}}).encode()
        self.poner(BASE_BUENA, cuerpo, params={"id": dataset_id})

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.llamadas.append((url, dict(params or {})))
        lista = self.rutas.get(self._clave(url, params))
        if not lista:
            return Respuesta(404, b"<html><body>404 Not Found</body></html>", url)
        respuesta = lista.pop(0) if len(lista) > 1 else lista[0]
        if isinstance(respuesta, BaseException):
            raise respuesta
        return respuesta

    def pedidas(self, url):
        return sum(1 for u, _ in self.llamadas if u == url)


@pytest.fixture
def web(monkeypatch):
    fake = Web()
    esperas = []
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(A.time, "sleep", esperas.append)
    monkeypatch.setattr(A, "anio_en_curso", lambda: 2026)
    fake.esperas = esperas
    return fake


def _recurso(rid, nombre, formato, url, **extra):
    return {"id": rid, "name": nombre, "format": formato, "url": url, **extra}


CSV_2019 = (
    '"Expediente";"NIF";"Importe";"Código postal";"Observaciones"\r\n'
    '"0001/2019";"B00000001";"1.234,50";"05001";"NA"\r\n'
    '"0001/2019";"B00000001";"1.234,50";"05001";"NA"\r\n'
    '"0003/2019";"A0000000Z";"-";"22001";"Pago en €, año";"campo de más"\r\n'
).encode("cp1252")

SPREADSHEETML_2020 = """<?xml version="1.0" encoding="UTF-8"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
 <Worksheet ss:Name="Menores 2020">
  <Table>
   <Row><Cell ss:MergeAcross="3"><Data ss:Type="String">CONTRATOS MENORES 2020</Data></Cell></Row>
   <Row ss:Index="3">
    <Cell><Data ss:Type="String">Expediente</Data></Cell><Cell><Data ss:Type="String">NIF</Data></Cell>
    <Cell><Data ss:Type="String">Importe</Data></Cell><Cell><Data ss:Type="String">Código postal</Data></Cell>
    <Cell><Data ss:Type="String">Lote</Data></Cell>
   </Row>
   <Row>
    <Cell><Data ss:Type="String">0001/2020</Data></Cell><Cell><Data ss:Type="String">B00000001</Data></Cell>
    <Cell><Data ss:Type="Number">1234.50</Data></Cell><Cell><Data ss:Type="String">05001</Data></Cell>
    <Cell><Data ss:Type="String">01</Data></Cell>
   </Row>
   <Row>
    <Cell><Data ss:Type="String">0002/2020</Data></Cell>
    <Cell ss:Index="3"><Data ss:Type="Number">99</Data></Cell>
    <Cell><ss:Data ss:Type="String" xmlns="http://www.w3.org/TR/REC-html40"><B>44</B>002</ss:Data></Cell>
   </Row>
  </Table>
 </Worksheet>
</Workbook>
""".encode("utf-8")


def _web_basica(web, registro=b"Expediente;Importe\nR1;10\n"):
    web.paquete("registro-de-contratos-de-la-comunidad-autonoma-de-aragon-desde-2023", [
        _recurso("r-reg", "Registro de contratos desde 2023", "CSV", f"{F}/registro.csv")])
    web.paquete("contratos-gobierno-de-aragon", [
        _recurso("r-cm-2019-csv", "Contratos menores 2019", "CSV", f"{F}/cm2019.csv"),
        _recurso("r-cm-2019-xml", "Contratos menores 2019", "XLS", f"{F}/cm2019.xls.xml"),
        _recurso("r-cm-2020-xml", "Contratos menores del año 2020", "XLS", f"{F}/cm2020.xls.xml"),
        _recurso("r-ca-2009", "Contratos adjudicados 2009 (CSV)", "CSV", f"{F}/ca2009.csv"),
        _recurso("r-doc", "Descripción de los campos", "PDF", f"{F}/campos.pdf"),
    ])
    web.paquete("anuncios-del-perfil-del-contratante-del-gobierno-de-aragon", [])
    web.poner(f"{F}/registro.csv", registro)
    web.poner(f"{F}/cm2019.csv", CSV_2019)
    web.poner(f"{F}/cm2020.xls.xml", SPREADSHEETML_2020)
    web.poner(f"{F}/ca2009.csv", b"Expediente,Adjudicatario\n0009/2009,Empresa\n")


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

def test_salida_por_defecto_es_repo_aragon():
    assert A.DIR_SALIDA == REPO_ROOT / "aragon"


def test_datasets_confirmados():
    ids = [ds["id"] for ds in A.DATASETS]
    assert ids == [
        "registro-de-contratos-de-la-comunidad-autonoma-de-aragon-desde-2023",
        "contratos-gobierno-de-aragon",
        "anuncios-del-perfil-del-contratante-del-gobierno-de-aragon",
    ]
    assert "9c05a8a6-ef3f-4223-94a5-9669a6e5a48e" in A.DATASETS[1]["ids_alternativos"]


@pytest.mark.parametrize("nombre, serie, anio", [
    ("Contratos menores 2019", "contratos_menores", 2019),
    ("Contratos menores del año 2020 (CSV)", "contratos_menores", 2020),
    ("Contratos adjudicados 2009", "contratos_adjudicados", 2009),
    ("Registro de contratos desde 2023", "registro_de_contratos_desde", None),
    ("Contratos menores 2014-2025", "contratos_menores", None),
])
def test_serie_y_anio_del_nombre(nombre, serie, anio):
    assert A.serie_de(nombre) == serie
    assert A.anio_de(nombre) == anio


@pytest.mark.parametrize("recurso, formato", [
    ({"format": "CSV", "url": "x/a.csv"}, "csv"),
    ({"format": "XLS", "url": "x/a.xls.xml"}, "xml"),
    ({"format": "", "url": "https://opendata.aragon.es/GA_OD_Core/download?view_id=1&formato=json"}, "json"),
    ({"format": None, "url": "x/a.xlsx"}, "xlsx"),
    ({"format": "PDF", "url": "x/a.pdf"}, None),
])
def test_formato_recurso(recurso, formato):
    assert A.formato_recurso(recurso) == formato


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------

def test_spreadsheetml_indices_celdas_combinadas_y_titulo(tmp_path):
    ruta = tmp_path / "m.xls.xml"
    ruta.write_bytes(SPREADSHEETML_2020)
    assert A.tipo_contenido(ruta) == "spreadsheetml"
    df = A.leer_tabular(ruta)["datos"][0]
    assert list(df.columns[:5]) == ["Expediente", "NIF", "Importe", "Código postal", "Lote"]
    assert df["Expediente"].tolist() == ["0001/2020", "0002/2020"]
    assert df["NIF"].tolist() == ["B00000001", None]
    assert df["Importe"].tolist() == ["1234.50", "99"]          # texto tal cual
    assert df["Código postal"].tolist() == ["05001", "44002"]   # cero inicial y HTML dentro de Data
    assert df["Lote"].tolist() == ["01", None]
    assert df["_fila_origen"].tolist() == ["4", "5"]            # ss:Index="3" en la cabecera
    assert df["_encabezado"].tolist() == ["CONTRATOS MENORES 2020"] * 2
    assert df["_hoja"].tolist() == ["Menores 2020"] * 2


def test_csv_como_texto_sin_perder_filas_ni_campos(tmp_path):
    ruta = tmp_path / "c.csv"
    ruta.write_bytes(CSV_2019)
    df = A.leer_tabular(ruta)["datos"][0]
    assert len(df) == 3                                         # la fila repetida se conserva
    assert df["Código postal"].tolist() == ["05001", "05001", "22001"]
    assert df["Importe"].tolist() == ["1.234,50", "1.234,50", "-"]
    assert df["Observaciones"].tolist() == ["NA", "NA", "Pago en €, año"]
    assert df["Unnamed: 5"].tolist() == [None, None, "campo de más"]


def test_json_numeros_con_su_texto_y_listas(tmp_path):
    ruta = tmp_path / "d.json"
    ruta.write_text('{"data": [{"a": "007", "b": 1000.50, "c": {"d": 1}}, {"a": null, "e": [1, 2.0]}]}')
    df = A.leer_tabular(ruta)["datos"][0]
    primera = df.to_dict("records")[0]
    assert {k: v for k, v in primera.items() if k != "e"} == {"a": "007", "b": "1000.50", "c/d": "1", "_fila_origen": "1"}
    assert pd.isna(primera["e"])                                # clave ausente en ese registro
    assert df["a"].tolist()[1] is None                          # null del JSON
    assert df["e"].tolist()[1] == "[1,2.0]"


def test_excel_titulo_enteros_y_fechas(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    import datetime as dt
    libro = openpyxl.Workbook()
    hoja = libro.active
    hoja.append(["Relación de contratos menores"])
    hoja.append([])
    hoja.append(["Expediente", "Importe", "Fecha", "CP"])
    hoja.append(["0012", 1234.5, dt.datetime(2024, 1, 15), "02001"])
    hoja.append(["0013", 5.0, dt.datetime(2024, 1, 16, 10, 30), None])
    ruta = tmp_path / "x.xlsx"
    libro.save(ruta)
    df = A.leer_tabular(ruta)["datos"][0]
    assert df[["Expediente", "Importe", "Fecha", "CP"]].values.tolist() == [
        ["0012", "1234.5", "2024-01-15", "02001"],
        ["0013", "5", "2024-01-16 10:30:00", None],
    ]
    assert df["_encabezado"].iloc[0] == "Relación de contratos menores"


def test_excel_celdas_de_error_se_conservan_como_texto(tmp_path):
    """Una celda de error (#N/A, #DIV/0!) es un valor del fichero: no puede
    acabar como nulo (pandas.read_excel la convertía en NaN)."""
    openpyxl = pytest.importorskip("openpyxl")
    libro = openpyxl.Workbook()
    hoja = libro.active
    hoja.append(["Expediente", "Importe", "Adjudicatario"])
    hoja.append(["0001", "#N/A", "#DIV/0!"])
    hoja.append([])
    hoja.append(["0002", 7, "NA"])
    assert hoja["B2"].data_type == "e"
    ruta = tmp_path / "errores.xlsx"
    libro.save(ruta)
    df = A.leer_tabular(ruta)["datos"][0]
    assert df["Expediente"].tolist() == ["0001", "0002"]
    assert df["Importe"].tolist() == ["#N/A", "7"]
    assert df["Adjudicatario"].tolist() == ["#DIV/0!", "NA"]
    assert df["_fila_origen"].tolist() == ["2", "4"]


# .xls del Gobierno de Aragón (contratos, menores y encargos, 2018-2025): HTML en ISO-8859-1 declarado,
# con la cabecera en <TH> sueltas dentro de <TABLE>, fuera de todo <TR> (así llegan los 17 ficheros).
# El byte 0xA4 es el euro (ISO-8859-15) y 0xA0, el espacio duro de '&nbsp;' escrito tal cual.
CABECERA_GOBIERNO = ["Obj", "Órgano", "Procedimiento de adjudicación", "Tipo de contrato",
                     "Importe de licitación", "Importe de adjudicación", "Instrumento de publicación",
                     "Número de licitadores", "Identidad del adjudicatario", "Ejercicio", "Código Expediente", "M"]


def _html_gobierno(filas, cabecera=CABECERA_GOBIERNO, codificacion="latin-1"):
    ths = "".join(f"<TH>{c}</TH>\n" for c in cabecera)
    trs = "".join('<TR WIDTH="100%">\n'
                  + "".join(f'<TD VALIGN="TOP" NOWRAP>\n{v}\n \n\n</TD>\n' for v in fila)
                  + "\n</TR>\n" for fila in filas)
    texto = ('<HTML>\n<head>\n<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1"/>\n'
             '</head>\n<BODY>\n<TABLE BORDER="1" width="100%" CELLSPACING="1" CELLPADDING="1">\n'
             f"{ths}{trs}</TABLE>\n</BODY>\n</HTML>\n")
    return texto.encode(codificacion).replace("¤".encode(codificacion), b"\xa4")


FILAS_GOBIERNO_2025 = [
    ["Hannover\xa0Messe\xa0(31\xa0marzo)\xa02025", "Aragón Exterior, S.A.U. (AREX)", "Sin procedimiento",
     "Servicios", "17.810,00", "17.810,00", "Ninguno", "1", "DEUTSCHE\xa0MESSE\xa0AG", "2025", "CF2503/0010",
     "&nbsp;"],
    ["Canon: 1.028 ¤/año/máquina", "SERVICIO ARAGONÉS DE SALUD", "Abierto", "Suministros", "45.164,55",
     "44.290,70", "https://aplicaciones.aragon.es/pcon/pcon-public/", "&nbsp;", "BESTMEDIC SL", "2025",
     "CONEX20255200001325", "CONTRATO MENOR"],
]


def test_xls_html_cabecera_en_th_fuera_de_tr_no_pierde_la_primera_fila(tmp_path):
    """Antes la cabecera <TH> no se veía: la primera fila de datos hacía de cabecera (las columnas se
    llamaban 'Hannover Messe…', 'Aragón Exterior…') y ese contrato se perdía."""
    ruta = tmp_path / "gobierno_2025.xls"
    ruta.write_bytes(_html_gobierno(FILAS_GOBIERNO_2025))
    assert A.tipo_contenido(ruta) == "html"
    df = A.leer_tabular(ruta)["datos"][0]
    assert [c for c in df.columns if not c.startswith("_")] == CABECERA_GOBIERNO
    assert len(df) == 2
    assert df["Obj"].tolist() == ["Hannover\xa0Messe\xa0(31\xa0marzo)\xa02025", "Canon: 1.028 €/año/máquina"]
    assert df["Órgano"].tolist() == ["Aragón Exterior, S.A.U. (AREX)", "SERVICIO ARAGONÉS DE SALUD"]
    assert df["Identidad del adjudicatario"].tolist() == ["DEUTSCHE\xa0MESSE\xa0AG", "BESTMEDIC SL"]
    assert df["Código Expediente"].tolist() == ["CF2503/0010", "CONEX20255200001325"]
    assert L(df["Número de licitadores"]) == ["1", None]            # '&nbsp;' es una celda vacía
    assert L(df["M"]) == [None, "CONTRATO MENOR"]
    assert df["_fila_origen"].tolist() == ["2", "3"]                 # la cabecera es la fila 1
    assert "_encabezado" not in df.columns


def test_xls_html_celdas_sueltas_forman_filas_en_su_orden(tmp_path):
    """Las celdas fuera de <tr> seguidas son una fila, en su sitio del fichero: una cabecera suelta
    con colspan, filas <tr> y un pie suelto después."""
    html = ("<html><body><table>"
            "<th>A</th><th colspan='2'>B</th>"
            "<tr><td>1</td><td>2</td><td>3</td></tr>"
            "<tr><td>4</td><td></td><td>6</td></tr>"
            "<td>pie</td><td>7</td>"
            "</table></body></html>").encode()
    ruta = tmp_path / "t.xls"
    ruta.write_bytes(html)
    assert A.filas_html(ruta) == [("tabla1", [["A", "B", None], ["1", "2", "3"], ["4", None, "6"],
                                              ["pie", "7"]])]
    df = A.leer_tabular(ruta)["datos"][0]
    assert list(df.columns[:3]) == ["A", "B", "Unnamed: 2"]
    assert df.values[:, :3].tolist() == [["1", "2", "3"], ["4", None, "6"], ["pie", "7", None]]
    assert df["_fila_origen"].tolist() == ["2", "3", "4"]


def test_xls_html_codificacion_euro_0xa4_y_cp1252(tmp_path):
    """0xA4 es '€' (el Gobierno escribe el euro como en ISO-8859-15) y los bytes de cp1252 (0x80 '€',
    0x93 '“') también, aunque el fichero traiga uno que cp1252 no define (0x8D, de un 'Í' en UTF-8
    leído como latin-1): antes ese fichero entero se leía como latin-1 y 0x80 era el control U+0080."""
    datos = (b"<html><body><table><th>Obj</th><th>Importe</th>"
             b"<tr><td>35,00 \xa4/ Tn</td><td>14,88 \x80/ud</td></tr>"
             b"<tr><td>\x93Nueva edici\xf3n</td><td>VEH\xc3\x8dCULOS</td></tr></table></body></html>")
    ruta = tmp_path / "c.xls"
    ruta.write_bytes(datos)
    df = A.leer_tabular(ruta)["datos"][0]
    assert df["Obj"].tolist() == ["35,00 €/ Tn", "“Nueva edición"]
    assert df["Importe"].tolist() == ["14,88 €/ud", "VEHÃ\x8dCULOS"]
    # Un HTML en UTF-8 se lee como UTF-8 (el '¤' que traiga es un '¤')
    utf8 = tmp_path / "u.xls"
    utf8.write_bytes("<html><table><th>Obj</th><tr><td>Órgano ¤ €</td></tr></table></html>".encode("utf-8"))
    assert A.leer_tabular(utf8)["datos"][0]["Obj"].tolist() == ["Órgano ¤ €"]


def test_registro_marca_la_razon_social_que_es_un_codigo_de_pais(tmp_path):
    """El Registro publica en razon_social_adjudicatario el código de país en vez del nombre (50 filas de
    mayores y menores en sept. 2026, igual en su JSON): no hay lectura que lo arregle; se marca la fila
    (_razon_social_es_pais) sin tocar ningún valor. 'AST' y 'MAZ' son nombres (siglas), no países."""
    df = pd.DataFrame({
        "numero_de_expediente": ["E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"],
        "razon_social_adjudicatario": ["ES", " AT ", "ATU", "AST", "MAZ", "CR CONFECCIONES RAMOS SL", None,
                                       "ES", "NL"],
        "nif_adjudicatario": ["A28122125", "ATU65728938", "ATU65728938", "Q5000455E", "B99083404",
                              "B50349323", "B50349323", "ESPAÑA", None],
    }, dtype=object)
    marcado = A.marcar_razon_social_pais(df)
    assert marcado["_razon_social_es_pais"].tolist() == [True, True, True, False, False, False, False, True, True]
    assert marcado.drop(columns="_razon_social_es_pais").equals(df)          # ningún valor cambia
    otra = pd.DataFrame({"razon_social_cesionario": ["ES"]})
    assert list(A.marcar_razon_social_pais(otra).columns) == ["razon_social_cesionario"]


def test_main_gobierno_html_y_registro_con_la_marca(web, tmp_path):
    _web_basica(web, registro=("numero_de_expediente,razon_social_adjudicatario,nif_adjudicatario\r\n"
                               "ECU_SGT_2024_51,ES,A28122125\r\n"
                               "SERPA2-1123002622,AT,ATU65728938\r\n"
                               "HAP_SGT_2024_EMP5,AST,Q5000455E\r\n").encode("utf-8"))
    web.paquete("contratos-gobierno-de-aragon", [
        _recurso("r-cg-2025", "Contratos Gobierno de Aragón 2025", "XLS", f"{F}/cg2025.xls")])
    web.poner(f"{F}/cg2025.xls", _html_gobierno(FILAS_GOBIERNO_2025))
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    gobierno = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_gobierno_de_aragon.parquet")
    assert [c for c in gobierno.columns if not c.startswith("_")] == CABECERA_GOBIERNO
    assert gobierno["Código Expediente"].tolist() == ["CF2503/0010", "CONEX20255200001325"]
    assert "_razon_social_es_pais" not in gobierno.columns
    registro = pd.read_parquet(tmp_path / "registro_contratos__registro_de_contratos_desde.parquet")
    assert registro["razon_social_adjudicatario"].tolist() == ["ES", "AT", "AST"]
    assert registro["_razon_social_es_pais"].tolist() == [True, True, False]
    assert registro["_razon_social_es_pais"].dtype == bool
    columnas = list(registro.columns)
    assert columnas.index("_razon_social_es_pais") == columnas.index("_fuente") - 1   # antes de los metadatos


def test_gobierno_html_versiones_conservan_la_primera_fila_retirada(web, tmp_path):
    """Regla 3: el parquet se rehace con el código actual desde todas las versiones del .xls. Si el
    portal retira el primer contrato del fichero, sigue en la salida con _en_ultima_descarga=False (antes
    era la cabecera de esa versión y no estaba en ninguna)."""
    _web_basica(web)
    web.paquete("contratos-gobierno-de-aragon", [
        _recurso("r-cg-2025", "Contratos Gobierno de Aragón 2025", "XLS", f"{F}/cg2025.xls")])
    web.poner(f"{F}/cg2025.xls", _html_gobierno(FILAS_GOBIERNO_2025))
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    crudo = tmp_path / "raw" / "contratos_gobierno" / "r-cg-2025.xls"
    os.utime(crudo, (1_767_225_600, 1_767_225_600))            # 2026-01-01T00:00:00Z
    tercera = ["Obra nueva", "Departamento X", "Abierto", "Obras", "1,00", "1,00", "-", "2", "EMPRESA SL",
               "2025", "EXP-3", "&nbsp;"]
    web.poner(f"{F}/cg2025.xls", _html_gobierno(FILAS_GOBIERNO_2025[1:] + [tercera]))
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    df = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_gobierno_de_aragon.parquet")
    assert df[["Código Expediente", "_en_ultima_descarga"]].values.tolist() == [
        ["CF2503/0010", False], ["CONEX20255200001325", True], ["EXP-3", True]]
    assert df["_primera_descarga"].tolist()[:2] == ["2026-01-01T00:00:00Z"] * 2


# ---------------------------------------------------------------------------
# Descargas
# ---------------------------------------------------------------------------

def test_descargar_reintenta_con_backoff(web, tmp_path):
    url = f"{F}/x.csv"
    web.poner(url, Respuesta(503), Respuesta(200, [b"a;b\n", requests.exceptions.ConnectionError("corte")]),
              Respuesta(200, b"a;b\n1;2\n"))
    estado, tam = A.descargar(url, tmp_path / "x.csv", "csv")
    assert (estado, tam) == ("nuevo", 8)
    assert web.esperas == [2, 4]
    assert (tmp_path / "x.csv").read_bytes() == b"a;b\n1;2\n"
    assert not list(tmp_path.glob(".*"))                       # sin temporales


def test_descargar_url_http_prueba_primero_https(web, tmp_path):
    # El CKAN da enlaces http:// de servidores que sirven https (las series de
    # menores 2024-2025 del Gobierno): el proxy de la nube rechaza HTTP plano (403)
    http = "http://serviciosciudadano.aragon.es/cgi-bin/AODB/BRSCGI?CMD=VERLST&EJER=2025"
    web.poner(http, Respuesta(403, b"<html>Forbidden</html>"))
    web.poner("https" + http[4:], b"a;b\n1;2\n")
    assert A.descargar(http, tmp_path / "x.csv", "csv") == ("nuevo", 8)
    assert web.pedidas(http) == 0 and web.esperas == []
    # Si el servidor no sirve https, se usa el enlace publicado
    otra = "http://otro.aragon.es/x.csv"
    web.poner("https" + otra[4:], requests.exceptions.SSLError("sin https"))
    web.poner(otra, b"c;d\n3;4\n")
    assert A.descargar(otra, tmp_path / "y.csv", "csv") == ("nuevo", 8)
    assert (tmp_path / "y.csv").read_bytes() == b"c;d\n3;4\n"


def test_descarga_fallida_o_html_no_toca_la_copia_anterior(web, tmp_path):
    destino = tmp_path / "x.csv"
    destino.write_bytes(b"a;b\n1;2\n")
    web.poner(f"{F}/x.csv", Respuesta(200, b"<!DOCTYPE html><html><body>Mantenimiento</body></html>"))
    with pytest.raises(A.ErrorDescarga):
        A.descargar(f"{F}/x.csv", destino, "csv")
    with pytest.raises(A.ErrorDescarga) as error:
        A.descargar(f"{F}/no-existe.csv", destino, "csv")
    assert error.value.estado == 404
    assert web.esperas == []                                   # un 404 no se reintenta
    assert destino.read_bytes() == b"a;b\n1;2\n"
    assert not list(tmp_path.glob(".*"))


# ---------------------------------------------------------------------------
# Gobierno de Aragón (CKAN) de extremo a extremo
# ---------------------------------------------------------------------------

def test_main_descubre_recursos_elige_formato_y_une_columnas(web, tmp_path):
    _web_basica(web)
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0

    # La primera base candidata no responde: se prueba una vez y se usa la otra
    assert web.pedidas(BASE_MALA) == 1
    # El 2019 está en CSV y en SpreadsheetML: solo se descarga el CSV
    assert web.pedidas(f"{F}/cm2019.xls.xml") == 0
    assert web.pedidas(f"{F}/campos.pdf") == 0
    assert (tmp_path / "raw" / "contratos_gobierno" / "r-cm-2019-csv.csv").read_bytes() == CSV_2019
    assert (tmp_path / "raw" / "contratos_gobierno" / "r-cm-2020-xml.xls.xml").exists()

    df = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_menores.parquet")
    assert len(df) == 5
    columnas = list(df.columns)
    assert columnas[:7] == ["Expediente", "NIF", "Importe", "Código postal", "Observaciones",
                            "Unnamed: 5", "Lote"]                                    # unión de esquemas
    assert df["Código postal"].tolist() == ["05001", "05001", "22001", "05001", "44002"]
    assert df["_anio_recurso"].tolist() == ["2019"] * 3 + ["2020"] * 2
    assert df["_recurso_id"].tolist() == ["r-cm-2019-csv"] * 3 + ["r-cm-2020-xml"] * 2
    assert df["_formato"].tolist() == ["csv"] * 3 + ["xml"] * 2
    assert df["_archivo_origen"].iloc[0] == "raw/contratos_gobierno/r-cm-2019-csv.csv"
    assert df["_fuente"].iloc[0] == "https://opendata.aragon.es/datos/catalogo/dataset/contratos-gobierno-de-aragon"
    assert df["_en_ultima_descarga"].tolist() == [True] * 5
    assert set(A.METADATOS[:4]) <= set(columnas)
    assert all(pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object
               for c in columnas if c != "_en_ultima_descarga")

    adjudicados = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_adjudicados.parquet")
    assert adjudicados["Expediente"].tolist() == ["0009/2009"]
    registro = pd.read_parquet(tmp_path / "registro_contratos__registro_de_contratos_desde.parquet")
    assert L(registro["_anio_recurso"]) == [None]

    manifiesto = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    entrada = manifiesto["raw/contratos_gobierno/r-cm-2019-csv.csv"]
    assert entrada["estado"] == "publicado" and entrada["url"] == f"{F}/cm2019.csv"
    assert len(entrada["sha256"]) == 64 and entrada["fecha_descarga"]


def test_csv_caido_usa_el_spreadsheetml_del_mismo_anio(web, tmp_path):
    _web_basica(web)
    web.poner(f"{F}/cm2019.csv", Respuesta(404))
    web.poner(f"{F}/cm2019.xls.xml", SPREADSHEETML_2020)
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0      # aviso, no fallo
    df = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_menores.parquet")
    assert df["_recurso_id"].tolist() == ["r-cm-2019-xml"] * 2 + ["r-cm-2020-xml"] * 2


def test_anios_cerrados_no_se_vuelven_a_pedir_y_el_en_curso_si(web, tmp_path):
    _web_basica(web)
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    web.llamadas.clear()
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    assert web.pedidas(f"{F}/cm2019.csv") == 0
    assert web.pedidas(f"{F}/ca2009.csv") == 0
    assert web.pedidas(f"{F}/registro.csv") == 1                # acumulativo: se comprueba siempre
    web.llamadas.clear()
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza", "--comprobar-todo"]) == 0
    assert web.pedidas(f"{F}/cm2019.csv") == 1


def test_fallo_de_package_show_da_error_y_conserva_lo_anterior(web, tmp_path):
    _web_basica(web)
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    web.rutas.clear()                                          # el portal cae
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 1
    df = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_menores.parquet")
    assert len(df) == 5 and df["_en_ultima_descarga"].all()


# ---------------------------------------------------------------------------
# Histórico: registros retirados o modificados por la administración
# ---------------------------------------------------------------------------

def _registro(tmp_path):
    return pd.read_parquet(tmp_path / "registro_contratos__registro_de_contratos_desde.parquet")


def test_registro_retirado_y_modificado_se_conservan(web, tmp_path):
    _web_basica(web, registro=b"Expediente;Importe\nE1;100\nE2;200\nE3;300\n")
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    crudo = tmp_path / "raw" / "registro_contratos" / "r-reg.csv"
    os.utime(crudo, (1_767_225_600, 1_767_225_600))            # 2026-01-01T00:00:00Z

    # La administración retira E2 y cambia el importe de E3
    web.poner(f"{F}/registro.csv", b"Expediente;Importe\nE1;100\nE3;350\n")
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    df = _registro(tmp_path)
    assert df[["Expediente", "Importe", "_en_ultima_descarga"]].values.tolist() == [
        ["E1", "100", True], ["E2", "200", False], ["E3", "300", False], ["E3", "350", True]]
    assert df["_primera_descarga"].iloc[1] == "2026-01-01T00:00:00Z"
    # La versión anterior del fichero crudo sigue en _historico/
    historico = tmp_path / "raw" / "registro_contratos" / "_historico" / "r-reg__20260101T000000Z.csv"
    assert historico.read_bytes() == b"Expediente;Importe\nE1;100\nE2;200\nE3;300\n"

    # Descarga fallida: no se pierde nada y el script acaba con error
    web.poner(f"{F}/registro.csv", Respuesta(503))
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 1
    assert _registro(tmp_path)[["Expediente", "Importe", "_en_ultima_descarga"]].values.tolist() == \
        df[["Expediente", "Importe", "_en_ultima_descarga"]].values.tolist()

    # Descarga vacía (solo cabecera): no marca nada como retirado
    web.poner(f"{F}/registro.csv", b"Expediente;Importe\n")
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    vacia = _registro(tmp_path)
    assert vacia["_en_ultima_descarga"].tolist() == [True, False, False, True]


def test_recurso_que_el_portal_deja_de_listar_se_conserva(web, tmp_path):
    _web_basica(web)
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    web.paquete("contratos-gobierno-de-aragon", [
        _recurso("r-cm-2020-xml", "Contratos menores del año 2020", "XLS", f"{F}/cm2020.xls.xml")])
    assert A.main(["--salida", str(tmp_path), "--sin-zaragoza"]) == 0
    df = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_menores.parquet")
    assert len(df) == 5
    retirado = df["_recurso_id"] == "r-cm-2019-csv"
    assert not df.loc[retirado, "_en_ultima_descarga"].any()
    assert df.loc[~retirado, "_en_ultima_descarga"].all()
    adjudicados = pd.read_parquet(tmp_path / "contratos_gobierno__contratos_adjudicados.parquet")
    assert adjudicados["_en_ultima_descarga"].tolist() == [False]
    assert (tmp_path / "raw" / "contratos_gobierno" / "r-cm-2019-csv.csv").exists()


# ---------------------------------------------------------------------------
# Ayuntamiento de Zaragoza
# ---------------------------------------------------------------------------

PAGINA_OCP = """<html><body>
<a href="/en/publication/1/download?name=full.jsonl.gz">Completo</a>
<a href="/en/publication/1/download?name=2024.jsonl.gz">2024</a>
<a href='/en/publication/1/download?name=2025.jsonl.gz'>2025</a>
<a href="/en/publication/1/download?name=2025.csv.tar.gz">CSV</a>
</body></html>""".encode()
DESCARGA_OCP = "https://data.open-contracting.org/en/publication/1/download?name={}"

RELEASE_2024 = (
    '{"ocid": "ocds-1xraxc-001", "id": "001-1", "tag": ["award"], '
    '"tender": {"id": "T1", "value": {"amount": 1000.50, "currency": "EUR"}, "numberOfTenderers": 3}, '
    '"awards": [{"id": "A1", "value": {"amount": 900.10}, '
    '"suppliers": [{"id": "ES-B00000001", "name": "Empresa"}]}], '
    '"contracts": [{"id": "C1", "awardID": "A1"}], '
    '"parties": [{"id": "ES-B00000001", "name": "Empresa", "roles": ["supplier"]}]}'
)
PAQUETE_2025 = '{"uri": "x", "releases": [{"ocid": "ocds-1xraxc-002", "id": "002-1", "tender": {"value": {"amount": 7}}}]}'


def _web_zaragoza(web, linea_2025=PAQUETE_2025):
    web.poner(A.ZARAGOZA_OCDS_PUBLICACION, Respuesta(200, PAGINA_OCP, A.ZARAGOZA_OCDS_PUBLICACION))
    web.poner(DESCARGA_OCP.format("2024.jsonl.gz"), gzip.compress((RELEASE_2024 + "\n").encode()))
    web.poner(DESCARGA_OCP.format("2025.jsonl.gz"), gzip.compress((linea_2025 + "\n\n").encode()))


def test_zaragoza_ocds_descarga_anuales_y_aplana(web, tmp_path):
    _web_basica(web)
    _web_zaragoza(web)
    assert A.main(["--salida", str(tmp_path)]) == 0
    assert web.pedidas(DESCARGA_OCP.format("full.jsonl.gz")) == 0      # los anuales ya son el total
    crudo = tmp_path / "raw" / "zaragoza_ocds" / "2024.jsonl.gz"
    assert gzip.decompress(crudo.read_bytes()).decode() == RELEASE_2024 + "\n"   # tal cual

    releases = pd.read_parquet(tmp_path / "zaragoza_ocds_releases.parquet")
    assert releases["ocid"].tolist() == ["ocds-1xraxc-001", "ocds-1xraxc-002"]
    assert releases["tender/value/amount"].tolist() == ["1000.50", "7"]
    assert L(releases["tag"]) == ['["award"]', None]
    assert "awards" not in releases.columns
    awards = pd.read_parquet(tmp_path / "zaragoza_ocds_awards.parquet")
    assert awards[["ocid", "release_id", "id", "value/amount"]].values.tolist() == [
        ["ocds-1xraxc-001", "001-1", "A1", "900.10"]]
    assert json.loads(awards["suppliers"].iloc[0]) == [{"id": "ES-B00000001", "name": "Empresa"}]
    contracts = pd.read_parquet(tmp_path / "zaragoza_ocds_contracts.parquet")
    assert contracts["awardID"].tolist() == ["A1"]
    parties = pd.read_parquet(tmp_path / "zaragoza_ocds_parties.parquet")
    assert parties["roles"].tolist() == ['["supplier"]']
    assert parties["_url_origen"].iloc[0] == DESCARGA_OCP.format("2024.jsonl.gz")

    # Segunda ejecución: 2024 está cerrado; 2025 se comprueba y cambia
    web.llamadas.clear()
    nuevo = PAQUETE_2025.replace('"amount": 7', '"amount": 8')
    web.poner(DESCARGA_OCP.format("2025.jsonl.gz"), gzip.compress((nuevo + "\n").encode()))
    assert A.main(["--salida", str(tmp_path)]) == 0
    assert web.pedidas(DESCARGA_OCP.format("2024.jsonl.gz")) == 0
    assert web.pedidas(DESCARGA_OCP.format("2025.jsonl.gz")) == 1
    releases = pd.read_parquet(tmp_path / "zaragoza_ocds_releases.parquet")
    assert releases[["tender/value/amount", "_en_ultima_descarga"]].values.tolist() == [
        ["1000.50", True], ["7", False], ["8", True]]
    assert len(list((tmp_path / "raw" / "zaragoza_ocds" / "_historico").iterdir())) == 1


def test_zaragoza_ocds_pagina_caida_es_un_fallo(web, tmp_path):
    _web_basica(web)
    assert A.main(["--salida", str(tmp_path)]) == 1


def test_zaragoza_api_opcional_guarda_todas_las_paginas(web, tmp_path, monkeypatch):
    _web_basica(web)
    _web_zaragoza(web)
    monkeypatch.setattr(A, "FILAS_POR_PAGINA_API", 2)
    api = A.ZARAGOZA_API_CONTRATOS
    web.poner(api, json.dumps({"totalCount": 3, "result": [{"id": 1, "importe": "0100"}, {"id": 2}]}).encode(),
              params={"rows": 2, "start": 0})
    web.poner(api, json.dumps({"totalCount": 3, "result": [{"id": 3, "entidad": {"nif": "P5030300G"}}]}).encode(),
              params={"rows": 2, "start": 2})
    assert A.main(["--salida", str(tmp_path), "--zaragoza-api"]) == 0
    guardado = json.loads((tmp_path / "raw" / "zaragoza_api" / "contrato.json").read_text(encoding="utf-8"))
    assert len(guardado) == 2
    df = pd.read_parquet(tmp_path / "zaragoza_api_contratos.parquet")
    assert df["id"].tolist() == ["1", "2", "3"]
    assert L(df["importe"]) == ["0100", None, None]
    assert L(df["entidad/nif"]) == [None, None, "P5030300G"]
