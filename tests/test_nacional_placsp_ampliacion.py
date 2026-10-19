"""Tests de la ampliación del CODICE de la PLACSP (nacional/licitaciones.py): las tablas _ampliado,
_condiciones, _anuncios y _documentos, con lo que el ATOM publica y las tablas anteriores no leían.

Casi todo sobre entradas reales de los ZIP de la PLACSP (tests/fixtures/entry_*_real.xml, con su
procedencia en la cabecera): campos presentes, ausentes, vacíos y mal escritos. Y lo que no puede
cambiar: la tabla principal y las de detalle de antes salen idénticas byte a byte, también si leer la
ampliación falla.
"""

import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from nacional import licitaciones as lic

FIXTURES = Path(__file__).parent / "fixtures"
REALES = ["entry_ampliacion_real.xml", "entry_garantias_real.xml", "entry_limite_lotes_real.xml",
          "entry_solvencia_lote_real.xml", "entry_subcontratacion_real.xml", "entry_menor_real.xml",
          "entry_agregacion_real.xml", "entry_encargo_real.xml", "entry_cpm_real.xml"]
# Tablas que había antes de la ampliación (no pueden cambiar)
ANTERIORES = ("principal", "resultados", "adjudicatarios", "lotes", "criterios", "modificaciones", "borrados")
NS_FEED = ('xmlns="http://www.w3.org/2005/Atom" '
           'xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2" '
           'xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2" '
           'xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2" '
           'xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2"')


def _texto(nombre):
    """Texto de la entrada de un fixture, sin la cabecera ni las declaraciones de namespace."""
    texto = (FIXTURES / nombre).read_text(encoding="utf-8")
    texto = re.sub(r"<!--.*?-->\s*", "", texto, flags=re.S)
    return re.sub(r"<entry\s[^>]*>", "<entry>", texto, count=1)


def _parsear(texto):
    return lic.parsear_entry(ET.fromstring(texto.replace("<entry>", f"<entry {NS_FEED}>", 1)))


def _entry(nombre):
    return _parsear(_texto(nombre))


def _cambiar(nombre, viejo, nuevo):
    """La entrada real de un fixture con un cambio (el texto `viejo` tiene que estar una sola vez)."""
    texto = _texto(nombre)
    assert texto.count(viejo) == 1, viejo
    return texto.replace(viejo, nuevo)


def _zip(ruta, entradas):
    feed = f'<feed {NS_FEED}>' + "".join(entradas) + "</feed>"
    with zipfile.ZipFile(ruta, "w") as zf:
        zf.writestr("a.atom", feed)
    return ruta


def _sin(fila, *quitar):
    return {k: v for k, v in fila.items() if v is not None and k not in quitar}


# ─────────────────────────────────────────────────────────────
# _ampliado: una fila por entrada
# ─────────────────────────────────────────────────────────────

class TestAmpliado:
    def test_una_fila_por_entrada_con_las_columnas_en_orden(self):
        for nombre in REALES:
            filas = _entry(nombre)["_ampliado"]
            assert len(filas) == 1, nombre
            assert list(filas[0]) == lic.COLUMNAS_AMPLIADO, nombre

    def test_recursos_prorroga_subcontratacion_y_organo(self):
        a = _entry("entry_ampliacion_real.xml")["_ampliado"][0]
        assert _sin(a) == {
            "prorroga_periodo": "Periodo máximo de dos años más.",
            "num_recursos": 1,
            "subcontratacion_descripcion": (
                "Empresa adjudic. solo podrá subcontr. empresas especiali. venta y suminist productos, utensilios "
                "limpieza, tanto para utiliz. por serv. de limp. contratado como materiales uso exclusivo personal "
                "CEGC."),
            "tipo_organo_code": "3",
            "actividad_organo_code": "22",
        }

    def test_periodo_del_al_y_limites_de_lotes(self):
        a = _entry("entry_limite_lotes_real.xml")["_ampliado"][0]
        assert _sin(a) == {
            "fecha_inicio_ejecucion": "2019-03-01", "fecha_fin_ejecucion": "2019-11-29",
            "presentacion_lotes_code": "3", "presentacion_lotes": "A uno o varios lotes",
            "max_lotes_oferta": 2, "max_lotes_adjudicatario": 2, "tipo_organo_code": "1",
        }

    def test_prorroga_con_descripcion_y_periodo_y_porcentaje_de_subcontratacion(self):
        a = _entry("entry_subcontratacion_real.xml")["_ampliado"][0]
        assert a["prorroga_descripcion"] == "Posibilidade de dúas prórrogas de un ano cada unha"
        assert a["subcontratacion_porcentaje"] == 60.0
        assert a["subcontratacion_descripcion"] == ("Segundo a cláusula 27.b) do prego de cláusulas "
                                                    "administrativas particulares")
        assert a["variantes"] is False and a["subasta_electronica"] is None   # sin AuctionTerms (2013)

    def test_subasta_y_variantes(self):
        a = _entry("entry_solvencia_lote_real.xml")["_ampliado"][0]
        assert a["subasta_electronica"] is False and a["variantes"] is False

    def test_menor_y_agregacion(self):
        menor = _entry("entry_menor_real.xml")["_ampliado"][0]
        assert _sin(menor) == {"fecha_inicio_ejecucion": "2025-11-28", "fecha_fin_ejecucion": "2025-12-26",
                               "tipo_organo_code": "5", "actividad_organo_code": "1"}
        # La agregación no publica casi nada de esto: los campos ausentes quedan nulos
        assert _sin(_entry("entry_agregacion_real.xml")["_ampliado"][0]) == {"subasta_electronica": False}

    def test_varias_actividades_del_organo(self):
        texto = _cambiar("entry_menor_real.xml", "<cbc:ActivityCode",
                         '<cbc:ActivityCode>14</cbc:ActivityCode><cbc:ActivityCode')
        assert _parsear(texto)["_ampliado"][0]["actividad_organo_code"] == "14 | 1"


# ─────────────────────────────────────────────────────────────
# _condiciones: garantías, solvencia, clasificación, requisitos y condiciones de ejecución
# ─────────────────────────────────────────────────────────────

class TestCondiciones:
    def test_licitacion_con_todo(self):
        filas = _entry("entry_ampliacion_real.xml")["_condiciones"]
        assert [(f["lote"], f["grupo"], f["orden"], f["orden_solicitud"], f["codigo"]) for f in filas] == [
            (None, "garantia", 1, None, "2"),
            (None, "clasificacion", 2, 1, "U1-1"),
            (None, "solvencia_economica", 3, 1, "5"),
            (None, "solvencia_tecnica", 4, 1, "OSR-COMPTASK"),
            (None, "requisito", 5, 1, "4"), (None, "requisito", 6, 1, "9"), (None, "requisito", 7, 1, "5"),
            (None, "requisito", 8, 1, "3"), (None, "requisito", 9, 1, "1"), (None, "requisito", 10, 1, "2"),
            (None, "condicion_ejecucion", 11, None, "2"), (None, "condicion_ejecucion", 12, None, "1"),
        ]
        garantia, clasificacion, economica, tecnica = filas[:4]
        assert (garantia["porcentaje"], garantia["importe"], garantia["umbral"]) == (5.0, None, None)
        assert clasificacion["nombre"] == "RequiredBusinessProfileCode"
        assert economica["umbral"] == 189000.0 and economica["descripcion"].startswith("Volumen anual de negocios")
        assert tecnica["umbral"] == 126000.0
        assert filas[4]["descripcion"] == "Cumplimiento con las obligaciones con la Seguridad Social"
        assert filas[10]["nombre"] == "Consideraciones de tipo social"
        assert filas[11]["nombre"] == "Consideraciones de tipo medioambiental"
        assert filas[11]["descripcion"].startswith("Los productos de limpieza utilizados deberán ser altamente")
        assert not any("_textos_originales" in f for f in filas)

    def test_garantia_provisional_con_importe_y_definitiva_con_porcentaje(self):
        filas = _entry("entry_garantias_real.xml")["_condiciones"]
        assert [_sin(f, "descripcion") for f in filas] == [
            {"grupo": "garantia", "orden": 1, "codigo": "1", "importe": 6000.0},
            {"grupo": "garantia", "orden": 2, "codigo": "2", "porcentaje": 2.0},
            {"grupo": "solvencia_descripcion", "orden": 3, "orden_solicitud": 1},
            {"grupo": "requisito", "orden": 4, "orden_solicitud": 1, "codigo": "2"},
        ]
        assert filas[2]["descripcion"] == "Los especificados en Los Pliegos de Prescripciones Técncias."
        assert filas[3]["descripcion"] is None   # requisito sin descripción

    def test_solvencia_de_un_lote(self):
        filas = _entry("entry_solvencia_lote_real.xml")["_condiciones"]
        assert [(f["lote"], f["grupo"], f["orden"], f["codigo"]) for f in filas] == [
            (None, "garantia", 1, "2"), (None, "solvencia_economica", 2, "3"), (None, "requisito", 3, "1"),
            ("1", "solvencia_tecnica", 1, "ZZZ")]
        assert filas[3]["descripcion"].startswith("Acreditación de disponer de una estación de servicio")
        # El lote casa con el de _lotes
        assert "1" in {lote["lote"] for lote in _entry("entry_solvencia_lote_real.xml")["_lotes"]}

    def test_sin_terminos_ni_lotes(self):
        assert _entry("entry_agregacion_real.xml")["_condiciones"] == []
        assert _entry("entry_menor_real.xml")["_condiciones"] == []
        assert lic.parsear_condiciones(None) == []

    def test_clasificacion_sin_categorias_y_plantilla_y_anos(self):
        # Variantes del esquema que no salen en los fixtures: un grupo de clasificación sin categorías y
        # los años de actividad y la plantilla mínima del TendererQualificationRequest
        texto = _cambiar("entry_garantias_real.xml", "<cac:TendererQualificationRequest>",
                         "<cac:TendererQualificationRequest><cac:RequiredBusinessClassificationScheme>"
                         "<cbc:ID>C</cbc:ID></cac:RequiredBusinessClassificationScheme>"
                         "<cbc:EmployeeQuantity>12</cbc:EmployeeQuantity>"
                         "<cbc:OperatingYearsDescription>Tres años</cbc:OperatingYearsDescription>")
        filas = _parsear(texto)["_condiciones"]
        assert [_sin(f, "descripcion") for f in filas[2:6]] == [
            {"grupo": "solvencia_descripcion", "orden": 3, "orden_solicitud": 1},
            {"grupo": "plantilla_minima", "orden": 4, "orden_solicitud": 1, "umbral": 12.0},
            {"grupo": "anos_actividad", "orden": 5, "orden_solicitud": 1},
            {"grupo": "clasificacion", "orden": 6, "orden_solicitud": 1, "nombre": "C"},
        ]
        assert filas[4]["descripcion"] == "Tres años"


# ─────────────────────────────────────────────────────────────
# _anuncios: todos los anuncios, con su tipo, medio, fecha y documento
# ─────────────────────────────────────────────────────────────

def _clave_anuncio(f):
    return (f["orden_anuncio"], f["tipo_anuncio_code"], f["orden_publicacion"], f["medio_publicacion"],
            f["orden_referencia"], f["fecha_publicacion"], f["documento_tipo_code"])


class TestAnuncios:
    def test_una_fila_por_referencia_con_el_documento_de_la_adjudicacion(self):
        filas = _entry("entry_ampliacion_real.xml")["_anuncios"]
        perfil = "Perfil del Contratante"
        assert [_clave_anuncio(f) for f in filas] == [
            (1, "DOC_CN", 1, "DOUE", 1, "2022-09-12", None),
            (2, "DOC_CAN_ADJ", 1, perfil, 1, "2022-12-01", None),
            (2, "DOC_CAN_ADJ", 1, perfil, 2, None, "ACTA_ADJ"),
            (3, "DOC_CD", 1, perfil, 1, "2022-09-09", None),
            (4, "DOC_CN", 1, perfil, 1, "2022-09-09", None),
            (5, "DOC_FORM", 1, perfil, 1, "2022-12-31", None),
            (5, "DOC_FORM", 1, perfil, 2, None, "ACTA_FORM"),
        ]
        resolucion = filas[2]
        assert resolucion["documento_tipo"] == "Documento de Acta de Adjudicación"
        assert resolucion["documento_nombre"] == "20221130_DECRETO 2022-0181_ADJUDIC_limpieza_CEGC_2021-10.pdf"
        assert resolucion["documento_url"].startswith("https://contrataciondelestado.es/")
        # El envío al DOUE es del medio
        assert (filas[0]["envio_agencia"], filas[0]["envio_fecha"], filas[0]["envio_hora"]) == (
            "DOUE", "2022-09-07+02:00", None)
        assert all(f["envio_agencia"] is None for f in filas[1:])

    def test_un_medio_sin_referencias_no_se_pierde(self):
        filas = _entry("entry_garantias_real.xml")["_anuncios"]
        assert _clave_anuncio(filas[0]) == (1, "DOC_CN", 1, "DOUE", None, None, None)
        assert len(filas) == 9

    def test_varios_medios_con_su_envio(self):
        filas = _entry("entry_limite_lotes_real.xml")["_anuncios"]
        assert [(f["orden_anuncio"], f["medio_publicacion"], f["envio_agencia"], f["envio_fecha"],
                 f["fecha_publicacion"]) for f in filas[:5]] == [
            (1, "BOE", None, None, "2018-12-29"), (1, "BOE", None, None, "2018-12-26"),
            (1, "DOUE", "DOUE", "2018-12-26+01:00", "2018-12-28"),
            (2, "BOE", "BOE", "2019-05-21+02:00", "2019-05-25"),
            (2, "DOUE", "DOUE", "2019-05-21+02:00", "2019-05-22")]

    def test_varios_envios_de_un_medio(self):
        # 159.236 medios de los ZIP tienen más de una solicitud de publicación: van todas, en orden
        texto = _cambiar("entry_limite_lotes_real.xml", "<cbc-place-ext:SendDate>2018-12-26+01:00</cbc-place-ext:SendDate>",
                         "<cbc-place-ext:SendDate>2018-12-26+01:00</cbc-place-ext:SendDate>"
                         "</cac-place-ext:AdditionalPublicationRequest><cac-place-ext:AdditionalPublicationRequest>"
                         "<cbc:AgencyID>DOUE</cbc:AgencyID><cbc-place-ext:SendDate>2018-12-27+01:00</cbc-place-ext:SendDate>"
                         "<cbc-place-ext:SendTime>10:00:00</cbc-place-ext:SendTime>")
        fila = _parsear(texto)["_anuncios"][2]
        assert (fila["envio_agencia"], fila["envio_fecha"], fila["envio_hora"]) == (
            "DOUE | DOUE", "2018-12-26+01:00 | 2018-12-27+01:00", " | 10:00:00")

    def test_un_anuncio_sin_medios(self):
        texto = _cambiar("entry_menor_real.xml", "</cac-place-ext:ValidNoticeInfo>",
                         "</cac-place-ext:ValidNoticeInfo><cac-place-ext:ValidNoticeInfo>"
                         "<cbc-place-ext:NoticeTypeCode>DOC_MOD</cbc-place-ext:NoticeTypeCode>"
                         "</cac-place-ext:ValidNoticeInfo>")
        filas = _parsear(texto)["_anuncios"]
        assert _clave_anuncio(filas[-1]) == (2, "DOC_MOD", None, None, None, None, None)

    @pytest.mark.parametrize("nombre", REALES + ["entry_detalle.xml", "entry_lotes.xml"])
    def test_la_fecha_de_publicacion_de_la_principal_sale_de_los_anuncios(self, nombre):
        r = _entry(nombre)
        fechas = [(f["tipo_anuncio_code"], f["fecha_publicacion"]) for f in r["_anuncios"] if f["fecha_publicacion"]]
        licitacion = [f for t, f in fechas if t == "DOC_CN"]
        esperada = min(licitacion or [f for _, f in fechas], key=lambda f: f[:10], default=None)
        assert r["fecha_publicacion"] == (esperada[:10] if esperada else None)


# ─────────────────────────────────────────────────────────────
# _documentos: el DocumentTypeCode y el hash de cada documento
# ─────────────────────────────────────────────────────────────

def _con_nombre(r, elemento):
    """Filas de _documentos de un elemento con el nombre y la URL de su posición en la principal."""
    nombres = (r[elemento] or "").split(" | ")
    urls = (r[f"{elemento}_url"] or "").split(" | ")
    return [{**d, "nombre": nombres[d["orden"] - 1], "url": urls[d["orden"] - 1]}
            for d in r["_documentos"] if d["elemento"] == elemento]


class TestDocumentos:
    @pytest.mark.parametrize("nombre", REALES + ["entry_detalle.xml"])
    def test_cada_fila_es_el_documento_de_su_posicion_en_la_principal(self, nombre):
        r = _entry(nombre)
        raiz = ET.fromstring(_texto(nombre).replace("<entry>", f"<entry {NS_FEED}>", 1))
        status = next(h for h in raiz if h.tag.endswith("Status"))
        for elemento, xpath in lic.ELEMENTOS_DOCUMENTO:
            docs = [d for d in r["_documentos"] if d["elemento"] == elemento]
            assert [d["orden"] for d in docs] == list(range(1, len(docs) + 1))
            # Tantas filas como posiciones en las listas de la principal, con los mismos xpath
            assert len(docs) == len(status.findall(xpath, lic.NS))
            if docs:
                assert len(r[f"{elemento}_url"].split(" | ")) == len(docs), (nombre, elemento)
            else:
                assert r[elemento] is None and r[f"{elemento}_url"] is None

    def test_tipo_de_los_documentos_generales_y_hash_de_los_pliegos(self):
        r = _entry("entry_ampliacion_real.xml")
        docs = r["_documentos"]
        assert len(docs) == 32
        assert set(docs[0]) == {"elemento", "orden", "id_documento", "tipo_documento_code", "tipo_documento", "hash"}
        pcap = _con_nombre(r, "pliego_administrativo")[0]
        assert (pcap["nombre"], pcap["hash"]) == ("20220420_PCAP_servicio_LIMPIEZA.pdf", "WLaogUalpXY/96pvoVlFolrx8UE=")
        assert pcap["id_documento"] is None and pcap["tipo_documento_code"] is None
        generales = _con_nombre(r, "documentos_generales")
        assert len(generales) == 25
        assert (generales[0]["nombre"], generales[0]["tipo_documento_code"], generales[0]["id_documento"]) == (
            "Acta del órgano de asistencia", "12", "9d132b99-a0fd-4937-8504-d784a7833e48")
        assert generales[0]["url"].endswith("DocumentIdParam=9d132b99-a0fd-4937-8504-d784a7833e48")
        assert {(d["nombre"], d["tipo_documento_code"]) for d in generales} >= {
            ("Memoria justificativa", "9"), ("Informe de insuficiencia de medios", "10"),
            ("Documento de aprobación del expediente", "11"), ("Acuerdo de iniciación del expediente", "8"),
            ("Suspensión del procedimiento", "4"), ("Información subrogación", "ZZZ")}
        assert all(d["hash"] is None and d["tipo_documento"] is None for d in generales)

    def test_encargo_y_consulta(self):
        encargo = _entry("entry_encargo_real.xml")["_documentos"]
        assert [(d["elemento"], d["tipo_documento_code"]) for d in encargo] == [("documentos_generales", "1")]
        consulta = _con_nombre(_entry("entry_cpm_real.xml"), "documentos_generales")
        assert [(d["nombre"], d["tipo_documento_code"], d["id_documento"]) for d in consulta] == [
            ("Documento de información detallada de la consulta", "2", "c0f144cc-6b35-44a2-ba20-427dfaaae1e1")]

    def test_sin_documentos(self):
        assert _entry("entry_menor_real.xml")["_documentos"] == []


# ─────────────────────────────────────────────────────────────
# Campos vacíos y mal escritos: nulos, con su texto en textos_originales de la fila
# ─────────────────────────────────────────────────────────────

class TestVaciosYMalEscritos:
    def test_elementos_vacios_quedan_nulos_sin_textos(self):
        texto = _cambiar("entry_limite_lotes_real.xml", "<cbc:StartDate>2019-03-01</cbc:StartDate>",
                         "<cbc:StartDate/>")
        texto = texto.replace("<cbc:MaximumLotPresentationQuantity>2</cbc:MaximumLotPresentationQuantity>",
                              "<cbc:MaximumLotPresentationQuantity> </cbc:MaximumLotPresentationQuantity>")
        r = _parsear(texto)
        a = r["_ampliado"][0]
        assert a["fecha_inicio_ejecucion"] is None and a["max_lotes_oferta"] is None
        assert a["fecha_fin_ejecucion"] == "2019-11-29"
        assert "_textos_originales" not in a
        texto = _cambiar("entry_garantias_real.xml", "<cbc:AmountRate>2</cbc:AmountRate>", "<cbc:AmountRate></cbc:AmountRate>")
        garantia = _parsear(texto)["_condiciones"][1]
        assert garantia["porcentaje"] is None and "_textos_originales" not in garantia

    def test_textos_que_no_son_numero_ni_booleano_se_conservan(self):
        texto = _cambiar("entry_garantias_real.xml", "<cbc:AmountRate>2</cbc:AmountRate>",
                         "<cbc:AmountRate>5,5</cbc:AmountRate>")
        texto = texto.replace("<cbc:VariantConstraintIndicator>false</cbc:VariantConstraintIndicator>",
                              "<cbc:VariantConstraintIndicator>no</cbc:VariantConstraintIndicator>")
        r = _parsear(texto)
        assert r["_condiciones"][1]["porcentaje"] is None
        assert r["_condiciones"][1]["_textos_originales"] == {"porcentaje": "5,5"}
        assert r["_ampliado"][0]["variantes"] is None
        assert r["_ampliado"][0]["_textos_originales"] == {"variantes": "no"}
        # Nada de esto toca los textos_originales de la principal
        assert "_textos_originales" not in r

    @pytest.mark.parametrize("nombre, viejo, nuevo, tabla, fila, campo", [
        ("entry_ampliacion_real.xml", "<cbc:ReceivedAppealQuantity>1<", "<cbc:ReceivedAppealQuantity>uno<",
         "_ampliado", 0, "num_recursos"),
        ("entry_subcontratacion_real.xml", "<cbc:Rate>60<", "<cbc:Rate>60%<", "_ampliado", 0,
         "subcontratacion_porcentaje"),
        ("entry_limite_lotes_real.xml", "<cbc:MaximumLotPresentationQuantity>2<",
         "<cbc:MaximumLotPresentationQuantity>2.0<", "_ampliado", 0, "max_lotes_oferta"),
        ("entry_limite_lotes_real.xml", "<cbc:MaximumTendererAwardedLotsQuantity>2<",
         "<cbc:MaximumTendererAwardedLotsQuantity>dos<", "_ampliado", 0, "max_lotes_adjudicatario"),
        ("entry_solvencia_lote_real.xml", "<cbc:AuctionConstraintIndicator>false<",
         "<cbc:AuctionConstraintIndicator>NO<", "_ampliado", 0, "subasta_electronica"),
        ("entry_garantias_real.xml", '<cbc:LiabilityAmount currencyID="EUR">6000<',
         '<cbc:LiabilityAmount currencyID="EUR">6.000 €<', "_condiciones", 0, "importe"),
        ("entry_ampliacion_real.xml", "<cbc:ThresholdQuantity>189000<", "<cbc:ThresholdQuantity>189.000,00<",
         "_condiciones", 2, "umbral"),
    ])
    def test_cada_numero_o_booleano_mal_escrito_conserva_su_texto(self, nombre, viejo, nuevo, tabla, fila, campo):
        bien = _entry(nombre)[tabla][fila]
        mal = _parsear(_cambiar(nombre, viejo, nuevo))[tabla][fila]
        assert bien[campo] is not None and "_textos_originales" not in bien
        texto = nuevo[nuevo.index(">") + 1:-1]
        assert mal[campo] is None and mal["_textos_originales"] == {campo: texto}

    def test_un_error_al_leer_la_ampliacion_no_descarta_la_entrada(self, monkeypatch):
        bien = _entry("entry_ampliacion_real.xml")

        def falla(status):
            raise ValueError("estructura inesperada")
        monkeypatch.setattr(lic, "parsear_anuncios", falla)
        mal = _entry("entry_ampliacion_real.xml")
        assert mal["_ampliado"] == [{**dict.fromkeys(lic.COLUMNAS_AMPLIADO),
                                     "_textos_originales": {"error_ampliacion": "ValueError: estructura inesperada"}}]
        assert mal["_condiciones"] == mal["_anuncios"] == mal["_documentos"] == []
        quitar = ("_ampliado", "_condiciones", "_anuncios", "_documentos")
        assert {k: v for k, v in mal.items() if k not in quitar} == {k: v for k, v in bien.items() if k not in quitar}


# ─────────────────────────────────────────────────────────────
# Exportación: tablas nuevas y las anteriores, idénticas
# ─────────────────────────────────────────────────────────────

def _entradas_de_prueba():
    """Las entradas reales y tres variantes que cambian el tipo de las columnas según la parte: fecha
    mal escrita, número mal escrito y una entrada sin nada de la ampliación."""
    entradas = [_texto(n) for n in REALES + ["entry_detalle.xml", "entry_lotes.xml"]]
    entradas.append(_cambiar("entry_limite_lotes_real.xml", "<cbc:StartDate>2019-03-01</cbc:StartDate>",
                             "<cbc:StartDate>2019-02-30</cbc:StartDate>")
                    .replace("<updated>2019-01-02T08:56:42.228+01:00</updated>",
                             "<updated>2019-01-03T08:56:42.228+01:00</updated>"))
    entradas.append(_cambiar("entry_garantias_real.xml", "<cbc:AmountRate>2</cbc:AmountRate>",
                             "<cbc:AmountRate>5,5</cbc:AmountRate>")
                    .replace("<updated>2013-01-11T08:05:42.970+01:00</updated>",
                             "<updated>2013-01-12T08:05:42.970+01:00</updated>"))
    return entradas


def _exportar(tmp_path, nombre, monkeypatch, **cambios):
    """Exporta _entradas_de_prueba() a tmp_path/nombre/salida (lotes de 3 entradas), con los atributos
    de lic cambiados por `cambios` solo durante la exportación."""
    for atributo, valor in cambios.items():
        monkeypatch.setattr(lic, atributo, valor)
    destino = tmp_path / nombre
    destino.mkdir()
    monkeypatch.setattr(lic, "OUTPUT_DIR", destino / "salida")
    z = _zip(destino / "licitacionesPerfilesContratanteCompleto3_2024.zip", _entradas_de_prueba())
    lic.exportar_datos(lic.procesar_zip(z, "licitaciones"), "prueba", lote=3)
    monkeypatch.undo()
    return destino / "salida"


class TestExportacionAmpliacion:
    def test_tablas_nuevas(self, tmp_path, monkeypatch):
        salida = _exportar(tmp_path, "normal", monkeypatch)
        principal = pd.read_parquet(salida / "prueba.parquet")
        n = len(principal)
        tablas = {t: pd.read_parquet(salida / f"prueba_{t}.parquet") for t in lic.TABLAS_AMPLIACION}
        for nombre, tabla in tablas.items():
            assert list(tabla.columns[:3]) == ["id", "expediente", "conjunto"], nombre
            assert list(tabla.columns[-4:]) == ["textos_originales", "fecha_updated", "es_ultima_version",
                                                "entrada_repetida"], nombre
            assert (salida / f"prueba_{nombre}.csv").exists()
            # Cada fila casa con su entrada por id + fecha_updated
            claves = set(zip(principal["id"], principal["fecha_updated"]))
            assert set(zip(tabla["id"], tabla["fecha_updated"])) <= claves, nombre
        ampliado = tablas["ampliado"]
        assert list(ampliado.columns[3:-4]) == lic.COLUMNAS_AMPLIADO
        assert len(ampliado) == n
        assert ampliado[["id", "fecha_updated"]].equals(principal[["id", "fecha_updated"]])
        for col in ("fecha_inicio_ejecucion", "fecha_fin_ejecucion"):
            assert pd.api.types.is_datetime64_any_dtype(ampliado[col])
        for col in ("fecha_publicacion", "fecha_emision"):
            assert pd.api.types.is_datetime64_any_dtype(tablas["anuncios"][col]), col
        fila = ampliado[ampliado["id"].str.endswith("/2808424")]
        assert fila["fecha_fin_ejecucion"].tolist() == [pd.Timestamp("2019-11-29")] * 2
        # La fecha imposible queda nula y su texto, en textos_originales de su fila
        mal = fila[fila["fecha_inicio_ejecucion"].isna()]
        assert mal["textos_originales"].tolist() == ['{"fecha_inicio_ejecucion": "2019-02-30"}']
        assert fila["fecha_inicio_ejecucion"].dropna().tolist() == [pd.Timestamp("2019-03-01")]
        # Los envíos van como texto, tal cual
        envio = tablas["anuncios"].dropna(subset=["envio_fecha"])
        assert "2018-12-26+01:00" in set(envio["envio_fecha"])
        garantias = tablas["condiciones"][tablas["condiciones"]["grupo"] == "garantia"]
        assert garantias["textos_originales"].dropna().tolist() == ['{"porcentaje": "5,5"}']
        # La principal no se entera
        assert principal["textos_originales"].isna().all()
        # Todas las filas de cada entrada, ninguna de más
        for t in ("condiciones", "anuncios", "documentos"):
            assert len(tablas[t]) == sum(len(_parsear(x)[f"_{t}"]) for x in _entradas_de_prueba()), t

    def test_las_tablas_anteriores_no_cambian_ni_un_byte(self, tmp_path, monkeypatch):
        normal = _exportar(tmp_path, "normal", monkeypatch)
        # Como el código de antes: sin ampliación
        vacia = {"_ampliado": [], "_condiciones": [], "_anuncios": [], "_documentos": []}
        sin = _exportar(tmp_path, "sin", monkeypatch, parsear_ampliacion=lambda status: dict(vacia))

        def falla(status):
            raise RuntimeError("roto")
        con_error = _exportar(tmp_path, "error", monkeypatch, parsear_anuncios=falla)
        for tabla in ANTERIORES:
            base = "prueba" if tabla == "principal" else f"prueba_{tabla}"
            for sufijo in (".parquet", ".csv"):
                ref = sin / f"{base}{sufijo}"
                if not ref.exists():
                    continue
                assert (normal / f"{base}{sufijo}").read_bytes() == ref.read_bytes(), base + sufijo
                assert (con_error / f"{base}{sufijo}").read_bytes() == ref.read_bytes(), base + sufijo
        assert not any((sin / f"prueba_{t}.parquet").exists() for t in lic.TABLAS_AMPLIACION)
        errores = pd.read_parquet(con_error / "prueba_ampliado.parquet")["textos_originales"]
        assert set(errores) == {'{"error_ampliacion": "RuntimeError: roto"}'}
        assert not (con_error / "prueba_anuncios.parquet").exists()

    @pytest.mark.parametrize("lote", [1, 2, 1000])
    def test_por_lotes_igual_que_un_unico_dataframe(self, tmp_path, monkeypatch, lote):
        """Las tablas nuevas, como las demás, salen iguales con cualquier tamaño de lote: con lotes de una
        entrada cambia el tipo de cada columna según la parte (fechas sin ningún valor, enteros con nulos...)."""
        import test_nacional_placsp as base
        z = _zip(tmp_path / "licitacionesPerfilesContratanteCompleto3_2024.zip", _entradas_de_prueba())
        base._exportar_referencia(lic.procesar_zip(z, "licitaciones"), tmp_path / "ref", "ref")
        copias = [("licitaciones", z, z.name)]
        with lic.ExportacionPlacsp("porlotes", tmp_path / "porlotes", lote=lote) as exportacion:
            lic.procesar_copias(copias, exportacion)
            exportacion.cerrar()
        base._comparar_con_referencia(tmp_path / "ref", "ref", tmp_path / "porlotes", "porlotes")
        assert {f.stem for f in (tmp_path / "porlotes").glob("*.parquet")} >= {
            f"porlotes_{t}" for t in lic.TABLAS_AMPLIACION}
