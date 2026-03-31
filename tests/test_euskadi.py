import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - depende del entorno local
    pd = None


REPO_ROOT = Path(__file__).resolve().parents[1]

if "requests" not in sys.modules:  # pragma: no cover - depende del entorno local
    sys.modules["requests"] = SimpleNamespace(
        get=None,
        Session=lambda: None,
        exceptions=SimpleNamespace(HTTPError=Exception),
    )


def load_module(name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


eus_download = load_module(
    "euskadi_download",
    REPO_ROOT / "Euskadi" / "ccaa_euskadi.py",
)
eus_consolidation = (
    load_module(
        "euskadi_consolidation",
        REPO_ROOT / "Euskadi" / "consolidacion_euskadi.py",
    )
    if pd is not None
    else None
)
eus_detail = (
    load_module(
        "euskadi_detail",
        REPO_ROOT / "Euskadi" / "detalle_euskadi.py",
    )
    if pd is not None
    else None
)

SAMPLE_DETAIL_XML = b"""<?xml version="1.0" encoding="ISO-8859-1"?>
<contractingAnnouncement id="expcm160403" lang="es">
  <language>es</language>
  <contracting>
    <idExpOrigen desc="Expediente">470181</idExpOrigen>
    <firstPublicationDate desc="Fecha de la primera publicacion">03/01/2024 05:03</firstPublicationDate>
    <lastPublicationDate desc="Fecha de la ultima publicacion">03/01/2024 05:40</lastPublicationDate>
    <codExp desc="Expediente">B911-2021-00011</codExp>
    <contractPeriod desc="Plazo">1</contractPeriod>
    <contractPeriodType id="4" desc="Duracion del contrato">Meses</contractPeriodType>
    <processingStatus id="AD" desc="Estado de la tramitacion">Adjudicacion</processingStatus>
    <contractingAuthority id="299">
      <name desc="Poder adjudicador">Ayuntamiento de Prueba</name>
      <nutsCode desc="Codigo NUTS">ES213</nutsCode>
      <address desc="Direccion Postal">Calle Mayor 1</address>
      <webPage desc="Direccion Web">https://ejemplo.eus</webPage>
    </contractingAuthority>
    <entityDriving id="747">
      <name desc="Entidad impulsora">P4800000A - Ayuntamiento de Prueba</name>
    </entityDriving>
    <contractingBody id="912198">
      <name desc="Organo contratacion">Alcaldia</name>
    </contractingBody>
    <contractingType id="3" desc="Tipo de contrato">Suministros</contractingType>
    <subject desc="Objeto del contrato">Suministro de prueba</subject>
    <cpvs desc="CPV's">
      <cpv id="39100000-3">
        <name desc="CPV">Mobiliario</name>
        <main desc="Principal">true</main>
      </cpv>
    </cpvs>
    <placeExecutionNUTS desc="NUTS del lugar ejecucion principal">
      <placeExecution id="ES213">
        <code desc="Codigo NUTS">ES213</code>
        <name desc="Descripcion">Bizkaia</name>
        <main desc="Principal">true</main>
      </placeExecution>
    </placeExecutionNUTS>
    <flags>
      <flag id="contrato_menor" desc="Contrato menor">true</flag>
      <flag id="sara" desc="Sujeto a regulacion armonizada">false</flag>
      <flag id="division_lotes" desc="Division en lotes">false</flag>
      <flag id="prorroga" desc="Prorrogas">false</flag>
    </flags>
    <processing desc="Tipo tramitacion">Ordinaria</processing>
    <adjudicationProcedure desc="Procedimiento">Directo/Contrato Menor</adjudicationProcedure>
    <offersManagement>
      <offerManagement>
        <name desc="Razon Social">Proveedor de prueba, S.L.</name>
        <cif desc="CIF">B12345678</cif>
        <provincia desc="Provincia">Bizkaia</provincia>
      </offerManagement>
    </offersManagement>
    <formalizations>
      <formalization>
        <contractCode desc="Codigo del contrato">B911-2021-00011776_00001</contractCode>
        <state desc="Estado Contrato">Ejecucion</state>
        <id desc="CIF">B12345678</id>
        <businessName desc="Razon Social">Proveedor de prueba, S.L.</businessName>
        <dateCompanySignature desc="F. Firma Empresa / Formalizacion">23/11/2021</dateCompanySignature>
      </formalization>
    </formalizations>
    <resolutions>
      <resolution>
        <priceWithoutVAT desc="Precio sin IVA">4.170</priceWithoutVAT>
        <priceWithVAT desc="Precio con IVA">5.045,7</priceWithVAT>
        <adjInfo desc="Adjudicacion">
          <date desc="Fecha adjudicacion">23/11/2021</date>
          <licitorCompanies desc="Empresa Adjudicataria">
            <name desc="Razon Social">Proveedor de prueba, S.L.</name>
            <cif desc="CIF">B12345678</cif>
          </licitorCompanies>
        </adjInfo>
      </resolution>
    </resolutions>
    <biddersNumber desc="Numero de ofertas">1</biddersNumber>
    <pymeNumOffers desc="Ofertas PYMEs">1</pymeNumOffers>
    <otherEstatesNumOffers desc="Ofertas resto del estado">0</otherEstatesNumOffers>
    <thirdCountriesNumOffers desc="Ofertas terceros paises">0</thirdCountriesNumOffers>
    <electronicallyNumOffers desc="Ofertas electronicas">1</electronicallyNumOffers>
  </contracting>
</contractingAnnouncement>
"""

SAMPLE_LEGACY_XML = b"""<?xml version="1.0" encoding="ISO-8859-1"?>
<record name="es_JASO3871" type="content">
  <item name="v79_idioma"><value>es</value></item>
  <item name="v79_idRec"><value>JASO3871</value></item>
  <item name="contratacion">
    <value>
      <item name="contratacion_expediente"><value>S-019-DAPJ-2014</value></item>
      <item name="contratacion_fecha_primera_publicacion"><value>29/09/2014 15:28:20</value></item>
      <item name="contratacion_fecha_de_publicacion_documento"><value>13/08/2020 13:48:31</value></item>
      <item name="contratacion_estado_tramitacion"><value><item name="valor"><value>Modificacion del Contrato</value></item></value></item>
      <item name="contratacion_tipo_contrato"><value><item name="valor"><value>Suministros</value></item></value></item>
      <item name="contratacion_tramitacion"><value><item name="valor"><value>Ordinaria</value></item></value></item>
      <item name="contratacion_procedimiento"><value><item name="valor"><value>Abierto</value></item></value></item>
      <item name="contratacion_titulo_contrato"><value>Suministro legacy</value></item>
      <item name="contratacion_num_licitadores"><value>1</value></item>
      <item name="contratacion_contrato_menor"><value>No</value></item>
      <item name="contratacion_division_lotes"><value>No</value></item>
      <item name="contratacion_poder_adjudicador">
        <value>
          <item name="valor"><value>Gobierno Vasco</value></item>
          <item name="contratacion_codigo_nuts"><value>ES211</value></item>
          <item name="direccion"><value>Donostia-San Sebastian, 1</value></item>
          <item name="direccion_web"><value>http://www.euskadi.eus</value></item>
        </value>
      </item>
      <item name="contratacion_entidad_impulsora"><value><item name="valor"><value>Administracion Publica y Justicia</value></item></value></item>
      <item name="contratacion_entidad_tramitadora"><value><item name="valor"><value>Administracion Publica y Justicia</value></item></value></item>
      <item name="contratacion_codigo_cpv">
        <value>
          <item name="0">
            <value>
              <item name="contratacion_cpv"><value>33696500-0</value></item>
              <item name="contratacion_desc_cpv"><value>Reactivos de laboratorio</value></item>
            </value>
          </item>
        </value>
      </item>
      <item name="contratacion_empresas_licitadoras">
        <value>
          <item name="0">
            <value>
              <item name="contratacion_empresa_licitadora_razon_social"><value>Proveedor legacy, S.A.</value></item>
              <item name="contratacion_empresa_licitadora_cif"><value>B99999999</value></item>
              <item name="contratacion_empresa_licitadora_provincia"><value>Barcelona</value></item>
            </value>
          </item>
        </value>
      </item>
      <item name="contratacion_formalizacion_contrato">
        <value>
          <item name="0">
            <value>
              <item name="empresa"><value>Proveedor legacy, S.A.</value></item>
              <item name="fecha_firma"><value>29/12/2014</value></item>
            </value>
          </item>
        </value>
      </item>
      <item name="contratacion_informe_adjudicacion_definitiva">
        <value>
          <item name="0">
            <value>
              <item name="empresa"><value>Proveedor legacy, S.A.</value></item>
              <item name="precio"><value>159.001,5</value></item>
              <item name="precioIVA"><value>174.901,5</value></item>
            </value>
          </item>
        </value>
      </item>
    </value>
  </item>
</record>
"""

SAMPLE_DETAIL_HTML = b"""
<html>
  <body>
    <div class="barraTitulo"><h1>145K</h1></div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Objeto del contrato</div>
      <div class="col-xs-6 col-md-8">Servicios del area de juventud</div>
    </div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Expediente</div>
      <div class="col-xs-6 col-md-8">145K</div>
    </div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Contrato menor</div>
      <div class="col-xs-6 col-md-8">No</div>
    </div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Procedimiento de adjudicaci\xc3\xb3n</div>
      <div class="col-xs-6 col-md-8">Abierto</div>
    </div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Presupuesto del contrato sin IVA</div>
      <div class="col-xs-6 col-md-8">12.345,67</div>
    </div>
    <div class="row">
      <div class="col-xs-12 col-sm-4 col-md-4">Poder adjudicador</div>
      <div class="col-xs-6 col-md-8">Ayuntamiento de Prueba</div>
    </div>
  </body>
</html>
"""

SAMPLE_INDEX_XML = b"""<?xml version='1.0' encoding='ISO-8859-1'?>
<content oid='r01dtpd1660f9dd88c2937996c799ee99752e5dca5' objectType='0'>
  <internalName>expjaso13726</internalName>
  <documents>
    <document oid='r01doc-eu' objectType='1'>
      <language>eu</language>
      <dataFiles>
        <dataFile mainDataFile='true' oid='eu-file' objectType='2'>
          <metaDataList>
            <metaData metaDataOid='contratacion_expediente'>
              <metaDataValue><![CDATA[2336]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_contrato_menor'>
              <metaDataValue><![CDATA[Ez]]></metaDataValue>
            </metaData>
          </metaDataList>
        </dataFile>
      </dataFiles>
    </document>
    <document oid='r01doc-es' objectType='1'>
      <language>es</language>
      <publicationInfo>
        <publishContext repositoryNumericId='1' repositoryAlias='contenidos' repositoryOid='Inter' state='1'>
          <publishDate>05/07/2019 [11:34:59:175]</publishDate>
        </publishContext>
      </publicationInfo>
      <dataFiles>
        <dataFile mainDataFile='true' oid='es-file' objectType='2'>
          <metaDataList>
            <metaData metaDataOid='contratacion_expediente'>
              <metaDataValue><![CDATA[2336]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_objeto_contrato'>
              <metaDataValue><![CDATA[Asistencia tecnica de prueba]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_estado_tramitacion'>
              <metaDataValue><![CDATA[FO]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_tipo_contrato'>
              <metaDataValue><![CDATA[Servicios]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_poder_adjudicador_titulo'>
              <metaDataValue><![CDATA[Consorcio de Aguas Bilbao Bizkaia]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_poder_adjudicador_url'>
              <metaDataValue><![CDATA[/contenidos/anuncio_contratacion/expjaso13726/es_doc/images/logo.jpg]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_entidad_impulsora'>
              <metaDataValue><![CDATA[Consorcio de Aguas Bilbao Bizkaia]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_organo_contratacion'>
              <metaDataValue><![CDATA[Gerente]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_procedimiento'>
              <metaDataValue><![CDATA[Abierto]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_contrato_menor'>
              <metaDataValue><![CDATA[No]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_presupuesto_sin_iva'>
              <metaDataValue><![CDATA[12.345,67]]></metaDataValue>
            </metaData>
            <metaData metaDataOid='contratacion_presupuesto_con_iva'>
              <metaDataValue><![CDATA[14.938,26]]></metaDataValue>
            </metaData>
          </metaDataList>
        </dataFile>
      </dataFiles>
    </document>
  </documents>
</content>
"""

SAMPLE_INDEX_XML_SPARSE = b"""<?xml version='1.0' encoding='ISO-8859-1'?>
<content oid='r01dtpd166e489df03263be8b087182d176290d337' objectType='0'>
  <author>V79</author>
  <workAreaRelativePath>anuncio_contratacion/expjaso14922</workAreaRelativePath>
  <structureCatalogs>
    <structureCatalog labelRoleInStructure='2'>
      <terms>
        <term lang='es'><termText><![CDATA[Ayuntamiento de Barakaldo]]></termText></term>
      </terms>
    </structureCatalog>
    <structureCatalog labelRoleInStructure='1'>
      <terms>
        <term lang='es'><termText><![CDATA[Bizkaia]]></termText></term>
      </terms>
    </structureCatalog>
    <structureCatalog labelRoleInStructure='3'>
      <terms>
        <term lang='es'><termText><![CDATA[Ayuntamiento de Barakaldo]]></termText></term>
      </terms>
    </structureCatalog>
    <structureCatalog labelRoleInStructure='1'>
      <terms>
        <term lang='es'><termText><![CDATA[Suministros]]></termText></term>
      </terms>
    </structureCatalog>
  </structureCatalogs>
  <internalName>expjaso14922</internalName>
  <name>expjaso14922</name>
  <createDate>05/11/2018 [16:42:12:739]</createDate>
</content>
"""


class EuskadiDownloadTests(unittest.TestCase):
    def test_download_defaults_live_under_euskadi_directory(self):
        self.assertEqual(
            eus_download.DEFAULT_BASE_DIR,
            (REPO_ROOT / "Euskadi" / "datos_euskadi_contratacion_v4"),
        )
        self.assertEqual(
            eus_download.DEFAULT_LOG_PATH,
            (REPO_ROOT / "Euskadi" / "descarga_euskadi_v4.log"),
        )

    def test_build_dirs_uses_expected_subdirectories(self):
        base_dir = Path("/tmp/euskadi-test")
        dirs = eus_download.build_dirs(base_dir)

        self.assertEqual(dirs["api_contracts"], base_dir / "A1_api_contratos")
        self.assertEqual(dirs["xlsx_anual"], base_dir / "B1_xlsx_sector_publico_anual")
        self.assertEqual(dirs["vitoria"], base_dir / "C2_vitoria_gasteiz")

    def test_configure_runtime_updates_base_and_log_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "data"
            log_path = Path(tmpdir) / "logs" / "download.log"

            eus_download.configure_runtime(base_dir=base_dir, log_path=log_path)

            self.assertEqual(eus_download.BASE_DIR, base_dir.resolve())
            self.assertEqual(eus_download.LOG_PATH, log_path.resolve())
            self.assertEqual(
                eus_download.DIRS["bilbao"],
                base_dir.resolve() / "C1_bilbao",
            )

    def test_is_real_data_filters_fake_html_and_accepts_json(self):
        self.assertFalse(
            eus_download.is_real_data(b"<html><body>404 error</body></html>", ".json")
        )
        payload = b'{"items": [{"id": 1}]}' * 12
        self.assertTrue(eus_download.is_real_data(payload, ".json"))


class EuskadiConsolidationTests(unittest.TestCase):
    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_consolidation_defaults_live_under_euskadi_directory(self):
        self.assertEqual(
            eus_consolidation.DEFAULT_INPUT_DIR,
            (REPO_ROOT / "Euskadi" / "datos_euskadi_contratacion_v4"),
        )
        self.assertEqual(
            eus_consolidation.DEFAULT_OUTPUT_DIR,
            (REPO_ROOT / "Euskadi" / "euskadi_parquet"),
        )

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_configure_runtime_updates_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            log_path = Path(tmpdir) / "logs" / "consolidation.log"

            eus_consolidation.configure_runtime(
                input_dir=input_dir,
                output_dir=output_dir,
                log_path=log_path,
            )

            self.assertEqual(eus_consolidation.INPUT_DIR, input_dir.resolve())
            self.assertEqual(eus_consolidation.OUTPUT_DIR, output_dir.resolve())
            self.assertEqual(eus_consolidation.LOG_PATH, log_path.resolve())
            self.assertEqual(
                eus_consolidation.PATHS["api_authorities"],
                input_dir.resolve() / "A3_api_poderes",
            )

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_load_json_pages_merges_items_from_multiple_pages(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            (directory / "page_1.json").write_text(
                json.dumps({"items": [{"id": 1, "name": "Uno"}]}),
                encoding="utf-8",
            )
            (directory / "page_2.json").write_text(
                json.dumps({"items": [{"id": 2, "name": "Dos"}]}),
                encoding="utf-8",
            )

            df = eus_consolidation.load_json_pages(directory)

            self.assertEqual(len(df), 2)
            self.assertEqual(sorted(df["id"].tolist()), [1, 2])

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_safe_str_columns_normalizes_object_columns(self):
        df = pd.DataFrame(
            {
                "texto": ["hola", None],
                "numero": [1, 2],
            }
        )

        result = eus_consolidation.safe_str_columns(df.copy())

        self.assertEqual(result["texto"].iloc[0], "hola")
        self.assertTrue(pd.isna(result["texto"].iloc[1]))
        self.assertEqual(result["numero"].tolist(), [1, 2])


class EuskadiDetailTests(unittest.TestCase):
    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_detail_defaults_live_under_euskadi_directory(self):
        self.assertEqual(
            eus_detail.DEFAULT_MASTER_PATH,
            (REPO_ROOT / "Euskadi" / "euskadi_parquet" / "contratos_master.parquet"),
        )
        self.assertEqual(
            eus_detail.DEFAULT_OUTPUT_PATH,
            (
                REPO_ROOT
                / "Euskadi"
                / "euskadi_parquet"
                / "contratos_master_detallado.parquet"
            ),
        )

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_parse_contracting_xml_extracts_stable_detail_fields(self):
        parsed = eus_detail.parse_contracting_xml(SAMPLE_DETAIL_XML)

        self.assertEqual(parsed["detail_xml_id"], "expcm160403")
        self.assertEqual(parsed["detail_cod_exp"], "B911-2021-00011")
        self.assertEqual(
            parsed["detail_contracting_authority_name"], "Ayuntamiento de Prueba"
        )
        self.assertEqual(parsed["detail_contracting_type"], "Suministros")
        self.assertEqual(parsed["detail_cpv_codes"], "39100000-3")
        self.assertEqual(parsed["detail_nuts_codes"], "ES213")
        self.assertTrue(parsed["detail_flag_contrato_menor"])
        self.assertFalse(parsed["detail_flag_sara"])
        self.assertEqual(
            parsed["detail_award_company_name"], "Proveedor de prueba, S.L."
        )
        self.assertEqual(parsed["detail_contract_code"], "B911-2021-00011776_00001")

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_parse_contracting_xml_supports_legacy_record_schema(self):
        parsed = eus_detail.parse_contracting_xml(SAMPLE_LEGACY_XML)

        self.assertEqual(parsed["detail_xml_id"], "JASO3871")
        self.assertEqual(parsed["detail_cod_exp"], "S-019-DAPJ-2014")
        self.assertEqual(parsed["detail_contracting_authority_name"], "Gobierno Vasco")
        self.assertEqual(parsed["detail_contracting_type"], "Suministros")
        self.assertEqual(parsed["detail_cpv_codes"], "33696500-0")
        self.assertEqual(parsed["detail_offer_company_name"], "Proveedor legacy, S.A.")
        self.assertEqual(parsed["detail_award_price_without_vat"], "159.001,5")

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_parse_contracting_html_extracts_pairs(self):
        parsed = eus_detail.parse_contracting_html(SAMPLE_DETAIL_HTML)

        self.assertEqual(parsed["detail_html_title"], "145K")
        self.assertEqual(parsed["detail_cod_exp"], "145K")
        self.assertEqual(parsed["detail_subject"], "Servicios del area de juventud")
        self.assertEqual(parsed["detail_adjudication_procedure"], "Abierto")
        self.assertEqual(parsed["detail_budget_without_vat"], "12.345,67")
        self.assertFalse(parsed["detail_flag_contrato_menor"])

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_parse_contracting_xml_supports_index_content_schema(self):
        parsed = eus_detail.parse_contracting_xml(SAMPLE_INDEX_XML)

        self.assertEqual(
            parsed["detail_xml_id"], "r01dtpd1660f9dd88c2937996c799ee99752e5dca5"
        )
        self.assertEqual(parsed["detail_cod_exp"], "2336")
        self.assertEqual(parsed["detail_subject"], "Asistencia tecnica de prueba")
        self.assertEqual(parsed["detail_contracting_type"], "Servicios")
        self.assertEqual(
            parsed["detail_contracting_authority_name"],
            "Consorcio de Aguas Bilbao Bizkaia",
        )
        self.assertEqual(parsed["detail_adjudication_procedure"], "Abierto")
        self.assertEqual(parsed["detail_budget_without_vat"], "12.345,67")
        self.assertFalse(parsed["detail_flag_contrato_menor"])
        self.assertEqual(parsed["detail_language"], "es")

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_parse_contracting_xml_supports_sparse_index_content_schema(self):
        parsed = eus_detail.parse_contracting_xml(SAMPLE_INDEX_XML_SPARSE)

        self.assertEqual(
            parsed["detail_xml_id"], "r01dtpd166e489df03263be8b087182d176290d337"
        )
        self.assertEqual(parsed["detail_cod_exp"], "expjaso14922")
        self.assertEqual(
            parsed["detail_contracting_authority_name"], "Ayuntamiento de Barakaldo"
        )
        self.assertEqual(parsed["detail_contracting_type"], "Suministros")
        self.assertTrue(parsed["detail_index_partial"])
        self.assertEqual(parsed["detail_index_metadata_count"], 0)

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_build_detail_key_prefers_working_xml_fallbacks(self):
        row = {
            "xml_datos": "16/06/2021",
            "xml_metadatos": "r01etpd161c28959474fb69e0183c83bf86dc7f801",
            "url_amigable": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/expcm134551/r01Index/expcm134551-idxContent.xml",
            "friendlyurl": None,
            "url_física": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/expcm134551/opendata/expcm134551.zip",
            "physicalurl": None,
        }
        self.assertEqual(
            eus_detail.build_detail_key(row),
            "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/expcm134551/r01Index/expcm134551-idxContent.xml",
        )

        row = {
            "xml_datos": "http://opendata.euskadi.eus/contenidos/anuncio_contratacion/expjaso9484/opendata/expjaso9484.zip",
            "xml_metadatos": None,
            "url_amigable": None,
            "friendlyurl": None,
            "url_física": "http://opendata.euskadi.eus/contenidos/anuncio_contratacion/expjaso9484/r01Index/expjaso9484-idxContent.xml",
            "physicalurl": None,
        }
        self.assertEqual(
            eus_detail.build_detail_key(row),
            "http://opendata.euskadi.eus/contenidos/anuncio_contratacion/expjaso9484/r01Index/expjaso9484-idxContent.xml",
        )

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_merge_detail_joins_cache_back_to_master(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path = Path(tmpdir) / "contratos_master.parquet"
            cache_path = Path(tmpdir) / "contratos_master_detail.sqlite3"
            output_path = Path(tmpdir) / "contratos_master_detallado.parquet"

            master_df = pd.DataFrame(
                {
                    "xml_datos": [
                        "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/expdemo1/r01Index/expdemo1-idxContent.xml",
                        None,
                    ],
                    "xml_metadatos": [None, None],
                    "url_amigable": [None, None],
                    "friendlyurl": [None, None],
                    "url_física": [None, None],
                    "physicalurl": [None, None],
                    "titulo_del_contrato": ["Uno", "Dos"],
                }
            )
            master_df.to_parquet(master_path, index=False)

            eus_detail.ensure_cache(cache_path)
            eus_detail.write_cache_rows(
                cache_path,
                [
                    {
                        "xml_url": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/expdemo1/r01Index/expdemo1-idxContent.xml",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "updated_at": "2026-03-31T00:00:00",
                        "mapped_json": json.dumps(
                            {
                                "detail_cod_exp": "EXP-1",
                                "detail_contracting_type": "Servicios",
                            }
                        ),
                        "raw_zlib": None,
                    }
                ],
            )

            info = eus_detail.merge_detail(master_path, cache_path, output_path)
            merged = pd.read_parquet(output_path)

            self.assertEqual(info["rows"], 2)
            self.assertEqual(merged.loc[0, "detail_cod_exp"], "EXP-1")
            self.assertEqual(merged.loc[0, "detail_status"], "done")
            self.assertEqual(merged.loc[1, "detail_status"], "no_locator")

    @unittest.skipUnless(pd is not None, "pandas no disponible")
    def test_build_candidates_resume_skips_terminal_statuses(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            master_path = Path(tmpdir) / "contratos_master.parquet"
            cache_path = Path(tmpdir) / "contratos_master_detail.sqlite3"

            pd.DataFrame(
                {
                    "xml_datos": [
                        "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/done/r01Index/done-idxContent.xml",
                        "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/not-found/r01Index/not-found-idxContent.xml",
                        "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/failed/r01Index/failed-idxContent.xml",
                        "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/pending/r01Index/pending-idxContent.xml",
                    ],
                    "xml_metadatos": [None, None, None, None],
                    "url_amigable": [None, None, None, None],
                    "friendlyurl": [None, None, None, None],
                    "url_física": [None, None, None, None],
                    "physicalurl": [None, None, None, None],
                }
            ).to_parquet(master_path, index=False)

            eus_detail.ensure_cache(cache_path)
            eus_detail.write_cache_rows(
                cache_path,
                [
                    {
                        "xml_url": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/done/r01Index/done-idxContent.xml",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "updated_at": "2026-03-31T00:00:00",
                        "mapped_json": "{}",
                        "raw_zlib": None,
                    },
                    {
                        "xml_url": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/not-found/r01Index/not-found-idxContent.xml",
                        "status": "not_found",
                        "attempts": 1,
                        "last_error": "404",
                        "updated_at": "2026-03-31T00:00:00",
                        "mapped_json": None,
                        "raw_zlib": None,
                    },
                    {
                        "xml_url": "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/failed/r01Index/failed-idxContent.xml",
                        "status": "failed",
                        "attempts": 1,
                        "last_error": "timeout",
                        "updated_at": "2026-03-31T00:00:00",
                        "mapped_json": None,
                        "raw_zlib": None,
                    },
                ],
            )

            candidates, total_available = eus_detail.build_candidates(
                master_path=master_path,
                cache_path=cache_path,
                resume=True,
                retry_not_found=False,
            )

            self.assertEqual(total_available, 4)
            self.assertEqual(
                [candidate["detail_key"] for candidate in candidates],
                [
                    "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/failed/r01Index/failed-idxContent.xml",
                    "https://www.contratacion.euskadi.eus/contenidos/anuncio_contratacion/pending/r01Index/pending-idxContent.xml",
                ],
            )


if __name__ == "__main__":
    unittest.main()
