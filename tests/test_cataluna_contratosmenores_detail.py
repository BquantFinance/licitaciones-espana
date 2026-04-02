import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_cataluna_contratosmenores_detail.py"
SPEC = importlib.util.spec_from_file_location("cataluna_contratosmenores_detail", MODULE_PATH)
cataluna_contratosmenores_detail = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cataluna_contratosmenores_detail)


class CatalunyaContratosMenoresDetailTests(unittest.TestCase):
    def test_defaults_live_under_repo_tree(self):
        self.assertEqual(
            cataluna_contratosmenores_detail.DEFAULT_INPUT_PATH,
            REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors.parquet",
        )
        self.assertEqual(
            cataluna_contratosmenores_detail.DEFAULT_OUTPUT_PATH,
            REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors_detallado.parquet",
        )
        self.assertEqual(
            cataluna_contratosmenores_detail.DEFAULT_CACHE_PATH,
            REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors_detail.sqlite3",
        )

    def test_build_artifact_paths_uses_expected_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = cataluna_contratosmenores_detail.build_artifact_paths(
                Path(tmpdir) / "base.parquet",
                Path(tmpdir) / "out" / "contractacio_menors_detallado.parquet",
                Path(tmpdir) / "out" / "contractacio_menors_detail.sqlite3",
            )
            self.assertEqual(paths["output"].name, "contractacio_menors_detallado.parquet")
            self.assertEqual(paths["cache"].name, "contractacio_menors_detail.sqlite3")
            self.assertEqual(paths["summary"].name, "contractacio_menors_detallado_detail_summary.json")

    def test_parse_detail_payload_modern_non_aggregated(self):
        payload = {
            "publicacioId": 300736144,
            "expedientId": "exp-modern",
            "fase": 1008286,
            "titol": "Publicacion moderna",
            "publicacioAntiga": False,
            "dades": {
                "dataPublicacioReal": "2026-04-02T10:00:00.000Z",
                "dataPublicacioPlanificada": None,
                "organ": {
                    "nom": "Institut de Seguretat Publica de Catalunya",
                    "nif": "Q0801494F",
                    "web": "http://ispc.gencat.cat",
                    "email": "contractacio.ispc@gencat.cat",
                    "nuts": "ES511",
                    "personesContacte": [{"nom": "Merce"}],
                },
                "publicacio": {
                    "dadesBasiquesPublicacio": {
                        "codiExpedient": "21/2026",
                        "denominacio": {"text": "Incendis industrials"},
                        "descripcio": {"text": "Practiques d'extincio"},
                        "fasePublicacio": {"id": 1008286, "text": "Adjudicacio"},
                        "tipusPublicacioExpedient": {"id": 1000000, "text": "Contracte"},
                        "tipusTramitacio": {"id": 470, "text": "Ordinaria"},
                        "tipusContracte": {"id": 393, "text": "Serveis"},
                        "procedimentAdjudicacio": {"id": 403, "text": "Contracte menor"},
                        "tipusOfertaElectronica": {"id": 413, "text": "No requerida"},
                        "normativaAplicable": {"id": 1008186, "text": "LCSP"},
                        "unitatContractacio": {"text": "Contractacio ISPC"},
                        "publicacioAgregada": False,
                    },
                    "dadesExpedient": {
                        "lleiAssociada": "Llei 2017/2019",
                        "esPublicacioAntiga": False,
                        "noAdmetrePreguntes": False,
                    },
                    "dadesPublicacio": {
                        "pressupostLicitacio": 51704.91,
                    },
                    "dadesPublicacioLot": [
                        {
                            "lotId": 0,
                            "numeroLot": "1",
                            "totalOfertesRebudes": 3,
                            "identitatEmpresa": [
                                {"empresa": "FIRE SAFETY SOLUTIONS, S.L."},
                                {"empresa": "FAST Enginyeria SL"},
                            ],
                            "informesActesMesesContractacio": {
                                "docs": [
                                    {"id": 1, "titol": "acta1.pdf"},
                                    {"id": 2, "titol": "acta2.pdf"},
                                ]
                            },
                        }
                    ],
                },
            },
        }

        parsed = cataluna_contratosmenores_detail.parse_detail_payload(payload)
        self.assertEqual(parsed["detail_source_kind"], "modern")
        self.assertEqual(parsed["detail_codi_expediente"], "21/2026")
        self.assertEqual(parsed["detail_tipo_contrato"], "Serveis")
        self.assertEqual(parsed["detail_lot_count"], 1)
        self.assertEqual(parsed["detail_lot_ofertas_total"], 3)
        self.assertEqual(parsed["detail_document_count"], 2)
        self.assertIn("FAST Enginyeria SL", parsed["detail_lot_empresas"])

    def test_parse_detail_payload_aggregated(self):
        payload = {
            "publicacioId": 300739111,
            "expedientId": "exp-agg",
            "fase": 1000046,
            "titol": "Contractes menors 1r trimestre 2026",
            "publicacioAntiga": False,
            "dades": {
                "dataPublicacioReal": "2026-03-31T23:00:00.000Z",
                "organ": {"nom": "SIRUSA", "personesContacte": []},
                "publicacio": {
                    "dadesBasiquesPublicacio": {
                        "codiExpedient": "CM_1rT_2026",
                        "denominacio": {"text": "Contractes menors 1r trimestre 2026"},
                        "descripcio": {"text": "Contractes menors del primer trimestre"},
                        "fasePublicacio": {"id": 1000046, "text": "Publicacio agregada de contractes"},
                        "tipusPublicacioExpedient": {"id": 1000000, "text": "Contracte"},
                        "tipusTramitacio": None,
                        "tipusContracte": None,
                        "procedimentAdjudicacio": {"id": 403, "text": "Contracte menor"},
                        "tipusOfertaElectronica": None,
                        "normativaAplicable": {"id": 1008186, "text": "LCSP"},
                        "unitatContractacio": None,
                        "publicacioAgregada": True,
                    },
                    "dadesExpedient": {
                        "lleiAssociada": "Llei 2017/2019",
                        "esPublicacioAntiga": False,
                        "noAdmetrePreguntes": False,
                    },
                    "dadesPublicacio": {
                        "contractesAgregada": [
                            {
                                "expedient": "67/2026",
                                "importAdjudicacioSenseIva": 599.2,
                                "importAdjudicacioAmbIva": 725.03,
                                "vec": 900,
                                "dataAdjudicacio": "2026-03-04T23:00:00.000Z",
                                "dataIniciExecucio": "2026-03-04T23:00:00.000Z",
                                "dataFiExecucio": "2026-07-04T22:00:00.000Z",
                                "nomAdjudicatari": "ACSA",
                                "codiCpv": {"codi": "50230000-6", "text": "Transport"},
                                "numOfertesRebudes": 2,
                                "numOfertesPime": 1,
                            },
                            {
                                "expedient": "68/2026",
                                "importAdjudicacioSenseIva": 11265.97,
                                "importAdjudicacioAmbIva": 13631.82,
                                "vec": 12000,
                                "dataAdjudicacio": "2026-02-18T23:00:00.000Z",
                                "dataIniciExecucio": "2026-02-18T23:00:00.000Z",
                                "dataFiExecucio": "2027-02-18T23:00:00.000Z",
                                "nomAdjudicatari": "CARLES RAVELL VIDAL",
                                "codiCpv": {"codi": "72500000-0", "text": "Serveis informatics"},
                                "numOfertesRebudes": 1,
                                "numOfertesPime": 1,
                            },
                        ]
                    },
                    "dadesPublicacioLot": [],
                },
            },
        }

        parsed = cataluna_contratosmenores_detail.parse_detail_payload(payload)
        self.assertEqual(parsed["detail_source_kind"], "aggregated")
        self.assertEqual(parsed["detail_contractes_agregada_count"], 2)
        self.assertAlmostEqual(parsed["detail_contractes_agregada_importe_total_sin_iva"], 11865.17)
        self.assertEqual(parsed["detail_contractes_agregada_ofertas_total"], 3)
        self.assertIn("50230000-6", parsed["detail_contractes_agregada_cpv_codes"])
        self.assertIn("CARLES RAVELL VIDAL", parsed["detail_contractes_agregada_adjudicatarios"])

    def test_parse_detail_payload_legacy(self):
        payload = {
            "publicacioId": 53602807,
            "expedientId": "exp-legacy",
            "fase": 1000040,
            "titol": "Publicacion legacy",
            "publicacioAntiga": True,
            "dades": {
                "dataPublicacio": "2019-10-22T00:00:00.000Z",
                "dadesBasiques": {
                    "codiExpedient": "127/2019/CMSERV2",
                    "denominacio": "Formacio i coaching laboral",
                    "nomOrgan": "Ajuntament de Viladecans",
                    "nomUnitat": "Contractacio",
                    "nomFaseCpp": "Licitacio",
                    "tipusPublicacio": 1000000,
                    "tipusTramitacio": "Ordinari",
                    "tipusContracte": "Serveis",
                    "subtipusContracte": "Serveis d'educacio",
                    "procedimentAdjudicacio": "Contracte menor",
                },
                "dadesContracte": {
                    "pressupostLicitacioSenseIva": 7575.0,
                    "pressupostLicitacioAmbIva": 7575.0,
                    "valorEstimatContracte": 7575.0,
                    "iva": 0.0,
                    "ambitGeografic": "Viladecans",
                    "terminiPresentacioOfertes": "2019-10-24T08:00:00.000Z",
                    "durada": {
                        "iniciTermini": "2019-10-31T23:00:00.000Z",
                        "fiTermini": "2020-06-30T22:00:00.000Z",
                        "observacions": None,
                    },
                },
                "documentacio": {
                    "altreDocuments": [
                        {"id": 1, "titol": "DECLARACIO RESPONSABLE.doc"},
                        {"id": 2, "titol": "SOLLICITUD OFERTA.docx"},
                    ],
                    "documentsAnunciLicitacio": [
                        {"id": 3, "titol": "ANUNCI LICITACIO.doc"},
                    ],
                },
                "lots": [{"numeroLot": 1}, {"numeroLot": 2}],
            },
        }

        parsed = cataluna_contratosmenores_detail.parse_detail_payload(payload)
        self.assertEqual(parsed["detail_source_kind"], "legacy")
        self.assertEqual(parsed["detail_organ_name"], "Ajuntament de Viladecans")
        self.assertEqual(parsed["detail_document_count"], 3)
        self.assertEqual(parsed["detail_lot_count"], 2)
        self.assertEqual(parsed["detail_valor_estimado_contrato"], 7575.0)

    def test_select_pending_candidates_respects_cache_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "detail.sqlite3")
            cataluna_contratosmenores_detail.ensure_cache_schema(conn)
            conn.execute(
                """
                INSERT INTO detalles
                (record_key, publicacio_id, status, attempts, last_error, source_kind, detail_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("300739111", "300739111", "done", 1, None, "aggregated", "{}", "2026-04-02T00:00:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO detalles
                (record_key, publicacio_id, status, attempts, last_error, source_kind, detail_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("53602807", "53602807", "not_found", 1, "404", None, None, "2026-04-02T00:00:00+00:00"),
            )
            conn.commit()

            pending, cache_hits = cataluna_contratosmenores_detail.select_pending_candidates(
                conn,
                ["300739111", "53602807", "300736144"],
                retry_not_found=False,
                retry_failed=False,
            )
            self.assertEqual(pending, ["300736144"])
            self.assertEqual(cache_hits, 2)

            pending_retry, _cache_hits_retry = cataluna_contratosmenores_detail.select_pending_candidates(
                conn,
                ["300739111", "53602807", "300736144"],
                retry_not_found=True,
                retry_failed=False,
            )
            self.assertEqual(pending_retry, ["53602807", "300736144"])

    def test_coerce_object_columns_for_parquet_normalizes_mixed_types(self):
        df = pd.DataFrame(
            {
                "detail_tipo_publicacion_expediente": [1000000, "Contracte", None],
                "detail_source_kind": ["legacy", "modern", "aggregated"],
            }
        )
        coerced = cataluna_contratosmenores_detail.coerce_object_columns_for_parquet(df)
        self.assertEqual(str(coerced["detail_tipo_publicacion_expediente"].dtype), "string")
        self.assertEqual(coerced.loc[0, "detail_tipo_publicacion_expediente"], "1000000")
        self.assertEqual(coerced.loc[1, "detail_tipo_publicacion_expediente"], "Contracte")


if __name__ == "__main__":
    unittest.main()
