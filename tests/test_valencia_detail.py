import importlib.util
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_valencia_detail.py"
SPEC = importlib.util.spec_from_file_location("ccaa_valencia_detail", MODULE_PATH)
ccaa_valencia_detail = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ccaa_valencia_detail)


class ValenciaDetailTests(unittest.TestCase):
    def test_defaults_live_under_repo_tree(self):
        self.assertEqual(ccaa_valencia_detail.DEFAULT_INPUT_PATH, REPO_ROOT / "valencia" / "contratacion")
        self.assertEqual(
            ccaa_valencia_detail.DEFAULT_OUTPUT_PATH,
            REPO_ROOT / "valencia" / "contratacion" / "contratacion_detallada.parquet",
        )
        self.assertEqual(
            ccaa_valencia_detail.DEFAULT_CACHE_PATH,
            REPO_ROOT / "valencia" / "contratacion" / "contratacion_detail.sqlite3",
        )

    def test_build_detail_key_normalizes_numeric_values(self):
        self.assertEqual(ccaa_valencia_detail.build_detail_key(2024, 2667.0), "2024:2667")
        self.assertEqual(ccaa_valencia_detail.build_detail_key("2021", "20262"), "2021:20262")
        self.assertIsNone(ccaa_valencia_detail.build_detail_key(None, "12"))

    def test_parse_helpers_normalize_values(self):
        self.assertEqual(ccaa_valencia_detail.parse_euro_amount("1.536,70 €"), 1536.70)
        self.assertEqual(ccaa_valencia_detail.parse_date_es("26/03/2026"), "2026-03-26")
        self.assertTrue(ccaa_valencia_detail.parse_bool_text("Sí"))
        self.assertFalse(ccaa_valencia_detail.parse_bool_text("No"))
        self.assertEqual(
            ccaa_valencia_detail.strip_html_text("<span style=color:black>Texto &amp; valor</span>"),
            "Texto & valor",
        )

    def test_parse_detail_payload_maps_relevant_fields(self):
        payload = {
            "id": {"anyo": 2024, "numreg": 2667, "numversion": 1},
            "expediente": "IV-MY01/2023 LOTE IV",
            "deconentiadj": "IVASS",
            "deunidad": "Unidad de Contratacion",
            "objeto": "Servicio de dinamizacion",
            "desctipocont": "Servicios",
            "descproced": "Abierto",
            "descTramitacion": "Ordinario",
            "descmenor": "No menor",
            "desclugareje": "Valencia/València",
            "nomcontratista": "ADJUDICATARIO SA",
            "nifMascara": "A12345678",
            "descPais": "España",
            "imadjudicacion": "93.603,90 €",
            "imadjudicacioniva": "16.245,30 €",
            "imadjudicacionsiniva": "77.358,60 €",
            "impLicitacion": "188.422,80 €",
            "impLicitacionIva": "32.701,48 €",
            "impLicitacionSinIva": "155.721,32 €",
            "importeDefinitivo": "93.603,90 €",
            "valorEstimado": "155.721,32 €",
            "feadjudicacion": "12/01/2024",
            "feformalizacion": "15/01/2024",
            "fechaPrevFin": "31/12/2024",
            "plazoEjecucion": 12,
            "descUniPlazoEjecucion": "Meses",
            "plazoMaxContrato": 12,
            "descUniMaxContrato": "Meses",
            "numAnualidades": 1,
            "numLotes": 2,
            "numInvitadas": 5,
            "numLicitaciones": 1,
            "amarco": "No",
            "expSistemaMarco": None,
            "descSistemaPrecios": "A tanto alzado",
            "descFormAdj": "Pluralidad de criterios",
            "financiadoEu": "No",
            "fondoEuropeo": None,
            "pyme": "Sí",
            "incluClausulas": "Sí",
            "reservado": "No",
            "cpvPrincipal": "85311200-4 - Servicios sociales",
            "urlplacsp": "https://placsp.example",
            "motivoNecesidad": "Necesidad operativa",
            "clasificaciones": [{"foo": "bar"}],
            "menor": "N",
        }

        detail = ccaa_valencia_detail.parse_detail_payload(payload)

        self.assertEqual(detail["detail_numreg"], 2667)
        self.assertEqual(detail["detail_importe_adjudicacion_sin_iva"], 77358.60)
        self.assertEqual(detail["detail_fecha_adjudicacion"], "2024-01-12")
        self.assertFalse(detail["detail_es_menor"])
        self.assertEqual(detail["detail_clasificaciones_count"], 1)

    def test_select_pending_candidates_respects_resume_and_retry_not_found(self):
        candidates = [
            ccaa_valencia_detail.DetailCandidate("2024:1", "2024", "1", "EXP-1"),
            ccaa_valencia_detail.DetailCandidate("2024:2", "2024", "2", "EXP-2"),
            ccaa_valencia_detail.DetailCandidate("2024:3", "2024", "3", "EXP-3"),
        ]
        cache_rows = {
            "2024:1": {"status": "done"},
            "2024:2": {"status": "not_found"},
        }

        pending, cache_hits = ccaa_valencia_detail.select_pending_candidates(
            candidates, cache_rows, resume=True, retry_not_found=False, limit=None
        )
        self.assertEqual([candidate.detail_key for candidate in pending], ["2024:3"])
        self.assertEqual(cache_hits, 2)

        pending_retry, _ = ccaa_valencia_detail.select_pending_candidates(
            candidates, cache_rows, resume=True, retry_not_found=True, limit=None
        )
        self.assertEqual([candidate.detail_key for candidate in pending_retry], ["2024:2", "2024:3"])

    def test_select_pending_candidates_applies_limit_before_resume_filtering(self):
        candidates = [
            ccaa_valencia_detail.DetailCandidate("2024:1", "2024", "1", "EXP-1"),
            ccaa_valencia_detail.DetailCandidate("2024:2", "2024", "2", "EXP-2"),
            ccaa_valencia_detail.DetailCandidate("2024:3", "2024", "3", "EXP-3"),
        ]
        cache_rows = {"2024:1": {"status": "done"}}

        pending, cache_hits = ccaa_valencia_detail.select_pending_candidates(
            candidates, cache_rows, resume=True, retry_not_found=False, limit=1
        )

        self.assertEqual(pending, [])
        self.assertEqual(cache_hits, 1)

    def test_select_search_match_prefers_unique_match(self):
        match, reason = ccaa_valencia_detail.select_search_match(
            [{"id": {"anyo": 2024, "numreg": 2667}}],
            expected_year="2024",
        )

        self.assertEqual(match, ("2024", "2667"))
        self.assertEqual(reason, "search_1")

    def test_select_search_match_prefers_expected_year_when_unique(self):
        match, reason = ccaa_valencia_detail.select_search_match(
            [
                {"id": {"anyo": 2023, "numreg": 1}},
                {"id": {"anyo": 2024, "numreg": 2}},
            ],
            expected_year="2024",
        )

        self.assertEqual(match, ("2024", "2"))
        self.assertEqual(reason, "search_year_1:2024")

    def test_select_search_match_prefers_exact_expediente_in_same_year(self):
        match, reason = ccaa_valencia_detail.select_search_match(
            [
                {"id": {"anyo": 2024, "numreg": 47457}, "expediente": "CNME 54/2024/2281"},
                {"id": {"anyo": 2024, "numreg": 37989}, "expediente": "54/2024"},
                {"id": {"anyo": 2024, "numreg": 6461}, "expediente": "AEROCAS 54/2024"},
            ],
            expected_year="2024",
            expected_expediente="54/2024",
        )

        self.assertEqual(match, ("2024", "37989"))
        self.assertEqual(reason, "search_exact_year_1:2024")

    def test_select_search_match_marks_ambiguous_or_empty_results(self):
        match, reason = ccaa_valencia_detail.select_search_match([], expected_year="2024")
        self.assertIsNone(match)
        self.assertEqual(reason, "search_0")

        match, reason = ccaa_valencia_detail.select_search_match(
            [
                {"id": {"anyo": 2024, "numreg": 2}},
                {"id": {"anyo": 2024, "numreg": 3}},
            ],
            expected_year="2024",
        )
        self.assertIsNone(match)
        self.assertEqual(reason, "search_ambiguous:2")

    def test_load_base_dataframe_excludes_existing_detail_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "Contratos_2024.parquet"
            detail_path = root / "contratacion_detallada.parquet"

            pd.DataFrame(
                [
                    {"EJERCICIO": 2024, "NUMERO_DE_REGISTRO": 12, "EXPEDIENTE": "EXP-12"},
                    {"EJERCICIO": 2024, "NUMERO_DE_REGISTRO": 13, "EXPEDIENTE": "EXP-13"},
                ]
            ).to_parquet(base_path, index=False)
            pd.DataFrame([{"detail_key": "2024:12", "detail_status": "done"}]).to_parquet(detail_path, index=False)

            dataframe = ccaa_valencia_detail.load_base_dataframe(root)

            self.assertEqual(len(dataframe), 2)
            self.assertIn("detail_key", dataframe.columns)
            self.assertEqual(sorted(dataframe["detail_key"].tolist()), ["2024:12", "2024:13"])

    def test_cache_roundtrip_and_merge(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "detail.sqlite3"
            output_path = Path(tmpdir) / "detallado.parquet"
            ccaa_valencia_detail.ensure_cache_schema(cache_path)

            ccaa_valencia_detail.upsert_cache_rows(
                cache_path,
                [
                    {
                        "detail_key": "2024:2667",
                        "ejercicio": "2024",
                        "numero_registro": "2667",
                        "expediente": "EXP-2667",
                        "source_url": "https://example.com",
                        "source_kind": "conreg_registro",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "fetched_at": "2026-04-03T12:00:00Z",
                        "payload_json": '{"detail_objeto":"Servicio","detail_valor_estimado":155721.32}',
                    }
                ],
            )

            base_df = pd.DataFrame(
                [
                    {
                        "ejercicio": 2024,
                        "numero_de_registro": 2667,
                        "expediente": "EXP-2667",
                        "detail_key": "2024:2667",
                    }
                ]
            )

            merge_stats = ccaa_valencia_detail.run_merge(base_df, cache_path, output_path)
            merged = pd.read_parquet(output_path)

            self.assertEqual(merge_stats["rows"], 1)
            self.assertEqual(merged.loc[0, "detail_objeto"], "Servicio")
            self.assertEqual(merged.loc[0, "detail_status"], "done")

    def test_merge_handles_not_found_rows_without_payload_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "detail.sqlite3"
            output_path = Path(tmpdir) / "detallado.parquet"
            ccaa_valencia_detail.ensure_cache_schema(cache_path)

            ccaa_valencia_detail.upsert_cache_rows(
                cache_path,
                [
                    {
                        "detail_key": "2024:1",
                        "ejercicio": "2024",
                        "numero_registro": "1",
                        "expediente": "EXP-1",
                        "source_url": "https://example.com/1",
                        "source_kind": "conreg_registro",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "fetched_at": "2026-04-03T12:00:00Z",
                        "payload_json": '{"detail_objeto":"Servicio"}',
                    },
                    {
                        "detail_key": "2024:2",
                        "ejercicio": "2024",
                        "numero_registro": "2",
                        "expediente": "EXP-2",
                        "source_url": "https://example.com/2",
                        "source_kind": "conreg_registro",
                        "status": "not_found",
                        "attempts": 1,
                        "last_error": "search_0",
                        "fetched_at": "2026-04-03T12:01:00Z",
                        "payload_json": None,
                    },
                ],
            )

            base_df = pd.DataFrame(
                [
                    {
                        "ejercicio": 2024,
                        "numero_de_registro": 1,
                        "expediente": "EXP-1",
                        "detail_key": "2024:1",
                    },
                    {
                        "ejercicio": 2024,
                        "numero_de_registro": 2,
                        "expediente": "EXP-2",
                        "detail_key": "2024:2",
                    },
                ]
            )

            merge_stats = ccaa_valencia_detail.run_merge(base_df, cache_path, output_path)
            merged = pd.read_parquet(output_path).sort_values("detail_key").reset_index(drop=True)

            self.assertEqual(merge_stats["rows"], 2)
            self.assertEqual(merge_stats["base_fallback_done"], 1)
            self.assertEqual(merged.loc[0, "detail_status"], "done")
            self.assertEqual(merged.loc[1, "detail_status"], "done_base_fallback")
            self.assertEqual(merged.loc[1, "detail_source_kind"], "base_row_fallback")
            self.assertEqual(merged.loc[1, "detail_expediente"], "EXP-2")

    def test_build_base_fallback_dataframe_maps_open_data_fields(self):
        base_df = pd.DataFrame(
            [
                {
                    "detail_key": "2024:2",
                    "ejercicio": 2024,
                    "numero_de_registro": 2,
                    "expediente": "EXP-2",
                    "conselleria_ent_adj": "Conselleria X",
                    "unidad": "Unidad Y",
                    "objeto": "Objeto base",
                    "tipo": "Servicios",
                    "procedimiento": "Abierto",
                    "tipo_tramitacion": "Ordinario",
                    "clase_de_contrato": "Menor",
                    "lugar_ejecucion": "Valencia",
                    "nombre_o_razon_social": "Proveedor SA",
                    "cif_nif_enmascarado": "A12345678",
                    "nacionalidad_adjudicatario": "España",
                    "imp_total_adjud": "121,00",
                    "imp_adjud_iva": "21,00",
                    "imp_adjud_sin_iva": "100,00",
                    "importe_total_licit": "242,00",
                    "importe_licit_iva": "42,00",
                    "importe_licit_sin_iva": "200,00",
                    "imp_definitivo_contrato": "121,00",
                    "valor_estimado_sin_iva": "200,00",
                    "fecha_adjudicacion": "2024/03/12 00:00:00.000000000",
                    "fecha_formalizacion": "2024/03/15 00:00:00.000000000",
                    "plazo_de_ejecucion": 12,
                    "unidades_plazo_ejecucion": "Meses",
                    "numero_de_lotes": 2,
                    "num_empresas_invitadas": 3,
                    "num_licitadores": 4,
                    "acuerdo_contrato_marco": "No",
                    "sistema_de_precios": "A tanto alzado",
                    "forma_de_adjudicacion": "Pluralidad de criterios",
                    "finan_con_fondos_europeos": "No",
                    "pyme": "Sí",
                    "contrato_reservado": "No",
                    "codigo_cpv": "12345678-9",
                    "cpv": "Servicio de prueba",
                    "url_licitacion": "https://placsp.example/exp-2",
                }
            ]
        )
        detail_df = pd.DataFrame(
            [
                {
                    "detail_key": "2024:2",
                    "detail_status": "not_found",
                    "detail_last_error": "search_0",
                }
            ]
        )

        fallback = ccaa_valencia_detail.build_base_fallback_dataframe(base_df, detail_df)

        self.assertEqual(len(fallback), 1)
        row = fallback.iloc[0]
        self.assertEqual(row["detail_status"], "done_base_fallback")
        self.assertTrue(row["detail_es_menor"])
        self.assertEqual(row["detail_cpv_principal"], "12345678-9 - Servicio de prueba")
        self.assertEqual(row["detail_source_kind"], "base_row_fallback")
        self.assertEqual(row["detail_url_placsp"], "https://placsp.example/exp-2")

    def test_build_candidates_counts_missing_keys(self):
        dataframe = pd.DataFrame(
            [
                {"ejercicio": 2024, "numero_de_registro": 1, "expediente": "A", "detail_key": "2024:1"},
                {"ejercicio": None, "numero_de_registro": 2, "expediente": "B", "detail_key": None},
            ]
        )

        candidates, missing = ccaa_valencia_detail.build_candidates(dataframe)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].detail_key, "2024:1")
        self.assertEqual(missing, 1)


if __name__ == "__main__":
    unittest.main()
