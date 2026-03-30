import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_andalucia.py"
SPEC = importlib.util.spec_from_file_location("ccaa_andalucia", MODULE_PATH)
ccaa_andalucia = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ccaa_andalucia)


class AndaluciaScraperTests(unittest.TestCase):
    def test_dt_extracts_iso_date_prefix(self):
        self.assertEqual(ccaa_andalucia._dt("2026-03-23T14:31:00+0100"), "2026-03-23")
        self.assertEqual(ccaa_andalucia._dt(None), "")

    def test_flatten_keeps_expected_columns(self):
        record = ccaa_andalucia.flatten(
            {
                "idExpediente": 123,
                "numeroExpediente": "EXP-2026-001",
                "titulo": "Contrato de prueba",
                "tipoContrato": {"codigo": "SERV", "descripcion": "Servicios"},
                "perfilContratante": {
                    "codigo": "SYBS03",
                    "descripcion": "Servicio Andaluz de Salud",
                    "codigoDir3": "A01000000",
                },
                "estado": {"codigo": "ADJ", "nombre": "Adjudicado"},
                "importeLicitacion": 100.0,
                "valorEstimado": 90.0,
                "fechaPublicacion": "2026-01-15T10:00:00+0100",
                "fechaLimitePresentacion": "2026-01-31T23:59:59+0100",
                "codigoProcedimiento": "9",
                "codigoTipoTramitacion": "O",
                "formaPresentacion": "M",
                "cofinanciadoUE": "N",
                "subastaElectronica": "N",
                "sistemaRacionalizacion": "SCON_BAC",
                "codigosCpv": ["33140000", "33190000"],
                "provinciasEjecucion": ["29", "41"],
                "adjudicaciones": [
                    {
                        "nifAdjudicatario": "A12345678;",
                        "importeAdjudicacion": 95.0,
                        "importeAdjudicacionConIva": 114.95,
                    },
                    {"nifAdjudicatario": "B87654321;"},
                ],
                "anuncios": [
                    {"fechaPublicacion": "2026-01-10T08:00:00+0100"},
                    {"fechaPublicacion": "2026-01-20T08:00:00+0100"},
                ],
                "mediosPublicacion": [{"codigo": "DOUE"}, {"codigo": "PLACSP"}],
                "lotes": [1, 2],
            }
        )

        self.assertEqual(record["id_expediente"], 123)
        self.assertEqual(record["tipo_contrato_codigo"], "SERV")
        self.assertEqual(record["codigo_perfil"], "SYBS03")
        self.assertEqual(record["adjudicatario_nif"], "A12345678")
        self.assertEqual(record["todos_adjudicatarios_nif"], "A12345678;B87654321")
        self.assertEqual(record["cpv"], "33140000;33190000")
        self.assertEqual(record["provincias_ejecucion"], "29;41")
        self.assertEqual(record["num_adjudicaciones"], 2)
        self.assertEqual(record["num_anuncios"], 2)
        self.assertEqual(record["num_lotes"], 2)
        self.assertTrue(record["url_detalle"].endswith("idExpediente=123"))

    def test_cnt_propagates_scraper_error(self):
        with patch.object(
            ccaa_andalucia,
            "es",
            side_effect=ccaa_andalucia.ScraperError("boom"),
        ):
            with self.assertRaises(ccaa_andalucia.ScraperError):
                ccaa_andalucia.cnt()

    def test_es_raises_on_non_retryable_http_error(self):
        response = Mock()
        response.ok = False
        response.status_code = 403
        response.text = "forbidden"

        with patch.object(ccaa_andalucia.S, "post", return_value=response):
            with self.assertRaises(ccaa_andalucia.ScraperError):
                ccaa_andalucia.es({"query": {"match_all": {}}}, timeout=1)

    @unittest.skipUnless(ccaa_andalucia.HAS_PANDAS, "pandas no disponible")
    def test_save_csv_and_parquet(self):
        records = [
            {
                "id_expediente": 1,
                "numero_expediente": "EXP-1",
                "titulo": "Uno",
                "num_adjudicaciones": 0,
                "num_lotes": 0,
                "num_anuncios": 0,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch.object(ccaa_andalucia, "DATA_DIR", output_dir):
                csv_path = ccaa_andalucia.save_csv(records, "test.csv")
                parquet_path = ccaa_andalucia.save_parquet(records, "test.parquet")

            self.assertTrue(csv_path.exists())
            self.assertTrue(parquet_path.exists())

    def test_main_handles_help_and_unknown_command(self):
        with patch("sys.stdout", new_callable=io.StringIO) as stdout:
            rc = ccaa_andalucia.main([])
        self.assertEqual(rc, 0)
        self.assertIn("scrape-std", stdout.getvalue())

        with patch("sys.stdout", new_callable=io.StringIO) as stdout:
            rc = ccaa_andalucia.main(["desconocido"])
        self.assertEqual(rc, 1)
        self.assertIn("Comando desconocido", stdout.getvalue())

    def test_paginate_multisort_continues_after_duplicate_page(self):
        def build_hit(expediente_id):
            return {"_source": {"idExpediente": expediente_id}}

        responses = [
            {"hits": {"hits": [build_hit(1), build_hit(2)]}},
            {"hits": {"hits": [build_hit(1), build_hit(2)]}},
            {"hits": {"hits": [build_hit(3)]}},
            {"hits": {"hits": []}},
        ]

        with patch.object(ccaa_andalucia, "SORT_COMBOS", [[{"idExpediente": "asc"}]]), patch.object(
            ccaa_andalucia,
            "MAX_FROM",
            200,
        ), patch.object(
            ccaa_andalucia,
            "PAGE_SIZE",
            100,
        ), patch.object(
            ccaa_andalucia,
            "DELAY",
            0,
        ), patch.object(
            ccaa_andalucia,
            "es",
            side_effect=responses,
        ):
            records = ccaa_andalucia.paginate_multisort(target=3)

        self.assertEqual([record["id_expediente"] for record in records], [1, 2, 3])

    def test_scrape_recursive_uses_known_total_to_avoid_duplicate_count(self):
        with patch.object(ccaa_andalucia, "cnt") as mocked_count, patch.object(
            ccaa_andalucia,
            "paginate",
            return_value=([{"id_expediente": 1}], 1),
        ):
            records = []
            seen = set()
            got = ccaa_andalucia.scrape_recursive(
                must=[{"match": {"codigoProcedimiento": 20}}],
                must_not=[],
                label="known_total_case",
                all_records=records,
                seen_ids=seen,
                known_total=1,
            )

        mocked_count.assert_not_called()
        self.assertEqual(got, 1)
        self.assertEqual(len(records), 1)

    def test_get_perfiles_uses_cache_when_present(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "perfiles_cache.json"
            cache_path.write_text('["B","A"]', encoding="utf-8")

            with patch.object(ccaa_andalucia, "_PERFILES", None), patch.object(
                ccaa_andalucia,
                "PERFILES_CACHE_PATH",
                cache_path,
            ), patch.object(
                ccaa_andalucia,
                "es",
            ) as mocked_es:
                perfiles = ccaa_andalucia.get_perfiles()

        mocked_es.assert_not_called()
        self.assertEqual(perfiles, ["A", "B"])

    def test_build_unknown_standard_exclusions_includes_known_procs(self):
        exclusions = ccaa_andalucia.build_unknown_standard_exclusions(
            [ccaa_andalucia.mn("estado.codigo", "BRR")]
        )
        proc_exclusions = [item for item in exclusions if "codigoProcedimiento" in str(item)]
        self.assertEqual(len(proc_exclusions), len([proc for proc in ccaa_andalucia.PROCS if proc != 9]))

    def test_scrape_std_adds_unknown_proc_branch(self):
        counts = iter([100, 10, 5])
        scrape_calls = []

        def fake_cnt(*args, **kwargs):
            return next(counts)

        def fake_scrape_recursive(must, must_not, label, all_records, seen, dim_idx=0, known_total=None):
            scrape_calls.append((label, must, must_not, known_total))
            if label == "p2":
                all_records.extend([{"id_expediente": 1}, {"id_expediente": 2}])
                seen.update({1, 2})
                return 2
            if label == "p_unknown":
                all_records.append({"id_expediente": 3})
                seen.add(3)
                return 1
            return 0

        with patch.object(ccaa_andalucia, "PROCS", [2, 9]), patch.object(
            ccaa_andalucia,
            "init",
        ), patch.object(
            ccaa_andalucia,
            "cnt",
            side_effect=fake_cnt,
        ), patch.object(
            ccaa_andalucia,
            "scrape_recursive",
            side_effect=fake_scrape_recursive,
        ), patch.object(
            ccaa_andalucia,
            "save_csv",
        ):
            records = ccaa_andalucia.scrape_std()

        self.assertEqual(len(records), 3)
        labels = [call[0] for call in scrape_calls]
        self.assertIn("p2", labels)
        self.assertIn("p_unknown", labels)


if __name__ == "__main__":
    unittest.main()
