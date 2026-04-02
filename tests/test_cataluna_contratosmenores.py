import importlib.util
import json
import ssl
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_cataluna_contratosmenores.py"
SPEC = importlib.util.spec_from_file_location("cataluna_contratosmenores", MODULE_PATH)
cataluna_contratosmenores = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cataluna_contratosmenores)


class CatalunyaContratosMenoresTests(unittest.TestCase):
    def test_defaults_live_under_repo_tree(self):
        self.assertEqual(
            cataluna_contratosmenores.DEFAULT_OUTPUT_PATH,
            REPO_ROOT / "catalunya" / "contratacion" / "contractacio_menors.parquet",
        )
        self.assertEqual(
            cataluna_contratosmenores.DEFAULT_LOG_PATH,
            MODULE_PATH.parent / "ccaa_cataluna_contratosmenores.log",
        )

    def test_parse_fases_arg_and_selection(self):
        self.assertEqual(
            cataluna_contratosmenores.parse_fases_arg("0, 40,1100"),
            [0, 40, 1100],
        )
        self.assertEqual(
            cataluna_contratosmenores.resolve_selected_fases(raw_fases="0,40"),
            [0, 40],
        )
        self.assertEqual(
            cataluna_contratosmenores.resolve_selected_fases(include_agregadas=False),
            cataluna_contratosmenores.FASES_NORMAL,
        )

    def test_resolve_selected_fases_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            cataluna_contratosmenores.resolve_selected_fases(raw_fases="9999")

    def test_build_output_artifact_paths_creates_expected_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = cataluna_contratosmenores.build_output_artifact_paths(
                Path(tmpdir) / "nested" / "contractacio_menors.parquet"
            )

            self.assertEqual(paths["output"].name, "contractacio_menors.parquet")
            self.assertEqual(paths["raw"].name, "contractacio_menors_raw.parquet")
            self.assertEqual(paths["checkpoint"].name, "contractacio_menors_checkpoint.json")
            self.assertEqual(paths["analysis"].name, "contractacio_menors_duplicate_analysis.json")
            self.assertEqual(paths["summary"].name, "contractacio_menors_summary.json")
            self.assertEqual(paths["branch_summary"].name, "contractacio_menors_branch_summary.json")
            self.assertEqual(paths["branches_dir"].name, "contractacio_menors_branches")
            self.assertTrue(paths["output"].parent.exists())

    def test_checkpoint_roundtrip_preserves_selected_fases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "checkpoint.json"
            checkpoint = cataluna_contratosmenores.Checkpoint(
                completed_fases=[0, 40],
                completed_branches=["faseVigent-40__ambit-1500002"],
                total_records_so_far=123,
                requests_made=456,
                selected_fases=[0, 40, 1100],
            )
            checkpoint.save(checkpoint_path)

            restored = cataluna_contratosmenores.Checkpoint.load(checkpoint_path)
            self.assertEqual(restored.completed_fases, [0, 40])
            self.assertEqual(restored.completed_branches, ["faseVigent-40__ambit-1500002"])
            self.assertEqual(restored.total_records_so_far, 123)
            self.assertEqual(restored.requests_made, 456)
            self.assertEqual(restored.selected_fases, [0, 40, 1100])

    def test_build_ssl_context_supports_verified_and_insecure_modes(self):
        verified = cataluna_contratosmenores.build_ssl_context()
        insecure = cataluna_contratosmenores.build_ssl_context(verify_ssl=False)

        self.assertIsInstance(verified, ssl.SSLContext)
        self.assertFalse(insecure)

    def test_should_segment_by_organ_first_for_agregadas(self):
        self.assertTrue(
            cataluna_contratosmenores.should_segment_by_organ_first(
                {"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393}
            )
        )
        self.assertTrue(
            cataluna_contratosmenores.should_segment_by_organ_first(
                {"faseVigent": 1000, "ambit": 1500001, "tipusContracte": 394}
            )
        )
        self.assertFalse(
            cataluna_contratosmenores.should_segment_by_organ_first(
                {"faseVigent": 800, "ambit": 1500002, "tipusContracte": 393}
            )
        )
        self.assertFalse(
            cataluna_contratosmenores.should_segment_by_organ_first(
                {"faseVigent": 50, "ambit": 1500001, "tipusContracte": 393}
            )
        )

    def test_smart_deduplicate_prefers_more_complete_rows(self):
        df = pd.DataFrame(
            [
                {"id": 1, "descripcio": "A", "dataPublicacio": None, "organ": None},
                {"id": 1, "descripcio": "A", "dataPublicacio": "2026-04-02", "organ": "Ajuntament"},
                {"id": 2, "descripcio": "B", "dataPublicacio": "2026-04-01", "organ": "Diputació"},
            ]
        )

        deduped = cataluna_contratosmenores.smart_deduplicate(
            df,
            ["id", "descripcio"],
            prefer_cols=["dataPublicacio"],
        )

        self.assertEqual(len(deduped), 2)
        kept = deduped.loc[deduped["id"] == 1].iloc[0]
        self.assertEqual(kept["organ"], "Ajuntament")

    def test_analyze_duplicates_reports_differences(self):
        df = pd.DataFrame(
            [
                {"id": 1, "descripcio": "A", "organ": "X", "import": 10},
                {"id": 1, "descripcio": "A", "organ": "Y", "import": 10},
                {"id": 2, "descripcio": "B", "organ": "Z", "import": 20},
            ]
        )

        analysis = cataluna_contratosmenores.analyze_duplicates(df, ["id", "descripcio"])

        self.assertEqual(analysis["duplicate_rows"], 2)
        self.assertEqual(analysis["duplicate_groups"], 1)
        self.assertEqual(analysis["differing_columns"][0]["column"], "organ")

    def test_write_run_summary_persists_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary_path = Path(tmpdir) / "summary.json"
            cataluna_contratosmenores.write_run_summary(
                summary_path,
                {"selected_fases": [0, 40], "rows": 12},
            )

            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["selected_fases"], [0, 40])
            self.assertEqual(payload["rows"], 12)

    def test_upsert_branch_summary_replaces_existing_branch(self):
        summaries = [{"branch_key": "faseVigent-900__ambit-1500001", "rows": 10}]
        cataluna_contratosmenores.upsert_branch_summary(
            summaries,
            {"branch_key": "faseVigent-900__ambit-1500001", "rows": 25},
        )
        self.assertEqual(summaries, [{"branch_key": "faseVigent-900__ambit-1500001", "rows": 25}])

    def test_build_branch_file_path_uses_branch_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "contractacio_menors.parquet"
            branches_dir = Path(tmpdir) / "contractacio_menors_branches"
            branch_path = cataluna_contratosmenores.build_branch_file_path(
                output_path,
                branches_dir,
                "faseVigent-900__ambit-1500001__tipusContracte-393",
            )
            self.assertTrue(branch_path.parent.exists())
            self.assertEqual(
                branch_path.name,
                "contractacio_menors_faseVigent-900__ambit-1500001__tipusContracte-393.parquet",
            )

    def test_build_branch_key_prioritizes_phase_first(self):
        branch_key = cataluna_contratosmenores.build_branch_key(
            {"tipusContracte": 393, "ambit": 1500001, "faseVigent": 900}
        )
        self.assertEqual(branch_key, "faseVigent-900__ambit-1500001__tipusContracte-393")
        self.assertTrue(cataluna_contratosmenores.branch_key_belongs_to_phase(branch_key, 900))


class CatalunyaContratosMenoresAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_count_uses_cache(self):
        calls = []

        async def fake_fetch_json(_session, _url, params=None, stats=None):
            calls.append(params)
            return {"totalElements": 123}

        with patch.object(cataluna_contratosmenores, "fetch_json", side_effect=fake_fetch_json):
            stats = cataluna_contratosmenores.ScraperStats()
            cache = {}
            params = {"faseVigent": 40, "ambit": 1500002}
            count1 = await cataluna_contratosmenores.get_count(None, params, stats, count_cache=cache)
            count2 = await cataluna_contratosmenores.get_count(None, params, stats, count_cache=cache)

        self.assertEqual(count1, 123)
        self.assertEqual(count2, 123)
        self.assertEqual(len(calls), 1)
        self.assertEqual(stats.count_cache_hits, 1)

    async def test_build_branch_plan_splits_aggregated_phase_into_tipus_branches(self):
        async def fake_get_count(_session, params, _stats, count_cache=None):
            if params == {"faseVigent": 900}:
                return 10000
            if params == {"faseVigent": 900, "ambit": 1500001}:
                return 10000
            if params == {"faseVigent": 900, "ambit": 1500002}:
                return 200
            if params == {"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393}:
                return 10000
            if params == {"faseVigent": 900, "ambit": 1500001, "tipusContracte": 394}:
                return 25
            return 0

        with patch.object(cataluna_contratosmenores, "get_count", side_effect=fake_get_count):
            plan = await cataluna_contratosmenores.build_branch_plan(
                None,
                900,
                cataluna_contratosmenores.ScraperStats(),
                count_cache={},
            )

        self.assertEqual(
            [(entry.params, entry.strategy, entry.root_count) for entry in plan],
            [
                ({"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393}, "organ-first", 10000),
                ({"faseVigent": 900, "ambit": 1500001, "tipusContracte": 394}, "organ-first", 25),
                ({"faseVigent": 900, "ambit": 1500002}, "ambit", 200),
            ],
        )

    async def test_aggregated_fases_segment_by_organ_before_procedure(self):
        async def fake_get_count(_session, params, _stats, count_cache=None):
            if params == {"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393}:
                return 10000
            if params == {
                "faseVigent": 900,
                "ambit": 1500001,
                "tipusContracte": 393,
                "organ": 42,
            }:
                return 5
            if "procedimentAdjudicacio" in params:
                raise AssertionError("Aggregated fases should not probe procediment before organ")
            return 0

        async def fake_get_organs_for_ambit(_session, ambit_id, _stats):
            self.assertEqual(ambit_id, 1500001)
            return [{"id": 42}]

        async def fake_scrape_segment(_session, params, _stats, desc="", max_pages=100, both_orders=False):
            self.assertEqual(
                params,
                {"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393, "organ": 42},
            )
            return [{"id": 1, "descripcio": "fallback organ row"}]

        with patch.object(cataluna_contratosmenores, "get_count", side_effect=fake_get_count), patch.object(
            cataluna_contratosmenores,
            "get_organs_for_ambit",
            side_effect=fake_get_organs_for_ambit,
        ), patch.object(
            cataluna_contratosmenores,
            "scrape_segment",
            side_effect=fake_scrape_segment,
        ):
            rows = await cataluna_contratosmenores.scrape_with_segmentation(
                session=None,
                base_params={"faseVigent": 900, "ambit": 1500001, "tipusContracte": 393},
                stats=cataluna_contratosmenores.ScraperStats(),
                organs_cache={},
                count_cache={},
            )

        self.assertEqual(rows, [{"id": 1, "descripcio": "fallback organ row"}])

    async def test_procedure_segmentation_falls_back_to_organ_when_all_children_are_empty(self):
        async def fake_get_count(_session, params, _stats, count_cache=None):
            if params == {"faseVigent": 50, "ambit": 1500001, "tipusContracte": 393}:
                return 10000
            if params == {
                "faseVigent": 50,
                "ambit": 1500001,
                "tipusContracte": 393,
                "organ": 42,
            }:
                return 5
            if (
                params.get("faseVigent") == 50
                and params.get("ambit") == 1500001
                and params.get("tipusContracte") == 393
                and "procedimentAdjudicacio" in params
            ):
                return 0
            return 0

        async def fake_get_organs_for_ambit(_session, ambit_id, _stats):
            self.assertEqual(ambit_id, 1500001)
            return [{"id": 42}]

        async def fake_scrape_segment(_session, params, _stats, desc="", max_pages=100, both_orders=False):
            self.assertEqual(
                params,
                {"faseVigent": 50, "ambit": 1500001, "tipusContracte": 393, "organ": 42},
            )
            return [{"id": 1, "descripcio": "fallback organ row"}]

        with patch.object(cataluna_contratosmenores, "get_count", side_effect=fake_get_count), patch.object(
            cataluna_contratosmenores,
            "get_organs_for_ambit",
            side_effect=fake_get_organs_for_ambit,
        ), patch.object(
            cataluna_contratosmenores,
            "scrape_segment",
            side_effect=fake_scrape_segment,
        ):
            rows = await cataluna_contratosmenores.scrape_with_segmentation(
                session=None,
                base_params={"faseVigent": 50, "ambit": 1500001, "tipusContracte": 393},
                stats=cataluna_contratosmenores.ScraperStats(),
                organs_cache={},
                count_cache={},
            )

        self.assertEqual(rows, [{"id": 1, "descripcio": "fallback organ row"}])

    async def test_organ_level_zero_procedure_retries_same_organ_with_both_orders(self):
        async def fake_get_count(_session, params, _stats, count_cache=None):
            if params == {
                "faseVigent": 900,
                "ambit": 1500001,
                "tipusContracte": 394,
                "organ": 202252,
            }:
                return 10000
            if "procedimentAdjudicacio" in params:
                return 0
            return 0

        async def fake_scrape_segment(_session, params, _stats, desc="", max_pages=100, both_orders=False):
            self.assertEqual(
                params,
                {
                    "faseVigent": 900,
                    "ambit": 1500001,
                    "tipusContracte": 394,
                    "organ": 202252,
                },
            )
            self.assertTrue(both_orders)
            return [{"id": 99, "descripcio": "organ both orders row"}]

        with patch.object(cataluna_contratosmenores, "get_count", side_effect=fake_get_count), patch.object(
            cataluna_contratosmenores,
            "scrape_segment",
            side_effect=fake_scrape_segment,
        ):
            rows = await cataluna_contratosmenores.scrape_with_segmentation(
                session=None,
                base_params={
                    "faseVigent": 900,
                    "ambit": 1500001,
                    "tipusContracte": 394,
                    "organ": 202252,
                },
                stats=cataluna_contratosmenores.ScraperStats(),
                organs_cache={},
                count_cache={},
            )

        self.assertEqual(rows, [{"id": 99, "descripcio": "organ both orders row"}])


if __name__ == "__main__":
    unittest.main()
