import importlib.util
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

DOWNLOAD_MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_valencia.py"
DOWNLOAD_SPEC = importlib.util.spec_from_file_location("ccaa_valencia", DOWNLOAD_MODULE_PATH)
ccaa_valencia = importlib.util.module_from_spec(DOWNLOAD_SPEC)
DOWNLOAD_SPEC.loader.exec_module(ccaa_valencia)

PARQUET_MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_valencia_parquet.py"
PARQUET_SPEC = importlib.util.spec_from_file_location("ccaa_valencia_parquet", PARQUET_MODULE_PATH)
ccaa_valencia_parquet = importlib.util.module_from_spec(PARQUET_SPEC)
PARQUET_SPEC.loader.exec_module(ccaa_valencia_parquet)


class ValenciaDownloadTests(unittest.TestCase):
    def test_defaults_live_under_repo_tree(self):
        self.assertEqual(ccaa_valencia.DEFAULT_DOWNLOAD_DIR, REPO_ROOT / "valencia_datos")
        self.assertEqual(ccaa_valencia.DEFAULT_LOG_PATH, DOWNLOAD_MODULE_PATH.parent / "ccaa_valencia.log")

    def test_parse_and_resolve_categories(self):
        self.assertEqual(
            ccaa_valencia.parse_categories_arg("contratacion, lobbies,transporte"),
            ["contratacion", "lobbies", "transporte"],
        )
        self.assertEqual(
            ccaa_valencia.resolve_selected_categories("contratacion,lobbies"),
            ["contratacion", "lobbies"],
        )
        with self.assertRaises(ValueError):
            ccaa_valencia.resolve_selected_categories("inventada")

    def test_build_runtime_paths_uses_download_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = ccaa_valencia.build_runtime_paths(Path(tmpdir) / "nested" / "valencia_datos")

            self.assertTrue(paths["download_dir"].exists())
            self.assertEqual(paths["manifest"].name, "valencia_manifest.json")
            self.assertEqual(paths["summary"].name, "valencia_summary.json")

    def test_extract_csv_resources_filters_non_csv(self):
        info = {
            "resources": [
                {"id": "a", "format": "CSV", "name": "uno"},
                {"id": "b", "format": "JSON", "name": "dos"},
                {"id": "c", "format": "text/csv", "name": "tres"},
            ]
        }

        resources = ccaa_valencia.extract_csv_resources(info)

        self.assertEqual([resource["id"] for resource in resources], ["a", "c"])

    def test_build_output_filename_handles_collisions(self):
        used_names = set()
        first = ccaa_valencia.build_output_filename({"id": "abc12345", "name": "Grupos de interés"}, used_names)
        second = ccaa_valencia.build_output_filename({"id": "xyz98765", "name": "Grupos de interés"}, used_names)

        self.assertEqual(first, "Grupos_de_interés.csv")
        self.assertEqual(second, "Grupos_de_interés__xyz98765.csv")

    def test_should_download_resource_uses_manifest_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "lobbies" / "recurso.csv"
            output_path.parent.mkdir(parents=True)
            output_path.write_text("id\n1\n", encoding="utf-8")
            resource = ccaa_valencia.ResourceDownload(
                category="lobbies",
                dataset_id="sec-regia-grupos",
                dataset_title="Grupos",
                package_metadata_modified="2026-04-01T10:00:00",
                resource_id="abc",
                resource_name="recurso",
                resource_url="https://example.com/data.csv",
                resource_format="CSV",
                resource_last_modified="2026-04-01T09:00:00",
                output_path=output_path,
            )

            previous_manifest = {
                "abc": {
                    "url": "https://example.com/data.csv",
                    "package_metadata_modified": "2026-04-01T10:00:00",
                    "resource_last_modified": "2026-04-01T09:00:00",
                }
            }

            self.assertFalse(ccaa_valencia.should_download_resource(resource, previous_manifest))

            previous_manifest["abc"]["resource_last_modified"] = "2026-04-02T09:00:00"
            self.assertTrue(ccaa_valencia.should_download_resource(resource, previous_manifest))


class ValenciaParquetTests(unittest.TestCase):
    def test_parquet_defaults_live_under_repo_tree(self):
        self.assertEqual(ccaa_valencia_parquet.DEFAULT_INPUT_DIR, REPO_ROOT / "valencia_datos")
        self.assertEqual(ccaa_valencia_parquet.DEFAULT_OUTPUT_DIR, REPO_ROOT / "valencia")
        self.assertEqual(
            ccaa_valencia_parquet.DEFAULT_LOG_PATH,
            PARQUET_MODULE_PATH.parent / "ccaa_valencia_parquet.log",
        )

    def test_detect_encoding_and_separator(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "sample.csv"
            csv_path.write_text("col1;col2\nuno;dos\n", encoding="utf-8")

            encoding, separator = ccaa_valencia_parquet.detect_encoding_and_sep(csv_path)

            self.assertEqual(encoding, "utf-8")
            self.assertEqual(separator, ";")

    def test_resolve_selected_categories_uses_existing_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir)
            (input_dir / "contratacion").mkdir()
            (input_dir / "lobbies").mkdir()

            self.assertEqual(
                ccaa_valencia_parquet.resolve_selected_categories(input_dir, None),
                ["contratacion", "lobbies"],
            )
            self.assertEqual(
                ccaa_valencia_parquet.resolve_selected_categories(input_dir, "lobbies"),
                ["lobbies"],
            )
            with self.assertRaises(ValueError):
                ccaa_valencia_parquet.resolve_selected_categories(input_dir, "inventada")

    def test_should_convert_respects_force_and_mtime(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "input.csv"
            parquet_path = Path(tmpdir) / "out.parquet"
            csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
            parquet_path.write_text("fake", encoding="utf-8")

            self.assertFalse(ccaa_valencia_parquet.should_convert(csv_path, parquet_path))

            time.sleep(0.01)
            csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
            self.assertTrue(ccaa_valencia_parquet.should_convert(csv_path, parquet_path))
            self.assertTrue(ccaa_valencia_parquet.should_convert(csv_path, parquet_path, force=True))


if __name__ == "__main__":
    unittest.main()
