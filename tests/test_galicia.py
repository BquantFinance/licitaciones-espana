import importlib.util
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "galicia" / "scraper_galicia.py"
SPEC = importlib.util.spec_from_file_location("scraper_galicia", MODULE_PATH)
scraper_galicia = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scraper_galicia)


DETAIL_HTML = """
<html>
  <head>
    <title>Detalle procedemento: 824839 - Contratos Públicos de Galicia</title>
  </head>
  <body>
    <h2>Información del procedimiento</h2>
    <dl>
      <dt>Referencia</dt><dd>REF-2026-001</dd>
      <dt>Tipo de tramitación</dt><dd>Ordinaria</dd>
      <dt>Tipo de procedimiento</dt><dd>Abierto</dd>
      <dt>Tipo de contrato</dt><dd>Servicios</dd>
      <dt>Orzamento base de licitación</dt><dd>1.234,56 €</dd>
      <dt>Valor estimado</dt><dd>2.345,67 €</dd>
      <dt>Nº lotes</dt><dd>2</dd>
      <dt>Fecha de difusión en la Plataforma de Contratos:</dt><dd>23/03/2026</dd>
      <dt>Fecha formalización:</dt><dd>24/03/2026</dd>
      <dt>Órgano:</dt><dd>SERGAS</dd>
      <dt>Correo electrónico:</dt><dd>contratacion@example.com</dd>
      <dt>Compra pública estratéxica:</dt><dd>Sí</dd>
    </dl>
    <table>
      <tr><th>Perfil</th><th>BOP</th><th>DOG</th><th>BOE</th><th>Fecha envío DOUE</th></tr>
      <tr><td>Perfil</td><td></td><td></td><td></td><td>23/03/2026</td></tr>
    </table>
    <table>
      <tr><th>código CPV</th><th>Lote</th><th>Fecha difusión</th></tr>
      <tr><td>12345678</td><td>1</td><td>23/03/2026</td></tr>
      <tr><td>87654321</td><td>2</td><td>23/03/2026</td></tr>
    </table>
    <table>
      <tr><th>NUT</th><th>Lote</th><th>Fecha difusión</th></tr>
      <tr><td>ES111</td><td>1</td><td>23/03/2026</td></tr>
    </table>
    <table>
      <tr><th>Título</th><th>Fecha</th><th>Estado</th><th>Descarga</th></tr>
      <tr><td>Pliego</td><td>23/03/2026</td><td>Publicado</td><td><a href="/doc1.pdf">PDF</a></td></tr>
      <tr><td>Anuncio</td><td>24/03/2026</td><td>Publicado</td><td><a href="/doc2.pdf">PDF</a></td></tr>
    </table>
  </body>
</html>
"""

DETAIL_HTML_WITH_MALFORMED_LINK = """
<html>
  <head>
    <title>Detalle procedemento: 999999 - Contratos Públicos de Galicia</title>
  </head>
  <body>
    <h2>Información del procedimiento</h2>
    <dl>
      <dt>Referencia</dt><dd>REF-BAD-LINK</dd>
      <dt>Tipo de contrato</dt><dd>Servicios <a href="http://[broken">Enlace roto</a></dd>
    </dl>
    <table>
      <tr><th>Título</th><th>Fecha</th><th>Estado</th><th>Descarga</th></tr>
      <tr><td>Documento</td><td>24/03/2026</td><td>Publicado</td><td><a href="http://[bad-doc">PDF</a></td></tr>
    </table>
  </body>
</html>
"""


class GaliciaScraperTests(unittest.TestCase):
    def test_paginate_lic_raises_on_partial_http_failure(self):
        with patch.object(scraper_galicia.Session, "_init", return_value=None):
            session = scraper_galicia.Session()

        visit = Mock(status_code=200, ok=True)
        visit.json.return_value = {}
        page1 = Mock(status_code=200, ok=True)
        page1.json.return_value = {
            "recordsTotal": 150,
            "data": [{"id": i, "objeto": f"Contrato {i}"} for i in range(100)],
        }
        page2 = Mock(status_code=403, ok=False)

        with patch.object(
            session.s,
            "request",
            side_effect=[visit, page1, page2],
        ), patch("sys.stdout", new_callable=io.StringIO):
            with self.assertRaises(scraper_galicia.ScraperError):
                scraper_galicia.paginate_lic(session, 48)

    def test_to_dataframe_cleans_html_dates_importes_and_deduplicates(self):
        records = [
            {
                "id": 1,
                "_tipo": "CM",
                "_organismo_id": 48,
                "objeto": "<b>Contrato</b>   de  prueba",
                "importe": "1.234,56 €",
                "publicado": "2026-03-01T10:00:00+0100",
            },
            {
                "id": 1,
                "_tipo": "CM",
                "_organismo_id": 48,
                "objeto": "<b>Contrato</b>   de  prueba",
                "importe": "1.234,56 €",
                "publicado": "2026-03-01T10:00:00+0100",
            },
        ]

        df = scraper_galicia.to_dataframe(records)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["objeto"], "Contrato de prueba")
        self.assertEqual(df.iloc[0]["importe"], 1234.56)
        self.assertEqual(df.iloc[0]["publicado"].date().isoformat(), "2026-03-01")

    def test_parse_detail_html_maps_pairs_and_tables(self):
        parsed = scraper_galicia.parse_detail_html(DETAIL_HTML)
        mapped = parsed["mapped"]

        self.assertEqual(mapped["detail_referencia"], "REF-2026-001")
        self.assertEqual(mapped["detail_tipo_tramitacion"], "Ordinaria")
        self.assertEqual(mapped["detail_tipo_contrato"], "Servicios")
        self.assertEqual(mapped["detail_presupuesto_base_eur"], 1234.56)
        self.assertEqual(mapped["detail_valor_estimado_eur"], 2345.67)
        self.assertEqual(mapped["detail_num_lotes"], 2.0)
        self.assertEqual(mapped["detail_cpv_codes"], "12345678, 87654321")
        self.assertEqual(mapped["detail_nuts_codes"], "ES111")
        self.assertEqual(mapped["detail_documentos_count"], 2)
        self.assertEqual(mapped["detail_publicaciones_count"], 1)
        self.assertEqual(mapped["detail_fecha_difusion"], "2026-03-23T00:00:00")
        self.assertEqual(mapped["detail_fecha_formalizacion"], "2026-03-24T00:00:00")

    def test_parse_detail_html_tolerates_malformed_links(self):
        parsed = scraper_galicia.parse_detail_html(DETAIL_HTML_WITH_MALFORMED_LINK)

        self.assertEqual(parsed["mapped"]["detail_referencia"], "REF-BAD-LINK")
        self.assertEqual(parsed["pairs"][1]["links"], ["http://[broken"])
        self.assertEqual(parsed["tables"][0]["rows"][0]["links"][-1], ["http://[bad-doc"])

    def test_build_detail_payload_supports_lic_and_cm(self):
        lic_payload = scraper_galicia.build_detail_payload("LIC", "123", 48)
        cm_payload = scraper_galicia.build_detail_payload("CM", "456", 33)

        self.assertEqual(lic_payload["N"], "123")
        self.assertEqual(lic_payload["S"], "C")
        self.assertEqual(cm_payload["N"], "CM456")
        self.assertEqual(cm_payload["S"], "CM")
        self.assertEqual(cm_payload["OR"], "33")

    def test_session_get_json_raises_scraper_error_on_non_retryable_http(self):
        with patch.object(scraper_galicia.Session, "_init", return_value=None):
            session = scraper_galicia.Session()

        response = Mock(status_code=403, ok=False)
        with patch.object(session.s, "request", return_value=response):
            with self.assertRaises(scraper_galicia.ScraperError):
                session.get_json("https://example.com/test", {})

    def test_discover_raises_when_any_probe_fails(self):
        with patch.object(scraper_galicia.Session, "_init", return_value=None):
            session = scraper_galicia.Session()

        def fake_get_json(url, params, retry=0, headers=None, count_error=True):
            del params, retry, headers, count_error
            if "licitaciones/table" in url:
                raise scraper_galicia.ScraperError("boom")
            return {"recordsTotal": 0}

        with patch.object(session, "get_json", side_effect=fake_get_json), patch(
            "sys.stdout",
            new_callable=io.StringIO,
        ):
            with self.assertRaises(scraper_galicia.ScraperError):
                scraper_galicia.discover(session, max_id=1, workers=1)

    def test_append_base_records_writes_stable_schema(self):
        records = [
            {
                "id": 1,
                "_tipo": "LIC",
                "_organismo_id": 48,
                "objeto": "Contrato",
                "importe": 100.0,
                "publicado": "2026-03-01",
                "estadoDesc": "Publicado",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            written = scraper_galicia.append_base_records(records, output_dir)
            self.assertEqual(written, 1)
            df = pd.read_csv(output_dir / scraper_galicia.BASE_CSV_NAME, sep=";")
            self.assertEqual(list(df.columns), scraper_galicia.BASE_EXPORT_FIELDS)
            self.assertEqual(df.iloc[0]["estadoDesc"], "Publicado")

    def test_iter_detail_batches_skips_done_cache_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            scraper_galicia.append_base_records(
                [
                    {"id": 1, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Uno"},
                    {"id": 2, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Dos"},
                ],
                output_dir,
            )
            conn = scraper_galicia.init_detail_db(output_dir)
            scraper_galicia.persist_detail_results(
                conn,
                [
                    {
                        "record_type": "LIC",
                        "record_id": "1",
                        "organismo_id": 48,
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "updated_at": "2026-03-23T00:00:00",
                        "detail_url": "https://example.com/1",
                        "page_title": "Detalle procedemento: 1",
                        "html_sha256": "abc",
                        "mapped_json": json.dumps({"detail_referencia": "REF-1"}),
                        "raw_gzip": None,
                    }
                ],
            )

            batches = list(
                scraper_galicia.iter_detail_batches(
                    output_dir / scraper_galicia.BASE_CSV_NAME,
                    conn,
                    only_type="all",
                    batch_size=10,
                    force=False,
                )
            )

            self.assertEqual(len(batches), 1)
            self.assertEqual([item["id"] for item in batches[0]], ["2"])

    def test_iter_detail_batches_retryable_only_can_target_and_ignore_max(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            scraper_galicia.append_base_records(
                [
                    {"id": 1, "_tipo": "CM", "_organismo_id": 11, "objeto": "Uno"},
                    {"id": 2, "_tipo": "CM", "_organismo_id": 11, "objeto": "Dos"},
                    {"id": 3, "_tipo": "CM", "_organismo_id": 11, "objeto": "Tres"},
                    {"id": 4, "_tipo": "CM", "_organismo_id": 11, "objeto": "Cuatro"},
                ],
                output_dir,
            )
            conn = scraper_galicia.init_detail_db(output_dir)
            scraper_galicia.persist_detail_results(
                conn,
                [
                    {
                        "record_type": "CM",
                        "record_id": "1",
                        "organismo_id": 11,
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "updated_at": "2026-03-26T00:00:00",
                        "detail_url": "https://example.com/1",
                        "page_title": "Detalle procedemento: 1",
                        "html_sha256": "abc",
                        "mapped_json": json.dumps({"detail_referencia": "REF-1"}),
                        "raw_gzip": None,
                    },
                    {
                        "record_type": "CM",
                        "record_id": "2",
                        "organismo_id": 11,
                        "status": "retryable",
                        "attempts": 2,
                        "last_error": "portal",
                        "last_http_status": None,
                        "updated_at": "2026-03-26T00:00:00",
                        "detail_url": "https://example.com/2",
                        "page_title": None,
                        "html_sha256": None,
                        "mapped_json": None,
                        "raw_gzip": None,
                    },
                    {
                        "record_type": "CM",
                        "record_id": "3",
                        "organismo_id": 11,
                        "status": "retryable",
                        "attempts": scraper_galicia.DETAIL_MAX_ATTEMPTS,
                        "last_error": "portal",
                        "last_http_status": None,
                        "updated_at": "2026-03-26T00:00:00",
                        "detail_url": "https://example.com/3",
                        "page_title": None,
                        "html_sha256": None,
                        "mapped_json": None,
                        "raw_gzip": None,
                    },
                ],
            )

            batches_default = list(
                scraper_galicia.iter_detail_batches(
                    output_dir / scraper_galicia.BASE_CSV_NAME,
                    conn,
                    only_type="all",
                    batch_size=10,
                    retryable_only=True,
                )
            )
            self.assertEqual(len(batches_default), 1)
            self.assertEqual([item["id"] for item in batches_default[0]], ["2"])

            batches_ignore_max = list(
                scraper_galicia.iter_detail_batches(
                    output_dir / scraper_galicia.BASE_CSV_NAME,
                    conn,
                    only_type="all",
                    batch_size=10,
                    retryable_only=True,
                    retryable_ignore_max_attempts=True,
                )
            )
            self.assertEqual(len(batches_ignore_max), 1)
            self.assertEqual([item["id"] for item in batches_ignore_max[0]], ["2", "3"])

    def test_query_detail_rows_chunks_large_record_sets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            conn = scraper_galicia.init_detail_db(output_dir)
            scraper_galicia.persist_detail_results(
                conn,
                [
                    {
                        "record_type": "LIC",
                        "record_id": "1199",
                        "organismo_id": 48,
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "updated_at": "2026-03-26T00:00:00",
                        "detail_url": "https://example.com/1199",
                        "page_title": "Detalle procedemento: 1199",
                        "html_sha256": "abc",
                        "mapped_json": json.dumps({"detail_referencia": "REF-1199"}),
                        "raw_gzip": None,
                    }
                ],
            )

            records = [
                {"id": str(i), "_tipo": "LIC", "_organismo_id": 48}
                for i in range(1200)
            ]

            rows = scraper_galicia.query_detail_rows(conn, records)

            self.assertIn(("LIC", "1199", 48), rows)
            self.assertEqual(rows[("LIC", "1199", 48)]["status"], "done")

    def test_merge_base_and_detail_injects_detail_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            scraper_galicia.append_base_records(
                [
                    {
                        "id": 824839,
                        "_tipo": "LIC",
                        "_organismo_id": 48,
                        "objeto": "Contrato",
                        "importe": "1.234,56 €",
                        "publicado": "2026-03-23",
                    }
                ],
                output_dir,
            )
            conn = scraper_galicia.init_detail_db(output_dir)
            scraper_galicia.persist_detail_results(
                conn,
                [
                    {
                        "record_type": "LIC",
                        "record_id": "824839",
                        "organismo_id": 48,
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "updated_at": "2026-03-23T00:00:00",
                        "detail_url": "https://example.com/824839",
                        "page_title": "Detalle procedemento: 824839",
                        "html_sha256": "abc123",
                        "mapped_json": json.dumps(
                            {
                                "detail_referencia": "REF-2026-001",
                                "detail_tipo_contrato": "Servicios",
                            }
                        ),
                        "raw_gzip": sqlite3.Binary(b""),
                    }
                ],
            )

            final_csv_path, parquet_path = scraper_galicia.merge_base_and_detail(output_dir)
            df = pd.read_csv(final_csv_path, sep=";")

            self.assertTrue(final_csv_path.exists())
            self.assertEqual(df.iloc[0]["detail_referencia"], "REF-2026-001")
            self.assertEqual(df.iloc[0]["detail_tipo_contrato"], "Servicios")
            self.assertEqual(df.iloc[0]["detail_status"], "done")
            if scraper_galicia.HAS_PYARROW:
                self.assertTrue(parquet_path.exists())

    def test_save_outputs_writes_csv_and_parquet(self):
        records = [
            {
                "id": 1,
                "_tipo": "LIC",
                "_organismo_id": 48,
                "objeto": "Contrato",
                "importe": 100.0,
                "publicado": "2026-03-01",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            df, csv_path, parquet_path = scraper_galicia.save_outputs(records, output_dir, "[TEST] ")
            self.assertEqual(len(df), 1)
            self.assertTrue(csv_path.exists())
            if scraper_galicia.HAS_PYARROW:
                self.assertTrue(parquet_path.exists())

    def test_build_parser_defaults_to_repo_paths_and_all_mode(self):
        parser = scraper_galicia.build_parser()
        args = parser.parse_args([])

        self.assertEqual(args.mode, "all")
        self.assertEqual(Path(args.output), scraper_galicia.DEFAULT_OUTPUT_DIR)
        self.assertEqual(Path(args.log_path), scraper_galicia.DEFAULT_LOG_PATH)
        self.assertEqual(args.detail_workers, scraper_galicia.DETAIL_WORKERS)


if __name__ == "__main__":
    unittest.main()
