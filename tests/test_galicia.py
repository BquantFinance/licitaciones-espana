import importlib.util
import io
import json
import re
import sqlite3
import tempfile
import threading
import unittest
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests


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

PORTAL_DETAIL_TEMPLATE = """
<html>
  <head>
    <title>Detalle procedemento: {n} - Contratos Públicos de Galicia</title>
  </head>
  <body>
    <h2>Información del procedimiento</h2>
    <dl>
      <dt>Referencia</dt><dd>REF-{n}</dd>
      <dt>Tipo de contrato</dt><dd>Servicios</dd>
      <dt>Orzamento base de licitación</dt><dd>1.234,56 €</dd>
      <dt>Fecha de difusión en la Plataforma de Contratos:</dt><dd>23/03/2026</dd>
    </dl>
    <table>
      <tr><th>código CPV</th><th>Lote</th><th>Fecha difusión</th></tr>
      <tr><td>12345678</td><td>1</td><td>23/03/2026</td></tr>
    </table>
  </body>
</html>
"""


class FakePortal:
    """contratosdegalicia.gal simulado: tablas DataTables LIC/CM + ficha HTML."""

    def __init__(self, lic=None, cm=None):
        self.lic = lic or {}
        self.cm = cm or {}
        self.requests = []
        self.lock = threading.Lock()

    def __call__(self, session, method, url, params=None, data=None, timeout=None, headers=None, **kwargs):
        with self.lock:
            self.requests.append(
                {
                    "method": method,
                    "url": url,
                    "params": dict(params or {}),
                    "data": dict(data or {}),
                    "referer": (headers or {}).get("Referer") or session.headers.get("Referer"),
                }
            )
        response = Mock(status_code=200, ok=True)
        match = re.search(r"/api/v1/organismos/(\d+)/(licitaciones|contratosmenores)/table$", url)
        if match:
            org_id = int(match.group(1))
            start = int(params["start"])
            length = int(params["length"])
            if match.group(2) == "licitaciones":
                rows = self.lic.get(org_id, [])
                total = len(rows)
            else:
                all_rows = self.cm.get(org_id, [])
                # README: recordsTotal es global (ignora el filtro de fechas).
                total = len(all_rows)
                first = date.fromisoformat(params["datestart"])
                last = date.fromisoformat(params["dateend"])
                rows = [
                    row for row in all_rows
                    if first <= date.fromisoformat(row["publicado"][:10]) <= last
                ]
            response.json.return_value = {
                "draw": int(params["draw"]),
                "recordsTotal": total,
                "recordsFiltered": total,
                "data": [dict(row) for row in rows[start : start + length]],
            }
            return response
        if method == "POST" and url.endswith("/licitacion"):
            response.text = PORTAL_DETAIL_TEMPLATE.format(n=data["N"])
            return response
        if "resultadoIndex.jsp" in url or "consultaOrganismo.jsp" in url:
            response.text = "<html></html>"
            return response
        response.status_code = 404
        response.ok = False
        return response

    def calls(self, fragment, method=None):
        return [
            item for item in self.requests
            if fragment in item["url"] and (method is None or item["method"] == method)
        ]


def fake_lic_records(count, first_id=824000):
    return [
        {
            "id": first_id + i,
            "publicado": f"2024-{1 + i % 12:02d}-15T00:00:00+0100",
            "objeto": f"<b>Obra</b>   número {i}",
            "importe": 1234.56 + i,  # la API devuelve números JSON
            "estado": 4,
            "estadoDesc": "Adjudicado",
            "modificado": "2024-12-01T10:30:00+0100",
        }
        for i in range(count)
    ]


def fake_cm_records(count, first_id=500000):
    return [
        {
            "id": first_id + i,
            "publicado": f"{2018 + i % 8}-{1 + i % 12:02d}-{1 + i % 28:02d}T00:00:00+0100",
            "objeto": f"MATERIAL SANITARIO {i}",
            "importe": [674.78, 82.5, 15000.0, 129.18][i % 4],
            # NIPC portugués: NIF puramente numérico que debe seguir siendo texto.
            "nif": ["B12345678", "515414581"][i % 2],
            "adjudicatario": f"Empresa {i % 7}",
            "duracion": "1 mes",
        }
        for i in range(count)
    ]


def cli_args(output_dir, *extra):
    return [
        *extra,
        "--output", str(output_dir),
        "--log-path", str(Path(output_dir) / "scraper_galicia.log"),
        "--delay", "0",
        "--detail-delay", "0",
        "--detail-jitter", "0",
        "--detail-workers", "2",
    ]


def run_main(argv, portal):
    """Ejecuta el CLI con HTTP simulado, restaurando el estado global del módulo."""
    stdout = io.StringIO()
    with patch.object(requests.Session, "request", autospec=True, side_effect=portal), patch.object(
        scraper_galicia, "_LOG_PATH", None
    ), patch.object(scraper_galicia, "DELAY", scraper_galicia.DELAY), patch.object(
        scraper_galicia, "PAGE_SIZE", scraper_galicia.PAGE_SIZE
    ), patch("sys.stdout", stdout):
        code = scraper_galicia.main(argv)
    return code, stdout.getvalue()


def detail_cache_row(record_type, record_id, organismo_id, status, attempts=1):
    return {
        "record_type": record_type,
        "record_id": str(record_id),
        "organismo_id": organismo_id,
        "status": status,
        "attempts": attempts,
        "last_error": None if status == "done" else "portal",
        "last_http_status": None,
        "updated_at": "2026-03-26T00:00:00",
        "detail_url": f"https://example.com/{record_id}",
        "page_title": "Detalle procedemento" if status == "done" else None,
        "html_sha256": "abc" if status == "done" else None,
        "mapped_json": json.dumps({"detail_referencia": f"REF-{record_id}"}) if status == "done" else None,
        "raw_gzip": None,
    }


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

    # ── Regresiones ──────────────────────────────────────────────────────────

    def test_to_dataframe_keeps_numeric_importes_and_parses_spanish_text(self):
        # La API devuelve números JSON: antes se les quitaba el "." como si fuera
        # separador de miles (674.78 → 67478, 15000.0 → 150000).
        importes = [674.78, 82.5, 15000.0, 100, "1.234,56 €", "12.50", "1.234", None]
        records = [
            {"id": i, "_tipo": "CM", "_organismo_id": 48, "importe": importe}
            for i, importe in enumerate(importes)
        ]

        df = scraper_galicia.to_dataframe(records)

        self.assertEqual(
            df["importe"].tolist()[:-1],
            [674.78, 82.5, 15000.0, 100.0, 1234.56, 12.5, 1234.0],
        )
        self.assertTrue(pd.isna(df["importe"].iloc[-1]))

    def test_to_dataframe_cleans_text_without_pandas_deprecation_warnings(self):
        records = [
            {
                "id": 1,
                "_tipo": "LIC",
                "_organismo_id": 48,
                "objeto": "<b>Contrato</b>   de  prueba",
                "importe": 10.5,
                "publicado": "2026-03-01T10:00:00+0100",
            }
        ]

        # Con pandas 3 `select_dtypes(include="object")` solo cogía las columnas
        # `str` vía un camino deprecado (Pandas4Warning) que pandas 4 elimina.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            df = scraper_galicia.to_dataframe(records)

        self.assertEqual(df.iloc[0]["objeto"], "Contrato de prueba")

    def test_parse_datetime_series_handles_mixed_formats_and_out_of_range_dates(self):
        series = pd.Series(
            [
                "2026-03-01T10:00:00+0100",
                "2026-03-02",
                "2026-03-03 11:12:13",
                "05/04/2026",
                "5/4/2026 10:00",
                None,
                "0201-03-01",
                "basura",
            ]
        )

        parsed = scraper_galicia.parse_datetime_series(series)

        self.assertEqual(
            [value.isoformat() for value in parsed.iloc[:5]],
            [
                "2026-03-01T10:00:00",
                "2026-03-02T00:00:00",
                "2026-03-03T11:12:13",
                "2026-04-05T00:00:00",
                "2026-04-05T10:00:00",
            ],
        )
        self.assertTrue(parsed.iloc[5:].isna().all())

    def test_classify_detail_error_treats_network_errors_as_retryable(self):
        timeout = (
            "https://www.contratosdegalicia.gal/licitacion: HTTPSConnectionPool(host="
            "'www.contratosdegalicia.gal', port=443): Read timed out. (read timeout=30)"
        )
        self.assertEqual(scraper_galicia.classify_detail_error(timeout), (True, None))
        self.assertEqual(scraper_galicia.classify_detail_error("x: HTTP 403"), (True, 403))
        self.assertEqual(scraper_galicia.classify_detail_error("x: HTTP 503"), (True, 503))
        self.assertEqual(scraper_galicia.classify_detail_error("x: HTTP 404"), (False, 404))

    def test_run_detail_enrichment_stops_queued_batches_after_ban(self):
        calls = []

        class BannedSession:
            def ensure_org_context(self, org_id):
                pass

            def post_html(self, url, data, retry=0, headers=None, count_error=True):
                calls.append(data["N"])
                raise scraper_galicia.ScraperError(f"{url}: HTTP 403")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            scraper_galicia.append_base_records(
                [{"id": i, "_tipo": "LIC", "_organismo_id": 48, "objeto": "x"} for i in range(30)],
                output_dir,
            )
            with patch.object(
                scraper_galicia, "get_detail_session", return_value=BannedSession()
            ), patch.object(scraper_galicia, "BAN_ERROR_THRESHOLD", 5), patch(
                "sys.stdout", new_callable=io.StringIO
            ):
                with self.assertRaises(scraper_galicia.ScraperError):
                    scraper_galicia.run_detail_enrichment(
                        output_dir / scraper_galicia.BASE_CSV_NAME,
                        output_dir,
                        workers=1,
                        batch_size=5,
                        detail_delay=0.2,
                        detail_jitter=0,
                    )

            conn = scraper_galicia.init_detail_db(output_dir)
            persisted = conn.execute("SELECT COUNT(*) FROM detail_cache").fetchone()[0]

        # Antes el lote encolado se ejecutaba entero (10 peticiones) contra el
        # portal ya baneado y sus resultados se tiraban.
        self.assertLess(len(calls), 10)
        self.assertEqual(persisted, len(calls))

    def test_load_base_resume_falls_back_to_csv_when_progress_is_unreadable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [{"id": 1, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Uno"}],
                    output_dir,
                )
                (output_dir / scraper_galicia.BASE_PROGRESS_NAME).write_text(
                    '{"completed_orgs": [4', encoding="utf-8"
                )
                completed, _ = scraper_galicia.load_base_resume(output_dir)

        # Con un conjunto vacío --resume volvía a añadir todos los organismos.
        self.assertEqual(completed, {48})

    def test_base_resume_after_interrupted_append_does_not_duplicate_rows(self):
        portal = FakePortal(lic={2: fake_lic_records(3)}, cm={3: fake_cm_records(4)})
        real_append = scraper_galicia.append_base_records

        def append_then_crash(records, output_dir, label=""):
            written = real_append(records, output_dir, label=label)
            if records and records[0]["_organismo_id"] == 3:
                # Corte en mitad del organismo: filas escritas + línea a medias,
                # sin llegar al checkpoint.
                with (Path(output_dir) / scraper_galicia.BASE_CSV_NAME).open("a", encoding="utf-8") as fh:
                    fh.write("999;cortad")
                raise KeyboardInterrupt
            return written

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch.object(scraper_galicia, "append_base_records", side_effect=append_then_crash):
                code, _ = run_main(cli_args(output_dir, "base", "--max-org-id", "3"), portal)
            self.assertEqual(code, 130)

            code, _ = run_main(cli_args(output_dir, "base", "--resume", "--max-org-id", "3"), portal)
            self.assertEqual(code, 0)

            df = pd.read_csv(output_dir / scraper_galicia.BASE_CSV_NAME, sep=";", encoding="utf-8-sig")
            progress = json.loads((output_dir / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))

        self.assertEqual(len(df), 7)
        self.assertFalse(df.duplicated(["id", "_tipo", "_organismo_id"]).any())
        self.assertEqual(df.groupby("_organismo_id").size().to_dict(), {2: 3, 3: 4})
        self.assertEqual(progress["completed_orgs"], [2, 3])

    def test_main_retryable_only_without_resume_keeps_detail_cache(self):
        portal = FakePortal()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
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
                [detail_cache_row("LIC", 1, 48, "done"), detail_cache_row("LIC", 2, 48, "retryable")],
            )
            conn.close()

            code, _ = run_main(cli_args(output_dir, "detail", "--retryable-only"), portal)

            conn = scraper_galicia.init_detail_db(output_dir)
            statuses = {
                row["record_id"]: row["status"]
                for row in conn.execute("SELECT record_id, status FROM detail_cache")
            }
            conn.close()

        # Antes se borraba la caché (el "done" se perdía) y no había nada que rescatar.
        self.assertEqual(code, 0)
        self.assertEqual(statuses, {"1": "done", "2": "done"})
        self.assertEqual([item["data"]["N"] for item in portal.calls("/licitacion", "POST")], ["2"])

    def test_merge_copies_base_values_verbatim_and_parquet_types(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [
                        {
                            "id": 7,
                            "_tipo": "LIC",
                            "_organismo_id": 48,
                            "objeto": "Obra",
                            "importe": 1000.5,
                            "estado": 3,
                            "publicado": "2026-03-01T00:00:00+0100",
                        }
                    ],
                    output_dir,
                )
                scraper_galicia.append_base_records(
                    [
                        {
                            "id": 8,
                            "_tipo": "CM",
                            "_organismo_id": 49,
                            "objeto": "Material",
                            "importe": 12.5,
                            "nif": "501234567",
                            "publicado": "2026-03-02T00:00:00+0100",
                        }
                    ],
                    output_dir,
                )
                final_csv_path, parquet_path = scraper_galicia.merge_base_and_detail(output_dir)

            final = pd.read_csv(final_csv_path, sep=";", dtype=str, keep_default_na=False)
            self.assertFalse((output_dir / (scraper_galicia.FINAL_CSV_NAME + ".tmp")).exists())
            parquet = pd.read_parquet(parquet_path) if scraper_galicia.HAS_PYARROW else None

        # Antes: "nan" literal en los vacíos, "3.0" y el NIF numérico como "501234567.0".
        self.assertNotIn("nan", final.to_numpy().ravel().tolist())
        self.assertEqual(final["estado"].tolist(), ["3", ""])
        self.assertEqual(final["nif"].tolist(), ["", "501234567"])
        self.assertEqual(final["detail_status"].tolist(), ["missing", "missing"])
        if parquet is not None:
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(parquet["publicado"]))
            self.assertEqual(parquet["publicado"].dt.year.tolist(), [2026, 2026])
            self.assertEqual(parquet["nif"].tolist()[1], "501234567")
            self.assertEqual(parquet["importe"].tolist(), [1000.5, 12.5])

    def test_merge_does_not_leave_truncated_final_csv_on_failure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [{"id": i, "_tipo": "LIC", "_organismo_id": 48, "objeto": "x"} for i in range(3)],
                    output_dir,
                )
                # Corte (Ctrl+C) al procesar el segundo chunk del merge.
                with patch.object(
                    scraper_galicia,
                    "load_detail_map",
                    side_effect=[{}, KeyboardInterrupt()],
                ):
                    with self.assertRaises(KeyboardInterrupt):
                        scraper_galicia.merge_base_and_detail(output_dir, chunksize=1)

            # Antes quedaba un contratos_galicia.csv con cabecera + 1 fila.
            self.assertFalse((output_dir / scraper_galicia.FINAL_CSV_NAME).exists())

    def test_csv_to_parquet_streams_chunks_with_a_stable_schema(self):
        if not scraper_galicia.HAS_PYARROW:
            self.skipTest("pyarrow no disponible")
        csv_text = (
            "id;estado;objeto;nif;importe;publicado\n"
            "1;;;;10;2026-03-01\n"
            "2;;;;;2026-03-02 10:00:00\n"
            "3;4;Obra;515414581;12.5;\n"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "in.csv"
            csv_path.write_text(csv_text, encoding="utf-8-sig")
            with patch("sys.stdout", new_callable=io.StringIO):
                parquet_path = scraper_galicia.csv_to_parquet(
                    csv_path, Path(tmpdir) / "out.parquet", chunksize=1
                )
            df = pd.read_parquet(parquet_path)

        # Con chunks de 1 fila cada chunk infiere tipos distintos: el esquema debe
        # salir de todo el fichero (estado float, objeto/nif texto, id int).
        self.assertEqual(str(df["id"].dtype), "int64")
        self.assertEqual(str(df["estado"].dtype), "float64")
        self.assertEqual(df["objeto"].tolist()[2], "Obra")
        self.assertEqual(df["nif"].tolist()[2], "515414581")
        self.assertTrue(pd.isna(df["importe"].iloc[1]))
        self.assertEqual(df["importe"].tolist()[2], 12.5)
        self.assertEqual(
            df["publicado"].tolist()[:2],
            [pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-02 10:00:00")],
        )

    # ── Extremo a extremo (CLI + portal simulado) ────────────────────────────

    def test_main_all_single_org_end_to_end(self):
        lic = fake_lic_records(150)  # 2 páginas de 100
        cm = fake_cm_records(230)
        portal = FakePortal(lic={48: lic}, cm={48: cm})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            # Comando documentado: python galicia/scraper_galicia.py --organismo 48
            # (modo por defecto "all": base -> detail -> merge).
            code, _ = run_main(cli_args(output_dir, "--organismo", "48"), portal)
            self.assertEqual(code, 0)

            base = pd.read_csv(output_dir / scraper_galicia.BASE_CSV_NAME, sep=";", encoding="utf-8-sig", dtype={"nif": str})
            final_text = pd.read_csv(
                output_dir / scraper_galicia.FINAL_CSV_NAME,
                sep=";",
                encoding="utf-8-sig",
                dtype=str,
                keep_default_na=False,
            )
            first_final = (output_dir / scraper_galicia.FINAL_CSV_NAME).read_bytes()
            requests_all = list(portal.requests)

            # Segunda pasada de detalle: no rehace trabajo (README).
            portal.requests.clear()
            code, out = run_main(cli_args(output_dir, "detail", "--resume"), portal)
            self.assertEqual(code, 0)
            self.assertEqual(portal.calls("/licitacion", "POST"), [])
            self.assertIn("DETALLE COMPLETO: 0 procesados", out)

            code, _ = run_main(cli_args(output_dir, "merge"), portal)
            self.assertEqual(code, 0)
            self.assertEqual((output_dir / scraper_galicia.FINAL_CSV_NAME).read_bytes(), first_final)

            if scraper_galicia.HAS_PYARROW:
                df_gal = pd.read_parquet(output_dir / scraper_galicia.FINAL_PARQUET_NAME)
                base_parquet = pd.read_parquet(output_dir / scraper_galicia.BASE_PARQUET_NAME)
            else:
                df_gal = base_parquet = None

        # Base: todas las filas, 12 columnas estables, sin duplicar.
        self.assertEqual(list(base.columns), scraper_galicia.BASE_EXPORT_FIELDS)
        self.assertEqual(base["_tipo"].value_counts().to_dict(), {"CM": 230, "LIC": 150})
        self.assertFalse(base.duplicated(["id", "_tipo"]).any())
        lic_row = base[base["id"] == 824000].iloc[0]
        self.assertEqual(lic_row["objeto"], "Obra número 0")
        self.assertEqual(lic_row["importe"], 1234.56)
        self.assertEqual(lic_row["publicado"], "2024-01-15")
        self.assertEqual(lic_row["modificado"], "2024-12-01 10:30:00")
        cm_rows = base[base["_tipo"] == "CM"].set_index("id")
        self.assertEqual(cm_rows.loc[500000, "importe"], 674.78)
        self.assertEqual(cm_rows.loc[500001, "nif"], "515414581")
        self.assertEqual(round(base["importe"].sum(), 2), round(sum(r["importe"] for r in lic + cm), 2))

        # Peticiones: LIC paginado, CM por ventanas de 3 meses contiguas hasta
        # DATE_ORIGIN con Referer del organismo, detalle por POST /licitacion.
        lic_calls = [r for r in requests_all if "/organismos/48/licitaciones/table" in r["url"]]
        self.assertEqual([r["params"]["start"] for r in lic_calls], ["0", "100"])
        cm_calls = [r for r in requests_all if "/organismos/48/contratosmenores/table" in r["url"]]
        self.assertTrue(all(r["referer"].endswith("consultaOrganismo.jsp?OR=48&N=48&lang=es") for r in cm_calls))
        windows = sorted(
            {(r["params"]["datestart"], r["params"]["dateend"]) for r in cm_calls},
            key=lambda window: window[1],
            reverse=True,
        )
        self.assertEqual(windows[0][1], datetime.now().strftime("%Y-%m-%d"))
        self.assertEqual(windows[-1][0], scraper_galicia.DATE_ORIGIN)
        for (start, _), (_, next_end) in zip(windows, windows[1:]):
            # Contiguas: sin huecos ni solapes entre ventanas.
            self.assertEqual(date.fromisoformat(next_end), date.fromisoformat(start) - timedelta(days=1))
        for start, end in windows:
            # Nunca más de 3 meses (el portal rechaza rangos mayores).
            self.assertLessEqual((date.fromisoformat(end) - date.fromisoformat(start)).days, 92)
        posts = [r for r in requests_all if r["method"] == "POST"]
        self.assertEqual(len(posts), 380)
        payloads = {(p["data"]["S"], p["data"]["N"], p["data"]["OR"]) for p in posts}
        self.assertIn(("C", "824000", "48"), payloads)
        self.assertIn(("CM", "CM500000", "48"), payloads)

        # Final: 12 + 50 columnas (README: 62), mismas filas, detalle mapeado.
        self.assertEqual(len(final_text.columns), 62)
        self.assertEqual(len(final_text), 380)
        self.assertEqual(set(final_text["detail_status"]), {"done"})
        final_by_key = final_text.set_index(["_tipo", "id"])
        self.assertEqual(final_by_key.loc[("CM", "500001"), "detail_referencia"], "REF-CM500001")
        self.assertEqual(final_by_key.loc[("CM", "500001"), "nif"], "515414581")
        self.assertEqual(final_by_key.loc[("LIC", "824000"), "detail_presupuesto_base_eur"], "1234.56")
        self.assertEqual(final_by_key.loc[("LIC", "824000"), "detail_cpv_codes"], "12345678")
        self.assertNotIn("nan", final_text.to_numpy().ravel().tolist())

        if df_gal is not None:
            # Ejemplos del README sobre contratos_galicia.parquet.
            df_gal_cm = df_gal[df_gal["_tipo"] == "CM"].copy()
            df_gal_cm["año"] = df_gal_cm["publicado"].dt.year
            by_year = df_gal_cm.groupby("año")["importe"].sum()
            self.assertEqual(sorted(by_year.index), list(range(2018, 2026)))
            self.assertAlmostEqual(by_year.sum(), sum(r["importe"] for r in cm), places=2)
            self.assertEqual(df_gal_cm.groupby("nif")["importe"].sum().index.tolist(), ["515414581", "B12345678"])
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(base_parquet["publicado"]))
            self.assertEqual(len(base_parquet), 380)

    def test_main_base_discovers_active_organismos(self):
        portal = FakePortal(lic={2: fake_lic_records(3)}, cm={3: fake_cm_records(4)})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            code, _ = run_main(cli_args(output_dir, "base", "--max-org-id", "3", "--workers", "2"), portal)
            base = pd.read_csv(output_dir / scraper_galicia.BASE_CSV_NAME, sep=";", encoding="utf-8-sig")

        self.assertEqual(code, 0)
        self.assertEqual(
            base.groupby(["_organismo_id", "_tipo"]).size().to_dict(),
            {(2, "LIC"): 3, (3, "CM"): 4},
        )
        # El organismo sin actividad (1) no se barre, y cada uno solo en su tipo.
        scraped = {
            (int(m.group(1)), m.group(2))
            for r in portal.requests
            if (m := re.search(r"/organismos/(\d+)/(\w+)/table", r["url"])) and r["params"]["length"] != "1"
        }
        self.assertEqual(scraped, {(2, "licitaciones"), (3, "contratosmenores")})


if __name__ == "__main__":
    unittest.main()
