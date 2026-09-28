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

DETAIL_HTML_WITH_AWARDS = """
<html>
  <head>
    <title>Detalle procedemento: 777777 - Contratos Públicos de Galicia</title>
  </head>
  <body>
    <h2>Información del procedimiento</h2>
    <dl>
      <dt>Referencia</dt><dd>REF-777</dd>
      <dt>Fecha formalización:</dt><dd>01/02/2026</dd>
      <dt>Fecha formalización:</dt><dd>15/02/2026</dd>
      <dt>Fecha adjudicación:</dt><dd>20/01/2026</dd>
    </dl>
    <h3>Adjudicaciones</h3>
    <table>
      <tr><th>Lote</th><th>Adjudicatario</th><th>NIF</th><th>Importe</th></tr>
      <tr><td>1</td><td>Empresa Uno SL</td><td>B11111111</td><td>1.000,00 €</td></tr>
      <tr><td>2</td><td>Empresa Dos SA</td><td>A22222222</td><td>2.500,50 €</td></tr>
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


EMPTY_DETAIL_HTML = """
<html>
  <head><title>Detalle procedemento: {n} - Contratos Públicos de Galicia</title></head>
  <body><h2>Información del procedimiento</h2></body>
</html>
"""


class FakePortal:
    """contratosdegalicia.gal simulado: tablas DataTables LIC/CM + ficha HTML.

    Como el portal real (verificado en vivo el 2026-09-28): en CM recordsTotal es
    el total del organismo y recordsFiltered el de la ventana de fechas.
    fail_orgs: organismos cuya tabla responde 403 (salvo las sondas del
    descubrimiento); empty_orgs: responden 0 registros (p. ej. sin contexto de
    sesión); empty_scan_orgs: lo mismo, pero solo al barrer (no en las sondas
    del descubrimiento); windowless_orgs: declaran sus CM (recordsTotal) pero
    todas las ventanas vuelven vacías; hidden_ids: filas que la paginación se
    salta aunque cuentan en recordsFiltered; details: N de la ficha -> código
    HTTP de error, 'vacia' o un texto que sustituye a la referencia (ficha
    cambiada).
    """

    def __init__(self, lic=None, cm=None):
        self.lic = lic or {}
        self.cm = cm or {}
        self.requests = []
        self.lock = threading.Lock()
        self.fail_orgs = set()
        self.empty_orgs = set()
        self.empty_scan_orgs = set()
        self.windowless_orgs = set()
        self.hidden_ids = set()
        self.details = {}

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
            if org_id in self.fail_orgs and length != 1:
                response.status_code = 403
                response.ok = False
                return response
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
                if org_id in self.windowless_orgs:
                    rows = []
            filtered = len(rows)
            if org_id in self.empty_orgs or (org_id in self.empty_scan_orgs and length != 1):
                rows, total, filtered = [], 0, 0
            visible = [row for row in rows if row["id"] not in self.hidden_ids]
            response.json.return_value = {
                "draw": int(params["draw"]),
                "recordsTotal": total,
                "recordsFiltered": filtered,
                "data": [dict(row) for row in visible[start : start + length]],
            }
            return response
        if method == "POST" and url.endswith("/licitacion"):
            special = self.details.get(data["N"])
            if isinstance(special, int):
                response.status_code = special
                response.ok = False
            elif special == "vacia":
                response.text = EMPTY_DETAIL_HTML.format(n=data["N"])
            else:
                response.text = PORTAL_DETAIL_TEMPLATE.format(n=data["N"])
                if special:
                    response.text = response.text.replace(f"REF-{data['N']}", special)
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

    def test_parse_detail_html_exports_award_rows_and_unmapped_labels(self):
        # Antes la tabla de adjudicaciones solo se contaba (en LIC es el único sitio con
        # adjudicatario e importe) y las etiquetas sin columna o repetidas se perdían.
        mapped = scraper_galicia.parse_detail_html(DETAIL_HTML_WITH_AWARDS)["mapped"]

        self.assertEqual(mapped["detail_adjudicaciones_count"], 2)
        self.assertEqual(
            json.loads(mapped["detail_adjudicaciones_json"]),
            [
                {"Lote": "1", "Adjudicatario": "Empresa Uno SL", "NIF": "B11111111", "Importe": "1.000,00 €"},
                {"Lote": "2", "Adjudicatario": "Empresa Dos SA", "NIF": "A22222222", "Importe": "2.500,50 €"},
            ],
        )
        self.assertEqual(mapped["detail_fecha_formalizacion_text"], "01/02/2026")
        self.assertEqual(
            json.loads(mapped["detail_campos_extra_json"]),
            [
                {"section": "Información del procedimiento", "label": "Fecha formalización:", "value": "15/02/2026"},
                {"section": "Información del procedimiento", "label": "Fecha adjudicación:", "value": "20/01/2026"},
            ],
        )

        plain = scraper_galicia.parse_detail_html(DETAIL_HTML)["mapped"]
        self.assertIsNone(plain["detail_adjudicaciones_json"])
        self.assertIsNone(plain["detail_campos_extra_json"])

    def test_merge_derives_new_detail_fields_from_raw_cache_of_old_runs(self):
        parsed = scraper_galicia.parse_detail_html(DETAIL_HTML_WITH_AWARDS)
        old_mapped = {
            key: value for key, value in parsed["mapped"].items()
            if key not in ("detail_adjudicaciones_json", "detail_campos_extra_json")
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [{"id": 777777, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Obra"}], output_dir
                )
                conn = scraper_galicia.init_detail_db(output_dir)
                row = detail_cache_row("LIC", 777777, 48, "done")
                row["mapped_json"] = json.dumps(old_mapped)
                row["raw_gzip"] = scraper_galicia.compress_text(
                    scraper_galicia.compact_json({"pairs": parsed["pairs"], "tables": parsed["tables"]})
                )
                scraper_galicia.persist_detail_results(conn, [row])
                conn.close()
                final_csv_path, _ = scraper_galicia.merge_base_and_detail(output_dir)
            final = pd.read_csv(final_csv_path, sep=";", dtype=str, keep_default_na=False)

        awards = json.loads(final.loc[0, "detail_adjudicaciones_json"])
        self.assertEqual([award["NIF"] for award in awards], ["B11111111", "A22222222"])
        self.assertIn("Fecha adjudicación:", final.loc[0, "detail_campos_extra_json"])

    def test_paginate_cm_full_warns_when_windows_miss_records_declared_by_portal(self):
        # recordsTotal es el total del organismo: un CM con fecha fuera de las ventanas
        # (aquí 1999) no se descarga nunca y antes no quedaba rastro
        records = fake_cm_records(3)
        records.append(dict(records[0], id=499999, publicado="1999-06-01T00:00:00+0100"))
        portal = FakePortal(cm={48: records})

        with patch.object(requests.Session, "request", autospec=True, side_effect=portal), patch.object(
            scraper_galicia, "_LOG_PATH", None
        ), patch.object(scraper_galicia, "DELAY", 0), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            session = scraper_galicia.Session()
            got = scraper_galicia.paginate_cm_full(session, 48)

        self.assertEqual(sorted(record["id"] for record in got), [500000, 500001, 500002])
        self.assertIn("Org 48 CM: DESAJUSTE el portal declara 4 y las ventanas", stdout.getvalue())

    def test_paginate_lic_warns_when_repeated_rows_hide_missing_ones(self):
        # La paginación por fecha con empates puede repetir una fila y saltarse otra: el
        # número de filas cuadraba con recordsTotal y no se avisaba
        with patch.object(scraper_galicia.Session, "_init", return_value=None):
            session = scraper_galicia.Session()
        visit = Mock(status_code=200, ok=True)
        page1 = Mock(status_code=200, ok=True)
        page1.json.return_value = {"recordsTotal": 3, "data": [{"id": 1}, {"id": 2}]}
        page2 = Mock(status_code=200, ok=True)
        page2.json.return_value = {"recordsTotal": 3, "data": [{"id": 2}]}

        with patch.object(session.s, "request", side_effect=[visit, page1, page2]), patch.object(
            scraper_galicia, "_LOG_PATH", None
        ), patch.object(scraper_galicia, "DELAY", 0), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            scraper_galicia.paginate_lic(session, 48)

        self.assertIn("Org 48 LIC: DESAJUSTE esperados=3 descargados=3 únicos=2", stdout.getvalue())

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

    def test_append_base_records_warns_about_api_fields_without_column(self):
        records = [{"id": 1, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Obra", "fechaAdjudicacion": "2026-03-01"}]

        with tempfile.TemporaryDirectory() as tmpdir, patch.object(scraper_galicia, "_LOG_PATH", None), patch(
            "sys.stdout", new_callable=io.StringIO
        ) as stdout:
            scraper_galicia.append_base_records(records, Path(tmpdir), label="[BASE ORG 48] ")

        self.assertIn("[BASE ORG 48] campos de la API sin columna en el CSV base (se descartan): ['fechaAdjudicacion']", stdout.getvalue())

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

    def test_csv_to_parquet_never_sends_hashes_or_huge_integers_to_to_numeric(self):
        # Un sha256 con pinta de notación científica (13 cifras de exponente: el de la
        # ficha 824009 del portal simulado, que llega a la tabla final en
        # test_main_all_single_org_end_to_end) y un entero de 24 cifras no son
        # números para el Parquet: se quedan como texto, sin pasar por to_numeric.
        # Los demás casos, como siempre.
        if not scraper_galicia.HAS_PYARROW:
            self.skipTest("pyarrow no disponible")
        sha = "3e4224959973966b37b65418d52f5d9025069f25caa6e0dfbba260c698a2e529"
        huge = "123456789012345678901234"
        csv_text = (
            "hash;grande;exp_raro;exp;normal;entero;infinito;espacios\n"
            f"{sha};{huge};1e4224959973966;1e5;10;1;inf; 12\n"
            f"abc;5;2;2.5E-3;;2;1;13 \n"
        )
        seen = []
        real_to_numeric = pd.to_numeric

        def spy(values, *args, **kwargs):
            seen.extend(str(v) for v in pd.Series(values, dtype=object).dropna())
            return real_to_numeric(values, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "in.csv"
            csv_path.write_text(csv_text, encoding="utf-8-sig")
            with patch("sys.stdout", new_callable=io.StringIO), patch.object(
                scraper_galicia.pd, "to_numeric", side_effect=spy
            ):
                parquet_path = scraper_galicia.csv_to_parquet(csv_path, Path(tmpdir) / "out.parquet")
            df = pd.read_parquet(parquet_path)

        for value in (sha, huge, "1e4224959973966"):
            self.assertNotIn(value, seen)
        self.assertEqual(df["hash"].tolist(), [sha, "abc"])
        self.assertEqual(df["grande"].tolist(), [huge, "5"])
        self.assertEqual(df["exp_raro"].tolist(), ["1e4224959973966", "2"])
        self.assertEqual(df["exp"].tolist(), [1e5, 2.5e-3])
        self.assertEqual(str(df["normal"].dtype), "float64")
        self.assertEqual(str(df["entero"].dtype), "int64")
        self.assertEqual(df["infinito"].tolist(), [float("inf"), 1.0])
        self.assertEqual(df["espacios"].tolist(), [12, 13])
        self.assertTrue(scraper_galicia.plain_numbers(["-1.5", ".5", "1.", "+3e-05", "1234567890123456.8", None]))
        self.assertFalse(scraper_galicia.plain_numbers(["1", "1234567890123456789"]))
        self.assertFalse(scraper_galicia.plain_numbers(["1e5000"]))
        self.assertFalse(scraper_galicia.plain_numbers(["١٢"]))

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

        # Final: 12 + 52 columnas (las 62 del README + adjudicaciones y campos extra en
        # JSON) y, al final, las 3 de control de comun/historico.py; mismas filas,
        # detalle mapeado.
        self.assertEqual(len(final_text.columns), 67)
        self.assertEqual(
            list(final_text.columns[-3:]), ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]
        )
        self.assertEqual(set(final_text["_en_ultima_descarga"]), {"True"})
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


# ── Sesgo del superviviente (comun/historico.py) ────────────────────────────

FECHA_1 = "2026-01-01T00:00:00Z"
FECHA_2 = "2026-02-01T00:00:00Z"
FECHA_3 = "2026-03-01T00:00:00Z"
FECHA_4 = "2026-04-01T00:00:00Z"
META = ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]


ISO_UTC = scraper_galicia.iso_utc


def run_at(argv, portal, fecha):
    """run_main con la fecha de ahora (la de la descarga y las marcas) fijada."""
    def now_is(momento=None):
        return fecha if momento is None else ISO_UTC(momento)

    with patch.object(scraper_galicia, "iso_utc", side_effect=now_is):
        return run_main(argv, portal)


def read_final(output_dir):
    return pd.read_csv(
        Path(output_dir) / scraper_galicia.FINAL_CSV_NAME,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
    )


def historico(output_dir):
    folder = Path(output_dir) / "_historico"
    return sorted(path.name for path in folder.iterdir()) if folder.is_dir() else []


def snapshot(output_dir):
    """Bytes de los ficheros de datos de la carpeta de salida (sin _historico/)."""
    return {
        path.name: path.read_bytes()
        for path in Path(output_dir).iterdir()
        if path.is_file() and path.suffix in (".csv", ".parquet")
    }


def published_seed(path, rows):
    """Parquet con el esquema del publicado v2026.02 (scraper antiguo): id e importe
    int64 (importe sin el punto decimal: inflado), estado texto ('nan' en CM),
    fechas datetime64 y el resto texto."""
    df = pd.DataFrame(
        {
            "id": pd.array([row["id"] for row in rows], dtype="Int64"),
            "objeto": [row.get("objeto", "Objeto") for row in rows],
            "importe": [int(str(float(row["importe"])).replace(".", "")) for row in rows],
            # Texto: '4.0' en LIC (float del scraper antiguo) y 'nan' en CM
            "estado": [
                row["estado"] if isinstance(row.get("estado"), str)
                else ("nan" if row.get("estado") is None else str(float(row["estado"])))
                for row in rows
            ],
            "estadoDesc": [row.get("estadoDesc") for row in rows],
            # Sin zona, como en el publicado
            "publicado": pd.to_datetime([row["publicado"][:19] for row in rows]),
            "modificado": pd.to_datetime([(row.get("modificado") or "")[:19] or None for row in rows]),
            "_organismo_id": pd.array([row["_organismo_id"] for row in rows], dtype="Int64"),
            "_tipo": [row["_tipo"] for row in rows],
            "nif": [row.get("nif") for row in rows],
            "adjudicatario": [row.get("adjudicatario") for row in rows],
            "duracion": [row.get("duracion") for row in rows],
        }
    )
    df.to_parquet(path, index=False)
    return path


class GaliciaHistoricoTests(unittest.TestCase):
    """Nada de lo descargado alguna vez se pierde (docs/CONTINUACION.md §2 y §3.2)."""

    def test_withdrawn_contracts_stay_with_en_ultima_descarga_false(self):
        lic = fake_lic_records(3)
        cm = fake_cm_records(4)
        portal = FakePortal(lic={48: list(lic)}, cm={48: list(cm)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            first = read_final(out)
            # El portal retira una licitación y un contrato menor (el único de su
            # trimestre: la ventana vuelve vacía, pero coherente)
            portal.lic[48] = [r for r in lic if r["id"] != 824001]
            portal.cm[48] = [r for r in cm if r["id"] != 500002]
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_2)[0], 0)
            final = read_final(out)
            hist = historico(out)
            if scraper_galicia.HAS_PYARROW:
                parquet = pd.read_parquet(out / scraper_galicia.FINAL_PARQUET_NAME)
            else:
                parquet = None

        self.assertEqual(len(final), 7)
        by_id = final.set_index("id")
        for rid in ("824001", "500002"):
            self.assertEqual(by_id.loc[rid, "_en_ultima_descarga"], "False")
            self.assertEqual(by_id.loc[rid, "_primera_descarga"], FECHA_1)
            self.assertEqual(by_id.loc[rid, "_ultima_descarga"], FECHA_1)
            # La fila retirada sigue tal cual (listado y ficha)
            antes = first.set_index("id").loc[rid].drop(META)
            self.assertTrue(by_id.loc[rid].drop(META).equals(antes))
        current = final[~final["id"].isin(["824001", "500002"])]
        self.assertEqual(set(current["_en_ultima_descarga"]), {"True"})
        self.assertEqual(set(current["_primera_descarga"]), {FECHA_1})
        self.assertEqual(set(current["_ultima_descarga"]), {FECHA_2})
        # Capa cruda y tabla anterior en _historico/
        self.assertTrue(any(n.startswith("contratos_galicia_base__") and n.endswith(".csv") for n in hist))
        self.assertTrue(any(n.startswith("contratos_galicia_base__") and n.endswith(".parquet") for n in hist))
        self.assertTrue(any(n.startswith("contratos_galicia__") and n.endswith(".csv") for n in hist))
        self.assertTrue(any(n.startswith("contratos_galicia__") and n.endswith(".parquet") for n in hist))
        if parquet is not None:
            self.assertEqual(str(parquet["_en_ultima_descarga"].dtype), "bool")
            self.assertEqual(int((~parquet["_en_ultima_descarga"]).sum()), 2)

    def test_changed_contract_keeps_the_previous_version(self):
        lic = fake_lic_records(2)
        cm = fake_cm_records(2)
        portal = FakePortal(lic={48: list(lic)}, cm={48: list(cm)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            # La licitación pasa a formalizada: otro estado y otra fecha de modificación
            portal.lic[48] = [
                dict(lic[0], estado=5, estadoDesc="Formalizado", modificado="2025-03-01T09:00:00+0100"),
                lic[1],
            ]
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_2)[0], 0)
            final = read_final(out)

        self.assertEqual(len(final), 5)
        versions = final[final["id"] == "824000"]
        self.assertEqual(
            versions[["estadoDesc", "_en_ultima_descarga", "_ultima_descarga"]].values.tolist(),
            [["Adjudicado", "False", FECHA_1], ["Formalizado", "True", FECHA_2]],
        )
        self.assertEqual(versions["modificado"].tolist(), ["2024-12-01 10:30:00", "2025-03-01 09:00:00"])
        # La ficha es la del contrato: la llevan las dos versiones
        self.assertEqual(set(versions["detail_referencia"]), {"REF-824000"})
        self.assertEqual(set(final.loc[final["id"] != "824000", "_en_ultima_descarga"]), {"True"})

    def _three_orgs(self):
        return FakePortal(
            lic={2: fake_lic_records(3, first_id=700000), 48: fake_lic_records(2)},
            cm={3: fake_cm_records(4, first_id=600000), 48: fake_cm_records(3)},
        )

    def test_single_org_run_does_not_withdraw_other_organismos(self):
        portal = self._three_orgs()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_1)[0], 0)
            self.assertEqual(len(read_final(out)), 12)
            # El portal retira un contrato de cada organismo; solo se vuelve a leer el 3
            portal.lic[2] = [r for r in portal.lic[2] if r["id"] != 700001]
            portal.cm[3] = [r for r in portal.cm[3] if r["id"] != 600001]
            portal.cm[48] = [r for r in portal.cm[48] if r["id"] != 500001]
            self.assertEqual(run_at(cli_args(out, "--organismo", "3"), portal, FECHA_2)[0], 0)
            final = read_final(out).set_index("id")
            # Otra descarga parcial (solo el 2): la retirada del 3, fuera de su
            # ámbito, sigue retirada
            self.assertEqual(run_at(cli_args(out, "--organismo", "2"), portal, FECHA_3)[0], 0)
            third = read_final(out).set_index("id")

        self.assertEqual(third.loc["600001", "_en_ultima_descarga"], "False")
        self.assertEqual(third.loc["700001", "_en_ultima_descarga"], "False")
        self.assertEqual(third.loc["500001", "_en_ultima_descarga"], "True")
        self.assertEqual(len(final), 12)
        self.assertEqual(final.loc["600001", "_en_ultima_descarga"], "False")
        for rid in ("700001", "500001"):
            # Fuera del ámbito: siguen como estaban
            self.assertEqual(final.loc[rid, "_en_ultima_descarga"], "True")
            self.assertEqual(final.loc[rid, "_ultima_descarga"], FECHA_1)
        self.assertEqual(set(final.loc[final["_organismo_id"] == "3", "_ultima_descarga"]), {FECHA_1, FECHA_2})
        self.assertEqual(set(final.loc[final["_organismo_id"] == "2", "_ultima_descarga"]), {FECHA_1})

    def test_skip_cm_run_neither_withdraws_cm_nor_duplicates_lic_written_otherwise(self):
        portal = FakePortal(lic={48: fake_lic_records(2)}, cm={48: fake_cm_records(3)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            first = read_final(out)
            portal.cm[48] = portal.cm[48][:1]
            self.assertEqual(run_at(cli_args(out, "--organismo", "48", "--skip-cm"), portal, FECHA_2)[0], 0)
            base = pd.read_csv(out / scraper_galicia.BASE_CSV_NAME, sep=";", dtype=str, keep_default_na=False)
            final = read_final(out)

        # Con CM en el mismo organismo, 'estado' se escribía 4.0; solo con LIC, 4: es
        # el mismo valor, no un contrato cambiado.
        self.assertEqual(set(first.loc[first["_tipo"] == "LIC", "estado"]), {"4.0"})
        self.assertEqual(set(base["estado"]), {"4"})
        self.assertEqual(len(final), 5)
        lic = final[final["_tipo"] == "LIC"]
        self.assertEqual(set(lic["estado"]), {"4.0"})
        self.assertEqual(set(lic["_ultima_descarga"]), {FECHA_2})
        cm = final[final["_tipo"] == "CM"]
        self.assertEqual(set(cm["_en_ultima_descarga"]), {"True"})
        self.assertEqual(set(cm["_ultima_descarga"]), {FECHA_1})

    def test_interrupted_run_withdraws_only_what_it_read_and_resume_completes_it(self):
        portal = self._three_orgs()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_1)[0], 0)
            portal.lic[2] = [r for r in portal.lic[2] if r["id"] != 700001]
            portal.cm[48] = [r for r in portal.cm[48] if r["id"] != 500001]
            portal.fail_orgs = {48}
            # La descarga se corta en el organismo 48 (HTTP 403)
            code, _ = run_at(cli_args(out, "base", "--max-org-id", "48"), portal, FECHA_2)
            self.assertEqual(code, 1)
            # Una descarga a medias sin acumular no deja empezar otra
            code, stdout = run_at(cli_args(out, "base", "--max-org-id", "48"), portal, FECHA_3)
            self.assertEqual(code, 1)
            self.assertIn("todavía no está en contratos_galicia.csv", stdout)
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_3)[0], 0)
            partial = read_final(out).set_index("id")
            # Se reanuda sin el fallo: la misma descarga (misma fecha), ahora completa
            portal.fail_orgs = set()
            self.assertEqual(run_at(cli_args(out, "base", "--resume", "--max-org-id", "48"), portal, FECHA_4)[0], 0)
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_4)[0], 0)
            final = read_final(out).set_index("id")
            manifest = json.loads((out / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))

        self.assertEqual(partial.loc["700001", "_en_ultima_descarga"], "False")
        self.assertEqual(partial.loc["500001", "_en_ultima_descarga"], "True")
        self.assertEqual(partial.loc["500001", "_ultima_descarga"], FECHA_1)
        self.assertEqual(set(partial.loc[partial["_organismo_id"] == "48", "_ultima_descarga"]), {FECHA_1})
        self.assertEqual(final.loc["500001", "_en_ultima_descarga"], "False")
        self.assertEqual(final.loc["500001", "_ultima_descarga"], FECHA_1)
        vigentes = final[final["_en_ultima_descarga"] == "True"]
        self.assertEqual(set(vigentes["_ultima_descarga"]), {FECHA_2})
        self.assertEqual(len(final), 12)
        self.assertEqual(manifest["fecha_descarga"], FECHA_2)
        self.assertTrue(manifest["acumulada"])
        self.assertEqual(sorted(manifest["ambito"]), ["2", "3", "48"])

    def test_incomplete_window_does_not_withdraw_its_contracts(self):
        cm = fake_cm_records(4, first_id=600000)
        # Otro contrato en la ventana de 600003 (2021-04-04): al retirarse este, la
        # ventana sigue llegando completa y no vacía.
        cm.append(dict(cm[3], id=600004))
        portal = FakePortal(cm={3: cm}, lic={3: fake_lic_records(3)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "3"), portal, FECHA_1)[0], 0)
            # La paginación se salta 600002 y la licitación 824001, que el portal sigue
            # contando (recordsFiltered / recordsTotal); 600003 sí se ha retirado de
            # verdad (su ventana llega completa).
            portal.hidden_ids = {600002, 824001}
            portal.cm[3] = [r for r in portal.cm[3] if r["id"] != 600003]
            code, stdout = run_at(cli_args(out, "--organismo", "3"), portal, FECHA_2)
            self.assertEqual(code, 0)
            final = read_final(out).set_index("id")
            manifest = json.loads((out / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))

        self.assertIn("ventana incompleta", stdout)
        self.assertIn("Org 3 LIC: DESAJUSTE esperados=3 descargados=2", stdout)
        self.assertEqual(final.loc["824001", "_en_ultima_descarga"], "True")
        self.assertEqual(final.loc["824001", "_ultima_descarga"], FECHA_1)
        self.assertFalse(manifest["ambito"]["3"]["LIC"])
        self.assertEqual(final.loc["600002", "_en_ultima_descarga"], "True")
        self.assertEqual(final.loc["600002", "_ultima_descarga"], FECHA_1)
        self.assertEqual(final.loc["600003", "_en_ultima_descarga"], "False")
        self.assertEqual(final.loc["600004", "_en_ultima_descarga"], "True")
        scope = manifest["ambito"]["3"]
        incomplete = scope["CM_incompletas"]
        self.assertEqual(len(incomplete), 1)
        self.assertEqual((incomplete[0]["filtrados"], incomplete[0]["filas"]), (1, 0))
        # Ámbito: dos tramos de ventanas completas, antes y después de la incompleta
        self.assertEqual(len(scope["CM"]), 2)
        self.assertEqual(scope["CM"][0][0], scraper_galicia.DATE_ORIGIN)
        self.assertEqual(
            (date.fromisoformat(scope["CM"][0][1]) + timedelta(days=1)).isoformat(), incomplete[0]["desde"]
        )
        self.assertEqual(
            (date.fromisoformat(incomplete[0]["hasta"]) + timedelta(days=1)).isoformat(), scope["CM"][1][0]
        )

    def test_failed_or_empty_downloads_withdraw_nothing(self):
        portal = self._three_orgs()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_1)[0], 0)
            before = snapshot(out)
            final_before = before[scraper_galicia.FINAL_CSV_NAME]

            # 1. Falla el único organismo pedido: no hay descarga y la tabla no cambia
            portal.fail_orgs = {48}
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_2)[0], 1)
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 1)
            self.assertEqual((out / scraper_galicia.FINAL_CSV_NAME).read_bytes(), final_before)
            # La descarga anterior no se ha perdido: está en _historico/
            self.assertTrue(any(n.startswith("contratos_galicia_base__") for n in historico(out)))

            # 2. El organismo responde vacío (0 registros): no se retira nada suyo
            portal.fail_orgs = set()
            portal.empty_orgs = {3}
            code, _ = run_at(cli_args(out, "--organismo", "3"), portal, FECHA_3)
            self.assertEqual(code, 1)  # sin ningún contrato no hay CSV base que acumular
            self.assertEqual((out / scraper_galicia.FINAL_CSV_NAME).read_bytes(), final_before)

            # 3. El 3 declara sus contratos menores pero ninguna ventana trae ninguno:
            # tampoco se retira nada suyo (el 48 sí se lee con normalidad)
            portal.empty_orgs = set()
            portal.windowless_orgs = {3}
            portal.lic[48] = portal.lic[48][:1]
            code, _ = run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_3)
            self.assertEqual(code, 0)
            windowless = read_final(out)
            self.assertEqual(
                windowless.loc[windowless["_organismo_id"] == "48", "_en_ultima_descarga"].value_counts().to_dict(),
                {"True": 4, "False": 1},
            )

            # 4. El 2 (solo LIC) y el 3 (solo CM) aparecen en el descubrimiento pero
            # al barrerlos responden 0 registros: no se retira nada suyo
            portal.windowless_orgs = set()
            portal.empty_scan_orgs = {2, 3}
            code, _ = run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_4)
            self.assertEqual(code, 0)
            final = read_final(out)
            manifest = json.loads((out / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))
            self.assertEqual(manifest["ambito"]["2"]["LIC"], False)
            self.assertEqual(manifest["ambito"]["3"]["CM"], [])

            # 5. Descubrimiento completo con el 3 vacío también en las sondas: no se barre
            portal.empty_scan_orgs = set()
            portal.empty_orgs = {3}
            code, _ = run_at(cli_args(out, "--max-org-id", "48"), portal, FECHA_4)
            self.assertEqual(code, 0)
            self.assertEqual(
                read_final(out).loc[lambda df: df["_organismo_id"] == "3", "_ultima_descarga"].tolist(), [FECHA_1] * 4
            )

            # 6. Un CSV base solo con la cabecera no cambia la tabla
            final_bytes = (out / scraper_galicia.FINAL_CSV_NAME).read_bytes()
            (out / scraper_galicia.BASE_CSV_NAME).write_text(
                ";".join(scraper_galicia.BASE_EXPORT_FIELDS) + "\n", encoding="utf-8-sig"
            )
            code, stdout = run_at(cli_args(out, "merge"), portal, FECHA_4)
            self.assertEqual(code, 1)
            self.assertIn("una descarga vacía no cambia la tabla final", stdout)
            self.assertEqual((out / scraper_galicia.FINAL_CSV_NAME).read_bytes(), final_bytes)

        # Tras el paso 4: el 2 y el 3 siguen como estaban (vistos por última vez en el
        # paso 3 y en la primera descarga); el 48, al día
        for org, n, last_seen in (("2", 3, FECHA_3), ("3", 4, FECHA_1)):
            rows = final[final["_organismo_id"] == org]
            self.assertEqual(len(rows), n)
            self.assertEqual(set(rows["_en_ultima_descarga"]), {"True"})
            self.assertEqual(set(rows["_ultima_descarga"]), {last_seen})
        org48 = final[(final["_organismo_id"] == "48") & (final["_en_ultima_descarga"] == "True")]
        self.assertEqual(len(org48), 4)
        self.assertEqual(set(org48["_ultima_descarga"]), {FECHA_4})

    def test_detail_cache_never_loses_a_downloaded_detail(self):
        portal = FakePortal(lic={48: fake_lic_records(2)}, cm={48: fake_cm_records(2)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            # Al volver a pedirlas: error, ficha vacía y ficha cambiada
            portal.details = {"824000": 404, "CM500000": "vacia", "824001": "REF-NUEVA"}
            self.assertEqual(run_at(cli_args(out, "detail", "--force-detail"), portal, FECHA_2)[0], 0)
            conn = scraper_galicia.init_detail_db(out)
            cache = {
                row["record_id"]: (row["status"], json.loads(row["mapped_json"])["detail_referencia"])
                for row in conn.execute("SELECT record_id, status, mapped_json FROM detail_cache")
            }
            archived = [
                (row["record_id"], json.loads(row["mapped_json"])["detail_referencia"])
                for row in conn.execute("SELECT record_id, mapped_json FROM detail_cache_historico")
            ]
            conn.close()
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 0)
            merged = read_final(out).set_index("id")
            # Sin la caché (perdida o sustituida), la tabla final conserva las fichas
            (out / scraper_galicia.DETAIL_DB_NAME).unlink()
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 0)
            without_cache = read_final(out).set_index("id")
            # ...también con una caché que tiene un error donde la tabla tenía la ficha
            conn = scraper_galicia.init_detail_db(out)
            scraper_galicia.persist_detail_results(conn, [detail_cache_row("LIC", 824000, 48, "failed")])
            conn.close()
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 0)
            failed_cache = read_final(out).set_index("id")

        self.assertEqual(
            cache,
            {
                "824000": ("done", "REF-824000"),
                "824001": ("done", "REF-NUEVA"),
                "500000": ("done", "REF-CM500000"),
                "500001": ("done", "REF-CM500001"),
            },
        )
        self.assertEqual(archived, [("824001", "REF-824001")])
        self.assertEqual(merged.loc["824000", "detail_referencia"], "REF-824000")
        self.assertEqual(merged.loc["500000", "detail_referencia"], "REF-CM500000")
        self.assertEqual(merged.loc["824001", "detail_referencia"], "REF-NUEVA")
        self.assertEqual(set(merged["detail_status"]), {"done"})
        self.assertTrue(without_cache.equals(merged))
        self.assertTrue(failed_cache.equals(merged))

    def test_detail_without_resume_keeps_cache_and_retries_exhausted_failures(self):
        portal = FakePortal(lic={48: fake_lic_records(2)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [dict(r, _organismo_id=48, _tipo="LIC") for r in portal.lic[48]], out
                )
            conn = scraper_galicia.init_detail_db(out)
            scraper_galicia.persist_detail_results(conn, [
                detail_cache_row("LIC", 824000, 48, "done"),
                detail_cache_row("LIC", 824001, 48, "failed", attempts=scraper_galicia.DETAIL_MAX_ATTEMPTS),
            ])
            conn.close()
            self.assertEqual(run_at(cli_args(out, "detail", "--resume"), portal, FECHA_1)[0], 0)
            self.assertEqual(portal.calls("/licitacion", "POST"), [])
            self.assertEqual(run_at(cli_args(out, "detail"), portal, FECHA_1)[0], 0)
            conn = scraper_galicia.init_detail_db(out)
            statuses = dict(conn.execute("SELECT record_id, status FROM detail_cache").fetchall())
            conn.close()

        # Sin --resume ya no se borra la caché: la 'done' sigue y la agotada se reintenta
        self.assertEqual([c["data"]["N"] for c in portal.calls("/licitacion", "POST")], ["824001"])
        self.assertEqual(statuses, {"824000": "done", "824001": "done"})

    def test_seed_adds_only_missing_keys_of_the_run_scope(self):
        lic = fake_lic_records(2)
        cm = fake_cm_records(2)  # publicado 2018-01-01 y 2019-02-02
        # Un contrato de 2023-06-01 que la paginación se salta (su ventana llega incompleta)
        skipped = dict(cm[0], id=500099, publicado="2023-06-01T00:00:00+0100")
        portal = FakePortal(
            lic={48: list(lic), 2: fake_lic_records(1, first_id=700000)}, cm={48: list(cm) + [skipped]}
        )
        portal.hidden_ids = {500099}
        seed_rows = [
            # En la descarga (con el importe inflado del publicado): no se añade
            dict(lic[0], _organismo_id=48, _tipo="LIC", estado="4.0"),
            dict(cm[1], _organismo_id=48, _tipo="CM"),
            # Retiradas por el portal, del ámbito de la descarga: se añaden
            dict(lic[1], id=824009, _organismo_id=48, _tipo="LIC", estado="6.0", importe=129.18),
            dict(cm[0], id=500007, _organismo_id=48, _tipo="CM", importe=674.78),
            # ...también de un trimestre que ahora el portal da vacío (retirado entero)
            dict(cm[0], id=500009, _organismo_id=48, _tipo="CM", importe=15000.0,
                 publicado="2022-01-10T00:00:00+0100"),
            # Mismo id que una licitación descargada pero es un CM: otra clave
            dict(cm[0], id=824000, _organismo_id=48, _tipo="CM", importe=82.5),
            # CM de la ventana incompleta y licitación de un organismo que no se ha
            # vuelto a leer: fuera del ámbito, no se añaden
            dict(cm[0], id=500008, _organismo_id=48, _tipo="CM", publicado="2023-06-01T00:00:00+0100"),
            dict(lic[0], id=700005, _organismo_id=2, _tipo="LIC"),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "contratos_galicia_publicado.parquet", seed_rows)
            plain, seeded = tmp / "sin", tmp / "con"
            self.assertEqual(run_at(cli_args(plain, "--organismo", "48"), portal, FECHA_1)[0], 0)
            code, stdout = run_at(cli_args(seeded, "--organismo", "48", "--semilla", str(seed)), portal, FECHA_1)
            self.assertEqual(code, 0)
            without_seed = read_final(plain)
            final = read_final(seeded)
            # Sembrar otra vez no añade nada ni cambia la tabla
            before = snapshot(seeded)
            self.assertEqual(run_at(cli_args(seeded, "merge", "--semilla", str(seed)), portal, FECHA_1)[0], 0)
            self.assertEqual(snapshot(seeded), before)
            self.assertEqual(historico(seeded), [])
            parquet = pd.read_parquet(seeded / scraper_galicia.FINAL_PARQUET_NAME) if scraper_galicia.HAS_PYARROW else None

            # Una salida de este script como semilla necesita --origen-semilla, y la
            # semilla no puede ser la propia salida
            if parquet is not None:
                code, stdout2 = run_at(
                    cli_args(plain, "merge", "--semilla", str(seeded / scraper_galicia.FINAL_PARQUET_NAME)),
                    portal, FECHA_1,
                )
                self.assertEqual(code, 1)
                self.assertIn("--origen-semilla", stdout2)
                code, stdout3 = run_at(
                    cli_args(plain, "merge", "--semilla", str(plain / scraper_galicia.FINAL_PARQUET_NAME)),
                    portal, FECHA_1,
                )
                self.assertEqual(code, 1)
                self.assertIn("usa otra carpeta de salida", stdout3)

        self.assertIn("8 filas leídas → 4 añadidas", stdout)
        self.assertIn("2 filas de la semilla fuera del ámbito", stdout)
        # Las filas de la descarga no cambian (solo se añaden las columnas de la
        # semilla); detail_updated_at es la hora de cada petición de ficha.
        download = final.iloc[: len(without_seed)]
        same = [column for column in without_seed.columns if column != "detail_updated_at"]
        self.assertTrue(download[same].equals(without_seed[same]))
        self.assertEqual(set(download["_origen"]), {""})
        self.assertEqual(set(download[scraper_galicia.SEED_AMOUNT_COLUMN]), {""})
        added = final.iloc[len(without_seed):]
        self.assertEqual(
            sorted(zip(added["_tipo"], added["id"])),
            [("CM", "500007"), ("CM", "500009"), ("CM", "824000"), ("LIC", "824009")],
        )
        self.assertEqual(set(added["_origen"]), {"release v2026.02"})
        self.assertEqual(set(added["_en_ultima_descarga"]), {"False"})
        self.assertEqual(set(added["_primera_descarga"]) | set(added["_ultima_descarga"]), {""})
        by_key = added.set_index(["_tipo", "id"])
        # Importe inflado del publicado: fuera de 'importe'
        self.assertEqual(by_key.loc[("LIC", "824009"), "importe"], "")
        self.assertEqual(by_key.loc[("LIC", "824009"), scraper_galicia.SEED_AMOUNT_COLUMN], "12918")
        self.assertEqual(by_key.loc[("CM", "500007"), scraper_galicia.SEED_AMOUNT_COLUMN], "67478")
        self.assertEqual(by_key.loc[("CM", "500009"), scraper_galicia.SEED_AMOUNT_COLUMN], "150000")
        self.assertEqual(by_key.loc[("LIC", "824009"), "estado"], "6.0")
        self.assertEqual(by_key.loc[("CM", "500007"), "estado"], "")
        self.assertEqual(by_key.loc[("CM", "500007"), "publicado"], "2018-01-01")
        self.assertEqual(set(added["detail_status"]), {"missing"})
        if parquet is not None:
            self.assertEqual(str(parquet["_en_ultima_descarga"].dtype), "bool")
            self.assertEqual(
                sorted(parquet[scraper_galicia.SEED_AMOUNT_COLUMN].dropna().tolist()), [825, 12918, 67478, 150000]
            )
            self.assertTrue(pd.api.types.is_datetime64_any_dtype(parquet["publicado"]))

    def test_seed_does_not_duplicate_a_contract_already_withdrawn_in_the_table(self):
        lic = fake_lic_records(2)
        portal = FakePortal(lic={48: list(lic)})
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            out = tmp / "salida"
            seed = published_seed(tmp / "publicado.parquet", [dict(lic[1], _organismo_id=48, _tipo="LIC", estado="4.0")])
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            portal.lic[48] = lic[:1]
            self.assertEqual(
                run_at(cli_args(out, "--organismo", "48", "--semilla", str(seed)), portal, FECHA_2)[0], 0
            )
            final = read_final(out)

        # 824001 ya estaba (de la primera descarga, ahora retirada): no se vuelve a añadir
        self.assertEqual(final["id"].tolist(), ["824000", "824001"])
        self.assertEqual(final["_en_ultima_descarga"].tolist(), ["True", "False"])
        self.assertEqual(set(final["_origen"]), {""})

    def test_rerun_without_changes_writes_nothing_new(self):
        portal = FakePortal(lic={48: fake_lic_records(2)}, cm={48: fake_cm_records(3)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            first = snapshot(out)
            base_mtime = (out / scraper_galicia.BASE_CSV_NAME).stat().st_mtime
            first_final = read_final(out)
            # 1. merge otra vez (sin descarga nueva): no cambia nada
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 0)
            self.assertEqual(snapshot(out), first)
            self.assertEqual(historico(out), [])
            # 2. Descarga nueva sin cambios en el portal: la capa cruda no cambia, no se
            # piden fichas y la tabla solo actualiza _ultima_descarga
            portal.requests.clear()
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_2)[0], 0)
            second = snapshot(out)
            posts = portal.calls("/licitacion", "POST")
            final = read_final(out)
            hist = historico(out)
            self.assertEqual((out / scraper_galicia.BASE_CSV_NAME).stat().st_mtime, base_mtime)

        for name in (scraper_galicia.BASE_CSV_NAME, scraper_galicia.BASE_PARQUET_NAME):
            self.assertEqual(second[name], first[name])
        self.assertEqual(posts, [])
        self.assertFalse(any(n.startswith("contratos_galicia_base__") for n in hist))
        self.assertEqual(len(final), len(first_final))
        self.assertTrue(final.drop(columns="_ultima_descarga").equals(first_final.drop(columns="_ultima_descarga")))
        self.assertEqual(set(final["_ultima_descarga"]), {FECHA_2})

    def test_resume_of_a_download_started_by_the_previous_scraper_version(self):
        portal = self._three_orgs()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            # Descarga a medias de la versión anterior: CSV base con el organismo 2 y un
            # progreso sin fecha ni ámbito
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [dict(r, _organismo_id=2, _tipo="LIC") for r in portal.lic[2]], out
                )
            base_csv = out / scraper_galicia.BASE_CSV_NAME
            (out / scraper_galicia.BASE_PROGRESS_NAME).write_text(
                json.dumps({"saved_at": "2026-09-28T06:27:23", "completed_orgs": [2],
                            "base_csv_bytes": base_csv.stat().st_size}),
                encoding="utf-8",
            )
            legacy_date = scraper_galicia.file_date_iso(base_csv)
            code, _ = run_at(cli_args(out, "--resume", "--max-org-id", "48"), portal, FECHA_2)
            self.assertEqual(code, 0)
            manifest = json.loads((out / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))
            final = read_final(out)
            lic_requests = {
                int(m.group(1)) for r in portal.requests
                if (m := re.search(r"/organismos/(\d+)/licitaciones/table", r["url"])) and r["params"]["length"] != "1"
            }

        # El 2 no se vuelve a pedir; la descarga lleva la fecha del CSV base empezado
        self.assertNotIn(2, lic_requests)
        self.assertEqual(manifest["fecha_descarga"], legacy_date)
        self.assertEqual(sorted(manifest["ambito"]), ["3", "48"])
        self.assertTrue(manifest["acumulada"])
        self.assertEqual(len(final), 12)
        self.assertEqual(set(final["_ultima_descarga"]), {legacy_date})
        self.assertEqual(set(final["_en_ultima_descarga"]), {"True"})

    def test_legacy_base_without_final_table_must_be_merged_before_a_new_download(self):
        portal = FakePortal(lic={48: fake_lic_records(2)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            # CSV base de la versión anterior (sin manifiesto) y sin tabla final
            with patch("sys.stdout", new_callable=io.StringIO):
                scraper_galicia.append_base_records(
                    [dict(r, _organismo_id=48, _tipo="LIC") for r in portal.lic[48]], out
                )
            base_bytes = (out / scraper_galicia.BASE_CSV_NAME).read_bytes()
            code, stdout = run_at(cli_args(out, "base", "--organismo", "48"), portal, FECHA_2)
            self.assertEqual(code, 1)
            self.assertIn("todavía no está en contratos_galicia.csv", stdout)
            self.assertEqual((out / scraper_galicia.BASE_CSV_NAME).read_bytes(), base_bytes)
            self.assertEqual(historico(out), [])
            # Tras 'merge' ya se puede empezar otra descarga (la anterior va a _historico/)
            self.assertEqual(run_at(cli_args(out, "merge"), portal, FECHA_2)[0], 0)
            portal.lic[48] = portal.lic[48] + fake_lic_records(1, first_id=824500)
            self.assertEqual(run_at(cli_args(out, "base", "--organismo", "48"), portal, FECHA_3)[0], 0)
            hist = historico(out)
        self.assertTrue(any(n.startswith("contratos_galicia_base__") and n.endswith(".csv") for n in hist))

    def test_final_table_of_the_previous_scraper_version_is_kept(self):
        lic = fake_lic_records(3)
        portal = FakePortal(lic={48: list(lic)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            # Tabla final como la escribía la versión anterior: sin columnas de control
            legacy = read_final(out).drop(columns=META)
            legacy.to_csv(out / scraper_galicia.FINAL_CSV_NAME, sep=";", index=False, encoding="utf-8-sig")
            legacy_date = scraper_galicia.file_date_iso(out / scraper_galicia.FINAL_CSV_NAME)
            portal.lic[48] = lic[:2]
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_2)[0], 0)
            final = read_final(out).set_index("id")

        self.assertEqual(len(final), 3)
        self.assertEqual(final.loc["824002", "_en_ultima_descarga"], "False")
        self.assertEqual(final.loc["824002", "_ultima_descarga"], legacy_date)
        self.assertEqual(final.loc["824002", "detail_referencia"], "REF-824002")
        self.assertEqual(set(final["_primera_descarga"]), {legacy_date})

    def test_final_parquet_without_its_csv_is_not_replaced(self):
        if not scraper_galicia.HAS_PYARROW:
            self.skipTest("pyarrow no disponible")
        portal = FakePortal(lic={48: fake_lic_records(2)})
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            self.assertEqual(run_at(cli_args(out, "--organismo", "48"), portal, FECHA_1)[0], 0)
            (out / scraper_galicia.FINAL_CSV_NAME).unlink()
            parquet = (out / scraper_galicia.FINAL_PARQUET_NAME).read_bytes()
            code, stdout = run_at(cli_args(out, "merge"), portal, FECHA_2)
            self.assertEqual(code, 1)
            self.assertIn("la tabla final se acumula desde su CSV", stdout)
            self.assertEqual((out / scraper_galicia.FINAL_PARQUET_NAME).read_bytes(), parquet)
            self.assertFalse((out / scraper_galicia.FINAL_CSV_NAME).exists())

    def test_window_check_requires_exactly_the_declared_rows_inside_the_window(self):
        def check(recs, filtrados):
            informe = {
                "filtrados": filtrados,
                "filas": len(recs),
                "unicos": len({r.get("id") for r in recs if r.get("id") not in (None, "")}),
            }
            return scraper_galicia.window_check(recs, "2026-01-01", "2026-03-31", informe)

        ok = [{"id": 1, "publicado": "01-01-2026"}, {"id": 2, "publicado": "31-03-2026"}]
        self.assertTrue(check(ok, 2)["completa"])
        self.assertTrue(check([], 0)["completa"])  # vacía y coherente
        self.assertFalse(check(ok, 3)["completa"])  # faltan filas
        self.assertFalse(check(ok, None)["completa"])  # sin recordsFiltered no se sabe
        self.assertFalse(check(ok + [dict(ok[0])], 3)["completa"])  # fila repetida
        outside = check([ok[0], {"id": 3, "publicado": "01-04-2026"}], 2)
        self.assertEqual((outside["fuera"], outside["completa"]), (1, False))
        no_date = check([ok[0], {"id": 3, "publicado": None}], 2)
        self.assertEqual((no_date["fuera"], no_date["completa"]), (1, False))
        no_id = check([ok[0], {"id": None, "publicado": "02-01-2026"}], 2)
        self.assertEqual((no_id["sin_id"], no_id["completa"]), (1, False))

    def test_cm_window_with_a_row_without_id_is_incomplete(self):
        rows = fake_cm_records(2)
        rows.append(dict(rows[0], id=None))  # en la ventana de 500000
        portal = FakePortal(cm={48: rows})
        informe = {}
        with patch.object(requests.Session, "request", autospec=True, side_effect=portal), patch.object(
            scraper_galicia, "_LOG_PATH", None
        ), patch.object(scraper_galicia, "DELAY", 0), patch("sys.stdout", new_callable=io.StringIO):
            session = scraper_galicia.Session()
            got = scraper_galicia.paginate_cm_full(session, 48, informe=informe)

        self.assertEqual(sorted(record["id"] for record in got), [500000, 500001])
        incomplete = informe["CM"]["incompletas"]
        self.assertEqual(len(incomplete), 1)
        self.assertEqual((incomplete[0]["filtrados"], incomplete[0]["filas"], incomplete[0]["unicos"]), (2, 2, 1))
        self.assertEqual(incomplete[0]["sin_id"], 1)
        covered = [
            window for window in informe["CM"]["ventanas"]
            if window[0] <= "2018-01-01" <= window[1]
        ]
        self.assertEqual(covered, [])

    def test_save_outputs_keeps_the_previous_version(self):
        record = {"id": 1, "_tipo": "LIC", "_organismo_id": 48, "objeto": "Contrato", "importe": 100.0}
        with tempfile.TemporaryDirectory() as tmpdir, patch("sys.stdout", new_callable=io.StringIO):
            out = Path(tmpdir)
            scraper_galicia.save_outputs([record], out)
            scraper_galicia.save_outputs([record], out)
            self.assertEqual(historico(out), [])
            scraper_galicia.save_outputs([dict(record, objeto="Otro")], out)
            hist = historico(out)
        self.assertEqual(len([n for n in hist if n.endswith(".csv")]), 1)
        if scraper_galicia.HAS_PYARROW:
            self.assertEqual(len([n for n in hist if n.endswith(".parquet")]), 1)

    def test_listing_fingerprint_compares_values_as_in_parquet(self):
        rows = pd.DataFrame(
            {
                "id": ["1", "1.0", "1", "1", "1", "2"],
                "estado": ["4", "4.0", "4", "4", "5", "4"],
                "publicado": ["2024-01-15", "2024-01-15 00:00:00", "2024-01-16", "2024-01-15", "2024-01-15",
                              "2024-01-15"],
                "importe": ["10.5"] * 6,
                "_organismo_id": ["48"] * 6,
                "_tipo": ["LIC"] * 6,
                "objeto": ["Obra"] * 6,
            }
        )
        huella = scraper_galicia.listing_fingerprint(rows)
        self.assertEqual(huella[0], huella[1])
        self.assertEqual(len(set(huella[[0, 2, 4, 5]])), 4)
        self.assertEqual(huella[0], huella[3])
        # Un texto que no es número no se confunde con el vacío
        extra = pd.DataFrame({"id": ["", "x"], "_tipo": ["CM", "CM"]})
        a, b = scraper_galicia.listing_fingerprint(extra)
        self.assertNotEqual(a, b)

    def test_rows_in_scope_uses_complete_windows_by_publicado(self):
        df = pd.DataFrame(
            {
                "_tipo": ["CM", "CM", "CM", "CM", "LIC", "LIC", "CM"],
                "_organismo_id": ["3", "3", "3", "3", "3", "2", "48"],
                "publicado": ["2026-01-01", "2026-03-31 00:00:00", "2025-12-31", "", "2020-01-01",
                              "2020-01-01", "1999-01-01"],
            }
        )
        scope = {"3": {"LIC": False, "CM": [["2026-01-01", "2026-03-31"]]}, "2": {"LIC": True}, "48": {"CM": "todo"}}
        self.assertEqual(
            scraper_galicia.rows_in_scope(df, scope).tolist(),
            [True, True, False, False, False, True, True],
        )


# ── Semilla de organismos que el portal ha retirado enteros ─────────────────

def seed_lic(org, rid):
    """Licitación de la semilla del organismo `org` (published_seed)."""
    return dict(fake_lic_records(1, first_id=rid)[0], _organismo_id=org, _tipo="LIC")


def seed_cm(org, rid, publicado="2019-02-02T00:00:00+0100"):
    """Contrato menor de la semilla del organismo `org` (published_seed)."""
    return dict(fake_cm_records(1, first_id=rid)[0], _organismo_id=org, _tipo="CM", publicado=publicado)


def read_manifest(output_dir):
    return json.loads((Path(output_dir) / scraper_galicia.BASE_PROGRESS_NAME).read_text(encoding="utf-8"))


def seed_added(final):
    """(_tipo, id) de las filas de la tabla final que vienen de la semilla."""
    return sorted(zip(final.loc[final["_origen"] != "", "_tipo"], final.loc[final["_origen"] != "", "id"]))


class GaliciaOrganismosRetiradosTests(unittest.TestCase):
    """Filas de la semilla de organismos que el portal ha retirado enteros
    (decisión 4 del propietario, PLAN_PUESTA_A_PUNTO): se añaden solo si la
    descarga leyó la lista completa de organismos del portal y no están en ella."""

    def _portal(self):
        # Organismos 2 (LIC), 3 (CM) y 48 (los dos). La paginación se salta un CM del
        # 48 (2023-06-01), así que su ventana llega incompleta. El 5 ya no está.
        portal = FakePortal(
            lic={2: fake_lic_records(3, first_id=700000), 48: fake_lic_records(2)},
            cm={
                3: fake_cm_records(4, first_id=600000),
                48: fake_cm_records(3) + [dict(fake_cm_records(1)[0], id=500099, publicado="2023-06-01T00:00:00+0100")],
            },
        )
        portal.hidden_ids = {500099}
        return portal

    def test_seed_rows_of_an_organism_missing_from_the_portal_list_are_added(self):
        portal = self._portal()
        seed_rows = [
            # 5: no está en la lista del portal (retirado entero): se añaden
            seed_lic(5, 910000), seed_lic(5, 910001), seed_cm(5, 910100),
            # 99: fuera de los ids probados (--max-org-id 48): no se sabe, no se añade
            seed_lic(99, 990000),
            # 48 está en la lista: un CM de su ventana incompleta no se añade
            seed_cm(48, 500098, "2023-06-01T00:00:00+0100"),
            # 3 está en la lista: un CM retirado de una ventana completa sí (ámbito)
            seed_cm(3, 600009),
            # 2: una licitación que sigue en el portal (clave presente)
            seed_lic(2, 700000),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", seed_rows)
            out = tmp / "salida"
            code, stdout = run_at(cli_args(out, "--max-org-id", "48", "--semilla", str(seed)), portal, FECHA_1)
            self.assertEqual(code, 0)
            final = read_final(out)
            manifest = read_manifest(out)
            # Otro merge con la misma semilla no añade nada ni cambia la tabla
            before = snapshot(out)
            code, stdout2 = run_at(cli_args(out, "merge", "--semilla", str(seed)), portal, FECHA_1)
            self.assertEqual(code, 0)
            self.assertEqual(snapshot(out), before)
            self.assertEqual(historico(out), [])

        self.assertIn("7 filas leídas → 4 añadidas", stdout)
        self.assertIn("2 filas de la semilla fuera del ámbito", stdout)
        self.assertIn("organismo 99: 1", stdout)
        self.assertIn("organismo 48: 1", stdout)
        self.assertIn("3 filas añadidas de 1 organismos que el portal ha retirado enteros", stdout)
        self.assertIn(f"no están en su lista de organismos del {FECHA_1}): organismo 5: 3", stdout)
        self.assertIn("de semillas 4 (de organismos retirados 3)", stdout)
        self.assertIn("7 filas leídas → 0 añadidas", stdout2)
        self.assertNotIn("retirado enteros", stdout2)
        self.assertEqual(
            seed_added(final),
            [("CM", "600009"), ("CM", "910100"), ("LIC", "910000"), ("LIC", "910001")],
        )
        added = final[final["_origen"] != ""]
        self.assertEqual(set(added["_origen"]), {"release v2026.02"})
        self.assertEqual(set(added["_en_ultima_descarga"]), {"False"})
        self.assertEqual(set(added["_primera_descarga"]) | set(added["_ultima_descarga"]), {""})
        self.assertEqual(set(added.loc[added["_organismo_id"] == "5", scraper_galicia.SEED_AMOUNT_COLUMN]),
                         {"123456", "67478"})
        self.assertEqual(set(final.loc[final["_origen"] == "", "_en_ultima_descarga"]), {"True"})
        # La lista de organismos del portal queda en el manifiesto
        self.assertEqual(len(manifest["descubrimientos"]), 1)
        lista = manifest["descubrimientos"][0]
        self.assertEqual((lista["fecha"], lista["hasta"]), (FECHA_1, 48))
        self.assertEqual(
            lista["organismos"],
            {"2": {"CM": 0, "LIC": 3}, "3": {"CM": 4, "LIC": 0}, "48": {"CM": 4, "LIC": 2}},
        )

    def test_download_without_the_portal_list_retires_no_organism(self):
        seed_rows = [seed_lic(5, 910000), seed_cm(5, 910100), seed_lic(48, 824009)]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", seed_rows)
            # 1. Descarga de un solo organismo: no lee la lista de organismos
            single = tmp / "organismo"
            code, stdout = run_at(
                cli_args(single, "--organismo", "48", "--semilla", str(seed)), self._portal(), FECHA_1
            )
            self.assertEqual(code, 0)
            self.assertNotIn("descubrimientos", read_manifest(single))
            single_added = seed_added(read_final(single))
            # 2. Descarga de la versión anterior del scraper: manifiesto sin la lista
            legacy = tmp / "anterior"
            self.assertEqual(run_at(cli_args(legacy, "base", "--max-org-id", "48"), self._portal(), FECHA_1)[0], 0)
            manifest = read_manifest(legacy)
            del manifest["descubrimientos"]
            (legacy / scraper_galicia.BASE_PROGRESS_NAME).write_text(json.dumps(manifest), encoding="utf-8")
            code, stdout_legacy = run_at(cli_args(legacy, "merge", "--semilla", str(seed)), self._portal(), FECHA_1)
            self.assertEqual(code, 0)
            legacy_added = seed_added(read_final(legacy))
            # 3. CSV base sin manifiesto (versión más antigua)
            (legacy / scraper_galicia.BASE_PROGRESS_NAME).unlink()
            code, stdout_none = run_at(cli_args(legacy, "merge", "--semilla", str(seed)), self._portal(), FECHA_1)
            self.assertEqual(code, 0)
            none_added = seed_added(read_final(legacy))

        # La licitación retirada del 48 (leído completo) sí se añade; las del 5, no
        self.assertEqual(single_added, [("LIC", "824009")])
        self.assertEqual(legacy_added, [("LIC", "824009")])
        self.assertEqual(none_added, [("LIC", "824009")])
        for out in (stdout, stdout_legacy, stdout_none):
            self.assertIn("sin la lista completa de organismos del portal", out)
            self.assertIn("no se da por retirado ningún organismo; 2 filas de la semilla de 1 organismos que "
                          "esta descarga no ha leído no se añaden", out)
            self.assertIn("(de organismos retirados 0)", out)
            self.assertNotIn("retirado enteros", out)
        self.assertIn("descarga con --organismo", stdout)

    def test_interrupted_download_does_not_retire_listed_organisms_it_did_not_read(self):
        portal = self._portal()
        portal.fail_orgs = {48}
        seed_rows = [
            seed_lic(5, 910000),
            # Del 48, que está en la lista pero la descarga se corta antes de leerlo
            seed_lic(48, 824009),
            seed_cm(48, 500097),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", seed_rows)
            out = tmp / "salida"
            code, _ = run_at(cli_args(out, "base", "--max-org-id", "48"), portal, FECHA_1)
            self.assertEqual(code, 1)
            code, stdout = run_at(cli_args(out, "merge", "--semilla", str(seed)), portal, FECHA_1)
            self.assertEqual(code, 0)
            partial = seed_added(read_final(out))
            # Se reanuda sin el fallo: el 48 se lee y sus filas retiradas entran por el ámbito
            portal.fail_orgs = set()
            self.assertEqual(run_at(cli_args(out, "base", "--resume", "--max-org-id", "48"), portal, FECHA_2)[0], 0)
            code, stdout2 = run_at(cli_args(out, "merge", "--semilla", str(seed)), portal, FECHA_2)
            self.assertEqual(code, 0)
            final = read_final(out)
            manifest = read_manifest(out)

        self.assertEqual(partial, [("LIC", "910000")])
        self.assertIn("2 filas de la semilla fuera del ámbito", stdout)
        self.assertIn("organismo 48: 2", stdout)
        self.assertIn("1 filas añadidas de 1 organismos que el portal ha retirado enteros", stdout)
        self.assertEqual(seed_added(final), [("CM", "500097"), ("LIC", "824009"), ("LIC", "910000")])
        self.assertIn("3 filas leídas → 2 añadidas", stdout2)
        self.assertEqual(len(final[(final["_tipo"] == "LIC") & (final["id"] == "910000")]), 1)
        # Cada ejecución de base sin --organismo guarda su lista
        self.assertEqual([d["fecha"] for d in manifest["descubrimientos"]], [FECHA_1, FECHA_2])

    def test_organism_listed_before_a_resume_is_not_retired_by_the_resumed_list(self):
        portal = self._portal()
        portal.fail_orgs = {48}
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", [seed_lic(48, 824000), seed_lic(5, 910000)])
            out = tmp / "salida"
            self.assertEqual(run_at(cli_args(out, "base", "--max-org-id", "48"), portal, FECHA_1)[0], 1)
            # Al reanudar, el 48 ya no está en la lista, pero estaba en la de la misma
            # descarga y no se llegó a leer: no se da por retirado
            portal.fail_orgs = set()
            del portal.lic[48], portal.cm[48]
            self.assertEqual(run_at(cli_args(out, "base", "--resume", "--max-org-id", "48"), portal, FECHA_2)[0], 0)
            code, stdout = run_at(cli_args(out, "merge", "--semilla", str(seed)), portal, FECHA_2)
            self.assertEqual(code, 0)
            final = read_final(out)
            manifest = read_manifest(out)

        self.assertEqual(seed_added(final), [("LIC", "910000")])
        self.assertIn("organismo 48: 1", stdout)
        self.assertEqual([sorted(d["organismos"]) for d in manifest["descubrimientos"]], [["2", "3", "48"], ["2", "3"]])

    def test_implausible_portal_list_retires_no_organism(self):
        # Ningún organismo con contratos menores: la sonda de CM ha respondido vacío
        portal = FakePortal(lic={2: fake_lic_records(3, first_id=700000), 48: fake_lic_records(2)})
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", [seed_lic(5, 910000), seed_cm(5, 910100)])
            out = tmp / "salida"
            code, stdout = run_at(cli_args(out, "--max-org-id", "48", "--semilla", str(seed)), portal, FECHA_1)
            self.assertEqual(code, 0)
            final = read_final(out)

        self.assertEqual(seed_added(final), [])
        self.assertIn(f"la lista de organismos del {FECHA_1} no trae ninguno con CM", stdout)
        self.assertIn("2 filas de la semilla de 1 organismos que esta descarga no ha leído no se añaden", stdout)

    def test_too_many_retired_organisms_retire_none_until_confirmed(self):
        seed_rows = [seed_lic(5, 910000), seed_lic(6, 920000), seed_cm(6, 920100), seed_lic(7, 930000),
                     seed_lic(48, 824009)]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            seed = published_seed(tmp / "publicado.parquet", seed_rows)
            out = tmp / "salida"
            code, stdout = run_at(
                cli_args(out, "--max-org-id", "48", "--semilla", str(seed), "--max-organismos-retirados", "2"),
                self._portal(), FECHA_1,
            )
            self.assertEqual(code, 0)
            blocked = seed_added(read_final(out))
            code, stdout2 = run_at(
                cli_args(out, "merge", "--semilla", str(seed), "--max-organismos-retirados", "3"),
                self._portal(), FECHA_1,
            )
            self.assertEqual(code, 0)
            confirmed = seed_added(read_final(out))
            before = snapshot(out)
            code, stdout3 = run_at(
                cli_args(out, "merge", "--semilla", str(seed), "--max-organismos-retirados", "-1"),
                self._portal(), FECHA_1,
            )
            self.assertEqual(code, 1)
            self.assertIn("--max-organismos-retirados no puede ser negativo", stdout3)
            self.assertEqual(snapshot(out), before)

        self.assertEqual(blocked, [("LIC", "824009")])
        self.assertIn("5 filas leídas → 1 añadidas", stdout)
        self.assertIn("4 filas de la semilla fuera del ámbito", stdout)
        self.assertIn("3 organismos de la semilla añadirían filas por no estar en la lista del portal, más que "
                      "--max-organismos-retirados (2)", stdout)
        self.assertIn("repite 'merge' con --max-organismos-retirados 3: organismo 6: 2", stdout)
        self.assertEqual(
            confirmed,
            [("CM", "920100"), ("LIC", "824009"), ("LIC", "910000"), ("LIC", "920000"), ("LIC", "930000")],
        )
        self.assertIn("4 filas añadidas de 3 organismos que el portal ha retirado enteros", stdout2)

    def test_zero_rows_seed_and_seed_without_retired_organisms(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            empty = published_seed(tmp / "vacia.parquet", [])
            present = published_seed(tmp / "presentes.parquet", [seed_lic(2, 700000), seed_cm(3, 600009)])
            out = tmp / "salida"
            code, stdout = run_at(
                cli_args(out, "--max-org-id", "48", "--semilla", str(empty), "--semilla", str(present)),
                self._portal(), FECHA_1,
            )
            self.assertEqual(code, 0)
            final = read_final(out)

        self.assertIn("0 filas leídas → 0 añadidas", stdout)
        self.assertIn("2 filas leídas → 1 añadidas", stdout)
        self.assertNotIn("retirado enteros", stdout)
        self.assertNotIn("sin la lista completa", stdout)
        self.assertIn("de semillas 1 (de organismos retirados 0)", stdout)
        self.assertEqual(seed_added(final), [("CM", "600009")])

    def test_portal_organisms_and_retired_organisms(self):
        portal_organisms = scraper_galicia.portal_organisms
        retired_organisms = scraper_galicia.retired_organisms

        def lista(fecha, hasta, organismos):
            return {"fecha": fecha, "hasta": hasta, "organismos": organismos}

        uno = lista(FECHA_1, 48, {"2": {"CM": 0, "LIC": 3}, "3": {"CM": 4, "LIC": 0}})
        otra = lista(FECHA_2, 40, {"2": {"CM": 0, "LIC": 3}, "3": {"CM": 4, "LIC": 0}, "9": {"CM": 1, "LIC": 0}})
        # Sin lista, con una sonda que ha respondido vacío para todos o ilegible: no hay lista
        self.assertIn("motivo", portal_organisms({}))
        self.assertIn("motivo", portal_organisms({"descubrimientos": []}))
        self.assertIn("motivo", portal_organisms({"descubrimientos": [lista(FECHA_1, 48, {"2": {"CM": 0, "LIC": 3}})]}))
        self.assertIn("motivo", portal_organisms({"descubrimientos": [uno, lista(FECHA_2, 48, {"3": {"CM": 4}})]}))
        self.assertIn("motivo", portal_organisms({"descubrimientos": [{"fecha": FECHA_1, "organismos": {}}]}))
        self.assertIn("motivo", portal_organisms({"descubrimientos": [dict(uno, hasta="x")]}))
        # Dos listas en la misma descarga: presentes en alguna, ids probados en todas
        portal = portal_organisms({"descubrimientos": [uno, otra]})
        self.assertEqual(portal, {"presentes": {"2", "3", "9"}, "hasta": 40, "fechas": [FECHA_1, FECHA_2]})
        orgs = ["2", "3", "5", "9", "12", "40", "41", "0", "", "x", "5.5", "١٢"]
        self.assertEqual(retired_organisms(orgs, portal, {}), {"5", "12", "40"})
        # Uno que la descarga ha leído (ámbito) no está retirado aunque no esté en la lista
        self.assertEqual(retired_organisms(orgs, portal, {"12": {}}), {"5", "40"})
        self.assertEqual(retired_organisms(orgs, {"motivo": "sin lista"}, {}), set())
        self.assertEqual(retired_organisms([], portal, {}), set())


if __name__ == "__main__":
    unittest.main()
