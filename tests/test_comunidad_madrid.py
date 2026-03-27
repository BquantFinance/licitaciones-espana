import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = (
    REPO_ROOT / "comunidad_madrid" / "descarga_contratacion_comunidad_madrid_v1.py"
)
SPEC = importlib.util.spec_from_file_location("comunidad_madrid_scraper", MODULE_PATH)
cam = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cam
SPEC.loader.exec_module(cam)


SEARCH_HTML_WITH_DETAIL = """
<html>
  <body>
    <div class="contratos-result">
      <ul>
        <li>
          <div class="views-field views-field-nothing">
            <span class="field-content">
              <a href="/contrato-publico/servicio-mantenimiento-plataforma">Servicio de mantenimiento</a>
            </span>
          </div>
          <div class="views-field views-field-numero-expediente">
            <span class="views-label">Número expediente: </span>
            <span class="field-content">FIB 2026/006</span>
          </div>
          <div class="views-field views-field-tipo-publicacion">
            <span class="views-label">Tipo de publicación: </span>
            <span class="field-content">Convocatoria anunciada a licitación</span>
          </div>
          <div class="views-field views-field-estado">
            <span class="views-label">Estado o situación: </span>
            <span class="field-content">En plazo</span>
          </div>
        </li>
      </ul>
    </div>
  </body>
</html>
"""


SEARCH_HTML_NO_DETAIL = """
<html>
  <body>
    <div class="contratos-result">
      <ul>
        <li>
          Productos Farmacéuticos
          <div class="views-field views-field-numero-expediente">
            <span class="views-label">Número expediente: </span>
            <span class="field-content">2024008886</span>
          </div>
          <div class="views-field views-field-tipo-publicacion">
            <span class="views-label">Tipo de publicación: </span>
            <span class="field-content">Contratos Menores</span>
          </div>
        </li>
      </ul>
    </div>
  </body>
</html>
"""


SEARCH_HTML_MINOR_DETAIL = """
<html>
  <body>
    <div class="contratos-result">
      <ul>
        <li>
          <div class="views-field views-field-title">
            <span class="field-content">
              <a href="/contrato/D766_9897">Productos Farmacéuticos</a>
            </span>
          </div>
          <div class="views-field views-field-numero-expediente">
            <span class="views-label">Número expediente: </span>
            <span class="field-content">2024008886</span>
          </div>
          <div class="views-field views-field-tipo-publicacion">
            <span class="views-label">Tipo de publicación: </span>
            <span class="field-content">Contratos Menores</span>
          </div>
        </li>
      </ul>
    </div>
  </body>
</html>
"""


DETAIL_HTML = """
<html>
  <head>
    <title>Servicio de mantenimiento</title>
  </head>
  <body>
    <div class="node__content">
      <div class="field">
        <div class="field__label">Tipo de publicación</div>
        <div class="field__item">Convocatoria anunciada a licitación</div>
      </div>
      <div class="field">
        <div class="field__label">Situación</div>
        <div class="field__item">En plazo</div>
      </div>
      <div class="field">
        <div class="field__label">Número de expediente</div>
        <div class="field__item">FIB 2026/006</div>
      </div>
      <div class="field">
        <div class="field__label">Referencia</div>
        <div class="field__item">C12567</div>
      </div>
      <div class="field">
        <div class="field__label">Entidad adjudicadora</div>
        <div class="field__item">Consejería de Sanidad</div>
      </div>
      <div class="field">
        <div class="field__label">Procedimiento de adjudicación</div>
        <div class="field__item">Abierto simplificado</div>
      </div>
      <div class="field">
        <div class="field__label">Presupuesto base licitación sin impuestos</div>
        <div class="field__item">6.000,00 euros</div>
      </div>
      <div class="field">
        <div class="field__label">Fecha y hora límite de presentación de ofertas o solicitudes de participación</div>
        <div class="field__item">30 de marzo del  2026 14:00</div>
      </div>
      <div id="pcon-pliego-de-condiciones">
        <div class="archivos"><a href="/doc1.pdf">Descargar</a></div>
        <div class="archivos"><a href="/doc2.pdf">Descargar</a></div>
      </div>
    </div>
  </body>
</html>
"""


DETAIL_HTML_MINOR = """
<html>
  <head>
    <title>Productos Farmacéuticos</title>
  </head>
  <body>
    <div class="node__content">
      <div class="field">
        <div class="field__label">Tipo de publicación</div>
        <div class="field__item">Contratos Menores</div>
      </div>
      <div class="field">
        <div class="field__label">Tipo de resolución</div>
        <div class="field__item">Adjudicación</div>
      </div>
      <div class="field">
        <div class="field__label">Número de expediente</div>
        <div class="field__item">2024008886</div>
      </div>
      <div class="field">
        <div class="field__label">Referencia</div>
        <div class="field__item">D766_9897</div>
      </div>
      <div class="field">
        <div class="field__label">Código de la entidad adjudicadora</div>
        <div class="field__item">A13037412</div>
      </div>
      <div class="field">
        <div class="field__label">Entidad adjudicadora</div>
        <div class="field__item">Hospital General Universitario Gregorio Marañón</div>
      </div>
      <div class="field">
        <div class="field__label">Procedimiento de adjudicación</div>
        <div class="field__item">Contrato menor</div>
      </div>
      <div class="field">
        <div class="field__label">Nº Ofertas</div>
        <div class="field__item">3</div>
      </div>
      <div class="field">
        <div class="field__label">NIF del adjudicatario</div>
        <div class="field__item">A12345678</div>
      </div>
      <div class="field">
        <div class="field__label">Nombre o razón social del adjudicatario</div>
        <div class="field__item">Laboratorios Demo SA</div>
      </div>
      <div class="field">
        <div class="field__label">Fecha del contrato</div>
        <div class="field__item">12/03/2024</div>
      </div>
      <div class="field">
        <div class="field__label">Importe adjudicación (sin IVA)</div>
        <div class="field__item">255,27 euros</div>
      </div>
      <div class="field">
        <div class="field__label">Importe adjudicación (con IVA)</div>
        <div class="field__item">308,88 euros</div>
      </div>
      <div class="field">
        <div class="field__label">Duración del contrato</div>
        <div class="field__item">30 días</div>
      </div>
    </div>
  </body>
</html>
"""


class ComunidadMadridTests(unittest.TestCase):
    def test_resolver_captcha_and_antibot_transform(self):
        self.assertEqual(cam.resolver_captcha("3 + 8 ="), 11)
        self.assertEqual(cam.transformar_antibot_key("abcdef"), "efcdab")

    def test_parse_search_result_html_with_detail_link(self):
        parsed = cam.parse_search_result_html(SEARCH_HTML_WITH_DETAIL)

        self.assertEqual(
            parsed["detail_url"],
            "https://contratos-publicos.comunidad.madrid/contrato-publico/servicio-mantenimiento-plataforma",
        )
        self.assertEqual(parsed["summary"]["numero expediente"], "FIB 2026/006")
        self.assertEqual(
            parsed["summary"]["tipo de publicacion"],
            "Convocatoria anunciada a licitación",
        )

    def test_parse_search_result_html_without_detail_link(self):
        parsed = cam.parse_search_result_html(SEARCH_HTML_NO_DETAIL)

        self.assertIsNone(parsed["detail_url"])
        self.assertEqual(parsed["title"], "Productos Farmacéuticos")
        self.assertEqual(parsed["summary"]["numero expediente"], "2024008886")

    def test_parse_search_result_html_with_minor_detail_link(self):
        parsed = cam.parse_search_result_html(SEARCH_HTML_MINOR_DETAIL)

        self.assertEqual(
            parsed["detail_url"],
            "https://contratos-publicos.comunidad.madrid/contrato/D766_9897",
        )
        self.assertEqual(parsed["title"], "Productos Farmacéuticos")
        self.assertEqual(parsed["summary"]["tipo de publicacion"], "Contratos Menores")

    def test_parse_detail_html_maps_fields_and_documents(self):
        parsed = cam.parse_detail_html(DETAIL_HTML)

        self.assertEqual(parsed["page_title"], "Servicio de mantenimiento")
        self.assertEqual(parsed["mapped"]["detail_numero_expediente"], "FIB 2026/006")
        self.assertEqual(
            parsed["mapped"]["detail_procedimiento_adjudicacion"],
            "Abierto simplificado",
        )
        self.assertEqual(
            parsed["mapped"]["detail_presupuesto_base_sin_impuestos"], 6000.0
        )
        self.assertEqual(parsed["mapped"]["detail_documentos_count"], 2)
        self.assertEqual(
            parsed["mapped"]["detail_fecha_limite_presentacion"],
            "2026-03-30T14:00:00",
        )

    def test_parse_detail_html_maps_minor_fields(self):
        parsed = cam.parse_detail_html(DETAIL_HTML_MINOR)

        self.assertEqual(parsed["mapped"]["detail_tipo_resolucion"], "Adjudicación")
        self.assertEqual(
            parsed["mapped"]["detail_codigo_entidad_adjudicadora"], "A13037412"
        )
        self.assertEqual(parsed["mapped"]["detail_num_ofertas"], 3.0)
        self.assertEqual(parsed["mapped"]["detail_nif_adjudicatario"], "A12345678")
        self.assertEqual(
            parsed["mapped"]["detail_nombre_razon_social_adjudicatario"],
            "Laboratorios Demo SA",
        )
        self.assertEqual(
            parsed["mapped"]["detail_importe_adjudicacion_sin_iva"], 255.27
        )
        self.assertEqual(
            parsed["mapped"]["detail_importe_adjudicacion_con_iva"], 308.88
        )
        self.assertEqual(
            parsed["mapped"]["detail_fecha_contrato"], "2024-03-12T00:00:00"
        )

    def test_build_record_key_is_stable(self):
        record = {
            "Tipo de Publicación": "Convocatoria anunciada a licitación",
            "Entidad Adjudicadora": "Consejería de Sanidad",
            "Nº Expediente": "FIB 2026/006",
            "Referencia": "C12567",
            "Título del contrato": "Servicio de mantenimiento",
        }
        key1 = cam.build_record_key(record)
        key2 = cam.build_record_key(dict(record))

        self.assertEqual(key1, key2)

    def test_fetch_detail_for_row_filters_by_tipo_publicacion(self):
        row = {
            "Tipo de Publicación": "Anuncio de información previa",
            "Entidad Adjudicadora": "Consejería de Economía",
            "Nº Expediente": "C-923M/006-19 (A/SER-034642/2019)",
            "Referencia": "",
            "Título del contrato": "Vigilancia y seguridad",
            "record_key": "rk-1",
        }

        search_response = Mock(status_code=200)
        search_response.raise_for_status = Mock()
        search_response.text = (
            SEARCH_HTML_WITH_DETAIL.replace(
                "Convocatoria anunciada a licitación", "Anuncio de información previa"
            )
            .replace(
                "/contrato-publico/servicio-mantenimiento-plataforma",
                "/contrato-publico/vigilancia-seguridad",
            )
            .replace("FIB 2026/006", "C-923M/006-19 (A/SER-034642/2019)")
        )
        detail_response = Mock(status_code=200)
        detail_response.raise_for_status = Mock()
        detail_response.text = DETAIL_HTML

        fake_session = Mock()
        fake_session.get.side_effect = [search_response, detail_response]

        with patch.object(cam, "_thread_session", return_value=fake_session):
            result = cam.fetch_detail_for_row(
                row, attempts_done=0, delay=0.0, jitter=0.0
            )

        self.assertEqual(result["status"], "done")
        first_call = fake_session.get.call_args_list[0]
        self.assertIn("f[0]", first_call.kwargs["params"])
        self.assertEqual(
            first_call.kwargs["params"]["f[0]"],
            "tipo_publicacion:Anuncio de información previa",
        )

    def test_fetch_detail_for_row_resolves_minor_detail(self):
        row = {
            "Tipo de Publicación": "Contratos Menores",
            "Entidad Adjudicadora": "Hospital General Universitario Gregorio Marañón",
            "Nº Expediente": "2024008886",
            "Referencia": "D766_9897",
            "Título del contrato": "Productos Farmacéuticos",
            "record_key": "rk-2",
        }

        search_response = Mock(status_code=200)
        search_response.raise_for_status = Mock()
        search_response.text = SEARCH_HTML_MINOR_DETAIL
        detail_response = Mock(status_code=200)
        detail_response.raise_for_status = Mock()
        detail_response.text = DETAIL_HTML_MINOR

        fake_session = Mock()
        fake_session.get.side_effect = [search_response, detail_response]

        with patch.object(cam, "_thread_session", return_value=fake_session):
            result = cam.fetch_detail_for_row(
                row, attempts_done=0, delay=0.0, jitter=0.0
            )

        self.assertEqual(result["status"], "done")
        self.assertEqual(
            result["detail_url"],
            "https://contratos-publicos.comunidad.madrid/contrato/D766_9897",
        )
        mapped = json.loads(result["mapped_json"])
        self.assertEqual(mapped["detail_tipo_resolucion"], "Adjudicación")
        self.assertEqual(mapped["detail_importe_adjudicacion_con_iva"], 308.88)
        self.assertEqual(result["last_error"], None)
        first_call = fake_session.get.call_args_list[0]
        self.assertEqual(first_call.kwargs["params"]["referencia"], "D766_9897")
        self.assertEqual(
            first_call.kwargs["params"]["f[0]"],
            "tipo_publicacion:Contratos Menores",
        )

    def test_iter_detail_batches_skips_done_and_includes_pending_menores(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            paths = cam.build_paths(output_dir)
            cam.ensure_dirs(paths)
            df = pd.DataFrame(
                [
                    {
                        "Tipo de Publicación": "Contratos Menores",
                        "Entidad Adjudicadora": "A",
                        "Nº Expediente": "1",
                        "Referencia": "R1",
                        "Título del contrato": "Minor",
                    },
                    {
                        "Tipo de Publicación": "Convocatoria anunciada a licitación",
                        "Entidad Adjudicadora": "B",
                        "Nº Expediente": "2",
                        "Referencia": "R2",
                        "Título del contrato": "Tender",
                    },
                ]
            )
            df.to_csv(paths.base_csv, index=False, sep=";", encoding="utf-8-sig")
            conn = cam.init_detail_db(output_dir)
            record_key = cam.build_record_key(df.iloc[1].to_dict())
            cam.persist_detail_results(
                conn,
                [
                    {
                        "record_key": record_key,
                        "tipo_publicacion": "Convocatoria anunciada a licitación",
                        "referencia": "R2",
                        "numero_expediente": "2",
                        "entidad_adjudicadora": "B",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "detail_url": "https://example.com",
                        "page_title": "Tender",
                        "updated_at": "2026-03-26T00:00:00",
                        "search_summary_json": json.dumps({}),
                        "mapped_json": json.dumps({"detail_referencia": "R2"}),
                    }
                ],
            )

            batches = list(cam.iter_detail_batches(paths.base_csv, conn))
            self.assertEqual(len(batches), 1)
            self.assertEqual(len(batches[0]), 1)
            self.assertEqual(batches[0][0]["Referencia"], "R1")

    def test_query_detail_rows_returns_chunked_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            conn = cam.init_detail_db(output_dir)
            cam.persist_detail_results(
                conn,
                [
                    {
                        "record_key": "abc",
                        "tipo_publicacion": "Convocatoria anunciada a licitación",
                        "referencia": "R",
                        "numero_expediente": "1",
                        "entidad_adjudicadora": "B",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "detail_url": "https://example.com",
                        "page_title": "Tender",
                        "updated_at": "2026-03-26T00:00:00",
                        "search_summary_json": json.dumps({}),
                        "mapped_json": json.dumps({"detail_referencia": "R"}),
                    }
                ],
            )
            rows = cam.query_detail_rows(conn, ["abc", "def"], chunk_size=1)

            self.assertIn("abc", rows)
            self.assertEqual(rows["abc"]["status"], "done")

    def test_merge_base_and_detail_creates_final_dataset(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            paths = cam.build_paths(output_dir)
            cam.ensure_dirs(paths)
            base_df = pd.DataFrame(
                [
                    {
                        "Tipo de Publicación": "Convocatoria anunciada a licitación",
                        "Entidad Adjudicadora": "Consejería de Sanidad",
                        "Nº Expediente": "FIB 2026/006",
                        "Referencia": "C12567",
                        "Título del contrato": "Servicio de mantenimiento",
                    }
                ]
            )
            base_df.to_csv(paths.base_csv, index=False, sep=";", encoding="utf-8-sig")
            conn = cam.init_detail_db(output_dir)
            record_key = cam.build_record_key(base_df.iloc[0].to_dict())
            cam.persist_detail_results(
                conn,
                [
                    {
                        "record_key": record_key,
                        "tipo_publicacion": "Convocatoria anunciada a licitación",
                        "referencia": "C12567",
                        "numero_expediente": "FIB 2026/006",
                        "entidad_adjudicadora": "Consejería de Sanidad",
                        "status": "done",
                        "attempts": 1,
                        "last_error": None,
                        "last_http_status": None,
                        "detail_url": "https://example.com/detail",
                        "page_title": "Servicio de mantenimiento",
                        "updated_at": "2026-03-26T00:00:00",
                        "search_summary_json": json.dumps({}),
                        "mapped_json": json.dumps(
                            {
                                "detail_referencia": "C12567",
                                "detail_presupuesto_base_sin_impuestos": 6000.0,
                            }
                        ),
                    }
                ],
            )

            final_csv, parquet_path = cam.merge_base_and_detail(output_dir)
            merged = pd.read_csv(final_csv, sep=";")

            self.assertTrue(final_csv.exists())
            self.assertEqual(merged.iloc[0]["detail_referencia"], "C12567")
            self.assertEqual(merged.iloc[0]["detail_status"], "done")
            if cam.HAS_PYARROW:
                self.assertTrue(parquet_path.exists())

    def test_build_parser_defaults(self):
        args = cam.build_parser().parse_args([])

        self.assertEqual(args.mode, "help")
        self.assertEqual(Path(args.output), cam.DEFAULT_OUTPUT_DIR)
        self.assertEqual(args.detail_workers, cam.DETAIL_WORKERS)
        self.assertTrue(args.include_menores)


if __name__ == "__main__":
    unittest.main()
