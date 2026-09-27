import importlib.util
import io
import json
import re
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_andalucia.py"
SPEC = importlib.util.spec_from_file_location("ccaa_andalucia", MODULE_PATH)
ccaa_andalucia = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ccaa_andalucia)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.ok = status_code < 400
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise ccaa_andalucia.requests.HTTPError(f"HTTP {self.status_code}")


class FakeElastic:
    """Emula el proxy Elasticsearch del portal sobre documentos en memoria.

    - bool must/must_not con `match` (tokens alfanumericos en minusculas, OR entre tokens,
      campos con ruta "a.b" y valores multiples como provinciasEjecucion);
    - sort de un campo con missing al final, min/max para multivaluados y desempate por
      orden de indexacion;
    - from/size con ventana maxima: si from + size la supera responde HTTP 400 como ES.
    """

    def __init__(self, docs, max_window):
        self.docs = docs
        self.max_window = max_window
        self.bodies = []
        self.post_urls = set()
        self.post_timeouts = set()
        self.get_urls = []
        self._clause_cache = {}
        self._sorted_cache = {}

    @staticmethod
    def _tokens(value):
        return set(re.findall(r"[0-9a-z]+", str(value).lower()))

    @staticmethod
    def _field_values(doc, path):
        values = [doc]
        for part in path.split("."):
            children = []
            for value in values:
                if isinstance(value, dict) and value.get(part) is not None:
                    child = value[part]
                    children.extend(child if isinstance(child, list) else [child])
            values = children
        return values

    def _clause_matches(self, clause):
        ((path, query),) = clause["match"].items()
        if isinstance(query, dict):
            query = query["query"]
        key = (path, str(query))
        if key not in self._clause_cache:
            wanted = self._tokens(query)
            self._clause_cache[key] = frozenset(
                index
                for index, doc in enumerate(self.docs)
                if any(wanted & self._tokens(value) for value in self._field_values(doc, path))
            )
        return self._clause_cache[key]

    def _select(self, body):
        cache_key = json.dumps({"query": body["query"], "sort": body.get("sort")}, sort_keys=True)
        if cache_key in self._sorted_cache:
            return self._sorted_cache[cache_key]
        boolean = body["query"]["bool"]
        selected = set(range(len(self.docs)))
        for clause in boolean.get("must", []):
            selected &= self._clause_matches(clause)
        for clause in boolean.get("must_not", []):
            selected -= self._clause_matches(clause)
        hits = sorted(selected)
        if body.get("sort"):
            ((path, order),) = body["sort"][0].items()
            present = [index for index in hits if self._field_values(self.docs[index], path)]
            missing = [index for index in hits if not self._field_values(self.docs[index], path)]
            if order == "asc":
                present.sort(key=lambda index: min(self._field_values(self.docs[index], path)))
            else:
                present.sort(key=lambda index: max(self._field_values(self.docs[index], path)), reverse=True)
            hits = present + missing
        self._sorted_cache[cache_key] = hits
        return hits

    def post(self, url, json=None, timeout=None, **kwargs):
        self.post_urls.add(url)
        self.post_timeouts.add(timeout)
        self.bodies.append(json)
        size = json.get("size", 10)
        offset = json.get("from", 0)
        if offset + size > self.max_window:
            return FakeResponse(
                400,
                {"error": {"type": "illegal_argument_exception", "reason": "Result window is too large"}},
            )
        hits = self._select(json)
        page = hits[offset : offset + size] if size else []
        return FakeResponse(
            200,
            {
                "hits": {
                    "total": {"value": len(hits), "relation": "eq"},
                    "hits": [{"_source": self.docs[index]} for index in page],
                }
            },
        )

    def get(self, url, timeout=None, **kwargs):
        self.get_urls.append(url)
        return FakeResponse(200, {})


def make_doc(doc_id, *, proc="1", tipo="SERV", estado="ADJ", tram="O", perfil="CONS01", provs=("41",),
             fp="E", anio="2023", importe=None, awarded=False, medios=None):
    publicado = date(2018, 1, 1) + timedelta(days=doc_id // 4)
    doc = {
        "idExpediente": doc_id,
        "numeroExpediente": f"CONTR {anio} {doc_id:05d}",
        "titulo": f"Contrato {doc_id:05d}",
        "estado": {"codigo": estado, "nombre": f"Estado {estado}"},
        "perfilContratante": {"codigo": perfil, "descripcion": f"Organo {perfil}", "codigoDir3": "A01002820"},
        "importeLicitacion": importe if importe is not None else 100000.0 + doc_id,
        "valorEstimado": 80000.0 + doc_id,
        "fechaPublicacion": f"{publicado.isoformat()}T10:00:00+0100",
        "fechaLimitePresentacion": f"{(publicado + timedelta(days=15)).isoformat()}T23:59:59+0100",
        "codigosCpv": ["33140000"],
        "mediosPublicacion": medios if medios is not None else [{"codigo": "PLACSP"}],
        "lotes": [],
    }
    if proc is not None:
        doc["codigoProcedimiento"] = proc
    if tipo is not None:
        doc["tipoContrato"] = {"codigo": tipo, "descripcion": f"Tipo {tipo}"}
    if tram is not None:
        doc["codigoTipoTramitacion"] = tram
    if provs:
        doc["provinciasEjecucion"] = list(provs)
    if fp is not None:
        doc["formaPresentacion"] = fp
    if awarded:
        doc["adjudicaciones"] = [
            {
                "nifAdjudicatario": f"B{doc_id:08d};",
                "importeAdjudicacion": 1000.0 + doc_id,
                "importeAdjudicacionConIva": round((1000.0 + doc_id) * 1.21, 2),
            },
            {"nifAdjudicatario": "A00000001;", "importeAdjudicacion": 5.0, "importeAdjudicacionConIva": 6.05},
        ]
    return doc


def build_dataset():
    """~7K expedientes pensados para una ventana de 1.000 resultados (MAX_FROM=900).

    Obliga a recorrer todas las ramas: paginacion directa, particion por dimensiones,
    ramas "null" (sin tipo, sin tramitacion, sin provincia, procedimiento desconocido,
    perfil no descubierto), provincias multivaluadas y multi-sort para un bloque de 2.350
    expedientes identicos en las 8 dimensiones. Los BRR nunca deben aparecer.
    """
    docs = []
    std_ids = set()
    men_ids = set()

    def add(count, target=None, **fields):
        for _ in range(count):
            doc_id = len(docs) + 1
            docs.append(make_doc(doc_id, **fields))
            if target is not None:
                target.add(doc_id)

    add(300, std_ids, proc="1", perfil="CONS01")
    add(600, std_ids, proc="2", tipo="SERV", estado="RES", perfil="CONS02")
    add(600, std_ids, proc="2", tipo="SERV", estado="ADJ", perfil="CONS01", awarded=True)
    add(950, std_ids, proc="2", tipo="SUM", estado="PUB", perfil="CONS02")  # 900 < n <= ventana
    add(500, std_ids, proc="2", tipo=None, estado="PUB", perfil="CONS01")
    add(120, std_ids, proc=None, tipo="OBR", estado="PUB")
    add(50, std_ids, proc="99", tipo="OBR", estado="PUB")
    add(70, None, proc="1", estado="BRR")
    add(600, men_ids, proc="9", tipo="SERV", estado="ADJ", perfil="UNIV01", awarded=True,
        medios=[{"codigo": None}, {"codigo": "BOJA"}])
    add(400, men_ids, proc="9", tipo="SUM", estado="RES", perfil="HIDDEN01", provs=("29",), fp="M", anio="2024")
    add(150, None, proc="9", tipo="SUM", estado="BRR", perfil="SYBS03", provs=("29",), fp="M", anio="2024")
    add(300, men_ids, proc="9", tipo="SUM", estado="RES", tram=None, perfil="CONS02")
    add(200, men_ids, proc="9", tipo="SUM", estado="RES", perfil="SYBS03", provs=(), fp="M", anio="2024")
    heavy_start = len(docs) + 1
    add(50, men_ids, proc="9", tipo="SUM", estado="RES", perfil="SYBS03", provs=("29", "41"), fp="M", anio="2024")
    add(2300, men_ids, proc="9", tipo="SUM", estado="RES", perfil="SYBS03", provs=("29",), fp="M", anio="2024")
    # Bloque inseparable de 2.350: idExpediente asc/desc cubren 2.000 y los 350 del medio
    # solo salen ordenando por importeLicitacion asc, detras de una pagina entera de
    # expedientes ya vistos (los 100 primeros del bloque tienen los importes mas bajos)
    heavy = docs[heavy_start - 1 :]
    for position, doc in enumerate(heavy[:100]):
        doc["importeLicitacion"] = float(position + 1)
    for position, doc in enumerate(heavy[1000:1350]):
        doc["importeLicitacion"] = float(101 + position)
    return docs, std_ids, men_ids


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

    def test_paginate_multisort_skips_sort_rejected_by_the_index(self):
        # Ordenar por un campo de texto sin fielddata da HTTP 400: antes abortaba el scrape
        # entero en el primer bloque grande que llegaba a esa ordenacion
        def fake_es(body, timeout=None):
            field = next(iter(body["sort"][0]))
            if field == "titulo":
                raise ccaa_andalucia.ScraperError("HTTP no reintentable 400: fielddata", status_code=400)
            if body.get("from", 0):
                return {"hits": {"hits": []}}
            ids = {"idExpediente": [1, 2], "importeLicitacion": [3]}[field]
            return {"hits": {"hits": [{"_source": {"idExpediente": value}} for value in ids]}}

        sorts = [[{"idExpediente": "asc"}], [{"titulo": "asc"}], [{"importeLicitacion": "asc"}]]
        with patch.object(ccaa_andalucia, "SORT_COMBOS", sorts), patch.object(ccaa_andalucia, "DELAY", 0), patch.object(
            ccaa_andalucia, "es", side_effect=fake_es
        ), patch.object(ccaa_andalucia.time, "sleep"), self.assertLogs(ccaa_andalucia.log, level="WARNING") as logs:
            records = ccaa_andalucia.paginate_multisort(target=3, label="blk")

        self.assertEqual([record["id_expediente"] for record in records], [1, 2, 3])
        self.assertIn("ordenacion titulo:asc no admitida", "\n".join(logs.output))

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

    def test_get_perfiles_uses_cache_as_seed_and_adds_new_perfiles(self):
        # La cache no se actualizaba nunca: los perfiles nuevos solo salian por la rama null
        responses = [
            {"hits": {"hits": [{"_source": {"perfilContratante": {"codigo": "C"}}}]}},
            {"hits": {"hits": []}},
        ]
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
                side_effect=responses,
            ) as mocked_es, patch.object(ccaa_andalucia.time, "sleep"):
                perfiles = ccaa_andalucia.get_perfiles()
            cached = json.loads(cache_path.read_text(encoding="utf-8"))

        self.assertEqual(perfiles, ["A", "B", "C"])
        self.assertEqual(cached, ["A", "B", "C"])
        first_query = mocked_es.call_args_list[0].args[0]
        self.assertEqual(
            first_query["query"]["bool"]["must_not"],
            [ccaa_andalucia.mn("perfilContratante.codigo", "A"), ccaa_andalucia.mn("perfilContratante.codigo", "B")],
        )
        second_query = mocked_es.call_args_list[1].args[0]
        self.assertEqual(len(second_query["query"]["bool"]["must_not"]), 3)

    def test_partition_lists_cover_codes_present_in_published_data(self):
        # Valores de licitaciones_andalucia.parquet que faltaban en las listas (solo salian
        # por la rama null de cada dimension)
        for code in ["ANUL", "AP", "C", "CERR", "E", "PUBANUL", "SUS"]:
            self.assertIn(code, ccaa_andalucia.ESTADOS)
        for code in ["CMIN", "CONOBR"]:
            self.assertIn(code, ccaa_andalucia.TIPOS)
        for code in ["99", "00"]:
            self.assertIn(code, ccaa_andalucia.PROVS)
        self.assertIn("A", ccaa_andalucia.FPS)
        # Antes acababa en 2026 fijo y empezaba en 2018
        self.assertEqual(ccaa_andalucia.YEARS[-1], str(date.today().year + 1))
        for year in ("2015", "2016", "2017", "2026", str(date.today().year)):
            self.assertIn(year, ccaa_andalucia.YEARS)

    def test_flatten_keeps_every_award_lot_notice_and_unmapped_field_as_json(self):
        source = {
            "idExpediente": 9,
            "titulo": "Con lotes",
            "perfilContratante": {"codigo": "SYBS03", "descripcion": "SAS", "codigoDir3": "A1", "nif": "Q1"},
            "estado": {"codigo": "RES", "nombre": "Resuelta"},
            "mediosPublicacion": [{"codigo": "BOJA"}],
            "fechaAdjudicacion": "2026-02-01T00:00:00+0100",
            "adjudicaciones": [
                {"nifAdjudicatario": "A1;", "importeAdjudicacion": 10.0, "lote": 1},
                {"nifAdjudicatario": "B2;", "importeAdjudicacion": 20.0, "lote": 2},
            ],
            "lotes": [{"numero": 1, "importe": 10.0}, {"numero": 2, "importe": 20.0}],
            "anuncios": [{"tipo": "ADJ", "fechaPublicacion": "2026-02-02T09:00:00+0100"}],
        }

        record = ccaa_andalucia.flatten(source)

        self.assertEqual(record["importe_adjudicacion"], 10.0)  # la columna sigue siendo la 1a
        self.assertEqual(json.loads(record["adjudicaciones_json"]), source["adjudicaciones"])
        self.assertEqual(json.loads(record["lotes_json"]), source["lotes"])
        self.assertEqual(json.loads(record["anuncios_json"]), source["anuncios"])
        self.assertEqual(
            json.loads(record["campos_extra_json"]),
            {"perfilContratante": source["perfilContratante"], "fechaAdjudicacion": "2026-02-01T00:00:00+0100"},
        )

        plain = ccaa_andalucia.flatten({"idExpediente": 1, "estado": {"codigo": "PUB", "nombre": "Publicada"}})
        for column in ("adjudicaciones_json", "lotes_json", "anuncios_json", "campos_extra_json"):
            self.assertEqual(plain[column], "")

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

    def test_flatten_tolerates_null_codigo_in_medios_publicacion(self):
        record = ccaa_andalucia.flatten(
            {"idExpediente": 7, "mediosPublicacion": [{"codigo": None}, {"codigo": "BOJA"}, {}]}
        )
        self.assertEqual(record["medios_publicacion"], ";BOJA;")

    @unittest.skipUnless(ccaa_andalucia.HAS_PANDAS, "pandas no disponible")
    def test_save_parquet_handles_records_without_award_and_mixed_types(self):
        awarded = ccaa_andalucia.flatten(
            {
                "idExpediente": 1,
                "importeLicitacion": 100.0,
                "codigoNormativa": 2017,
                "adjudicaciones": [
                    {"nifAdjudicatario": "A1;", "importeAdjudicacion": 95.0, "importeAdjudicacionConIva": 114.95}
                ],
            }
        )
        pending = ccaa_andalucia.flatten({"idExpediente": 2, "importeLicitacion": 50})

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(ccaa_andalucia, "DATA_DIR", Path(tmpdir)):
                path = ccaa_andalucia.save_parquet([awarded, pending], "test.parquet")
            frame = ccaa_andalucia.pd.read_parquet(path)

        self.assertEqual(frame["importe_licitacion"].tolist(), [100.0, 50.0])
        self.assertEqual(frame["importe_adjudicacion"].iloc[0], 95.0)
        self.assertEqual(frame["importe_adjudicacion_iva"].iloc[0], 114.95)
        self.assertTrue(ccaa_andalucia.pd.isna(frame["importe_adjudicacion"].iloc[1]))
        self.assertTrue(ccaa_andalucia.pd.isna(frame["valor_estimado"]).all())
        self.assertEqual(frame["codigo_normativa"].tolist(), ["2017", ""])

    @unittest.skipUnless(ccaa_andalucia.HAS_PANDAS, "pandas no disponible")
    def test_save_parquet_keeps_non_numeric_amounts_as_text(self):
        records = [
            ccaa_andalucia.flatten({"idExpediente": 1, "importeLicitacion": "N/D"}),
            ccaa_andalucia.flatten({"idExpediente": 2, "importeLicitacion": 5.0}),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(ccaa_andalucia, "DATA_DIR", Path(tmpdir)):
                path = ccaa_andalucia.save_parquet(records, "test.parquet")
            frame = ccaa_andalucia.pd.read_parquet(path)

        self.assertEqual(frame["importe_licitacion"].tolist(), ["N/D", "5.0"])

    def test_scrape_recursive_warns_when_null_branch_is_skipped(self):
        values = [f"V{index}" for index in range(900)]
        with patch.object(ccaa_andalucia, "DIMS", [("campo", values)]), patch.object(
            ccaa_andalucia,
            "cnt",
            return_value=0,
        ), patch.object(ccaa_andalucia.time, "sleep"):
            with self.assertLogs(ccaa_andalucia.log, level="WARNING") as logs:
                got = ccaa_andalucia.scrape_recursive(
                    [],
                    [],
                    "lbl",
                    [],
                    set(),
                    known_total=ccaa_andalucia.MAX_FROM + ccaa_andalucia.PAGE_SIZE + 1,
                )

        self.assertEqual(got, 0)
        self.assertIn("lbl/null_campo", "\n".join(logs.output))


@unittest.skipUnless(ccaa_andalucia.HAS_PANDAS, "pandas no disponible")
class AndaluciaEndToEndTests(unittest.TestCase):
    """Ejecuta los comandos del CLI (scrape-std, scrape-men, scrape) contra FakeElastic con
    una ventana de 1.000 resultados, y comprueba cobertura, duplicados y salidas."""

    MAX_FROM = 900

    def setUp(self):
        self.docs, self.std_ids, self.men_ids = build_dataset()
        self.fake = FakeElastic(self.docs, max_window=self.MAX_FROM + ccaa_andalucia.PAGE_SIZE)
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.data_dir = Path(tmpdir.name)

    def run_cli(self, command):
        patches = [
            patch.object(ccaa_andalucia, "DATA_DIR", self.data_dir),
            patch.object(ccaa_andalucia, "PERFILES_CACHE_PATH", self.data_dir / "perfiles_cache.json"),
            patch.object(ccaa_andalucia, "_PERFILES", None),
            patch.object(ccaa_andalucia, "MAX_FROM", self.MAX_FROM),
            patch.object(ccaa_andalucia, "DELAY", 0),
            patch.object(ccaa_andalucia.time, "sleep"),
            patch.object(ccaa_andalucia.S, "post", side_effect=self.fake.post),
            patch.object(ccaa_andalucia.S, "get", side_effect=self.fake.get),
            patch("sys.stdout", new_callable=io.StringIO),
        ]
        for active in patches:
            active.start()
        try:
            with self.assertLogs(ccaa_andalucia.log, level="INFO") as logs:
                rc = ccaa_andalucia.main([command])
            output = ccaa_andalucia.sys.stdout.getvalue()
        finally:
            for active in reversed(patches):
                active.stop()

        # Sin avisos: ni PARTIAL en multi-sort ni ramas omitidas
        self.assertEqual([record.getMessage() for record in logs.records if record.levelname != "INFO"], [])
        self.assertEqual(rc, 0)
        self.assertEqual(self.fake.post_urls, {ccaa_andalucia.ES_URL})
        self.assertNotIn(None, self.fake.post_timeouts)
        self.assertTrue(
            all(body.get("from", 0) + body["size"] <= self.MAX_FROM + 100 for body in self.fake.bodies)
        )
        init_url = f"{ccaa_andalucia.BASE}/perfiles-licitaciones/licitaciones-publicadas"
        self.assertTrue(self.fake.get_urls)
        self.assertEqual(set(self.fake.get_urls), {init_url})
        return output

    def read_csv_ids(self, filename):
        frame = ccaa_andalucia.pd.read_csv(self.data_dir / filename, encoding="utf-8-sig")
        self.assertEqual(list(frame.columns), ccaa_andalucia.CSV_COLS)
        ids = frame["id_expediente"].tolist()
        self.assertEqual(len(ids), len(set(ids)), f"duplicados en {filename}")
        return set(ids)

    def test_cli_scrape_std_recovers_every_standard_record(self):
        output = self.run_cli("scrape-std")

        self.assertIn(f"SCRAPE ESTANDAR: {len(self.std_ids):,}", output)
        self.assertEqual(self.read_csv_ids("licitaciones_std.csv"), self.std_ids)
        self.assertTrue((self.data_dir / "licitaciones_std_progress.csv").exists())

    def test_cli_scrape_men_recovers_every_menor_record(self):
        output = self.run_cli("scrape-men")

        self.assertIn(f"SCRAPE MENORES: {len(self.men_ids):,}", output)
        self.assertEqual(self.read_csv_ids("licitaciones_menores.csv"), self.men_ids)
        # HIDDEN01 y UNIV01 no salen en las ventanas de descubrimiento: se anaden pidiendo
        # expedientes de perfiles aun no vistos (antes solo salian por la rama null)
        cached = json.loads((self.data_dir / "perfiles_cache.json").read_text(encoding="utf-8"))
        self.assertEqual(cached, ["CONS01", "CONS02", "HIDDEN01", "SYBS03", "UNIV01"])
        sorts_used = {json.dumps(body.get("sort")) for body in self.fake.bodies}
        self.assertIn(json.dumps([{"importeLicitacion": "asc"}]), sorts_used)

    def test_cli_scrape_writes_documented_outputs_with_expected_mapping(self):
        self.run_cli("scrape")

        all_ids = self.std_ids | self.men_ids
        self.assertEqual(self.read_csv_ids("licitaciones_std.csv"), self.std_ids)
        self.assertEqual(self.read_csv_ids("licitaciones_menores.csv"), self.men_ids)
        self.assertEqual(self.read_csv_ids("licitaciones_all.csv"), all_ids)

        frame = ccaa_andalucia.pd.read_parquet(self.data_dir / "licitaciones_andalucia.parquet")
        self.assertEqual(list(frame.columns), ccaa_andalucia.CSV_COLS)
        self.assertEqual(len(frame), len(all_ids))
        self.assertEqual(set(frame["id_expediente"]), all_ids)
        for column in ccaa_andalucia.AMOUNT_COLS:
            self.assertTrue(ccaa_andalucia.pd.api.types.is_float_dtype(frame[column]), column)
        by_id = frame.set_index("id_expediente")

        awarded = by_id.loc[901]  # primer SERV/ADJ estandar con dos adjudicaciones
        publicado = date(2018, 1, 1) + timedelta(days=901 // 4)
        self.assertEqual(awarded["importe_licitacion"], 100901.0)
        self.assertEqual(awarded["valor_estimado"], 80901.0)
        self.assertEqual(awarded["importe_adjudicacion"], 1901.0)
        self.assertEqual(awarded["importe_adjudicacion_iva"], round(1901.0 * 1.21, 2))
        self.assertEqual(awarded["adjudicatario_nif"], "B00000901")
        self.assertEqual(awarded["todos_adjudicatarios_nif"], "B00000901;A00000001")
        self.assertEqual(awarded["num_adjudicaciones"], 2)
        self.assertEqual(awarded["fecha_publicacion"], publicado.isoformat())
        self.assertEqual(awarded["fecha_limite_presentacion"], (publicado + timedelta(days=15)).isoformat())
        self.assertEqual(awarded["codigo_perfil"], "CONS01")
        self.assertEqual(awarded["organo_contratacion"], "Organo CONS01")
        self.assertEqual(awarded["tipo_contrato_codigo"], "SERV")
        self.assertEqual(awarded["estado_codigo"], "ADJ")
        self.assertEqual(str(awarded["codigo_procedimiento"]), "2")
        self.assertTrue(awarded["url_detalle"].endswith("idExpediente=901"))

        pending = by_id.loc[1]
        self.assertTrue(ccaa_andalucia.pd.isna(pending["importe_adjudicacion"]))
        self.assertEqual(pending["num_adjudicaciones"], 0)

        menor = by_id.loc[min(self.men_ids)]  # UNIV01 con mediosPublicacion[0].codigo = null
        self.assertEqual(str(menor["codigo_procedimiento"]), "9")
        self.assertEqual(menor["medios_publicacion"], ";BOJA")

        # La 2a adjudicacion (importe 5.0) solo estaba en el NIF de todos_adjudicatarios_nif
        awards = json.loads(awarded["adjudicaciones_json"])
        self.assertEqual([award["importeAdjudicacion"] for award in awards], [1901.0, 5.0])
        self.assertEqual(json.loads(awarded["lotes_json"] or "[]"), [])

    def test_cli_scrape_men_completes_a_stale_perfil_cache(self):
        (self.data_dir / "perfiles_cache.json").write_text('["CONS01"]', encoding="utf-8")

        self.run_cli("scrape-men")

        self.assertEqual(self.read_csv_ids("licitaciones_menores.csv"), self.men_ids)
        cached = json.loads((self.data_dir / "perfiles_cache.json").read_text(encoding="utf-8"))
        self.assertEqual(cached, ["CONS01", "CONS02", "HIDDEN01", "SYBS03", "UNIV01"])


if __name__ == "__main__":
    unittest.main()
