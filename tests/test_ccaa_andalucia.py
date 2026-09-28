import copy
import gzip
import importlib.util
import io
import json
import os
import re
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_andalucia.py"
SPEC = importlib.util.spec_from_file_location("ccaa_andalucia", MODULE_PATH)
ccaa_andalucia = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ccaa_andalucia)
pd = ccaa_andalucia.pd
np = ccaa_andalucia.np

# Columnas de comun/historico.py que se anaden a las de siempre
META = ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")


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
        counts = iter([15, 10, 5])  # total, p2 y procedimiento desconocido
        scrape_calls = []

        def fake_cnt(*args, **kwargs):
            return next(counts)

        def fake_scrape_recursive(must, must_not, label, all_records, seen, dim_idx=0, known_total=None, **kwargs):
            scrape_calls.append((label, must, must_not, known_total))
            ids = {"p2": [1, 2], "p_unknown": [3]}.get(label, [])
            for expediente_id in ids:
                all_records.append({"id_expediente": expediente_id, "_source": {"idExpediente": expediente_id}})
                seen.add(expediente_id)
            return len(ids)

        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            ccaa_andalucia, "DATA_DIR", Path(tmpdir)
        ), patch.object(ccaa_andalucia, "PROCS", [2, 9]), patch.object(
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
        ), patch("sys.stdout", new_callable=io.StringIO):
            resumen = ccaa_andalucia.scrape_std()
            crudo = Path(tmpdir) / "raw" / "std.jsonl.gz"
            documentos = list(ccaa_andalucia._documentos_crudo(crudo))
            self.assertFalse((Path(tmpdir) / "raw" / "_en_curso").exists())

        self.assertEqual(resumen["documentos"], 3)
        self.assertEqual([documento["idExpediente"] for documento in documentos], [1, 2, 3])
        labels = [call[0] for call in scrape_calls]
        self.assertEqual(labels, ["p2", "p_unknown"])
        self.assertEqual(scrape_calls[1][3], 5)

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
        incompletos = []
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
                    incompletos=incompletos,
                )

        self.assertEqual(got, 0)
        self.assertIn("lbl/null_campo", "\n".join(logs.output))
        # Lo que pueda estar en esa rama no se da por retirado
        self.assertEqual([(i["etiqueta"], i["motivo"], len(i["must_not"])) for i in incompletos],
                         [("lbl/null_campo", "rama sin valor omitida", 900)])

    def test_scrape_recursive_records_a_page_run_that_stops_before_the_total(self):
        incompletos = []
        with patch.object(ccaa_andalucia, "paginate", return_value=([{"id_expediente": 1}], 3)):
            ccaa_andalucia.scrape_recursive([mm("a", 1)], [], "hoja", [], set(), known_total=2,
                                            incompletos=incompletos)
        self.assertEqual(
            [(i["etiqueta"], i["must"], i["total"], i["descargados"], i["motivo"]) for i in incompletos],
            [("hoja", [mm("a", 1)], 3, 1, "paginacion incompleta")],
        )


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
        self.assertEqual(list(frame.columns), ccaa_andalucia.CSV_COLS + list(META))
        self.assertTrue(frame["_en_ultima_descarga"].all())
        ids = frame["id_expediente"].tolist()
        self.assertEqual(len(ids), len(set(ids)), f"duplicados en {filename}")
        return set(ids)

    def test_cli_scrape_std_recovers_every_standard_record(self):
        output = self.run_cli("scrape-std")

        self.assertIn(f"SCRAPE ESTANDAR: {len(self.std_ids):,}", output)
        self.assertEqual(self.read_csv_ids("licitaciones_std.csv"), self.std_ids)
        # La capa cruda sustituye a licitaciones_std_progress.csv (y a la descarga a medias)
        crudo = self.data_dir / "raw" / "std.jsonl.gz"
        self.assertEqual({doc["idExpediente"] for doc in ccaa_andalucia._documentos_crudo(crudo)}, self.std_ids)
        self.assertFalse((self.data_dir / "raw" / "_en_curso").exists())
        self.assertFalse((self.data_dir / "licitaciones_std_progress.csv").exists())

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
        self.assertEqual(list(frame.columns), ccaa_andalucia.CSV_COLS + list(META))
        self.assertTrue(frame["_en_ultima_descarga"].all())
        self.assertTrue((frame["_primera_descarga"] == frame["_ultima_descarga"]).all())
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


# ---------------------------------------------------------------------------
# Sesgo del superviviente: ambito, capa cruda, acumulacion, semilla y reanudacion
# ---------------------------------------------------------------------------

mm = ccaa_andalucia.mm
mn = ccaa_andalucia.mn


class CoincidenciasTests(unittest.TestCase):
    """_Coincidencias: si una fila cae SEGURO / POSIBLE dentro de una consulta 'match'."""

    def setUp(self):
        self.tabla = pd.DataFrame(
            {
                "codigo_procedimiento": pd.Series([9, "9", 19, 9.0, None], dtype=object),
                "tipo_contrato_codigo": ["SUM", "sum", "CONSERV", "", "SUM"],
                "provincias_ejecucion": ["29;41", "41", "", "04", "29"],
                "numero_expediente": ["CONTR 2024 00001", "CONTR/2024/2", "SAS2024", "2023 2024", "CONTR 2024.5"],
                "estado_codigo": ["RES", "RES", "RES", "BRR", "RES"],
            }
        )
        self.ev = ccaa_andalucia._Coincidencias(self.tabla)

    def comprobar(self, consulta, seguro, posible):
        self.assertEqual(self.ev.seguro(consulta).tolist(), seguro)
        self.assertEqual(self.ev.posible(consulta).tolist(), posible)

    def test_codigos_numericos_sin_confundir_9_con_19(self):
        self.comprobar({"must": [mm("codigoProcedimiento", 9)]}, [True, True, False, True, False],
                       [True, True, False, True, False])

    def test_codigo_exacto_es_seguro_y_la_misma_palabra_posible(self):
        # 'sum' casaria si el campo es texto analizado, no si es keyword: posible, no seguro
        self.comprobar({"must": [mm("tipoContrato.codigo", "SUM")]}, [True, False, False, False, True],
                       [True, True, False, False, True])

    def test_provincias_multivalor(self):
        self.comprobar({"must": [mm("provinciasEjecucion", "41")]}, [True, True, False, False, False],
                       [True, True, False, False, False])

    def test_ano_del_numero_de_expediente(self):
        # Seguro solo como palabra entre espacios; '2024.5' o 'CONTR/2024/2' pueden serlo
        self.comprobar({"must": [mm("numeroExpediente", "2024")]}, [True, False, False, True, False],
                       [True, True, False, True, True])

    def test_rama_sin_valor(self):
        consulta = {"must_not": [mn("tipoContrato.codigo", valor) for valor in ("SUM", "SERV")]}
        self.comprobar(consulta, [False, False, True, True, False], [False, True, True, True, False])

    def test_campo_o_clausula_desconocidos_nunca_son_seguros(self):
        for consulta in ({"must": [mm("otroCampo", 1)]}, {"must_not": [{"range": {"a": {"gt": 1}}}]}):
            self.comprobar(consulta, [False] * 5, [True] * 5)

    def test_ambito_de_una_descarga_quita_lo_que_puede_estar_en_una_consulta_incompleta(self):
        alcance = {"must": [mm("codigoProcedimiento", 9)], "must_not": [mn("estado.codigo", "BRR")]}
        self.assertEqual(self.ev.seguro(alcance).tolist(), [True, True, False, False, False])
        cabecera = {"alcance": alcance, "incompletos": [{"must": [mm("provinciasEjecucion", "29")], "must_not": []}]}
        self.assertEqual(self.ev.ambito(cabecera).tolist(), [False, True, False, False, False])
        # El alcance cuenta si es SEGURO ('sum' no lo es) y la consulta incompleta si es POSIBLE
        por_tipo = {"alcance": {"must": [mm("tipoContrato.codigo", "SUM")]}, "incompletos": []}
        self.assertEqual(self.ev.ambito(por_tipo).tolist(), [True, False, False, False, True])
        cabecera = {"alcance": alcance, "incompletos": [{"must": [mm("tipoContrato.codigo", "SUM")]}]}
        self.assertEqual(self.ev.ambito(cabecera).tolist(), [False] * 5)
        # Sin la columna de un campo del alcance no se puede asegurar nada
        sin_estado = ccaa_andalucia._Coincidencias(self.tabla.drop(columns="estado_codigo"))
        self.assertEqual(sin_estado.seguro(alcance).tolist(), [False] * 5)


class AcumularPorTrozosTests(unittest.TestCase):
    def test_por_trozos_da_lo_mismo_que_una_sola_llamada(self):
        def tabla(ids, sufijo):
            return pd.DataFrame({
                "id_expediente": ids,
                "titulo": [f"T{i}{sufijo if i % 7 == 0 else ''}" for i in ids],
                "importe_licitacion": [float(i) for i in ids],
            })

        # 1-300 (el 5 servido dos veces y el 11 retirado antes); ahora faltan 251-300, cambian
        # los multiplos de 7, el 5 viene una vez, vuelve el 11 y hay 400 nuevos
        primera = tabla(list(range(1, 301)) + [5], "")
        anterior = ccaa_andalucia.acumular(None, primera, "d1")
        anterior.loc[anterior["id_expediente"] == 11, "_en_ultima_descarga"] = False
        filas = tabla(list(range(1, 251)) + list(range(1000, 1400)), " (cambiado)")
        # 280 ya no esta, pero puede estar en una consulta incompleta: no se retira
        cabecera = {"alcance": {}, "incompletos": [{"must": [mm("idExpediente", 280)]}]}

        resultados = []
        for filas_por_trozo in (10_000, 50):
            with patch.object(ccaa_andalucia, "FILAS_POR_TROZO", filas_por_trozo), patch.object(
                ccaa_andalucia, "acumular", wraps=ccaa_andalucia.acumular
            ) as llamadas:
                resultados.append(ccaa_andalucia._acumular_version(anterior.copy(), filas.copy(), "d2", cabecera))
            self.assertEqual(llamadas.call_count, 1 if filas_por_trozo == 10_000 else 13)
        una, trozos = resultados
        pd.testing.assert_frame_equal(trozos, una)
        self.assertEqual(list(una.columns), ["id_expediente", "titulo", "importe_licitacion"] + list(META))
        vigencia = una.groupby("id_expediente")["_en_ultima_descarga"].apply(list)
        self.assertEqual(vigencia[300], [False])
        self.assertEqual(vigencia[7], [False, True])
        self.assertEqual(vigencia[5], [True, False])
        self.assertEqual(vigencia[11], [True])
        self.assertEqual(vigencia[280], [True])
        self.assertEqual(una["id_expediente"].tolist()[-400:], list(range(1000, 1400)))

    def test_un_trozo_sin_filas_nuevas_no_impide_retirar(self):
        anterior = ccaa_andalucia.acumular(None, pd.DataFrame({"id_expediente": list(range(1, 21))}), "d1")
        filas = pd.DataFrame({"id_expediente": [1, 2, 3]})
        with patch.object(ccaa_andalucia, "FILAS_POR_TROZO", 2):
            acumulada = ccaa_andalucia._acumular_version(anterior, filas, "d2", {"alcance": {}, "incompletos": []})
        self.assertEqual(acumulada["_en_ultima_descarga"].tolist(), [True] * 3 + [False] * 17)


VENTANA = 100  # MAX_FROM=0: una sola pagina de 100 por consulta y ordenacion


def portal_compacto():
    """~630 expedientes para una ventana de 100 resultados. std: p1 (60, perfil PA) y p2
    (SERV 80 de PA y SUM 70 de PB). Menores: SERV 90 de PA (ano 2023) y en SUM/RES/O 60 de
    PB y 250 de PC iguales en las 8 dimensiones y en todos los campos de ordenacion salvo
    idExpediente: el multi-sort solo ve los 100 primeros y los 100 ultimos (tope). Los 10
    BRR no se descargan nunca."""
    docs, grupos = [], {}

    def add(nombre, count, **fields):
        grupos[nombre] = []
        for _ in range(count):
            doc_id = len(docs) + 1
            docs.append(make_doc(doc_id, **fields))
            grupos[nombre].append(doc_id)

    add("std_p1", 60, proc="1", perfil="PA")
    add("std_p2_serv", 80, proc="2", tipo="SERV", perfil="PA")
    add("std_p2_sum", 70, proc="2", tipo="SUM", perfil="PB", awarded=True)
    add("brr", 10, proc="1", estado="BRR", perfil="PA")
    add("men_serv_2023", 90, proc="9", tipo="SERV", estado="RES", perfil="PA", anio="2023")
    add("men_pb", 60, proc="9", tipo="SUM", estado="RES", perfil="PB", provs=("29",), fp="M", anio="2024")
    add("men_pc", 250, proc="9", tipo="SUM", estado="RES", perfil="PC", provs=("29",), fp="M", anio="2024")
    for doc_id in grupos["men_pc"]:
        docs[doc_id - 1].update(
            {
                "numeroExpediente": "CONTR 2024 SAS",
                "titulo": "Suministro",
                "importeLicitacion": 10.0,
                "fechaPublicacion": "2024-01-01T10:00:00+0100",
                "fechaLimitePresentacion": "2024-01-15T23:59:59+0100",
            }
        )
    return docs, grupos


def _con(body, campo, valor):
    return {"match": {campo: valor}} in body["query"]["bool"].get("must", [])


class Portal(FakeElastic):
    """FakeElastic con fallos: HTTP 503 en las consultas de documentos que cumplen
    `fallar(body)` y el recuento que devuelva `recuento(body)` (si no es None)."""

    def __init__(self, docs, fallar=None, recuento=None):
        super().__init__(docs, max_window=VENTANA)
        self.fallar = fallar
        self.recuento = recuento

    def post(self, url, json=None, timeout=None, **kwargs):
        if json.get("size") and self.fallar is not None and self.fallar(json):
            self.bodies.append(json)
            return FakeResponse(503, {"error": "no disponible"})
        if not json.get("size") and self.recuento is not None and self.recuento(json) is not None:
            self.bodies.append(json)
            return FakeResponse(200, {"hits": {"total": {"value": self.recuento(json), "relation": "eq"}, "hits": []}})
        return super().post(url, json=json, timeout=timeout, **kwargs)


def publicado_antiguo(docs):
    """Parquet con el esquema y los errores del publicado v2026.02: sin las columnas JSON,
    vacios de texto como 'nan', recuentos decimales vacios sin adjudicaciones ni anuncios,
    importe_adjudicacion_iva vacio y codigo_procedimiento entero."""
    filas = []
    for doc in docs:
        fila = ccaa_andalucia.flatten(doc)
        for columna in ("adjudicaciones_json", "lotes_json", "anuncios_json", "campos_extra_json"):
            del fila[columna]
        fila["importe_adjudicacion_iva"] = None
        for columna in ("num_adjudicaciones", "num_anuncios"):
            fila[columna] = fila[columna] or None
        for columna, valor in fila.items():
            if valor == "":
                fila[columna] = None if columna in ccaa_andalucia.AMOUNT_COLS else "nan"
        filas.append(fila)
    tabla = pd.DataFrame(filas)
    tabla["codigo_procedimiento"] = tabla["codigo_procedimiento"].astype("int64")
    for columna in ccaa_andalucia.AMOUNT_COLS + ["num_adjudicaciones", "num_anuncios"]:
        tabla[columna] = tabla[columna].astype("float64")
    return tabla


class _Captura(ccaa_andalucia.logging.Handler):
    def __init__(self):
        super().__init__(ccaa_andalucia.logging.INFO)
        self.mensajes = []

    def emit(self, record):
        self.mensajes.append((record.levelname, record.getMessage()))


class HistoricoAndaluciaTests(unittest.TestCase):
    """Re-ejecuciones contra un portal falso: una 'fecha' nueva por ejecucion (la version
    cruda que escribe la ejecucion k tiene fecha 2026-01-01 + k dias)."""

    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.tmp = Path(tmpdir.name)
        self.salida = self.tmp / "salida"
        self.salida.mkdir()
        self.docs, self.grupos = portal_compacto()
        self.ejecuciones = 0

    def fecha(self, ejecucion):
        return (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=ejecucion)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def ejecutar(self, *args, docs=None, portal=None, mismo_dia=False):
        if not mismo_dia:
            self.ejecuciones += 1
        self.portal = portal or Portal(copy.deepcopy(self.docs if docs is None else docs))
        momento = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() + self.ejecuciones * 86400
        escribir_crudo = ccaa_andalucia._escribir_crudo

        def con_fecha(destino, cabecera, documentos):
            estado = escribir_crudo(destino, cabecera, documentos)
            if estado != "sin_cambios":
                os.utime(destino, (momento, momento))
            return estado

        captura = _Captura()
        nivel = ccaa_andalucia.log.level
        patches = [
            patch.object(ccaa_andalucia, "DATA_DIR", self.salida),
            patch.object(ccaa_andalucia, "PERFILES_CACHE_PATH", self.salida / "perfiles_cache.json"),
            patch.object(ccaa_andalucia, "_PERFILES", None),
            patch.object(ccaa_andalucia, "MAX_FROM", 0),
            patch.object(ccaa_andalucia, "DELAY", 0),
            patch.object(ccaa_andalucia.time, "sleep"),
            patch.object(ccaa_andalucia.S, "post", side_effect=self.portal.post),
            patch.object(ccaa_andalucia.S, "get", side_effect=self.portal.get),
            patch.object(ccaa_andalucia, "_escribir_crudo", side_effect=con_fecha),
            patch("sys.stdout", new_callable=io.StringIO),
        ]
        for active in patches:
            active.start()
        ccaa_andalucia.log.addHandler(captura)
        ccaa_andalucia.log.setLevel(ccaa_andalucia.logging.INFO)
        try:
            rc = ccaa_andalucia.main(list(args))
            self.stdout = ccaa_andalucia.sys.stdout.getvalue()
        finally:
            ccaa_andalucia.log.removeHandler(captura)
            ccaa_andalucia.log.setLevel(nivel)
            for active in reversed(patches):
                active.stop()
        self.mensajes = captura.mensajes
        return rc

    def sin(self, *ids, docs=None):
        quitar = set(ids)
        return [doc for doc in copy.deepcopy(self.docs if docs is None else docs) if doc["idExpediente"] not in quitar]

    def tabla(self):
        return pd.read_parquet(self.salida / "licitaciones_andalucia.parquet")

    def vigencia(self, tabla=None):
        """{id_expediente: [_en_ultima_descarga de cada fila]}"""
        tabla = self.tabla() if tabla is None else tabla
        vigencia = {}
        for expediente, vigente in zip(tabla["id_expediente"], tabla["_en_ultima_descarga"]):
            vigencia.setdefault(int(expediente), []).append(bool(vigente))
        return vigencia

    def ficheros(self):
        return {
            str(path.relative_to(self.salida)): (path.stat().st_mtime_ns, path.read_bytes())
            for path in sorted(self.salida.rglob("*"))
            if path.is_file() and path.name != "scraper.log"
        }

    def avisos(self):
        return [mensaje for nivel, mensaje in self.mensajes if nivel != "INFO"]

    # -- una sola descarga ------------------------------------------------------

    def test_una_descarga_es_la_tabla_de_siempre_mas_tres_columnas(self):
        self.assertEqual(self.ejecutar("scrape"), 0)

        tabla = self.tabla()
        self.assertEqual(list(tabla.columns), ccaa_andalucia.CSV_COLS + list(META))
        self.assertTrue(tabla["_en_ultima_descarga"].all())
        self.assertEqual(set(tabla["_primera_descarga"]), {self.fecha(1)})
        # Las mismas filas y valores que flatten() + los tipos de siempre sobre lo descargado
        documentos = [
            documento
            for nombre in ("std.jsonl.gz", "menores.jsonl.gz")
            for documento in ccaa_andalucia._documentos_crudo(self.salida / "raw" / nombre)
        ]
        esperado = ccaa_andalucia._tipos_salida(
            ccaa_andalucia.records_to_dataframe([ccaa_andalucia.flatten(documento) for documento in documentos])
        )
        ccaa_andalucia._escribir_parquet(esperado, self.tmp / "esperado.parquet")
        pd.testing.assert_frame_equal(
            tabla[ccaa_andalucia.CSV_COLS], pd.read_parquet(self.tmp / "esperado.parquet")
        )
        # std y menores sin BRR; de los 250 iguales de PC, los 200 que alcanza el multi-sort
        esperados = set().union(*(self.grupos[g] for g in ("std_p1", "std_p2_serv", "std_p2_sum",
                                                             "men_serv_2023", "men_pb")))
        esperados |= set(self.grupos["men_pc"][:100] + self.grupos["men_pc"][-100:])
        self.assertEqual(set(tabla["id_expediente"]), esperados)
        todos = pd.read_csv(self.salida / "licitaciones_all.csv", encoding="utf-8-sig")
        menores = pd.read_csv(self.salida / "licitaciones_menores.csv", encoding="utf-8-sig")
        self.assertEqual(len(todos), len(tabla))
        self.assertEqual(set(menores["codigo_procedimiento"]), {9})

    def test_la_capa_cruda_guarda_cada_source_tal_cual_con_su_cabecera(self):
        self.ejecutar("scrape")

        crudo = self.salida / "raw" / "menores.jsonl.gz"
        por_id = {doc["idExpediente"]: doc for doc in self.docs}
        for documento in ccaa_andalucia._documentos_crudo(crudo):
            self.assertEqual(documento, por_id[documento["idExpediente"]])
            self.assertEqual(list(documento), list(por_id[documento["idExpediente"]]))  # mismo orden de campos
        cabecera = ccaa_andalucia._cabecera_crudo(crudo)
        self.assertEqual(cabecera["formato"], ccaa_andalucia.FORMATO_CRUDO)
        self.assertEqual((cabecera["total"], cabecera["documentos"]), (400, 350))
        self.assertEqual(cabecera["alcance"]["must"], [mm("codigoProcedimiento", 9)])
        self.assertEqual([i["etiqueta"] for i in cabecera["incompletos"]], ["men/SUM/RES/O/PC/29/M/2024"])
        self.assertEqual(cabecera["incompletos"][0]["motivo"], "tope de 10.000 resultados")
        # gzip sin fecha ni nombre: el mismo contenido da los mismos bytes
        self.assertEqual(crudo.read_bytes()[4:8], b"\x00\x00\x00\x00")
        self.assertFalse((self.salida / "raw" / "_en_curso").exists())

    # -- re-ejecuciones ------------------------------------------------------

    def test_sin_cambios_no_se_escribe_nada(self):
        self.ejecutar("scrape")
        antes = self.ficheros()

        self.assertEqual(self.ejecutar("scrape"), 0)

        self.assertEqual(self.ficheros(), antes)
        self.assertFalse(list(self.salida.rglob(ccaa_andalucia.HISTORICO)))
        self.assertIn("Salidas sin cambios", "\n".join(mensaje for _, mensaje in self.mensajes))

    def test_registro_retirado_y_modificado_se_conservan(self):
        self.ejecutar("scrape")
        retirado_std, retirado_men = self.grupos["std_p1"][0], self.grupos["men_pb"][0]
        cambiado = self.grupos["men_serv_2023"][5]
        docs = self.sin(retirado_std, retirado_men)
        next(doc for doc in docs if doc["idExpediente"] == cambiado)["titulo"] = "Titulo corregido"

        self.assertEqual(self.ejecutar("scrape", docs=docs), 0)

        tabla = self.tabla()
        vigencia = self.vigencia(tabla)
        self.assertEqual(vigencia[retirado_std], [False])
        self.assertEqual(vigencia[retirado_men], [False])
        filas = tabla[tabla["id_expediente"] == cambiado]
        self.assertEqual(filas["titulo"].tolist(), [f"Contrato {cambiado:05d}", "Titulo corregido"])
        self.assertEqual(filas["_en_ultima_descarga"].tolist(), [False, True])
        self.assertEqual(filas["_ultima_descarga"].tolist(), [self.fecha(1), self.fecha(2)])
        retirada = tabla[tabla["id_expediente"] == retirado_men].iloc[0]
        self.assertEqual((retirada["_primera_descarga"], retirada["_ultima_descarga"]), (self.fecha(1), self.fecha(1)))
        otros = {expediente: flags for expediente, flags in vigencia.items()
                 if expediente not in (retirado_std, retirado_men, cambiado)}
        self.assertTrue(all(flags == [True] for flags in otros.values()))
        # La version anterior de cada descarga y de la salida, en _historico/
        self.assertEqual(len(list((self.salida / "raw" / ccaa_andalucia.HISTORICO).iterdir())), 2)
        self.assertEqual(len(list((self.salida / ccaa_andalucia.HISTORICO).glob("licitaciones_andalucia__*"))), 1)
        menores = pd.read_csv(self.salida / "licitaciones_menores.csv", encoding="utf-8-sig")
        self.assertFalse(menores.loc[menores["id_expediente"] == retirado_men, "_en_ultima_descarga"].item())

    def test_misma_descarga_comprimida_de_otra_forma_no_es_una_version_nueva(self):
        # Otra version de zlib puede comprimir distinto el mismo contenido
        self.ejecutar("scrape-men")
        crudo = self.salida / "raw" / "menores.jsonl.gz"
        fecha = crudo.stat().st_mtime
        crudo.write_bytes(gzip.compress(gzip.decompress(crudo.read_bytes()), compresslevel=1))
        os.utime(crudo, (fecha, fecha))
        self.ejecutar("procesar", mismo_dia=True)  # la salida incorpora esa copia (otro sha256)
        antes = self.ficheros()

        self.assertEqual(self.ejecutar("scrape-men"), 0)

        self.assertEqual(self.ficheros(), antes)

    def test_dos_descargas_con_la_misma_fecha_se_incorporan_las_dos(self):
        self.ejecutar("scrape-men")
        quitado = self.grupos["men_pb"][0]

        self.ejecutar("scrape-men", docs=self.sin(quitado), mismo_dia=True)

        self.assertEqual(len(list((self.salida / "raw" / ccaa_andalucia.HISTORICO).iterdir())), 1)
        self.assertEqual(self.vigencia()[quitado], [False])

    def test_misma_cifra_entera_o_decimal_no_es_un_cambio(self):
        # En la descarga entera importeLicitacion mezcla decimales y enteros (float64); en la
        # parcial de PB solo hay enteros: antes quedaba int64 y 5000 no casaba con 5000.0
        docs = copy.deepcopy(self.docs)
        for doc in docs:
            if doc["idExpediente"] in self.grupos["men_pb"]:
                doc["importeLicitacion"] = 5000
                doc["valorEstimado"] = 4000
        self.ejecutar("scrape-men", docs=docs)
        filas = len(self.tabla())

        self.assertEqual(self.ejecutar("scrape-men", "--perfil", "PB", docs=docs), 0)

        tabla = self.tabla()
        self.assertEqual(len(tabla), filas)
        self.assertTrue(tabla["_en_ultima_descarga"].all())

    def test_registro_que_vuelve_se_reactiva_sin_duplicarse(self):
        self.ejecutar("scrape")
        vuelve = self.grupos["men_serv_2023"][0]
        self.ejecutar("scrape", docs=self.sin(vuelve))
        self.assertEqual(self.vigencia()[vuelve], [False])

        self.ejecutar("scrape")

        self.assertEqual(self.vigencia()[vuelve], [True])

    def test_descarga_parcial_por_perfil_no_retira_fuera_de_su_alcance(self):
        self.ejecutar("scrape")
        de_pa, de_pb = self.grupos["men_serv_2023"][0], self.grupos["men_pb"][0]
        licitacion_pb = self.grupos["std_p2_sum"][0]

        self.assertEqual(self.ejecutar("scrape-men", "--perfil", "PB", docs=self.sin(de_pa, de_pb, licitacion_pb)), 0)

        vigencia = self.vigencia()
        self.assertEqual(vigencia[de_pb], [False])  # menor de PB: releido y ya no esta
        self.assertEqual(vigencia[de_pa], [True])  # menor de otro perfil: fuera del alcance
        self.assertEqual(vigencia[licitacion_pb], [True])  # licitacion de PB: no se ha descargado
        self.assertTrue((self.salida / "raw" / "menores__perfil-PB.jsonl.gz").exists())
        # El perfil fijo no se vuelve a partir
        self.assertFalse(any("perfilContratante" in json.dumps(body.get("sort")) for body in self.portal.bodies))
        paginas_de_otros = [body for body in self.portal.bodies
                            if body.get("size") and body.get("sort") and not _con(body, "perfilContratante.codigo", "PB")]
        self.assertEqual(paginas_de_otros, [])

    def test_descarga_parcial_no_parte_por_la_dimension_que_fija(self):
        self.ejecutar("scrape")
        con_tope = self.grupos["men_pc"]
        de_pb = self.grupos["men_pb"][0]

        self.assertEqual(self.ejecutar("scrape-men", "--perfil", "PC", docs=self.sin(con_tope[0], de_pb)), 0)

        dobles = [body for body in self.portal.bodies
                  if json.dumps(body["query"]).count("perfilContratante.codigo") > 1]
        self.assertEqual(dobles, [])
        cabecera = ccaa_andalucia._cabecera_crudo(self.salida / "raw" / "menores__perfil-PC.jsonl.gz")
        self.assertEqual([i["etiqueta"] for i in cabecera["incompletos"]], ["men/SUM/RES/O/29/M/2024"])
        vigencia = self.vigencia()
        self.assertEqual(vigencia[con_tope[0]], [True])  # consulta con tope
        self.assertEqual(vigencia[de_pb], [True])  # fuera del alcance

    def test_descarga_parcial_por_ano(self):
        self.ejecutar("scrape")
        de_2023, de_2024 = self.grupos["men_serv_2023"][0], self.grupos["men_pb"][0]

        self.assertEqual(self.ejecutar("scrape-men", "--anio", "2023", docs=self.sin(de_2023, de_2024)), 0)

        vigencia = self.vigencia()
        self.assertEqual(vigencia[de_2023], [False])
        self.assertEqual(vigencia[de_2024], [True])

    def test_solo_licitaciones_no_toca_los_menores(self):
        self.ejecutar("scrape")
        licitacion, menor = self.grupos["std_p2_serv"][0], self.grupos["men_pb"][0]

        self.assertEqual(self.ejecutar("scrape-std", docs=self.sin(licitacion, menor)), 0)

        vigencia = self.vigencia()
        self.assertEqual(vigencia[licitacion], [False])
        self.assertEqual(vigencia[menor], [True])

    def test_consulta_con_tope_no_retira_lo_que_puede_estar_en_ella(self):
        self.ejecutar("scrape")
        self.assertTrue(any("PARTIAL" in aviso for aviso in self.avisos()))
        con_tope = self.grupos["men_pc"]
        quitado, cambiado, de_pb = con_tope[0], con_tope[-1], self.grupos["men_pb"][0]
        docs = self.sin(quitado, de_pb)
        next(doc for doc in docs if doc["idExpediente"] == cambiado)["valorEstimado"] = 1.0

        self.assertEqual(self.ejecutar("scrape-men", docs=docs), 0)

        tabla = self.tabla()
        vigencia = self.vigencia(tabla)
        self.assertEqual(vigencia[quitado], [True])  # pudo quedar fuera por el tope: no se retira
        self.assertEqual(vigencia[de_pb], [False])  # fuera de la consulta con tope si
        # Un expediente que vuelve cambiado se retira aunque este en la consulta con tope
        filas = tabla[tabla["id_expediente"] == cambiado]
        self.assertEqual(filas["valor_estimado"].tolist(), [80000.0 + cambiado, 1.0])
        self.assertEqual(filas["_en_ultima_descarga"].tolist(), [False, True])
        # El que entra en la ventana al salir 'quitado' se anade; ninguno de PC se retira
        self.assertEqual(vigencia[con_tope[100]], [True])
        self.assertTrue(all(flags[-1] for expediente, flags in vigencia.items() if expediente in con_tope))

    def test_descarga_que_falla_no_retira_ni_escribe_nada_y_se_reanuda(self):
        self.ejecutar("scrape")
        salida = self.salida / "licitaciones_andalucia.parquet"
        antes = {nombre: contenido for nombre, contenido in self.ficheros().items()
                 if nombre != "perfiles_cache.json"}
        quitado = self.grupos["men_serv_2023"][0]
        docs = self.sin(quitado)
        falla = Portal(
            copy.deepcopy(docs),
            fallar=lambda body: _con(body, "codigoProcedimiento", 9) and _con(body, "tipoContrato.codigo", "SUM"),
        )

        self.assertEqual(self.ejecutar("scrape-men", portal=falla), 1)

        despues = {nombre: contenido for nombre, contenido in self.ficheros().items()
                   if not nombre.startswith("raw/_en_curso") and nombre != "perfiles_cache.json"}
        self.assertEqual(despues, antes)
        self.assertEqual(self.vigencia()[quitado], [True])
        estado = json.loads((self.salida / "raw" / "_en_curso" / "menores" / "estado.json").read_text("utf-8"))
        self.assertEqual([bloque["etiqueta"] for bloque in estado["bloques"]], ["men/SERV"])

        # Se reanuda: men/SERV no se vuelve a pedir y el resultado es el de una descarga entera
        self.assertEqual(self.ejecutar("scrape-men", docs=docs), 0)
        paginas_serv = [body for body in self.portal.bodies if body.get("size")
                        and _con(body, "codigoProcedimiento", 9) and _con(body, "tipoContrato.codigo", "SERV")]
        self.assertEqual(paginas_serv, [])
        self.assertIn("Reanudando menores", "\n".join(mensaje for _, mensaje in self.mensajes))
        self.assertFalse((self.salida / "raw" / "_en_curso").exists())
        self.assertEqual(self.vigencia()[quitado], [False])
        self.assertTrue(salida.exists())

    def test_recuentos_que_no_cubren_el_total_no_retiran_nada(self):
        self.ejecutar("scrape")
        quitado = self.grupos["men_serv_2023"][0]
        docs = self.sin(quitado)
        # El recuento del bloque men/SUM da 0 con HTTP 200
        falla = Portal(
            copy.deepcopy(docs),
            recuento=lambda body: 0 if (_con(body, "tipoContrato.codigo", "SUM")
                                        and len(body["query"]["bool"]["must"]) == 2) else None,
        )

        self.assertEqual(self.ejecutar("scrape-men", portal=falla), 0)

        self.assertTrue(any("no cubren el total" in aviso for aviso in self.avisos()))
        vigencia = self.vigencia()
        self.assertEqual(vigencia[quitado], [True])
        self.assertTrue(all(vigencia[expediente] == [True] for expediente in self.grupos["men_pb"]))

    def test_recuentos_que_no_cubren_un_trozo_solo_protegen_ese_trozo(self):
        self.ejecutar("scrape")
        de_pb, de_serv = self.grupos["men_pb"][0], self.grupos["men_serv_2023"][0]
        docs = self.sin(de_pb, de_serv)
        # El recuento de men/SUM/RES/O/PB da 0 con HTTP 200: men/SUM/RES/O queda incompleto
        falla = Portal(
            copy.deepcopy(docs),
            recuento=lambda body: 0 if (_con(body, "perfilContratante.codigo", "PB")
                                        and _con(body, "codigoProcedimiento", 9)) else None,
        )

        self.assertEqual(self.ejecutar("scrape-men", portal=falla), 0)

        self.assertTrue(any("men/SUM/RES/O: los recuentos" in aviso for aviso in self.avisos()))
        vigencia = self.vigencia()
        self.assertEqual(vigencia[de_pb], [True])  # pudo quedar sin descargar
        self.assertEqual(vigencia[de_serv], [False])  # otro trozo, completo
        self.assertTrue(all(vigencia[expediente] == [True] for expediente in self.grupos["men_pb"]))

    def test_descarga_vacia_no_retira_nada(self):
        self.ejecutar("scrape")
        antes = self.ficheros()

        solo_brr = [doc for doc in self.docs if doc["estado"]["codigo"] == "BRR"]
        self.assertEqual(self.ejecutar("scrape", docs=solo_brr), 1)

        self.assertEqual(self.ficheros(), antes)
        self.assertTrue(any("descarga vacia" in aviso for aviso in self.avisos()))

    # -- salida anterior de otro codigo ---------------------------------------

    def test_salida_del_codigo_anterior_no_se_casa_fila_a_fila_y_se_archiva(self):
        salida = self.salida / "licitaciones_andalucia.parquet"
        publicado_antiguo(self.docs).to_parquet(salida, index=False)
        antiguo = salida.read_bytes()

        self.assertEqual(self.ejecutar("scrape-men"), 0)

        self.assertTrue(any("no tiene las columnas de historico" in aviso for aviso in self.avisos()))
        tabla = self.tabla()
        self.assertTrue(tabla["_en_ultima_descarga"].all())
        self.assertEqual(len(tabla), 350)
        archivados = list((self.salida / ccaa_andalucia.HISTORICO).glob("licitaciones_andalucia__*"))
        self.assertEqual([path.read_bytes() for path in archivados], [antiguo])

    def test_salida_ilegible_no_se_sobrescribe(self):
        salida = self.salida / "licitaciones_andalucia.parquet"
        puntero = b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 48856894\n"
        salida.write_bytes(puntero)

        self.assertEqual(self.ejecutar("scrape-men"), 1)

        self.assertEqual(salida.read_bytes(), puntero)
        self.assertTrue((self.salida / "raw" / "menores.jsonl.gz").exists())
        salida.rename(self.tmp / "puntero")
        self.assertEqual(self.ejecutar("procesar"), 0)
        self.assertEqual(len(self.tabla()), 350)

    # -- semilla ----------------------------------------------------------------

    def test_semilla_anade_solo_las_claves_que_faltan_dentro_del_ambito(self):
        antiguo = self.grupos["men_serv_2023"][3]
        publicado = copy.deepcopy([doc for doc in self.docs if doc["estado"]["codigo"] != "BRR"])
        next(doc for doc in publicado if doc["idExpediente"] == antiguo)["titulo"] = "Titulo de febrero"
        retirado_pb = make_doc(9001, proc="9", tipo="SUM", estado="RES", perfil="PB", provs=("29",), fp="M",
                               anio="2024")
        retirado_pc = make_doc(9002, proc="9", tipo="SUM", estado="RES", perfil="PC", provs=("29",), fp="M",
                               anio="2024")
        retirado_pc["numeroExpediente"] = "CONTR 2024 SAS"
        retirado_std = make_doc(9003, proc="2", tipo="SERV", perfil="PA")
        ruta = self.tmp / "licitaciones_andalucia_v2026.02.parquet"
        publicado_antiguo(publicado + [retirado_pb, retirado_pc, retirado_std]).to_parquet(ruta, index=False)

        self.ejecutar("scrape-men")
        descargada = self.tabla()
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(ruta)), 0)

        tabla = self.tabla()
        sembradas = tabla[tabla["_origen"].notna()]
        # 9002 puede estar en la consulta con tope y de std no hay ninguna descarga
        self.assertEqual(sembradas["id_expediente"].tolist(), [9001])
        fila = sembradas.iloc[0]
        self.assertEqual(fila["_origen"], "release v2026.02")
        self.assertFalse(fila["_en_ultima_descarga"])
        self.assertEqual(fila["todos_adjudicatarios_nif"], "")  # 'nan' del publicado
        self.assertEqual(fila["num_adjudicaciones"], 0)
        self.assertTrue(pd.isna(fila["adjudicaciones_json"]))
        # Las filas descargadas no cambian (tampoco la del expediente con otro titulo en febrero)
        pd.testing.assert_frame_equal(
            tabla[tabla["_origen"].isna()].drop(columns="_origen").reset_index(drop=True), descargada
        )
        self.assertEqual(tabla.loc[tabla["id_expediente"] == antiguo, "titulo"].tolist(), [f"Contrato {antiguo:05d}"])
        self.assertIn("fuera del ámbito", self.stdout)

        # Con la descarga de std, la licitacion retirada tambien entra
        self.ejecutar("scrape-std")
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(ruta)), 0)
        tabla = self.tabla()
        self.assertEqual(sorted(tabla.loc[tabla["_origen"].notna(), "id_expediente"]), [9001, 9003])
        self.assertEqual(tabla["id_expediente"].duplicated().sum(), 0)

        # Sembrar otra vez no anade ni escribe nada
        antes = self.ficheros()
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(ruta)), 0)
        self.assertEqual(self.ficheros(), antes)

    def test_semilla_con_las_dos_descargas_anade_de_las_dos(self):
        retirado_men = make_doc(9001, proc="9", tipo="SERV", estado="RES", perfil="PA", anio="2023")
        retirado_std = make_doc(9003, proc="2", tipo="SERV", perfil="PA")
        ruta = self.tmp / "publicado.parquet"
        publicado_antiguo([retirado_men, retirado_std]).to_parquet(ruta, index=False)

        self.assertEqual(self.ejecutar("scrape", "--semilla", str(ruta)), 0)

        tabla = self.tabla()
        self.assertEqual(sorted(tabla.loc[tabla["_origen"].notna(), "id_expediente"]), [9001, 9003])
        menores = pd.read_csv(self.salida / "licitaciones_menores.csv", encoding="utf-8-sig")
        self.assertIn(9001, set(menores["id_expediente"]))

    def test_semilla_que_no_se_puede_usar(self):
        self.ejecutar("scrape-men")
        salida = self.salida / "licitaciones_andalucia.parquet"
        copia = self.tmp / "salida_anterior.parquet"
        copia.write_bytes(salida.read_bytes())
        antes = self.ficheros()

        self.assertEqual(self.ejecutar("procesar", "--semilla", str(salida)), 2)
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(salida), "--origen-semilla", "x"), 2)
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(self.tmp / "no_existe.parquet")), 2)
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(copia)), 2)  # sin --origen-semilla
        self.assertEqual(self.ficheros(), antes)
        self.assertEqual(self.ejecutar("procesar", "--semilla", str(copia), "--origen-semilla", "release v2026.09"), 0)
        self.assertEqual(self.ficheros(), antes)  # todas sus claves ya estan

    # -- CLI --------------------------------------------------------------------

    def test_salida_por_opcion_sin_tocar_la_carpeta_por_defecto(self):
        defecto = self.tmp / "defecto"
        defecto.mkdir()
        otra = self.tmp / "otra"
        raiz = ccaa_andalucia.logging.getLogger()
        handlers = list(raiz.handlers)
        portal = Portal(copy.deepcopy(self.docs))
        try:
            with patch.object(ccaa_andalucia, "DATA_DIR", defecto), patch.object(
                ccaa_andalucia, "PERFILES_CACHE_PATH", defecto / "perfiles_cache.json"
            ), patch.object(ccaa_andalucia, "_PERFILES", None), patch.object(ccaa_andalucia, "MAX_FROM", 0), patch.object(
                ccaa_andalucia, "DELAY", 0
            ), patch.object(ccaa_andalucia.time, "sleep"), patch.object(
                ccaa_andalucia.S, "post", side_effect=portal.post
            ), patch.object(ccaa_andalucia.S, "get", side_effect=portal.get), patch(
                "sys.stdout", new_callable=io.StringIO
            ):
                rc = ccaa_andalucia.main(["scrape-men", "--salida", str(otra)])
        finally:
            for handler in list(raiz.handlers):
                if handler not in handlers:
                    raiz.removeHandler(handler)
                    handler.close()
            for handler in handlers:
                if handler not in raiz.handlers:
                    raiz.addHandler(handler)

        self.assertEqual(rc, 0)
        self.assertEqual(list(defecto.iterdir()), [])
        self.assertTrue((otra / "licitaciones_andalucia.parquet").exists())
        self.assertTrue((otra / "raw" / "menores.jsonl.gz").exists())
        self.assertTrue((otra / "perfiles_cache.json").exists())

    def test_anio_invalido(self):
        self.assertEqual(self.ejecutar("scrape-men", "--anio", "24"), 2)


if __name__ == "__main__":
    unittest.main()
