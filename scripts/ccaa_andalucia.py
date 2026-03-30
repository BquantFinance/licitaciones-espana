#!/usr/bin/env python3
"""
===========================================================================
Scraper de licitaciones de la Junta de Andalucia
===========================================================================

Extrae licitaciones regulares y contratos menores desde el proxy
Elasticsearch del portal de perfiles del contratante.
"""

import csv
import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from requests import RequestException

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    pd = None
    HAS_PANDAS = False

BASE = "https://www.juntadeandalucia.es/haciendayadministracionpublica/apl/pdc-front-publico"
ES_URL = f"{BASE}/elastic/sirec_pdc_expedientes/_search?pretty"
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "ccaa_Andalucia"
DATA_DIR.mkdir(exist_ok=True)
PERFILES_CACHE_PATH = DATA_DIR / "perfiles_cache.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

DELAY = 0.3
PAGE_SIZE = 100
MAX_FROM = 9900
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
COUNT_TIMEOUT = 60
DEFAULT_TIMEOUT = 90
INTEGER_DEFAULTS = {
    "num_adjudicaciones": 0,
    "num_lotes": 0,
    "num_anuncios": 0,
}

S = requests.Session()
S.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.juntadeandalucia.es",
        "Referer": f"{BASE}/perfiles-licitaciones/buscador-general",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
)


class ScraperError(RuntimeError):
    """Error operativo del scraper de Andalucia."""


CSV_COLS = [
    "id_expediente",
    "numero_expediente",
    "titulo",
    "tipo_contrato",
    "tipo_contrato_codigo",
    "organo_contratacion",
    "codigo_perfil",
    "codigo_dir3",
    "estado",
    "estado_codigo",
    "importe_licitacion",
    "valor_estimado",
    "importe_adjudicacion",
    "importe_adjudicacion_iva",
    "fecha_publicacion",
    "fecha_limite_presentacion",
    "anuncio_primera_fecha",
    "anuncio_ultima_fecha",
    "adjudicatario_nif",
    "todos_adjudicatarios_nif",
    "num_adjudicaciones",
    "codigo_procedimiento",
    "codigo_tramitacion",
    "codigo_normativa",
    "forma_presentacion",
    "cofinanciado_ue",
    "subasta_electronica",
    "sistema_racionalizacion",
    "cpv",
    "provincias_ejecucion",
    "medios_publicacion",
    "num_lotes",
    "num_anuncios",
    "url_detalle",
]

PROCS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
TIPOS = [
    "SERV",
    "SUM",
    "OBR",
    "PRIV",
    "PATR",
    "ESP",
    "COL",
    "CSER",
    "GEST",
    "CONS",
    "MIX",
    "ACON",
    "CA",
    "OBRA",
    "ADMESP",
    "ARRED",
    "PAT",
    "GESSERVPUB",
    "CONOBRPUB",
    "COLABPUBPR",
    "CONSERV",
]
ESTADOS = ["RES", "PUB", "ADJ", "EVA", "ANU", "DES", "PRE", "FOR", "REN", "PEN", "CER", "REV", "ABD", "PAA"]
TRAMS = ["O", "U", "E", "S", "N"]
PROVS = ["04", "11", "14", "18", "21", "23", "29", "41", "51", "52", "98"]
FPS = ["E", "P", "M", "N", "S", "O"]
YEARS = ["2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]

SORT_COMBOS = [
    [{"idExpediente": "asc"}],
    [{"idExpediente": "desc"}],
    [{"importeLicitacion": "asc"}],
    [{"importeLicitacion": "desc"}],
    [{"numeroExpediente": "asc"}],
    [{"numeroExpediente": "desc"}],
    [{"titulo": "asc"}],
    [{"titulo": "desc"}],
    [{"fechaLimitePresentacion": "asc"}],
    [{"fechaLimitePresentacion": "desc"}],
    [{"adjudicaciones.importeAdjudicacion": "asc"}],
    [{"adjudicaciones.importeAdjudicacion": "desc"}],
]

DIMS = [
    ("tipoContrato.codigo", TIPOS),
    ("estado.codigo", ESTADOS),
    ("codigoTipoTramitacion", TRAMS),
    ("perfilContratante.codigo", "PERFILES"),
    ("provinciasEjecucion", PROVS),
    ("formaPresentacion", FPS),
    ("numeroExpediente", YEARS),
]

_PERFILES = None


def init():
    try:
        response = S.get(f"{BASE}/perfiles-licitaciones/licitaciones-publicadas", timeout=20)
        response.raise_for_status()
    except RequestException as exc:
        raise ScraperError("No se pudo inicializar la sesion con el portal de Andalucia") from exc


def build_query(must=None, must_not=None, *, size=PAGE_SIZE, sort=None, offset=None, track_total_hits=True):
    query = {"query": {"bool": {}}, "size": size, "track_total_hits": track_total_hits}
    if must:
        query["query"]["bool"]["must"] = must
    if must_not:
        query["query"]["bool"]["must_not"] = must_not
    if sort is not None:
        query["sort"] = sort
    if offset is not None:
        query["from"] = offset
    return query


def es(body, timeout=DEFAULT_TIMEOUT):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = S.post(ES_URL, json=body, timeout=timeout)
            if response.ok:
                try:
                    return response.json()
                except ValueError as exc:
                    last_error = exc
                    log.warning("Respuesta JSON invalida en intento %s/%s", attempt, MAX_RETRIES)
            elif response.status_code in RETRYABLE_STATUS_CODES:
                last_error = ScraperError(
                    f"HTTP {response.status_code} consultando Andalucia API"
                )
                log.warning(
                    "Respuesta reintentable HTTP %s en intento %s/%s",
                    response.status_code,
                    attempt,
                    MAX_RETRIES,
                )
            else:
                body_preview = response.text[:200].replace("\n", " ").strip()
                raise ScraperError(
                    f"HTTP no reintentable {response.status_code} consultando Andalucia API: {body_preview}"
                )
        except RequestException as exc:
            last_error = exc
            log.warning("Fallo de red en intento %s/%s: %s", attempt, MAX_RETRIES, exc)

        if attempt < MAX_RETRIES:
            time.sleep(2)

    raise ScraperError("La API de Andalucia fallo tras varios reintentos") from last_error


def cnt(must=None, must_not=None):
    data = es(build_query(must=must, must_not=must_not, size=0), timeout=COUNT_TIMEOUT)
    total = data.get("hits", {}).get("total", {})
    return total.get("value", 0) if isinstance(total, dict) else total


def mm(field, value):
    return {"match": {field: value}}


def mn(field, value):
    if isinstance(value, str):
        return {"match": {field: {"query": value}}}
    return {"match": {field: value}}


def get_perfiles():
    global _PERFILES
    if _PERFILES:
        return _PERFILES

    if PERFILES_CACHE_PATH.exists():
        try:
            cached = json.loads(PERFILES_CACHE_PATH.read_text(encoding="utf-8"))
            if isinstance(cached, list) and cached:
                _PERFILES = sorted(str(value) for value in cached if value)
                log.info("Loaded %s perfil codes from cache", len(_PERFILES))
                return _PERFILES
        except (OSError, ValueError) as exc:
            log.warning("No se pudo leer la cache de perfiles: %s", exc)

    log.info("Discovering perfil codes...")
    perfiles = set()
    discovery_sorts = [
        ("idExpediente", "asc"),
        ("idExpediente", "desc"),
        ("fechaPublicacion", "asc"),
        ("fechaPublicacion", "desc"),
    ]

    for sort_field, sort_order in discovery_sorts:
        for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
            data = es(
                build_query(
                    size=PAGE_SIZE,
                    sort=[{sort_field: sort_order}],
                    offset=offset,
                )
            )
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            for hit in hits:
                perfil = hit.get("_source", {}).get("perfilContratante", {})
                if isinstance(perfil, dict) and perfil.get("codigo"):
                    perfiles.add(perfil["codigo"])
            time.sleep(0.1)
        log.info("  %s %s: %s codes", sort_field, sort_order, len(perfiles))

    _PERFILES = sorted(perfiles)
    try:
        PERFILES_CACHE_PATH.write_text(
            json.dumps(_PERFILES, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        log.warning("No se pudo guardar la cache de perfiles: %s", exc)
    log.info("  Total: %s perfil codes", len(_PERFILES))
    return _PERFILES


def build_unknown_standard_exclusions(base_must_not):
    exclusions = list(base_must_not)
    for proc in PROCS:
        if proc == 9:
            continue
        exclusions.append(mn("codigoProcedimiento", proc))
    return exclusions


def extract(data):
    return [flatten(hit.get("_source", {})) for hit in data.get("hits", {}).get("hits", [])]


def flatten(source):
    row = {column: INTEGER_DEFAULTS.get(column, "") for column in CSV_COLS}
    row["id_expediente"] = source.get("idExpediente", "")
    row["numero_expediente"] = source.get("numeroExpediente", "")
    row["titulo"] = source.get("titulo", "")

    tipo_contrato = source.get("tipoContrato") or {}
    if isinstance(tipo_contrato, dict):
        row["tipo_contrato"] = tipo_contrato.get("descripcion", "")
        row["tipo_contrato_codigo"] = tipo_contrato.get("codigo", "")
    else:
        row["tipo_contrato"] = str(tipo_contrato or "")

    perfil = source.get("perfilContratante") or {}
    if isinstance(perfil, dict):
        row["organo_contratacion"] = perfil.get("descripcion", "")
        row["codigo_perfil"] = perfil.get("codigo", "")
        row["codigo_dir3"] = perfil.get("codigoDir3", "")

    estado = source.get("estado") or {}
    if isinstance(estado, dict):
        row["estado"] = estado.get("nombre", "")
        row["estado_codigo"] = estado.get("codigo", "")

    row["importe_licitacion"] = source.get("importeLicitacion", "")
    row["valor_estimado"] = source.get("valorEstimado", "")
    row["fecha_publicacion"] = _dt(source.get("fechaPublicacion"))
    row["fecha_limite_presentacion"] = _dt(source.get("fechaLimitePresentacion"))
    row["codigo_procedimiento"] = source.get("codigoProcedimiento", "")
    row["codigo_tramitacion"] = source.get("codigoTipoTramitacion", "")
    row["codigo_normativa"] = source.get("codigoNormativa", "")
    row["forma_presentacion"] = source.get("formaPresentacion", "")
    row["cofinanciado_ue"] = source.get("cofinanciadoUE", "")
    row["subasta_electronica"] = source.get("subastaElectronica", "")
    row["sistema_racionalizacion"] = source.get("sistemaRacionalizacion", "")

    cpvs = source.get("codigosCpv") or []
    if isinstance(cpvs, list):
        row["cpv"] = ";".join(str(code) for code in cpvs)

    provincias = source.get("provinciasEjecucion") or []
    if isinstance(provincias, list):
        row["provincias_ejecucion"] = ";".join(str(provincia) for provincia in provincias)

    adjudicaciones = source.get("adjudicaciones") or []
    if isinstance(adjudicaciones, list) and adjudicaciones:
        first_award = adjudicaciones[0] if isinstance(adjudicaciones[0], dict) else {}
        nif = first_award.get("nifAdjudicatario") or ""
        row["adjudicatario_nif"] = nif.rstrip(";") if isinstance(nif, str) else str(nif)
        row["importe_adjudicacion"] = first_award.get("importeAdjudicacion", "")
        row["importe_adjudicacion_iva"] = first_award.get("importeAdjudicacionConIva", "")
        row["num_adjudicaciones"] = len(adjudicaciones)
        if len(adjudicaciones) > 1:
            row["todos_adjudicatarios_nif"] = ";".join(
                (award.get("nifAdjudicatario") or "").rstrip(";")
                for award in adjudicaciones
                if isinstance(award, dict)
            )

    anuncios = source.get("anuncios") or []
    if isinstance(anuncios, list) and anuncios:
        fechas = [
            anuncio.get("fechaPublicacion")
            for anuncio in anuncios
            if isinstance(anuncio, dict) and anuncio.get("fechaPublicacion")
        ]
        if fechas:
            row["anuncio_primera_fecha"] = _dt(min(fechas))
            row["anuncio_ultima_fecha"] = _dt(max(fechas))
        row["num_anuncios"] = len(anuncios)

    medios = source.get("mediosPublicacion") or []
    if isinstance(medios, list):
        row["medios_publicacion"] = ";".join(
            medio.get("codigo", "") for medio in medios if isinstance(medio, dict)
        )

    row["num_lotes"] = len(source.get("lotes") or [])
    row["url_detalle"] = (
        f"{BASE}/perfiles-licitaciones/detalle-licitacion?idExpediente={row['id_expediente']}"
    )
    return row


def _dt(value):
    if not value:
        return ""
    match = re.match(r"(\d{4}-\d{2}-\d{2})", str(value))
    return match.group(1) if match else str(value)[:10]


def paginate(must=None, must_not=None, sort=None, label=""):
    del label
    if sort is None:
        sort = [{"idExpediente": "asc"}]

    records = []
    seen = set()
    total_count = None

    for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
        data = es(build_query(must=must, must_not=must_not, sort=sort, offset=offset))
        if total_count is None:
            total = data.get("hits", {}).get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total

        batch = extract(data)
        if not batch:
            break

        for record in batch:
            expediente_id = record["id_expediente"]
            if expediente_id not in seen:
                seen.add(expediente_id)
                records.append(record)

        if len(records) >= total_count:
            break
        time.sleep(DELAY)

    return records, total_count or 0


def paginate_multisort(must=None, must_not=None, label="", target=None):
    all_records = []
    seen = set()
    if target is None:
        target = cnt(must=must, must_not=must_not)

    for sort_index, sort in enumerate(SORT_COMBOS, start=1):
        sort_name = f"{list(sort[0].keys())[0]}:{list(sort[0].values())[0]}"
        new_this_sort = 0

        for offset in range(0, MAX_FROM + PAGE_SIZE, PAGE_SIZE):
            data = es(build_query(must=must, must_not=must_not, sort=sort, offset=offset))
            batch = extract(data)
            if not batch:
                break

            for record in batch:
                expediente_id = record["id_expediente"]
                if expediente_id not in seen:
                    seen.add(expediente_id)
                    all_records.append(record)
                    new_this_sort += 1

            # No cortamos al ver una pagina 100% duplicada: una ventana posterior
            # puede seguir aportando ids unicos dentro del limite de 10k.
            time.sleep(DELAY)

        pct = len(all_records) / target * 100 if target else 0
        log.info(
            "    %s sort %s/%s (%s): +%s -> %s/%s (%.0f%%)",
            label,
            sort_index,
            len(SORT_COMBOS),
            sort_name,
            f"{new_this_sort:,}",
            f"{len(all_records):,}",
            f"{target:,}",
            pct,
        )

        if len(all_records) >= target:
            break

    if len(all_records) < target:
        log.warning(
            "  %s: %s/%s (%.0f%%) PARTIAL",
            label,
            f"{len(all_records):,}",
            f"{target:,}",
            (len(all_records) / target * 100) if target else 0,
        )

    return all_records


def scrape_recursive(must, must_not, label, all_records, seen_ids, dim_idx=0, known_total=None):
    total = known_total if known_total is not None else cnt(must=must, must_not=must_not)
    if total == 0:
        return 0

    if total <= MAX_FROM + PAGE_SIZE:
        records, _ = paginate(must=must, must_not=must_not, label=label)
        new_records = 0
        for record in records:
            expediente_id = record["id_expediente"]
            if expediente_id not in seen_ids:
                seen_ids.add(expediente_id)
                all_records.append(record)
                new_records += 1
        return new_records

    if dim_idx < len(DIMS):
        field, values = DIMS[dim_idx]
        dim_name = field.split(".")[-1]
        if values == "PERFILES":
            values = get_perfiles()

        log.info("  %s (%s) -> %s (%s vals)", label, f"{total:,}", dim_name, len(values))
        total_new = 0

        for value in values:
            sub_must = list(must) + [mm(field, value)]
            sub_count = cnt(must=sub_must, must_not=must_not)
            if sub_count == 0:
                continue
            total_new += scrape_recursive(
                sub_must,
                must_not,
                f"{label}/{value}",
                all_records,
                seen_ids,
                dim_idx + 1,
                known_total=sub_count,
            )
            time.sleep(0.05)

        if total_new < total:
            excluded = list(must_not)
            for value in values:
                excluded.append(mn(field, value) if isinstance(value, str) else {"match": {field: value}})

            if len(excluded) < 900:
                null_count = cnt(must=must, must_not=excluded)
                if null_count > 0:
                    log.info("  %s/null_%s: %s", label, dim_name, f"{null_count:,}")
                    total_new += scrape_recursive(
                        must,
                        excluded,
                        f"{label}/null_{dim_name}",
                        all_records,
                        seen_ids,
                        dim_idx + 1,
                        known_total=null_count,
                    )

        return total_new

    log.info("  %s (%s) -> multi-sort", label, f"{total:,}")
    records = paginate_multisort(must=must, must_not=must_not, label=label, target=total)
    new_records = 0
    for record in records:
        expediente_id = record["id_expediente"]
        if expediente_id not in seen_ids:
            seen_ids.add(expediente_id)
            all_records.append(record)
            new_records += 1
    return new_records


def clean_records(records):
    cleaned = []
    for record in records:
        row = {column: record.get(column, INTEGER_DEFAULTS.get(column, "")) for column in CSV_COLS}
        for key, value in record.items():
            if key.startswith("_") or key in row:
                continue
            row[key] = value
        cleaned.append(row)
    return cleaned


def records_to_dataframe(records):
    if not HAS_PANDAS:
        raise ScraperError("pandas no esta disponible; no se puede generar DataFrame")
    cleaned = clean_records(records)
    dataframe = pd.DataFrame(cleaned)
    ordered = [column for column in CSV_COLS if column in dataframe.columns]
    extra = [column for column in dataframe.columns if column not in CSV_COLS]
    return dataframe[ordered + extra]


def save_csv(records, filename):
    if not records:
        return None

    path = DATA_DIR / filename
    if HAS_PANDAS:
        records_to_dataframe(records).to_csv(path, index=False, encoding="utf-8-sig")
    else:
        cleaned = clean_records(records)
        keys = list(cleaned[0].keys())
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(cleaned)

    log.info("Guardado CSV %s (%s)", path, f"{len(records):,}")
    return path


def save_parquet(records, filename):
    if not records:
        return None
    if not HAS_PANDAS:
        log.warning("pandas/pyarrow no disponible; se omite la salida Parquet")
        return None

    path = DATA_DIR / filename
    records_to_dataframe(records).to_parquet(path, index=False, compression="snappy")
    log.info("Guardado Parquet %s (%s)", path, f"{len(records):,}")
    return path


def scrape_std():
    init()
    base_must_not = [mn("estado.codigo", "BRR"), mn("codigoProcedimiento", 9)]
    total = cnt(must_not=base_must_not)
    print("=" * 70)
    print(f"  SCRAPE ESTANDAR: {total:,}")
    print("=" * 70)

    all_records = []
    seen = set()
    started_at = time.time()

    for proc in PROCS:
        if proc == 9:
            continue
        proc_total = cnt(must=[mm("codigoProcedimiento", proc)], must_not=base_must_not)
        if proc_total == 0:
            continue

        log.info("\n%s\n  proc=%s: %s", "-" * 60, proc, f"{proc_total:,}")
        scrape_recursive(
            [mm("codigoProcedimiento", proc)],
            base_must_not,
            f"p{proc}",
            all_records,
            seen,
            0,
            known_total=proc_total,
        )

        elapsed = time.time() - started_at
        rate = len(all_records) / elapsed if elapsed else 0
        eta = (total - len(all_records)) / rate / 60 if rate > 0 else 0
        pct = len(all_records) / total * 100 if total else 0
        log.info("  %s/%s (%.1f%%) %.0f/s ETA=%.1fm", f"{len(all_records):,}", f"{total:,}", pct, rate, eta)
        save_csv(all_records, "licitaciones_std_progress.csv")

    unknown_proc_exclusions = build_unknown_standard_exclusions(base_must_not)
    unknown_proc_total = cnt(must_not=unknown_proc_exclusions)
    if unknown_proc_total > 0:
        log.info("\n%s\n  proc=unknown/null: %s", "-" * 60, f"{unknown_proc_total:,}")
        scrape_recursive(
            [],
            unknown_proc_exclusions,
            "p_unknown",
            all_records,
            seen,
            0,
            known_total=unknown_proc_total,
        )
        elapsed = time.time() - started_at
        rate = len(all_records) / elapsed if elapsed else 0
        eta = (total - len(all_records)) / rate / 60 if rate > 0 else 0
        pct = len(all_records) / total * 100 if total else 0
        log.info("  %s/%s (%.1f%%) %.0f/s ETA=%.1fm", f"{len(all_records):,}", f"{total:,}", pct, rate, eta)
        save_csv(all_records, "licitaciones_std_progress.csv")

    save_csv(all_records, "licitaciones_std.csv")
    log.info("  STD: %s/%s in %.1fm", f"{len(all_records):,}", f"{total:,}", (time.time() - started_at) / 60)
    return all_records


def scrape_menores():
    init()
    base_must_not = [mn("estado.codigo", "BRR")]
    total = cnt(must=[mm("codigoProcedimiento", 9)], must_not=base_must_not)
    print("=" * 70)
    print(f"  SCRAPE MENORES: {total:,}")
    print("=" * 70)

    all_records = []
    seen = set()
    started_at = time.time()
    scrape_recursive(
        [mm("codigoProcedimiento", 9)],
        base_must_not,
        "men",
        all_records,
        seen,
        0,
        known_total=total,
    )

    save_csv(all_records, "licitaciones_menores.csv")
    log.info(
        "  MENORES: %s/%s in %.1fm",
        f"{len(all_records):,}",
        f"{total:,}",
        (time.time() - started_at) / 60,
    )
    return all_records


def scrape_all():
    standard = scrape_std()
    menores = scrape_menores()

    seen = set()
    deduped = []
    for record in standard + menores:
        expediente_id = record["id_expediente"]
        if expediente_id in seen:
            continue
        seen.add(expediente_id)
        deduped.append(record)

    save_csv(deduped, "licitaciones_all.csv")
    save_parquet(deduped, "licitaciones_andalucia.parquet")
    log.info("  ALL: %s", f"{len(deduped):,}")
    return deduped


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print(
            f"""
  python {Path(__file__).name} scrape-std    Licitaciones regulares
  python {Path(__file__).name} scrape-men    Contratos menores
  python {Path(__file__).name} scrape        Dataset completo + Parquet
"""
        )
        return 0

    command = args[0].lower()
    if command == "scrape-std":
        scrape_std()
    elif command == "scrape-men":
        scrape_menores()
    elif command == "scrape":
        scrape_all()
    else:
        print(f"Comando desconocido: {command}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
