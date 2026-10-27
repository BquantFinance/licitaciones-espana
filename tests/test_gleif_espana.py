"""Tests offline de gleif/gleif_espana.py.

La API de la Golden Copy de GLEIF (metadatos de la publicación y los tres ZIP
completos) se simula con un ``requests.get`` falso. Los CSV tienen el formato
real (cabecera con los nombres XPath, todos los campos entre comillas, coma,
UTF-8) con las columnas que usa el script y alguna más; los LEI y los NIF son
inventados.
"""

import csv
import importlib.util
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "gleif" / "gleif_espana.py"


def _cargar():
    spec = importlib.util.spec_from_file_location("gleif_espana", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()

CAB_LEI = ["LEI", "Entity.LegalName", "Entity.LegalAddress.City", "Entity.LegalAddress.Country",
           "Entity.HeadquartersAddress.Country", "Entity.RegistrationAuthority.RegistrationAuthorityID",
           "Entity.RegistrationAuthority.RegistrationAuthorityEntityID", "Entity.LegalJurisdiction",
           "Entity.EntityCategory", "Entity.EntityStatus", "Registration.RegistrationStatus"]
CAB_RR = ["Relationship.StartNode.NodeID", "Relationship.StartNode.NodeIDType", "Relationship.EndNode.NodeID",
          "Relationship.EndNode.NodeIDType", "Relationship.RelationshipType", "Relationship.RelationshipStatus",
          "Registration.RegistrationStatus"]
CAB_REPEX = ["LEI", "Exception.Category", "Exception.Reason.1"]

# A y B españolas del mismo grupo (B -> A -> P, matriz francesa); C española con jurisdicción ES-MD
# y domicilio fuera; U alemana sin relación con España; S española empresario individual; D española
# con un DNI como identificador; Q, matriz de P (relación entre dos extranjeras).
LEI = {
    "A": ["A0000000000000000001", "EMPRESA A, S.A.", "Madrid", "ES", "ES", "RA000780", "A00000001", "ES", "GENERAL",
          "ACTIVE", "ISSUED"],
    "B": ["B0000000000000000002", "EMPRESA B, S.L.", "Sevilla", "ES", "ES", "RA000533", "1000012345678", "ES",
          "GENERAL", "ACTIVE", "ISSUED"],
    "C": ["C0000000000000000003", "SOCIEDAD, C S.A.", "Lisboa", "PT", "PT", "RA000999", "PT123", "ES-MD", "GENERAL",
          "ACTIVE", "LAPSED"],
    "P": ["P0000000000000000004", "PARENT SAS", "Paris", "FR", "FR", "RA000189", "552100554", "FR", "GENERAL",
          "ACTIVE", "ISSUED"],
    "Q": ["Q0000000000000000005", "GRANDPARENT AG", "Zürich", "CH", "CH", "RA000548", "CHE-1", "CH", "GENERAL",
          "ACTIVE", "ISSUED"],
    "U": ["U0000000000000000006", "UNRELATED GMBH", "Berlin", "DE", "DE", "RA000197", "HRB 1", "DE", "GENERAL",
          "ACTIVE", "ISSUED"],
    "S": ["S0000000000000000007", "NOMBRE APELLIDO APELLIDO", "Bilbao", "ES", "ES", "RA000780", "B00000099", "ES",
          "SOLE_PROPRIETOR", "ACTIVE", "ISSUED"],
    "D": ["D0000000000000000008", "OTRO NOMBRE APELLIDO", "Vigo", "ES", "ES", "RA000780", "87654321-X", "ES", "",
          "ACTIVE", "ISSUED"],
}
L = {k: v[0] for k, v in LEI.items()}
RR = [
    [L["A"], "LEI", L["P"], "LEI", "IS_ULTIMATELY_CONSOLIDATED_BY", "ACTIVE", "PUBLISHED"],
    [L["B"], "LEI", L["A"], "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "ACTIVE", "PUBLISHED"],
    [L["P"], "LEI", L["Q"], "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "ACTIVE", "PUBLISHED"],
    [L["S"], "LEI", L["P"], "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "ACTIVE", "PUBLISHED"],
    [L["U"], "LEI", L["Q"], "LEI", "IS_ULTIMATELY_CONSOLIDATED_BY", "ACTIVE", "PUBLISHED"],
]
REPEX = [
    [L["A"], "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NON_PUBLIC"],
    [L["C"], "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT", "NATURAL_PERSONS"],
    [L["U"], "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT", "NON_CONSOLIDATING"],
    [L["S"], "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT", "NATURAL_PERSONS"],
]


def zip_csv(nombre, cabecera, filas):
    texto = io.StringIO()
    w = csv.writer(texto, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow(cabecera)
    w.writerows(filas)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(nombre, texto.getvalue().encode("utf-8"))
    return buf.getvalue()


class Respuesta:
    def __init__(self, codigo=200, cuerpo=b"", json_=None):
        self.status_code = codigo
        self._cuerpo = cuerpo
        self._json = json_
        self.headers = {}

    def json(self):
        if self._json is None:
            raise ValueError("no es JSON")
        return self._json

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._cuerpo), chunk_size):
            yield self._cuerpo[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Portal:
    """GLEIF falso: metadatos de la publicación y los tres ZIP."""

    def __init__(self, lei=None, rr=None, repex=None, fecha="2026-10-26 08:00:00"):
        lei = list(LEI.values()) if lei is None else lei
        self.zips = {
            "lei2": zip_csv("20261026-0800-gleif-goldencopy-lei2-golden-copy.csv", CAB_LEI, lei),
            "rr": zip_csv("20261026-0800-gleif-goldencopy-rr-golden-copy.csv", CAB_RR, RR if rr is None else rr),
            "repex": zip_csv("20261026-0800-gleif-goldencopy-repex-golden-copy.csv", CAB_REPEX,
                             REPEX if repex is None else repex),
        }
        self.registros = {"lei2": len(lei), "rr": len(RR if rr is None else rr), "repex": len(REPEX if repex is None else repex)}
        self.fecha = fecha
        self.bytes_api = {}
        self.cuerpo = {}
        self.peticiones = []

    def url(self, f):
        return f"https://goldencopy.gleif.org/storage/golden-copy-files/2026/10/26/1/20261026-0800-{f}-golden-copy.csv.zip"

    def metadatos(self):
        datos = {"publish_date": self.fecha}
        for f, z in self.zips.items():
            datos[f] = {"full_file": {"csv": {"url": self.url(f), "size": self.bytes_api.get(f, len(z)),
                                              "record_count": self.registros[f]}}}
        return {"data": datos}

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.peticiones.append(url)
        assert "licitaciones-espana" in headers["User-Agent"]
        if url == M.URL_PUBLICACION:
            return Respuesta(json_=self.metadatos())
        for f in self.zips:
            if url == self.url(f):
                return Respuesta(cuerpo=self.cuerpo.get(f, self.zips[f]))
        return Respuesta(404)


@pytest.fixture
def portal(monkeypatch):
    p = Portal()
    monkeypatch.setattr(requests, "get", p.get)
    monkeypatch.setattr("time.sleep", lambda s: None)
    return p


def leer(salida, nombre):
    return pd.read_parquet(salida / f"{nombre}.parquet")


def test_ambito_espanolas_enlazadas_y_sin_personas(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    ent = leer(tmp_path, "entidades")
    assert set(ent["LEI"]) == {L["A"], L["B"], L["C"], L["P"]}       # ni U, ni Q, ni S, ni D
    rel = leer(tmp_path, "relaciones")
    pares = set(zip(rel["Relationship.StartNode.NodeID"], rel["Relationship.EndNode.NodeID"]))
    assert pares == {(L["A"], L["P"]), (L["B"], L["A"])}             # ni P->Q, ni S->P, ni U->Q
    exc = leer(tmp_path, "excepciones")
    assert set(exc["LEI"]) == {L["A"], L["C"]}
    assert ent["_en_ultima_descarga"].all()
    assert set(ent["_publicacion"]) == {"2026-10-26 08:00:00"}
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert man["espanolas"] == 3 and man["enlazadas_fuera"] == 1 and man["personas_fisicas_quitadas"] == 2
    assert man["ficheros"]["lei2"]["filas_conservadas"] == 4
    assert len(man["ficheros"]["lei2"]["sha256"]) == 64
    # Los ZIP originales (con los empresarios individuales) no se quedan
    assert not list((tmp_path / ".descargas").glob("*.zip"))
    # Ni el nombre ni el DNI de las personas físicas aparecen en ningún fichero escrito
    for p in tmp_path.rglob("*"):
        if p.is_file() and p.suffix in (".gz", ".json", ".txt"):
            datos = p.read_bytes()
            if p.suffix == ".gz":
                import gzip
                datos = gzip.decompress(datos)
            assert b"NOMBRE APELLIDO" not in datos and b"87654321" not in datos and L["S"].encode() not in datos


def test_crudo_tal_cual_y_cabecera_completa(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    crudo = pd.read_csv(tmp_path / "raw" / "lei2_es.csv.gz", dtype=str, keep_default_na=False)
    assert list(crudo.columns) == CAB_LEI
    fila = crudo[crudo["LEI"] == L["B"]].iloc[0].tolist()
    assert fila == LEI["B"]
    ent = leer(tmp_path, "entidades")
    # vacío en origen = nulo en el Parquet; todo lo demás, texto
    assert ent.loc[ent["LEI"] == L["A"], "Entity.RegistrationAuthority.RegistrationAuthorityEntityID"].iloc[0] == "A00000001"


def test_segunda_ejecucion_sin_cambios(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = {p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv.gz")}
    assert M.main(["--salida", str(tmp_path)]) == 0
    despues = {p.name: p.read_bytes() for p in (tmp_path / "raw").glob("*.csv.gz")}
    assert antes == despues
    assert not (tmp_path / "raw" / "_historico").exists()
    assert len(leer(tmp_path, "entidades")) == 4
    assert "sin_cambios" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_entidad_que_sale_del_ambito_se_conserva(portal, tmp_path, monkeypatch):
    assert M.main(["--salida", str(tmp_path)]) == 0
    lei = [v for k, v in LEI.items() if k != "C"]
    p2 = Portal(lei=lei, fecha="2026-10-26 16:00:00")
    monkeypatch.setattr(requests, "get", p2.get)
    assert M.main(["--salida", str(tmp_path)]) == 0
    ent = leer(tmp_path, "entidades")
    c = ent[ent["LEI"] == L["C"]]
    assert len(c) == 1 and not c["_en_ultima_descarga"].iloc[0]
    assert ent[ent["LEI"] == L["A"]]["_en_ultima_descarga"].all()
    assert len(list((tmp_path / "raw" / "_historico").glob("lei2_es.csv__*.gz"))) == 1


def test_bytes_distintos_de_la_api_es_error_y_no_toca_nada(portal, tmp_path):
    portal.bytes_api["rr"] = 1
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert not (tmp_path / "raw" / "lei2_es.csv.gz").exists()
    assert not list((tmp_path / ".descargas").glob("*.zip"))
    assert "ERROR" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_recuento_distinto_es_error(portal, tmp_path):
    portal.registros["lei2"] += 1
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert not (tmp_path / "raw" / "lei2_es.csv.gz").exists()
    assert not list((tmp_path / "raw").glob(".*.nuevo"))


def test_cero_espanolas_es_error_y_conserva_lo_anterior(portal, tmp_path, monkeypatch):
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = (tmp_path / "raw" / "lei2_es.csv.gz").read_bytes()
    p2 = Portal(lei=[LEI["U"], LEI["Q"]], rr=[], repex=[])
    monkeypatch.setattr(requests, "get", p2.get)
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert (tmp_path / "raw" / "lei2_es.csv.gz").read_bytes() == antes
    assert leer(tmp_path, "entidades")["_en_ultima_descarga"].all()


def test_html_en_vez_de_zip(portal, tmp_path):
    portal.cuerpo["repex"] = b"<html><body>mantenimiento</body></html>"
    portal.bytes_api["repex"] = len(portal.cuerpo["repex"])
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert "invalido" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert not list((tmp_path / ".descargas").glob("*.zip"))


def test_fallo_de_la_api_de_metadatos(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **k: Respuesta(403))
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert not (tmp_path / "entidades.parquet").exists()


def test_conservar_zip(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path), "--conservar-zip"]) == 0
    assert len(list((tmp_path / ".descargas").glob("*.zip"))) == 3


def test_solo_parquet_sin_red(portal, tmp_path, monkeypatch):
    assert M.main(["--salida", str(tmp_path)]) == 0
    (tmp_path / "entidades.parquet").unlink()
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no debe pedir nada"))
    assert M.main(["--salida", str(tmp_path), "--solo-parquet"]) == 0
    assert len(leer(tmp_path, "entidades")) == 4


@pytest.mark.parametrize("ident, espanola, esperado", [
    ("12345678Z", True, True), ("12345678-z", True, True), ("X1234567L", True, True), ("K1234567A", True, True),
    ("B12345678", True, False), ("1000012345678", True, False), ("12345678Z", False, False), ("", True, False)])
def test_persona_fisica_por_identificador(ident, espanola, esperado):
    fila = {M.C_REGISTRO_ID: ident, M.C_CATEGORIA: "GENERAL"}
    assert M.es_persona_fisica(fila, espanola) is esperado


def test_empresario_individual_de_cualquier_pais():
    assert M.es_persona_fisica({M.C_CATEGORIA: "SOLE_PROPRIETOR", M.C_REGISTRO_ID: "HRB 1"}, False)


@pytest.mark.parametrize("legal, sede, jur, esperado", [
    ("ES", "FR", "FR", True), ("FR", "ES", "FR", True), ("PT", "PT", "ES-MD", True), ("PT", "PT", "ES", True),
    ("FR", "FR", "ESX", False), ("FR", "FR", "", False)])
def test_es_espanola(legal, sede, jur, esperado):
    assert M.es_espanola({M.C_PAIS_LEGAL: legal, M.C_PAIS_SEDE: sede, M.C_JURISDICCION: jur}) is esperado
