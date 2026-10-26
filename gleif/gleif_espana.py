#!/usr/bin/env python3
"""
=============================================================================
GLEIF - ENTIDADES ESPAÑOLAS CON LEI, SUS MATRICES Y SUS FILIALES
=============================================================================
Descarga la Golden Copy de la Global LEI Foundation (GLEIF) y guarda la parte
española: cada entidad con LEI (Legal Entity Identifier, ISO 17442) con
domicilio legal, sede o jurisdicción en España, las entidades de cualquier país
enlazadas con ellas por una relación de «nivel 2» (quién consolida a quién:
matriz directa y matriz última, sucursales internacionales, fondos y sus
gestoras) y las excepciones que declara cada entidad española cuando no
informa de su matriz (no consolida, la controlan personas físicas, no se
conoce, no es pública…). Es el único dato abierto y gratuito de grupos
empresariales con matriz declarada: sirve para saber qué adjudicatarios son
del mismo grupo.

Ejecutar:  python gleif/gleif_espana.py [--salida DIR] [--solo-parquet] [--conservar-zip]

Salida (por defecto <repo>/gleif_espana/):
    raw/lei2_es.csv.gz            filas de la Golden Copy LEI-CDF (las 338 columnas, tal cual) de las
                                  entidades del ámbito
    raw/rr_es.csv.gz              filas de la Golden Copy RR-CDF (relaciones) con un extremo en el ámbito
    raw/repex_es.csv.gz           filas de las Reporting Exceptions de las entidades españolas
    raw/_historico/               versiones anteriores de cada fichero (nunca se borran)
    raw/_manifiesto.json          publicación usada, URL, bytes y sha256 de cada ZIP original, filas
                                  leídas y conservadas, empresarios individuales quitados
    raw/descarga_log.txt          resumen de cada ejecución (se añade al final)
    entidades.parquet, relaciones.parquet, excepciones.parquet  (+ _historico/)
                                  registros acumulados (comun/historico.py: acumular), como texto

Ámbito:
- Entidad española: Entity.LegalAddress.Country = 'ES', o
  Entity.HeadquartersAddress.Country = 'ES', o Entity.LegalJurisdiction 'ES' o
  'ES-..'.
- Enlazada: cualquier entidad que sea el otro extremo de una relación con una
  española (una matriz extranjera, una filial en el extranjero, la gestora de un
  fondo).
- Fuera, siempre: los empresarios individuales (personas físicas). Se quitan
  las entidades con Entity.EntityCategory = 'SOLE_PROPRIETOR' y las españolas
  cuyo identificador de registro (RegistrationAuthorityEntityID) tiene forma de
  DNI o NIE; y las relaciones y excepciones que las citan. Sus filas no se
  escriben en ningún sitio. Por eso los ZIP originales (que las traen, de todo
  el mundo) se borran al terminar salvo con --conservar-zip; su sha256 queda en
  el manifiesto.

Sesgo del superviviente (docs/PRINCIPIOS.md; comun/historico.py):
- Capa cruda: cada CSV del ámbito pasa por guardar_version (gzip sin fecha, así
  que el mismo contenido da los mismos bytes): si no cambia no se toca y si
  cambia la copia anterior va a _historico/.
- Parquet: acumular() contra el Parquet anterior. GLEIF no borra un LEI (cambia
  su estado: LAPSED, RETIRED, ANNULLED…), pero una entidad puede salir del
  ámbito (se muda) y una relación puede dejar de publicarse: esas filas siguen
  con _en_ultima_descarga=False. Una fila que cambia (la fecha de última
  actualización cambia a menudo) aparece como la versión antigua y la nueva.

Qué se pide en cada ejecución: los metadatos de la última publicación (1
petición) y los tres ZIP completos (3; 483, 23 y 60 MB el 2026-10-26). Sale con
1 si algo no se pudo descargar o leer, sin tocar los ficheros anteriores.

FUENTES
-------
Verificado en vivo el 2026-10-26 (confianza A):
  - API de la Golden Copy: https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest
    -> {"data": {"publish_date": "2026-10-26 08:00:00", "lei2": {"full_file": {"csv":
    {"url", "size", "record_count"...}}, "delta_files": {...}}, "rr": {...}, "repex": {...}}}.
    Tres publicaciones al día (00:00, 08:00 y 16:00 UTC).
  - Ficheros completos de esa publicación: lei2 3.473.002 entidades (ZIP de 482,8 MB, CSV de
    5,05 GB y 338 columnas), rr 491.767 relaciones (23,4 MB) y repex 6.439.162 excepciones (59,5 MB).
    CSV UTF-8, coma, todos los campos entre comillas, cabecera con los nombres XPath del formato
    («Entity.LegalName», «Relationship.StartNode.NodeID»...).
  - España: 195.434 entidades (105.861 con el registro emitido, ISSUED); 159.746 inscritas por el
    Registro Mercantil (RA000533, con su número de inscripción) y 10.415 con el NIF como
    identificador (RA000780); 24.091 traen un NIF de persona jurídica en
    RegistrationAuthorityEntityID. Relaciones con un extremo español: 22.607 (8.400 de matriz
    última y 8.207 de matriz directa, activas). 131.474 empresarios individuales en el mundo.
  - Licencia: CC0 1.0 (https://www.gleif.org/en/meta/lei-data-terms-of-use).
=============================================================================
"""

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import _sha256, acumular, guardar_registros, guardar_version, leer_registros  # noqa: E402
from comun import red  # noqa: E402

URL_PUBLICACION = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"
FICHEROS = ("lei2", "rr", "repex")          # LEI-CDF, RR-CDF y Reporting Exceptions
PAIS = "ES"
SALIDA = Path(__file__).resolve().parent.parent / "gleif_espana"
TITULO = "GLEIF - ENTIDADES ESPAÑOLAS"
csv.field_size_limit(1 << 27)

# Columnas de la Golden Copy que se usan (el resto se guarda tal cual)
C_LEI = "LEI"
C_PAIS_LEGAL = "Entity.LegalAddress.Country"
C_PAIS_SEDE = "Entity.HeadquartersAddress.Country"
C_JURISDICCION = "Entity.LegalJurisdiction"
C_CATEGORIA = "Entity.EntityCategory"
C_REGISTRO_ID = "Entity.RegistrationAuthority.RegistrationAuthorityEntityID"
C_INICIO = "Relationship.StartNode.NodeID"
C_FIN = "Relationship.EndNode.NodeID"
C_REPEX_LEI = "LEI"
EMPRESARIO_INDIVIDUAL = "SOLE_PROPRIETOR"
# DNI (8 cifras y letra) o NIE (X, Y, Z) y NIF de personas físicas (K, L, M), sin separadores
PATRON_PERSONA = re.compile(r"^(?:\d{8}[A-Z]|[XYZKLM]\d{7}[A-Z])$")
# Columnas que añade el script (no cuentan al comparar versiones)
METADATOS_ORIGEN = ("_publicacion", "_fecha_descarga")


def ahora():
    return datetime.now(timezone.utc)


def es_espanola(fila):
    """La entidad tiene domicilio legal, sede o jurisdicción en España."""
    jur = (fila.get(C_JURISDICCION) or "").upper()
    return PAIS in ((fila.get(C_PAIS_LEGAL) or "").upper(), (fila.get(C_PAIS_SEDE) or "").upper()) \
        or jur == PAIS or jur.startswith(PAIS + "-")


def es_persona_fisica(fila, espanola):
    """Empresario individual: categoría SOLE_PROPRIETOR o, en España, identificador con forma de DNI/NIE."""
    if (fila.get(C_CATEGORIA) or "").upper() == EMPRESARIO_INDIVIDUAL:
        return True
    ident = re.sub(r"[^0-9A-Za-z]", "", fila.get(C_REGISTRO_ID) or "").upper()
    return bool(espanola and PATRON_PERSONA.match(ident))


def publicacion_vigente():
    """Fecha de la última publicación y URL, bytes y registros de cada fichero completo (CSV)."""
    datos = red.pedir_json(URL_PUBLICACION)["data"]
    out = {"publicacion": datos["publish_date"], "ficheros": {}}
    for f in FICHEROS:
        csv_ = datos[f]["full_file"]["csv"]
        out["ficheros"][f] = {"url": csv_["url"], "bytes": int(csv_["size"]), "registros": int(csv_["record_count"])}
    return out


def validar_zip(ruta, tipo):
    """Motivo por el que la descarga no es el ZIP con un CSV de la Golden Copy, o None."""
    if not zipfile.is_zipfile(ruta):
        return "no es un ZIP"
    with zipfile.ZipFile(ruta) as z:
        nombres = [n for n in z.namelist() if n.lower().endswith(".csv")]
    return None if len(nombres) == 1 else f"se esperaba un CSV en el ZIP y hay {len(nombres)}"


def filas(ruta_zip):
    """(cabecera, iterador de dict) del CSV de un ZIP de la Golden Copy, sin descomprimirlo a disco."""
    z = zipfile.ZipFile(ruta_zip)
    nombre = next(n for n in z.namelist() if n.lower().endswith(".csv"))
    texto = io.TextIOWrapper(z.open(nombre), encoding="utf-8", newline="")
    lector = csv.reader(texto)
    cabecera = next(lector)

    def gen():
        try:
            for valores in lector:
                if valores:
                    yield dict(zip(cabecera, valores))
        finally:
            texto.close()
            z.close()
    return cabecera, gen()


def ambito(zips, resumen):
    """LEI del ámbito: españolas + enlazadas por una relación con una española, sin personas físicas.

    Recorre el LEI-CDF una vez (españolas y empresarios individuales) y las relaciones una vez.
    Devuelve (conservar, espanolas, personas): los LEI que se guardan, los españoles y los de personas
    físicas (de todo el mundo), que no se guardan."""
    espanolas, personas, quitadas = set(), set(), set()
    _, it = filas(zips["lei2"])
    leidas = 0
    for f in it:
        leidas += 1
        esp = es_espanola(f)
        if es_persona_fisica(f, esp):
            personas.add(f[C_LEI])
            if esp:
                quitadas.add(f[C_LEI])
        elif esp:
            espanolas.add(f[C_LEI])
    resumen["lei2_leidas"] = leidas
    enlazadas = set()
    _, it = filas(zips["rr"])
    for f in it:
        a, b = f.get(C_INICIO), f.get(C_FIN)
        if a in espanolas or b in espanolas:
            enlazadas.update(x for x in (a, b) if x)
    conservar = (espanolas | enlazadas) - personas
    quitadas |= enlazadas & personas
    resumen.update(espanolas=len(espanolas), enlazadas_fuera=len(conservar - espanolas),
                   personas_fisicas_quitadas=len(quitadas))
    return conservar, espanolas, personas


def extraer(ruta_zip, destino, quedarse):
    """Escribe en `destino` (CSV gzip sin fecha, cabecera tal cual) las filas para las que quedarse(fila)
    es cierto. Devuelve el número de filas escritas."""
    cabecera, it = filas(ruta_zip)
    n = 0
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "wb") as bruto, gzip.GzipFile(fileobj=bruto, mode="wb", mtime=0, filename="") as gz, \
            io.TextIOWrapper(gz, encoding="utf-8", newline="") as texto:
        w = csv.writer(texto, quoting=csv.QUOTE_ALL, lineterminator="\n")
        w.writerow(cabecera)
        for f in it:
            if quedarse(f):
                w.writerow([f.get(c, "") for c in cabecera])
                n += 1
    return n


def leer_crudo(ruta):
    """Un CSV crudo como DataFrame de texto (vacío = nulo)."""
    df = pd.read_csv(ruta, dtype=str, keep_default_na=False, compression="gzip")
    return df.replace({"": None})


class Resumen:
    def __init__(self):
        self.inicio = ahora()
        self.datos = {}
        self.errores = []

    def escribir(self, raw):
        raw.mkdir(parents=True, exist_ok=True)
        lineas = [f"=== {self.inicio.isoformat(timespec='seconds')} {TITULO}"]
        lineas += [f"  {k}: {v}" for k, v in self.datos.items()]
        lineas += [f"  ERROR: {e}" for e in self.errores]
        with open(raw / "descarga_log.txt", "a", encoding="utf-8") as fh:
            fh.write("\n".join(lineas) + "\n")
        print("\n".join(lineas))


def descargar(salida, resumen, conservar_zip=False):
    """Descarga la publicación vigente y escribe la capa cruda del ámbito. Devuelve True si todo fue bien."""
    raw = salida / "raw"
    trabajo = salida / ".descargas"
    try:
        pub = publicacion_vigente()
    except (red.ErrorPortal, KeyError, ValueError, TypeError) as e:
        resumen.errores.append(f"metadatos de la publicación: {e}")
        return False
    resumen.datos["publicacion"] = pub["publicacion"]
    manifiesto = {"publicacion": pub["publicacion"], "url_publicacion": URL_PUBLICACION,
                  "fecha_descarga": ahora().isoformat(timespec="seconds"), "ficheros": {}}
    zips = {}
    try:
        return _descargar_y_extraer(pub, raw, trabajo, zips, manifiesto, resumen)
    finally:
        if not conservar_zip:
            for z in zips.values():
                if z.exists():
                    z.unlink()
            if trabajo.is_dir() and not any(trabajo.iterdir()):
                trabajo.rmdir()


def _descargar_y_extraer(pub, raw, trabajo, zips, manifiesto, resumen):
    for f, meta in pub["ficheros"].items():
        destino = trabajo / Path(meta["url"]).name
        estado, detalle = red.descargar(meta["url"], destino, tipo="zip", validar_contenido=validar_zip)
        if estado not in ("nuevo", "actualizado", "sin_cambios"):
            resumen.errores.append(f"{f}: {estado} {detalle}")
            return False
        zips[f] = destino
        if os.path.getsize(destino) != meta["bytes"]:
            resumen.errores.append(f"{f}: {os.path.getsize(destino)} bytes y la API dice {meta['bytes']}")
            return False
        manifiesto["ficheros"][f] = {"url": meta["url"], "bytes": meta["bytes"], "sha256": _sha256(destino),
                                     "registros_api": meta["registros"]}
    tmps = {}
    try:
        conservar, espanolas, personas = ambito(zips, resumen.datos)
        if resumen.datos["lei2_leidas"] != pub["ficheros"]["lei2"]["registros"]:
            raise ValueError(f"LEI-CDF: {resumen.datos['lei2_leidas']} filas y la API dice "
                             f"{pub['ficheros']['lei2']['registros']}")
        if not espanolas:
            raise ValueError("ninguna entidad española: no se toca nada")
        nuevos = {
            "lei2": lambda f: f.get(C_LEI) in conservar,
            "rr": lambda f: (f.get(C_INICIO) in espanolas or f.get(C_FIN) in espanolas)
            and f.get(C_INICIO) not in personas and f.get(C_FIN) not in personas,
            "repex": lambda f: f.get(C_REPEX_LEI) in espanolas,
        }
        # Los tres se extraen antes de tocar raw/: un fallo a mitad no deja publicaciones mezcladas
        for f in FICHEROS:
            tmps[f] = raw / f".{f}_es.csv.gz.nuevo"
            n = extraer(zips[f], tmps[f], nuevos[f])
            manifiesto["ficheros"][f]["filas_conservadas"] = n
            resumen.datos[f"{f}_conservadas"] = n
    except (OSError, ValueError, zipfile.BadZipFile, csv.Error, KeyError) as e:
        resumen.errores.append(f"lectura de la Golden Copy: {type(e).__name__}: {e}")
        for t in tmps.values():
            if t.exists():
                t.unlink()
        return False
    for f in FICHEROS:
        resumen.datos[f"{f}_es.csv.gz"] = guardar_version(raw / f"{f}_es.csv.gz", desde=tmps[f])
    manifiesto.update({k: resumen.datos[k] for k in ("lei2_leidas", "espanolas", "enlazadas_fuera",
                                                     "personas_fisicas_quitadas")})
    raw.mkdir(parents=True, exist_ok=True)
    (raw / "_manifiesto.json").write_text(json.dumps(manifiesto, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def construir_parquet(salida, resumen):
    """Parquet acumulado de cada fichero crudo (todas las columnas como texto). Devuelve True si todo fue bien."""
    raw = salida / "raw"
    try:
        manifiesto = json.loads((raw / "_manifiesto.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        resumen.errores.append(f"sin manifiesto: {e}")
        return False
    fecha = manifiesto["fecha_descarga"]
    for f, nombre in (("lei2", "entidades"), ("rr", "relaciones"), ("repex", "excepciones")):
        try:
            nuevos = leer_crudo(raw / f"{f}_es.csv.gz")
        except (OSError, ValueError) as e:
            resumen.errores.append(f"{f}: {e}")
            return False
        nuevos["_publicacion"] = manifiesto["publicacion"]
        nuevos["_fecha_descarga"] = fecha
        destino = salida / f"{nombre}.parquet"
        out = acumular(leer_registros(destino), nuevos, fecha, ignorar=METADATOS_ORIGEN)
        resumen.datos[f"{nombre}.parquet"] = (f"{guardar_registros(out, destino)}: {len(out)} filas, "
                                              f"{int(out['_en_ultima_descarga'].sum())} en la última descarga")
    return True


def main(argv=None):
    p = argparse.ArgumentParser(description=TITULO)
    p.add_argument("--salida", type=Path, default=SALIDA)
    p.add_argument("--solo-parquet", action="store_true", help="no descarga: rehace los Parquet desde raw/")
    p.add_argument("--conservar-zip", action="store_true",
                   help="no borra los ZIP originales (traen empresarios individuales de todo el mundo)")
    a = p.parse_args(argv)
    t0 = time.time()
    resumen = Resumen()
    ok = True
    if not a.solo_parquet:
        ok = descargar(a.salida, resumen, a.conservar_zip)
    if ok:
        ok = construir_parquet(a.salida, resumen)
    resumen.datos["segundos"] = round(time.time() - t0)
    resumen.escribir(a.salida / "raw")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
