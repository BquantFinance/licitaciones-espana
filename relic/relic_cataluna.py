#!/usr/bin/env python3
"""
=============================================================================
RELIC - PROHIBICIONES DE CONTRATAR E INSCRIPCIONES DEL REGISTRO DE LICITADORES DE CATALUÑA
=============================================================================
Descarga el conjunto abierto del Registre Electrònic d'Empreses Licitadores i
Classificades de Catalunya (RELIC, Generalitat de Catalunya, Socrata
t3wj-j4pu) y guarda sus inscripciones de personas jurídicas: si la empresa
tiene una prohibición de contratar (ámbito, causa del art. 71 LCSP, fechas de
resolución, inicio y fin) y sus clasificaciones. El portal solo publica las
prohibiciones VIGENTES (las caducadas se cancelan, art. 73.2 LCSP): ejecutado
cada semana, el Parquet acumulado guarda las que el portal deja de servir.

Ejecutar:  python relic/relic_cataluna.py [--salida DIR] [--solo-parquet]

Salida (por defecto <repo>/relic_cataluna/):
    raw/relic_pj.csv.gz           filas del conjunto de las personas jurídicas (las 17 columnas, tal cual)
    raw/_historico/               versiones anteriores (nunca se borran)
    raw/_manifiesto.json          URL, fecha, bytes y sha256 de la descarga original, filas leídas y
                                  conservadas, personas físicas quitadas
    raw/descarga_log.txt          resumen de cada ejecución (se añade al final)
    inscripciones.parquet         todas las filas conservadas, acumuladas (comun/historico.py: acumular)
    prohibiciones.parquet         las filas con «Prohibició de contractar» = true, acumuladas: una
                                  prohibición que el portal retira sigue con _en_ultima_descarga=False

Personas físicas, fuera siempre: el RELIC inscribe también a empresarios
individuales con su nombre y no trae NIF ni tipo de persona. Solo se conservan
las filas cuyo nombre lleva una forma jurídica o una palabra de entidad
(S.L., S.A., SCCL, cooperativa, fundación, asociación, ayuntamiento,
consorcio, GmbH, Ltd...: FORMAS); las comunidades de bienes, que no son
personas jurídicas y suelen llevar los nombres de los comuneros, tampoco. Una
persona jurídica cuyo nombre no lleva ninguna se pierde (se cuenta en el
manifiesto); a cambio, ningún nombre de persona se escribe en ningún sitio.
La descarga original, que los trae, se borra al terminar (su sha256 queda en
el manifiesto).

Qué se pide en cada ejecución: 1 petición (el CSV completo, ~3,5 MB). El
robots.txt del portal pide Crawl-delay: 1. Sale con 1 si algo no se pudo
descargar o leer, sin tocar los ficheros anteriores.

FUENTES
-------
Verificado en vivo el 2026-10-27 (confianza A):
  - https://analisi.transparenciacatalunya.cat/api/views/t3wj-j4pu/rows.csv?accessType=DOWNLOAD
    CSV UTF-8, coma, 17 columnas en catalán («Nom empresa», «Número registral», «Data
    actualització», «Prohibició de contractar», «Àmbit de la prohibició», «Data resolució
    prohibició», «Data inici prohibició», «Causa prohibició», «Data fi prohibició», «Classificació
    empresarial», «Suspensió classificació», «Tipus classificació», «Grup classificació», «Subgrup
    classificació», «Categoria classificació», «Data atorgament classificació», «Sigles
    classificació»); una fila por empresa y clasificación; fechas dd/mm/aaaa.
  - 27.293 filas el 2026-10-27; 168 con prohibición (todas con fin en 2026 o después).
  - Ficha: https://analisi.transparenciacatalunya.cat/d/t3wj-j4pu (actualización diaria).
  - Licencia: Llicència oberta d'ús d'informació - Catalunya (DATA_LICENSE.md).
=============================================================================
"""

import argparse
import csv
import gzip
import io
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import _sha256, acumular, guardar_registros, guardar_version, leer_registros  # noqa: E402
from comun import red  # noqa: E402

URL = "https://analisi.transparenciacatalunya.cat/api/views/t3wj-j4pu/rows.csv?accessType=DOWNLOAD"
SALIDA = Path(__file__).resolve().parent.parent / "relic_cataluna"
TITULO = "RELIC - INSCRIPCIONES Y PROHIBICIONES DE CONTRATAR (CATALUÑA)"
C_NOMBRE = "Nom empresa"
C_REGISTRO = "Número registral"
C_PROHIBICION = "Prohibició de contractar"
COLUMNAS = (C_NOMBRE, C_REGISTRO, "Data actualització", C_PROHIBICION, "Àmbit de la prohibició",
            "Data resolució prohibició", "Data inici prohibició", "Causa prohibició", "Data fi prohibició",
            "Classificació empresarial", "Suspensió classificació", "Tipus classificació", "Grup classificació",
            "Subgrup classificació", "Categoria classificació", "Data atorgament classificació",
            "Sigles classificació")
METADATOS_ORIGEN = ("_fecha_descarga",)
# Formas jurídicas y palabras de entidad (sobre el nombre en mayúsculas, sin tildes y con los puntos y los espacios
# entre letras sueltas quitados: «S. L. U.» -> «SLU»). Ninguna es un nombre ni un apellido.
FORMAS = re.compile(
    r"\b(SL|SLU|SLL|SLP|SLNE|SA|SAU|SAL|SAD|SCCL|SCL|SCOOP|SCOOPV|SCP|SRL|SRLS|SPA|SAS|SARL|SASU|EURL|SNC|"
    r"GMBH|AG|KG|BV|NV|LTD|LIMITED|LLC|LLP|PLC|INC|AIE|AEIE|UTE|SGR|LDA|"
    r"SOCIETAT|SOCIEDAD|SOCIEDADE|SOCIETE|SOCIETA|COOPERATIVA|COOPERATIVE|"
    r"FUNDACIO|FUNDACION|FUNDACAO|FOUNDATION|ASSOCIACIO|ASOCIACION|ASSOCIATION|FEDERACIO|FEDERACION|"
    r"CONFEDERACIO|CONFEDERACION|GREMI|GREMIO|COL LEGI|COLLEGI|COLEGIO OFICIAL|AJUNTAMENT|AYUNTAMIENTO|CONSORCI|"
    r"CONSORCIO|UNIVERSITAT|UNIVERSIDAD|MUTUA|MUTUALITAT|SINDICAT|SINDICATO|CARITAS|CREU ROJA|CRUZ ROJA|"
    r"CONGREGACIO|CONGREGACION|PATRONAT|PATRONATO|AGRUPACIO|AGRUPACION|MANCOMUNITAT|MANCOMUNIDAD|CLUB|"
    r"ASSOC|SCCLP|SCPP|SRC|S COOP|SOC COOP)\b")
# Comunidad de bienes: no es persona jurídica y suele llevar los nombres de los comuneros
COMUNIDAD_BIENES = re.compile(r"\b(CB|COMUNITAT DE BENS|COMUNIDAD DE BIENES)\b")


def ahora():
    return datetime.now(timezone.utc)


def normalizar_nombre(nombre):
    """Mayúsculas, sin tildes, sin puntos entre letras de la forma («S.L.» -> «SL») y con espacios simples."""
    s = unicodedata.normalize("NFKD", nombre or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).upper()
    s = s.replace("·", " ").replace("L.L", "L L")
    s = re.sub(r"(?<=\b[A-Z])\.\s?(?=[A-Z]\b)", "", s)    # S.L. / S. L. -> SL (letras sueltas)
    s = re.sub(r"(?<=\b[A-Z]{2})\.(?=[A-Z]\b)", "", s)     # SL.U -> SLU
    s = re.sub(r"[.,;:()\"'/-]", " ", s)
    s = re.sub(r"\b([A-Z]) (?=[A-Z]\b)", r"\1", s)          # S L U -> SLU
    return re.sub(r"\s+", " ", s).strip()


def es_persona_juridica(nombre):
    """El nombre lleva una forma jurídica o una palabra de entidad y no es una comunidad de bienes."""
    n = normalizar_nombre(nombre)
    return bool(FORMAS.search(n)) and not COMUNIDAD_BIENES.search(n)


def validar_csv(ruta, tipo=None):
    """Motivo por el que la descarga no es el CSV del RELIC, o None."""
    try:
        with open(ruta, encoding="utf-8-sig", newline="") as fh:
            cabecera = next(csv.reader(fh))
    except (OSError, UnicodeDecodeError, StopIteration, csv.Error) as e:
        return f"no es un CSV UTF-8 legible ({type(e).__name__})"
    faltan = [c for c in COLUMNAS if c not in cabecera]
    return f"faltan columnas: {', '.join(faltan)}" if faltan else None


def filtrar(origen, destino, resumen):
    """Escribe en `destino` (CSV gzip sin fecha, cabecera tal cual) las filas de personas jurídicas."""
    leidas = conservadas = mal = 0
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(origen, encoding="utf-8-sig", newline="") as fh, open(destino, "wb") as bruto, \
            gzip.GzipFile(fileobj=bruto, mode="wb", mtime=0, filename="") as gz, \
            io.TextIOWrapper(gz, encoding="utf-8", newline="") as texto:
        lector = csv.reader(fh)
        cabecera = next(lector)
        i = cabecera.index(C_NOMBRE)
        w = csv.writer(texto, lineterminator="\n")
        w.writerow(cabecera)
        for fila in lector:
            if not fila:
                continue
            leidas += 1
            if len(fila) != len(cabecera):
                mal += 1
            elif es_persona_juridica(fila[i]):
                w.writerow(fila)
                conservadas += 1
    resumen.datos.update(filas_leidas=leidas, filas_conservadas=conservadas,
                         filas_quitadas_persona_fisica=leidas - conservadas - mal, filas_mal_formadas=mal)
    return conservadas


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


def descargar(salida, resumen):
    """Descarga el CSV, escribe la capa cruda de personas jurídicas y borra el original. True si todo fue bien."""
    raw = salida / "raw"
    trabajo = salida / ".descargas"
    original = trabajo / "relic_t3wj-j4pu.csv"
    tmp = raw / ".relic_pj.csv.gz.nuevo"
    try:
        estado, detalle = red.descargar(URL, original, tipo="csv", validar_contenido=validar_csv)
        if estado not in ("nuevo", "actualizado", "sin_cambios"):
            resumen.errores.append(f"descarga: {estado} {detalle}")
            return False
        manifiesto = {"url": URL, "fecha_descarga": ahora().isoformat(timespec="seconds"),
                      "bytes": original.stat().st_size, "sha256": _sha256(original)}
        try:
            n = filtrar(original, tmp, resumen)
        except (OSError, ValueError, csv.Error) as e:
            resumen.errores.append(f"lectura del CSV: {type(e).__name__}: {e}")
            return False
        if not n:
            resumen.errores.append("ninguna fila de persona jurídica: no se toca nada")
            return False
        resumen.datos["relic_pj.csv.gz"] = guardar_version(raw / "relic_pj.csv.gz", desde=tmp)
        manifiesto.update({k: resumen.datos[k] for k in ("filas_leidas", "filas_conservadas",
                                                         "filas_quitadas_persona_fisica", "filas_mal_formadas")})
        (raw / "_manifiesto.json").write_text(json.dumps(manifiesto, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    finally:
        for p in (tmp, original):
            if p.exists():
                p.unlink()
        hist = trabajo / "_historico"   # red.descargar versiona el original: tampoco se guarda
        if hist.is_dir():
            for p in hist.iterdir():
                p.unlink()
            hist.rmdir()
        if trabajo.is_dir() and not any(trabajo.iterdir()):
            trabajo.rmdir()


def construir_parquet(salida, resumen):
    """Parquet acumulados (texto) de la capa cruda: todas las inscripciones y las prohibiciones."""
    raw = salida / "raw"
    try:
        manifiesto = json.loads((raw / "_manifiesto.json").read_text(encoding="utf-8"))
        nuevos = pd.read_csv(raw / "relic_pj.csv.gz", dtype=str, keep_default_na=False, compression="gzip")
    except (OSError, ValueError) as e:
        resumen.errores.append(f"capa cruda: {e}")
        return False
    nuevos = nuevos.replace({"": None})
    fecha = manifiesto["fecha_descarga"]
    nuevos["_fecha_descarga"] = fecha
    prohib = nuevos[nuevos[C_PROHIBICION].fillna("").str.lower() == "true"]
    for nombre, df in (("inscripciones", nuevos), ("prohibiciones", prohib)):
        destino = salida / f"{nombre}.parquet"
        out = acumular(leer_registros(destino), df.reset_index(drop=True), fecha, ignorar=METADATOS_ORIGEN)
        resumen.datos[f"{nombre}.parquet"] = (f"{guardar_registros(out, destino)}: {len(out)} filas, "
                                              f"{int(out['_en_ultima_descarga'].sum())} en la última descarga")
    return True


def main(argv=None):
    p = argparse.ArgumentParser(description=TITULO)
    p.add_argument("--salida", type=Path, default=SALIDA)
    p.add_argument("--solo-parquet", action="store_true", help="no descarga: rehace los Parquet desde raw/")
    a = p.parse_args(argv)
    t0 = time.time()
    resumen = Resumen()
    ok = True
    if not a.solo_parquet:
        ok = descargar(a.salida, resumen)
    if ok:
        ok = construir_parquet(a.salida, resumen)
    resumen.datos["segundos"] = round(time.time() - t0)
    resumen.escribir(a.salida / "raw")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
