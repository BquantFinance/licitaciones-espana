#!/usr/bin/env python3
"""
=============================================================================
REGIA - GRUPOS DE INTERÉS DE LA GENERALITAT VALENCIANA Y SUS ACTIVIDADES DE INFLUENCIA
=============================================================================
Descarga, de los datos abiertos de la Generalitat Valenciana, el Registro de
Grupos de Interés (REGIA, Ley 25/2018) y el inventario de sus actividades de
influencia: cada grupo inscrito con su número REGIA, su NIF, la fecha de
inscripción, la categoría y los ámbitos; y cada reunión (o contacto) con su
fecha, tipo, asuntos tratados, los grupos que asistieron y los cargos de la
Generalitat con los que se reunieron (el cargo, no la persona).

Ejecutar:  python regia/regia_valencia.py [--salida DIR] [--solo-parquet]

Salida (por defecto <repo>/regia_valencia/):
    raw/grupos.csv.gz               grupos inscritos (sin dirección ni correo, sin personas físicas)
    raw/reuniones.csv.gz            actividades de influencia (id_reunion, tipo, asuntos, fecha, huella)
    raw/reuniones_grupos.csv.gz     qué grupos estuvieron en cada reunión (sin personas físicas)
    raw/reuniones_cargos.csv.gz     qué cargos de la Generalitat estuvieron en cada reunión
    raw/_historico/                 versiones anteriores (nunca se borran)
    raw/_manifiesto.json            URL, bytes y sha256 de cada descarga original, filas leídas y conservadas
    raw/descarga_log.txt            resumen de cada ejecución (se añade al final)
    grupos.parquet, reuniones.parquet, reuniones_grupos.parquet, reuniones_cargos.parquet
                                    registros acumulados (comun/historico.py: acumular), como texto

Personas físicas, fuera siempre: el portal enmascara a los empresarios
individuales (nombre «Empresario individual» u «Otros», NIF «*********»), pero
publica su dirección y su correo. Esas filas no se escriben (tampoco las de tipo
«Empresario individual» sin el NIF de una persona jurídica), ni las reuniones
que las citan como grupo (la reunión sí, con sus otros grupos y cargos). Las
columnas «direccion» y «email» no se guardan de ningún grupo (el correo suele
ser el de una persona). En «asuntos_tratados», un nombre con tratamiento
(«don», «doña», «D.», «Dña.», «Sr.», «Sra.», «senyor», «senyora» seguido de
nombre y apellidos) se sustituye por «[persona física]», como en el BOE.

Qué se pide en cada ejecución: 4 peticiones (los 4 CSV, ~1,6 MB), con
PAUSA = 11 s entre ellas: el robots.txt del portal pide Crawl-Delay: 10 y
prohíbe /api/, así que se piden las URL de descarga de cada recurso, no la API
de CKAN. Sale con 1 si algo no se pudo descargar o leer, sin tocar los
ficheros anteriores.

FUENTES
-------
Verificado en vivo el 2026-10-27 (confianza A):
  - Conjunto «Datos relativos a las entidades inscritas en el Registro de Grupos de Interés»
    (dadesobertes.gva.es, dataset 720f442d-…): sec_regia_grupos.csv. Punto y coma, UTF-8, la
    cabecera con espacios tras el separador; 2.210 grupos (25 «Empresario individual» y 8
    «Otros» con el NIF enmascarado).
  - Conjunto «Inventario de actividades de influencia» (dataset 073e744e-…): tres CSV, punto y
    coma, UTF-8: regia_actividades_de_influencia_actividades.csv (2.550 actividades 2022-2026),
    _grupos.csv (2.706 filas reunión-grupo, 24 con el NIF enmascarado) y _altos_cargos.csv
    (cargos, p. ej. «DG DE TRIBUTOS Y JUEGO»).
  - Licencia: la de los datos abiertos de la Generalitat Valenciana (DATA_LICENSE.md).
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
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import _sha256, acumular, guardar_registros, guardar_version, leer_registros  # noqa: E402
from comun import red  # noqa: E402

BASE = "https://dadesobertes.gva.es/dataset/"
RECURSOS = {   # nombre en raw/ -> (URL del recurso, columnas que se esperan)
    "grupos": (BASE + "720f442d-e477-41ed-987a-b7d499be6076/resource/7332bd8e-103a-4669-aecc-612d68fd9f5c/download/"
                      "sec_regia_grupos.csv",
               ("nombre_gi", "num_regia", "nif", "fecha", "categoria", "subcategoria", "tipo_organizacion")),
    "reuniones": (BASE + "073e744e-37d1-42c9-b521-48e79ccf6813/resource/cc33efbd-d9ad-464f-9677-0341f7c81328/download/"
                         "regia_actividades_de_influencia_actividades.csv",
                  ("id_reunion", "tipo", "asuntos_tratados", "fecha")),
    "reuniones_grupos": (BASE + "073e744e-37d1-42c9-b521-48e79ccf6813/resource/6f51d31e-db86-447f-a3c3-9a0c626c7356/"
                                "download/regia_actividades_de_influencia_grupos.csv",
                         ("id_reunion", "nombre_gi", "num_doc_identidad", "num_regia")),
    "reuniones_cargos": (BASE + "073e744e-37d1-42c9-b521-48e79ccf6813/resource/84644298-3d37-46c1-aeda-3a4fd9da18d4/"
                                "download/regia_actividades_de_influencia_altos_cargos.csv",
                         ("id_reunion", "cargo")),
}
PAUSA = 11.0                        # robots.txt: Crawl-Delay: 10
SALIDA = Path(__file__).resolve().parent.parent / "regia_valencia"
TITULO = "REGIA - GRUPOS DE INTERÉS (COMUNITAT VALENCIANA)"
COLUMNAS_FUERA = ("direccion", "email")
METADATOS_ORIGEN = ("_fecha_descarga",)
ENMASCARADO = re.compile(r"^\*+$")
DNI_NIE = re.compile(r"^(?:\d{8}[A-Z]|[XYZKLM]\d{7}[A-Z])$")
CIF = re.compile(r"^[ABCDEFGHJNPQRSUVW]\d{7}[0-9A-J]$")       # NIF de persona jurídica
MAYUS = "A-ZÁÉÍÓÚÜÑÀÈÒÏÇ"
PALABRA = rf"(?:[{MAYUS}][\w{MAYUS}áéíóúüñàèòïç'-]+|[{MAYUS}]\.)"
TRATAMIENTO = r"(?:don|doña|D\.|Dña\.|Dª|Sr\.|Sra\.|senyor|senyora)"
PERSONA = re.compile(rf"\b{TRATAMIENTO}\s+{PALABRA}"
                     rf"(?:\s+(?:de\s+(?:la\s+|los\s+|las\s+)?|del\s+|y\s+|i\s+)?{PALABRA}){{1,5}}")
SUSTITUTO = "[persona física]"


def ahora():
    return datetime.now(timezone.utc)


def es_persona_fisica(nif, tipo_org=None, nombre=None):
    """NIF enmascarado o de DNI/NIE, o un empresario individual sin NIF de persona jurídica (dos sociedades limitadas
    inscritas como «Empresario individual» con su CIF se conservan)."""
    n = re.sub(r"[^0-9A-Za-z*]", "", nif or "").upper()
    if ENMASCARADO.match(n) or DNI_NIE.match(n):
        return True
    individual = "empresario individual" in ((tipo_org or "").strip().lower(), (nombre or "").strip().lower())
    return individual and not CIF.match(n)


def sin_personas(texto):
    """(texto con los nombres con tratamiento sustituidos, cuántos)."""
    return PERSONA.subn(SUSTITUTO, texto or "")


def leer_csv(ruta):
    """CSV del portal: punto y coma, UTF-8, cabecera con espacios tras el separador. Lista de dict de texto."""
    with open(ruta, encoding="utf-8-sig", newline="") as fh:
        lector = csv.reader(fh, delimiter=";")
        cabecera = [c.strip() for c in next(lector)]
        return cabecera, [dict(zip(cabecera, (v.strip() for v in fila))) for fila in lector if fila]


def validador(esperadas):
    def validar(ruta, tipo=None):
        try:
            cabecera, _ = leer_csv(ruta)
        except (OSError, UnicodeDecodeError, StopIteration, csv.Error) as e:
            return f"no es un CSV UTF-8 legible ({type(e).__name__})"
        faltan = [c for c in esperadas if c not in cabecera]
        return f"faltan columnas: {', '.join(faltan)}" if faltan else None
    return validar


def escribir_csv_gz(destino, cabecera, filas):
    """CSV gzip sin fecha (el mismo contenido da los mismos bytes), punto y coma."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "wb") as bruto, gzip.GzipFile(fileobj=bruto, mode="wb", mtime=0, filename="") as gz, \
            io.TextIOWrapper(gz, encoding="utf-8", newline="") as texto:
        w = csv.writer(texto, delimiter=";", lineterminator="\n")
        w.writerow(cabecera)
        for f in filas:
            w.writerow([f.get(c, "") for c in cabecera])


def depurar(nombre, cabecera, filas, resumen):
    """(cabecera, filas) sin personas físicas ni las columnas que no se guardan."""
    leidas = len(filas)
    if nombre == "grupos":
        filas = [f for f in filas if not es_persona_fisica(f.get("nif"), f.get("tipo_organizacion"), f.get("nombre_gi"))]
        cabecera = [c for c in cabecera if c not in COLUMNAS_FUERA]
    elif nombre == "reuniones_grupos":
        filas = [f for f in filas if not es_persona_fisica(f.get("num_doc_identidad"), None, f.get("nombre_gi"))]
    elif nombre == "reuniones":
        sustituidos = 0
        for f in filas:
            for c in ("asuntos_tratados", "huella"):
                f[c], n = sin_personas(f.get(c))
                sustituidos += n
        resumen.datos["reuniones_nombres_sustituidos"] = sustituidos
    resumen.datos[f"{nombre}_leidas"] = leidas
    resumen.datos[f"{nombre}_conservadas"] = len(filas)
    return cabecera, filas


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
    """Descarga los 4 CSV, escribe la capa cruda depurada y borra los originales. True si todo fue bien."""
    raw = salida / "raw"
    trabajo = salida / ".descargas"
    manifiesto = {"fecha_descarga": ahora().isoformat(timespec="seconds"), "ficheros": {}}
    originales, nuevos = {}, {}
    try:
        for i, (nombre, (url, esperadas)) in enumerate(RECURSOS.items()):
            if i:
                time.sleep(PAUSA)
            destino = trabajo / Path(url).name
            estado, detalle = red.descargar(url, destino, tipo="csv", validar_contenido=validador(esperadas))
            if estado not in ("nuevo", "actualizado", "sin_cambios"):
                resumen.errores.append(f"{nombre}: {estado} {detalle}")
                return False
            originales[nombre] = destino
            manifiesto["ficheros"][nombre] = {"url": url, "bytes": destino.stat().st_size, "sha256": _sha256(destino)}
        # Se depuran los cuatro antes de tocar raw/: un fallo a mitad no deja descargas mezcladas
        for nombre, ruta in originales.items():
            cabecera, filas = depurar(nombre, *leer_csv(ruta), resumen)
            if nombre in ("grupos", "reuniones") and not filas:
                raise ValueError(f"{nombre}: ninguna fila")
            nuevos[nombre] = raw / f".{nombre}.csv.gz.nuevo"
            escribir_csv_gz(nuevos[nombre], cabecera, filas)
            manifiesto["ficheros"][nombre].update(filas_leidas=resumen.datos[f"{nombre}_leidas"],
                                                  filas_conservadas=resumen.datos[f"{nombre}_conservadas"])
    except (OSError, ValueError, csv.Error, StopIteration, UnicodeDecodeError) as e:
        resumen.errores.append(f"lectura: {type(e).__name__}: {e}")
        return False
    finally:
        for p in originales.values():
            if p.exists():
                p.unlink()
        hist = trabajo / "_historico"   # red.descargar versiona los originales: tampoco se guardan
        if hist.is_dir():
            for p in hist.iterdir():
                p.unlink()
            hist.rmdir()
        if trabajo.is_dir() and not any(trabajo.iterdir()):
            trabajo.rmdir()
        if resumen.errores:
            for p in nuevos.values():
                if p.exists():
                    p.unlink()
    for nombre, tmp in nuevos.items():
        resumen.datos[f"{nombre}.csv.gz"] = guardar_version(raw / f"{nombre}.csv.gz", desde=tmp)
    (raw / "_manifiesto.json").write_text(json.dumps(manifiesto, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def construir_parquet(salida, resumen):
    """Parquet acumulado de cada CSV crudo (todas las columnas como texto)."""
    raw = salida / "raw"
    try:
        fecha = json.loads((raw / "_manifiesto.json").read_text(encoding="utf-8"))["fecha_descarga"]
    except (OSError, ValueError, KeyError) as e:
        resumen.errores.append(f"sin manifiesto: {e}")
        return False
    for nombre in RECURSOS:
        try:
            nuevos = pd.read_csv(raw / f"{nombre}.csv.gz", sep=";", dtype=str, keep_default_na=False,
                                 compression="gzip").replace({"": None})
        except (OSError, ValueError) as e:
            resumen.errores.append(f"{nombre}: {e}")
            return False
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
