#!/usr/bin/env python3
"""Esquema de una release: columnas, tipos y filas de cada Parquet, leyendo solo sus pies.

Lee el pie (los metadatos) de cada Parquet de una release descomprimida y escribe un Markdown por ZIP con sus
ficheros, sus filas y, de cada fichero, sus columnas con su tipo. No lee los datos: el pie de un Parquet trae el
esquema y el número de filas, así que basta con unos KB por fichero aunque la release ocupe 11 GB.

Uso:
    python scripts/esquema_release.py --datos CARPETA --release v2026.10
    python scripts/esquema_release.py --datos CARPETA --release v2026.10 --manifest MANIFEST.csv --salida docs/esquema

CARPETA tiene una subcarpeta por ZIP, como la deja `unzip X.zip -d X` (para `ted.zip`, la carpeta `ted/`). Se
recorren todos los Parquet de cada una, también los de `v2026.02/`; las carpetas `_historico/` (versiones
anteriores de una salida de los scripts) no forman parte de la release y se saltan.

Salida (por defecto docs/esquema/):
    <zip>.md     una página por ZIP: sus ficheros y, de cada fichero de la release actual, sus columnas
    README.md    el índice: ficheros, filas y columnas de cada ZIP

Con --manifest (el MANIFEST.csv de la release) se comprueban además las filas y las columnas de cada fichero
contra el MANIFEST: si algo no coincide, o falta o sobra un Parquet, lo dice y sale con 1. Sin ningún Parquet en
la carpeta, también sale con 1. La salida no lleva rutas de la máquina, solo nombres relativos a la release, y
es la misma en cada ejecución con los mismos ficheros.
"""

import argparse
import csv
import re
import sys
from pathlib import Path

import pyarrow.parquet as pq

RAIZ_REPO = Path(__file__).resolve().parent.parent
CARPETA_V2026_02 = "v2026.02"
SALTAR = {"_historico"}

# Ficha de cada ZIP en docs/fuentes/ (los nacional_* comparten la de la PLACSP)
FICHAS = {
    "andalucia": "andalucia", "andalucia_menores": "andalucia_menores", "aragon": "aragon",
    "asturias": "asturias", "borme": "borme", "calidad_licitaciones_resultado": "calidad",
    "canarias": "canarias", "castilla_la_mancha": "castilla_la_mancha", "castilla_leon": "castilla_leon",
    "catalunya": "catalunya", "catalunya_menores": "catalunya", "comunidad_madrid": "comunidad_madrid",
    "contratos_galicia": "galicia", "euskadi": "euskadi", "extremadura": "extremadura", "la_rioja": "la_rioja",
    "madrid_ayuntamiento": "madrid_ayuntamiento", "municipios": "municipios", "murcia": "murcia",
    "ted": "ted", "valencia": "valencia", "valencia_menores": "valencia_menores",
}

# Marcas explicadas en docs/ESQUEMA.md: columna -> ancla
MARCAS = {
    "_primera_descarga": "control-de-versiones", "_ultima_descarga": "control-de-versiones",
    "_en_ultima_descarga": "control-de-versiones", "_origen": "control-de-versiones",
    "_fecha_descarga": "control-de-versiones",
    "_fuente": "procedencia", "_dataset": "procedencia", "_recurso": "procedencia", "_recurso_id": "procedencia",
    "_conjunto": "procedencia", "_archivo_origen": "procedencia", "_archivo_fuente": "procedencia",
    "_url_origen": "procedencia", "_fichero_publicado": "procedencia", "_nombre_publicado": "procedencia",
    "_formato": "procedencia", "_miembro": "procedencia", "_hoja": "procedencia", "_fila_origen": "procedencia",
    "_anio_fichero": "procedencia", "_anio_recurso": "procedencia", "_anio": "procedencia",
    "_trimestre": "procedencia", "_mes": "procedencia", "_periodo": "procedencia", "_parte": "procedencia",
    "_titulo": "procedencia", "_pagina": "procedencia", "_encabezado": "procedencia",
    "_titulo_tabla": "procedencia", "_titulo_hoja": "procedencia", "_estructura": "procedencia",
    "_duplicado": "calidad-del-origen", "_columnas_corridas": "calidad-del-origen",
    "_registro_irregular": "calidad-del-origen", "_repetido_en_csv": "calidad-del-origen",
    "_repetido_de": "calidad-del-origen", "_lineas_unidas": "calidad-del-origen",
    "_textos_originales": "calidad-del-origen", "textos_originales": "calidad-del-origen",
    "_fila_vacia": "calidad-del-origen", "_resto_json": "calidad-del-origen",
    "n_versiones": "versiones-de-la-placsp", "es_ultima_version": "versiones-de-la-placsp",
    "entrada_repetida": "versiones-de-la-placsp",
    "_xml_eforms": "ted", "ganadora_por": "ted",
}
PATRONES = [
    (re.compile(r"^.+_corregido$"), "importes-corregidos"),
    (re.compile(r"^correccion_.+$"), "importes-corregidos"),
    (re.compile(r"^_columna_extra_\d+$"), "calidad-del-origen"),
    (re.compile(r"^_.+_sin_cortes$"), "calidad-del-origen"),
    (re.compile(r"^_.+_num$"), "valores-convertidos"),
    (re.compile(r"^.+_texto$"), "valores-convertidos"),
]


def miles(n):
    """1234567 -> '1.234.567' (como el resto de la documentación)."""
    return f"{n:,}".replace(",", ".")


def ancla_marca(columna):
    """Ancla de docs/ESQUEMA.md que explica la columna, o None."""
    if columna in MARCAS:
        return MARCAS[columna]
    for patron, ancla in PATRONES:
        if patron.match(columna):
            return ancla
    return None


def leer_pie(ruta):
    """(filas, [(columna, tipo)], grupos de filas) del pie de un Parquet, sin leer sus datos."""
    pf = pq.ParquetFile(ruta)
    try:
        esquema = pf.schema_arrow
        md = pf.metadata
        return md.num_rows, [(c.name, str(c.type)) for c in esquema], md.num_row_groups
    finally:
        pf.close()


def ficheros_de(carpeta):
    """Ficheros de la carpeta de un ZIP (rutas relativas, en orden), sin las carpetas que no son de la release."""
    out = []
    for p in sorted(carpeta.rglob("*")):
        rel = p.relative_to(carpeta)
        if any(parte in SALTAR for parte in rel.parts) or not p.is_file():
            continue
        out.append(rel)
    return out


def carpetas_zip(datos):
    """Subcarpetas de la release que tienen algún Parquet: una por ZIP."""
    return [d for d in sorted(datos.iterdir()) if d.is_dir() and d.name not in SALTAR
            and any(f.suffix == ".parquet" for f in ficheros_de(d))]


def leer_manifest(ruta):
    """{(zip, fichero): (filas, columnas)} del MANIFEST.csv de la release (las filas sin número, como None)."""
    out = {}
    with open(ruta, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            num = lambda v: int(v) if v not in ("", None) else None  # noqa: E731
            out[(r["zip"], r["fichero"])] = (num(r.get("filas")), num(r.get("columnas")))
    return out


def describir(datos):
    """Lo que hay en la release: {zip: [{'fichero', 'v2026_02', 'parquet', 'filas', 'columnas', 'grupos'}]}."""
    release = {}
    for carpeta in carpetas_zip(datos):
        filas = []
        for rel in ficheros_de(carpeta):
            if rel.name in ("LEEME.txt", "MANIFEST.csv") and len(rel.parts) == 1:
                continue
            item = {"fichero": rel.as_posix(), "v2026_02": rel.parts[0] == CARPETA_V2026_02,
                    "parquet": rel.suffix == ".parquet", "filas": None, "columnas": None, "grupos": None}
            if item["parquet"]:
                item["filas"], item["columnas"], item["grupos"] = leer_pie(carpeta / rel)
            filas.append(item)
        release[f"{carpeta.name}.zip"] = filas
    return release


def comprobar(release, manifest):
    """Diferencias entre los pies y el MANIFEST: lista de textos (vacía si todo coincide)."""
    malos = []
    vistos = set()
    for zip_, items in release.items():
        for it in items:
            clave = (zip_, it["fichero"])
            vistos.add(clave)
            if clave not in manifest:
                malos.append(f"{zip_} {it['fichero']}: no está en el MANIFEST")
                continue
            filas, cols = manifest[clave]
            if it["parquet"] and (filas, cols) != (it["filas"], len(it["columnas"])):
                malos.append(f"{zip_} {it['fichero']}: {it['filas']} filas y {len(it['columnas'])} columnas; "
                             f"el MANIFEST dice {filas} y {cols}")
    zips = set(release)
    for (zip_, fichero) in sorted(set(manifest) - vistos):
        if zip_ in zips and fichero.endswith(".parquet"):
            malos.append(f"{zip_} {fichero}: está en el MANIFEST y no en la carpeta")
    return malos


def celda_marca(columna, ficha):
    ancla = ancla_marca(columna)
    if ancla:
        return f"[marca](../ESQUEMA.md#{ancla})"
    if columna.startswith("_"):
        return f"[de la fuente](../fuentes/{ficha}.md)" if ficha else "de la fuente"
    return ""


def pagina_zip(zip_, items, release_nombre):
    stem = zip_[:-4]
    ficha = FICHAS.get(stem) or ("placsp" if stem.startswith("nacional_") else None)
    actuales = [i for i in items if not i["v2026_02"]]
    antiguos = [i for i in items if i["v2026_02"]]
    lineas = [f"# `{zip_}`", ""]
    lineas.append(f"Esquema de los Parquet de `{zip_}` en la release {release_nombre}, leído de sus pies con "
                  f"[`scripts/esquema_release.py`](../../scripts/esquema_release.py) (no a mano)."
                  + (f" Qué es cada tabla y cómo usarla: [ficha de la fuente](../fuentes/{ficha}.md)." if ficha else "")
                  + " Las marcas `_…` comunes: [`docs/ESQUEMA.md`](../ESQUEMA.md).")
    lineas.append("")
    if actuales:
        lineas += ["| Fichero | Filas | Columnas |", "|---|--:|--:|"]
        for it in actuales:
            if it["parquet"]:
                lineas.append(f"| `{it['fichero']}` | {miles(it['filas'])} | {len(it['columnas'])} |")
            else:
                lineas.append(f"| `{it['fichero']}` | — | — |")
        lineas.append("")
    for it in actuales:
        if not it["parquet"]:
            continue
        lineas += [f"## `{it['fichero']}`", "",
                   f"{miles(it['filas'])} filas · {len(it['columnas'])} columnas · {it['grupos']} "
                   f"{'grupo' if it['grupos'] == 1 else 'grupos'} de filas", "",
                   "| # | Columna | Tipo | |", "|--:|---|---|---|"]
        for n, (col, tipo) in enumerate(it["columnas"], 1):
            nombre = col.replace("|", "\\|").replace("`", "'")
            lineas.append(f"| {n} | `{nombre}` | `{tipo}` | {celda_marca(col, ficha)} |")
        lineas.append("")
    if antiguos:
        n_pq = sum(1 for i in antiguos if i["parquet"])
        lineas += [f"## Carpeta `{CARPETA_V2026_02}/`", "",
                   f"Ficheros de v2026.02 que van tal cual en esta release ({len(antiguos)}, {n_pq} Parquet), porque "
                   "la descarga actual no los cubre: su esquema es el de v2026.02. La columna `nota` del "
                   "`MANIFEST.csv` del ZIP dice por qué está cada uno.", "",
                   "| Fichero | Filas | Columnas |", "|---|--:|--:|"]
        for it in antiguos:
            if it["parquet"]:
                lineas.append(f"| `{it['fichero']}` | {miles(it['filas'])} | {len(it['columnas'])} |")
            else:
                lineas.append(f"| `{it['fichero']}` | — | — |")
        lineas.append("")
    lineas += ["---", "", "[Todos los ZIP](README.md) · [Marcas comunes](../ESQUEMA.md)", ""]
    return "\n".join(lineas)


def indice(release, release_nombre):
    total_f = sum(1 for items in release.values() for i in items if i["parquet"] and not i["v2026_02"])
    total_n = sum(i["filas"] for items in release.values() for i in items if i["parquet"] and not i["v2026_02"])
    lineas = ["# Esquema de la release", "",
              f"Columnas, tipos y filas de cada Parquet de la release {release_nombre}, una página por ZIP. Se "
              "genera leyendo solo los pies de los Parquet con "
              "[`scripts/esquema_release.py`](../../scripts/esquema_release.py); las marcas `_…` que se repiten "
              "en varias fuentes se explican en [`docs/ESQUEMA.md`](../ESQUEMA.md).", "",
              f"{len(release)} ZIP, {total_f} Parquet y {miles(total_n)} filas, sin contar las carpetas "
              f"`{CARPETA_V2026_02}/`.", "",
              "| ZIP | Parquet | Filas | En `v2026.02/` |", "|---|--:|--:|--:|"]
    for zip_, items in release.items():
        act = [i for i in items if i["parquet"] and not i["v2026_02"]]
        ant = [i for i in items if i["v2026_02"]]
        lineas.append(f"| [`{zip_}`]({zip_[:-4]}.md) | {len(act)} | {miles(sum(i['filas'] for i in act))} | "
                      f"{len(ant) or '—'} |")
    lineas += ["", "Para regenerarlo con otra release, descomprimido cada ZIP en su carpeta (`unzip X.zip -d X`):",
               "", "```bash",
               "python scripts/esquema_release.py --datos CARPETA --release vAAAA.MM --manifest CARPETA/MANIFEST.csv",
               "```", ""]
    return "\n".join(lineas)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--datos", type=Path, required=True, help="release descomprimida, una carpeta por ZIP")
    ap.add_argument("--release", default="", help="nombre de la release para los textos (p. ej. v2026.10)")
    ap.add_argument("--salida", type=Path, default=RAIZ_REPO / "docs" / "esquema", help="carpeta de los .md")
    ap.add_argument("--manifest", type=Path, help="MANIFEST.csv de la release, para comprobar filas y columnas")
    a = ap.parse_args(argv)
    if not a.datos.is_dir():
        print(f"no existe la carpeta {a.datos}", file=sys.stderr)
        return 1
    release = describir(a.datos)
    if not release:
        print("no hay ningún Parquet en ninguna subcarpeta: ¿está descomprimida cada ZIP en su carpeta?",
              file=sys.stderr)
        return 1
    nombre = a.release or "descomprimida"
    a.salida.mkdir(parents=True, exist_ok=True)
    for zip_, items in release.items():
        (a.salida / f"{zip_[:-4]}.md").write_text(pagina_zip(zip_, items, nombre), encoding="utf-8")
    (a.salida / "README.md").write_text(indice(release, nombre), encoding="utf-8")
    n = sum(1 for items in release.values() for i in items if i["parquet"])
    print(f"{len(release)} ZIP y {n} Parquet: {a.salida}")
    if a.manifest:
        malos = comprobar(release, leer_manifest(a.manifest))
        for m in malos:
            print("DISTINTO:", m, file=sys.stderr)
        if malos:
            return 1
        print("filas y columnas iguales que en el MANIFEST")
    return 0


if __name__ == "__main__":
    sys.exit(main())
