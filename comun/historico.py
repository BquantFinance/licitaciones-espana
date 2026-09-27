"""
HISTÓRICO DE DESCARGAS: CONTROL DEL SESGO DEL SUPERVIVIENTE
===========================================================
Los portales retiran y modifican registros. Si una nueva descarga sobrescribe
la anterior, lo que la administración deja de servir desaparece en silencio y
los datos solo muestran a los "supervivientes". Reglas para todos los scrapers:

1. Capa cruda inmutable (guardar_version): una descarga nunca machaca la
   anterior. Si el contenido es idéntico no se toca nada; si cambió, la copia
   previa pasa a <dir>/_historico/<nombre>__<AAAAMMDDTHHMMSSZ><ext> (fecha de
   esa copia) y la nueva ocupa su lugar. No se borra nada.
2. Registros acumulados (acumular): cada fila que se ha visto alguna vez se
   conserva con _primera_descarga, _ultima_descarga y _en_ultima_descarga.
   Si la administración retira o cambia un registro, la fila antigua sigue
   ahí con _en_ultima_descarga=False (un registro cambiado aparece como la
   versión antigua, ya no servida, y la nueva).
3. Re-ejecuciones incrementales: se completa lo que falta; lo que ya se tiene
   solo se vuelve a pedir para detectar cambios, y entonces se aplican 1 y 2.

Los datos publicados en un release (p.ej. v2026.02) son la instantánea más
antigua disponible: para no perder lo que se haya retirado desde entonces,
se pueden usar como primera descarga: acumular(acumular(None, publicado,
'2026-02-..'), nuevos, hoy), siempre que el código que los generó sea
compatible con el actual (si no, las mismas filas no casarían).
"""

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HISTORICO = "_historico"
COLUMNAS_META = ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")
# Columnas que cambian en cada descarga sin que cambie el registro
IGNORAR_POR_DEFECTO = ("_fecha_descarga",)


# ─────────────────────────────────────────────────────────────
# 1. Ficheros descargados
# ─────────────────────────────────────────────────────────────

def _sha256(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return h.hexdigest()


def _sello(momento):
    return momento.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ruta_historica(destino, momento):
    """Ruta en _historico/ de la versión de `destino` descargada en `momento`."""
    destino = Path(destino)
    return destino.parent / HISTORICO / f"{destino.stem}__{_sello(momento)}{destino.suffix}"


def guardar_version(destino, contenido=None, *, desde=None):
    """Guarda una descarga en `destino` sin perder la versión anterior.

    Recibe el contenido en bytes (`contenido`) o la ruta de un fichero ya
    descargado (`desde`, p.ej. un .part escrito en streaming), que se mueve.
    Devuelve 'nuevo', 'sin_cambios' (idéntico al actual: no se toca) o
    'actualizado' (la versión anterior queda en _historico/).
    """
    destino = Path(destino)
    if (contenido is None) == (desde is None):
        raise ValueError("Indica contenido o desde, no ambos")
    destino.parent.mkdir(parents=True, exist_ok=True)
    if contenido is not None:
        desde = destino.with_name(f".{destino.name}.nuevo")
        desde.write_bytes(contenido)
    desde = Path(desde)

    if not destino.exists():
        os.replace(desde, destino)
        return "nuevo"
    if _sha256(desde) == _sha256(destino):
        desde.unlink()
        return "sin_cambios"

    previo = datetime.fromtimestamp(destino.stat().st_mtime, timezone.utc)
    archivo = ruta_historica(destino, previo)
    archivo.parent.mkdir(exist_ok=True)
    n = 1
    while archivo.exists():   # dos versiones con el mismo sello: no pisar ninguna
        archivo = archivo.with_name(f"{archivo.stem}_{n}{archivo.suffix}")
        n += 1
    os.replace(destino, archivo)
    os.replace(desde, destino)
    return "actualizado"


def versiones(destino):
    """Versiones guardadas de `destino`, de la más antigua a la actual."""
    destino = Path(destino)
    carpeta = destino.parent / HISTORICO
    antiguas = sorted(carpeta.glob(f"{destino.stem}__*{destino.suffix}")) if carpeta.is_dir() else []
    return antiguas + ([destino] if destino.exists() else [])


# ─────────────────────────────────────────────────────────────
# 2. Registros
# ─────────────────────────────────────────────────────────────

def _claves(df, columnas):
    """Hash por fila de `columnas`, comparando valores como texto (nulo = nulo)."""
    if not columnas:
        return pd.Series(0, index=df.index, dtype="uint64")
    texto = pd.DataFrame({
        c: df[c].astype(object).map(lambda v: "\x00" if _es_nulo(v) else str(v))
        for c in columnas
    }, index=df.index)
    return pd.util.hash_pandas_object(texto, index=False)


def _es_nulo(v):
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):   # listas, arrays…
        return False


def acumular(anterior, nuevos, fecha, ambito=None, ignorar=IGNORAR_POR_DEFECTO,
             permitir_vacio=False):
    """Une la descarga `nuevos` (fecha `fecha`) con los registros `anterior`.

    - Ninguna fila de `anterior` se elimina.
    - Una fila de `nuevos` idéntica a una de `anterior` (en las columnas que
      tienen ambas, salvo COLUMNAS_META e `ignorar`) no se duplica: se
      actualiza su _ultima_descarga. Se compara como multiconjunto: si el
      origen sirve dos filas idénticas se conservan las dos.
    - Las filas de `anterior` que no están en `nuevos` quedan con
      _en_ultima_descarga=False, pero solo dentro de `ambito`: columnas que
      delimitan lo que se ha vuelto a descargar (p.ej. ['_archivo_origen'] o
      ['anio']); fuera de él no se sabe si siguen publicadas y no cambian.
    - Columnas nuevas se añaden (nulas en las filas que no las tenían).
    - Una descarga vacía es casi siempre un fallo (no que la administración lo
      haya retirado todo): da error salvo permitir_vacio=True.
    """
    if len(nuevos) == 0 and not permitir_vacio:
        raise ValueError("Descarga vacía: no se marca nada como retirado")
    nuevos = nuevos.reset_index(drop=True).copy()
    if anterior is None or len(anterior) == 0:
        out = nuevos
        out["_primera_descarga"] = fecha
        out["_ultima_descarga"] = fecha
        out["_en_ultima_descarga"] = True
        return out

    anterior = anterior.reset_index(drop=True).copy()
    excluir = set(COLUMNAS_META) | set(ignorar or ())
    comunes = [c for c in nuevos.columns if c in anterior.columns and c not in excluir]

    k_ant = _claves(anterior, comunes)
    k_nue = _claves(nuevos, comunes)
    pos_ant = pd.Series(anterior.index.to_numpy(), index=pd.MultiIndex.from_arrays(
        [k_ant.to_numpy(), k_ant.groupby(k_ant).cumcount().to_numpy()]))
    pos = pos_ant.reindex(pd.MultiIndex.from_arrays(
        [k_nue.to_numpy(), k_nue.groupby(k_nue).cumcount().to_numpy()]))
    casada = pos.notna().to_numpy()
    i_ant = pos.to_numpy()[casada].astype("int64")

    if ambito:
        vistos = set(map(tuple, nuevos[ambito].astype(str).to_numpy()))
        en_ambito = [t in vistos for t in map(tuple, anterior[ambito].astype(str).to_numpy())]
    else:
        en_ambito = slice(None)
    anterior.loc[en_ambito, "_en_ultima_descarga"] = False
    anterior.loc[i_ant, "_ultima_descarga"] = fecha
    anterior.loc[i_ant, "_en_ultima_descarga"] = True
    for c in nuevos.columns:
        if c not in anterior.columns:
            anterior[c] = pd.Series([None] * len(anterior), dtype=object)
            anterior.loc[i_ant, c] = nuevos.loc[casada, c].to_numpy()

    altas = nuevos.loc[~casada].copy()
    altas["_primera_descarga"] = fecha
    altas["_ultima_descarga"] = fecha
    altas["_en_ultima_descarga"] = True
    out = pd.concat([anterior, altas], ignore_index=True, sort=False)
    out["_en_ultima_descarga"] = out["_en_ultima_descarga"].astype(bool)
    return out


def leer_registros(ruta):
    """Registros acumulados guardados en `ruta` (parquet) o None si no existen."""
    ruta = Path(ruta)
    return pd.read_parquet(ruta) if ruta.exists() else None


def guardar_registros(df, ruta):
    """Escribe los registros acumulados de forma atómica, guardando la versión
    anterior del fichero en _historico/ (guardar_version)."""
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_name(f".{ruta.name}.nuevo")
    df.to_parquet(tmp, index=False)
    return guardar_version(ruta, desde=tmp)
