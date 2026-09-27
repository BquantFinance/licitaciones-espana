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

4. Semilla (sembrar): cuando el código actual y el que generó el publicado no
   extraen igual los valores, se casa por una clave estable (p.ej. id y fecha
   de actualización) en vez de por fila entera: del publicado solo se añaden
   las filas cuya clave no aparece en la descarga nueva, marcadas con
   _origen='release v2026.02' y _en_ultima_descarga=False. Nunca se modifica
   ni se duplica una fila de la descarga. Dos filas con la clave incompleta
   en una de ellas (p.ej. una fecha que el código antiguo no supo leer) se
   comparan por el resto de la clave y por las columnas de contenido que los
   dos códigos extraen igual (hay que verificarlas con datos reales), en los
   dos sentidos: así dos publicados que traen la misma entrada, uno sin fecha
   y otro con ella, no la duplican. Solo se siembra dentro del ámbito de la
   ejecución (en_ambito): fuera de él no se sabe si la fila sigue publicada.
   seleccionar_semilla decide qué filas se añaden sin cargar la descarga
   entera (la usa nacional/licitaciones.py, por lotes); archivar() guarda en
   _historico/ una salida que la ejecución nueva ya no produce.
"""

import hashlib
import os
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

HISTORICO = "_historico"
COLUMNAS_META = ("_primera_descarga", "_ultima_descarga", "_en_ultima_descarga")
# Columnas que cambian en cada descarga sin que cambie el registro
IGNORAR_POR_DEFECTO = ("_fecha_descarga",)
# Semilla: origen por defecto de las filas añadidas y columnas de control
ORIGEN_SEMILLA = "release v2026.02"
COLUMNAS_SEMILLA = ("_origen", "_en_ultima_descarga")
# Motivo de cada fila de la semilla (seleccionar_semilla)
ANADIDA = "añadida"
PRESENTE_CLAVE = "clave presente"
PRESENTE_CONTENIDO = "contenido presente"
FUERA_AMBITO = "fuera del ámbito"


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

    archivar(destino)
    os.replace(desde, destino)
    return "actualizado"


def archivar(destino):
    """Mueve la copia actual de `destino` a _historico/ con el sello de su fecha
    (ruta_historica) sin poner nada en su lugar, p.ej. una salida que una
    ejecución nueva ya no produce. Devuelve la ruta en _historico/."""
    destino = Path(destino)
    previo = datetime.fromtimestamp(destino.stat().st_mtime, timezone.utc)
    archivo = ruta_historica(destino, previo)
    archivo.parent.mkdir(exist_ok=True)
    n = 1
    while archivo.exists():   # dos versiones con el mismo sello: no pisar ninguna
        archivo = archivo.with_name(f"{archivo.stem}_{n}{archivo.suffix}")
        n += 1
    os.replace(destino, archivo)
    return archivo


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


# ─────────────────────────────────────────────────────────────
# 3. Semilla: la instantánea publicada como la más antigua
# ─────────────────────────────────────────────────────────────

def _texto_valor(v):
    """Un valor como texto comparable (None si es nulo), ver texto_canonico."""
    if _es_nulo(v):
        return None
    if isinstance(v, (bool, np.bool_)):
        return str(bool(v))
    if isinstance(v, (int, np.integer)):
        return repr(float(v)) if abs(int(v)) < 2 ** 53 else str(int(v))
    if isinstance(v, (float, np.floating)):
        return repr(float(v))
    if isinstance(v, datetime):
        v = pd.Timestamp(v)
        if v.tzinfo is not None:
            return v.tz_convert("UTC").isoformat()
        return v.date().isoformat() if v == v.normalize() else v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def texto_canonico(serie):
    """Valores de `serie` como texto comparable entre fuentes que guardan el
    mismo dato con tipos distintos (None si es nulo): números como float
    (5 == 5.0 == '5.0'), fechas sin hora como AAAA-MM-DD (date32, datetime a
    medianoche u objeto date), instantes con zona en UTC y el resto con str()."""
    serie = pd.Series(serie)
    return pd.Series([_texto_valor(v) for v in serie.astype(object)], index=serie.index, dtype=object)


def instantes_ns(serie):
    """Instantes como entero de ns en UTC (IntegerArray; NaT/no fecha = NA).
    Sin zona se toman como UTC; el texto se lee en ISO 8601."""
    s = pd.Series(serie).reset_index(drop=True)
    if not pd.api.types.is_datetime64_any_dtype(s):
        objetos = s.astype(object)
        textos = objetos.dropna().map(type).eq(str).all()
        s = pd.to_datetime(objetos, errors="coerce", utc=True,
                           **({"format": "ISO8601"} if textos else {}))
    elif s.dt.tz is None:
        s = s.dt.tz_localize("UTC")
    nulo = s.isna().to_numpy()
    ns = s.dt.tz_convert("UTC").dt.tz_localize(None).dt.as_unit("ns").to_numpy().view("int64")
    return pd.arrays.IntegerArray(np.where(nulo, 0, ns).astype("int64"), nulo)


def _es_fecha(serie):
    """Si la columna es de fechas (tipo datetime u objetos datetime/date)."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return True
    valores = serie.dropna()
    return (serie.dtype == object and len(valores) > 0
            and valores.map(lambda v: isinstance(v, (datetime, date))).all())


def _valores_clave(a, b):
    """Valores de una columna de la clave en dos tablas, comparables entre sí:
    instantes (si alguna es de fechas) como entero de ns en UTC, enteros tal
    cual y el resto como texto (como en acumular: 1 y "1" son el mismo
    valor). Nulo = NA/None."""
    a, b = pd.Series(a).reset_index(drop=True), pd.Series(b).reset_index(drop=True)
    if _es_fecha(a) or _es_fecha(b):
        return instantes_ns(a), instantes_ns(b)
    if pd.api.types.is_integer_dtype(a) and pd.api.types.is_integer_dtype(b):
        return pd.array(a, dtype="Int64"), pd.array(b, dtype="Int64")

    def texto(s):
        return pd.Series([None if _es_nulo(v) else str(v) for v in s.astype(object)], dtype=object)
    return texto(a), texto(b)


def combinar_codigos(pares, n_a, nulo_es_valor=False):
    """Código entero por fila a partir de pares (valores_a, valores_b) de
    varias columnas: la misma combinación, el mismo código en las dos tablas.
    Con algún nulo: -1, salvo nulo_es_valor=True (nulo == nulo)."""
    n = n_a + (len(pares[0][1]) if pares else 0)
    total = np.zeros(n, dtype="int64")
    nula = np.zeros(n, dtype=bool)
    for va, vb in pares:
        valores = pd.concat([pd.Series(va), pd.Series(vb)], ignore_index=True)
        codigos, unicos = pd.factorize(valores)
        if nulo_es_valor:
            codigos = np.where(codigos < 0, len(unicos), codigos)
        else:
            nula |= codigos < 0
        total = pd.factorize(total * (len(unicos) + 2) + (codigos + 1))[0].astype("int64")
    total[nula] = -1
    return total[:n_a], total[n_a:]


def codigos_clave(a, b, columnas, nulo_es_valor=False):
    """Códigos enteros de las filas de dos tablas por `columnas`: la misma
    combinación de valores tiene el mismo código en las dos. Una fila con algún
    valor nulo tiene código -1, salvo con nulo_es_valor=True (nulo == nulo)."""
    return combinar_codigos([_valores_clave(a[c], b[c]) for c in columnas], len(a), nulo_es_valor)


def _coincidencias(pares, con_valor, filas_a, filas_b, contenido_nuevos, contenido_semilla):
    """Posiciones de filas_b (en la semilla) que coinciden con alguna de filas_a
    (en la descarga) en las columnas de la clave con_valor y en el contenido:
    el texto canónico de las columnas que tienen las dos, con nulo == nulo.
    El contenido solo se pide para las filas que comparten esa parte de la
    clave (posiciones ordenadas, como las recibe cada función)."""
    def trozo(valores, filas):
        return pd.Series(valores).iloc[filas].reset_index(drop=True)

    if con_valor:
        ka, kb = combinar_codigos([(trozo(pares[c][0], filas_a), trozo(pares[c][1], filas_b))
                                   for c in con_valor], len(filas_a))
        filas_a = filas_a[(ka >= 0) & np.isin(ka, kb[kb >= 0])]
        filas_b = filas_b[(kb >= 0) & np.isin(kb, ka[ka >= 0])]
    if len(filas_a) == 0 or len(filas_b) == 0:
        return filas_b[:0]
    cont_a = contenido_nuevos(filas_a).reset_index(drop=True)
    cont_b = contenido_semilla(filas_b).reset_index(drop=True)
    comunes = [c for c in cont_b.columns if c in cont_a.columns]
    if not comunes:
        return filas_b[:0]   # sin contenido que comparar no se puede saber si está: se añade
    comparar = [(trozo(pares[c][0], filas_a), trozo(pares[c][1], filas_b)) for c in con_valor]
    comparar += [(texto_canonico(cont_a[c]).reset_index(drop=True),
                  texto_canonico(cont_b[c]).reset_index(drop=True)) for c in comunes]
    ta, tb = combinar_codigos(comparar, len(filas_a), nulo_es_valor=True)
    return filas_b[np.isin(tb, ta)]


def _patrones(nulos, filas):
    """(patrón de nulos de la clave, filas con ese patrón) de las posiciones `filas`."""
    if len(filas) == 0:
        return []
    return [(patron, filas[(nulos[filas] == patron).all(axis=1)])
            for patron in np.unique(nulos[filas], axis=0)]


def seleccionar_semilla(claves_nuevos, claves_semilla, contenido_nuevos=None,
                        contenido_semilla=None, en_ambito=None):
    """Motivo de cada fila de la semilla: ANADIDA, PRESENTE_CLAVE,
    PRESENTE_CONTENIDO o FUERA_AMBITO.

    claves_nuevos / claves_semilla: DataFrames con las columnas de la clave
    estable de todas las filas de la descarga (con las semillas ya
    incorporadas) y de la semilla. Una fila con la clave completa se añade
    solo si su clave no aparece en la descarga. Una fila de la semilla y otra
    de la descarga con algún valor de la clave nulo en una de las dos (p.ej.
    una fecha que el código antiguo no supo leer) son la misma entrada si
    coinciden en las columnas de la clave que tiene la incompleta y en el
    contenido, en los dos sentidos:
    - una fila de la semilla con la clave incompleta no se añade si coincide
      con alguna fila de la descarga;
    - una con la clave completa que no está en la descarga tampoco, si
      coincide con una fila de la descarga con la clave incompleta (p.ej. la
      misma entrada sembrada antes desde otro publicado que no supo leer su
      fecha): así el orden de las semillas no duplica entradas. Hace falta
      alguna columna de la clave con valor en esa fila de la descarga.
    contenido_nuevos(filas) y contenido_semilla(filas) devuelven las columnas
    de contenido para las posiciones dadas (así la descarga no tiene que estar
    entera en memoria). Sin ellas, las filas con la clave incompleta se añaden
    (no se puede comprobar que ya estén).
    en_ambito (booleano por fila de la semilla): las filas de fuera del ámbito
    de la descarga (p.ej. años que no se han vuelto a descargar) no se
    comparan ni se añaden: FUERA_AMBITO (no se sabe si siguen publicadas).
    """
    columnas = list(claves_semilla.columns)
    pares = {c: _valores_clave(claves_nuevos[c], claves_semilla[c]) for c in columnas}
    ca, cb = combinar_codigos(list(pares.values()), len(claves_nuevos))
    motivo = np.full(len(claves_semilla), ANADIDA, dtype=object)
    dentro = (np.ones(len(claves_semilla), dtype=bool) if en_ambito is None
              else np.asarray(en_ambito, dtype=bool))
    motivo[~dentro] = FUERA_AMBITO
    completa = cb >= 0
    motivo[dentro & completa & np.isin(cb, ca[ca >= 0])] = PRESENTE_CLAVE
    if contenido_nuevos is None or contenido_semilla is None:
        return motivo

    # Columnas de la clave sin valor en cada fila (tras normalizar)
    def nulos(lado):
        return np.column_stack([pd.isna(pd.Series(pares[c][lado])).to_numpy() for c in columnas])

    def comparar(con_valor, filas_a, filas_b):
        return _coincidencias(pares, con_valor, filas_a, filas_b, contenido_nuevos, contenido_semilla)

    # 1. Filas de la semilla con la clave incompleta frente a toda la descarga
    todas = np.arange(len(claves_nuevos))
    for patron, filas_b in _patrones(nulos(1), np.flatnonzero(dentro & ~completa)):
        con_valor = [c for c, nulo in zip(columnas, patron) if not nulo]
        motivo[comparar(con_valor, todas, filas_b)] = PRESENTE_CONTENIDO
    # 2. Filas de la semilla con la clave completa que no está, frente a las de
    #    la descarga con la clave incompleta
    ausentes = np.flatnonzero(completa & (motivo == ANADIDA))
    nulos_a = nulos(0) if len(ausentes) else np.zeros((0, len(columnas)), dtype=bool)
    for patron, filas_a in _patrones(nulos_a, np.flatnonzero(nulos_a.any(axis=1) & ~nulos_a.all(axis=1))):
        con_valor = [c for c, nulo in zip(columnas, patron) if not nulo]
        motivo[comparar(con_valor, filas_a, ausentes)] = PRESENTE_CONTENIDO
        ausentes = np.flatnonzero(completa & (motivo == ANADIDA))
    return motivo


def informe_semilla(motivo, origen, claves=None, n_ejemplos=5):
    """Recuento de seleccionar_semilla: filas leídas, añadidas, descartadas por
    clave o por contenido presentes, fuera del ámbito y unos ejemplos de cada
    caso: la clave de las primeras filas, con claves(filas) -> DataFrame (o un
    DataFrame)."""
    motivo = np.asarray(motivo, dtype=object)
    informe = {"origen": origen, "leidas": len(motivo),
               "anadidas": int((motivo == ANADIDA).sum()),
               "descartadas_clave": int((motivo == PRESENTE_CLAVE).sum()),
               "descartadas_contenido": int((motivo == PRESENTE_CONTENIDO).sum()),
               "fuera_ambito": int((motivo == FUERA_AMBITO).sum()),
               "ejemplos": {}}
    if isinstance(claves, pd.DataFrame):
        tabla = claves
        claves = lambda filas: tabla.iloc[filas]  # noqa: E731
    for caso in (ANADIDA, PRESENTE_CLAVE, PRESENTE_CONTENIDO, FUERA_AMBITO):
        filas = np.flatnonzero(motivo == caso)[:n_ejemplos]
        informe["ejemplos"][caso] = [] if claves is None or len(filas) == 0 else [
            tuple(None if _es_nulo(v) else str(v) for v in fila)
            for fila in claves(filas).itertuples(index=False)]
    return informe


def imprimir_informe_semilla(informe):
    """Imprime el informe de una semilla (informe_semilla). Si trae
    'fuera_ambito_detalle' ({etiqueta: filas}), también ese desglose."""
    print(f"   🌱 Semilla {informe.get('ruta', '')} ({informe['origen']}): {informe['leidas']:,} filas leídas → "
          f"{informe['anadidas']:,} añadidas; descartadas: {informe['descartadas_clave']:,} con la clave "
          f"presente y {informe['descartadas_contenido']:,} con el contenido presente")
    if informe.get("fuera_ambito"):
        detalle = informe.get("fuera_ambito_detalle") or {}
        print(f"   ⚠ {informe['fuera_ambito']:,} filas de la semilla fuera del ámbito de esta ejecución "
              f"(no se han vuelto a descargar; no se añaden)"
              + (": " + ", ".join(f"{k}: {n:,}" for k, n in detalle.items()) if detalle else ""))
    for caso, ejemplos in informe["ejemplos"].items():
        for ejemplo in ejemplos[:3]:
            print(f"      · {caso}: {ejemplo}")


def _armonizar(semilla, nuevos):
    """Columnas de la semilla con el tipo que tienen en `nuevos` cuando se puede
    sin perder ningún valor (fechas guardadas como texto, números como texto):
    así unir las dos tablas no cambia el tipo de las columnas de la descarga."""
    semilla = semilla.copy()
    for col in semilla.columns:
        if col not in nuevos.columns or semilla[col].dtype == nuevos[col].dtype:
            continue
        destino, origen = nuevos[col], semilla[col]
        try:
            if pd.api.types.is_datetime64_any_dtype(destino):
                convertida = pd.Series(instantes_ns(origen), index=origen.index)
                convertida = pd.to_datetime(convertida, unit="ns", utc=True)
                if destino.dt.tz is None:
                    convertida = convertida.dt.tz_localize(None)
                else:
                    convertida = convertida.dt.tz_convert(destino.dt.tz)
                convertida = convertida.astype(destino.dtype)
            elif pd.api.types.is_numeric_dtype(destino) and not pd.api.types.is_bool_dtype(destino):
                convertida = pd.to_numeric(origen, errors="coerce")
            else:
                continue
        except (TypeError, ValueError, OverflowError):
            continue
        if int(convertida.isna().sum()) == int(origen.isna().sum()):
            semilla[col] = convertida
    return semilla


def sembrar(nuevos, semilla, clave, origen=ORIGEN_SEMILLA, contenido=None, en_ambito=None):
    """Incorpora una instantánea publicada (`semilla`) como la más antigua.

    - Una fila de la semilla se añade solo si su clave estable (`clave`) no
      aparece en `nuevos`, que es la descarga nueva más las semillas ya
      incorporadas (sembrar dos veces la misma semilla no añade nada). Nunca
      se modifica ni se duplica una fila de `nuevos`. Si la semilla trae
      varias filas con la misma clave ausente se añaden todas (así se
      publicaron).
    - Filas con algún valor nulo en la clave: una de la semilla se considera
      presente si alguna fila de `nuevos` coincide en las columnas de la
      clave que sí tiene y en `contenido`; y una de la semilla con la clave
      completa ausente, si coincide así con una fila de `nuevos` con la clave
      incompleta (ver seleccionar_semilla); si no, se añaden. Por defecto,
      `contenido` son las columnas comunes (salvo la clave y las de control)
      con algún valor en las filas de la descarga (_origen nulo): las que solo
      traen semillas anteriores no cuentan, y sembrar otra vez da lo mismo.
    - en_ambito (booleano por fila de la semilla): las filas de fuera del
      ámbito de la descarga no se añaden (FUERA_AMBITO en el informe).
    - Las columnas de la semilla se pasan al tipo que tienen en `nuevos`
      cuando se puede sin perder valores (fechas o números guardados como
      texto); si no, pandas elige un tipo común al unirlas.
    - Marcas: las filas de `nuevos` llevan _origen nulo (o el que ya tenían)
      y _en_ultima_descarga=True salvo que ya la trajeran; las añadidas,
      _origen=origen (o el suyo si ya lo traían) y _en_ultima_descarga=False.
    - Columnas: las de `nuevos`, luego _origen y _en_ultima_descarga y al
      final las que solo tiene la semilla. En las filas añadidas quedan nulas
      las columnas que la semilla no tiene.

    Devuelve (DataFrame, informe) con el informe de informe_semilla.
    """
    clave = [clave] if isinstance(clave, str) else list(clave)
    nuevos = nuevos.reset_index(drop=True)
    semilla = _armonizar(semilla.reset_index(drop=True), nuevos)
    excluir = set(COLUMNAS_META) | set(COLUMNAS_SEMILLA) | set(clave)
    if contenido is None:
        descarga = nuevos[nuevos["_origen"].isna()] if "_origen" in nuevos.columns else nuevos
        contenido = [c for c in semilla.columns if c in nuevos.columns and c not in excluir
                     and descarga[c].notna().any()]
    contenido = [c for c in contenido if c in nuevos.columns and c in semilla.columns]

    motivo = seleccionar_semilla(
        nuevos[clave], semilla[clave],
        lambda filas: nuevos.loc[filas, contenido],
        lambda filas: semilla.loc[filas, contenido], en_ambito)

    salida = nuevos.copy()
    if "_origen" not in salida.columns:
        salida["_origen"] = pd.Series([None] * len(salida), dtype=object)
    if "_en_ultima_descarga" not in salida.columns:
        salida["_en_ultima_descarga"] = True
    anadidas = semilla.loc[motivo == ANADIDA].copy()
    propio = anadidas["_origen"] if "_origen" in anadidas.columns else pd.Series(None, index=anadidas.index)
    anadidas["_origen"] = propio.astype(object).where(propio.notna(), origen)
    anadidas["_en_ultima_descarga"] = False
    columnas = list(salida.columns) + [c for c in anadidas.columns if c not in salida.columns]
    out = pd.concat([salida, anadidas], ignore_index=True, sort=False)[columnas]
    out["_en_ultima_descarga"] = out["_en_ultima_descarga"].astype(bool)
    return out, informe_semilla(motivo, origen, semilla[clave])
