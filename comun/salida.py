"""
SALIDA: PARQUET ACUMULADO Y METADATOS DEL PORTAL
================================================
Escritura de los scrapers autonómicos y municipales que comparten el mismo
bloque (Castilla y León, Extremadura, La Rioja, Murcia, los contratos
menores de Andalucía, Canarias, Castilla-La Mancha, la Comunitat Valenciana
y los municipios). Antes, cada script tenía su copia; aquí queda una sola,
con el mismo código.

- guardar_json: metadatos del portal (catálogos, fichas) con sus versiones
  (comun.historico.guardar_version).
- escribir_parquet: todas las columnas como texto (_como_texto), salvo
  _en_ultima_descarga, booleana; si no cambia, no se toca, y si cambia la
  versión anterior pasa a _historico/.
- acumular_fichero: los registros de un fichero crudo a lo largo de todas
  sus versiones (comun.historico.acumular), con la fecha de cada versión
  (fecha_version).
- construir_parquet: el Parquet de una serie desde todos sus ficheros,
  conservando las filas de los que ya no están.

Cada script tiene sus propias columnas de metadatos (METADATOS_ORIGEN,
ORDEN_METADATOS) y su propia lectura (leer_tabla): lo que depende de ellas
lo recibe como argumento (solo de palabra), y el script conserva una
envoltura de una línea que le pasa los suyos, buscados en el módulo en cada
llamada como antes. Por eso ordenar_columnas, de 4 líneas, sigue en cada
script.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from comun.historico import HISTORICO, acumular, guardar_version, leer_registros, versiones


# ─────────────────────────────────────────────────────────────
# 1. Fechas de las versiones de los ficheros crudos
# ─────────────────────────────────────────────────────────────

def iso(momento):
    """Fecha ISO 8601 en UTC al segundo: '2026-09-27T14:43:00Z' (ordenable como texto)."""
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fecha_version(ruta):
    """Fecha de una versión de un fichero crudo: el sello que guardar_version pone
    en _historico/ (fecha de esa copia) o, para la copia actual, su fecha de
    modificación. Al pasar a _historico/ la copia conserva la misma fecha."""
    ruta = Path(ruta)
    if ruta.parent.name == HISTORICO:
        sellos = re.findall(r"__(\d{8}T\d{6}Z)", ruta.name)
        if sellos:
            return iso(datetime.strptime(sellos[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc))
    return iso(datetime.fromtimestamp(ruta.stat().st_mtime, timezone.utc))


# ─────────────────────────────────────────────────────────────
# 2. Escritura: metadatos del portal y Parquet como texto
# ─────────────────────────────────────────────────────────────

def guardar_json(destino, datos):
    """Guarda metadatos del portal (catálogo, fichas) conservando sus versiones."""
    contenido = json.dumps(datos, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    return guardar_version(destino, contenido)


def _como_texto(serie):
    """Serie de texto (None = nulo) para escribirla como string en Parquet."""
    if isinstance(serie.dtype, pd.StringDtype):
        return serie
    valores = serie.astype(object)
    if pd.api.types.infer_dtype(valores, skipna=True) in ("string", "empty"):
        return valores
    return valores.map(lambda v: v if isinstance(v, str) else (None if pd.isna(v) else str(v)))


def escribir_parquet(df, destino, *, ordenar_columnas):
    """Parquet con todas las columnas como texto (_en_ultima_descarga booleana), en
    el orden de ordenar_columnas(df), la del script (sus metadatos al final).
    Se escribe en un temporal y pasa por guardar_version: la versión anterior
    del Parquet queda en _historico/."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    df = ordenar_columnas(df).copy()
    campos = []
    for columna in df.columns:
        if columna == "_en_ultima_descarga":
            df[columna] = df[columna].astype(bool)
            campos.append(pa.field(columna, pa.bool_()))
        else:
            df[columna] = _como_texto(df[columna])
            campos.append(pa.field(str(columna), pa.string()))
    tabla = pa.Table.from_pandas(df, schema=pa.schema(campos), preserve_index=False)
    tmp = destino.with_name(f".{destino.name}.nuevo")
    try:
        pq.write_table(tabla, tmp, compression="snappy")
        return guardar_version(destino, desde=tmp)
    finally:
        if tmp.exists():
            tmp.unlink()


# ─────────────────────────────────────────────────────────────
# 3. Registros acumulados de todas las versiones
# ─────────────────────────────────────────────────────────────

def acumular_fichero(actual, rel, anterior, metadatos, manifiesto, resumen, *, metadatos_origen, leer_tabla):
    """Registros de un fichero crudo a lo largo de todas sus versiones.

    anterior: filas de este fichero en el Parquet anterior (o None). Solo se
    aplican las versiones posteriores a su _ultima_descarga, en orden
    cronológico, con acumular(): nada de lo visto se pierde; lo que el portal
    retira o cambia queda con _en_ultima_descarga=False. Después se aplica la
    última comprobación: sin cambios (actualiza _ultima_descarga) o retirado
    por el portal (todo el fichero pasa a _en_ultima_descarga=False).

    Del script: metadatos_origen (sus columnas de origen, que no cuentan al
    comparar registros entre versiones) y leer_tabla (su lectura de cada versión).
    """
    info = manifiesto.get(rel)
    corte = None
    if anterior is not None and len(anterior) and "_ultima_descarga" in anterior.columns:
        corte = anterior["_ultima_descarga"].dropna().astype(str).max()
    lista = versiones(actual)
    comprobado = info.get("comprobado")
    posterior = comprobado is not None and (corte is None or comprobado > corte)
    retirado = info.get("publicado") is False and posterior
    reconfirmado = (posterior and not retirado and bool(lista) and lista[-1] == Path(actual)
                    and comprobado > fecha_version(actual))
    ignorar = metadatos_origen
    ultima = None
    for version in lista:
        fecha = fecha_version(version)
        nueva = corte is None or fecha > corte
        if not nueva and not (reconfirmado and version == lista[-1]):
            continue
        try:
            df, avisos = leer_tabla(version)
        except Exception as e:
            resumen.fallidos.append(f"{rel}: no se pudo leer la versión {version.name}: {e}")
            continue
        resumen.avisos.extend(avisos)
        df = df.copy()
        for columna, valor in metadatos.items():
            df[columna] = valor
        df["_fecha_descarga"] = fecha
        ultima = df
        if not nueva:
            continue
        try:
            anterior = acumular(anterior, df, fecha, ignorar=ignorar)
        except ValueError:
            resumen.avisos.append(f"{rel}: la versión del {fecha} no tiene filas; no se marca nada como retirado")
    if anterior is None or not len(anterior):
        return anterior
    if retirado:
        anterior = acumular(anterior, pd.DataFrame(), comprobado, ignorar=ignorar, permitir_vacio=True)
    elif reconfirmado and ultima is not None and len(ultima):
        anterior = acumular(anterior, ultima, comprobado, ignorar=ignorar)
    return anterior


def construir_parquet(destino, ficheros, raw, manifiesto, resumen, *, metadatos_origen, acumular_fichero,
                      escribir_parquet):
    """Genera `destino` con los registros acumulados de `ficheros`
    (lista de (ruta_actual, rel, metadatos)), cada uno desde todas sus
    versiones en raw/ con el código actual.

    Las filas de ficheros que ya no se procesan (retirados, otro formato...) se
    conservan: si el fichero sigue en raw/ se vuelve a procesar con sus
    versiones; si no, se copian tal cual del Parquet anterior.

    Del script: metadatos_origen, acumular_fichero y escribir_parquet (los
    suyos, con sus columnas y su lectura).
    """
    destino = Path(destino)
    try:
        previo = leer_registros(destino)
    except Exception as e:
        resumen.fallidos.append(f"{destino.name}: no se pudo leer el Parquet anterior ({e}); no se regenera")
        return None
    por_fichero = {}
    if previo is not None and "_archivo_origen" in previo.columns:
        por_fichero = {str(k): g for k, g in previo.groupby("_archivo_origen", sort=False)}
    ficheros = list(ficheros)
    procesados = {rel for _, rel, _ in ficheros}
    for rel, grupo in por_fichero.items():
        if rel not in procesados and (Path(raw) / rel).exists():
            metadatos = {c: grupo[c].iloc[0] for c in metadatos_origen
                         if c in grupo.columns and c not in ("_hoja", "_fecha_descarga")}
            ficheros.append((Path(raw) / rel, rel, metadatos))
            procesados.add(rel)
    partes = []
    for actual, rel, metadatos in ficheros:
        # Con el código actual desde todas las versiones del crudo (regla 3): si solo se aplicaran
        # las versiones posteriores al Parquet anterior, un arreglo de lectura (p.ej. la
        # codificación cp850 de 2014-2018 en Murcia) no llegaría nunca a las filas ya guardadas
        por_fichero.pop(rel, None)
        registros = acumular_fichero(actual, rel, None, metadatos, manifiesto, resumen)
        if registros is not None and len(registros):
            partes.append(registros)
    for rel, grupo in por_fichero.items():
        resumen.avisos.append(f"{destino.name}: {rel} ya no está en raw/; se conservan sus {len(grupo):,} filas")
        partes.append(grupo)
    if not partes:
        return None
    df = pd.concat(partes, ignore_index=True, sort=False)
    estado = escribir_parquet(df, destino)
    retiradas = int((~df["_en_ultima_descarga"].astype(bool)).sum())
    resumen.parquets.append((destino.name, len(df), len(df.columns), retiradas))
    print(f"  💾 {destino.name}: {len(df):,} filas ({estado})")
    return df
