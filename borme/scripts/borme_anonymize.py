#!/usr/bin/env python3
"""
BORME: versiones públicas con las personas seudonimizadas
=========================================================
Toma la salida cruda del parser (borme_empresas.parquet + borme_cargos.parquet)
y genera las versiones que se publican.

Los nombres de las personas físicas de los cargos se sustituyen por un código
seudónimo, `persona_hash` (SHA-256 del nombre con una sal, truncado a 16
caracteres hexadecimales; ver hash_persona). Es una seudonimización, no una
anonimización:
  - El código es el mismo para el mismo nombre, en cualquier fecha y empresa. Es
    lo que permite seguir a un administrador entre empresas.
  - Por eso junta a los homónimos: dos personas que se llaman igual comparten
    código y cuentan como una sola. Y al revés: una misma persona escrita de dos
    formas (con o sin tilde, en otro orden) tiene dos códigos.
  - No impide reconocer a una persona cuyo nombre ya se conoce. Los cargos con
    `persona_hash` siguen siendo datos personales.

Salidas:
  1. borme_empresas_pub.parquet   — Actos mercantiles por empresa, tal como los
                                     publica el BORME (sin el objeto social)
  2. borme_cargos_pub.parquet     — Cargos con persona_hash en vez del nombre
  3. borme_grafo_admin.parquet    — Grafo empresa↔empresa por administrador
                                     compartido (build_admin_graph; main() no lo
                                     escribe)

Uso:
  python borme_anonymize.py --input ./borme_pdfs --output ./data

Fuente de los datos: Agencia Estatal Boletín Oficial del Estado (https://www.boe.es)
"""

import hashlib
import argparse
import logging
from pathlib import Path
from itertools import combinations

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)

# Columnas de control de borme_batch_parser.py (comun/historico.py): se publican con
# los datos, así se distinguen las versiones anteriores de un acto
# (_en_ultima_descarga=False) y las filas de la semilla (_origen)
COLUMNAS_CONTROL = ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga", "_origen"]


def hash_persona(name: str, salt: str = "borme_2024") -> str:
    """Código seudónimo de un nombre de persona.

    SHA-256 de "<salt>:<NOMBRE>", truncado a 16 caracteres hexadecimales. El
    nombre solo se normaliza con strip() y upper(), así que el mismo nombre da
    siempre el mismo código (los homónimos comparten código) y dos grafías del
    mismo nombre dan dos códigos. Es una seudonimización, no una anonimización
    (ver el docstring del módulo). Un nombre vacío o que no es texto da "".
    """
    if not name or not isinstance(name, str):
        return ""
    raw = f"{salt}:{name.strip().upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def anonymize_empresas(df_emp: pd.DataFrame) -> pd.DataFrame:
    """Limpia borme_empresas: quita campos de texto libre que podrían
    contener nombres de personas (objeto_social a veces menciona personas)."""
    log.info("Anonimizando empresas...")

    # Columnas a mantener (sin objeto_social que puede tener nombres)
    keep_cols = [
        "fecha_borme", "num_borme", "num_entrada",
        "empresa", "empresa_norm", "provincia", "cod_provincia",
        "tipo_borme", "actos", "domicilio", "capital_euros",
        "fecha_constitucion", "hoja_registral", "tomo", "inscripcion",
        "fecha_inscripcion", "pdf_filename",
    ]
    cols = [c for c in keep_cols + COLUMNAS_CONTROL if c in df_emp.columns]
    # Sin .copy(): no se modifica y la copia duplicaba la tabla (9,6 M de filas con la semilla)
    df = df_emp[cols]

    # El domicilio social se publica tal cual en el BORME ("CALLE NUM (MUNICIPIO)")
    # y se conserva completo.

    log.info(f"  {len(df):,} filas, {df['empresa_norm'].nunique():,} empresas")
    return df


def _donde(condicion: pd.Series, valores: pd.Series, otros: pd.Series) -> pd.Series:
    """valores.where(condicion, otros). Si las dos son texto de Arrow con el mismo tipo
    (pandas 3), con if_else de Arrow y el mismo tipo de resultado: where pasa por objetos
    de Python (1,4 GB más con los 17,8 M de cargos de la semilla)."""
    if (isinstance(valores.dtype, pd.StringDtype) and valores.dtype.storage == "pyarrow"
            and valores.dtype == otros.dtype):
        resultado = pc.if_else(pa.array(condicion.to_numpy(dtype=bool)),
                               pa.chunked_array(pa.array(valores.array)), pa.chunked_array(pa.array(otros.array)))
        return pd.Series(pd.array(resultado, dtype=valores.dtype), index=valores.index, name=valores.name)
    return valores.where(condicion, otros)


def anonymize_cargos(df_car: pd.DataFrame) -> pd.DataFrame:
    """Sustituye el nombre de cada persona por su código seudónimo (persona_hash).

    Las filas de la semilla no traen el nombre: conservan el persona_hash publicado."""
    log.info("Anonimizando cargos...")

    # Copia superficial: solo se sustituyen columnas enteras (una copia completa duplicaba
    # la tabla, 17,8 M de filas con la semilla)
    df = df_car.copy(deep=False)
    nombres = df["persona"] if "persona" in df.columns else pd.Series(None, index=df.index, dtype=object)
    hashes = nombres.apply(hash_persona)
    # Filas de la semilla (release publicado, borme_batch_parser.py --semilla): no
    # traen el nombre sino el hash publicado, que se conserva tal cual
    if "persona_hash" in df.columns:
        hashes = _donde(nombres.notna(), hashes, df["persona_hash"])
    df["persona_hash"] = hashes

    # Quitar el nombre
    df = df.drop(columns=["persona"], errors="ignore")

    # Reordenar
    col_order = [
        "fecha_borme", "num_entrada", "empresa", "empresa_norm",
        "provincia", "hoja_registral", "tipo_acto", "cargo",
        "persona_hash", "pdf_filename",
    ] + COLUMNAS_CONTROL
    cols = [c for c in col_order if c in df.columns]
    df = df[cols]

    log.info(f"  {len(df):,} filas, {df['persona_hash'].nunique():,} personas (hasheadas)")
    return df


def build_admin_graph(df_car_anon: pd.DataFrame, max_empresas_per_admin: int = 20) -> pd.DataFrame:
    """Construye grafo de empresas que comparten administrador.

    Cada fila = un par de empresas con al menos un admin en común.
    Admins en >max_empresas_per_admin empresas se excluyen (profesionales
    de despachos que administran cientos de sociedades — no son señal).

    persona_hash junta a los homónimos: un par de empresas puede salir conectado
    por dos personas distintas que se llaman igual. Tenlo en cuenta antes de
    leerlo como señal.

    Columnas: empresa_a, empresa_b, n_admins_compartidos, admin_hashes.
    """
    log.info("Construyendo grafo de administradores compartidos...")

    # Solo nombramientos/reelecciones (admins activos)
    admins = df_car_anon[
        df_car_anon["tipo_acto"].isin(["nombramiento", "reeleccion"])
    ].copy()

    # Personas con >1 empresa
    persona_empresas = admins.groupby("persona_hash")["empresa_norm"].apply(
        lambda x: sorted(set(x))
    )
    multi = persona_empresas[persona_empresas.apply(len).between(2, max_empresas_per_admin)]
    log.info(f"  {len(multi):,} admins en 2-{max_empresas_per_admin} empresas")

    excluded = persona_empresas[persona_empresas.apply(len) > max_empresas_per_admin]
    if len(excluded) > 0:
        log.info(f"  {len(excluded):,} admins excluidos (>{max_empresas_per_admin} empresas — profesionales)")

    pair_counts = {}
    pair_hashes = {}
    processed = 0

    for persona_hash, empresas in multi.items():
        for a, b in combinations(empresas, 2):
            key = (a, b)
            pair_counts[key] = pair_counts.get(key, 0) + 1
            if key not in pair_hashes:
                pair_hashes[key] = set()
            pair_hashes[key].add(persona_hash)

        processed += 1
        if processed % 200_000 == 0:
            log.info(f"    {processed:,}/{len(multi):,} admins procesados, {len(pair_counts):,} pares")

    if not pair_counts:
        log.info("  Sin conexiones encontradas")
        return pd.DataFrame()

    log.info(f"  Generando DataFrame con {len(pair_counts):,} pares...")
    rows = []
    for (a, b), count in pair_counts.items():
        rows.append({
            "empresa_a": a,
            "empresa_b": b,
            "n_admins_compartidos": count,
            "admin_hashes": "|".join(sorted(pair_hashes[(a, b)])),
        })

    grafo = pd.DataFrame(rows)
    grafo = grafo.sort_values("n_admins_compartidos", ascending=False)
    log.info(f"  {len(grafo):,} pares de empresas conectadas")
    log.info(f"  Max admins compartidos: {grafo['n_admins_compartidos'].max()}")

    return grafo


def main():
    parser = argparse.ArgumentParser(description="BORME Anonymizer")
    parser.add_argument("--input", required=True,
                        help="Carpeta con borme_empresas.parquet y borme_cargos.parquet")
    parser.add_argument("--output", required=True,
                        help="Carpeta de salida para datos anonimizados")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Cargar. Las tablas se seudonimizan y se escriben de una en una: con la semilla del
    # release son 9,6 M y 17,8 M de filas y las dos a la vez (más sus copias) llegaban a
    # 10 GiB. El número de filas de las dos sale de sus metadatos antes de
    # nada (como cuando se cargaban las dos al empezar: si falta una tabla, no se escribe nada)
    log.info(f"Cargando datos de {input_dir}...")
    ruta_emp = input_dir / "borme_empresas.parquet"
    ruta_car = input_dir / "borme_cargos.parquet"
    n_emp, n_car = (pq.ParquetFile(ruta).metadata.num_rows for ruta in (ruta_emp, ruta_car))
    log.info(f"  Empresas: {n_emp:,} filas")
    log.info(f"  Cargos: {n_car:,} filas")

    # Seudonimizar y guardar
    path_emp = output_dir / "borme_empresas_pub.parquet"
    df_emp_pub = anonymize_empresas(pd.read_parquet(ruta_emp))
    filas_emp, unicas_emp = len(df_emp_pub), df_emp_pub['empresa_norm'].nunique()
    df_emp_pub.to_parquet(path_emp, index=False, engine="pyarrow")
    del df_emp_pub

    df_car_pub = anonymize_cargos(pd.read_parquet(ruta_car))
    log.info("\nGuardando...")
    log.info(f"  {path_emp} ({path_emp.stat().st_size / 1e6:.1f} MB)")

    path_car = output_dir / "borme_cargos_pub.parquet"
    df_car_pub.to_parquet(path_car, index=False, engine="pyarrow")
    log.info(f"  {path_car} ({path_car.stat().st_size / 1e6:.1f} MB)")
    filas_car = len(df_car_pub)
    del df_car_pub

    # Resumen
    log.info(f"\n{'='*60}")
    log.info("ANONIMIZACIÓN COMPLETADA")
    log.info(f"{'='*60}")
    log.info(f"  Empresas:  {filas_emp:,} filas ({unicas_emp:,} únicas)")
    log.info(f"  Cargos:    {filas_car:,} filas (personas hasheadas)")
    log.info("")
    log.info("  ✅ Datos listos para subir al repo público")
    log.info("  ⚠️  NO subir borme_empresas.parquet ni borme_cargos.parquet originales")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
