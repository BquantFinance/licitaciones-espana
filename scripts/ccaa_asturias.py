import argparse
import re
import sys
import requests
import pandas as pd
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
import time
import logging

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import (  # noqa: E402
    COLUMNAS_META, HISTORICO, acumular, guardar_version, imprimir_informe_semilla, sembrar, versiones,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

FIRST_YEAR = 2019
# Último año cuyo CSV consta publicado (asturias_contracts_ALL_YEARS.parquet llega a 2024).
# Los siguientes se piden hasta el año en curso: el fichero de un año incluye las altas
# hasta marzo del siguiente y se publica después, así que un 404 en esos últimos años
# significa "aún no publicado" y no es un error.
LAST_VERIFIED_YEAR = 2024
DATASET_TEMPLATE = "dataset-contratacion-centralizada-{year}.csv"
PATRON_DATASET = re.compile(r"dataset-contratacion-centralizada-(\d{4})\.csv$")
PARQUET_NAME = "asturias_contracts_ALL_YEARS.parquet"
# Clave estable de una inscripción del registro de contratos (para la semilla). En el
# publicado v2026.02 es única salvo 6 filas vacías y un número repetido en origen
# (00012791-24, dos contratos distintos): sembrar() compara esos casos por contenido.
CLAVE = ["year", "Nº INSCRIPCION"]


def version_date(path):
    """Fecha de una versión de un CSV crudo: el sello que guardar_version pone en
    _historico/ (fecha de esa copia) o, para la copia actual, su fecha de modificación."""
    path = Path(path)
    if path.parent.name == HISTORICO:
        stamps = re.findall(r"__(\d{8}T\d{6}Z)", path.name)
        if stamps:
            return datetime.strptime(stamps[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")


class AsturiasToParquet:
    """
    Descarga TODOS los años de Asturias, maneja duplicados, 
    fuerza tipos compatibles y guarda en Parquet.

    Sesgo del superviviente (comun/historico.py):
    - Cada CSV descargado se guarda tal cual en raw/ con guardar_version: si el
      Principado lo cambia, la versión anterior pasa a raw/_historico/.
    - El Parquet se construye desde todas las versiones de cada CSV con
      acumular(): lo que se retira o cambia se conserva con
      _en_ultima_descarga=False. Un año que deja de servirse (404) después de
      haberse descargado conserva sus filas, marcadas como retiradas.
    - El Parquet anterior pasa a _historico/ (guardar_version).
    - Semillas (--semilla, p.ej. el parquet publicado en v2026.02): se añaden las
      inscripciones (CLAVE) de los años procesados que no están en la descarga,
      con _origen; en las ejecuciones siguientes se conservan. Traen los errores
      conocidos del publicado (README): IVA de 2023 multiplicado por 10 y
      expedientes con letras convertidos en NaN.
    """
    
    def __init__(self, output_dir=None, last_year=None, semillas=()):
        self.base_url = "https://descargas.asturias.es/asturias/opendata/SectorPublico/contratacion"
        # Por defecto <repo>/ccaa_asturias (ruta documentada en el README), sin depender del cwd
        if output_dir is None:
            output_dir = Path(__file__).resolve().parent.parent / "ccaa_asturias"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Originales tal cual (guardar_version: las versiones anteriores, en raw/_historico/)
        self.raw_dir = self.output_dir / "raw"
        self.semillas = [Path(semilla) for semilla in semillas]
        self.checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        
        # Antes la lista acababa en 2024 fija: 2025 y siguientes no se descargaban nunca
        if last_year is None:
            last_year = date.today().year
        self.datasets = {
            year: DATASET_TEMPLATE.format(year=year)
            for year in range(FIRST_YEAR, max(last_year, LAST_VERIFIED_YEAR) + 1)
        }
        self.all_dfs = []
    
    def deduplicate_columns(self, df):
        """Renombra columnas duplicadas."""
        if not df.columns.duplicated().any():
            return df
        
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            dup_mask = cols == dup
            new_names = [f"{dup}_dup{i+1}" if i > 0 else dup for i in range(dup_mask.sum())]
            cols.loc[dup_mask] = new_names
        
        df.columns = cols
        return df
    
    @staticmethod
    def decode_content(content_bytes):
        """Los CSV están en Windows-1252: leídos como latin-1, las comillas tipográficas,
        guiones largos, '€' o '…' quedaban como caracteres de control invisibles
        (\\x93, \\x96, \\x80...). latin-1 solo si el fichero no es cp1252 válido."""
        try:
            return content_bytes.decode('cp1252')
        except UnicodeDecodeError:
            return content_bytes.decode('latin-1')

    @staticmethod
    def read_csv_text(content, **kwargs):
        """read_csv de un CSV ya decodificado: el mismo para el parseo con tipos
        (parse_text) y para el texto tal cual (read_text), así salen las mismas filas."""
        return pd.read_csv(StringIO(content), sep='§', engine='python', **kwargs)

    def read_text(self, content_bytes):
        """El CSV de un año tal cual se publicó: todo texto (celda vacía = ''), con las
        mismas filas y los mismos nombres de columna que parse_year."""
        df = self.read_csv_text(self.decode_content(content_bytes), dtype=str,
                                keep_default_na=False, on_bad_lines=lambda campos: None)
        df.columns = [str(c).strip() for c in df.columns]
        return self.deduplicate_columns(df)

    def parse_year(self, content_bytes, year):
        """Parsea un año."""
        return self.parse_text(self.decode_content(content_bytes), year)

    def parse_text(self, content, year):
        """Parsea el texto de un CSV anual (el descargado o el acumulado de year_table)."""

        # Las líneas con más campos que la cabecera se descartaban en silencio
        # (on_bad_lines='skip'): ahora se avisa y se guardan tal cual para revisarlas
        # (list.append devuelve None, que para pandas es "no meter la línea en la tabla")
        bad_lines = []
        
        df = self.read_csv_text(content, on_bad_lines=bad_lines.append)
        
        # Una página HTML (error, mantenimiento...) no contiene '§' y se lee como una
        # sola columna: no es el CSV esperado y no debe mezclarse con los datos
        if df.empty or len(df.columns) < 2:
            raise ValueError(
                f"contenido inesperado ({len(df)} filas x {len(df.columns)} columnas), "
                "no parece el CSV separado por '§'"
            )

        if bad_lines:
            bad_path = self.output_dir / f"lineas_descartadas_{year}.csv"
            bad_path.write_text(
                "".join("§".join(fields) + "\n" for fields in bad_lines),
                encoding="utf-8",
            )
            logger.warning(
                f"{year} - {len(bad_lines):,} líneas con más campos que la cabecera "
                f"no caben en la tabla: guardadas en {bad_path}"
            )

        df.columns = [str(c).strip() for c in df.columns]
        df = self.deduplicate_columns(df)
        df['year'] = year
        df['source_file'] = DATASET_TEMPLATE.format(year=year)
        
        return df

    def year_table(self, year, filename, served=None):
        """Filas de un año desde todas las versiones guardadas de su CSV (raw/ y
        raw/_historico/), de la más antigua a la actual, con acumular().

        Se compara el texto publicado (read_text): que pandas infiera otro tipo en
        otra versión (21 frente a 21.0) no es un cambio, y una columna nueva no
        duplica las filas. La tabla acumulada se vuelve a leer como el CSV original
        (parse_text), así los tipos y el tratamiento de los importes son los de
        siempre. served: True si se ha descargado en esta ejecución (lo vigente
        lleva la fecha de hoy), False si ha dado 404 (sus filas pasan a retiradas)
        y None si no se ha pedido (no cambia nada)."""
        texts, last = None, None
        for version in versiones(self.raw_dir / filename):
            last = self.read_text(version.read_bytes())
            texts = acumular(texts, last, version_date(version), ignorar=())
        if texts is None:
            return None
        if served:
            texts = acumular(texts, last, self.checked_at, ignorar=())
        elif served is False:
            texts = acumular(texts, last.iloc[0:0], self.checked_at, ignorar=(), permitir_vacio=True)
        data = [c for c in texts.columns if c not in COLUMNAS_META]
        buffer = StringIO()
        texts[data].to_csv(buffer, sep='§', index=False)
        df = self.parse_text(buffer.getvalue(), year)
        if len(df) != len(texts):
            raise ValueError(f"{year}: la tabla acumulada tiene {len(texts):,} filas y al leerla salen {len(df):,}")
        for col in COLUMNAS_META:
            df[col] = texts[col].to_numpy()
        retiradas = int((~df["_en_ultima_descarga"]).sum())
        if retiradas:
            logger.info(f"{year} - {retiradas:,} filas ya no se publican (se conservan con _en_ultima_descarga=False)")
        return df

    def years_in_raw(self):
        """Años con algún CSV guardado en raw/ (también los que ya no se piden)."""
        if not self.raw_dir.is_dir():
            return set()
        nombres = [p.name for p in self.raw_dir.glob("*.csv")]
        if (self.raw_dir / HISTORICO).is_dir():
            nombres += [p.name.split("__")[0] + ".csv" for p in (self.raw_dir / HISTORICO).glob("*.csv")]
        return {int(m.group(1)) for m in map(PATRON_DATASET.search, nombres) if m}
    
    def process_year(self, year, filename):
        """Descarga y parsea un año. Devuelve None si el año aún no está publicado."""
        url = f"{self.base_url}/{filename}"
        
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {year}...")
            
            response = requests.get(url, timeout=180)
            if response.status_code == 404 and year > LAST_VERIFIED_YEAR:
                logger.warning(f"{year} - {url} no existe (404): año aún no publicado")
                return None
            # Sin esto un 404/500 (página HTML) se parseaba como si fuera el CSV del año
            response.raise_for_status()
            content_bytes = response.content
            
            logger.info(f"{year} - Downloaded: {len(content_bytes):,} bytes")
            
            df = self.parse_year(content_bytes, year)
            logger.info(f"{year} - Parsed: {len(df):,} rows x {len(df.columns)} cols")
            # Solo un CSV válido llega aquí (una página HTML ya ha fallado al parsear)
            estado = guardar_version(self.raw_dir / filename, content_bytes)
            logger.info(f"{year} - Original en raw/: {estado}")
            
            self.all_dfs.append(df)
            return True
                
        except Exception as e:
            logger.error(f"{year} - Error: {e}")
            return False
    
    def force_compatible_types(self, df):
        """
        Fuerza tipos de datos compatibles con Parquet/PyArrow.
        Convierte TODO a string o float, nunca deja object mixto.
        """
        logger.info("Forcing compatible types for Parquet...")
        
        # Patrones de columnas numéricas (intentar convertir). Sin 'Nº'/'NUMERO': son
        # identificadores (Nº INSCRIPCION, Nº EXPEDIENTE ORGANO...), no cantidades
        numeric_patterns = ['AÑO', 'ANO', 'YEAR', 'PRESUPUESTO', 'IMPORTE', 'IMP.', 'IMP ', 
                          'EURO', 'IVA', 'CANTIDAD', 'TOTAL', 'BASE']
        
        for col in df.columns:
            # pandas 3 lee el texto con dtype "str" (no "object"): con la comparación
            # '== object' los importes quedaban como texto en el Parquet
            if pd.api.types.is_string_dtype(df[col].dtype):
                # Verificar si parece numérica por nombre ('Nº EXPEDIENTE ORGANO' contiene
                # 'ANO' de ORGANO: los identificadores se excluyen explícitamente)
                looks_numeric = (
                    not col.upper().startswith('Nº')
                    and any(pat in col.upper() for pat in numeric_patterns)
                )
                
                if looks_numeric:
                    try:
                        # Solo los valores de texto llevan formato español. Los que ya son
                        # numéricos (años en que read_csv parseó la columna como int/float)
                        # se conservan: quitar el '.' de 21.0 daba 210 (IVA de 2023 x10)
                        is_text = df[col].map(lambda x: isinstance(x, str))
                        # Limpiar formato español (comas como decimales, puntos como miles)
                        cleaned = df[col].where(is_text).astype(str).str.replace('.', '', regex=False)  # Quitar separadores de miles
                        cleaned = cleaned.str.replace(',', '.', regex=False)  # Coma decimal a punto
                        cleaned = cleaned.str.replace(' ', '', regex=False)  # Quitar espacios
                        cleaned = cleaned.str.replace('€', '', regex=False)  # Quitar símbolo euro
                        cleaned = cleaned.str.replace('EUR', '', regex=False)  # Quitar EUR
                        
                        # Convertir a numérico, forzar errores a NaN
                        numeric = pd.to_numeric(cleaned, errors='coerce')
                        numeric = numeric.fillna(pd.to_numeric(df[col].where(~is_text), errors='coerce'))
                        
                        # Numérica solo si TODOS los valores con contenido lo son. Con el
                        # umbral del 50 % los demás se convertían en NaN y se perdían (p. ej.
                        # los expedientes "SUM/2019/12" de los contratos mayores)
                        has_value = df[col].notna() & df[col].astype(str).str.strip().ne('')
                        not_numeric = has_value & numeric.isna()
                        if has_value.any() and not not_numeric.any():
                            df[col] = numeric
                            logger.debug(f"  {col}: numeric")
                            continue
                        if numeric.notna().sum() / len(df) > 0.5:
                            logger.warning(
                                f"  {col}: {not_numeric.sum():,} valores no numéricos "
                                f"(p. ej. {df.loc[not_numeric, col].iloc[0]!r}): se guarda como texto"
                            )
                    except Exception:
                        pass
                
                # Si no es numérica o falló, forzar a string
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
                logger.debug(f"  {col}: string")
        
        return df
    
    def apply_seeds(self, combined, years):
        """Añade las filas de las semillas: las ya sembradas en el Parquet anterior
        (_origen) y las de --semilla. Solo entran las inscripciones (CLAVE) de los
        años procesados que no están en la descarga; nunca se modifica ni se
        duplica una fila descargada."""
        fuentes = []
        previous = self.output_dir / PARQUET_NAME
        if previous.exists():
            anterior = pd.read_parquet(previous)
            if "_origen" in anterior.columns and anterior["_origen"].notna().any():
                fuentes.append(("salida anterior", anterior[anterior["_origen"].notna()]))
        fuentes += [(str(ruta), pd.read_parquet(ruta)) for ruta in self.semillas]
        for nombre, semilla in fuentes:
            en_ambito = semilla["year"].isin(years).to_numpy() if "year" in semilla.columns else None
            combined, informe = sembrar(combined, semilla, CLAVE, en_ambito=en_ambito)
            informe["ruta"] = nombre
            imprimir_informe_semilla(informe)
        return combined

    def save_final_parquet(self, years=None):
        """Concatena, normaliza tipos y guarda."""
        if not self.all_dfs:
            logger.error("No data!")
            return None
        
        logger.info(f"\n{'='*60}")
        logger.info("CONCATENATING ALL YEARS")
        logger.info(f"{'='*60}")
        
        for df in self.all_dfs:
            logger.info(f"Year {df['year'].iloc[0]}: {len(df.columns)} cols, {len(df):,} rows")
        
        # Concatenar
        combined = pd.concat(self.all_dfs, axis=0, join='outer', ignore_index=True)
        logger.info(f"\nCombined: {len(combined):,} rows x {len(combined.columns)} cols")
        
        # FORZAR TIPOS COMPATIBLES
        combined = self.force_compatible_types(combined)
        if years is not None:
            combined = self.apply_seeds(combined, years)
        
        # Guardar Parquet (la versión anterior queda en _historico/)
        parquet_path = self.output_dir / PARQUET_NAME
        
        try:
            tmp_path = parquet_path.with_name(f".{parquet_path.name}.nuevo")
            combined.to_parquet(tmp_path, index=False, compression='snappy', engine='pyarrow')
            estado = guardar_version(parquet_path, desde=tmp_path)
            logger.info(f"\n✓✓✓ PARQUET SAVED ({estado}): {parquet_path}")
            logger.info(f"Size: {parquet_path.stat().st_size / (1024**2):.2f} MB")
        except Exception as e:
            logger.error(f"Parquet error: {e}")
            # Fallback a CSV comprimido
            csv_path = self.output_dir / "asturias_contracts_ALL_YEARS.csv.gz"
            combined.to_csv(csv_path, index=False, sep=';', encoding='utf-8-sig', compression='gzip')
            logger.info(f"✓ CSV saved instead: {csv_path}")
        
        # Muestra
        sample_path = self.output_dir / "sample_1000_rows.csv"
        combined.head(1000).to_csv(sample_path, index=False, sep=';', encoding='utf-8-sig')
        logger.info(f"✓ Sample: {sample_path}")
        
        return combined
    
    def run(self):
        failed_years = []
        unpublished_years = []
        downloaded_years = []
        for year, filename in self.datasets.items():
            result = self.process_year(year, filename)
            if result is None:
                unpublished_years.append(year)
            elif result:
                downloaded_years.append(year)
            else:
                failed_years.append(year)
            time.sleep(0.5)
        # Solo pueden faltar los últimos años: un año sin fichero seguido de otro que sí
        # existe es un hueco en los datos, no un año pendiente de publicar
        failed_years += [
            year for year in unpublished_years
            if any(later > year for later in downloaded_years)
        ]
        if failed_years:
            # No sobrescribir el Parquet completo con un dataset al que le faltan años
            logger.error(f"Años con error: {sorted(failed_years)}. No se guarda un Parquet parcial.")
            return None
        if unpublished_years:
            logger.warning(f"Años aún sin publicar (se omiten): {unpublished_years}")
        # El Parquet se construye desde todas las versiones guardadas de cada año,
        # también las de años que ya no se sirven o que esta ejecución no pide
        self.all_dfs, years = [], []
        for year in sorted(set(self.datasets) | self.years_in_raw()):
            served = True if year in downloaded_years else (False if year in unpublished_years else None)
            table = self.year_table(year, DATASET_TEMPLATE.format(year=year), served)
            if table is not None:
                self.all_dfs.append(table)
                years.append(year)
        return self.save_final_parquet(years)

def main(argv=None):
    parser = argparse.ArgumentParser(description="Contratación centralizada del Principado de Asturias")
    parser.add_argument("--salida", type=Path, default=None,
                        help="carpeta de salida (por defecto <repo>/ccaa_asturias)")
    parser.add_argument("--semilla", type=Path, action="append", default=[],
                        help="parquet publicado (p.ej. el de v2026.02): añade las inscripciones que ya no se sirven")
    args = parser.parse_args(argv)
    return AsturiasToParquet(output_dir=args.salida, semillas=args.semilla).run()


if __name__ == "__main__":
    df = main()
    if df is None:
        raise SystemExit(1)