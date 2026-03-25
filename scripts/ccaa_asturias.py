import requests
import pandas as pd
import numpy as np
from io import StringIO
from pathlib import Path
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AsturiasToParquet:
    """
    Descarga TODOS los años de Asturias, maneja duplicados, 
    fuerza tipos compatibles y guarda en Parquet.
    """
    
    def __init__(self, output_dir="./asturias_data"):
        self.base_url = "https://descargas.asturias.es/asturias/opendata/SectorPublico/contratacion"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.datasets = {
            2019: "dataset-contratacion-centralizada-2019.csv",
            2020: "dataset-contratacion-centralizada-2020.csv",
            2021: "dataset-contratacion-centralizada-2021.csv",
            2022: "dataset-contratacion-centralizada-2022.csv",
            2023: "dataset-contratacion-centralizada-2023.csv",
            2024: "dataset-contratacion-centralizada-2024.csv",
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
    
    def parse_year(self, content_bytes, year):
        """Parsea un año."""
        content = content_bytes.decode('latin-1')
        
        df = pd.read_csv(
            StringIO(content),
            sep='§',
            engine='python',
            on_bad_lines='skip',
            encoding='latin-1'
        )
        
        df.columns = [str(c).strip() for c in df.columns]
        df = self.deduplicate_columns(df)
        df['year'] = year
        df['source_file'] = f"dataset-contratacion-centralizada-{year}.csv"
        
        return df
    
    def process_year(self, year, filename):
        """Descarga y parsea un año."""
        url = f"{self.base_url}/{filename}"
        
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing {year}...")
            
            response = requests.get(url, timeout=180)
            content_bytes = response.content
            
            logger.info(f"{year} - Downloaded: {len(content_bytes):,} bytes")
            
            df = self.parse_year(content_bytes, year)
            logger.info(f"{year} - Parsed: {len(df):,} rows x {len(df.columns)} cols")
            
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
        
        # Patrones de columnas numéricas (intentar convertir)
        numeric_patterns = ['AÑO', 'ANO', 'YEAR', 'PRESUPUESTO', 'IMPORTE', 'IMP.', 'IMP ', 
                          'EURO', 'IVA', 'CANTIDAD', 'NUMERO', 'Nº', 'TOTAL', 'BASE']
        
        for col in df.columns:
            if df[col].dtype == 'object':
                # Verificar si parece numérica por nombre
                looks_numeric = any(pat in col.upper() for pat in numeric_patterns)
                
                if looks_numeric:
                    try:
                        # Limpiar formato español (comas como decimales, puntos como miles)
                        cleaned = df[col].astype(str).str.replace('.', '', regex=False)  # Quitar separadores de miles
                        cleaned = cleaned.str.replace(',', '.', regex=False)  # Coma decimal a punto
                        cleaned = cleaned.str.replace(' ', '', regex=False)  # Quitar espacios
                        cleaned = cleaned.str.replace('€', '', regex=False)  # Quitar símbolo euro
                        cleaned = cleaned.str.replace('EUR', '', regex=False)  # Quitar EUR
                        
                        # Convertir a numérico, forzar errores a NaN
                        numeric = pd.to_numeric(cleaned, errors='coerce')
                        
                        # Si más del 50% son numéricos, usar numérico
                        if numeric.notna().sum() / len(df) > 0.5:
                            df[col] = numeric
                            logger.debug(f"  {col}: numeric")
                            continue
                    except:
                        pass
                
                # Si no es numérica o falló, forzar a string
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
                logger.debug(f"  {col}: string")
        
        return df
    
    def save_final_parquet(self):
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
        
        # Guardar Parquet
        parquet_path = self.output_dir / "asturias_contracts_ALL_YEARS.parquet"
        
        try:
            combined.to_parquet(parquet_path, index=False, compression='snappy', engine='pyarrow')
            logger.info(f"\n✓✓✓ PARQUET SAVED: {parquet_path}")
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
        for year, filename in self.datasets.items():
            self.process_year(year, filename)
            time.sleep(0.5)
        return self.save_final_parquet()

if __name__ == "__main__":
    processor = AsturiasToParquet()
    df = processor.run()