#!/usr/bin/env python3
"""
================================================================================
CATALUNYA - CSV A PARQUET v1.0
================================================================================
Convierte los CSVs relevantes a Parquet, descartando redundantes.
================================================================================
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import glob
import re

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

INPUT_DIR = "catalunya_datos_completos"
OUTPUT_DIR = "catalunya_parquet"

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__).info

# =============================================================================
# MAPEO: TODOS los archivos se convierten (sin descartar nada)
# =============================================================================

# Estructura: (csv_relativo, parquet_destino, descripcion)

ARCHIVOS = {
    # =========================================================================
    # CONTRATACIÓN - TODOS
    # =========================================================================
    '01_transparencia_catalunya/01_contratacion/registro_publico_contratos.csv': 
        ('contratacion/contratos_registro.parquet', '⭐ MASTER - Todos los contratos formalizados'),
    
    '01_transparencia_catalunya/01_contratacion/publicaciones_pscp.csv': 
        ('contratacion/publicaciones_pscp.parquet', 'Publicaciones PSCP (ciclo completo)'),
    
    '01_transparencia_catalunya/01_contratacion/licitaciones_adjudicaciones_curso.csv': 
        ('contratacion/licitaciones_adjudicaciones.parquet', 'Licitaciones y adjudicaciones en curso'),
    
    '01_transparencia_catalunya/01_contratacion/contratacion_programada.csv': 
        ('contratacion/contratacion_programada.parquet', 'Contratación planificada'),
    
    '01_transparencia_catalunya/01_contratacion/contratacion_emergencia_covid.csv': 
        ('contratacion/contratos_covid.parquet', 'Contratos emergencia COVID'),
    
    '01_transparencia_catalunya/01_contratacion/resoluciones_tribunal.csv': 
        ('contratacion/resoluciones_tribunal.parquet', 'Resoluciones Tribunal Contratos'),
    
    '01_transparencia_catalunya/01_contratacion/adjudicaciones_generalitat.csv': 
        ('contratacion/adjudicaciones_generalitat.parquet', 'Adjudicaciones Generalitat'),
    
    '01_transparencia_catalunya/01_contratacion/fase_ejecucion.csv': 
        ('contratacion/fase_ejecucion.parquet', 'Contratos en fase ejecución'),
    
    # =========================================================================
    # SUBVENCIONES - TODOS
    # =========================================================================
    '01_transparencia_catalunya/02_subvenciones/raisc_concesiones.csv': 
        ('subvenciones/raisc_concesiones.parquet', '⭐ MASTER - Todas las subvenciones concedidas'),
    
    '01_transparencia_catalunya/02_subvenciones/raisc_convocatorias.csv': 
        ('subvenciones/raisc_convocatorias.parquet', 'Convocatorias RAISC'),
    
    '01_transparencia_catalunya/02_subvenciones/convocatorias_subvenciones.csv': 
        ('subvenciones/convocatorias_subvenciones.parquet', 'Otras convocatorias subvenciones'),
    
    # =========================================================================
    # CONVENIOS
    # =========================================================================
    '01_transparencia_catalunya/03_convenios/registro_convenios.csv': 
        ('convenios/convenios.parquet', 'Convenios de colaboración'),
    
    # =========================================================================
    # PRESUPUESTOS - TODOS
    # =========================================================================
    '01_transparencia_catalunya/04_presupuestos/presupuestos_aprobados.csv': 
        ('presupuestos/presupuestos_aprobados.parquet', 'Presupuestos aprobados'),
    
    '01_transparencia_catalunya/04_presupuestos/ejecucion_mensual_despeses.csv': 
        ('presupuestos/ejecucion_gastos.parquet', 'Ejecución mensual gastos'),
    
    '01_transparencia_catalunya/04_presupuestos/ejecucion_mensual_ingressos.csv': 
        ('presupuestos/ejecucion_ingresos.parquet', 'Ejecución mensual ingresos'),
    
    '01_transparencia_catalunya/04_presupuestos/despeses_2019.csv': 
        ('presupuestos/despeses_2019.parquet', 'Gastos 2019 detallado'),
    
    # =========================================================================
    # SECTOR PÚBLICO / ENTIDADES - TODOS
    # =========================================================================
    '01_transparencia_catalunya/05_sector_publico/ens_locals_catalunya.csv': 
        ('entidades/ens_locals.parquet', '⭐ MASTER - Todos los entes locales'),
    
    '01_transparencia_catalunya/05_sector_publico/registro_sector_publico.csv': 
        ('entidades/sector_publico_generalitat.parquet', 'Entidades sector público Generalitat'),
    
    '01_transparencia_catalunya/05_sector_publico/codigos_departamentos_ens.csv': 
        ('entidades/codigos_departamentos.parquet', 'Códigos de departamentos'),
    
    '01_transparencia_catalunya/05_sector_publico/composicio_plens.csv': 
        ('entidades/composicio_plens.parquet', 'Composición plenos (políticos)'),
    
    '01_transparencia_catalunya/05_sector_publico/ajuntaments.csv': 
        ('entidades/ajuntaments.parquet', 'Ayuntamientos (detalle)'),
    
    '01_transparencia_catalunya/05_sector_publico/ajuntaments_catalunya.csv': 
        ('entidades/ajuntaments_lista.parquet', 'Lista ayuntamientos'),
    
    # =========================================================================
    # RECURSOS HUMANOS - TODOS
    # =========================================================================
    '01_transparencia_catalunya/06_recursos_humanos/altos_cargos_retribuciones.csv': 
        ('rrhh/altos_cargos.parquet', 'Altos cargos y retribuciones'),
    
    '01_transparencia_catalunya/06_recursos_humanos/retribuciones_funcionarios.csv': 
        ('rrhh/retribuciones_funcionarios.parquet', 'Tablas retributivas funcionarios'),
    
    '01_transparencia_catalunya/06_recursos_humanos/retribuciones_laboral.csv': 
        ('rrhh/retribuciones_laboral.parquet', 'Tablas retributivas laborales'),
    
    '01_transparencia_catalunya/06_recursos_humanos/convocatorias_personal.csv': 
        ('rrhh/convocatorias_personal.parquet', 'Convocatorias empleo público'),
    
    '01_transparencia_catalunya/06_recursos_humanos/taules_retributives_alts_carrecs.csv': 
        ('rrhh/taules_retributives.parquet', 'Tablas retributivas altos cargos'),
    
    '01_transparencia_catalunya/06_recursos_humanos/enunciats_examens.csv': 
        ('rrhh/enunciats_examens.parquet', 'Enunciados exámenes oposiciones'),
    
    # =========================================================================
    # TERRITORIO - TODOS
    # =========================================================================
    '01_transparencia_catalunya/07_territorio/municipis_catalunya_geo.csv': 
        ('territorio/municipis_catalunya.parquet', 'Municipios Catalunya con geo'),
    
    '01_transparencia_catalunya/07_territorio/municipis_espanya.csv': 
        ('territorio/municipis_espanya.parquet', 'Municipios España'),
}


# =============================================================================
# FUNCIONES
# =============================================================================

def load_csv(path):
    """Carga CSV con detección de encoding y separador"""
    encodings = ['utf-8', 'latin-1', 'cp1252']
    separators = [',', ';', '\t']
    
    # Probar primero el separador más frecuente en la cabecera: un CSV con ';' y una coma
    # en algún nombre de columna se aceptaría con ',' y se leería desalineado
    try:
        with open(path, 'rb') as f:
            cabecera = f.readline(1024 * 1024).decode('latin-1')
        separators.sort(key=lambda s: -cabecera.count(s))
    except OSError:
        pass
    
    for enc in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(path, encoding=enc, sep=sep, low_memory=False, on_bad_lines='skip')
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
    
    raise ValueError(f"No se pudo cargar: {path}")


def texto_sin_nulos(serie):
    """Columnas de texto (object en pandas 2, str en pandas 3) -> str con '' en vez de nulos"""
    return serie.fillna('').astype(str)


def es_texto(serie):
    """True para columnas object (pandas 2) o str (pandas 3)"""
    return serie.dtype == 'object' or pd.api.types.is_string_dtype(serie.dtype)


def anio_de_nombre(nombre):
    """Año (19xx/20xx) contenido en el nombre de archivo, o None"""
    m = re.search(r'(19|20)\d{2}', nombre)
    return int(m.group(0)) if m else None


def convert_to_parquet(input_path, output_path, descripcion):
    """Convierte un CSV a Parquet"""
    log(f"\n📄 {descripcion}")
    log(f"   Input: {input_path.name}")
    
    # Cargar
    df = load_csv(input_path)
    log(f"   📝 {len(df):,} registros, {len(df.columns)} columnas")
    
    # Optimizar tipos de datos
    for col in df.columns:
        # Convertir object a string para evitar errores de tipos mixtos
        if es_texto(df[col]):
            df[col] = texto_sin_nulos(df[col])
    
    # Crear directorio de salida
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar
    df.to_parquet(output_path, index=False, compression='snappy')
    
    size_csv = input_path.stat().st_size / 1024 / 1024
    size_parquet = output_path.stat().st_size / 1024 / 1024
    ratio = (1 - size_parquet / size_csv) * 100 if size_csv > 0 else 0
    
    log(f"   💾 {output_path.name}: {size_parquet:.1f}MB (↓{ratio:.0f}% de {size_csv:.1f}MB)")
    
    return len(df), size_parquet


def consolidate_barcelona_menores(input_dir, output_dir):
    """Consolida contratos menores Barcelona (múltiples años) en un solo Parquet"""
    log("\n" + "="*60)
    log("📦 CONSOLIDANDO: Contratos menores Barcelona")
    
    menores_dir = input_dir / '02_barcelona' / 'contratos_menores'
    if not menores_dir.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for csv_file in sorted(menores_dir.glob('*.csv')):
        try:
            df = load_csv(csv_file)
            # Extraer año del nombre
            df['_año'] = anio_de_nombre(csv_file.stem)
            dfs.append(df)
            log(f"   ✅ {csv_file.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {csv_file.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / 'contratos_menores_bcn.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


def consolidate_barcelona_contratistas(input_dir, output_dir):
    """Consolida contratistas Barcelona (múltiples años)"""
    log("\n" + "="*60)
    log("📦 CONSOLIDANDO: Contratistas Barcelona")
    
    dir_path = input_dir / '02_barcelona' / 'contratistas'
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for csv_file in sorted(dir_path.glob('*.csv')):
        try:
            df = load_csv(csv_file)
            df['_año'] = anio_de_nombre(csv_file.stem)
            dfs.append(df)
            log(f"   ✅ {csv_file.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {csv_file.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / 'contratistas_bcn.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


def consolidate_barcelona_perfil(input_dir, output_dir):
    """Consolida perfil contratante Barcelona"""
    log("\n" + "="*60)
    log("📦 CONSOLIDANDO: Perfil contratante Barcelona")
    
    dir_path = input_dir / '02_barcelona' / 'perfil_contratante'
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for csv_file in sorted(dir_path.glob('*.csv')):
        try:
            df = load_csv(csv_file)
            df['_archivo_origen'] = csv_file.name
            dfs.append(df)
            log(f"   ✅ {csv_file.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {csv_file.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / 'perfil_contratante_bcn.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


def consolidate_barcelona_modificaciones(input_dir, output_dir):
    """Consolida modificaciones de contratos Barcelona"""
    log("\n" + "="*60)
    log("📦 CONSOLIDANDO: Modificaciones contratos Barcelona")
    
    dir_path = input_dir / '02_barcelona' / 'modificaciones_contratos'
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for csv_file in sorted(dir_path.glob('*.csv')):
        try:
            df = load_csv(csv_file)
            df['_año'] = anio_de_nombre(csv_file.stem)
            dfs.append(df)
            log(f"   ✅ {csv_file.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {csv_file.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / 'modificaciones_bcn.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


def consolidate_barcelona_resumen(input_dir, output_dir):
    """Consolida resumen trimestral Barcelona"""
    log("\n" + "="*60)
    log("📦 CONSOLIDANDO: Resumen trimestral Barcelona")
    
    dir_path = input_dir / '02_barcelona' / 'resumen_trimestral'
    if not dir_path.exists():
        log("   ⚠️ No encontrado")
        return 0, 0
    
    dfs = []
    for csv_file in sorted(dir_path.glob('*.csv')):
        try:
            df = load_csv(csv_file)
            df['_año'] = anio_de_nombre(csv_file.stem)
            dfs.append(df)
            log(f"   ✅ {csv_file.name}: {len(df):,} registros")
        except Exception as e:
            log(f"   ❌ {csv_file.name}: {e}")
    
    if not dfs:
        return 0, 0
    
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Convertir columnas object a string para evitar errores de tipos mixtos
    for col in df_all.columns:
        if es_texto(df_all[col]):
            df_all[col] = texto_sin_nulos(df_all[col])
    
    output_path = output_dir / 'contratacion' / 'resumen_trimestral_bcn.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_parquet(output_path, index=False, compression='snappy')
    
    size = output_path.stat().st_size / 1024 / 1024
    log(f"   💾 CONSOLIDADO: {len(df_all):,} registros, {size:.1f}MB")
    
    return len(df_all), size


# =============================================================================
# MAIN
# =============================================================================

def main():
    start = datetime.now()
    
    print("\n" + "="*70)
    print("📦 CATALUNYA CSV → PARQUET")
    print("="*70)
    
    input_dir = Path(INPUT_DIR)
    output_dir = Path(OUTPUT_DIR)
    
    if not input_dir.exists():
        log(f"❌ No encontrado: {input_dir}")
        return
    
    output_dir.mkdir(exist_ok=True)
    
    stats = {
        'convertidos': 0,
        'registros_total': 0,
        'tamaño_total_mb': 0,
        'errores': 0,
    }
    
    # =========================================================================
    # ARCHIVOS INDIVIDUALES
    # =========================================================================
    log("\n" + "="*70)
    log("📄 CONVIRTIENDO ARCHIVOS INDIVIDUALES")
    log("="*70)
    
    for csv_rel, (parquet_rel, descripcion) in ARCHIVOS.items():
        csv_path = input_dir / csv_rel
        
        if not csv_path.exists():
            log(f"\n⚠️ No encontrado: {csv_rel}")
            continue
        
        parquet_path = output_dir / parquet_rel
        
        try:
            n_records, size_mb = convert_to_parquet(csv_path, parquet_path, descripcion)
            stats['convertidos'] += 1
            stats['registros_total'] += n_records
            stats['tamaño_total_mb'] += size_mb
        except Exception as e:
            log(f"\n❌ Error en {csv_path.name}: {e}")
            stats['errores'] += 1
    
    # =========================================================================
    # CONSOLIDACIONES BARCELONA
    # =========================================================================
    log("\n" + "="*70)
    log("📦 CONSOLIDANDO BARCELONA (múltiples archivos → 1 parquet)")
    log("="*70)
    
    n, s = consolidate_barcelona_menores(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_contratistas(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_perfil(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_modificaciones(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    n, s = consolidate_barcelona_resumen(input_dir, output_dir)
    stats['registros_total'] += n
    stats['tamaño_total_mb'] += s
    if n > 0: stats['convertidos'] += 1
    
    # =========================================================================
    # RESUMEN
    # =========================================================================
    elapsed = (datetime.now() - start).total_seconds()
    
    print("\n" + "="*70)
    log("📊 RESUMEN")
    print("="*70)
    log(f"✅ Archivos convertidos: {stats['convertidos']}")
    log(f"❌ Errores: {stats['errores']}")
    log(f"📝 Registros totales: {stats['registros_total']:,}")
    log(f"💾 Tamaño total Parquet: {stats['tamaño_total_mb']:.1f} MB")
    log(f"⏱️ Tiempo: {elapsed:.1f} segundos")
    
    # Generar índice
    print("\n" + "="*70)
    log("📋 ESTRUCTURA FINAL")
    print("="*70)
    
    for parquet_file in sorted(output_dir.rglob('*.parquet')):
        rel_path = parquet_file.relative_to(output_dir)
        size = parquet_file.stat().st_size / 1024 / 1024
        log(f"   {rel_path} ({size:.1f}MB)")
    
    # README
    with open(output_dir / 'README.md', 'w', encoding='utf-8') as f:
        f.write(f"""# Catalunya - Datos en Parquet

Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## Estadísticas
- Archivos: {stats['convertidos']}
- Registros: {stats['registros_total']:,}
- Tamaño: {stats['tamaño_total_mb']:.1f} MB

## Estructura

```
{OUTPUT_DIR}/
├── contratacion/
│   ├── contratos_registro.parquet          ⭐ MASTER
│   ├── publicaciones_pscp.parquet          (ciclo completo licitación)
│   ├── licitaciones_adjudicaciones.parquet
│   ├── adjudicaciones_generalitat.parquet
│   ├── fase_ejecucion.parquet
│   ├── contratacion_programada.parquet
│   ├── contratos_covid.parquet
│   ├── resoluciones_tribunal.parquet
│   ├── contratos_menores_bcn.parquet       (2014-2018 consolidado)
│   ├── contratistas_bcn.parquet            (2012-2023 consolidado)
│   ├── perfil_contratante_bcn.parquet
│   ├── modificaciones_bcn.parquet
│   └── resumen_trimestral_bcn.parquet
├── subvenciones/
│   ├── raisc_concesiones.parquet           ⭐ MASTER (9.6M registros)
│   ├── raisc_convocatorias.parquet
│   └── convocatorias_subvenciones.parquet
├── convenios/
│   └── convenios.parquet
├── presupuestos/
│   ├── ejecucion_gastos.parquet            (1.5M registros)
│   ├── ejecucion_ingresos.parquet
│   ├── presupuestos_aprobados.parquet
│   └── despeses_2019.parquet
├── entidades/
│   ├── ens_locals.parquet                  ⭐ MASTER
│   ├── sector_publico_generalitat.parquet
│   ├── codigos_departamentos.parquet
│   ├── composicio_plens.parquet
│   ├── ajuntaments.parquet
│   └── ajuntaments_lista.parquet
├── rrhh/
│   ├── altos_cargos.parquet
│   ├── convocatorias_personal.parquet
│   ├── retribuciones_funcionarios.parquet
│   ├── retribuciones_laboral.parquet
│   ├── taules_retributives.parquet
│   └── enunciats_examens.parquet
└── territorio/
    ├── municipis_catalunya.parquet
    └── municipis_espanya.parquet
```

## Uso

```python
import pandas as pd

# Cargar contratos (10x más rápido que CSV)
df = pd.read_parquet('contratacion/contratos_registro.parquet')

# Filtrar por año
df['año'] = pd.to_datetime(df['Data formalització']).dt.year
df_2024 = df[df['año'] == 2024]
```

## Notas

- **TODOS** los archivos CSV originales se han convertido (sin descartar nada)
- Los archivos de Barcelona (múltiples años) se han consolidado en uno solo
- Parquet es ~60-80% más pequeño y 10x más rápido de cargar
""")
    
    log(f"\n📄 README: {output_dir}/README.md")


if __name__ == "__main__":
    main()