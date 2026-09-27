#!/usr/bin/env python3
"""
=============================================================================
DESCARGA COMPLETA DE DATOS ABIERTOS - COMUNITAT VALENCIANA
=============================================================================
Portal: https://dadesobertes.gva.es (CKAN API)
Versión: 4.0 FINAL - Todos los IDs verificados y corregidos
Fecha: Enero 2026

Categorías incluidas (14):
- Contratación pública (2014-2025 + DANA)
- Subvenciones y ayudas (2022-2025 + DANA)
- Presupuestos (2024-2025)
- Convenios (2018-2022)
- Lobbies - Registro REGIA (único en España)
- Empleo (ERE/ERTE 2000-2025, DANA, paro, contratos)
- Siniestralidad laboral (2015-2024)
- Patrimonio (inmuebles GVA, BIC, BRL)
- Entidades (locales, asociaciones)
- Territorio (centros docentes)
- Turismo (hoteles, casas rurales, VUT, campings, albergues, etc.)
- Sanidad (mapa sanitario, mortalidad, centros)
- Transporte (autobús interurbano)
- Paro registrado (estadísticas LABORA)
=============================================================================
"""

import requests
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import guardar_version  # noqa: E402

# Configuración
BASE_URL = "https://dadesobertes.gva.es"
API_URL = f"{BASE_URL}/api/3/action/package_show"
OUTPUT_DIR = Path("valencia_datos")

# ============================================================================
# CATÁLOGO COMPLETO DE DATASETS - IDs VERIFICADOS Y CORREGIDOS
# ============================================================================

DATASETS = {
    # ------------------------------------------------------------------------
    # CONTRATACIÓN PÚBLICA
    # Registro Oficial de Contratos de la Generalitat (REGCON)
    # ------------------------------------------------------------------------
    "contratacion": [
        "eco-gvo-contratos-2014",
        "eco-gvo-contratos-2015",
        "eco-gvo-contratos-2016",
        "eco-gvo-contratos-2017",
        "eco-gvo-contratos-2018",
        "eco-gvo-contratos-2019",
        "eco-gvo-contratos-2020",
        "eco-gvo-contratos-2021",
        "eco-gvo-contratos-2022",
        "eco-gvo-contratos-2023",
        "eco-gvo-contratos-2024",
        "eco-gvo-contratos-2025",
        "eco-contratos-dana",                    # Contratos relacionados con DANA
    ],
    
    # ------------------------------------------------------------------------
    # SUBVENCIONES Y AYUDAS
    # Ayudas concedidas por la Generalitat (mensual)
    # ------------------------------------------------------------------------
    "subvenciones": [
        "eco-gvo-subv-2022",
        "eco-gvo-subv-2023",
        "eco-gvo-subv-2024",
        "eco-gvo-subv-2025",
        "eco-ayudas-dana",                       # Subvenciones DANA (no familias)
        "eco-pmp-subvenciones",                  # Periodo medio pago subvenciones
    ],
    
    # ------------------------------------------------------------------------
    # PRESUPUESTOS
    # Ejecución presupuestaria (solo disponible 2024-2025)
    # ------------------------------------------------------------------------
    "presupuestos": [
        "sec-nefis-visor-2024",
        "sec-nefis-visor-2025",
    ],
    
    # ------------------------------------------------------------------------
    # CONVENIOS
    # Convenios suscritos por la GVA (2018-2022, 2023+ no disponibles)
    # ------------------------------------------------------------------------
    "convenios": [
        "gob-convenios-2018",
        "gob-convenios-2019",
        "gob-convenios-2020",
        "gob-convenios-2021",
        "gob-convenios-2022",
    ],
    
    # ------------------------------------------------------------------------
    # LOBBIES - REGISTRO REGIA
    # Registro de Grupos de Interés (único en España a nivel autonómico)
    # ------------------------------------------------------------------------
    "lobbies": [
        "sec-regia-actividades",                 # Actividades de influencia
        "sec-regia-grupos",                      # Grupos de interés registrados
    ],
    
    # ------------------------------------------------------------------------
    # EMPLEO Y MERCADO LABORAL
    # ERE/ERTE, DANA, contratos
    # ------------------------------------------------------------------------
    "empleo": [
        # ERE/ERTE histórico (2000-2025)
        "tra-eres-ertes-v2-2025",
        "tra-eres-ertes-v2-2024",
        # ERTE DANA
        "emp-erte-dana-cv",                      # ERTE DANA detallado
        "emp-erte-dana-pob",                     # ERTE DANA por poblaciones
        "emp-erte-dana-agr",                     # ERTE DANA agregado
        # Estadísticas de contratación por año (LABORA)
        "tra-ocu-contratos-2024",
        "tra-ocu-contratos-2023",
        "tra-ocu-contratos-2022",
    ],
    
    # ------------------------------------------------------------------------
    # PARO REGISTRADO
    # Estadísticas de demandantes (LABORA)
    # ------------------------------------------------------------------------
    "paro": [
        "datos-de-paro-en-la-comunitat-valenciana",
        "datos-del-paro-en-la-comunidad-valenciana-2024",
        "tra-reg-paro-2024",                     # Demandantes activos parados
        "tra-reg-paro-2023",
        "tra-reg-paro-2022",
        "tra-reg-paro-2021",
        "tra-reg-paro-2020",
        "tra-reg-paro-2019",
        "tra-reg-paro-2018",
        "tra-reg-paro-2017",
    ],
    
    # ------------------------------------------------------------------------
    # SINIESTRALIDAD LABORAL
    # Accidentes de trabajo (2015-2024)
    # ------------------------------------------------------------------------
    "siniestralidad": [
        "tra-accidentes-2024",
        "tra-accidentes-2023",
        "tra-accidentes-2022",
        "tra-accidentes-2021",
        "tra-accidentes-2020",
        "tra-accidentes-2019",
        "tra-accidentes-2018",
        "tra-accidentes-2017",
        "tra-accidentes-2016",
        "tra-accidentes-2015",
    ],
    
    # ------------------------------------------------------------------------
    # PATRIMONIO Y BIENES
    # Inmuebles de la Generalitat, BIC, BRL
    # ------------------------------------------------------------------------
    "patrimonio": [
        "hac-bie-inm",                           # Bienes inmuebles GVA
        "hac-bie-inmuebles",                     # BIC y BRL inmuebles
        "hac-bie-inmateriales",                  # Bienes inmateriales culturales
    ],
    
    # ------------------------------------------------------------------------
    # ENTIDADES Y ASOCIACIONES
    # ------------------------------------------------------------------------
    "entidades": [
        "sec-mapets",                            # Entidades locales CV
        "soc-asociaciones",                      # Asociaciones CV
    ],
    
    # ------------------------------------------------------------------------
    # TERRITORIO Y EDUCACIÓN
    # ------------------------------------------------------------------------
    "territorio": [
        "edu-centros",                           # Centros docentes CV
    ],
    
    # ------------------------------------------------------------------------
    # TURISMO - IDs CORREGIDOS Y COMPLETOS
    # Registro de empresas turísticas
    # ------------------------------------------------------------------------
    "turismo": [
        # Datos actualizados diariamente (tur-gestur-*)
        "tur-gestur-vt",                         # Viviendas uso turístico
        "tur-gestur-cr",                         # Casas rurales
        "tur-gestur-ca",                         # Campings (actualizado)
        "tur-gestur-ap",                         # Áreas de pernocta
        "tur-gestur-afp",                        # Acampada finca particular
        "tur-gestur-agv",                        # Agencias de viajes (actualizado)
        "tur-gestur-alb",                        # Albergues turísticos (CORREGIDO)
        # Datos históricos/semanales
        "dades-turisme-hotels-comunitat-valenciana",          # Hoteles (incluye hostales)
        "dades-turisme-campings-comunitat-valenciana",        # Campings histórico
        "dades-turisme-allotjament-rural-comunitat-valenciana", # Alojamiento rural
        "dades-turisme-agencies-viatges-comunitat-valenciana",  # Agencias histórico
        "dades-turisme-habitatges-comunitat-valenciana-2025",   # VUT 2025
        "dades-turisme-actiu-comunitat-valenciana",           # Turismo activo
    ],
    
    # ------------------------------------------------------------------------
    # SANIDAD
    # Mapa sanitario, mortalidad, centros
    # ------------------------------------------------------------------------
    "sanidad": [
        "sanidad-sip",                           # Mapa sanitario (CSV con tramos INE)
        "san-reg-centros-2020",                  # Registro de centros sanitarios
        "sal-tm-cv",                             # TAE mortalidad CV
        "sal-tmb-cv",                            # Tasa bruta mortalidad
    ],
    
    # ------------------------------------------------------------------------
    # TRANSPORTE
    # Autobús interurbano, rutas
    # ------------------------------------------------------------------------
    "transporte": [
        "tra-hyr-atmv-horaris-i-rutes",          # Itinerarios y horarios autobús
    ],
}


# Series anuales (ids que acaban en -AAAA): en cada ejecución se buscan en el
# portal los años que faltan en el catálogo, desde este año hasta el actual
# (antes el catálogo fijo llegaba a 2025 y no pedía nunca 2026 ni años anteriores
# publicados con el mismo patrón)
ANIO_MINIMO_SERIES = 2000


def existe_dataset(dataset_id):
    """True si package_show encuentra el dataset, False si no existe (404/403),
    None si no se pudo comprobar (error de red o respuesta inesperada)."""
    try:
        response = requests.get(API_URL, params={"id": dataset_id}, timeout=30)
        if response.status_code in (403, 404):
            return False
        response.raise_for_status()
        return bool(response.json().get("success"))
    except Exception:
        return None


def ampliar_series_anuales(datasets, anio_actual=None, anio_minimo=ANIO_MINIMO_SERIES, fallidos=None):
    """Copia de datasets con los años que existen en el portal y faltaban.

    Para cada prefijo con año del catálogo (p. ej. 'eco-gvo-contratos-2014' ->
    'eco-gvo-contratos-') se prueba package_show con los años de anio_minimo a
    anio_actual que no están. Los encontrados se añaden al final de su categoría:
    el orden de los ya conocidos (y por tanto sus nombres de archivo) no cambia.
    """
    anio_actual = anio_actual or datetime.now().year
    ampliados = {}
    for categoria, ids in datasets.items():
        anios_por_prefijo = {}
        for dataset_id in ids:
            m = re.fullmatch(r"(.+-)((?:19|20)\d{2})", dataset_id)
            if m:
                anios_por_prefijo.setdefault(m.group(1), set()).add(int(m.group(2)))
        nuevos = []
        for prefijo, anios in anios_por_prefijo.items():
            for anio in range(anio_minimo, anio_actual + 1):
                candidato = f"{prefijo}{anio}"
                if anio in anios or candidato in ids or candidato in nuevos:
                    continue
                existe = existe_dataset(candidato)
                if existe:
                    print(f"  🆕 Dataset nuevo en el portal: {candidato}")
                    nuevos.append(candidato)
                elif existe is None and fallidos is not None:
                    fallidos.append(f"{candidato}: no se pudo comprobar si existe")
        ampliados[categoria] = list(ids) + nuevos
    return ampliados


def fecha_ckan(valor):
    """Fecha ISO de CKAN (UTC sin zona) -> epoch, o None"""
    if not valor:
        return None
    try:
        fecha = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        return None
    if fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=timezone.utc)
    return fecha.timestamp()


def recurso_actualizado(resource, filepath):
    """True si el portal indica que el recurso cambió después de la copia local"""
    modificado = fecha_ckan(resource.get("last_modified") or resource.get("metadata_modified"))
    return modificado is not None and modificado > os.path.getmtime(filepath)


def get_dataset_info(dataset_id):
    """Obtiene información del dataset vía API CKAN"""
    try:
        response = requests.get(API_URL, params={"id": dataset_id}, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("success"):
            return data.get("result", {})
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 404:
            print(f"  ⚠️ No encontrado (404): {dataset_id}")
        elif status == 403:
            print(f"  🔒 Acceso denegado (403): {dataset_id}")
        else:
            print(f"  ⚠️ Error HTTP {status}: {dataset_id}")
    except requests.exceptions.Timeout:
        print(f"  ⏱️ Timeout: {dataset_id}")
    except Exception as e:
        print(f"  ⚠️ Error: {e}")
    return None


def descarga_vacia(ruta, muestra=64 * 1024):
    """True si el archivo no tiene datos: vacío, solo espacios o solo la cabecera."""
    with open(ruta, "rb") as f:
        inicio = f.read(muestra + 1)
    if len(inicio) > muestra:
        return False
    return sum(1 for linea in inicio.splitlines() if linea.strip()) <= 1


def download_file(url, filepath):
    """Descarga un archivo con manejo de errores.

    Se escribe en un temporal '.part' y solo se renombra al nombre final cuando
    la descarga termina bien: una descarga cortada (Ctrl+C, caída de red) no
    puede quedar en disco como si estuviera completa ("Ya existe" en la
    siguiente ejecución). Si ya había copia y el contenido cambió, la anterior
    se conserva en _historico/ (guardar_version); si la descarga llega vacía,
    no se toca.
    """
    filepath = Path(filepath)
    tmp_path = filepath.with_name(filepath.name + '.part')
    try:
        with requests.get(url, timeout=300, stream=True) as response:
            response.raise_for_status()

            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Una descarga vacía (o solo cabecera) es casi siempre un fallo del
        # portal: no sustituye a la copia que ya se tenía
        if filepath.exists() and descarga_vacia(tmp_path):
            return False, "Descarga vacía (se conserva la copia anterior)"
        # Nunca se machaca la copia anterior: si cambió, pasa a _historico/
        # (control del sesgo del superviviente, ver comun/historico.py)
        if guardar_version(filepath, desde=tmp_path) == "sin_cambios":
            # Mismo contenido: se anota como verificado ahora (mtime) para no
            # volver a descargarlo en cada ejecución por el last_modified del portal
            os.utime(filepath)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        return True, size_mb
    except requests.exceptions.Timeout:
        return False, "Timeout (5 min)"
    except Exception as e:
        return False, str(e)[:50]
    finally:
        # Si no se llegó a renombrar (error o interrupción), borrar el parcial
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def sanitize_filename(name):
    """Limpia nombre de archivo para Windows/Linux"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    while '  ' in name:
        name = name.replace('  ', ' ')
    return name[:200].strip()


def process_dataset(dataset_id, category_dir, usados=None, fallidos=None):
    """Procesa un dataset y descarga sus recursos CSV.

    usados: nombres de archivo (en minúsculas) ya asignados en la carpeta de la
        categoría en esta ejecución. main() comparte el mismo conjunto entre los
        datasets de una categoría para que dos recursos con el mismo nombre no
        acaben en el mismo archivo (antes el segundo se daba por "Ya existe" y
        se perdía, p. ej. los recursos sin año de tra-reg-paro-2017/2018/2019).
    fallidos: lista opcional donde se anotan los errores de API o de descarga.
    """
    print(f"\n📦 {dataset_id}")
    if usados is None:
        usados = set()

    info = get_dataset_info(dataset_id)
    if not info:
        if fallidos is not None:
            fallidos.append(f"{dataset_id}: sin respuesta válida de la API")
        return 0, 0

    resources = info.get("resources") or []

    # Filtrar recursos CSV (CKAN puede devolver "format": null)
    csv_resources = [r for r in resources if (r.get("format") or "").strip().upper() in ["CSV", "TEXT/CSV"]]

    if not csv_resources:
        print(f"  ⚠️ No hay recursos CSV (puede tener JSON/XML)")
        return 0, 0

    downloaded = 0
    total_size = 0

    for resource in csv_resources:
        url = resource.get("url")
        name = resource.get("name") or "data"

        if not url:
            continue

        filename = sanitize_filename(name) or "data"
        if not filename.lower().endswith('.csv'):
            filename += '.csv'

        # Nombre repetido en la categoría: añadir el id del dataset (y un
        # contador si hiciera falta). El orden de datasets y recursos es estable,
        # así que cada recurso recibe el mismo nombre en cada ejecución y la
        # reanudación ("Ya existe") sigue funcionando.
        if filename.lower() in usados:
            base = filename[:-4].rstrip()[:150]
            candidato = f"{base}_{dataset_id}.csv"
            n = 2
            while candidato.lower() in usados:
                candidato = f"{base}_{dataset_id}_{n}.csv"
                n += 1
            filename = candidato
        usados.add(filename.lower())

        filepath = category_dir / filename
        
        # Verificar si ya existe. Si el portal lo ha actualizado después (datasets
        # del año en curso, listados diarios...) se vuelve a descargar: antes la
        # primera copia se quedaba para siempre (el contratos 2025 publicado tiene 32 filas)
        # La copia anterior no se pierde: si el contenido cambió pasa a _historico/
        existia = filepath.exists()
        if existia and not recurso_actualizado(resource, filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  ⏭️ Ya existe: {filename}")
            total_size += size_mb
            downloaded += 1
            continue
        
        # Descargar
        print(f"  ⬇️ {filename}{' (actualizado en el portal)' if existia else ''}...", end=" ", flush=True)
        success, result = download_file(url, filepath)
        
        if success:
            print(f"✅ ({result:.1f} MB)")
            downloaded += 1
            total_size += result
        else:
            print(f"❌ {result}")
            # Si falla la actualización se conserva la copia anterior (download_file
            # escribe en un .part y solo sustituye el archivo al terminar bien)
            if filepath.exists() and not existia:
                filepath.unlink()
            if fallidos is not None:
                fallidos.append(f"{dataset_id}: {filename} ({result})")

        time.sleep(0.3)
    
    return downloaded, total_size


def main():
    start_time = datetime.now()
    
    print("=" * 70)
    print("DESCARGA COMPLETA DE DATOS ABIERTOS - COMUNITAT VALENCIANA")
    print("=" * 70)
    print(f"Portal: {BASE_URL}")
    print(f"Destino: {OUTPUT_DIR.absolute()}")
    print(f"Categorías: {len(DATASETS)}")
    print(f"Datasets totales: {sum(len(v) for v in DATASETS.values())}")
    print(f"Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    total_files = 0
    total_size = 0
    stats = {}
    errors = []
    fallidos = []

    print("\n🔎 Buscando años nuevos de las series anuales...")
    catalogo = ampliar_series_anuales(DATASETS, fallidos=fallidos)
    descubiertos = [d for cat, lista in catalogo.items() for d in lista if d not in DATASETS.get(cat, [])]

    for category, datasets in catalogo.items():
        print(f"\n{'=' * 70}")
        print(f"📁 CATEGORÍA: {category.upper()}")
        print("=" * 70)

        category_dir = OUTPUT_DIR / category
        category_dir.mkdir(parents=True, exist_ok=True)
        usados = set()  # nombres de archivo ya asignados en esta categoría

        cat_files = 0
        cat_size = 0

        for dataset_id in datasets:
            files, size = process_dataset(dataset_id, category_dir, usados, fallidos)
            if files == 0:
                errors.append(dataset_id)
            cat_files += files
            cat_size += size
        
        stats[category] = {"files": cat_files, "size": cat_size}
        total_files += cat_files
        total_size += cat_size
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Resumen final
    print("\n" + "=" * 70)
    if fallidos:
        print(f"⚠️ DESCARGA COMPLETADA CON ERRORES ({len(fallidos)})")
    else:
        print("✅ DESCARGA COMPLETADA")
    print("=" * 70)
    print(f"   Archivos descargados: {total_files}")
    print(f"   Tamaño total: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
    print(f"   Duración: {duration}")
    print(f"   Ubicación: {OUTPUT_DIR.absolute()}")
    
    # Mostrar estructura
    print("\n📂 ESTRUCTURA FINAL:")
    for category, data in sorted(stats.items()):
        if data["files"] > 0:
            print(f"  📁 {category}/")
            print(f"      {data['files']} archivos, {data['size']:.1f} MB")
    
    if descubiertos:
        print(f"\n🆕 DATASETS NUEVOS DESCARGADOS ({len(descubiertos)}): {', '.join(descubiertos)}")

    # Mostrar errores
    if errors:
        print(f"\n⚠️ DATASETS SIN CSV ({len(errors)}):")
        for e in errors:
            print(f"   - {e}")

    if fallidos:
        print(f"\n❌ ERRORES DE DESCARGA ({len(fallidos)}) - vuelve a ejecutar el script para reintentarlos:")
        for e in fallidos:
            print(f"   - {e}")

    # Guardar log
    log_file = OUTPUT_DIR / "descarga_log.txt"
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"DESCARGA DATOS ABIERTOS - COMUNITAT VALENCIANA\n")
        f.write(f"{'=' * 50}\n")
        f.write(f"Fecha: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duración: {duration}\n")
        f.write(f"Total archivos: {total_files}\n")
        f.write(f"Total tamaño: {total_size:.1f} MB ({total_size/1024:.2f} GB)\n\n")

        if descubiertos:
            f.write("DATASETS NUEVOS (no estaban en el catálogo del script):\n")
            f.write("-" * 50 + "\n")
            for d in descubiertos:
                f.write(f"  - {d}\n")
            f.write("\n")
        
        f.write("DETALLE POR CATEGORÍA:\n")
        f.write("-" * 50 + "\n")
        for category, data in sorted(stats.items()):
            f.write(f"{category}: {data['files']} archivos, {data['size']:.1f} MB\n")
        
        if errors:
            f.write(f"\nDATASETS SIN CSV:\n")
            f.write("-" * 50 + "\n")
            for e in errors:
                f.write(f"  - {e}\n")

        if fallidos:
            f.write("\nERRORES DE DESCARGA:\n")
            f.write("-" * 50 + "\n")
            for e in fallidos:
                f.write(f"  - {e}\n")

    print(f"\n📝 Log guardado: {log_file}")
    
    # Instrucciones siguientes
    print("\n" + "=" * 70)
    print("📌 SIGUIENTE PASO: Convertir a Parquet")
    print("=" * 70)
    print("   python ccaa_valencia_parquet.py")
    print("   (Reducirá ~5 GB a ~500-800 MB)")

    # Código de salida != 0 si algo falló, para que cron/CI no lo dé por bueno
    return 1 if fallidos else 0


if __name__ == "__main__":
    sys.exit(main())