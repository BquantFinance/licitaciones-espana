"""Descarga el inventario de ZIP oficiales conservando hashes y fallos."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import time
import zipfile

import requests

from nacional.licitaciones import CONJUNTOS


def descargar(item, destino):
    conjunto, nombre = item["conjunto"], item["archivo_origen"]
    if conjunto not in CONJUNTOS or Path(nombre).name != nombre or not nombre.endswith(".zip"):
        raise ValueError(f"Fuente no válida: {item}")
    url = CONJUNTOS[conjunto]["url_base"].replace("contrataciondelsectorpublico.gob.es", "contrataciondelestado.es") + nombre
    path = destino / conjunto / nombre
    path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    error = None
    for intento in range(3):
        try:
            if not path.exists():
                part = path.with_suffix(".zip.part")
                with requests.get(url, stream=True, timeout=(30, 120)) as response:
                    response.raise_for_status()
                    with part.open("wb") as stream:
                        for chunk in response.iter_content(1024 * 1024):
                            stream.write(chunk)
                with zipfile.ZipFile(part) as z:
                    if z.testzip() is not None:
                        raise ValueError("ZIP con CRC incorrecto")
                part.replace(path)
            with zipfile.ZipFile(path) as z:
                files = len(z.infolist())
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            return {**item, "url": url, "archivo": str(path.resolve()), "bytes": path.stat().st_size,
                    "sha256": digest, "miembros_zip": files, "segundos": round(time.monotonic()-start, 2), "estado": "ok"}
        except (requests.RequestException, OSError, ValueError, zipfile.BadZipFile) as exc:
            error = str(exc)
            if intento < 2:
                time.sleep(2 ** intento)
    return {**item, "url": url, "estado": "error", "error": error}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventario", type=Path, required=True)
    parser.add_argument("--destino", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2, choices=range(1, 5))
    args = parser.parse_args()
    items = json.loads(args.inventario.read_text())
    report = []
    args.destino.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(descargar, item, args.destino) for item in items]
        for future in as_completed(futures):
            result = future.result()
            report.append(result)
            print(f"[{len(report)}/{len(items)}] {result['estado']} {result['archivo_origen']} {result.get('bytes', 0)/1e6:.1f} MB", flush=True)
            temp = args.destino / "descargas.json.tmp"
            temp.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
            temp.replace(args.destino / "descargas.json")
    if any(r["estado"] != "ok" for r in report):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
