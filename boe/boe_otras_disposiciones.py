#!/usr/bin/env python3
"""
=============================================================================
BOE - SECCIÓN III «OTRAS DISPOSICIONES»: CONVENIOS, ADENDAS, PROTOCOLOS…
=============================================================================
Descarga, día a día, la sección III del sumario del Boletín Oficial del
Estado por su API de datos abiertos y genera un Parquet con una fila por
disposición: identificador (BOE-A-…), fecha, departamento, epígrafe, título y
enlaces al PDF, al HTML y al XML. La sección III es donde se publican los
convenios de la Administración General del Estado (desde la Ley 40/2015, art.
48.8, un convenio solo es eficaz cuando se inscribe en el REOICO y se publica
en el BOE), sus adendas y prórrogas, los protocolos generales de actuación,
encargos, ayudas, premios y otras resoluciones. Con los convenios se ve con
quién se relaciona cada órgano fuera de los contratos: universidades,
fundaciones, empresas, otras administraciones.

Ejecutar:  python boe/boe_otras_disposiciones.py [--salida DIR] [--desde AAAA-MM-DD]
           [--hasta AAAA-MM-DD] [--comprobar-todo] [--solo-parquet]

Salida (por defecto <repo>/boe_otras_disposiciones/):
    raw/<año>/<AAAAMMDD>.json     la sección III del sumario de ese día (JSON ordenado), con los títulos
                                  tal cual salvo los nombres de personas físicas (ver abajo)
    raw/<año>/_historico/         versiones anteriores de cada día (nunca se borran)
    raw/_manifiesto.json          por día: URL, sha256 y bytes del sumario completo, disposiciones,
                                  fecha de descarga, última comprobación; los días sin BOE (404)
    raw/descarga_log.txt          resumen de cada ejecución (se añade al final)
    disposiciones.parquet, _historico/   registros acumulados de todos los días, como texto

Columnas: identificador, control, fecha (AAAAMMDD del diario), diario (número),
departamento, departamento_codigo, epigrafe, titulo, url_pdf, url_html,
url_xml. Añadidas por el script:
- _tipo: 'convenio_colectivo' si el título habla de un convenio colectivo
  (laboral, entre una empresa o un sector y los sindicatos, que la Dirección
  General de Trabajo registra y publica aquí); si no, la primera de convenio,
  adenda, protocolo, encargo, encomienda o acuerdo que aparece en el título
  como palabra (minúsculas); vacía si no hay ninguna. Es una marca para
  filtrar, no una clasificación jurídica: «Resolución ... por la que se publica
  la Adenda al Convenio ...» es 'adenda'.
- _personas_quitadas: cuántos nombres de personas físicas se han sustituido
  en el título por «[persona física]» (tratamiento con «don», «doña», «D.» o
  «Dña.» seguido de nombre y apellidos: premios, honores, convenios con
  particulares). Las personas jurídicas no se tocan.
- De origen (no cuentan al comparar versiones): _archivo_origen y
  _fecha_descarga; y las 3 de control de comun/historico.py.

Sesgo del superviviente (docs/PRINCIPIOS.md; comun/historico.py):
- Capa cruda: el extracto de cada día pasa por guardar_version: si no cambia no
  se toca y si cambia (una corrección del sumario) la copia anterior va a
  _historico/. El sumario completo no se guarda (trae nombres de personas en
  otras secciones); su sha256 queda en el manifiesto y el BOE lo sirve siempre
  igual para cualquier fecha.
- Parquet: acumular() con el día como ámbito: una disposición que desaparece
  del sumario de un día que se vuelve a pedir sigue con _en_ultima_descarga=False.

Qué se pide en cada ejecución (una petición por día, con PAUSA entre ellas):
- Los días que faltan desde --desde (por defecto, PRIMER_DIA o 2 × DIAS_REPASO
  días antes del último que se tiene) hasta --hasta (por defecto, hoy en UTC), y
  los últimos DIAS_REPASO días ya descargados, por si el sumario cambió; todos
  los ya descargados solo con --comprobar-todo. Los domingos no se piden: solo
  hay ediciones extraordinarias, raras y casi siempre de la sección I (la del
  15-mar-2020 trae solo la I). Un año entero son unas 310 peticiones;
  2016-10-02 a hoy, unas 3.100.
- Un 404 es un día sin BOE (se anota y no se vuelve a pedir). Por eso no se
  debe pedir el día en curso antes de que salga el BOE: con --hasta por defecto
  y de madrugada en España, la fecha UTC aún es la del día anterior. 403 o 429
  paran la ejecución. Sale con 1 si algún día no se pudo descargar o leer.

FUENTES
-------
Verificado en vivo el 2026-10-26 (confianza A):
  - API: https://www.boe.es/datosabiertos/api/boe/sumario/AAAAMMDD con la cabecera
    «Accept: application/json» (sin ella responde XML). Respuesta:
    {"status": {"code": "200"}, "data": {"sumario": {"metadatos": {...}, "diario": [{"numero",
    "sumario_diario": {...}, "seccion": [{"codigo": "3", "nombre": "III. Otras disposiciones",
    "departamento": [{"codigo", "nombre", "epigrafe": [{"nombre", "item": [...]}], "item": ...}]}]}]}}}.
    Un elemento suelto en vez de una lista de uno es lo normal (item, epigrafe, departamento):
    el script acepta los dos. Cada item trae identificador, control, titulo, url_pdf ({"texto": ...}),
    url_html y url_xml.
  - 2024-01-01 a 2026-10-24 (descarga del 2026-10-26): 882 sumarios, uno por día de lunes a sábado
    (ningún 404), de 65 a 642 KB; la sección III, de 3 a 98 disposiciones al día (mediana 31), 29.477
    en total: 9.848 convenios, 1.831 acuerdos, 1.679 adendas y 850 convenios colectivos por la marca
    _tipo. La CNMV publica aquí también sus sanciones por infracciones muy graves.
  - Condiciones de reutilización: licencia tipo de la Agencia Estatal BOE (Resolución de 27 de
    junio de 2024): citar la fuente con un enlace a boe.es; respetar el RGPD.
=============================================================================
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import acumular, guardar_registros, guardar_version, leer_registros  # noqa: E402
from comun.red import (CABECERAS, CODIGOS_REINTENTABLES, ERRORES_RED, INTENTOS, TIMEOUT_API,  # noqa: E402
                       _espera)

URL_SUMARIO = "https://www.boe.es/datosabiertos/api/boe/sumario/{dia}"
SECCION = "3"
PRIMER_DIA = date(2016, 10, 2)        # entrada en vigor de la Ley 40/2015 (convenios publicados en el BOE)
DIAS_REPASO = 7
PAUSA = 5.0
SALIDA = Path(__file__).resolve().parent.parent / "boe_otras_disposiciones"
TITULO = "BOE - SECCIÓN III"
COLUMNAS = ("identificador", "control", "fecha", "diario", "departamento", "departamento_codigo", "epigrafe",
            "titulo", "url_pdf", "url_html", "url_xml")
METADATOS_ORIGEN = ("_archivo_origen", "_fecha_descarga")
TIPOS = re.compile(r"\b(convenio|adenda|protocolo|encargo|encomienda|acuerdo)\b", re.IGNORECASE)
MAYUS = "A-ZÁÉÍÓÚÜÑ"
PALABRA = rf"(?:[{MAYUS}][\w{MAYUS}áéíóúüñ'-]+|[{MAYUS}]\.)"     # «Rosa», «Martínez-Vela» o una inicial «J.»
TRATAMIENTO = r"(?:don|doña|D\.|Dña\.|Dª)"
PERSONA = re.compile(rf"\b{TRATAMIENTO}\s+{PALABRA}"
                     rf"(?:\s+(?:de\s+(?:la\s+|los\s+|las\s+)?|del\s+|y\s+(?!{TRATAMIENTO}\s))?{PALABRA}){{1,5}}")
COLECTIVO = re.compile(r"\bconvenios?\s+colectivos?\b", re.IGNORECASE)
SUSTITUTO = "[persona física]"


class Parada(Exception):
    """403/429: el portal pide que se pare."""


def ahora():
    return datetime.now(timezone.utc)


def _lista(v):
    return v if isinstance(v, list) else ([v] if v else [])


def sin_personas(titulo):
    """(título con los nombres de personas físicas sustituidos, cuántos)."""
    return PERSONA.subn(SUSTITUTO, titulo or "")


def tipo(titulo):
    """Marca de tipo por la primera palabra clave del título; los convenios colectivos (laborales,
    entre empresas y sindicatos) van aparte."""
    if COLECTIVO.search(titulo or ""):
        return "convenio_colectivo"
    m = TIPOS.search(titulo or "")
    return m.group(1).lower() if m else None


def seccion3(sumario, dia):
    """Disposiciones de la sección III de un sumario (dict de la API), en el orden del sumario."""
    out = []
    for diario in _lista(sumario.get("diario")):
        for sec in _lista(diario.get("seccion")):
            if str(sec.get("codigo")) != SECCION:
                continue
            for dep in _lista(sec.get("departamento")):
                grupos = [(ep.get("nombre"), _lista(ep.get("item"))) for ep in _lista(dep.get("epigrafe"))]
                grupos.append((None, _lista(dep.get("item"))))
                for epigrafe, items in grupos:
                    for it in items:
                        titulo, n = sin_personas(it.get("titulo"))
                        pdf = it.get("url_pdf")
                        out.append({"identificador": it.get("identificador"), "control": it.get("control"),
                                    "fecha": dia, "diario": str(diario.get("numero") or ""),
                                    "departamento": dep.get("nombre"), "departamento_codigo": dep.get("codigo"),
                                    "epigrafe": epigrafe, "titulo": titulo,
                                    "url_pdf": pdf.get("texto") if isinstance(pdf, dict) else pdf,
                                    "url_html": it.get("url_html"), "url_xml": it.get("url_xml"),
                                    "_tipo": tipo(titulo), "_personas_quitadas": n})
    return out


def pedir_sumario(dia):
    """(código, bytes) del sumario de `dia` (AAAAMMDD) en JSON, con reintentos de red, 429 y 5xx."""
    url = URL_SUMARIO.format(dia=dia)
    cabeceras = dict(CABECERAS, Accept="application/json")
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, headers=cabeceras, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo == 403:
                raise Parada(f"HTTP 403 en {dia}")
            if codigo not in CODIGOS_REINTENTABLES:
                return codigo, respuesta.content
            detalle = f"HTTP {codigo}"
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    if detalle == "HTTP 429":                     # 429 tras todos los reintentos: se para
        raise Parada(f"HTTP 429 en {dia}")
    return None, f"{detalle} (tras {INTENTOS} intentos)"


class Manifiesto:
    def __init__(self, raw):
        self.ruta = Path(raw) / "_manifiesto.json"
        try:
            self.datos = json.loads(self.ruta.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            self.datos = {}

    def guardar(self):
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_name(f".{self.ruta.name}.tmp")
        tmp.write_text(json.dumps(self.datos, ensure_ascii=False, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(self.ruta)


def dias_a_pedir(manifiesto, desde, hasta, comprobar_todo):
    """Días (date) que se piden: los que faltan entre desde y hasta, los últimos DIAS_REPASO que ya se
    tienen y, con comprobar_todo, todos. Nunca domingos ni días anotados sin BOE."""
    tenidos = sorted(d for d, v in manifiesto.datos.items() if v.get("estado") == "ok")
    out = []
    d = desde
    repaso = set(tenidos[-DIAS_REPASO:])
    while d <= hasta:
        clave = d.strftime("%Y%m%d")
        v = manifiesto.datos.get(clave, {})
        if d.weekday() != 6 and v.get("estado") != "sin_boe" and (
                v.get("estado") != "ok" or comprobar_todo or clave in repaso):
            out.append(d)
        d += timedelta(days=1)
    return out


def descargar(salida, desde, hasta, comprobar_todo, resumen):
    raw = salida / "raw"
    man = Manifiesto(raw)
    if desde is None:
        tenidos = sorted(d for d, v in man.datos.items() if v.get("estado") == "ok")
        desde = (datetime.strptime(tenidos[-1], "%Y%m%d").date() - timedelta(days=DIAS_REPASO * 2)) if tenidos \
            else PRIMER_DIA
    ok = True
    for i, d in enumerate(dias_a_pedir(man, desde, hasta, comprobar_todo)):
        if i:
            time.sleep(PAUSA)
        dia = d.strftime("%Y%m%d")
        try:
            codigo, cuerpo = pedir_sumario(dia)
        except Parada as e:
            resumen["errores"].append(f"parada: {e}")
            ok = False
            break
        entrada = man.datos.setdefault(dia, {})
        entrada.update(url=URL_SUMARIO.format(dia=dia), comprobado=ahora().isoformat(timespec="seconds"))
        if codigo == 404:
            entrada["estado"] = "sin_boe"
            resumen["sin_boe"] += 1
        elif codigo == 200:
            try:
                sumario = json.loads(cuerpo)["data"]["sumario"]
                items = seccion3(sumario, dia)
            except (ValueError, KeyError, TypeError, AttributeError) as e:
                resumen["errores"].append(f"{dia}: respuesta ilegible ({type(e).__name__})")
                ok = False
                continue
            contenido = json.dumps({"dia": dia, "metadatos": sumario.get("metadatos"), "seccion3": items},
                                   ensure_ascii=False, indent=0, sort_keys=True).encode("utf-8")
            estado = guardar_version(raw / dia[:4] / f"{dia}.json", contenido)
            entrada.update(estado="ok", sumario_sha256=hashlib.sha256(cuerpo).hexdigest(),
                           sumario_bytes=len(cuerpo), disposiciones=len(items))
            if estado != "sin_cambios" or "fecha_descarga" not in entrada:
                entrada["fecha_descarga"] = entrada["comprobado"]
            resumen[estado] += 1
        else:
            resumen["errores"].append(f"{dia}: {codigo or cuerpo}")
            ok = False
        man.guardar()
    man.guardar()
    return ok


def construir_parquet(salida, resumen):
    raw = salida / "raw"
    man = Manifiesto(raw)
    filas = []
    for dia, v in sorted(man.datos.items()):
        if v.get("estado") != "ok":
            continue
        ruta = raw / dia[:4] / f"{dia}.json"
        try:
            datos = json.loads(ruta.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            resumen["errores"].append(f"{ruta.name}: {e}")
            return False
        for it in datos["seccion3"]:
            fila = {c: it.get(c) for c in COLUMNAS}
            fila["_tipo"] = it.get("_tipo")
            fila["_personas_quitadas"] = str(it.get("_personas_quitadas", 0))
            fila["_archivo_origen"] = f"raw/{dia[:4]}/{dia}.json"
            fila["_fecha_descarga"] = v.get("fecha_descarga")
            filas.append(fila)
    if not filas:
        resumen["errores"].append("ninguna disposición en raw/: no se toca el Parquet")
        return False
    nuevos = pd.DataFrame(filas).astype(object).where(lambda df: df.notna(), None)
    destino = salida / "disposiciones.parquet"
    anterior = leer_registros(destino)
    out = acumular(anterior, nuevos, ahora().isoformat(timespec="seconds"), ambito=["fecha"],
                   ignorar=METADATOS_ORIGEN)
    resumen["parquet"] = f"{guardar_registros(out, destino)}: {len(out)} filas"
    return True


def main(argv=None):
    p = argparse.ArgumentParser(description=TITULO)
    p.add_argument("--salida", type=Path, default=SALIDA)
    p.add_argument("--desde", type=date.fromisoformat, default=None)
    p.add_argument("--hasta", type=date.fromisoformat, default=None)
    p.add_argument("--comprobar-todo", action="store_true")
    p.add_argument("--solo-parquet", action="store_true")
    a = p.parse_args(argv)
    hasta = a.hasta or ahora().date()
    resumen = {"inicio": ahora().isoformat(timespec="seconds"), "nuevo": 0, "actualizado": 0, "sin_cambios": 0,
               "sin_boe": 0, "errores": []}
    ok = True
    if not a.solo_parquet:
        ok = descargar(a.salida, a.desde, hasta, a.comprobar_todo, resumen)
    ok = construir_parquet(a.salida, resumen) and ok
    raw = a.salida / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    lineas = [f"=== {resumen['inicio']} {TITULO}"] + [f"  {k}: {v}" for k, v in resumen.items()
                                                     if k not in ("inicio", "errores")]
    lineas += [f"  ERROR: {e}" for e in resumen["errores"]]
    with open(raw / "descarga_log.txt", "a", encoding="utf-8") as fh:
        fh.write("\n".join(lineas) + "\n")
    print("\n".join(lineas))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
