"""
RED: PETICIONES CON REINTENTOS
==============================
Peticiones HTTP de los scrapers autonómicos y municipales que comparten el
mismo bloque de red (Castilla y León, Extremadura, La Rioja, Murcia, los
contratos menores de Andalucía, Canarias, Castilla-La Mancha, la Comunitat
Valenciana y los municipios). Antes, cada script tenía su copia; aquí queda
una sola, con el mismo código.

- pedir_json / pedir_texto: GET con reintentos. Se reintentan los fallos de
  red, los códigos de CODIGOS_REINTENTABLES (408, 425, 429 y 5xx) y, en
  pedir_json, una respuesta que no es JSON; un 4xx lanza ErrorPortal con el
  código, sin reintentar.
- descargar: descarga en streaming a un temporal, lo valida con la función
  validar_contenido del scraper (cada portal tiene la suya) y lo entrega a
  comun.historico.guardar_version: si no cambió no se toca nada y si cambió
  la copia previa pasa a _historico/.
- _espera: backoff exponencial (2, 4, 8, 16 s, con tope en ESPERA_MAXIMA) o
  lo que pida el servidor en Retry-After.

Las constantes valen lo mismo en todos los scripts que las importan. Un
script que necesita otro valor no la importa: la define él y la usan sus
propias funciones (p. ej. TIMEOUT_API = 120 en Castilla-La Mancha, con su
propia función de petición).

Las peticiones van por requests.get y las esperas por time.sleep, buscados en
el módulo en cada llamada: los tests que los sustituyen con monkeypatch
(requests.get, time.sleep) siguen funcionando igual.
"""

import time
from pathlib import Path

import requests

from comun.historico import guardar_version

# Los mismos valores en todos los scripts que las importan
CABECERAS = {"User-Agent": "licitaciones-espana (+https://github.com/BquantFinance/licitaciones-espana)"}
TIMEOUT_API = 60
TIMEOUT_DESCARGA = 600
INTENTOS = 5
ESPERA_BASE = 2.0            # segundos: 2, 4, 8, 16 entre intentos
ESPERA_MAXIMA = 120.0
CODIGOS_REINTENTABLES = {408, 425, 429, 500, 502, 503, 504}
ERRORES_RED = (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
               requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError)


class ErrorPortal(Exception):
    """Fallo de un portal: un 4xx (con su código en .codigo) o los reintentos agotados."""

    def __init__(self, mensaje, codigo=None):
        super().__init__(mensaje)
        self.codigo = codigo


def _espera(intento, respuesta=None):
    """Backoff exponencial, o lo que pida el servidor en Retry-After."""
    valor = (getattr(respuesta, "headers", None) or {}).get("Retry-After") if respuesta is not None else None
    if valor and str(valor).strip().isdigit():
        return min(float(valor), ESPERA_MAXIMA)
    return min(ESPERA_BASE * 2 ** (intento - 1), ESPERA_MAXIMA)


def pedir_json(url, params=None):
    """GET a una API JSON con reintentos (red, 429, 5xx, respuesta no JSON).
    Un 4xx no se reintenta: se lanza ErrorPortal con el código."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.json()
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except ValueError as e:
            detalle = f"respuesta no JSON ({str(e)[:80]})"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def pedir_texto(url):
    """GET de una página HTML con reintentos (red, 429, 5xx); un 4xx lanza
    ErrorPortal."""
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        respuesta = None
        try:
            respuesta = requests.get(url, headers=CABECERAS, timeout=TIMEOUT_API)
            codigo = respuesta.status_code
            if codigo not in CODIGOS_REINTENTABLES:
                if codigo >= 400:
                    raise ErrorPortal(f"HTTP {codigo}", codigo)
                return respuesta.text
            detalle = f"HTTP {codigo}"
        except ErrorPortal:
            raise
        except ERRORES_RED as e:
            detalle = f"{type(e).__name__}: {str(e)[:150]}"
        except requests.exceptions.RequestException as e:
            raise ErrorPortal(f"{type(e).__name__}: {str(e)[:150]}") from e
        if intento < INTENTOS:
            time.sleep(_espera(intento, respuesta))
    raise ErrorPortal(f"{detalle} (tras {INTENTOS} intentos)")


def descargar(url, destino, params=None, tipo=None, *, validar_contenido):
    """Descarga `url` en `destino` sin perder nunca la versión anterior.

    Escribe en un temporal, lo valida con validar_contenido(ruta, tipo), la
    función del scraper que devuelve el motivo si no es el fichero esperado (o
    None), y lo entrega a guardar_version(): si el contenido no cambió no se
    toca nada y si cambió la copia previa pasa a _historico/. Reintenta con
    backoff los fallos de red, 429 y 5xx (también cortes a mitad de descarga).
    Devuelve (estado, detalle): 'nuevo' | 'actualizado' | 'sin_cambios',
    'no_existe' (404/410), 'invalido' (no es el fichero esperado) o 'error'.
    """
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(f".{destino.name}.part")
    detalle = ""
    try:
        for intento in range(1, INTENTOS + 1):
            respuesta = None
            try:
                with requests.get(url, params=params, headers=CABECERAS, timeout=TIMEOUT_DESCARGA,
                                  stream=True) as respuesta:
                    codigo = respuesta.status_code
                    if codigo in (404, 410):
                        return "no_existe", f"HTTP {codigo}"
                    if codigo not in CODIGOS_REINTENTABLES:
                        if codigo >= 400:
                            return "error", f"HTTP {codigo}"
                        with open(tmp, "wb") as f:
                            for trozo in respuesta.iter_content(chunk_size=1 << 16):
                                if trozo:
                                    f.write(trozo)
                        motivo = validar_contenido(tmp, tipo)
                        if motivo:
                            return "invalido", motivo
                        return guardar_version(destino, desde=tmp), ""
                    detalle = f"HTTP {codigo}"
            except ERRORES_RED as e:
                detalle = f"{type(e).__name__}: {str(e)[:150]}"
            except requests.exceptions.RequestException as e:
                return "error", f"{type(e).__name__}: {str(e)[:150]}"
            if intento < INTENTOS:
                time.sleep(_espera(intento, respuesta))
        return "error", f"{detalle} (tras {INTENTOS} intentos)"
    finally:
        if tmp.exists():
            tmp.unlink()
