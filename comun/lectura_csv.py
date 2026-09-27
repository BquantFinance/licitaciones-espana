"""
LECTURA DE CSV CON COMILLAS LITERALES
=====================================
Algunos portales exportan CSV sin escapar las comillas del texto: un título
que empieza por '"' (p.ej. '"ACONDICIONAMIENTO DE CAMINO RURAL...') abre, para
cualquier lector estándar (módulo csv, pandas), un campo entrecomillado que
se cierra en la siguiente comilla del fichero, a veces decenas de líneas más
abajo. Todos los registros de en medio acaban dentro de ese campo: no se
pierde el texto, pero sí los registros como filas (en Castilla y León, 108
contratos menores en 2026).

registros_csv lee como el módulo csv salvo en ese caso: una comilla al
principio de un campo solo abre un campo entrecomillado si lo que abarca
hasta su cierre no contiene ninguna línea con forma de registro completo
(todas sus líneas con al menos ancho - 1 separadores fuera de comillas, con
ancho el nº de campos de la cabecera). Si la contiene, o si la comilla no se cierra nunca, la
comilla es literal: se conserva como parte del texto y el campo termina en el
siguiente separador o fin de línea, como uno sin comillas. Un campo
entrecomillado legítimo con saltos de línea (una descripción en varias
líneas) se lee igual que con el módulo csv.
"""

import csv
import io

# Columnas mínimas para distinguir una comilla literal (ver _comilla_literal)
ANCHO_MINIMO = 4


def _separadores_fuera_de_comillas(linea, sep):
    """Separadores de una línea leída sola, sin contar los de dentro de un
    campo entrecomillado que se cierra en la misma línea."""
    n, dentro, i = 0, False, 0
    while i < len(linea):
        c = linea[i]
        if dentro:
            if c == '"':
                if i + 1 < len(linea) and linea[i + 1] == '"':
                    i += 1
                else:
                    dentro = False
        elif c == '"' and (i == 0 or linea[i - 1] == sep):
            cierre = _cierre(linea, i)
            if cierre is not None:
                dentro = True
        elif c == sep:
            n += 1
        i += 1
    return n


def _cierre(texto, inicio):
    """Posición de la comilla que cierra el campo entrecomillado que abre la
    comilla de `inicio` o None si no se cierra. Como en el módulo csv (no
    estricto), cierra la primera comilla que no es doble ('""' es una comilla
    escapada); lo que la siga hasta el separador se añade al campo."""
    i = inicio + 1
    while True:
        j = texto.find('"', i)
        if j < 0:
            return None
        if j + 1 < len(texto) and texto[j + 1] == '"':
            i = j + 2
            continue
        return j


def _fin_de_linea(texto, i):
    """Posición del salto de línea (o fin del texto) de la línea de `i`."""
    fin = len(texto)
    for salto in ('\r', '\n'):
        j = texto.find(salto, i)
        if 0 <= j < fin:
            fin = j
    return fin


def _comilla_literal(texto, inicio, cierre, sep, ancho, campos_previos):
    """Si la comilla de `inicio` (principio de un campo) es literal: no se
    cierra nunca, o el campo entrecomillado hasta `cierre` abarca varias
    líneas que, leídas cada una por su cuenta (y la primera con esta comilla
    como texto), tienen todas al menos `ancho` campos (algunos portales
    terminan cada línea con un separador de más) y al menos una línea
    queda entera dentro: son registros enteros que el campo se tragaría. Un
    campo legítimo en varias líneas (una descripción) no cumple esto. Con
    menos de ANCHO_MINIMO columnas no se distingue bien (una línea de texto
    con una coma ya tendría forma de registro): se lee como el módulo csv."""
    if cierre is None:
        return True
    if ancho < ANCHO_MINIMO or not any(salto in texto[inicio:cierre] for salto in ('\r', '\n')):
        return False
    lineas = texto[inicio:_fin_de_linea(texto, cierre)].replace('\r\n', '\n').replace('\r', '\n').split('\n')
    if len(lineas) < 3:
        return False   # ninguna línea entera dentro: no se traga ningún registro
    if campos_previos + _separadores_fuera_de_comillas(lineas[0][1:], sep) + 1 < ancho:
        return False
    return all(_separadores_fuera_de_comillas(linea, sep) + 1 >= ancho for linea in lineas[1:])


def registros_csv(texto, sep, ancho=None):
    """Registros (listas de campos) de un CSV ya decodificado, como
    csv.reader(delimiter=sep) salvo las comillas literales (ver el módulo).
    ancho: nº de campos de un registro completo; por defecto, el de la
    cabecera (primer registro). Devuelve (registros, comillas_literales)."""
    if ancho is None:
        primera = next(csv.reader(io.StringIO(texto, newline=''), delimiter=sep), [])
        ancho = len(primera)
    registros, fila, literales = [], [], 0
    i, n = 0, len(texto)
    while i < n:
        if not fila and texto[i] in ('\r', '\n'):
            # Línea vacía: no es un registro (como en el módulo csv)
            i += 2 if texto.startswith('\r\n', i) else 1
            continue
        # Un campo empieza en i
        if texto[i] == '"':
            cierre = _cierre(texto, i)
            if not _comilla_literal(texto, i, cierre, sep, ancho, len(fila)):
                valor = texto[i + 1:cierre].replace('""', '"')
                i = cierre + 1
                # Texto pegado tras la comilla de cierre hasta el separador
                # (el módulo csv lo añade al campo): se conserva igual
                fin = i
                while fin < n and texto[fin] not in (sep, '\r', '\n'):
                    fin += 1
                valor += texto[i:fin]
                i = fin
            else:
                literales += 1
                fin = i
                while fin < n and texto[fin] not in (sep, '\r', '\n'):
                    fin += 1
                valor = texto[i:fin]
                i = fin
        else:
            fin = i
            while fin < n and texto[fin] not in (sep, '\r', '\n'):
                fin += 1
            valor = texto[i:fin]
            i = fin
        fila.append(valor)
        if i < n and texto[i] == sep:
            i += 1
            if i == n:   # separador al final del texto: un último campo vacío
                fila.append('')
            continue
        # Fin de línea o de texto: fin del registro
        if i < n and texto[i] == '\r':
            i += 1
        if i < n and texto[i] == '\n':
            i += 1
        registros.append(fila)
        fila = []
    if fila:
        registros.append(fila)
    return registros, literales
