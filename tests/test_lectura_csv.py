"""Tests de comun/lectura_csv.py: CSV con comillas literales al principio de un campo."""
import csv
import io
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from comun.lectura_csv import registros_csv  # noqa: E402


def _csv_reader(texto, sep):
    return [f for f in csv.reader(io.StringIO(texto, newline=''), delimiter=sep) if f]


def test_csv_bien_formado_igual_que_el_modulo_csv():
    """Con CSV bien escritos (csv.writer escapa las comillas y entrecomilla lo
    que hace falta) el resultado es exactamente el del módulo csv."""
    azar = random.Random(20260927)
    trozos = ['a', 'b c', '"', '""', ';', ',', '\n', '\r\n', 'ñ€', '', '1.234,56', ' ']
    for _ in range(300):
        sep = azar.choice([';', ','])
        ancho = azar.randint(1, 6)
        filas = [[''.join(azar.choice(trozos) for _ in range(azar.randint(0, 4))) for _ in range(ancho)]
                 for _ in range(azar.randint(1, 8))]
        salida = io.StringIO(newline='')
        csv.writer(salida, delimiter=sep, lineterminator=azar.choice(['\n', '\r\n'])).writerows(filas)
        texto = salida.getvalue()
        registros, literales = registros_csv(texto, sep)
        assert registros == _csv_reader(texto, sep), texto
        assert literales == 0


def test_comilla_literal_que_se_tragaria_registros():
    """Caso real de Castilla y León: un título que empieza por comilla sin
    cerrar se tragaba los registros siguientes hasta la próxima comilla."""
    texto = ('Codigo;Titulo;Organo;Importe\n'
             'B1;"ACONDICIONAMIENTO DE CAMINO RURAL (ZAMORA);Consejeria A;45.572,23\n'
             'B2;SUSTITUCION CENTRAL DE ALARMA;Delegacion Avila;1.804,00\n'
             'B3;MATERIAL DIDACTICO;Gerencia;500,00\n'
             'B4;REPRESENTACION JUANA I, LA SEMILLA DE LA LOCURA"-ANA RONCERO;Delegacion Avila;1149.5\n')
    assert len(_csv_reader(texto, ';')) == 2        # el módulo csv junta B1..B4 en un registro
    registros, literales = registros_csv(texto, ';')
    assert literales == 1
    assert registros == [
        ['Codigo', 'Titulo', 'Organo', 'Importe'],
        ['B1', '"ACONDICIONAMIENTO DE CAMINO RURAL (ZAMORA)', 'Consejeria A', '45.572,23'],
        ['B2', 'SUSTITUCION CENTRAL DE ALARMA', 'Delegacion Avila', '1.804,00'],
        ['B3', 'MATERIAL DIDACTICO', 'Gerencia', '500,00'],
        ['B4', 'REPRESENTACION JUANA I, LA SEMILLA DE LA LOCURA"-ANA RONCERO', 'Delegacion Avila', '1149.5'],
    ]


def test_campo_entrecomillado_legitimo_con_saltos_de_linea():
    """Una descripción en varias líneas (sin forma de registro) sigue siendo un campo."""
    texto = 'a;b;c\n1;"primera linea\nsegunda; con un separador\ntercera";3\n4;5;6\n'
    registros, literales = registros_csv(texto, ';')
    assert registros == _csv_reader(texto, ';') == [
        ['a', 'b', 'c'], ['1', 'primera linea\nsegunda; con un separador\ntercera', '3'], ['4', '5', '6']]
    assert literales == 0


def test_comilla_que_no_se_cierra_nunca():
    """Sin comilla de cierre en todo el fichero la comilla es literal (el
    módulo csv se tragaría el resto del fichero en un campo)."""
    texto = 'a;b\n1;"sin cierre\n2;dos\n3;tres\n'
    registros, literales = registros_csv(texto, ';')
    assert registros == [['a', 'b'], ['1', '"sin cierre'], ['2', 'dos'], ['3', 'tres']]
    assert literales == 1


def test_comillas_escapadas_y_texto_tras_el_cierre_como_el_modulo_csv():
    texto = 'a;b;c\n"x ""y"" z";"ABC" DEF;fin\n'
    registros, _ = registros_csv(texto, ';')
    assert registros == _csv_reader(texto, ';') == [['a', 'b', 'c'], ['x "y" z', 'ABC DEF', 'fin']]


def test_saltos_crlf_y_lineas_vacias():
    texto = 'a;b\r\n1;"x\r\ny"\r\n\r\n2;z\r\n'
    registros, _ = registros_csv(texto, ';')
    assert registros == _csv_reader(texto, ';') == [['a', 'b'], ['1', 'x\r\ny'], ['2', 'z']]
