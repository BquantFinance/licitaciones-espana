"""Tests de comun/historico.py (control del sesgo del superviviente)."""
import os

import numpy as np
import pandas as pd
import pytest

from comun import historico as h


def _mtime(ruta, epoch):
    os.utime(ruta, (epoch, epoch))


# ── guardar_version ──────────────────────────────────────────

def test_guardar_version_nuevo_sin_cambios_y_actualizado(tmp_path):
    destino = tmp_path / "datos" / "contratos_2025.csv"
    assert h.guardar_version(destino, b"a;b\n1;2\n") == "nuevo"
    _mtime(destino, 1_700_000_000)                     # 2023-11-14T22:13:20Z

    assert h.guardar_version(destino, b"a;b\n1;2\n") == "sin_cambios"
    assert not (destino.parent / h.HISTORICO).exists()

    assert h.guardar_version(destino, b"a;b\n1;2\n3;4\n") == "actualizado"
    antigua = destino.parent / h.HISTORICO / "contratos_2025__20231114T221320Z.csv"
    assert antigua.read_bytes() == b"a;b\n1;2\n"       # la versión anterior no se pierde
    assert destino.read_bytes() == b"a;b\n1;2\n3;4\n"
    assert h.versiones(destino) == [antigua, destino]
    assert not list(destino.parent.glob(".*"))         # sin temporales


def test_guardar_version_desde_fichero_y_sellos_repetidos(tmp_path):
    destino = tmp_path / "x.zip"
    for i, contenido in enumerate([b"v1", b"v2", b"v3"]):
        part = tmp_path / "x.zip.part"
        part.write_bytes(contenido)
        h.guardar_version(destino, desde=part)
        _mtime(destino, 1_700_000_000)                  # mismo sello para todas
        assert not part.exists()
    assert [p.read_bytes() for p in h.versiones(destino)] == [b"v1", b"v2", b"v3"]


def test_guardar_version_exige_contenido_o_desde(tmp_path):
    with pytest.raises(ValueError):
        h.guardar_version(tmp_path / "a")
    with pytest.raises(ValueError):
        h.guardar_version(tmp_path / "a", b"x", desde=tmp_path / "b")


# ── acumular ─────────────────────────────────────────────────

def _df(filas, cols=("id", "importe")):
    return pd.DataFrame(filas, columns=list(cols))


def test_acumular_primera_descarga():
    out = h.acumular(None, _df([["1", "10"], ["2", "20"]]), "2026-01-01")
    assert out["_primera_descarga"].tolist() == ["2026-01-01"] * 2
    assert out["_en_ultima_descarga"].tolist() == [True, True]


def test_registro_retirado_y_modificado_se_conservan():
    t1 = h.acumular(None, _df([["1", "10"], ["2", "20"], ["3", "30"]]), "2026-01-01")
    # la administración retira el 2 y cambia el importe del 3
    t2 = h.acumular(t1, _df([["1", "10"], ["3", "35"]]), "2026-02-01")
    assert t2[["id", "importe", "_primera_descarga", "_ultima_descarga",
               "_en_ultima_descarga"]].values.tolist() == [
        ["1", "10", "2026-01-01", "2026-02-01", True],
        ["2", "20", "2026-01-01", "2026-01-01", False],    # retirado: sigue ahí
        ["3", "30", "2026-01-01", "2026-01-01", False],    # versión antigua
        ["3", "35", "2026-02-01", "2026-02-01", True],     # versión nueva
    ]


def test_registro_que_vuelve_a_aparecer_se_reactiva_sin_duplicarse():
    t1 = h.acumular(None, _df([["1", "10"], ["2", "20"]]), "d1")
    t2 = h.acumular(t1, _df([["1", "10"]]), "d2")
    t3 = h.acumular(t2, _df([["1", "10"], ["2", "20"]]), "d3")
    assert len(t3) == 2
    assert t3["_en_ultima_descarga"].tolist() == [True, True]
    assert t3["_ultima_descarga"].tolist() == ["d3", "d3"]


def test_duplicados_de_origen_como_multiconjunto():
    t1 = h.acumular(None, _df([["1", "10"], ["1", "10"]]), "d1")   # servido dos veces
    t2 = h.acumular(t1, _df([["1", "10"]]), "d2")                  # ahora una
    assert len(t2) == 2
    assert t2["_en_ultima_descarga"].tolist() == [True, False]
    t3 = h.acumular(t2, _df([["1", "10"], ["1", "10"], ["1", "10"]]), "d3")
    assert len(t3) == 3
    assert t3["_en_ultima_descarga"].tolist() == [True, True, True]


def test_ambito_limita_lo_que_se_marca_como_retirado():
    cols = ("anio", "id")
    t1 = h.acumular(None, _df([["2024", "a"], ["2025", "b"]], cols), "d1")
    # solo se vuelve a descargar 2025 (y ya no trae 'b')
    t2 = h.acumular(t1, _df([["2025", "c"]], cols), "d2", ambito=["anio"])
    estado = dict(zip(t2["id"], t2["_en_ultima_descarga"]))
    assert estado == {"a": True, "b": False, "c": True}   # 2024 no se ha comprobado


def test_columnas_nuevas_ignoradas_y_tipos():
    t1 = h.acumular(None, pd.DataFrame({"id": [1, 2], "_fecha_descarga": ["d1", "d1"]}), "d1")
    nuevos = pd.DataFrame({"id": ["1", "2"], "cpv": ["0913", None],
                           "_fecha_descarga": ["d2", "d2"]})
    t2 = h.acumular(t1, nuevos, "d2")
    assert len(t2) == 2                                   # 1 y "1" son el mismo valor
    assert t2["cpv"].iloc[0] == "0913" and pd.isna(t2["cpv"].iloc[1])   # columna nueva rellenada
    assert t2["_fecha_descarga"].tolist() == ["d1", "d1"] # no rompe la igualdad


def test_sin_columnas_en_comun_no_se_empareja_por_posicion():
    """Dos versiones sin ninguna columna en común (p.ej. el portal cambia todas las cabeceras): antes
    _claves daba la misma clave a todas las filas y el multiconjunto las emparejaba por posición: la
    fila A/X/100 se fundía con C/Z/300 y quedaba como vigente. Sin nada que comparar, ninguna casa."""
    t1 = h.acumular(None, _df([["A", "X", "100"], ["B", "Y", "200"]], ("a", "b", "c")), "d1")
    t2 = h.acumular(t1, _df([["C", "Z", "300"]], ("d", "e", "f")), "d2")
    filas = t2[["a", "b", "c", "d", "e", "f", "_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]]
    assert [[None if pd.isna(v) else v for v in fila] for fila in filas.values.tolist()] == [
        ["A", "X", "100", None, None, None, "d1", "d1", False],     # retirada: se conserva, sin fundirse
        ["B", "Y", "200", None, None, None, "d1", "d1", False],
        [None, None, None, "C", "Z", "300", "d2", "d2", True],      # la nueva, como alta
    ]
    # Tampoco si lo único en común son las columnas que no se comparan (las meta y `ignorar`)
    t1 = h.acumular(None, pd.DataFrame({"a": ["A"], "_fecha_descarga": ["d1"]}), "d1")
    t2 = h.acumular(t1, pd.DataFrame({"d": ["C"], "_fecha_descarga": ["d2"]}), "d2")
    assert t2["a"].tolist()[0] == "A" and pd.isna(t2["d"].tolist()[0]) and len(t2) == 2
    assert t2["_en_ultima_descarga"].tolist() == [False, True]


def test_descarga_vacia_no_retira_nada():
    t1 = h.acumular(None, _df([["1", "10"]]), "d1")
    with pytest.raises(ValueError):
        h.acumular(t1, _df([]), "d2")
    t2 = h.acumular(t1, _df([]), "d2", permitir_vacio=True)
    assert t2["_en_ultima_descarga"].tolist() == [False]


def test_guardar_y_leer_registros_conserva_version_anterior(tmp_path):
    ruta = tmp_path / "registros.parquet"
    assert h.leer_registros(ruta) is None
    t1 = h.acumular(None, _df([["1", "10"]]), "d1")
    assert h.guardar_registros(t1, ruta) == "nuevo"
    t2 = h.acumular(h.leer_registros(ruta), _df([["2", "20"]]), "d2")
    assert h.guardar_registros(t2, ruta) == "actualizado"
    assert len(h.versiones(ruta)) == 2
    assert h.leer_registros(ruta)["id"].tolist() == ["1", "2"]


def test_archivar_mueve_a_historico_sin_sustituir(tmp_path):
    destino = tmp_path / "tabla.parquet"
    destino.write_bytes(b"v1")
    _mtime(destino, 1_700_000_000)
    archivo = h.archivar(destino)
    assert not destino.exists()
    assert archivo == destino.parent / h.HISTORICO / "tabla__20231114T221320Z.parquet"
    assert archivo.read_bytes() == b"v1"
    destino.write_bytes(b"v2")
    _mtime(destino, 1_700_000_000)                       # mismo sello: no se pisa
    assert h.archivar(destino).read_bytes() == b"v2" and archivo.read_bytes() == b"v1"


# ── sembrar: la instantánea publicada como la más antigua ────

def _descarga():
    return pd.DataFrame({
        "id": ["a", "a", "b", "c"],
        "fecha": pd.to_datetime(["2026-01-01T10:00:00.5Z", "2026-02-01T00:00:00Z",
                                 "2026-01-05T00:00:00Z", "2026-03-01T00:00:00Z"], utc=True, format="ISO8601"),
        "importe": [10.0, 12.0, 5.0, 7.0],
        "estado": ["PUB", "ADJ", "PUB", "RES"],
    })


def _publicado():
    """Instantánea antigua: otra semántica de tipos (fechas como texto, importes
    enteros), una columna que la descarga ya no tiene y fechas que no se leyeron."""
    return pd.DataFrame({
        "id": ["a", "a", "d", "d", "b", "b", "e", "c"],
        "fecha": ["2026-01-01T10:00:00.500+00:00",   # presente (mismo instante, otro formato)
                  "2025-12-01T00:00:00Z",            # versión retirada de 'a'
                  "2025-11-11T00:00:00Z", "2025-11-11T00:00:00Z",   # id retirado, publicado dos veces
                  None,                               # fecha ilegible, mismo contenido que 'b' → presente
                  None,                               # fecha ilegible, contenido que ya no está → se añade
                  None,                               # fecha ilegible e id retirado → se añade
                  "2026-03-01T00:00:00Z"],           # presente
        "importe": [10, 9, 1, 1, 5, 6, 3, 7],
        "estado": ["PUB", "PUB", "PUB", "PUB", "PUB", "ANUL", "PUB", "RES"],
        "vieja": ["x"] * 8,
    })


def test_sembrar_anade_solo_las_claves_que_faltan():
    nuevos = _descarga()
    out, informe = h.sembrar(nuevos, _publicado(), ["id", "fecha"])
    # Las filas de la descarga, intactas y primero
    pd.testing.assert_frame_equal(out.iloc[:4][list(nuevos.columns)], nuevos)
    assert out["_origen"].iloc[:4].isna().all() and out["_en_ultima_descarga"].iloc[:4].all()
    anadidas = out.iloc[4:]
    assert anadidas["id"].tolist() == ["a", "d", "d", "b", "e"]      # 'd' dos veces: así se publicó
    assert anadidas["importe"].tolist() == [9, 1, 1, 6, 3]
    assert (anadidas["_origen"] == h.ORIGEN_SEMILLA).all()
    assert not anadidas["_en_ultima_descarga"].any()
    assert list(out.columns) == list(nuevos.columns) + ["_origen", "_en_ultima_descarga", "vieja"]
    assert out["vieja"].iloc[:4].isna().all()
    assert (informe["leidas"], informe["anadidas"], informe["descartadas_clave"],
            informe["descartadas_contenido"]) == (8, 5, 2, 1)
    assert informe["ejemplos"][h.PRESENTE_CONTENIDO] == [("b", None)]
    assert informe["ejemplos"][h.PRESENTE_CLAVE][0][0] == "a"


def test_sembrar_dos_veces_no_anade_nada():
    una, _ = h.sembrar(_descarga(), _publicado(), ["id", "fecha"])
    dos, informe = h.sembrar(una, _publicado(), ["id", "fecha"])
    assert informe["anadidas"] == 0 and len(dos) == len(una)
    pd.testing.assert_frame_equal(dos, una)


def test_sembrar_prioridad_entre_semillas_y_origen_propio():
    primera = _publicado().iloc[[1]].assign(_origen="release v2025.01")
    segunda = _publicado().iloc[[1, 2]]
    out, _ = h.sembrar(_descarga(), primera, ["id", "fecha"], origen="otra")
    out, informe = h.sembrar(out, segunda, ["id", "fecha"])
    # La fila de la primera semilla no se duplica y conserva su _origen
    assert out["_origen"].tolist()[4:] == ["release v2025.01", h.ORIGEN_SEMILLA]
    assert informe["descartadas_clave"] == 1


def test_sembrar_sin_contenido_que_comparar_anade_las_claves_incompletas():
    publicado = _publicado()
    out, informe = h.sembrar(_descarga(), publicado, ["id", "fecha"], contenido=[])
    # Sin columnas con las que comprobarlo, una fila sin fecha no se da por presente
    assert informe["descartadas_contenido"] == 0
    assert out.iloc[4:]["id"].tolist() == ["a", "d", "d", "b", "b", "e"]


def test_seleccionar_semilla_por_lotes():
    # Sin cargar la descarga entera: el contenido se pide solo para las filas candidatas
    nuevos, publicado = _descarga(), _publicado()
    pedidas = []

    def contenido_nuevos(filas):
        pedidas.extend(filas.tolist())
        return nuevos.loc[filas, ["importe", "estado"]]

    motivo = h.seleccionar_semilla(nuevos[["id", "fecha"]], publicado[["id", "fecha"]], contenido_nuevos,
                                   lambda filas: publicado.loc[filas, ["importe", "estado"]])
    assert motivo.tolist() == [h.PRESENTE_CLAVE, h.ANADIDA, h.ANADIDA, h.ANADIDA,
                               h.PRESENTE_CONTENIDO, h.ANADIDA, h.ANADIDA, h.PRESENTE_CLAVE]
    assert pedidas == [2]          # solo la fila de la descarga con el id 'b'


def test_texto_canonico_compara_tipos_distintos():
    from datetime import date
    a = h.texto_canonico(pd.Series([5, 1.5, None, date(2024, 1, 2), pd.Timestamp("2024-01-02"), True, "x"],
                                   dtype=object))
    assert a.tolist() == ["5.0", "1.5", None, "2024-01-02", "2024-01-02", "True", "x"]
    utc = h.texto_canonico(pd.Series(pd.to_datetime(["2024-01-02T01:00:00+01:00"], utc=True)))
    assert utc.tolist() == ["2024-01-02T00:00:00+00:00"]


def test_dos_semillas_con_la_misma_entrada_sin_fecha_y_con_fecha():
    # Regresión: la misma entrada retirada de la descarga, en un publicado sin
    # fecha (el código antiguo no supo leerla) y en otro con ella, se añadía dos
    # veces si el publicado sin fecha iba primero. Sale una sola vez en los dos órdenes
    def publicado(fecha, importe=4.0):
        return pd.DataFrame({"id": ["x"], "fecha": [fecha], "importe": [importe], "estado": ["RES"]})

    sin_fecha, con_fecha = publicado(None), publicado("2025-06-01T00:00:00Z")
    for primera, segunda in [(sin_fecha, con_fecha), (con_fecha, sin_fecha)]:
        out, _ = h.sembrar(_descarga(), primera, ["id", "fecha"])
        out, informe = h.sembrar(out, segunda, ["id", "fecha"])
        assert out["id"].tolist().count("x") == 1
        assert (informe["anadidas"], informe["descartadas_contenido"]) == (0, 1)
    # Con otro contenido es otra versión: se añade
    out, _ = h.sembrar(_descarga(), sin_fecha, ["id", "fecha"])
    out, informe = h.sembrar(out, publicado("2025-06-01T00:00:00Z", importe=5.0), ["id", "fecha"])
    assert out["id"].tolist().count("x") == 2 and informe["anadidas"] == 1


def test_seleccionar_semilla_clave_completa_frente_a_descarga_sin_fecha():
    # Simétrico: una fila con la clave completa que no está en la descarga es la
    # misma entrada que una fila de la descarga sin fecha con su id y su contenido.
    # El contenido solo se pide para las filas que comparten id
    nuevos = pd.DataFrame({"id": ["x", "y", "z"], "fecha": [None, None, "2026-01-01T00:00:00Z"],
                           "importe": [1.0, 2.0, 3.0]})
    semilla = pd.DataFrame({"id": ["x", "x", "y", "w"],
                            "fecha": ["2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z", "2025-01-01T00:00:00Z",
                                      "2025-01-01T00:00:00Z"],
                            "importe": [1.0, 9.0, 2.0, 1.0]})
    pedidas_n, pedidas_s = [], []

    def contenido(df, pedidas):
        def f(filas):
            pedidas.extend(filas.tolist())
            return df.loc[filas, ["importe"]]
        return f

    motivo = h.seleccionar_semilla(nuevos[["id", "fecha"]], semilla[["id", "fecha"]],
                                   contenido(nuevos, pedidas_n), contenido(semilla, pedidas_s))
    assert motivo.tolist() == [h.PRESENTE_CONTENIDO, h.ANADIDA, h.PRESENTE_CONTENIDO, h.ANADIDA]
    assert pedidas_n == [0, 1] and pedidas_s == [0, 1, 2]


def test_sembrar_solo_dentro_del_ambito():
    # Regresión: fuera del ámbito de la descarga (p.ej. años o conjuntos que esta
    # ejecución no ha leído) no se sabe si la fila sigue publicada: no se añade
    # como retirada, no se compara y se cuenta aparte
    publicado = _publicado()
    dentro = (publicado["id"] != "d").to_numpy()
    out, informe = h.sembrar(_descarga(), publicado, ["id", "fecha"], en_ambito=dentro)
    assert out.iloc[4:]["id"].tolist() == ["a", "b", "e"]
    assert (informe["anadidas"], informe["fuera_ambito"]) == (3, 2)
    assert informe["ejemplos"][h.FUERA_AMBITO][0][0] == "d"
    motivo = h.seleccionar_semilla(_descarga()[["id", "fecha"]], publicado[["id", "fecha"]],
                                   en_ambito=np.zeros(len(publicado), dtype=bool))
    assert set(motivo) == {h.FUERA_AMBITO}
