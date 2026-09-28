"""
Importes corregidos junto a los publicados (issue #22).

Los datos se sirven tal como los publica la administracion: esta capa no toca
las columnas publicadas. Anade al lado, para cada campo de CAMPOS_CORREGIBLES,
su version corregida y el motivo, para que el buscador pueda ensenar las dos y
los agregados (rankings, concentracion) y el modelo usen la corregida:

  <campo>_corregido     el importe que usar: el publicado si no hay correccion;
                        vacio si el publicado no es fiable y no hay estimacion
  correccion_<campo>    vacio (sin correccion) o el motivo:
    registro       error de la fuente verificado a mano en errores_fuente.csv,
                   con su evidencia; se aplica a las versiones que aun publican
                   el valor erroneo
    escala_x100    la adjudicacion es 100 o 1.000 veces el presupuesto y, entre
    escala_x1000   100 o 1.000, vuelve a su orden (la coma decimal perdida en
                   origen): con las mismas cifras que el presupuesto (desde
                   1.000 EUR) o entre el 50 % y el 105 % de el (desde 10.000 EUR)
    inverosimil    adjudicacion de 100 veces el presupuesto o mas, sin correccion
                   fiable: se deja vacia
    no_comparable  presupuesto de menos de 1.000 EUR con una adjudicacion de 100
                   veces o mas: suele ser un precio unitario o simbolico (1 EUR);
                   se deja vacio el presupuesto y la adjudicacion se mantiene

El salto de escala se mide sobre el mismo par que INT-CONS-08 e INT-FIA-12:
presupuesto y adjudicacion sin IVA y, si faltan, con IVA; solo se corrige el
par sin IVA.
"""
import os

import numpy as np
import pandas as pd

REGISTRO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "errores_fuente.csv")
COLUMNAS_REGISTRO = ["fuente", "id", "campo", "valor_publicado", "valor_probable",
                     "certeza", "evidencia", "referencia", "verificado"]
CAMPOS_CORREGIBLES = ["importe_sin_iva", "valor_estimado_contrato", "importe_adjudicacion"]

SALTO_ESCALA = 100                # adjudicacion >= 100 veces el presupuesto
PRESUPUESTO_COMPARABLE = 1_000    # por debajo, precio unitario o simbolico
PRESUPUESTO_BANDA = 10_000        # desde aqui, correccion por banda
BANDA_ADJ_LIC = (0.5, 1.05)       # adjudicacion / presupuesto tras corregir
EXPONENTES = (2, 3)               # coma decimal perdida: x100 o x1000


def _num(s):
    return s if pd.api.types.is_numeric_dtype(s) else pd.to_numeric(s, errors="coerce")


def par_presupuesto_adjudicacion(df):
    """(presupuesto, adjudicacion, es_sin_iva) de cada fila: el primer par
    informado con presupuesto > 0, sin IVA y si no con IVA. NaN en las filas sin
    par; (None, None, None) si no hay columnas para ninguno."""
    lic_par = adj_par = sin_iva = None
    for cl, ca in [("importe_sin_iva", "importe_adjudicacion"), ("importe_con_iva", "importe_adj_con_iva")]:
        if cl not in df.columns or ca not in df.columns:
            continue
        lic = _num(df[cl]).astype(float); adj = _num(df[ca]).astype(float)
        par = lic.notna() & adj.notna() & (lic > 0)
        if lic_par is None:
            lic_par, adj_par = lic.where(par), adj.where(par)
            sin_iva = par if cl == "importe_sin_iva" else pd.Series(False, index=df.index)
        else:
            usar = par & lic_par.isna()
            lic_par, adj_par = lic_par.mask(usar, lic), adj_par.mask(usar, adj)
    return lic_par, adj_par, sin_iva


def salto_escala(lic, adj):
    """True donde la adjudicacion es SALTO_ESCALA veces el presupuesto o mas;
    NA donde falta alguno o la adjudicacion no es positiva."""
    ev = lic.notna() & (adj > 0)
    return (adj >= lic * SALTO_ESCALA).where(ev).astype("boolean")


def cargar_registro(path=REGISTRO, fuente="placsp"):
    """Filas de 'fuente' del registro de errores (None si no hay ninguna)."""
    if not path or not os.path.exists(path):
        return None
    reg = pd.read_csv(path, dtype=str, keep_default_na=False)
    faltan = set(COLUMNAS_REGISTRO) - set(reg.columns)
    if faltan:
        raise ValueError(f"{path}: faltan las columnas {sorted(faltan)}")
    reg = reg[reg["fuente"] == fuente]
    return reg.reset_index(drop=True) if len(reg) else None


def _registro(df, registro, campo):
    """(casa, valor_probable) de 'campo' en cada fila: casa por id y valor
    publicado, asi que una version con otro valor (anterior al error o ya
    corregida en la fuente) no se toca."""
    casa = np.zeros(len(df), dtype=bool)
    valor = np.full(len(df), np.nan)
    if registro is None or "id" not in df.columns or campo not in df.columns:
        return casa, valor
    reg = registro[registro["campo"] == campo]
    if not len(reg):
        return casa, valor
    pos = np.flatnonzero(df["id"].isin(set(reg["id"])).to_numpy())
    if not len(pos):
        return casa, valor
    ids = df["id"].iloc[pos].to_numpy(dtype=object)
    publicado = _num(df[campo].iloc[pos]).astype(float).to_numpy()
    for e in reg.itertuples(index=False):
        m = (ids == e.id) & np.isclose(publicado, float(e.valor_publicado), rtol=0, atol=0.005)
        casa[pos[m]] = True
        valor[pos[m]] = float(e.valor_probable) if e.valor_probable.strip() else np.nan
    return casa, valor


def corregir_importes(df, registro=None):
    """Columnas <campo>_corregido y correccion_<campo> de CAMPOS_CORREGIBLES
    (las que haya en df), con el indice de df. Ver el docstring del modulo."""
    out = pd.DataFrame(index=df.index)
    lic, adj, sin_iva = par_presupuesto_adjudicacion(df)
    if lic is not None:
        salto = salto_escala(lic, adj).fillna(False).to_numpy(dtype=bool)
        sin_iva = sin_iva.to_numpy(dtype=bool)
        lic = lic.to_numpy(); adj = adj.to_numpy()
    for campo in CAMPOS_CORREGIBLES:
        if campo not in df.columns:
            continue
        corregido = _num(df[campo]).astype(float).to_numpy().copy()
        motivo = np.full(len(df), None, dtype=object)
        if lic is not None and campo == "importe_sin_iva":
            m = salto & sin_iva & (lic < PRESUPUESTO_COMPARABLE)
            corregido[m] = np.nan; motivo[m] = "no_comparable"
        if lic is not None and campo == "importe_adjudicacion":
            m = salto & ~(sin_iva & (lic < PRESUPUESTO_COMPARABLE))
            corregido[m] = np.nan; motivo[m] = "inverosimil"
            for k in EXPONENTES:
                with np.errstate(invalid="ignore", divide="ignore"):
                    escalado = adj / 10**k
                    q = escalado / lic
                exacto = np.isclose(escalado, lic, rtol=0, atol=0.005) & (lic >= PRESUPUESTO_COMPARABLE)
                banda = (q >= BANDA_ADJ_LIC[0]) & (q <= BANDA_ADJ_LIC[1]) & (lic >= PRESUPUESTO_BANDA)
                ok = m & sin_iva & (exacto | banda)
                corregido[ok] = escalado[ok]; motivo[ok] = f"escala_x{10**k}"
        casa, valor = _registro(df, registro, campo)
        corregido[casa] = valor[casa]; motivo[casa] = "registro"
        out[f"{campo}_corregido"] = corregido
        out[f"correccion_{campo}"] = pd.array(motivo, dtype="string")
    return out


def resumen(correcciones):
    """Filas por campo y motivo de correccion."""
    filas = []
    for c in correcciones.columns:
        if c.startswith("correccion_"):
            for motivo, n in correcciones[c].value_counts().items():
                filas.append((c[len("correccion_"):], motivo, int(n)))
    return pd.DataFrame(filas, columns=["campo", "motivo", "filas"])
