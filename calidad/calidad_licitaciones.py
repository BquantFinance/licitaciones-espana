"""
============================================================================
PIPELINE DE CALIDAD — Licitaciones Publicas de Espana (v4.0)
============================================================================
20 indicadores sobre PLACSP nacional + TED + BORME.

Uso:
  python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet

  python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
    --ted ted/crossval_sara_v2.parquet \
    --borme borme_empresas.parquet

  python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
    --ted ted/crossval_sara_v2.parquet \
    --borme borme_empresas.parquet \
    -s 200000

Salida: calidad/calidad_licitaciones_resultado.parquet
============================================================================
"""
import pandas as pd
import numpy as np
import re
import argparse
import os
import sys

if sys.stdout.encoding != 'utf-8':
    try: sys.stdout.reconfigure(encoding='utf-8')
    except: pass

# ======================================================================
# CONFIGURACION
# ======================================================================

CONFIG = {
    "importe_minimo": 1.0,
    "fecha_min": "1990-01-01",
    "fecha_max": "2030-12-31",
    "umbral_menor_obras": 40_000,
    "umbral_menor_servicios": 15_000,
    "umbral_menor_suministros": 15_000,
    "tolerancia_adj_lic": 0.05,
    "max_ofertas_global": 500,
    "min_plazo_dias": 0,
    "max_plazo_dias": 365,
    "pbl_outlier": 50_000_000,
}

CATALOGO = {
    "INT-VAL-01": {"nombre": "Importe de licitacion en formato valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-02": {"nombre": "Importe de adjudicacion en formato valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-03": {"nombre": "Importe minimo plausible", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-04": {"nombre": "Numero de licitadores es entero", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-05": {"nombre": "Numero de licitadores no negativo", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-06": {"nombre": "Fecha de publicacion en formato valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-07": {"nombre": "Fecha de adjudicacion en formato valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-09": {"nombre": "Codigo CPV valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-10": {"nombre": "Codigo territorial valido", "dimension": "Validez", "fuente": "PPDS"},
    "INT-VAL-12": {"nombre": "Identificacion valida del adjudicatario (NIF/NIE)", "dimension": "Validez", "fuente": "Yo con asistencia de la IA"},
    "INT-VAL-14": {"nombre": "Clasificacion correcta procedimiento segun cuantia", "dimension": "Validez", "fuente": "Jaime Gomez-Obregon"},
    "INT-CONS-01": {"nombre": "Si hay adjudicacion, num ofertas >= 1", "dimension": "Consistencia", "fuente": "PPDS"},
    "INT-CONS-08": {"nombre": "Importe coherente entre licitacion y adjudicacion", "dimension": "Consistencia", "fuente": "PPDS"},
    "INT-CONS-18": {"nombre": "Adjudicatario existe en BORME (Registro Mercantil)", "dimension": "Consistencia", "fuente": "Yo con asistencia de la IA"},
    "INT-CONS-20": {"nombre": "Contrato SARA publicado en TED", "dimension": "Consistencia", "fuente": "PPDS"},
    "INT-FIA-01": {"nombre": "Num ofertas dentro de rango razonable", "dimension": "Fiabilidad", "fuente": "PPDS"},
    "INT-FIA-04": {"nombre": "Plazo de presentacion de ofertas razonable", "dimension": "Fiabilidad", "fuente": "PPDS"},
    "INT-FIA-08": {"nombre": "PBL atipico/inverosimil (outlier)", "dimension": "Fiabilidad", "fuente": "PPDS"},
    "INT-FIA-09": {"nombre": "PA plausible respecto a comparables por CPV", "dimension": "Fiabilidad", "fuente": "PPDS"},
    "INT-FIA-11": {"nombre": "Trazabilidad minima del expediente", "dimension": "Fiabilidad", "fuente": "PPDS"},
}

# ======================================================================
# UTILIDADES
# ======================================================================

def _dt(s):
    return s if pd.api.types.is_datetime64_any_dtype(s) else pd.to_datetime(s, errors="coerce")
def _num(s):
    return s if pd.api.types.is_numeric_dtype(s) else pd.to_numeric(s, errors="coerce")

_NIF_LETRAS = "TRWAGMYFPDXBNJZSQVHLCKE"
def _nif_letra(n): return _NIF_LETRAS[int(n) % 23]
def _cif_ok(cif):
    t = cif[0]
    if t not in "ABCDEFGHJNPQRSUVW": return False
    d = cif[1:8]
    if not d.isdigit(): return False
    sp = sum(int(x) for x in d[1::2])
    si = 0
    for x in d[0::2]:
        db = int(x)*2; si += db//10 + db%10
    ctrl = (10-(sp+si)%10)%10; cc = cif[8]
    if t in "KPQS": return cc == "JABCDEFGHI"[ctrl]
    elif t in "ABEH": return cc == str(ctrl)
    return cc == str(ctrl) or cc == "JABCDEFGHI"[ctrl]

def validar_nif(v):
    if pd.isna(v): return False
    raw = str(v)
    if "@" in raw: return False
    s = raw.strip().upper().replace("-","").replace(" ","").replace(".","")
    if len(s)<8 or len(s)>9: return False
    if re.match(r"^\d{8}[A-Z]$",s): return s[8]==_nif_letra(s[:8])
    if re.match(r"^[XYZ]\d{7}[A-Z]$",s): return s[8]==_nif_letra({"X":"0","Y":"1","Z":"2"}[s[0]]+s[1:8])
    if re.match(r"^[A-Z]\d{7}[A-Z0-9]$",s): return _cif_ok(s)
    return False

_CPV_RE = re.compile(r"^\d{8}(-\d)?$")
def validar_cpv(v):
    if pd.isna(v): return False
    s = str(v).strip()
    # Handle float64 from parquet: "42933300.0" -> "42933300"
    if s.endswith(".0") and s[:-2].replace("-","").isdigit():
        s = s[:-2]
    try:
        n = float(s.split("-")[0])
        if n == int(n):
            base = str(int(n)).zfill(8)
            if len(base) == 8:
                return True
    except (ValueError, OverflowError):
        pass
    return bool(_CPV_RE.match(s))

def div_cpv(v):
    if pd.isna(v): return ""
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].replace("-","").isdigit():
        s = s[:-2]
    try:
        n = float(s.split("-")[0])
        if n == int(n):
            s = str(int(n)).zfill(8)
    except (ValueError, OverflowError):
        pass
    return s[:2] if len(s)>=2 and s[:2].isdigit() else ""

_NUTS_RE = re.compile(r"^ES[0-9A-Z]{0,3}$", re.I)
def validar_nuts(v):
    if pd.isna(v): return False
    return bool(_NUTS_RE.match(str(v).strip()))

def normalizar_nombre_empresa(nombre):
    if pd.isna(nombre): return ""
    s = str(nombre).upper().strip()
    for suf in [" SOCIEDAD LIMITADA"," SOCIEDAD ANONIMA"," S.L.U."," S.L.L.",
                " S.L."," S.A.U."," S.A."," SLU"," SLL"," SLP"," SL"," SAU"," SA",
                " S.COOP"," SCOOP"," S COOP"," S.C."," SC"," UNIPERSONAL",
                " EN CONSTITUCION",",",".","-"]:
        s = s.replace(suf,"")
    return re.sub(r"\s+"," ",s).strip()


# ======================================================================
# 17 INDICADORES BASE
# ======================================================================

def calcular_indicadores_base(df):
    r = pd.DataFrame(index=df.index)

    # VAL-01
    c = "importe_sin_iva" if "importe_sin_iva" in df.columns else "importe_con_iva" if "importe_con_iva" in df.columns else None
    r["INT-VAL-01"] = _num(df[c]).notna() if c else np.nan

    # VAL-02
    c = "importe_adjudicacion" if "importe_adjudicacion" in df.columns else "importe_adj_con_iva" if "importe_adj_con_iva" in df.columns else None
    r["INT-VAL-02"] = _num(df[c]).notna() if c else np.nan

    # VAL-03
    ic = [c for c in ["importe_sin_iva","importe_con_iva","importe_adjudicacion","importe_adj_con_iva"] if c in df.columns]
    if ic:
        imp = df[ic].apply(pd.to_numeric,errors="coerce")
        r["INT-VAL-03"] = (imp>=CONFIG["importe_minimo"]).any(axis=1)|imp.isna().all(axis=1)
    else: r["INT-VAL-03"] = np.nan

    # VAL-04/05
    if "num_ofertas" in df.columns:
        num = _num(df["num_ofertas"])
        r["INT-VAL-04"] = num.isna()|(num%1==0)
        r["INT-VAL-05"] = num.isna()|(num>=0)
    else: r["INT-VAL-04"]=np.nan; r["INT-VAL-05"]=np.nan

    # VAL-06
    if "fecha_publicacion" in df.columns:
        fp = _dt(df["fecha_publicacion"])
        r["INT-VAL-06"] = fp.notna()&(fp>=CONFIG["fecha_min"])&(fp<=CONFIG["fecha_max"])
    else: r["INT-VAL-06"] = np.nan

    # VAL-07
    if "fecha_adjudicacion" in df.columns:
        fa = _dt(df["fecha_adjudicacion"])
        r["INT-VAL-07"] = fa.notna()&(fa>=CONFIG["fecha_min"])&(fa<=CONFIG["fecha_max"])
    else: r["INT-VAL-07"] = np.nan

    # VAL-09
    r["INT-VAL-09"] = df["cpv_principal"].apply(validar_cpv) if "cpv_principal" in df.columns else np.nan

    # VAL-10
    c = "nuts" if "nuts" in df.columns else "ubicacion" if "ubicacion" in df.columns else None
    r["INT-VAL-10"] = df[c].apply(validar_nuts) if c else np.nan

    # VAL-12
    r["INT-VAL-12"] = df["nif_adjudicatario"].apply(validar_nif) if "nif_adjudicatario" in df.columns else np.nan

    # VAL-14
    if all(c in df.columns for c in ["tipo_contrato"]):
        ci = "importe_adjudicacion" if "importe_adjudicacion" in df.columns else "importe_sin_iva" if "importe_sin_iva" in df.columns else None
        if ci:
            imp = _num(df[ci]); tipo = df["tipo_contrato"].astype(str).str.lower().str.strip()
            if "conjunto" in df.columns:
                es_menor = df["conjunto"].astype(str).str.lower()=="menores"
                if "procedimiento" in df.columns:
                    es_menor = es_menor|df["procedimiento"].astype(str).str.lower().str.contains("menor",na=False)
            elif "procedimiento" in df.columns:
                es_menor = df["procedimiento"].astype(str).str.lower().str.contains("menor",na=False)
            else: es_menor = pd.Series(False,index=df.index)
            umbral = pd.Series(CONFIG["umbral_menor_servicios"],index=df.index,dtype=float)
            umbral[tipo.str.contains("obra",na=False)] = CONFIG["umbral_menor_obras"]
            umbral[tipo.str.contains("suministro",na=False)] = CONFIG["umbral_menor_suministros"]
            r["INT-VAL-14"] = ~es_menor|imp.isna()|(imp<=umbral)
        else: r["INT-VAL-14"] = np.nan
    else: r["INT-VAL-14"] = np.nan

    # CONS-01
    if all(c in df.columns for c in ["estado","num_ofertas"]):
        est = df["estado"].astype(str).str.lower().str.strip()
        r["INT-CONS-01"] = ~est.str.contains("adjud|formaliz|resuel",na=False)|(_num(df["num_ofertas"])>=1)
    else: r["INT-CONS-01"] = np.nan

    # CONS-08
    done=False
    for cl,ca in [("importe_sin_iva","importe_adjudicacion"),("importe_con_iva","importe_adj_con_iva")]:
        if cl in df.columns and ca in df.columns:
            lic=_num(df[cl]); adj=_num(df[ca]); both=lic.notna()&adj.notna()&(lic>0)
            r["INT-CONS-08"] = ~both|(adj<=lic*(1+CONFIG["tolerancia_adj_lic"])); done=True; break
    if not done: r["INT-CONS-08"] = np.nan

    # FIA-01
    if "num_ofertas" in df.columns:
        num=_num(df["num_ofertas"]); mx=CONFIG["max_ofertas_global"]
        p = num.isna()|((num>=0)&(num<=mx))
        if "cpv_principal" in df.columns:
            dv=df["cpv_principal"].apply(div_cpv)
            p99=num.groupby(dv).transform(lambda x:x.quantile(0.99)).fillna(mx)
            p = p&(num.isna()|(num<=p99))
        r["INT-FIA-01"] = p
    else: r["INT-FIA-01"] = np.nan

    # FIA-04
    if all(c in df.columns for c in ["fecha_publicacion","fecha_limite"]):
        fp=_dt(df["fecha_publicacion"]); fl=_dt(df["fecha_limite"]); dias=(fl-fp).dt.days; both=fp.notna()&fl.notna()
        r["INT-FIA-04"] = ~both|((dias>=CONFIG["min_plazo_dias"])&(dias<=CONFIG["max_plazo_dias"]))
    else: r["INT-FIA-04"] = np.nan

    # FIA-08
    c = "importe_sin_iva" if "importe_sin_iva" in df.columns else "importe_con_iva" if "importe_con_iva" in df.columns else None
    r["INT-FIA-08"] = (_num(df[c]).isna()|(_num(df[c])<=CONFIG["pbl_outlier"])) if c else np.nan

    # FIA-09
    ca = "importe_adjudicacion" if "importe_adjudicacion" in df.columns else "importe_adj_con_iva" if "importe_adj_con_iva" in df.columns else None
    if ca and "cpv_principal" in df.columns:
        pa=_num(df[ca]); dv=df["cpv_principal"].apply(div_cpv)
        q1=pa.groupby(dv).transform(lambda x:x.quantile(0.01)); q99=pa.groupby(dv).transform(lambda x:x.quantile(0.99))
        ev=pa.notna()&q1.notna()&q99.notna()&(pa>0)
        r["INT-FIA-09"] = ~ev|((pa>=q1)&(pa<=q99))
    else: r["INT-FIA-09"] = np.nan

    # FIA-11
    has_exp = df["expediente"].notna() if "expediente" in df.columns else pd.Series(False,index=df.index)
    has_url = df["url"].notna() if "url" in df.columns else pd.Series(False,index=df.index)
    has_id = df["id"].notna() if "id" in df.columns else pd.Series(False,index=df.index)
    r["INT-FIA-11"] = has_exp&(has_url|has_id)

    return r


# ======================================================================
# CONS-20 (TED) y CONS-18 (BORME)
# ======================================================================

def calcular_cons20(df, path_ted):
    print(f"  Cargando TED: {path_ted}")
    ted = pd.read_parquet(path_ted, columns=["expediente","nif_adjudicatario",
                                              "_ted_validated","_ted_missing",
                                              "_match_strategy"])
    print(f"  {len(ted):,} contratos SARA")
    ted["_key"] = ted["expediente"].astype(str)+"|"+ted["nif_adjudicatario"].astype(str)
    td = dict(zip(ted["_key"], ted["_ted_validated"]))
    n_val = ted["_ted_validated"].sum()
    print(f"  Validados por 5 estrategias: {n_val:,} ({n_val/len(ted)*100:.1f}%)")
    del ted
    if all(c in df.columns for c in ["expediente","nif_adjudicatario"]):
        keys = df["expediente"].astype(str)+"|"+df["nif_adjudicatario"].astype(str)
        res = keys.map(td)
        n_eval = res.notna().sum()
        n_ok = (res==True).sum()
        n_miss = (res==False).sum()
        print(f"  SARA en nacional: {n_eval:,} | En TED: {n_ok:,} ({n_ok/max(n_eval,1)*100:.1f}%) | Missing: {n_miss:,}")
        return res
    return pd.Series(np.nan, index=df.index)

def cargar_borme(path):
    if not path or not os.path.exists(path): return None
    print(f"  Cargando BORME: {path}")
    b = pd.read_parquet(path, columns=["empresa_norm"])
    e = set(b["empresa_norm"].dropna().unique()); del b
    print(f"  {len(e):,} empresas unicas"); return e

def aplicar_borme(df, empresas):
    if empresas is None or "adjudicatario" not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=object).astype("boolean")
    adj_n = df["adjudicatario"].apply(normalizar_nombre_empresa)
    tiene = df["adjudicatario"].notna()&(adj_n!="")
    es_emp = df["nif_adjudicatario"].astype(str).str.strip().str.upper().str.match(r"^[A-HJ-NP-SUVW]",na=False) if "nif_adjudicatario" in df.columns else pd.Series(False,index=df.index)
    ev = tiene&es_emp
    if ev.sum()==0: return pd.Series(np.nan,index=df.index,dtype=object).astype("boolean")
    enc = adj_n.isin(empresas)
    res = pd.Series(np.nan,index=df.index,dtype=object); res.loc[ev]=enc.loc[ev]
    return res.astype("boolean")


# ======================================================================
# SCORE Y RESUMEN
# ======================================================================

def calcular_score(sc):
    ev=sc.notna().sum(axis=1); pa=sc.fillna(False).sum(axis=1)
    s=(pa/ev*100).round(1); s[ev==0]=np.nan; return s

def imprimir_resumen(sc, n, df):
    print(f"\n{'='*95}\n  RESUMEN ({n:,} contratos)\n{'='*95}")
    dim=""
    for col in sc.columns:
        m=CATALOGO.get(col,{}); d=m.get("dimension","")
        if d!=dim: dim=d; print(f"\n  -- {dim.upper()} {'-'*(70-len(dim))}")
        s=sc[col]; t=s.notna().sum()
        if t==0: continue
        inv=int(t-s.sum()); pct=inv/t*100
        ico="!!" if pct>5 else "  "
        print(f"  {ico}{col:<14s} | {pct:5.1f}% | {t:>10,} eval | {m.get('nombre','')[:50]}")

    # Menores vs regulares
    if "conjunto" in df.columns:
        conj=df["conjunto"].astype(str).str.lower(); es_men=conj=="menores"
        nm=es_men.sum()
        if nm>0 and nm<n:
            print(f"\n  -- MENORES ({nm:,}) vs REGULARES ({n-nm:,}) --")
            print(f"  {'INDICADOR':<16s} | {'MEN%':>7s} | {'REG%':>7s} | {'DIFF':>8s}")
            for col in sc.columns:
                sm=sc.loc[es_men,col]; sr=sc.loc[~es_men,col]
                tm=sm.notna().sum(); tr=sr.notna().sum()
                if tm==0 or tr==0: continue
                pm=(tm-sm.sum())/tm*100; pr=(tr-sr.sum())/tr*100
                flag=" <<<" if abs(pm-pr)>10 else ""
                print(f"  {col:<16s} | {pm:6.1f}% | {pr:6.1f}% | {pm-pr:>+7.1f}pp{flag}")
    print(f"{'='*95}")


# ======================================================================
# PIPELINE
# ======================================================================

def run(args):
    os.makedirs(args.output, exist_ok=True)
    empresas_borme = cargar_borme(args.borme) if args.borme else None

    print(f"\n  Cargando {args.input}...")
    df = pd.read_parquet(args.input)
    if args.sample:
        df=df.sample(min(args.sample,len(df)),random_state=42)
        print(f"  Muestra: {len(df):,}")
    print(f"  {len(df):,} contratos x {len(df.columns)} columnas")

    if "conjunto" in df.columns:
        print(f"\n  Desglose:")
        for val, cnt in df["conjunto"].value_counts().items():
            print(f"    {str(val):<20s} {cnt:>12,} ({cnt/len(df)*100:.1f}%)")

    print(f"\n  Calculando 17 indicadores base...")
    sc = calcular_indicadores_base(df)

    if args.ted and os.path.exists(args.ted):
        print(f"\n  Calculando INT-CONS-20 (TED SARA)...")
        sc["INT-CONS-20"] = calcular_cons20(df, args.ted)

    if empresas_borme is not None:
        print(f"\n  Calculando INT-CONS-18 (BORME)...")
        r18=aplicar_borme(df,empresas_borme)
        if r18.notna().any(): sc["INT-CONS-18"]=r18

    score = calcular_score(sc)
    imprimir_resumen(sc, len(df), df)

    # Consolidar
    print(f"\n  Consolidando CSV...")
    res = pd.concat([df.reset_index(drop=True),sc.reset_index(drop=True),
                      score.rename("score_calidad").reset_index(drop=True)],axis=1)
    if "conjunto" in df.columns:
        res["es_menor"]=df["conjunto"].astype(str).str.lower().values=="menores"

    p=os.path.join(args.output,"calidad_licitaciones_resultado.parquet")
    res.to_parquet(p,index=False)
    size_mb = os.path.getsize(p)/1024**2
    print(f"\n  -> {p}")
    print(f"     {len(res):,} filas x {len(res.columns)} columnas ({size_mb:.0f} MB)")

    print(f"\n  Score: media={score.mean():.1f}  mediana={score.median():.1f}")
    if "conjunto" in df.columns:
        conj=df["conjunto"].astype(str).str.lower().values
        s_m=score[conj=="menores"]; s_r=score[conj!="menores"]
        if len(s_m)>0: print(f"  Score menores:   media={s_m.mean():.1f}  mediana={s_m.median():.1f}")
        if len(s_r)>0: print(f"  Score regulares: media={s_r.mean():.1f}  mediana={s_r.median():.1f}")

    print(f"\n>> Listo.\n")


def main():
    p=argparse.ArgumentParser(description="Pipeline calidad PLACSP v4 — 20 indicadores")
    p.add_argument("-i","--input",required=True,help="Nacional parquet")
    p.add_argument("-o","--output",default="calidad")
    p.add_argument("-s","--sample",type=int,default=None)
    p.add_argument("--ted",default=None,help="crossval_sara_v2.parquet")
    p.add_argument("--borme",default=None,help="borme_empresas.parquet")
    run(p.parse_args())

if __name__ == "__main__":
    main()
