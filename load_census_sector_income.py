"""
Calcula a mediana sintética de renda domiciliar per capita (vl_renda_setor)
para setores censitários a partir de distribuições em classes.

Modos de entrada suportados:
- CSV (recomendado inicialmente): arquivo com distribuição por classes por município
  contendo ao menos as colunas: cd_mun, categoria, valor, periodo (opcional).
  Configure via env CENSO_CLASSES_CSV (default: data/censo_renda_percapita_classes_mun.csv)

- IBGE (experimental): tenta buscar automaticamente uma tabela de Censo 2022 que
  possua a classificação "classes de rendimento nominal mensal domiciliar per capita"
  e variável de contagem/percentual da população residente por classe. Para ativar,
  defina USE_IBGE=1. Você pode forçar os IDs via:
    CLASSES_TABELA, CLASSES_VARIAVEL, CLASSES_CLASSIFICACAO

Saída:
- Atualiza a tabela PostGIS (default: sp_setores) adicionando/preenchendo a coluna
  vl_renda_setor, mapeando a mediana do respectivo município (CD_MUN) quando a
  granularidade for municipal. Caso a granularidade seja setorial, faz o join por CD_SETOR.

Observação:
- As classes podem estar em frações do salário mínimo (SM) ou em R$ nominais.
  Para classes em SM, informe o salário mínimo do ano de referência via env SALARIO_MIN_{ANO},
  por exemplo SALARIO_MIN_2022=1212, SALARIO_MIN_2024=1412. Se não informado, usa
  defaults: 2022=1212, 2023=1320, 2024=1412, 2025=1512.
"""

import os
import re
import math
import json
import pandas as pd
import requests
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text


# -------------------- Config --------------------
DB_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")
TABLE_NAME = os.getenv("CENSO_TABLE", "sp_setores")
CSV_PATH = os.getenv("CENSO_CLASSES_CSV", "data/censo_renda_percapita_classes_mun.csv")
PERIODOS = os.getenv("IBGE_PERIODOS", os.getenv("IBGE_PERIODO", "2022"))

# SM defaults por ano
DEFAULT_SM = {
    "2022": 1212.0,
    "2023": 1320.0,
    "2024": 1412.0,
    "2025": 1512.0,
}


def salario_minimo_para_periodo(periodos: str) -> float:
    ano = None
    # tenta extrair ano AAAA
    m = re.search(r"(20\d{2})", str(periodos))
    if m:
        ano = m.group(1)
    return float(os.getenv(f"SALARIO_MIN_{ano}", DEFAULT_SM.get(ano or "2022", 1212.0)))


def parse_bounds_from_label(label: str, sm: float) -> Tuple[Optional[float], Optional[float]]:
    """
    Converte rótulos de categorias em limites (min, max) em R$.
    Suporta:
    - "Sem rendimento" => (0, 0)
    - Frações de SM: "Até 1/8", "Mais de 1/2 a 1", "Mais de 1 a 2 salários mínimos"
    - Faixas em R$: "Até 105", "Mais de 105 a 210", "Acima de 2100"

    Retorna (min_valor, max_valor) em R$; max=None para aberta superior.
    """
    s = label.strip().lower()
    # sem rendimento
    if "sem rendimento" in s:
        return 0.0, 0.0

    # detectar referência a salário mínimo
    if "salário" in s or "salario" in s or "sm" in s:
        # extrair frações tipo 1/2, 1, 2 etc.
        # padrões: "até X sm", "mais de A a B sm", "mais de X sm"
        frac_pattern = r"([0-9]+(?:\/[0-9]+)?)"
        # Até X SM
        m = re.search(rf"até\s*{frac_pattern}.*(sm|sal[aá]rio)", s)
        if m:
            x = m.group(1)
            val = eval_fraction(x) * sm
            return 0.0, val
        # Mais de A a B SM
        m = re.search(rf"mais de\s*{frac_pattern}.*a\s*{frac_pattern}.*(sm|sal[aá]rio)", s)
        if m:
            a, b = m.group(1), m.group(2)
            return eval_fraction(a) * sm, eval_fraction(b) * sm
        # Mais de X SM (aberta superior)
        m = re.search(rf"mais de\s*{frac_pattern}.*(sm|sal[aá]rio)", s)
        if m:
            a = m.group(1)
            return eval_fraction(a) * sm, None

    # valores em R$
    # Até 105
    m = re.search(r"at[eé]\s*([0-9]+(?:[\.,][0-9]+)?)", s)
    if m:
        v = to_float(m.group(1))
        return 0.0, v
    # Mais de 105 a 210
    m = re.search(r"mais de\s*([0-9]+(?:[\.,][0-9]+)?)\s*a\s*([0-9]+(?:[\.,][0-9]+)?)", s)
    if m:
        a, b = to_float(m.group(1)), to_float(m.group(2))
        return a, b
    # Mais de 2100 (aberta superior)
    m = re.search(r"mais de\s*([0-9]+(?:[\.,][0-9]+)?)$", s)
    if m:
        a = to_float(m.group(1))
        return a, None

    return None, None


def eval_fraction(expr: str) -> float:
    if "/" in expr:
        n, d = expr.split("/", 1)
        return float(n) / float(d)
    return float(expr)


def to_float(txt: str) -> float:
    return float(str(txt).replace(".", "").replace(",", "."))


def synthetic_median_from_classes(classes: List[Tuple[float, Optional[float], float]]) -> float:
    """
    Calcula a mediana por interpolação linear a partir de classes:
    classes: lista de tuplas (min, max, freq) com min/max em R$ e freq em contagem ou percentual.
    Retorna a mediana em R$.
    """
    # ordenar por limite inferior
    classes = sorted(classes, key=lambda x: (x[0] if x[0] is not None else -math.inf))
    total = sum(c[2] for c in classes if c[2] is not None)
    if total <= 0:
        return float("nan")
    target = 0.5 * total
    cum_prev = 0.0
    for (a, b, f) in classes:
        if f is None or f == 0:
            continue
        next_cum = cum_prev + f
        if next_cum >= target:
            # encontrou classe mediana
            L = a if a is not None else 0.0
            # largura da classe
            if b is None or b <= L:
                # última aberta ou intervalo inválido: retorna L
                return float(L)
            w = b - L
            # posição dentro da classe
            inside = (target - cum_prev) / f
            return float(L + inside * w)
        cum_prev = next_cum
    # fallback: retorna último limite inferior
    last = classes[-1][0]
    return float(last if last is not None else 0.0)


def compute_median_from_csv(csv_path: str, sm: float) -> pd.DataFrame:
    """Lê CSV com colunas: cd_mun, categoria, valor, periodo(opcional) e devolve df[cd_mun, mediana]."""
    df = pd.read_csv(csv_path)
    req_cols = {"cd_mun", "categoria", "valor"}
    if not req_cols.issubset(set(c.lower() for c in df.columns)):
        raise RuntimeError(
            f"CSV deve conter colunas {req_cols}. Colunas encontradas: {list(df.columns)}"
        )
    # normalizar nomes
    cols_map = {c: c.lower() for c in df.columns}
    df.rename(columns=cols_map, inplace=True)
    df["cd_mun"] = df["cd_mun"].astype(str)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

    # agrupa por município
    rows = []
    for cd_mun, g in df.groupby("cd_mun"):
        classes = []
        for _, r in g.iterrows():
            a, b = parse_bounds_from_label(str(r["categoria"]), sm)
            if a is None and b is None:
                continue
            classes.append((a or 0.0, b, float(r["valor"])) )
        if not classes:
            continue
        med = synthetic_median_from_classes(classes)
        rows.append({"cd_mun": cd_mun, "vl_renda_setor": med})
    return pd.DataFrame(rows)


def upsert_to_db(df_median: pd.DataFrame):
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        # cria coluna se não existir
        check = conn.execute(text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:tbl AND column_name='vl_renda_setor'
            """
        ), {"tbl": TABLE_NAME.lower()}).fetchone()
        if not check:
            conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD COLUMN vl_renda_setor NUMERIC"))
            print("Database table altered: added vl_renda_setor column.")

        # subir dados temporários para join por município
        df_tmp = df_median.copy()
        df_tmp.to_sql("temp_mediana_mun", con=conn, if_exists="replace", index=False)
        # atualizar setores
        conn.execute(text(f"""
            UPDATE {TABLE_NAME} tgt
            SET vl_renda_setor = src.vl_renda_setor
            FROM temp_mediana_mun src
            WHERE tgt."CD_MUN"::text = src.cd_mun::text
        """))
        conn.execute(text("DROP TABLE IF EXISTS temp_mediana_mun"))
    print("vl_renda_setor atualizada por município (CD_MUN).")


def maybe_fetch_from_ibge(sm: float) -> Optional[pd.DataFrame]:
    """Tenta buscar dados de distribuição por classes via API de Agregados (v3).
    Requer variáveis/ids corretamente configurados, caso contrário retorna None.
    """
    if os.getenv("USE_IBGE", "0") != "1":
        return None

    tabela = os.getenv("CLASSES_TABELA")
    variavel = os.getenv("CLASSES_VARIAVEL")
    classificacao = os.getenv("CLASSES_CLASSIFICACAO")  # ex.: 1234[cat1,cat2,...]
    if not (tabela and variavel and classificacao):
        print("USE_IBGE=1 setado, mas CLASSES_TABELA/VARIAVEL/CLASSIFICACAO não informados. Abortando IBGE.")
        return None

    base = f"https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos/{PERIODOS}/variaveis/{variavel}"
    url = base + f"?localidades=N6[ALL_DB]&classificacao={classificacao}&view=flat"
    print(f"Fetching IBGE (flat): {url}")
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Falha no fetch IBGE: {e}")
        return None

    # Esperado (flat): primeira linha metadados; linhas seguintes com colunas como:
    # "localidade", "categoria" (ou D1C/D1N), "valor" etc.
    if not isinstance(data, list) or len(data) < 2:
        print("Formato inesperado do flat; retornar None.")
        return None
    meta, rows = data[0], data[1:]
    # tentar descobrir nomes de colunas
    # normalizar keys
    norm_rows = []
    for r in rows:
        rr = {k.lower(): v for k, v in r.items()}
        norm_rows.append(rr)

    # heurística para localizar campos
    def first_key(options: List[str], d: dict) -> Optional[str]:
        for k in options:
            if k in d:
                return k
        return None

    # assumir que campos existem
    sample = norm_rows[0]
    k_loc = first_key(["localidade", "n6", "id_localidade", "cd_mun"], sample)
    k_cat = first_key(["categoria", "d1n", "classe", "faixa"], sample)
    k_val = first_key(["valor", "v"], sample)
    if not (k_loc and k_cat and k_val):
        print("Não foi possível identificar colunas no flat. Abortando IBGE.")
        return None

    df = pd.DataFrame([{ "cd_mun": str(r[k_loc]), "categoria": r[k_cat], "valor": r[k_val] } for r in norm_rows])
    # converter valor numérico (pode vir como str com .., X, etc.)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)

    # construir medianas
    out = []
    for cd_mun, g in df.groupby("cd_mun"):
        classes = []
        for _, r in g.iterrows():
            a, b = parse_bounds_from_label(str(r["categoria"]), sm)
            if a is None and b is None:
                continue
            classes.append((a or 0.0, b, float(r["valor"])) )
        if not classes:
            continue
        med = synthetic_median_from_classes(classes)
        out.append({"cd_mun": cd_mun, "vl_renda_setor": med})
    return pd.DataFrame(out)


def main():
    sm = salario_minimo_para_periodo(PERIODOS)
    print(f"Usando salário mínimo R$ {sm:.2f} para período {PERIODOS}.")

    df_median: Optional[pd.DataFrame] = None

    # 1) Tenta via CSV
    if os.path.exists(CSV_PATH):
        print(f"Lendo CSV de classes: {CSV_PATH}")
        df_median = compute_median_from_csv(CSV_PATH, sm)
    else:
        print(f"CSV não encontrado em {CSV_PATH}. Tentando IBGE (se USE_IBGE=1)...")
        df_median = maybe_fetch_from_ibge(sm)

    if df_median is None or df_median.empty:
        print("Não foi possível obter medianas. Forneça um CSV com distribuição por classes ou configure USE_IBGE=1 com os IDs corretos.")
        raise SystemExit(2)

    print(f"Calculadas {len(df_median)} medianas.")
    upsert_to_db(df_median)
    print("Concluído.")


if __name__ == "__main__":
    main()
