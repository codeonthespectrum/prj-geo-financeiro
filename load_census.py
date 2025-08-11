import os
import pandas as pd
import requests
from typing import List
from sqlalchemy import create_engine, text

# Configuração do banco via variável de ambiente (alinhado ao docker-compose)
db_url = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")
table_name = os.getenv("CENSO_TABLE", "sp_setores")

agregado_tabela = os.getenv("AGREGADO_TABELA", "3563")
agregado_variaveis = os.getenv("AGREGADO_VARIAVEIS", "2011")
periodos = os.getenv("IBGE_PERIODOS", os.getenv("IBGE_PERIODO", "2024"))
localidades = os.getenv("IBGE_LOCALIDADES", "[3550308]")
classificacao = os.getenv("AGREGADO_CLASSIFICACAO", "")

if not agregado_tabela or not agregado_variaveis or not periodos or not localidades:
    print(
        "Parâmetros insuficientes para API Agregados. Informe AGREGADO_TABELA, AGREGADO_VARIAVEIS, IBGE_PERIODOS e IBGE_LOCALIDADES.\n"
        "Ex.: AGREGADO_TABELA=XXXX AGREGADO_VARIAVEIS=YYYY IBGE_PERIODOS=2022 IBGE_LOCALIDADES=N6[3550308]"
    )
    raise SystemExit(2)

base_url = (
    f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado_tabela}/periodos/{periodos}/variaveis/{agregado_variaveis}"
)

def _build_url(localidades_param: str) -> str:
    p = f"?localidades={localidades_param}"
    if classificacao:
        p += f"&classificacao={classificacao}"
    return base_url + p

engine = create_engine(db_url, pool_pre_ping=True)
print("Database engine created successfully.")

def _fetch_chunked(loc_codes: List[str]) -> list:
    """Busca em lotes (comma-separated) para evitar URLs muito longas."""
    data_all = []
    chunk_size = int(os.getenv("IBGE_CHUNK_SIZE", "100"))
    for i in range(0, len(loc_codes), chunk_size):
        sub = loc_codes[i:i+chunk_size]
        loc_param = f"N6[{','.join(sub)}]"
        url = _build_url(loc_param)
        print(f"Fetching IBGE Agregados from: {url}")
        try:
            r = requests.get(url, timeout=90)
            r.raise_for_status()
            d = r.json()
            if isinstance(d, list):
                data_all.extend(d)
        except Exception as e:
            print(f"Falha no fetch do lote {i//chunk_size+1}: {e}")
    return data_all

# Determina estratégia de busca
data = None
loc_param_upper = localidades.upper() if isinstance(localidades, str) else str(localidades)
if loc_param_upper.startswith("N6[ALL_DB]"):
    # Lê todos os municípios existentes na tabela alvo
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT DISTINCT \"CD_MUN\"::text FROM {table_name} WHERE \"CD_MUN\" IS NOT NULL")).fetchall()
    mun_codes = [r[0] for r in rows]
    if not mun_codes:
        print("Nenhum CD_MUN encontrado em sp_setores para N6[ALL_DB].")
        raise SystemExit(1)
    data = _fetch_chunked(mun_codes)
else:
    api_url = _build_url(localidades)
    print(f"Fetching IBGE Agregados from: {api_url}")
    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Falha ao acessar API Agregados: {e}")
        raise SystemExit(1)

# Parse do retorno v3 (agregados): resultados -> series -> localidade{id} / serie{periodo: valor}
df_renda = None
try:
    rows = []
    if isinstance(data, list):
        for bloco in data:
            for res in bloco.get("resultados", []) if isinstance(bloco, dict) else []:
                for serie in res.get("series", []):
                    loc = serie.get("localidade", {}) or {}
                    loc_id = str(loc.get("id")) if loc else None
                    serie_dict = serie.get("serie", {}) or {}
                    if not serie_dict:
                        continue
                    # tenta pegar o período solicitado, senão o último disponível
                    valor = None
                    if periodos and periodos != "last":
                        valor = serie_dict.get(str(periodos))
                    if valor is None:
                        last_key = sorted(serie_dict.keys())[-1]
                        valor = serie_dict.get(last_key)
                    if loc_id is not None and valor is not None:
                        rows.append({"cd_setor": loc_id, "vl_renda": valor})
    if rows:
        df_renda = pd.DataFrame(rows)
    if df_renda is None or df_renda.empty:
        raise ValueError("Resposta vazia ou sem séries válidas para os parâmetros informados.")
except Exception as e:
    print(f"Erro ao parsear a resposta dos Agregados: {e}")
    raise SystemExit(1)

print(f"Data loaded with {len(df_renda)} records successfully.")

# Normalização
df_renda["vl_renda"] = pd.to_numeric(df_renda["vl_renda"], errors="coerce")
df_renda.dropna(subset=["vl_renda"], inplace=True)
df_renda["cd_setor"] = df_renda["cd_setor"].astype(str)

# Verificação de granularidade: setor costuma ter 13+ dígitos
avg_len = int(df_renda["cd_setor"].str.len().mean()) if not df_renda.empty else 0
if avg_len < 13:
    print(
        f"Os códigos retornados (média de comprimento {avg_len}) não são de setor censitário. Tentando mapeamento por granularidade."
    )
    id_len = int(df_renda["cd_setor"].str.len().mode().iat[0]) if not df_renda.empty else 0

    with engine.begin() as conn:
        # Adiciona a coluna se não existir
        col_check = conn.execute(text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :tbl AND column_name = 'vl_renda'
            """
        ), {"tbl": table_name.lower()}).fetchone()
        if not col_check:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN vl_renda NUMERIC"))
            print("Database table altered: added vl_renda column.")

        applied = False
        if id_len == 7:
            # Mapeia por município (N6)
            tmp = df_renda.rename(columns={"cd_setor": "cd_mun"}).copy()
            tmp.to_sql('temp_renda_mun', con=conn, if_exists='replace', index=False)
            update_sql = text(f"""
                UPDATE {table_name} tgt
                SET vl_renda = src.vl_renda
                FROM temp_renda_mun src
                WHERE tgt."CD_MUN"::text = src.cd_mun::text
            """)
            conn.execute(update_sql)
            conn.execute(text('DROP TABLE IF EXISTS temp_renda_mun'))
            print("vl_renda atualizada por município (CD_MUN).")
            applied = True
        elif id_len == 2:
            # Mapeia por UF (N3)
            tmp = df_renda.rename(columns={"cd_setor": "cd_uf"}).copy()
            tmp.to_sql('temp_renda_uf', con=conn, if_exists='replace', index=False)
            update_sql = text(f"""
                UPDATE {table_name} tgt
                SET vl_renda = src.vl_renda
                FROM temp_renda_uf src
                WHERE tgt."CD_UF"::text = src.cd_uf::text
            """)
            conn.execute(update_sql)
            conn.execute(text('DROP TABLE IF EXISTS temp_renda_uf'))
            print("vl_renda atualizada por UF (CD_UF).")
            applied = True

    # Sempre salvar staging para referência
    staging_table = os.getenv("AGREGADO_STAGING_TABLE", "ibge_agregado_result")
    try:
        with engine.begin() as conn:
            df_to_save = df_renda.copy()
            df_to_save.rename(columns={"cd_setor": "id_localidade", "vl_renda": "valor"}, inplace=True)
            df_to_save.to_sql(staging_table, con=conn, if_exists='replace', index=False)
        print(f"Dados agregados salvos em staging '{staging_table}'.")
    except Exception as e:
        print(f"Falha ao salvar staging '{staging_table}': {e}")

    if not applied:
        print(f"Granularidade (len={id_len}) não suportada para atualização automática. Nenhuma atualização aplicada em {table_name}.")
    raise SystemExit(0)

with engine.begin() as conn:
    # Adiciona a coluna se não existir
    col_check = conn.execute(text(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :tbl AND column_name = 'vl_renda'
        """
    ), {"tbl": table_name.lower()}).fetchone()
    if not col_check:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN vl_renda NUMERIC"))
        print("Database table altered: added vl_renda column.")

    # Sobe os dados temporariamente
    # Use a conexão SQLAlchemy direta
    df_renda.to_sql('temp_renda', con=conn, if_exists='replace', index=False)

    # Descobre coluna de join existente na tabela-alvo (case-insensitive)
    preferred_order = [
        "cd_setor",  # preferida
        "cd_censit",
        "cd_set",
        "cd_setor_censitario",
        "cd_censitario",
    ]
    rows = conn.execute(text(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :tbl
        """
    ), {"tbl": table_name.lower()}).fetchall()
    name_map = {r[0].lower(): r[0] for r in rows}  # lower -> original
    join_col_key = next((c for c in preferred_order if c in name_map), None)
    if not join_col_key:
        raise SystemExit(
            f"Nenhuma coluna de junção encontrada em {table_name}. Procure por uma destas: {preferred_order}"
        )
    join_col = name_map[join_col_key]  # preserva o case original
    print(f"Using join column: {join_col}")

    # Cita o identificador para evitar problemas de case (ex.: "CD_SETOR")
    update_query = text(f"""
        UPDATE {table_name} tgt
        SET vl_renda = src.vl_renda
        FROM temp_renda src
        WHERE tgt."{join_col}"::text = src.cd_setor::text
    """)
    conn.execute(update_query)
    conn.execute(text('DROP TABLE IF EXISTS temp_renda'))

print("Successfully updated the PostGIS table with income data.")