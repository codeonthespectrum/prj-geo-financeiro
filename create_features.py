import os
from sqlalchemy import create_engine, text

# Conexão ao banco (usa DATABASE_URL do compose; fallback para serviço db)
db_url = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")

# Tabelas e parâmetros
setores_table = os.getenv("SETORES_TABLE", "sp_setores")
pois_table = os.getenv("POIS_TABLE", "pois_metro_sp")
dist_col = os.getenv("DIST_COL", "distancia_metro_m")
crs_metric = int(os.getenv("CRS_METRIC", "31983"))  # SIRGAS 2000 / UTM 23S

engine = create_engine(db_url, pool_pre_ping=True)

with engine.begin() as conn:
    # Garantir extensão e índices (idempotente)
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{setores_table}_geom ON {setores_table} USING GIST (geom);"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{pois_table}_geom ON {pois_table} USING GIST (geom);"))

    # Adicionar coluna de distância, se não existir
    conn.execute(text(
        f"ALTER TABLE {setores_table} ADD COLUMN IF NOT EXISTS {dist_col} DOUBLE PRECISION;"
    ))
    print(f"Column '{dist_col}' ensured on '{setores_table}'.")

    # Atualizar distâncias (em metros) até a estação de metrô mais próxima (todas as linhas)
    print(f"Updating '{dist_col}' with nearest metro station distances (CRS metric {crs_metric})...")
    update_query = f"""
        UPDATE {setores_table} s
        SET {dist_col} = (
            SELECT ST_Distance(
                ST_Transform(s.geom, {crs_metric}),
                ST_Transform(p.geom, {crs_metric})
            )
            FROM {pois_table} p
            ORDER BY s.geom <-> p.geom
            LIMIT 1
        )
        WHERE s.geom IS NOT NULL;
    """
    conn.execute(text(update_query))

    # Distâncias por linha (cria colunas distancia_metro_<linha>)
    print("Computing distances per metro line...")
    linhas = [r[0] for r in conn.execute(text(f"SELECT DISTINCT emt_linha FROM {pois_table} WHERE emt_linha IS NOT NULL"))]
    for linha in linhas:
        # slug seguro para nome de coluna
        slug = linha.strip().upper().replace('Ç','C').replace('Ã','A').replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Â','A').replace('Ê','E').replace('Ô','O')
        slug = ''.join(ch for ch in slug if ch.isalnum() or ch=='_')
        col = f"distancia_metro_{slug}"
        conn.execute(text(f"ALTER TABLE {setores_table} ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION;"))
        q = f"""
            UPDATE {setores_table} s
            SET {col} = (
                SELECT ST_Distance(
                    ST_Transform(s.geom, {crs_metric}),
                    ST_Transform(p.geom, {crs_metric})
                )
                FROM {pois_table} p
                WHERE p.emt_linha = :linha
                ORDER BY s.geom <-> p.geom
                LIMIT 1
            )
            WHERE s.geom IS NOT NULL;
        """
        conn.execute(text(q), {"linha": linha})
        print(f"Updated {col}")

print(f"Successfully updated '{dist_col}' with distances from metro stations.")