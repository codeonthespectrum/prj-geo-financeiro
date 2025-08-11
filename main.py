# app/main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from sqlalchemy import create_engine, text
from typing import Optional
import geopandas as gpd
import os


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")

app = FastAPI(
    title="API de Dados Geoespaciais de SP",
    description="Uma API para servir dados de renda e proximidade a metrôs por setor censitário."
)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Métricas base conhecidas; demais (distancia_metro_*) serão descobertas dinamicamente via information_schema
ALLOWED_METRICS = ["distancia_metro_m", "vl_renda", "vl_renda_setor"]

# CORS para desenvolvimento (Live Server e localhost)
origins = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://127.0.0.1",
    "http://localhost",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Bem-vindo à API Geo. Acesse /docs para ver a documentação."}


@app.get("/heatmap", response_class=Response)
def get_heatmap(
    metric: str = Query("distancia_metro_m"),
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    simplify: Optional[float] = Query(None, gt=0, description="tolerância de simplificação em graus"),
    limit: Optional[int] = Query(None, gt=0, le=50000, description="limite máximo de features")
):
  
    print(f"Requisição recebida para a métrica: {metric}")
    
    # Verifica se a coluna existe na tabela alvo (whitelist dinâmica)
    with engine.connect() as conn:
        exists = conn.execute(text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='sp_setores' AND column_name = :metric
            """
        ), {"metric": metric}).fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail=f"Métrica '{metric}' não disponível na tabela.")

    # Montagem dinâmica segura
    geom_expr = "geom"
    if simplify is not None:
        geom_expr = f"ST_SimplifyPreserveTopology(geom, :simplify) AS geom"
    else:
        geom_expr = "geom"

    where_clauses = [f'"{metric}" IS NOT NULL']
    params = {}
    if simplify is not None:
        params["simplify"] = float(simplify)

    # Aplica filtro espacial por bbox se informado
    if bbox:
        try:
            parts = [float(x) for x in bbox.split(',')]
            if len(parts) != 4:
                raise ValueError("bbox deve ter 4 números")
            minx, miny, maxx, maxy = parts
            # sanity check
            if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
                raise ValueError("bbox inválido")
            where_clauses.append("ST_Intersects(geom, ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))")
            params.update({"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Parâmetro bbox inválido: {e}")

    where_sql = " AND ".join(where_clauses)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""

    sql_query = f"""
        SELECT "CD_SETOR" as id, "{metric}", {geom_expr}
        FROM sp_setores
        WHERE {where_sql}
        {limit_sql}
    """
    
    try:
        # Lê os dados do PostGIS usando GeoPandas (geom nativo)
        gdf = gpd.read_postgis(text(sql_query), engine, geom_col='geom', params=params)
        
        # Converte o GeoDataFrame para uma string GeoJSON
        geojson_str = gdf.to_json()
        
        # Retorna a resposta como GeoJSON
        return Response(content=geojson_str, media_type="application/json")

    except Exception as e:
        print(f"Erro ao consultar o banco: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar a solicitação.")


@app.get("/metrics")
def list_metrics():
    """
    Lista as métricas suportadas e a cobertura (linhas não nulas) em sp_setores.
    """
    results = []
    with engine.connect() as conn:
        # total de linhas na tabela
        total = conn.execute(text("SELECT COUNT(*) FROM sp_setores")).scalar() or 0

        # Descobre métricas dinâmicas de distância por linha
        dyn_cols = conn.execute(text(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='sp_setores' AND column_name LIKE 'distancia_metro_%'
            """
        )).fetchall()
        dyn_metrics = [r[0] for r in dyn_cols]

        for metric in sorted(set(ALLOWED_METRICS + dyn_metrics)):
            # checa existência da coluna
            exists = conn.execute(
                text(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='sp_setores' AND column_name = :metric
                    """
                ),
                {"metric": metric},
            ).fetchone() is not None

            non_null = 0
            if exists and total > 0:
                # conta valores não nulos da métrica
                # Nota: não é possível parametrizar o nome da coluna; garantimos segurança via lista ALLOWED_METRICS
                count_sql = text(f'SELECT COUNT(*) FROM sp_setores WHERE "{metric}" IS NOT NULL')
                non_null = conn.execute(count_sql).scalar() or 0

            coverage_pct = (non_null / total * 100.0) if total > 0 else 0.0

            results.append({
                "metric": metric,
                "exists": exists,
                "non_null": int(non_null),
                "total": int(total),
                "coverage_pct": round(coverage_pct, 2),
            })

    return {"metrics": results}


@app.get("/stats")
def stats(
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    sample_limit: int = Query(800, gt=50, le=5000),
    bins: int = Query(8, gt=3, le=20, description="número de faixas para médias por distância"),
    bin_mode: str = Query("width", pattern="^(width|quantile)$", description="método de bins: width (largura igual) ou quantile"),
    renda_metric: str = Query("vl_renda"),
    dist_metric: str = Query("distancia_metro_m")
):
    """
    Estatísticas entre vl_renda e distancia_metro_m na área (bbox):
    - count: número de setores com ambas as métricas
    - r: correlação de Pearson (vl_renda vs distancia_metro_m)
    - pairs: amostra de pares [vl_renda, distancia_metro_m]
    """
    # Validação de colunas (whitelist dinâmica via information_schema)
    safe_name = lambda s: isinstance(s, str) and len(s) <= 64 and s.replace('_','').isalnum()
    if not (safe_name(renda_metric) and safe_name(dist_metric)):
        raise HTTPException(status_code=400, detail="Nomes de colunas inválidos.")
    with engine.connect() as conn:
        chk = conn.execute(text(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='sp_setores' AND column_name IN (:r,:d)
            """
        ), {"r": renda_metric, "d": dist_metric}).fetchall()
        if len(chk) < 2:
            raise HTTPException(status_code=400, detail="Coluna de renda ou distância não existe em sp_setores.")

    renda_col = f'"{renda_metric}"'
    dist_col = f'"{dist_metric}"'

    where_clauses = [f'{renda_col} IS NOT NULL', f'{dist_col} IS NOT NULL']
    params = {}

    if bbox:
        try:
            parts = [float(x) for x in bbox.split(',')]
            if len(parts) != 4:
                raise ValueError("bbox deve ter 4 números")
            minx, miny, maxx, maxy = parts
            if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
                raise ValueError("bbox inválido")
            where_clauses.append("ST_Intersects(geom, ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))")
            params.update({"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Parâmetro bbox inválido: {e}")

    where_sql = " AND ".join(where_clauses)

    # Agregados estatísticos (usa corr do PostgreSQL)
    agg_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        )
        SELECT COUNT(*) AS n,
               corr(renda, dist) AS r,
               MIN(renda) AS renda_min, MAX(renda) AS renda_max,
               MIN(dist) AS dist_min, MAX(dist) AS dist_max
        FROM s
        """
    )
    # Spearman via ranks
    spear_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist
            FROM sp_setores
            WHERE {where_sql}
        ), r AS (
            SELECT PERCENT_RANK() OVER (ORDER BY renda) AS renda_rnk,
                   PERCENT_RANK() OVER (ORDER BY dist)  AS dist_rnk
            FROM s
        )
        SELECT corr(renda_rnk, dist_rnk) AS r_s FROM r
        """
    )
    # Correlação após remover efeito fixo municipal (demeaning por CD_MUN)
    demean_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        ), j AS (
            SELECT (s.renda - m.renda_m) AS renda_res,
                   (s.dist  - m.dist_m)  AS dist_res
            FROM s JOIN m USING (cd_mun)
        )
        SELECT COUNT(*) AS n_res, corr(renda_res, dist_res) AS r_res FROM j
        """
    )
    # Spearman nos residuais
    spear_demean_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        ), j AS (
            SELECT (s.renda - m.renda_m) AS renda_res,
                   (s.dist  - m.dist_m)  AS dist_res
            FROM s JOIN m USING (cd_mun)
        ), r AS (
            SELECT PERCENT_RANK() OVER (ORDER BY renda_res) AS rr,
                   PERCENT_RANK() OVER (ORDER BY dist_res)  AS dr
            FROM j
        )
        SELECT corr(rr, dr) AS r_s_res FROM r
        """
    )
    # Amostra de pares
    sample_sql = text(
        f"""
    SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist
        FROM sp_setores
        WHERE {where_sql}
        ORDER BY random()
        LIMIT :lim
        """
    )
    # Bins por distância - largura igual
    bins_width_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist
            FROM sp_setores
            WHERE {where_sql}
        ), ext AS (
            SELECT MIN(dist) AS min_d, MAX(dist) AS max_d FROM s
        ), params AS (
            SELECT CAST(:bins AS int) AS bins
        ), edges AS (
            SELECT i AS bin,
                   ext.min_d + i * (ext.max_d - ext.min_d) / NULLIF(params.bins, 0) AS min_edge,
                   ext.min_d + (i+1) * (ext.max_d - ext.min_d) / NULLIF(params.bins, 0) AS max_edge,
                   params.bins
            FROM generate_series(0, (SELECT bins-1 FROM params)) i, ext, params
        )
        SELECT e.bin::int,
               e.min_edge,
               e.max_edge,
               COUNT(s.*)             AS n,
               AVG(s.renda)           AS renda_avg,
               AVG(s.dist)            AS dist_avg
        FROM edges e
        LEFT JOIN s
          ON s.dist >= e.min_edge AND (s.dist < e.max_edge OR (e.bin = e.bins-1 AND s.dist <= e.max_edge))
        GROUP BY e.bin, e.min_edge, e.max_edge
        ORDER BY e.bin
        """
    )
    # Bins por distância - quantis (ntile)
    bins_quant_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist
            FROM sp_setores
            WHERE {where_sql}
        ), r AS (
            SELECT renda, dist, NTILE(:bins) OVER (ORDER BY dist) AS bin
            FROM s
        )
        SELECT bin::int,
               MIN(dist) AS min_edge,
               MAX(dist) AS max_edge,
               COUNT(*)  AS n,
               AVG(renda) AS renda_avg,
               AVG(dist)  AS dist_avg
        FROM r
        GROUP BY bin
        ORDER BY bin
        """
    )
    # Bins intra (demeaned) - largura igual
    bins_demean_width_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        ), j AS (
            SELECT (s.renda - m.renda_m) AS renda_res,
                   (s.dist  - m.dist_m)  AS dist_res
            FROM s JOIN m USING (cd_mun)
        ), ext AS (
            SELECT MIN(dist_res) AS min_d, MAX(dist_res) AS max_d FROM j
        ), params AS (
            SELECT CAST(:bins AS int) AS bins
        ), edges AS (
            SELECT i AS bin,
                   ext.min_d + i * (ext.max_d - ext.min_d) / NULLIF(params.bins, 0) AS min_edge,
                   ext.min_d + (i+1) * (ext.max_d - ext.min_d) / NULLIF(params.bins, 0) AS max_edge,
                   params.bins
            FROM generate_series(0, (SELECT bins-1 FROM params)) i, ext, params
        )
        SELECT e.bin::int,
               e.min_edge,
               e.max_edge,
               COUNT(j.*)               AS n,
               AVG(j.renda_res)         AS renda_avg,
               AVG(j.dist_res)          AS dist_avg
        FROM edges e
        LEFT JOIN j
          ON j.dist_res >= e.min_edge AND (j.dist_res < e.max_edge OR (e.bin = e.bins-1 AND j.dist_res <= e.max_edge))
        GROUP BY e.bin, e.min_edge, e.max_edge
        ORDER BY e.bin
        """
    )
    # Bins intra (demeaned) - quantis
    bins_demean_quant_sql = text(
        f"""
        WITH s AS (
            SELECT {renda_col}::float8 AS renda, {dist_col}::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        ), j AS (
            SELECT (s.renda - m.renda_m) AS renda_res,
                   (s.dist  - m.dist_m)  AS dist_res
            FROM s JOIN m USING (cd_mun)
        ), r AS (
            SELECT renda_res, dist_res, NTILE(:bins) OVER (ORDER BY dist_res) AS bin
            FROM j
        )
        SELECT bin::int,
               MIN(dist_res) AS min_edge,
               MAX(dist_res) AS max_edge,
               COUNT(*)      AS n,
               AVG(renda_res) AS renda_avg,
               AVG(dist_res)  AS dist_avg
        FROM r
        GROUP BY bin
        ORDER BY bin
        """
    )
    # Correlação intermunicipal (between)
    between_sql = text(
        f"""
        WITH s AS (
            SELECT "vl_renda"::float8 AS renda, "distancia_metro_m"::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        )
        SELECT COUNT(*) AS n_mun, corr(renda_m, dist_m) AS r_between FROM m
        """
    )
    between_spear_sql = text(
        f"""
        WITH s AS (
            SELECT "vl_renda"::float8 AS renda, "distancia_metro_m"::float8 AS dist, "CD_MUN"::text AS cd_mun
            FROM sp_setores
            WHERE {where_sql}
        ), m AS (
            SELECT cd_mun, AVG(renda) AS renda_m, AVG(dist) AS dist_m FROM s GROUP BY cd_mun
        ), r AS (
            SELECT PERCENT_RANK() OVER (ORDER BY renda_m) AS rr,
                   PERCENT_RANK() OVER (ORDER BY dist_m)  AS dr
            FROM m
        )
        SELECT corr(rr, dr) AS r_s_between FROM r
        """
    )

    with engine.connect() as conn:
        agg_row = conn.execute(agg_sql, params).mappings().first()
        spear_row = conn.execute(spear_sql, params).mappings().first()
        demean_row = conn.execute(demean_sql, params).mappings().first()
        spear_demean_row = conn.execute(spear_demean_sql, params).mappings().first()
        params2 = dict(params)
        params2["lim"] = int(sample_limit)
        pairs = conn.execute(sample_sql, params2).fetchall()
        params3 = dict(params)
        params3["bins"] = int(bins)
        if bin_mode == "quantile":
            bins_rows = conn.execute(bins_quant_sql, params3).mappings().all()
            bins_demean_rows = conn.execute(bins_demean_quant_sql, params3).mappings().all()
        else:
            bins_rows = conn.execute(bins_width_sql, params3).mappings().all()
            bins_demean_rows = conn.execute(bins_demean_width_sql, params3).mappings().all()

        between_row = conn.execute(between_sql, params).mappings().first()
        between_spear_row = conn.execute(between_spear_sql, params).mappings().first()

    result = {
        "count": int(agg_row["n"]) if agg_row and agg_row["n"] is not None else 0,
        "r": float(agg_row["r"]) if agg_row and agg_row["r"] is not None else None,
        "r_s": float(spear_row["r_s"]) if spear_row and spear_row["r_s"] is not None else None,
        "r_demeaned": float(demean_row["r_res"]) if demean_row and demean_row["r_res"] is not None else None,
        "r_s_demeaned": float(spear_demean_row["r_s_res"]) if spear_demean_row and spear_demean_row["r_s_res"] is not None else None,
    "r_between": float(between_row["r_between"]) if between_row and between_row["r_between"] is not None else None,
    "r_s_between": float(between_spear_row["r_s_between"]) if between_spear_row and between_spear_row["r_s_between"] is not None else None,
        "renda_min": float(agg_row["renda_min"]) if agg_row and agg_row["renda_min"] is not None else None,
        "renda_max": float(agg_row["renda_max"]) if agg_row and agg_row["renda_max"] is not None else None,
        "dist_min": float(agg_row["dist_min"]) if agg_row and agg_row["dist_min"] is not None else None,
        "dist_max": float(agg_row["dist_max"]) if agg_row and agg_row["dist_max"] is not None else None,
        "pairs": [[float(r), float(d)] for (r, d) in pairs],
        "bins": [
            {
                "bin": int(row["bin"]),
                "min": float(row["min_edge"]) if row["min_edge"] is not None else None,
                "max": float(row["max_edge"]) if row["max_edge"] is not None else None,
                "n": int(row["n"]),
                "renda_avg": float(row["renda_avg"]) if row["renda_avg"] is not None else None,
                "dist_avg": float(row["dist_avg"]) if row["dist_avg"] is not None else None,
            }
            for row in bins_rows
        ],
        "bins_demeaned": [
            {
                "bin": int(row["bin"]),
                "min": float(row["min_edge"]) if row["min_edge"] is not None else None,
                "max": float(row["max_edge"]) if row["max_edge"] is not None else None,
                "n": int(row["n"]),
                "renda_avg": float(row["renda_avg"]) if row["renda_avg"] is not None else None,
                "dist_avg": float(row["dist_avg"]) if row["dist_avg"] is not None else None,
            }
            for row in bins_demean_rows
        ],
    }
    return result


@app.get("/stations", response_class=Response)
def get_stations(
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    limit: Optional[int] = Query(2000, gt=0, le=10000)
):
    """Retorna estações de metrô como GeoJSON com filtro opcional por bbox."""
    where = []
    params = {}
    if bbox:
        try:
            parts = [float(x) for x in bbox.split(',')]
            if len(parts) != 4:
                raise ValueError("bbox deve ter 4 números")
            minx, miny, maxx, maxy = parts
            if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
                raise ValueError("bbox inválido")
            where.append("ST_Intersects(geom, ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))")
            params.update({"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Parâmetro bbox inválido: {e}")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    lim_sql = f" LIMIT {int(limit)}" if limit else ""

    sql = f"""
        SELECT *
        FROM pois_metro_sp
        {where_sql}
        {lim_sql}
    """
    try:
        gdf = gpd.read_postgis(text(sql), engine, geom_col='geom', params=params)
        return Response(content=gdf.to_json(), media_type="application/json")
    except Exception as e:
        print(f"Erro ao consultar estações: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar estações.")


@app.get("/points", response_class=Response)
def get_points(
    metric: str = Query("distancia_metro_m"),
    bbox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    limit: Optional[int] = Query(20000, ge=50, le=100000, description="limite de pontos"),
    snap: Optional[float] = Query(None, gt=0, description="tamanho da grade em graus para agregar pontos (ST_SnapToGrid)")
):
    """Retorna pontos (centróides) de setores com a métrica solicitada para visualização leve."""
    # validar coluna
    with engine.connect() as conn:
        exists = conn.execute(text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='sp_setores' AND column_name = :metric
            """
        ), {"metric": metric}).fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail=f"Métrica '{metric}' não disponível em sp_setores.")

    where = [f'"{metric}" IS NOT NULL']
    params = {}
    if bbox:
        try:
            parts = [float(x) for x in bbox.split(',')]
            if len(parts) != 4:
                raise ValueError("bbox deve ter 4 números")
            minx, miny, maxx, maxy = parts
            if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
                raise ValueError("bbox inválido")
            where.append("ST_Intersects(geom, ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326))")
            params.update({"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Parâmetro bbox inválido: {e}")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    lim_sql = f" LIMIT {int(limit)}" if limit else ""
    if snap is None:
        sql = f"""
            SELECT "CD_SETOR" AS id, "{metric}" AS value, ST_PointOnSurface(geom) AS geom
            FROM sp_setores
            {where_sql}
            {lim_sql}
        """
    else:
        sql = f"""
            WITH s AS (
                SELECT "{metric}"::float8 AS value,
                       ST_SnapToGrid(ST_PointOnSurface(geom), :snap) AS g
                FROM sp_setores
                {where_sql}
                {lim_sql}
            )
            SELECT NULL::text AS id,
                   AVG(value) AS value,
                   g AS geom
            FROM s
            GROUP BY g
        """
    try:
        # inclui bind para :snap quando solicitado
        params_exec = dict(params)
        if snap is not None:
            params_exec["snap"] = float(snap)
        # debug leve
        try:
            print(f"/points params -> keys: {list(params_exec.keys())}, snap={params_exec.get('snap')}")
        except Exception:
            pass
        gdf = gpd.read_postgis(text(sql), engine, geom_col='geom', params=params_exec)
        # renomear para manter compatibilidade com front (properties[metric])
        if 'value' in gdf.columns:
            gdf.rename(columns={'value': metric}, inplace=True)
        return Response(content=gdf.to_json(), media_type="application/json")
    except Exception as e:
        print(f"Erro ao consultar pontos: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar pontos.")


@app.get("/lines")
def list_lines():
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT DISTINCT emt_linha FROM pois_metro_sp WHERE emt_linha IS NOT NULL ORDER BY 1"))
        return {"lines": [r[0] for r in rows]}


@app.get("/line_extent")
def line_extent(linha: str = Query(..., description="nome da linha em maiúsculas (ex.: LILAS)")):
    with engine.connect() as conn:
        row = conn.execute(text(
            """
            SELECT ST_Extent(geom) AS e
            FROM pois_metro_sp
            WHERE emt_linha = :linha
            """
        ), {"linha": linha}).first()
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Linha não encontrada.")
        # ST_Extent retorna BOX(minx miny,maxx maxy)
        extent = row[0]
        box = extent.replace('BOX(', '').replace(')', '')
        min_part, max_part = box.split(',')
        minx, miny = [float(x) for x in min_part.split(' ')]
        maxx, maxy = [float(x) for x in max_part.split(' ')]
        cx = (minx + maxx) / 2.0
        cy = (miny + maxy) / 2.0
        return {"bbox": [minx, miny, maxx, maxy], "center": [cy, cx]}