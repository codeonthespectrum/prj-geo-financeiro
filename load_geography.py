import geopandas as gpd
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry, WKTElement
import os

# Use a URL do banco do ambiente (docker-compose define DATABASE_URL para 'db'),
# com fallback seguro para o host 'db' na rede do Compose.
db_url = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")
shapefile_path = "data/SP_setores_CD2022.shp"
table_name = "sp_setores"
geometry_column = "geom"
crs_target = "EPSG:4326"

engine = create_engine(db_url, pool_pre_ping=True)
print(f"Database engine created successfully. URL: {db_url}")

# ---- leitura e preparaçao de dados --------
print("Loading shapefile...")
gdf = gpd.read_file(shapefile_path)
print(f"Shapefile loaded with {len(gdf)} features successfully.")
#-------------------------------------------

if gdf.crs is None or gdf.crs.to_string() != crs_target:
    print(f"Reprojecting from {gdf.crs} to {crs_target}...")
    gdf = gdf.to_crs(crs_target)
    
gdf.rename(columns={'geometry': geometry_column}, inplace=True)
gdf[geometry_column] = gdf[geometry_column].apply(lambda x: WKTElement(x.wkt, srid=4326))

# --------- postgis --------
# Garante que a extensão PostGIS está habilitada no banco de destino
with engine.begin() as conn:
    try:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        print("PostGIS extension ensured.")
    except Exception as e:
        # Não falhar se já estiver habilitado/sem permissões; apenas reportar
        print(f"Warning: could not ensure PostGIS extension: {e}")

print(f"Saving data to PostGIS table '{table_name}'...")
gdf.to_sql(
    table_name,
    engine,
    if_exists='replace',
    index=False,
    # Usa GEOMETRY genérico para evitar conflitos Polygon/MultiPolygon
    dtype={geometry_column: Geometry(geometry_type='GEOMETRY', srid=4326)}
)
print("Successfully saved data to PostGIS.")