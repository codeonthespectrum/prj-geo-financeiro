import os
import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement

engine = create_engine(os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@db:5432/geo"))

shapefile_path = "data/SIRGAS_SHP_estacaometro.shp"
table_name = "pois_metro_sp"
geometry_column = "geom"
crs_target = "EPSG:4326"
# CRS de origem (quando ausente) — SIRGAS 2000 / UTM 23S é típico para SP
crs_source_default = os.getenv("POIS_SOURCE_CRS", "EPSG:31983")

print("Database engine created successfully.")

# ---- leitura e preparaçao de dados --------
print("Loading shapefile...")
gdf = gpd.read_file(shapefile_path)
print(f"Shapefile loaded with {len(gdf)} features successfully.")
#-------------------------------------------

# Define CRS de origem, se ausente
if gdf.crs is None:
    print(f"Input CRS is None — setting source CRS to {crs_source_default} (override with POIS_SOURCE_CRS)")
    gdf.set_crs(crs_source_default, inplace=True)

# Reprojetar para 4326 se necessário
if str(gdf.crs) != crs_target:
    print(f"Reprojecting from {gdf.crs} to {crs_target}...")
    gdf = gdf.to_crs(crs_target)

gdf.rename(columns={'geometry': geometry_column}, inplace=True)
gdf[geometry_column] = gdf[geometry_column].apply(lambda x: WKTElement(x.wkt, srid=4326))

# --------- postgis --------
print(f"Saving data to PostGIS table '{table_name}'...")
gdf.to_sql(
    table_name,
    engine,
    if_exists='replace',
    index=False,
    dtype={geometry_column: Geometry('POINT', srid=4326)}
)
print("Successfully saved data to PostGIS.")