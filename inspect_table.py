import os
from sqlalchemy import create_engine, text

db_url = os.getenv("DATABASE_URL", "postgresql://myuser:mypassword@db:5432/geodb")
engine = create_engine(db_url, pool_pre_ping=True)

def list_columns(table: str):
    with engine.connect() as conn:
        cols = conn.execute(text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:tbl
            ORDER BY ordinal_position
            """
        ), {"tbl": table.lower()}).fetchall()
        return [(c[0], c[1]) for c in cols]

def geom_info(table: str):
    with engine.connect() as conn:
        rows = conn.execute(text(
            """
            SELECT f_geometry_column, type, srid
            FROM geometry_columns
            WHERE f_table_schema='public' AND f_table_name=:tbl
            """
        ), {"tbl": table.lower()}).fetchall()
        return rows

if __name__ == "__main__":
    table = os.getenv("CENSO_TABLE", "sp_setores")
    print("Table:", table)
    print("Columns:")
    for name, dtype in list_columns(table):
        print(f"- {name} :: {dtype}")
    print("Geometry:")
    for r in geom_info(table):
        print("- ", r)
