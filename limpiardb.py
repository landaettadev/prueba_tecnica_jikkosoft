from sqlalchemy import create_engine, text

DATABASE_URL = 'postgresql+psycopg2://postgres:landaettadev@localhost:5432/consumos_db'
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE consumos;"))
print("Tabla consumos vaciada correctamente.")
