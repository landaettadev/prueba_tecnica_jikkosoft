from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table

DATABASE_URL = 'postgresql+psycopg2://postgres:landaettadev@localhost:5432/consumos_db'


engine = create_engine(DATABASE_URL)
metadata = MetaData()
    

consumos = Table('consumos', metadata,
    Column('id', String, primary_key=True),      
    Column('txt_id', String),                    
    Column('año', Integer),
    Column('destino', String),
    Column('estrato', Float),                   
    Column('consumo', Float),
    Column('tasa', Float),                        
)


metadata.create_all(engine)

print("Tabla 'consumos' creada exitosamente.")
