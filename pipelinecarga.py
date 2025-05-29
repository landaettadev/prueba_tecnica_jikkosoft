import os
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from io import StringIO

load_dotenv()

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'pipeline.log')

logging.basicConfig(
    filename=LOG_FILE,
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def setup_logger():
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, LOG_LEVEL))
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

setup_logger()

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'landaettadev')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'consumos_db')

DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
POSTGRES_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres'

engine = create_engine(DATABASE_URL, echo=False, future=True)

COLUMN_MAPPING = {
    'año': 'anio',
    'año.': 'anio',
    'anio': 'anio',
    'anio.': 'anio'
}

def auto_read_csv(path):
    """
    Lee un CSV detectando el delimitador automáticamente.
    Soporta ; , o tabulación.
    """
    with open(path, 'r', encoding='utf-8') as f:
        sample = f.read(2048)
    sep = ';' if sample.count(';') > sample.count(',') else ','
    if sample.count('\t') > sample.count(sep):
        sep = '\t'
    df = pd.read_csv(path, sep=sep, engine='python')
    logging.info(f'Leído archivo {os.path.basename(path)} con delimitador "{sep}" y columnas: {df.columns.tolist()}')
    return df

def prepare_table(engine):
    try:
        postgres_engine = create_engine(POSTGRES_URL)
        
        with postgres_engine.connect() as conn:
            conn.execute(text("COMMIT;"))
            conn.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}') THEN
                        CREATE DATABASE {DB_NAME};
                    END IF;
                END
                $$;
            """))
        
        with engine.connect() as conn:
            conn.execute(text("SET search_path TO public;"))
            conn.execute(text("BEGIN;"))
            try:
                conn.execute(text("""
                    DROP TABLE IF EXISTS stats CASCADE;
                    CREATE TABLE stats (
                        id SMALLINT PRIMARY KEY DEFAULT 1,
                        total_rows BIGINT DEFAULT 0,
                        suma_tasa NUMERIC DEFAULT 0,
                        CHECK (id = 1)
                    );
                    
                    INSERT INTO stats (id, total_rows, suma_tasa)
                    VALUES (1, 0, 0);
                    
                    GRANT ALL PRIVILEGES ON TABLE stats TO PUBLIC;
                """))
                conn.execute(text("COMMIT;"))
                logging.info("Tabla 'stats' creada correctamente.")
            except Exception as e:
                conn.execute(text("ROLLBACK;"))
                logging.error(f"Error creando tabla stats: {e}")
                raise
            
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'stats'
                );
            """)).scalar()
            
            if not result:
                raise Exception("La tabla stats no se creó correctamente")

            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'consumos'
                );
            """)).scalar()
            
            if not result:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS consumos (
                        id VARCHAR PRIMARY KEY,
                        txt_id VARCHAR,
                        año INTEGER,
                        destino VARCHAR,
                        estrato FLOAT,
                        consumo FLOAT,
                        tasa FLOAT
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_consumos_destino ON consumos(destino);
                    CREATE INDEX IF NOT EXISTS idx_consumos_año ON consumos(año);
                    
                    GRANT ALL PRIVILEGES ON TABLE consumos TO PUBLIC;
                """))
                logging.info("Tabla 'consumos' creada correctamente.")
            else:
                conn.execute(text("TRUNCATE TABLE consumos;"))
                conn.execute(text("UPDATE stats SET total_rows = 0, suma_tasa = 0;"))
                logging.info("Tabla 'consumos' vaciada correctamente.")
                
    except Exception as e:
        logging.error(f"Error en la preparación de la tabla: {e}")
        raise


# Lectura y transformación de datos
def parse_percentage(value):
    if isinstance(value, str):
        value = value.replace('%', '').replace(',', '.')
        try:
            return float(value) / 100
        except:
            return 0.0
    return float(value)

def parse_float_european(val):
    if isinstance(val, str):
        val = val.replace('.', '').replace(',', '.')
    try:
        return float(val)
    except:
        return 0.0

def load_reference_files(data_folder):
    try:
        if not data_folder.exists():
            raise ValueError(f"La carpeta {data_folder} no existe")

        files = list(data_folder.glob('*'))
        
        tarifa_file = next((f for f in files if 'tarifa_por_destino' in f.name.lower() and f.is_file()), None)
        min_file = next((f for f in files if 'minimos' in f.name.lower() and f.is_file()), None)
        max_file = next((f for f in files if 'maximos' in f.name.lower() and f.is_file()), None)

        if not all([tarifa_file, min_file, max_file]):
            missing = []
            if not tarifa_file: missing.append("tarifa_por_destino.csv")
            if not min_file: missing.append("minimos.csv")
            if not max_file: missing.append("maximos.csv")
            raise ValueError(f"Faltan los siguientes archivos de referencia: {', '.join(missing)}")

        tarifas = pd.read_csv(tarifa_file, sep=',', encoding='utf-8')
        minimos = pd.read_csv(min_file, sep=',', encoding='utf-8')
        maximos = pd.read_csv(max_file, sep=',', encoding='utf-8')

        tarifas.columns = [c.strip().lower() for c in tarifas.columns]
        minimos.columns = [c.strip().lower() for c in minimos.columns]
        maximos.columns = [c.strip().lower() for c in maximos.columns]

        required_tarifa_cols = ['destino', 'tarifa sobre consumo']
        required_min_max_cols = ['año', 'mínimo']
        required_max_cols = ['año', 'máximo']

        missing_tarifa = [col for col in required_tarifa_cols if col not in tarifas.columns]
        missing_min = [col for col in required_min_max_cols if col not in minimos.columns]
        missing_max = [col for col in required_max_cols if col not in maximos.columns]

        if missing_tarifa or missing_min or missing_max:
            raise ValueError(f"Columnas faltantes en archivos de referencia: " +
                           f"tarifas: {missing_tarifa}, " +
                           f"mínimos: {missing_min}, " +
                           f"máximos: {missing_max}")

        tarifas['tarifa sobre consumo'] = tarifas['tarifa sobre consumo'].apply(parse_percentage)
        minimos['mínimo'] = minimos['mínimo'].apply(parse_float_european)
        maximos['máximo'] = maximos['máximo'].apply(parse_float_european)
        
        minimos['año'] = pd.to_numeric(minimos['año'], errors='coerce').fillna(0).astype(int)
        maximos['año'] = pd.to_numeric(maximos['año'], errors='coerce').fillna(0).astype(int)

        logging.info("Archivos de referencia cargados correctamente")
        return tarifas, minimos, maximos

    except Exception as e:
        logging.error(f"Error cargando archivos de referencia: {e}")
        raise


def procesar_lote(chunk, tarifas, minimos, maximos, txt_id):
    try:
        required_columns = ['id', 'año', 'destino', 'estrato', 'consumo']
        missing_columns = [col for col in required_columns if col not in chunk.columns]
        if missing_columns:
            raise ValueError(f"Columnas requeridas faltantes: {missing_columns}")

        chunk = chunk.copy()

        tarifa_dict = dict(zip(tarifas['destino'], tarifas['tarifa sobre consumo']))
        min_dict = dict(zip(minimos['año'], minimos['mínimo']))
        max_dict = dict(zip(maximos['año'], maximos['máximo']))

        chunk['año'] = pd.to_numeric(chunk['año'], errors='coerce').fillna(0).astype(int)
        chunk['consumo'] = chunk['consumo'].apply(parse_float_european)
        chunk['estrato'] = pd.to_numeric(chunk['estrato'], errors='coerce').fillna(0)
        
        # Identificar destinos inválidos pero no eliminarlos
        invalid_destinos = chunk[~chunk['destino'].isin(tarifa_dict.keys())]
        if not invalid_destinos.empty:
            logging.warning(f"Destinos inválidos encontrados: {invalid_destinos['destino'].unique()}")
          
            default_tarifa = tarifas['tarifa sobre consumo'].mean()
            for destino in invalid_destinos['destino'].unique():
                tarifa_dict[destino] = default_tarifa

        chunk.loc[:, 'tarifa'] = chunk['destino'].map(tarifa_dict)
        chunk.loc[:, 'tasa'] = chunk['consumo'] * chunk['tarifa']
        chunk.loc[:, 'minimo'] = chunk['año'].map(min_dict)
        chunk.loc[:, 'maximo'] = chunk['año'].map(max_dict)

        def ajustar_tasa(row):
            tasa = row['tasa']
            if not pd.isna(row['minimo']):
                tasa = max(tasa, row['minimo'])
            if not pd.isna(row['maximo']):
                tasa = min(tasa, row['maximo'])
            return tasa

        chunk.loc[:, 'tasa'] = chunk.apply(ajustar_tasa, axis=1)
        chunk.loc[:, 'txt_id'] = txt_id
        
        chunk = chunk.drop_duplicates(subset=['id'], keep='last')
        chunk = chunk.dropna(subset=['id', 'año', 'destino', 'consumo', 'tasa'])
        
        return chunk
    except Exception as e:
        logging.error(f"Error procesando lote: {e}")
        raise

def insert_chunk(chunk, engine):
    """Inserta un chunk usando COPY para mejor rendimiento"""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TEMP TABLE temp_consumos (
                    id VARCHAR,
                    txt_id VARCHAR,
                    año INTEGER,
                    destino VARCHAR,
                    estrato FLOAT,
                    consumo FLOAT,
                    tasa FLOAT
                ) ON COMMIT DROP;
            """))
            
            cols_insert = ['id', 'txt_id', 'año', 'destino', 'estrato', 'consumo', 'tasa']
            chunk_to_insert = chunk[cols_insert].copy()
            
            with conn.connection.cursor() as cur:
                output = StringIO()
                chunk_to_insert.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, 'temp_consumos', null='')
            
            conn.execute(text("""
                INSERT INTO consumos (id, txt_id, año, destino, estrato, consumo, tasa)
                SELECT id, txt_id, año, destino, estrato, consumo, tasa
                FROM temp_consumos
                ON CONFLICT (id) DO UPDATE SET
                    txt_id = EXCLUDED.txt_id,
                    año = EXCLUDED.año,
                    destino = EXCLUDED.destino,
                    estrato = EXCLUDED.estrato,
                    consumo = EXCLUDED.consumo,
                    tasa = EXCLUDED.tasa;
            """))
            
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'stats'
                );
            """)).scalar()
            
            if result:
                conn.execute(text("""
                    UPDATE stats 
                    SET total_rows = total_rows + :n,
                        suma_tasa = suma_tasa + :s
                    WHERE id = 1;
                """), {
                    'n': len(chunk),
                    's': float(chunk['tasa'].sum())
                })
            else:
                logging.warning("Tabla 'stats' no encontrada. No se actualizarán las estadísticas.")
            
            conn.commit()
            
    except Exception as e:
        logging.error(f"Error insertando lote: {e}")
        raise


# Pipeline principal
def main():
    logging.info("=== INICIO PIPELINE ===")
    BASE_DIR = Path(__file__).parent
    DATA_FOLDER = BASE_DIR / os.getenv('DATA_FOLDER', 'datasets')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    
    try:

        if not DATA_FOLDER.exists():
            raise ValueError(f"La carpeta {DATA_FOLDER} no existe. Por favor, crea la carpeta 'datasets' y coloca los archivos necesarios.")


        try:
            prepare_table(engine)
        except Exception as e:
            logging.error(f"Error preparando la base de datos: {e}")
            raise


        try:
            tarifas, minimos, maximos = load_reference_files(DATA_FOLDER)
        except Exception as e:
            logging.error(f"Error cargando archivos de referencia: {e}")
            raise


        dataset_files = sorted([f for f in DATA_FOLDER.glob('dataset-*.txt')])
        
        if not dataset_files:
            raise ValueError("No se encontraron archivos dataset-*.txt en la carpeta datasets")

        for file in dataset_files:
            txt_id = file.stem
            file_count = 0
            file_sum = 0.0
            
            try:
 
                with open(file, 'r', encoding='utf-8') as f:
                    sample = f.read(2048)
                sep = ';' if sample.count(';') > sample.count(',') else ','
                if sample.count('\t') > sample.count(sep):
                    sep = '\t'
                
                for chunk in pd.read_csv(file, chunksize=BATCH_SIZE, sep=sep, engine='python'):
                    chunk.columns = [c.strip().lower() for c in chunk.columns]
                    chunk = procesar_lote(chunk, tarifas, minimos, maximos, txt_id)
                    
                    if not chunk.empty:
                        insert_chunk(chunk, engine)
                        
                        file_count += len(chunk)
                        file_sum += chunk['tasa'].sum()
                        
                        logging.info(f"Archivo: {file.name} | Lote actual: {len(chunk)} | Total archivo: {file_count} | Suma tasa archivo: {file_sum:,.2f}")
                
                logging.info(f"Archivo {file.name} procesado completamente. Total registros: {file_count}, Suma tasa: {file_sum:,.2f}")
                
            except Exception as e:
                logging.error(f"Error procesando archivo {file.name}: {e}")
                continue

        try:
            with engine.connect() as conn:
                res = conn.execute(text("""
                    SELECT COUNT(*) as recuento, 
                           AVG(tasa) as promedio, 
                           SUM(tasa) as suma, 
                           MIN(tasa) as minimo, 
                           MAX(tasa) as maximo
                    FROM consumos
                """)).fetchone()
                
            logging.info("--- Resumen Final ---")
            print("\n--- Resumen Final ---")
            print(f"Total registros: {res.recuento}")
            print(f"Promedio tasa: {res.promedio:.2f}")
            print(f"Suma total tasa: {res.suma:,.2f}")
            print(f"Valor mínimo tasa: {res.minimo:.2f}")
            print(f"Valor máximo tasa: {res.maximo:.2f}")
            
        except SQLAlchemyError as e:
            logging.error(f"Error consultando resumen: {e}")
            
    except Exception as e:
        logging.error(f"Error en el pipeline: {e}")
        raise
    finally:
        logging.info("=== FIN PIPELINE ===")
if __name__ == '__main__':
    main()
    