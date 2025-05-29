import pandas as pd
import os

DATA_FOLDER = 'datasets'  

tarifa_file = [f for f in os.listdir(DATA_FOLDER) if f.startswith('tarifa_por_destino') and f.endswith('.csv')][0]
tarifas = pd.read_csv(os.path.join(DATA_FOLDER, tarifa_file))
print(tarifas.columns)
print(tarifas.head())
