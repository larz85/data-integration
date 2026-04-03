import duckdb
import requests
import zipfile
import io
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar el token desde el archivo .env
load_dotenv()
md_token = os.getenv("MOTHERDUCK_TOKEN")

if not md_token:
    raise ValueError("Error: No se encontró MOTHERDUCK_TOKEN en el archivo .env")

def descargar_y_cargar_awards(year):
    print(f"Iniciando extracción para el año {year}...")
    url = f"https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/awa-masivo.zip"
    
    print(f"Descargando {url}...")
    response = requests.get(url, verify=False)
    response.raise_for_status()
    
    print("Descomprimiendo en memoria...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        award_filename = next(f for f in z.namelist() if f.endswith('awards.csv'))
        
        print(f"Extrayendo {award_filename}...")
        with z.open(award_filename) as f:
            # Especificamos los tipos de datos clave para evitar errores al subir a la nube
            df = pd.read_csv(f, dtype={'compiledRelease/awards/0/id': str})
    
    return df

# Conexión a MotherDuck
print("Conectando a MotherDuck (aml_proveedores_py)...")
# Al pasar md:nombre_db, DuckDB sabe que debe ir a la nube usando el token del entorno
conn = duckdb.connect(f'md:aml_proveedores_py?motherduck_token={md_token}')

# Extraer 2025 y 2026
df_2025 = descargar_y_cargar_awards(2025)
df_2026 = descargar_y_cargar_awards(2026)

# Unimos ambos DataFrames
df_total = pd.concat([df_2025, df_2026], ignore_index=True)

# Cargar directamente a la nube
print(f"\nCargando {len(df_total)} registros de montos a la tabla 'raw_awards_csv' en la nube...")

# Asegurar el esquema correcto y subir los datos
conn.execute("CREATE SCHEMA IF NOT EXISTS main")
conn.execute("CREATE OR REPLACE TABLE main.raw_awards_csv AS SELECT * FROM df_total")

print("¡Extracción y carga a MotherDuck completada con éxito!")
conn.close()