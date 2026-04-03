import os
import time
import httpx
import subprocess
from typing import Optional
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect_dbt.cli.commands import DbtCoreOperation

# Cargar variables de entorno
load_dotenv()

AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT = os.getenv("AIRBYTE_PORT", "8000")
AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME", "airbyte")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD", "password")

# Nuevas variables para las dos conexiones
AIRBYTE_CONN_DNCP = os.getenv("AIRBYTE_CONNECTION_ID_DNCP")
AIRBYTE_CONN_SANCTIONS = os.getenv("AIRBYTE_CONNECTION_ID_OPENSANCTIONS")

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", ".")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", ".")

# Tarea de Extracción y Carga (Airbyte parametrizado)
@task(name="Extract and Load (Airbyte)", retries=3, retry_delay_seconds=60)
def extract_and_load(connection_id: str, source_name: str):
    logger = get_run_logger()
    base_url = f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1"
    
    with httpx.Client(timeout=None, auth=(AIRBYTE_USERNAME, AIRBYTE_PASSWORD)) as client:
        logger.info(f"Iniciando sync en Airbyte para {source_name} (ID: {connection_id})")
        
        response = client.post(
            f"{base_url}/connections/sync",
            json={"connectionId": connection_id}
        )
        
        if response.status_code == 409: 
            logger.warning(f"Ya hay un sync en curso para {source_name}, esperando...")
            jobs_resp = client.post(f"{base_url}/jobs/list",
                                    json={"configTypes": ["sync"], "configId": connection_id})
            job_id = jobs_resp.json()["jobs"][0]["job"]["id"]
        else:
            response.raise_for_status()
            job_id = response.json()["job"]["id"]
            
        logger.info(f"Monitoreando Job de {source_name} (ID: {job_id})...")
        
        while True:
            status_resp = client.post(f"{base_url}/jobs/get", json={"id": job_id})
            status = status_resp.json()["job"]["status"]
            
            if status == "succeeded":
                logger.info(f"¡Sincronización de {source_name} completada con éxito!")
                return job_id
            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"El Job de {source_name} falló con estado: {status}")
            
            time.sleep(10)

# Tarea: Extracción de Montos (Python)
@task(name="Extract Montos (Python Custom)", retries=1)
def extract_montos():
    logger = get_run_logger()
    logger.info("Iniciando descarga y extracción de montos de adjudicaciones...")
    
    resultado = subprocess.run(["python", "extraer_montos.py"], capture_output=True, text=True)
    
    if resultado.returncode != 0:
        raise RuntimeError(f"Falló la extracción de montos:\n{resultado.stderr}")
        
    logger.info("¡Extracción de montos completada con éxito!\n" + resultado.stdout)
    return True

# Tarea de Transformación (dbt run)
@task(name="Transform with dbt")
def transform(select: Optional[str] = None):
    commands = ["dbt deps"]
    if select:
        commands.append(f"dbt run --select {select}")
    else:
        commands.append("dbt run")
        
    DbtCoreOperation(
        commands=commands,
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROFILES_DIR)
    ).run()

# Tarea de Calidad (dbt test)
@task(name="Test Data Quality")
def test_data():
    DbtCoreOperation(
        commands=["dbt test"],
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(DBT_PROFILES_DIR)
    ).run()

# Flow principal
@flow(name="Pipeline Integridad Pública")
def aml_pipeline(
    run_extract: bool = True,
    run_transform: bool = True,
    run_tests: bool = True,
    dbt_select: Optional[str] = None
):
    logger = get_run_logger()
    logger.info("Iniciando pipeline End-to-End: DNCP vs OpenSanctions")
    
    if run_extract:
        # Llamamos a la función dos veces, una por cada fuente
        extract_and_load(connection_id=AIRBYTE_CONN_DNCP, source_name="DNCP")
        extract_and_load(connection_id=AIRBYTE_CONN_SANCTIONS, source_name="OpenSanctions")
        
    if run_transform:
        transform(select=dbt_select)
        
    if run_tests:
        test_data()
        
    logger.info("¡Pipeline completado con éxito!")
    return {"status": "success"}

if __name__ == "__main__":
    
    # Comentar esto para programarlo (deploy):
    # aml_pipeline()
    
    # Decomentar esto para programarlo (deploy):
    aml_pipeline.serve(
        name="monitoreo-diario",
        cron="0 4 * * *", # Todos los días a las 2:00 AM
        tags=["proyecto_final", "airbyte", "dbt", "duckdb"]
    )