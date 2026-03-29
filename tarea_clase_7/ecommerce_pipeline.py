import os
import time
import httpx
from typing import Optional
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect_dbt.cli.commands import DbtCoreOperation


# 1. Cargar variables de entorno del archivo .env
load_dotenv()

AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT = os.getenv("AIRBYTE_PORT", "8000")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME", "airbyte")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD", "password")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", ".")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", ".")

# 2. Tarea de Extracción y Carga (Airbyte)
@task(name="Extract and Load", retries=2, retry_delay_seconds=60)
def extract_and_load():
    logger = get_run_logger()
    base_url = f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1"
    
    with httpx.Client(timeout=30, auth=(AIRBYTE_USERNAME, AIRBYTE_PASSWORD)) as client:
        logger.info(f"Iniciando sync en Airbyte para la conexión: {AIRBYTE_CONNECTION_ID}")
        
        # Disparar la sincronización
        response = client.post(
            f"{base_url}/connections/sync",
            json={"connectionId": AIRBYTE_CONNECTION_ID}
        )
        
        # LÓGICA DE REINTENTOS Y JOBS EN CURSO (Status 409)
        if response.status_code == 409: # Sync ya en curso
            logger.warning("Ya hay un sync en curso, esperando a que termine...")
            jobs_resp = client.post(f"{base_url}/jobs/list",
                                    json={"configTypes": ["sync"], "configId": AIRBYTE_CONNECTION_ID})
            # Obtenemos el ID del trabajo que ya estaba corriendo
            job_id = jobs_resp.json()["jobs"][0]["job"]["id"]
        else:
            response.raise_for_status()
            # Obtenemos el ID del nuevo trabajo que acabamos de crear
            job_id = response.json()["job"]["id"]
            
        logger.info(f"Monitoreando Job de Airbyte (ID: {job_id})...")
        
        # Polling: Preguntar cada 10 segundos si ya terminó
        while True:
            status_resp = client.post(f"{base_url}/jobs/get", json={"id": job_id})
            status = status_resp.json()["job"]["status"]
            
            if status == "succeeded":
                logger.info("¡Sincronización de Airbyte completada con éxito!")
                return job_id
            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"El Job de Airbyte falló con estado: {status}")
            
            time.sleep(10)

# 3. Tarea de Transformación (dbt)
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

# 4. El Flow principal que une las tareas
@flow(name="Ecommerce ELT Pipeline")
def ecommerce_pipeline(
    run_extract: bool = True,
    run_transform: bool = True,
    run_tests: bool = True,
    run_docs: bool = False,
    dbt_select: Optional[str] = None
):
    logger = get_run_logger()
    logger.info("Iniciando pipeline ELT de Maven Fuzzy Factory")
    
    if run_extract:
        extract_and_load()
        
    if run_transform:
        transform(select=dbt_select)
        
    # Nota: El profesor llama a estas funciones, si no las tienes definidas, 
    # puedes comentarlas para que Python no te tire error de "not defined".
    # if run_tests:
    #     test_data(select=dbt_select)
    # if run_docs:
    #     generate_docs()
        
    logger.info("Pipeline completado!")
    return {"status": "success"}

if __name__ == "__main__":
    ecommerce_pipeline.serve(
        name="ecommerce-daily",             # Nombre de tu automatización
        cron="0 6 * * *",                   # Expresión cron: Todos los días a las 8:00 AM
        parameters={
            "run_extract": True,
            "run_transform": True,
            "run_tests": True,
            "run_docs": False
        },
        tags=["tarea_7", "airbyte", "dbt", "duckdb"],
        description="Pipeline automatizado para extraer datos con Airbyte y transformar con dbt."
    )