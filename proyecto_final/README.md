# Monitoreo de Integridad Pública (Pipeline End-to-End)

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-1.5+-FF694B.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-MotherDuck-yellow.svg)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-blueviolet.svg)
![Airbyte](https://img.shields.io/badge/Airbyte-EL-000000.svg)
![Metabase](https://img.shields.io/badge/Metabase-BI-509EE3.svg)

## Descripción del Proyecto
Este proyecto implementa un *Modern Data Stack* automatizado para la detección de riesgos de Lavado de Activos (AML) en el sector público de Paraguay. El pipeline extrae, limpia y cruza datos de contrataciones públicas (DNCP) contra listas de sanciones internacionales (OpenSanctions), consolidando la información en un Data Warehouse en la nube para su posterior análisis en tableros de Business Intelligence.

## Arquitectura (Paradigma ELT)

1. **Extracción y Carga (EL):** - **Airbyte:** Ingesta de datos vía API REST (Procesos DNCP y OpenSanctions).
   - **Python Custom (`extraer_montos.py`):** Extracción *in-memory* de archivos `.zip` pesados (adjudicaciones anuales) para evitar cuellos de botella de I/O.
2. **Almacenamiento:** **MotherDuck** (DuckDB Cloud) como Data Warehouse centralizado.
3. **Transformación (T):** **dbt** (Data Build Tool) para normalizar JSONs anidados, limpiar strings y construir un modelo dimensional basado en una *One Big Table* (OBT).
4. **Calidad de Datos:** Uso de `dbt-expectations` para asegurar integridad referencial y validación mediante expresiones regulares (Regex).
5. **Orquestación:** **Prefect** controla el linaje y dependencias de las tareas con programación CRON diaria en la zona horaria `America/Asuncion`.
6. **Visualización:** **Metabase** conectado a MotherDuck para análisis de riesgos e impacto económico.

## Estructura del Repositorio

```text
proyecto_final/
├── models/
│   ├── staging/        # Modelos de limpieza y desanidado (JSON unnest)
│   └── marts/          # Modelo dimensional final (obt_monitoreo_integridad)
├── tests/              # Pruebas de calidad de datos configuradas en dbt
├── extraer_montos.py   # Script de extracción bulk para archivos ZIP
├── orquestador.py      # Flujo principal de Prefect (DAG)
├── dbt_project.yml     # Configuración principal de dbt
├── packages.yml        # Dependencias de dbt (ej. dbt-expectations)
├── .env.example        # Plantilla de variables de entorno
└── README.md           # Documentación del proyecto
```

## Instalación y Ejecución Local

### 1. Prerrequisitos
- Python 3.9+
- Docker (Para correr Airbyte y Metabase localmente si no se usan versiones Cloud)
- Cuenta en [MotherDuck](https://motherduck.com/) con un token de acceso válido.

### 2. Configuración del Entorno
Clona el repositorio y crea un entorno virtual:
```bash
git clone <tu-url-del-repo>
cd proyecto_final
python -m venv dbt-env
source dbt-env/bin/activate  # En Windows: dbt-env\Scripts\activate
```

### 3. Variables de Entorno
Copia el archivo de ejemplo y configura tus credenciales:
```bash
cp .env.example .env
```
Asegúrate de completar el `MOTHERDUCK_TOKEN` en el archivo `.env`.

### 4. Inicializar dbt
Instala las dependencias de dbt (paquetes):
```bash
dbt deps
```

### 5. Ejecutar el Pipeline
Para correr el pipeline completo de forma manual (Extracción -> Carga -> Transformación -> Tests):
```bash
python orquestador.py
```
*Nota: El script está configurado para programar despliegues automatizados (Deployments) en Prefect Cloud si se utiliza `aml_pipeline.serve()`.*

## Business Intelligence
El resultado final del pipeline es consumido por Metabase. Los tableros generados permiten:
- Cuantificar el monto total adjudicado a empresas con alertas de riesgo.
- Identificar el "Top 5" de instituciones estatales con mayor nivel de exposición a listas de sanciones internacionales.
- Monitorear la evolución mensual de licitaciones y categorías de compra.