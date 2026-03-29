# Pipeline ELT Automatizado: E-commerce Maven Fuzzy Factory

Este repositorio contiene el proyecto resultante de la tarea de la clase 7 para del módulo de **Introducción a la Integración de Datos**. Consiste en un pipeline de datos ELT (Extract, Load, Transform) de extremo a extremo, completamente automatizado, diseñado para procesar y analizar las operaciones transaccionales de un e-commerce.

## Arquitectura y Stack Tecnológico

El proyecto utiliza un stack de datos moderno para garantizar la escalabilidad, modularidad y automatización del flujo de datos:

* **Extracción y Carga (Ingesta):** [Airbyte](https://airbyte.com/) (Conexión de origen MySQL a destino MotherDuck).
* **Data Warehouse (Destino):** [MotherDuck](https://motherduck.com/) (DuckDB en la nube).
* **Transformación y Modelado:** [dbt (Data Build Tool)](https://www.getdbt.com/) con el adaptador `dbt-duckdb`.
* **Orquestación:** [Prefect](https://www.prefect.io/) (Despliegue y ejecución programada).
* **Visualización:** [Metabase](https://www.metabase.com/) (Dashboard interactivo con variables SQL).
* **Lenguaje Base:** Python 3.13.

## Estructura del Proyecto

El proyecto sigue la estructura estándar de dbt, complementada con los scripts de orquestación de Prefect y variables de entorno:

```text
tarea_clase_7/
├── .env                        # Variables de entorno y credenciales (ignorado en git)
├── dbt_project.yml             # Configuración principal del proyecto dbt
├── dev.duckdb                  # Base de datos local DuckDB (generada)
├── ecommerce_pipeline.py       # Script orquestador en Python (Prefect + Airbyte + dbt)
├── dockerfile.metabase         # Configuración para levantar Metabase
├── README.md                   # Documentación del proyecto
├── models/                     # Modelos de transformación SQL
│   ├── staging/                # Capa de limpieza, casteos y estandarización
│   │   ├── _sources.yml        # Definición de las fuentes de datos (Airbyte)
│   │   ├── stg_order_items.sql
│   │   ├── stg_orders.sql
│   │   └── stg_sessions.sql
│   └── marts/                  # Capa de negocio y tablas materializadas finales
│       ├── fct_channel_performance.sql
│       ├── fct_daily_sales.sql
│       └── obt_orders_enriched.sql
├── macros/                     # Macros y funciones personalizadas de dbt
├── tests/                      # Tests de dbt para validación de integridad de datos
├── seeds/                      # Archivos CSV estáticos de dbt
└── analyses/                   # Consultas SQL exploratorias
```

## Requisitos Previos

Para ejecutar este proyecto localmente, necesitas tener instalado y configurado lo siguiente:
1. Python 3.x y entorno virtual (ej. `dbt-env`).
2. Una instancia local de Airbyte corriendo en el puerto 8000.
3. Una instancia local de Metabase.
4. Una cuenta activa en MotherDuck con su respectivo Token.
5. Un servidor local de Prefect.

## Configuración y Ejecución

### 1. Variables de Entorno

Para que el script orquestador pueda comunicarse con las distintas herramientas, es necesario crear un archivo `.env` en la raíz del proyecto. A continuación se detalla el propósito de cada variable requerida:

* **Configuración de Airbyte:**
  * `AIRBYTE_HOST`: La dirección donde se está ejecutando la instancia de Airbyte (generalmente `localhost`).
  * `AIRBYTE_PORT`: El puerto de la API de Airbyte (por defecto es `8000`).
  * `AIRBYTE_CONNECTION_ID`: El identificador único (UUID) de la conexión específica configurada en Airbyte (MySQL a MotherDuck). Lo puedes encontrar en la URL de tu conexión en la interfaz web de Airbyte.
  * `AIRBYTE_USERNAME` y `AIRBYTE_PASSWORD`: Las credenciales de acceso a tu instancia local de Airbyte (si tienes la autenticación activada).

* **Configuración de dbt:**
  * `DBT_PROJECT_DIR`: La ruta al directorio que contiene el archivo `dbt_project.yml`. Si ejecutas el script desde la raíz del proyecto, suele ser `./`.
  * `DBT_PROFILES_DIR`: La ruta al directorio que contiene tu archivo `profiles.yml` con las credenciales de la base de datos. Dependiendo de tu instalación, puede ser la carpeta actual (`./`) o la ruta global de dbt (`~/.dbt/`).

* **Credenciales de Destino y Orquestación:**
  * `MOTHERDUCK_TOKEN`: El token de autenticación generado desde la interfaz web de MotherDuck para permitir que dbt se conecte al Data Warehouse en la nube.
  * `PREFECT_API_URL`: La URL de la API del servidor local de Prefect para registrar las ejecuciones del flujo.

**Ejemplo del archivo `.env`:**

```env
AIRBYTE_HOST=localhost
AIRBYTE_PORT=8000
AIRBYTE_CONNECTION_ID=tu_id_de_conexion_aqui
AIRBYTE_USERNAME=tu_usuario
AIRBYTE_PASSWORD=tu_password
DBT_PROJECT_DIR=./
DBT_PROFILES_DIR=~/.dbt/
MOTHERDUCK_TOKEN=eyJh... (tu token completo aquí)
PREFECT_API_URL=[http://127.0.0.1:4200/api](http://127.0.0.1:4200/api)
```