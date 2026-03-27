# Pipeline Analítico de Clima y Calidad del Aire (Paraguay)

Este proyecto implementa un pipeline de datos (ELT) completo para extraer, cargar y transformar datos climáticos y de contaminación del aire de las ciudades de Asunción, San Lorenzo y Ciudad del Este. 

El objetivo es consolidar métricas ambientales provenientes de la API de OpenWeatherMap en una única tabla analítica (One Big Table) optimizada para su consumo en herramientas de Business Intelligence.

## Arquitectura y Tecnologías

El flujo de datos sigue una arquitectura moderna de datos:
* **Extracción y Carga (EL):** Airbyte (Conectores REST API personalizados + List Partition Router).
* **Data Warehouse:** MotherDuck / DuckDB (Base de datos columnar en la nube).
* **Transformación (T):** dbt (Data Build Tool) con materialización OBT.

## Estructura del Proyecto y Archivos Clave

El proyecto sigue las mejores prácticas de dbt, separando las transformaciones en tres capas lógicas. A continuación se detallan los directorios y los modelos SQL principales:

```text
tarea_clase_5/
├── dbt_project.yml                         # Configuración principal y metadatos del proyecto dbt
├── README.md                               # Este documento
└── models/
    ├── staging/                            # 1. Capa de limpieza y estandarización (Vistas)
    │   ├── _sources.yml                    # Definición de las tablas crudas origen en MotherDuck
    │   ├── stg_clima.sql                   # Extracción, casteo de JSON y traducción de columnas climáticas
    │   └── stg_contaminacion.sql           # Extracción del Índice de Calidad del Aire (ICA)
    │
    ├── intermediate/                       # 2. Capa de cruce y lógica de negocio (Vistas)
    │   └── int_environmental_metrics.sql   # JOIN de ambas fuentes y traducción de métricas/categorías
    │
    └── marts/                              # 3. Capa analítica final (Tablas Físicas)
        └── obt_environmental_snapshots.sql # One Big Table (OBT) desnormalizada lista para BI
```

## Cómo ejecutar este proyecto localmente

Sigue estos pasos para clonar el repositorio y ejecutar las transformaciones en tu propio entorno.

### Requisitos Previos
* Tener instalado Python 3.8+ o superior.
* Tener una cuenta gratuita en [MotherDuck](https://motherduck.com/) y obtener tu Token de acceso.

### 1. Instalación y Configuración
Navega a la carpeta de la tarea e instala las dependencias necesarias de dbt para DuckDB:

```bash
cd tarea_clase_5
pip install dbt-core dbt-duckdb
```

### 2. Configurar el Perfil (profiles.yml)
Para que dbt pueda conectarse a MotherDuck, configura tu archivo `profiles.yml` (usualmente en `~/.dbt/profiles.yml` en Linux/Mac o `%USERPROFILE%\.dbt\profiles.yml` en Windows) agregando la siguiente configuración. Reemplaza `<TU_TOKEN_DE_MOTHERDUCK>` con tu token real:

```yaml
mi_proyecto_dbt:
  outputs:
    dev:
      type: duckdb
      path: 'md:_share/tu_base_de_datos/tu_token?motherduck_token=<TU_TOKEN_DE_MOTHERDUCK>'
      schema: main
      threads: 4
  target: dev
```

### 3. Ejecutar el pipeline
Verifica la conexión y ejecuta todos los modelos para construir las vistas de staging y materializar la tabla OBT:

```bash
dbt debug
dbt run
```

### 4. Visualizar el Linaje de Datos (DAG)
Para explorar la documentación autogenerada y el grafo de dependencias de los modelos, ejecuta:

```bash
dbt docs generate
dbt docs serve
```

Esto abrirá un servidor local en tu navegador donde podrás inspeccionar visualmente cómo fluyen los datos desde los *sources* hasta el modelo final.

---
**Autor:** Luis Rios - *Maestría en Inteligencia Artificial y Análisis de Datos*
