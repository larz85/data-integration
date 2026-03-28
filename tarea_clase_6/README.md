# Pipeline Analítico Ambiental - Calidad de Datos y Testing (Clase 6)

Este proyecto extiende el pipeline ELT desarrollado en la Tarea 5, implementando una capa robusta de **Calidad de Datos, Testing y Documentación** utilizando dbt. 

El objetivo principal es garantizar que los datos climáticos y de contaminación del aire de Paraguay sean confiables, consistentes y estén listos para su consumo en dashboards de Business Intelligence, evitando el efecto *"Garbage In, Garbage Out"*.

## Estructura del Proyecto y Archivos de Testing

Se añadieron nuevos directorios y archivos de configuración para soportar la capa de pruebas:

```text
tarea_clase_6/
├── dbt_project.yml                 # Configuración principal
├── packages.yml                    # Dependencias de paquetes (dbt-expectations)
├── README.md                       # Este documento
├── models/
│   ├── staging/_stg_models.yml     # Documentación y tests (Generic + Expectations) de staging
│   └── marts/_marts_models.yml     # Documentación y tests (Generic) de la One Big Table
│
└── tests/                          # Singular Tests (Reglas de negocio personalizadas)
    ├── assert_contaminacion_consistente.sql # Valida que el ICA numérico coincida con su categoría de texto
    └── assert_viento_positivo.sql           # Audita que no existan velocidades de viento negativas
```

## Cómo ejecutar este proyecto localmente

Sigue estos pasos para reproducir las transformaciones y ejecutar la suite de pruebas automatizadas.

### 1. Instalación de Dependencias
Asegúrate de estar en la carpeta del proyecto e instala el paquete `dbt-expectations` definido en el `packages.yml`:
```bash
dbt deps
```

### 2. Ejecución y Testing (dbt build)
El comando `build` es la mejor práctica, ya que ejecuta los modelos respetando su linaje y corre los tests inmediatamente después de materializar cada capa. Si un test crítico falla, el pipeline se detiene.
```bash
dbt build
```
*(Nota: Algunos tests de anomalías históricas pueden estar configurados con `severity: warn` para alertar sin romper el pipeline).*

### 3. Exploración de la Documentación (Catálogo de Datos)
Para visualizar las descripciones de las columnas, los tests aplicados a cada campo y el DAG (Grafo Acíclico Dirigido), ejecuta:
```bash
dbt docs generate
dbt docs serve
```
Esto abrirá un servidor local en tu navegador con el catálogo interactivo de datos.

---
**Autor:** Luis Rios - *Maestría en Inteligencia Artificial y Análisis de Datos*