# Integración de Datos - Repositorio de Tareas

Bienvenido a mi repositorio de entregables prácticos para la materia de **Integración de Datos**, correspondiente a la Maestría en Inteligencia Artificial y Análisis de Datos.

Este repositorio está estructurado de manera que cada carpeta representa una tarea o asignación específica desarrollada a lo largo de las clases del módulo.

## Índice de Tareas

Para revisar el detalle, las instrucciones de ejecución y el código de cada entrega, ingresa a la carpeta correspondiente:

* **[tarea_clase_5](./tarea_clase_5/) - Pipeline ELT de Datos Ambientales**
  Implementación de un flujo de datos completo extrayendo métricas de la API de OpenWeatherMap. Utiliza **Airbyte** para la extracción, **MotherDuck (DuckDB)** como Data Warehouse y **dbt** para la transformación y modelado final en una One Big Table (OBT).

* **[tarea_clase_6](./tarea_clase_6/) - Calidad de Datos, Testing y Documentación**
  Extensión del pipeline de la clase 5 agregando una capa robusta de **Data Quality**. Implementación de tests genéricos, tests estadísticos avanzados con el paquete `dbt-expectations`, validaciones singulares de reglas de negocio en SQL y generación automática del catálogo de datos y el linaje (DAG).

* **[`/tarea_clase_7`](./tarea_clase_7) - Pipeline ELT Automatizado:** Consiste en un pipeline de extremo a extremo para el e-commerce *Maven Fuzzy Factory*. Implementa ingesta con **Airbyte**, almacenamiento en la nube con **MotherDuck (DuckDB)**, transformación con **dbt**, orquestación automatizada en Python con **Prefect**, y un panel interactivo de BI en **Metabase**.

* **[proyecto_final](./proyecto_final/) - Proyecto Final: Monitoreo de Integridad Pública (AML)**
  Trabajo final de la materia. Un *Modern Data Stack* automatizado para la detección de riesgos de Lavado de Activos en el sector público. Cruza datos de contrataciones (DNCP) con listas de sanciones internacionales (OpenSanctions) utilizando **Airbyte**, scripts customizados en **Python**, **dbt**, **MotherDuck**, orquestación con **Prefect** y visualización de riesgos en **Metabase**.

*(Las próximas tareas del curso se irán agregando en sus respectivos directorios conforme avance la materia).*

## Stack Tecnológico del Curso

Durante el desarrollo de estas tareas, aplicamos las siguientes herramientas del Modern Data Stack:
* **Extracción y Carga (EL):** Airbyte
* **Almacenamiento (Data Warehouse):** MotherDuck / DuckDB
* **Transformación y Modelado (T):** dbt (Data Build Tool), SQL
* **Calidad de Datos (DQ):** dbt tests, dbt-expectations
* **Orquestación de Flujos:** Prefect, Python
* **Visualización (BI):** Metabase
* **Control de Versiones:** Git

---
**Autor:** Luis Ríos (Proyecto Final en coautoría con Víctor Mendoza)