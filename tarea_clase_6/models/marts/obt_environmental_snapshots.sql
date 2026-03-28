{{
    config(
        materialized='table'
    )
}}


SELECT 
    fecha_captura,
    nombre_ciudad,
    codigo_pais,
    latitud,
    longitud,
    condicion_espanol,
    descripcion_clima,
    temperatura_celsius,
    sensacion_termica_celsius,
    porcentaje_humedad,
    presion_hpa,
    velocidad_viento_kmh,
    indice_ica,
    categoria_calidad_aire
FROM {{ ref('int_environmental_metrics') }}