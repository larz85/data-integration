SELECT
    -- Metadatos de carga
    _airbyte_extracted_at AS fecha_captura,
    
    -- Datos de ubicación
    name AS nombre_ciudad,
    sys->>'country' AS codigo_pais,
    (coord->>'lat')::FLOAT AS latitud,
    (coord->>'lon')::FLOAT AS longitud,
    
    -- Condiciones climáticas
    weather->0->>'main' AS condicion_principal,
    weather->0->>'description' AS descripcion_clima,
    
    -- Métricas (casteadas y convertidas)
    (main->>'temp')::FLOAT - 273.15 AS temperatura_celsius,
    (main->>'feels_like')::FLOAT - 273.15 AS sensacion_termica_celsius,
    (main->>'humidity')::INT AS porcentaje_humedad,
    (main->>'pressure')::INT AS presion_hpa,
    (wind->>'speed')::FLOAT * 3.6 AS velocidad_viento_kmh
FROM {{ source('raw', 'raw_openweather_gratis') }}