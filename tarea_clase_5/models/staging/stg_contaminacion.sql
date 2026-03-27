SELECT
    -- Metadatos de carga
    _airbyte_extracted_at AS fecha_captura,
    
    -- Datos de ubicación
    (coord->>'lat')::FLOAT AS latitud,
    (coord->>'lon')::FLOAT AS longitud,
    
    -- Métrica de contaminación (Índice de Calidad del Aire: 1 = Bueno, 5 = Muy Malo)
    (list->0->'main'->>'aqi')::INT AS indice_ica

FROM {{ source('raw', 'raw_air_pollution') }}