WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_opensanctions_org') }}
),

limpieza AS (
    SELECT
        id AS sancionado_id,
        schema AS tipo_entidad,
        name AS nombre_original,
        REGEXP_REPLACE(UPPER(name), '[^A-Z0-9]', '', 'g') AS nombre_limpio_cruce,
        countries AS paises_relacionados
    FROM source
    WHERE name IS NOT NULL
)

SELECT * FROM limpieza