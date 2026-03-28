-- test para validar que la velocidad del viento siempre sea 0 o positiva
SELECT
    nombre_ciudad,
    velocidad_viento_kmh
FROM {{ ref('stg_clima') }}
WHERE velocidad_viento_kmh < 0