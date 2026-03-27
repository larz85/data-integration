WITH clima AS (
    SELECT * FROM {{ ref('stg_clima') }}
),

contaminacion AS (
    SELECT * FROM {{ ref('stg_contaminacion') }}
),

unido AS (
    SELECT 
        c.fecha_captura,
        c.nombre_ciudad,
        c.codigo_pais,
        c.latitud,
        c.longitud,
        c.condicion_principal,
        c.descripcion_clima,
        c.temperatura_celsius,
        c.sensacion_termica_celsius,
        c.porcentaje_humedad,
        c.presion_hpa,
        c.velocidad_viento_kmh,
        p.indice_ica,
        
        -- Lógica de negocio 1: Traducción de la condición climática principal
        CASE c.condicion_principal
            WHEN 'Clear' THEN 'Despejado'
            WHEN 'Clouds' THEN 'Nublado'
            WHEN 'Rain' THEN 'Lluvia'
            WHEN 'Snow' THEN 'Nieve'
            WHEN 'Thunderstorm' THEN 'Tormenta'
            WHEN 'Drizzle' THEN 'Llovizna'
            ELSE c.condicion_principal
        END AS condicion_espanol,

        -- Lógica de negocio 2: Categorización del Índice de Calidad del Aire (ICA)
        CASE p.indice_ica
            WHEN 1 THEN 'Bueno'
            WHEN 2 THEN 'Justo'
            WHEN 3 THEN 'Moderado'
            WHEN 4 THEN 'Malo'
            WHEN 5 THEN 'Muy Malo'
            ELSE 'Desconocido'
        END AS categoria_calidad_aire

    FROM clima c
    -- Hacemos el cruce por coordenadas y por el día en que se extrajeron los datos
    LEFT JOIN contaminacion p 
        ON c.latitud = p.latitud 
        AND c.longitud = p.longitud
        AND CAST(c.fecha_captura AS DATE) = CAST(p.fecha_captura AS DATE)
)

SELECT * FROM unido