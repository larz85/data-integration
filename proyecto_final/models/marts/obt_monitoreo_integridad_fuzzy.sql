{{ config(enabled=false) }}

WITH dncp AS (
    SELECT * FROM {{ ref('stg_dncp_adjudicaciones') }}
),

sancionados AS (
    SELECT * FROM {{ ref('stg_opensanctions') }}
),

adjudicaciones AS (
    SELECT * FROM {{ ref('stg_dncp_adjudicaciones_detalles') }}
),

-- Datos de adjudicaciones con sus detalles para traer los montos
base_dncp AS (
    SELECT 
        -- Datos del Contrato Público (DNCP)
        d.ocid,
        d.licitacion_id,
        d.licitacion_titulo,
        d.categoria,
        d.estado,
        d.comprador,
        d.fecha_registro,
        d.proveedor_ruc,
        d.proveedor_nombre,
        -- Datos de detalles de adjudicaciones
        a.adjudicacion_id,
        a.fecha_adjudicacion,
        a.monto_adjudicacion,
    FROM dncp AS d
    LEFT JOIN adjudicaciones AS a ON d.adjudicacion_id = a.adjudicacion_id
),

-- Cruce por nombre exacto de proveedor
cruce_exacto AS (
    SELECT 
        b.*,
        -- Datos de la Sanción Internacional
        s.sancionado_id,
        s.tipo_entidad,
        s.paises_relacionados,
        TRUE AS es_alerta_sancion,
        'Cruce Exacto' AS metodo_cruce,
        1.0 AS score_similitud
    FROM base_dncp b
    INNER JOIN sancionados s ON b.proveedor_nombre_cruce = s.nombre_limpio_cruce
),

-- Proveedores que no coincidieron
no_cruzados AS (
    SELECT b.*
    FROM base_dncp b
    LEFT JOIN cruce_exacto c ON b.adjudicacion_id = c.adjudicacion_id
    WHERE c.adjudicacion_id IS NULL
),

-- Cruce por nombre de proveedor fuzzy (Algoritmo Jaro-Winkler)
cruce_fuzzy AS (
    SELECT 
        n.*,
        -- Datos de la Sanción Internacional
        s.sancionado_id,
        s.tipo_entidad,
        s.paises_relacionados,
        TRUE AS es_alerta_sancion,
        'Fuzzy Match' AS metodo_cruce,
        -- Calcular el porcentaje de similitud
        jaro_winkler_similarity(n.proveedor_nombre_cruce, s.nombre_limpio_cruce) AS score_similitud
    FROM no_cruzados n
    -- Umbral del 90% de similitud para evitar falsos positivos
    INNER JOIN sancionados s 
        ON jaro_winkler_similarity(n.proveedor_nombre_cruce, s.nombre_limpio_cruce) >= 0.90
    -- Si coincide con varios, elegir solo la similitud más alta
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY n.adjudicacion_id 
        ORDER BY jaro_winkler_similarity(n.proveedor_nombre_cruce, s.nombre_limpio_cruce) DESC
    ) = 1
),

-- Proveedores sin alertas
limpios AS (
    SELECT 
        n.*,
        CAST(NULL AS VARCHAR) AS sancionado_id, 
        CAST(NULL AS VARCHAR) AS tipo_entidad, 
        CAST(NULL AS VARCHAR) AS paises_relacionados,
        FALSE AS es_alerta_sancion,
        'Sin Alerta' AS metodo_cruce,
        0.0 AS score_similitud
    FROM no_cruzados n
    LEFT JOIN cruce_fuzzy f ON n.adjudicacion_id = f.adjudicacion_id
    WHERE f.adjudicacion_id IS NULL
),

-- Consolidación de todos los casos
obt_final AS (
    SELECT * FROM cruce_exacto
    UNION ALL
    SELECT * FROM cruce_fuzzy
    UNION ALL
    SELECT * FROM limpios
)

SELECT * FROM obt_final