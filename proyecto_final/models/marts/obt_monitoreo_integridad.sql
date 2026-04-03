WITH dncp AS (
    SELECT * FROM {{ ref('stg_dncp_adjudicaciones') }}
),

sancionados AS (
    SELECT * FROM {{ ref('stg_opensanctions') }}
),

adjudicaciones AS (
    SELECT * FROM {{ ref('stg_dncp_adjudicaciones_detalles') }}
),

cruce_obt AS (
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
        -- Datos de la Sanción Internacional
        s.sancionado_id,
        s.tipo_entidad,
        s.paises_relacionados,
        -- LA BANDERA DE ALERTA
        CASE 
            WHEN s.sancionado_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS es_alerta_sancion
    FROM dncp AS d
    LEFT JOIN adjudicaciones AS a
        ON d.adjudicacion_id = a.adjudicacion_id
    LEFT JOIN sancionados AS s
        ON d.proveedor_nombre_cruce = s.nombre_limpio_cruce
)

SELECT * FROM cruce_obt