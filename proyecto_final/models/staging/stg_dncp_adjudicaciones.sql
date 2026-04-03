WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_procesos_dncp') }}
),

unnest_records AS (
    SELECT 
        UNNEST(CAST(records AS JSON[])) AS record_json
    FROM source
    WHERE records IS NOT NULL
),

-- La api en ocaciones devuelve datos vacios en todos estos campos
licitaciones AS (
    SELECT
        COALESCE(
            json_extract_string(record_json, '$.compiledRelease.ocid'), 
            json_extract_string(record_json, '$.ocid'),
            'OCID No Provisto'
        ) AS ocid,
        COALESCE(json_extract_string(record_json, '$.compiledRelease.tender.id'), 'Sin ID') AS licitacion_id,
        COALESCE(json_extract_string(record_json, '$.compiledRelease.tender.title'), 'Título no detallado en el resumen de la API') AS licitacion_titulo,
        COALESCE(json_extract_string(record_json, '$.compiledRelease.tender.mainProcurementCategoryDetails'), 'Categoría No Especificada') AS categoria,
        COALESCE(json_extract_string(record_json, '$.compiledRelease.tender.statusDetails'), 'Estado Desconocido') AS estado,
        COALESCE(json_extract_string(record_json, '$.compiledRelease.buyer.name'), 'Institución No Detallada') AS comprador,      
        CAST(json_extract_string(record_json, '$.compiledRelease.date') AS TIMESTAMP) AS fecha_registro,
        json_extract(record_json, '$.compiledRelease.awards') AS awards_array
    FROM unnest_records
),

unnest_awards AS (
    SELECT
        ocid, licitacion_id, licitacion_titulo, categoria, estado, comprador, fecha_registro,
        UNNEST(CAST(awards_array AS JSON[])) AS award_json
    FROM licitaciones
    WHERE awards_array IS NOT NULL 
      AND CAST(awards_array AS VARCHAR) != 'null'
),

unnest_suppliers AS (
    SELECT
        ocid, licitacion_id, licitacion_titulo, categoria, estado, comprador, fecha_registro,
        json_extract_string(award_json, '$.id') AS adjudicacion_id,
        UNNEST(CAST(json_extract(award_json, '$.suppliers') AS JSON[])) AS supplier_json
    FROM unnest_awards
    WHERE json_extract(award_json, '$.suppliers') IS NOT NULL 
      AND CAST(json_extract(award_json, '$.suppliers') AS VARCHAR) != 'null'
)

SELECT
    ocid,
    licitacion_id,
    licitacion_titulo,
    categoria,
    estado,
    comprador,
    fecha_registro,
    adjudicacion_id,
    json_extract_string(supplier_json, '$.id') AS proveedor_ruc,
    json_extract_string(supplier_json, '$.name') AS proveedor_nombre,
    REGEXP_REPLACE(UPPER(json_extract_string(supplier_json, '$.name')), '[^A-Z0-9]', '', 'g') AS proveedor_nombre_cruce
FROM unnest_suppliers