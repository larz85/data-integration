WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_awards_csv') }}
)

SELECT 
	"compiledRelease/awards/0/id" as adjudicacion_id,
    "compiledRelease/awards/0/date" as fecha_adjudicacion,
    -- Forzamos para que todas las licitaciones en guaraníes pasen a dólares
    -- Las cotizaciones podríamos buscarlas de otro datasource pero quedaría parA la segunda versión
	COALESCE("compiledRelease/awards/0/value/amount" / CASE WHEN "compiledRelease/awards/0/value/currency" = 'PYG' THEN 6500 ELSE 1 END, 0) as monto_adjudicacion
FROM source