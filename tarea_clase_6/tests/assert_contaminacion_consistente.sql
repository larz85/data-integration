-- test para validar la lógica de negocio de la calidad del aire
SELECT
    nombre_ciudad,
    indice_ica,
    categoria_calidad_aire
FROM {{ ref('obt_environmental_snapshots') }}
WHERE 
    (indice_ica = 1 AND categoria_calidad_aire != 'Bueno')
    OR (indice_ica = 2 AND categoria_calidad_aire != 'Justo')
    OR (indice_ica = 3 AND categoria_calidad_aire != 'Moderado')
    OR (indice_ica = 4 AND categoria_calidad_aire != 'Malo')
    OR (indice_ica = 5 AND categoria_calidad_aire != 'Muy Malo')
    OR (indice_ica NOT IN (1, 2, 3, 4, 5) AND categoria_calidad_aire != 'Desconocido')