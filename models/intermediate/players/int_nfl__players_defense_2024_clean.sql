-- Identifier les lignes en doublon via le row number

WITH row_nb_table AS(
    SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY player_pk ORDER BY team) AS row_nb
FROM {{ ref('stg_nfl__players_defense_2024') }}

)

-- Sélectionner uniquement une ligne, la ligne agrégée (team = 2TM) identifiée grâce au row number = 1

SELECT
    *
FROM row_nb_table
WHERE 
    (row_nb = 1) AND (player <> 'League Average')