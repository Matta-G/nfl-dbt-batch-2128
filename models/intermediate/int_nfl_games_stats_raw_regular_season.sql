SELECT *
    , CAST(CONCAT(season, '-01-01') AS DATE) AS season_date
FROM {{ ref('stg_nfl_dbt__raw_nfl_games_stats') }}
WHERE week NOT IN ('Wildcard', 'Division', 'Conference', 'Superbowl')
