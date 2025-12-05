SELECT *
, CAST(CONCAT(season, '-01-01') AS DATE) AS season_date
FROM {{ ref('int_nfl_games_stats_all') }}
WHERE week NOT IN ('Wildcard', 'Division', 'Conference', 'Superbowl')

