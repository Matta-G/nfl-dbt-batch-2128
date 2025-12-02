SELECT * FROM {{ ref('int_nfl_games_stats_away') }}
UNION ALL
SELECT * FROM {{ ref('int_nfl_games_stats_home') }}
