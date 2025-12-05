WITH sub1 AS (
SELECT  
season,
team,
    COUNT(CASE WHEN status = 'winner' THEN 1 END) AS nb_winner,
    COUNT(CASE WHEN status = 'loser'  THEN 1 END) AS nb_loser,
    COUNT(CASE WHEN status = 'draw'   THEN 1 END) AS nb_draw,
    -- ratio de winner sur total des matchs
    ROUND(SAFE_DIVIDE(
            COUNT(CASE WHEN status = 'winner' THEN 1 END),
            COUNT(*)   -- total de matchs de l'équipe
        ),2) AS win_ratio

FROM {{ ref('int_nfl_games_stats_all_regular_season') }}
GROUP BY season, team), 

ranked AS (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY season
        ORDER BY win_ratio DESC
    ) AS rank_winner
    FROM sub1
)

SELECT 
season, 
team, 
nb_winner,
nb_loser, 
nb_draw,
win_ratio, 
rank_winner,
FROM ranked
ORDER BY season, nb_winner DESC