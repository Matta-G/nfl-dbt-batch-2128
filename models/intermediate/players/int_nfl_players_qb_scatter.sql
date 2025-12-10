SELECT
    qb_cat,
    qb_scatter_cat,
    AVG(pass_and_rush_yds) AS tot_yds,
    AVG(qb_win_rate) AS qb_win_rate

FROM {{ ref('int_nfl_players_qb_scaled') }}
GROUP BY qb_cat, qb_scatter_cat
ORDER BY tot_yds DESC