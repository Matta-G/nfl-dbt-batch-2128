WITH temp AS (
    SELECT
        qb_cat,
        pos,
        games_started,
        games_played,
        qb_comp_perc_scaled,
        qb_td_int_ratio_scaled,
        qb_avg_yrds_per_attmpt_scaled,
        qb_yrds_per_game_scaled,
        qb_qb_rating_scaled,
        qb_sack_avoid_perc_scaled,
        

    FROM {{ ref('int_nfl_players_qb_scaled') }}
    )

SELECT
    qb_cat,
    pos,
    ROUND(AVG(qb_comp_perc_scaled),2) AS comp_perc_scaled,
    ROUND(AVG(qb_td_int_ratio_scaled),2) AS td_int_ratio_scaled,
    ROUND(AVG(qb_avg_yrds_per_attmpt_scaled),2) AS avg_yrds_per_attmpt_scaled,
    ROUND(AVG(qb_yrds_per_game_scaled),2) AS yrds_per_game_scaled,
    ROUND(AVG(qb_qb_rating_scaled),2) AS qb_rating_scaled,
    ROUND(AVG(qb_sack_avoid_perc_scaled),2) AS qb_sack_avoid_perc_scaled,

FROM temp
GROUP BY qb_cat, pos
HAVING pos = 'QB'