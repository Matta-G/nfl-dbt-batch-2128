WITH max_table AS (
    SELECT
        player_pk,
        player,
        pos,
        pass_yrds,
        rec_yrds,
        rush_yrds,
        pass_and_rush_yds,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY pass_and_rush_yds DESC) AS qb_rank,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY pass_and_rush_yds DESC) AS qb_team_rank,
        
        -- Infos générales
        age,
        team,
        games_played,
        games_started,
        qb_win_rate,

        -- Stats QB
        qb_comp_perc,
        ROUND(qb_td_int_ratio,2) AS qb_td_int_ratio,
        qb_avg_yrds_per_attmpt,
        qb_yrds_per_game,
        qb_qb_rating,
        qb_sack_perc,
        
        -- QB MIN
        MIN(qb_comp_perc) OVER() AS min_qb_comp_perc,
        MIN(qb_td_int_ratio) OVER () AS min_qb_td_int_ratio,
        MIN(qb_avg_yrds_per_attmpt) OVER () AS min_qb_avg_yrds_per_attmpt,
        MIN(qb_yrds_per_game) OVER () AS min_qb_yrds_per_game,
        MIN(qb_qb_rating) OVER () AS min_qb_qb_rating,
        MIN(qb_sack_perc) OVER () AS min_qb_sack_perc,

        -- QB MAX
        MAX(qb_comp_perc) OVER() AS max_qb_comp_perc,
        MAX(qb_td_int_ratio) OVER () AS max_qb_td_int_ratio,
        MAX(qb_avg_yrds_per_attmpt) OVER () AS max_qb_avg_yrds_per_attmpt,
        MAX(qb_yrds_per_game) OVER () AS max_qb_yrds_per_game,
        MAX(qb_qb_rating) OVER () AS max_qb_qb_rating,
        MAX(qb_sack_perc) OVER () AS max_qb_sack_perc,

    FROM {{ ref('int_nfl_players_offense_final') }} 
    WHERE (pos = 'QB') AND (games_started >= 3)
)

SELECT
    player_pk,
    player,
    team,
    pos,
    games_played,
    games_started,
    pass_yrds,
    rush_yrds,
    qb_rank,
    qb_team_rank,
    qb_win_rate,
    pass_and_rush_yds,

    CASE
        WHEN team = 'Cardinals' THEN 'Kyler Murray'
        WHEN qb_rank <= 5 THEN 'Top 5 quarterback'
        ELSE 'Average quarterback' 
    END AS qb_cat,
    CASE
        WHEN team = 'Cardinals' THEN 'Kyler Murray'
        WHEN qb_rank <= 5 THEN 'Top 5 quarterback'
        ELSE player 
    END AS qb_scatter_cat,
    -- Infos générales
    age,
    
    SAFE_DIVIDE((qb_comp_perc - min_qb_comp_perc),(max_qb_comp_perc - min_qb_comp_perc)) AS qb_comp_perc_scaled,
    SAFE_DIVIDE((qb_td_int_ratio - min_qb_td_int_ratio),(max_qb_td_int_ratio - min_qb_td_int_ratio)) AS qb_td_int_ratio_scaled,
    SAFE_DIVIDE((qb_avg_yrds_per_attmpt - min_qb_avg_yrds_per_attmpt),(max_qb_avg_yrds_per_attmpt - min_qb_avg_yrds_per_attmpt)) AS qb_avg_yrds_per_attmpt_scaled,
    SAFE_DIVIDE((qb_yrds_per_game - min_qb_yrds_per_game),(max_qb_yrds_per_game - min_qb_yrds_per_game)) AS qb_yrds_per_game_scaled,
    SAFE_DIVIDE((qb_qb_rating - min_qb_qb_rating),(max_qb_qb_rating - min_qb_qb_rating)) AS qb_qb_rating_scaled,
    1 - SAFE_DIVIDE((qb_sack_perc - min_qb_sack_perc),(max_qb_sack_perc - min_qb_sack_perc)) AS qb_sack_avoid_perc_scaled,
    
FROM max_table
ORDER BY qb_rank
