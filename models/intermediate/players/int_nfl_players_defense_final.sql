SELECT
    player_pk,

    -- General Information
    player,
    team,
    pos_category,
    pos,
    age,
    games_played,
    games_started,
    win_loose_ratio AS win_rate,
    lb_tackles_comb,
    ROW_NUMBER() OVER (PARTITION BY pos_category ORDER BY lb_tackles_comb DESC) AS tackle_rank,

    CASE
        WHEN age >= 20
            AND age <=24 THEN '20-24'
        WHEN age >= 25
            AND age <=29 THEN '25-29'
        ELSE '30-40'
    END as age_category,

    -- Defensive Backs
    ROUND(SAFE_DIVIDE(db_int,games_played),2) AS db_int_per_game, -- Interceptions / Game
    ROUND(SAFE_DIVIDE(db_pass_defended,games_played),2) AS df_pd_per_game, -- Passes Defended / Game
    ROUND(SAFE_DIVIDE(db_target,games_played),2) AS dbt_tgt_per_game, -- Times Targeted as a Defender / Game
    ROUND(SAFE_DIVIDE(db_completed_pass,games_played),2) AS db_cmp_per_game, --Completed passes when the WR of the defender is targeted / Game
    db_lb_cmp_pct, -- Completion Percentage Allowed Targets % (The lower the better)
    ROUND(SAFE_DIVIDE(db_yrds_aftr_ctch,games_played),2) AS db_yrds_aftr_ctch_per_game, -- -- Yards after catch on completion / Game (The lower the better)
    
    -- Linebackers
    ROUND(SAFE_DIVIDE(lb_tackles_comb,games_played),2) AS lb_tackles_comb_per_game, -- Tackles combined / Game
    ROUND(SAFE_DIVIDE(lb_tackles_solo,games_played),2) AS lb_tackles_solo_per_game, -- Solo tackles / Game
    ROUND(SAFE_DIVIDE(lb_tackles_tfl,games_played),2) AS lb_tackles_tfl_per_game, -- Tackles for loss / Game
    ROUND(SAFE_DIVIDE(lb_press,games_played),2) AS lb_press_per_game, -- Pressures / Game
    lb_tackles_succ_rate_pct, -- Tackles success rate percentage : combined / missed tackles

FROM {{ ref('int_nfl__players_defense_2024_clean') }}
LEFT JOIN {{ ref('stg_nfl__2024_standings') }}
USING (team)
WHERE pos_category <> 'Other'
ORDER BY lb_tackles_comb DESC