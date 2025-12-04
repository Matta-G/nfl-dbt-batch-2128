SELECT
    player_pk,

    -- General Information
    player,
    team,
    pos_category,
    pos,

    -- Defensive Backs
    ROUND(SAFE_DIVIDE(db_int,g),2) AS db_int_per_game, -- Interceptions / Game
    ROUND(SAFE_DIVIDE(db_pass_defended,g),2) AS df_pd_per_game, -- Passes Defended / Game
    ROUND(SAFE_DIVIDE(db_target,g),2) AS dbt_tgt_per_game, -- Times Targeted as a Defender / Game
    ROUND(SAFE_DIVIDE(db_completed_pass,g),2) AS db_cmp_per_game, --Completed passes when the WR of the defender is targeted / Game
    db_lb_cmp_pct, -- Completion Percentage Allowed Targets % (The lower the better)
    ROUND(SAFE_DIVIDE(db_yrds_aftr_ctch,g),2) AS db_yrds_aftr_ctch_per_game, -- -- Yards after catch on completion / Game (The lower the better)
    
    -- Linebackers
    ROUND(SAFE_DIVIDE(lb_tackles_comb,g),2) AS lb_tackles_comb_per_game, -- Tackles combined / Game
    ROUND(SAFE_DIVIDE(lb_tackles_solo,g),2) AS lb_tackles_solo_per_game, -- Solo tackles / Game
    ROUND(SAFE_DIVIDE(lb_tackles_tfl,g),2) AS lb_tackles_tfl_per_game, -- Tackles for loss / Game
    ROUND(SAFE_DIVIDE(lb_press,g),2) AS lb_press_per_game, -- Pressures / Game
    lb_tackles_succ_rate_pct, -- Tackles success rate percentage : combined / missed tackles

FROM {{ ref('int_nfl__players_defense_2024_clean') }}