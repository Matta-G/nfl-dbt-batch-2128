WITH max_table AS (
    SELECT
        player_pk,
        player,
        pos,
        lb_tackles_comb,
        ROW_NUMBER() OVER (PARTITION BY pos_category ORDER BY lb_tackles_comb DESC) AS lb_rank,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY lb_tackles_comb DESC) AS lb_team_rank,
        
        -- Infos générales
        age,
        team,
        games_played,
        games_started,
        win_rate,

        -- Stats LB
        lb_tackles_comb_per_game, -- Tackles combined / Game
        lb_tackles_solo_per_game, -- Solo tackles / Game
        lb_tackles_tfl_per_game, -- Tackles for loss / Game
        lb_press_per_game, -- Pressures / Game
        lb_tackles_succ_rate_pct, -- Tackles success rate percentage : combined / missed tackles
        db_lb_cmp_pct AS lb_cmp_pct, -- Completion Percentage Allowed Targets % (The lower the better)

        
        -- LB MIN
        MIN(lb_tackles_comb_per_game) OVER() AS min_lb_tackles_comb_per_game,
        MIN(lb_tackles_solo_per_game) OVER () AS min_lb_tackles_solo_per_game,
        MIN(lb_tackles_tfl_per_game) OVER () AS min_lb_tackles_tfl_per_game,
        MIN(lb_press_per_game) OVER () AS min_lb_press_per_game,
        MIN(lb_tackles_succ_rate_pct) OVER () AS min_lb_tackles_succ_rate_pct,
        MIN(db_lb_cmp_pct) OVER () AS min_lb_cmp_pct,

        -- LB MAX
        MAX(lb_tackles_comb_per_game) OVER() AS max_lb_tackles_comb_per_game,
        MAX(lb_tackles_solo_per_game) OVER () AS max_lb_tackles_solo_per_game,
        MAX(lb_tackles_tfl_per_game) OVER () AS max_lb_tackles_tfl_per_game,
        MAX(lb_press_per_game) OVER () AS max_lb_press_per_game,
        MAX(lb_tackles_succ_rate_pct) OVER () AS max_lb_tackles_succ_rate_pct,
        MAX(db_lb_cmp_pct) OVER () AS max_lb_cmp_pct,

    FROM {{ ref('int_nfl_players_defense_final') }}
    WHERE (pos IN ('ILB', 'LILB', 'RILB')) AND (games_started >= 3)
    ORDER BY lb_press_per_game DESC
)

SELECT
    player_pk,
    player,
    team,
    pos,
    games_played,
    games_started,
    lb_tackles_succ_rate_pct,
    lb_rank,
    lb_team_rank,
    win_rate,

    CASE
        WHEN player_pk = 'WhitKy00' THEN 'Kyzir White'
        WHEN lb_rank <= 5 THEN 'Top 5 (average)'
        ELSE 'League average' 
    END AS lb_cat,
    CASE
        WHEN player_pk = 'WhitKy00' THEN 'Kyzir White'
        WHEN lb_rank <= 5 THEN 'Top 5 (average)'
        ELSE player 
    END AS lb_scatter_cat,
    -- Infos générales
    age,
    -- LB Stats scaled (Min/Max)
    SAFE_DIVIDE((lb_tackles_comb_per_game - min_lb_tackles_comb_per_game),(max_lb_tackles_comb_per_game - min_lb_tackles_comb_per_game)) AS lb_tackles_comb_per_game_scaled,
    SAFE_DIVIDE((lb_tackles_solo_per_game - min_lb_tackles_solo_per_game),(max_lb_tackles_solo_per_game - min_lb_tackles_solo_per_game)) AS lb_tackles_solo_per_game_scaled,
    SAFE_DIVIDE((lb_tackles_tfl_per_game - min_lb_tackles_tfl_per_game),(max_lb_tackles_tfl_per_game - min_lb_tackles_tfl_per_game)) AS lb_tackles_tfl_per_game_scaled,
    SAFE_DIVIDE((lb_press_per_game - min_lb_press_per_game),(max_lb_press_per_game - min_lb_press_per_game)) AS lb_press_per_game_scaled,
    SAFE_DIVIDE((lb_tackles_succ_rate_pct - min_lb_tackles_succ_rate_pct),(max_lb_tackles_succ_rate_pct - min_lb_tackles_succ_rate_pct)) AS lb_tackles_succ_rate_pct_scaled,
    SAFE_DIVIDE((lb_cmp_pct - min_lb_cmp_pct),(max_lb_cmp_pct - min_lb_cmp_pct)) AS lb_cmp_pct_scaled,
FROM max_table
ORDER BY lb_rank
