WITH temp AS (
    SELECT
        -- Info générales
        player_pk,
        lb_cat,
        games_started,
        games_played,

        -- Stats
        lb_tackles_comb_per_game_scaled,
        lb_tackles_solo_per_game_scaled,
        lb_tackles_tfl_per_game_scaled,
        lb_press_per_game_scaled,
        lb_tackles_succ_rate_pct_scaled,
        lb_cmp_pct_scaled

    FROM {{ ref('int_nfl_players_lb_scaled') }}
    )

SELECT
    lb_cat,
    ROUND(AVG(lb_tackles_comb_per_game_scaled),2) AS tackles_comb_per_game_scaled,
    ROUND(AVG(lb_tackles_solo_per_game_scaled),2) AS tackles_solo_per_game_scaled,
    ROUND(AVG(lb_tackles_tfl_per_game_scaled),2) AS tackles_for_loss_per_game_scaled,
    ROUND(AVG(lb_press_per_game_scaled),2) AS press_per_game_scaled,
    ROUND(AVG(lb_tackles_succ_rate_pct_scaled),2) AS tackles_succ_rate_pct_scaled,
    1-ROUND(AVG(lb_cmp_pct_scaled),2) AS not_cmp_pass_pct_scaled,

FROM temp
GROUP BY lb_cat

