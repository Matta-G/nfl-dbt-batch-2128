WITH temp AS (
    SELECT
        -- Info générales
        rb_cat,
        pos,
        games_started,
        games_played,

        -- Stats
        rb_tot_yards_per_game_scaled,
        rb_rsh_succ_perc_scaled,
        rb_rsh_yrds_bfr_contact_per_attmpt_scaled,
        rb_rsh_yrds_aftr_contact_per_attmpt_scaled,
        rb_rsh_yrds_per_attmpt_scaled,
        rb_rsh_yrds_per_game_scaled

    FROM {{ ref('int_nfl_players_rb_scaled') }}
    )

SELECT
    rb_cat,
    pos,
    ROUND(AVG(rb_tot_yards_per_game_scaled),2) AS tot_yards_per_game_scaled,
    ROUND(AVG(rb_rsh_succ_perc_scaled),2) AS rsh_succ_perc_scaled,
    ROUND(AVG(rb_rsh_yrds_bfr_contact_per_attmpt_scaled),2) AS rsh_yrds_bfr_contact_per_attmpt_scaled,
    ROUND(AVG(rb_rsh_yrds_aftr_contact_per_attmpt_scaled),2) AS rsh_yrds_aftr_contact_per_attmpt_scaled,
    ROUND(AVG(rb_rsh_yrds_per_attmpt_scaled),2) AS rsh_yrds_per_attmpt_scaled,
    ROUND(AVG(rb_rsh_yrds_per_game_scaled),2) AS rsh_yrds_per_game_scaled,

FROM temp
GROUP BY rb_cat, pos
HAVING pos = 'RB'


