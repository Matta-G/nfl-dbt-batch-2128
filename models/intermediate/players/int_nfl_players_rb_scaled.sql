WITH max_table AS (
    SELECT
        player_pk,
        player,
        pos,
        rush_yrds,
        rec_yrds,
        rb_tot_yds,
        rushing_att,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY rb_tot_yds DESC) AS rb_rank,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY rb_tot_yds DESC) AS rb_team_rank,
        
        -- Infos générales
        age,
        team,
        games_played,
        games_started,
        win_rate,

        -- Stats RB
        rb_tot_yards_per_game,
        rb_rsh_succ_perc,
        rb_rsh_yrds_bfr_contact_per_attmpt,
        rb_rsh_yrds_aftr_contact_per_attmpt, 
        rb_rsh_yrds_per_attmpt,
        rb_rsh_yrds_per_game,
        
        -- RB MIN
        MIN(rb_tot_yards_per_game) OVER() AS min_rb_tot_yards_per_game,
        MIN(rb_rsh_succ_perc) OVER () AS min_rb_rsh_succ_perc,
        MIN(rb_rsh_yrds_bfr_contact_per_attmpt) OVER () AS min_rb_rsh_yrds_bfr_contact_per_attmpt,
        MIN(rb_rsh_yrds_aftr_contact_per_attmpt) OVER () AS min_rb_rsh_yrds_aftr_contact_per_attmpt,
        MIN(rb_rsh_yrds_per_attmpt) OVER () AS min_rb_rsh_yrds_per_attmpt,
        MIN(rb_rsh_yrds_per_game) OVER () AS min_rb_rsh_yrds_per_game,

        -- RB MAX
        MAX(rb_tot_yards_per_game) OVER() AS max_rb_tot_yards_per_game,
        MAX(rb_rsh_succ_perc) OVER () AS max_rb_rsh_succ_perc,
        MAX(rb_rsh_yrds_bfr_contact_per_attmpt) OVER () AS max_rb_rsh_yrds_bfr_contact_per_attmpt,
        MAX(rb_rsh_yrds_aftr_contact_per_attmpt) OVER () AS max_rb_rsh_yrds_aftr_contact_per_attmpt,
        MAX(rb_rsh_yrds_per_attmpt) OVER () AS max_rb_rsh_yrds_per_attmpt,
        MAX(rb_rsh_yrds_per_game) OVER () AS max_rb_rsh_yrds_per_game,

    FROM {{ ref('int_nfl_players_offense_final') }} 
    WHERE (pos = 'RB') AND (games_started >= 3)
)

SELECT
    player_pk,
    player,
    team,
    pos,
    games_played,
    games_started,
    rush_yrds,
    rushing_att,
    rb_rsh_yrds_per_attmpt,
    rec_yrds,
    rb_tot_yds,
    rb_rank,
    rb_team_rank,
    win_rate,

    CASE
        WHEN team = 'Cardinals' THEN 'James Conner'
        WHEN rb_rank <= 5 THEN 'Top 5 (average)'
        ELSE 'League average' 
    END AS rb_cat,
    CASE
        WHEN team = 'Cardinals' THEN 'James Conner'
        WHEN rb_rank <= 5 THEN 'Top 5 (average)'
        ELSE player 
    END AS rb_scatter_cat,
    -- Infos générales
    age,
    -- RB Stats scaled (Min/Max)
    SAFE_DIVIDE((rb_tot_yards_per_game - min_rb_tot_yards_per_game),(max_rb_tot_yards_per_game - min_rb_tot_yards_per_game)) AS rb_tot_yards_per_game_scaled,
    SAFE_DIVIDE((rb_rsh_succ_perc - min_rb_rsh_succ_perc),(max_rb_rsh_succ_perc - min_rb_rsh_succ_perc)) AS rb_rsh_succ_perc_scaled,
    SAFE_DIVIDE((rb_rsh_yrds_bfr_contact_per_attmpt - min_rb_rsh_yrds_bfr_contact_per_attmpt),(max_rb_rsh_yrds_bfr_contact_per_attmpt - min_rb_rsh_yrds_bfr_contact_per_attmpt)) AS rb_rsh_yrds_bfr_contact_per_attmpt_scaled,
    SAFE_DIVIDE((rb_rsh_yrds_aftr_contact_per_attmpt - min_rb_rsh_yrds_aftr_contact_per_attmpt),(max_rb_rsh_yrds_aftr_contact_per_attmpt - min_rb_rsh_yrds_aftr_contact_per_attmpt)) AS rb_rsh_yrds_aftr_contact_per_attmpt_scaled,
    SAFE_DIVIDE((rb_rsh_yrds_per_attmpt - min_rb_rsh_yrds_per_attmpt),(max_rb_rsh_yrds_per_attmpt - min_rb_rsh_yrds_per_attmpt)) AS rb_rsh_yrds_per_attmpt_scaled,
    SAFE_DIVIDE((rb_rsh_yrds_per_game - min_rb_rsh_yrds_per_game),(max_rb_rsh_yrds_per_game - min_rb_rsh_yrds_per_game)) AS rb_rsh_yrds_per_game_scaled,
    
FROM max_table
ORDER BY rb_rank
