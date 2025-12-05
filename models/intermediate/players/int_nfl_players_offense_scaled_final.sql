WITH max_table AS (
    SELECT
        player_pk,
        player,
        pos,
        pass_yrds,
        rec_yrds,
        rush_yrds,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY pass_yrds DESC) AS qb_rank,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY rec_yrds DESC) AS wr_rank,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY rush_yrds DESC) AS rb_rank,
        
        -- Infos générales
        age,

        -- Stats QB
        qb_comp_perc,
        ROUND(qb_td_int_ratio,2) AS qb_td_int_ratio,
        qb_avg_yrds_per_attmpt,
        qb_yrds_per_game,
        qb_qb_rating,
        qb_sack_perc,

        -- Stats WR
        wr_rec_yds_per_rec,
        wr_rec_succ_perc,
        wr_rec_1st_down,
        wr_yds_aft_ctch_per_rec,
        wr_dropped_perc_per_target,
        wr_receiving_ctch_perc,

        -- Stats RB
        rb_tot_yards_per_game,
        rb_rsh_succ_perc,
        rb_rsh_yrds_bfr_contact_per_attmpt,
        rb_rsh_yrds_aftr_contact_per_attmpt,
        rb_rsh_yrds_per_attmpt,
        rb_rsh_yrds_per_game,
        
        -- QB MAX
        MAX(qb_comp_perc) OVER() AS max_qb_comp_perc,
        MAX(qb_td_int_ratio) OVER () AS max_qb_td_int_ratio,
        MAX(qb_avg_yrds_per_attmpt) OVER () AS max_qb_avg_yrds_per_attmpt,
        MAX(qb_yrds_per_game) OVER () AS max_qb_yrds_per_game,
        MAX(qb_qb_rating) OVER () AS max_qb_qb_rating,
        MAX(qb_sack_perc) OVER () AS max_qb_sack_perc,
        -- WR MAX
        MAX(wr_rec_yds_per_rec) OVER () AS max_wr_rec_yds_per_rec,
        MAX(wr_rec_succ_perc) OVER () AS max_wr_rec_succ_perc,
        MAX(wr_rec_1st_down) OVER () AS max_wr_rec_1st_down,
        MAX(wr_yds_aft_ctch_per_rec) OVER () AS max_wr_yds_aft_ctch_per_rec,
        MAX(wr_dropped_perc_per_target) OVER () AS max_wr_dropped_perc_per_target,
        MAX(wr_receiving_ctch_perc) OVER () AS max_wr_receiving_ctch_perc,
        -- RB MAX
        MAX(rb_tot_yards_per_game) OVER () AS max_rb_tot_yards_per_game,
        MAX(rb_rsh_succ_perc) OVER () AS max_rb_rsh_succ_perc,
        MAX(rb_rsh_yrds_bfr_contact_per_attmpt) OVER () AS max_rb_rsh_yrds_bfr_contact_per_attmpt,
        MAX(rb_rsh_yrds_aftr_contact_per_attmpt) OVER () AS max_rb_rsh_yrds_aftr_contact_per_attmpt,
        MAX(rb_rsh_yrds_per_attmpt) OVER () AS max_rb_rsh_yrds_per_attmpt,
        MAX(rb_rsh_yrds_per_game) OVER () AS max_rb_rsh_yrds_per_game

    FROM {{ ref('int_nfl_players_offense_final') }}
)

SELECT
    player_pk,
    player,
    pos,
    pass_yrds,
    qb_rank,
    CASE
        WHEN qb_rank <= 5 THEN 'Top 5 quarterback'
        ELSE 'Average quarterback' 
    END AS qb_cat,
    CASE
        WHEN wr_rank <= 5 THEN 'Top 5 receiver'
        ELSE 'Average receiver' 
    END AS wr_cat,
    CASE
        WHEN rb_rank <= 5 THEN 'Top 5 running back'
        ELSE 'Average running back' 
    END AS rb_cat,
    -- Infos générales
    age,
    -- Stats QB
    SAFE_DIVIDE(qb_comp_perc, max_qb_comp_perc) AS qb_comp_perc_scaled,
    SAFE_DIVIDE(qb_td_int_ratio, max_qb_td_int_ratio) AS qb_td_int_ratio_scaled,
    SAFE_DIVIDE(qb_avg_yrds_per_attmpt, max_qb_avg_yrds_per_attmpt) AS qb_avg_yrds_per_attmpt_scaled,
    SAFE_DIVIDE(qb_yrds_per_game, max_qb_yrds_per_game) AS qb_yrds_per_game_scaled,
    SAFE_DIVIDE(qb_qb_rating, max_qb_qb_rating) AS qb_qb_rating_scaled,
    SAFE_DIVIDE(qb_sack_perc, max_qb_sack_perc) AS qb_sack_perc_scaled,
    -- Stats WR
    SAFE_DIVIDE(wr_rec_yds_per_rec, max_wr_rec_yds_per_rec) AS wr_rec_yds_per_rec_scaled,
    SAFE_DIVIDE(wr_rec_succ_perc, max_wr_rec_succ_perc) AS wr_rec_succ_perc_scaled,
    SAFE_DIVIDE(wr_rec_1st_down, max_wr_rec_1st_down) AS wr_rec_1st_down_scaled,
    SAFE_DIVIDE(wr_yds_aft_ctch_per_rec, max_wr_yds_aft_ctch_per_rec) AS wr_yds_aft_ctch_per_rec_scaled,
    SAFE_DIVIDE(wr_dropped_perc_per_target, max_wr_dropped_perc_per_target) AS wr_dropped_perc_per_target_scaled,
    SAFE_DIVIDE(wr_receiving_ctch_perc, max_wr_receiving_ctch_perc) AS wr_receiving_ctch_perc_scaled,
    -- Stats RB
    SAFE_DIVIDE(rb_tot_yards_per_game, max_rb_tot_yards_per_game) AS rb_tot_yards_per_game_scaled,
    SAFE_DIVIDE(rb_rsh_succ_perc, max_rb_rsh_succ_perc) AS rb_rsh_succ_perc_scaled,
    SAFE_DIVIDE(rb_rsh_yrds_bfr_contact_per_attmpt, max_rb_rsh_yrds_bfr_contact_per_attmpt) AS rb_rsh_yrds_bfr_contact_per_attmpt_scaled,
    SAFE_DIVIDE(rb_rsh_yrds_aftr_contact_per_attmpt, max_rb_rsh_yrds_aftr_contact_per_attmpt) AS rb_rsh_yrds_aftr_contact_per_attmpt_scaled,
    SAFE_DIVIDE(rb_rsh_yrds_per_attmpt, max_rb_rsh_yrds_per_attmpt) AS rb_rsh_yrds_per_attmpt_scaled,
    SAFE_DIVIDE(rb_rsh_yrds_per_game, max_rb_rsh_yrds_per_game) AS rb_rsh_yrds_per_game_scaled
    
FROM max_table