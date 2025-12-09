WITH temp AS (
    SELECT
        wr_cat,
        pos,
        games_started,
        games_played,

        -- Stats WR
        wr_rec_yds_per_rec_scaled,
        wr_rec_succ_perc_scaled,
        wr_rec_1st_down_scaled,
        wr_yds_aft_ctch_per_rec_scaled,
        wr_dropped_perc_per_target_scaled,
        wr_receiving_ctch_perc_scaled,

    FROM {{ ref('int_nfl_players_wr_scaled') }}
    )

SELECT
    wr_cat,
    pos,
    ROUND(AVG(wr_rec_yds_per_rec_scaled),2) AS rec_yds_per_rec_scaled,
    ROUND(AVG(wr_rec_succ_perc_scaled),2) AS rec_succ_perc_scaled,
    ROUND(AVG(wr_rec_1st_down_scaled),2) AS rec_1st_down_scaled,
    ROUND(AVG(wr_yds_aft_ctch_per_rec_scaled),2) AS yds_aft_ctch_per_rec_scaled,
    ROUND(AVG(wr_dropped_perc_per_target_scaled),2) AS dropped_perc_per_target_scaled,
    ROUND(AVG(wr_receiving_ctch_perc_scaled),2) AS receiving_ctch_perc_scaled,

FROM temp
GROUP BY wr_cat, pos
HAVING pos = 'WR'