WITH max_table AS (
    SELECT
        player_pk,
        pos,
        rec_yrds,
        ROW_NUMBER() OVER (PARTITION BY pos ORDER BY rec_yrds DESC) AS wr_rank,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY rec_yrds DESC) AS wr_team_rank,
        
        -- Infos générales
        age,
        team,
        games_played,
        games_started,

        -- Stats WR
        wr_rec_yds_per_rec,
        wr_rec_succ_perc,
        wr_rec_1st_down,
        wr_yds_aft_ctch_per_rec,
        wr_dropped_perc_per_target,
        wr_receiving_ctch_perc,
        
        -- WR MAX
        MAX(wr_rec_yds_per_rec) OVER () AS max_wr_rec_yds_per_rec,
        MAX(wr_rec_succ_perc) OVER () AS max_wr_rec_succ_perc,
        MAX(wr_rec_1st_down) OVER () AS max_wr_rec_1st_down,
        MAX(wr_yds_aft_ctch_per_rec) OVER () AS max_wr_yds_aft_ctch_per_rec,
        MAX(wr_dropped_perc_per_target) OVER () AS max_wr_dropped_perc_per_target,
        MAX(wr_receiving_ctch_perc) OVER () AS max_wr_receiving_ctch_perc,

    FROM {{ ref('int_nfl_players_offense_final') }} 
    WHERE (pos = 'WR') AND (games_started >= 3)
)

SELECT
    player_pk,
    team,
    pos,
    rec_yrds,
    games_played,
    games_started,
    wr_rank,
    wr_team_rank,
    CASE
        WHEN team = 'Cardinals' THEN 'Cardinals'
        WHEN wr_rank <= 5 THEN 'Top 5 Wide Receiver'
        ELSE 'Average wide receiver' 
    END AS wr_cat,
    -- Infos générales
    age,
    -- Stats WR
    SAFE_DIVIDE(wr_rec_yds_per_rec, max_wr_rec_yds_per_rec) AS wr_rec_yds_per_rec_scaled,
    SAFE_DIVIDE(wr_rec_succ_perc, max_wr_rec_succ_perc) AS wr_rec_succ_perc_scaled,
    SAFE_DIVIDE(wr_rec_1st_down, max_wr_rec_1st_down) AS wr_rec_1st_down_scaled,
    SAFE_DIVIDE(wr_yds_aft_ctch_per_rec, max_wr_yds_aft_ctch_per_rec) AS wr_yds_aft_ctch_per_rec_scaled,
    SAFE_DIVIDE(wr_dropped_perc_per_target, max_wr_dropped_perc_per_target) AS wr_dropped_perc_per_target_scaled,
    SAFE_DIVIDE(wr_receiving_ctch_perc, max_wr_receiving_ctch_perc) AS wr_receiving_ctch_perc_scaled,
    
FROM max_table
ORDER BY wr_rank
