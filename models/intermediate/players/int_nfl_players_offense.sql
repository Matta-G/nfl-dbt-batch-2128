-- Récupérer tous les unique Player_PK

WITH union_table AS (
  SELECT 
    player_pk
  FROM `nfl-batch2128.dbt_mr.int_nfl_players_passing_2024_clean`

  UNION DISTINCT

  SELECT 
    player_pk
  FROM `nfl-batch2128.dbt_mr.int_nfl_players_rushing_2024_clean` 

  UNION DISTINCT
  
  SELECT 
    player_pk
  FROM `nfl-batch2128.dbt_mr.int_nfl__players_receiving_2024_clean` 
)

-- Ajouter les colonnes que l'on va utiliser pour l'analyse de chaque poste

SELECT
  player_pk,
  -- Infos générales
  CASE 
    WHEN pass.player IS NOT NULL THEN pass.player
    WHEN rec.player IS NOT NULL THEN rec.player
    ELSE rush.player
  END AS player,
  CASE 
    WHEN pass.age IS NOT NULL THEN pass.age
    WHEN rec.age IS NOT NULL THEN rec.age
    ELSE rush.age
  END AS age,
  CASE 
    WHEN pass.pos IS NOT NULL THEN pass.pos
    WHEN rec.pos IS NOT NULL THEN rec.pos
    ELSE rush.pos
  END AS pos,
  -- Stats QB
  pass.comp_perc AS qb_comp_perc,
  SAFE_DIVIDE(pass.td,pass.int) AS qb_td_int_ratio,
  pass.avg_yrds_per_attmpt AS qb_avg_yrds_per_attmpt,
  pass.yrds_per_game AS qb_yrds_per_game,
  pass.qb_rating AS qb_qb_rating,
  pass.sack_perc AS qb_sack_perc,
  -- Stats WR
  rec.rec_yds_per_rec AS wr_rec_yds_per_rec,
  rec.rec_succ_perc AS wr_rec_succ_perc,
  rec.rec_1st_down AS wr_rec_1st_down,
  rec.yds_aft_ctch_per_rec AS wr_yds_aft_ctch_per_rec,
  rec.dropped_perc_per_target AS wr_dropped_perc_per_target,
  rec.receiving_ctch_perc AS wr_receiving_ctch_perc,
  -- Stats RB
  rush_yrds_per_game + receiving_yrds_per_game AS rb_tot_yards_per_game,
  rush_succ_perc AS rb_rsh_succ_perc,
  rush_yards_before_contact_per_attmpt AS rb_rsh_yrds_bfr_contact_per_attmpt,
  rush_yards_after_contact_per_attmpt AS rb_rsh_yrds_aftr_contact_per_attmpt, 
  rush_yrds_per_attmpt AS rb_rsh_yrds_per_attmpt,
  rush_yrds_per_game AS rb_rsh_yrds_per_game
FROM union_table
LEFT JOIN {{ ref('int_nfl_players_passing_2024_clean') }}  AS pass
USING (player_pk)
LEFT JOIN {{ ref('int_nfl__players_receiving_2024_clean') }} AS rec
USING(player_pk) 
LEFT JOIN {{ ref('int_nfl_players_rushing_2024_clean') }}  AS rush
USING(player_pk)
ORDER BY pos