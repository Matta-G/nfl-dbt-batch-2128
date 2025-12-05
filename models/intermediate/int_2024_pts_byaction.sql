WITH sub1 as (SELECT 
*
, IF (istouchdown = 1, 6, 0) as pts_td
, IF (istwopointconversionsuccessful = 1, 2, 0) as pts_2pc
 FROM {{ ref('int_2024_play_by_play') }}),

 sub2 as (SELECT 
 gameid
 , formation
 , playtype
 , SUM (sub1.pts_2pc + sub1.pts_td) as pts_tot

 FROM sub1
 GROUP BY gameid, formation, playtype)

 SELECT
 formation
 , playtype
 , SUM (pts_tot) as pts_tot
 FROM sub2
 GROUP BY formation, playtype