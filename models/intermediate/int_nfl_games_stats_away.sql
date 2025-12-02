with source as (


   select * from {{ ref('stg_nfl_dbt__raw_nfl_games_stats') }}


),


renamed as (


   select
       season,
       week,
       date,
       matchid,
       away AS team,
       score_away AS score,
       first_downs_away AS first_downs,
       first_downs_from_passing_away AS first_downs_from_passing,
       first_downs_from_rushing_away AS first_downs_from_rushing,
       first_downs_from_penalty_away AS first_downs_from_penalty,
       third_down_comp_away AS third_down_comp,
       third_down_att_away AS third_down_att,
       fourth_down_comp_away AS fourth_down_comp,
       fourth_down_att_away AS fourth_down_att,
       plays_away AS plays,
       drives_away AS drives,
       yards_away as yards,
       pass_comp_away as pass_comp,
       pass_att_away as pass_att,
       pass_yards_away as pass_yards,
       sacks_num_away as sacks_num,
       sacks_yards_away as sacks_yards,
       rush_att_away as rush_att,
       rush_yards_away as rush_yards,
       pen_num_away as pen_num,
       pen_yards_away as pen_yards,
       redzone_comp_away as redzone_comp,
       redzone_att_away as redzone_att,
       fumbles_away as fumbles,
       interceptions_away as interceptions,
       possession_away as possession,
       'away' as location


   from source


)


SELECT *
FROM renamed