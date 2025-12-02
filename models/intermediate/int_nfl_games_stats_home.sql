with source as (


   select * from {{ ref('stg_nfl_dbt__raw_nfl_games_stats') }}


),


renamed as (


   select
       season,
       week,
       date,
       matchid,
       home AS team,
       score_home AS score,
       first_downs_home AS first_downs,
       first_downs_from_passing_home AS first_downs_from_passing,
       first_downs_from_rushing_home AS first_downs_from_rushing,
       first_downs_from_penalty_home AS first_downs_from_penalty,
       third_down_comp_home AS third_down_comp,
       third_down_att_home AS third_down_att,
       fourth_down_comp_home AS fourth_down_comp,
       fourth_down_att_home AS fourth_down_att,
       plays_home AS plays,
       drives_home AS drives,
       yards_home as yards,
       pass_comp_home as pass_comp,
       pass_att_home as pass_att,
       pass_yards_home as pass_yards,
       sacks_num_home as sacks_num,
       sacks_yards_home as sacks_yards,
       rush_att_home as rush_att,
       rush_yards_home as rush_yards,
       pen_num_home as pen_num,
       pen_yards_home as pen_yards,
       redzone_comp_home as redzone_comp,
       redzone_att_home as redzone_att,
       fumbles_home as fumbles,
       interceptions_home as interceptions,
       possession_home as possession,
       'home' as location

   from source


)


SELECT *
from renamed