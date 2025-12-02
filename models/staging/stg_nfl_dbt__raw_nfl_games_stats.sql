with


source as (


   select * from {{ source('nfl_dbt', 'raw_nfl_games_stats') }}


),


renamed as (

<<<<<<< HEAD
=======
    select
        season,
        week,
        date,
        away,
        home,
        score_away,
        score_home,
        first_downs_away,
        first_downs_home,
        first_downs_from_passing_away,
        first_downs_from_passing_home,
        first_downs_from_rushing_away,
        first_downs_from_rushing_home,
        first_downs_from_penalty_away,
        first_downs_from_penalty_home,
        third_down_comp_away,
        third_down_att_away,
        third_down_comp_home,
        third_down_att_home,
        fourth_down_comp_away,
        fourth_down_att_away,
        fourth_down_comp_home,
        fourth_down_att_home,
        plays_away,
        plays_home,
        drives_away,
        drives_home,
        yards_away,
        yards_home,
        pass_comp_away,
        pass_att_away,
        pass_yards_away,
        pass_comp_home,
        pass_att_home,
        pass_yards_home,
        sacks_num_away,
        sacks_yards_away,
        sacks_num_home,
        sacks_yards_home,
        rush_att_away,
        rush_yards_away,
        rush_att_home,
        rush_yards_home,
        pen_num_away,
        pen_yards_away,
        pen_num_home,
        pen_yards_home,
        redzone_comp_away,
        redzone_att_away,
        redzone_comp_home,
        redzone_att_home,
        fumbles_away,
        fumbles_home,
        interceptions_away,
        interceptions_home,
        possession_away,
        possession_home
>>>>>>> e7abfe630f82e95b0c8f05cdb7db2a47dbf5b316

   select
       season,
       week,
       date,
       concat(
       regexp_replace(cast(date as string), r'[^0-9]', ''),
       '_',
       left(home, 4),
       '_',
       left(away, 4)
   ) as matchid,
       away,
       home,
       score_away,
       score_home,
       CASE WHEN score_away<score_home THEN 'loser'
            WHEN score_away>score_home THEN 'winner'
            ELSE 'draw' END as status_away,
        CASE WHEN score_home<score_away THEN 'loser'
            WHEN score_home>score_away THEN 'winner'
            ELSE 'draw' END as status_home,
       first_downs_away,
       first_downs_home,
       first_downs_from_passing_away,
       first_downs_from_passing_home,
       first_downs_from_rushing_away,
       first_downs_from_rushing_home,
       first_downs_from_penalty_away,
       first_downs_from_penalty_home,
       third_down_comp_away,
       third_down_att_away,
       third_down_comp_home,
       third_down_att_home,
       fourth_down_comp_away,
       fourth_down_att_away,
       fourth_down_comp_home,
       fourth_down_att_home,
       plays_away,
       plays_home,
       drives_away,
       drives_home,
       yards_away,
       yards_home,
       pass_comp_away,
       pass_att_away,
       pass_yards_away,
       pass_comp_home,
       pass_att_home,
       pass_yards_home,
       sacks_num_away,
       sacks_yards_away,
       sacks_num_home,
       sacks_yards_home,
       rush_att_away,
       rush_yards_away,
       rush_att_home,
       rush_yards_home,
       pen_num_away,
       pen_yards_away,
       pen_num_home,
       pen_yards_home,
       redzone_comp_away,
       redzone_att_away,
       redzone_comp_home,
       redzone_att_home,
       fumbles_away,
       fumbles_home,
       interceptions_away,
       interceptions_home,
       possession_away,
       possession_home


   from source


)


SELECT *
<<<<<<< HEAD
FROM renamed
=======
from renamed
>>>>>>> e7abfe630f82e95b0c8f05cdb7db2a47dbf5b316
