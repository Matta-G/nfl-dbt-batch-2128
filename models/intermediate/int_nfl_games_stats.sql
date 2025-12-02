with games as (

    select *
    from {{ ref('stg_nfl_dbt__raw_nfl_games_stats') }}

),

team_games as (

    -- Vue équipe extérieur
    select
        season,
        week,
        date,
        away as team,
        false as is_home,

        drives_away as drives_for,
        drives_home as drives_against,

        first_downs_away as first_downs_for,
        first_downs_home as first_downs_against,

        first_downs_from_passing_away as first_downs_from_passing_for,
        first_downs_from_passing_home as first_downs_from_passing_against,

        first_downs_from_penalty_away as first_downs_from_penalty_for,
        first_downs_from_penalty_home as first_downs_from_penalty_against,

        first_downs_from_rushing_away as first_downs_from_rushing_for,
        first_downs_from_rushing_home as first_downs_from_rushing_against,

        fourth_down_att_away as fourth_down_att_for,
        fourth_down_att_home as fourth_down_att_against,

        fourth_down_comp_away as fourth_down_comp_for,
        fourth_down_comp_home as fourth_down_comp_against,

        fumbles_away as fumbles_for,
        fumbles_home as fumbles_against,

        interceptions_away as interceptions_for,
        interceptions_home as interceptions_against,

        pass_att_away as pass_att_for,
        pass_att_home as pass_att_against,

        pass_comp_away as pass_comp_for,
        pass_comp_home as pass_comp_against,

        pass_yards_away as pass_yards_for,
        pass_yards_home as pass_yards_against,

        pen_num_away as pen_num_for,
        pen_num_home as pen_num_against,

        pen_yards_away as pen_yards_for,
        pen_yards_home as pen_yards_against,

        plays_away as plays_for,
        plays_home as plays_against,

        possession_away as possession_for,
        possession_home as possession_against,

        redzone_att_away as redzone_att_for,
        redzone_att_home as redzone_att_against,

        redzone_comp_away as redzone_comp_for,
        redzone_comp_home as redzone_comp_against,

        rush_att_away as rush_att_for,
        rush_att_home as rush_att_against,

        rush_yards_away as rush_yards_for,
        rush_yards_home as rush_yards_against,

        sacks_num_away as sacks_num_for,
        sacks_num_home as sacks_num_against,

        sacks_yards_away as sacks_yards_for,
        sacks_yards_home as sacks_yards_against,

        score_away as score_for,
        score_home as score_against,

        third_down_att_away as third_down_att_for,
        third_down_att_home as third_down_att_against,

        third_down_comp_away as third_down_comp_for,
        third_down_comp_home as third_down_comp_against,

        yards_away as yards_for,
        yards_home as yards_against

    from games

    union all

    -- Vue équipe domicile
    select
        season,
        week,
        date,
        home as team,
        true as is_home,

        drives_home as drives_for,
        drives_away as drives_against,

        first_downs_home as first_downs_for,
        first_downs_away as first_downs_against,

        first_downs_from_passing_home as first_downs_from_passing_for,
        first_downs_from_passing_away as first_downs_from_passing_against,

        first_downs_from_penalty_home as first_downs_from_penalty_for,
        first_downs_from_penalty_away as first_downs_from_penalty_against,

        first_downs_from_rushing_home as first_downs_from_rushing_for,
        first_downs_from_rushing_away as first_downs_from_rushing_against,

        fourth_down_att_home as fourth_down_att_for,
        fourth_down_att_away as fourth_down_att_against,

        fourth_down_comp_home as fourth_down_comp_for,
        fourth_down_comp_away as fourth_down_comp_against,

        fumbles_home as fumbles_for,
        fumbles_away as fumbles_against,

        interceptions_home as interceptions_for,
        interceptions_away as interceptions_against,

        pass_att_home as pass_att_for,
        pass_att_away as pass_att_against,

        pass_comp_home as pass_comp_for,
        pass_comp_away as pass_comp_against,

        pass_yards_home as pass_yards_for,
        pass_yards_away as pass_yards_against,

        pen_num_home as pen_num_for,
        pen_num_away as pen_num_against,

        pen_yards_home as pen_yards_for,
        pen_yards_away as pen_yards_against,

        plays_home as plays_for,
        plays_away as plays_against,

        possession_home as possession_for,
        possession_away as possession_against,

        redzone_att_home as redzone_att_for,
        redzone_att_away as redzone_att_against,

        redzone_comp_home as redzone_comp_for,
        redzone_comp_away as redzone_comp_against,

        rush_att_home as rush_att_for,
        rush_att_away as rush_att_against,

        rush_yards_home as rush_yards_for,
        rush_yards_away as rush_yards_against,

        sacks_num_home as sacks_num_for,
        sacks_num_away as sacks_num_against,

        sacks_yards_home as sacks_yards_for,
        sacks_yards_away as sacks_yards_against,

        score_home as score_for,
        score_away as score_against,

        third_down_att_home as third_down_att_for,
        third_down_att_away as third_down_att_against,

        third_down_comp_home as third_down_comp_for,
        third_down_comp_away as third_down_comp_against,

        yards_home as yards_for,
        yards_away as yards_against

    from games

)

select *
from team_games