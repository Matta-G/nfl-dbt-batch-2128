with 

source as (

    select * from {{ source('nfl', '2024_agg_rankings_teams') }}

),

renamed as (

    select
        team,
        def_yards_allowed_rk,
        def_passing_rk,
        def_rushing_rk,
        def_scoring_rk,
        def_red_zone_rk,
        def_3rd_down_rk,
        off_total_rank,
        off_passing_rank,
        off_rushing_rank,
        off_scoring_rank,
        off_redzone_rank,
        off_third_rank

    from source

)

select * from renamed