with 

source as (

    select * from {{ source('nfl', 'players_rushing_2024') }}

),

renamed as (

    select
        rk,
        player,
        age,
        team,
        pos,
        g,
        gs,
        rushing_att,
        rushing_yds,
        rushing_td,
        rushing_1d,
        rushing_succ_,
        rushing_lng,
        rushing_y_a,
        rushing_y_g,
        rushing_a_g,
        fmb,
        awards,
        player_pk

    from source

)

select * from renamed