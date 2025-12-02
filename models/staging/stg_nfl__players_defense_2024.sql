with 

source as (

    select * from {{ source('nfl', 'players_defense_2024') }}

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
        def_interceptions_int,
        def_interceptions_yds,
        def_interceptions_inttd,
        def_interceptions_lng,
        def_interceptions_pd,
        fumbles_ff,
        fumbles_fmb,
        fumbles_fr,
        fumbles_yds,
        fumbles_frtd,
        sk,
        tackles_comb,
        tackles_solo,
        tackles_ast,
        tackles_tfl,
        tackles_qbhits,
        sfty,
        awards,
        player_pk

    from source

)

select * from renamed