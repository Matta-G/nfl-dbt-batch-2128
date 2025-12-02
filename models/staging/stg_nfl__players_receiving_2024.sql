with 

source as (

    select * from {{ source('nfl', 'players_receiving_2024') }}

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
        receiving_tgt,
        receiving_rec,
        receiving_yds,
        receiving_y_r,
        receiving_td,
        receiving_1d,
        receiving_succ_,
        receiving_lng,
        receiving_r_g,
        receiving_y_g,
        receiving_ctch_,
        receiving_y_tgt,
        fmb,
        awards,
        player_pk

    from source

)

select * from renamed