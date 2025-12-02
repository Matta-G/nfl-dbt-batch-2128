with 

source as (

    select * from {{ source('nfl', 'players_passing_2024') }}

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
        qbrec,
        cmp,
        att,
        cmp_,
        yds,
        td,
        td_,
        int,
        int_,
        _1d,
        succ_,
        lng,
        y_a,
        ay_a,
        y_c,
        y_g,
        rate,
        qbr,
        sk,
        yds_lost,
        sk_,
        ny_a,
        any_a,
        _4qc,
        gwd,
        awards,
        player_pk

    from source

)

select * from renamed