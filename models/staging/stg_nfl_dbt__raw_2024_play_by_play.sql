with 

source as (

    select * from {{ source('nfl_dbt', 'raw_2024_play_by_play') }}

),

renamed as (

    select
        gameid,
        gamedate,
        quarter,
        minute,
        second,
        offenseteam,
        defenseteam,
        down,
        togo,
        yardline,
        description,
        seasonyear,
        yards,
        formation,
        playtype,
        isrush,
        ispass,
        isincomplete,
        istouchdown,
        passtype,
        issack,
        isinterception,
        isfumble,
        ispenalty,
        istwopointconversion,
        istwopointconversionsuccessful,
        rushdirection,
        yardlinefixed,
        yardlinedirection,

    from source

)

select * from renamed