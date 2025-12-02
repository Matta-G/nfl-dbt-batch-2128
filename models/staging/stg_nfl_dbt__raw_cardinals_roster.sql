with 

source as (

    select * from {{ source('nfl_dbt', 'raw_cardinals_roster') }}

),

renamed as (

    select
        no_,
        player,
        age,
        pos,
        g,
        gs,
        wt,
        ht,
        birthdate,
        yrs

    from source

)

select * from renamed