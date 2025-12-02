with 

source as (

    select * from {{ source('nfl', 'players_rushing_2024') }}

),

renamed as (

    select
        rk,
        player,
        age,
         CASE team
    WHEN 'SFO'  THEN '49ers'
    WHEN 'TEN' THEN 'Titans'
    WHEN 'NYJ' THEN 'Jets'
    WHEN 'MIA' THEN 'Dolphins'
    WHEN 'NOR'  THEN 'Saints'
    WHEN 'KAN'  THEN 'Chiefs'
    WHEN 'NWE'  THEN 'Patriots'
    WHEN 'PHI' THEN 'Eagles'
    WHEN 'CIN' THEN 'Bengals'
    WHEN 'BUF' THEN 'Bills'
    WHEN 'HOU' THEN 'Texans'
    WHEN 'IND' THEN 'Colts'
    WHEN 'CHI' THEN 'Bears'
    WHEN 'LAR'  THEN 'Rams'
    WHEN 'LAC' THEN 'Chargers'
    WHEN 'NYG' THEN 'Giants'
    WHEN 'TAM'  THEN 'Buccaneers'
    WHEN 'LVR'  THEN 'Raiders'
    WHEN 'GNB'  THEN 'Packers'
    WHEN 'CLE' THEN 'Browns'
    WHEN 'ARI' THEN 'Cardinals'
    WHEN 'DEN' THEN 'Broncos'
    WHEN 'PIT' THEN 'Steelers'
    WHEN 'SEA' THEN 'Seahawks'
    WHEN 'DAL' THEN 'Cowboys'
    WHEN 'WAS' THEN 'Commanders'
    WHEN 'DET' THEN 'Lions'
    WHEN 'JAX' THEN 'Jaguars'
    WHEN 'CAR' THEN 'Panthers'
    WHEN 'BAL' THEN 'Ravens'
    WHEN 'MIN' THEN 'Vikings'
    WHEN 'ATL' THEN 'Falcons'
    ELSE team
END as team,
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