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
        CASE offenseteam
    WHEN 'SF'  THEN '49ers'
    WHEN 'TEN' THEN 'Titans'
    WHEN 'NYJ' THEN 'Jets'
    WHEN 'MIA' THEN 'Dolphins'
    WHEN 'NO'  THEN 'Saints'
    WHEN 'KC'  THEN 'Chiefs'
    WHEN 'NE'  THEN 'Patriots'
    WHEN 'PHI' THEN 'Eagles'
    WHEN 'CIN' THEN 'Bengals'
    WHEN 'BUF' THEN 'Bills'
    WHEN 'HOU' THEN 'Texans'
    WHEN 'IND' THEN 'Colts'
    WHEN 'CHI' THEN 'Bears'
    WHEN 'LA'  THEN 'Rams'
    WHEN 'LAC' THEN 'Chargers'
    WHEN 'NYG' THEN 'Giants'
    WHEN 'TB'  THEN 'Buccaneers'
    WHEN 'LV'  THEN 'Raiders'
    WHEN 'GB'  THEN 'Packers'
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
    ELSE offenseteam
END as offenseteam,
        CASE defenseteam
    WHEN 'SF'  THEN '49ers'
    WHEN 'TEN' THEN 'Titans'
    WHEN 'NYJ' THEN 'Jets'
    WHEN 'MIA' THEN 'Dolphins'
    WHEN 'NO'  THEN 'Saints'
    WHEN 'KC'  THEN 'Chiefs'
    WHEN 'NE'  THEN 'Patriots'
    WHEN 'PHI' THEN 'Eagles'
    WHEN 'CIN' THEN 'Bengals'
    WHEN 'BUF' THEN 'Bills'
    WHEN 'HOU' THEN 'Texans'
    WHEN 'IND' THEN 'Colts'
    WHEN 'CHI' THEN 'Bears'
    WHEN 'LA'  THEN 'Rams'
    WHEN 'LAC' THEN 'Chargers'
    WHEN 'NYG' THEN 'Giants'
    WHEN 'TB'  THEN 'Buccaneers'
    WHEN 'LV'  THEN 'Raiders'
    WHEN 'GB'  THEN 'Packers'
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
    ELSE defenseteam
END as defenseteam,
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
    where PlayType is not null
  and PlayType not in ('KICK OFF', 'TIMEOUT', 'NO PLAY', 'CLOCK STOP', 'EXCEPTION', 'PENALTY')

)

select * from renamed