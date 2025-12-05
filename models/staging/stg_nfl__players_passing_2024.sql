with 

source as (

    select * from {{ source('nfl', 'players_passing_2024') }}

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
        CASE
            WHEN pos IN('QB') THEN 'Quarterback'
            WHEN pos IN('WR','TE') THEN 'Receivers'
            WHEN pos IN('OL', 'C', 'T', 'LT', 'RT', 'LG', 'RG') THEN 'OL'
            WHEN pos IN('RB', 'FB', 'FB/DL') THEN 'Runners'
            ELSE 'Other'
        END as pos_category,
        pos,
        g,
        gs,
        qbrec,
        cmp,
        att,
        cmp_ AS comp_perc,
        yds AS pass_yrds,
        td,
        td_,
        `int`,
        int_,
        _1d,
        succ_,
        lng,
        y_a,
        ay_a AS avg_yrds_per_attmpt,
        y_c,
        y_g AS yrds_per_game,
        rate,
        qbr AS qb_rating,
        sk,
        yds_lost,
        sk_ AS sack_perc,
        ny_a,
        any_a,
        _4qc,
        gwd,
        awards,
        player_pk

    from source

)

select * from renamed