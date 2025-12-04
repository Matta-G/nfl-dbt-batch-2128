with 

source as (

    select * from {{ source('nfl', 'players_receiving_2024') }}

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
        receiving_tgt,
        receiving_rec,
        receiving_yds,
        receiving_y_r AS rec_yds_per_rec,
        receiving_td,
        receiving_1d AS rec_1st_down,
        receiving_succ_ AS rec_succ_perc,
        yactch_r AS yds_aft_ctch_per_rec,	
        drop__tgt AS dropped_perc_per_target,
        receiving_lng,
        receiving_r_g AS receiving_yrds_per_game,
        receiving_y_g,
        receiving_ctch_ AS receiving_ctch_perc,
        receiving_y_tgt,
        fmb,
        awards,
        player_pk

    from source

)

select * from renamed