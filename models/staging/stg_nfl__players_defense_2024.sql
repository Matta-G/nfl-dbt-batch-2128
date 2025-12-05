with 

source as (

    select * from {{ source('nfl', 'players_defense_2024') }}

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
            WHEN pos IN('CB', 'LCB', 'RCB', 'LCB', 'RCB', 'S', 'SS', 'FS') THEN 'Defensive Back'
            WHEN pos IN('LB', 'MLB', 'ILB', 'LILB', 'RILB', 'OLB', 'LOLB', 'ROLB', 'LLB', 'RLB') THEN 'Linebacker'
            WHEN pos IN('DE', 'LDE', 'RDE', 'DT', 'NT', 'DL', 'LDT', 'RDT') THEN 'Defensive Line'
            ELSE 'Other'
        END as pos_category,
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