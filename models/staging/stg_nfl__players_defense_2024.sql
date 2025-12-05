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
            WHEN pos IN('CB', 'LCB', 'RCB', 'LCB', 'RCB', 'S', 'SS', 'FS', 'DB') THEN 'DB'
            WHEN pos IN('LB', 'MLB', 'ILB', 'LILB', 'RILB', 'OLB', 'LOLB', 'ROLB', 'LLB', 'RLB') THEN 'LB'
            WHEN pos IN('DE', 'LDE', 'RDE', 'DT', 'NT', 'DL', 'LDT', 'RDT') THEN 'DL'
            ELSE 'Other'
        END as pos_category,
        pos,
        g,
        gs,
        db_int,
        def_interceptions_yds,
        def_interceptions_inttd,
        def_interceptions_lng,
        db_pd AS db_pass_defended, --Passes Defended
        db_tgt AS db_target, --Times Targeted as a Defender
        db_cmp AS db_completed_pass, --Completed passes when the WR of the defender is targeted (The lower the better)
        db_lb_cmp_pct, -- Completion Percentage Allowed Targets % (The lower the better)
        db_yac AS db_yrds_aftr_ctch, -- Yards after catch on completion (The lower the better)
        fumbles_ff,
        fumbles_fmb,
        fumbles_fr,
        fumbles_yds,
        fumbles_frtd,
        sk,
        lb_tackles_comb,
        lb_tackles_solo,
        lb_tackles_tfl,
        lb_press,
        lb_tackles_succ_rate_pct, -- Tackles success rate percentage : combined / missed tackles
        tackles_ast,
        tackles_qbhits,
        sfty,
        awards,
        player_pk

    from source

)

select * from renamed