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
        rushing_att,
        rushing_yds AS rush_yrds,
        rushing_td,
        rushing_1d AS rush_1st_down,
        rushing_succ_ AS rush_succ_perc,
        rushing_lng,
        CAST(YBC_A AS FLOAT64) AS rush_yards_before_contact_per_attmpt,
        CAST(YAC_A AS FLOAT64) AS rush_yards_after_contact_per_attmpt, 
        rushing_y_a AS rush_yrds_per_attmpt,
        rushing_y_g AS rush_yrds_per_game,
        rushing_a_g,
        fmb,
        awards,
        player_pk

    from source

)

select * from renamed