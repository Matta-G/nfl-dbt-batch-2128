with 

source as (

    select * from {{ source('nfl', '2024_standings') }}

),

renamed as (

    SELECT
        CASE tm
            WHEN 'San Francisco 49ers'     THEN '49ers'
            WHEN 'Tennessee Titans'        THEN 'Titans'
            WHEN 'New York Jets'           THEN 'Jets'
            WHEN 'Miami Dolphins'          THEN 'Dolphins'
            WHEN 'New Orleans Saints'      THEN 'Saints'
            WHEN 'Kansas City Chiefs*'     THEN 'Chiefs'
            WHEN 'New England Patriots'    THEN 'Patriots'
            WHEN 'Philadelphia Eagles*'    THEN 'Eagles'
            WHEN 'Cincinnati Bengals'      THEN 'Bengals'
            WHEN 'Buffalo Bills*'          THEN 'Bills'
            WHEN 'Houston Texans*'         THEN 'Texans'
            WHEN 'Indianapolis Colts'      THEN 'Colts'
            WHEN 'Chicago Bears'           THEN 'Bears'
            WHEN 'Los Angeles Rams*'       THEN 'Rams'
            WHEN 'Los Angeles Chargers+'   THEN 'Chargers'
            WHEN 'New York Giants'         THEN 'Giants'
            WHEN 'Tampa Bay Buccaneers*'   THEN 'Buccaneers'
            WHEN 'Las Vegas Raiders'       THEN 'Raiders'
            WHEN 'Green Bay Packers+'      THEN 'Packers'
            WHEN 'Cleveland Browns'        THEN 'Browns'
            WHEN 'Arizona Cardinals'       THEN 'Cardinals'
            WHEN 'Denver Broncos+'         THEN 'Broncos'
            WHEN 'Pittsburgh Steelers+'    THEN 'Steelers'
            WHEN 'Seattle Seahawks'        THEN 'Seahawks'
            WHEN 'Dallas Cowboys'          THEN 'Cowboys'
            WHEN 'Washington Commanders+'  THEN 'Commanders'
            WHEN 'Detroit Lions*'          THEN 'Lions'
            WHEN 'Jacksonville Jaguars'    THEN 'Jaguars'
            WHEN 'Carolina Panthers'       THEN 'Panthers'
            WHEN 'Baltimore Ravens*'       THEN 'Ravens'
            WHEN 'Minnesota Vikings+'      THEN 'Vikings'
            WHEN 'Atlanta Falcons'         THEN 'Falcons'
            ELSE tm
        END as team,
        w AS wins,
        l AS losses,
        w_l_ AS win_loose_ratio,
        pf,
        pa,
        pd,
        mov,
        sos,
        srs,
        osrs,
        dsrs

    from source

)

select * from renamed