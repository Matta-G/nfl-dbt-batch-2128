SELECT 
    *,
    CASE
        WHEN yardline <= 20
        AND yardlinedirection like '%OPP%' then true
        ELSE false
    END AS RedZone
FROM {{ ref('stg_nfl_dbt__raw_2024_play_by_play') }}