SELECT 
    *
    ,CASE
        WHEN yardline <= 20
        AND yardlinedirection like '%OPP%' then true
        ELSE false
    END AS RedZone
    , IF (istouchdown = 1, 6, 0) as pts_td
    , IF (istwopointconversionsuccessful = 1, 2, 0) as pts_2pc
    , CASE 
    WHEN REGEXP_CONTAINS(UPPER(description), r'EXTRA POINT IS GOOD') THEN 1
    ELSE 0
END AS pt_extrapoint
    , CASE
    WHEN down != 0
       AND  SAFE_CAST(
      REGEXP_EXTRACT(
        description,
        r'FOR (\d+) YARDS'
      ) AS INT64
    ) >= togo
    THEN 1
    ELSE 0
  END AS is1stdown
  , CASE 
    WHEN ispass = 1 THEN 'pass'
    WHEN isrush = 1 THEN 'rush'
    ELSE 'special'
    END AS pass_rush

FROM {{ ref('stg_nfl_dbt__raw_2024_play_by_play') }}