/*

-- Liste des players_pk qui ont 2TM en nom d'équipe 

SELECT
    player_pk
FROM {{ ref('stg_nfl__players_defense_2024') }}
WHERE 
    team = '2TM'
*/

-- Exclure les lignes avec le détail des équipes pour les joueurs qui apparaissent plusieurs fois, afin de garder 1 ligne par joueur (celle avec '2TM' en équipe, les statistiques agrégées)

SELECT
    *
FROM {{ ref('stg_nfl__players_defense_2024') }}
WHERE 
    player_pk NOT IN ('DodsTy01','SmitPr00','SmitZa00','RobeRo00','RossJo01','JoneEr01','HolmJa01','WilsMa04','McClNi00','MayeMa00','WelcKr00','HerbKh00','FinlAJ00','HarrCh03','LongDa04','BrowBa01','UcheJo00','CartZa00','DaviKh00','JeffQu00','McMiRa02','WhitTr01','NgakYa00','BakeJe00','HollKa00','LattMa01','BowsTy00','EvanAk00','DaviJa11','PhilJo01','EdwaMi01','ForbEm00','AdamMy00','MathOc00','StilBe00','AdamJa00','AlexKw00','FlowTr01','GileJo01','MurpCa00','PittAn01','LeotEk00','McGiT.00')
    OR (player_pk IN ('DodsTy01','SmitPr00','SmitZa00','RobeRo00','RossJo01','JoneEr01','HolmJa01','WilsMa04','McClNi00','MayeMa00','WelcKr00','HerbKh00','FinlAJ00','HarrCh03','LongDa04','BrowBa01','UcheJo00','CartZa00','DaviKh00','JeffQu00','McMiRa02','WhitTr01','NgakYa00','BakeJe00','HollKa00','LattMa01','BowsTy00','EvanAk00','DaviJa11','PhilJo01','EdwaMi01','ForbEm00','AdamMy00','MathOc00','StilBe00','AdamJa00','AlexKw00','FlowTr01','GileJo01','MurpCa00','PittAn01','LeotEk00','McGiT.00') AND team = '2TM')