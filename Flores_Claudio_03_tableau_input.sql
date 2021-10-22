
# Selects lineups used in every game

SET @sql = NULL;
SET @team_id = 19584; #NYCFC team id

SELECT GROUP_CONCAT(DISTINCT CONCAT('SUM(IF(Position=''',Position,''',1,0)) ',Position)) 
INTO @sql
FROM whoscored.game_stats 
WHERE Team_ID = @team_id 
AND N BETWEEN 1 AND 11;

SET @sql = CONCAT('CREATE TEMPORARY TABLE whoscored.lineup SELECT ', @sql,' FROM whoscored.game_stats WHERE Team_ID = ',@team_id,' AND N BETWEEN 1 AND 11 GROUP BY GameID');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# Groups most used lineups
SET @sql =  (SELECT GROUP_CONCAT(DISTINCT CONCAT(Position)) 
			FROM whoscored.game_stats 
			WHERE Team_ID = @team_id 
			AND N BETWEEN 1 AND 11);   
            
SET @sql = CONCAT('CREATE TABLE whoscored.lineup_grouped SELECT ',@sql,',count(*) q, ROW_NUMBER() OVER (ORDER BY count(*) desc) q_order FROM whoscored.lineup GROUP BY ',@sql);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# Count of positions played by players
CREATE TEMPORARY TABLE whoscored.pos_players
SELECT Player_ID, 
	Position, 
    COUNT(*) Q, 
    ROW_NUMBER()  OVER (PARTITION BY Position ORDER BY COUNT(*) desc) pos_order,
    ROW_NUMBER() OVER (PARTITION BY Player_ID ORDER BY COUNT(*) desc) ply_order
FROM whoscored.game_stats
WHERE Team_ID = @team_id
	AND N between 1 and 11
GROUP BY Player_ID, Position;


# Pivot table of most common lineup
CREATE TEMPORARY TABLE whoscored.main_lineup (Position varchar(5), Q int);

SELECT GROUP_CONCAT('(''',Position,''',(SELECT ',Position,' FROM whoscored.lineup_grouped WHERE q_order = 1))')
INTO @sql
FROM  whoscored.pos_players
WHERE pos_order = 1;

SET @sql = CONCAT('INSERT INTO whoscored.main_lineup VALUES ',@sql,';');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# Stats of all players
CREATE TEMPORARY TABLE whoscored.player_summary (Player_ID int, Position varchar(5), Games int, 
	ShotsTotal float, ShotOnTarget float, KeyPassTotal float, PassSuccessInMatch float, DuelAerialWon float, Touches float, rating float, TackleWonTotal float, ClearanceTotal float, ShotBlocked float, 
    FoulCommitted float, OffsideGiven float, TurnOver float, Dispossessed float, TotalPasses float, DribbleWon float,	PassCrossTotal float, PassCrossAccurate float, PassLongBallTotal float, PassLongBallAccurate float, 
    PassThroughBallTotal float, PassThroughBallAccurate float, InterceptionAll float,
	Percentile_ShotsTotal float, Percentile_ShotOnTarget float, Percentile_KeyPassTotal float, Percentile_PassSuccessInMatch float, Percentile_DuelAerialWon float, Percentile_Touches float, Percentile_rating float, 
    Percentile_TackleWonTotal float, Percentile_ClearanceTotal float, Percentile_ShotBlocked float, Percentile_FoulCommitted float, Percentile_OffsideGiven float, Percentile_TurnOver float, 
    Percentile_Dispossessed float, Percentile_TotalPasses float, Percentile_DribbleWon float, Percentile_PassCrossTotal float, Percentile_PassCrossAccurate float, Percentile_PassLongBallTotal float, Percentile_PassLongBallAccurate float, 
    Percentile_PassThroughBallTotal float, Percentile_PassThroughBallAccurate float, Percentile_InterceptionAll float);

INSERT INTO whoscored.player_summary 
	(Player_ID, Position, Games, ShotsTotal, Percentile_ShotsTotal, ShotOnTarget, Percentile_ShotOnTarget, KeyPassTotal, Percentile_KeyPassTotal, PassSuccessInMatch, Percentile_PassSuccessInMatch, 
    DuelAerialWon, Percentile_DuelAerialWon, Touches, Percentile_Touches, rating, Percentile_rating, TackleWonTotal, Percentile_TackleWonTotal, InterceptionAll, Percentile_InterceptionAll, 
    ClearanceTotal, Percentile_ClearanceTotal, ShotBlocked, Percentile_ShotBlocked, FoulCommitted, Percentile_FoulCommitted, OffsideGiven, Percentile_OffsideGiven, TurnOver, Percentile_TurnOver, 
    Dispossessed, Percentile_Dispossessed, TotalPasses, Percentile_TotalPasses, DribbleWon, Percentile_DribbleWon, PassCrossTotal, Percentile_PassCrossTotal, PassCrossAccurate, Percentile_PassCrossAccurate, PassLongBallTotal, Percentile_PassLongBallTotal, 
    PassLongBallAccurate, Percentile_PassLongBallAccurate, PassThroughBallTotal, Percentile_PassThroughBallTotal, PassThroughBallAccurate, Percentile_PassThroughBallAccurate)
SELECT Player_ID, Position,	COUNT(*) Games,	
	AVG(ShotsTotal) ShotsTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotsTotal) ASC) Percentile_ShotsTotal,
    AVG(ShotOnTarget) ShotOnTarget, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotOnTarget) ASC) Percentile_ShotOnTarget,
    AVG(KeyPassTotal) KeyPassTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(KeyPassTotal) ASC) Percentile_KeyPassTotal,
    round(SUM(TotalPasses*PassSuccessInMatch/100),0)/sum(TotalPasses) PassSuccessInMatch, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY round(SUM(TotalPasses*PassSuccessInMatch/100),0)/sum(TotalPasses) ASC) Percentile_PassSuccessInMatch,
    AVG(DuelAerialWon) DuelAerialWon, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(DuelAerialWon) ASC) Percentile_DuelAerialWon,
    AVG(Touches) Touches, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(Touches) ASC) Percentile_Touches,
    AVG(rating) rating, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(rating) ASC) Percentile_rating,
    AVG(TackleWonTotal) TackleWonTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TackleWonTotal) ASC) Percentile_TackleWonTotal,
    AVG(InterceptionAll) InterceptionAll, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(InterceptionAll) ASC) Percentile_InterceptionAll,
    AVG(ClearanceTotal) ClearanceTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ClearanceTotal) ASC) Percentile_ClearanceTotal,
    AVG(ShotBlocked) ShotBlocked, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(ShotBlocked) ASC) Percentile_ShotBlocked,
    AVG(FoulCommitted) FoulCommitted, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(FoulCommitted) ASC) Percentile_FoulCommitted,
    AVG(OffsideGiven) OffsideGiven, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(OffsideGiven) ASC) Percentile_OffsideGiven,
    AVG(TurnOver) TurnOver, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TurnOver) ASC) Percentile_TurnOver,
    AVG(Dispossessed) Dispossessed, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(Dispossessed) ASC) Percentile_Dispossessed,
    AVG(TotalPasses) TotalPasses, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(TotalPasses) ASC) Percentile_TotalPasses,
    AVG(DribbleWon) DribbleWon, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(DribbleWon) ASC) Percentile_DribbleWon,
    AVG(PassCrossTotal) PassCrossTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassCrossTotal) ASC) Percentile_PassCrossTotal,
    AVG(PassCrossAccurate) PassCrossAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassCrossAccurate) ASC) Percentile_PassCrossAccurate,
    AVG(PassLongBallTotal) PassLongBallTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassLongBallTotal) ASC) Percentile_PassLongBallTotal,
    AVG(PassLongBallAccurate) PassLongBallAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassLongBallAccurate) ASC) Percentile_PassLongBallAccurate,
    AVG(PassThroughBallTotal) PassThroughBallTotal, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassThroughBallTotal) ASC) Percentile_PassThroughBallTotal,
    AVG(PassThroughBallAccurate) PassThroughBallAccurate, PERCENT_RANK() OVER (PARTITION BY Position ORDER BY AVG(PassThroughBallAccurate) ASC) Percentile_PassThroughBallAccurate
FROM whoscored.game_stats
WHERE N BETWEEN 1 AND 11
	AND Position <> ''
GROUP BY Player_ID, Position;
 
    
# Selection of most common players used in main lineup with their stats
CREATE TEMPORARY TABLE whoscored.main_lineup_stats
SELECT t1.Position, t2.Player_ID, t3.Games, 
	t3.ShotsTotal, t3.ShotOnTarget, t3.KeyPassTotal, t3.PassSuccessInMatch, t3.DuelAerialWon, t3.Touches, t3.rating, t3.TackleWonTotal, t3.InterceptionAll, t3.ClearanceTotal, t3.ShotBlocked, t3.FoulCommitted, t3.OffsideGiven, 
    t3.TurnOver, t3.Dispossessed, t3.TotalPasses, t3.PassCrossTotal, t3.PassCrossAccurate, t3.PassLongBallTotal, t3.PassLongBallAccurate, t3.PassThroughBallTotal, t3.PassThroughBallAccurate,
    t3.Percentile_ShotsTotal, t3.Percentile_ShotOnTarget, t3.Percentile_KeyPassTotal, t3.Percentile_PassSuccessInMatch, t3.Percentile_DuelAerialWon, t3.Percentile_Touches, t3.Percentile_rating, t3.Percentile_TackleWonTotal, 
    t3.Percentile_InterceptionAll, t3.Percentile_ClearanceTotal, t3.Percentile_ShotBlocked, t3.Percentile_FoulCommitted, t3.Percentile_OffsideGiven, t3.Percentile_TurnOver, t3.Percentile_Dispossessed, t3.Percentile_TotalPasses, 
    t3.Percentile_PassCrossTotal, t3.Percentile_PassCrossAccurate, t3.Percentile_PassLongBallTotal, t3.Percentile_PassLongBallAccurate, t3.Percentile_PassThroughBallTotal, t3.Percentile_PassThroughBallAccurate
FROM whoscored.main_lineup t1
JOIN whoscored.pos_players t2
	ON t1.Position = t2.Position
JOIN whoscored.player_summary t3
	ON t2.Player_ID = t3.Player_ID
    AND t2.Position = t3.Position
WHERE t1.Q > 0
	AND t2.pos_order <= t1.Q
ORDER BY t1.Position;


# Table with relevant kpi per position
CREATE TEMPORARY TABLE whoscored.kpis_position (Position varchar(5), N_KPI varchar(6), KPI varchar(50));
INSERT INTO whoscored.kpis_position VALUES ('GK','KPI_1','Percentile_PassSuccessInMatch'),('GK','KPI_2','Percentile_DuelAerialWon'),('GK','KPI_3','Percentile_ClearanceTotal'),('GK','KPI_4','Percentile_PassLongBAllAccurate'),('GK','KPI_5','Percentile_TotalPasses'),('GK','KPI_6','Percentile_Touches');
INSERT INTO whoscored.kpis_position VALUES ('DC','KPI_1','Percentile_PassLongBAllAccurate'),('DC','KPI_2','Percentile_DuelAerialWon'),('DC','KPI_3','Percentile_ClearanceTotal'),('DC','KPI_4','Percentile_TackleWonTotal'),('DC','KPI_5','Percentile_ShotBlocked'),('DC','KPI_6','Percentile_FoulCommitted');
INSERT INTO whoscored.kpis_position VALUES ('DL','KPI_1','Percentile_PassLongBAllAccurate'),('DL','KPI_2','Percentile_PassCrossTotal'),('DL','KPI_3','Percentile_DribbleWon'),('DL','KPI_4','Percentile_TackleWonTotal'),('DL','KPI_5','Percentile_InterceptionAll'),('DL','KPI_6','Percentile_FoulCommitted');
INSERT INTO whoscored.kpis_position VALUES ('DR','KPI_1','Percentile_PassLongBAllAccurate'),('DR','KPI_2','Percentile_PassCrossTotal'),('DR','KPI_3','Percentile_DribbleWon'),('DR','KPI_4','Percentile_TackleWonTotal'),('DR','KPI_5','Percentile_InterceptionAll'),('DR','KPI_6','Percentile_FoulCommitted');
INSERT INTO whoscored.kpis_position VALUES ('DMC','KPI_1','Percentile_PassSuccessInMatch'),('DMC','KPI_2','Percentile_InterceptionAll'),('DMC','KPI_3','Percentile_TackleWonTotal'),('DMC','KPI_4','Percentile_PassLongBAllAccurate'),('DMC','KPI_5','Percentile_TotalPasses'),('DMC','KPI_6','Percentile_Dispossessed');
INSERT INTO whoscored.kpis_position VALUES ('AMC','KPI_1','Percentile_PassSuccessInMatch'),('AMC','KPI_2','Percentile_KeyPassTotal'),('AMC','KPI_3','Percentile_PassCrossTotal'),('AMC','KPI_4','Percentile_DribbleWon'),('AMC','KPI_5','Percentile_PassThroughBallTotal'),('AMC','KPI_6','Percentile_ShotsTotal');
INSERT INTO whoscored.kpis_position VALUES ('AML','KPI_1','Percentile_PassSuccessInMatch'),('AML','KPI_2','Percentile_KeyPassTotal'),('AML','KPI_3','Percentile_PassCrossTotal'),('AML','KPI_4','Percentile_DribbleWon'),('AML','KPI_5','Percentile_PassThroughBallTotal'),('AML','KPI_6','Percentile_ShotsTotal');
INSERT INTO whoscored.kpis_position VALUES ('AMR','KPI_1','Percentile_PassSuccessInMatch'),('AMR','KPI_2','Percentile_KeyPassTotal'),('AMR','KPI_3','Percentile_PassCrossTotal'),('AMR','KPI_4','Percentile_DribbleWon'),('AMR','KPI_5','Percentile_PassThroughBallTotal'),('AMR','KPI_6','Percentile_ShotsTotal');
INSERT INTO whoscored.kpis_position VALUES ('FW','KPI_1','Percentile_PassSuccessInMatch'),('FW','KPI_2','Percentile_ShotsTotal'),('FW','KPI_3','Percentile_ShotOnTarget'),('FW','KPI_4','Percentile_KeyPassTotal'),('FW','KPI_5','Percentile_DuelAerialWon'),('FW','KPI_6','Percentile_DribbleWon');


#Selects main kpis per position for all players, using previous table
CREATE TABLE whoscored.kpis_position_main6 (Player_ID int, N int, Position varchar(5), Games int, KPI_1 float, KPI_2 float, KPI_3 float, KPI_4 float, KPI_5 float, KPI_6 float);

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'GK';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 1, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','GK',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 2, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DL';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 3, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DL',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DR';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 4, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DR',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'DMC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 5, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','DMC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AMC';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 6, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AMC',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AML';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 7, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AML',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'AMR';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 8, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','AMR',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT GROUP_CONCAT(KPI,' AS ',N_KPI) INTO @sql FROM whoscored.kpis_position WHERE Position = 'FW';
SET @sql = CONCAT('INSERT INTO whoscored.kpis_position_main6 SELECT Player_ID, 9, Position, Games,',@sql,' FROM  whoscored.player_summary WHERE Position = ''','FW',''' and Games >= 5;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# Table with player names and teams
CREATE TEMPORARY TABLE whoscored.player_team (Player_ID int, Player_Name varchar(50), Team varchar(50), Date_Order int)
SELECT t2.Player_ID, t2.Player_Name, t2.Team, ROW_NUMBER() OVER (PARTITION BY t2.Player_ID ORDER BY t1.Date desc) Date_Order
from whoscored.games t1
join whoscored.game_stats  t2
	on t1.GameID = t2.GameID;


#Selects top 5 player replacements per player
CREATE TEMPORARY TABLE whoscored.player_replacements
SELECT t2.N, t2.Position, t2.Player_ID, t4.Player_ID Player_ID_rep, t4.Player_Name Player_Name_Rep, t4.Team Team_Rep, t3.Games Games_rep, 
	round(100*t3.KPI_1,0) KPI_1_rep, 
    round(100*t3.KPI_2,0) KPI_2_rep, 
    round(100*t3.KPI_3,0) KPI_3_rep, 
    round(100*t3.KPI_4,0) KPI_4_rep, 
    round(100*t3.KPI_5,0) KPI_5_rep, 
    round(100*t3.KPI_6,0) KPI_6_rep,
    round(100*(t3.KPI_1 + t3.KPI_2 + t3.KPI_3 + t3.KPI_4 + t3.KPI_5 + t3.KPI_6)/6,0) AVG_KPI_rep,
    (POWER(CASE WHEN t3.KPI_1<t2.KPI_1 THEN ABS(t3.KPI_1-t2.KPI_1) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_2<t2.KPI_2 THEN ABS(t3.KPI_2-t2.KPI_2) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_3<t2.KPI_3 THEN ABS(t3.KPI_3-t2.KPI_3) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_4<t2.KPI_4 THEN ABS(t3.KPI_4-t2.KPI_4) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_5<t2.KPI_5 THEN ABS(t3.KPI_5-t2.KPI_5) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_6<t2.KPI_6 THEN ABS(t3.KPI_6-t2.KPI_6) ELSE 0 END,2))/6 Difference,
    ROW_NUMBER() OVER (PARTITION BY t2.Position, t2.Player_ID ORDER BY (t3.KPI_1 + t3.KPI_2 + t3.KPI_3 + t3.KPI_4 + t3.KPI_5 + t3.KPI_6)/6 DESC, (POWER(CASE WHEN t3.KPI_1<t2.KPI_1 THEN ABS(t3.KPI_1-t2.KPI_1) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_2<t2.KPI_2 THEN ABS(t3.KPI_2-t2.KPI_2) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_3<t2.KPI_3 THEN ABS(t3.KPI_3-t2.KPI_3) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_4<t2.KPI_4 THEN ABS(t3.KPI_4-t2.KPI_4) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_5<t2.KPI_5 THEN ABS(t3.KPI_5-t2.KPI_5) ELSE 0 END,2) + POWER(CASE WHEN t3.KPI_6<t2.KPI_6 THEN ABS(t3.KPI_6-t2.KPI_6) ELSE 0 END,2))/6 ASC) Difference_Order
FROM whoscored.main_lineup_stats t1
JOIN whoscored.kpis_position_main6 t2 
	ON t1.Position = t2.Position
    AND t1.Player_ID = t2.Player_ID
LEFT JOIN whoscored.kpis_position_main6 t3
	ON t2.Position = t3.Position
    AND t2.Player_ID <> t3.Player_ID
LEFT JOIN whoscored.player_team t4
	ON t3.Player_ID = t4.Player_ID
where t4.Date_Order = 1;


# NYCFC main lineup with stats
CREATE TEMPORARY TABLE whoscored.player_final
SELECT t2.N, t2.Position, t2.Player_ID, t3.Player_Name, t3.Team Team_Rep, t2.Games,
	round(100*(t2.KPI_1),0) KPI_1,
    round(100*(t2.KPI_2),0) KPI_2, 
    round(100*(t2.KPI_3),0) KPI_3, 
    round(100*(t2.KPI_4),0) KPI_4, 
    round(100*(t2.KPI_5),0) KPI_5, 
    round(100*(t2.KPI_6),0) KPI_6,
    round(100*(t2.KPI_1 + t2.KPI_2 + t2.KPI_3 + t2.KPI_4 + t2.KPI_5 + t2.KPI_6)/6,0) AVG_KPI
FROM whoscored.main_lineup_stats t1
JOIN whoscored.kpis_position_main6 t2 
	ON t1.Position = t2.Position
    AND t1.Player_ID = t2.Player_ID
LEFT JOIN whoscored.player_team t3
	ON t2.Player_ID = t3.Player_ID
where t3.Date_Order = 1;


DROP TABLE whoscored.lineup_grouped;
DROP TABLE whoscored.kpis_position_main6;

SELECT *
FROM whoscored.kpis_position;

SELECT * 
FROM whoscored.player_replacements
WHERE Difference_Order <= 5
ORDER BY N, Player_ID, Difference_Order;

SELECT * 
FROM whoscored.player_final
ORDER BY N ASC;

