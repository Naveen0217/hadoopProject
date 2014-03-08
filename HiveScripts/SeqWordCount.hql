DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count 
FROM (SELECT EXPLODE(SPLIT(LCASE(REGEXP_REPLACE(sentence,'[\\p{Punct},\\p{Cntrl}]','')),' '))
AS word FROM sequencefile_table) w GROUP BY word ORDER BY count DESC, word ASC;
SELECT * FROM wc;
