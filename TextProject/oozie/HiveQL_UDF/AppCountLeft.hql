ADD JAR ${lib}/HiveCounts-1.0.jar;
CREATE TEMPORARY FUNCTION left AS 'edu.appstate.kepplemr.hivecounts.LeftNeighbors';

DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM
(
    SELECT left(sentence, 'Appalachian') as word
	FROM corpora
	WHERE left(sentence, 'Appalachian') IS NOT NULL
) w 
GROUP BY word ORDER BY count DESC, word ASC;
INSERT OVERWRITE DIRECTORY '${output}/hiveCountUDFLeft'
SELECT word,count FROM wc;