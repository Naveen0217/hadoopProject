ADD JAR ${lib}/CountsUDAF-0.0.1.jar;
CREATE TEMPORARY FUNCTION right AS 'edu.appstate.kepplemr.hivecounts.RightNeighbors';

DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM
(
    SELECT right(sentence, 'Appalachian') as word
	FROM corpora
	WHERE right(sentence, 'Appalachian') IS NOT NULL
) w 
GROUP BY word ORDER BY count DESC, word ASC;
INSERT OVERWRITE DIRECTORY '${output}/hiveCountUDFRight'
SELECT word,count FROM wc;