ADD JAR CountsUDAF-0.0.1.jar;
CREATE TEMPORARY FUNCTION left AS 'edu.appstate.kepplemr.hivecounts.LeftNeighbors';

INSERT OVERWRITE DIRECTORY '${output}'
SELECT word, count(1) AS count
FROM
(
    SELECT left(sentence, 'Appalachian') as word
	FROM corpora
	WHERE left(sentence, 'Appalachian') IS NOT NULL;
) w 
GROUP BY word ORDER BY count DESC, word ASC;


