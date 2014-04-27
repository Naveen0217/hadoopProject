ADD JAR ${lib}/HiveCounts-1.0.jar;
CREATE TEMPORARY FUNCTION cooc AS 'edu.appstate.kepplemr.hivecounts.Cooccurrences';
DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM 
(
  SELECT EXPLODE(SPLIT(sent, ' '))
  AS word
  FROM
  (
    SELECT cooc(sentence, 'Appalachian') 
    AS sent
    FROM corpora
  ) x
) w GROUP BY word ORDER BY count DESC, word ASC;
INSERT OVERWRITE DIRECTORY '${output}/hiveUDFCooc'
SELECT word,count FROM wc
WHERE word != "";