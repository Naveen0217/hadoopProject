ADD JAR ${lib}/HiveCounts-1.0.jar;
CREATE TEMPORARY FUNCTION countWord AS 'edu.appstate.kepplemr.hivecounts.CountWord';

INSERT OVERWRITE DIRECTORY '${output}/hiveUDFCount'
SELECT 'Appalachian', SUM(word)
FROM
(
  SELECT countWord(sentence, 'Appalachian') as word
  FROM corpora
) w