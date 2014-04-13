ADD JAR ./target/CountsUDAF-0.0.1.jar;
CREATE TEMPORARY FUNCTION left AS 'edu.appstate.kepplemr.hivecounts.LeftNeighbors';

-- SELECT left(sentence, 'Appalachian') as word
-- FROM corpora
-- WHERE left(sentence, 'Appalachian') IS NOT NULL;

DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM
(
  SELECT word
  FROM
  (
    SELECT left(sentence, 'appalachian')
    AS word
    FROM corpora
    WHERE left(sentence, 'appalachian') IS NOT NULL
  ) m
) k GROUP BY word ORDER BY count DESC, word ASC;
SELECT * from wc;
