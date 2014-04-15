DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM 
(
  SELECT LOWER(SPLIT(SUBSTR(sent, INSTR(LOWER(sent), "appalachian")), ' ')[1])
  AS word
  FROM
  (
    SELECT sentence AS sent
    FROM corpora
    WHERE INSTR(LOWER(sentence), "appalachian") > 0
  ) x
) w GROUP BY word ORDER BY count DESC, word ASC;
SELECT * FROM wc;
