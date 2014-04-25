DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM 
(
  SELECT REVERSE(SPLIT(LTRIM(REVERSE(LOWER(SUBSTR(sent, 0, INSTR(LOWER(sent), "appalachian")-2)))), ' ')[0])
  AS word
  FROM
  (
    SELECT sentence AS sent
    FROM corpora
    WHERE INSTR(LOWER(sentence), "appalachian") > 0
  ) x
) w GROUP BY word ORDER BY count DESC, word ASC;
