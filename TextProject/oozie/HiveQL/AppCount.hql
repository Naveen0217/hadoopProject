DROP TABLE IF EXISTS wc;
CREATE TABLE wc AS SELECT word, count(1) AS count
FROM 
(
  SELECT EXPLODE(SPLIT(LOWER(sent), ' '))
  AS word
  FROM
  (
    SELECT sentence AS sent
    FROM corpora
    WHERE INSTR(LOWER(REGEXP_REPLACE(sentence,'[\\p{Punct},\\p{Cntrl}]','')), "appalachian") > 0
  ) xxx
) w GROUP BY word ORDER BY count DESC, word ASC;
SELECT word,count FROM wc
WHERE INSTR(LOWER(word), "appalachian") != 0;