SELECT sentence as sent
FROM corpora
WHERE INSTR(LOWER(REGEXP_REPLACE(sentence,'[\\p{Punct},\\p{Cntrl}]','')), "appalachian") > 0;
