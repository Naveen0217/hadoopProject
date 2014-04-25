A = load '/user/sqoop/sqoopOut/eng_wikipedia_2010_1M/*' AS (sent:chararray);
B = FILTER A BY sent MATCHES '.*[aA]ppalachian.*';
C = foreach B generate flatten(TOKENIZE((chararray)LOWER($0))) as (word:chararray);
D = group C by word;
E = foreach D generate COUNT(C), group;
store E into './wordcount';