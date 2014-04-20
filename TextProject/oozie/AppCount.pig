A = load '$input';
B = foreach A generate flatten(TOKENIZE((chararray)LOWER($0))) as (word:chararray);
C = FILTER B BY word MATCHES 'appalachian';
D = group C by word;
E = foreach D generate COUNT(C), group;
store E into '$output';
