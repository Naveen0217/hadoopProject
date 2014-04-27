A = load '/user/sqoop/sqoopOut/eng_wikipedia_2010_1M/*';
B = foreach A generate LOWER(REGEX_EXTRACT($0, '(?<= [aA]ppalachian )(\\w+)', 1)) as word;
C = FILTER B BY word MATCHES '\\w+';
D = GROUP C By word;
E = foreach D generate COUNT(C), group;
store E into '$output/pigRight';