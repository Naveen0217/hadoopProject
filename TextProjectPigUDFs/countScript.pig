REGISTER ./target/Counts-0.0.1.jar;
DEFINE LeftNeighbors edu.appstate.kepplemr.pigcounts.LeftNeighbors();
DEFINE RightNeighbors edu.appstate.kepplemr.pigcounts.RightNeighbors();
DEFINE Coocurrences edu.appstate.kepplemr.pigcounts.Coocurrences();

-- Compute left neighbors
A = load '/user/michael/sqoopText/eng_wikipedia_2010_1M/*' AS (sent:chararray);
B = FOREACH A GENERATE LeftNeighbors(sent,'appalachian') AS word;
C = group B by word;
D = foreach C generate COUNT(B), group;
store D into './wordcount/left';

-- Compute right neighbors
A = load '/user/michael/sqoopText/eng_wikipedia_2010_1M/*' AS (sent:chararray);
B = FOREACH A GENERATE RightNeighbors(sent,'appalachian') AS word;
C = group B by word;
D = foreach C generate COUNT(B), group;
store D into './wordcount/right';

-- Compute co-occurences
A = load '/user/michael/sqoopText/eng_wikipedia_2010_1M/*' AS (sent:chararray);
B = FOREACH A GENERATE LeftNeighbors(sent,'appalachian') AS sent;
C = FOREACH B generate flatten(TOKENIZE((chararray)$0)) as word;
D = group C by word;
E = foreach D generate COUNT(C), group;
store E into './wordcount/co';
