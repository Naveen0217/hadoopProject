REGISTER '$lib/PigCounts-1.0.jar';
DEFINE rightNeighbors edu.appstate.kepplemr.pigcounts.RightNeighbors();

A = load '$input' AS (sent:chararray);
B = FOREACH A GENERATE rightNeighbors(sent,'appalachian') AS word;
C = group B by word;
D = foreach C generate COUNT(B), group;
store D into '$output/pigUDFRight';