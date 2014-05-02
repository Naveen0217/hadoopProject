REGISTER '$lib/PigCounts-1.0.jar';
DEFINE LeftNeighbors edu.appstate.kepplemr.pigcounts.LeftNeighbors();

A = load '$input' AS (sent:chararray);
B = FOREACH A GENERATE LeftNeighbors(sent,'appalachian') AS word;
C = group B by word;
D = foreach C generate COUNT(B), group;
store D into '$output/pigUDFLeft';