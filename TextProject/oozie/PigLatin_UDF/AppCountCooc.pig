REGISTER '$lib/PigCounts-1.0.jar';
DEFINE countCooc edu.appstate.kepplemr.pigcounts.Cooccurrences();

A = load '$input' AS (sent:chararray);
B = FOREACH A GENERATE countCooc(sent,'appalachian') AS sent;
C = FOREACH B generate flatten(TOKENIZE((chararray)$0)) as word;
D = group C by word;
E = foreach D generate COUNT(C), group;
store E into '$output/pigUDFCooc';