REGISTER '$lib/PigCounts-1.0.jar';

A = load '$input' USING edu.appstate.kepplemr.pigcounts.LoadCooc('appalachian') AS (sent:chararray);
B = FOREACH A generate flatten(TOKENIZE((chararray)$0)) as word;
C = group B by word;
D = foreach C generate COUNT(B), group;
store D into '$output/pigUDFLoadCooc';