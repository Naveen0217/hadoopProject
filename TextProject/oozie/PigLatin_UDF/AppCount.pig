REGISTER '$lib/PigCounts-1.0.jar';

A = load '$input' USING edu.appstate.kepplemr.pigcounts.CountWord(' appalachian ') AS (num:int);
B = GROUP A BY num;
C = FOREACH B GENERATE SUM(A.num);
store C into '$output/pigUDFCount';