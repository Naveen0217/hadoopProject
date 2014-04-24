DROP TABLE corpora;
CREATE EXTERNAL TABLE corpora 
(
  sentence STRING
)
row format delimited fields terminated by ','
location '${table}';
