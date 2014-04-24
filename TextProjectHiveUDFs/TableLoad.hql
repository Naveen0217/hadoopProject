DROP TABLE corpora;
CREATE EXTERNAL TABLE corpora 
(
  sentence STRING
)
row format delimited fields terminated by ','
location 'hdfs://mothership:8020/user/sqoop/sqoopOut/eng_wikipedia_2010_1M';
