DROP TABLE corpora;
CREATE EXTERNAL TABLE corpora 
(
  sentence STRING
)
row format delimited fields terminated by ','
location '/user/sqoop/sqoopOut/eng_wikipedia_2010_1M';
