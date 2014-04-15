DROP TABLE corpora;
CREATE EXTERNAL TABLE corpora 
(
  sentence STRING
)
row format delimited fields terminated by ','
location '/user/sqoop/output/eng_wikipedia_2010_1M';
