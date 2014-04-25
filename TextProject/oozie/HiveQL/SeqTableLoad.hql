CREATE EXTERNAL TABLE IF NOT EXISTS sequencefile_table (sentence STRING) stored as sequencefile;
LOAD DATA INPATH "/user/michael/seqout" INTO TABLE sequencefile_table;
SELECT * FROM sequencefile_table;