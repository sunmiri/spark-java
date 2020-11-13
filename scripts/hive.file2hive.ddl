drop table default.file2hive;

CREATE EXTERNAL TABLE `default.file2hive`(`itemName` string, `itemDesc` string, `isActive` string) PARTITIONED BY (`createdDate` string) LOCATION 'hdfs://user/test/output/json/';

msck repair table default.file2hive; 