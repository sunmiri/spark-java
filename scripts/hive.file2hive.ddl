drop table default.file2hive;

CREATE EXTERNAL TABLE `default.file2hive`(`itemName` string, `itemDesc` string, `isActive` string) PARTITIONED BY (`createdDate` string) LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/user/test/output/json/';

msck repair table default.file2hive; 

select * from default.file2hive; 
