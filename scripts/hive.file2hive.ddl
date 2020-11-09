drop table default.file2hive;

CREATE EXTERNAL TABLE `default.file2hive`(
  `item_name` string,
  `item_desc` string,
  `is_active` string)
 #PARTITIONED BY (`created_date` string)
 LOCATION 'hdfs://user/test/output/json/';

msck repair table default.file2hive; 