#SSH to WebClient from Welcome/Launch

#As HDFS user
sudo -su hdfs
hadoop fs -mkdir -p /user/test/
hadoop fs -chmod -R 777 /user/test/
exit

#As root user
hadoop fs -mkdir -p /user/test/input/csv/
#copy the csv file to sandbox
hadoop fs -put ./item.csv /user/test/input/csv/

export SPARK_MAJOR_VERSION=2
export HADOOP_YARN_HOME=/usr/hdp/3.0.1.0-187/hadoop-yarn                                                                 
export JAVA_HOME=/usr/lib/jvm/java                                                                                       
export HADOOP_HOME=/usr/hdp/3.0.1.0-187/hadoop
export SPARK_HOME=/usr/hdp/3.0.1.0-187/spark2