#Prepare

############# ON SANDBOX/REMOTE-SERVER/DRIVER-NODE #############
#As HDFS user (one-time)
#sudo -su hdfs
#hadoop fs -mkdir -p /user/test/
#hadoop fs -chmod -R 777 /user/test/
#exit

#As root user (every time file changes)
hadoop fs -mkdir -p /user/test/output/jdbc/json/

#Execute
export JAVA_HOME=/usr/bin/java

echo JAVA_HOME=$JAVA_HOME
export PATH=$PATH:$JAVA_HOME/bin/:
echo PATH=$PATH
which java
java -version

export SPARK_HOME="/usr/hdp/current/spark2-client"
export SPARK_MAJOR_VERSION=2
#export HADOOP_CONF_DIR="/usr/hdp/current/hadoop-client"


echo "Submitting Spark Job Jdbc2Hive"


/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--driver-memory 1G \
--executor-memory 1G \
--executor-cores 1 \
--class com.css.java.JdbcToHive \
../bin/spark-java-0.0.1-SNAPSHOT.jar jdbc2hive.properties


hadoop fs -ls /user/test/output/jdbc/json/