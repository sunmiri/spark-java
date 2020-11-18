
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


echo "Submitting Spark Job KafkaToHive"

hadoop fs -mkdir -p /user/test/output/kafka/json/

/usr/hdp/current/spark2-client/bin/spark-submit \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--executor-memory 1G \
--driver-memory 1G \
--executor-cores 1 \
--class com.css.java.SSReadFromKafka \
../bin/spark-java-0.0.1-SNAPSHOT.jar kafka2hive.properties


hadoop fs -ls /user/test/output/kafka/json/