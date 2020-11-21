############## KAFKA ##############
Topic List:
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --list

Create Topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --create --topic topic1 --partitions 2 --replication-factor 1
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --create --topic topic2 --partitions 2 --replication-factor 1

Publish Data:
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667 --topic topic1
#Enter following JSON text data after you see ">"
{"itemName":"apples", "itemDesc":"gala apples", "isActive":"1",     "createdDate":"2020-11-19T23.09:30"}
{"itemName":"bananas", "itemDesc":"yellow bananas", "isActive":"1", "createdDate":"2020-11-19T23.09:30"}


#CHeck Status
#List Spark Applications connected to Kafka.
/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667 --list

#Get details of an particular spark app like LAG's
/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --bootstrap-server sandbox-hdp.hortonworks.com:6667 --group atlas --describe
############## YARN ##############
#checking what spark applications are running?
yarn application -list
Application-Id,	    			Application-Name,	Application-Type,	User,	Queue,		State,		Final-State	 Progress,	Tracking-URL
application_1604702460659_0002,	"File-2-Hive",	    SPARK,	      		root,	default,	RUNNING,	UNDEFINED	 10%		http://sandbox-hdp.hortonworks.com:4040


#Get spark yarn logs
yarn logs -applicationId APP_ID > yarnlogs_file2hive.log
#yarn logs -applicationId application_1604702460659_0002 > yarnlogs_file2hive.log
vi yarnlogs_file2hive.log

#get spark driver logs
vi driverlogs_file2hive.log

############## SPARK-LAUNCH ##############

#Run program in background and redirect logs to a file
nohup sh runcmd_file2hive.sh > driverlogs_file2hive.log &

#Stop a running program
ps -aef | grep "spark-java-0.0.1-SNAPSHOT"
kill -9 <process-id>

#Incremental Builds
Change Code
Build Code
cd .../spark-java/
scp target/spark-java-0.0.1-SNAPSHOT.jar root@<virtualbox-ip>:/tmp/
ssh root@<virtualbox-ip>
[root@sandbox-host ~]# scp /tmp/spark-java-0.0.1-SNAPSHOT.jar root@<sandbox-ip>:/tmp/
ssh root@<sandbox-ip>
cd ~/test/scripts/
cp -fp /tmp/spark-java-0.0.1-SNAPSHOT.jar ../bin/
nohup sh runcmd_file2hive.sh > driverlogs_file2hive.log &
