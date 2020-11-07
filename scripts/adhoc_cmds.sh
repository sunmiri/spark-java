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
