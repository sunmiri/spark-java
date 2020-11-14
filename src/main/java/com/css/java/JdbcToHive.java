package com.css.java;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.css.java.functions.LoopFunction;

/**
 * Spark Batch Job Read File from specified location
 * 
 * @author sunilmiriyala
 *
 */
public class JdbcToHive {
	SparkSession spark;
	Properties props;
	public static final String COMMA_DELIMITER = ",";
	public static final String COLON_DELIMITER = ":";
	public static final String SEMICOLON_DELIMITER = ";";
	public static final String SLASH = "/";
	public static final String DOT = ".";
	public static final String ATSIGN = "@";
	public static final String QUESTION_MARK = "?";
	public static final String EQUALS = "=";
	public static final String LESSTHANEQUALS = "<=";
	public static final String AMPERSAND = "&";
	public static final String SELECT = "SELECT ";
	public static final String ALL_COLUMNS = "*";
	public static final String FROM = " FROM ";
	public static final String WHERE = " WHERE ";
	public static final String JDBC = "jdbc";
	public static final String USER = "user";
	public static final String PASSWORD = "password";
	public static final String DBNAME = "databaseName";
	public static final String DATABASE = "DATABASE";

	public JdbcToHive(String arg) {
		// Load Properties File
		// Propetyfile: jdbc2hive.properties (add new)
		// Important Key-Value to be included in the property file
		// databasename,username,password,server,port,drivername
		// drivername for mysql: com.mysql.jdbc.Driver
		System.out.println("JdbcToHive::arg:" + arg);
		try {
			props = new Properties();
			props.load(new FileReader(arg));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("JdbcToHive::props:" + props);

		// Initialize SparkSession
		this.spark = SparkSession.builder().appName(props.getProperty("app.name")).enableHiveSupport().getOrCreate();
	}

	public void start() throws Exception {
		Dataset<Row> dbdata = readSourceData();
		dbdata.show(true);
		dbdata.foreach(new LoopFunction());
		// perform applicable filter. FilterFun (may have to change as per data inside
		// the table)
		// perform map MapFun (change logic as per your db columns). Create a POJO
		// matching your table
		// convert Row to POJO and return it.
		// write the new Dataset<POJO> to hdfs. Just like in File2Hive
		// create hive table on hdfs
	}

	public Dataset<Row> readSourceData() throws Exception {
		// Sql connection
		// use getConnectionString. It will return something like:
		// "jdbc:mysql://localhost:3306/<dbname>?user=someuser&password=somepassword;"
		// use that to establish connection

		try {
			ArrayList<String> al = new ArrayList<String>();
			String query = "select * from items where isactive=1"; // write your query here. //TODO change this
			Map<String, String> options = new HashMap<String, String>();
			options.put("driver", props.getProperty("dbdriver")); // TODO set this in property
			options.put("url", "jdbc:mysql://" + props.getProperty("dbhost") + ":" + props.getProperty("dbport") + "/"
					+ props.getProperty("dbname") + "?useSSL=false");
			// TODO set this in property
			options.put("dbtable", props.getProperty("dbtable", "items"));
			options.put("query", query);
			options.put("pushDownPredicate", "true");
			options.put("user", props.getProperty("dbuser")); // TODO set this in property
			options.put("password", props.getProperty("dbpassword")); // TODO set this in property
			System.out.println("readSourceData::connecting with options:" + options);
			Dataset<Row> jdbcDF = this.spark.read().format("jdbc").options(options).load();
			return jdbcDF;
		} catch (Throwable t) {
			System.out.println("Exception::" + t);
			t.printStackTrace();
		}
		return null;
	}

	private String getConnectionString(String protocolName, String hostName, String portNumber, String databaseName,
			String userName, String password) {
		StringBuffer connStringBuffer = new StringBuffer(JDBC);
		connStringBuffer.append(COLON_DELIMITER);
		connStringBuffer.append(protocolName);
		connStringBuffer.append(COLON_DELIMITER);
		connStringBuffer.append(SLASH);
		connStringBuffer.append(SLASH);
		connStringBuffer.append(hostName);
		connStringBuffer.append(COLON_DELIMITER);
		connStringBuffer.append(portNumber);
		connStringBuffer.append(SLASH);
		if (databaseName != null)
			connStringBuffer.append(databaseName);
		connStringBuffer.append(QUESTION_MARK);
		connStringBuffer.append(USER);
		connStringBuffer.append(EQUALS);
		connStringBuffer.append(userName);
		connStringBuffer.append(AMPERSAND);
		connStringBuffer.append(PASSWORD);
		connStringBuffer.append(EQUALS);
		connStringBuffer.append(password);
		return connStringBuffer.toString();
	}

	public void stop() {
		this.spark.stop();
	}

	public static void main(String[] args) throws Exception {
		JdbcToHive j2h = new JdbcToHive(args[0]);
		j2h.start();
		j2h.stop();
	}
}
