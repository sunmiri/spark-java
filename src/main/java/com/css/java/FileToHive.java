package com.css.java;

import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark Batch Job Read File from specified location
 * 
 * @author sunilmiriyala
 * 
 *         More examples:
 *         https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples
 *         https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples/streaming
 *
 */
public class FileToHive implements Serializable {
	SparkSession spark;
	JavaSparkContext jsc;
	Properties props;

	FileToHive(String arg) {
		System.out.println("FileToHive::arg:" + arg);
		try {
			props = new Properties();
			props.load(new FileReader(arg));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("FileToHive::props:" + props);
		this.spark = SparkSession.builder().appName(props.getProperty("app.name")).enableHiveSupport().getOrCreate();
		this.jsc = new JavaSparkContext(spark.sparkContext());
	}

	public void start() throws Exception {
		Dataset<Row> itemDF = this.spark.read().format(props.getProperty("input.file.format"))
				.option("sep", props.getProperty("input.file.seperator"))
				.option("inferSchema", props.getProperty("input.file.inferSchema"))
				.option("header", props.getProperty("input.file.with_header"))
				.load(props.getProperty("input.file.name"));
		System.out.println("start::itemDF:" + itemDF);
		itemDF.show(true);
		itemDF.foreach(new ForeachFunction() {
			public void call(Object t) throws Exception {
				System.out.println("foreach::t:" + t);
			}
		});
	}

	public void stop() {
		this.spark.stop();
	}

	public static void main(String[] args) throws Exception {
		FileToHive f2h = new FileToHive(args[0]);
		f2h.start();
		f2h.stop();
	}
}
