package com.css.java;

import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.css.java.functions.FilterFun;
import com.css.java.functions.FlatMapFun;
import com.css.java.functions.LoopFunction;
import com.css.java.functions.MapFun;

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
	// JavaSparkContext jsc;
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
		// this.jsc = new JavaSparkContext(spark.sparkContext());
	}

	public void start() throws Exception {
		Dataset<Row> itemDF = this.spark.read().format(props.getProperty("input.file.format"))
				.option("sep", props.getProperty("input.file.seperator"))
				.option("inferSchema", props.getProperty("input.file.inferSchema"))
				.option("header", props.getProperty("input.file.with_header"))
				.load(props.getProperty("input.file.name"));
		System.out.println("start::itemDF:" + itemDF);
		itemDF.show(true);

		// Action Call: For-Each:
		itemDF.foreach(new LoopFunction());

		// Map: One-to-One Transformation call
		Dataset<Row> mapDF = itemDF.map(new MapFun(), Encoders.STRING());
		System.out.println("start::mapDF:" + mapDF);

		// Flat-Map: One-to-Many Transformation Call
		Dataset<Row> flatMapDF = itemDF.flatMap(new FlatMapFun(), Encoders.STRING());
		System.out.println("start::flatMapDF:" + flatMapDF);

		Dataset<Row> filterDF = itemDF.filter(new FilterFun());
		System.out.println("start::filterDF:" + filterDF);

		// Action: Write
		itemDF.write().mode(SaveMode.Overwrite) // .sortBy("item_name") // .partitionBy("is_active")
				.json(props.getProperty("output.path"));
		System.out.println("start::itemDF done writing to path:" + props.getProperty("output.path"));
		//https://stackoverflow.com/questions/52799025/error-using-spark-save-does-not-support-bucketing-right-now

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
