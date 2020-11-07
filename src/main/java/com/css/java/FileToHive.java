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
		// start::itemDF:[item_name: string, item_desc: string ... 1 more field]
		/**
		 * +---------+------------+-----------------------------------------------------------------------------------------------------+
		 * |item_name| item_desc|is_active |
		 * +---------+------------+-----------------------------------------------------------------------------------------------------+
		 * | SKU0001| Sugar| 1.0|
		 */
		itemDF.show(true);

		// Action Call: For-Each:
		// itemDF.foreach(new LoopFunction());

		// Map: One-to-One Transformation call
		Dataset<String> mapDF1 = itemDF.map((Row r) -> r.getString(0), Encoders.STRING());
		System.out.println("start::mapDF1:" + mapDF1);
		mapDF1.show(true);

		Dataset<String> mapDF = itemDF.map(new MapFun(), Encoders.STRING());
		System.out.println("start::mapDF:" + mapDF);
		mapDF.show(true);
		// start::mapDF:[value: string]

		// Flat-Map: One-to-Many Transformation Call
		Dataset<String> flatMapDF = itemDF.flatMap(new FlatMapFun(), Encoders.STRING());
		System.out.println("start::flatMapDF:" + flatMapDF);
		flatMapDF.show(true);
		// start::flatMapDF:[value: string]

		Dataset<Row> filterDF = itemDF.filter(new FilterFun());
		System.out.println("start::filterDF:" + filterDF);
		filterDF.show(true);
		// start::filterDF:[item_name: string, item_desc: string ... 1 more field]

		// Action: Write
		itemDF.write().mode(SaveMode.Overwrite) // .sortBy("item_name") // .partitionBy("is_active")
				.json(props.getProperty("output.path"));
		System.out.println("start::itemDF done writing to path:" + props.getProperty("output.path"));
		// https://stackoverflow.com/questions/52799025/error-using-spark-save-does-not-support-bucketing-right-now

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
