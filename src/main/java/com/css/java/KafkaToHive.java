package com.css.java;

import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.css.java.functions.FilterFun;
import com.css.java.functions.FlatMapFun;

public class KafkaToHive implements Serializable {
	SparkSession spark;
	Properties props;

	public KafkaToHive(String arg) {
		try {
			props = new Properties();
			props.load(new FileReader(arg));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("KafkaToHive::props:" + props);
		this.spark = SparkSession.builder().appName(props.getProperty("app.name")).enableHiveSupport().getOrCreate();
	}

	public void start() throws Exception {
		System.out.println("start");
		Dataset<Row> kafkaDF = this.spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", props.getProperty("kafka.bootstrap.servers"))
				.option("subscribe", props.getProperty("kafka.topic.list")).load();
		// "CAST(key AS STRING)",

		StructType struct1 = new StructType(
				new StructField[] { new StructField("item_name", DataTypes.StringType, false, Metadata.empty()),
						new StructField("item_desc", DataTypes.StringType, false, Metadata.empty()),
						new StructField("is_active", DataTypes.IntegerType, false, Metadata.empty()) });

		StructType struct2 = new StructType(
				new StructField[] { new StructField("item_name", DataTypes.StringType, false, Metadata.empty()),
						new StructField("item_desc", DataTypes.StringType, false, Metadata.empty()),
						new StructField("is_active", DataTypes.IntegerType, false, Metadata.empty()) });

		kafkaDF.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());
		// kafkaDF = kafkaDF.selectExpr("CAST(value AS
		// STRING)").select(from_json(("value"), struct1).as("data"))
		// .select("data.*");
		// kafkaDF.foreach(new LoopFunction());

		//Dataset<String> flatMapDF = kafkaDF.flatMap(new FlatMapFun(), Encoders.STRING());
		//Dataset<Row> filterDF = kafkaDF.filter(new FilterFun());
		// filterDF.write().mode(SaveMode.Append).json(props.getProperty("output.path"));
		// Exception in thread "main" org.apache.spark.sql.AnalysisException: Complete
		// output mode not supported when there are no streaming aggregations on
		// streaming DataFrames/Datasets;;
		//filterDF.writeStream().outputMode("append").format("console").start().awaitTermination();
		// Caused by: java.lang.ClassCastException:
		// org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema cannot be cast
		// to java.lang.String
	}

	public void stop() throws Exception {
		System.out.println("stop");
		this.spark.stop();
	}

	public static void main(String[] args) throws Exception {
		KafkaToHive k2h = new KafkaToHive(args[0]);
		k2h.start();
		k2h.stop();
	}
}
