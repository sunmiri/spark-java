package com.css.java;

import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.css.java.bean.Items;

public class SSReadFromKafka implements Serializable {
	SparkSession spark;
	Properties props;

	public SSReadFromKafka(String arg) {
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
		// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
		Dataset<Row> kafkaDF = this.spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", props.getProperty("kafka.bootstrap.servers"))
				.option("subscribe", props.getProperty("kafka.topic.list")).load();
		// "CAST(key AS STRING)",
		// Exception in thread "main" org.apache.spark.sql.AnalysisException: Queries
		// with streaming sources must be executed with writeStream.start();;
		// DONT BREAK THE CHAIN WITH VOID FUNCTION CALL.
		// kafkaDF.show(true);

		StructType schema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("itemName", DataTypes.StringType, false),
						DataTypes.createStructField("itemDesc", DataTypes.StringType, false),
						DataTypes.createStructField("isActive", DataTypes.IntegerType, false),
						DataTypes.createStructField("createdDate", DataTypes.StringType, false) });

		Dataset<Items> itemsDF = kafkaDF
				.selectExpr("CAST(value AS STRING) as message").select(org.apache.spark.sql.functions
						.from_json(org.apache.spark.sql.functions.col("message"), schema).as("json"))
				.select("json.*").as(Encoders.bean(Items.class));

		// Exception in thread "main" org.apache.spark.sql.AnalysisException: Complete
		// output mode not supported when there are no streaming aggregations on
		// streaming DataFrames/Datasets;;
		itemsDF.writeStream().outputMode("append").format("console").start().awaitTermination();
		// Caused by: java.lang.ClassCastException:
		// org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema cannot be cast
		// to java.lang.String
	}

	public void stop() throws Exception {
		System.out.println("stop");
		this.spark.stop();
	}

	public static void main(String[] args) throws Exception {
		SSReadFromKafka k2h = new SSReadFromKafka(args[0]);
		k2h.start();
	}
}
