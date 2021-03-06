package com.css.java;

import java.io.FileReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.css.java.functions.ForEachRDD;
import com.css.java.functions.MapFun2;

public class DSKafkaToHive implements Serializable {

	Properties props;
	SparkConf conf;
	JavaStreamingContext jssc;
	SparkSession spark;
	Broadcast<Dataset<Row>> usersDFBC;

	public DSKafkaToHive(String arg) {
		try {
			props = new Properties();
			props.load(new FileReader(arg));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("DSKafkaToHive::props:" + props);

		spark = SparkSession.builder().appName(props.getProperty("app.name")).enableHiveSupport().getOrCreate();
		conf = spark.sparkContext().conf();
		jssc = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext()),
				Durations.seconds(Integer.parseInt(props.getProperty("spark.batch.duration.secs", "5"))));

		Dataset<Row> usersDF = this.spark.read().format(props.getProperty("input.file.format"))
				.option("sep", props.getProperty("input.file.seperator"))
				.option("inferSchema", props.getProperty("input.file.inferSchema"))
				.option("header", props.getProperty("input.file.with_header"))
				.load(props.getProperty("input.file.name"));
		System.out.println("DSKafkaToHive::usersDF:" + usersDF);
		usersDFBC = jssc.sparkContext().broadcast(usersDF);
		System.out.println("DSKafkaToHive::Added usersDF to Broadcast::" + usersDFBC);
	}

	public void start() throws Exception {
		System.out.println("start");

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", props.getProperty("app.name"));
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList(props.getProperty("kafka.topic.list").split(","));

		JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		System.out.println("start::dstream:" + dstream);

		JavaDStream<String> jds = dstream.map(new MapFun2());
		System.out.println("start::jds:" + jds);

		jds.foreachRDD(new ForEachRDD(this.props, this.usersDFBC));

		jssc.start();
		jssc.awaitTermination();
	}

	public void stop() throws Exception {
		System.out.println("stop");
		jssc.stop(true, true);
	}

	public static void main(String[] args) throws Exception {
		DSKafkaToHive k2h = new DSKafkaToHive(args[0]);
		k2h.start();
	}
}
