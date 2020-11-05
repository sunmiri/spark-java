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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.css.java.functions.FilterFun2;
import com.css.java.functions.FilterFun3;
import com.css.java.functions.ForVoidFun2;
import com.css.java.functions.ForVoidFun3;
import com.css.java.functions.MapFun2;

public class KafkaDStreamToHive implements Serializable {

	Properties props;
	SparkConf conf;
	JavaStreamingContext jssc;

	public KafkaDStreamToHive(String arg) {
		try {
			props = new Properties();
			props.load(new FileReader(arg));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("KafkaDStreamToHive::props:" + props);

		conf = new SparkConf().setAppName(props.getProperty("app.name"));
		jssc = new JavaStreamingContext(conf,
				Durations.seconds(Integer.parseInt(props.getProperty("spark.batch.duration.secs", "5"))));

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

		/*
		 * dstream.foreachRDD(rdd -> { OffsetRange[] offsetRanges = ((HasOffsetRanges)
		 * rdd.rdd()).offsetRanges(); rdd.foreachPartition(consumerRecords -> {
		 * OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
		 * System.out.println("start::foreachRDD:Topic:" + o.topic() + ",Part:" +
		 * o.partition() + ",From:" + o.fromOffset() + ",To:" + o.untilOffset()); });
		 * });
		 */

		JavaDStream<String> jds = dstream.map(new MapFun2());
		System.out.println("start::jds:" + jds);

		JavaDStream<String> jds1 = jds.filter(new FilterFun2());
		System.out.println("start::jds1:" + jds1);

		JavaDStream<String> jds2 = jds.filter(new FilterFun3());
		System.out.println("start::jds2:" + jds2);

		jds1.foreachRDD(new ForVoidFun2());
		jds2.foreachRDD(new ForVoidFun3());

		jssc.start();
		jssc.awaitTermination();
	}

	public void stop() throws Exception {
		System.out.println("stop");
		jssc.stop(true, true);
	}

	public static void main(String[] args) throws Exception {
		KafkaDStreamToHive k2h = new KafkaDStreamToHive(args[0]);
		k2h.start();
		k2h.stop();
	}
}
