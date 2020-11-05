package com.css.java.functions;

import java.io.Serializable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

public class MapFun2 implements Serializable, Function<ConsumerRecord<String, String>, String> {
	@Override
	public String call(ConsumerRecord<String, String> v1) throws Exception {
		System.out.println("MapFun2::v1:" + v1);
		return v1.value();
	}

}
