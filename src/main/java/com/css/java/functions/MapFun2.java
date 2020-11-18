package com.css.java.functions;

import java.io.Serializable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

import com.css.java.bean.Items;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MapFun2 implements Serializable, Function<ConsumerRecord<String, String>, String> {
	final ObjectMapper mapper = new ObjectMapper();

	@Override
	public String call(ConsumerRecord<String, String> v1) throws Exception {
		System.out.println("MapFun2::v1:" + v1);
		String val = v1.value();

		Items item = mapper.readValue(val, Items.class);

		return val;
	}

}
