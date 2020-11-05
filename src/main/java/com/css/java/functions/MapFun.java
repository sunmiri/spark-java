package com.css.java.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;

public class MapFun implements MapFunction, Serializable {
	@Override
	public Object call(Object value) throws Exception {
		System.out.println("map::value:" + value);
		return value;
	}

}
