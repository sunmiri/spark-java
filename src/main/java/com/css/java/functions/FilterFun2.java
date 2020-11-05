package com.css.java.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

public class FilterFun2 implements Serializable, Function<String, Boolean> {
	@Override
	public Boolean call(String v1) throws Exception {
		System.out.println("FilterFun2::v1:" + v1);
		return Boolean.TRUE;
	}

}
