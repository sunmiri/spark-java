package com.css.java.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class FilterFun implements Serializable, FilterFunction<Row> {
	@Override
	public boolean call(Row value) throws Exception {
		System.out.println("filter::value:" + value);
		return true;
	}

}
