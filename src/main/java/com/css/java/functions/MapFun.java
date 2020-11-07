package com.css.java.functions;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class MapFun implements MapFunction<Row, String>, Serializable {
	static Logger log = Logger.getLogger("MapFun");

	@Override
	public String call(Row value) throws Exception {
		log.info("call::val:" + value);
		System.out.println("map::value:" + value);
		return value.getString(0);
	}
}
