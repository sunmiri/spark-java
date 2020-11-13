package com.css.java.functions;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class FilterFun implements Serializable, FilterFunction<Row> {
	static Logger log = Logger.getLogger("FilterFun");

	@Override
	public boolean call(Row value) throws Exception {
		log.info("call::value:" + value);
		System.out.println("filter::value:" + value);
		if (value.getInt(2) == 0)
			return false;
		return true;
	}

}
