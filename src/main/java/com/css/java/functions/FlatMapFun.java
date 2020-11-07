package com.css.java.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

public class FlatMapFun implements Serializable, FlatMapFunction<Row, String> {
	static Logger log = Logger.getLogger("FlatMapFun");

	@Override
	public Iterator<String> call(Row t) throws Exception {
		log.info("call::t:" + t);
		System.out.println("flatmap::t:" + t);
		ArrayList al = new ArrayList();
		al.add(t.getString(0));
		al.add(t.getString(1));
		if (t.getDouble(2) == 1)
			al.add("Active");
		else
			al.add("In-Active");
		return al.iterator();
	}

}
