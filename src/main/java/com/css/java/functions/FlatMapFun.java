package com.css.java.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapFun implements Serializable, FlatMapFunction {
	@Override
	public Iterator call(Object t) throws Exception {
		System.out.println("flatmap::t:" + t);
		ArrayList al = new ArrayList();
		al.add(t);
		return al.iterator();
	}

}
