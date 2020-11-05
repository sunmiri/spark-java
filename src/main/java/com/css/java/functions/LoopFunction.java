package com.css.java.functions;

import java.io.Serializable;

import org.apache.spark.api.java.function.ForeachFunction;

public class LoopFunction implements Serializable, ForeachFunction {
	public void call(Object t) throws Exception {
		System.out.println("foreach::t:" + t);
	}
}
