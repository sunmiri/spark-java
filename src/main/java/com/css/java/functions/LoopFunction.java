package com.css.java.functions;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.ForeachFunction;

public class LoopFunction implements Serializable, ForeachFunction {
	static Logger log = Logger.getLogger("LoopFunction");

	public void call(Object t) throws Exception {
		log.info("call::t:" + t);
		System.out.println("foreach::t:" + t);
	}
}
