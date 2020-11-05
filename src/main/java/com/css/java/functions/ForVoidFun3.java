package com.css.java.functions;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

public class ForVoidFun3 implements Serializable, VoidFunction<JavaRDD<String>> {
	@Override
	public void call(JavaRDD<String> t) throws Exception {
		System.out.println("ForVoidFun3::t:" + t);
		t.foreachPartition(new VoidFunction<Iterator<String>>() {
			@Override
			public void call(Iterator<String> t) throws Exception {
				// TODO Auto-generated method stub
				// System.out.println("ForVoidFun2::partition::t:" + t);
				while (t.hasNext()) {
					System.out.println("ForVoidFun3::partition::t:" + t.next());
				}
			}
		});
	}

}
