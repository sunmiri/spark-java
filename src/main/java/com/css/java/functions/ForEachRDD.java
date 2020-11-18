package com.css.java.functions;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.css.java.bean.Items;

public class ForEachRDD implements Serializable, VoidFunction<JavaRDD<String>> {
	Properties props;

	public ForEachRDD(Properties props) {
		this.props = props;
	}

	@Override
	public void call(JavaRDD<String> rdd) throws Exception {
		SparkSession ss = SparkSession.builder().getOrCreate();
		SQLContext sqlContext = new SQLContext(rdd.rdd().sparkContext());
		Dataset<Row> rddDF = sqlContext.read().json(rdd);
		System.out.println("call::rddDF:" + rddDF);
		rddDF.printSchema();
		rddDF.show(true);

		Dataset<Items> flatMapDF = rddDF.flatMap(new FlatMapFun(), Encoders.bean(Items.class));
		System.out.println("call::flatMapDF:" + flatMapDF);
		flatMapDF.printSchema();
		flatMapDF.show(true);
		flatMapDF.write().mode(SaveMode.Append).partitionBy("createdDate").json(props.getProperty("output.path"));
		System.out.println("call::flatMapDF done writing to path:" + props.getProperty("output.path"));
	}

}
