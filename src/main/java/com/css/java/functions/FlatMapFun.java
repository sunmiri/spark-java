package com.css.java.functions;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import com.css.java.bean.Items;

public class FlatMapFun implements Serializable, FlatMapFunction<Row, Items> {
	static Logger log = Logger.getLogger("FlatMapFun");
	final SimpleDateFormat sdf_in = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	final SimpleDateFormat sdf_out = new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public Iterator<Items> call(Row t) throws Exception {
		log.info("call::t:" + t);
		System.out.println("flatmap::t:" + t);
		ArrayList al = new ArrayList();

		Items i = new Items();

		i.setItemName(t.getString(0));
		i.setItemDesc(t.getString(1));
		if (t.getInt(2) == 1)
			i.setIsActive("Active");
		else
			i.setIsActive("In-Active");
		String cd_str = sdf_out.format(t.getTimestamp(3));
		i.setCreatedDate(cd_str);

		al.add(i);

		return al.iterator();
	}

}
