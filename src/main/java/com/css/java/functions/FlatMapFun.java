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
		System.out.println("flatmap.call::t:" + t);
		ArrayList al = new ArrayList();

		Items i = new Items();
		String itemName = (String) t.getAs("itemName");
		String itemDesc = (String) t.getAs("itemDesc");
		String isActive = (String) t.getAs("isActive");
		String createdDate = (String) t.getAs("createdDate");
		System.out.println("flatmap.call::itemName:" + itemName + ", itemDesc:" + itemDesc + ", isActive:" + isActive
				+ ", createdDate:" + createdDate);

		i.setItemName(itemName); // t.getString(0));
		i.setItemDesc(itemDesc); // t.getString(1));

		if (isActive.equals("1"))
			i.setIsActive("Active");
		else
			i.setIsActive("In-Active");

		i.setCreatedDate(createdDate);

		al.add(i);

		return al.iterator();
	}

}
