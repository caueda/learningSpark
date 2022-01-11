package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.oracle.jrockit.jfr.DataType;

public class MainSqlInMemoryData {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder()
				.appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.getOrCreate();

		List<Row> inMemory = new ArrayList<>();

		inMemory.add(RowFactory.create("WARN", "16 December 2018"));

		StructType schema = new StructType(new StructField[]{
				new StructField("Level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
		});
		sparkSession.createDataFrame(inMemory, schema).show();

		sparkSession.close();
	}

}