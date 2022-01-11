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

		Dataset<Row> dataset = sparkSession
				.read()
				.option("header", true)
				.csv("src/main/resources/biglog.txt");

		dataset.createOrReplaceTempView("logging_table");

		sparkSession.sql(
				"select level, " +
						"date_format(datetime, 'MMMM') as month, " +
						"count(datetime) qtde " +
				" from logging_table " +
						" group by level, date_format(datetime, 'MMMM') " +
						" order by level, date_format(datetime, 'MMMM') ")
						.show();

		sparkSession.close();
	}

}
