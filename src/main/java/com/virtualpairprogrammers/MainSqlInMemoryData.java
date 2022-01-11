package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

//		dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM')");
		dataset = dataset.select(col("level"),
				date_format(col("datetime"), "MMMM").alias("month"),
				date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

		//Pivot Table
		dataset = dataset.groupBy(col("level"))
				.pivot("month",
						Stream.of(
								"January",
								"February",
								"March",
								"April",
								"May",
								"June",
								"July",
								"August",
								"September",
								"October",
								"November",
								"December",
								"Fakemonth"
						).collect(Collectors.toList()))
				.count().na().fill(0L);
		dataset.show(100);

		sparkSession.close();
	}

}
