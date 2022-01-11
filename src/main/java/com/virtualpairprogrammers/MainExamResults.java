package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class MainExamResults {

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
				.csv("src/main/resources/exams/students.csv");

		dataset = dataset.groupBy("subject")
				.pivot("year")
				.agg(   round(avg(col("score")),2).alias("average")
						,max(col("score").cast(DataTypes.IntegerType)).alias("max score ")
				);

		dataset.show(100);

		sparkSession.close();
	}

}
