package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

		initialRdd
				.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
				.filter(sentence -> sentence.trim().length() > 0)
				.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
				.filter(word -> word.trim().length() > 0)
				.filter(Util::isNotBoring)
				.mapToPair(word -> new Tuple2<String, Long>( word, 1L))
				.reduceByKey((v1, v2) -> v1 + v2)
				.mapToPair(stringLongTuple2 -> new Tuple2<>(stringLongTuple2._2, stringLongTuple2._1))
				.sortByKey(false)
				.take(10).forEach(System.out::println);

		sc.close();
	}

}
