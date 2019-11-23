package com.soner.spark.main;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MainTestProductSparkStreaming {

	private static void productCount() {

		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("product data");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//create RDD by filtering with data
		JavaRDD<String> productRDD = sparkContext.textFile("product.csv");
		JavaRDD<String> product = productRDD.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 2187177062880767377L;

			public Boolean call(String arg) throws Exception {
				return arg.contains("TV");
			}
		});

		//  By using mapTopair, reduceByKey, we can determine how many data are available.
		JavaPairRDD<String, Integer> productValues = product.flatMap(x -> Arrays.asList(x.split(" ")[1]).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((y, z) -> y + z);

		// Iterator to traverse the list
		Iterator<Tuple2<String, Integer>> iterator = productValues.toLocalIterator();
		System.out.println("Product Counts : ");
		while (iterator.hasNext())
			System.out.println(iterator.next());

		sparkContext.close();
	}

	public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, SQLException {
		//Product Count Example
		productCount();
	}
}
