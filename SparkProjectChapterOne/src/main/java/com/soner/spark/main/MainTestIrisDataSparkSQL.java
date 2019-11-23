package com.soner.spark.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MainTestIrisDataSparkSQL {

	private static void SparkSQLIrisData() throws ClassNotFoundException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("iris data");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// SparkSession
		SparkSession spark = SparkSession.builder().getOrCreate();

		// Iris database connection
		Dataset<Row> dataFrame = spark.read().format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:xe")
				.option("dbtable", "IRIS.IRIS_DATA").option("user", "IRIS").option("password", "ir123456")
				.option("driver", "oracle.jdbc.driver.OracleDriver").load();

		// Iris data in DB
		dataFrame.show();

		// Use groupBy in iris data frame
		Dataset<Row> resultGroupBy = dataFrame.groupBy(dataFrame.col("VARIETY")).count();
		resultGroupBy.show();

		// Use avg functions in iris data frame
		Dataset<Row> results = dataFrame.groupBy("VARIETY").agg(org.apache.spark.sql.functions.avg("SEPAL_LENGTH"),
				org.apache.spark.sql.functions.avg("SEPAL_WIDTH"));
		results.show();

		// Generate the schema based on the string of schema
		StructType schema = new StructType(new StructField[]{
				new StructField("SepalLength", DataTypes.createDecimalType(), true, Metadata.empty()),	
				new StructField("SepalWidth", DataTypes.createDecimalType(), false, Metadata.empty()),
				new StructField("PetalLength", DataTypes.createDecimalType(), true, Metadata.empty()),	
				new StructField("PetalWidth", DataTypes.createDecimalType(),false, Metadata.empty()),
				new StructField("Variety", DataTypes.StringType,false, Metadata.empty())});
		
		Dataset<Row> df = spark.createDataFrame(dataFrame.toJavaRDD(), schema);
		df.createOrReplaceTempView("IRIS");

		// Use query in iris schema
		dataFrame.sparkSession().sql("SELECT PetalLength,PetalWidth,Variety from IRIS WHERE PetalLength>3 ORDER BY PetalLength ASC").show();
		
		sparkContext.close();
	}

	public static void main(String[] args) throws ClassNotFoundException {
		//Iris Data SparkSQL Example
		SparkSQLIrisData();

	}

}
