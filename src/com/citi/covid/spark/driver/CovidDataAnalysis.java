package com.citi.covid.spark.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CovidDataAnalysis {
	public static void main(String[] args) throws InterruptedException {
		 JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Covid Analysis").setMaster("local"));
		 SparkSession spark = SparkSession.builder().appName("spark-covid-analysis").getOrCreate();
		 Dataset<Row> rows = spark.read().option("header", true).csv("/Users/dpq/git/CovidDataAnalysis/inputData/covid.csv");
		 
		 
		 
		 rows.show(50);
		 
		 rows.printSchema();
		 
		 sc.close();
	}
}
