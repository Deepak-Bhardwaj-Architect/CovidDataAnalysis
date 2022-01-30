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
		 Dataset<Row> covidSql = spark.read().option("header", true).csv("/Users/dpq/git/CovidDataAnalysis/inputData/covid.csv");
		 covidSql = covidSql.cache();
		 covidSql.show(5);
		 
		 System.out.println("COUNT:::::::  "+covidSql.count() );
		 System.out.println("Execution plan details before execution:::::::  "+covidSql.queryExecution());
		 System.out.println("Countrywise data");
		 covidSql.groupBy("iso_code").count().orderBy("count").show();
		
		 covidSql.registerTempTable("COVID_DATA");
		 
		 covidSql.sqlContext().sql("Select distinct iso_code from COVID_DATA").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, count(*) from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_cases) as total_cases from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select * from COVID_DATA where iso_code='NIU'").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(new_cases) as total_new_cases from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_deaths) as total_deaths from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(new_deaths) as new_deaths from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_cases_per_million) as total_cases_per_million from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(new_cases_per_million) as new_cases_per_million from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_deaths_per_million) as total_deaths_per_million from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(new_deaths_per_million) as new_deaths_per_million from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_tests) as total_tests from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code, sum(total_tests_per_thousand) as total_tests_per_thousand from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code as Country_Code, sum(total_tests_per_thousand) as total_tests_per_thousand from COVID_DATA group by iso_code").show();
		 
		 covidSql.sqlContext().sql("Select iso_code as Country_Code,"
			 		+ " sum(total_cases) as total_cases,"
			 		+ " sum(new_cases) as new_cases,"
			 		+ " sum(total_deaths) as total_deaths,"
			 		+ " sum(new_deaths) as new_deaths,"
			 		+ " sum(new_cases_per_million) as new_cases_per_million,"
			 		+ " sum(total_deaths_per_million) as total_deaths_per_million,"
			 		+ " sum(new_cases_smoothed_per_million) as new_cases_smoothed_per_million,"
			 		+ " sum(icu_patients) as icu_patients,"
			 		+ " sum(total_tests) as total_tests,"
			 		+ " sum(new_tests) as new_tests,"
			 		+ " sum(people_fully_vaccinated_per_hundred) as people_fully_vaccinated_per_hundred,"
			 		+ " sum(weekly_hosp_admissions) as weekly_hosp_admissions,"
			 		+ " sum(people_vaccinated) as people_vaccinated,"
			 		+ " sum(total_boosters) as total_boosters"
			 		+ " from COVID_DATA group by iso_code").show();
		 
		 
		 
		 
		 sc.close();
	}
}
