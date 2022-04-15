package com.kat.spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Application {
	
	public static void main(String args[]) {
		
		// Create a session
		SparkSession spark = new SparkSession.Builder()
				.appName("Save csv to DB")
				.master("local")
				.getOrCreate();

		// get data
		Dataset<Row> df = spark.read().format("csv")
			.option("header", true)
			.load("src/main/resources/user_comments.txt");
		
		df.show();
		
		// Transformation
		df = df.withColumn("full_name", 
				concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());

		df.show();
		
		// Write to MySQL DB
		String dbConnectionUrl = "jdbc:mysql://localhost:3306/spark_data";
		Properties prop = new Properties();
	    prop.setProperty("driver", "com.mysql.jdbc.Driver");
	    prop.setProperty("user", "spark");
	    prop.setProperty("password", "<yourpassword>");
	    
	    df.write()
	    	.mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "user_comments", prop);
	}
}