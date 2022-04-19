package com.kat.spark.apps;

import static com.kat.spark.utils.SparkUtils.*;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;


import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class UserComments {
	
	public static void main(String args[]) {

		reduceLogging();
		
		// Create a session
		SparkSession sparkSession = getOrCreateSession("Save csv to DB");

		// get data
		Dataset<Row> df = sparkSession.read().format("csv")
			.option("header", true)
			.load("src/main/resources/user_comments.txt");
		
		df.show();
		// We can specify the number of rows and the number of characters to display in the show method
		
		// Transformation
		df = df.withColumn("full_name", 
				concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());

		df.show();
		
		// Write to MySQL DB
		Properties props = connectionProps();
		String dbConnectionUrl = "jdbc:mysql://localhost:3306/spark_data";

		log.info("Saving data to MySQL DB");
	    df.write()
	    	.mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "user_comments", props);
	}
}