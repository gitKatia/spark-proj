package com.kat.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static com.kat.spark.utils.SparkUtils.reduceLogging;

public class SchemaPrinter {

	public static void main(String[] args) {

		reduceLogging();

		CsvReader csvSchemaReader = new CsvReader();
		Dataset<Row> productsDataFrameInferred = csvSchemaReader.getDataFrame(true);
		productsDataFrameInferred.show(4, 40);
		productsDataFrameInferred.printSchema();

		Dataset<Row> productsDataFrame = csvSchemaReader.getDataFrame(false);
		productsDataFrame.show(4, 40);
		productsDataFrame.printSchema();

		JsonReader jsonSchemaReader = new JsonReader();
		Dataset<Row> dataFrameFromSingleLineJson = jsonSchemaReader.readJson(true);
		dataFrameFromSingleLineJson.show();
		dataFrameFromSingleLineJson.printSchema();
		Dataset<Row> dataFrameFromMultiLineJson = jsonSchemaReader.readJson(false);
		dataFrameFromMultiLineJson.show();
		dataFrameFromMultiLineJson.printSchema();
	}
}
