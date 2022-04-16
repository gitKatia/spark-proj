package com.kat.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.kat.spark.utils.SparkUtils.getOrCreateSession;

public class JsonReader {

	private static final String SINGLE_LINE_JSON_FILE_PATH = "src/main/resources/single_line.json";
	private static final String MULTI_LINE_JSON_FILE_PATH = "src/main/resources/multi_line.json";
	private static final String APP_NAME = "JSON to data frame";

	private Dataset<Row> readSingleLineJson() {
		SparkSession sparkSession = getOrCreateSession(APP_NAME);

		return sparkSession.read().format("json")
				.load(SINGLE_LINE_JSON_FILE_PATH);
	}

	private Dataset<Row> readMultiLineJson() {
		SparkSession sparkSession = getOrCreateSession(APP_NAME);

		return sparkSession.read().format("json")
				.option("multiline", true)
				.load(MULTI_LINE_JSON_FILE_PATH);
	}

	  public  Dataset<Row> readJson(boolean singleLine) {
	    return singleLine ? readSingleLineJson() : readMultiLineJson();
	  }
}



