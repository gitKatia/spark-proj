package com.kat.spark.apps;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.kat.spark.utils.SparkUtils.getOrCreateSession;
import static com.kat.spark.utils.SparkUtils.reduceLogging;
import static java.lang.String.format;

@Slf4j
public class DataSetExample {

	private static final String APP_NAME = "Spark SQL Data frame API";

	public static void main(String[] args) {

		reduceLogging();
		SparkSession sparkSession = getOrCreateSession(APP_NAME);

		List<String> words = Arrays.asList("Banana", "Car", "Glass", "Banana", "Banana", "Computer", "Car", "IS", "HE");

		// I can get a data set from a list
		Dataset<String> wordDataSet =  sparkSession.createDataset(words, Encoders.STRING());
		applyFilterTransformation(wordDataSet);
		applyMapTransformation(wordDataSet);
		applyReduceTransformation(wordDataSet);
	}

	private static void applyFilterTransformation(Dataset<String> wordDataSet) {
		// I can get a data frame from a data set
		Dataset<Row> wordDataFrame =  wordDataSet.toDF();
		log.info("Show dataframe:");
		wordDataFrame.show();

		List<String> filterList = Arrays.asList("'this'", "'is'", "'he'");

		String filters = filterList.stream()
				.map(String::toUpperCase)
				.collect(Collectors.joining(",", "(", ")"));
		String sqlFilter = format("value not in %s", filters);
		Dataset<Row> filteredDf = wordDataFrame.filter(sqlFilter);
		log.info("Show filtered dataframe:");
		filteredDf.show();
	}

	private static void applyMapTransformation(Dataset<String> wordDataSet) {
		Dataset<String> dataSet = wordDataSet.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
		dataSet.show(10);
	}

	private static void applyReduceTransformation(Dataset<String> wordDataSet) {
		String reducedValue = wordDataSet.reduce((ReduceFunction<String>) (v1, v2) -> v1 + "-" + v2);
		log.info(reducedValue);
	}
}
