package com.kat.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.kat.spark.utils.SparkUtils.getOrCreateSession;

@Slf4j
public class CsvReader {

	private static final String PRODUCTS_FILE_PATH = "src/main/resources/products.txt";
	private static final String APP_NAME = "Complex CSV to data frame";

	public Dataset<Row> getDataFrame(boolean inferSchema) {
		return inferSchema ? dataFrameWithInferredSchema() : dataFrameWithDefinedSchema();
	}

	private Dataset<Row> dataFrameWithInferredSchema() {
		SparkSession sparkSession = getOrCreateSession(APP_NAME);
		return sparkSession.read().format("csv")
				.option("header", "true")
				.option("multiline", true)
				.option("sep", ";")
				.option("quote", "^")
				.option("dateFormat", "M/d/y")
				.option("inferSchema", true)
				.load(PRODUCTS_FILE_PATH);
	}

	private Dataset<Row> dataFrameWithDefinedSchema() {

		SparkSession sparkSession = getOrCreateSession(APP_NAME);

		StructType schema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("product_id", DataTypes.IntegerType, true),
				DataTypes.createStructField("item_name", DataTypes.StringType, false),
				DataTypes.createStructField("published_on", DataTypes.DateType, true),
				DataTypes.createStructField("url", DataTypes.StringType, false) });

		return sparkSession.read().format("csv")
				.option("header", "true")
				.option("multiline", true)
				.option("sep", ";")
				.option("dateFormat", "M/d/y")
				.option("quote", "^")
				.schema(schema)
				.load(PRODUCTS_FILE_PATH);
	}
}
