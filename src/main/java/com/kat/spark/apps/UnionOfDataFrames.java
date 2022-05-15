package com.kat.spark.apps;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.kat.spark.utils.SparkUtils.getOrCreateSession;
import static com.kat.spark.utils.SparkUtils.reduceLogging;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

@Slf4j
public class UnionOfDataFrames {

    private static final String DURHAM_PARKS_FILE_PATH = "src/main/resources/durham-parks.json";
    private static final String PHILADELPHIA_RECREATIONS_FILE_PATH = "src/main/resources/philadelphia_recreations.csv";
    private static final String APP_NAME = "Data set Combination";

    public static void main(String[] args) {

        reduceLogging();
        SparkSession sparkSession = getOrCreateSession(APP_NAME);

        Dataset<Row> durhamDf = buildDurhamParksDataFrame(sparkSession);
        durhamDf.printSchema();
        durhamDf.show(10);

        Dataset<Row> philDf = buildPhilParksDataFrame(sparkSession);
        philDf.printSchema();
        philDf.show(10);

        Dataset<Row> unionDf = dataFrameUnionByName(durhamDf, philDf);
        unionDf.show(20);
        unionDf.printSchema();
    }

    private static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession){
        Dataset<Row> df = sparkSession.read().format("json")
                .option("multiline", true)
                .load(DURHAM_PARKS_FILE_PATH);

        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
                df.col("fields.objectid"), lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("fields")
                .drop("geometry")
                .drop("record_timestamp")
                .drop("recordid")
                .drop("datasetid");

        return df;
    }

    private static Dataset<Row> buildPhilParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("multiline", true)
                .option("header", true)
                .load(PHILADELPHIA_RECREATIONS_FILE_PATH);

		//df = df.filter(lower(df.col("USE_")).like("%park%"));
        df = df.filter("lower(USE_) like '%park%' ");

        df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", lit("UNKNONW"))
                .withColumn("geoY", lit("UNKNONW"))
                .drop("SITE_NAME")
                .drop("OBJECTID")
                .drop("CHILD_OF")
                .drop("TYPE")
                .drop("USE_")
                .drop("DESCRIPTION")
                .drop("SQ_FEET")
                .drop("ALLIAS")
                .drop("CHRONOLOGY")
                .drop("NOTES")
                .drop("DATE_EDITED")
                .drop("EDITED_BY")
                .drop("OCCUPANT")
                .drop("TENANT")
                .drop("LABEL");

        return df;
    }

    private static Dataset<Row> dataFrameUnionByName(Dataset<Row> df1, Dataset<Row> df2) {

        // Match by column names using the unionByName() method.
        // if we use just the union() method, it matches the columns based on order.
        Dataset<Row> df = df1.unionByName(df2);

        log.info("Number of records: {}", df.count());

        df = df.repartition(5);

        Partition[] partitions = df.rdd().partitions();
        log.info("Total number of Partitions: {}", partitions.length);
        return df;
    }
}
