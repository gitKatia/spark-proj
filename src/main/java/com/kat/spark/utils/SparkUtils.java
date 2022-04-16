package com.kat.spark.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkUtils {

    public static void reduceLogging() {
        // To reduce logs
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
    }

    public static SparkSession getOrCreateSession(String appName) {
        return SparkSession.builder()
                .appName(appName)
                .master("local")
                .getOrCreate();
    }

    public static Properties connectionProps() {
        Properties props = new Properties();
        props.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        props.setProperty("user", "spark");
        props.setProperty("password", "<your_password>");
        return props;
    }
}
