package org.gbif.pipelines.spark.util;

import org.apache.spark.sql.SparkSession;

public class SparkTestSession {
  public static SparkSession create() {
    return createBuilder().getOrCreate();
  }

  public static SparkSession.Builder createBuilder() {
    return SparkSession.builder()
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1");
  }
}
