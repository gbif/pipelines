package org.gbif.pipelines.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.BeforeClass;

/** Base class to keep a shared Spark context for parallel tests. */
public class BaseSparkTest {

  static transient SparkContext sc;
  static transient JavaSparkContext jsc;
  static transient SQLContext sqlContext;

  @BeforeClass
  public static void setup() {
    if (sc == null) {
      SparkConf conf =
          new SparkConf()
              .setMaster("local[*]")
              .setAppName("test")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.ui.enabled", "false")
              .set("spark.testing", "true"); // ignore memory check
      sc = SparkContext.getOrCreate(conf);
      jsc = new JavaSparkContext(sc);
      sqlContext = new SQLContext(jsc);
    }
  }
}
