package org.gbif.pipelines.spark.util;

import java.io.IOException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

/** Common spark utility methods for creating SparkSession and FileSystem instances. */
@Slf4j
public class SparkUtil {

  public static SparkSession getSparkSession(
      String master,
      String appName,
      PipelinesConfig config,
      BiConsumer<SparkSession.Builder, PipelinesConfig> fcn) {

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName(appName);
    if (master != null) {
      sparkBuilder = sparkBuilder.master(master);
      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");
    }
    fcn.accept(sparkBuilder, config);
    return sparkBuilder.getOrCreate();
  }

  public static FileSystem getFileSystem(SparkSession spark, PipelinesConfig config)
      throws IOException {

    FileSystem fileSystem;
    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    if (config.getHdfsSiteConfig() != null && config.getCoreSiteConfig() != null) {
      hadoopConf.addResource(new Path(config.getHdfsSiteConfig()));
      hadoopConf.addResource(new Path(config.getCoreSiteConfig()));
      fileSystem = FileSystem.get(hadoopConf);
    } else {
      log.warn("Using local filesystem - this is suitable for local development only");
      fileSystem = FileSystem.getLocal(hadoopConf);
    }
    return fileSystem;
  }

  /**
   * Reads parquet as the given bean class, with the schema of the class instead of the one of the
   * files. Parquet written before a field was added to the class (e.g. by an older version, for a
   * dataset not interpreted again) is read with that field as null, instead of failing as
   * unresolved.
   */
  public static <T> Dataset<T> readParquetAs(SparkSession spark, String path, Class<T> clazz) {
    Encoder<T> encoder = Encoders.bean(clazz);
    return spark.read().schema(encoder.schema()).parquet(path).as(encoder);
  }
}
