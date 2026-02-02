package org.gbif.pipelines.spark;

import java.io.IOException;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

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
}
