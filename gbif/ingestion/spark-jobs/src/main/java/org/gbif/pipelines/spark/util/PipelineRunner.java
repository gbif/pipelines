package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.util.SparkUtil.getFileSystem;
import static org.gbif.pipelines.spark.util.SparkUtil.getSparkSession;

import java.util.function.BiConsumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class PipelineRunner {

  @FunctionalInterface
  public interface WithFileSystem {
    void run(SparkSession spark, FileSystem fs) throws Exception;
  }

  @FunctionalInterface
  public interface WithoutFileSystem {
    void run(SparkSession spark) throws Exception;
  }

  public static void run(
      PipelineArgs args,
      PipelinesConfig config,
      BiConsumer<SparkSession.Builder, PipelinesConfig> sparkConfig,
      WithFileSystem body)
      throws Exception {

    SparkSession spark = null;
    FileSystem fileSystem = null;
    try {
      spark = getSparkSession(args.master, args.appName, config, sparkConfig);
      fileSystem = getFileSystem(spark, config);
      body.run(spark, fileSystem);
    } finally {
      if (fileSystem != null) fileSystem.close();
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }
    if (args.useSystemExit) System.exit(0);
  }

  public static void run(
      PipelineArgs args,
      PipelinesConfig config,
      BiConsumer<SparkSession.Builder, PipelinesConfig> sparkConfig,
      WithoutFileSystem body)
      throws Exception {

    SparkSession spark = null;
    try {
      spark = getSparkSession(args.master, args.appName, config, sparkConfig);
      body.run(spark);
    } finally {
      if (spark != null) {
        spark.stop();
        spark.close();
      }
    }
    if (args.useSystemExit) System.exit(0);
  }
}
