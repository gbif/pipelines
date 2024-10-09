package org.gbif.pipelines.common.beam.metrics;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.BasePipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

/**
 * Class to work with Apache Beam metrics, gets metrics from {@link MetricResults} and converts to a
 * yaml string format
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetricsHandler {

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link MetricResults} and converts to
   * a yaml string format SparkRunner doesn't support committed
   */
  public static String getCountersInfo(MetricResults results) {

    MetricQueryResults queryResults = results.queryMetrics(MetricsFilter.builder().build());

    Function<MetricResult<Long>, String> convert =
        mr -> mr.getName().getName() + "Attempted: " + mr.getAttempted() + "\n";

    StringBuilder builder = new StringBuilder();
    queryResults.getCounters().forEach(x -> builder.append(convert.apply(x)));

    String result = builder.toString();
    log.info("Added pipeline metadata - {}", result.replace("\n", ", "));
    return result;
  }

  /** Method works with String data */
  public static void saveMetricsToFile(HdfsConfigs hdfsConfigs, String path, String metrics) {

    if (path != null && !path.isEmpty()) {
      log.info("Trying to write pipeline's metadata to a file - {}", path);

      FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
      try {
        FsUtils.createFile(fs, path, metrics);
        log.info("Metadata was written to a file - {}", path);
      } catch (IOException ex) {
        log.warn("Write pipelines metadata file", ex);
      }
    }
  }

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link MetricResults} and converts to
   * a yaml file and save it
   */
  public static void saveCountersToInputPathFile(
      BasePipelineOptions options, MetricResults results) {
    saveCountersToFile(options, results, true);
  }

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link MetricResults} and converts to
   * a yaml file and save it
   */
  public static void saveCountersToTargetPathFile(
      BasePipelineOptions options, MetricResults results) {
    saveCountersToFile(options, results, false);
  }

  public static boolean deleteMetricsFile(BasePipelineOptions options) {
    FileSystem fs = getFileSystemForOptions(options);
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    Path path = new Path(metadataPath);
    try {
      return fs.exists(path) && fs.delete(path, true);
    } catch (IOException e) {
      log.error("Can't delete {} directory, cause - {}", metadataPath, e.getCause());
      return false;
    }
  }

  /** Method works with String data */
  public static void saveMetricsToFile(
      BasePipelineOptions options, String metrics, boolean isInput) {
    Optional.ofNullable(options.getMetaFileName())
        .ifPresent(
            metadataName -> {
              String metadataPath = "";
              if (!metadataName.isEmpty()) {
                metadataPath = PathBuilder.buildDatasetAttemptPath(options, metadataName, isInput);
              }
              String hdfsSiteConfig = "";
              String coreSiteConfig = "";
              // FIXME InterpretationPipelineOptions should be refactored
              // splitting out the HDFS part into a separate interface
              // which InterpretationPipelineOptions can extend
              if (options instanceof InterpretationPipelineOptions) {
                InterpretationPipelineOptions o = (InterpretationPipelineOptions) options;
                hdfsSiteConfig = o.getHdfsSiteConfig();
                coreSiteConfig = o.getCoreSiteConfig();
              }
              MetricsHandler.saveMetricsToFile(
                  HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig), metadataPath, metrics);
            });
  }

  private static FileSystem getFileSystemForOptions(BasePipelineOptions options) {
    String hdfsSiteConfig = "";
    String coreSiteConfig = "";
    // FIXME InterpretationPipelineOptions should be refactored
    // splitting out the HDFS part into a separate interface
    // which InterpretationPipelineOptions can extend
    if (options instanceof InterpretationPipelineOptions) {
      InterpretationPipelineOptions o = (InterpretationPipelineOptions) options;
      hdfsSiteConfig = o.getHdfsSiteConfig();
      coreSiteConfig = o.getCoreSiteConfig();
    }

    return FsUtils.getFileSystem(
        HdfsConfigs.create(hdfsSiteConfig, coreSiteConfig), options.getInputPath());
  }

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link MetricResults} and converts to
   * a yaml file and save it
   */
  private static void saveCountersToFile(
      BasePipelineOptions options, MetricResults results, boolean isInput) {
    String countersInfo = getCountersInfo(results);
    saveMetricsToFile(options, countersInfo, isInput);
  }
}
