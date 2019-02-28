package org.gbif.pipelines.ingest.utils;

import java.io.IOException;
import java.util.function.Function;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hadoop.fs.FileSystem;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Class to work with Apache Beam metrics, gets metrics from {@link PipelineResult} and converts to a yaml string format
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetricsHandler {

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link PipelineResult} and converts to a yaml string
   * format
   * SparkRunner doesn't support committed
   */
  public static String getCountersInfo(PipelineResult pr) {

    MetricQueryResults queryResults = pr.metrics().queryMetrics(MetricsFilter.builder().build());

    Function<MetricResult<Long>, String> convert =
        mr -> mr.getName().getName() + "Attempted: " + mr.getAttempted() + "\n";

    StringBuilder builder = new StringBuilder();
    queryResults.getCounters().forEach(x -> {
      String line = convert.apply(x);
      builder.append(line);
    });

    String result = builder.toString();
    log.info("Added pipeline metadata - {}", result.replace("\n", ", "));
    return result;
  }

  /**
   * Method works with Apache Beam metrics, gets metrics from {@link PipelineResult} and converts to a yaml file and
   * save it
   */
  public static void saveCountersToFile(String hdfsSiteConfig, String path, PipelineResult result) {

    if (path != null && !path.isEmpty()) {
      log.info("Trying to write pipeline's metadata to a file - {}", path);

      String countersInfo = getCountersInfo(result);

      try (FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, path)) {
        FsUtils.createFile(fs, path, countersInfo);
        log.info("Metadata was written to a file - {}", path);
      } catch (IOException ex) {
        log.warn("Write pipelines metadata file", ex);
      }
    }

  }

}
