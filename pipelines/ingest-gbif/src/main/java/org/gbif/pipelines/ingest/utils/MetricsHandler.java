package org.gbif.pipelines.ingest.utils;

import java.io.IOException;
import java.util.function.Function;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: DOC
 **/
public class MetricsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);

  private MetricsHandler() {
  }

  /**
   * TODO: DOC
   * SparkRunner doesn't support committed
   **/
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
    LOG.info("Added pipeline metadata - {}", result.replace("\n", ", "));
    return result;
  }

  /**
   * TODO: DOC
   **/
  public static void saveCountersToFile(String hdfsSiteConfig, String path, PipelineResult result) {

    if (path == null || path.isEmpty()) {
      return;
    }

    LOG.info("Trying to write pipeline's metadata to a file - {}", path);

    String countersInfo = getCountersInfo(result);

    try (FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, path)) {
      FsUtils.createFile(fs, path, countersInfo);
      LOG.info("Metadata was written to a file - {}", path);
    } catch (IOException ex) {
      LOG.warn("Write pipelines metadata file", ex);
    }
  }

}
