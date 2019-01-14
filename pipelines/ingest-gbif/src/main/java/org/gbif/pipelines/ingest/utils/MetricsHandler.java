package org.gbif.pipelines.ingest.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.commons.io.FileUtils;
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
   **/
  public static List<String> getCountersInfo(PipelineResult result) {

    MetricQueryResults queryResults = result.metrics().queryMetrics(MetricsFilter.builder().build());

    Function<MetricResult<Long>, String> convert =
        mr -> mr.getName().getName() + "Attempted: " + mr.getAttempted() + "\n"
            + mr.getName().getName() + "Commited: " + mr.getCommitted() + "\n";

    List<String> lines = new ArrayList<>();
    queryResults.getCounters().forEach(x -> {
      String line = convert.apply(x);
      lines.add(line);
      LOG.info("Added pipeline metadata - {}", line.replace("\n", ", "));
    });

    return lines;
  }

  /**
   * TODO: DOC
   **/
  public static void saveCountersToFile(PipelineResult result, String filePath) {

    if (filePath == null || filePath.isEmpty()) {
      return;
    }

    LOG.info("Trying to write pipeline's metadata to a file - {}", filePath);

    List<String> lines = getCountersInfo(result);

    File file = Paths.get(filePath).toFile();
    try {
      FileUtils.writeLines(file, lines);
      LOG.info("Metadata was written to a file - {}", filePath);
    } catch (IOException ex) {
      LOG.warn("Write pipelines metadata file", ex);
    }
  }

}
