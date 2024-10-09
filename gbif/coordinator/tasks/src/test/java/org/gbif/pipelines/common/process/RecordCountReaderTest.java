package org.gbif.pipelines.common.process;

import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class RecordCountReaderTest {

  @Test
  public void fileTest() throws Exception {

    // State
    StepConfiguration stepConfiguration = new StepConfiguration();
    stepConfiguration.repositoryPath = this.getClass().getResource("/ingest").getPath();

    String datasetKey = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
    String attempt = "60";
    Long messageNumber = 1L;
    String fileName = "archive-to-verbatim.yml";

    long result =
        RecordCountReader.builder()
            .stepConfig(stepConfiguration)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .messageNumber(messageNumber)
            .metaFileName(fileName)
            .metricName(Metrics.ARCHIVE_TO_ER_COUNT)
            .alternativeMetricName(Metrics.ARCHIVE_TO_OCC_COUNT)
            .build()
            .get();

    Assert.assertEquals(10L, result);
  }

  @Test
  public void messageTest() throws Exception {

    // State
    StepConfiguration stepConfiguration = new StepConfiguration();
    stepConfiguration.repositoryPath = this.getClass().getResource("/ingest").getPath();

    String datasetKey = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
    String attempt = "60";
    Long messageNumber = 100L;
    String fileName = "archive-to-verbatim.yml";

    long result =
        RecordCountReader.builder()
            .stepConfig(stepConfiguration)
            .datasetKey(datasetKey)
            .attempt(attempt)
            .messageNumber(messageNumber)
            .metaFileName(fileName)
            .metricName(Metrics.ARCHIVE_TO_ER_COUNT)
            .alternativeMetricName(Metrics.ARCHIVE_TO_OCC_COUNT)
            .build()
            .get();

    Assert.assertEquals(messageNumber.longValue(), result);
  }
}
