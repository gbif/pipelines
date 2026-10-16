package org.gbif.pipelines.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ValidatorDwcDpToVerbatimCallbackTest {

  @Test
  void convertsMetricsIntoValidatorVerbatimMessage(@TempDir Path tempDir) throws Exception {
    UUID datasetId = UUID.randomUUID();
    int attempt = 1;
    Set<String> steps =
        Set.of(
            StepType.VALIDATOR_DWCDP_TO_VERBATIM.name(),
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER.name());

    try (FileSystem fileSystem = FileSystem.getLocal(new Configuration())) {
      org.apache.hadoop.fs.Path metricsDir =
          new org.apache.hadoop.fs.Path("file://" + tempDir + "/" + datasetId + "/" + attempt);
      fileSystem.mkdirs(metricsDir);
      MetricsUtil.writeMetricsYaml(
          fileSystem,
          Map.of(Metrics.ARCHIVE_TO_OCC_COUNT, 12L, Metrics.EVENT_CORE_RECORDS_COUNT, 3L),
          metricsDir + "/archive-to-verbatim.yml");

      PipelinesConfig config = new PipelinesConfig();
      config.setOutputPath("file://" + tempDir);
      config.setInputPath("file://" + tempDir);
      config.getStandalone().getRegistry().setWsUrl("http://localhost:0");
      config.getStandalone().getRegistry().setUser("test");
      config.getStandalone().getRegistry().setPassword("test");

      ValidatorDwcDpToVerbatimCallback callback =
          new ValidatorDwcDpToVerbatimCallback(config, null, null);
      callback.fileSystem = fileSystem;

      DwcDpToVerbatimMessage message =
          new DwcDpToVerbatimMessage(datasetId, attempt, steps, 1L, true, true, true);
      PipelinesVerbatimMessage outgoing = callback.createOutgoingMessage(message);

      assertEquals(datasetId, outgoing.getDatasetUuid());
      assertEquals(steps, outgoing.getPipelineSteps());
      assertEquals(DatasetType.SAMPLING_EVENT, outgoing.getDatasetType());
      assertEquals(12L, outgoing.getValidationResult().getNumberOfRecords());
      assertEquals(3L, outgoing.getValidationResult().getNumberOfEventRecords());
      assertFalse(outgoing.getValidationResult().isTripletValid());
      assertTrue(outgoing.getValidationResult().isOccurrenceIdValid());
    }
  }
}
