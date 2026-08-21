package org.gbif.pipelines.coordinator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DwcDpToVerbatimCallbackTest {

  private DwcDpToVerbatimCallback newCallback(Path tempDir, FileSystem fs) {
    PipelinesConfig config = new PipelinesConfig();
    config.setOutputPath("file://" + tempDir);
    config.setInputPath("file://" + tempDir);

    // RegistryConfig requires a username in internal checks
    config.getStandalone().getRegistry().setWsUrl("http://localhost:0");
    config.getStandalone().getRegistry().setUser("test");
    config.getStandalone().getRegistry().setPassword("test");

    DwcDpToVerbatimCallback callback = new DwcDpToVerbatimCallback(config, null, null);
    callback.fileSystem = fs;
    return callback;
  }

  private void writeMetrics(
      FileSystem fs, Path tempDir, String datasetId, int attempt, long occCount, long eventCount)
      throws Exception {
    org.apache.hadoop.fs.Path metaDir =
        new org.apache.hadoop.fs.Path("file://" + tempDir + "/" + datasetId + "/" + attempt);
    fs.mkdirs(metaDir);
    MetricsUtil.writeMetricsYaml(
        fs,
        java.util.Map.of(
            Metrics.ARCHIVE_TO_OCC_COUNT, occCount,
            Metrics.EVENT_CORE_RECORDS_COUNT, eventCount),
        metaDir + "/archive-to-verbatim.yml");
  }

  @Test
  void virtualOccurrencesOnly_triggersVerbatimMessage_evenWhenFlagIsStale(@TempDir Path tempDir)
      throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String datasetId = UUID.randomUUID().toString();
    int attempt = 1;

    writeMetrics(fs, tempDir, datasetId, attempt, 5L, 10L);

    DwcDpToVerbatimMessage message =
        new DwcDpToVerbatimMessage(
            UUID.fromString(datasetId),
            attempt,
            Set.of("DWCDP_TO_VERBATIM"),
            1L,
            false, // containsOccurrences — stale/wrong on purpose
            true, // containsEvents
            true);

    PipelineBasedMessage out = newCallback(tempDir, fs).createOutgoingMessage(message);

    assertInstanceOf(
        PipelinesVerbatimMessage.class,
        out,
        "virtual-only occurrences must still route through VerbatimMessageHandler, "
            + "not go straight to PipelinesEventsMessage");
    PipelinesVerbatimMessage verbatim = (PipelinesVerbatimMessage) out;
    assertEquals(5L, verbatim.getValidationResult().getNumberOfRecords());
    assertEquals(10L, verbatim.getValidationResult().getNumberOfEventRecords());
  }

  @Test
  void genuineEventOnly_stillSendsEventsMessage(@TempDir Path tempDir) throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String datasetId = UUID.randomUUID().toString();
    int attempt = 1;

    // No physical, no virtual — a real event-only dataset. Must NOT regress into
    // always sending PipelinesVerbatimMessage.
    writeMetrics(fs, tempDir, datasetId, attempt, 0L, 10L);

    DwcDpToVerbatimMessage message =
        new DwcDpToVerbatimMessage(
            UUID.fromString(datasetId),
            attempt,
            Set.of("DWCDP_TO_VERBATIM"),
            1L,
            false,
            true,
            true);

    PipelineBasedMessage out = newCallback(tempDir, fs).createOutgoingMessage(message);

    assertInstanceOf(PipelinesEventsMessage.class, out);
  }
}
