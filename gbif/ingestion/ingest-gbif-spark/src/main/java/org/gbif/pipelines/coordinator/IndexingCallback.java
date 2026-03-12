package org.gbif.pipelines.coordinator;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.IndexingPipeline;
import org.gbif.pipelines.spark.util.EsIndexUtils;

@Slf4j
public class IndexingCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesIndexedMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  private static final Object LOCK = new Object();
  private String defaultIndexName = null;

  public IndexingCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);

    if (isStandalone()) {
      log.info("Running in standalone mode, initialising index");
      try {
        initialiseIndex();
      } catch (IOException e) {
        log.error("Error initialising index", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected StepType getStepType() {
    return StepType.INTERPRETED_TO_INDEX;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    IndexingPipeline.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    IndexingPipeline.runIndexing(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getOccurrenceIndexAlias(),
        defaultIndexName,
        pipelinesConfig.getStandalone().getOccurrenceIndexSchema(),
        pipelinesConfig.getStandalone().getOccurrenceIndexNumberOfShards(),
        OccurrenceJsonRecord.class,
        Directories.OCCURRENCE_JSON);
  }

  private void initialiseIndex() throws IOException {

    if (defaultIndexName != null) {
      return;
    }

    synchronized (LOCK) {
      if (defaultIndexName != null) {
        return;
      }
      defaultIndexName =
          EsIndexUtils.initialiseDefaultIndex(
              pipelinesConfig, httpClient, DatasetType.OCCURRENCE, "NOT_USED", 1);
    }
  }

  @Override
  protected String getMetaFileName() {
    return IndexingPipeline.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        isStandalone() ? "STANDALONE" : "DISTRIBUTED",
        message.getExecutionId(),
        message.getEndpointType());
  }
}
