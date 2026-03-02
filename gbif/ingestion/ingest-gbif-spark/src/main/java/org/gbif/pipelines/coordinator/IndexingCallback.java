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
import org.gbif.pipelines.EsIndexUtils;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.Indexing;

@Slf4j
public class IndexingCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesIndexedMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  private String defaultIndexName = null;

  public IndexingCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.INTERPRETED_TO_INDEX;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Indexing.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {

    initialiseIndex(message);

    Indexing.runIndexing(
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

  private void initialiseIndex(PipelinesInterpretedMessage message) throws IOException {
    if (defaultIndexName != null) {
      return;
    }
    defaultIndexName =
        EsIndexUtils.initialiseDefaultIndex(
            pipelinesConfig,
            httpClient,
            DatasetType.OCCURRENCE,
            message.getDatasetUuid().toString(),
            message.getAttempt());
    assert defaultIndexName != null;
  }

  @Override
  protected String getMetaFileName() {
    return Indexing.METRICS_FILENAME;
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
