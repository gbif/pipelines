package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.spark.Directories.EVENT_JSON;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.pipelines.EsIndexUtils;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.spark.Indexing;

@Slf4j
public class EventsIndexingCallback
    extends PipelinesCallback<PipelinesEventsInterpretedMessage, PipelinesEventsIndexedMessage>
    implements MessageCallback<PipelinesEventsInterpretedMessage> {

  private static final Object LOCK = new Object();
  private String defaultIndexName = null;

  public EventsIndexingCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.EVENTS_INTERPRETED_TO_INDEX;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Indexing.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {

    initialiseIndex(message);
    Indexing.runIndexing(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getEventIndexAlias(),
        defaultIndexName,
        pipelinesConfig.getStandalone().getEventIndexSchema(),
        pipelinesConfig.getStandalone().getEventIndexNumberOfShards(),
        ParentJsonRecord.class,
        EVENT_JSON);
  }

  private void initialiseIndex(PipelinesInterpretationMessage message) throws IOException {
    if (defaultIndexName != null) {
      return;
    }
    synchronized (LOCK) {
      defaultIndexName =
          EsIndexUtils.initialiseDefaultIndex(
              pipelinesConfig,
              httpClient,
              DatasetType.SAMPLING_EVENT,
              message.getDatasetUuid().toString(),
              message.getAttempt());
    }
  }

  @Override
  protected String getMetaFileName() {
    return Indexing.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesEventsInterpretedMessage> getMessageClass() {
    return PipelinesEventsInterpretedMessage.class;
  }

  @Override
  public PipelinesEventsIndexedMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsIndexedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfOccurrenceRecords(),
        message.getNumberOfEventRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getRunner());
  }
}
