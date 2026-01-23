package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsHdfsViewMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.TableBuild;

@Slf4j
public class EventsTableBuildCallback
    extends PipelinesCallback<PipelinesEventsInterpretedMessage, PipelinesEventsHdfsViewMessage>
    implements MessageCallback<PipelinesEventsInterpretedMessage> {

  protected String tableName;
  protected String sourceDirectory;

  public EventsTableBuildCallback(
      PipelinesConfig pipelinesConfig,
      MessagePublisher publisher,
      String master,
      String tableName,
      String sourceDirectory) {
    super(pipelinesConfig, publisher, master);
    this.tableName = tableName;
    this.sourceDirectory = sourceDirectory;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    TableBuild.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.EVENTS_HDFS_VIEW;
  }

  @Override
  protected void runPipeline(PipelinesEventsInterpretedMessage message) throws Exception {
    TableBuild.runTableBuild(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        tableName,
        sourceDirectory);
  }

  @Override
  protected String getMetaFileName() {
    return "event-to-hdfs.yaml";
  }

  @Override
  public PipelinesEventsHdfsViewMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsHdfsViewMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfOccurrenceRecords(),
        message.getNumberOfEventRecords(),
        isStandalone() ? "STANDALONE" : "DISTRIBUTED",
        message.getExecutionId());
  }

  @Override
  public Class<PipelinesEventsInterpretedMessage> getMessageClass() {
    return PipelinesEventsInterpretedMessage.class;
  }
}
