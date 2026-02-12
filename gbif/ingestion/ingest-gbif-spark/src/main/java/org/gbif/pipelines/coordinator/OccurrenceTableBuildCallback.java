package org.gbif.pipelines.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.TableBuild;

@Slf4j
public class OccurrenceTableBuildCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesHdfsViewMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  protected String tableName;
  protected String sourceDirectory;

  List<PipelinesInterpretedMessage> buffer = new ArrayList<>();

  public OccurrenceTableBuildCallback(
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
  public void init() throws IOException {
    super.init();
    TableBuild.initialiseTargetTables(
        sparkSession, this.pipelinesConfig.getHiveDB(), this.tableName);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    TableBuild.configSparkSession(sparkBuilder, config);
    // initialise tables ?
  }

  @Override
  protected StepType getStepType() {
    return StepType.HDFS_VIEW;
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    TableBuild.runTableBuild(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        Map.of(message.getDatasetUuid(), message.getAttempt()),
        tableName,
        sourceDirectory);
  }

  @Override
  protected String getMetaFileName() {
    return "occurrence-to-hdfs.yaml";
  }

  @Override
  public PipelinesHdfsViewMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesHdfsViewMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps(), null, null);
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }
}
