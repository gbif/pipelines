package org.gbif.pipelines.coordinator;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpNfsToHdfsMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.DataPackageConversionPipeline;

@Slf4j
public class DwcDpNfsToHdfsStandaloneCallback
    extends PipelinesCallback<DwcDpNfsToHdfsMessage, PipelineBasedMessage>
    implements CloseableMessageCallback<DwcDpNfsToHdfsMessage> {

  public DwcDpNfsToHdfsStandaloneCallback(
      PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.TO_VERBATIM;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder builder, PipelinesConfig config) {
    // no special spark config needed
  }

  @Override
  protected void runPipeline(DwcDpNfsToHdfsMessage message) throws Exception {
    String datasetId = message.getDatasetUuid().toString();
    int attempt = message.getAttempt();

    DataPackageConversionPipeline.runCopy(
        new DataPackageConversionPipeline.CopyConfig(
            sparkSession,
            pipelinesConfig.getInputPath(),
            pipelinesConfig.getOutputPath(),
            datasetId,
            attempt,
            pipelinesConfig.getPartitionSizeInMB() * 1024L * 1024L));
  }

  @Override
  protected String getMetaFileName() {
    return null; // no metrics file for this step yet
  }

  @Override
  public Class<DwcDpNfsToHdfsMessage> getMessageClass() {
    return DwcDpNfsToHdfsMessage.class;
  }

  @Override
  public PipelineBasedMessage createOutgoingMessage(DwcDpNfsToHdfsMessage message) {
    // TODO add next message in process
    return null; // no downstream message yet; add when next step exists
  }
}
