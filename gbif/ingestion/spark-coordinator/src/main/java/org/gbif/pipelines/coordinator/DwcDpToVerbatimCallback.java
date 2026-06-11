package org.gbif.pipelines.coordinator;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.DwcDpToVerbatimPipeline;

/**
 * Standalone callback for the DWCDP_TO_VERBATIM step. Runs {@link DwcDpToVerbatimPipeline}
 * in-process using the embedded SparkSession managed by {@link PipelinesCallback}.
 */
@Slf4j
public class DwcDpToVerbatimCallback
    extends PipelinesCallback<DwcDpToVerbatimMessage, PipelineBasedMessage>
    implements CloseableMessageCallback<DwcDpToVerbatimMessage> {

  public DwcDpToVerbatimCallback(
      PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.DWCDP_TO_VERBATIM;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder builder, PipelinesConfig config) {}

  @Override
  protected void runPipeline(DwcDpToVerbatimMessage message) throws Exception {
    DwcDpToVerbatimPipeline.run(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        message.isContainsEvents(),
        message.isContainsOccurrences());
  }

  @Override
  protected String getMetaFileName() {
    return null;
  }

  @Override
  public Class<DwcDpToVerbatimMessage> getMessageClass() {
    return DwcDpToVerbatimMessage.class;
  }

  @Override
  public PipelineBasedMessage createOutgoingMessage(DwcDpToVerbatimMessage message) {
    // TODO: emit next step message (e.g. VERBATIM_TO_IDENTIFIER) once wired
    return null;
  }
}
