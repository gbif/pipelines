package org.gbif.pipelines.coordinator;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.DwcDpToVerbatimPipeline;

/**
 * Standalone callback for the DWCDP_TO_VERBATIM step. Runs {@link DwcDpToVerbatimPipeline}
 * in-process using the embedded SparkSession managed by {@link PipelinesCallback}.
 *
 * <p>On success emits either a {@link PipelinesVerbatimMessage} (occurrence or event+occurrence
 * datasets) or a {@link PipelinesEventsMessage} (event-only datasets) to the balancer, following
 * the same branching logic as the workflow graphs in {@code PipelinesWorkflow}.
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

  /**
   * Emits the next message in the pipeline chain:
   *
   * <ul>
   *   <li>containsOccurrences (± events) → {@link PipelinesVerbatimMessage} →
   *       VERBATIM_TO_IDENTIFIER (OCCURRENCE_WF_GRAPH or EVENT_OCCURRENCE_WF_GRAPH)
   *   <li>events only → {@link PipelinesEventsMessage} → EVENTS_VERBATIM_TO_INTERPRETED
   *       (EVENT_WF_GRAPH)
   * </ul>
   *
   * Runner is left null — the balancer decides standalone vs distributed for the next step.
   */
  @Override
  public PipelineBasedMessage createOutgoingMessage(DwcDpToVerbatimMessage message) {
    if (message.isContainsOccurrences()) {
      // OCCURRENCE_WF_GRAPH or EVENT_OCCURRENCE_WF_GRAPH
      // tripletValid=false: DwC-DP uses occurrenceID, not triplets
      // occurrenceIdValid=true: occurrenceID is the primary key in DwC-DP
      // record counts unknown at this point — balancer will route on verbatim.avro size
      ValidationResult validationResult = new ValidationResult(false, true, true, null, null);

      DatasetType datasetType =
          message.isContainsEvents() ? DatasetType.SAMPLING_EVENT : DatasetType.OCCURRENCE;

      return new PipelinesVerbatimMessage(
          message.getDatasetUuid(),
          message.getAttempt(),
          Set.of(RecordType.ALL.name()),
          message.getPipelineSteps(),
          null, // runner — balancer decides
          null, // endpointType — not applicable for DwC-DP
          null, // extraPath
          validationResult,
          null, // resetPrefix
          message.getExecutionId(),
          datasetType);
    } else {
      // EVENT_WF_GRAPH — events only, no occurrences
      return new PipelinesEventsMessage(
          message.getDatasetUuid(),
          message.getAttempt(),
          message.getPipelineSteps(),
          null, // numberOfEventRecords — unknown at this point
          null, // numberOfOccurrenceRecords
          null, // runner — balancer decides
          false, // repeatAttempt
          null, // resetPrefix
          message.getExecutionId(),
          null, // endpointType — not applicable for DwC-DP
          null, // validationResult
          Set.of(RecordType.ALL.name()),
          DatasetType.SAMPLING_EVENT);
    }
  }
}
