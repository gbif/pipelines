package org.gbif.pipelines.coordinator;

import java.io.IOException;
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
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.DwcDpToVerbatimPipeline;
import org.gbif.pipelines.spark.util.PathUtil;

/**
 * Standalone callback for the DWCDP_TO_VERBATIM step. Runs {@link DwcDpToVerbatimPipeline}
 * in-process using the embedded SparkSession managed by {@link PipelinesCallback}.
 *
 * <p>On success emits either a {@link PipelinesVerbatimMessage} (occurrence or event+occurrence
 * datasets) or a {@link PipelinesEventsMessage} (event-only datasets) to the balancer, following
 * the same branching logic as the workflow graphs in {@code PipelinesWorkflow}.
 *
 * <p>Record counts are read back from {@code archive-to-verbatim.yml} (written during {@code
 * runPipeline}) rather than carried via instance state — {@link PipelinesCallback} instances may be
 * shared across {@code --listenerThreads}, so any state set in {@code runPipeline} and read in
 * {@link #createOutgoingMessage} would be a cross-message race. Re-reading from HDFS keyed only by
 * {@code datasetUuid}/{@code attempt} from the message is stateless and safe, mirroring how {@code
 * VerbatimMessageHandler} reads the same file in the balancer.
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
   * Record counts are read back from {@code archive-to-verbatim.yml}, written during {@code
   * runPipeline} for this same dataset/attempt. Runner is left null — the balancer decides
   * standalone vs distributed for the next step.
   */
  @Override
  public PipelineBasedMessage createOutgoingMessage(DwcDpToVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    long occurrenceCount =
        readMetric(datasetId, message.getAttempt(), Metrics.ARCHIVE_TO_OCC_COUNT);
    long eventCount = readMetric(datasetId, message.getAttempt(), Metrics.EVENT_CORE_RECORDS_COUNT);

    if (message.isContainsOccurrences()) {
      // OCCURRENCE_WF_GRAPH or EVENT_OCCURRENCE_WF_GRAPH
      // tripletValid=false: DwC-DP occurrences use occurrenceID not
      // catalogNumber/institutionCode/collectionCode triplets
      // occurrenceIdValid=true: occurrenceID is the primary identifier in DwC-DP occurrence records
      // useExtendedRecordId=false: DwC-DP occurrenceIDs are UUIDs, not numeric GBIF IDs —
      //   must go through HBase keygen to generate/look up stable numeric GBIF IDs
      ValidationResult validationResult =
          new ValidationResult(false, true, false, occurrenceCount, eventCount);

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
      // useExtendedRecordId=false: same reasoning as occurrence branch
      ValidationResult validationResult =
          new ValidationResult(false, true, false, occurrenceCount, eventCount);

      return new PipelinesEventsMessage(
          message.getDatasetUuid(),
          message.getAttempt(),
          message.getPipelineSteps(),
          eventCount,
          occurrenceCount,
          null, // runner — balancer decides
          false, // repeatAttempt
          null, // resetPrefix
          message.getExecutionId(),
          null, // endpointType — not applicable for DwC-DP
          validationResult,
          Set.of(RecordType.ALL.name()),
          DatasetType.SAMPLING_EVENT);
    }
  }

  /**
   * Reads a single Long metric from {@code archive-to-verbatim.yml}, written during {@code
   * runPipeline} via {@code DwcDpVerbatimConverter.writeMetrics}. Returns 0 if the file or key is
   * missing rather than failing the whole step — a missing count shouldn't block routing the next
   * message.
   */
  private long readMetric(String datasetId, int attempt, String key) {
    String metricsPath =
        PathUtil.interpretedAttemptPath(pipelinesConfig.getOutputPath(), datasetId, attempt)
            + "/"
            + Pipeline.ARCHIVE_TO_VERBATIM
            + ".yml";
    try {
      return PostprocessValidation.getValueByKey(fileSystem, metricsPath, key)
          .map(Long::parseLong)
          .orElse(0L);
    } catch (IOException e) {
      log.warn(
          "Could not read metric {} from {} for dataset {}, defaulting to 0: {}",
          key,
          metricsPath,
          datasetId,
          e.getMessage());
      return 0L;
    }
  }
}
