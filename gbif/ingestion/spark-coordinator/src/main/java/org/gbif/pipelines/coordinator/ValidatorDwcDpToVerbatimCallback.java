package org.gbif.pipelines.coordinator;

import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.spark.DwcDpToVerbatimPipeline;
import org.gbif.pipelines.spark.util.PathUtil;

/** Converts staged DwC-DP data to verbatim records for the validator workflow. */
@Slf4j
public class ValidatorDwcDpToVerbatimCallback
    extends ValidatorCallback<DwcDpToVerbatimMessage, PipelinesVerbatimMessage> {

  public ValidatorDwcDpToVerbatimCallback(
      PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VALIDATOR_DWCDP_TO_VERBATIM;
  }

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
    return Pipeline.ARCHIVE_TO_VERBATIM + ".yml";
  }

  @Override
  public Class<DwcDpToVerbatimMessage> getMessageClass() {
    return DwcDpToVerbatimMessage.class;
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(DwcDpToVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    long occurrenceCount =
        readMetric(datasetId, message.getAttempt(), Metrics.ARCHIVE_TO_OCC_COUNT);
    long eventCount = readMetric(datasetId, message.getAttempt(), Metrics.EVENT_CORE_RECORDS_COUNT);

    ValidationResult validationResult =
        new ValidationResult(false, true, false, occurrenceCount, eventCount);
    DatasetType datasetType =
        message.isContainsEvents() ? DatasetType.SAMPLING_EVENT : DatasetType.OCCURRENCE;

    // The validator's next step consumes PipelinesVerbatimMessage, including for event-only data.
    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        Set.of(RecordType.ALL.name()),
        message.getPipelineSteps(),
        null,
        null,
        null,
        validationResult,
        null,
        message.getExecutionId(),
        datasetType);
  }

  private long readMetric(String datasetId, int attempt, String key) {
    String metricsPath =
        PathUtil.interpretedAttemptPath(pipelinesConfig.getOutputPath(), datasetId, attempt)
            + "/"
            + Pipeline.ARCHIVE_TO_VERBATIM
            + ".yml";

    try {
      Map<String, Long> metrics = MetricsUtil.readMetricsYaml(fileSystem, metricsPath);
      return metrics.getOrDefault(key, 0L);
    } catch (PipelinesException e) {
      log.warn("Metric {} not found in {} — defaulting to 0", key, metricsPath);
      return 0L;
    }
  }
}
