package org.gbif.pipelines.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.pipelines.InterpretationType.RecordType;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.spark.DwcDpToVerbatimPipeline;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter;
import org.gbif.pipelines.spark.util.PathUtil;

/** Converts staged DwC-DP data to verbatim records for the validator workflow. */
@Slf4j
public class ValidatorDwcDpToVerbatimCallback
    extends ValidatorCallback<DwcDpToVerbatimMessage, PipelinesVerbatimMessage> {

  private static final ObjectMapper JSON = OrchestratorJson.mapper();

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

  /**
   * The data shape comes from the staged tables, not from message flags: a table with rows is
   * converted. The flags are set on the message because both the standalone and the distributed run
   * read them from there.
   */
  @Override
  protected void prepareForOrchestrator(DwcDpToVerbatimMessage message) {
    String stagePath =
        PathUtil.interpretedAttemptPath(
                pipelinesConfig.getOutputPath(),
                message.getDatasetUuid().toString(),
                message.getAttempt())
            + "/"
            + Pipeline.DWCDP_STAGE
            + ".yml";
    Map<String, Long> counts = MetricsUtil.readMetricsYaml(fileSystem, stagePath);
    message.setContainsEvents(counts.getOrDefault("COUNT_EVENT", 0L) > 0);
    message.setContainsOccurrences(counts.getOrDefault("COUNT_OCCURRENCE", 0L) > 0);
    log.info(
        "Staged data of {} attempt {}: events={}, occurrences={}",
        message.getDatasetUuid(),
        message.getAttempt(),
        message.isContainsEvents(),
        message.isContainsOccurrences());
  }

  /** What the conversion produced, for the next requests, and the conversion report. */
  @Override
  protected OrchestratorResult orchestratorResult(DwcDpToVerbatimMessage message) {
    String datasetId = message.getDatasetUuid().toString();
    long occurrenceCount =
        readMetric(datasetId, message.getAttempt(), Metrics.ARCHIVE_TO_OCC_COUNT);
    long eventCount = readMetric(datasetId, message.getAttempt(), Metrics.EVENT_CORE_RECORDS_COUNT);
    DatasetType datasetType =
        message.isContainsEvents() ? DatasetType.SAMPLING_EVENT : DatasetType.OCCURRENCE;
    Map<String, String> outputs =
        Map.of(
            OutputKeys.DATASET_TYPE, datasetType.name(),
            OutputKeys.OCCURRENCE_RECORDS, Long.toString(occurrenceCount),
            OutputKeys.EVENT_RECORDS, Long.toString(eventCount));
    return new OrchestratorResult(outputs, conversionReport(datasetId, message.getAttempt()));
  }

  /**
   * The compact plan and the statistics, as {@code {"compact": ..., "statistics": ...}}. The
   * detailed plan can be large and stays in HDFS. Missing files are left out.
   */
  private List<Report> conversionReport(String datasetId, int attempt) {
    String base =
        PathUtil.interpretedAttemptPath(pipelinesConfig.getInputPath(), datasetId, attempt) + "/";
    ObjectNode content = JSON.createObjectNode();
    readJson(base + DwcDpVerbatimConverter.INGEST_PLAN_COMPACT_JSON)
        .ifPresent(json -> content.set("compact", json));
    readJson(base + DwcDpVerbatimConverter.STATISTICS_REPORT_JSON)
        .ifPresent(json -> content.set("statistics", json));
    return content.isEmpty()
        ? List.of()
        : List.of(new Report(ReportTypes.DWCDP_CONVERSION, content, null));
  }

  private Optional<JsonNode> readJson(String path) {
    try (InputStream in = fileSystem.open(new Path(path))) {
      return Optional.of(JSON.readTree(in));
    } catch (IOException e) {
      log.warn("Conversion report {} not readable: {}", path, e.getMessage());
      return Optional.empty();
    }
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
