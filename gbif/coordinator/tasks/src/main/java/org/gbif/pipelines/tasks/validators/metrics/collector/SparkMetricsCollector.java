package org.gbif.pipelines.tasks.validators.metrics.collector;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.pipelines.validator.DwcaFileTermCounter;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.IndexableRules;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;

/**
 * {@link MetricsCollector} that reads pre-computed metrics from an HDFS JSON file written by the
 * {@code ValidatorMetricsPipeline} Spark job instead of querying Elasticsearch.
 *
 * <p>The Spark job must have run and written {@code collect-metrics.json} to the dataset's output
 * directory before this collector is invoked.
 */
@Slf4j
@Builder
public class SparkMetricsCollector implements MetricsCollector {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final RetryingValidationClient validationClient;
  private final PipelinesIndexedMessage message;
  private final StepType stepType;

  @Override
  public void collect() {
    log.info(
        "Collecting Spark-based metrics for {} ({})",
        message.getDatasetUuid(),
        message.getEndpointType());

    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Archive archive = DwcaUtils.fromLocation(inputPath);

    collectMetrics(archive);
    collectChecklistsMetrics(archive);
  }

  @SneakyThrows
  private void collectMetrics(Archive archive) {
    // Raw term counts come from the archive (same as DwcaMetricsCollector)
    List<FileInfo> fileInfos = DwcaFileTermCounter.process(archive);

    // Read Spark-computed metrics from HDFS
    Metrics sparkMetrics = SparkMetricsReader.readSparkMetrics(config, message);

    // Overlay Spark metrics onto the archive-derived file infos
    SparkMetricsReader.applySparkMetrics(fileInfos, sparkMetrics);

    // Get saved metrics object and merge with the result
    Validation validation = validationClient.get(message.getDatasetUuid());
    Validations.mergeWithValidation(validation, Metrics.builder().fileInfos(fileInfos).build());

    SparkMetricsReader.updateIssuesFromMetaInfos(config, message, validation);

    validation
        .getMetrics()
        .setIndexeable(IndexableRules.isIndexable(stepType, validation.getMetrics()));

    log.info("Updating validation for {}", message.getDatasetUuid());
    validationClient.update(validation.getKey(), validation);
  }

  @SneakyThrows
  private void collectChecklistsMetrics(Archive archive) {
    if (archive.getExtension(DwcTerm.Taxon) != null
        || DwcTerm.Taxon == archive.getCore().getRowType()) {
      PipelinesChecklistValidatorMessage checklistValidatorMessage =
          new PipelinesChecklistValidatorMessage(
              message.getDatasetUuid(),
              message.getAttempt(),
              message.getPipelineSteps(),
              message.getExecutionId(),
              FileFormat.DWCA.name());

      org.gbif.pipelines.tasks.Validations.updateStatus(
          validationClient, message.getDatasetUuid(), stepType, Status.QUEUED);

      log.info("Sending checklist validation message for {}", message.getDatasetUuid());
      Object response =
          publisher.sendAndReceive(
              checklistValidatorMessage,
              PipelinesChecklistValidatorMessage.ROUTING_KEY,
              false,
              UUID.randomUUID().toString());
      log.info("Checklist validation completed, response: {}", response);
    }
  }
}
