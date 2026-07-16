package org.gbif.pipelines.tasks.validators.metrics.collector;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.validator.api.DwcFileType.CORE;
import static org.gbif.validator.api.EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.pipelines.validator.DwcaFileTermCounter;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.IndexableRules;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

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

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final ValidationWsClient validationClient;
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
    SparkMetrics sparkMetrics = readSparkMetrics();

    // Overlay Spark metrics onto the archive-derived file infos
    applySparkMetrics(fileInfos, sparkMetrics);

    // Get saved metrics object and merge with the result
    Validation validation = validationClient.get(message.getDatasetUuid());
    Validations.mergeWithValidation(validation, Metrics.builder().fileInfos(fileInfos).build());

    updateIssuesFromMetaInfos(validation);

    validation
        .getMetrics()
        .setIndexeable(IndexableRules.isIndexable(stepType, validation.getMetrics()));

    log.info("Updating validation for {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  private SparkMetrics readSparkMetrics() throws IOException {
    StepConfiguration stepConfig = config.stepConfig;
    String datasetId = message.getDatasetUuid().toString();
    String attempt = message.getAttempt().toString();

    String metricsPath =
        String.join("/", stepConfig.repositoryPath, datasetId, attempt, "collect-metrics.json");

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);

    try (FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, metricsPath)) {
      org.apache.hadoop.fs.Path fsPath = new org.apache.hadoop.fs.Path(metricsPath);
      if (!fs.exists(fsPath)) {
        throw new IOException(
            "Spark validator metrics file not found at "
                + metricsPath
                + " — ensure ValidatorMetricsPipeline ran before this step.");
      }
      try (var in = fs.open(fsPath)) {
        return MAPPER.readValue(in.getWrappedStream(), SparkMetrics.class);
      }
    }
  }

  /**
   * Overlays Spark-computed indexed counts, issue info, and interpreted term counts onto {@code
   * fileInfos} produced by reading the archive.
   */
  private void applySparkMetrics(List<FileInfo> fileInfos, SparkMetrics sparkMetrics) {
    for (FileInfo fileInfo : fileInfos) {
      if (fileInfo.getRowType() == null) continue;

      if (DwcTerm.Occurrence.qualifiedName().equals(fileInfo.getRowType())) {
        fileInfo.setIndexedCount(sparkMetrics.indexedCount);
        fileInfo.setIssues(
            sparkMetrics.issues != null ? sparkMetrics.issues : Collections.emptyList());

        if (sparkMetrics.interpretedFieldCounts != null) {
          for (TermInfo termInfo : fileInfo.getTerms()) {
            Long count = sparkMetrics.interpretedFieldCounts.get(termInfo.getTerm());
            if (count != null) {
              termInfo.setInterpretedIndexed(count);
            }
          }
        }
      }
    }
  }

  @SneakyThrows
  private void updateIssuesFromMetaInfos(Validation validation) {
    StepConfiguration stepConfig = config.stepConfig;
    String datasetId = message.getDatasetUuid().toString();
    String attempt = message.getAttempt().toString();
    String metaFileName = config.interpretationMetaFileName;

    String metaPath = String.join("/", stepConfig.repositoryPath, datasetId, attempt, metaFileName);
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);
    Optional<Long> fileNumber =
        HdfsUtils.getLongByKey(
            hdfsConfigs,
            metaPath,
            PipelinesVariables.Metrics.DUPLICATE_IDS_COUNT + PipelinesVariables.Metrics.ATTEMPTED);

    if (fileNumber.isPresent() && fileNumber.get() > 0L) {
      for (FileInfo file : validation.getMetrics().getFileInfos()) {
        if (file.getFileType().equals(CORE)) {
          if (file.getIssues() == null || file.getIssues().isEmpty()) {
            file.setIssues(
                Collections.singletonList(IssueInfo.create(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED)));
          } else if (file.getIssues().stream()
              .noneMatch(x -> x.getIssue().equals(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED.name()))) {
            file.getIssues().add(IssueInfo.create(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED));
          }
        }
      }
    }
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

  /** Internal representation of the JSON file written by {@code ValidatorMetricsPipeline}. */
  private static class SparkMetrics {
    public long indexedCount;
    public List<IssueInfo> issues;
    public Map<String, Long> interpretedFieldCounts;
  }
}
