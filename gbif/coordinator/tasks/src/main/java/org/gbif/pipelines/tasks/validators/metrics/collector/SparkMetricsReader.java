package org.gbif.pipelines.tasks.validators.metrics.collector;

import static org.gbif.validator.api.DwcFileType.CORE;
import static org.gbif.validator.api.EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;

/**
 * Shared helpers for {@link MetricsCollector} implementations that overlay metrics computed by the
 * {@code ValidatorMetricsPipeline} Spark job (read from {@code collect-metrics.json} on HDFS) onto
 * {@link FileInfo}s derived from the raw source archive.
 */
@UtilityClass
public class SparkMetricsReader {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @SneakyThrows
  public Metrics readSparkMetrics(
      MetricsCollectorConfiguration config, PipelinesIndexedMessage message) {
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
        return MAPPER.readValue(in.getWrappedStream(), Metrics.class);
      }
    }
  }

  /**
   * Overlays Spark-computed indexed counts, issue info, and interpreted term counts onto {@code
   * fileInfos} produced by reading the raw source archive.
   */
  public void applySparkMetrics(List<FileInfo> fileInfos, Metrics sparkMetrics) {
    if (sparkMetrics.getFileInfos() == null) return;

    sparkMetrics.getFileInfos().stream()
        .filter(sf -> DwcTerm.Occurrence.qualifiedName().equals(sf.getRowType()))
        .findFirst()
        .ifPresent(
            sparkFileInfo -> {
              Map<String, Long> interpretedCounts =
                  sparkFileInfo.getTerms() == null
                      ? Collections.emptyMap()
                      : sparkFileInfo.getTerms().stream()
                          .filter(t -> t.getInterpretedIndexed() != null)
                          .collect(
                              Collectors.toMap(TermInfo::getTerm, TermInfo::getInterpretedIndexed));

              for (FileInfo fileInfo : fileInfos) {
                if (!DwcTerm.Occurrence.qualifiedName().equals(fileInfo.getRowType())) continue;
                fileInfo.setIndexedCount(sparkFileInfo.getIndexedCount());
                fileInfo.setIssues(
                    sparkFileInfo.getIssues() != null
                        ? sparkFileInfo.getIssues()
                        : Collections.emptyList());
                for (TermInfo termInfo : fileInfo.getTerms()) {
                  Long count = interpretedCounts.get(termInfo.getTerm());
                  if (count != null) {
                    termInfo.setInterpretedIndexed(count);
                  }
                }
              }
            });
  }

  @SneakyThrows
  public void updateIssuesFromMetaInfos(
      MetricsCollectorConfiguration config,
      PipelinesIndexedMessage message,
      Validation validation) {
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
}
