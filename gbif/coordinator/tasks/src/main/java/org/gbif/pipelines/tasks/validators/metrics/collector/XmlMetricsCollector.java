package org.gbif.pipelines.tasks.validators.metrics.collector;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.converters.utils.XmlFilesReader;
import org.gbif.converters.utils.XmlTermExtractor;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.pojo.FileNameTerm;
import org.gbif.pipelines.tasks.client.RetryingValidationClient;
import org.gbif.pipelines.tasks.validators.metrics.MetricsCollectorConfiguration;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.IndexableRules;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;

/**
 * {@link MetricsCollector} for XML-based (e.g. ABCD) archives. Raw term counts come from the XML
 * source files, and indexed counts / issues / interpreted term counts are overlaid from the {@code
 * collect-metrics.json} written by the {@code ValidatorMetricsPipeline} Spark job.
 */
@Slf4j
@Builder
public class XmlMetricsCollector implements MetricsCollector {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final RetryingValidationClient validationClient;
  private final PipelinesIndexedMessage message;
  private final StepType stepType;

  @SneakyThrows
  @Override
  public void collect() {
    log.info("Collect {} metrics for {}", message.getEndpointType(), message.getDatasetUuid());
    collectMetrics(message);
  }

  @SneakyThrows
  private void collectMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());

    List<File> files = XmlFilesReader.getInputFiles(inputPath.toFile());
    // Extract all terms
    List<FileInfo> fileInfos = convertToFileInfo(files);

    // Read Spark-computed metrics from HDFS
    Metrics sparkMetrics = SparkMetricsReader.readSparkMetrics(config, message);

    // Overlay Spark metrics onto the XML-derived file infos
    SparkMetricsReader.applySparkMetrics(fileInfos, sparkMetrics);

    // Get saved metrics object and merge with the result
    Validation validation = validationClient.get(message.getDatasetUuid());
    Validations.mergeWithValidation(validation, Metrics.builder().fileInfos(fileInfos).build());

    SparkMetricsReader.updateIssuesFromMetaInfos(config, message, validation);

    // Set isIndexable
    validation
        .getMetrics()
        .setIndexeable(IndexableRules.isIndexable(stepType, validation.getMetrics()));

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation.getKey(), validation);
  }

  private List<FileInfo> convertToFileInfo(List<File> files) {
    XmlTermExtractor extractor = XmlTermExtractor.extract(files);
    Map<FileNameTerm, Set<Term>> coreTerms = extractor.getCore();
    Map<FileNameTerm, Set<Term>> extensionsTerms = extractor.getExtenstionsTerms();

    List<FileInfo> fileInfos = new ArrayList<>();

    // Core file
    coreTerms.forEach(
        (key, value) -> {
          List<TermInfo> termInfoList =
              value.stream()
                  .map(x -> TermInfo.builder().term(x.qualifiedName()).build())
                  .collect(Collectors.toList());

          FileInfo info =
              FileInfo.builder()
                  .fileName(key.getFileName())
                  .rowType(key.getTermQualifiedName())
                  .fileType(DwcFileType.CORE)
                  .terms(termInfoList)
                  .build();
          fileInfos.add(info);
        });

    // Extension file
    extensionsTerms.forEach(
        (key, value) -> {
          List<TermInfo> termInfoList =
              value.stream()
                  .map(x -> TermInfo.builder().term(x.qualifiedName()).build())
                  .collect(Collectors.toList());

          FileInfo info =
              FileInfo.builder()
                  .fileName(key.getFileName())
                  .rowType(key.getTermQualifiedName())
                  .fileType(DwcFileType.EXTENSION)
                  .terms(termInfoList)
                  .build();
          fileInfos.add(info);
        });

    return fileInfos;
  }
}
