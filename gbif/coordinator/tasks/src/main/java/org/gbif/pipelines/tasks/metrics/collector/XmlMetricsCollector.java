package org.gbif.pipelines.tasks.metrics.collector;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.converters.utils.XmlFilesReader;
import org.gbif.converters.utils.XmlTermExtractor;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.tasks.metrics.MetricsCollectorConfiguration;
import org.gbif.pipelines.validator.IndexMetricsCollector;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.IndexableRules;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class XmlMetricsCollector implements MetricsCollector {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final ValidationWsClient validationClient;
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
    XmlTermExtractor extractor = XmlTermExtractor.extract(files);
    Set<Term> coreTerms = extractor.getCore();
    Map<Extension, Set<Term>> extensionsTerms = extractor.getExtenstionsTerms();

    // Collect metrics from ES
    Metrics metrics =
        IndexMetricsCollector.builder()
            .coreTerms(coreTerms)
            .extensionsTerms(extensionsTerms)
            .key(message.getDatasetUuid())
            .index(config.indexName)
            .corePrefix(config.corePrefix)
            .extensionsPrefix(config.extensionsPrefix)
            .esHost(config.esConfig.hosts)
            .build()
            .collect();

    // Get saved metrics object and merge with the result
    Validation validation = validationClient.get(message.getDatasetUuid());
    Validations.mergeWithValidation(validation, metrics);

    // Set isIndexable
    validation
        .getMetrics()
        .setIndexeable(IndexableRules.isIndexable(stepType, validation.getMetrics()));

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }
}
