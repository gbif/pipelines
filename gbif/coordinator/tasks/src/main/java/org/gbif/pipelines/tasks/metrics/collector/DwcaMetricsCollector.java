package org.gbif.pipelines.tasks.metrics.collector;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.pojo.FileNameTerm;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.metrics.MetricsCollectorConfiguration;
import org.gbif.pipelines.validator.IndexMetricsCollector;
import org.gbif.pipelines.validator.LineCounter;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.IndexableRules;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DwcaMetricsCollector implements MetricsCollector {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final ValidationWsClient validationClient;
  private final PipelinesIndexedMessage message;
  private final StepType stepType;

  @Override
  public void collect() {
    log.info("Collect {} metrics for {}", message.getEndpointType(), message.getDatasetUuid());
    collectMetrics(message);
    collectChecklistsMetrics(message);
  }

  @SneakyThrows
  private void collectChecklistsMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Archive archive = DwcaUtils.fromLocation(inputPath);
    if (archive.getExtension(DwcTerm.Taxon) != null
        || DwcTerm.Taxon == archive.getCore().getRowType()) {
      PipelinesChecklistValidatorMessage checklistValidatorMessage =
          new PipelinesChecklistValidatorMessage(
              message.getDatasetUuid(),
              message.getAttempt(),
              message.getPipelineSteps(),
              message.getExecutionId(),
              FileFormat.DWCA.name());

      // Set status to QUEUED before sending the message
      org.gbif.pipelines.tasks.Validations.updateStatus(
          validationClient, message.getDatasetUuid(), stepType, Status.QUEUED);

      log.info("Send checklist validation message and waiting for the response...");
      Object response =
          publisher.sendAndReceive(
              checklistValidatorMessage, // message
              PipelinesChecklistValidatorMessage.ROUTING_KEY, // routing key
              false, // persistent message
              UUID.randomUUID().toString()); // correlationId
      log.info("Checklist validation has finished, the response is received - {}", response);
    }
  }

  @SneakyThrows
  private void collectMetrics(PipelinesIndexedMessage message) {
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());

    Archive archive = DwcaUtils.fromLocation(inputPath);
    ArchiveFile core = archive.getCore();
    // Get core file name
    String coreFileName = core.getLocationFile().getName();
    // Extract all terms
    Map<FileNameTerm, Set<Term>> coreTerms = DwcaUtils.getCoreTerms(archive);
    Map<FileNameTerm, Set<Term>> extensionsTerms = DwcaUtils.getExtensionsTerms(archive);

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

    // Count core file lines
    setFileInfo(metrics, coreFileName, LineCounter.count(core));

    // Count extensions files lines
    archive
        .getExtensions()
        .forEach(ext -> setFileInfo(metrics, ext.getTitle(), LineCounter.count(ext)));

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

  private void setFileInfo(Metrics metrics, String fileName, Long count) {
    for (FileInfo fileInfo : metrics.getFileInfos()) {
      if (fileInfo.getFileName().equals(fileName)) {
        fileInfo.setCount(count);
      }
    }
  }
}
