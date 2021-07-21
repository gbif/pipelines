package org.gbif.pipelines.crawler.validator;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwc.Archive;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaTermUtils;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ArchiveValidationReport;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesArchiveValidatorMessage} is received. */
@Slf4j
public class ArchiveValidatorCallback
    extends AbstractMessageCallback<PipelinesArchiveValidatorMessage>
    implements StepHandler<PipelinesArchiveValidatorMessage, PipelineBasedMessage> {

  private final ArchiveValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;

  public ArchiveValidatorCallback(
      ArchiveValidatorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ValidationWsClient validationClient,
      SchemaValidatorFactory schemaValidatorFactory) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.historyClient = historyClient;
    this.validationClient = validationClient;
    this.schemaValidatorFactory = schemaValidatorFactory;
  }

  @Override
  public void handleMessage(PipelinesArchiveValidatorMessage message) {
    PipelinesCallback.<PipelinesArchiveValidatorMessage, PipelineBasedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .curator(curator)
        .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesArchiveValidatorMessage message) {
    return message.getFileFormat() != null && message.getDatasetUuid() != null;
  }

  @Override
  public Runnable createRunnable(PipelinesArchiveValidatorMessage message) {
    return () -> {
      log.info("Running validatoin for {}", message.getDatasetUuid());
      if (message.getFileFormat().equals(FileFormat.DWCA.name())) {
        validateDwca(message);
      } else {
        log.info("File format {} is not supported!", message.getFileFormat());
        Validation validation = validationClient.get(message.getDatasetUuid());
        validation.setStatus(Status.FAILED);
        validationClient.update(validation);
      }
    };
  }

  private void merge(Validation validation, Metrics metrics) {
    if (validation != null && metrics != null) {
      Metrics validationMetrics = validation.getMetrics();
      if (validationMetrics == null) {
        validation.setMetrics(metrics);
      } else {
        validationMetrics.setArchiveValidationReport(metrics.getArchiveValidationReport());
        validationMetrics.setXmlSchemaValidatorResult(metrics.getXmlSchemaValidatorResult());
      }
    }
  }

  private void validateDwca(PipelinesArchiveValidatorMessage message) {
    Metrics metrics = Metrics.builder().build();
    Optional<XmlSchemaValidatorResult> xmlSchemaValidatorResult = validateEmlSchema(message);
    if (xmlSchemaValidatorResult.isPresent()) {
      if (xmlSchemaValidatorResult.get().isValid()) {
        metrics = validateDwcaFile(message);
      }
      metrics.setXmlSchemaValidatorResult(xmlSchemaValidatorResult.get());
    }
    Validation validation = validationClient.get(message.getDatasetUuid());
    merge(validation, metrics);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  @SneakyThrows
  private Optional<XmlSchemaValidatorResult> validateEmlSchema(
      PipelinesArchiveValidatorMessage message) {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());
    Path inputPath =
        buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid()).resolve("eml.xml");

    try {
      byte[] bytes = Files.readAllBytes(inputPath);
      String xmlDoc = new String(bytes, StandardCharsets.UTF_8);
      return Optional.ofNullable(
          schemaValidatorFactory.newValidatorFromDocument(xmlDoc).validate(xmlDoc));
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex.getMessage(), ex);
    }
  }

  private Metrics validateDwcaFile(PipelinesArchiveValidatorMessage message) {
    log.info("Running DWCA validation for {}", message.getDatasetUuid());
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Archive archive = DwcaTermUtils.fromLocation(inputPath);

    DwcaValidationReport report =
        DwcaValidator.builder()
            .archive(archive)
            .datasetKey(message.getDatasetUuid())
            .datasetType(DatasetType.OCCURRENCE)
            .maxExampleErrors(config.maxExampleErrors)
            .maxRecords(config.maxRecords)
            .build()
            .validate();

    return Metrics.builder()
        .archiveValidationReport(
            ArchiveValidationReport.builder()
                .genericReport(report.getGenericReport())
                .occurrenceReport(report.getOccurrenceReport())
                .invalidationReason(report.getInvalidationReason())
                .build())
        .build();
  }

  @SneakyThrows
  @Override
  public PipelineBasedMessage createOutgoingMessage(PipelinesArchiveValidatorMessage message) {
    if (message.getFileFormat().equals(FileFormat.DWCA.name())) {
      return new PipelinesDwcaMessage(
          message.getDatasetUuid(),
          DatasetType.OCCURRENCE,
          new URI(config.stepConfig.registry.wsUrl),
          message.getAttempt(),
          new DwcaValidationReport(
              message.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)),
          message.getPipelineSteps(),
          EndpointType.DWC_ARCHIVE,
          Platform.PIPELINES,
          message.getExecutionId(),
          true);
    }
    throw new IllegalArgumentException(
        "File format " + message.getFileFormat() + " is not supported");
  }
}
