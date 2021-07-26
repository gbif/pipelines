package org.gbif.pipelines.crawler.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwc.Archive;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaTermUtils;
import org.gbif.pipelines.crawler.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ArchiveValidationReport;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DwcaArchiveValidator {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  @SneakyThrows
  public PipelinesDwcaMessage createOutgoingMessage() {
    PipelinesDwcaMessage m = new PipelinesDwcaMessage();
    m.setDatasetUuid(message.getDatasetUuid());
    m.setAttempt(message.getAttempt());
    m.setSource(new URI(config.stepConfig.registry.wsUrl));
    m.setValidationReport(
        new DwcaValidationReport(
            message.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)));
    m.setPipelineSteps(message.getPipelineSteps());
    m.setValidator(config.validatorOnly);
    m.setExecutionId(message.getExecutionId());
    m.setDatasetType(DatasetType.OCCURRENCE);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    m.setPlatform(Platform.PIPELINES);
    return m;
  }

  public void validate() {
    Metrics metrics = Metrics.builder().build();
    Optional<XmlSchemaValidatorResult> xmlSchemaValidatorResult = validateEmlSchema();
    if (xmlSchemaValidatorResult.isPresent()) {
      if (xmlSchemaValidatorResult.get().isValid()) {
        metrics = validateDwcaFile();
      }
      metrics.setXmlSchemaValidatorResult(xmlSchemaValidatorResult.get());
    }
    Validation validation = validationClient.get(message.getDatasetUuid());
    merge(validation, metrics);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
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

  @SneakyThrows
  private Optional<XmlSchemaValidatorResult> validateEmlSchema() {
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

  private Metrics validateDwcaFile() {
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
}
