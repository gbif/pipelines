package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.pipelines.tasks.validators.validator.validate.DatasetTypeUtils.getDatasetType;

import java.net.URI;
import java.util.*;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.Validations;
import org.gbif.utils.file.ClosableIterator;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
public class DwcaArchiveValidator extends BaseDwcaArchiveValidator {

  @Builder
  public DwcaArchiveValidator(
      ArchiveValidatorConfiguration config,
      ValidationWsClient validationClient,
      SchemaValidatorFactory schemaValidatorFactory,
      PipelinesArchiveValidatorMessage message) {
    super(config, validationClient, schemaValidatorFactory, message);
  }

  @Override
  protected Validation runValidations(Validation validation) {
    Validation result = validateDwcaArchive(validation);
    if (config.failOnNoOccurrences) {
      validateOccurrencesPresent().ifPresent(fi -> Validations.mergeFileInfo(result, fi));
    }
    return result;
  }

  /**
   * Checks the archive contains occurrence records: an Occurrence core must not be empty, and a
   * non-Occurrence core must have a non-empty Occurrence extension.
   */
  @SneakyThrows
  private Optional<Metrics.FileInfo> validateOccurrencesPresent() {
    log.info("Checking occurrence records are present for {}", message.getDatasetUuid());
    Archive archive;
    try {
      archive =
          DwcaUtils.fromLocation(
              buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid()));
    } catch (UnsupportedArchiveException ex) {
      // Already reported as a fatal issue by the DwC-A validation
      return Optional.empty();
    }

    ArchiveFile core = archive.getCore();
    String coreFileName = core.getFirstLocationFile().getName();

    if (core.getRowType() == DwcTerm.Occurrence) {
      return isEmpty(core)
          ? Optional.of(
              fatal(
                  DwcFileType.CORE,
                  coreFileName,
                  EvaluationType.OCCURRENCE_CORE_EMPTY,
                  "Occurrence core file contains no records"))
          : Optional.empty();
    }

    ArchiveFile occurrenceExt = archive.getExtension(DwcTerm.Occurrence);
    if (occurrenceExt == null) {
      return Optional.of(
          fatal(
              DwcFileType.CORE,
              coreFileName,
              EvaluationType.OCCURRENCE_EXTENSION_MISSING,
              "Archive with a "
                  + core.getRowType().simpleName()
                  + " core has no Occurrence extension"));
    }

    if (isEmpty(occurrenceExt)) {
      return Optional.of(
          fatal(
              DwcFileType.EXTENSION,
              occurrenceExt.getFirstLocationFile().getName(),
              EvaluationType.OCCURRENCE_EXTENSION_EMPTY,
              "Occurrence extension file contains no records"));
    }

    return Optional.empty();
  }

  private static boolean isEmpty(ArchiveFile archiveFile) throws Exception {
    try (ClosableIterator<Record> it = archiveFile.iterator()) {
      return !it.hasNext();
    }
  }

  private static Metrics.FileInfo fatal(
      DwcFileType fileType, String fileName, EvaluationType type, String message) {
    return Metrics.FileInfo.builder()
        .fileType(fileType)
        .fileName(fileName)
        .issues(
            Collections.singletonList(Metrics.IssueInfo.create(type, Level.FATAL.name(), message)))
        .build();
  }

  @Override
  @SneakyThrows
  public PipelineBasedMessage createOutgoingMessage() {
    Optional<DatasetType> datasetTypeOpt =
        getDatasetType(config.archiveRepository, message.getDatasetUuid());
    PipelinesDwcaMessage m = new PipelinesDwcaMessage();
    m.setDatasetUuid(message.getDatasetUuid());
    m.setAttempt(message.getAttempt());
    m.setSource(new URI(config.stepConfig.registry.wsUrl));
    m.setValidationReport(
        new DwcaValidationReport(
            message.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)));
    m.setPipelineSteps(message.getPipelineSteps());
    m.setExecutionId(message.getExecutionId());
    datasetTypeOpt.ifPresent(m::setDatasetType);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    return m;
  }
}
