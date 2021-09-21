package org.gbif.pipelines.crawler.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.validator.api.EvaluationType.OCCURRENCE_NOT_UNIQUELY_IDENTIFIED;
import static org.gbif.validator.api.EvaluationType.RECORD_NOT_UNIQUELY_IDENTIFIED;
import static org.gbif.validator.api.EvaluationType.RECORD_REFERENTIAL_INTEGRITY_VIOLATION;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.GenericValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.dwc.Archive;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.crawler.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.pipelines.validator.Validations;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DwcaArchiveValidator {

  protected static final String EML_XML = "eml.xml";

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;
  private final MessagePublisher publisher;

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
    getDatasetType().ifPresent(m::setDatasetType);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    m.setPlatform(Platform.PIPELINES);
    return m;
  }

  public void validate() {
    Validation validation = validationClient.get(message.getDatasetUuid());
    FileInfo emlFile = validateEmlSchema();
    FileInfo occurrenceFile = validateDwcaFile();

    Validations.mergeFileInfo(validation, emlFile);
    Validations.mergeFileInfo(validation, occurrenceFile);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  @SneakyThrows
  private FileInfo validateEmlSchema() {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());
    Path inputPath =
        buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid()).resolve(EML_XML);

    try {
      byte[] bytes = Files.readAllBytes(inputPath);
      String xmlDoc = new String(bytes, StandardCharsets.UTF_8);
      XmlSchemaValidatorResult result =
          schemaValidatorFactory.newValidatorFromDocument(xmlDoc).validate(xmlDoc);

      List<IssueInfo> issueInfos = Collections.emptyList();
      if (result != null) {
        issueInfos =
            result.getErrors().stream()
                .map(
                    x ->
                        IssueInfo.builder()
                            .issueCategory(EvaluationCategory.METADATA_CONTENT)
                            .extra(x.getError())
                            .build())
                .collect(Collectors.toList());
      }

      return FileInfo.builder()
          .fileType(DwcFileType.METADATA)
          .fileName(EML_XML)
          .issues(issueInfos)
          .build();

    } catch (Exception ex) {
      throw new IllegalArgumentException(ex.getMessage(), ex);
    }
  }

  private FileInfo validateDwcaFile() {
    log.info("Running DWCA validation for {}", message.getDatasetUuid());
    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
    Archive archive = DwcaUtils.fromLocation(inputPath);

    DatasetType datasetType = getDatasetType(archive);

    if (DatasetType.OCCURRENCE == datasetType) {

      DwcaValidationReport report =
          DwcaValidator.builder()
              .archive(archive)
              .datasetKey(message.getDatasetUuid())
              .datasetType(getDatasetType(archive))
              .maxExampleErrors(config.maxExampleErrors)
              .maxRecords(config.maxRecords)
              .build()
              .validate();

      List<IssueInfo> issueInfos = new ArrayList<>();

      // Generic report
      GenericValidationReport genericReport = report.getGenericReport();
      if (genericReport != null && !genericReport.isValid()) {
        if (genericReport.getDuplicateIds().size() > 0) {
          issueInfos.add(
              IssueInfo.builder()
                  .issueCategory(EvaluationCategory.RESOURCE_STRUCTURE)
                  .issue(RECORD_NOT_UNIQUELY_IDENTIFIED.name())
                  .extra(genericReport.getInvalidationReason())
                  .build());
        }
        if (genericReport.getRowNumbersMissingId().size() > 0) {
          issueInfos.add(
              IssueInfo.builder()
                  .issueCategory(EvaluationCategory.RESOURCE_STRUCTURE)
                  .issue(RECORD_REFERENTIAL_INTEGRITY_VIOLATION.name())
                  .extra(genericReport.getInvalidationReason())
                  .build());
        }
      }

      // Occurrence report
      OccurrenceValidationReport occurrenceReport = report.getOccurrenceReport();
      if (occurrenceReport != null && !occurrenceReport.isValid()) {
        issueInfos.add(
            IssueInfo.builder()
                .issueCategory(EvaluationCategory.RESOURCE_STRUCTURE)
                .issue(OCCURRENCE_NOT_UNIQUELY_IDENTIFIED.name())
                .extra(occurrenceReport.getInvalidationReason())
                .build());
      }

      if (issueInfos.isEmpty()) {
        return null;
      } else {
        return FileInfo.builder().fileType(DwcFileType.CORE).issues(issueInfos).build();
      }
    }

    if (DatasetType.CHECKLIST == datasetType) {
      sendChecklistValidatorMessage();
    }

    return null;
  }

  @SneakyThrows
  private void sendChecklistValidatorMessage() {
    PipelinesChecklistValidatorMessage checklistValidatorMessage =
        new PipelinesChecklistValidatorMessage(
            message.getDatasetUuid(),
            message.getAttempt(),
            message.getPipelineSteps(),
            message.getExecutionId(),
            message.getFileFormat());
    publisher.send(checklistValidatorMessage);
  }

  /** Gets the dataset type from the Archive parameter. */
  private static DatasetType getDatasetType(Archive archive) {
    return DwcTerm.Taxon == archive.getCore().getRowType()
        ? DatasetType.CHECKLIST
        : DatasetType.OCCURRENCE;
  }

  /** Gets the dataset type form the current archive data. */
  private Optional<DatasetType> getDatasetType() {
    try {
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      return Optional.of(getDatasetType(DwcaUtils.fromLocation(inputPath)));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
