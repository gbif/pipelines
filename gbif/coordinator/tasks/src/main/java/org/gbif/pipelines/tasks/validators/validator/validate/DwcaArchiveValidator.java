package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.validator.api.DwcFileType.CORE;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.validation.MetadataPath;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.BasicMetadataEvaluator;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.FileInfo.FileInfoBuilder;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DwcaArchiveValidator implements ArchiveValidator {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  @Override
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
    m.setExecutionId(message.getExecutionId());
    getDatasetType().ifPresent(m::setDatasetType);
    m.setEndpointType(EndpointType.DWC_ARCHIVE);
    m.setPlatform(Platform.PIPELINES);
    return m;
  }

  @Override
  public void validate() {
    log.info("Running DWCA validator");
    Validation validation = validationClient.get(message.getDatasetUuid());

    // EML
    FileInfo emlFile = validateEmlFile();
    Validations.mergeFileInfo(validation, emlFile);

    // Occurrence
    validateOccurrenceFile()
        .ifPresent(occurrenceFile -> Validations.mergeFileInfo(validation, occurrenceFile));

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);

    boolean hasFatalIssues =
        validation.getMetrics().getFileInfos().stream()
            .flatMap(x -> x.getIssues().stream())
            .flatMap(x -> x.getSamples().stream())
            .flatMap(x -> x.getRelatedData().keySet().stream())
            .anyMatch(x -> x.equals(Level.FATAL.name()));
    if (hasFatalIssues) {
      throw new IllegalArgumentException("Discovered fatal issue");
    }
  }

  @SneakyThrows
  private FileInfo validateEmlFile() {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());

    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());

    Optional<Path> emlPath = MetadataPath.parsePath(inputPath);

    FileInfoBuilder fileInfoBuilder = FileInfo.builder().fileType(DwcFileType.METADATA);

    if (!emlPath.isPresent()) {
      return fileInfoBuilder
          .issues(
              Collections.singletonList(
                  IssueInfo.create(
                      EvaluationType.EML_NOT_FOUND,
                      Level.FATAL.name(),
                      "metadata file was not found")))
          .build();
    }

    try {
      String xmlDoc = new String(Files.readAllBytes(emlPath.get()), StandardCharsets.UTF_8);

      List<IssueInfo> issueInfos = new ArrayList<>();
      // Validate XML file
      issueInfos.addAll(schemaValidatorFactory.validate(xmlDoc));
      // Check licence, authors and etc
      issueInfos.addAll(BasicMetadataEvaluator.evaluate(xmlDoc));

      return fileInfoBuilder
          .issues(issueInfos)
          .fileName(emlPath.get().getFileName().toString())
          .build();

    } catch (Exception ex) {
      return fileInfoBuilder
          .fileName(emlPath.get().toString())
          .issues(
              Collections.singletonList(
                  IssueInfo.create(
                      EvaluationType.EML_GBIF_SCHEMA,
                      Level.FATAL.name(),
                      ex.getLocalizedMessage())))
          .build();
    }
  }

  private Optional<FileInfo> validateOccurrenceFile() {

    FileInfoBuilder fileInfoBuilder =
        FileInfo.builder().rowType(DwcTerm.Occurrence.qualifiedName());

    try {
      log.info("Running DWCA validation for {}", message.getDatasetUuid());
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive = DwcaUtils.fromLocation(inputPath);

      List<IssueInfo> issueInfos =
          DwcaValidator.builder()
              .archive(archive)
              .datasetKey(message.getDatasetUuid())
              .datasetType(getDatasetType(archive))
              .maxExampleErrors(config.maxExampleErrors)
              .maxRecords(config.maxRecords)
              .build()
              .validate();

      String fileName;
      DwcFileType dwcFileType;
      if (archive.getCore().getRowType() == DwcTerm.Occurrence) {
        fileName = archive.getCore().getFirstLocationFile().getName();
        dwcFileType = CORE;
      } else if (archive.getExtension(DwcTerm.Occurrence) != null) {
        fileName = archive.getExtension(DwcTerm.Occurrence).getFirstLocationFile().getName();
        dwcFileType = DwcFileType.EXTENSION;
      } else {
        return Optional.empty();
      }

      fileInfoBuilder.fileType(dwcFileType).fileName(fileName).issues(issueInfos).build();

    } catch (UnsupportedArchiveException ex) {
      fileInfoBuilder.issues(
          Collections.singletonList(
              IssueInfo.create(
                  EvaluationType.UNHANDLED_ERROR, Level.FATAL.name(), ex.getLocalizedMessage())));
    }
    return Optional.of(fileInfoBuilder.build());
  }

  /** Gets the dataset type from the Archive parameter. */
  private static DatasetType getDatasetType(Archive archive) {
    Term rowType = archive.getCore().getRowType();
    if (rowType == DwcTerm.Occurrence) {
      return DatasetType.OCCURRENCE;
    }
    if (rowType == DwcTerm.Event) {
      return DatasetType.SAMPLING_EVENT;
    }
    if (rowType == DwcTerm.Taxon) {
      return DatasetType.CHECKLIST;
    }
    throw new IllegalArgumentException("DatasetType is not valid");
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
