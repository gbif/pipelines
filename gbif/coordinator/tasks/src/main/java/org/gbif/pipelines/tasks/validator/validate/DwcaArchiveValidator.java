package org.gbif.pipelines.tasks.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

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
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validator.ArchiveValidatorConfiguration;
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

  protected static final String EML_XML = "eml.xml";

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
    m.setValidator(config.validatorOnly);
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
    FileInfo emlFile = validateEmlFile();
    FileInfo occurrenceFile = validateDwcaFile();

    Validations.mergeFileInfo(validation, emlFile);
    Validations.mergeFileInfo(validation, occurrenceFile);

    log.info("Update validation key {}", message.getDatasetUuid());
    validationClient.update(validation);
  }

  @SneakyThrows
  private FileInfo validateEmlFile() {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());
    Path inputPath =
        buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid()).resolve(EML_XML);

    FileInfoBuilder fileInfoBuilder =
        FileInfo.builder().fileType(DwcFileType.METADATA).fileName(EML_XML);

    if (!Files.exists(inputPath)) {
      return fileInfoBuilder
          .issues(
              Collections.singletonList(
                  IssueInfo.create(
                      EvaluationType.EML_NOT_FOUND,
                      Level.FATAL.name(),
                      "meta.xml file was not found")))
          .build();
    }

    try {
      String xmlDoc = new String(Files.readAllBytes(inputPath), StandardCharsets.UTF_8);

      List<IssueInfo> issueInfos = new ArrayList<>();
      // Validate XML file
      issueInfos.addAll(schemaValidatorFactory.validate(xmlDoc));
      // Check licence, authors and etc
      issueInfos.addAll(BasicMetadataEvaluator.evaluate(xmlDoc));

      return fileInfoBuilder.issues(issueInfos).build();

    } catch (Exception ex) {
      return fileInfoBuilder
          .issues(
              Collections.singletonList(
                  IssueInfo.create(
                      EvaluationType.EML_GBIF_SCHEMA,
                      Level.FATAL.name(),
                      ex.getLocalizedMessage())))
          .build();
    }
  }

  private FileInfo validateDwcaFile() {
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
      fileName = archive.getCore().getLocationFile().getName();
      dwcFileType = DwcFileType.CORE;
    } else {
      fileName = archive.getExtension(DwcTerm.Occurrence).getLocationFile().getName();
      dwcFileType = DwcFileType.EXTENSION;
    }

    return FileInfo.builder()
        .fileType(dwcFileType)
        .rowType(DwcTerm.Occurrence.qualifiedName())
        .fileName(fileName)
        .issues(issueInfos)
        .build();
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
