package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.pipelines.tasks.validators.validator.validate.DatasetTypeUtils.getDatasetType;
import static org.gbif.validator.api.DwcFileType.CORE;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.MetadataPath;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaFileTermCounter;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.BasicMetadataEvaluator;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@AllArgsConstructor
public abstract class BaseDwcaArchiveValidator implements ArchiveValidator {

  protected final ArchiveValidatorConfiguration config;
  protected final ValidationWsClient validationClient;
  protected final SchemaValidatorFactory schemaValidatorFactory;
  protected final PipelinesArchiveValidatorMessage message;

  @Override
  public void validate() {
    log.info("Running DWCA validator");
    Validation validation = validationClient.get(message.getDatasetUuid());

    validation = runValidations(validation);

    log.info("Update validation {} with key {}", validation, message.getDatasetUuid());
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

  protected abstract Validation runValidations(Validation validation);

  protected Validation validateDwcaArchive(Validation validation) {
    // EML
    Metrics.FileInfo emlFile = validateEmlFile();
    Validations.mergeFileInfo(validation, emlFile);

    // Core file (Occurrence, Event or Checklist) and, when present, the Occurrence extension
    // (e.g. attached to a Sampling Event dataset)
    List<Metrics.FileInfo> validatedFileInfos = validateDwcaFiles();
    validatedFileInfos.forEach(fileInfo -> Validations.mergeFileInfo(validation, fileInfo));

    // add term counts
    List<Metrics.FileInfo> termCounts =
        DwcaFileTermCounter.process(
            DwcaUtils.fromLocation(
                buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid())));
    termCounts.forEach(fileInfo -> Validations.mergeFileInfo(validation, fileInfo));

    return validation;
  }

  @SneakyThrows
  private Metrics.FileInfo validateEmlFile() {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());

    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());

    Optional<Path> emlPath = MetadataPath.parsePath(inputPath);

    Metrics.FileInfo.FileInfoBuilder fileInfoBuilder =
        Metrics.FileInfo.builder().fileType(DwcFileType.METADATA);

    if (emlPath.isEmpty()) {
      return fileInfoBuilder
          .issues(
              Collections.singletonList(
                  Metrics.IssueInfo.create(
                      EvaluationType.EML_NOT_FOUND,
                      Level.FATAL.name(),
                      "metadata file was not found")))
          .build();
    }

    try {
      String xmlDoc = Files.readString(emlPath.get());

      List<Metrics.IssueInfo> issueInfos = new ArrayList<>();
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
                  Metrics.IssueInfo.create(
                      EvaluationType.EML_GBIF_SCHEMA,
                      Level.FATAL.name(),
                      ex.getLocalizedMessage())))
          .build();
    }
  }

  /**
   * Validates the DwC-A core file (Occurrence, Event or Checklist) and, when present, the
   * Occurrence extension attached to a non-Occurrence core (e.g. a Sampling Event dataset).
   */
  private List<Metrics.FileInfo> validateDwcaFiles() {
    try {
      log.info("Running DWCA validation for {}", message.getDatasetUuid());
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive = DwcaUtils.fromLocation(inputPath);
      DatasetType datasetType = getDatasetType(archive);

      DwcaValidationReport report =
          DwcaValidator.builder()
              .archive(archive)
              .datasetKey(message.getDatasetUuid())
              .datasetType(datasetType)
              .maxExampleErrors(config.maxExampleErrors)
              .maxRecords(config.maxRecords)
              .build()
              .validateAsReport();

      List<Metrics.FileInfo> fileInfos = new ArrayList<>();

      // Core file: Occurrence core uses the occurrence report, Event/Checklist core uses the
      // generic report
      boolean isOccurrenceCore = archive.getCore().getRowType() == DwcTerm.Occurrence;
      List<Metrics.IssueInfo> coreIssues =
          isOccurrenceCore
              ? DwcaValidator.occurrenceIssues(report.getOccurrenceReport())
              : DwcaValidator.genericIssues(report.getGenericReport());
      fileInfos.add(
          Metrics.FileInfo.builder()
              .rowType(archive.getCore().getRowType().qualifiedName())
              .fileType(CORE)
              .fileName(archive.getCore().getFirstLocationFile().getName())
              .issues(coreIssues)
              .build());

      // Occurrence extension, e.g. attached to a Sampling Event dataset
      if (!isOccurrenceCore && archive.getExtension(DwcTerm.Occurrence) != null) {
        fileInfos.add(
            Metrics.FileInfo.builder()
                .rowType(DwcTerm.Occurrence.qualifiedName())
                .fileType(DwcFileType.EXTENSION)
                .fileName(archive.getExtension(DwcTerm.Occurrence).getFirstLocationFile().getName())
                .issues(DwcaValidator.occurrenceIssues(report.getOccurrenceReport()))
                .build());
      }

      return fileInfos;

    } catch (UnsupportedArchiveException ex) {
      return Collections.singletonList(
          Metrics.FileInfo.builder()
              .issues(
                  Collections.singletonList(
                      Metrics.IssueInfo.create(
                          EvaluationType.UNHANDLED_ERROR,
                          Level.FATAL.name(),
                          ex.getLocalizedMessage())))
              .build());
    }
  }
}
