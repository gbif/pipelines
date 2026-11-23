package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.pipelines.tasks.validators.validator.validate.DatasetTypeUtils.getDatasetType;
import static org.gbif.validator.api.DwcFileType.CORE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.eml.validation.ArchiveMetadata;
import org.gbif.eml.validation.EmlIssue;
import org.gbif.eml.validation.EmlSchema;
import org.gbif.eml.validation.EmlValidator;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaFileTermCounter;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.pipelines.validator.Validations;
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
      log.error("Discovered fatal issues in validation for dataset {}", message.getDatasetUuid());
      throw new IllegalArgumentException("Discovered fatal issue");
    }
  }

  protected abstract Validation runValidations(Validation validation);

  /**
   * The archive's EML: validated against the schema it declares (bundled; GBIF profile 1.3 when it
   * declares none or an unknown one), then the GBIF metadata quality rules.
   */
  private static final EmlValidator EML_VALIDATOR =
      EmlValidator.builder().defaultSchema(EmlSchema.GBIF_PROFILE_1_3).qualityRules(true).build();

  /**
   * An issue of gbif-eml-validation in the shape validator-ws showed: schema problems as {@code
   * EML_GBIF_SCHEMA} with the severity and the parser's message (and position); the quality rules
   * under their own evaluation types; metadata that could not be read as {@code EML_GBIF_SCHEMA}.
   */
  static Metrics.IssueInfo toIssueInfo(EmlIssue issue) {
    switch (issue.type()) {
      case TITLE_MISSING_OR_TOO_SHORT:
      case LICENSE_MISSING_OR_UNKNOWN:
      case DESCRIPTION_MISSING_OR_TOO_SHORT:
      case RESOURCE_CONTACTS_MISSING_OR_INCOMPLETE:
        return Metrics.IssueInfo.create(EvaluationType.valueOf(issue.type().name()));
      case METADATA_UNREADABLE:
        return Metrics.IssueInfo.create(EvaluationType.EML_GBIF_SCHEMA);
      default:
        String message =
            issue.line() == null
                ? issue.message()
                : "line " + issue.line() + ", column " + issue.column() + ": " + issue.message();
        return Metrics.IssueInfo.create(
            EvaluationType.EML_GBIF_SCHEMA, issue.severity().name(), message);
    }
  }

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

    Optional<Path> emlPath = ArchiveMetadata.find(inputPath);

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
      // Schema (bundled, no network) and the GBIF quality rules: gbif-eml-validation
      List<Metrics.IssueInfo> issueInfos =
          EML_VALIDATOR.validate(emlPath.get()).issues().stream()
              .map(BaseDwcaArchiveValidator::toIssueInfo)
              .collect(Collectors.toList());

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
