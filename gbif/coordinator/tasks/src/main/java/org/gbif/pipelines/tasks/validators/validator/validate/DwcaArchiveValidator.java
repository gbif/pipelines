package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;
import static org.gbif.validator.api.DwcFileType.CORE;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.validation.MetadataPath;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.DwcaValidator;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.rules.BasicMetadataEvaluator;
import org.gbif.utils.file.ClosableIterator;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.FileFormat;
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
  public PipelineBasedMessage createOutgoingMessage() {
    Optional<DatasetType> datasetTypeOpt = getDatasetType();

    if (datasetTypeOpt.isPresent() && datasetTypeOpt.get() == DatasetType.CHECKLIST) {
      PipelinesChecklistValidatorMessage m =
          new PipelinesChecklistValidatorMessage(
              message.getDatasetUuid(),
              message.getAttempt(),
              message.getPipelineSteps(),
              message.getExecutionId(),
              FileFormat.DWCA.name());
      return m;
    }

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

  @Override
  public void validate() {
    log.info("Running DWCA validator");
    Validation validation = validationClient.get(message.getDatasetUuid());

    // EML
    FileInfo emlFile = validateEmlFile();
    Validations.mergeFileInfo(validation, emlFile);

    // Core file (Occurrence, Event or Checklist) and, when present, the Occurrence extension
    // (e.g. attached to a Sampling Event dataset)
    DwcaCounts dwcaCounts =
        generateCounts(buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid()));
    validateDwcaFiles(dwcaCounts)
        .forEach(fileInfo -> Validations.mergeFileInfo(validation, fileInfo));
    // Add FileInfo for other extensions
    addExtensionFileInfo(validation, dwcaCounts);

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

  private void addExtensionFileInfo(Validation validation, DwcaCounts dwcaCounts) {
    try {
      log.info("Running DWCA validation for {}", message.getDatasetUuid());
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive = DwcaUtils.fromLocation(inputPath);
      archive
          .getExtensions()
          .forEach(
              ext -> {
                if (ext.getRowType() != DwcTerm.Occurrence) {
                  Long extensionCount =
                      dwcaCounts.extensionCounts.putIfAbsent(ext.getRowType().qualifiedName(), 0L);
                  FileInfo extensionFileInfo =
                      FileInfo.builder()
                          .fileType(DwcFileType.EXTENSION)
                          .fileName(ext.getFirstLocationFile().getName())
                          .rowType(ext.getRowType().qualifiedName())
                          .count(extensionCount)
                          .build();

                  validation.getMetrics().getFileInfos().add(extensionFileInfo);
                }
              });

    } catch (Exception e) {
      log.error("Unable to read archive", e);
    }
  }

  @SneakyThrows
  private FileInfo validateEmlFile() {
    log.info("Running EML schema validation for {}", message.getDatasetUuid());

    Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());

    Optional<Path> emlPath = MetadataPath.parsePath(inputPath);

    FileInfoBuilder fileInfoBuilder = FileInfo.builder().fileType(DwcFileType.METADATA);

    if (emlPath.isEmpty()) {
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
      String xmlDoc = Files.readString(emlPath.get());

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

  /**
   * Validates the DwC-A core file (Occurrence, Event or Checklist) and, when present, the
   * Occurrence extension attached to a non-Occurrence core (e.g. a Sampling Event dataset).
   */
  private List<FileInfo> validateDwcaFiles(DwcaCounts dwcaCounts) {
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

      List<FileInfo> fileInfos = new ArrayList<>();

      // Core file: Occurrence core uses the occurrence report, Event/Checklist core uses the
      // generic report
      boolean isOccurrenceCore = archive.getCore().getRowType() == DwcTerm.Occurrence;
      List<IssueInfo> coreIssues =
          isOccurrenceCore
              ? DwcaValidator.occurrenceIssues(report.getOccurrenceReport())
              : DwcaValidator.genericIssues(report.getGenericReport());
      fileInfos.add(
          FileInfo.builder()
              .rowType(archive.getCore().getRowType().qualifiedName())
              .fileType(CORE)
              .fileName(archive.getCore().getFirstLocationFile().getName())
              .count(dwcaCounts.coreCount)
              .issues(coreIssues)
              .build());

      // Occurrence extension, e.g. attached to a Sampling Event dataset
      if (!isOccurrenceCore && archive.getExtension(DwcTerm.Occurrence) != null) {
        fileInfos.add(
            FileInfo.builder()
                .rowType(DwcTerm.Occurrence.qualifiedName())
                .fileType(DwcFileType.EXTENSION)
                .fileName(archive.getExtension(DwcTerm.Occurrence).getFirstLocationFile().getName())
                .count(dwcaCounts.extensionCounts.get(DwcTerm.Occurrence.qualifiedName()))
                .issues(DwcaValidator.occurrenceIssues(report.getOccurrenceReport()))
                .build());
      }

      return fileInfos;

    } catch (UnsupportedArchiveException ex) {
      return Collections.singletonList(
          FileInfo.builder()
              .issues(
                  Collections.singletonList(
                      IssueInfo.create(
                          EvaluationType.UNHANDLED_ERROR,
                          Level.FATAL.name(),
                          ex.getLocalizedMessage())))
              .build());
    }
  }

  /**
   * Generate counts for core and extensions.
   *
   * @param path to archive to read
   * @return counts for core and extensions
   */
  private DwcaCounts generateCounts(Path path) {
    // record count
    long coreCount = 0;
    Map<String, Long> extensionCounts = new HashMap<>();
    try {
      Archive archive = DwcaUtils.fromLocation(path);
      try (ClosableIterator<org.gbif.dwc.record.StarRecord> iterator = archive.iterator()) {
        while (iterator.hasNext()) {
          coreCount++;
          StarRecord starRecord = iterator.next();
          starRecord
              .extensions()
              .forEach(
                  (term, records) -> {
                    Long count = extensionCounts.getOrDefault(term.qualifiedName(), 0L);
                    extensionCounts.put(term.qualifiedName(), count + records.size());
                  });
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
      }
    } catch (Exception e) {
      log.error("Unable to read archive", e);
    }
    return DwcaCounts.builder().coreCount(coreCount).extensionCounts(extensionCounts).build();
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
      log.error("Couldn't get dataset type for validation {}", message.getDatasetUuid(), ex);
      return Optional.empty();
    }
  }

  @Builder
  static class DwcaCounts {
    long coreCount = -1;
    Map<String, Long> extensionCounts = new HashMap<>();
  }
}
