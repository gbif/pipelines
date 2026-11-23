package org.gbif.pipelines.tasks.validators.validator.validate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Validation;

/**
 * The result of an archive check run by the flow-orchestrator: the file infos validator-ws used to
 * store, the verdict, and the outputs later steps need ({@code datasetType}, and {@code
 * clbDatasetKey} for a checklist submitted to ChecklistBank).
 *
 * <p>An archive with a FATAL issue is not valid (the legacy validator failed the validation
 * instead); the orchestrator rejects it and skips the remaining steps.
 */
public record OrchestratedArchiveValidation(List<FileInfo> files, Map<String, String> outputs) {

  public OrchestratedArchiveValidation {
    files = List.copyOf(files);
    outputs = Map.copyOf(outputs);
  }

  /** The file infos of a validation object the validators filled in. */
  public static List<FileInfo> files(Validation validation) {
    return Optional.ofNullable(validation.getMetrics())
        .map(Metrics::getFileInfos)
        .orElse(List.of());
  }

  /** The first FATAL issue, as {@code ISSUE: message}. */
  public Optional<String> firstFatal() {
    for (FileInfo file : files) {
      for (IssueInfo issue : file.getIssues()) {
        Optional<String> message =
            issue.getSamples().stream()
                .map(sample -> sample.getRelatedData())
                .filter(Objects::nonNull)
                .filter(data -> data.containsKey(Level.FATAL.name()))
                .map(data -> data.get(Level.FATAL.name()))
                .findFirst();
        if (message.isPresent()) {
          return Optional.of(
              issue.getIssue() + (message.get() == null ? "" : ": " + message.get()));
        }
      }
    }
    return Optional.empty();
  }

  public boolean valid() {
    return firstFatal().isEmpty();
  }

  /**
   * The {@code ARCHIVE_VALIDATION} report: {@code {"valid", "invalidationReason", "files"}}, files
   * in the shape of validator-ws's {@code metrics.files}.
   */
  public Map<String, Object> report() {
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("valid", valid());
    report.put("invalidationReason", firstFatal().orElse(null));
    report.put("files", files);
    return report;
  }
}
