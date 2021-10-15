package org.gbif.pipelines.validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Validations {

  /** Merge the validation response received from API and collected ES metrics */
  public static void mergeWithValidation(Validation validation, Metrics metrics) {
    if (validation != null && metrics != null) {
      Metrics validationMetrics = validation.getMetrics();
      if (validationMetrics == null) {
        validation.setMetrics(metrics);
      } else {
        metrics.getFileInfos().forEach(fi -> Validations.mergeFileInfo(validation, fi));
      }
    }
  }

  public static void mergeFileInfo(Validation validation, FileInfo fileInfo) {
    if (validation != null && fileInfo != null) {
      if (validation.getMetrics() == null) {
        validation.setMetrics(
            Metrics.builder().fileInfos(Collections.singletonList(fileInfo)).build());
      } else if (fileInfo.getFileName() == null) {
        addFileInfo(validation, validation.getMetrics().getFileInfos(), fileInfo);
      } else {
        Optional<FileInfo> match =
            validation.getMetrics().getFileInfos().stream()
                .filter(fi -> fi.getFileName().equals(fileInfo.getFileName()))
                .findAny();

        List<FileInfo> all =
            validation.getMetrics().getFileInfos().stream()
                .filter(fi -> !fi.getFileName().equals(fileInfo.getFileName()))
                .collect(Collectors.toList());

        FileInfo merged;
        if (match.isPresent()) {
          merged = mergeNonNullFields(match.get(), fileInfo);
        } else {
          merged = fileInfo;
        }

        addFileInfo(validation, all, merged);
      }
    }
  }

  private static void addFileInfo(Validation validation, List<FileInfo> all, FileInfo newFileInfo) {
    List<FileInfo> result = new ArrayList<>(all.size() + 1);
    result.addAll(all);
    result.add(newFileInfo);

    validation.getMetrics().setFileInfos(result);
  }

  private static FileInfo mergeNonNullFields(FileInfo f1, FileInfo f2) {
    String fileName = Optional.ofNullable(f1.getFileName()).orElse(f2.getFileName());
    DwcFileType fileType = Optional.ofNullable(f1.getFileType()).orElse(f2.getFileType());
    Long count = Optional.ofNullable(f1.getCount()).orElse(f2.getCount());
    Long indexedCount = Optional.ofNullable(f1.getIndexedCount()).orElse(f2.getIndexedCount());
    String rowType = Optional.ofNullable(f1.getRowType()).orElse(f2.getRowType());

    List<TermInfo> termInfos = new ArrayList<>(f1.getTerms().size() + f2.getTerms().size());
    termInfos.addAll(f1.getTerms());
    termInfos.addAll(f2.getTerms());

    List<IssueInfo> issueInfos = new ArrayList<>(f1.getTerms().size() + f2.getTerms().size());
    issueInfos.addAll(f1.getIssues());
    issueInfos.addAll(f2.getIssues());

    return FileInfo.builder()
        .fileName(fileName)
        .fileType(fileType)
        .count(count)
        .indexedCount(indexedCount)
        .rowType(rowType)
        .terms(termInfos)
        .issues(issueInfos)
        .build();
  }
}
