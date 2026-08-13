package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;

public record ValidationResult(List<ValidationIssue> issues) {
  public ValidationResult {
    issues = List.copyOf(issues);
  }

  public boolean isValid() {
    return issues.stream().noneMatch(i -> i.severity() == ValidationIssue.Severity.ERROR);
  }
}
