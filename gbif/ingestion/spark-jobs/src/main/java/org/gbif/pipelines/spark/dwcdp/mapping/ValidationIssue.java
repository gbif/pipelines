package org.gbif.pipelines.spark.dwcdp.mapping;

public record ValidationIssue(Severity severity, String message) {
  public enum Severity {
    ERROR,
    WARNING
  }
}
