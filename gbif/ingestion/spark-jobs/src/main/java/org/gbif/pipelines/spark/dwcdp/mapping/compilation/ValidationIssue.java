package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

public record ValidationIssue(Severity severity, String message) {
  public enum Severity {
    ERROR
  }
}
