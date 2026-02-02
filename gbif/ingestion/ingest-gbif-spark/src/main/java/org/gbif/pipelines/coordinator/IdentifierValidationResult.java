package org.gbif.pipelines.coordinator;

public record IdentifierValidationResult(
    double totalRecords,
    double absentIdentifierRecords,
    boolean isResultValid,
    String validationMessage) {}
