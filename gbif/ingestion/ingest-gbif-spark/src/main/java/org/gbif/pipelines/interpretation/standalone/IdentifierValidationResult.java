package org.gbif.pipelines.interpretation.standalone;

public record IdentifierValidationResult(
    double totalRecords,
    double absentIdentifierRecords,
    boolean isResultValid,
    String validationMessage) {}
