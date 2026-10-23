package org.gbif.pipelines.coordinator;

public record IdentifierValidationResult(
    long totalRecords,
    long absentIdentifierRecords,
    long duplicateIdentifiers,
    boolean isResultValid,
    String validationMessage) {}
