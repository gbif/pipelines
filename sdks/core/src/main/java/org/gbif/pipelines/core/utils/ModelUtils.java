package org.gbif.pipelines.core.utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;

/** Helps to work with org.gbif.pipelines.io.avro models */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelUtils {

  public static String extractValue(ExtendedRecord er, Term term) {
    return er.getCoreTerms().get(term.qualifiedName());
  }

  /**
   * Extracts a Term value, if such value has a variation of the word "null" it is transformed to
   * null.
   */
  public static String extractNullAwareValue(ExtendedRecord er, Term term) {
    String value = extractValue(er, term);
    return value != null && ("null".equalsIgnoreCase(value.trim()) || value.isEmpty())
        ? null
        : value;
  }

  public static Optional<String> extractOptValue(ExtendedRecord er, Term term) {
    return Optional.ofNullable(extractValue(er, term));
  }

  public static boolean hasValue(ExtendedRecord er, Term term) {
    return extractOptValue(er, term).isPresent();
  }

  /** Checks if a {@link ExtendedRecord} is null or empty. */
  public static void checkNullOrEmpty(ExtendedRecord extendedRecord) {
    Objects.requireNonNull(extendedRecord, "ExtendedRecord cannot be null");

    if (extendedRecord.getId() == null || extendedRecord.getId().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with id is required");
    }

    if (extendedRecord.getCoreTerms() == null || extendedRecord.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with core terms is required");
    }
  }

  public static void addIssue(Issues model, String issue) {
    if (!model.getIssues().getIssueList().contains(issue)) {
      model.getIssues().getIssueList().add(issue);
    }
  }

  public static void addIssue(Issues model, OccurrenceIssue issue) {
    addIssue(model, issue.name());
  }

  public static void addIssue(Issues model, List<String> issues) {
    issues.forEach(x -> addIssue(model, x));
  }

  public static void addIssue(Issues model, Set<String> issues) {
    issues.forEach(x -> addIssue(model, x));
  }

  public static void addIssueSet(Issues model, Set<OccurrenceIssue> issues) {
    issues.forEach(x -> addIssue(model, x));
  }
}
