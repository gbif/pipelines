package org.gbif.pipelines.core.utils;

import static org.gbif.pipelines.core.utils.IdentificationUtils.extractFromIdentificationExtension;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;

/** Helps to work with org.gbif.pipelines.io.avro models */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ModelUtils {

  public static final String DEFAULT_SEPARATOR = "\\|";

  public static String extractValue(ExtendedRecord er, Term term) {
    String value = er.getCoreTerms().get(term.qualifiedName());
    return value != null ? value : extractFromIdentificationExtension(er, term);
  }

  public static String extractValue(Map<String, String> termsSource, Term term) {
    return termsSource.get(term.qualifiedName());
  }

  /**
   * Extracts a Term value, if such value has a variation of the word "null" it is transformed to
   * null.
   */
  public static String extractNullAwareValue(ExtendedRecord er, Term term) {
    String value = extractValue(er, term);
    if (hasValue(value)) {
      return value;
    } else {
      String valueFromIdentificationExtension = extractFromIdentificationExtension(er, term);
      return hasValue(valueFromIdentificationExtension) ? valueFromIdentificationExtension : null;
    }
  }

  public static String extractNullAwareValue(Map<String, String> termsSource, Term term) {
    String value = extractValue(termsSource, term);
    return hasValue(value) ? value : null;
  }

  public static boolean hasValue(String value) {
    return value != null && !value.isEmpty() && !"null".equalsIgnoreCase(value.trim());
  }

  public static Optional<String> extractNullAwareOptValue(ExtendedRecord er, Term term) {
    return Optional.ofNullable(extractNullAwareValue(er, term));
  }

  public static Optional<String> extractNullAwareOptValue(
      Map<String, String> termsSource, Term term) {
    return Optional.ofNullable(extractNullAwareValue(termsSource, term));
  }

  public static Optional<String> extractOptValue(ExtendedRecord er, Term term) {
    return Optional.ofNullable(extractValue(er, term));
  }

  public static Optional<List<String>> extractOptListValue(ExtendedRecord er, Term term) {
    return extractOptValue(er, term)
        .filter(x -> !x.isEmpty())
        .map(
            x ->
                Stream.of(x.split(DEFAULT_SEPARATOR))
                    .map(String::trim)
                    .filter(v -> !v.isEmpty())
                    .collect(Collectors.toList()));
  }

  public static boolean hasValue(ExtendedRecord er, Term term) {
    return extractOptValue(er, term).isPresent();
  }

  public static boolean hasValueNullAware(ExtendedRecord er, Term term) {
    return extractNullAwareOptValue(er, term).isPresent();
  }

  public static boolean hasExtension(ExtendedRecord er, Extension extension) {
    return hasExtension(er, extension.getRowType());
  }

  public static boolean hasExtension(ExtendedRecord er, String extension) {
    return Optional.ofNullable(er.getExtensions().get(extension))
        .filter(l -> !l.isEmpty())
        .isPresent();
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
