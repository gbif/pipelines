package org.gbif.pipelines.core.utils;

import static org.gbif.pipelines.core.utils.IdentificationUtils.extractFromIdentificationExtension;

import java.util.*;
import java.util.stream.Collectors;
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
    if (er == null || term == null) {
      return null;
    }
    String value = er.getCoreTerms().get(term.qualifiedName());
    return value != null ? value.trim() : extractFromIdentificationExtension(er, term);
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

  public static Optional<String> extractLengthAwareOptValue(ExtendedRecord er, Term term) {
    String value = extractNullAwareValue(er, term);
    // Indexing limit length
    value = value != null && value.getBytes().length >= 32000 ? null : value;
    return Optional.ofNullable(value);
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

  public static List<String> extractListValue(ExtendedRecord er, Term term) {
    return extractListValue(DEFAULT_SEPARATOR, er, term);
  }

  public static List<String> extractListValue(String separatorRegex, ExtendedRecord er, Term term) {
    return extractListValue(extractValue(er, term), separatorRegex);
  }

  public static List<String> extractListValue(String rawValue) {
    return extractListValue(rawValue, DEFAULT_SEPARATOR);
  }

  public static List<String> extractListValue(String rawValue, String separatorRegex) {
    return Optional.ofNullable(rawValue)
        .filter(x -> !x.isEmpty())
        .map(
            x ->
                Arrays.stream(x.split(separatorRegex))
                    .map(String::trim)
                    .filter(v -> !v.isEmpty())
                    .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
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
    if (model.getIssues() == null) {
      model.setIssues(org.gbif.pipelines.io.avro.IssueRecord.newBuilder().build());
    }
    if (model.getIssues().getIssueList() == null) {
      model.getIssues().setIssueList(new ArrayList<>());
    }
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
