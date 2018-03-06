package org.gbif.pipelines.labs.interpretation.lineage.quality;

import java.util.function.Predicate;

public class Assertion<T> implements Predicate<T> {

  public enum Severity {
    VALIDATION, MEASUREMENT, AMENDMENT;
  }

  private final Predicate<T> evaluation;
  private final Severity severity;
  private final String message;

  @Override
  public boolean test(T t) {
    return evaluation.test(t);
  }

  private Assertion(Predicate<T> evaluation, Severity severity, String message){
    this.evaluation = evaluation;
    this.severity = severity;
    this.message = message;
  }

  public static <U> Assertion<U> validation(Predicate<U> evaluation, String message) {
    return new Assertion<>(evaluation, Severity.VALIDATION, message);
  }

  public static <U> Assertion<U> measurement(Predicate<U> evaluation, String message) {
    return new Assertion<>(evaluation, Severity.MEASUREMENT, message);
  }

  public Severity getSeverity() {
    return severity;
  }

  public String getMessage() {
    return message;
  }
}
