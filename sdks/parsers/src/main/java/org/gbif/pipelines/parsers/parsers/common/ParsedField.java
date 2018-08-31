package org.gbif.pipelines.parsers.parsers.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Models a parsed field.
 *
 * <p>A field can be as simple as a single {@link org.gbif.dwc.terms.DwcTerm} or a wrapper with
 * several fields inside.
 */
public class ParsedField<T> {

  private final T result;
  private final List<String> issues;
  private final boolean successful;

  private ParsedField(Builder<T> builder) {
    result = builder.result;
    issues = builder.issues;
    successful = builder.successful;
  }

  public static <S> ParsedField<S> fail(S result, List<String> issues) {
    return ParsedField.<S>newBuilder().result(result).issues(issues).build();
  }

  public static <S> ParsedField<S> fail(String issue) {
    return ParsedField.<S>newBuilder().withIssue(issue).build();
  }

  public static <S> ParsedField<S> fail(List<String> issues) {
    return ParsedField.<S>newBuilder().issues(issues).build();
  }

  public static <S> ParsedField<S> fail() {
    return ParsedField.<S>newBuilder().build();
  }

  public static <S> ParsedField<S> success(S result, List<String> issues) {
    return ParsedField.<S>newBuilder().successful(true).result(result).issues(issues).build();
  }

  public static <S> ParsedField<S> success(S result) {
    return ParsedField.<S>newBuilder().successful(true).result(result).build();
  }

  public T getResult() {
    return result;
  }

  public List<String> getIssues() {
    return issues;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public static <T> Builder<T> newBuilder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    private T result;
    private List<String> issues = new ArrayList<>();
    private boolean successful;

    public Builder<T> result(T result) {
      this.result = result;
      return this;
    }

    public Builder<T> issues(List<String> issues) {
      this.issues = issues;
      return this;
    }

    public Builder<T> withIssue(String issue) {
      issues.add(issue);
      return this;
    }

    public Builder<T> successful(boolean successful) {
      this.successful = successful;
      return this;
    }

    public ParsedField<T> build() {
      return new ParsedField<>(this);
    }
  }
}
