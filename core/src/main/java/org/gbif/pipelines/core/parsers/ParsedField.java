package org.gbif.pipelines.core.parsers;

import java.util.ArrayList;
import java.util.List;

/**
 * Models a parsed field.
 * <p>
 * A field can be as simple as a single {@link org.gbif.dwc.terms.DwcTerm} or a wrapper with several fields inside.
 */
public class ParsedField<T> {

  // TODO: do we need confidence??

  private T result;
  private List<InterpretationIssue> issues = new ArrayList<>();
  private boolean successful;

  private ParsedField(Builder<T> builder) {
    this.result = builder.result;
    this.issues = builder.issues;
    this.successful = builder.successful;
  }

  public static <S> ParsedField<S> fail(S result, List<InterpretationIssue> issues) {
    return ParsedField.<S>newBuilder().result(result).issues(issues).build();
  }

  public static <S> ParsedField<S> fail(InterpretationIssue issue) {
    return ParsedField.<S>newBuilder().withIssue(issue).build();
  }

  public static <S> ParsedField<S> fail(List<InterpretationIssue> issues) {
    return ParsedField.<S>newBuilder().issues(issues).build();
  }

  public static <S> ParsedField<S> fail() {
    return ParsedField.<S>newBuilder().build();
  }

  public static <S> ParsedField<S> success(S result, List<InterpretationIssue> issues) {
    return ParsedField.<S>newBuilder().successful(true).result(result).issues(issues).build();
  }

  public T getResult() {
    return result;
  }

  public List<InterpretationIssue> getIssues() {
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
    private List<InterpretationIssue> issues = new ArrayList<>();
    private boolean successful;

    public Builder<T> result(T result) {
      this.result = result;
      return this;
    }

    public Builder<T> issues(List<InterpretationIssue> issues) {
      this.issues = issues;
      return this;
    }

    public Builder<T> withIssue(InterpretationIssue issue) {
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
