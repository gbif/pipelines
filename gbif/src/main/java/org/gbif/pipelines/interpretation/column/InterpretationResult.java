package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The wrapper class encapsulating interpreted value ith issues and lineages
 */
public class InterpretationResult<T> {

  private final T result;
  private final boolean isSuccessFull;
  private final List<Issue> issueList;
  private final List<Lineage> lineageList;

  /**
   * use this method to get InterpretationResult when the interpretation was successful
   */
  public static <U> InterpretationResult<U> withSuccess(U result) {
    return new InterpretationResult<>(result, true, new ArrayList<>(), new ArrayList<>());
  }

  /**
   * use this method to get InterpretationResult when the interpretation was failed or interpreted with issues and lineages
   */
  public static <U> InterpretationResult<U> withIssueAndLineage(U interpretedResult, List<Issue> issues,
                                                                List<Lineage> lineages) {
    return new InterpretationResult<>(interpretedResult, false, issues, lineages);
  }

  private InterpretationResult(T result, boolean isSuccessFull, List<Issue> issueList,
                               List<Lineage> lineageList) {
    this.result = result;
    this.isSuccessFull = isSuccessFull;
    this.issueList = issueList;
    this.lineageList = lineageList;
  }

  /**
   * returns interpreted result
   */
  public Optional<T> getResult() {
    return Optional.ofNullable(result);
  }

  /**
   * interpretation was successful true when done without issues and lineages
   */
  public boolean isSuccessFull() {
    return isSuccessFull;
  }

  /**
   * Helps embedding custom code with appropriate actions when the result was successful or failed.
   */
  public void ifSuccessfulThenElse(Consumer<InterpretationResult<T>> successfulConsumer,
                                   Consumer<InterpretationResult<T>> failedConsumer) {
    if (isSuccessFull) {
      successfulConsumer.accept(this);
    } else {
      failedConsumer.accept(this);
    }
  }

  /**
   * get list of issues
   */
  public List<Issue> getIssueList() {
    return issueList;
  }

  /**
   * get List of lineages
   */
  public List<Lineage> getLineageList() {
    return lineageList;
  }

  @Override
  public String toString() {
    return "InterpretationResult{"
           + "result="
           + result
           + ", isSuccessFull="
           + isSuccessFull
           + ", issueList="
           + issueList
           + ", lineageList="
           + lineageList
           + '}';
  }
}
