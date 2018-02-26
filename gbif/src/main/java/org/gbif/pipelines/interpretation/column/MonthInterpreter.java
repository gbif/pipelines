package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.DateTimeException;
import java.time.Month;

/**
 * interprets month and add issue and lineages to it.
 */
class MonthInterpreter implements Interpretable<String, Integer> {

  @Override
  public InterpretationResult<Integer> apply(String input) {
    String trimmedInput = input == null ? null : input.trim();
    try {
      if (trimmedInput == null) return null;
      return InterpretationResult.withSuccess(Month.of(Integer.parseInt(trimmedInput)).getValue());
    } catch (IllegalArgumentException | DateTimeException ex) {
      String issueText = "Month cannot be parsed because of " + ex.getMessage();
      String lineageText = "Since Month cannot be parsed setting it to null";
      return withIssueAndLineage(IssueType.PARSE_ERROR, issueText, LineageType.SET_TO_NULL, lineageText);
    }

  }
}
