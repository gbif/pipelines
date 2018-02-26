package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.Objects;

/**
 * interprets day and adds issues and lineages with it
 */
class DayInterpreter implements Interpretable<String, Integer> {

  private static final int MIN_DAY = 1;
  private static final int MAX_DAY = 31;

  @Override
  public InterpretationResult<Integer> apply(String input) {
    if (Objects.isNull(input)) {
      return null;
    }

    try {
      return dayRangeCheck(Integer.parseInt(input.trim()));
    } catch (NumberFormatException ex1) {
      //if parse failed
      String issueText = "Day cannot be parsed because of " + ex1.getMessage();
      String lineageText = "Since day cannot be parsed setting it to null";
      return withIssueAndLineage(IssueType.PARSE_ERROR, issueText, LineageType.SET_TO_NULL, lineageText);
    }
  }

  /**
   * check if the interpreted Day is out of range
   */
  private InterpretationResult<Integer> dayRangeCheck(Integer interpretedDay) {
    if (MIN_DAY > interpretedDay || MAX_DAY < interpretedDay) {
      String issueText = "Day can be between 1-31";
      String lineageText = "Since " + IssueType.DAY_OUT_OF_RANGE.name() + " setting it to null";
      return withIssueAndLineage(IssueType.DAY_OUT_OF_RANGE, issueText, LineageType.SET_TO_NULL, lineageText);
    }
    return InterpretationResult.withSuccess(interpretedDay);
  }
}
