package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

/**
 * interprets day and adds issues and lineages with it
 */
class DayInterpreter implements Interpretable<String> {

  private static final int MIN_DAY = 1;
  private static final int MAX_DAY = 31;

  @Override
  public Integer interpret(String input) throws InterpretationException {
    String trimmedInput = input == null ? null : input.trim();
    try {
      if (trimmedInput == null) return null;
      return dayRangeCheck(Integer.parseInt(trimmedInput));
    } catch (NumberFormatException ex1) {
      //if parse failed
      InterpretationException iex = new InterpretationException();
      iex.addIssue(IssueType.PARSE_ERROR, "Day cannot be parsed because of " + ex1.getMessage());
      iex.addLineage(LineageType.SET_TO_NULL, "Since day cannot be parsed setting it to null");
      iex.setInterpretedValue(null);
      throw iex;
    }
  }

  /**
   * check if the interpreted Day is out of range
   */
  private Integer dayRangeCheck(Integer interpretedDay) throws InterpretationException {
    if (interpretedDay < MIN_DAY || interpretedDay > MAX_DAY) {
      InterpretationException iex = new InterpretationException();
      iex.addIssue(IssueType.DAY_OUT_OF_RANGE, "Day can be between 1-31");
      iex.addLineage(LineageType.SET_TO_NULL, "Since " + IssueType.DAY_OUT_OF_RANGE.name() + " setting it to null");
      iex.setInterpretedValue(null);
      throw iex;
    }
    return interpretedDay;
  }
}
