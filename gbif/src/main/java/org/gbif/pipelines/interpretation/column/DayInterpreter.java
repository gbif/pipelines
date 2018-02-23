package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.Collections;
import java.util.List;

/**
 * interprets day and adds issues and lineages with it
 */
class DayInterpreter implements Interpretable<String, Integer> {

  private static final int MIN_DAY = 1;
  private static final int MAX_DAY = 31;

  @Override
  public InterpretationResult<Integer> apply(String input) {
    String trimmedInput = input == null ? null : input.trim();
    try {
      if (trimmedInput == null) return null;
      return dayRangeCheck(Integer.parseInt(trimmedInput));
    } catch (NumberFormatException ex1) {
      //if parse failed
      List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                       .setIssueType(IssueType.PARSE_ERROR)
                                                       .setRemark("Day cannot be parsed because of " + ex1.getMessage())
                                                       .build());
      List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                           .setLineageType(LineageType.SET_TO_NULL)
                                                           .setRemark("Since day cannot be parsed setting it to null")
                                                           .build());
      return InterpretationResult.withIssueAndLineage(null, issues, lineages);
    }
  }

  /**
   * check if the interpreted Day is out of range
   */
  private InterpretationResult<Integer> dayRangeCheck(Integer interpretedDay) {
    if (interpretedDay < MIN_DAY || interpretedDay > MAX_DAY) {
      List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                       .setIssueType(IssueType.DAY_OUT_OF_RANGE)
                                                       .setRemark("Day can be between 1-31")
                                                       .build());
      List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                           .setLineageType(LineageType.SET_TO_NULL)
                                                           .setRemark("Since " + IssueType.DAY_OUT_OF_RANGE.name()
                                                                      + " setting it to null")
                                                           .build());
      return InterpretationResult.withIssueAndLineage(null, issues, lineages);
    }
    return InterpretationResult.withSuccess(interpretedDay);
  }
}
