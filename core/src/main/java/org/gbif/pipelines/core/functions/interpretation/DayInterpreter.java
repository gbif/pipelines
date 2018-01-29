package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.ArrayList;
import java.util.List;

/**
 * interprets day and adds issues and lineages with it
 */
public class DayInterpreter implements Interpretable<String, Integer> {

  private static final int min_day = 1;
  private static final int max_day = 31;

  @Override
  public Integer interpret(String input) throws InterpretationException {
    Integer x;
    List<Issue> issues = new ArrayList<>();
    List<Lineage> lineages = new ArrayList<>();

    try {
      x = Integer.parseInt(input);
    } catch (NumberFormatException ex) {
      //if parse failed
      issues.add(Issue.newBuilder()
                   .setRemark("Day cannot be parsed because of " + ex.getMessage())
                   .setIssueType(IssueType.OTHERS)
                   .build());
      lineages.add(Lineage.newBuilder()
                     .setRemark("Since day cannot be parsed setting it to null")
                     .setLineageType(LineageType.SET_TO_NULL)
                     .build());
      InterpretationException iex = new InterpretationException(issues, lineages);
      iex.setInterpretedValue(null);
      throw iex;
    }
    //if day out of range
    if (x < min_day || x > max_day) {
      issues.add(Issue.newBuilder()
                   .setRemark("Day can be between 1-31")
                   .setIssueType(IssueType.DAY_OUT_OF_RANGE)
                   .build());
      lineages.add(Lineage.newBuilder()
                     .setRemark("Since " + IssueType.DAY_OUT_OF_RANGE.name() + " setting it to null")
                     .setLineageType(LineageType.SET_TO_NULL)
                     .build());
      InterpretationException iex = new InterpretationException(issues, lineages);
      iex.setInterpretedValue(null);
      throw iex;
    }
    return x;

  }
}
