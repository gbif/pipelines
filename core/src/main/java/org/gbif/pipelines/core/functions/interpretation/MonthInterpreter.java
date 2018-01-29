package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.Month;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * interprets month and add issue and lineages to it.
 */
public class MonthInterpreter implements Interpretable<String, Integer> {

  @Override
  public Integer interpret(String input) throws InterpretationException {

    List<Issue> issues = new ArrayList<>();
    List<Lineage> lineages = new ArrayList<>();

    Month m;
    try {
      m = Month.of(Integer.parseInt(input.trim()));
      return m.getValue();
    } catch (IllegalArgumentException | DateTimeParseException ex1) {
      //if parse failed
      issues.add(Issue.newBuilder()
                   .setRemark("Month cannot be parsed because of " + ex1.getMessage())
                   .setIssueType(IssueType.PARSE_ERROR)
                   .build());
      lineages.add(Lineage.newBuilder()
                     .setRemark("Since Month cannot be parsed setting it to null")
                     .setLineageType(LineageType.SET_TO_NULL)
                     .build());
    }
    InterpretationException iex = new InterpretationException(issues, lineages);
    iex.setInterpretedValue(null);
    throw iex;

  }
}
