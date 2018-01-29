package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.Year;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * year interpreter interprets the years and add issues and lineages to it.
 */
public class YearInterpreter implements Interpretable<String, Integer> {

  @Override
  public Integer interpret(String input) throws InterpretationException {
    Year x;
    List<Issue> issues = new ArrayList<>();
    List<Lineage> lineages = new ArrayList<>();

    try {
      x = Year.parse(input);
      return x.getValue();
    } catch (DateTimeParseException ex) {
      //if parse failed
      issues.add(Issue.newBuilder()
                   .setRemark("Year cannot be parsed because of " + ex.getMessage())
                   .setIssueType(IssueType.PARSE_ERROR)
                   .build());
      lineages.add(Lineage.newBuilder()
                     .setRemark("Since Year cannot be parsed setting it to null")
                     .setLineageType(LineageType.SET_TO_NULL)
                     .build());
      InterpretationException iex = new InterpretationException(issues, lineages);
      iex.setInterpretedValue(null);
      throw iex;
    }
  }
}