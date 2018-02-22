package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.Year;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;

/**
 * year interpreter interprets the years and add issues and lineages to it.
 */
class YearInterpreter implements Interpretable<String, Integer> {

  @Override
  public InterpretationResult<Integer> apply(String input) {
    String trimmedInput = input == null ? null : input.trim();

    try {
      if (trimmedInput == null) return null;
      return InterpretationResult.withSuccess(Year.parse(trimmedInput).getValue());
    } catch (DateTimeParseException ex) {
      //if parse failed
      List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                       .setRemark("Year cannot be parsed because of " + ex.getMessage())
                                                       .setIssueType(IssueType.PARSE_ERROR)
                                                       .build());
      List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                           .setRemark("Since Year cannot be parsed setting it to null")
                                                           .setLineageType(LineageType.SET_TO_NULL)
                                                           .build());
      return InterpretationResult.withIssueAndLineage(null, issues, lineages);
    }
  }
}
