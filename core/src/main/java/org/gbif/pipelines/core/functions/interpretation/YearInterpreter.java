package org.gbif.pipelines.core.functions.interpretation;

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
public class YearInterpreter implements Interpretable<String, Integer> {

  @Override
  public Integer interpret(String input) throws InterpretationException {
    String trimmedInput = input == null ? null : input.trim();

    try {
      if (trimmedInput == null) return null;
      return Year.parse(trimmedInput).getValue();
    } catch (DateTimeParseException ex) {
      //if parse failed
      final List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                             .setRemark("Year cannot be parsed because of "
                                                                        + ex.getMessage())
                                                             .setIssueType(IssueType.PARSE_ERROR)
                                                             .build());
      final List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                                 .setRemark(
                                                                   "Since Year cannot be parsed setting it to null")
                                                                 .setLineageType(LineageType.SET_TO_NULL)
                                                                 .build());
      throw new InterpretationException(issues, lineages, null);
    }
  }
}