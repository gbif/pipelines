package org.gbif.pipelines.interpretation.column;


import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.Year;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * year interpreter interprets the years and add issues and lineages to it.
 */
class YearInterpreter implements Interpretable<String, Integer> {

  @Override
  public InterpretationResult<Integer> apply(String input) {
    if (Objects.isNull(input)) {
      return null;
    }

    try {
      return InterpretationResult.withSuccess(Year.parse(input.trim()).getValue());
    } catch (DateTimeParseException ex) {
      String issueText = "Year cannot be parsed because of " + ex.getMessage();
      String lineageText = "Since Year cannot be parsed setting it to null";
      return withIssueAndLineage(IssueType.PARSE_ERROR, issueText, LineageType.SET_TO_NULL, lineageText);
    }
  }
}