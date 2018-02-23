package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.time.DateTimeException;
import java.time.Month;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * interprets month and add issue and lineages to it.
 */
class MonthInterpreter implements Interpretable<String, Integer> {

  @Override
  public InterpretationResult<Integer> apply(String input) {
    try {
      return Optional.ofNullable(input).map(String::trim)
        .map(trimmedInput -> InterpretationResult.withSuccess(Month.of(Integer.parseInt(trimmedInput)).getValue()))
        .orElse(null);
    } catch (IllegalArgumentException | DateTimeException ex) {
      //if parse failed
      List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                       .setRemark("Month cannot be parsed because of " + ex.getMessage())
                                                       .setIssueType(IssueType.PARSE_ERROR)
                                                       .build());
      List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                           .setRemark("Since Month cannot be parsed setting it to null")
                                                           .setLineageType(LineageType.SET_TO_NULL)
                                                           .build());
      return InterpretationResult.withIssueAndLineage(null, issues, lineages);
    }

  }
}
