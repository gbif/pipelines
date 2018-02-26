package org.gbif.pipelines.interpretation.column;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * interpret interface for various term based interpreter
 * throw InterpretationException in case found issues or lineages
 * T stands for Input parameter and U stands for output parameter
 */
@FunctionalInterface
interface Interpretable<T, U> extends Function<T, InterpretationResult<U>> {

  default InterpretationResult<U> withIssueAndLineage(IssueType issueType, String issueText, LineageType lineageType, String lineageText){
    Issue issue = Issue.newBuilder()
      .setIssueType(issueType)
      .setRemark(issueText)
      .build();
    List<Issue> issues = Collections.singletonList(issue);

    Lineage lineage = Lineage.newBuilder()
      .setLineageType(lineageType)
      .setRemark(lineageText)
      .build();
    List<Lineage> lineages = Collections.singletonList(lineage);

    return InterpretationResult.withIssueAndLineage(null, issues, lineages);
  }

}
