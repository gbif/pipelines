package org.gbif.pipelines.interpretation.utils;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.Interpretation.Trace;

import java.util.Collections;
import java.util.function.Predicate;

/**
 * Utility class for the taxonomic interpretation.
 */
public final class TaxonomyUtils {

  private TaxonomyUtils() {}

  /**
   * Creates a predicate to test if a {@link NameUsageMatch2} is empty.
   *
   * @return {@link Predicate}
   */
  public static Predicate<NameUsageMatch2> emptyNameUsageMatchResponse() {
    return (NameUsageMatch2 response) -> response == null
                                         || response.getUsage() == null
                                         || response.getClassification() == null
                                         || response.getDiagnostics() == null;
  }

  /**
   * Looks for issues according to the match type and adds a validation in the interpretation in case that issues
   * were found.
   */
  public static void checkMatchIssue(MatchType matchType, Interpretation interpretation) {
    // TODO: fieldName shouldn't be required in Trace. Remove nulls when Interpretation is fixed.
    if (MatchType.NONE.equals(matchType)) {
      interpretation.withValidation(Collections.singletonList(Trace.of(null, IssueType.TAXON_MATCH_NONE)));
    } else if (MatchType.FUZZY.equals(matchType)) {
      interpretation.withValidation(Collections.singletonList(Trace.of(null, IssueType.TAXON_MATCH_FUZZY)));
    } else if (MatchType.HIGHERRANK.equals(matchType)) {
      interpretation.withValidation(Collections.singletonList(Trace.of(null, IssueType.TAXON_MATCH_HIGHERRANK)));
    }
  }

}
