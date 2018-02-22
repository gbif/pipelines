package org.gbif.pipelines.interpretation.taxonomy;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.MatchType;

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
  public static void checkMatchIssue(NameUsageMatch.MatchType matchType, Interpretation interpretation) {
    if (MatchType.NONE.equals(matchType)) {
      interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.TAXON_MATCH_NONE));
    } else if (MatchType.FUZZY.equals(matchType)) {
      interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.TAXON_MATCH_FUZZY));
    } else if (MatchType.HIGHERRANK.equals(matchType)) {
      interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.TAXON_MATCH_HIGHERRANK));
    }
  }

}
