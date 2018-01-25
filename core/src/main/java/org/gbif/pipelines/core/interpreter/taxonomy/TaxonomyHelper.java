package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.RankedNameResponse;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;

import java.util.ArrayList;
import java.util.List;

final class TaxonomyHelper {

  private TaxonomyHelper() {
    // Do not instantiate
  }

  static boolean isSuccesfulMatch(SpeciesMatch2ResponseModel responseModel) {
    return !NameUsageMatch.MatchType.NONE.equals(NameUsageMatch.MatchType.valueOf(responseModel.getDiagnostics()
                                                                                    .getMatchType()));
  }

  static List<OccurrenceIssue> checkIssues(SpeciesMatch2ResponseModel responseModel) {

    List<OccurrenceIssue> issues = new ArrayList<>();

    switch (NameUsageMatch.MatchType.valueOf(responseModel.getDiagnostics().getMatchType())) {
      case NONE:
        issues.add(OccurrenceIssue.TAXON_MATCH_NONE);
        break;
      case FUZZY:
        issues.add(OccurrenceIssue.TAXON_MATCH_FUZZY);
        break;
      case HIGHERRANK:
        issues.add(OccurrenceIssue.TAXON_MATCH_HIGHERRANK);
        break;
    }

    return issues;
  }

  static RankedNameResponse getUsage(SpeciesMatch2ResponseModel responseModel) {
    return responseModel.getAcceptedUsage() == null ? responseModel.getUsage() : responseModel.getAcceptedUsage();
  }

}
