package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.RankedNameResponse;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper for the taxonomic interpretation.
 */
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

  static void setRankData(TaxonRecord taxonRecord, Rank rank, RankedNameResponse response) {
    if (response != null) {
      switch (rank) {
        case KINGDOM:
          taxonRecord.setKingdom(response.getName());
          taxonRecord.setKingdomKey(response.getKey());
          break;
        case PHYLUM:
          taxonRecord.setPhylum(response.getName());
          taxonRecord.setPhylumKey(response.getKey());
          break;
        case CLASS:
          taxonRecord.setClass$(response.getName());
          taxonRecord.setClassKey(response.getKey());
          break;
        case ORDER:
          taxonRecord.setOrder(response.getName());
          taxonRecord.setOrderKey(response.getKey());
          break;
        case FAMILY:
          taxonRecord.setFamily(response.getName());
          taxonRecord.setFamilyKey(response.getKey());
          break;
        case GENUS:
          taxonRecord.setGenus(response.getName());
          taxonRecord.setGenusKey(response.getKey());
          break;
        case SUBGENUS:
          taxonRecord.setSubgenus(response.getName());
          taxonRecord.setSubgenusKey(response.getKey());
          break;
        case SPECIES:
          taxonRecord.setSpecies(response.getName());
          taxonRecord.setSpeciesKey(response.getKey());
          break;
      }
    }
  }

}
