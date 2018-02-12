package org.gbif.pipelines.ws.match2;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.interpretation.taxonomy.TaxonomyInterpretationException;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

/**
 * Handles the calls to the species match WS.
 */
public class SpeciesMatchManager {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchManager.class);

  private SpeciesMatchManager() {}

  /**
   * Matches a {@link ExtendedRecord} to an existing specie using the species match 2 WS.
   *
   * @param extendedRecord avro file with the taxonomic data
   *
   * @return {@link NameUsageMatch2} with the match received from the WS.
   *
   * @throws TaxonomyInterpretationException in case of errors
   */
  public static NameUsageMatch2 getMatch(ExtendedRecord extendedRecord) throws TaxonomyInterpretationException {
    NameUsageMatch2 nameUsageMatch = callSpeciesMatchWs(extendedRecord.getCoreTerms());

    if (!isSuccessfulMatch(nameUsageMatch) && hasIdentifications(extendedRecord)) {
      LOG.info("Retrying match with identification extension");
      // get identifications
      List<Map<CharSequence, CharSequence>> identifications =
        extendedRecord.getExtensions().get(DwcTerm.Identification.qualifiedName());

      // FIXME: use new generic functions to parse the date??
      // sort them by date identified
      // Ask Markus D if this can be moved to the API?
      identifications.sort(Comparator.comparing((Map<CharSequence, CharSequence> map) -> LocalDateTime.parse(map.get(
        DwcTerm.dateIdentified))).reversed());
      for (Map<CharSequence, CharSequence> record : identifications) {
        nameUsageMatch = callSpeciesMatchWs(record);
        if (isSuccessfulMatch(nameUsageMatch)) {
          LOG.info("match with identificationId {} succeed", record.get(DwcTerm.identificationID.name()));
          return nameUsageMatch;
        }
      }

    }

    return nameUsageMatch;
  }

  private static NameUsageMatch2 callSpeciesMatchWs(Map<CharSequence, CharSequence> terms)
    throws TaxonomyInterpretationException {
    TaxonomyFieldsWorkingCopy workingCopy = new TaxonomyFieldsWorkingCopy(terms);

    SpeciesMatch2Service service = SpeciesMatch2ServiceRest.SINGLE.getService();

    Call<NameUsageMatch2> call = service.match2(workingCopy.kingdom,
                                                workingCopy.phylum,
                                                workingCopy.clazz,
                                                workingCopy.order,
                                                workingCopy.family,
                                                workingCopy.genus,
                                                workingCopy.rank != null ? workingCopy.rank.name() : null,
                                                workingCopy.scientificName,
                                                false,
                                                false);

    NameUsageMatch2 responseModel = null;

    try {
      Response<NameUsageMatch2> response = call.execute();

      responseModel = response.body();
    } catch (IOException e) {
      throw new TaxonomyInterpretationException("Error calling the match2 species name WS", e);
    }

    // checking for unexpected response
    if (isEmptyResponse(responseModel)) {
      throw new TaxonomyInterpretationException("Empty response from match2 species name WS");
    }

    return responseModel;
  }

  private static boolean isEmptyResponse(NameUsageMatch2 response) {
    return response == null || (response.getUsage() == null
                                && response.getClassification() == null
                                && response.getDiagnostics() == null);
  }

  private static boolean isSuccessfulMatch(NameUsageMatch2 responseModel) {
    return !NameUsageMatch.MatchType.NONE.equals(responseModel.getDiagnostics().getMatchType());
  }

  private static boolean hasIdentifications(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions().containsKey(DwcTerm.Identification.qualifiedName());
  }

  /**
   * Represents a copy with clean and processed terms.
   */
  private static class TaxonomyFieldsWorkingCopy {

    private static final RankParser RANK_PARSER = RankParser.getInstance();

    String kingdom;
    String phylum;
    String clazz;
    String order;
    String family;
    String genus;
    String scientificName;
    String authorship;
    String genericName;
    String specificEpithet;
    String infraspecificEpithet;
    Rank rank;

    TaxonomyFieldsWorkingCopy(Map<CharSequence, CharSequence> terms) {
      cleanAndProcessFields(terms);
    }

    void cleanAndProcessFields(Map<CharSequence, CharSequence> terms) {
      kingdom = ClassificationUtils.clean(value(terms, DwcTerm.kingdom));
      phylum = ClassificationUtils.clean(value(terms, DwcTerm.phylum));
      clazz = ClassificationUtils.clean(value(terms, DwcTerm.class_));
      order = ClassificationUtils.clean(value(terms, DwcTerm.order));
      family = ClassificationUtils.clean(value(terms, DwcTerm.family));
      genus = ClassificationUtils.clean(value(terms, DwcTerm.genus));
      authorship = ClassificationUtils.cleanAuthor(value(terms, DwcTerm.scientificNameAuthorship));
      genericName = ClassificationUtils.clean(value(terms, GbifTerm.genericName));
      specificEpithet = ClassificationUtils.cleanAuthor(value(terms, DwcTerm.specificEpithet));
      infraspecificEpithet = ClassificationUtils.cleanAuthor(value(terms, DwcTerm.infraspecificEpithet));
      rank = interpretRank(terms);
      scientificName = buildScientificName(value(terms, DwcTerm.scientificName),
                                           authorship,
                                           genericName,
                                           genus,
                                           specificEpithet,
                                           infraspecificEpithet);
    }

    Rank interpretRank(Map<CharSequence, CharSequence> terms) {
      Rank rankFound = null;

      if (hasTerm(terms, DwcTerm.taxonRank)) {
        rankFound = RANK_PARSER.parse(value(terms, DwcTerm.taxonRank)).getPayload();
      }
      // try again with verbatim if it exists
      if (rankFound == null && hasTerm(terms, DwcTerm.verbatimTaxonRank)) {
        rankFound = RANK_PARSER.parse(value(terms, DwcTerm.verbatimTaxonRank)).getPayload();
      }
      // derive from atomized fields
      if (rankFound == null && hasTerm(terms, DwcTerm.genus)) {
        if (hasTerm(terms, DwcTerm.specificEpithet)) {
          if (hasTerm(terms, DwcTerm.infraspecificEpithet)) {
            rankFound = Rank.INFRASPECIFIC_NAME;
          } else {
            rankFound = Rank.SPECIES;
          }
        } else {
          rankFound = Rank.GENUS;
        }
      }
      return rankFound;
    }

    /**
     * Assembles the most complete scientific name based on full and individual name parts.
     */
    String buildScientificName(
      String scientificName,
      String authorship,
      String genericName,
      String genus,
      String specificEpithet,
      String infraspecificEpithet
    ) {
      String sciname = ClassificationUtils.clean(scientificName);
      if (sciname == null) {
        // handle case when the scientific name is null and only given as atomized fields: genus & speciesEpitheton
        ParsedName pn = new ParsedName();
        if (genericName != null && !genericName.isEmpty()) {
          pn.setGenusOrAbove(genericName);
        } else {
          pn.setGenusOrAbove(genus);
        }
        pn.setSpecificEpithet(specificEpithet);
        pn.setInfraSpecificEpithet(infraspecificEpithet);
        pn.setAuthorship(authorship);
        sciname = pn.canonicalNameComplete();

      } else if (authorship != null && !authorship.isEmpty() && !sciname.toLowerCase()
        .contains(authorship.toLowerCase())) {
        sciname = sciname + " " + authorship;
      }

      return sciname;
    }

    private static boolean hasTerm(Map<CharSequence, CharSequence> terms, Term term) {
      return Optional.ofNullable(value(terms, term)).filter(value -> !value.isEmpty()).isPresent();
    }

    private static String value(Map<CharSequence, CharSequence> terms, Term term) {
      CharSequence val = terms.get(term.qualifiedName());
      return val == null ? null : val.toString();
    }

  }

}
