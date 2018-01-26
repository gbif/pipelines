package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.RankParser;
import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.SpeciesMatch2Service;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.SpeciesMatch2ServiceRest;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.isSuccesfulMatch;

/**
 * Handles the calls to the species match WS.
 */
public class SpeciesMatchManager {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchManager.class);

  /**
   * Matches a {@link ExtendedRecord} to an existing specie using the species match 2 WS.
   *
   * @param extendedRecord avro file with the taxonomic data
   *
   * @return {@link SpeciesMatch2ResponseModel} with the match received from the WS.
   *
   * @throws TaxonomyInterpretationException in case of errors
   */
  public static SpeciesMatch2ResponseModel getMatch(ExtendedRecord extendedRecord)
    throws TaxonomyInterpretationException {
    SpeciesMatch2ResponseModel responseModel = callSpeciesMatchWs(extendedRecord.getCoreTerms());

    if (!isSuccesfulMatch(responseModel)) {
      LOG.info("Retrying match with identification extension");
      // we try with the identification extension
      if (extendedRecord.getExtensions().containsKey(Extension.IDENTIFICATION)) {
        // get identifications
        final List<Map<CharSequence, CharSequence>> identifications =
          extendedRecord.getExtensions().get(Extension.IDENTIFICATION);

        // FIXME: use new generic functions to parse the date??
        // sort them by date identified
        identifications.sort(Comparator.comparing((Map<CharSequence, CharSequence> map) -> LocalDateTime.parse(map.get(
          DwcTerm.dateIdentified))).reversed());
        for (Map<CharSequence, CharSequence> record : identifications) {
          responseModel = callSpeciesMatchWs(record);
          if (isSuccesfulMatch(responseModel)) {
            LOG.info("match with identificationId {} succeed", record.get(DwcTerm.identificationID));
            return responseModel;
          }
        }
      }
    }

    return responseModel;
  }

  private static SpeciesMatch2ResponseModel callSpeciesMatchWs(Map<CharSequence, CharSequence> terms)
    throws TaxonomyInterpretationException {
    TaxonomyFieldsWorkingCopy workingCopy = new TaxonomyFieldsWorkingCopy(terms);

    SpeciesMatch2Service service = SpeciesMatch2ServiceRest.SINGLE.getService();

    Call<SpeciesMatch2ResponseModel> call = service.match2(workingCopy.kingdom,
                                                           workingCopy.phylum,
                                                           workingCopy.clazz,
                                                           workingCopy.order,
                                                           workingCopy.family,
                                                           workingCopy.genus,
                                                           workingCopy.rank != null ? workingCopy.rank.name() : null,
                                                           workingCopy.scientificName,
                                                           false,
                                                           false);

    SpeciesMatch2ResponseModel responseModel = null;

    try {
      Response<SpeciesMatch2ResponseModel> response = call.execute();

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

  private static boolean isEmptyResponse(SpeciesMatch2ResponseModel response) {
    return response == null || (response.getUsage() == null
                                && response.getClassification() == null
                                && response.getDiagnostics() == null);
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
      Rank rank = null;

      if (hasTerm(terms, DwcTerm.taxonRank)) {
        rank = RANK_PARSER.parse(value(terms, DwcTerm.taxonRank)).getPayload();
      }
      // try again with verbatim if it exists
      if (rank == null && hasTerm(terms, DwcTerm.verbatimTaxonRank)) {
        rank = RANK_PARSER.parse(value(terms, DwcTerm.verbatimTaxonRank)).getPayload();
      }
      // derive from atomized fields
      if (rank == null && hasTerm(terms, DwcTerm.genus)) {
        if (hasTerm(terms, DwcTerm.specificEpithet)) {
          if (hasTerm(terms, DwcTerm.infraspecificEpithet)) {
            rank = Rank.INFRASPECIFIC_NAME;
          } else {
            rank = Rank.SPECIES;
          }
        } else {
          rank = Rank.GENUS;
        }
      }
      return rank;
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
        if (!StringUtils.isBlank(genericName)) {
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

    private String value(Map<CharSequence, CharSequence> terms, Term term) {
      CharSequence val = terms.get(term.qualifiedName());
      return val == null ? null : val.toString();
    }

    private boolean hasTerm(Map<CharSequence, CharSequence> terms, Term term) {
      return !Strings.isNullOrEmpty(value(terms, term));
    }

  }

}
