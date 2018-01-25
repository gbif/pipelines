package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.exception.UnparsableException;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.RankedNameResponse;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.interpreter.taxonomy.SpeciesMatchManager.getMatch;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.checkIssues;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.getUsage;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.isSuccesfulMatch;

public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  private static final NameParserGbifV1 NAME_PARSER = new NameParserGbifV1();

  public static InterpretedTaxonomy interpretTaxonomyFields(ExtendedRecord extendedRecord) {

    // get match from WS
    SpeciesMatch2ResponseModel responseModel = getMatch(extendedRecord);

    // create interpreted taxonomy
    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // check issues
    interpretedTaxonomy.getIssues().addAll(checkIssues(responseModel));

    if (!isSuccesfulMatch(responseModel)) {
      // handle unsuccessful response
      return handleNotSuccesfulMatch(responseModel, extendedRecord);
    }

    // handle succesful response
    return handleSuccesfulMatch(responseModel, extendedRecord);
  }

  private static InterpretedTaxonomy handleSuccesfulMatch(
    SpeciesMatch2ResponseModel responseModel, ExtendedRecord extendedRecord
  ) {
    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // TODO: add all fields
    TaxonRecord taxonRecord = new TaxonRecord();
    interpretedTaxonomy.setTaxonRecord(taxonRecord);
    taxonRecord.setId(extendedRecord.getId());

    // ranks
    Map<Rank, RankedNameResponse> ranksResponse = responseModel.getClassification()
      .stream()
      .collect(Collectors.toMap(rankedName -> Rank.valueOf(rankedName.getRank()), rankedName -> rankedName));

    // parse name into pieces - we dont get them from the nub lookup
    try {
      ParsedName pn = NAME_PARSER.parse(getUsage(responseModel).getName(), Rank.valueOf(getUsage(responseModel)
                                                                                          .getRank()));
      taxonRecord.setGenericName(pn.getGenusOrAbove());
//      taxonRecord.setSpecificEpithet(pn.getSpecificEpithet());
//      taxonRecord.setInfraspecificEpithet(pn.getInfraSpecificEpithet());
    } catch (UnparsableException e) {
      if (e.type.isParsable()) {
        LOG.warn("Fail to parse backbone {} name for occurrence {}: {}", e.type, extendedRecord.getId(), e.name);
      }
    }

    // TODO: adapt this
//    for (Rank r : Rank.DWC_RANKS) {
//      ClassificationUtils.setHigherRank(occ, r, match.getHigherRank(r));
//      ClassificationUtils.setHigherRankKey(occ, r, match.getHigherRankKey(r));
//    }

    return interpretedTaxonomy;
  }

  private static InterpretedTaxonomy handleNotSuccesfulMatch(
    SpeciesMatch2ResponseModel responseModel, ExtendedRecord extendedRecord
  ) {
    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // TODO: apply unknown kingdom

    return interpretedTaxonomy;
  }

}
