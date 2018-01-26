package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.exception.UnparsableException;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.Rank;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.RankedNameResponse;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.interpreter.taxonomy.SpeciesMatchManager.getMatch;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.checkIssues;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.getUsage;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.isSuccesfulMatch;
import static org.gbif.pipelines.core.interpreter.taxonomy.TaxonomyHelper.setRankData;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields should be based in the
 * Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 * <p>
 * The interpretation uses the species match WS to match the taxonomic fields to an existing specie. Configuration
 * of the WS has to be set in the "species-ws.properties".
 * </p>
 */
public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  private static final NameParserGbifV1 NAME_PARSER = new NameParserGbifV1();

  /**
   * Interprets a taxonomy from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static InterpretedTaxonomy interpretTaxonomyFields(ExtendedRecord extendedRecord)
    throws TaxonomyInterpretationException {
    Optional.ofNullable(extendedRecord)
      .filter(record -> record.getCoreTerms() != null)
      .filter(record -> !record.getCoreTerms().isEmpty())
      .orElseThrow(() -> new TaxonomyInterpretationException("ExtendedRecord received is null or has no core terms. "
                                                             + "Please check how this ExtendedRecord was created."));

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

    LOG.info("Handling succesful response from record {}", extendedRecord.getId());

    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // create taxonRecord with occurrenceId
    TaxonRecord taxonRecord = new TaxonRecord();
    interpretedTaxonomy.setTaxonRecord(taxonRecord);
    taxonRecord.setId(extendedRecord.getId());

    // parse name into pieces - we dont get them from the nub lookup
    try {
      ParsedName pn =
        NAME_PARSER.parse(getUsage(responseModel).getName(), Rank.valueOf(getUsage(responseModel).getRank()));
      taxonRecord.setGenericName(pn.getGenusOrAbove());
      taxonRecord.setSpecificEpithet(pn.getSpecificEpithet());
      taxonRecord.setInfraspecificEpithet(pn.getInfraSpecificEpithet());
    } catch (UnparsableException e) {
      if (e.type.isParsable()) {
        LOG.warn("Fail to parse backbone {} name for occurrence {}: {}", e.type, extendedRecord.getId(), e.name);
      }
    }

    // diagnostics
    // TODO: do we need to add more diagnostic fields??
    // FIXME: confidence not returned yet by the WS
    // taxonRecord.setConfidence(responseModel.getDiagnostics().getConfidence());

    // nomenclature
    if (responseModel.getNomenclature() != null) {
      taxonRecord.setNomenclatureId(responseModel.getNomenclature().getId());
      taxonRecord.setNomenclatureSource(responseModel.getNomenclature().getSource());
    }

    // set usages
    taxonRecord.setUsageKey(responseModel.getUsage().getKey());
    taxonRecord.setUsageName(responseModel.getUsage().getName());
    taxonRecord.setUsageRank(responseModel.getUsage().getRank());

    if (responseModel.getAcceptedUsage() != null) {
      taxonRecord.setAcceptedUsageKey(responseModel.getAcceptedUsage().getKey());
      taxonRecord.setAcceptedUsageName(responseModel.getAcceptedUsage().getName());
      taxonRecord.setAcceptedUsageRank(responseModel.getAcceptedUsage().getRank());
    }

    // map with classifications by rank
    Map<Rank, RankedNameResponse> ranksResponse = responseModel.getClassification()
      .stream()
      .collect(Collectors.toMap(rankedName -> Rank.valueOf(rankedName.getRank()), rankedName -> rankedName));

    for (Rank rank : Rank.LINNEAN_RANKS) {
      setRankData(taxonRecord, rank, ranksResponse.get(rank));
    }

    return interpretedTaxonomy;
  }

  private static InterpretedTaxonomy handleNotSuccesfulMatch(
    SpeciesMatch2ResponseModel responseModel, ExtendedRecord extendedRecord
  ) {

    LOG.info("Handling unsuccesful response from record {}", extendedRecord.getId());

    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // create taxonRecord with occurrenceId
    TaxonRecord taxonRecord = new TaxonRecord();
    interpretedTaxonomy.setTaxonRecord(taxonRecord);
    taxonRecord.setId(extendedRecord.getId());

    // set unknown kingdom
    taxonRecord.setUsageRank(Rank.KINGDOM.name());
    taxonRecord.setUsageName(Kingdom.INCERTAE_SEDIS.scientificName());
    taxonRecord.setUsageKey(Kingdom.INCERTAE_SEDIS.nubUsageKey());

    return interpretedTaxonomy;
  }

}
