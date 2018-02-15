package org.gbif.pipelines.interpretation.interpreters;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.utils.AvroDataUtils;
import org.gbif.pipelines.interpretation.taxonomy.TaxonomyInterpretationException;
import org.gbif.pipelines.interpretation.adapters.TaxonRecordAdapter;
import org.gbif.pipelines.interpretation.taxonomy.InterpretedTaxonomy;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.Validation;
import org.gbif.pipelines.ws.match2.SpeciesMatchManager;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields should be based in the
 * Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 * <p>
 * The interpretation uses the species match WS to match the taxonomic fields to an existing specie. Configuration
 * of the WS has to be set in the "ws.properties".
 * </p>
 */
public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  /**
   * Utility class must have private constructor.
   */
  private  TaxonomyInterpreter() {
    //DO nothing
  }

  /**
   * Interprets a taxonomy from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static InterpretedTaxonomy interpretTaxonomyFields(ExtendedRecord extendedRecord)
    throws TaxonomyInterpretationException {

    AvroDataUtils.checkNullOrEmpty(extendedRecord);

    // get match from WS
    NameUsageMatch2 responseModel = SpeciesMatchManager.getMatch(extendedRecord);

    // create interpreted taxonomy
    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // adapt taxon record
    TaxonRecord taxonRecord = new TaxonRecordAdapter().adapt(responseModel);
    taxonRecord.setId(extendedRecord.getId());
    interpretedTaxonomy.setTaxonRecord(taxonRecord);

    // check issues
    List<Validation> validations = checkIssues(responseModel, extendedRecord.getId());
    if (!validations.isEmpty()) {
      interpretedTaxonomy.setOccurrenceIssue(OccurrenceIssue.newBuilder()
                                               .setId(extendedRecord.getId())
                                               .setIssues(validations)
                                               .build());
    }

    return interpretedTaxonomy;
  }

  private static List<Validation> checkIssues(NameUsageMatch2 responseModel, CharSequence id) {

    List<Validation> validations = new ArrayList<>();

    switch (responseModel.getDiagnostics().getMatchType()) {
      case NONE:
        LOG.info("Match type NONE for occurrence {}", id);
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE));
        break;
      case FUZZY:
        LOG.info("Match type FUZZY for occurrence {}", id);
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY));
        break;
      case HIGHERRANK:
        LOG.info("Match type HIGHERRANK for occurrence {}", id);
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK));
        break;
    }

    return validations;
  }

  private static Validation createValidation(org.gbif.api.vocabulary.OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name())
      .build();
  }

}
