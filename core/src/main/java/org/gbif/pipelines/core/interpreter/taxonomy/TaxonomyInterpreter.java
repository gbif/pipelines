package org.gbif.pipelines.core.interpreter.taxonomy;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.nameparser.NameParserGbifV1;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.interpreter.taxonomy.SpeciesMatchManager.getMatch;

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
      .filter(record -> record.getId() != null)
      .filter(record -> record.getId() != "")
      .orElseThrow(() -> new TaxonomyInterpretationException("ExtendedRecord with id and core terms is required. "
                                                             + "Please check how this ExtendedRecord was created."));

    // get match from WS
    NameUsageMatch2 responseModel = getMatch(extendedRecord);

    // create interpreted taxonomy
    InterpretedTaxonomy interpretedTaxonomy = new InterpretedTaxonomy();

    // adapt taxon record
    TaxonRecord taxonRecord = new TaxonRecordAdapter().adapt(responseModel);
    taxonRecord.setId(extendedRecord.getId());
    interpretedTaxonomy.setTaxonRecord(taxonRecord);

    // check issues
    List<Validation> validations = checkIssues(responseModel, extendedRecord.getId());
    interpretedTaxonomy.setOccurrenceIssue(OccurrenceIssue.newBuilder()
                                             .setId(extendedRecord.getId())
                                             .setIssues(validations)
                                             .build());

    return interpretedTaxonomy;
  }

  private static List<Validation> checkIssues(NameUsageMatch2 responseModel, CharSequence id) {

    List<Validation> validations = new ArrayList<>();

    switch (responseModel.getDiagnostics().getMatchType()) {
      case NONE:
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE));
        break;
      case FUZZY:
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY));
        break;
      case HIGHERRANK:
        validations.add(createValidation(org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK));
        break;
    }

    LOG.warn("Match type {} for occurrence {}", responseModel.getDiagnostics().getMatchType().name(), id);

    return validations;
  }

  private static Validation createValidation(org.gbif.api.vocabulary.OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name())
      .build();
  }

}
