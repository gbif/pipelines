package org.gbif.pipelines.core.interpretation;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonomyValidator;
import org.gbif.pipelines.core.utils.AvroDataValidator;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.match2.SpeciesMatchv2Client;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.function.Function;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields should be based in the
 * Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 * <p>
 * The interpretation uses the species match WS to match the taxonomic fields to an existing specie. Configuration
 * of the WS has to be set in the "http.properties".
 * </p>
 */
public interface TaxonomyInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  static TaxonomyInterpreter taxonomyInterpreter(TaxonRecord taxonRecord) {
    return (ExtendedRecord extendedRecord) -> {

      AvroDataValidator.checkNullOrEmpty(extendedRecord);

      // get match from WS
      HttpResponse<NameUsageMatch2> response = SpeciesMatchv2Client.getMatch(extendedRecord);

      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      if (response.isError()) {
        interpretation.withValidation(Trace.of(IssueType.INTERPRETATION_ERROR,
                                               response.getErrorCode() + response.getErrorMessage()));
        return interpretation;
      }

      if (TaxonomyValidator.isEmpty(response.getBody())) {
        // TODO: maybe I would need to add to the enum a new issue for this, sth like "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_NONE, "No results from match service"));
        return interpretation;
      }

      MatchType matchType = response.getBody().getDiagnostics().getMatchType();

      // TODO: fieldName shouldn't be required in Trace. Remove nulls when Interpretation is fixed.
      if (MatchType.NONE == matchType) {
        interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_NONE));
      } else if (MatchType.FUZZY == matchType) {
        interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_FUZZY));
      } else if (MatchType.HIGHERRANK == matchType) {
        interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_HIGHERRANK));
      }

      // convert taxon record
      TaxonRecordConverter.convert(response.getBody(), taxonRecord);
      taxonRecord.setId(extendedRecord.getId());

      return interpretation;
    };
  }

}
