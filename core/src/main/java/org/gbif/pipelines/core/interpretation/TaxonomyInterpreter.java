package org.gbif.pipelines.core.interpretation;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonomyValidator;
import org.gbif.pipelines.core.utils.AvroDataValidator;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.match2.SpeciesMatchv2Client;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.IssueType;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;

import java.util.function.BiConsumer;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match WS to match the taxonomic fields to an existing
 * specie. Configuration of the WS has to be set in the "http.properties".
 */
public interface TaxonomyInterpreter
    extends BiConsumer<ExtendedRecord, Interpretation<TaxonRecord>> {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  static TaxonomyInterpreter taxonomyInterpreter(Config wsConfig) {
    return (extendedRecord, interpretation) -> {
      AvroDataValidator.checkNullOrEmpty(extendedRecord);

      // get match from WS
      HttpResponse<NameUsageMatch2> response =
          SpeciesMatchv2Client.create(wsConfig).getMatch(extendedRecord);

      if (response.isError()) {
        String message = response.getErrorCode() + response.getErrorMessage();
        interpretation.withValidation(Trace.of(IssueType.INTERPRETATION_ERROR, message));
      } else if (TaxonomyValidator.isEmpty(response.getBody())) {
        // TODO: maybe I would need to add to the enum a new issue for this, sth like
        // "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        String message = "No results from match service";
        interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_NONE, message));
      } else {

        MatchType matchType = response.getBody().getDiagnostics().getMatchType();

        if (MatchType.NONE == matchType) {
          interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_NONE));
        } else if (MatchType.FUZZY == matchType) {
          interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_FUZZY));
        } else if (MatchType.HIGHERRANK == matchType) {
          interpretation.withValidation(Trace.of(IssueType.TAXON_MATCH_HIGHERRANK));
        }

        // convert taxon record
        TaxonRecord taxonRecord = interpretation.getValue();
        TaxonRecordConverter.convert(response.getBody(), taxonRecord);
        taxonRecord.setId(extendedRecord.getId());
      }
    };
  }
}
