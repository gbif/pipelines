package org.gbif.pipelines.interpretation.interpreters;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.utils.AvroDataUtils;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.adapters.TaxonRecordAdapter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.ws.WsResponse;
import org.gbif.pipelines.ws.match2.SpeciesMatchCaller;

import java.util.Collections;
import java.util.function.Function;

import static org.gbif.pipelines.interpretation.Interpretation.Trace;
import static org.gbif.pipelines.interpretation.Interpretation.of;
import static org.gbif.pipelines.interpretation.utils.TaxonomyUtils.checkMatchIssue;
import static org.gbif.pipelines.interpretation.utils.TaxonomyUtils.emptyNameUsageMatchResponse;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields should be based in the
 * Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 * <p>
 * The interpretation uses the species match WS to match the taxonomic fields to an existing specie. Configuration
 * of the WS has to be set in the "ws.properties".
 * </p>
 */
public interface TaxonomyInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  static TaxonomyInterpreter taxonomyInterpreter(TaxonRecord taxonRecord) {
    return (ExtendedRecord extendedRecord) -> {

      AvroDataUtils.checkNullOrEmpty(extendedRecord);

      // get match from WS
      WsResponse<NameUsageMatch2> response = SpeciesMatchCaller.getMatch(extendedRecord);

      Interpretation interpretation = of(extendedRecord);

      if (response.isError()) {
        interpretation.withValidation(Collections.singletonList(Trace.of(null,
                                                                         IssueType.INTERPRETATION_ERROR,
                                                                         response.getErrorCode().toString()
                                                                         + response.getErrorMessage())));
        return interpretation;
      }

      if (response.isResponsyEmpty(emptyNameUsageMatchResponse())) {
        // TODO: maybe I would need to add to the enum a new issue for this, sth like "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        interpretation.withValidation(Collections.singletonList(Trace.of(null,
                                                                         IssueType.TAXON_MATCH_NONE,
                                                                         "No results from match service")));
        return interpretation;
      }

      checkMatchIssue(response.getBody().getDiagnostics().getMatchType(), interpretation);

      // adapt taxon record
      TaxonRecordAdapter.adapt(response.getBody(), taxonRecord);
      taxonRecord.setId(extendedRecord.getId());

      return interpretation;
    };
  }

}
