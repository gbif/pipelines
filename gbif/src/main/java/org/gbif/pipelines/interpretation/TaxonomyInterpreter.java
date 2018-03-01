package org.gbif.pipelines.interpretation;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.utils.AvroDataUtils;
import org.gbif.pipelines.http.HttpResponse;
import org.gbif.pipelines.http.config.Config;
import org.gbif.pipelines.http.match2.SpeciesMatch2Client;
import org.gbif.pipelines.http.match2.SpeciesMatch2Rest;
import org.gbif.pipelines.http.match2.SpeciesMatch2Service;
import org.gbif.pipelines.interpretation.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.interpretation.parsers.taxonomy.TaxonomyValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

import static org.gbif.pipelines.core.functions.interpretation.error.IssueType.INTERPRETATION_ERROR;
import static org.gbif.pipelines.core.functions.interpretation.error.IssueType.TAXON_MATCH_FUZZY;
import static org.gbif.pipelines.core.functions.interpretation.error.IssueType.TAXON_MATCH_HIGHERRANK;
import static org.gbif.pipelines.core.functions.interpretation.error.IssueType.TAXON_MATCH_NONE;
import static org.gbif.pipelines.interpretation.Interpretation.Trace;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields should be based in the
 * Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 * <p>
 * The interpretation uses the species match WS to match the taxonomic fields to an existing specie. Configuration
 * of the WS has to be set in the "http.properties".
 * </p>
 */
public interface TaxonomyInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  public static class Builder  {

    private Config config;

    public TaxonomyInterpreter.Builder withConfig(Config config) {
      this.config = config;
      return this;
    }

    private SpeciesMatch2Client buildClient() {
      return Objects.isNull(config) ? new SpeciesMatch2Client(SpeciesMatch2Rest.getInstance().getService()) :
        new SpeciesMatch2Client(SpeciesMatch2Rest.getInstance(config).getService());
    }

    TaxonomyInterpreter build(TaxonRecord taxonRecord) {
      return taxonomyInterpreter(taxonRecord, buildClient());
    }


    /**
     * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
     */
    private TaxonomyInterpreter taxonomyInterpreter(TaxonRecord taxonRecord, SpeciesMatch2Client speciesMatch2Client) {
      return (ExtendedRecord extendedRecord) -> {

        AvroDataUtils.checkNullOrEmpty(extendedRecord);

        // get match from WS
        HttpResponse<NameUsageMatch2> response = speciesMatch2Client.getMatch(extendedRecord);

        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

        if (response.isError()) {
          interpretation.withValidation(Collections.singletonList(Trace.of(null,
                                                                           INTERPRETATION_ERROR,
                                                                           response.getErrorCode().toString()
                                                                           + response.getErrorMessage())));
          return interpretation;
        }

        if (TaxonomyValidator.isEmpty(response.getBody())) {
          // TODO: maybe I would need to add to the enum a new issue for this, sth like "NO_MATCHING_RESULTS". This
          // happens when we get an empty response from the WS
          interpretation.withValidation(Collections.singletonList(Trace.of(null,
                                                                           TAXON_MATCH_NONE,
                                                                           "No results from match service")));
          return interpretation;
        }

        MatchType matchType = response.getBody().getDiagnostics().getMatchType();

        // TODO: fieldName shouldn't be required in Trace. Remove nulls when Interpretation is fixed.
        if (MatchType.NONE.equals(matchType)) {
          interpretation.withValidation(Collections.singletonList(Trace.of(null, TAXON_MATCH_NONE)));
        } else if (MatchType.FUZZY.equals(matchType)) {
          interpretation.withValidation(Collections.singletonList(Trace.of(null, TAXON_MATCH_FUZZY)));
        } else if (MatchType.HIGHERRANK.equals(matchType)) {
          interpretation.withValidation(Collections.singletonList(Trace.of(null, TAXON_MATCH_HIGHERRANK)));
        }

        // adapt taxon record
        TaxonRecordConverter.adapt(response.getBody(), taxonRecord);
        taxonRecord.setId(extendedRecord.getId());

        return interpretation;
      };
    }
  }

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  static TaxonomyInterpreter taxonomyInterpreter(TaxonRecord taxonRecord) {
    return new Builder().build(taxonRecord);
  }

}
