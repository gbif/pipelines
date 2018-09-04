package org.gbif.pipelines.core.interpreters;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.gbif.pipelines.parsers.ws.HttpResponse;
import org.gbif.pipelines.parsers.ws.client.match2.SpeciesMatchv2Client;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.function.BiConsumer;

import static org.gbif.api.vocabulary.OccurrenceIssue.INTERPRETATION_ERROR;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match WS to match the taxonomic fields to an existing
 * specie. Configuration of the WS has to be set in the "http.properties".
 */
public class TaxonomyInterpreter {

  private TaxonomyInterpreter() {}

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, TaxonRecord> taxonomyInterpreter(WsConfig wsConfig) {
    return (er, tr) -> {
      ModelUtils.checkNullOrEmpty(er);

      // get match from WS
      HttpResponse<NameUsageMatch2> response = SpeciesMatchv2Client.create(wsConfig).getMatch(er);

      if (response.isError()) {
        addIssue(tr, INTERPRETATION_ERROR);
      } else if (isEmpty(response.getBody())) {
        // "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        addIssue(tr, TAXON_MATCH_NONE);
      } else {

        MatchType matchType = response.getBody().getDiagnostics().getMatchType();

        if (MatchType.NONE == matchType) {
          addIssue(tr, TAXON_MATCH_NONE);
        } else if (MatchType.FUZZY == matchType) {
          addIssue(tr, TAXON_MATCH_FUZZY);
        } else if (MatchType.HIGHERRANK == matchType) {
          addIssue(tr, TAXON_MATCH_HIGHERRANK);
        }

        // convert taxon record
        TaxonRecordConverter.convert(response.getBody(), tr);
        tr.setId(er.getId());
      }
    };
  }

  private static boolean isEmpty(NameUsageMatch2 response) {
    return response == null
        || response.getUsage() == null
        || response.getClassification() == null
        || response.getDiagnostics() == null;
  }
}
