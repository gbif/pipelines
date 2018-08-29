package org.gbif.pipelines.core.interpretation;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.Context;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonomyValidator;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.match2.SpeciesMatchv2Client;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.function.BiConsumer;

import static org.gbif.api.vocabulary.OccurrenceIssue.INTERPRETATION_ERROR;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match WS to match the taxonomic fields to an existing
 * specie. Configuration of the WS has to be set in the "http.properties".
 */
public class TaxonomyInterpreter {

  private TaxonomyInterpreter() {}

  public static Context<ExtendedRecord, TaxonRecord> createContext(ExtendedRecord er) {
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().build();
    return new Context<>(er, taxonRecord);
  }

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, TaxonRecord> taxonomyInterpreter(Config wsConfig) {
    return (er, tr) -> {
      ModelUtils.checkNullOrEmpty(er);

      // get match from WS
      HttpResponse<NameUsageMatch2> response = SpeciesMatchv2Client.create(wsConfig).getMatch(er);

      if (response.isError()) {
        tr.getIssues().getIssueList().add(INTERPRETATION_ERROR.name());
      } else if (TaxonomyValidator.isEmpty(response.getBody())) {
        // TODO: maybe I would need to add to the enum a new issue for this, sth like
        // "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        tr.getIssues().getIssueList().add(TAXON_MATCH_NONE.name());
      } else {

        MatchType matchType = response.getBody().getDiagnostics().getMatchType();

        if (MatchType.NONE == matchType) {
          tr.getIssues().getIssueList().add(TAXON_MATCH_NONE.name());
        } else if (MatchType.FUZZY == matchType) {
          tr.getIssues().getIssueList().add(TAXON_MATCH_FUZZY.name());
        } else if (MatchType.HIGHERRANK == matchType) {
          tr.getIssues().getIssueList().add(TAXON_MATCH_HIGHERRANK.name());
        }

        // convert taxon record
        TaxonRecordConverter.convert(response.getBody(), tr);
        tr.setId(er.getId());
      }
    };
  }
}
