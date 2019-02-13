package org.gbif.pipelines.core.interpreters;

import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.parsers.utils.ModelUtils;
import org.gbif.rest.client.species.NameUsageMatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.vocabulary.OccurrenceIssue.INTERPRETATION_ERROR;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_FUZZY;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_HIGHERRANK;
import static org.gbif.api.vocabulary.OccurrenceIssue.TAXON_MATCH_NONE;
import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an existing
 * specie.
 */
public class TaxonomyInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpreter.class);

  private TaxonomyInterpreter() {}

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, TaxonRecord> taxonomyInterpreter(
      KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore) {
    return (er, tr) -> {
      ModelUtils.checkNullOrEmpty(er);

      SpeciesMatchRequest matchRequest = SpeciesMatchRequest.builder()
          .withKingdom(extractValue(er, DwcTerm.kingdom))
          .withPhylum(extractValue(er, DwcTerm.phylum))
          .withClass_(extractValue(er, DwcTerm.class_))
          .withOrder(extractValue(er, DwcTerm.order))
          .withFamily(extractValue(er, DwcTerm.family))
          .withGenus(extractValue(er, DwcTerm.genus))
          .withScientificName(extractValue(er, DwcTerm.scientificName))
          .build();

      NameUsageMatch usageMatch = null;
      try {
        usageMatch = kvStore.get(matchRequest);
      } catch (NoSuchElementException | NullPointerException ex) {
        LOG.error(ex.getMessage(), ex);
      }

      if (usageMatch == null) {
        addIssue(tr, INTERPRETATION_ERROR);
      } else if (isEmpty(usageMatch)) {
        // "NO_MATCHING_RESULTS". This
        // happens when we get an empty response from the WS
        addIssue(tr, TAXON_MATCH_NONE);
      } else {

        MatchType matchType = usageMatch.getDiagnostics().getMatchType();

        if (MatchType.NONE == matchType) {
          addIssue(tr, TAXON_MATCH_NONE);
        } else if (MatchType.FUZZY == matchType) {
          addIssue(tr, TAXON_MATCH_FUZZY);
        } else if (MatchType.HIGHERRANK == matchType) {
          addIssue(tr, TAXON_MATCH_HIGHERRANK);
        }

        // convert taxon record
        TaxonRecordConverter.convert(usageMatch, tr);
        tr.setId(er.getId());
      }
    };
  }

  private static boolean isEmpty(NameUsageMatch response) {
    return response == null
        || response.getUsage() == null
        || response.getClassification() == null
        || response.getDiagnostics() == null;
  }
}
