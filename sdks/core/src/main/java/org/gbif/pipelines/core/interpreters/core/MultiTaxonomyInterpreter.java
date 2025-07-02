package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.createNameUsageMatchRequest;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.createTaxonRecord;
import static org.gbif.pipelines.core.utils.ModelUtils.*;

import java.util.*;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based on the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an
 * existing species.
 *
 * <p>The interpretation will match against each of the configured taxonomies.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaxonomyInterpreter {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, MultiTaxonRecord> interpretMultiTaxonomy(
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore,
      List<String> checklistKeys) {
    return (er, mtr) -> {
      if (kvStore == null) {
        return;
      }

      ModelUtils.checkNullOrEmpty(er);
      final List<TaxonRecord> trs = new ArrayList<>();

      for (String checklistKey : checklistKeys) {
        final NameUsageMatchRequest nameUsageMatchRequest =
            createNameUsageMatchRequest(er, checklistKey);
        TaxonRecord taxonRecord =
            TaxonRecord.newBuilder().setId(er.getId()).setDatasetKey(checklistKey).build();
        createTaxonRecord(nameUsageMatchRequest, kvStore, taxonRecord);
        trs.add(taxonRecord);
      }

      mtr.setId(er.getId());
      mtr.setTaxonRecords(trs);
      setCoreId(er, mtr);
      setParentEventId(er, mtr);
    };
  }

  /** Sets the coreId field. */
  public static void setCoreId(ExtendedRecord er, MultiTaxonRecord mtr) {
    Optional.ofNullable(er.getCoreId()).ifPresent(mtr::setCoreId);
  }

  /** Sets the parentEventId field. */
  public static void setParentEventId(ExtendedRecord er, MultiTaxonRecord mtr) {
    extractOptValue(er, DwcTerm.parentEventID).ifPresent(mtr::setParentId);
  }
}
