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
 * should be based in the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an
 * existing specie.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaxonomyInterpreter {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, MultiTaxonRecord> taxonomyInterpreter(
      Map<String, KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>> kvStores) {
    return (er, mtr) -> {
      if (kvStores == null || kvStores.isEmpty()) {
        return;
      }

      ModelUtils.checkNullOrEmpty(er);

      final NameUsageMatchRequest nameUsageMatchRequest = createNameUsageMatchRequest(er);
      final List<TaxonRecord> trs = new ArrayList<>();

      for (Map.Entry<String, KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
          kvStoreEntry : kvStores.entrySet()) {

        String datasetKey = kvStoreEntry.getKey();
        TaxonRecord taxonRecord =
            TaxonRecord.newBuilder().setId(er.getId()).setDatasetKey(datasetKey).build();
        createTaxonRecord(nameUsageMatchRequest, kvStoreEntry.getValue(), er, taxonRecord);
        trs.add(taxonRecord);
      }

      mtr.setId(er.getId());
      mtr.setTaxonRecords(trs);
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
