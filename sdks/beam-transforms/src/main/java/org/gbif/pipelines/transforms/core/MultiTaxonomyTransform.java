package org.gbif.pipelines.transforms.core;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.TAXONOMY;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MultiTaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link MultiTaxonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MultiTaxonRecord} using {@link
 * ExtendedRecord} as a source and {@link MultiTaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class MultiTaxonomyTransform extends Transform<ExtendedRecord, MultiTaxonRecord> {

  private final SerializableSupplier<
          List<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>>
      kvStoresSupplier;
  private List<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>> kvStores;

  @Builder(buildMethodName = "create")
  private MultiTaxonomyTransform(
      SerializableSupplier<List<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>>
          kvStoresSupplier) {
    super(
        MultiTaxonRecord.class,
        TAXONOMY,
        MultiTaxonomyTransform.class.getName(),
        TAXON_RECORDS_COUNT);
    this.kvStoresSupplier = kvStoresSupplier;
  }

  /** Maps {@link MultiTaxonRecord} to key value, where key is {@link MultiTaxonRecord#getId} */
  public MapElements<MultiTaxonRecord, KV<String, MultiTaxonRecord>> asKv(boolean useCoreId) {
    return MapElements.into(new TypeDescriptor<KV<String, MultiTaxonRecord>>() {})
        .via((MultiTaxonRecord tr) -> KV.of(useCoreId ? tr.getCoreId() : tr.getId(), tr));
  }

  /** Maps {@link MultiTaxonRecord} to key value, where key is {@link MultiTaxonRecord#getId} */
  public MapElements<MultiTaxonRecord, KV<String, MultiTaxonRecord>> toKv() {
    return asKv(false);
  }

  public MultiTaxonomyTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (kvStores == null && kvStoresSupplier != null) {
      log.info("Initialize NameUsageMatchKvStore");
      kvStores = kvStoresSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public MultiTaxonomyTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (kvStores != null && !kvStores.isEmpty()) {
      log.info("Close NameUsageMatchKvStores");
      kvStores.forEach(
          kvStore -> {
            try {
              kvStore.close();
            } catch (IOException ex) {
              log.error("Error closing KV Store", ex);
            }
          });
    }
  }

  @Override
  public Optional<MultiTaxonRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(MultiTaxonRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(MultiTaxonomyInterpreter.taxonomyInterpreter(kvStores))
        .via(MultiTaxonomyInterpreter::setCoreId)
        .via(MultiTaxonomyInterpreter::setParentEventId)
        .skipWhen(tr -> tr.getId() == null)
        .getOfNullable();
  }
}
