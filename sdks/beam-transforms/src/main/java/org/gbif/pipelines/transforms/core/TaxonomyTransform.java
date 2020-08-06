package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.TAXON_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.TAXONOMY;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.species.NameUsageMatch;

/**
 * Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value to
 * keyValue and transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class TaxonomyTransform extends Transform<ExtendedRecord, TaxonRecord> {

  private final SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>>
      kvStoreSupplier;
  private KeyValueStore<SpeciesMatchRequest, NameUsageMatch> kvStore;

  @Builder(buildMethodName = "create")
  private TaxonomyTransform(
      SerializableSupplier<KeyValueStore<SpeciesMatchRequest, NameUsageMatch>> kvStoreSupplier) {
    super(TaxonRecord.class, TAXONOMY, TaxonomyTransform.class.getName(), TAXON_RECORDS_COUNT);
    this.kvStoreSupplier = kvStoreSupplier;
  }

  /** Maps {@link TaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<TaxonRecord, KV<String, TaxonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
        .via((TaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  public TaxonomyTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (kvStore == null && kvStoreSupplier != null) {
      log.info("Initialize NameUsageMatchKvStore");
      kvStore = kvStoreSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (Objects.nonNull(kvStore)) {
      try {
        log.info("Close NameUsageMatchKvStore");
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KV Store", ex);
      }
    }
  }

  @Override
  public Optional<TaxonRecord> convert(ExtendedRecord source) {
    TaxonRecord tr = TaxonRecord.newBuilder().setCreated(Instant.now().toEpochMilli()).build();

    Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(TaxonomyInterpreter.taxonomyInterpreter(kvStore));

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return tr.getId() == null ? Optional.empty() : Optional.of(tr);
  }
}
