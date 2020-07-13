package au.org.ala.pipelines.transforms;

import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import au.org.ala.pipelines.interpreters.ALATaxonomyInterpreter;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_TAXONOMY;

/**
 *
 * ALA taxonomy transform for adding ALA taxonomy to interpreted occurrence data.
 *
 * Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class ALATaxonomyTransform extends Transform<ExtendedRecord, ALATaxonRecord> {

  private KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvStore;
  private SerializableSupplier<KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch>> kvStoreSupplier;

  @Builder(buildMethodName = "create")
  private ALATaxonomyTransform(
          SerializableSupplier<KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch>> kvStoreSupplier,
          KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvStore) {
    super(ALATaxonRecord.class, ALA_TAXONOMY, ALATaxonomyTransform.class.getName(), "alaTaxonRecordsCount");
    this.kvStore = kvStore;
    this.kvStoreSupplier = kvStoreSupplier;
  }

  /** Maps {@link ALATaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALATaxonRecord, KV<String, ALATaxonRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALATaxonRecord>>() {})
        .via((ALATaxonRecord tr) -> KV.of(tr.getId(), tr));
  }

  public ALATaxonomyTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public ALATaxonomyTransform init() {
    setup();
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
//    if (Objects.nonNull(kvStore)) {
//      try {
//        log.info("Close NameUsageMatchKvStore");
//        //kvStore.close();
//      } catch (IOException ex) {
//        log.error("Error closing KV Store", ex);
//      }
//    }
  }

  @Override
  public Optional<ALATaxonRecord> convert(ExtendedRecord source) {

    ALATaxonRecord tr = ALATaxonRecord.newBuilder().setId(source.getId()).build();
    Interpretation.from(source)
        .to(tr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(ALATaxonomyInterpreter.alaTaxonomyInterpreter(kvStore));

    // the id is null when there is an error in the interpretation. In these
    // cases we do not write the taxonRecord because it is totally empty.
    return  Optional.of(tr);
  }
}
