package org.gbif.pipelines.transforms.specific;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.AustraliaSpatialInterpreter;
import org.gbif.pipelines.io.avro.AustraliaSpatialRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AUSTRALIA_SPATIAL_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.AUSTRALIA_SPATIAL;

/**
 * Beam level transformations for the Australia location, reads an avro, writes an avro, maps from value to keyValue
 * and transforms form {@link LocationRecord} to {@link AustraliaSpatialRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link AustraliaSpatialRecord} using {@link LocationRecord} as
 * a source and {@link AustraliaSpatialInterpreter} as interpretation steps
 */
@Slf4j
public class AustraliaSpatialTransform extends Transform<LocationRecord, AustraliaSpatialRecord> {

  private SerializableSupplier<KeyValueStore<LatLng, String>> kvStoreSupplier;
  private KeyValueStore<LatLng, String> kvStore;

  @Builder(buildMethodName = "create")
  private AustraliaSpatialTransform(
      SerializableSupplier<KeyValueStore<LatLng, String>> kvStoreSupplier,
      KeyValueStore<LatLng, String> kvStore) {
    super(AustraliaSpatialRecord.class, AUSTRALIA_SPATIAL, AustraliaSpatialTransform.class.getName(), AUSTRALIA_SPATIAL_RECORDS_COUNT);
    this.kvStoreSupplier = kvStoreSupplier;
    this.kvStore = kvStore;
  }

  /** Maps {@link AustraliaSpatialRecord} to key value, where key is {@link AustraliaSpatialRecord#getId} */
  public MapElements<AustraliaSpatialRecord, KV<String, AustraliaSpatialRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, AustraliaSpatialRecord>>() {})
        .via((AustraliaSpatialRecord ar) -> KV.of(ar.getId(), ar));
  }

  public AustraliaSpatialTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Setup
  public void setup() {
    if (kvStore == null && kvStoreSupplier != null) {
      kvStore = kvStoreSupplier.get();
    }
  }

  @Teardown
  public void tearDown() {
    if (kvStore != null) {
      try {
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KVStore", ex);
      }
    }
  }

  @Override
  public Optional<AustraliaSpatialRecord> convert(LocationRecord source) {
    return Interpretation.from(source)
        .to(lr -> AustraliaSpatialRecord.newBuilder()
            .setId(lr.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build())
        .when(lr -> Optional.ofNullable(lr.getCountryCode())
            .filter(c -> c.equals(Country.AUSTRALIA.getIso2LetterCode()))
            .filter(c -> new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()).isValid())
            .isPresent())
        .via(AustraliaSpatialInterpreter.interpret(kvStore))
        .get();
  }

}
