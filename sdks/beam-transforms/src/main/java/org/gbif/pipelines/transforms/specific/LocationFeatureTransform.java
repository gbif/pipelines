package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_FEATURE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION_FEATURE;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.LocationFeatureInterpreter;
import org.gbif.pipelines.io.avro.LocationFeatureRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the Australia location, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form {@link LocationRecord} to {@link LocationFeatureRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link LocationFeatureRecord} using {@link
 * LocationRecord} as a source and {@link LocationFeatureInterpreter} as interpretation steps
 */
@Slf4j
public class LocationFeatureTransform extends Transform<LocationRecord, LocationFeatureRecord> {

  private final SerializableSupplier<KeyValueStore<LatLng, String>> kvStoreSupplier;
  private KeyValueStore<LatLng, String> kvStore;

  @Builder(buildMethodName = "create")
  private LocationFeatureTransform(
      SerializableSupplier<KeyValueStore<LatLng, String>> kvStoreSupplier) {
    super(
        LocationFeatureRecord.class,
        LOCATION_FEATURE,
        LocationFeatureTransform.class.getName(),
        LOCATION_FEATURE_RECORDS_COUNT);
    this.kvStoreSupplier = kvStoreSupplier;
  }

  /**
   * Maps {@link LocationFeatureRecord} to key value, where key is {@link
   * LocationFeatureRecord#getId}
   */
  public MapElements<LocationFeatureRecord, KV<String, LocationFeatureRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationFeatureRecord>>() {})
        .via((LocationFeatureRecord ar) -> KV.of(ar.getId(), ar));
  }

  public LocationFeatureTransform counterFn(SerializableConsumer<String> counterFn) {
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
  public Optional<LocationFeatureRecord> convert(LocationRecord source) {
    return Interpretation.from(source)
        .to(
            lr ->
                LocationFeatureRecord.newBuilder()
                    .setId(lr.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(
            lr ->
                Optional.ofNullable(lr.getCountryCode())
                    .filter(c -> c.equals(Country.AUSTRALIA.getIso2LetterCode()))
                    .filter(
                        c ->
                            new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude()).isValid())
                    .isPresent())
        .via(LocationFeatureInterpreter.interpret(kvStore))
        .getOfNullable();
  }
}
