package org.gbif.pipelines.transforms.core;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.LOCATION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.geocode.GeocodeResponse;

/**
 * Beam level transformations for the DWC Location, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link LocationRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
 * as a source and {@link LocationInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#location</a>
 */
@Slf4j
public class LocationTransform extends Transform<ExtendedRecord, LocationRecord> {

  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeKvStoreSupplier;
  private KeyValueStore<LatLng, GeocodeResponse> geocodeKvStore;

  private PCollectionView<MetadataRecord> metadataView;

  @Builder(buildMethodName = "create")
  protected LocationTransform(
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeKvStoreSupplier,
      PCollectionView<MetadataRecord> metadataView) {
    super(
        LocationRecord.class, LOCATION, LocationTransform.class.getName(), LOCATION_RECORDS_COUNT);
    this.geocodeKvStoreSupplier = geocodeKvStoreSupplier;
    this.metadataView = metadataView;
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return asKv(false);
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getCoreId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toCoreIdKv() {
    return asKv(true);
  }

  private MapElements<LocationRecord, KV<String, LocationRecord>> asKv(boolean useCoreId) {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(useCoreId ? lr.getCoreId() : lr.getId(), lr));
  }

  public LocationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public SingleOutput<ExtendedRecord, LocationRecord> interpret(
      PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return interpret();
  }

  @Override
  public SingleOutput<ExtendedRecord, LocationRecord> interpret() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (geocodeKvStore == null && geocodeKvStoreSupplier != null) {
      log.info("Initialize geocodeKvStore");
      geocodeKvStore = geocodeKvStoreSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public LocationTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (geocodeKvStore != null) {
      try {
        log.info("Close geocodeKvStore");
        geocodeKvStore.close();
      } catch (IOException ex) {
        log.warn("Can't close geocodeKvStore - {}", ex.getMessage());
      }
    }
  }

  @Override
  public Optional<LocationRecord> convert(ExtendedRecord source) {
    throw new IllegalArgumentException("Method is not implemented!");
  }

  @Override
  @ProcessElement
  public void processElement(ProcessContext c) {
    processElement(c.element(), c.sideInput(metadataView)).ifPresent(c::output);
  }

  public Optional<LocationRecord> processElement(ExtendedRecord source, MetadataRecord mdr) {

    return Interpretation.from(source)
        .to(
            er ->
                LocationRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(LocationInterpreter.interpretCountryAndCoordinates(geocodeKvStore, mdr))
        .via(LocationInterpreter.interpretContinent(geocodeKvStore))
        .via(LocationInterpreter.interpretGadm(geocodeKvStore))
        .via(LocationInterpreter::interpretWaterBody)
        .via(LocationInterpreter::interpretStateProvince)
        .via(LocationInterpreter::interpretMinimumElevationInMeters)
        .via(LocationInterpreter::interpretMaximumElevationInMeters)
        .via(LocationInterpreter::interpretElevation)
        .via(LocationInterpreter::interpretMinimumDepthInMeters)
        .via(LocationInterpreter::interpretMaximumDepthInMeters)
        .via(LocationInterpreter::interpretDepth)
        .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
        .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
        .via(LocationInterpreter::interpretCoordinatePrecision)
        .via(LocationInterpreter::interpretCoordinateUncertaintyInMeters)
        .via(LocationInterpreter.calculateCentroidDistance(geocodeKvStore))
        .via(LocationInterpreter::interpretLocality)
        .via(LocationInterpreter::interpretFootprintWKT)
        .via(LocationInterpreter::interpretHigherGeography)
        .via(LocationInterpreter::interpretGeoreferencedBy)
        .via(LocationInterpreter::interpretGbifRegion)
        .via(LocationInterpreter::interpretPublishedByGbifRegion)
        .via(LocationInterpreter::setCoreId)
        .via(LocationInterpreter::setParentEventId)
        .via(r -> this.incCounter())
        .getOfNullable();
  }
}
