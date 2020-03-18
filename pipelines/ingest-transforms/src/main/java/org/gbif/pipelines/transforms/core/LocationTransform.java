package org.gbif.pipelines.transforms.core;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

import org.gbif.pipelines.core.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.kv.GeocodeServiceFactory;
import org.gbif.pipelines.parsers.config.factory.KvConfigFactory;
import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.pipelines.parsers.parsers.location.GeocodeService;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.Transform;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;

/**
 * Beam level transformations for the DWC Location, reads an avro, writes an avro, maps from value to keyValue and
 * transforms form {@link ExtendedRecord} to {@link LocationRecord}.
 * <p>
 * ParDo runs sequence of interpretations for {@link LocationRecord} using {@link ExtendedRecord}
 * as a source and {@link LocationInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#location</a>
 */
@Slf4j
public class LocationTransform extends Transform<ExtendedRecord, LocationRecord> {

  @Getter
  private final KvConfig kvConfig;

  @Getter
  @Setter
  private GeocodeService service;
  private PCollectionView<MetadataRecord> metadataView;

  public LocationTransform(KvConfig kvConfig) {
    super(LocationRecord.class, LOCATION, LocationTransform.class.getName(), LOCATION_RECORDS_COUNT);
    this.kvConfig = kvConfig;
  }

  public static LocationTransform create() {
    return new LocationTransform(null);
  }

  public static LocationTransform create(KvConfig kvConfig) {
    return new LocationTransform(kvConfig);
  }

  public static LocationTransform create(String propertiesPath) {
    KvConfig config = KvConfigFactory.create(Paths.get(propertiesPath), KvConfigFactory.GEOCODE_PREFIX);
    return new LocationTransform(config);
  }

  public static LocationTransform create(Properties properties) {
    KvConfig config = KvConfigFactory.create(properties, KvConfigFactory.GEOCODE_PREFIX);
    return new LocationTransform(config);
  }

  public static LocationTransform create(GeocodeService service) {
    LocationTransform lt = new LocationTransform(null);
    lt.service = service;
    return lt;
  }

  public SingleOutput<ExtendedRecord, LocationRecord> interpret(PCollectionView<MetadataRecord> metadataView) {
    this.metadataView = metadataView;
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  public LocationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Initializes resources using singleton factory can be useful in case of non-Beam pipeline */
  public LocationTransform init() {
    service = GeocodeServiceFactory.getInstance(kvConfig);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (service == null) {
      service = GeocodeServiceFactory.create(kvConfig);
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    service.close();
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

    LocationRecord lr = LocationRecord.newBuilder()
        .setId(source.getId())
        .setCreated(Instant.now().toEpochMilli())
        .build();

    Optional<LocationRecord> result = Interpretation.from(source)
        .to(lr)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(LocationInterpreter.interpretCountryAndCoordinates(service, mdr))
        .via(LocationInterpreter::interpretContinent)
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
        .via(LocationInterpreter::interpretLocality)
        .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }
}
