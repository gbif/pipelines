package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.LOCATION_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.LOCATION;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.pipelines.interpreters.ALALocationInterpreter;
import au.org.ala.pipelines.vocabulary.*;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.geocode.GeocodeResponse;

@Slf4j
public class LocationTransform extends Transform<ExtendedRecord, LocationRecord> {

  private final ALAPipelinesConfig alaConfig;
  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> countryKvStoreSupplier;
  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>>
      stateProvinceKvStoreSupplier;
  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> biomeKvStoreSupplier;
  private final List<DateComponentOrdering> orderings;
  private final SerializableFunction<String, String> preprocessDateFn;

  private KeyValueStore<LatLng, GeocodeResponse> countryKvStore;
  private KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore;
  private KeyValueStore<LatLng, GeocodeResponse> biomeKvStore;

  private ALALocationInterpreter alaLocationInterpreter;
  private CentrePoints countryCentrePoints;
  private CentrePoints stateProvinceCentrePoints;
  private StateProvinceParser stateProvinceParser;

  @Builder(buildMethodName = "create")
  private LocationTransform(
      ALAPipelinesConfig alaConfig,
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> countryKvStoreSupplier,
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> stateProvinceKvStoreSupplier,
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> biomeKvStoreSupplier,
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {

    super(
        LocationRecord.class,
        LOCATION,
        org.gbif.pipelines.transforms.core.LocationTransform.class.getName(),
        LOCATION_RECORDS_COUNT);
    this.alaConfig = alaConfig;
    this.countryKvStoreSupplier = countryKvStoreSupplier;
    this.stateProvinceKvStoreSupplier = stateProvinceKvStoreSupplier;
    this.biomeKvStoreSupplier = biomeKvStoreSupplier;
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  /** Beam @Setup initializes resources */
  @SneakyThrows
  @Setup
  public void setup() {
    if (countryKvStore == null && countryKvStoreSupplier != null) {
      log.info("Initialize countryKvStore");
      countryKvStore = countryKvStoreSupplier.get();
    }
    if (stateProvinceKvStore == null && stateProvinceKvStoreSupplier != null) {
      log.info("Initialize stateProvinceKvStore");
      stateProvinceKvStore = stateProvinceKvStoreSupplier.get();
    }
    if (biomeKvStore == null && biomeKvStoreSupplier != null) {
      log.info("Initialize biomeKvStore");
      biomeKvStore = biomeKvStoreSupplier.get();
    }

    countryCentrePoints = CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
    stateProvinceCentrePoints =
        StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
    stateProvinceParser =
        StateProvinceParser.getInstance(
            alaConfig.getLocationInfoConfig().getStateProvinceNamesFile());

    if (alaLocationInterpreter == null) {
      alaLocationInterpreter =
          ALALocationInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
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
    try {
      if (countryKvStore != null) {
        log.info("Close countryKvStore");
        countryKvStore.close();
      }
      if (stateProvinceKvStore != null) {
        log.info("Close stateProvinceKvStore");
        stateProvinceKvStore.close();
      }
      if (biomeKvStore != null) {
        log.info("Close biomeKvStore");
        biomeKvStore.close();
      }
    } catch (IOException ex) {
      log.warn("Can't close geocodeKvStore - {}", ex.getMessage());
    }
  }

  public LocationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<LocationRecord> convert(ExtendedRecord source) {

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    Optional<LocationRecord> result =
        Interpretation.from(source)
            .to(lr)
            .when(er -> !er.getCoreTerms().isEmpty())
            .via(LocationInterpreter.interpretCountryAndCoordinates(countryKvStore, null))
            .via(ALALocationInterpreter.interpretStateProvince(stateProvinceKvStore))
            .via(LocationInterpreter.interpretContinent(countryKvStore))
            .via(ALALocationInterpreter.interpretBiome(biomeKvStore))
            .via(LocationInterpreter::interpretWaterBody)
            .via(LocationInterpreter::interpretMinimumElevationInMeters)
            .via(LocationInterpreter::interpretMaximumElevationInMeters)
            .via(LocationInterpreter::interpretElevation)
            .via(LocationInterpreter::interpretMinimumDepthInMeters)
            .via(LocationInterpreter::interpretMaximumDepthInMeters)
            .via(LocationInterpreter::interpretDepth)
            .via(LocationInterpreter::interpretMinimumDistanceAboveSurfaceInMeters)
            .via(LocationInterpreter::interpretMaximumDistanceAboveSurfaceInMeters)
            .via(LocationInterpreter::interpretCoordinatePrecision)
            .via(ALALocationInterpreter::interpretCoordinateUncertaintyInMeters)
            .via(alaLocationInterpreter::interpretGeoreferencedDate)
            .via(ALALocationInterpreter::interpretGeoreferenceTerms)
            .via(
                ALALocationInterpreter.verifyLocationInfo(
                    countryCentrePoints, stateProvinceCentrePoints, stateProvinceParser))
            .via(ALALocationInterpreter.validateStateProvince(stateProvinceParser))
            .via(LocationInterpreter::interpretLocality)
            .via(LocationInterpreter::interpretFootprintWKT)
            .via(LocationInterpreter::setCoreId)
            .via(LocationInterpreter::setParentEventId)
            .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }
}
