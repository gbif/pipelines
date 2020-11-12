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
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.LocationInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.geocode.GeocodeResponse;

@Slf4j
public class LocationTransform extends Transform<ExtendedRecord, LocationRecord> {

  private final ALAPipelinesConfig alaConfig;
  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> countryKvStoreSupplier;
  private final SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>>
      stateProvinceKvStoreSupplier;
  private final List<DateComponentOrdering> orderings;
  private final SerializableFunction<String, String> preprocessDateFn;

  private KeyValueStore<LatLng, GeocodeResponse> countryKvStore;
  private KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore;
  private ALALocationInterpreter alaLocationInterpreter;
  private CentrePoints countryCentrePoints;
  private CentrePoints stateProvinceCentrePoints;
  private Vocab stateProvinceVocab;

  @Setter private PCollectionView<MetadataRecord> metadataView;

  @Builder(buildMethodName = "create")
  private LocationTransform(
      ALAPipelinesConfig alaConfig,
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> countryKvStoreSupplier,
      SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> stateProvinceKvStoreSupplier,
      PCollectionView<MetadataRecord> metadataView,
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
    this.metadataView = metadataView;
    this.orderings = orderings;
    this.preprocessDateFn = preprocessDateFn;
  }

  /** Maps {@link LocationRecord} to key value, where key is {@link LocationRecord#getId} */
  public MapElements<LocationRecord, KV<String, LocationRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
        .via((LocationRecord lr) -> KV.of(lr.getId(), lr));
  }

  @Override
  public SingleOutput<ExtendedRecord, LocationRecord> interpret() {
    return ParDo.of(this).withSideInputs(metadataView);
  }

  /** Beam @Setup initializes resources */
  @SneakyThrows
  @Setup
  public void setup() {
    if (countryKvStore == null && countryKvStoreSupplier != null) {
      log.info("Initialize geocodeKvStore");
      countryKvStore = countryKvStoreSupplier.get();
    }
    if (stateProvinceKvStore == null && stateProvinceKvStoreSupplier != null) {
      log.info("Initialize geocodeKvStore");
      stateProvinceKvStore = stateProvinceKvStoreSupplier.get();
    }

    countryCentrePoints = CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
    stateProvinceCentrePoints =
        StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
    stateProvinceVocab =
        StateProvince.getInstance(alaConfig.getLocationInfoConfig().getStateProvinceNamesFile());

    if (alaLocationInterpreter == null) {
      alaLocationInterpreter =
          ALALocationInterpreter.builder()
              .orderings(orderings)
              .preprocessDateFn(preprocessDateFn)
              .create();
    }
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
    } catch (IOException ex) {
      log.warn("Can't close geocodeKvStore - {}", ex.getMessage());
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

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    Optional<LocationRecord> result =
        Interpretation.from(source)
            .to(lr)
            .when(er -> !er.getCoreTerms().isEmpty())
            .via(LocationInterpreter.interpretCountryAndCoordinates(countryKvStore, mdr))
            .via(LocationInterpreter::interpretContinent)
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
                    countryCentrePoints, stateProvinceCentrePoints, stateProvinceVocab))
            .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }
}
