package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.parsers.location.GeocodeService;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class LocationTransformTest {

  private static class RemoveDateCreated extends DoFn<LocationRecord, LocationRecord> implements Serializable {

    @ProcessElement
    public void processElement(ProcessContext context) {
      LocationRecord locationRecord = LocationRecord.newBuilder(context.element()).setCreated(0L).build();
      context.output(locationRecord);
    }
  }

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private static GeocodeResponse toGeocodeResponse(Country country) {
    Location location = new Location();
    location.setIsoCountryCode2Digit(country.getIso2LetterCode());
    return new GeocodeResponse(Collections.singletonList(location));
  }

  @Test
  public void emptyLrTest() {

    // State
    GeocodeService service = GeocodeService.create(new KeyValueTestStoreStub<>());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();

    MetadataRecord mdr = MetadataRecord.newBuilder().setId("777").build();

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata", Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    // When
    PCollection<LocationRecord> recordCollection =
        p.apply(Create.of(er)).apply(LocationTransform.create(service).interpret(metadataView))
            .apply("Cleaning Date created", ParDo.of(new RemoveDateCreated()));

    // Should
    PAssert.that(recordCollection).empty();
    p.run();
  }

  @Test
  public void transformationTest() {

    // State
    KeyValueTestStoreStub<LatLng, GeocodeResponse> kvStore = new KeyValueTestStoreStub<>();
    kvStore.put(new LatLng(56.26d, 9.51d), toGeocodeResponse(Country.DENMARK));
    kvStore.put(new LatLng(36.21d, 138.25d), toGeocodeResponse(Country.JAPAN));
    GeocodeService service = GeocodeService.create(kvStore);

    final String[] denmark = {
        "0",
        Country.DENMARK.getTitle(),
        Country.DENMARK.getIso2LetterCode(),
        "EUROPE",
        "100.0",
        "110.0",
        "111.0",
        "200.0",
        "Ocean",
        "220.0",
        "222.0",
        "30.0",
        "0.00001",
        "56.26",
        "9.51",
        "Copenhagen",
        "GEODETIC_DATUM_ASSUMED_WGS84",
        "155.5",
        "44.5",
        "105.0",
        "5.0",
        "false"
    };
    final String[] japan = {
        "1",
        Country.JAPAN.getTitle(),
        Country.JAPAN.getIso2LetterCode(),
        "ASIA",
        "100.0",
        "110.0",
        "111.0",
        "200.0",
        "Ocean",
        "220.0",
        "222.0",
        "30.0",
        "0.00001",
        "36.21",
        "138.25",
        "Tokyo",
        "GEODETIC_DATUM_ASSUMED_WGS84",
        "155.5",
        "44.5",
        "105.0",
        "5.0",
        "true"
    };

    final MetadataRecord mdr = MetadataRecord.newBuilder()
        .setId("0")
        .setDatasetPublishingCountry(Country.DENMARK.getIso2LetterCode())
        .setDatasetKey(UUID.randomUUID().toString())
        .build();
    final List<ExtendedRecord> records = createExtendedRecordList(mdr, denmark, japan);
    final List<LocationRecord> locations = createLocationList(mdr, denmark, japan);

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata", Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    // When
    PCollection<LocationRecord> recordCollection =
        p.apply(Create.of(records)).apply(LocationTransform.create(service).interpret(metadataView))
            .apply("Cleaning Date created", ParDo.of(new RemoveDateCreated()));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(locations);

    // run pipeline with the options required
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(MetadataRecord metadataRecord, String[]... locations) {
    return Arrays.stream(locations)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
              Map<String, String> terms = record.getCoreTerms();
              terms.put(DwcTerm.country.qualifiedName(), x[1]);
              terms.put(DwcTerm.countryCode.qualifiedName(), x[2]);
              terms.put(DwcTerm.continent.qualifiedName(), x[3]);
              terms.put(DwcTerm.minimumElevationInMeters.qualifiedName(), x[4]);
              terms.put(DwcTerm.maximumElevationInMeters.qualifiedName(), x[5]);
              terms.put(DwcTerm.minimumDepthInMeters.qualifiedName(), x[6]);
              terms.put(DwcTerm.maximumDepthInMeters.qualifiedName(), x[7]);
              terms.put(DwcTerm.waterBody.qualifiedName(), x[8]);
              terms.put(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(), x[9]);
              terms.put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), x[10]);
              terms.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), x[11]);
              terms.put(DwcTerm.coordinatePrecision.qualifiedName(), x[12]);
              terms.put(DwcTerm.decimalLatitude.qualifiedName(), x[13]);
              terms.put(DwcTerm.decimalLongitude.qualifiedName(), x[14]);
              terms.put(DwcTerm.stateProvince.qualifiedName(), x[15]);
              terms.put(GbifTerm.publishingCountry.qualifiedName(), metadataRecord.getDatasetPublishingCountry());
              return record;
            })
        .collect(Collectors.toList());
  }

  private List<LocationRecord> createLocationList(MetadataRecord mdr, String[]... locations) {
    return Arrays.stream(locations)
        .map(
            x -> {
              LocationRecord record =
                  LocationRecord.newBuilder()
                      .setId(x[0])
                      .setCountry(x[1])
                      .setCountryCode(x[2])
                      .setContinent(x[3])
                      .setMinimumElevationInMeters(Double.valueOf(x[4]))
                      .setMaximumElevationInMeters(Double.valueOf(x[5]))
                      .setMinimumDepthInMeters(Double.valueOf(x[6]))
                      .setMaximumDepthInMeters(Double.valueOf(x[7]))
                      .setWaterBody(x[8])
                      .setMinimumDistanceAboveSurfaceInMeters(Double.valueOf(x[9]))
                      .setMaximumDistanceAboveSurfaceInMeters(Double.valueOf(x[10]))
                      .setCoordinateUncertaintyInMeters(Double.valueOf(x[11]))
                      .setCoordinatePrecision(Double.valueOf(x[12]))
                      .setDecimalLatitude(Double.valueOf(x[13]))
                      .setDecimalLongitude(Double.valueOf(x[14]))
                      .setStateProvince(x[15])
                      .setDepth(Double.valueOf(x[17]))
                      .setDepthAccuracy(Double.valueOf(x[18]))
                      .setElevation(Double.valueOf(x[19]))
                      .setElevationAccuracy(Double.valueOf(x[20]))
                      .setRepatriated(Boolean.parseBoolean(x[21]))
                      .setHasCoordinate(true)
                      .setHasGeospatialIssue(false)
                      .setPublishingCountry(mdr.getDatasetPublishingCountry())
                      .setCreated(0L)
                      .build();
              record.getIssues().getIssueList().add(x[16]);
              return record;
            })
        .collect(Collectors.toList());
  }
}
