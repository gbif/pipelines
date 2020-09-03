package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class LocationTransformTest {

  private static class RemoveDateCreated extends DoFn<LocationRecord, LocationRecord>
      implements Serializable {

    @ProcessElement
    public void processElement(ProcessContext context) {
      LocationRecord locationRecord =
          LocationRecord.newBuilder(context.element()).setCreated(0L).build();
      context.output(locationRecord);
    }
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static GeocodeResponse toGeocodeResponse(Country country) {
    List<Location> locations = new ArrayList<>();

    if (country != null) {
      Location political = new Location();
      political.setIsoCountryCode2Digit(country.getIso2LetterCode());
      locations.add(political);

      Location gadm0 = new Location();
      Location gadm1 = new Location();
      Location gadm2 = new Location();
      if (country.equals(Country.DENMARK)) {
        gadm0.setId("DNK");
        gadm0.setType("GADM0");
        gadm0.setSource("http://gadm.org/");
        gadm0.setName("Denmark");
        gadm0.setIsoCountryCode2Digit("DK");
        gadm0.setDistance(0d);

        gadm1.setId("DNK.2_1");
        gadm1.setType("GADM1");
        gadm1.setSource("http://gadm.org/");
        gadm1.setName("Midtjylland");
        gadm1.setIsoCountryCode2Digit("DK");
        gadm1.setDistance(0d);

        gadm2.setId("DNK.2.14_1");
        gadm2.setType("GADM2");
        gadm2.setSource("http://gadm.org/");
        gadm2.setName("Silkeborg");
        gadm2.setIsoCountryCode2Digit("DK");
        gadm2.setDistance(0d);
      } else {
        gadm0.setId("JPN");
        gadm0.setType("GADM0");
        gadm0.setSource("http://gadm.org/");
        gadm0.setName("Japan");
        gadm0.setIsoCountryCode2Digit("JP");
        gadm0.setDistance(0d);

        gadm1.setId("JPN.26_1");
        gadm1.setType("GADM1");
        gadm1.setSource("http://gadm.org/");
        gadm1.setName("Nagano");
        gadm1.setIsoCountryCode2Digit("JP");
        gadm1.setDistance(0d);

        gadm2.setId("JPN.26.40_1");
        gadm2.setType("GADM2");
        gadm2.setSource("http://gadm.org/");
        gadm2.setName("Nagawa");
        gadm2.setIsoCountryCode2Digit("JP");
        gadm2.setDistance(0d);
      }
      locations.add(gadm0);
      locations.add(gadm1);
      locations.add(gadm2);
    }

    return new GeocodeResponse(locations);
  }

  @Test
  public void emptyLrTest() {

    // State
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeKvStore =
        () -> GeocodeKvStore.create(new KeyValueTestStoreStub<>());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();

    MetadataRecord mdr = MetadataRecord.newBuilder().setId("777").build();

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata", Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    // When
    PCollection<LocationRecord> recordCollection =
        p.apply(Create.of(er))
            .apply(
                LocationTransform.builder()
                    .geocodeKvStoreSupplier(geocodeKvStore)
                    .metadataView(metadataView)
                    .create()
                    .interpret())
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
    kvStore.put(new LatLng(88.21d, -32.01d), toGeocodeResponse(null));
    SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> geocodeKvStore =
        () -> GeocodeKvStore.create(kvStore);

    final String[] denmark = {
      "0", // 0
      Country.DENMARK.getTitle(),
      Country.DENMARK.getIso2LetterCode(),
      "EUROPE",
      "100.0",
      "110.0", // 5
      "111.0",
      "200.0",
      "Ocean",
      "220.0",
      "222.0", // 10
      "30.0",
      "0.00001",
      "56.26",
      "9.51",
      "Copenhagen", // 15
      "GEODETIC_DATUM_ASSUMED_WGS84",
      "155.5",
      "44.5",
      "105.0",
      "5.0", // 20
      "false",
      "DNK",
      "DNK.2_1",
      "DNK.2.14_1",
      null, // 25
      "Denmark",
      "Midtjylland",
      "Silkeborg",
      null
    };
    final String[] japan = {
      "1", // 0
      Country.JAPAN.getTitle(),
      Country.JAPAN.getIso2LetterCode(),
      "ASIA",
      "100.0",
      "110.0", // 5
      "111.0",
      "200.0",
      "Ocean",
      "220.0",
      "222.0", // 10
      "30.0",
      "0.00001",
      "36.21",
      "138.25",
      "Tokyo", // 15
      "GEODETIC_DATUM_ASSUMED_WGS84",
      "155.5",
      "44.5",
      "105.0",
      "5.0", // 20
      "true",
      "JPN",
      "JPN.26_1",
      "JPN.26.40_1",
      null, // 25
      "Japan",
      "Nagano",
      "Nagawa",
      null
    };
    final String[] arctic = {
      "2", // 0
      null,
      null,
      null,
      "-80.0",
      "-40.0", // 5
      "0.0",
      "5.0",
      "Arctic Ocean",
      "0.0",
      "-1.5", // 10
      "500.0",
      "0.01",
      "88.21",
      "-32.01",
      null, // 15
      "GEODETIC_DATUM_ASSUMED_WGS84",
      "2.5",
      "2.5",
      "-60.0",
      "20.0", // 20
      null,
      null,
      null,
      null,
      null, // 25
      null,
      null,
      null,
      null
    };

    final MetadataRecord mdr =
        MetadataRecord.newBuilder()
            .setId("0")
            .setDatasetPublishingCountry(Country.DENMARK.getIso2LetterCode())
            .setDatasetKey(UUID.randomUUID().toString())
            .build();
    final List<ExtendedRecord> records = createExtendedRecordList(mdr, denmark, japan, arctic);
    final List<LocationRecord> locations = createLocationList(mdr, denmark, japan, arctic);

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Create test metadata", Create.of(mdr))
            .apply("Convert into view", View.asSingleton());

    // When
    PCollection<LocationRecord> recordCollection =
        p.apply(Create.of(records))
            .apply(
                LocationTransform.builder()
                    .geocodeKvStoreSupplier(geocodeKvStore)
                    .metadataView(metadataView)
                    .create()
                    .interpret())
            .apply("Cleaning Date created", ParDo.of(new RemoveDateCreated()));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(locations);

    // run pipeline with the options required
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(
      MetadataRecord metadataRecord, String[]... locations) {
    return Arrays.stream(locations)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
              Map<String, String> terms = record.getCoreTerms();
              Optional.ofNullable(x[1])
                  .ifPresent(y -> terms.put(DwcTerm.country.qualifiedName(), y));
              Optional.ofNullable(x[2])
                  .ifPresent(y -> terms.put(DwcTerm.countryCode.qualifiedName(), y));
              Optional.ofNullable(x[3])
                  .ifPresent(y -> terms.put(DwcTerm.continent.qualifiedName(), y));
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
              Optional.ofNullable(x[15])
                  .ifPresent(y -> terms.put(DwcTerm.stateProvince.qualifiedName(), y));
              terms.put(
                  GbifTerm.publishingCountry.qualifiedName(),
                  metadataRecord.getDatasetPublishingCountry());
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
                      .setRepatriated(x[21] == null ? null : Boolean.parseBoolean(x[21]))
                      .setGadm(
                          x[23] == null
                              ? null
                              : GadmFeatures.newBuilder()
                                  .setLevel0Gid(x[22])
                                  .setLevel1Gid(x[23])
                                  .setLevel2Gid(x[24])
                                  .setLevel3Gid(x[25])
                                  .setLevel0Name(x[26])
                                  .setLevel1Name(x[27])
                                  .setLevel2Name(x[28])
                                  .setLevel3Name(x[29])
                                  .build())
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
