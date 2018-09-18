package org.gbif.pipelines.transforms;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore
@RunWith(JUnit4.class)
public class LocationTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Public field because {@link ClassRule} requires it. */
  @ClassRule public static final MockWebServer MOCK_SERVER = new MockWebServer();

  private static WsConfig wsConfig;

  @ClassRule
  public static final ExternalResource CONFIG_RESOURCE =
      new ExternalResource() {

        @Override
        protected void before() {
          wsConfig = WsConfigFactory.create(MOCK_SERVER.url("/").toString());
        }
      };

  @AfterClass
  public static void close() throws IOException {
    MOCK_SERVER.close();
  }

  @Test
  @Category(NeedsRunner.class)
  public void transformationTest() {

    // State
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
      "GEODETIC_DATUM_ASSUMED_WGS84"
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
      "GEODETIC_DATUM_ASSUMED_WGS84"
    };

    final List<ExtendedRecord> records = createExtendedRecordList(denmark, japan);
    final List<LocationRecord> locations = createLocationList(denmark, japan);

    enqueueGeocodeResponses();

    // When
    PCollection<LocationRecord> recordCollection =
        p.apply(Create.of(records)).apply(RecordTransforms.location(wsConfig));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(locations);

    // run pipeline with the options required
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... locations) {
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
              return record;
            })
        .collect(Collectors.toList());
  }

  private List<LocationRecord> createLocationList(String[]... locations) {
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
                      .build();
              record.getIssues().getIssueList().add(x[16]);
              return record;
            })
        .collect(Collectors.toList());
  }

  private static void enqueueGeocodeResponses() {
    Stream.of("denmark-reverse.json", "japan-reverse.json")
        .map(f -> Thread.currentThread().getContextClassLoader().getResourceAsStream(f))
        .forEach(
            is -> {
              BufferedSource source = Okio.buffer(Okio.source(is));
              MockResponse mockResponse = new MockResponse();
              try {
                MOCK_SERVER.enqueue(
                    mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
              } catch (IOException e) {
                Assert.fail(e.getMessage());
              }
            });
  }
}
