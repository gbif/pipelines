package org.gbif.pipelines.transform.record;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Location;
import org.gbif.pipelines.transform.Kv2Value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LocationTransformTest {

  private final static String WS_PROPERTIES_PATH = "ws.properties";

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  private static MockWebServer mockServer;

  @BeforeClass
  public static void setUp() throws IOException {
    mockServer = new MockWebServer();
    // TODO: check if the port is in use??
    mockServer.start(1111);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    mockServer.shutdown();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    final String[] denmark =
      {"0", Country.DENMARK.getTitle(), Country.DENMARK.getIso2LetterCode(), "EUROPE", "100.0", "110.0", "111.0",
        "200.0", "Ocean", "220.0", "222.0", "30.0", "0.00001", "56.26", "9.51", "Copenhagen"};
    final String[] japan =
      {"1", Country.JAPAN.getTitle(), Country.JAPAN.getIso2LetterCode(), "ASIA", "100.0", "110.0", "111.0", "200.0",
        "Ocean", "220.0", "222.0", "30.0", "0.00001", "36.21", "138.25", "Tokyo"};

    final List<ExtendedRecord> records = createExtendedRecordList(denmark, japan);
    enqueueGeocodeResponses();

    // Expected
    final List<Location> locations = createLocationList(denmark, japan);

    // When
    LocationTransform locationTransform = LocationTransform.create().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(records));

    PCollectionTuple tuple = inputStream.apply(locationTransform);

    PCollection<Location> recordCollection = tuple.get(locationTransform.getDataTag()).apply(Kv2Value.create());

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(locations);

    // run pipeline with the options required
    DataProcessingPipelineOptions options = PipelineOptionsFactory.create().as(DataProcessingPipelineOptions.class);
    options.setWsProperties(WS_PROPERTIES_PATH);
    p.run(options);
  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... locations) {
    return Arrays.stream(locations).map(x -> {
      ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
      record.getCoreTerms().put(DwcTerm.country.qualifiedName(), x[1]);
      record.getCoreTerms().put(DwcTerm.countryCode.qualifiedName(), x[2]);
      record.getCoreTerms().put(DwcTerm.continent.qualifiedName(), x[3]);
      record.getCoreTerms().put(DwcTerm.minimumElevationInMeters.qualifiedName(), x[4]);
      record.getCoreTerms().put(DwcTerm.maximumElevationInMeters.qualifiedName(), x[5]);
      record.getCoreTerms().put(DwcTerm.minimumDepthInMeters.qualifiedName(), x[6]);
      record.getCoreTerms().put(DwcTerm.maximumDepthInMeters.qualifiedName(), x[7]);
      record.getCoreTerms().put(DwcTerm.waterBody.qualifiedName(), x[8]);
      record.getCoreTerms().put(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(), x[9]);
      record.getCoreTerms().put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), x[10]);
      record.getCoreTerms().put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), x[11]);
      record.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), x[12]);
      record.getCoreTerms().put(DwcTerm.decimalLatitude.qualifiedName(), x[13]);
      record.getCoreTerms().put(DwcTerm.decimalLongitude.qualifiedName(), x[14]);
      record.getCoreTerms().put(DwcTerm.stateProvince.qualifiedName(), x[15]);
      return record;
    }).collect(Collectors.toList());
  }

  private List<Location> createLocationList(String[]... locations) {
    return Arrays.stream(locations)
      .map(x -> Location.newBuilder()
        .setOccurrenceID(x[0])
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
        .build())
      .collect(Collectors.toList());
  }

  private static void enqueueGeocodeResponses() {
    Arrays.asList("denmark-reverse.json", "japan-reverse.json").forEach(fileName -> {
      InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
      BufferedSource source = Okio.buffer(Okio.source(inputStream));
      MockResponse mockResponse = new MockResponse();
      try {
        mockServer.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    });
  }

}
