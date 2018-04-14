package org.gbif.pipelines.labs.transform;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedOccurrence;
import org.gbif.pipelines.io.avro.ExtendedRecord;
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
public class ExtendedOccurrenceTransformTest {

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
    final String[] one =
      {"0", "OBSERVATION", "MALE", "INTRODUCED", "SPOROPHYTE", "HOLOTYPE", "2", Country.DENMARK.getTitle(),
        Country.DENMARK.getIso2LetterCode(), "EUROPE", "1", "1", "2018", "2018-01-01", "100.0", "110.0", "111.0",
        "200.0", "Ocean", "220.0", "222.0", "30.0", "0.00001", "56.26", "9.51"};

    final String[] two =
      {"1", "UNKNOWN", "HERMAPHRODITE", "INTRODUCED", "GAMETE", "HAPANTOTYPE", "1", Country.JAPAN.getTitle(),
        Country.JAPAN.getIso2LetterCode(), "ASIA", "1", "1", "2018", "2018-01-01", "100.0", "110.0", "111.0", "200.0",
        "Ocean", "220.0", "222.0", "30.0", "0.00001", "36.21", "138.25"};
    enqueueGeocodeResponses();

    final List<ExtendedRecord> records = createExtendedRecordList(one, two);

    // Expected
    final List<ExtendedOccurrence> interpretedRecords = createInterpretedExtendedRecordList(one, two);

    // When
    ExtendedOccurrenceTransform occurrenceTransform = ExtendedOccurrenceTransform.create().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(records));

    PCollectionTuple tuple = inputStream.apply(occurrenceTransform);

    PCollection<ExtendedOccurrence> recordCollection =
      tuple.get(occurrenceTransform.getDataTag()).apply(Kv2Value.create());

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(interpretedRecords);
    p.run();

  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... records) {
    return Arrays.stream(records).map(x -> {
      ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
      record.getCoreTerms().put(DwcTerm.basisOfRecord.qualifiedName(), x[1]);
      record.getCoreTerms().put(DwcTerm.sex.qualifiedName(), x[2]);
      record.getCoreTerms().put(DwcTerm.establishmentMeans.qualifiedName(), x[3]);
      record.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), x[4]);
      record.getCoreTerms().put(DwcTerm.typeStatus.qualifiedName(), x[5]);
      record.getCoreTerms().put(DwcTerm.individualCount.qualifiedName(), x[6]);
      record.getCoreTerms().put(DwcTerm.country.qualifiedName(), x[7]);
      record.getCoreTerms().put(DwcTerm.countryCode.qualifiedName(), x[8]);
      record.getCoreTerms().put(DwcTerm.continent.qualifiedName(), x[9]);
      record.getCoreTerms().put(DwcTerm.day.qualifiedName(), x[10]);
      record.getCoreTerms().put(DwcTerm.month.qualifiedName(), x[11]);
      record.getCoreTerms().put(DwcTerm.year.qualifiedName(), x[12]);
      record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), x[13]);
      record.getCoreTerms().put(DwcTerm.minimumDepthInMeters.qualifiedName(), x[14]);
      record.getCoreTerms().put(DwcTerm.maximumDepthInMeters.qualifiedName(), x[15]);
      record.getCoreTerms().put(DwcTerm.minimumElevationInMeters.qualifiedName(), x[16]);
      record.getCoreTerms().put(DwcTerm.maximumElevationInMeters.qualifiedName(), x[17]);
      record.getCoreTerms().put(DwcTerm.waterBody.qualifiedName(), x[18]);
      record.getCoreTerms().put(DwcTerm.minimumDistanceAboveSurfaceInMeters.qualifiedName(), x[19]);
      record.getCoreTerms().put(DwcTerm.maximumDistanceAboveSurfaceInMeters.qualifiedName(), x[20]);
      record.getCoreTerms().put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), x[21]);
      record.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), x[22]);
      record.getCoreTerms().put(DwcTerm.decimalLatitude.qualifiedName(), x[23]);
      record.getCoreTerms().put(DwcTerm.decimalLongitude.qualifiedName(), x[24]);

      return record;
    }).collect(Collectors.toList());
  }

  private List<ExtendedOccurrence> createInterpretedExtendedRecordList(String[]... records) {
    return Arrays.stream(records)
      .map(x -> ExtendedOccurrence.newBuilder()
        .setOccurrenceID(x[0])
        .setBasisOfRecord(x[1])
        .setSex(x[2])
        .setEstablishmentMeans(x[3])
        .setLifeStage(x[4])
        .setTypeStatus(x[5])
        .setIndividualCount(x[6])
        .setCountry(x[7])
        .setCountryCode(x[8])
        .setContinent(x[9])
        .setDay(Integer.valueOf(x[10]))
        .setMonth(Integer.valueOf(x[11]))
        .setYear(Integer.valueOf(x[12]))
        .setEventDate(EventDate.newBuilder().setGte(x[13]).build())
        .setMinimumDepthInMeters(Double.valueOf(x[14]))
        .setMaximumDepthInMeters(Double.valueOf(x[15]))
        .setMinimumElevationInMeters(Double.valueOf(x[16]))
        .setMaximumElevationInMeters(Double.valueOf(x[17]))
        .setWaterBody(x[18])
        .setMinimumDistanceAboveSurfaceInMeters(Double.valueOf(x[19]))
        .setMaximumDistanceAboveSurfaceInMeters(Double.valueOf(x[20]))
        .setCoordinateUncertaintyInMeters(Double.valueOf(x[21]))
        .setCoordinatePrecision(Double.valueOf(x[22]))
        .setDecimalLatitude(Double.valueOf(x[23]))
        .setDecimalLongitude(Double.valueOf(x[24]))
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
