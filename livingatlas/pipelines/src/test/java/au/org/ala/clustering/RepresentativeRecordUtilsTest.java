package au.org.ala.clustering;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.gbif.pipelines.io.avro.OccurrenceFeatures;
import org.junit.Test;

public class RepresentativeRecordUtilsTest {

  @Test
  public void testDetermineCoordPrecision() throws IOException {

    assertEquals(
        new Integer(1),
        RepresentativeRecordUtils.determineCoordPrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12.1d)
                .setDecimalLongitude(12.1d)
                .build()));

    assertEquals(
        new Integer(0),
        RepresentativeRecordUtils.determineCoordPrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12d)
                .setDecimalLongitude(12d)
                .build()));

    assertEquals(
        new Integer(5),
        RepresentativeRecordUtils.determineCoordPrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12.12345d)
                .setDecimalLongitude(12.12345d)
                .build()));

    assertEquals(
        new Integer(1),
        RepresentativeRecordUtils.determineCoordPrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12.1000d)
                .setDecimalLongitude(12.1000d)
                .build()));
  }

  @Test
  public void testDetermineDatePrecision() throws IOException {
    assertEquals(
        new Integer(3),
        RepresentativeRecordUtils.determineDatePrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setYear(1979)
                .setMonth(12)
                .setDay(1)
                .build()));

    assertEquals(
        new Integer(2),
        RepresentativeRecordUtils.determineDatePrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setYear(1979)
                .setMonth(12)
                .build()));

    assertEquals(
        new Integer(1),
        RepresentativeRecordUtils.determineDatePrecision(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setYear(1979)
                .build()));

    assertEquals(
        new Integer(0),
        RepresentativeRecordUtils.determineDatePrecision(
            OccurrenceFeatures.newBuilder().setHashKey("1").setDatasetKey("1").setId("1").build()));
  }

  @Test
  public void testRankDatePrecision() throws IOException {
    List<OccurrenceFeatures> testSet =
        Arrays.asList(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setYear(1979)
                .setMonth(12)
                .setDay(1)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("2")
                .setDatasetKey("1")
                .setId("2")
                .setYear(1979)
                .setMonth(12)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("3")
                .setDatasetKey("1")
                .setId("3")
                .setYear(1979)
                .build());

    List<OccurrenceFeatures> results = RepresentativeRecordUtils.rankDatePrecision(testSet);
    assertTrue(results.size() == 1);
    assertEquals(results.get(0).getId(), "1");
  }

  @Test
  public void testRankCoordPrecision() throws IOException {
    List<OccurrenceFeatures> testSet =
        Arrays.asList(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12.123d)
                .setDecimalLongitude(12.123d)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("2")
                .setDatasetKey("1")
                .setId("2")
                .setDecimalLatitude(12.12d)
                .setDecimalLongitude(12.12d)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("3")
                .setDatasetKey("1")
                .setId("3")
                .setDecimalLatitude(12d)
                .setDecimalLongitude(12d)
                .build());

    List<OccurrenceFeatures> results = RepresentativeRecordUtils.rankCoordPrecision(testSet);
    assertTrue(results.size() == 1);
    assertEquals(results.get(0).getId(), "1");
  }

  @Test
  public void testFindRepresentative() throws IOException {
    List<OccurrenceFeatures> testSet =
        Arrays.asList(
            OccurrenceFeatures.newBuilder()
                .setHashKey("1")
                .setDatasetKey("1")
                .setId("1")
                .setDecimalLatitude(12.123d)
                .setDecimalLongitude(12.123d)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("2")
                .setDatasetKey("1")
                .setId("2")
                .setDecimalLatitude(12.12d)
                .setDecimalLongitude(12.12d)
                .build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("3")
                .setDatasetKey("1")
                .setId("3")
                .setDecimalLatitude(12d)
                .setDecimalLongitude(12d)
                .build());

    OccurrenceFeatures result = RepresentativeRecordUtils.findRepresentativeRecord(testSet);
    assertNotNull(result);
    assertEquals(result.getId(), "1");
  }

  @Test
  public void testFindRepresentativeChooseOldestUUID() throws IOException {

    String uuid1 = UUID.randomUUID().toString();

    String uuid2 = UUID.randomUUID().toString();

    String uuid3 = UUID.randomUUID().toString();

    List<OccurrenceFeatures> testSet =
        Arrays.asList(
            OccurrenceFeatures.newBuilder().setHashKey("1").setDatasetKey("1").setId(uuid2).build(),
            OccurrenceFeatures.newBuilder().setHashKey("2").setDatasetKey("1").setId(uuid1).build(),
            OccurrenceFeatures.newBuilder()
                .setHashKey("3")
                .setDatasetKey("1")
                .setId(uuid3)
                .build());

    OccurrenceFeatures result = RepresentativeRecordUtils.findRepresentativeRecord(testSet);
    assertNotNull(result);
    assertEquals(result.getId(), uuid1);
  }
}
