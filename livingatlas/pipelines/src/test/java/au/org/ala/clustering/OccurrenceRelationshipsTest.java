package au.org.ala.clustering;

import static au.org.ala.clustering.RelationshipAssertion.FEATURE_ASSERTION.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.gbif.pipelines.io.avro.OccurrenceFeatures;
import org.junit.Test;

/** Tests for relationship assertion. */
public class OccurrenceRelationshipsTest {

  @Test
  public void testSimpleAssertions() throws IOException {

    OccurrenceFeatures o1 =
        OccurrenceFeatures.newBuilder()
            .setId("o1")
            .setHashKey("1")
            .setDatasetKey("1")
            .setOccurrenceID("1")
            .setSpeciesKey("1")
            .setDecimalLatitude(44.0d)
            .setDecimalLongitude(44.0d)
            .setCatalogNumber("TIM1")
            .setYear(1978)
            .setMonth(12)
            .setDay(21)
            .build();

    OccurrenceFeatures o2 =
        OccurrenceFeatures.newBuilder()
            .setId("o2")
            .setHashKey("1")
            .setDatasetKey("2")
            .setOccurrenceID("2")
            .setSpeciesKey("1")
            .setDecimalLatitude(44.0d)
            .setDecimalLongitude(44.0d)
            //      .setCatalogNumber("TIM1")
            .setYear(1978)
            .setMonth(12)
            .setDay(21)
            .build();

    RelationshipAssertion assertion = OccurrenceRelationships.generate(o1, o2);

    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_ACCEPTED_SPECIES));
  }

  /** Real data from records 2332470913, 2571156410 which should cluster. */
  @Test
  public void testCortinarius() throws IOException {
    OccurrenceFeatures o1 =
        OccurrenceFeatures.newBuilder()
            .setId("o1")
            .setHashKey("1")
            .setDatasetKey("1")
            .setOccurrenceID("urn:catalog:O:F:304835")
            .setRecordNumber("TEB 12-16")
            .setSpeciesKey("3348943")
            .setDecimalLatitude(60.3302d)
            .setDecimalLongitude(10.4647d)
            .setCatalogNumber("304835")
            .setYear(2016)
            .setMonth(6)
            .setDay(11)
            .setEventDate(
                LocalDateTime.parse("2016-06-11T00:00:00")
                    .toInstant(ZoneOffset.UTC)
                    .getEpochSecond())
            .build();

    OccurrenceFeatures o2 =
        OccurrenceFeatures.newBuilder()
            .setId("o2")
            .setHashKey("1")
            .setDatasetKey("2")
            .setOccurrenceID("urn:uuid:152ce614-69e1-4fbe-8f1c-3340d0a15491")
            .setSpeciesKey("3348943")
            .setDecimalLatitude(60.330181d)
            .setDecimalLongitude(10.464743d)
            .setCatalogNumber("O-DFL-6644/2-D")
            .setRecordNumber("TEB 12-16")
            .setYear(2016)
            .setMonth(6)
            .setDay(11)
            .setEventDate(
                LocalDateTime.parse("2016-06-11T00:00:00")
                    .toInstant(ZoneOffset.UTC)
                    .getEpochSecond())
            .build();

    RelationshipAssertion assertion = OccurrenceRelationships.generate(o1, o2);

    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_ACCEPTED_SPECIES));
  }

  // Test even with nonsense a Holotype of the same name must be the same specimen (or worth
  // investigating a data issue)
  @Test
  public void testHolotype() throws IOException {
    OccurrenceFeatures o1 =
        OccurrenceFeatures.newBuilder()
            .setId("o1")
            .setHashKey("1")
            .setDatasetKey("1")
            .setTaxonKey("3350984")
            .setDecimalLatitude(10d)
            .setDecimalLongitude(10d)
            .setCountryCode("DK")
            .setTypeStatus("HoloType")
            .build();

    OccurrenceFeatures o2 =
        OccurrenceFeatures.newBuilder()
            .setId("o2")
            .setHashKey("1")
            .setDatasetKey("2")
            .setTaxonKey("3350984")
            .setDecimalLatitude(20d) // different
            .setDecimalLongitude(20d) // different
            .setCountryCode("NO") // different
            .setTypeStatus("HoloType")
            .build();

    RelationshipAssertion assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_SPECIMEN));
  }

  // Test that two records with same collector, approximate location but a day apart match.
  // https://github.com/gbif/occurrence/issues/177
  @Test
  public void testDayApart() throws IOException {
    // real records where a trap set one evening and visited the next day is shared twice using
    // different
    // days
    OccurrenceFeatures o1 =
        OccurrenceFeatures.newBuilder()
            .setId("49635968")
            .setHashKey("1")
            .setDatasetKey("1")
            .setSpeciesKey("1850114")
            .setDecimalLatitude(55.737d)
            .setDecimalLongitude(12.538d)
            .setYear(2004)
            .setMonth(8)
            .setDay(1) // day trap set
            .setCountryCode("DK")
            .setRecordedBy("Donald Hobern")
            .build();

    OccurrenceFeatures o2 =
        OccurrenceFeatures.newBuilder()
            .setId("1227719129")
            .setHashKey("1")
            .setDatasetKey("2")
            .setSpeciesKey("1850114")
            .setDecimalLatitude(55.736932d) // different
            .setDecimalLongitude(12.538104d)
            .setYear(2004)
            .setMonth(8)
            .setDay(2) // day collected
            .setCountryCode("DK")
            .setRecordedBy("Donald Hobern")
            .build();

    RelationshipAssertion assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(
        assertion.justificationContainsAll(
            APPROXIMATE_DATE, WITHIN_200m, SAME_COUNTRY, SAME_RECORDER_NAME));
  }

  // test 3 decimal place rounding example clusters
  @Test
  public void test3DP() throws IOException {
    // real records of Seigler & Miller
    OccurrenceFeatures o1 =
        OccurrenceFeatures.newBuilder()
            .setId("1675790844")
            .setHashKey("1")
            .setDatasetKey("1")
            .setSpeciesKey("3794925")
            .setDecimalLatitude(21.8656d)
            .setDecimalLongitude(-102.909d)
            .setYear(2007)
            .setMonth(5)
            .setDay(26)
            .setRecordedBy("D. S. Seigler & J. T. Miller")
            .build();

    OccurrenceFeatures o2 =
        OccurrenceFeatures.newBuilder()
            .setId("2268858676")
            .setHashKey("1")
            .setDatasetKey("2")
            .setSpeciesKey("3794925")
            .setDecimalLatitude(21.86558d)
            .setDecimalLongitude(-102.90929d)
            .setYear(2007)
            .setMonth(5)
            .setDay(26)
            .setRecordedBy(
                "David S. Seigler|J.T. Miller") // we should at some point detect this match
            .build();

    RelationshipAssertion assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(assertion.justificationContainsAll(SAME_DATE, WITHIN_200m, SAME_ACCEPTED_SPECIES));
  }

  @Test
  public void testNormaliseID() {
    assertEquals("ABC", OccurrenceRelationships.normalizeID(" A-/, B \\C"));
    // These are examples of collectors we could be able to organize in the future
    assertEquals(
        "DAVIDSSEIGLERJTMILLER",
        OccurrenceRelationships.normalizeID("David S. Seigler|J.T. Miller"));
    assertEquals(
        "DSSEIGLERJTMILLER", OccurrenceRelationships.normalizeID("D. S. Seigler & J. T. Miller"));
  }
}
