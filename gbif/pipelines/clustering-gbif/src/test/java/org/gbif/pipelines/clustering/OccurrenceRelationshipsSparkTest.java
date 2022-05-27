package org.gbif.pipelines.clustering;

import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.APPROXIMATE_DATE;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.SAME_ACCEPTED_SPECIES;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.SAME_COUNTRY;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.SAME_DATE;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.SAME_RECORDER_NAME;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.SAME_SPECIMEN;
import static org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion.FeatureAssertion.WITHIN_200m;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;
import org.junit.Test;

/**
 * Tests for relationship assertions when using Spark Row as the source. Further tests should be
 * added to the OccurrenceRelationshipsTest and only spark specific tests added here.
 */
public class OccurrenceRelationshipsSparkTest extends BaseSparkTest {

  // Schema mirrors the production occurrence HDFS view of GBIF with the exception of taxa keys
  // being String
  // due to https://github.com/gbif/pipelines/issues/484 to allow for GBIF/ALA use.
  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("gbifId", DataTypes.LongType, true),
            DataTypes.createStructField("datasetKey", DataTypes.StringType, true),
            DataTypes.createStructField("basisOfRecord", DataTypes.StringType, true),
            DataTypes.createStructField("publishingoOrgKey", DataTypes.StringType, true),
            DataTypes.createStructField("datasetName", DataTypes.StringType, true),
            DataTypes.createStructField("publisher", DataTypes.StringType, true),
            DataTypes.createStructField("kingdomKey", DataTypes.StringType, true),
            DataTypes.createStructField("phylumKey", DataTypes.StringType, true),
            DataTypes.createStructField("classKey", DataTypes.StringType, true),
            DataTypes.createStructField("orderKey", DataTypes.StringType, true),
            DataTypes.createStructField("familyKey", DataTypes.StringType, true),
            DataTypes.createStructField("genusKey", DataTypes.StringType, true),
            DataTypes.createStructField("speciesKey", DataTypes.StringType, true),
            DataTypes.createStructField("acceptedTaxonKey", DataTypes.StringType, true),
            DataTypes.createStructField("taxonKey", DataTypes.StringType, true),
            DataTypes.createStructField("scientificName", DataTypes.StringType, true),
            DataTypes.createStructField("acceptedScientificName", DataTypes.StringType, true),
            DataTypes.createStructField("kingdom", DataTypes.StringType, true),
            DataTypes.createStructField("phylum", DataTypes.StringType, true),
            DataTypes.createStructField("order", DataTypes.StringType, true),
            DataTypes.createStructField("family", DataTypes.StringType, true),
            DataTypes.createStructField("genus", DataTypes.StringType, true),
            DataTypes.createStructField("species", DataTypes.StringType, true),
            DataTypes.createStructField("genericName", DataTypes.StringType, true),
            DataTypes.createStructField("specificEpithet", DataTypes.StringType, true),
            DataTypes.createStructField("taxonRank", DataTypes.StringType, true),
            DataTypes.createStructField(
                "typeStatus", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField("preparations", DataTypes.StringType, true),
            DataTypes.createStructField("decimalLatitude", DataTypes.DoubleType, true),
            DataTypes.createStructField("decimalLongitude", DataTypes.DoubleType, true),
            DataTypes.createStructField("countryCode", DataTypes.StringType, true),
            DataTypes.createStructField("year", DataTypes.IntegerType, true),
            DataTypes.createStructField("month", DataTypes.IntegerType, true),
            DataTypes.createStructField("day", DataTypes.IntegerType, true),
            DataTypes.createStructField("eventDate", DataTypes.StringType, true),
            DataTypes.createStructField("recordNumber", DataTypes.StringType, true),
            DataTypes.createStructField("fieldNumber", DataTypes.StringType, true),
            DataTypes.createStructField("occurrenceID", DataTypes.StringType, true),
            DataTypes.createStructField(
                "otherCatalogNumbers", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField("institutionCode", DataTypes.StringType, true),
            DataTypes.createStructField("collectionCode", DataTypes.StringType, true),
            DataTypes.createStructField("catalogNumber", DataTypes.StringType, true),
            DataTypes.createStructField(
                "recordedBy", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField(
                "recordedByID", DataTypes.createArrayType(DataTypes.StringType), true),
          });

  @Test
  public void testSimpleAssertions() {
    OccurrenceFeatures o1 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("occurrenceID", "1")
                .with("speciesKey", "1")
                .with("decimalLatitude", 44.0d)
                .with("decimalLongitude", 44.0d)
                .with("catalogNumber", "TIM1")
                .with("year", 1978)
                .with("month", 12)
                .with("day", 21)
                .buildWithSchema());

    OccurrenceFeatures o2 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("occurrenceID", "2")
                .with("speciesKey", "1")
                .with("decimalLatitude", 44.0d)
                .with("decimalLongitude", 44.0d)
                .with("catalogNumber", "//TIM1")
                .with("year", 1978)
                .with("month", 12)
                .with("day", 21)
                .buildWithSchema());

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);

    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_ACCEPTED_SPECIES));
  }

  /** Real data from records 2332470913, 2571156410 which should cluster. */
  @Test
  public void testCortinarius() {
    OccurrenceFeatures o1 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("occurrenceID", "urn:catalog:O:F:304835")
                .with("recordNumber", "TEB 12-16")
                .with("speciesKey", "3348943")
                .with("decimalLatitude", 60.3302d)
                .with("decimalLongitude", 10.4647d)
                .with("catalogNumber", "304835")
                .with("year", 2016)
                .with("month", 6)
                .with("day", 11)
                .with("eventDate", "2016-06-11T00:00:00")
                .buildWithSchema());

    OccurrenceFeatures o2 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("occurrenceID", "urn:uuid:152ce614-69e1-4fbe-8f1c-3340d0a15491")
                .with("speciesKey", "3348943")
                .with("decimalLatitude", 60.330181d)
                .with("decimalLongitude", 10.464743d)
                .with("catalogNumber", "O-DFL-6644/2-D")
                .with("recordNumber", "TEB 12-16")
                .with("year", 2016)
                .with("month", 6)
                .with("day", 11)
                .with("eventDate", "2016-06-11T00:00:00")
                .buildWithSchema());

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);

    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_ACCEPTED_SPECIES));
  }

  // Test even with nonsense a Holotype of the same name must be the same specimen (or worth
  // investigating a data issue)
  @Test
  public void testHolotype() {
    OccurrenceFeatures o1 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("taxonKey", "3350984")
                .with("decimalLatitude", 10d)
                .with("decimalLongitude", 10d)
                .with("countryCode", "DK")
                .with("typeStatus", Lists.newArrayList("HoloType"))
                .buildWithSchema());

    OccurrenceFeatures o2 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("taxonKey", "3350984")
                .with("decimalLatitude", 20d) // different
                .with("decimalLongitude", 20d) // different
                .with("countryCode", "NO") // different
                .with("typeStatus", Lists.newArrayList("HoloType"))
                .buildWithSchema());

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_SPECIMEN));
  }

  // Test that two records with same collector, approximate location but a day apart match.
  // https://github.com/gbif/occurrence/issues/177
  @Test
  public void testDayApart() {
    // real records where a trap set one evening and visited the next day is shared twice using
    // different
    // days
    OccurrenceFeatures o1 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("gbifId", 49635968)
                .with("speciesKey", "1850114")
                .with("decimalLatitude", 55.737d)
                .with("decimalLongitude", 12.538d)
                .with("year", 2004)
                .with("month", 8)
                .with("day", 1) // day trap set
                .with("countryCode", "DK")
                .with("recordedBy", Lists.newArrayList("Donald Hobern"))
                .buildWithSchema());

    OccurrenceFeatures o2 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("gbifId", 1227719129)
                .with("speciesKey", "1850114")
                .with("decimalLatitude", 55.736932d) // different
                .with("decimalLongitude", 12.538104d)
                .with("year", 2004)
                .with("month", 8)
                .with("day", 2) // day collected
                .with("countryCode", "DK")
                .with("recordedBy", Lists.newArrayList("Donald Hobern"))
                .buildWithSchema());

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(
        assertion.justificationContainsAll(
            APPROXIMATE_DATE, WITHIN_200m, SAME_COUNTRY, SAME_RECORDER_NAME));
  }

  // test 3 decimal place rounding example clusters
  @Test
  public void test3DP() {
    // real records of Seigler & Miller
    OccurrenceFeatures o1 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("gbifId", 1675790844)
                .with("speciesKey", "3794925")
                .with("decimalLatitude", 21.8656d)
                .with("decimalLongitude", -102.909d)
                .with("year", 2007)
                .with("month", 5)
                .with("day", 26)
                .with("recordedBy", Lists.newArrayList("D. S. Seigler", "J. T. Miller"))
                .buildWithSchema());

    OccurrenceFeatures o2 =
        new RowOccurrenceFeatures(
            new RowBuilder()
                .with("gbifId", 2268858676L)
                .with("speciesKey", "3794925")
                .with("decimalLatitude", 21.86558d)
                .with("decimalLongitude", -102.90929d)
                .with("year", 2007)
                .with("month", 5)
                .with("day", 26)
                .with(
                    "recordedBy", Lists.newArrayList("Seigler", "J. T. Miller")) // Miller overlaps
                .buildWithSchema());

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);
    assertNotNull(assertion);
    assertTrue(
        assertion.justificationContainsAll(
            SAME_DATE, WITHIN_200m, SAME_ACCEPTED_SPECIES, SAME_RECORDER_NAME));
  }

  @Test
  public void testNormaliseID() {
    assertEquals("ABC", OccurrenceRelationships.normalizeID(" A-/, B \\C"));
    assertEquals(
        "DAVIDSSEIGLERJTMILLER",
        OccurrenceRelationships.normalizeID("David S. Seigler|J.T. Miller"));
    assertEquals(
        "DSSEIGLERJTMILLER", OccurrenceRelationships.normalizeID("D. S. Seigler & J. T. Miller"));
  }

  /** Test to simply verify that a Spark dataset row operates the same as the isolated Row tests. */
  @Test
  public void testWithSpark() {
    List<Row> rows =
        Arrays.asList(
            new RowBuilder()
                .with("occurrenceID", "1")
                .with("speciesKey", "1")
                .with("decimalLatitude", 44.0d)
                .with("decimalLongitude", 44.0d)
                .with("catalogNumber", "TIM1")
                .with("year", 1978)
                .with("month", 12)
                .with("day", 21)
                .buildSchemaless(),
            new RowBuilder()
                .with("occurrenceID", "2")
                .with("speciesKey", "1")
                .with("decimalLatitude", 44.0d)
                .with("decimalLongitude", 44.0d)
                .with("catalogNumber", "//TIM1")
                .with("year", 1978)
                .with("month", 12)
                .with("day", 21)
                .buildSchemaless());
    final Dataset<Row> data = sqlContext.createDataFrame(rows, SCHEMA);
    List<Row> rowData = data.collectAsList();

    OccurrenceFeatures o1 = new RowOccurrenceFeatures(rowData.get(0));
    OccurrenceFeatures o2 = new RowOccurrenceFeatures(rowData.get(1));

    RelationshipAssertion<OccurrenceFeatures> assertion = OccurrenceRelationships.generate(o1, o2);

    assertNotNull(assertion);
    assertTrue(assertion.justificationContains(SAME_ACCEPTED_SPECIES));
  }

  /**
   * Utility builder of rows. Rows will adhere to the schema but may be constructed with or without.
   */
  private static class RowBuilder {
    private final Object[] values = new Object[SCHEMA.fieldNames().length];

    private RowBuilder with(String field, Object value) {
      values[Arrays.asList(SCHEMA.fieldNames()).indexOf(field)] = value;
      return this;
    }

    private Row buildSchemaless() {
      return RowFactory.create(values);
    }

    private Row buildWithSchema() {
      return new GenericRowWithSchema(values, SCHEMA);
    }
  }
}
