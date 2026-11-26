package org.gbif.pipelines.spark.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.spark.pojo.legacy.LegacyOccurrence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleRecordLoaderTest {

  private static SparkSession spark;
  private static final ObjectMapper MAPPER = MapperUtil.MAPPER_NON_EMPTY;

  @BeforeClass
  public static void setUp() {
    spark = SparkTestSession.create();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  private static ExtendedRecord verbatim() {
    return ExtendedRecord.newBuilder()
        .setId("r1")
        .setCoreTerms(Map.of("http://rs.tdwg.org/dwc/terms/occurrenceID", "occ-1"))
        .build();
  }

  private static IdentifierRecord identifier() {
    return IdentifierRecord.newBuilder().setId("r1").setInternalId("123").build();
  }

  private static LocationRecord location() {
    return LocationRecord.newBuilder()
        .setId("r1")
        .setCountryCode("DK")
        .setDecimalLatitude(55.7)
        .setDecimalLongitude(12.5)
        .build();
  }

  @Test
  public void readsLegacyJsonLayout() throws Exception {
    String path = Files.createTempDirectory("legacy-simple-occurrence").resolve("so").toString();
    LegacyOccurrence legacy =
        LegacyOccurrence.builder()
            .id("r1")
            .internalId("123")
            .verbatim(MAPPER.writeValueAsString(verbatim()))
            .identifier(MAPPER.writeValueAsString(identifier()))
            .location(MAPPER.writeValueAsString(location()))
            .build();
    spark
        .createDataset(List.of(legacy), Encoders.bean(LegacyOccurrence.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(path);

    assertTrue(SimpleRecordLoader.isLegacyJson(spark.read().parquet(path).schema()));

    Occurrence o = SimpleRecordLoader.loadOccurrences(spark, path).first();
    assertEquals("123", o.getInternalId());
    assertEquals(
        "occ-1", o.getVerbatim().getCoreTerms().get("http://rs.tdwg.org/dwc/terms/occurrenceID"));
    assertEquals("123", o.getIdentifier().getInternalId());
    assertEquals("DK", o.getLocation().getCountryCode());
    assertEquals(55.7, o.getLocation().getDecimalLatitude(), 0.0);
    assertNull(o.getBasic());
  }

  @Test
  public void readsTypedLayout() throws Exception {
    String path = Files.createTempDirectory("typed-simple-occurrence").resolve("so").toString();
    Occurrence typed =
        Occurrence.builder()
            .id("r1")
            .internalId("123")
            .verbatim(verbatim())
            .identifier(identifier())
            .location(location())
            .build();
    spark
        .createDataset(List.of(typed), Encoders.bean(Occurrence.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(path);

    assertFalse(SimpleRecordLoader.isLegacyJson(spark.read().parquet(path).schema()));

    Occurrence o = SimpleRecordLoader.loadOccurrences(spark, path).first();
    assertEquals(typed.getVerbatim(), o.getVerbatim());
    assertEquals(typed.getIdentifier(), o.getIdentifier());
    assertEquals(typed.getLocation(), o.getLocation());
  }
}
