package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackage;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DwcDpVerbatimConverterTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("DwcDpVerbatimConverterTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- resolveTermUri ----

  @Test
  void resolveTermUri_knownDwcTerm_returnsQualifiedUri() {
    assertEquals(DwcTerm.eventID.qualifiedName(), DwcDpVerbatimConverter.resolveTermUri("eventID"));
  }

  @Test
  void resolveTermUri_unknownTerm_returnsRawName() {
    // TermFactory creates an UnknownTerm for unrecognised names rather than returning null,
    // so we guard against that and fall back to the raw column name.
    String result = DwcDpVerbatimConverter.resolveTermUri("someUnknownField");
    assertEquals("someUnknownField", result);
  }

  // ---- rowToTermMap ----

  @Test
  void rowToTermMap_nullValuesOmitted() {
    Dataset<Row> ds =
        spark.createDataFrame(
            List.of(RowFactory.create("EVT001", null)), schema("eventID", "eventDate"));

    Row row = ds.collectAsList().get(0);
    Map<String, String> terms =
        DwcDpVerbatimConverter.rowToTermMap(row, new String[] {"eventID", "eventDate"});

    assertEquals(1, terms.size());
    assertEquals("EVT001", terms.get(DwcTerm.eventID.qualifiedName()));
    assertNull(terms.get(DwcTerm.eventDate.qualifiedName()));
  }

  // ---- event-core path ----

  @Test
  void eventCoreRecordsProducedFromEventTable(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(RowFactory.create("EVT001", "2024-01-15")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate");

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, r.getCoreRowType());
    assertEquals("2024-01-15", r.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));
    assertTrue(r.getExtensions().isEmpty());
  }

  @Test
  void occurrenceTableAttachedAsExtensionToEventCore(@TempDir Path dir) throws Exception {
    writeParquet(
        dir, "data/event.parquet", schema("eventID"), List.of(RowFactory.create("EVT001")));

    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "eventID", "scientificName"),
        List.of(RowFactory.create("OCC001", "EVT001", "Quercus robur")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> occExt =
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
    assertEquals(1, occExt.size());
    assertEquals("Quercus robur", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));
  }

  @Test
  void multipleOccurrencesPerEventAllAttached(@TempDir Path dir) throws Exception {
    writeParquet(
        dir, "data/event.parquet", schema("eventID"), List.of(RowFactory.create("EVT001")));

    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "eventID", "scientificName"),
        List.of(
            RowFactory.create("OCC001", "EVT001", "Quercus robur"),
            RowFactory.create("OCC002", "EVT001", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> occExt =
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
    assertEquals(2, occExt.size());
  }

  @Test
  void nullEventIdRowsAreSkipped(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(RowFactory.create("EVT001", "2024-01-15"), RowFactory.create(null, "2024-01-16")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate");

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    assertEquals("EVT001", records.get(0).getId());
  }

  @Test
  void mediaAttachedToEventViaEventMediaJoin(@TempDir Path dir) throws Exception {
    writeParquet(
        dir, "data/event.parquet", schema("eventID"), List.of(RowFactory.create("EVT001")));

    writeParquet(
        dir,
        "data/media.parquet",
        schema("mediaID", "accessURI", "mediaType"),
        List.of(RowFactory.create("MED001", "https://example.com/img.jpg", "StillImage")));

    writeParquet(
        dir,
        "data/event-media.parquet",
        schema("mediaID", "eventID"),
        List.of(RowFactory.create("MED001", "EVT001")));

    DataPackage dp = DataPackageFixtures.withEventAndMedia(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> mediaExt =
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_MULTIMEDIA);
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img.jpg",
        mediaExt.get(0).get(DwcDpVerbatimConverter.resolveTermUri("accessURI")));
  }

  // ---- occurrence-core path ----

  @Test
  void occurrenceCoreRecordsProducedFromOccurrenceTable(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "scientificName"),
        List.of(RowFactory.create("OCC001", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withOccurrence(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildOccurrenceCoreDataset(spark, dp, "file://" + dir)
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("OCC001", r.getId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_OCCURRENCE, r.getCoreRowType());
    assertEquals("Pinus sylvestris", r.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertTrue(r.getExtensions().isEmpty());
  }

  @Test
  void nullOccurrenceIdRowsAreSkipped(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "scientificName"),
        List.of(
            RowFactory.create("OCC001", "Pinus sylvestris"), RowFactory.create(null, "Unknown")));

    DataPackage dp = DataPackageFixtures.withOccurrence(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildOccurrenceCoreDataset(spark, dp, "file://" + dir)
            .collectAsList();

    assertEquals(1, records.size());
    assertEquals("OCC001", records.get(0).getId());
  }

  // ---- round-trip: write via Spark Avro, read back as IdentifiersPipeline would ----

  @Test
  void eventCoreRoundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate", "decimalLatitude", "decimalLongitude"),
        List.of(RowFactory.create("EVT001", "2024-06-15", "51.5", "-0.1")));

    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "eventID", "scientificName"),
        List.of(RowFactory.create("OCC001", "EVT001", "Quercus robur")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence(dir);

    // Write via Spark Avro — same as DwcDpVerbatimConverter.convert() does
    String verbatimPath = "file://" + dir + "/verbatim.avro";
    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .write()
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    // Read back exactly as IdentifiersPipeline / EventInterpretationPipeline does
    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(org.apache.spark.sql.Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);

    // id and coreRowType
    assertEquals("EVT001", r.getId());
    assertEquals("EVT001", r.getCoreId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, r.getCoreRowType());

    // core terms
    assertEquals("2024-06-15", r.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));
    assertEquals("51.5", r.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("-0.1", r.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));

    // occurrence extension present and populated
    List<Map<String, String>> occExt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt);
    assertEquals(1, occExt.size());
    assertEquals("Quercus robur", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("OCC001", occExt.get(0).get(DwcTerm.occurrenceID.qualifiedName()));
  }

  @Test
  void occurrenceCoreRoundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "scientificName", "decimalLatitude"),
        List.of(RowFactory.create("OCC001", "Pinus sylvestris", "60.2")));

    DataPackage dp = DataPackageFixtures.withOccurrence(dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    DwcDpVerbatimConverter.buildOccurrenceCoreDataset(spark, dp, "file://" + dir)
        .write()
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(org.apache.spark.sql.Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);

    assertEquals("OCC001", r.getId());
    assertEquals("OCC001", r.getCoreId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_OCCURRENCE, r.getCoreRowType());
    assertEquals("Pinus sylvestris", r.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("60.2", r.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertTrue(r.getExtensions().isEmpty());
  }

  @Test
  void eventCoreWithMediaRoundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(RowFactory.create("EVT001", "2024-06-15")));

    writeParquet(
        dir,
        "data/media.parquet",
        schema("mediaID", "accessURI", "mediaType"),
        List.of(RowFactory.create("MED001", "https://example.com/img.jpg", "StillImage")));

    writeParquet(
        dir,
        "data/event-media.parquet",
        schema("mediaID", "eventID"),
        List.of(RowFactory.create("MED001", "EVT001")));

    DataPackage dp = DataPackageFixtures.withEventAndMedia(dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .write()
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(org.apache.spark.sql.Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, r.getCoreRowType());

    // media extension populated via event-media join
    List<Map<String, String>> mediaExt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt);
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img.jpg",
        mediaExt.get(0).get(DwcDpVerbatimConverter.resolveTermUri("accessURI")));
    assertEquals(
        "StillImage", mediaExt.get(0).get(DwcDpVerbatimConverter.resolveTermUri("mediaType")));

    // no occurrence extension
    assertNull(r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE));
  }

  @Test
  void eventCoreWithOccurrenceAndMediaRoundTripAvroWriteAndRead(@TempDir Path dir)
      throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(RowFactory.create("EVT001", "2024-06-15")));

    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "eventID", "scientificName"),
        List.of(RowFactory.create("OCC001", "EVT001", "Quercus robur")));

    writeParquet(
        dir,
        "data/media.parquet",
        schema("mediaID", "accessURI", "mediaType"),
        List.of(RowFactory.create("MED001", "https://example.com/img.jpg", "StillImage")));

    writeParquet(
        dir,
        "data/event-media.parquet",
        schema("mediaID", "eventID"),
        List.of(RowFactory.create("MED001", "EVT001")));

    DataPackage dp = DataPackageFixtures.withEventOccurrenceAndMedia(dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .write()
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(org.apache.spark.sql.Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, r.getCoreRowType());

    // occurrence extension
    List<Map<String, String>> occExt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt);
    assertEquals(1, occExt.size());
    assertEquals("Quercus robur", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));

    // media extension via event-media join
    List<Map<String, String>> mediaExt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt);
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img.jpg",
        mediaExt.get(0).get(DwcDpVerbatimConverter.resolveTermUri("accessURI")));
  }

  @Test
  void occurrenceCoreWithMediaRoundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "scientificName"),
        List.of(RowFactory.create("OCC001", "Pinus sylvestris")));

    writeParquet(
        dir,
        "data/media.parquet",
        schema("mediaID", "accessURI", "mediaType"),
        List.of(
            RowFactory.create("MED001", "https://example.com/img1.jpg", "StillImage"),
            RowFactory.create("MED002", "https://example.com/img2.jpg", "StillImage")));

    writeParquet(
        dir,
        "data/occurrence-media.parquet",
        schema("mediaID", "occurrenceID"),
        List.of(RowFactory.create("MED001", "OCC001"), RowFactory.create("MED002", "OCC001")));

    DataPackage dp = DataPackageFixtures.withOccurrenceAndMedia(dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    DwcDpVerbatimConverter.buildOccurrenceCoreDataset(spark, dp, "file://" + dir)
        .write()
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(org.apache.spark.sql.Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("OCC001", r.getId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_OCCURRENCE, r.getCoreRowType());
    assertEquals("Pinus sylvestris", r.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));

    // two media records attached via occurrence-media join
    List<Map<String, String>> mediaExt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt);
    assertEquals(2, mediaExt.size());

    List<String> uris =
        mediaExt.stream()
            .map(m -> m.get(DwcDpVerbatimConverter.resolveTermUri("accessURI")))
            .sorted()
            .toList();
    assertEquals("https://example.com/img1.jpg", uris.get(0));
    assertEquals("https://example.com/img2.jpg", uris.get(1));
  }

  // ---- helpers ----

  private void writeParquet(Path dir, String relativePath, StructType schema, List<Row> rows) {
    spark.createDataFrame(rows, schema).write().parquet("file://" + dir.resolve(relativePath));
  }

  private static StructType schema(String... names) {
    StructField[] fields = new StructField[names.length];
    for (int i = 0; i < names.length; i++) {
      fields[i] = DataTypes.createStructField(names[i], DataTypes.StringType, true);
    }
    return DataTypes.createStructType(fields);
  }
}
