package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
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
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    // Read back exactly as IdentifiersPipeline / EventInterpretationPipeline does
    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);

    // id and coreRowType
    assertEquals("EVT001", r.getId());
    assertEquals(null, r.getCoreId());
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
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);

    assertEquals("OCC001", r.getId());
    assertNull(r.getCoreId());
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
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
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
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
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
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
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

  // ---- assertion (eMoF) extension ----

  @Test
  void eventAssertionFieldsRemappedToEmofTermsInExtension(@TempDir Path dir) throws Exception {
    // event table uses internal surrogate PK; eventID is the natural DwC identifier
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    // assertion links via event_fk (surrogate), not via eventID
    writeParquet(
        dir,
        "data/event-assertion.parquet",
        schema("assertionID", "event_fk", "assertionType", "assertionValue", "assertionUnit"),
        List.of(RowFactory.create("A001", "EPK-001", "Temperature", "25.0", "Celsius")));

    DataPackage dp = DataPackageFixtures.withEventAndAssertion(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());

    List<Map<String, String>> emof =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension should be present");
    assertEquals(1, emof.size());

    Map<String, String> row = emof.get(0);
    // DP assertion fields must be stored under their DwC-A eMoF term URIs
    assertEquals("A001", row.get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Temperature", row.get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("25.0", row.get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("Celsius", row.get(DwcTerm.measurementUnit.qualifiedName()));

    // raw DP column names must NOT appear in the extension
    assertFalse(row.containsKey("assertionType"), "raw assertionType must not appear");
    assertFalse(row.containsKey("assertionValue"), "raw assertionValue must not appear");
    assertFalse(row.containsKey("event_fk"), "internal FK must not appear");
  }

  @Test
  void occurrenceAssertionFieldsRemappedToEmofTermsInExtension(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrence_pk", "occurrenceID", "scientificName"),
        List.of(RowFactory.create("OPK-001", "OCC001", "Quercus robur")));

    writeParquet(
        dir,
        "data/occurrence-assertion.parquet",
        schema("assertionID", "occurrence_fk", "assertionType", "assertionValue", "assertionUnit"),
        List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    DataPackage dp = DataPackageFixtures.withOccurrenceAndAssertion(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildOccurrenceCoreDataset(spark, dp, "file://" + dir)
            .collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("OCC001", r.getId());

    List<Map<String, String>> emof =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof);
    assertEquals(1, emof.size());

    Map<String, String> row = emof.get(0);
    assertEquals("A001", row.get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Mass", row.get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("3.2", row.get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("g", row.get(DwcTerm.measurementUnit.qualifiedName()));
    assertFalse(row.containsKey("occurrence_fk"), "internal FK must not appear");
  }

  @Test
  void assertionProtocolFkResolvedToMeasurementMethodViaProtocolTable(@TempDir Path dir)
      throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    writeParquet(
        dir,
        "data/event-assertion.parquet",
        schema(
            "assertionID", "event_fk", "assertionType", "assertionValue", "assertionProtocol_fk"),
        List.of(RowFactory.create("A001", "EPK-001", "Count", "42", "PROTO-001")));

    writeParquet(
        dir,
        "data/protocol.parquet",
        schema("protocol_pk", "protocolDescription"),
        List.of(RowFactory.create("PROTO-001", "Point count survey")));

    DataPackage dp = DataPackageFixtures.withEventAssertionAndProtocol(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> emof =
        records
            .get(0)
            .getExtensions()
            .get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof);

    Map<String, String> row = emof.get(0);
    // protocol description, not the raw FK value
    assertEquals("Point count survey", row.get(DwcTerm.measurementMethod.qualifiedName()));
    assertFalse(row.containsKey("assertionProtocol_fk"), "protocol FK must not appear");
  }

  @Test
  void assertionProtocolFkUsedAsFallbackMeasurementMethodWhenProtocolTableAbsent(@TempDir Path dir)
      throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    // Parquet includes assertionProtocol_fk; fixture does not declare a protocol table
    writeParquet(
        dir,
        "data/event-assertion.parquet",
        schema("assertionID", "event_fk", "assertionType", "assertionProtocol_fk"),
        List.of(RowFactory.create("A001", "EPK-001", "Count", "PROTO-001")));

    DataPackage dp = DataPackageFixtures.withEventAndAssertion(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> emof =
        records
            .get(0)
            .getExtensions()
            .get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof);

    Map<String, String> row = emof.get(0);
    // raw FK value is kept as measurementMethod when no protocol table is available
    assertEquals("PROTO-001", row.get(DwcTerm.measurementMethod.qualifiedName()));
  }

  // ---- Humboldt Ecological Inventory Extension ----

  @Test
  void surveyFieldsAttachedAsHumboldtExtensionToEventCore(@TempDir Path dir) throws Exception {
    // event table uses internal surrogate PK; eventID is the natural DwC identifier
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    // survey links via event_fk (surrogate); siteCount and reportedWeather are Humboldt terms
    writeParquet(
        dir,
        "data/survey.parquet",
        schema("survey_pk", "event_fk", "siteCount", "reportedWeather"),
        List.of(RowFactory.create("SPK-001", "EPK-001", "3", "Clear")));

    DataPackage dp = DataPackageFixtures.withEventAndSurvey(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());

    List<Map<String, String>> humboldt =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(humboldt, "Humboldt extension should be present");
    assertEquals(1, humboldt.size());

    Map<String, String> hRow = humboldt.get(0);
    // Survey field names match Humboldt term names directly
    assertEquals("3", hRow.get(DwcDpVerbatimConverter.resolveTermUri("siteCount")));
    assertEquals("Clear", hRow.get(DwcDpVerbatimConverter.resolveTermUri("reportedWeather")));

    // Internal FK/PK columns must NOT appear in the extension
    assertFalse(hRow.containsKey("event_fk"), "event_fk must not appear");
    assertFalse(hRow.containsKey("survey_pk"), "survey_pk must not appear");
  }

  @Test
  void surveyTargetsFanOutToSeparateHumboldtRowsPerTarget(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    writeParquet(
        dir,
        "data/survey.parquet",
        schema("survey_pk", "event_fk", "siteCount"),
        List.of(RowFactory.create("SPK-001", "EPK-001", "5")));

    // Two survey-target rows linked to the same survey → expect 2 Humboldt extension rows
    writeParquet(
        dir,
        "data/survey-target.parquet",
        schema("surveyTarget_pk", "surveyTargetDescription"),
        List.of(
            RowFactory.create("STP-001", "All birds"),
            RowFactory.create("STP-002", "All mammals")));

    writeParquet(
        dir,
        "data/survey-survey-target.parquet",
        schema("survey_fk", "surveyTarget_fk"),
        List.of(
            RowFactory.create("SPK-001", "STP-001"),
            RowFactory.create("SPK-001", "STP-002")));

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> humboldt =
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(humboldt);
    // One Humboldt row per survey-target
    assertEquals(2, humboldt.size());

    // Both rows carry the survey-level siteCount
    humboldt.forEach(
        hRow -> assertEquals("5", hRow.get(DwcDpVerbatimConverter.resolveTermUri("siteCount"))));

    // Each row carries one survey-target description
    List<String> descriptions =
        humboldt.stream()
            .map(hRow -> hRow.get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")))
            .sorted()
            .toList();
    assertEquals("All birds", descriptions.get(0));
    assertEquals("All mammals", descriptions.get(1));

    // Junction/PK columns must not appear
    humboldt.forEach(
        hRow -> {
          assertFalse(hRow.containsKey("survey_fk"));
          assertFalse(hRow.containsKey("surveyTarget_pk"));
          assertFalse(hRow.containsKey("survey_pk"));
        });
  }

  @Test
  void surveyWithNoLinkedTargetsProducesOneHumboldtRowWithSurveyFieldsOnly(@TempDir Path dir)
      throws Exception {
    // Survey-target tables ARE declared in the DataPackage, but no junction entry links
    // SPK-001 to any target. The left-outer join must still produce one Humboldt row
    // carrying the survey-level fields (siteCount) without any survey-target fields.
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    writeParquet(
        dir,
        "data/survey.parquet",
        schema("survey_pk", "event_fk", "siteCount"),
        List.of(RowFactory.create("SPK-001", "EPK-001", "7")));

    // A survey-target exists but is NOT linked to SPK-001
    writeParquet(
        dir,
        "data/survey-target.parquet",
        schema("surveyTarget_pk", "surveyTargetDescription"),
        List.of(RowFactory.create("STP-001", "All birds")));

    // Junction links a *different* survey key — SPK-001 has no junction rows
    writeParquet(
        dir,
        "data/survey-survey-target.parquet",
        schema("survey_fk", "surveyTarget_fk"),
        List.of(RowFactory.create("SPK-OTHER", "STP-001")));

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> humboldt =
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(humboldt);
    // One row even though no target is linked — the survey itself is the source
    assertEquals(1, humboldt.size());

    Map<String, String> hRow = humboldt.get(0);
    assertEquals("7", hRow.get(DwcDpVerbatimConverter.resolveTermUri("siteCount")));
    // No survey-target fields: null values are omitted by rowToTermMap
    assertNull(hRow.get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")));
  }

  @Test
  void mixedSurveys_someWithTargetsSomeWithout_produceCorrectHumboldtRowCounts(
      @TempDir Path dir) throws Exception {
    // Two events: EVT001's survey has 2 linked targets (→ 2 Humboldt rows),
    //             EVT002's survey has no linked targets (→ 1 Humboldt row).
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(
            RowFactory.create("EPK-001", "EVT001"),
            RowFactory.create("EPK-002", "EVT002")));

    writeParquet(
        dir,
        "data/survey.parquet",
        schema("survey_pk", "event_fk", "siteCount"),
        List.of(
            RowFactory.create("SPK-001", "EPK-001", "5"),
            RowFactory.create("SPK-002", "EPK-002", "2")));

    writeParquet(
        dir,
        "data/survey-target.parquet",
        schema("surveyTarget_pk", "surveyTargetDescription"),
        List.of(
            RowFactory.create("STP-001", "All birds"),
            RowFactory.create("STP-002", "All mammals")));

    // Only SPK-001 is linked to targets; SPK-002 has no junction entries
    writeParquet(
        dir,
        "data/survey-survey-target.parquet",
        schema("survey_fk", "surveyTarget_fk"),
        List.of(
            RowFactory.create("SPK-001", "STP-001"),
            RowFactory.create("SPK-001", "STP-002")));

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget(dir);

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(2, records.size());

    Map<String, List<ExtendedRecord>> byId =
        records.stream().collect(java.util.stream.Collectors.groupingBy(ExtendedRecord::getId));

    // EVT001: 2 survey-targets → 2 Humboldt rows, each with siteCount=5
    List<Map<String, String>> hEVT001 =
        byId.get("EVT001").get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(hEVT001);
    assertEquals(2, hEVT001.size());
    hEVT001.forEach(
        hRow -> assertEquals("5", hRow.get(DwcDpVerbatimConverter.resolveTermUri("siteCount"))));
    List<String> descriptions =
        hEVT001.stream()
            .map(hRow -> hRow.get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")))
            .sorted()
            .toList();
    assertEquals("All birds", descriptions.get(0));
    assertEquals("All mammals", descriptions.get(1));

    // EVT002: no survey-targets → 1 Humboldt row with survey fields only
    List<Map<String, String>> hEVT002 =
        byId.get("EVT002").get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(hEVT002);
    assertEquals(1, hEVT002.size());
    assertEquals("2", hEVT002.get(0).get(DwcDpVerbatimConverter.resolveTermUri("siteCount")));
    assertNull(hEVT002.get(0).get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")));
  }

  @Test
  void humboldtExtensionAbsentWhenNoSurveyTable(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    // DataPackage has no survey table
    DataPackage dp = DataPackageFixtures.withEvent(dir, "event_pk", "eventID");

    List<ExtendedRecord> records =
        DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir).collectAsList();

    assertEquals(1, records.size());
    assertNull(
        records.get(0).getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT),
        "Humboldt extension must be absent when survey table is not in the DataPackage");
  }

  // ---- avro output structure ----

  @Test
  void avroWrite_coalesceOne_producesSinglePartFile(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(
            RowFactory.create("EVT001", "2024-06-15"), RowFactory.create("EVT002", "2024-06-16")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate");

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(partsPath);

    // coalesce(1) always produces a directory — confirm exactly one part file inside
    File partsDir = dir.resolve("verbatim.avro.parts").toFile();
    assertTrue(partsDir.isDirectory());

    File[] avroFiles = partsDir.listFiles(f -> f.getName().endsWith(".avro"));
    assertNotNull(avroFiles);
    assertEquals(
        1,
        avroFiles.length,
        "Expected exactly one .avro part file after coalesce(1), found: "
            + Arrays.toString(avroFiles));
  }

  @Test
  void mergeToSingleFile_producesLiteralAvroFile(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(
            RowFactory.create("EVT001", "2024-06-15"), RowFactory.create("EVT002", "2024-06-16")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate");

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String targetPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());

    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, targetPath);

    // temp parts dir should be gone
    assertFalse(
        dir.resolve("verbatim.avro.parts").toFile().exists(),
        "temp parts directory should have been deleted");

    // verbatim.avro should be a literal file, not a directory
    File verbatimFile = dir.resolve("verbatim.avro").toFile();
    assertTrue(verbatimFile.exists(), "verbatim.avro should exist");
    assertFalse(verbatimFile.isDirectory(), "verbatim.avro should be a file, not a directory");
    assertTrue(verbatimFile.getName().endsWith(".avro"));

    // and it should still be readable by Spark
    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(targetPath)
            .as(Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(2, records.size());
    List<String> ids = records.stream().map(ExtendedRecord::getId).sorted().toList();
    assertEquals("EVT001", ids.get(0));
    assertEquals("EVT002", ids.get(1));
  }

  // ---- writeMetrics ----

  @Test
  void writeMetrics_writesExpectedCountsAsUtf16Yaml(@TempDir Path dir) throws Exception {
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

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String datasetBasePath = "file://" + dir;
    DwcDpVerbatimConverter.writeMetrics(spark, dp, datasetBasePath, fs, "test-dataset");

    org.apache.hadoop.fs.Path metricsPath =
        new org.apache.hadoop.fs.Path(datasetBasePath + "/archive-to-verbatim.yml");
    assertTrue(fs.exists(metricsPath), "archive-to-verbatim.yml should have been written");

    // Legacy format: written as UTF-16, but readers treat it as UTF-8 and strip the resulting
    // \u0000 bytes interleaved between ASCII characters. Mirrors HdfsUtils.getValueByKey.
    String content;
    try (var reader =
        new BufferedReader(new InputStreamReader(fs.open(metricsPath), StandardCharsets.UTF_8))) {
      content =
          reader.lines().map(line -> line.replace("\u0000", "")).collect(Collectors.joining("\n"));
    }

    Map<String, Object> yaml = new org.yaml.snakeyaml.Yaml().load(content);

    // erCount is always 0 for DwC-DP
    assertEquals(0, ((Number) yaml.get(Metrics.ARCHIVE_TO_ER_COUNT)).longValue());
    // 2 occurrence rows in the data package
    assertEquals(2, ((Number) yaml.get(Metrics.ARCHIVE_TO_OCC_COUNT)).longValue());
    // 1 event row in the data package
    assertEquals(1, ((Number) yaml.get(Metrics.EVENT_CORE_RECORDS_COUNT)).longValue());
    // largest table is occurrence (2 rows) vs event (1 row)
    assertEquals(2, ((Number) yaml.get(Metrics.ARCHIVE_TO_LARGEST_FILE_COUNT)).longValue());
  }

  @Test
  void writeMetrics_eventOnly_occurrenceCountIsZero(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate"),
        List.of(
            RowFactory.create("EVT001", "2024-06-15"),
            RowFactory.create("EVT002", "2024-06-16"),
            RowFactory.create("EVT003", "2024-06-17")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate");

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String datasetBasePath = "file://" + dir;
    var metrics =
        DwcDpVerbatimConverter.writeMetrics(spark, dp, datasetBasePath, fs, "test-dataset");

    assertEquals(0L, metrics.erCount());
    assertEquals(0L, metrics.occurrenceCount());
    assertEquals(3L, metrics.eventCount());
    assertEquals(3L, metrics.largestFileCount());
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
