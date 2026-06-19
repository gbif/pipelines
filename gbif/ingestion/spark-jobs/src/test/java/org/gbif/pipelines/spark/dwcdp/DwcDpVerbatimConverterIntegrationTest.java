package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.DataPackage;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests verifying that verbatim.avro written by {@link DwcDpVerbatimConverter}:
 *
 * <ol>
 *   <li>Uses the correct Avro schema (no union-wrapping of plain string fields)
 *   <li>Is readable by downstream pipelines exactly as they read it
 *   <li>Produces occurrence records that OccurrenceExtensionConverter can extract
 * </ol>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DwcDpVerbatimConverterIntegrationTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder()
            .appName("DwcDpVerbatimConverterIntegrationTest")
            .getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- avro schema compliance ----

  @Test
  void avroWrite_schemaCompliant_coreTermsAreNotUnionWrapped(@TempDir Path dir) throws Exception {
    org.apache.avro.Schema schema =
        new org.apache.avro.Schema.Parser()
            .parse(DwcDpVerbatimConverter.extendedRecordSchemaJson());

    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate", "decimalLatitude"),
        List.of(RowFactory.create("EVT001", "2024-06-15", "59.0")));

    DataPackage dp = DataPackageFixtures.withEvent(dir, "eventID", "eventDate", "decimalLatitude");

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String targetPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, targetPath);

    File avroFile = dir.resolve("verbatim.avro").toFile();
    assertTrue(
        avroFile.exists() && !avroFile.isDirectory(), "verbatim.avro should be a single file");

    org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> reader =
        new org.apache.avro.file.DataFileReader<>(
            avroFile, new org.apache.avro.generic.GenericDatumReader<>(schema));

    assertTrue(reader.hasNext(), "Should have at least one record");
    org.apache.avro.generic.GenericRecord record = reader.next();
    reader.close();

    String prettyJson =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writerWithDefaultPrettyPrinter()
            .writeValueAsString(
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(record.toString()));
    log.info("=== ExtendedRecord JSON (via GenericDatumReader with explicit schema) ===");
    log.info(prettyJson);
    log.info("=== end ===");

    Object id = record.get("id");
    assertNotNull(id);
    assertInstanceOf(
        org.apache.avro.util.Utf8.class,
        id,
        "id should be plain Avro Utf8 string, not union-wrapped. Got: "
            + id.getClass().getName()
            + " = "
            + id);
    assertEquals("EVT001", id.toString());

    Object coreRowType = record.get("coreRowType");
    assertNotNull(coreRowType);
    assertInstanceOf(
        org.apache.avro.util.Utf8.class,
        coreRowType,
        "coreRowType should be plain Avro Utf8 string. Got: "
            + coreRowType.getClass().getName()
            + " = "
            + coreRowType);
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, coreRowType.toString());

    @SuppressWarnings("unchecked")
    java.util.Map<Object, Object> coreTerms =
        (java.util.Map<Object, Object>) record.get("coreTerms");
    assertNotNull(coreTerms, "coreTerms should not be null");

    Object eventDateValue =
        coreTerms.get(new org.apache.avro.util.Utf8(DwcTerm.eventDate.qualifiedName()));
    assertNotNull(eventDateValue, "coreTerms should contain eventDate");
    assertInstanceOf(
        org.apache.avro.util.Utf8.class,
        eventDateValue,
        "coreTerms values should be plain Avro Utf8 strings, not union-wrapped. Got: "
            + eventDateValue.getClass().getName()
            + " = "
            + eventDateValue);
    assertEquals("2024-06-15", eventDateValue.toString());

    assertNull(record.get("coreId"), "coreId should be null for a core record");
  }

  // ---- downstream pipeline compatibility ----

  @Test
  void avroWrite_readableByEventInterpretationPipeline(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("eventID", "eventDate", "parentEventID", "decimalLatitude"),
        List.of(
            RowFactory.create("EVT001", "2024-06-15", null, "59.0"),
            RowFactory.create("EVT002", "2024-06-16", "EVT001", "59.1")));

    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "eventID", "scientificName"),
        List.of(
            RowFactory.create("OCC001", "EVT001", "Quercus robur"),
            RowFactory.create("OCC002", "EVT002", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence(dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, verbatimPath);

    Dataset<ExtendedRecord> loaded =
        spark.read().format("avro").load(verbatimPath).as(Encoders.bean(ExtendedRecord.class));

    Dataset<ExtendedRecord> filtered =
        loaded.filter(
            (FilterFunction<ExtendedRecord>) er -> er != null && !er.getCoreTerms().isEmpty());

    assertEquals(2, filtered.count(), "Both event records should survive the filter");

    List<ExtendedRecord> records = filtered.collectAsList();
    records.sort(Comparator.comparing(ExtendedRecord::getId));

    ExtendedRecord first = records.get(0);
    log.info("First record id={} coreRowType={}", first.getId(), first.getCoreRowType());
    log.info("coreTerms keys: {}", first.getCoreTerms().keySet());
    log.info("extensions keys: {}", first.getExtensions().keySet());

    assertEquals("EVT001", first.getId());
    assertNull(first.getCoreId(), "coreId should be null until set by downstream pipeline");

    String eventDate = first.getCoreTerms().get(DwcTerm.eventDate.qualifiedName());
    assertNotNull(eventDate, "eventDate should be accessible as a plain String");
    assertEquals("2024-06-15", eventDate);

    List<Map<String, String>> occExt =
        first.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt);
    assertEquals(1, occExt.size());

    log.info("Occurrence extension entry keys: {}", occExt.get(0).keySet());
    log.info(
        "occurrenceID value type: {}",
        occExt.get(0).get(DwcTerm.occurrenceID.qualifiedName()) != null
            ? occExt.get(0).get(DwcTerm.occurrenceID.qualifiedName()).getClass().getName()
            : "null");
    log.info("occurrenceID value: {}", occExt.get(0).get(DwcTerm.occurrenceID.qualifiedName()));
    log.info("scientificName value: {}", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));

    assertEquals("Quercus robur", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));

    Dataset<Row> eventIds =
        loaded.select(
            loaded.col("coreTerms.`" + DwcTerm.eventID.qualifiedName() + "`").alias("eventID"),
            loaded
                .col("coreTerms.`" + DwcTerm.parentEventID.qualifiedName() + "`")
                .alias("parentEventID"));

    List<Row> idRows = eventIds.orderBy("eventID").collectAsList();
    assertEquals(2, idRows.size());
    assertEquals("EVT001", idRows.get(0).getString(0));
    assertNull(idRows.get(0).getString(1));
    assertEquals("EVT002", idRows.get(1).getString(0));
    assertEquals("EVT001", idRows.get(1).getString(1));

    List<ExtendedRecord> rebuilt =
        filtered
            .map(
                (MapFunction<ExtendedRecord, ExtendedRecord>)
                    er ->
                        ExtendedRecord.newBuilder()
                            .setId(er.getId())
                            .setCoreId(er.getId())
                            .setCoreRowType(er.getCoreRowType())
                            .setCoreTerms(er.getCoreTerms())
                            .setExtensions(er.getExtensions())
                            .build(),
                Encoders.bean(ExtendedRecord.class))
            .collectAsList();

    assertEquals(2, rebuilt.size());
    rebuilt.sort(Comparator.comparing(ExtendedRecord::getId));
    assertEquals("EVT001", rebuilt.get(0).getId());
    assertEquals("EVT001", rebuilt.get(0).getCoreId());
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, rebuilt.get(0).getCoreRowType());
  }

  // ---- OccurrenceExtensionConverter compatibility ----

  /**
   * Uses the actual {@code IdentifiersPipeline.checkExtensionsForOccurrence} and {@code
   * OccurrenceExtensionConverter.convert()} to verify that occurrence records are correctly
   * extracted from our verbatim.avro.
   *
   * <p>This is the critical path — if this produces 0 records, VERBATIM_TO_IDENTIFIER processes 0
   * occurrences and VERBATIM_TO_INTERPRETED gets nothing.
   */
  @Test
  void avroWrite_occurrenceExtensionExtractableByIdentifiersPipeline(@TempDir Path dir)
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
        List.of(
            RowFactory.create("OCC001", "EVT001", "Quercus robur"),
            RowFactory.create("OCC002", "EVT001", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence(dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, "file://" + dir)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, verbatimPath);

    Dataset<ExtendedRecord> records =
        spark.read().format("avro").load(verbatimPath).as(Encoders.bean(ExtendedRecord.class));

    // Log raw structure before passing to the real converter
    records
        .collectAsList()
        .forEach(
            er -> {
              log.info("Raw record: id={} coreRowType={}", er.getId(), er.getCoreRowType());
              List<Map<String, String>> occExts =
                  er.getExtensions().get(DwcTerm.Occurrence.qualifiedName());
              if (occExts != null) {
                log.info("  occurrence extensions count: {}", occExts.size());
                if (!occExts.isEmpty()) {
                  Map<String, String> first = occExts.get(0);
                  log.info("  first ext keys: {}", first.keySet());
                  String occId = first.get(DwcTerm.occurrenceID.qualifiedName());
                  log.info(
                      "  occurrenceID value='{}' type={}",
                      occId,
                      occId != null ? occId.getClass().getName() : "null");
                }
              } else {
                log.warn(
                    "  no occurrence extension found under key: {}",
                    DwcTerm.Occurrence.qualifiedName());
              }
            });

    // Use the actual IdentifiersPipeline.checkExtensionsForOccurrence
    String outputPath = "file://" + dir + "/output";
    Dataset<ExtendedRecord> expanded =
        org.gbif.pipelines.spark.IdentifiersPipeline.checkExtensionsForOccurrence(
            spark, records, outputPath);

    log.info("Before expanded: {}", records.collectAsList());
    List<ExtendedRecord> expandedList = expanded.collectAsList();
    log.info("Expanded occurrence records count: {}", expandedList.size());
    expandedList.forEach(
        er ->
            log.info(
                "  Expanded: id={} coreId={} scientificName={}",
                er.getId(),
                er.getCoreId(),
                er.getCoreTerms().get(DwcTerm.scientificName.qualifiedName())));

    assertEquals(
        2,
        expandedList.size(),
        "Should have extracted 2 occurrence records from the event extension");
    expandedList.sort(Comparator.comparing(ExtendedRecord::getId));
    assertEquals("OCC001", expandedList.get(0).getId());
    assertEquals("EVT001", expandedList.get(0).getCoreId());
    assertEquals(
        "Quercus robur",
        expandedList.get(0).getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("OCC002", expandedList.get(1).getId());
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
