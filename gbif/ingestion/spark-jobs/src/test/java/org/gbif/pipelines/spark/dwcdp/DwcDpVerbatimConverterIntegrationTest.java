package org.gbif.pipelines.spark.dwcdp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
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

    DataPackage dp = DataPackageFixtures.withEvent("eventID", "eventDate", "decimalLatitude");
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String targetPath = "file://" + dir + "/verbatim.avro";

    EventCoreBuilder.build(spark, loader)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, targetPath);

    java.io.File avroFile = dir.resolve("verbatim.avro").toFile();
    assertTrue(
        avroFile.exists() && !avroFile.isDirectory(), "verbatim.avro should be a single file");

    org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> reader =
        new org.apache.avro.file.DataFileReader<>(
            avroFile, new org.apache.avro.generic.GenericDatumReader<>(schema));

    assertTrue(reader.hasNext(), "Should have at least one record");
    org.apache.avro.generic.GenericRecord record = reader.next();
    reader.close();

    Object id = record.get("id");
    assertNotNull(id);
    assertInstanceOf(
        org.apache.avro.util.Utf8.class,
        id,
        "id should be plain Avro Utf8 string, not union-wrapped. Got: " + id.getClass().getName());
    assertEquals("EVT001", id.toString());

    Object coreRowType = record.get("coreRowType");
    assertNotNull(coreRowType);
    assertInstanceOf(org.apache.avro.util.Utf8.class, coreRowType);
    assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_EVENT, coreRowType.toString());

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
        "coreTerms values should be plain Avro Utf8 strings, not union-wrapped");
    assertEquals("2024-06-15", eventDateValue.toString());

    assertNull(record.get("coreId"), "coreId should be null for a core record");
  }

  // ---- downstream pipeline compatibility ----

  @Test
  void avroWrite_readableByEventInterpretationPipeline(@TempDir Path dir) throws Exception {
    // event_pk added — OccurrenceExtensionBuilder resolves occurrence.event_fk against it
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID", "eventDate", "parentEventID", "decimalLatitude"),
        List.of(
            RowFactory.create("EPK-001", "EVT001", "2024-06-15", null, "59.0"),
            RowFactory.create("EPK-002", "EVT002", "2024-06-16", "EVT001", "59.1")));
    // event_fk instead of eventID — occurrence never carries the natural key directly
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "event_fk", "scientificName"),
        List.of(
            RowFactory.create("OCC001", "EPK-001", "Quercus robur"),
            RowFactory.create("OCC002", "EPK-002", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    EventCoreBuilder.build(spark, loader)
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
    assertEquals("EVT001", first.getId());
    assertNull(first.getCoreId(), "coreId should be null until set by downstream pipeline");
    assertEquals("2024-06-15", first.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));

    List<Map<String, String>> occExt = first.getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt);
    assertEquals(1, occExt.size());
    assertEquals("Quercus robur", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));

    // verify coreTerms are accessible as plain strings via Spark column projection
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

    // verify the records can be rebuilt with coreId set, as downstream pipelines do
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
    assertEquals("EVT001", rebuilt.get(0).getCoreId());
    assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_EVENT, rebuilt.get(0).getCoreRowType());
  }

  // ---- OccurrenceExtensionConverter compatibility ----

  @Test
  void avroWrite_occurrenceExtensionExtractableByIdentifiersPipeline(@TempDir Path dir)
      throws Exception {
    // event_pk added — OccurrenceExtensionBuilder resolves occurrence.event_fk against it
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID", "eventDate"),
        List.of(RowFactory.create("EPK-001", "EVT001", "2024-06-15")));
    // event_fk instead of eventID — occurrence never carries the natural key directly
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "event_fk", "scientificName"),
        List.of(
            RowFactory.create("OCC001", "EPK-001", "Quercus robur"),
            RowFactory.create("OCC002", "EPK-001", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    EventCoreBuilder.build(spark, loader)
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

    String outputPath = "file://" + dir + "/output";
    Dataset<ExtendedRecord> expanded =
        org.gbif.pipelines.spark.IdentifiersPipeline.checkExtensionsForOccurrence(
            spark, records, outputPath);

    List<ExtendedRecord> expandedList = expanded.collectAsList();
    log.info("Expanded occurrence records count: {}", expandedList.size());

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
