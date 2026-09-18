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
        schema("event_pk", "eventID", "eventDate", "decimalLatitude"),
        List.of(RowFactory.create("EPK-001", "EVT001", "2024-06-15", "59.0")));

    DataPackage dp =
        DataPackageFixtures.withEvent("event_pk", "eventID", "eventDate", "decimalLatitude");
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String targetPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(loader)
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
        schema("event_pk", "eventID", "eventDate", "parentEvent_fk", "decimalLatitude"),
        List.of(
            RowFactory.create("EPK-001", "EVT001", "2024-06-15", null, "59.0"),
            RowFactory.create("EPK-002", "EVT002", "2024-06-16", "EPK-001", "59.1")));
    // event_fk instead of eventID — occurrence never carries the natural key directly
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrence_pk", "occurrenceID", "event_fk", "scientificName"),
        List.of(
            RowFactory.create("OPK-001", "OCC001", "EPK-001", "Quercus robur"),
            RowFactory.create("OPK-002", "OCC002", "EPK-002", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(loader)
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

    List<Map<String, String>> occExt =
        first.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_OCCURRENCE);
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
    assertEquals(DwcDpVerbatimConverter.CORE_ROW_TYPE_EVENT, rebuilt.get(0).getCoreRowType());
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
        schema("occurrence_pk", "occurrenceID", "event_fk", "scientificName"),
        List.of(
            RowFactory.create("OPK-001", "OCC001", "EPK-001", "Quercus robur"),
            RowFactory.create("OPK-002", "OCC002", "EPK-001", "Pinus sylvestris")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String verbatimPath = "file://" + dir + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(loader)
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

  @Test
  void avroWrite_mixedOccurrenceDiscoveryPreservesBasisOfRecordPrecedence(@TempDir Path dir)
      throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID", "eventType"),
        List.of(
            RowFactory.create("E-OBS", "EVT-OBS", "Observation"),
            RowFactory.create("E-MACHINE", "EVT-MACHINE", "Sensor"),
            RowFactory.create("E-ANALYSIS", "EVT-ANALYSIS", "NucleotideAnalysis"),
            RowFactory.create("E-DEFAULT", "EVT-DEFAULT", "Other"),
            RowFactory.create("E-SAMPLE", "EVT-SAMPLE", "Observation"),
            RowFactory.create("E-PRESERVED", "EVT-PRESERVED", "Sensor"),
            RowFactory.create("E-FOSSIL", "EVT-FOSSIL", "Observation"),
            RowFactory.create("E-LIVING", "EVT-LIVING", "Sensor"),
            RowFactory.create("E-AMBIG", "EVT-AMBIG", "Sensor")));

    // The directly event-owned rows also include O-AMBIG, whose two evidence Materials deliberately
    // make Material enrichment ambiguous. The three final discovery-only rows omit event_fk and
    // must be discovered through Material -> Identification, Material -> Analysis ->
    // Identification, and Material -> Analysis -> Sequence -> Identification respectively.
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrence_pk", "occurrenceID", "event_fk"),
        List.of(
            RowFactory.create("O-OBS", "occ-observation", "E-OBS"),
            RowFactory.create("O-MACHINE", "occ-machine", "E-MACHINE"),
            RowFactory.create("O-ANALYSIS", "occ-analysis", "E-ANALYSIS"),
            RowFactory.create("O-DEFAULT", "occ-default", "E-DEFAULT"),
            RowFactory.create("O-SAMPLE", "occ-sample", "E-SAMPLE"),
            RowFactory.create("O-AMBIG", "occ-ambiguous", "E-AMBIG"),
            RowFactory.create("O-PRESERVED", "occ-preserved", null),
            RowFactory.create("O-FOSSIL", "occ-fossil", null),
            RowFactory.create("O-LIVING", "occ-living", null)));

    writeParquet(
        dir,
        "data/material.parquet",
        schema(
            "materialEntity_pk",
            "collectionEvent_fk",
            "evidenceForOccurrenceID",
            "materialEntityCategory",
            "catalogNumber"),
        List.of(
            // Direct occurrence -> evidence Material discovery. Material must override Event.
            RowFactory.create("M-SAMPLE", "E-SAMPLE", "occ-sample", "DNA extract", "CAT-SAMPLE"),
            // Two Materials cite the same Occurrence. Material enrichment requires exactly one
            // evidence Material, so neither category/catalogNumber may be selected for O-AMBIG.
            RowFactory.create("M-AMBIG-A", "E-AMBIG", "occ-ambiguous", "preserved", "CAT-AMBIG-A"),
            RowFactory.create("M-AMBIG-B", "E-AMBIG", "occ-ambiguous", "fossilized", "CAT-AMBIG-B"),
            // Event -> Material -> Identification -> Occurrence discovery.
            RowFactory.create("M-PRESERVED", "E-PRESERVED", null, "preserved", "CAT-PRESERVED"),
            // Event -> Material -> Analysis -> Identification -> Occurrence discovery.
            RowFactory.create("M-FOSSIL", "E-FOSSIL", null, "fossilized", "CAT-FOSSIL"),
            // Event -> Material -> Analysis -> Sequence -> Identification -> Occurrence discovery.
            RowFactory.create("M-LIVING", "E-LIVING", null, "living", "CAT-LIVING")));

    writeParquet(
        dir,
        "data/identification.parquet",
        schema(
            "identification_pk",
            "materialEntity_fk",
            "nucleotideAnalysis_fk",
            "nucleotideSequence_fk",
            "occurrence_fk"),
        List.of(
            // O-SAMPLE/M-SAMPLE is intentionally discoverable through both the direct
            // Event -> Occurrence -> evidence Material route and the indirect
            // Event -> Material -> Identification -> Occurrence route. It must still
            // materialize as one occurrence in one material context.
            RowFactory.create("I-SAMPLE", "M-SAMPLE", null, null, "O-SAMPLE"),
            RowFactory.create("I-PRESERVED", "M-PRESERVED", null, null, "O-PRESERVED"),
            RowFactory.create("I-FOSSIL", null, "A-FOSSIL", null, "O-FOSSIL"),
            RowFactory.create("I-LIVING", null, null, "S-LIVING", "O-LIVING")));

    writeParquet(
        dir,
        "data/nucleotide-analysis.parquet",
        schema("nucleotideAnalysis_pk", "materialEntity_fk", "nucleotideSequence_fk"),
        List.of(
            RowFactory.create("A-FOSSIL", "M-FOSSIL", null),
            RowFactory.create("A-LIVING", "M-LIVING", "S-LIVING")));

    writeParquet(
        dir,
        "data/nucleotide-sequence.parquet",
        schema("nucleotideSequence_pk"),
        List.of(RowFactory.create("S-LIVING")));

    DataPackage dp = DataPackageFixtures.withBasisOfRecordDiscoveryMatrix();
    String basePath = "file://" + dir;
    String partsPath = basePath + "/verbatim.avro.parts";
    String verbatimPath = basePath + "/verbatim.avro";

    DwcDpVerbatimConverter.buildEventCoreDataset(spark, dp, basePath)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, verbatimPath);

    Dataset<ExtendedRecord> eventRecords =
        spark.read().format("avro").load(verbatimPath).as(Encoders.bean(ExtendedRecord.class));
    Dataset<ExtendedRecord> occurrences =
        org.gbif.pipelines.spark.IdentifiersPipeline.checkExtensionsForOccurrence(
            spark, eventRecords, basePath + "/identifier-output");

    List<ExtendedRecord> occurrenceRecords = occurrences.collectAsList();
    assertEquals(
        9, occurrenceRecords.size(), "Every discovered occurrence should survive extraction");

    Map<String, String> basisOfRecordByOccurrence =
        occurrenceRecords.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    ExtendedRecord::getId,
                    record -> record.getCoreTerms().get(DwcTerm.basisOfRecord.qualifiedName())));
    assertEquals(
        Map.of(
            "occ-observation", "HumanObservation",
            "occ-machine", "MachineObservation",
            "occ-analysis", "MaterialSample",
            "occ-default", "Occurrence",
            "occ-sample", "MaterialSample",
            "occ-ambiguous", "MachineObservation",
            "occ-preserved", "PreservedSpecimen",
            "occ-fossil", "FossilSpecimen",
            "occ-living", "LivingSpecimen"),
        basisOfRecordByOccurrence);

    // Cross-event ownership is part of this regression: indirect discoveries must attach to the
    // Material's Event and must not leak into another Event's occurrence extension.
    Map<String, String> ownerByOccurrence =
        occurrenceRecords.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    ExtendedRecord::getId, ExtendedRecord::getCoreId));
    assertEquals(
        Map.of(
            "occ-observation", "EVT-OBS",
            "occ-machine", "EVT-MACHINE",
            "occ-analysis", "EVT-ANALYSIS",
            "occ-default", "EVT-DEFAULT",
            "occ-sample", "EVT-SAMPLE",
            "occ-ambiguous", "EVT-AMBIG",
            "occ-preserved", "EVT-PRESERVED",
            "occ-fossil", "EVT-FOSSIL",
            "occ-living", "EVT-LIVING"),
        ownerByOccurrence);

    Map<String, String> catalogByOccurrence =
        occurrenceRecords.stream()
            .filter(
                record -> record.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName()) != null)
            .collect(
                java.util.stream.Collectors.toMap(
                    ExtendedRecord::getId,
                    record -> record.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName())));
    assertEquals(
        Map.of(
            "occ-sample", "CAT-SAMPLE",
            "occ-preserved", "CAT-PRESERVED",
            "occ-fossil", "CAT-FOSSIL",
            "occ-living", "CAT-LIVING"),
        catalogByOccurrence);
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
