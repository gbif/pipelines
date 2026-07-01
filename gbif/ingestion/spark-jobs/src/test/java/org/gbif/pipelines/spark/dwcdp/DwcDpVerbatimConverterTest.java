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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TableLoaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DwcDpVerbatimConverter} covering orchestrator-level concerns:
 *
 * <ul>
 *   <li>Full end-to-end round-trips through Avro for each routing path (event-core,
 *       occurrence-core), covering all currently implemented tables.
 *   <li>Avro output structure ({@code coalesce(1)}, {@code mergeToSingleFile}).
 *   <li>Metrics writing.
 * </ul>
 *
 * <p>Unit-level concerns are covered in their respective classes:
 *
 * <ul>
 *   <li>Term resolution: {@link org.gbif.pipelines.spark.dwcdp.builder.TermResolverTest}
 *   <li>Row mapping: {@link org.gbif.pipelines.spark.dwcdp.builder.RowTermMapperTest}
 *   <li>Event-core builder: {@link org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilderTest}
 *   <li>Occurrence-core builder: {@link
 *       org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilderTest}
 *   <li>Organism join: {@link
 *       org.gbif.pipelines.spark.dwcdp.builder.extension.OrganismJoinBuilderTest}
 *   <li>coreTerms schema compliance: {@link
 *       org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreTermsComplianceTest}
 * </ul>
 */
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

  // ---- comprehensive round-trip: event-core ----

  /**
   * Full event-core round-trip through Avro covering all currently implemented tables: event,
   * occurrence (with organism join), and media (via event-media).
   *
   * <p>When new tables are added to the pipeline (e.g. identification), extend the fixture data and
   * add assertions here rather than adding a new permutation test.
   */
  @Test
  void eventCore_fullPackage_roundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    // event: two events, one parent-child relationship
    writeParquet(
        dir,
        "data/event.parquet",
        schema(
            "eventID",
            "parentEventID",
            "eventDate",
            "country",
            "decimalLatitude",
            "decimalLongitude"),
        List.of(
            RowFactory.create("EVT001", null, "2024-06-01", "DK", "55.6", "12.5"),
            RowFactory.create("EVT002", "EVT001", "2024-06-02", "DK", "55.7", "12.6")));

    // associatedOrganisms is NOT a DwC-DP occurrence field — contributed by organism join only
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema(
            "occurrenceID",
            "eventID",
            "organismID",
            "scientificName",
            "organismScope",
            "organismName",
            "organismRemarks",
            "occurrenceStatus",
            "sex"),
        List.of(
            RowFactory.create(
                "OCC001",
                "EVT001",
                "org-1",
                "Parus major",
                "multicellular organism",
                "Blue tit",
                null,
                "detected",
                "female"),
            RowFactory.create(
                "OCC002", "EVT001", null, "Quercus robur", null, null, null, "detected", null)));

    writeParquet(
        dir,
        "data/organism.parquet",
        schema(
            "organismID",
            "organismName",
            "organismScope",
            "organismRemarks",
            "associatedOrganisms"),
        List.of(
            RowFactory.create(
                "org-1", "Blue tit", "multicellular organism", null, "sibling of:org-2")));

    writeParquet(
        dir,
        "data/media.parquet",
        schema("mediaID", "accessURI", "mediaType"),
        List.of(
            RowFactory.create("MED001", "https://example.com/img1.jpg", "StillImage"),
            RowFactory.create("MED002", "https://example.com/img2.jpg", "StillImage")));

    writeParquet(
        dir,
        "data/event-media.parquet",
        schema("mediaID", "eventID"),
        List.of(RowFactory.create("MED001", "EVT001"), RowFactory.create("MED002", "EVT001")));

    DataPackage dp = DataPackageFixtures.withEventOccurrenceOrganismAndMedia();
    TableLoader loader = TableLoaders.parquetLoader(spark, dp, "file://" + dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    EventCoreBuilder.build(spark, loader)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
            .collectAsList();
    records.sort(Comparator.comparing(ExtendedRecord::getId));

    assertEquals(2, records.size());
    records.forEach(r -> assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_EVENT, r.getCoreRowType()));
    assertNull(records.get(0).getCoreId(), "coreId must be null at verbatim stage");

    ExtendedRecord evt001 = records.get(0);
    assertEquals("EVT001", evt001.getId());
    assertEquals("2024-06-01", evt001.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));
    assertEquals("DK", evt001.getCoreTerms().get(DwcTerm.country.qualifiedName()));
    assertEquals("55.6", evt001.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertNull(
        evt001.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        "parentEventID must be absent when null");

    ExtendedRecord evt002 = records.get(1);
    assertEquals(
        "EVT001",
        evt002.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        "parentEventID must survive round-trip");

    List<Map<String, String>> occExt =
        evt001.getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt, "occurrence extension must be present");
    assertEquals(2, occExt.size());
    occExt.sort(Comparator.comparing(m -> m.get(DwcTerm.occurrenceID.qualifiedName())));

    Map<String, String> occ001 = occExt.get(0);
    assertEquals("OCC001", occ001.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Parus major", occ001.get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("female", occ001.get(DwcTerm.sex.qualifiedName()));
    assertEquals("org-1", occ001.get(DwcTerm.organismID.qualifiedName()));
    assertEquals("Blue tit", occ001.get(DwcTerm.organismName.qualifiedName()));
    assertEquals("multicellular organism", occ001.get(DwcTerm.organismScope.qualifiedName()));
    assertEquals(
        "sibling of:org-2",
        occ001.get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms contributed by organism table must survive Avro round-trip");

    Map<String, String> occ002 = occExt.get(1);
    assertEquals("OCC002", occ002.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Quercus robur", occ002.get(DwcTerm.scientificName.qualifiedName()));
    assertNull(
        occ002.get(DwcTerm.organismID.qualifiedName()),
        "organismID must be absent when occurrence has no organism link");
    assertNull(
        occ002.get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms must be absent when occurrence has no organism link");

    List<Map<String, String>> mediaExt =
        evt001.getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt, "media extension must be present");
    assertEquals(2, mediaExt.size());
    List<String> mediaUris =
        mediaExt.stream().map(m -> m.get(TermResolver.resolve("accessURI"))).sorted().toList();
    assertEquals("https://example.com/img1.jpg", mediaUris.get(0));
    assertEquals("https://example.com/img2.jpg", mediaUris.get(1));

    assertNull(
        evt002.getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE), "EVT002 has no occurrences");
    assertNull(
        evt002.getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA), "EVT002 has no media");
  }

  // ---- comprehensive round-trip: occurrence-core ----

  /**
   * Full occurrence-core round-trip through Avro covering all currently implemented tables:
   * occurrence (with organism join) and media (via occurrence-media).
   *
   * <p>When new tables are added to the pipeline, extend the fixture and assertions here.
   */
  @Test
  void occurrenceCore_fullPackage_roundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    // associatedOrganisms is NOT a DwC-DP occurrence field — contributed by organism join only
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema(
            "occurrenceID",
            "eventID",
            "organismID",
            "scientificName",
            "organismScope",
            "organismName",
            "organismRemarks",
            "occurrenceStatus",
            "sex",
            "decimalLatitude",
            "decimalLongitude"),
        List.of(
            RowFactory.create(
                "OCC001",
                "EVT001",
                "org-1",
                "Parus major",
                "multicellular organism",
                "Blue tit",
                null,
                "detected",
                "female",
                "55.6",
                "12.5"),
            RowFactory.create(
                "OCC002",
                "EVT001",
                null,
                "Quercus robur",
                null,
                null,
                null,
                "detected",
                null,
                "55.7",
                "12.6")));

    writeParquet(
        dir,
        "data/organism.parquet",
        schema(
            "organismID",
            "organismName",
            "organismScope",
            "organismRemarks",
            "associatedOrganisms"),
        List.of(
            RowFactory.create(
                "org-1", "Blue tit", "multicellular organism", null, "sibling of:org-2")));

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

    DataPackage dp = DataPackageFixtures.withOccurrenceOrganismAndMedia();
    TableLoader loader = TableLoaders.parquetLoader(spark, dp, "file://" + dir);

    String verbatimPath = "file://" + dir + "/verbatim.avro";
    OccurrenceCoreBuilder.build(spark, loader)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .option("avroSchema", DwcDpVerbatimConverter.extendedRecordSchemaJson())
        .save(verbatimPath);

    List<ExtendedRecord> records =
        spark
            .read()
            .format("avro")
            .load(verbatimPath)
            .as(Encoders.bean(ExtendedRecord.class))
            .collectAsList();
    records.sort(Comparator.comparing(ExtendedRecord::getId));

    assertEquals(2, records.size());
    records.forEach(r -> assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_OCCURRENCE, r.getCoreRowType()));
    assertNull(records.get(0).getCoreId(), "coreId must be null at verbatim stage");

    ExtendedRecord occ001 = records.get(0);
    assertEquals("OCC001", occ001.getId());
    assertEquals("Parus major", occ001.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("55.6", occ001.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("female", occ001.getCoreTerms().get(DwcTerm.sex.qualifiedName()));
    assertEquals(
        "org-1",
        occ001.getCoreTerms().get(DwcTerm.organismID.qualifiedName()),
        "organismID FK must be retained");
    assertEquals(
        "Blue tit",
        occ001.getCoreTerms().get(DwcTerm.organismName.qualifiedName()),
        "occurrence value wins for overlapping organism field");
    assertEquals(
        "multicellular organism",
        occ001.getCoreTerms().get(DwcTerm.organismScope.qualifiedName()),
        "occurrence value wins for overlapping organism field");
    assertEquals(
        "sibling of:org-2",
        occ001.getCoreTerms().get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms contributed by organism table must survive Avro round-trip");

    List<Map<String, String>> mediaExt =
        occ001.getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt, "media extension must be present on OCC001");
    assertEquals(2, mediaExt.size());
    List<String> mediaUris =
        mediaExt.stream().map(m -> m.get(TermResolver.resolve("accessURI"))).sorted().toList();
    assertEquals("https://example.com/img1.jpg", mediaUris.get(0));
    assertEquals("https://example.com/img2.jpg", mediaUris.get(1));

    ExtendedRecord occ002 = records.get(1);
    assertEquals("OCC002", occ002.getId());
    assertEquals(
        "Quercus robur", occ002.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertNull(
        occ002.getCoreTerms().get(DwcTerm.organismID.qualifiedName()),
        "organismID must be absent when occurrence has no organism link");
    assertNull(
        occ002.getCoreTerms().get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms must be absent when occurrence has no organism link");
    assertNull(
        occ002.getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA), "OCC002 has no media");
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

    DataPackage dp = DataPackageFixtures.withEvent("eventID", "eventDate");
    TableLoader loader = TableLoaders.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    EventCoreBuilder.build(spark, loader)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(partsPath);

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

    DataPackage dp = DataPackageFixtures.withEvent("eventID", "eventDate");
    TableLoader loader = TableLoaders.parquetLoader(spark, dp, "file://" + dir);

    String partsPath = "file://" + dir + "/verbatim.avro.parts";
    String targetPath = "file://" + dir + "/verbatim.avro";

    EventCoreBuilder.build(spark, loader)
        .coalesce(1)
        .write()
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(partsPath);

    FileSystem fs = FileSystem.getLocal(new Configuration());
    DwcDpVerbatimConverter.mergeToSingleFile(fs, partsPath, targetPath);

    assertFalse(
        dir.resolve("verbatim.avro.parts").toFile().exists(),
        "temp parts directory should have been deleted");

    File verbatimFile = dir.resolve("verbatim.avro").toFile();
    assertTrue(verbatimFile.exists(), "verbatim.avro should exist");
    assertFalse(verbatimFile.isDirectory(), "verbatim.avro should be a file, not a directory");

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

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String datasetBasePath = "file://" + dir;

    DwcDpVerbatimConverter.writeMetrics(spark, dp, datasetBasePath, fs, "test-dataset");

    org.apache.hadoop.fs.Path metricsPath =
        new org.apache.hadoop.fs.Path(datasetBasePath + "/archive-to-verbatim.yml");
    assertTrue(fs.exists(metricsPath), "archive-to-verbatim.yml should have been written");

    String content;
    try (var reader =
        new BufferedReader(new InputStreamReader(fs.open(metricsPath), StandardCharsets.UTF_8))) {
      content =
          reader.lines().map(line -> line.replace("\u0000", "")).collect(Collectors.joining("\n"));
    }

    Map<String, Object> yaml = new org.yaml.snakeyaml.Yaml().load(content);
    assertEquals(0, ((Number) yaml.get(Metrics.ARCHIVE_TO_ER_COUNT)).longValue());
    assertEquals(2, ((Number) yaml.get(Metrics.ARCHIVE_TO_OCC_COUNT)).longValue());
    assertEquals(1, ((Number) yaml.get(Metrics.EVENT_CORE_RECORDS_COUNT)).longValue());
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

    DataPackage dp = DataPackageFixtures.withEvent("eventID", "eventDate");
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
