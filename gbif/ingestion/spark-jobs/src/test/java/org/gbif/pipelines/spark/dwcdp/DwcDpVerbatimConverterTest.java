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
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.util.SparkTest;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
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
   * occurrence (with organism join), media (via event-media), event assertions, and Humboldt survey
   * data.
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
            "event_pk",
            "eventID",
            "parentEventID",
            "eventDate",
            "country",
            "decimalLatitude",
            "decimalLongitude"),
        List.of(
            RowFactory.create("EPK-001", "EVT001", null, "2024-06-01", "DK", "55.6", "12.5"),
            RowFactory.create("EPK-002", "EVT002", "EVT001", "2024-06-02", "DK", "55.7", "12.6")));

    // occurrence carries event_fk (surrogate ref to event.event_pk), never a literal eventID.
    // associatedOrganisms is NOT a DwC-DP occurrence field — contributed by organism join only
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema(
            "occurrenceID",
            "event_fk",
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
                "EPK-001",
                "org-1",
                "Parus major",
                "multicellular organism",
                "Blue tit",
                null,
                "detected",
                "female"),
            RowFactory.create(
                "OCC002", "EPK-001", null, "Quercus robur", null, null, null, "detected", null)));

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

    // media carries media_pk — the surrogate key event-media's media_fk resolves against
    writeParquet(
        dir,
        "data/media.parquet",
        schema("media_pk", "accessURI", "mediaType"),
        List.of(
            RowFactory.create("MPK-001", "https://example.com/img1.jpg", "StillImage"),
            RowFactory.create("MPK-002", "https://example.com/img2.jpg", "StillImage")));

    // event-media carries event_fk + media_fk — surrogate refs, never eventID/mediaID directly
    writeParquet(
        dir,
        "data/event-media.parquet",
        schema("event_fk", "media_fk"),
        List.of(RowFactory.create("EPK-001", "MPK-001"), RowFactory.create("EPK-001", "MPK-002")));

    writeParquet(
        dir,
        "data/event-assertion.parquet",
        schema("assertionID", "event_fk", "assertionType", "assertionValue", "assertionUnit"),
        List.of(RowFactory.create("A001", "EPK-001", "Temperature", "25.0", "Celsius")));

    writeParquet(
        dir,
        "data/survey.parquet",
        schema("survey_pk", "event_fk", "siteCount", "reportedWeather"),
        List.of(RowFactory.create("SPK-001", "EPK-001", "3", "Clear")));

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
        List.of(RowFactory.create("SPK-001", "STP-001"), RowFactory.create("SPK-001", "STP-002")));

    DataPackage dp = DataPackageFixtures.withEventOccurrenceOrganismMediaAssertionAndSurvey();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

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

    List<Map<String, String>> emof =
        evt001.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension must be present");
    assertEquals(1, emof.size());
    assertEquals("A001", emof.get(0).get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Temperature", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));

    List<Map<String, String>> humboldt =
        evt001.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT);
    assertNotNull(humboldt, "Humboldt extension must be present");
    assertEquals(2, humboldt.size());
    List<String> surveyTargets =
        humboldt.stream()
            .map(h -> h.get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")))
            .sorted()
            .toList();
    assertEquals("All birds", surveyTargets.get(0));
    assertEquals("All mammals", surveyTargets.get(1));

    assertNull(
        evt002.getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE), "EVT002 has no occurrences");
    assertNull(
        evt002.getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA), "EVT002 has no media");
    assertNull(
        evt002.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT),
        "EVT002 has no eMoF");
    assertNull(
        evt002.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_HUMBOLDT),
        "EVT002 has no Humboldt");
  }

  // ---- round-trip: write via Spark Avro, read back as IdentifiersPipeline would ----

  @Test
  void eventCoreRoundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    // event_pk added — OccurrenceExtensionBuilder resolves occurrence.event_fk against it
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID", "eventDate", "decimalLatitude", "decimalLongitude"),
        List.of(RowFactory.create("EPK-001", "EVT001", "2024-06-15", "51.5", "-0.1")));

    // event_fk instead of eventID — occurrence never carries the natural key directly
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema("occurrenceID", "event_fk", "scientificName"),
        List.of(RowFactory.create("OCC001", "EPK-001", "Quercus robur")));

    DataPackage dp = DataPackageFixtures.withEventAndOccurrence();

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
        schema("occurrence_pk", "occurrenceID", "scientificName", "decimalLatitude"),
        List.of(RowFactory.create("OPK-001", "OCC001", "Pinus sylvestris", "60.2")));

    writeParquet(
        dir,
        "data/occurrence-assertion.parquet",
        schema("assertionID", "occurrence_fk", "assertionType", "assertionValue", "assertionUnit"),
        List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    DataPackage dp = DataPackageFixtures.withOccurrenceAndAssertion();

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
    List<Map<String, String>> emof =
        r.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension must be present");
    assertEquals(1, emof.size());
    assertEquals("A001", emof.get(0).get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Mass", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
  }

  // ---- comprehensive round-trip: occurrence-core ----

  /**
   * Full occurrence-core round-trip through Avro covering all currently implemented tables:
   * occurrence (with organism join), media (via occurrence-media), and occurrence assertions.
   *
   * <p>When new tables are added to the pipeline, extend the fixture and assertions here.
   */
  @Test
  void occurrenceCore_fullPackage_roundTripAvroWriteAndRead(@TempDir Path dir) throws Exception {
    // event_fk here is inert bystander data — occurrence-core never resolves back to event —
    // kept as event_fk (rather than the old eventID) just for consistency with the rest of
    // these fixtures.
    // associatedOrganisms is NOT a DwC-DP occurrence field — contributed by organism join only
    writeParquet(
        dir,
        "data/occurrence.parquet",
        schema(
            "occurrence_pk",
            "occurrenceID",
            "event_fk",
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
                "OPK-001",
                "OCC001",
                "EPK-001",
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
                "OPK-002",
                "OCC002",
                "EPK-001",
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

    // media carries media_pk — the surrogate key occurrence-media's media_fk resolves against
    writeParquet(
        dir,
        "data/media.parquet",
        schema("media_pk", "accessURI", "mediaType"),
        List.of(
            RowFactory.create("MPK-001", "https://example.com/img1.jpg", "StillImage"),
            RowFactory.create("MPK-002", "https://example.com/img2.jpg", "StillImage")));

    // occurrence-media carries occurrence_fk + media_fk — surrogate refs, never
    // occurrenceID/mediaID directly
    writeParquet(
        dir,
        "data/occurrence-media.parquet",
        schema("occurrence_fk", "media_fk"),
        List.of(RowFactory.create("OPK-001", "MPK-001"), RowFactory.create("OPK-001", "MPK-002")));

    writeParquet(
        dir,
        "data/occurrence-assertion.parquet",
        schema("assertionID", "occurrence_fk", "assertionType", "assertionValue", "assertionUnit"),
        List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    DataPackage dp = DataPackageFixtures.withOccurrenceOrganismMediaAndAssertion();
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

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

    List<Map<String, String>> emof =
        occ001.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension must be present on OCC001");
    assertEquals(1, emof.size());
    assertEquals("A001", emof.get(0).get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Mass", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));

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
    assertNull(
        occ002.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT),
        "OCC002 has no eMoF");
    assertNull(
        occ002.getExtensions().get(DwcDpVerbatimConverter.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT),
        "OCC002 has no eMoF");
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

    DataPackage dp = DataPackageFixtures.withEventAndAssertion();

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

    DataPackage dp = DataPackageFixtures.withOccurrenceAndAssertion();

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

    DataPackage dp = DataPackageFixtures.withEventAssertionAndProtocol();

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

    DataPackage dp = DataPackageFixtures.withEventAndAssertion();

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

    DataPackage dp = DataPackageFixtures.withEventAndSurvey();

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
        List.of(RowFactory.create("SPK-001", "STP-001"), RowFactory.create("SPK-001", "STP-002")));

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget();

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

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget();

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
  void mixedSurveys_someWithTargetsSomeWithout_produceCorrectHumboldtRowCounts(@TempDir Path dir)
      throws Exception {
    // Two events: EVT001's survey has 2 linked targets (→ 2 Humboldt rows),
    //             EVT002's survey has no linked targets (→ 1 Humboldt row).
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001"), RowFactory.create("EPK-002", "EVT002")));

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
        List.of(RowFactory.create("SPK-001", "STP-001"), RowFactory.create("SPK-001", "STP-002")));

    DataPackage dp = DataPackageFixtures.withEventSurveyAndTarget();

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
    assertNull(
        hEVT002.get(0).get(DwcDpVerbatimConverter.resolveTermUri("surveyTargetDescription")));
  }

  @Test
  void humboldtExtensionAbsentWhenNoSurveyTable(@TempDir Path dir) throws Exception {
    writeParquet(
        dir,
        "data/event.parquet",
        schema("event_pk", "eventID"),
        List.of(RowFactory.create("EPK-001", "EVT001")));

    // DataPackage has no survey table
    DataPackage dp = DataPackageFixtures.withEvent("event_pk", "eventID");

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

    DataPackage dp = DataPackageFixtures.withEvent("eventID", "eventDate");
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

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
    TableLoader loader = TestTableLoader.parquetLoader(spark, dp, "file://" + dir);

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
    SparkTest.writeParquet(spark, dir, relativePath, schema, rows);
  }

  private static StructType schema(String... names) {
    return SparkTest.schema(names);
  }
}
