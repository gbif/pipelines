package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.UsagePolicyJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultimediaMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("MultimediaMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withMultimedia(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directEventMediaMatchesLegacyPayload() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/event.jpg")),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(RowFactory.create("EPK-001", "MPK-001")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void occurrenceMediaIsPromotedToEventAndMatchesLegacyPayload() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/occurrence.jpg")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-001", "MPK-001")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void directAndOccurrenceMediaAreUnionedWithoutCartesianProduct() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(
                RowFactory.create("MPK-001", null, "https://example.com/event.jpg"),
                RowFactory.create("MPK-002", null, "https://example.com/occurrence.jpg")),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(RowFactory.create("EPK-001", "MPK-001")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-001", "MPK-002")));

    assertPayloadParity(loader, 2);
  }

  @Test
  void sameMediaPromotedThroughTwoBranchesIsDeduplicatedLikeLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/shared.jpg")),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(RowFactory.create("EPK-001", "MPK-001")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-001", "MPK-001")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void usagePolicyEnrichmentMatchesLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", "UP-1", "https://example.com/licensed.jpg")),
            UsagePolicyJoinBuilder.TABLE_USAGE_POLICY,
            usagePolicy(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(RowFactory.create("EPK-001", "MPK-001")));

    List<Map<String, String>> mapped = mappedMedia(loader);
    assertPayloadParity(loader, 1);
    assertEquals("CC_BY_4_0", mapped.get(0).get(DcTerm.license.qualifiedName()));
    assertEquals("Museum X", mapped.get(0).get(DcTerm.rightsHolder.qualifiedName()));
  }

  @Test
  void materialMediaIsPromotedWhenOccurrenceHasExactlyOneEvidenceMaterial() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/material.jpg")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-001")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void materialMediaIsSuppressedWhenOccurrenceHasMultipleEvidenceMaterials() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/ambiguous.jpg")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-001")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA));
    assertFalse(mapped.getExtensions().containsKey(MultimediaMapping.ROW_TYPE_MULTIMEDIA));
  }

  @Test
  void directOccurrenceAndMaterialMediaAreUnionedTogether() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(
                RowFactory.create("MPK-001", null, "https://example.com/event.jpg"),
                RowFactory.create("MPK-002", null, "https://example.com/occurrence.jpg"),
                RowFactory.create("MPK-003", null, "https://example.com/material.jpg")),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(RowFactory.create("EPK-001", "MPK-001")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-001", "MPK-002")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-003")));

    assertPayloadParity(loader, 3);
  }

  @Test
  void occurrenceWithoutMediaLinksDoesNotCreateEmptyMultimediaRow() {
    TableLoader loader = TestTableLoader.of("event", event(), "occurrence", occurrence());

    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());
    assertFalse(mapped.getExtensions().containsKey(MultimediaMapping.ROW_TYPE_MULTIMEDIA));
  }

  @Test
  void eventMultimediaIsCappedAtFiftyRowsPerParent() {
    List<Row> mediaRows =
        IntStream.range(0, 55)
            .mapToObj(
                i ->
                    RowFactory.create(
                        String.format("MPK-%03d", i),
                        null,
                        String.format("https://example.com/%03d.jpg", i)))
            .toList();
    List<Row> linkRows =
        IntStream.range(0, 55)
            .mapToObj(i -> RowFactory.create("EPK-001", String.format("MPK-%03d", i)))
            .toList();

    TableLoader loader =
        TestTableLoader.of(
            "event",
            event(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(mediaRows.toArray(Row[]::new)),
            MediaExtensionBuilder.TABLE_EVENT_MEDIA,
            eventMedia(linkRows.toArray(Row[]::new)));

    List<Map<String, String>> legacy = normalizeLegacyMedia(legacyMedia(loader));
    List<Map<String, String>> mapped = mappedMedia(loader);

    assertEquals(50, legacy.size());
    assertEquals(50, mapped.size());
    assertEquals(50, mapped.stream().distinct().count());
  }

  @Test
  void noMediaLinkTablesMeansAbsentExtension() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-001", null, "https://example.com/unlinked.jpg")));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA));
    assertFalse(mapped.getExtensions().containsKey(MultimediaMapping.ROW_TYPE_MULTIMEDIA));
  }

  @Test
  void governingTraceShowsUnionBranches() {
    String trace = engine.trace(plan);

    assertTrue(trace.contains("Extension: " + MultimediaMapping.ROW_TYPE_MULTIMEDIA));
    assertTrue(trace.contains("Row composition: UNION"));
    assertTrue(trace.contains("Max rows per parent: 50"));
    assertTrue(trace.contains("Fragment: event-media"));
    assertTrue(trace.contains("Fragment: occurrence-media-promoted-to-event"));
    assertTrue(trace.contains("Fragment: material-media-promoted-to-event"));
    assertTrue(trace.contains("Source: event-media"));
    assertTrue(trace.contains("Source: occurrence"));
    assertTrue(trace.contains("occurrence.occurrenceID -> material.evidenceForOccurrenceID"));
    assertTrue(trace.contains("ExactlyOne"));
  }

  private void assertPayloadParity(TableLoader loader, int expectedRows) {
    List<Map<String, String>> legacy = normalizeLegacyMedia(legacyMedia(loader));
    List<Map<String, String>> mapped = mappedMedia(loader);

    assertEquals(expectedRows, legacy.size());
    assertEquals(expectedRows, mapped.size());

    String accessUri = TermResolver.resolve("accessURI");
    Comparator<Map<String, String>> byAccessUri =
        Comparator.comparing(row -> row.get(accessUri), Comparator.nullsFirst(String::compareTo));
    assertEquals(
        legacy.stream().sorted(byAccessUri).toList(),
        mapped.stream().sorted(byAccessUri).toList());
  }

  private List<Map<String, String>> legacyMedia(TableLoader loader) {
    ExtendedRecord record = only(EventCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedMedia(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(MultimediaMapping.ROW_TYPE_MULTIMEDIA);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> normalizeLegacyMedia(List<Map<String, String>> rows) {
    String eventId = TermResolver.resolve("eventID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(eventId);
              return normalized;
            })
        .toList();
  }

  private Dataset<Row> event() {
    return spark.createDataFrame(
        List.of(RowFactory.create("EPK-001", "EVT001")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType));
  }

  private Dataset<Row> occurrence() {
    return spark.createDataFrame(
        List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001")),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> media(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType));
  }

  private Dataset<Row> eventMedia(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> occurrenceMedia(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> material(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> materialMedia(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> usagePolicy(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
