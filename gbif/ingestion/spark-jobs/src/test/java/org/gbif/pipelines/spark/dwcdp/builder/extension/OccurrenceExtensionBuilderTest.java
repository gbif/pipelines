package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests for {@link OccurrenceExtensionBuilder}, in particular the fix that folds a nested
 * occurrence's own {@code occurrence-media}/{@code occurrence-assertion} rows into its term map (as
 * {@code mediaExtJson}/{@code assertionExtJson}) before it gets aggregated into an event's {@code
 * occurrenceExtJson}. Before this fix, an occurrence's own photos/measurements were silently
 * dropped whenever that occurrence was nested under an event core rather than being core itself —
 * see {@link org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilderTest} for the equivalent
 * (already-covered) behavior on the occurrence-core path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("OccurrenceExtensionBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * {@code occurrence} carrying every surrogate key needed by every builder that resolves against
   * it in this test: {@code event_fk} (this class's own event resolution), {@code occurrence_pk}
   * (what {@link MediaExtensionBuilder}/{@link AssertionExtensionBuilder} resolve their own {@code
   * occurrence_fk} against when they independently reload the {@code occurrence} table).
   */
  private Dataset<Row> occurrencePkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceNoEventFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> mediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("materialEntityID", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("collectionEvent_fk", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialIdentifierDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- JSON helpers ----

  private List<Map<String, String>> parseOccurrenceExtJson(Row row) throws Exception {
    String json = row.getAs(OccurrenceExtensionBuilder.COL_OCCURRENCE_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }

  private List<Map<String, String>> parseNestedJson(String json) throws Exception {
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }

  private Map<String, String> singleOccurrence(Row eventRow) throws Exception {
    List<Map<String, String>> occExt = parseOccurrenceExtJson(eventRow);
    assertEquals(1, occExt.size());
    return occExt.get(0);
  }

  // ---- routing / absent-table cases ----

  @Test
  void occurrenceTableAbsent_returnsEmpty() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));

    Optional<Dataset<Row>> result =
        OccurrenceExtensionBuilder.build(spark, TestTableLoader.of("event", eventDf));

    assertTrue(result.isEmpty());
  }

  @Test
  void eventTableAbsent_returnsEmpty() {
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));

    Optional<Dataset<Row>> result =
        OccurrenceExtensionBuilder.build(spark, TestTableLoader.of("occurrence", occDf));

    assertTrue(result.isEmpty());
  }

  @Test
  void occurrenceTableWithoutEventFkColumn_returnsEmpty() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf = occurrenceNoEventFkDf(List.of(RowFactory.create("OCC001", "Parus major")));

    Optional<Dataset<Row>> result =
        OccurrenceExtensionBuilder.build(
            spark, TestTableLoader.of("event", eventDf, "occurrence", occDf));

    assertTrue(result.isEmpty());
  }

  // ---- baseline: no media/assertion tables present ----

  @Test
  void basicOccurrence_resolvesEventIdAndCarriesNoNestedExtensionKeys() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark, TestTableLoader.of("event", eventDf, "occurrence", occDf));

    assertTrue(resultOpt.isPresent());
    List<Row> rows = resultOpt.get().collectAsList();
    assertEquals(1, rows.size());
    assertEquals("EVT001", rows.get(0).getAs("eventID"));

    Map<String, String> occ = singleOccurrence(rows.get(0));
    assertEquals("OCC001", occ.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Parus major", occ.get(DwcTerm.scientificName.qualifiedName()));
    assertFalse(occ.containsKey(MediaExtensionBuilder.COL_MEDIA_EXT_JSON));
    assertFalse(occ.containsKey(AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON));
  }

  // ---- the fix: occurrence-media nested inside the occurrence's own term map ----

  @Test
  void occurrenceMedia_nestedInsideOccurrenceTermMap() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img1.jpg")));
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "occurrence", occDf,
                "media", mediaDf,
                "occurrence-media", occMediaDf));

    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    Map<String, String> occ = singleOccurrence(eventRow);

    String mediaJson = occ.get(MediaExtensionBuilder.COL_MEDIA_EXT_JSON);
    assertNotNull(mediaJson, "occurrence's own media must be nested under mediaExtJson");

    List<Map<String, String>> mediaExt = parseNestedJson(mediaJson);
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img1.jpg", mediaExt.get(0).get(TermResolver.resolve("accessURI")));
  }

  // ---- the fix: occurrence-assertion nested inside the occurrence's own term map ----

  @Test
  void occurrenceAssertion_nestedInsideOccurrenceTermMap() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));
    Dataset<Row> assertionDf =
        occurrenceAssertionDf(List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "occurrence", occDf,
                "occurrence-assertion", assertionDf));

    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    Map<String, String> occ = singleOccurrence(eventRow);

    String assertionJson = occ.get(AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON);
    assertNotNull(
        assertionJson, "occurrence's own assertions must be nested under assertionExtJson");

    List<Map<String, String>> emof = parseNestedJson(assertionJson);
    assertEquals(1, emof.size());
    // AssertionExtensionBuilder already remaps assertion columns to eMoF term names before this
    // builder ever sees them — the nested blob carries measurementType/measurementValue, not the
    // raw assertionType/assertionValue column names.
    assertEquals("Mass", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("3.2", emof.get(0).get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("g", emof.get(0).get(DwcTerm.measurementUnit.qualifiedName()));
  }

  // ---- correct per-occurrence scoping ----

  @Test
  void onlyOneOfTwoOccurrencesHasMedia_nestedMediaOnlyOnThatOccurrence() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(
            List.of(
                RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major"),
                RowFactory.create("OPK-002", "OCC002", "EPK-001", "Quercus robur")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img1.jpg")));
    // Only OCC001 (OPK-001) has a linked media row
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "occurrence", occDf,
                "media", mediaDf,
                "occurrence-media", occMediaDf));

    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    List<Map<String, String>> occExt = parseOccurrenceExtJson(eventRow);
    assertEquals(2, occExt.size());
    occExt.sort(
        (a, b) ->
            a.get(DwcTerm.occurrenceID.qualifiedName())
                .compareTo(b.get(DwcTerm.occurrenceID.qualifiedName())));

    Map<String, String> occ001 = occExt.get(0);
    assertEquals("OCC001", occ001.get(DwcTerm.occurrenceID.qualifiedName()));
    assertNotNull(occ001.get(MediaExtensionBuilder.COL_MEDIA_EXT_JSON));

    Map<String, String> occ002 = occExt.get(1);
    assertEquals("OCC002", occ002.get(DwcTerm.occurrenceID.qualifiedName()));
    assertFalse(
        occ002.containsKey(MediaExtensionBuilder.COL_MEDIA_EXT_JSON),
        "an occurrence with no linked media must not carry a mediaExtJson key at all");
  }

  // ---- organism enrichment composes correctly with the new nesting ----

  @Test
  void organismEnrichmentAndNestedMediaBothSurviveTogether() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));

    StructType occSchema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    Dataset<Row> occDf =
        spark.createDataFrame(
            List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "org-1", "Parus major")),
            occSchema);

    StructType organismSchema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    Dataset<Row> organismDf =
        spark.createDataFrame(
            List.of(RowFactory.create("org-1", "sibling of:org-2")), organismSchema);

    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img1.jpg")));
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "occurrence", occDf,
                "organism", organismDf,
                "media", mediaDf,
                "occurrence-media", occMediaDf));

    assertTrue(resultOpt.isPresent());
    Map<String, String> occ = singleOccurrence(resultOpt.get().collectAsList().get(0));

    // organism join (flat, runs first)
    assertEquals("sibling of:org-2", occ.get(DwcTerm.associatedOrganisms.qualifiedName()));
    // media nesting (runs after) — must not be clobbered by the organism join
    assertNotNull(occ.get(MediaExtensionBuilder.COL_MEDIA_EXT_JSON));
  }

  // ---- occurrence_pk surrogate must never leak ----

  @Test
  void occurrencePkSurrogate_neverLeaksIntoNestedTermMap() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img1.jpg")));
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "occurrence", occDf,
                "media", mediaDf,
                "occurrence-media", occMediaDf));

    assertTrue(resultOpt.isPresent());
    Map<String, String> occ = singleOccurrence(resultOpt.get().collectAsList().get(0));
    assertFalse(
        occ.containsKey("occurrence_pk"),
        "occurrence_pk is a surrogate key with no DwC term — it must never appear nested "
            + "inside occurrenceExtJson, even though MediaExtensionBuilder needed it internally "
            + "to resolve occurrence_fk");
  }

  @Test
  void unlinkedMaterial_becomesVirtualOccurrenceWithMaterialChildren() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> materialDf =
        materialDf(List.of(RowFactory.create("MPK-001", "MAT001", null, "EPK-001", "CAT001")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MEDIA-001", "https://example.com/material.jpg")));
    Dataset<Row> materialMediaDf =
        materialMediaDf(List.of(RowFactory.create("MPK-001", "MEDIA-001")));
    Dataset<Row> materialAssertionDf =
        materialAssertionDf(List.of(RowFactory.create("A001", "MPK-001", "Mass", "3.2")));
    Dataset<Row> materialIdentifierDf =
        materialIdentifierDf(List.of(RowFactory.create("MPK-001", "barcode-001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of(
                "event", eventDf,
                "material", materialDf,
                "media", mediaDf,
                "material-media", materialMediaDf,
                "material-assertion", materialAssertionDf,
                "material-identifier", materialIdentifierDf));

    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    assertEquals("EVT001", eventRow.getAs("eventID"));
    Map<String, String> occurrence = singleOccurrence(eventRow);
    assertEquals("MAT001", occurrence.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("MAT001", occurrence.get(DwcTerm.materialSampleID.qualifiedName()));
    assertEquals("MaterialSample", occurrence.get(DwcTerm.basisOfRecord.qualifiedName()));
    assertEquals("present", occurrence.get(DwcTerm.occurrenceStatus.qualifiedName()));
    assertEquals("CAT001", occurrence.get(DwcTerm.catalogNumber.qualifiedName()));
    assertFalse(occurrence.containsKey("materialEntity_pk"));

    assertEquals(
        "https://example.com/material.jpg",
        parseNestedJson(occurrence.get(MediaExtensionBuilder.COL_MEDIA_EXT_JSON))
            .get(0)
            .get(TermResolver.resolve("accessURI")));
    assertEquals(
        "Mass",
        parseNestedJson(occurrence.get(AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON))
            .get(0)
            .get(DwcTerm.measurementType.qualifiedName()));
    assertEquals(
        "barcode-001",
        parseNestedJson(occurrence.get(IdentifierExtensionBuilder.COL_IDENTIFIER_EXT_JSON))
            .get(0)
            .get(TermResolver.resolve("identifier")));
  }

  @Test
  void unlinkedMaterialWithoutMaterialEntityId_usesStableUrnFallback() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> materialDf =
        materialDf(List.of(RowFactory.create("MPK-001", null, null, "EPK-001", "CAT001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark, TestTableLoader.of("event", eventDf, "material", materialDf));

    assertTrue(resultOpt.isPresent());
    assertEquals(
        "urn:gbif:dwcdp:material:MPK-001",
        singleOccurrence(resultOpt.get().collectAsList().get(0))
            .get(DwcTerm.occurrenceID.qualifiedName()));
  }

  @Test
  void materialWithEvidenceForOccurrenceOutsidePackage_stillBecomesVirtualOccurrence()
      throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> materialDf =
        materialDf(List.of(RowFactory.create("MPK-001", "MAT001", "OCC001", "EPK-001", "CAT001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark, TestTableLoader.of("event", eventDf, "material", materialDf));

    // There's no local occurrence table for "OCC001" to be evidence for — evidenceForOccurrenceID
    // is a weak FK, so this legitimately references an occurrence outside this package. With
    // nothing local for it to duplicate, the material must still be promoted to a virtual
    // occurrence via its resolvable collectionEvent_fk, same as unlinked material would be.
    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    assertEquals("EVT001", eventRow.getAs("eventID"));
    Map<String, String> occurrence = singleOccurrence(eventRow);
    assertEquals("MAT001", occurrence.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("MaterialSample", occurrence.get(DwcTerm.basisOfRecord.qualifiedName()));
  }

  @Test
  void materialWithEvidenceForLocalOccurrence_enrichesItWithoutDuplicating() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occDf =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));
    Dataset<Row> materialDf =
        materialDf(List.of(RowFactory.create("MPK-001", "MAT001", "OCC001", "EPK-001", "CAT001")));

    Optional<Dataset<Row>> resultOpt =
        OccurrenceExtensionBuilder.build(
            spark,
            TestTableLoader.of("event", eventDf, "occurrence", occDf, "material", materialDf));

    // OCC001 is a real local occurrence — evidence resolves to it, so the material must enrich
    // that occurrence (e.g. catalogNumber) rather than also spawning a duplicate virtual one.
    // singleOccurrence itself asserts there's exactly one occurrence in the extension.
    assertTrue(resultOpt.isPresent());
    Row eventRow = resultOpt.get().collectAsList().get(0);
    Map<String, String> occurrence = singleOccurrence(eventRow);
    assertEquals("OCC001", occurrence.get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Parus major", occurrence.get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("CAT001", occurrence.get(DwcTerm.catalogNumber.qualifiedName()));
  }
}
