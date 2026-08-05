package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpRowTypes;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventCoreBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("EventCoreBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType)
            .add("country", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * Event fixture carrying both the surrogate {@code event_pk} and the natural {@code eventID} —
   * needed whenever a test also attaches a child table, since the extension builders now resolve
   * their surrogate {@code _fk} columns against {@code event_pk} before grouping by {@code
   * eventID}. {@link #eventDf} (natural key only) remains fine for tests that don't attach anything
   * to the event.
   */
  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * {@code occurrence} carries {@code event_fk} (a surrogate reference to {@code event.event_pk}) —
   * never a literal {@code eventID} — per the 1.0_DEV profile.
   */
  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /** {@code media} carries {@code media_pk} — never a literal {@code mediaID} for join purposes. */
  private Dataset<Row> mediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * {@code event-media} carries {@code event_fk} and {@code media_fk} — surrogate references to
   * {@code event.event_pk} and {@code media.media_pk} — never {@code eventID}/{@code mediaID}
   * directly.
   */
  private Dataset<Row> eventMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventIdentifierDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> protocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventProtocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("protocol_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> surveyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("siteCount", DataTypes.StringType)
            .add("reportedWeather", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> surveyTargetDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("surveyTarget_pk", DataTypes.StringType)
            .add("surveyTargetDescription", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> surveyLinkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("surveyTarget_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> surveyProtocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("protocol_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> namedProtocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolType", DataTypes.StringType)
            .add("protocolName", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventWithParentFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("parentEvent_fk", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- routing ----

  @Test
  void missingEventTable_throws() {
    assertThrows(
        IllegalStateException.class,
        () -> EventCoreBuilder.build(spark, TestTableLoader.of()),
        "Should throw when event table is absent — routing error in orchestrator");
  }

  // ---- core record shape ----

  @Test
  void eventRecord_hasCorrectIdAndCoreRowType() {
    Dataset<Row> eventDf = eventDf(List.of(RowFactory.create("EVT001", "2024-06-01", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord r = records.get(0);
    assertEquals("EVT001", r.getId());
    assertNull(r.getCoreId(), "coreId must be null at verbatim stage");
    assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_EVENT, r.getCoreRowType());
  }

  @Test
  void eventRecord_coreTermsContainEventFields() {
    Dataset<Row> eventDf = eventDf(List.of(RowFactory.create("EVT001", "2024-06-01", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals("2024-06-01", coreTerms.get(DwcTerm.eventDate.qualifiedName()));
    assertEquals("DK", coreTerms.get(DwcTerm.country.qualifiedName()));
  }

  @Test
  void nullEventIdRows_areFiltered() {
    Dataset<Row> eventDf =
        eventDf(
            List.of(
                RowFactory.create("EVT001", "2024-06-01", "DK"),
                RowFactory.create(null, "2024-06-02", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    assertEquals("EVT001", records.get(0).getId());
  }

  @Test
  void noExtensionTables_producesEmptyExtensions() {
    Dataset<Row> eventDf = eventDf(List.of(RowFactory.create("EVT001", "2024-06-01", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertTrue(records.get(0).getExtensions().isEmpty());
  }

  @Test
  void eventIdentifierTable_attachedAsExtension() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> identifierDf =
        eventIdentifierDf(
            List.of(RowFactory.create("EPK-001", "https://example.org/event/1", "URI")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event",
                    eventDf,
                    IdentifierExtensionBuilder.TABLE_EVENT_IDENTIFIER,
                    identifierDf))
            .collectAsList();

    List<Map<String, String>> identifiers =
        records.get(0).getExtensions().get(IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER);
    assertNotNull(identifiers, "identifier extension must be present");
    assertEquals(1, identifiers.size());
  }

  // ---- occurrence extension wiring ----

  @Test
  void occurrenceTable_attachedAsExtension() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occurrenceDf =
        occurrenceDf(List.of(RowFactory.create("OCC001", "EPK-001", "Parus major")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "occurrence", occurrenceDf))
            .collectAsList();

    List<Map<String, String>> occExt =
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt, "occurrence extension must be present");
    assertEquals(1, occExt.size());
    assertEquals("OCC001", occExt.get(0).get(DwcTerm.occurrenceID.qualifiedName()));
    assertEquals("Parus major", occExt.get(0).get(DwcTerm.scientificName.qualifiedName()));
  }

  @Test
  void multipleOccurrences_allAttachedToEvent() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occurrenceDf =
        occurrenceDf(
            List.of(
                RowFactory.create("OCC001", "EPK-001", "Parus major"),
                RowFactory.create("OCC002", "EPK-001", "Quercus robur"),
                RowFactory.create("OCC003", "EPK-001", "Pinus sylvestris")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "occurrence", occurrenceDf))
            .collectAsList();

    List<Map<String, String>> occExt =
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt);
    assertEquals(3, occExt.size());
  }

  @Test
  void eventWithNoMatchingOccurrences_occurrenceExtensionAbsent() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occurrenceDf =
        occurrenceDf(List.of(RowFactory.create("OCC001", "EPK-999", "Parus major")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "occurrence", occurrenceDf))
            .collectAsList();

    assertNull(
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE),
        "occurrence extension must be absent when no occurrences reference this event");
  }

  // ---- media extension wiring ----

  @Test
  void mediaTable_attachedAsMultimediaExtension() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img.jpg")));
    Dataset<Row> eventMediaDf = eventMediaDf(List.of(RowFactory.create("EPK-001", "MPK-001")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event",
                    eventDf,
                    MediaExtensionBuilder.TABLE_MEDIA,
                    mediaDf,
                    MediaExtensionBuilder.TABLE_EVENT_MEDIA,
                    eventMediaDf))
            .collectAsList();

    List<Map<String, String>> mediaExt =
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA);
    assertNotNull(mediaExt, "multimedia extension must be present");
    assertEquals(1, mediaExt.size());
    assertEquals(
        "https://example.com/img.jpg", mediaExt.get(0).get(TermResolver.resolve("accessURI")));
  }

  @Test
  void eventAssertionTable_attachedAsEmofExtension() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> assertionDf =
        eventAssertionDf(List.of(RowFactory.create("A001", "EPK-001", "Temperature", "25.0", "C")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "event-assertion", assertionDf))
            .collectAsList();

    List<Map<String, String>> emof =
        records
            .get(0)
            .getExtensions()
            .get(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension must be present");
    assertEquals(1, emof.size());
    assertEquals("A001", emof.get(0).get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Temperature", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("25.0", emof.get(0).get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("C", emof.get(0).get(DwcTerm.measurementUnit.qualifiedName()));
  }

  @Test
  void surveyTables_attachedAsHumboldtExtension() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> surveyTable =
        surveyDf(List.of(RowFactory.create("SPK-001", "EPK-001", "3", "Clear")));
    Dataset<Row> surveyTargetTable =
        surveyTargetDf(
            List.of(
                RowFactory.create("STP-001", "All birds"),
                RowFactory.create("STP-002", "All mammals")));
    Dataset<Row> surveyLinkTable =
        surveyLinkDf(
            List.of(
                RowFactory.create("SPK-001", "STP-001"), RowFactory.create("SPK-001", "STP-002")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event",
                    eventDf,
                    "survey",
                    surveyTable,
                    "survey-target",
                    surveyTargetTable,
                    "survey-survey-target",
                    surveyLinkTable))
            .collectAsList();

    List<Map<String, String>> humboldt =
        records.get(0).getExtensions().get(HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT);
    assertNotNull(humboldt, "Humboldt extension must be present");
    assertEquals(2, humboldt.size());
    assertEquals("3", humboldt.get(0).get(TermResolver.resolve("siteCount")));
  }

  @Test
  void surveyProtocols_useHumboldtTerms_andResolveFkFallbacks() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    StructType surveySchema =
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("samplingProtocol", DataTypes.StringType)
            .add("samplingProtocol_fk", DataTypes.StringType)
            .add("samplingEffortProtocol", DataTypes.StringType)
            .add("samplingEffortProtocol_fk", DataTypes.StringType);
    Dataset<Row> surveyTable =
        spark.createDataFrame(
            List.of(
                RowFactory.create("SPK-001", "EPK-001", null, "P-001", null, "P-002"),
                RowFactory.create(
                    "SPK-002",
                    "EPK-001",
                    "Publisher protocol",
                    "P-001",
                    "Publisher effort",
                    "P-002")),
            surveySchema);
    Dataset<Row> protocolTable =
        protocolDf(
            List.of(
                RowFactory.create("P-001", "Resolved protocol"),
                RowFactory.create("P-002", "Resolved effort")));

    List<Map<String, String>> humboldt =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf, "survey", surveyTable, "protocol", protocolTable))
            .collectAsList()
            .get(0)
            .getExtensions()
            .get(HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT);

    assertEquals(2, humboldt.size());
    Map<String, String> resolved =
        humboldt.stream()
            .filter(
                row ->
                    "Resolved protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    assertEquals("Resolved effort", resolved.get(EcoTerm.samplingEffortProtocol.qualifiedName()));
    assertNull(resolved.get(DwcTerm.samplingProtocol.qualifiedName()));

    Map<String, String> publisher =
        humboldt.stream()
            .filter(
                row ->
                    "Publisher protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    assertEquals("Publisher effort", publisher.get(EcoTerm.samplingEffortProtocol.qualifiedName()));
  }

  @Test
  void mediaTableAbsent_multimediaExtensionAbsent() {
    Dataset<Row> eventDf = eventDf(List.of(RowFactory.create("EVT001", "2024-06-01", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertNull(records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_MULTIMEDIA));
  }

  // ---- multiple events ----

  @Test
  void multipleEvents_eachProducesIndependentRecord() {
    Dataset<Row> eventDf =
        eventDf(
            List.of(
                RowFactory.create("EVT001", "2024-06-01", "DK"),
                RowFactory.create("EVT002", "2024-06-02", "SE"),
                RowFactory.create("EVT003", "2024-06-03", "NO")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(3, records.size());
    records.forEach(r -> assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_EVENT, r.getCoreRowType()));
  }

  @Test
  void occurrencesRoutedToCorrectEvent() {
    Dataset<Row> eventDf =
        eventPkDf(
            List.of(
                RowFactory.create("EPK-001", "EVT001"), RowFactory.create("EPK-002", "EVT002")));
    Dataset<Row> occurrenceDf =
        occurrenceDf(
            List.of(
                RowFactory.create("OCC001", "EPK-001", "Parus major"),
                RowFactory.create("OCC002", "EPK-002", "Quercus robur")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "occurrence", occurrenceDf))
            .collectAsList();
    records.sort((a, b) -> a.getId().compareTo(b.getId()));

    assertEquals(2, records.size());

    List<Map<String, String>> evt001Occ =
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(evt001Occ);
    assertEquals(1, evt001Occ.size());
    assertEquals("OCC001", evt001Occ.get(0).get(DwcTerm.occurrenceID.qualifiedName()));

    List<Map<String, String>> evt002Occ =
        records.get(1).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(evt002Occ);
    assertEquals(1, evt002Occ.size());
    assertEquals("OCC002", evt002Occ.get(0).get(DwcTerm.occurrenceID.qualifiedName()));
  }

  private Dataset<Row> occurrencePkAndMediaKeyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceMediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  @Test
  void occurrenceMediaTable_nestedInsideOccurrenceExtension() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> occurrenceDf =
        occurrencePkAndMediaKeyDf(
            List.of(RowFactory.create("OPK-001", "OCC001", "EPK-001", "Parus major")));
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/img.jpg")));
    Dataset<Row> occMediaDf = occurrenceMediaDf(List.of(RowFactory.create("OPK-001", "MPK-001")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event",
                    eventDf,
                    "occurrence",
                    occurrenceDf,
                    MediaExtensionBuilder.TABLE_MEDIA,
                    mediaDf,
                    MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
                    occMediaDf))
            .collectAsList();

    List<Map<String, String>> occExt =
        records.get(0).getExtensions().get(DwcDpRowTypes.ROW_TYPE_OCCURRENCE);
    assertNotNull(occExt, "occurrence extension must be present");
    assertEquals(1, occExt.size());

    String mediaJson = occExt.get(0).get(MediaExtensionBuilder.COL_MEDIA_EXT_JSON);
    assertNotNull(
        mediaJson,
        "occurrence's own occurrence-media must survive nested inside the occurrence "
            + "extension when occurrence is nested under event core — this was silently "
            + "dropped before the OccurrenceExtensionBuilder fix");
  }

  @Test
  void parentEventFk_resolvedToNaturalParentEventId() {
    Dataset<Row> eventDf =
        eventWithParentFkDf(
            List.of(
                RowFactory.create("EPK-001", "EVT001", null, "2024-06-01"),
                RowFactory.create("EPK-002", "EVT002", "EPK-001", "2024-06-02")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();
    records.sort((a, b) -> a.getId().compareTo(b.getId()));

    assertEquals(2, records.size());
    assertNull(
        records.get(0).getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        "EVT001 has no parent — parentEventID must be absent, not null-valued");
    assertEquals(
        "EVT001",
        records.get(1).getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        "parentEvent_fk (surrogate EPK-001) must resolve to the parent's natural eventID (EVT001), "
            + "not the raw surrogate value");
  }

  @Test
  void selfReferencingParentEventFk_stillResolvesToOwnEventId() {
    // A row whose parentEvent_fk points at its own event_pk — a data-quality bug at the source,
    // not something this builder should filter or special-case. CoreInterpreter downstream is
    // responsible for detecting this and flagging PARENT_EVENT_INFINITE_LINEAGE; this builder's
    // only job is to resolve the FK correctly so that detection has something to see.
    Dataset<Row> eventDf =
        eventWithParentFkDf(
            List.of(RowFactory.create("EPK-001", "EVT001", "EPK-001", "2024-06-01")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    assertEquals(
        "EVT001",
        records.get(0).getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()),
        "self-referencing parentEvent_fk must still resolve, not be silently dropped");
  }

  @Test
  void noParentEventFkColumn_parentEventIdAbsentAndNoErrors() {
    // Uses the plain eventDf() helper already defined in this file, which has no
    // parentEvent_fk/event_pk columns at all — the common case (no hierarchy in this dataset).
    Dataset<Row> eventDf = eventDf(List.of(RowFactory.create("EVT001", "2024-06-01", "DK")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    assertNull(records.get(0).getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()));
  }

  @Test
  void bareEventPkSurrogate_neverLeaksIntoCoreTerms() {
    Dataset<Row> eventDf =
        eventWithParentFkDf(List.of(RowFactory.create("EPK-001", "EVT001", null, "2024-06-01")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    assertFalse(
        records.get(0).getCoreTerms().containsKey("event_pk"),
        "event_pk is a surrogate key with no DwC term — it must never appear in coreTerms");
  }

  @Test
  void eventPkAndParentEventFkPresentButNoEventIdColumnAtAll_fallsBackToEventPk() {
    // eventID has no `required: true` constraint in the DwC-DP profile (only event_pk does), so
    // a package that never populated it can legitimately arrive with the column absent from the
    // Parquet schema entirely — not merely null-valued. build() must not crash, and — since
    // event_pk is required+unique — must not silently drop the record either: it falls back to
    // using event_pk as the record's eventID (see EventCoreBuilder.withEventIdFallback).
    StructType schema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("parentEvent_fk", DataTypes.StringType)
            .add("locality", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(List.of(RowFactory.create("EPK-001", null, "Some locality")), schema);

    List<ExtendedRecord> records =
        assertDoesNotThrow(
            () ->
                EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList(),
            "a missing eventID column must not crash the whole conversion");

    assertEquals(1, records.size());
    assertEquals("EPK-001", records.get(0).getId());
    assertEquals(
        "Some locality", records.get(0).getCoreTerms().get(DwcTerm.locality.qualifiedName()));
    assertFalse(
        records.get(0).getCoreTerms().containsKey("event_pk"),
        "event_pk is a surrogate key with no DwC term — it must never appear in coreTerms, "
            + "even when it's also serving as the eventID fallback");
  }

  @Test
  void eventIdFallback_alsoReachesSubBuildersThroughTheSharedLoader() throws Exception {
    // event-media resolves via event_fk -> event.event_pk -> eventID internally
    // (MediaExtensionBuilder reloads "event" from the same loader independently of this class's
    // own eventDf) — this proves the fallback wrap in EventCoreBuilder.build() actually reaches
    // every downstream consumer of "event", not just the core record built directly in this class.
    StructType schema = new StructType().add("event_pk", DataTypes.StringType);
    Dataset<Row> eventDf = spark.createDataFrame(List.of(RowFactory.create("EPK-001")), schema);
    Dataset<Row> mediaDf =
        mediaDf(List.of(RowFactory.create("MPK-001", "https://example.com/x.jpg")));
    Dataset<Row> eventMediaDf = eventMediaDf(List.of(RowFactory.create("EPK-001", "MPK-001")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf,
                    "media", mediaDf,
                    "event-media", eventMediaDf))
            .collectAsList();

    assertEquals(1, records.size());
    assertEquals("EPK-001", records.get(0).getId());
    assertTrue(
        records.get(0).getExtensions().containsKey(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA),
        "the multimedia extension must have resolved despite event having no natural eventID, "
            + "proving MediaExtensionBuilder saw the fallback too, not just this class's own "
            + "eventDf");
  }

  @Test
  void eventProtocolFkAndGeoreferenceProtocolFk_neverLeakAsRawSurrogates() {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(
            List.of(RowFactory.create("EVT001", "PROTO-001", "PROTO-002")), schema);

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf)).collectAsList();

    assertEquals(1, records.size());
    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertFalse(coreTerms.containsKey("eventProtocol_fk"));
    assertFalse(coreTerms.containsKey("georeferenceProtocol_fk"));
    // no protocol table in this fixture, so the raw FK values are the documented fallback
    assertEquals("PROTO-001", coreTerms.get(DwcTerm.samplingProtocol.qualifiedName()));
    assertEquals("PROTO-002", coreTerms.get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void eventGeoreferenceProtocolFk_usesTypedProtocolName() {
    StructType eventSchema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(List.of(RowFactory.create("EVT001", "PROTO-001")), eventSchema);
    StructType protocolSchema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolType", DataTypes.StringType)
            .add("protocolName", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    Dataset<Row> protocolDf =
        spark.createDataFrame(
            List.of(
                RowFactory.create("PROTO-001", "georeferencing", "Hand-held GPS receiver", null)),
            protocolSchema);

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf, "protocol", protocolDf))
            .collectAsList();

    assertEquals(
        "georeferencing: Hand-held GPS receiver",
        records.get(0).getCoreTerms().get(DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @Test
  void eventProtocolJunction_aggregatesAndMergesWithDirectProtocol() {
    StructType eventSchema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")), eventSchema);
    Dataset<Row> protocols =
        protocolDf(
            List.of(
                RowFactory.create("PPK-1", "Direct protocol"),
                RowFactory.create("PPK-2", "Junction protocol A"),
                RowFactory.create("PPK-3", "Junction protocol B")));
    Dataset<Row> eventProtocols =
        eventProtocolDf(
            List.of(
                RowFactory.create("EPK-001", "PPK-3"),
                RowFactory.create("EPK-001", "PPK-2"),
                RowFactory.create("EPK-001", "PPK-2")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf, "protocol", protocols, "event-protocol", eventProtocols))
            .collectAsList();

    assertEquals(
        "Direct protocol|Junction protocol A|Junction protocol B",
        records.get(0).getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
  }

  @Test
  void provenanceAttributionFields_aggregatedThroughFullEventCoreBuilder() {
    StructType eventSchema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(
            List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")), eventSchema);

    StructType provenanceSchema =
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType);
    Dataset<Row> provenanceDf =
        spark.createDataFrame(
            List.of(RowFactory.create("PPK-1", "PROV-1", "NSF Grant 123")), provenanceSchema);

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark, TestTableLoader.of("event", eventDf, "provenance", provenanceDf))
            .collectAsList();

    assertEquals(1, records.size());
    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals("NSF Grant 123", coreTerms.get(DwcTerm.fundingAttribution.qualifiedName()));
    assertFalse(coreTerms.containsKey("provenance_fk"));
  }

  // ---- survey-protocol: protocols reached only via the event's survey ----

  @Test
  void surveyProtocolJunction_reachesSamplingProtocolViaSurveyEventLink() {
    // event -> survey (event_fk) -> survey-protocol (survey_fk) -> protocol
    // no direct event-protocol link at all — this is exactly the previously-unmapped gap.
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> surveyDf = surveyDf(List.of(RowFactory.create("SPK-1", "EPK-001", "1", "Clear")));
    Dataset<Row> surveyProtocols = surveyProtocolDf(List.of(RowFactory.create("SPK-1", "PPK-1")));
    Dataset<Row> protocols =
        protocolDf(List.of(RowFactory.create("PPK-1", "Vegetation plot (Rel\u00e9v\u00e9)")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf,
                    "survey", surveyDf,
                    "survey-protocol", surveyProtocols,
                    "protocol", protocols))
            .collectAsList();

    assertEquals(1, records.size());
    assertEquals(
        "Vegetation plot (Rel\u00e9v\u00e9)",
        records.get(0).getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
  }

  @Test
  void surveyProtocolAndEventProtocol_bothMergeIntoSamplingProtocol() {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> surveyDf = surveyDf(List.of(RowFactory.create("SPK-1", "EPK-001", "1", "Clear")));
    Dataset<Row> surveyProtocols = surveyProtocolDf(List.of(RowFactory.create("SPK-1", "PPK-1")));
    Dataset<Row> eventProtocols = eventProtocolDf(List.of(RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> protocols =
        protocolDf(
            List.of(
                RowFactory.create("PPK-1", "Vegetation plot (Rel\u00e9v\u00e9)"),
                RowFactory.create(
                    "PPK-2", "2 observers sampled until no new species found for >=5 min")));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf,
                    "survey", surveyDf,
                    "survey-protocol", surveyProtocols,
                    "event-protocol", eventProtocols,
                    "protocol", protocols))
            .collectAsList();

    assertEquals(
        "2 observers sampled until no new species found for >=5 min"
            + "|Vegetation plot (Rel\u00e9v\u00e9)",
        records.get(0).getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
  }

  @Test
  void georeferenceTypedEventProtocol_alsoCoalescesIntoGeoreferenceProtocol() {
    // A protocol linked via the plain event-protocol junction (not the dedicated
    // georeferenceProtocol_fk field) but classified as protocolType "georeference"/
    // "georeferencing" must additionally reach dwc:georeferenceProtocol, while still
    // contributing to dwc:samplingProtocol like every other linked protocol.
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> eventProtocols =
        eventProtocolDf(
            List.of(RowFactory.create("EPK-001", "PPK-1"), RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> protocols =
        namedProtocolDf(
            List.of(
                RowFactory.create("PPK-1", "georeferencing", "Hand-held GPS receiver", null),
                RowFactory.create(
                    "PPK-2", "sampling", "Vegetation plot (Rel\u00e9v\u00e9)", null)));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf, "event-protocol", eventProtocols, "protocol", protocols))
            .collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals(
        "georeferencing: Hand-held GPS receiver",
        coreTerms.get(DwcTerm.georeferenceProtocol.qualifiedName()));
    assertEquals(
        "georeferencing: Hand-held GPS receiver|sampling: Vegetation plot (Rel\u00e9v\u00e9)",
        coreTerms.get(DwcTerm.samplingProtocol.qualifiedName()));
  }

  @Test
  void georeferenceTypedSurveyProtocol_alsoCoalescesIntoGeoreferenceProtocol() {
    // Reproduces the exact scenario reported: a "georeference"-typed protocol reachable only
    // through event -> survey -> survey-protocol (no direct event-protocol link at all) must
    // still reach dwc:georeferenceProtocol, alongside the sampling/samplingEffort protocols
    // contributing to dwc:samplingProtocol.
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> surveyDf = surveyDf(List.of(RowFactory.create("SPK-1", "EPK-001", "1", "Clear")));
    Dataset<Row> surveyProtocols =
        surveyProtocolDf(
            List.of(
                RowFactory.create("SPK-1", "PPK-1"),
                RowFactory.create("SPK-1", "PPK-2"),
                RowFactory.create("SPK-1", "PPK-3")));
    Dataset<Row> protocols =
        namedProtocolDf(
            List.of(
                RowFactory.create("PPK-1", "georeferencing", "Hand-held GPS receiver", null),
                RowFactory.create("PPK-2", "sampling", "Vegetation plot (Rel\u00e9v\u00e9)", null),
                RowFactory.create(
                    "PPK-3",
                    "samplingEffort",
                    "2 observers sampled until no new species found in the plot for >=5 min",
                    null)));

    List<ExtendedRecord> records =
        EventCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "event", eventDf,
                    "survey", surveyDf,
                    "survey-protocol", surveyProtocols,
                    "protocol", protocols))
            .collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals(
        "georeferencing: Hand-held GPS receiver",
        coreTerms.get(DwcTerm.georeferenceProtocol.qualifiedName()));
    assertEquals(
        "georeferencing: Hand-held GPS receiver"
            + "|samplingEffort: 2 observers sampled until no new species found in the plot for"
            + " >=5 min"
            + "|sampling: Vegetation plot (Rel\u00e9v\u00e9)",
        coreTerms.get(DwcTerm.samplingProtocol.qualifiedName()));
  }

  // ---- Agent: recordedByID/georeferencedByID/eventConductedByID resolution ----

  @Test
  void eventConductedByIdAndGeoreferencedById_resolvedFromAgentTable() {
    StructType eventSchema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventConductedByID", DataTypes.StringType)
            .add("georeferencedByID", DataTypes.StringType);
    Dataset<Row> eventDf =
        spark.createDataFrame(
            List.of(RowFactory.create("EVT001", "AGT-001", "AGT-002")), eventSchema);
    StructType agentSchema =
        new StructType()
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType);
    Dataset<Row> agentDf =
        spark.createDataFrame(
            List.of(
                RowFactory.create("AGT-001", "Jane Doe"), RowFactory.create("AGT-002", "John Roe")),
            agentSchema);

    List<ExtendedRecord> records =
        EventCoreBuilder.build(spark, TestTableLoader.of("event", eventDf, "agent", agentDf))
            .collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    // eventConductedBy/eventConductedByID are DwC-DP's names for what DwC-A calls
    // recordedBy/recordedByID on the event table (see DwcDpTermMappings) — the agent resolution
    // fills the raw eventConductedBy* column, and that rename happens afterwards during term
    // mapping, so the resolved value surfaces under dwc:recordedBy here.
    assertEquals("Jane Doe", coreTerms.get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals("John Roe", coreTerms.get(DwcTerm.georeferencedBy.qualifiedName()));
    // the ID fields themselves are real DwC terms and must survive alongside the resolved names
    assertEquals("AGT-001", coreTerms.get(DwcTerm.recordedByID.qualifiedName()));
    assertEquals("AGT-002", coreTerms.get(DwcTerm.georeferencedByID.qualifiedName()));
  }
}
