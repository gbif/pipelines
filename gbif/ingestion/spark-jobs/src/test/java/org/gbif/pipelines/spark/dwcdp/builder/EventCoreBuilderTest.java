package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpRowTypes;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
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
            "event", eventDf,
            "occurrence", occurrenceDf,
            MediaExtensionBuilder.TABLE_MEDIA, mediaDf,
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA, occMediaDf))
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
}
