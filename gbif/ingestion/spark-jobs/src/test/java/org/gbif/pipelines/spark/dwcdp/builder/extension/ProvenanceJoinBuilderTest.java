package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Fixture convention: {@code provenance_fk} values (both direct-on-event and on the
 * event-provenance junction table) always match a {@code provenance_pk} (surrogate) value, never a
 * {@code provenanceID} (natural) one — the same distinction as {@code occurrence.event_fk} matching
 * {@code event.event_pk} rather than {@code event.eventID} elsewhere in this codebase.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProvenanceJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("ProvenanceJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventWithDirectFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventNoLinksDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> provenanceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType)
            .add("fundingAttributionID", DataTypes.StringType)
            .add("projectID", DataTypes.StringType)
            .add("projectTitle", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventProvenanceJunctionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table / absent-column cases ----

  @Test
  void provenanceTableAbsent_returnsEventDfUnchanged() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));

    Dataset<Row> result = ProvenanceJoinBuilder.enrichEvents(TestTableLoader.of(), event);

    assertEquals(event.columns().length, result.columns().length);
  }

  @Test
  void noDirectFkAndNoJunctionTable_eventSurvivesWithNoAggregatedColumns() {
    Dataset<Row> event = eventNoLinksDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create(
                    "PPK-1", "PROV-1", "NSF Grant 123", "FID-1", "PID-1", "Survey X")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), event);

    assertEquals(1L, result.count());
    assertFalse(Arrays.asList(result.columns()).contains("fundingAttribution"));
  }

  // ---- direct FK ----

  @Test
  void directFk_singleLinkedProvenance_fieldsAggregated() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create(
                    "PPK-1", "PROV-1", "NSF Grant 123", "FID-1", "PID-1", "Survey X")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), event);

    Row row = result.first();
    assertEquals("NSF Grant 123", row.getAs("fundingAttribution"));
    assertEquals("FID-1", row.getAs("fundingAttributionID"));
    assertEquals("PID-1", row.getAs("projectID"));
    assertEquals("Survey X", row.getAs("projectTitle"));
  }

  @Test
  void directFkColumn_neverLeaksIntoResult() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create(
                    "PPK-1", "PROV-1", "NSF Grant 123", "FID-1", "PID-1", "Survey X")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), event);

    assertFalse(Arrays.asList(result.columns()).contains("provenance_fk"));
  }

  // ---- deterministic sort by provenanceID ----

  @Test
  void multipleLinkedProvenanceRecords_sortedByProvenanceIdRegardlessOfInputOrder() {
    Dataset<Row> event = eventNoLinksDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    // Deliberately inserted out of provenanceID order (PROV-3, PROV-1, PROV-2) to confirm the
    // output order is driven by sorting, not by row/partition order. Junction FK values (PPK-*)
    // match provenance_pk, not the provenanceID values used for sort-order verification below.
    Dataset<Row> junction =
        eventProvenanceJunctionDf(
            List.of(
                RowFactory.create("EPK-001", "PPK-3"),
                RowFactory.create("EPK-001", "PPK-1"),
                RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-3", "PROV-3", "Grant C", "FID-C", "PID-C", "Project C"),
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A"),
                RowFactory.create("PPK-2", "PROV-2", "Grant B", "FID-B", "PID-B", "Project B")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(
                "event",
                event,
                ProvenanceJoinBuilder.TABLE_PROVENANCE,
                provenance,
                ProvenanceJoinBuilder.TABLE_EVENT_PROVENANCE,
                junction),
            event);

    Row row = result.first();
    assertEquals("Grant A|Grant B|Grant C", row.getAs("fundingAttribution"));
    assertEquals("Project A|Project B|Project C", row.getAs("projectTitle"));
  }

  @Test
  void repeatedRuns_produceIdenticalOrder() {
    // Same setup as above, run twice independently — guards against the sort being an accident
    // of a particular execution rather than a real, enforced ordering.
    Dataset<Row> event = eventNoLinksDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction =
        eventProvenanceJunctionDf(
            List.of(
                RowFactory.create("EPK-001", "PPK-3"),
                RowFactory.create("EPK-001", "PPK-1"),
                RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-3", "PROV-3", "Grant C", "FID-C", "PID-C", "Project C"),
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A"),
                RowFactory.create("PPK-2", "PROV-2", "Grant B", "FID-B", "PID-B", "Project B")));

    String first =
        ProvenanceJoinBuilder.enrichEvents(
                TestTableLoader.of(
                    "event",
                    event,
                    ProvenanceJoinBuilder.TABLE_PROVENANCE,
                    provenance,
                    ProvenanceJoinBuilder.TABLE_EVENT_PROVENANCE,
                    junction),
                event)
            .first()
            .getAs("fundingAttribution");
    String second =
        ProvenanceJoinBuilder.enrichEvents(
                TestTableLoader.of(
                    "event",
                    event,
                    ProvenanceJoinBuilder.TABLE_PROVENANCE,
                    provenance,
                    ProvenanceJoinBuilder.TABLE_EVENT_PROVENANCE,
                    junction),
                event)
            .first()
            .getAs("fundingAttribution");

    assertEquals(first, second);
    assertEquals("Grant A|Grant B|Grant C", first);
  }

  // ---- direct FK + junction table combined, deduplicated ----

  @Test
  void directFkAndJunctionTable_unionedAndDeduplicated() {
    // Direct FK points at PPK-1 (surrogate for PROV-1); junction table ALSO links PPK-1
    // (duplicate, must not double up) plus PPK-2 (new link via the junction table only).
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));
    Dataset<Row> junction =
        eventProvenanceJunctionDf(
            List.of(RowFactory.create("EPK-001", "PPK-1"), RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A"),
                RowFactory.create("PPK-2", "PROV-2", "Grant B", "FID-B", "PID-B", "Project B")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(
                "event",
                event,
                ProvenanceJoinBuilder.TABLE_PROVENANCE,
                provenance,
                ProvenanceJoinBuilder.TABLE_EVENT_PROVENANCE,
                junction),
            event);

    Row row = result.first();
    assertEquals(
        "Grant A|Grant B",
        row.getAs("fundingAttribution"),
        "PPK-1 must appear once, not twice, despite being linked both directly and via the "
            + "junction table");
  }

  // ---- empty vs absent ----

  @Test
  void allLinkedProvenanceHaveNullField_resultIsNullNotEmptyString() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(RowFactory.create("PPK-1", "PROV-1", null, null, "PID-1", "Project A")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), event);

    Row row = result.first();
    assertTrue(
        row.isNullAt(row.fieldIndex("fundingAttribution")),
        "must be null, not an empty string, when every linked provenance record has this field null");
    assertEquals("Project A", row.getAs("projectTitle"));
  }

  @Test
  void eventWithNoMatchingProvenanceRow_survivesWithNullAggregatedFields() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-UNKNOWN")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(
                RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Project A")));

    Dataset<Row> result =
        ProvenanceJoinBuilder.enrichEvents(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance), event);

    assertEquals(1L, result.count(), "event must survive even with a dangling/unmatched FK");
    Row row = result.first();
    assertEquals("EVT001", row.getAs("eventID"));
  }

  // ---- computeFunnel ----

  @Test
  void computeFunnel_provenanceTableAbsent_returnsEmpty() {
    Dataset<Row> event =
        eventWithDirectFkDf(List.of(RowFactory.create("EPK-001", "EVT001", "PPK-1")));

    var result = ProvenanceJoinBuilder.computeFunnel(TestTableLoader.of("event", event));

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_eventTableAbsent_returnsEmpty() {
    Dataset<Row> provenance =
        provenanceDf(
            List.of(RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Proj A")));

    var result =
        ProvenanceJoinBuilder.computeFunnel(
            TestTableLoader.of(ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance));

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_noLinksAtAll_everyEventInNoLinkBucket() {
    Dataset<Row> event = eventNoLinksDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Proj A")));

    var result =
        ProvenanceJoinBuilder.computeFunnel(
            TestTableLoader.of("event", event, ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(2, buckets.size());
    assertEquals(1L, buckets.get(0).count(), "events (total)");
    assertEquals(1L, buckets.get(1).count(), "no provenance link");
  }

  @Test
  void computeFunnel_mixOfAttributedDanglingAndUnlinked() {
    Dataset<Row> event =
        eventWithDirectFkDf(
            List.of(
                RowFactory.create("EPK-001", "EVT001", "PPK-1"),
                RowFactory.create("EPK-002", "EVT002", "PPK-UNKNOWN"),
                RowFactory.create("EPK-003", "EVT003", null)));
    Dataset<Row> provenance =
        provenanceDf(
            List.of(RowFactory.create("PPK-1", "PROV-1", "Grant A", "FID-A", "PID-A", "Proj A")));

    var result =
        ProvenanceJoinBuilder.computeFunnel(
            TestTableLoader.of("event", event, ProvenanceJoinBuilder.TABLE_PROVENANCE, provenance));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(4, buckets.size());
    assertEquals(3L, buckets.get(0).count(), "events (total)");
    assertEquals(1L, buckets.get(1).count(), "no provenance link — EVT003");
    assertEquals(1L, buckets.get(2).count(), "linked, attribution merged — EVT001");
    assertEquals(1L, buckets.get(3).count(), "linked, all dangling — EVT002");
  }
}
