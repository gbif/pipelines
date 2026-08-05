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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AgentJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("AgentJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrenceWithRecordedByIdDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("recordedByID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceNoRecordedByIdDf(List<Row> rows) {
    StructType schema = new StructType().add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceWithRecordedByAndIdDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("recordedBy", DataTypes.StringType)
            .add("recordedByID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> agentDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> agentDfNoNameColumn(List<Row> rows) {
    StructType schema = new StructType().add("agentID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-column / absent-table cases ----

  @Test
  void resolveAgentName_idColumnAbsent_returnsDfUnchanged() {
    Dataset<Row> occ = occurrenceNoRecordedByIdDf(List.of(RowFactory.create("OCC001")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(), occ, "recordedByID", "recordedBy");

    assertEquals(occ.columns().length, result.columns().length);
  }

  @Test
  void resolveAgentName_agentTableAbsent_idColumnKeptNameColumnNotCreated() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(), occ, "recordedByID", "recordedBy");

    assertTrue(Arrays.asList(result.columns()).contains("recordedByID"));
    assertFalse(
        Arrays.asList(result.columns()).contains("recordedBy"),
        "no name column should be created when the agent table is absent entirely");
    assertEquals("AGT-001", result.first().getAs("recordedByID"));
  }

  @Test
  void resolveAgentName_agentTableMissingAgentIdColumn_returnsDfUnchanged() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));
    StructType schema = new StructType().add("preferredAgentName", DataTypes.StringType);
    Dataset<Row> agent = spark.createDataFrame(List.of(RowFactory.create("Jane Doe")), schema);

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertEquals(occ.columns().length, result.columns().length);
  }

  // ---- idColumn is always preserved verbatim ----

  @Test
  void resolveAgentName_idColumnNeverDroppedOrRenamed() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertTrue(Arrays.asList(result.columns()).contains("recordedByID"));
    Row row = result.first();
    assertEquals("AGT-001", row.getAs("recordedByID"));
    assertEquals("Jane Doe", row.getAs("recordedBy"));
  }

  // ---- coalesce semantics ----

  @Test
  void resolveAgentName_existingRecordedByPresent_neverOverwritten() {
    Dataset<Row> occ =
        occurrenceWithRecordedByAndIdDf(
            List.of(RowFactory.create("OCC001", "Publisher's own free text", "AGT-001")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertEquals(
        "Publisher's own free text",
        result.first().getAs("recordedBy"),
        "existing publisher-supplied text must win over the resolved agent name");
  }

  @Test
  void resolveAgentName_existingRecordedByNull_filledFromResolvedAgent() {
    Dataset<Row> occ =
        occurrenceWithRecordedByAndIdDf(List.of(RowFactory.create("OCC001", null, "AGT-001")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertEquals("Jane Doe", result.first().getAs("recordedBy"));
  }

  @Test
  void resolveAgentName_targetColumnDoesNotExistYet_createdFromResolvedValue() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertEquals("Jane Doe", result.first().getAs("recordedBy"));
  }

  @Test
  void resolveAgentName_noMatchingAgentRow_survivesWithNullName() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-UNKNOWN")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertEquals(1L, result.count(), "occurrence row must survive left join even with no match");
    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("recordedBy")));
    assertEquals("AGT-UNKNOWN", row.getAs("recordedByID"));
  }

  @Test
  void resolveAgentName_agentTableMissingPreferredName_nameResolvesToNull() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));
    Dataset<Row> agent = agentDfNoNameColumn(List.of(RowFactory.create("AGT-001")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("recordedBy")));
  }

  @Test
  void resolveAgentName_temporaryColumnsNeverLeakIntoResult() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-001", "Jane Doe")));

    Dataset<Row> result =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            TestTableLoader.of(AgentJoinBuilder.TABLE_AGENT, agent),
            occ,
            "recordedByID",
            "recordedBy");

    assertFalse(
        Arrays.stream(result.columns())
            .anyMatch(c -> c.contains("__agent_join_id") || c.contains("__resolved_agent_name")),
        "internal join/coalesce temp columns must not survive into the result");
  }

  // ---- computeFunnel ----

  @Test
  void computeFunnel_coreTableAbsent_returnsEmpty() {
    var result =
        AgentJoinBuilder.computeFunnel(
            TestTableLoader.of(), "occurrence", "recordedByID", "recordedBy");

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_idColumnAbsent_returnsEmpty() {
    Dataset<Row> occ = occurrenceNoRecordedByIdDf(List.of(RowFactory.create("OCC001")));

    var result =
        AgentJoinBuilder.computeFunnel(
            TestTableLoader.of("occurrence", occ), "occurrence", "recordedByID", "recordedBy");

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_noCandidates_singleZeroBucket() {
    Dataset<Row> occ = occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", null)));

    var result =
        AgentJoinBuilder.computeFunnel(
            TestTableLoader.of("occurrence", occ), "occurrence", "recordedByID", "recordedBy");

    assertTrue(result.isPresent());
    assertEquals(1, result.get().buckets().size());
    assertEquals(0L, result.get().buckets().get(0).count());
  }

  @Test
  void computeFunnel_agentTableAbsent_allCandidatesUnresolvedBucket() {
    Dataset<Row> occ =
        occurrenceWithRecordedByIdDf(List.of(RowFactory.create("OCC001", "AGT-001")));

    var result =
        AgentJoinBuilder.computeFunnel(
            TestTableLoader.of("occurrence", occ), "occurrence", "recordedByID", "recordedBy");

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(3, buckets.size());
    assertEquals(1L, buckets.get(0).count(), "candidates");
    assertEquals(0L, buckets.get(1).count(), "already had recordedBy");
    assertEquals(1L, buckets.get(2).count(), "agent table absent, unresolved");
  }

  @Test
  void computeFunnel_mixOfAlreadyHadResolvedAndUnresolved() {
    Dataset<Row> occ =
        occurrenceWithRecordedByAndIdDf(
            List.of(
                RowFactory.create("OCC001", "Publisher text", "AGT-001"),
                RowFactory.create("OCC002", null, "AGT-002"),
                RowFactory.create("OCC003", null, "AGT-UNKNOWN"),
                RowFactory.create("OCC004", null, null)));
    Dataset<Row> agent = agentDf(List.of(RowFactory.create("AGT-002", "Jane Doe")));

    var result =
        AgentJoinBuilder.computeFunnel(
            TestTableLoader.of("occurrence", occ, AgentJoinBuilder.TABLE_AGENT, agent),
            "occurrence",
            "recordedByID",
            "recordedBy");

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(4, buckets.size());
    assertEquals(3L, buckets.get(0).count(), "candidates (recordedByID set) — OCC004 excluded");
    assertEquals(1L, buckets.get(1).count(), "already had recordedBy — OCC001");
    assertEquals(1L, buckets.get(2).count(), "resolved, filled recordedBy — OCC002");
    assertEquals(1L, buckets.get(3).count(), "no matching agentID, unresolved — OCC003");
  }
}
