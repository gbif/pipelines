package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
class ProtocolJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("ProtocolJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventWithProtocolFkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventProtocol_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventNoProtocolFkDf(List<Row> rows) {
    StructType schema = new StructType().add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventWithGeoreferenceProtocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("georeferenceProtocol", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> protocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> eventProtocolJunctionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("event_fk", DataTypes.StringType)
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

  // ---- resolveProtocolFk: absent-column / absent-table cases ----

  @Test
  void resolveProtocolFk_columnAbsent_returnsDfUnchanged() {
    Dataset<Row> event = eventNoProtocolFkDf(List.of(RowFactory.create("EVT001")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFk(
            TestTableLoader.of(), event, "eventProtocol_fk", "samplingProtocol");

    assertEquals(event.columns().length, result.columns().length);
  }

  @Test
  void resolveProtocolFk_protocolTableAbsent_rawFkValueKeptUnderNewName() {
    Dataset<Row> event = eventWithProtocolFkDf(List.of(RowFactory.create("EVT001", "PROTO-001")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFk(
            TestTableLoader.of(), event, "eventProtocol_fk", "samplingProtocol");

    assertFalse(Arrays.asList(result.columns()).contains("eventProtocol_fk"));
    assertTrue(Arrays.asList(result.columns()).contains("samplingProtocol"));
    Row row = result.first();
    assertEquals("PROTO-001", row.getAs("samplingProtocol"));
  }

  @Test
  void resolveProtocolFk_protocolTablePresent_resolvedDescriptionUnderNewName() {
    Dataset<Row> event = eventWithProtocolFkDf(List.of(RowFactory.create("EVT001", "PROTO-001")));
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Point count survey")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFk(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "eventProtocol_fk",
            "samplingProtocol");

    assertFalse(Arrays.asList(result.columns()).contains("eventProtocol_fk"));
    Row row = result.first();
    assertEquals("Point count survey", row.getAs("samplingProtocol"));
  }

  @Test
  void resolveProtocolFk_protocolNameUsesTypedDisplayLabel() {
    Dataset<Row> event = eventWithProtocolFkDf(List.of(RowFactory.create("EVT001", "PROTO-001")));
    Dataset<Row> protocol =
        namedProtocolDf(
            List.of(
                RowFactory.create(
                    "PROTO-001", "sampling", "Point count survey", "Longer description")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFk(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "eventProtocol_fk",
            "samplingProtocol");

    assertEquals("sampling: Point count survey", result.first().getAs("samplingProtocol"));
  }

  @Test
  void resolveProtocolFk_noMatchingProtocolRow_survivesWithNullValue() {
    Dataset<Row> event =
        eventWithProtocolFkDf(List.of(RowFactory.create("EVT001", "PROTO-UNKNOWN")));
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Point count survey")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFk(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "eventProtocol_fk",
            "samplingProtocol");

    assertEquals(1L, result.count(), "event row must survive left join even with no match");
    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("samplingProtocol")));
  }

  // ---- resolveProtocolFkCoalesceInto ----

  @Test
  void coalesceInto_columnAbsent_returnsDfUnchanged() {
    Dataset<Row> event = eventNoProtocolFkDf(List.of(RowFactory.create("EVT001")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(), event, "georeferenceProtocol_fk", "georeferenceProtocol");

    assertEquals(event.columns().length, result.columns().length);
  }

  @Test
  void coalesceInto_existingValuePresent_neverOverwritten() {
    Dataset<Row> event =
        eventWithGeoreferenceProtocolDf(
            List.of(RowFactory.create("EVT001", "Publisher's own free text", "PROTO-001")));
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Resolved protocol text")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "georeferenceProtocol_fk",
            "georeferenceProtocol");

    Row row = result.first();
    assertEquals(
        "Publisher's own free text",
        row.getAs("georeferenceProtocol"),
        "existing publisher-supplied text must win over the resolved FK value");
  }

  @Test
  void coalesceInto_existingValueNull_filledFromResolvedProtocol() {
    Dataset<Row> event =
        eventWithGeoreferenceProtocolDf(List.of(RowFactory.create("EVT001", null, "PROTO-001")));
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Resolved protocol text")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "georeferenceProtocol_fk",
            "georeferenceProtocol");

    Row row = result.first();
    assertEquals("Resolved protocol text", row.getAs("georeferenceProtocol"));
  }

  @Test
  void coalesceInto_georeferenceProtocolUsesTypedProtocolName() {
    Dataset<Row> event =
        eventWithGeoreferenceProtocolDf(List.of(RowFactory.create("EVT001", null, "PROTO-001")));
    Dataset<Row> protocol =
        namedProtocolDf(
            List.of(
                RowFactory.create("PROTO-001", "georeferencing", "Hand-held GPS receiver", null)));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "georeferenceProtocol_fk",
            "georeferenceProtocol");

    assertEquals(
        "georeferencing: Hand-held GPS receiver", result.first().getAs("georeferenceProtocol"));
  }

  @Test
  void coalesceInto_existingValueNullAndProtocolTableAbsent_filledFromRawFkFallback() {
    Dataset<Row> event =
        eventWithGeoreferenceProtocolDf(List.of(RowFactory.create("EVT001", null, "PROTO-001")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(), event, "georeferenceProtocol_fk", "georeferenceProtocol");

    Row row = result.first();
    assertEquals(
        "PROTO-001",
        row.getAs("georeferenceProtocol"),
        "raw FK value is the fallback when the protocol table is absent, same policy as "
            + "AssertionExtensionBuilder's assertionProtocol_fk fallback");
  }

  @Test
  void coalesceInto_temporaryColumnNeverLeaksIntoResult() {
    Dataset<Row> event =
        eventWithGeoreferenceProtocolDf(List.of(RowFactory.create("EVT001", null, "PROTO-001")));
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Resolved protocol text")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "georeferenceProtocol_fk",
            "georeferenceProtocol");

    assertFalse(
        Arrays.stream(result.columns()).anyMatch(c -> c.contains("__resolved")),
        "the internal temp column used for coalescing must not survive into the result");
    assertFalse(Arrays.asList(result.columns()).contains("georeferenceProtocol_fk"));
  }

  @Test
  void coalesceInto_targetColumnDoesNotExistYet_createdFromResolvedValue() {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("georeferenceProtocol_fk", DataTypes.StringType);
    Dataset<Row> event =
        spark.createDataFrame(List.of(RowFactory.create("EVT001", "PROTO-001")), schema);
    Dataset<Row> protocol =
        protocolDf(List.of(RowFactory.create("PROTO-001", "Resolved protocol text")));

    Dataset<Row> result =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            TestTableLoader.of(ProtocolJoinBuilder.TABLE_PROTOCOL, protocol),
            event,
            "georeferenceProtocol_fk",
            "georeferenceProtocol");

    Row row = result.first();
    assertEquals("Resolved protocol text", row.getAs("georeferenceProtocol"));
  }

  // ---- aggregateJunctionProtocolDescriptions: protocolType filter ----

  @Test
  void aggregateWithTypeFilter_onlyMatchingTypeContributes() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction =
        eventProtocolJunctionDf(
            List.of(RowFactory.create("EPK-001", "PPK-1"), RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> protocol =
        namedProtocolDf(
            List.of(
                RowFactory.create("PPK-1", "georeferencing", "Hand-held GPS receiver", null),
                RowFactory.create("PPK-2", "sampling", "Vegetation plot", null)));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of(
                ProtocolJoinBuilder.TABLE_PROTOCOL, protocol, "event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);

    assertTrue(result.isPresent());
    Row row = result.get().first();
    assertEquals("EVT001", row.getAs("eventID"));
    assertEquals(
        "georeferencing: Hand-held GPS receiver",
        row.getAs("__aggregated_protocol_descriptions"),
        "only the georeferencing-typed protocol should contribute; the sampling one must not");
  }

  @Test
  void aggregateWithTypeFilter_caseInsensitiveMatch() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction = eventProtocolJunctionDf(List.of(RowFactory.create("EPK-001", "PPK-1")));
    Dataset<Row> protocol =
        namedProtocolDf(
            List.of(RowFactory.create("PPK-1", "GeoReference", "Hand-held GPS receiver", null)));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of(
                ProtocolJoinBuilder.TABLE_PROTOCOL, protocol, "event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);

    assertTrue(result.isPresent());
    assertEquals(1L, result.get().count());
  }

  @Test
  void aggregateWithTypeFilter_noMatchingType_emptyOptional() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction = eventProtocolJunctionDf(List.of(RowFactory.create("EPK-001", "PPK-1")));
    Dataset<Row> protocol =
        namedProtocolDf(List.of(RowFactory.create("PPK-1", "sampling", "Vegetation plot", null)));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of(
                ProtocolJoinBuilder.TABLE_PROTOCOL, protocol, "event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);

    assertTrue(result.isPresent(), "aggregation runs, it just produces no rows for this event");
    assertEquals(0L, result.get().count());
  }

  @Test
  void aggregateWithTypeFilter_protocolTableAbsent_returnsEmptyOptional() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction = eventProtocolJunctionDf(List.of(RowFactory.create("EPK-001", "PPK-1")));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of("event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);

    assertTrue(
        result.isEmpty(),
        "cannot classify by protocolType without the protocol table; must not guess");
  }

  @Test
  void aggregateWithTypeFilter_protocolTypeColumnAbsent_returnsEmptyOptional() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction = eventProtocolJunctionDf(List.of(RowFactory.create("EPK-001", "PPK-1")));
    Dataset<Row> protocol = protocolDf(List.of(RowFactory.create("PPK-1", "Vegetation plot")));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of(
                ProtocolJoinBuilder.TABLE_PROTOCOL, protocol, "event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);

    assertTrue(result.isEmpty());
  }

  @Test
  void aggregateNoTypeFilter_everyLinkedProtocolContributes() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> junction =
        eventProtocolJunctionDf(
            List.of(RowFactory.create("EPK-001", "PPK-1"), RowFactory.create("EPK-001", "PPK-2")));
    Dataset<Row> protocol =
        namedProtocolDf(
            List.of(
                RowFactory.create("PPK-1", "georeferencing", "Hand-held GPS receiver", null),
                RowFactory.create("PPK-2", "sampling", "Vegetation plot", null)));

    var result =
        ProtocolJoinBuilder.aggregateJunctionProtocolDescriptions(
            TestTableLoader.of(
                ProtocolJoinBuilder.TABLE_PROTOCOL, protocol, "event-protocol", junction),
            "event-protocol",
            "event_fk",
            event,
            "event_pk",
            "eventID",
            null);

    assertTrue(result.isPresent());
    String actual = result.get().first().getAs("__aggregated_protocol_descriptions");
    assertEquals(
        Set.of("georeferencing: Hand-held GPS receiver", "sampling: Vegetation plot"),
        Set.of(actual.split("\\|")));
  }

  @Test
  void georeferenceProtocolTypesConstant_containsBothSpellings() {
    assertEquals(
        Set.of("georeference", "georeferencing"), ProtocolJoinBuilder.GEOREFERENCE_PROTOCOL_TYPES);
  }
}
