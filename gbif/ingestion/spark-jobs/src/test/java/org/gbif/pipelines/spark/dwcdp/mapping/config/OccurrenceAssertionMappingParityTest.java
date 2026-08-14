package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Comparator;
import java.util.LinkedHashMap;
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
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceAssertionMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceAssertionMappingParityTest").getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withAssertions(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directOccurrenceAssertionsMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            "occurrence-assertion",
            occurrenceAssertions(
                assertion("A001", "OPK-1", "Temperature", "25.0", "C", null),
                assertion("A002", "OPK-1", "Humidity", "80", "%", null)));

    assertPayloadParity(loader, 2);
  }

  @Test
  void materialAssertionsMatchLegacyForExactlyOneEvidenceMaterial() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            "material-assertion",
            materialAssertions(
                assertion("A001", "MEPK-1", "Wing chord", "85.5", "mm", null)));

    assertPayloadParity(loader, 1);
  }

  @Test
  void directAndMaterialAssertionsAreUnionedWithoutCartesianProduct() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            "occurrence-assertion",
            occurrenceAssertions(
                assertion("A001", "OPK-1", "Temperature", "25.0", "C", null),
                assertion("A002", "OPK-1", "Humidity", "80", "%", null)),
            "material-assertion",
            materialAssertions(
                assertion("A003", "MEPK-1", "Wing chord", "85.5", "mm", null),
                assertion("A004", "MEPK-1", "Mass", "12.3", "g", null)));

    assertPayloadParity(loader, 4);
  }

  @Test
  void materialAssertionProtocolUsesDescriptionAndRawFallback() {
    TableLoader withProtocol =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            "material-assertion",
            materialAssertions(
                assertion("A001", "MEPK-1", "Wing chord", "85.5", "mm", "PROTO-1")),
            "protocol", protocol("PROTO-1", "Standard wing chord measurement"));

    assertPayloadParity(withProtocol, 1);
    assertEquals(
        "Standard wing chord measurement",
        mappedAssertions(withProtocol).get(0).get(DwcTerm.measurementMethod.qualifiedName()));

    TableLoader fallback =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            "material-assertion",
            materialAssertions(
                assertion("A001", "MEPK-1", "Wing chord", "85.5", "mm", "PROTO-RAW")));

    assertPayloadParity(fallback, 1);
    assertEquals(
        "PROTO-RAW",
        mappedAssertions(fallback).get(0).get(DwcTerm.measurementMethod.qualifiedName()));
  }

  @Test
  void ambiguousEvidenceMaterialSuppressesMaterialAssertions() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC001")),
            "material-assertion",
            materialAssertions(
                assertion("A001", "MEPK-1", "Wing chord", "85.5", "mm", null)));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());
    assertFalse(
        legacy
            .getExtensions()
            .containsKey(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
    assertFalse(
        mapped
            .getExtensions()
            .containsKey(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT));
  }

  @Test
  void governingAssertionFragmentsUseOccurrenceOwnershipAndMaterialGate() {
    CompiledExtension extension =
        engine.compile(plan).extensions().stream()
            .filter(e -> e.rowType().equals(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT))
            .findFirst()
            .orElseThrow();

    assertEquals(2, extension.fragments().size());

    var direct =
        extension.fragments().stream()
            .filter(f -> f.name().equals("occurrence-assertions"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_fk", direct.scopeKey().column());
    assertEquals("protocol", direct.path().relations().get(0).targetResource());

    var material =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-assertions-for-occurrence"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_pk", material.scopeKey().column());
    assertEquals("evidenceForOccurrenceID", material.path().relations().get(0).targetColumn());
    assertEquals("material-assertion", material.path().relations().get(1).targetResource());
  }

  private void assertPayloadParity(TableLoader loader, int expectedRows) {
    List<Map<String, String>> legacy = normalizeLegacy(legacyAssertions(loader));
    List<Map<String, String>> mapped = mappedAssertions(loader);
    assertEquals(expectedRows, legacy.size());
    assertEquals(expectedRows, mapped.size());

    String measurementId = DwcTerm.measurementID.qualifiedName();
    Comparator<Map<String, String>> byId =
        Comparator.comparing(row -> row.get(measurementId), Comparator.nullsFirst(String::compareTo));
    assertEquals(legacy.stream().sorted(byId).toList(), mapped.stream().sorted(byId).toList());
  }

  private List<Map<String, String>> legacyAssertions(TableLoader loader) {
    ExtendedRecord record = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows =
        record
            .getExtensions()
            .get(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedAssertions(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows =
        record.getExtensions().get(AssertionMapping.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> normalizeLegacy(List<Map<String, String>> rows) {
    String occurrenceId = TermResolver.resolve("occurrenceID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(occurrenceId);
              return normalized;
            })
        .toList();
  }

  private Dataset<Row> occurrence() {
    return spark.createDataFrame(
        List.of(RowFactory.create("OPK-1", "OCC001")),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> occurrenceAssertions(Row... rows) {
    return spark.createDataFrame(List.of(rows), assertionSchema("occurrence_fk"));
  }

  private Dataset<Row> materialAssertions(Row... rows) {
    return spark.createDataFrame(List.of(rows), assertionSchema("materialEntity_fk"));
  }

  private StructType assertionSchema(String parentFk) {
    return new StructType()
        .add("assertionID", DataTypes.StringType)
        .add(parentFk, DataTypes.StringType)
        .add("assertionType", DataTypes.StringType)
        .add("assertionTypeIRI", DataTypes.StringType)
        .add("assertionValue", DataTypes.StringType)
        .add("assertionValueIRI", DataTypes.StringType)
        .add("assertionUnit", DataTypes.StringType)
        .add("assertionUnitIRI", DataTypes.StringType)
        .add("assertionError", DataTypes.StringType)
        .add("assertionBy", DataTypes.StringType)
        .add("assertionMadeDate", DataTypes.StringType)
        .add("assertionRemarks", DataTypes.StringType)
        .add("assertionProtocol_fk", DataTypes.StringType);
  }

  private Row assertion(
      String id, String parentFk, String type, String value, String unit, String protocolFk) {
    return RowFactory.create(
        id,
        parentFk,
        type,
        null,
        value,
        null,
        unit,
        null,
        null,
        null,
        null,
        null,
        protocolFk);
  }

  private Dataset<Row> material(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> protocol(String pk, String description) {
    return spark.createDataFrame(
        List.of(RowFactory.create(pk, description)),
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
