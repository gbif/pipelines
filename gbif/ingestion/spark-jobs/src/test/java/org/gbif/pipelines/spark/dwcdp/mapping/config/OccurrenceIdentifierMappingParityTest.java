package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
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
class OccurrenceIdentifierMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceIdentifierMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withIdentifiers(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directAndMaterialIdentifiersMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence("OPK-1", "OCC001"),
            "material", material("MEPK-1", "OCC001"),
            "occurrence-identifier", occurrenceIdentifiers(RowFactory.create("OPK-1", "urn:occ:1", "internal")),
            "material-identifier", materialIdentifiers(RowFactory.create("MEPK-1", "urn:material:1", "catalogue")));

    List<Map<String, String>> legacy = normalizeLegacy(identifiers(only(OccurrenceCoreBuilder.build(spark, loader).collectAsList())));
    List<Map<String, String>> mapped = identifiers(only(engine.execute(loader, plan).collectAsList()));

    String identifier = TermResolver.resolve("identifier");
    Comparator<Map<String, String>> byIdentifier = Comparator.comparing(row -> row.get(identifier));
    assertEquals(legacy.stream().sorted(byIdentifier).toList(), mapped.stream().sorted(byIdentifier).toList());
  }

  @Test
  void ambiguousMaterialSuppressesOnlyMaterialIdentifierBranch() {
    Dataset<Row> materials =
        spark.createDataFrame(
            List.of(RowFactory.create("MEPK-1", "OCC001"), RowFactory.create("MEPK-2", "OCC001")),
            new StructType()
                .add("materialEntity_pk", DataTypes.StringType)
                .add("evidenceForOccurrenceID", DataTypes.StringType));
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence("OPK-1", "OCC001"),
            "material", materials,
            "occurrence-identifier", occurrenceIdentifiers(RowFactory.create("OPK-1", "urn:occ:1", "internal")),
            "material-identifier", materialIdentifiers(RowFactory.create("MEPK-1", "urn:material:1", "catalogue")));

    List<Map<String, String>> legacy = normalizeLegacy(identifiers(only(OccurrenceCoreBuilder.build(spark, loader).collectAsList())));
    List<Map<String, String>> mapped = identifiers(only(engine.execute(loader, plan).collectAsList()));

    assertEquals(1, mapped.size());
    assertEquals(legacy, mapped);
    assertEquals("urn:occ:1", mapped.get(0).get(TermResolver.resolve("identifier")));
  }

  @Test
  void absentIdentifierTablesMeanAbsentExtension() {
    TableLoader loader = TestTableLoader.of("occurrence", occurrence("OPK-1", "OCC001"));

    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());
    assertFalse(mapped.getExtensions().containsKey(IdentifierMapping.ROW_TYPE_IDENTIFIER));
  }

  @Test
  void governingIdentifierFragmentsUseOccurrenceOwnershipAndMaterialGate() {
    CompiledExtension extension =
        engine.compile(plan).extensions().stream()
            .filter(e -> e.rowType().equals(IdentifierMapping.ROW_TYPE_IDENTIFIER))
            .findFirst()
            .orElseThrow();

    assertEquals(2, extension.fragments().size());

    var direct =
        extension.fragments().stream()
            .filter(f -> f.name().equals("occurrence-identifiers"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_fk", direct.scopeKey().column());

    var materialFragment =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-identifiers-for-occurrence"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_pk", materialFragment.scopeKey().column());
    assertEquals("evidenceForOccurrenceID", materialFragment.path().relations().get(0).targetColumn());
  }

  private List<Map<String, String>> identifiers(ExtendedRecord record) {
    List<Map<String, String>> rows = record.getExtensions().get(IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER);
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

  private Dataset<Row> occurrence(String pk, String id) {
    return spark.createDataFrame(
        List.of(RowFactory.create(pk, id)),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> material(String pk, String occurrenceId) {
    return spark.createDataFrame(
        List.of(RowFactory.create(pk, occurrenceId)),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> occurrenceIdentifiers(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType));
  }

  private Dataset<Row> materialIdentifiers(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("identifier", DataTypes.StringType)
            .add("identifierType", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
