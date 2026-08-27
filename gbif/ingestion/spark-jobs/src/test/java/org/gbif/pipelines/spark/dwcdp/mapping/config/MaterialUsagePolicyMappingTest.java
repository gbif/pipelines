package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.ExtensionMaterializationResult;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkExtensionMaterializer;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaterialUsagePolicyMappingTest {

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("MaterialUsagePolicyMappingTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void materialUsagePolicyEnrichesOccurrenceWithRightsFields() {
    org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath occurrencePath =
        org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath.root("occurrence");
    ExtensionFragment base =
        ExtensionFragmentBuilder.extensionFragment(
                "occurrence-test-base", OccurrenceMapping.ROW_TYPE_OCCURRENCE, "occurrence")
            .scopeKey("event_fk")
            .rowIdentity(occurrencePath.field("occurrence_pk"))
            .field(
                TargetFieldMapping.oneOf(
                    DwcTerm.occurrenceID.qualifiedName(),
                    ValueAggregation.firstNonNull(),
                    occurrencePath.field("occurrenceID")))
            .build();

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "occurrence", occurrences(),
                    "material", materials(),
                    "material-usage-policy", materialUsagePolicies(),
                    "usage-policy", usagePolicies()),
                new ExtensionMapping(
                    OccurrenceMapping.ROW_TYPE_OCCURRENCE,
                    List.of(base, OccurrenceMapping.material(graph))));

    List<Row> rows = result.dataset().collectAsList();
    assertEquals(1, rows.size());
    Row row = rows.get(0);
    assertEquals("© Example Museum", row.getAs(result.columnName(TargetTerms.resolve("rights"))));
    assertEquals("Example Museum", row.getAs(result.columnName(TargetTerms.resolve("rightsHolder"))));
    assertEquals("public", row.getAs(result.columnName(TargetTerms.resolve("accessRights"))));
    assertEquals(
        "https://creativecommons.org/licenses/by/4.0/",
        row.getAs(result.columnName(TargetTerms.resolve("license"))));
  }

  private Dataset<Row> occurrences() {
    return spark.createDataFrame(
        List.of(RowFactory.create("OPK-001", "OCC-001", "EPK-001")),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> materials() {
    return spark.createDataFrame(
        List.of(RowFactory.create("MAT-PK-001", "OCC-001")),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> materialUsagePolicies() {
    return spark.createDataFrame(
        List.of(RowFactory.create("MAT-PK-001", "POLICY-001")),
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType));
  }

  private Dataset<Row> usagePolicies() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create(
                "POLICY-001",
                "© Example Museum",
                "Example Museum",
                "Example Museum",
                "public",
                "https://creativecommons.org/licenses/by/4.0/")),
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("rights", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType)
            .add("owner", DataTypes.StringType)
            .add("accessRights", DataTypes.StringType)
            .add("license", DataTypes.StringType));
  }
}
