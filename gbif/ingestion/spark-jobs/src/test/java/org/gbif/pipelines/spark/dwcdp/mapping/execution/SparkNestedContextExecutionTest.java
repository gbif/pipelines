package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedContextDiscoveryFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.NestedExtensionContext;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.InMemorySchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SparkNestedContextExecutionTest {
  private SparkSession spark;
  private SchemaGraph graph;
  private NestedExtensionContext context;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName(getClass().getSimpleName()).getOrCreate();
    graph =
        new InMemorySchemaGraph()
            .resource("parent", "parent_pk")
            .resource("row", "row_pk", "parent_fk", "semantic_context_fk", "publisher_value")
            .resource("context", "context_pk", "semantic_row_fk", "context_value")
            .resource("bridge", "row_fk", "context_fk")
            .relation(
                SchemaRelation.relation(
                    "parent",
                    "parent_pk",
                    "row",
                    "parent_fk",
                    null,
                    RelationCardinality.ONE_TO_MANY))
            .relation(
                SchemaRelation.relation(
                    "row",
                    "semantic_context_fk",
                    "context",
                    "context_pk",
                    null,
                    RelationCardinality.UNKNOWN,
                    true))
            .relation(
                SchemaRelation.relation(
                    "row", "row_pk", "bridge", "row_fk", null, RelationCardinality.ONE_TO_MANY))
            .relation(
                SchemaRelation.relation(
                    "bridge",
                    "context_fk",
                    "context",
                    "context_pk",
                    null,
                    RelationCardinality.MANY_TO_ONE));
    context = syntheticContext(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void sameContextDiscoveredThroughMultiplePathsIsDeduplicated() {
    TableLoader loader =
        loader(
            parents(row("P1")),
            rows(row("R1", "P1", "C1", "publisher")),
            contexts(row("C1", "publisher-row-link", "context-1")),
            bridges(row("R1", "C1")));

    SparkNestedContextDiscovery.Result result =
        new SparkNestedContextDiscovery(graph, context).discover(loader);

    assertRelation(result.ownership(), List.of(List.of("P1", "R1")));
    assertRelation(result.contextRows(), List.of(List.of("P1", "R1", "C1")));
    assertRelation(result.uniqueContext(), List.of(List.of("P1", "R1", "C1")));
  }

  @Test
  void distinctContextsForSameParentAndRowAreAmbiguousAndDoNotEnrich() {
    TableLoader loader =
        loader(
            parents(row("P1")),
            rows(row("R1", "P1", "C1", "publisher")),
            contexts(
                row("C1", "publisher-row-link-1", "context-1"),
                row("C2", "publisher-row-link-2", "context-2")),
            bridges(row("R1", "C2")));

    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(loader);
    assertEquals(2, discovery.contextRows().count());
    assertEquals(0, discovery.uniqueContext().count());

    TableLoader scoped = SparkNestedContextLoader.loader(loader, context, discovery).orElseThrow();
    Dataset<Row> scopedContexts = scoped.load("context").orElseThrow();
    assertEquals(0, scopedContexts.count());

    Dataset<Row> scopedRows = scoped.load("row").orElseThrow();
    Row scopedRow = scopedRows.first();
    String link = nestedLink();
    String navigationKey = scopedRow.getAs(link);
    assertEquals("urn:gbif:dwcdp:nested-context:P1:R1", navigationKey);
  }

  @Test
  void oneContextForMultipleLogicalRowsGetsIndependentNavigationKeys() {
    TableLoader loader =
        loader(
            parents(row("P1")),
            rows(row("R1", "P1", "C1", "publisher-1"), row("R2", "P1", "C1", "publisher-2")),
            contexts(row("C1", "publisher-row-link", "shared-context")),
            bridges());

    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(loader);
    TableLoader scoped = SparkNestedContextLoader.loader(loader, context, discovery).orElseThrow();

    String link = nestedLink();
    List<String> rowKeys =
        scoped.load("row").orElseThrow().select(link).collectAsList().stream()
            .map(
                r -> {
                  String navigationKey = r.getAs(link);
                  return navigationKey;
                })
            .sorted()
            .toList();
    List<String> contextKeys =
        scoped.load("context").orElseThrow().select(link).collectAsList().stream()
            .map(
                r -> {
                  String navigationKey = r.getAs(link);
                  return navigationKey;
                })
            .sorted()
            .toList();

    List<String> expected =
        List.of("urn:gbif:dwcdp:nested-context:P1:R1", "urn:gbif:dwcdp:nested-context:P1:R2");
    assertEquals(expected, rowKeys);
    assertEquals(expected, contextKeys);
  }

  @Test
  void sameLogicalRowUnderMultipleParentsRemainsParentScoped() {
    Dataset<Row> ownership =
        relation(
            List.of(row("P1", "R1"), row("P2", "R1")),
            SparkNestedContextDiscovery.COL_PARENT,
            SparkNestedContextDiscovery.COL_ROW);
    Dataset<Row> contextRows =
        relation(
            List.of(row("P1", "R1", "C1"), row("P2", "R1", "C2")),
            SparkNestedContextDiscovery.COL_PARENT,
            SparkNestedContextDiscovery.COL_ROW,
            SparkNestedContextDiscovery.COL_CONTEXT);
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery.Result(ownership, contextRows, contextRows);

    TableLoader base =
        loader(
            parents(row("P1"), row("P2")),
            rows(row("R1", "publisher-parent", "publisher-context", "publisher")),
            contexts(
                row("C1", "publisher-row-1", "context-for-p1"),
                row("C2", "publisher-row-2", "context-for-p2")),
            bridges());
    TableLoader scoped = SparkNestedContextLoader.loader(base, context, discovery).orElseThrow();

    MappingExecutionResult result =
        new SparkMappingPathExecutor(graph).execute(scoped, rowToContext());
    String parentAlias = result.pathResult().columnName(SchemaPath.root("row").field("parent_fk"));
    String contextAlias = alias(result.pathResult(), "context", "context_value");
    List<List<String>> actual =
        result.pathResult().dataset().select(parentAlias, contextAlias).collectAsList().stream()
            .map(
                r -> {
                  String parent = r.getAs(parentAlias);
                  String value = r.getAs(contextAlias);
                  return List.of(parent, value);
                })
            .sorted((a, b) -> a.get(0).compareTo(b.get(0)))
            .toList();

    assertEquals(List.of(List.of("P1", "context-for-p1"), List.of("P2", "context-for-p2")), actual);
  }

  @Test
  void requiredFieldProjectionPreservesInternalNestedContextNavigationColumn() {
    String link = nestedLink();
    Dataset<Row> scopedRows =
        rows(row("R1", "P1", "semantic-wrong", "publisher"))
            .withColumn(link, org.apache.spark.sql.functions.lit("scope-P1-R1"));
    Dataset<Row> scopedContexts =
        contexts(row("C1", "semantic-other", "context-1"))
            .withColumn(link, org.apache.spark.sql.functions.lit("scope-P1-R1"));
    TableLoader loader =
        resource -> {
          if (resource.equals("row")) return Optional.of(scopedRows);
          if (resource.equals("context")) return Optional.of(scopedContexts);
          return Optional.empty();
        };

    Mapping mapping = rowToContext();
    SchemaPath contextPath =
        SchemaPath.root("row")
            .append(
                SchemaRelation.relation(
                    "row",
                    "semantic_context_fk",
                    "context",
                    "semantic_row_fk",
                    null,
                    RelationCardinality.UNKNOWN));
    MappingExecutionResult result =
        new SparkMappingPathExecutor(graph)
            .execute(loader, mapping, Set.of(contextPath.field("context_value")));

    assertTrue(
        result.pathResult().aliases().keySet().stream()
            .anyMatch(field -> field.column().equals(link)));
    Row output = result.pathResult().dataset().first();
    String contextAlias = alias(result.pathResult(), "context", "context_value");
    String contextValue = output.getAs(contextAlias);
    assertEquals("context-1", contextValue);
  }

  @Test
  void pathExecutionPrefersInternalNestedContextLinkOverSemanticWeakForeignKeys() {
    String link = nestedLink();
    Dataset<Row> scopedRows =
        rows(row("R1", "P1", "semantic-C2", "publisher"))
            .withColumn(link, org.apache.spark.sql.functions.lit("scope-P1-R1"));
    Dataset<Row> scopedContexts =
        contexts(
                row("C1", "semantic-R2", "correct-context"),
                row("C2", "semantic-C2", "wrong-semantic-context"))
            .withColumn(
                link,
                org.apache
                    .spark
                    .sql
                    .functions
                    .when(
                        org.apache.spark.sql.functions.col("context_pk").equalTo("C1"),
                        org.apache.spark.sql.functions.lit("scope-P1-R1"))
                    .otherwise(org.apache.spark.sql.functions.lit("scope-other")));
    TableLoader loader =
        resource -> {
          if (resource.equals("row")) return Optional.of(scopedRows);
          if (resource.equals("context")) return Optional.of(scopedContexts);
          return Optional.empty();
        };

    MappingExecutionResult result =
        new SparkMappingPathExecutor(graph).execute(loader, rowToContext());
    Row output = result.pathResult().dataset().first();
    String contextAlias = alias(result.pathResult(), "context", "context_value");
    String contextValue = output.getAs(contextAlias);
    assertEquals("correct-context", contextValue);
  }

  @Test
  void scopedLoaderDoesNotRewritePublisherSemanticFields() {
    TableLoader loader =
        loader(
            parents(row("P1")),
            rows(row("R1", "P1", "C1", "publisher")),
            contexts(row("C1", "publisher-row-link", "context-1")),
            bridges());
    SparkNestedContextDiscovery.Result discovery =
        new SparkNestedContextDiscovery(graph, context).discover(loader);
    TableLoader scoped = SparkNestedContextLoader.loader(loader, context, discovery).orElseThrow();

    Row scopedRow = scoped.load("row").orElseThrow().first();
    Row scopedContext = scoped.load("context").orElseThrow().first();
    String semanticContextFk = scopedRow.getAs("semantic_context_fk");
    String semanticRowFk = scopedContext.getAs("semantic_row_fk");
    String publisherValue = scopedRow.getAs("publisher_value");

    assertEquals("C1", semanticContextFk);
    assertEquals("publisher-row-link", semanticRowFk);
    assertEquals("publisher", publisherValue);
    assertFalse(semanticContextFk.startsWith("urn:gbif:dwcdp:nested-context:"));
    assertFalse(semanticRowFk.startsWith("urn:gbif:dwcdp:nested-context:"));
  }

  private NestedExtensionContext syntheticContext(SchemaGraph schema) {
    MappingPath parent = MappingPath.root(schema, "parent");
    MappingPath directRow = parent.join("row").on("parent_pk", "parent_fk").fanOut();
    MappingPath directContext =
        directRow.join("context").on("semantic_context_fk", "context_pk").fanOut();
    MappingPath bridgedRow = parent.join("row").on("parent_pk", "parent_fk").fanOut();
    MappingPath bridgedBridge = bridgedRow.join("bridge").on("row_pk", "row_fk").fanOut();
    MappingPath bridged = bridgedBridge.join("context").on("context_fk", "context_pk").fanOut();
    MappingPath row = MappingPath.root(schema, "row");
    MappingPath contextual = MappingPath.root(schema, "context");

    return new NestedExtensionContext(
        "synthetic-extension",
        "parent",
        "row",
        "context",
        parent.field("parent_pk"),
        row.field("row_pk"),
        row.field("parent_fk"),
        row.field("semantic_context_fk"),
        contextual.field("context_pk"),
        contextual.field("semantic_row_fk"),
        Optional.empty(),
        List.of(
            new NestedContextDiscoveryFragment(
                "direct",
                directContext,
                parent.field("parent_pk"),
                directRow.field("row_pk"),
                Optional.of(directContext.field("context_pk"))),
            new NestedContextDiscoveryFragment(
                "bridged",
                bridged,
                parent.field("parent_pk"),
                bridgedRow.field("row_pk"),
                Optional.of(bridged.field("context_pk")))),
        List.of());
  }

  private Mapping rowToContext() {
    return MappingBuilder.mapping("row-context", "row")
        .join("context")
        .on("semantic_context_fk", "semantic_row_fk")
        .exactlyOne()
        .build();
  }

  private String nestedLink() {
    return SparkInternalColumns.nestedContextLink("semantic_context_fk", "semantic_row_fk");
  }

  private TableLoader loader(
      Dataset<Row> parents, Dataset<Row> rows, Dataset<Row> contexts, Dataset<Row> bridges) {
    Map<String, Dataset<Row>> tables = new LinkedHashMap<>();
    tables.put("parent", parents);
    tables.put("row", rows);
    tables.put("context", contexts);
    if (bridges != null) {
      tables.put("bridge", bridges);
    }
    return resource -> Optional.ofNullable(tables.get(resource));
  }

  private Dataset<Row> parents(Row... values) {
    return relation(List.of(values), "parent_pk");
  }

  private Dataset<Row> rows(Row... values) {
    return relation(
        List.of(values), "row_pk", "parent_fk", "semantic_context_fk", "publisher_value");
  }

  private Dataset<Row> contexts(Row... values) {
    return relation(List.of(values), "context_pk", "semantic_row_fk", "context_value");
  }

  private Dataset<Row> bridges(Row... values) {
    return relation(List.of(values), "row_fk", "context_fk");
  }

  private Dataset<Row> relation(List<Row> rows, String... columns) {
    StructType schema = new StructType();
    for (String column : columns) {
      schema = schema.add(column, DataTypes.StringType);
    }
    return spark.createDataFrame(rows, schema);
  }

  private static String alias(SparkPathResult result, String resource, String column) {
    return result.aliases().entrySet().stream()
        .filter(entry -> entry.getKey().path().currentResource().equals(resource))
        .filter(entry -> entry.getKey().column().equals(column))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElseThrow();
  }

  private static void assertRelation(Dataset<Row> dataset, List<List<String>> expected) {
    List<List<String>> actual =
        dataset.collectAsList().stream()
            .map(
                r ->
                    Arrays.stream(dataset.columns())
                        .map(
                            column -> {
                              String value = r.getAs(column);
                              return value;
                            })
                        .toList())
            .sorted((left, right) -> left.toString().compareTo(right.toString()))
            .toList();
    assertEquals(expected, actual);
  }

  private static Row row(Object... values) {
    return RowFactory.create(values);
  }
}
