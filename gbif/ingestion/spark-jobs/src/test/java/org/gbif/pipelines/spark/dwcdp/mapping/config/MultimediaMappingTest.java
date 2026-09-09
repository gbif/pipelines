package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
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
class MultimediaMappingTest {

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("MultimediaMappingTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void mediaMetadataUsesJunctionTables() {
    var usagePolicy = MultimediaMapping.usagePolicy(graph);
    assertEquals(
        List.of("media-usage-policy", "usage-policy"),
        usagePolicy.relations().stream().map(r -> r.targetResource()).toList());

    var creators = MultimediaMapping.creators(graph);
    assertEquals(
        List.of("media-agent-role", "agent"),
        creators.relations().stream().map(r -> r.targetResource()).toList());
    assertTrue(creators.relations().get(0).filter().requiredColumns().contains("agentRole"));
  }

  @Test
  void eventMediaRowContainsLicenseAndCreatorFromOwnedMetadata() {
    ExtensionMapping extension =
        new ExtensionMapping(
            MultimediaMapping.ROW_TYPE_MULTIMEDIA,
            ExtensionRowComposition.UNION,
            MultimediaMapping.eventFragments(graph));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "event-media", eventMedia(),
                    "media", media(),
                    "media-usage-policy", mediaUsagePolicy(),
                    "usage-policy", usagePolicy(),
                    "media-agent-role", mediaAgentRoles(),
                    "agent", agents()),
                extension);

    List<Row> rows = result.dataset().collectAsList();
    assertEquals(1, rows.size());
    Row row = rows.get(0);

    String license = row.getAs(result.columnName(DcTerm.license.qualifiedName()));
    String creator = row.getAs(result.columnName(DcTerm.creator.qualifiedName()));
    assertEquals("https://creativecommons.org/licenses/by/4.0/", license);
    assertEquals("Alice|Bob", creator);
  }

  @Test
  void occurrenceMediaPromotionDoesNotEmitRoutingOnlyRowsWithoutMedia() {
    ExtensionMapping extension =
        new ExtensionMapping(
            MultimediaMapping.ROW_TYPE_MULTIMEDIA,
            ExtensionRowComposition.UNION,
            List.of(MultimediaMapping.occurrenceMediaForEvent(graph)));

    Dataset<Row> occurrences =
        spark.createDataFrame(
            List.of(RowFactory.create("O1", "OCC-1", "E1"), RowFactory.create("O2", "OCC-2", "E1")),
            new StructType()
                .add("occurrence_pk", DataTypes.StringType)
                .add("occurrenceID", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType));
    Dataset<Row> occurrenceMedia =
        spark.createDataFrame(
            List.of(RowFactory.create("O1", "M1")),
            new StructType()
                .add("occurrence_fk", DataTypes.StringType)
                .add("media_fk", DataTypes.StringType));

    ExtensionMaterializationResult result =
        new SparkExtensionMaterializer(graph)
            .materialize(
                TestTableLoader.of(
                    "occurrence", occurrences,
                    "occurrence-media", occurrenceMedia,
                    "media", media()),
                extension);

    List<Row> rows = result.dataset().collectAsList();
    assertEquals(1, rows.size());
    String identifier = rows.get(0).getAs(result.columnName(DcTerm.identifier.qualifiedName()));
    assertEquals("https://example.org/image.jpg", identifier);
  }

  private Dataset<Row> eventMedia() {
    return spark.createDataFrame(
        List.of(RowFactory.create("E1", "M1")),
        new StructType()
            .add("event_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> media() {
    return spark.createDataFrame(
        List.of(RowFactory.create("M1", "https://example.org/image.jpg", "StillImage")),
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType)
            .add("mediaType", DataTypes.StringType));
  }

  private Dataset<Row> mediaUsagePolicy() {
    return spark.createDataFrame(
        List.of(RowFactory.create("M1", "UP1")),
        new StructType()
            .add("media_fk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType));
  }

  private Dataset<Row> usagePolicy() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create(
                "UP1",
                "Copyright",
                "Example holder",
                null,
                null,
                "https://creativecommons.org/licenses/by/4.0/")),
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("rights", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType)
            .add("owner", DataTypes.StringType)
            .add("accessRights", DataTypes.StringType)
            .add("license", DataTypes.StringType));
  }

  private Dataset<Row> mediaAgentRoles() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("M1", "A2", "creator", 2),
            RowFactory.create("M1", "A1", "creator", 1),
            RowFactory.create("M1", "A3", "reviewer", 3)),
        new StructType()
            .add("media_fk", DataTypes.StringType)
            .add("agent_fk", DataTypes.StringType)
            .add("agentRole", DataTypes.StringType)
            .add("agentRoleOrder", DataTypes.IntegerType));
  }

  private Dataset<Row> agents() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("A1", "Alice"),
            RowFactory.create("A2", "Bob"),
            RowFactory.create("A3", "Ignored")),
        new StructType()
            .add("agent_pk", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType));
  }
}
