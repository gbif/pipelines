package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests for {@link AssertionExtensionBuilder}, covering both the base event/occurrence eMoF
 * behavior and the newer {@code material-assertion} merge into occurrence's own eMoF extension.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AssertionExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("AssertionExtensionBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrencePkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType);
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

  private Dataset<Row> occurrenceAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- base behavior: event assertion ----

  @Test
  void eventAssertion_remappedToEmofTerms() throws Exception {
    Dataset<Row> eventDf = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> assertionDf =
        eventAssertionDf(List.of(RowFactory.create("A001", "EPK-001", "Temperature", "25.0", "C")));

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildEventAssertionExtension(
            spark, TestTableLoader.of("event", eventDf, "event-assertion", assertionDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> emof = parseAssertionJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, emof.size());
    assertEquals("Temperature", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
  }

  // ---- base behavior: direct occurrence assertion, no material ----

  @Test
  void occurrenceAssertion_remappedToEmofTerms() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> assertionDf =
        occurrenceAssertionDf(List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(
            spark, TestTableLoader.of("occurrence", occ, "occurrence-assertion", assertionDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> emof = parseAssertionJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, emof.size());
    assertEquals("Mass", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("3.2", emof.get(0).get(DwcTerm.measurementValue.qualifiedName()));
  }

  // ---- material-assertion merge ----

  @Test
  void materialAssertion_mergedIntoOccurrenceEmofWhenNoDirectAssertion() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> materialAssertionDf =
        materialAssertionDf(
            List.of(RowFactory.create("A001", "MEPK-1", "Wing chord", "85.5", "mm")));

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                "material-assertion",
                materialAssertionDf));

    assertTrue(
        resultOpt.isPresent(),
        "a specimen measurement must still produce an eMoF extension even with no direct "
            + "occurrence-assertion at all");
    List<Map<String, String>> emof = parseAssertionJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, emof.size());
    assertEquals("Wing chord", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("85.5", emof.get(0).get(DwcTerm.measurementValue.qualifiedName()));
  }

  @Test
  void directAndMaterialAssertion_bothPresent_unionedTogether() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> occurrenceAssertionDf =
        occurrenceAssertionDf(List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));
    Dataset<Row> materialAssertionDf =
        materialAssertionDf(
            List.of(RowFactory.create("A002", "MEPK-1", "Wing chord", "85.5", "mm")));

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                "occurrence-assertion",
                occurrenceAssertionDf,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                "material-assertion",
                materialAssertionDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> emof = parseAssertionJson(resultOpt.get().collectAsList().get(0));
    assertEquals(
        2, emof.size(), "both the occurrence's own and the specimen's measurement must appear");
  }

  @Test
  void materialAssertion_notMergedWhenOccurrenceHasAmbiguousMaterial() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    // Two material records citing the same occurrence — MaterialJoinBuilder's exactly-one rule
    // excludes both, so neither's assertions should merge in.
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001"), RowFactory.create("MEPK-2", "OCC001")));
    Dataset<Row> materialAssertionDf =
        materialAssertionDf(
            List.of(RowFactory.create("A001", "MEPK-1", "Wing chord", "85.5", "mm")));

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                "material-assertion",
                materialAssertionDf));

    assertTrue(
        resultOpt.isEmpty(),
        "with no direct occurrence-assertion and an ambiguous (two-record) material link, there "
            + "is nothing unambiguous to merge in at all");
  }

  @Test
  void materialAssertionProtocolFk_resolvedViaProtocolTableSameAsDirectPath() throws Exception {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionProtocol_fk", DataTypes.StringType);
    Dataset<Row> materialAssertionDf =
        spark.createDataFrame(
            List.of(RowFactory.create("A001", "MEPK-1", "Wing chord", "85.5", "PROTO-1")), schema);
    StructType protocolSchema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    Dataset<Row> protocolDf =
        spark.createDataFrame(
            List.of(RowFactory.create("PROTO-1", "Standard wing chord measurement")),
            protocolSchema);

    Optional<Dataset<Row>> resultOpt =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(
            spark,
            TestTableLoader.of(
                "occurrence",
                occ,
                MaterialJoinBuilder.TABLE_MATERIAL,
                material,
                "material-assertion",
                materialAssertionDf,
                "protocol",
                protocolDf));

    assertTrue(resultOpt.isPresent());
    List<Map<String, String>> emof = parseAssertionJson(resultOpt.get().collectAsList().get(0));
    assertEquals(1, emof.size());
    assertEquals(
        "Standard wing chord measurement",
        emof.get(0).get(DwcTerm.measurementMethod.qualifiedName()));
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> parseAssertionJson(Row row) throws Exception {
    String json = row.getAs(AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }
}
