package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
class MaterialProtocolJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("MaterialProtocolJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void materialProtocols_mergeWithOccurrenceSamplingProtocol() {
    Dataset<Row> occurrence =
        dataframe(
            List.of(RowFactory.create("OCC001", "Occurrence protocol")),
            new StructType()
                .add("occurrenceID", DataTypes.StringType)
                .add("samplingProtocol", DataTypes.StringType));
    Dataset<Row> material =
        dataframe(
            List.of(RowFactory.create("MEPK-1", "OCC001")),
            new StructType()
                .add("materialEntity_pk", DataTypes.StringType)
                .add("evidenceForOccurrenceID", DataTypes.StringType));
    Dataset<Row> materialProtocol =
        dataframe(
            List.of(
                RowFactory.create("MEPK-1", "PPK-2"),
                RowFactory.create("MEPK-1", "PPK-1"),
                RowFactory.create("MEPK-1", "PPK-1")),
            new StructType()
                .add("materialEntity_fk", DataTypes.StringType)
                .add("protocol_fk", DataTypes.StringType));
    Dataset<Row> protocol =
        dataframe(
            List.of(
                RowFactory.create("PPK-1", "Material protocol A"),
                RowFactory.create("PPK-2", "Material protocol B")),
            new StructType()
                .add("protocol_pk", DataTypes.StringType)
                .add("protocolDescription", DataTypes.StringType));

    Dataset<Row> result =
        MaterialProtocolJoinBuilder.enrichOccurrences(
            TestTableLoader.of(
                "material",
                material,
                "material-protocol",
                materialProtocol,
                "protocol",
                protocol,
                "occurrence",
                occurrence),
            occurrence);

    assertEquals(
        "Material protocol A|Material protocol B|Occurrence protocol",
        result.first().getAs("samplingProtocol"));
  }

  private Dataset<Row> dataframe(List<Row> rows, StructType schema) {
    return spark.createDataFrame(rows, schema);
  }

  // ---- computeFunnel ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    return dataframe(
        rows,
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType));
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    return dataframe(
        rows,
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  @Test
  void computeFunnel_noSingleMaterialLinks_returnsEmpty() {
    var result = MaterialProtocolJoinBuilder.computeFunnel(TestTableLoader.of());

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_noMaterialProtocolData_allUnresolvedBucket() {
    Dataset<Row> occurrence = occurrenceDf(List.of(RowFactory.create("OCC001", "Parus major")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));

    var result =
        MaterialProtocolJoinBuilder.computeFunnel(
            TestTableLoader.of("occurrence", occurrence, "material", material));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(2, buckets.size());
    assertEquals(1L, buckets.get(0).count(), "unambiguous single-material links (base)");
    assertEquals(1L, buckets.get(1).count(), "no material-protocol data available, unresolved");
  }

  @Test
  void computeFunnel_resolvedAndUnresolvedSplitCorrectly() {
    Dataset<Row> occurrence =
        occurrenceDf(
            List.of(
                RowFactory.create("OCC001", "Parus major"),
                RowFactory.create("OCC002", "Turdus merula")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001"), RowFactory.create("MEPK-2", "OCC002")));
    Dataset<Row> materialProtocol =
        dataframe(
            List.of(RowFactory.create("MEPK-1", "PPK-1")),
            new StructType()
                .add("materialEntity_fk", DataTypes.StringType)
                .add("protocol_fk", DataTypes.StringType));
    Dataset<Row> protocol =
        dataframe(
            List.of(RowFactory.create("PPK-1", "Material protocol A")),
            new StructType()
                .add("protocol_pk", DataTypes.StringType)
                .add("protocolDescription", DataTypes.StringType));

    var result =
        MaterialProtocolJoinBuilder.computeFunnel(
            TestTableLoader.of(
                "occurrence",
                occurrence,
                "material",
                material,
                "material-protocol",
                materialProtocol,
                "protocol",
                protocol));

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(3, buckets.size());
    assertEquals(2L, buckets.get(0).count(), "unambiguous single-material links (base)");
    assertEquals(1L, buckets.get(1).count(), "resolved, samplingProtocol merged — OCC001");
    assertEquals(
        1L, buckets.get(2).count(), "no material-protocol data for this material — OCC002");
  }
}
