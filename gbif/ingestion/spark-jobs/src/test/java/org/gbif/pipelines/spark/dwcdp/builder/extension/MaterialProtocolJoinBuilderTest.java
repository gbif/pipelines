package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                "material", material, "material-protocol", materialProtocol, "protocol", protocol),
            occurrence);

    assertEquals(
        "Material protocol A|Material protocol B|Occurrence protocol",
        result.first().getAs("samplingProtocol"));
  }

  private Dataset<Row> dataframe(List<Row> rows, StructType schema) {
    return spark.createDataFrame(rows, schema);
  }
}
