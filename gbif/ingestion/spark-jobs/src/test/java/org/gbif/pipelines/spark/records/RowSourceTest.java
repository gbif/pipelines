package org.gbif.pipelines.spark.records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RowSourceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static SparkSession spark;

  @BeforeClass
  public static void setUp() {
    spark = SparkTestSession.create();
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /** Every supported type gives what Jackson reads from the JSON Spark writes */
  @Test
  public void sameAsParsedJson() throws Exception {
    StructType nested =
        new StructType().add("name", DataTypes.StringType).add("rank", DataTypes.IntegerType);
    StructType schema =
        new StructType()
            .add("text", DataTypes.StringType)
            .add("flag", DataTypes.BooleanType)
            .add("small", DataTypes.ShortType)
            .add("int", DataTypes.IntegerType)
            .add("smallLong", DataTypes.LongType)
            .add("largeLong", DataTypes.LongType)
            .add("float", DataTypes.FloatType)
            .add("double", DataTypes.DoubleType)
            .add("notANumber", DataTypes.DoubleType)
            .add("decimal", DataTypes.createDecimalType(10, 3))
            .add("integralDecimal", DataTypes.createDecimalType(12, 0))
            .add("missing", DataTypes.StringType)
            .add("list", DataTypes.createArrayType(DataTypes.StringType))
            .add("structs", DataTypes.createArrayType(nested))
            .add("nested", nested)
            .add("map", DataTypes.createMapType(DataTypes.StringType, DataTypes.DoubleType));

    Row row =
        RowFactory.create(
            "a",
            true,
            (short) 3,
            42,
            7L,
            3_000_000_000L,
            1.1f,
            -12.345678901234,
            Double.NaN,
            new BigDecimal("12.500"),
            new BigDecimal("123456789012"),
            null,
            Arrays.asList("x", null, "z"),
            List.of(RowFactory.create("Panthera", null)),
            RowFactory.create(null, 5),
            Map.of("lat", 1.5));

    Dataset<Row> documents = spark.createDataFrame(List.of(row), schema);
    assertTrue(RowSource.supports(documents.schema()));

    Map<String, Object> fromJson =
        MAPPER.readValue(documents.toJSON().first(), new TypeReference<>() {});
    Map<String, Object> fromRow = RowSource.toSource(documents.first());

    assertEquals(fromJson, fromRow);
    assertFalse(fromRow.containsKey("missing"));
    assertEquals(7, fromRow.get("smallLong"));
    assertEquals(3_000_000_000L, fromRow.get("largeLong"));
    assertEquals(1.1d, fromRow.get("float"));
  }

  @Test
  public void timestampsAreNotSupported() {
    StructType schema =
        new StructType()
            .add("text", DataTypes.StringType)
            .add("modified", DataTypes.createArrayType(DataTypes.TimestampType));
    assertFalse(RowSource.supports(schema));
  }
}
