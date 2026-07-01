package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RowTermMapperTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("RowTermMapperTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void nullValues_areOmittedFromTermMap() {
    StructType schema =
        new StructType()
            .add("eventID", DataTypes.StringType)
            .add("eventDate", DataTypes.StringType);
    Dataset<Row> ds = spark.createDataFrame(List.of(RowFactory.create("EVT001", null)), schema);

    Row row = ds.collectAsList().get(0);
    Map<String, String> terms = RowTermMapper.toTermMap(row, new String[] {"eventID", "eventDate"});

    assertEquals(1, terms.size());
    assertEquals("EVT001", terms.get(DwcTerm.eventID.qualifiedName()));
    assertNull(terms.get(DwcTerm.eventDate.qualifiedName()));
  }

  @Test
  void columnNames_areResolvedToQualifiedUris() {
    StructType schema =
        new StructType().add("eventID", DataTypes.StringType).add("country", DataTypes.StringType);
    Dataset<Row> ds = spark.createDataFrame(List.of(RowFactory.create("EVT001", "DK")), schema);

    Row row = ds.collectAsList().get(0);
    Map<String, String> terms = RowTermMapper.toTermMap(row, new String[] {"eventID", "country"});

    assertEquals("EVT001", terms.get(DwcTerm.eventID.qualifiedName()));
    assertEquals("DK", terms.get(DwcTerm.country.qualifiedName()));
  }

  @Test
  void unresolvableColumnName_keptAsRawKey() {
    StructType schema = new StructType().add("somePublisherColumn", DataTypes.StringType);
    Dataset<Row> ds = spark.createDataFrame(List.of(RowFactory.create("value")), schema);

    Row row = ds.collectAsList().get(0);
    Map<String, String> terms = RowTermMapper.toTermMap(row, new String[] {"somePublisherColumn"});

    assertEquals("value", terms.get("somePublisherColumn"));
  }
}
