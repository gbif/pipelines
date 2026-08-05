package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
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
class UsagePolicyJoinBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("UsagePolicyJoinBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> mediaDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> usagePolicyDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- tests ----

  @Test
  void absentUsagePolicyTable_returnsOriginalDf() {
    Dataset<Row> media =
        mediaDf(List.of(RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg")));

    Dataset<Row> result = UsagePolicyJoinBuilder.enrich(TestTableLoader.of(), media);

    assertEquals(media.columns().length, result.columns().length);
    assertEquals(1L, result.count());
  }

  @Test
  void mediaWithoutUsagePolicyFkColumn_returnsOriginalDf() {
    StructType schema =
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType);
    Dataset<Row> media =
        spark.createDataFrame(
            List.of(RowFactory.create("MPK-001", "https://example.com/img.jpg")), schema);
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));

    Dataset<Row> result =
        UsagePolicyJoinBuilder.enrich(
            TestTableLoader.of(UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, upDf), media);

    assertEquals(2, result.columns().length, "should be original columns only");
    assertEquals(1L, result.count());
  }

  @Test
  void joinAddsLicenseAndRightsHolder() {
    Dataset<Row> media =
        mediaDf(List.of(RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));

    Dataset<Row> result =
        UsagePolicyJoinBuilder.enrich(
            TestTableLoader.of(UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, upDf), media);

    List<String> cols = Arrays.asList(result.columns());
    assertTrue(cols.contains("license"));
    assertTrue(cols.contains("rightsHolder"));
    assertTrue(cols.contains("accessURI"), "media's own columns must be preserved");

    Row row = result.filter(result.col("media_pk").equalTo("MPK-001")).first();
    assertEquals("CC_BY_4_0", row.getAs("license"));
    assertEquals("Museum X", row.getAs("rightsHolder"));
  }

  @Test
  void usagePolicySurrogatePkAndMediaFkNeverLeakIntoResult() {
    Dataset<Row> media =
        mediaDf(List.of(RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));

    Dataset<Row> result =
        UsagePolicyJoinBuilder.enrich(
            TestTableLoader.of(UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, upDf), media);

    List<String> cols = Arrays.asList(result.columns());
    assertTrue(
        cols.stream().noneMatch(c -> c.equals("usagePolicy_pk")),
        "usage-policy's own surrogate PK must never appear on media rows");
    assertTrue(
        cols.stream().noneMatch(c -> c.equals("usagePolicy_fk")),
        "the join FK itself must not survive into the media term map");
  }

  @Test
  void mediaWithNoMatchingUsagePolicy_survivesWithNullLicenseFields() {
    Dataset<Row> media =
        mediaDf(List.of(RowFactory.create("MPK-001", "UP-UNKNOWN", "https://example.com/img.jpg")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));

    Dataset<Row> result =
        UsagePolicyJoinBuilder.enrich(
            TestTableLoader.of(UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, upDf), media);

    assertEquals(1L, result.count(), "media row must survive left join even with no match");
    Row row = result.first();
    assertTrue(row.isNullAt(row.fieldIndex("license")));
  }

  @Test
  void joinDoesNotDuplicateColumnsAlreadyOnMedia() {
    Dataset<Row> media =
        mediaDf(List.of(RowFactory.create("MPK-001", "UP-1", "https://example.com/img.jpg")));
    Dataset<Row> upDf = usagePolicyDf(List.of(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")));

    Dataset<Row> result = UsagePolicyJoinBuilder.join(media, upDf);

    long distinctColCount = Arrays.stream(result.columns()).distinct().count();
    assertEquals(
        result.columns().length, distinctColCount, "result must have no duplicate column names");
  }

  // ---- computeFunnel ----

  @Test
  void computeFunnel_entityTableAbsent_returnsEmpty() {
    var result = UsagePolicyJoinBuilder.computeFunnel(TestTableLoader.of(), "media");

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_fkColumnAbsent_returnsEmpty() {
    StructType schema = new StructType().add("media_pk", DataTypes.StringType);
    Dataset<Row> media = spark.createDataFrame(List.of(RowFactory.create("MED-1")), schema);

    var result = UsagePolicyJoinBuilder.computeFunnel(TestTableLoader.of("media", media), "media");

    assertTrue(result.isEmpty());
  }

  @Test
  void computeFunnel_usagePolicyTableAbsent_allCandidatesUnresolved() {
    Dataset<Row> media = mediaDf(List.of(RowFactory.create("MED-1", "UP-1", "http://x")));

    var result = UsagePolicyJoinBuilder.computeFunnel(TestTableLoader.of("media", media), "media");

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(2, buckets.size());
    assertEquals(1L, buckets.get(0).count(), "candidates");
    assertEquals(1L, buckets.get(1).count(), "usage-policy table absent");
  }

  @Test
  void computeFunnel_resolvedAndUnresolvedSplitCorrectly() {
    Dataset<Row> media =
        mediaDf(
            List.of(
                RowFactory.create("MED-1", "UP-1", "http://x"),
                RowFactory.create("MED-2", "UP-UNKNOWN", "http://y"),
                RowFactory.create("MED-3", null, "http://z")));
    Dataset<Row> usagePolicy =
        usagePolicyDf(List.of(RowFactory.create("UP-1", "CC-BY 4.0", "Rights Holder A")));

    var result =
        UsagePolicyJoinBuilder.computeFunnel(
            TestTableLoader.of(
                "media", media, UsagePolicyJoinBuilder.TABLE_USAGE_POLICY, usagePolicy),
            "media");

    assertTrue(result.isPresent());
    var buckets = result.get().buckets();
    assertEquals(3, buckets.size());
    assertEquals(2L, buckets.get(0).count(), "candidates — MED-3 excluded");
    assertEquals(1L, buckets.get(1).count(), "resolved — MED-1");
    assertEquals(1L, buckets.get(2).count(), "dangling FK — MED-2");
  }
}
