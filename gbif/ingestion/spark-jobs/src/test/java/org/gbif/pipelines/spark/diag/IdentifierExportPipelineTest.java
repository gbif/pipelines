package org.gbif.pipelines.spark.diag;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.gbif.pipelines.spark.HbaseServer;
import org.gbif.pipelines.spark.pojo.LookupRow;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IdentifierExportPipelineTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static final String DATASET_A = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";
  private static final String DATASET_B = "168f92f9-8738-4959-8d42-e98cd18a7746";
  private static final String DATASET_EMPTY = "7ddf754f-d193-4cc9-b351-99906754a03b";
  private static final String TRIPLET = "IC|CC|CN1|null";

  private static SparkSession spark;
  private static FileSystem fs;
  private static PipelinesConfig config;
  private static long occ1;
  private static long occ2;
  private static long triplet;

  @BeforeClass
  public static void setUp() throws Exception {
    spark = SparkTestSession.createBuilder().appName("identifier-export-test").getOrCreate();
    fs = FileSystem.getLocal(new Configuration());

    KeygenConfig keygen = new KeygenConfig();
    keygen.setLookupTable(HbaseServer.CFG.getLookupTable());
    keygen.setZkConnectionString(HBASE_SERVER.getZkQuorum());
    config = new PipelinesConfig();
    config.setKeygen(keygen);

    // the connection factory is a JVM singleton, drop one left over from another test's cluster
    HbaseConnectionFactory.getInstance(keygen.getZkConnectionString(), null)
        .getConnection()
        .close();

    HBaseLockingKeyService serviceA =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), DATASET_A);
    occ1 = serviceA.generateKey(Collections.singleton("occ1")).getKey();
    occ2 = serviceA.generateKey(Collections.singleton("occ2")).getKey();
    triplet = serviceA.generateKey(Collections.singleton(TRIPLET)).getKey();
    // a second occurrenceID reusing the gbifID of occ1
    serviceA.putKey(DATASET_A + "|occ-reused", occ1);

    // must not be exported
    new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), DATASET_B)
        .generateKey(Collections.singleton("occ1"));
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void exportTest() throws Exception {

    String out = tmp.getRoot().getAbsolutePath() + "/identifier-export";

    Map<String, String> result =
        IdentifierExportPipeline.export(
            spark, fs, config, Arrays.asList(DATASET_A, DATASET_EMPTY), out, 2, 2, 100, false);

    assertEquals(IdentifierExportPipeline.OK, result.get(DATASET_A));
    assertEquals(IdentifierExportPipeline.OK, result.get(DATASET_EMPTY));
    assertTrue(Files.exists(java.nio.file.Path.of(out, DATASET_A, "_SUCCESS")));
    assertTrue(Files.exists(java.nio.file.Path.of(out, DATASET_A, "_export.yml")));
    assertTrue(Files.exists(java.nio.file.Path.of(out, DATASET_EMPTY, "_SUCCESS")));
    assertFalse(Files.exists(java.nio.file.Path.of(out, DATASET_B)));
    assertFalse(Files.exists(java.nio.file.Path.of(out, IdentifierExportPipeline.TMP_DIR)));

    List<LookupRow> rows =
        spark
            .read()
            .parquet(out + "/" + DATASET_A)
            .as(Encoders.bean(LookupRow.class))
            .collectAsList();
    assertEquals(4, rows.size());

    Map<String, LookupRow> byKey =
        rows.stream().collect(Collectors.toMap(LookupRow::getUniqueKey, Function.identity()));
    assertRow(byKey.get("occ1"), LookupRow.OCCURRENCE_ID, occ1);
    assertRow(byKey.get("occ2"), LookupRow.OCCURRENCE_ID, occ2);
    assertRow(byKey.get("occ-reused"), LookupRow.OCCURRENCE_ID, occ1);
    assertRow(byKey.get(TRIPLET), LookupRow.TRIPLET, triplet);

    byte[] expectedRowKey =
        HBaseStore.saltKey(DATASET_A + "|occ1", HBaseLockingKeyService.NUMBER_OF_BUCKETS);
    assertArrayEquals(expectedRowKey, byKey.get("occ1").getRowKeyBytes());
    assertEquals(
        String.format("%02d", byKey.get("occ1").getSalt()),
        byKey.get("occ1").getRowKey().substring(0, 2));

    assertEquals(0, spark.read().parquet(out + "/" + DATASET_EMPTY).count());

    String metrics = Files.readString(java.nio.file.Path.of(out, DATASET_A, "_export.yml"));
    assertTrue(metrics, metrics.contains("rows: 4"));
    assertTrue(metrics, metrics.contains("triplets: 1"));
    assertTrue(metrics, metrics.contains("distinctGbifIds: 3"));
    assertTrue(metrics, metrics.contains("gbifIdsWithMultipleOccurrenceIdsOrTriplets: 1"));

    // a second run skips completed datasets
    Map<String, String> rerun =
        IdentifierExportPipeline.export(
            spark, fs, config, Collections.singletonList(DATASET_A), out, 1, 2, 100, false);
    assertEquals(IdentifierExportPipeline.SKIPPED, rerun.get(DATASET_A));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidDatasetKeyTest() {
    IdentifierExportPipeline.parseDatasetKeys(Arrays.asList(DATASET_A, "not-a-uuid"));
  }

  @Test
  public void parseDatasetKeysTest() {
    assertEquals(
        Collections.singleton(DATASET_A),
        IdentifierExportPipeline.parseDatasetKeys(
            Arrays.asList("# comment", "", " " + DATASET_A.toUpperCase() + " ", DATASET_A)));
  }

  private static void assertRow(LookupRow row, String keyType, long gbifId) {
    assertNotNull(row);
    assertEquals(DATASET_A, row.getDatasetKey());
    assertEquals(keyType, row.getKeyType());
    assertEquals(Long.valueOf(gbifId), row.getGbifId());
    assertNotNull(row.getGbifIdTs());
    assertEquals("ALLOCATED", row.getStatus());
    assertNotNull(row.getStatusTs());
    assertNull(row.getLock());
    assertNull(row.getLockTs());
  }
}
