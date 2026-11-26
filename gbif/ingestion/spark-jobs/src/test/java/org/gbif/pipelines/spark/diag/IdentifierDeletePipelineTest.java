package org.gbif.pipelines.spark.diag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.beust.jcommander.ParameterException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;
import org.gbif.pipelines.spark.HbaseServer;
import org.gbif.pipelines.spark.pojo.LookupRow;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IdentifierDeletePipelineTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static final String DATASET_A = "0b9f1b8e-3a43-4c39-9d0e-5f2b7f6c1a01";
  private static final String DATASET_B = "6c5e2d1a-8f7b-4e2c-a1d3-9b8c7d6e5f02";
  private static final String TRIPLET_1 = "IC|CC|CN1|null";
  private static final String TRIPLET_2 = "IC|CC|CN2|null";

  private static SparkSession spark;
  private static FileSystem fs;
  private static PipelinesConfig config;
  private static HBaseLockingKeyService serviceA;
  private static HBaseLockingKeyService serviceB;

  @BeforeClass
  public static void setUp() throws Exception {
    spark = SparkTestSession.createBuilder().appName("identifier-delete-test").getOrCreate();
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

    serviceA = new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), DATASET_A);
    serviceA.generateKey(Collections.singleton("occ1"));
    serviceA.generateKey(Collections.singleton("occ2"));
    serviceA.generateKey(Collections.singleton(TRIPLET_1));
    serviceA.generateKey(Collections.singleton(TRIPLET_2));

    // same keys in another dataset, must not be touched
    serviceB = new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), DATASET_B);
    serviceB.generateKey(Collections.singleton("occ1"));
    serviceB.generateKey(Collections.singleton(TRIPLET_1));
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void deleteTripletsTest() throws Exception {

    String dryRunPath = tmp.getRoot().getAbsolutePath() + "/dry-run";

    // dry run: backs up but does not delete
    Map<String, String> dryRun =
        IdentifierDeletePipeline.delete(
            spark,
            fs,
            config,
            Collections.singletonList(DATASET_A),
            LookupRow.TRIPLET,
            dryRunPath,
            1,
            100,
            1,
            true);

    assertEquals(IdentifierDeletePipeline.OK, dryRun.get(DATASET_A));
    assertEquals(2, readBackup(dryRunPath, DATASET_A, LookupRow.TRIPLET).size());
    assertTrue(serviceA.findKey(Collections.singleton(TRIPLET_1)).isPresent());
    assertTrue(serviceA.findKey(Collections.singleton(TRIPLET_2)).isPresent());
    String dryMetrics =
        Files.readString(java.nio.file.Path.of(dryRunPath, DATASET_A, "_delete.yml"));
    assertTrue(dryMetrics, dryMetrics.contains("dryRun: 1"));
    assertFalse(dryMetrics, dryMetrics.contains("deleted"));

    // real run
    String runPath = tmp.getRoot().getAbsolutePath() + "/run";
    Map<String, String> result =
        IdentifierDeletePipeline.delete(
            spark,
            fs,
            config,
            Collections.singletonList(DATASET_A),
            LookupRow.TRIPLET,
            runPath,
            1,
            100,
            1,
            false);

    assertEquals(IdentifierDeletePipeline.OK, result.get(DATASET_A));

    List<LookupRow> backup = readBackup(runPath, DATASET_A, LookupRow.TRIPLET);
    assertEquals(2, backup.size());
    assertTrue(backup.stream().allMatch(r -> LookupRow.TRIPLET.equals(r.getKeyType())));

    // triplets of dataset A are gone
    assertFalse(serviceA.findKey(Collections.singleton(TRIPLET_1)).isPresent());
    assertFalse(serviceA.findKey(Collections.singleton(TRIPLET_2)).isPresent());
    // occurrenceIDs of dataset A are kept
    assertTrue(serviceA.findKey(Collections.singleton("occ1")).isPresent());
    assertTrue(serviceA.findKey(Collections.singleton("occ2")).isPresent());
    // dataset B is untouched
    assertTrue(serviceB.findKey(Collections.singleton("occ1")).isPresent());
    assertTrue(serviceB.findKey(Collections.singleton(TRIPLET_1)).isPresent());

    String metrics = Files.readString(java.nio.file.Path.of(runPath, DATASET_A, "_delete.yml"));
    assertTrue(metrics, metrics.contains("rows: 2"));
    assertTrue(metrics, metrics.contains("deleted: 2"));
    assertTrue(metrics, metrics.contains("remaining: 0"));
    assertTrue(metrics, metrics.contains("dryRun: 0"));

    // a second run into the same path must not overwrite the backup
    Map<String, String> rerun =
        IdentifierDeletePipeline.delete(
            spark,
            fs,
            config,
            Collections.singletonList(DATASET_A),
            LookupRow.TRIPLET,
            runPath,
            1,
            100,
            1,
            false);
    assertEquals(IdentifierDeletePipeline.FAILED, rerun.get(DATASET_A));
    assertEquals(2, readBackup(runPath, DATASET_A, LookupRow.TRIPLET).size());
  }

  @Test
  public void validKeyTypesTest() {
    IdentifierDeletePipeline.validateKeyType(LookupRow.OCCURRENCE_ID);
    IdentifierDeletePipeline.validateKeyType(LookupRow.TRIPLET);
    new IdentifierDeletePipeline.KeyTypeValidator().validate("--keyType", LookupRow.TRIPLET);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidKeyTypeTest() throws Exception {
    IdentifierDeletePipeline.delete(
        spark,
        fs,
        config,
        Arrays.asList(DATASET_A),
        "occurrence_id",
        tmp.getRoot().getAbsolutePath(),
        1,
        100,
        1,
        true);
  }

  @Test(expected = ParameterException.class)
  public void invalidKeyTypeArgTest() {
    new IdentifierDeletePipeline.KeyTypeValidator().validate("--keyType", "CATALOG_NUMBER");
  }

  private static List<LookupRow> readBackup(String runPath, String datasetKey, String keyType) {
    return spark
        .read()
        .parquet(runPath + "/" + datasetKey + "/" + keyType)
        .as(Encoders.bean(LookupRow.class))
        .collectAsList();
  }
}
