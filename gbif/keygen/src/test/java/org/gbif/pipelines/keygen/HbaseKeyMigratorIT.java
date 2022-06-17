package org.gbif.pipelines.keygen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HbaseKeyMigratorIT {
  private static final KeygenConfig CFG =
      KeygenConfig.builder()
          .zkConnectionString(null)
          .occurrenceTable("test_occurrence")
          .lookupTable("test_occurrence_lookup")
          .counterTable("test_occurrence_counter")
          .create();

  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccurrenceTable());

  private static Connection connection = null;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseLockingKeyService keyService;

  @Rule public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.master.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.regionserver.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL
        .getConfiguration()
        .setInt("hbase.regionserver.info.port", HBaseTestingUtility.randomFreePort());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @Before
  public void before() throws IOException {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    keyService = new HBaseLockingKeyService(CFG, connection);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void testLookupKeyMigration() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    KeyLookupResult oldKey =
        keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(keyService)
            .deleteKeys(false)
            .build()
            .migrate();

    // Should
    Optional<KeyLookupResult> newKey =
        keyService.findKey(Collections.singleton(newOccurrenceId), datasetKey);
    Optional<KeyLookupResult> oldExpiriedKey =
        keyService.findKey(Collections.singleton(oldOccurrenceId), datasetKey);

    assertTrue(migratedKey.isPresent());
    assertEquals(oldKey.getKey(), migratedKey.get().getKey());
    assertTrue(newKey.isPresent());
    assertEquals(oldKey.getKey(), newKey.get().getKey());
    assertFalse(oldExpiriedKey.isPresent());
  }

  @Test
  public void testNullLookupKey() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(keyService)
            .build()
            .migrate();

    // Should
    assertFalse(migratedKey.isPresent());
  }

  @Test
  public void testLookupKeyExisting() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);
    keyService.generateKey(Collections.singleton(newOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(keyService)
            .deleteKeys(false)
            .build()
            .migrate();

    // Should
    assertFalse(migratedKey.isPresent());
  }

  @Test
  public void testLookupKeyDeleteExisting() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    KeyLookupResult oldKey =
        keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);
    keyService.generateKey(Collections.singleton(newOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(keyService)
            .deleteKeys(true)
            .build()
            .migrate();

    // Should
    Optional<KeyLookupResult> newKey =
        keyService.findKey(Collections.singleton(newOccurrenceId), datasetKey);
    Optional<KeyLookupResult> oldExpiriedKey =
        keyService.findKey(Collections.singleton(oldOccurrenceId), datasetKey);

    assertTrue(migratedKey.isPresent());
    assertEquals(oldKey.getKey(), migratedKey.get().getKey());
    assertTrue(newKey.isPresent());
    assertEquals(oldKey.getKey(), newKey.get().getKey());
    assertFalse(oldExpiriedKey.isPresent());
  }
}
