package org.gbif.pipelines.keygen;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.config.OccHbaseConfiguration;
import org.gbif.pipelines.keygen.hbase.Columns;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HBaseLockingKeyServiceTest {

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";

  private static final OccHbaseConfiguration CFG = new OccHbaseConfiguration();

  static {
    CFG.setEnvironment("test");
  }

  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccTable());


  private static Connection CONNECTION = null;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseLockingKeyService keyService;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(LOOKUP_TABLE, CF);
    TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
    TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @Before
  public void before() throws IOException {
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    keyService = new HBaseLockingKeyService(CFG, CONNECTION);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    CONNECTION.close();
  }

  @Test
  public void testNoContention() {
    Set<String> uniqueIds = Sets.newHashSet();
    uniqueIds.add(A);
    uniqueIds.add(B);
    uniqueIds.add(C);
    KeyLookupResult result = keyService.generateKey(uniqueIds, "boo");
    assertEquals(1, result.getKey());
    assertTrue(result.isCreated());

    KeyLookupResult result2 = keyService.findKey(uniqueIds, "boo");
    assertEquals(1, result2.getKey());
    assertFalse(result2.isCreated());
  }

  @Test
  public void testAddOccIdToExistingTriplet() throws IOException {
    // setup: 1 finalized row, the triplet

    String datasetKey = UUID.randomUUID().toString();
    String triplet = "IC|CC|CN|null";
    byte[] lookupKey1 = Bytes.toBytes(datasetKey + "|" + triplet);
    Put put = new Put(lookupKey1);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    // test: keygen attempt uses previous unique id and a new "occurrenceId", expects the existing key to be returned
    KeyLookupResult result = keyService.generateKey(ImmutableSet.of(triplet, "ABCD"), datasetKey);
    assertEquals(2, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testSimpleIdContig() {
    KeyLookupResult result = null;
    for (int i = 0; i < 1000; i++) {
      Set<String> uniqueIds = ImmutableSet.of(String.valueOf(i));
      result = keyService.generateKey(uniqueIds, "boo");
    }
    assertEquals(1000, result.getKey());
  }

  @Test
  public void testResumeCountAfterFailure() {
    KeyLookupResult result = null;
    for (int i = 0; i < 250; i++) {
      Set<String> uniqueIds = ImmutableSet.of(String.valueOf(i));
      result = keyService.generateKey(uniqueIds, "boo");
    }
    assertEquals(250, result.getKey());

    // first one claimed up to 300, then "died". On restart we claim 300 to 400.
    HBaseLockingKeyService keyService2 =
        new HBaseLockingKeyService(CFG, CONNECTION);
    for (int i = 0; i < 50; i++) {
      Set<String> uniqueIds = ImmutableSet.of("A" + i);
      result = keyService2.generateKey(uniqueIds, "boo");
    }
    assertEquals(350, result.getKey());
  }

  @Test
  public void testLockWriteDie() throws IOException {
    // setup: 2 rows, each one gets as far as writing the new id but "dies" before releasing lock
    String datasetKey = UUID.randomUUID().toString();

    byte[] lock1 = Bytes.toBytes(UUID.randomUUID().toString());
    byte[] lookupKey1 = Bytes.toBytes(datasetKey + "|ABCD");
    Put put = new Put(lookupKey1);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock1);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(LOOKUP_TABLE))) {
      lookupTable.put(put);
      byte[] lock2 = Bytes.toBytes(UUID.randomUUID().toString());
      byte[] lookupKey2 = Bytes.toBytes(datasetKey + "|EFGH");
      put = new Put(lookupKey2);
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock2);
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(3L));
      lookupTable.put(put);
    }

    // test: 3rd keygen attempt uses both previous unique ids, expects a new key to be generated
    KeyLookupResult result = keyService.generateKey(ImmutableSet.of("ABCD", "EFGH"), datasetKey);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testConflictingIds() throws IOException {
    // setup: 2 rows with different lookupkeys and assigned ids

    String datasetKey = "fakeuuid";

    byte[] lookupKey1 = Bytes.toBytes(datasetKey + "|ABCD");
    Put put = new Put(lookupKey1);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(1L));
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(LOOKUP_TABLE))) {
      lookupTable.put(put);

      byte[] lookupKey2 = Bytes.toBytes(datasetKey + "|EFGH");
      put = new Put(lookupKey2);
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
      put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
      lookupTable.put(put);
    }
    // test: gen id for one occ with both lookupkeys
    exception.expect(IllegalStateException.class);
    exception.expectMessage(
        "Found inconsistent occurrence keys in looking up unique identifiers:[" + datasetKey + "|ABCD]=[1]["
            + datasetKey + "|EFGH]=[2]");
    keyService.generateKey(ImmutableSet.of("ABCD", "EFGH"), datasetKey);
  }

  @Test
  public void testStaleLock() throws IOException {
    String datasetKey = UUID.randomUUID().toString();
    // setup: lookupkey | null for status | uuid with old ts for lock | null for occurrence key

    byte[] lookupKey = Bytes.toBytes(datasetKey + "|ABCD");
    byte[] lock = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(lookupKey);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock);
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    KeyLookupResult result = keyService.generateKey(ImmutableSet.of("ABCD"), datasetKey);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testValidLockBecomesStale() throws IOException {
    String datasetKey = UUID.randomUUID().toString();
    // setup: lookupkey | null for status | uuid with old ts for lock | null for occurrence key

    // now minus the stale timeout + 10 seconds to force retry
    long ts = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5) + TimeUnit.SECONDS.toMillis(10);
    byte[] lookupKey = Bytes.toBytes(datasetKey + "|ABCD");
    byte[] lock = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(lookupKey);
    put.addColumn(CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), ts, lock);
    try (Table lookupTable = CONNECTION.getTable(TableName.valueOf(LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    //    System.out.println("start at [" + System.currentTimeMillis() + "]");
    KeyLookupResult result = keyService.generateKey(ImmutableSet.of("ABCD"), datasetKey);
    assertEquals(1, result.getKey());
    //    System.out.println("end at [" + System.currentTimeMillis() + "]");
  }

  @Test
  public void testThreadedIdContig() throws InterruptedException {
    // 5 threads concurrently allocated 1000 ids each, expect the next call to produce id 5001
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      Thread thread = new Thread(new KeyRequester(1000, keyService, String.valueOf(i)));
      thread.start();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      thread.join();
    }
    KeyLookupResult result = keyService.generateKey(ImmutableSet.of("asdf"), "wqer");
    assertEquals(5001, result.getKey());
  }

  private static class KeyRequester implements Runnable {

    private final int keyCount;
    private final HBaseLockingKeyService keyService;
    private final String name;

    private KeyRequester(int keyCount, HBaseLockingKeyService keyService, String name) {
      this.keyCount = keyCount;
      this.keyService = keyService;
      this.name = name;
    }

    @Override
    public void run() {
      for (int i = 0; i < keyCount; i++) {
        Set<String> uniqueIds = ImmutableSet.of(name + i);
        keyService.generateKey(uniqueIds, "boo");
      }
    }
  }
}
