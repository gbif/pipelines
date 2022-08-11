package org.gbif.pipelines.keygen;

import static org.gbif.pipelines.keygen.HBaseLockingKeyService.NUMBER_OF_BUCKETS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.gbif.pipelines.keygen.hbase.Columns;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HBaseLockingKeyServiceIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testNoContention() {
    Set<String> uniqueIds = new HashSet<>();
    uniqueIds.add(HbaseServer.A);
    uniqueIds.add(HbaseServer.B);
    uniqueIds.add(HbaseServer.C);
    KeyLookupResult result = HBASE_SERVER.keyService.generateKey(uniqueIds, "boo");
    assertEquals(1, result.getKey());
    assertTrue(result.isCreated());

    Optional<KeyLookupResult> result2 = HBASE_SERVER.keyService.findKey(uniqueIds, "boo");
    assertTrue(result2.isPresent());
    assertEquals(1, result2.get().getKey());
    assertFalse(result2.get().isCreated());
  }

  @Test
  public void testAddOccIdToExistingTriplet() throws IOException {
    // setup: 1 finalized row, the triplet

    String datasetKey = UUID.randomUUID().toString();
    String triplet = "IC|CC|CN|null";

    byte[] lookupKey1 = HBaseStore.saltKey(datasetKey + "|" + triplet, NUMBER_OF_BUCKETS);
    Put put = new Put(lookupKey1);
    put.addColumn(
        HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
    try (Table lookupTable =
        HBASE_SERVER.connection.getTable(TableName.valueOf(HbaseServer.LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    // test: keygen attempt uses previous unique id and a new "occurrenceId", expects the existing
    // key to be returned
    KeyLookupResult result =
        HBASE_SERVER.keyService.generateKey(
            new HashSet<>(Arrays.asList(triplet, "ABCD")), datasetKey);
    assertEquals(2, result.getKey());
    assertFalse(result.isCreated());
  }

  @Test
  public void testSimpleIdContig() {
    KeyLookupResult result = null;
    for (int i = 0; i < 1000; i++) {
      Set<String> uniqueIds = Collections.singleton(String.valueOf(i));
      result = HBASE_SERVER.keyService.generateKey(uniqueIds, "boo");
    }
    assertEquals(1000, result.getKey());
  }

  @Test
  public void testResumeCountAfterFailure() {
    KeyLookupResult result = null;
    for (int i = 0; i < 1001; i++) {
      Set<String> uniqueIds = Collections.singleton(String.valueOf(i));
      result = HBASE_SERVER.keyService.generateKey(uniqueIds, "boo");
    }
    assertEquals(1001, result.getKey());

    // first one claimed up to 2000, then "died". On restart we claim 2000 to 3000.
    HBaseLockingKeyService keyService2 =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.connection);
    for (int i = 0; i < 5; i++) {
      Set<String> uniqueIds = Collections.singleton("A" + i);
      result = keyService2.generateKey(uniqueIds, "boo");
    }
    assertEquals(2005, result.getKey());
  }

  @Test
  public void testLockWriteDie() throws IOException {
    // setup: 2 rows, each one gets as far as writing the new id but "dies" before releasing lock
    String datasetKey = UUID.randomUUID().toString();

    byte[] lock1 = Bytes.toBytes(UUID.randomUUID().toString());
    byte[] lookupKey1 = Bytes.toBytes(datasetKey + "|ABCD");
    Put put = new Put(lookupKey1);
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock1);
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
    try (Table lookupTable =
        HBASE_SERVER.connection.getTable(TableName.valueOf(HbaseServer.LOOKUP_TABLE))) {
      lookupTable.put(put);
      byte[] lock2 = Bytes.toBytes(UUID.randomUUID().toString());
      byte[] lookupKey2 = Bytes.toBytes(datasetKey + "|EFGH");
      put = new Put(lookupKey2);
      put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock2);
      put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(3L));
      lookupTable.put(put);
    }

    // test: 3rd keygen attempt uses both previous unique ids, expects a new key to be generated
    KeyLookupResult result =
        HBASE_SERVER.keyService.generateKey(
            new HashSet<>(Arrays.asList("ABCD", "EFGH")), datasetKey);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testConflictingIds() throws IOException {
    // setup: 2 rows with different lookupkeys and assigned ids

    String datasetKey = "fakeuuid";

    byte[] lookupKey1 = HBaseStore.saltKey(datasetKey + "|ABCD", NUMBER_OF_BUCKETS);
    Put put = new Put(lookupKey1);
    put.addColumn(
        HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(1L));
    try (Table lookupTable =
        HBASE_SERVER.connection.getTable(TableName.valueOf(HbaseServer.LOOKUP_TABLE))) {
      lookupTable.put(put);

      byte[] lookupKey2 = HBaseStore.saltKey(datasetKey + "|EFGH", NUMBER_OF_BUCKETS);
      put = new Put(lookupKey2);
      put.addColumn(
          HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_STATUS_COLUMN), Bytes.toBytes("ALLOCATED"));
      put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), Bytes.toBytes(2L));
      lookupTable.put(put);
    }
    // test: gen id for one occ with both lookupkeys
    exception.expect(IllegalStateException.class);
    exception.expectMessage(
        "Found inconsistent occurrence keys in looking up unique identifiers:["
            + datasetKey
            + "|ABCD]=[1]["
            + datasetKey
            + "|EFGH]=[2]");
    HBASE_SERVER.keyService.generateKey(new HashSet<>(Arrays.asList("ABCD", "EFGH")), datasetKey);
  }

  @Test
  public void testStaleLock() throws IOException {
    String datasetKey = UUID.randomUUID().toString();
    // setup: lookupkey | null for status | uuid with old ts for lock | null for occurrence key

    byte[] lookupKey = Bytes.toBytes(datasetKey + "|ABCD");
    byte[] lock = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(lookupKey);
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), 0, lock);
    try (Table lookupTable =
        HBASE_SERVER.connection.getTable(TableName.valueOf(HbaseServer.LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    KeyLookupResult result =
        HBASE_SERVER.keyService.generateKey(Collections.singleton("ABCD"), datasetKey);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testValidLockBecomesStale() throws IOException {
    String datasetKey = UUID.randomUUID().toString();
    // setup: lookupkey | null for status | uuid with old ts for lock | null for occurrence key

    // now minus the stale timeout + 10 seconds to force retry
    long ts =
        System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5) + TimeUnit.SECONDS.toMillis(10);
    byte[] lookupKey = Bytes.toBytes(datasetKey + "|ABCD");
    byte[] lock = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(lookupKey);
    put.addColumn(HbaseServer.CF, Bytes.toBytes(Columns.LOOKUP_LOCK_COLUMN), ts, lock);
    try (Table lookupTable =
        HBASE_SERVER.connection.getTable(TableName.valueOf(HbaseServer.LOOKUP_TABLE))) {
      lookupTable.put(put);
    }

    KeyLookupResult result =
        HBASE_SERVER.keyService.generateKey(Collections.singleton("ABCD"), datasetKey);
    assertEquals(1, result.getKey());
  }

  @Test
  public void testThreadedIdContig() throws InterruptedException {
    // 5 threads concurrently allocated 1000 ids each, expect the next call to produce id 5001
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Thread thread =
          new Thread(new KeyRequester(1000, HBASE_SERVER.keyService, String.valueOf(i)));
      thread.start();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      thread.join();
    }
    KeyLookupResult result =
        HBASE_SERVER.keyService.generateKey(Collections.singleton("asdf"), "wqer");
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
        Set<String> uniqueIds = Collections.singleton(name + i);
        keyService.generateKey(uniqueIds, "boo");
      }
    }
  }
}
