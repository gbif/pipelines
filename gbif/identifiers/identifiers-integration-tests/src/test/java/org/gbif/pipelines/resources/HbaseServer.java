package org.gbif.pipelines.resources;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.hbase.Columns;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.junit.rules.ExternalResource;

@Slf4j
@Getter
public class HbaseServer extends ExternalResource {

  public static final KeygenConfig CFG =
      KeygenConfig.builder()
          .counterTable("test_occurrence_counter")
          .lookupTable("test_occurrence_lookup")
          .occurrenceTable("test_occurrence")
          .zkConnectionString(null)
          .create();

  public static final TableName LOOKUP_TABLE = TableName.valueOf(CFG.getLookupTable());
  public static final byte[] CF = Bytes.toBytes("o");
  private static final TableName COUNTER_TABLE = TableName.valueOf(CFG.getCounterTable());
  private static final byte[] COUNTER_CF = Bytes.toBytes("o");
  private static final TableName OCCURRENCE_TABLE = TableName.valueOf(CFG.getOccurrenceTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final Object MUTEX = new Object();
  private static volatile HbaseServer instance;

  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  private HBaseStore<String> lookupTableStore = null;
  public Connection connection = null;
  public HBaseLockingKeyService keyService;

  public static HbaseServer getInstance() {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new HbaseServer();
        }
      }
    }
    return instance;
  }

  public void truncateTable() throws IOException {
    log.info("Truncate the table");
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    keyService = new HBaseLockingKeyService(CFG, connection, UUID.randomUUID().toString());
  }

  @Override
  protected void before() throws Exception {
    if (COUNTER.get() == 0) {

      log.info("Create hbase mini-cluster");
      TEST_UTIL.startMiniCluster(2);
      CFG.setZkConnectionString(getZKString());
      TEST_UTIL.createTable(LOOKUP_TABLE, CF);
      TEST_UTIL.createTable(COUNTER_TABLE, COUNTER_CF);
      TEST_UTIL.createTable(OCCURRENCE_TABLE, CF);
      connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());

      lookupTableStore =
          new HBaseStore<>(
              CFG.getLookupTable(),
              Columns.OCCURRENCE_COLUMN_FAMILY,
              connection,
              HBaseLockingKeyService.NUMBER_OF_BUCKETS);
    }
    COUNTER.addAndGet(1);
  }

  @SneakyThrows
  @Override
  protected void after() {
    if (COUNTER.addAndGet(-1) == 0) {
      log.info("Shut down hbase mini-cluster");
      TEST_UTIL.shutdownMiniCluster();
      if (connection != null) {
        connection.close();
      }
    }
  }

  public String getZKString() {
    return TEST_UTIL.getConfiguration().get("hbase.zookeeper.quorum");
  }

  public String getZNodeParent() {
    return TEST_UTIL.getConfiguration().get("zookeeper.znode.parent");
  }
}
