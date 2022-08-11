package org.gbif.pipelines.diagnostics.common;

import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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

  private static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final String COUNTER_CF_NAME = "o";
  private static final byte[] COUNTER_CF = Bytes.toBytes(COUNTER_CF_NAME);
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccurrenceTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private HBaseStore<String> lookupTableStore = null;
  private Connection connection = null;

  HBaseLockingKeyService keyService;

  public void truncateTable() throws IOException {
    log.info("Trancate the table");
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    keyService = new HBaseLockingKeyService(CFG, connection);
  }

  @Override
  protected void before() throws Exception {
    log.info("Create hbase mini-cluster");
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

    lookupTableStore =
        new HBaseStore<>(
            CFG.getLookupTable(),
            Columns.OCCURRENCE_COLUMN_FAMILY,
            connection,
            HBaseLockingKeyService.NUMBER_OF_BUCKETS);
  }

  @SneakyThrows
  @Override
  protected void after() {
    log.info("Shut down hbase mini-cluster");
    TEST_UTIL.shutdownMiniCluster();
    if (connection != null) {
      connection.close();
    }
  }
}
