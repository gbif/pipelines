package org.gbif.pipelines.keygen;

import java.io.IOException;
import java.util.UUID;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.junit.rules.ExternalResource;

@Slf4j
@Getter
public class HbaseServer extends ExternalResource {

  static final String A = "a";
  static final String B = "b";
  static final String C = "c";

  public static final KeygenConfig CFG =
      KeygenConfig.builder()
          .counterTable("test_occurrence_counter")
          .lookupTable("test_occurrence_lookup")
          .occurrenceTable("test_occurrence")
          .zkConnectionString(null)
          .create();

  static final byte[] CF = Bytes.toBytes("o");
  static final byte[] LOOKUP_TABLE = Bytes.toBytes(CFG.getLookupTable());
  private static final byte[] COUNTER_TABLE = Bytes.toBytes(CFG.getCounterTable());
  private static final byte[] COUNTER_CF = Bytes.toBytes("o");
  private static final byte[] OCCURRENCE_TABLE = Bytes.toBytes(CFG.getOccurrenceTable());

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  Connection connection;

  HBaseLockingKeyService keyService;

  public void truncateTable() throws IOException {
    log.info("Trancate the table");
    TEST_UTIL.truncateTable(LOOKUP_TABLE);
    TEST_UTIL.truncateTable(COUNTER_TABLE);
    TEST_UTIL.truncateTable(OCCURRENCE_TABLE);

    keyService = new HBaseLockingKeyService(CFG, connection, UUID.randomUUID().toString());
  }

  @Override
  protected void before() throws Exception {
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

  @SneakyThrows
  @Override
  protected void after() {
    TEST_UTIL.shutdownMiniCluster();
    if (connection != null) {
      connection.close();
    }
  }
}
